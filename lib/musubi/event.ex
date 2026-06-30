defmodule Musubi.Event do
  @moduledoc """
  Transient server-to-client push events (BDR-0032).

  `push_event/3` queues a fire-and-forget `{name, payload}` on the socket, the
  same accumulate-on-socket pattern as `Musubi.Stream`. The page server drains
  every store socket once per render cycle via `flush_pending/1` and folds the
  events into `Musubi.Page.PatchEnvelope.events`, so one `"patch"` push carries
  diff + events.

  Events own no version, ack, or retry: they ride the envelope, are dispatched
  once on the client, and are not replayed on reconnect. An event-only cycle
  still emits an envelope and bumps `version`.
  """

  alias Musubi.Socket
  alias Musubi.Type
  alias Musubi.Wire

  @accumulator_key :__musubi_events__

  @typedoc "Wire-shape push event folded into the patch envelope."
  @type event() :: %{name: String.t(), payload: term()}

  @doc """
  Queues a transient event on the socket. Returns the socket for pipe-chaining.

  `name` is an atom or string (stringified); `payload` is any wire-encodable
  term, serialized at flush via `Musubi.Wire.to_wire/1`.

  ## Examples

      socket = Musubi.Event.push_event(socket, :toast, %{msg: "saved"})
  """
  @spec push_event(Socket.t(), atom() | String.t(), term()) :: Socket.t()
  def push_event(%Socket{} = socket, name, payload) when is_atom(name) or is_binary(name) do
    pending = Socket.get_private(socket, @accumulator_key, [])
    Socket.put_private(socket, @accumulator_key, [{to_string(name), payload} | pending])
  end

  @doc """
  Drains the queued events for this cycle in FIFO order, wire-serializing each
  payload, and clears the accumulator.

  Called by the page runtime once per render cycle.

  ## Examples

      {events, socket} = Musubi.Event.flush_pending(socket)
  """
  @spec flush_pending(Socket.t()) :: {[event()], Socket.t()}
  def flush_pending(%Socket{} = socket) do
    case Socket.get_private(socket, @accumulator_key, []) do
      # Leave the socket untouched when nothing was queued, so a flush does not
      # churn private on every event-free render cycle.
      [] ->
        {[], socket}

      pending ->
        events =
          pending
          |> Enum.reverse()
          |> Enum.map(fn {name, payload} -> %{name: name, payload: Wire.to_wire(payload)} end)

        {events, Socket.put_private(socket, @accumulator_key, [])}
    end
  end

  @doc """
  Validates each drained event's wire payload against the root store's declared
  `event` schema (BDR-0032 dev-correctness, mirroring
  `Musubi.Hooks.ValidateReplySchema`). Events are declared on the root store, so
  `root_module` is the root regardless of which socket queued the event.

  Undeclared event names are skipped (a push with no matching `event` declaration
  is not validated). A declared event whose payload is missing a field or has a
  type mismatch raises `ArgumentError` (BDR-0003 let-it-crash) — there is no
  *security* validation here (events are server-pushed, trusted); this only
  catches developer mistakes. Returns `events` unchanged.
  """
  @spec validate_events!([event()], module()) :: [event()]
  def validate_events!(events, root_module) when is_list(events) and is_atom(root_module) do
    declared = declared_event_index(root_module)

    Enum.each(events, fn %{name: name, payload: payload} ->
      case Map.fetch(declared, name) do
        {:ok, fields} -> validate_fields!(root_module, name, fields, payload)
        :error -> :ok
      end
    end)

    events
  end

  @spec declared_event_index(module()) :: %{String.t() => [map()]}
  defp declared_event_index(root_module) do
    if function_exported?(root_module, :__musubi__, 1) do
      events = List.wrap(root_module.__musubi__(:events))
      Map.new(events, fn %{name: name, payload_fields: fields} -> {to_string(name), fields} end)
    else
      %{}
    end
  end

  @spec validate_fields!(module(), String.t(), [map()], map()) :: :ok
  defp validate_fields!(module, name, fields, payload) do
    errors = Enum.reduce(fields, [], &collect_field_error(&1, payload, module, &2))

    case errors do
      [] ->
        :ok

      list ->
        details = list |> Enum.reverse() |> Enum.map_join("; ", fn {f, m} -> "#{f}: #{m}" end)

        raise ArgumentError,
              "push event validation failed for #{inspect(module)} event #{inspect(name)}: #{details}"
    end
  end

  @spec collect_field_error(map(), map(), module(), [{atom(), String.t()}]) ::
          [{atom(), String.t()}]
  defp collect_field_error(%{name: fname, type: type_ast}, payload, module, acc) do
    case Map.fetch(payload, to_string(fname)) do
      {:ok, value} ->
        if Type.valid?(value, type_ast, module),
          do: acc,
          else: [{fname, "expected #{Macro.to_string(type_ast)}, got: #{inspect(value)}"} | acc]

      :error ->
        [{fname, "missing required field"} | acc]
    end
  end
end
