defmodule Musubi.Event do
  @moduledoc """
  Transient server-to-client push events (BDR-0032).

  `push_event/3` queues a fire-and-forget `{name, payload}` on the socket, the
  same accumulate-on-socket pattern as `Musubi.Stream`. Events are per-store:
  during `:after_serialize` aggregation the page server drains every store socket
  via `flush_pending/1`, stamps each event with the socket's `store_id`, and folds
  them into `Musubi.Page.PatchEnvelope.events`, so one `"patch"` push carries
  diff + events. The client dispatches per `(store_id, name)`.

  Events own no version, ack, or retry: they ride the envelope, are dispatched
  once on the client, and are not replayed on reconnect. An event-only cycle
  still emits an envelope and bumps `version`.
  """

  alias Musubi.Schema
  alias Musubi.Socket
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
  Validates each drained event's wire payload against `module`'s declared `event`
  schema (BDR-0032 dev-correctness, mirroring `Musubi.Hooks.ValidateReplySchema`).
  Events are per-store, so `module` is the store socket that queued them.
  `Musubi.Hooks.ValidateEvents` calls this per socket at the `:after_serialize`
  stage (attached to every store socket via `config :musubi, :store_hooks`).

  Undeclared event names are skipped (a push with no matching `event` declaration
  is not validated). A declared event whose payload is missing a field or has a
  type mismatch raises `ArgumentError` (BDR-0003 let-it-crash) — there is no
  *security* validation here (events are server-pushed, trusted); this only
  catches developer mistakes. Returns `events` unchanged.
  """
  @spec validate_events!([event()], module()) :: [event()]
  def validate_events!(events, module) when is_list(events) and is_atom(module) do
    declared =
      if function_exported?(module, :__musubi__, 1),
        do: List.wrap(module.__musubi__(:events)),
        else: []

    Enum.each(events, fn %{name: name, payload: payload} ->
      case Enum.find(declared, &(to_string(&1.name) == name)) do
        %{payload_fields: fields} -> validate_fields!(module, name, fields, payload)
        nil -> :ok
      end
    end)

    events
  end

  @spec validate_fields!(module(), String.t(), [map()], term()) :: :ok
  defp validate_fields!(_module, _name, [], _payload), do: :ok

  defp validate_fields!(module, name, _fields, payload) when not is_map(payload) do
    raise ArgumentError,
          "push event validation failed for #{inspect(module)} event #{inspect(name)}: " <>
            "payload must be a map, got: #{inspect(payload)}"
  end

  defp validate_fields!(module, name, fields, payload) do
    case Schema.collect_field_errors(fields, payload, module) do
      [] ->
        :ok

      errors ->
        details = Enum.map_join(errors, "; ", fn {f, m} -> "#{f}: #{m}" end)

        raise ArgumentError,
              "push event validation failed for #{inspect(module)} event #{inspect(name)}: #{details}"
    end
  end
end
