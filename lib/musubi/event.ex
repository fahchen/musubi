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
    events =
      socket
      |> Socket.get_private(@accumulator_key, [])
      |> Enum.reverse()
      |> Enum.map(fn {name, payload} -> %{name: name, payload: Wire.to_wire(payload)} end)

    {events, Socket.put_private(socket, @accumulator_key, [])}
  end
end
