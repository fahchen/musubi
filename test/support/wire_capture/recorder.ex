defmodule Musubi.WireCapture.Recorder do
  @moduledoc false
  # Drives one `Musubi.Transport.ConnectionChannel` scenario and records every
  # frame that crossed the socket, in order, as the JSON shape
  # `docs/rust-client.md` §12 specifies.
  #
  # `dir` is relative to the **client**: `out` is what the client sent
  # (`phx_join`, `command`, `allow_upload`, `phx_leave`, …), `in` is what the
  # server pushed back (`phx_reply` and the `"patch"` push).
  #
  # It deliberately does not use `ExUnit.Assertions`: `Phoenix.ChannelTest`'s
  # `subscribe_and_join/4`, `push/3` and `leave/1` are plain functions over a
  # `%Phoenix.Socket{}`, and `assert_push/2` is only `assert_receive` over a
  # `%Phoenix.Socket.Message{}`. Draining the mailbox by hand keeps the whole
  # harness usable from a Mix task, with no ExUnit application booted.
  #
  # The captured JSON is a `git diff --exit-code` drift gate, so output is
  # normalized to be byte-identical across runs — what varies and how it is
  # normalized is `docs/rust-client.md` §12's "Determinism" bullet; the entry-ref
  # renumbering itself is `normalize_entry_refs/1` below.

  alias Musubi.Transport.ConnectionChannel
  alias Musubi.WireCapture.Endpoint
  alias Musubi.WireCapture.Socket, as: CaptureSocket

  @topic_prefix "musubi:connection:"

  # How long the mailbox has to stay quiet before a client frame counts as
  # fully answered. Small on purpose: `sync/1`'s barrier (below) has already
  # delivered every envelope the frame caused by the time the drain starts, so
  # the wait only covers work genuinely off the command path — an
  # `assign_async` task — and those scenarios say so explicitly with `await/1`.
  @drain_ms 25

  # Drain window for a frame with no page server to sync against (a rejected
  # join), and the window `await/1` raises. Generous on purpose: it is used a
  # handful of times per capture, and it is the only thing standing between a
  # loaded machine and a spuriously truncated fixture.
  @slow_drain_ms 500

  defstruct [:socket, :page_pid, :root_id, frames: [], expected_state: :__unset__]

  @type t() :: %__MODULE__{
          socket: Phoenix.Socket.t() | nil,
          page_pid: pid() | nil,
          root_id: String.t() | nil,
          frames: [map()],
          expected_state: term()
        }

  @doc """
  Runs `fun` against a fresh recorder and returns the finished scenario map:
  `%{"scenario" => name, "frames" => [...], "expected_state" => ...}`.
  """
  @spec capture(String.t(), (t() -> t())) :: map()
  def capture(name, fun) when is_binary(name) and is_function(fun, 1) do
    recorder = fun.(%__MODULE__{})
    recorder = if recorder.expected_state == :__unset__, do: snapshot(recorder), else: recorder

    normalize_entry_refs(%{
      "scenario" => name,
      "frames" => Enum.reverse(recorder.frames),
      "expected_state" => recorder.expected_state
    })
  end

  @doc """
  Joins one root's channel. Join **is** the mount, so this records the
  `phx_join` out-frame, the join reply, and every patch the mount pushed.
  """
  @spec join(t(), module(), String.t(), map()) :: t()
  def join(%__MODULE__{} = recorder, module, id, params) do
    module_str = inspect(module)
    root_id = module_str <> ":" <> id
    payload = %{"module" => module_str, "id" => id, "params" => params}

    recorder = record_out(recorder, "phx_join", payload)

    case Phoenix.ChannelTest.subscribe_and_join(
           connect(),
           ConnectionChannel,
           @topic_prefix <> root_id,
           payload
         ) do
      {:ok, reply, socket} ->
        %{
          recorder
          | socket: socket,
            root_id: root_id,
            page_pid: socket.assigns[:__musubi_root__].pid
        }
        |> record_in("phx_reply", reply_payload(:ok, reply))
        |> drain()

      {:error, reason} ->
        record_in(recorder, "phx_reply", reply_payload(:error, reason))
    end
  end

  @doc "Pushes a `command` frame and records its reply plus any patch."
  @spec command(t(), [String.t()], String.t(), map()) :: t()
  def command(%__MODULE__{} = recorder, store_id, name, payload) do
    push(recorder, "command", %{
      "store_id" => store_id,
      "name" => name,
      "payload" => payload
    })
  end

  @doc "Pushes any client event verbatim (`allow_upload`, `upload_progress`, …)."
  @spec push(t(), String.t(), map()) :: t()
  def push(%__MODULE__{socket: %Phoenix.Socket{} = socket} = recorder, event, payload) do
    recorder = record_out(recorder, event, payload)
    _ref = Phoenix.ChannelTest.push(socket, event, payload)

    drain(recorder)
  end

  @doc "Leaves the channel, stopping the root server-side."
  @spec leave(t()) :: t()
  def leave(%__MODULE__{socket: %Phoenix.Socket{} = socket} = recorder) do
    recorder = record_out(recorder, "phx_leave", %{})
    _ref = Phoenix.ChannelTest.leave(socket)

    %{drain(recorder) | socket: nil, page_pid: nil}
  end

  @doc """
  Pins `expected_state` to the server's shadow tree **as of now** rather than
  at the end of the scenario. Needed by the version-gap fixture, where the
  client is expected to reject the last envelope and keep its previous
  document.
  """
  @spec snapshot(t()) :: t()
  def snapshot(%__MODULE__{page_pid: nil} = recorder), do: %{recorder | expected_state: nil}

  def snapshot(%__MODULE__{page_pid: pid} = recorder) do
    # `previous_wire_root` is the wire-form tree the last emitted envelope
    # brought the client to — precisely what a client's patched document holds
    # before hydration, and authored by the server rather than replayed from
    # the very ops under test.
    %{recorder | expected_state: :sys.get_state(pid).previous_wire_root}
  end

  @doc """
  Waits out one more slow drain window, for scenarios whose next envelope is
  produced off the command path (an `assign_async` task settling).
  """
  @spec await(t()) :: t()
  def await(%__MODULE__{} = recorder), do: drain(recorder, @slow_drain_ms)

  @doc """
  Drops the recorded `in` frame at `index` (0-based over `in` frames only).
  The only way to capture a version gap: the server never emits one, a dropped
  push is what produces it on the wire.
  """
  @spec drop_in_frame(t(), non_neg_integer()) :: t()
  def drop_in_frame(%__MODULE__{frames: frames} = recorder, index) do
    kept =
      frames
      |> Enum.reverse()
      |> Enum.map_reduce(0, fn
        %{"dir" => "in"} = frame, seen -> {{seen != index, frame}, seen + 1}
        frame, seen -> {{true, frame}, seen}
      end)
      |> elem(0)
      |> Enum.filter(&elem(&1, 0))
      |> Enum.map(&elem(&1, 1))
      |> Enum.reverse()

    %{recorder | frames: kept}
  end

  # ---------------------------------------------------------------------------
  # Transport
  # ---------------------------------------------------------------------------

  # `Phoenix.ChannelTest.socket/4` is a macro needing `@endpoint` and an
  # `ExUnit.OnExitHandler` supervisor, neither of which a Mix task has. The
  # struct it builds is public, so build it directly over a supervisor we own:
  # `Phoenix.ChannelTest.join/4` only reads `{Phoenix.ChannelTest, sup}` out of
  # `:transport` to start the channel process under it.
  defp connect do
    session = %{"test_pid" => self(), "user_id" => "u1"}
    connect_info = %{session: session, peer_data: %{address: {127, 0, 0, 1}}}

    # `:transport` carries `{Phoenix.ChannelTest, sup}` — what the test helper
    # puts there and what `Phoenix.ChannelTest.join/4` reads back out, even
    # though `Phoenix.Socket.t()` declares `transport: atom`. That upstream
    # mismatch is why `.dialyzer_ignore.exs` has an entry for this file.
    phoenix_socket = %Phoenix.Socket{
      assigns: %{},
      endpoint: Endpoint,
      handler: CaptureSocket,
      id: nil,
      pubsub_server: Endpoint.config(:pubsub_server),
      serializer: Phoenix.ChannelTest.NoopSerializer,
      transport: {Phoenix.ChannelTest, channel_supervisor()},
      transport_pid: self()
    }

    {:ok, connected} =
      CaptureSocket.connect(%{"current_user" => "connect-user"}, phoenix_socket, connect_info)

    connected
  end

  defp channel_supervisor do
    case Process.get(__MODULE__) do
      nil ->
        opts = [strategy: :one_for_one, max_restarts: 1_000_000, max_seconds: 1]
        {:ok, sup} = Supervisor.start_link([], opts)
        Process.put(__MODULE__, sup)
        sup

      sup ->
        sup
    end
  end

  # Barrier: a synchronous call into a GenServer returns only after every
  # message queued ahead of it has been handled. Two hops need it, in the order
  # the envelope travels them: the page server, whose render cycle sends the
  # envelope to the channel before returning, and the channel, which is what
  # actually `push/3`es it into our mailbox. Once both return, every envelope
  # this frame caused has been delivered and the drain window only has to cover
  # work that is genuinely off the command path.
  defp sync(%__MODULE__{page_pid: nil} = recorder), do: recorder

  defp sync(%__MODULE__{page_pid: pid, socket: socket} = recorder) do
    barrier(pid)
    barrier(channel_pid(socket))

    recorder
  end

  defp channel_pid(%Phoenix.Socket{channel_pid: pid}), do: pid
  defp channel_pid(_socket), do: nil

  # `leave/1` is itself a frame, and it stops the channel: a barrier that races
  # that exit has, by definition, nothing left to flush.
  defp barrier(pid) when is_pid(pid) do
    :sys.get_state(pid)
  catch
    :exit, _reason -> :ok
  end

  defp barrier(_pid), do: :ok

  # Collects everything the server sent until the mailbox goes quiet for
  # `timeout`. Anything that is not a channel frame (the fixture stores'
  # `send(test_pid, ...)` observation hooks, `:DOWN`s, …) is discarded.
  defp drain(recorder, timeout \\ @drain_ms)

  defp drain(%__MODULE__{page_pid: pid} = recorder, timeout) when is_pid(pid) do
    recorder |> sync() |> collect(timeout)
  end

  defp drain(recorder, _timeout), do: collect(recorder, @slow_drain_ms)

  defp collect(recorder, timeout) do
    receive do
      %Phoenix.Socket.Message{event: event, payload: payload} ->
        recorder |> record_in(event, payload) |> collect(timeout)

      %Phoenix.Socket.Reply{status: status, payload: payload} ->
        recorder |> record_in("phx_reply", reply_payload(status, payload)) |> collect(timeout)

      _other ->
        collect(recorder, timeout)
    after
      timeout -> recorder
    end
  end

  # ---------------------------------------------------------------------------
  # Frames
  # ---------------------------------------------------------------------------

  defp record_out(recorder, event, payload), do: record(recorder, "out", event, payload)
  defp record_in(recorder, event, payload), do: record(recorder, "in", event, payload)

  defp record(%__MODULE__{frames: frames} = recorder, dir, event, payload) do
    frame = %{"dir" => dir, "event" => event, "payload" => jsonable(payload)}

    %{recorder | frames: [frame | frames]}
  end

  # Phoenix replies are `{status, payload}` on the wire.
  defp reply_payload(status, payload) do
    %{"status" => to_string(status), "response" => payload}
  end

  # ---------------------------------------------------------------------------
  # Normalization
  # ---------------------------------------------------------------------------

  # Everything the runtime hands back is already JSON-shaped except atom keys
  # and atom values, which the real serializer would encode as strings.
  defp jsonable(%{__struct__: _module} = struct), do: struct |> Map.from_struct() |> jsonable()

  defp jsonable(map) when is_map(map),
    do: Map.new(map, fn {key, value} -> {jsonable_key(key), jsonable(value)} end)

  defp jsonable(list) when is_list(list), do: Enum.map(list, &jsonable/1)
  defp jsonable(atom) when is_atom(atom) and atom not in [nil, true, false], do: to_string(atom)
  defp jsonable(tuple) when is_tuple(tuple), do: tuple |> Tuple.to_list() |> jsonable()
  defp jsonable(other), do: other

  defp jsonable_key(key) when is_atom(key), do: Atom.to_string(key)
  defp jsonable_key(key), do: key

  # Upload entry refs are `"u_" <> Base.url_encode64(strong_rand_bytes(8))`, so
  # they differ on every run. Renumber them in first-appearance order over the
  # scenario's serialized form, which is stable because the frame list is.
  defp normalize_entry_refs(scenario) do
    encoded = Jason.encode!(scenario)

    ~r/u_[A-Za-z0-9_-]{11}/
    |> Regex.scan(encoded)
    |> List.flatten()
    |> Enum.uniq()
    |> Enum.with_index(1)
    |> Enum.reduce(encoded, fn {ref, index}, acc ->
      String.replace(acc, ref, "u_" <> String.pad_leading(Integer.to_string(index), 4, "0"))
    end)
    |> Jason.decode!()
  end
end
