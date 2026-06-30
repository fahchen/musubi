defmodule Musubi.Testing do
  @moduledoc """
  Test entry point for Musubi root stores, analogous to
  `Phoenix.LiveViewTest`. Wraps `Musubi.Page.Server.start_link/1` with
  test-friendly defaults and exposes the primary assertion surface
  (`render/2`).

  ## Primary surface

  Assert against the rendered wire-shape map — the same contract a
  client observes — not internal `socket.assigns`. `assigns/2` is an
  escape hatch for state not surfaced through `render/1`; prefer
  `render/2` for contract assertions.

  ## Example

      page = Musubi.Testing.mount(RoomStore, %{"room_code" => "AB12"})
      Musubi.Testing.dispatch_command(page, :ko, %{target: "p2"})

      assert Musubi.Testing.render(page) == %{
        winner: :p1,
        hp: %{p1: 100, p2: 0}
      }
  """

  alias Musubi.Page.PatchEnvelope
  alias Musubi.Page.Server
  alias Musubi.Socket
  alias Musubi.Wire

  defstruct [:pid, :root, :transport]

  @typedoc "Handle returned by `mount/3`; passed back into the other helpers."
  @type t :: %__MODULE__{pid: pid(), root: module(), transport: pid()}

  @doc """
  Mounts `module` as a root page. Push patches are delivered to the
  calling process; consume them with `ExUnit.Assertions.assert_receive/2`.
  Tears down on test exit via `start_supervised!`.

  ## Options

    * `:transport_pid` — pid that receives push patches. Defaults to `self()`.
  """
  @spec mount(module(), map(), keyword()) :: t()
  def mount(module, params \\ %{}, opts \\ []) when is_atom(module) and is_map(params) do
    transport = Keyword.get(opts, :transport_pid, self())

    pid =
      ExUnit.Callbacks.start_supervised!({Server, {module, params, %{transport_pid: transport}}})

    %__MODULE__{pid: pid, root: module, transport: transport}
  end

  @doc """
  Dispatches a command to a mounted store. Defaults to the root
  (`store_id: []`); pass a child path to address a nested store.

  Mirrors the client-side `proxy.dispatchCommand(name, payload)`
  contract.

  `payload` is given in native Elixir shape and wire-encoded via
  `Musubi.Wire.to_wire/1` before dispatch, so the store's
  `handle_command/3` receives the same string-keyed map a real client
  would deliver over the wire. Write `%{by: 3}` (or `%{"by" => 3}` —
  the encode is idempotent on wire data); atom keys and atom values are
  normalized to strings, symmetric with the egress encoding of replies.
  """
  @spec dispatch_command(t(), atom(), map(), Socket.store_id()) ::
          {:ok, map()} | {:error, term()}
  def dispatch_command(%__MODULE__{pid: pid}, name, payload, store_id \\ [])
      when is_atom(name) and is_map(payload) and is_list(store_id) do
    Server.command(pid, store_id, name, Wire.to_wire(payload))
  end

  @doc """
  Runs the addressed store's `render/1` against its current socket and
  returns the wire-shape map. Primary assertion surface — what the
  client would observe after the next reconcile.

  Values are returned as native Elixir terms (atom literals stay atoms);
  the JSON-string transformation happens on the way out to the client,
  not inside `render/1`.
  """
  @spec render(t(), Socket.store_id()) :: map()
  def render(%__MODULE__{pid: pid}, store_id \\ []) when is_list(store_id) do
    {:ok, %{socket: socket, module: module}} = Server.peek(pid, store_id)
    module.render(socket)
  end

  @doc """
  Returns the raw `socket.assigns` for the addressed store.

  Escape hatch — prefer `render/2` for contract assertions. Use only
  when the value you need is not exposed through `render/1` (e.g. a
  private field captured for later use).
  """
  @spec assigns(t(), Socket.store_id()) :: map()
  def assigns(%__MODULE__{pid: pid}, store_id \\ []) when is_list(store_id) do
    {:ok, %{socket: socket}} = Server.peek(pid, store_id)
    socket.assigns
  end

  @doc """
  Runs the `allow_upload` preflight against the mounted page and
  returns the reply.

  Bypasses the transport channel — useful for tests that exercise the
  preflight + entry add-op path without a Phoenix channel in the way.

  ## Options

    * `:endpoint` — Phoenix endpoint module used to sign tokens.
      Defaults to `Musubi.Testing.TestEndpoint`, a stub endpoint
      automatically registered in test mode.
  """
  @spec allow_upload(t(), atom(), [map()], keyword(), Socket.store_id()) ::
          {:ok, Musubi.Page.Server.preflight_reply()} | {:error, atom()}
  def allow_upload(%__MODULE__{pid: pid}, name, entries, opts \\ [], store_id \\ [])
      when is_atom(name) and is_list(entries) and is_list(opts) and is_list(store_id) do
    endpoint =
      Keyword.get_lazy(opts, :endpoint, fn ->
        raise ArgumentError,
              "Musubi.Testing.allow_upload/5 requires an :endpoint option for token signing"
      end)

    Server.allow_upload(pid, store_id, name, entries, endpoint)
  end

  @doc """
  Simulates a full upload for one entry: sends a single complete chunk
  via the page server's `upload_channel_chunk` API, then waits for the
  resulting patch envelope to land.
  """
  @spec simulate_upload(t(), atom(), String.t(), non_neg_integer(), Socket.store_id()) :: :ok
  def simulate_upload(%__MODULE__{pid: pid}, name, entry_ref, bytes_total, store_id \\ [])
      when is_atom(name) and is_binary(entry_ref) and is_integer(bytes_total) and
             is_list(store_id) do
    Server.upload_channel_chunk(pid, store_id, name, entry_ref, bytes_total, true)
    :ok
  end

  @doc """
  Targets a mounted child store with new assigns via `Musubi.send_update/3`
  (BDR-0030).

  The `assigns` map is delivered to the store's `update/2`, dirtying that
  subtree; the resulting coalesced patch lands on the transport pid for
  `ExUnit.Assertions.assert_receive/2`. A subsequent `render/2` or
  `assigns/2` observes the update — the peek `GenServer.call` syncs the
  page mailbox (FIFO), so the send is processed before the read returns.
  A `store_id` that is not mounted is a no-op (no patch is pushed).
  """
  @spec send_update(t(), Socket.store_id(), map()) :: :ok
  def send_update(%__MODULE__{pid: pid}, store_id, assigns)
      when is_list(store_id) and is_map(assigns) do
    Musubi.send_update(pid, store_id, assigns)
    :ok
  end

  @doc """
  Asserts a transient push event (BDR-0032) named `name` was delivered with
  `payload`, and returns the matched wire payload.

  Push events ride the patch envelope, so this scans `{:patch, _}` messages
  pushed to the test process and **consumes** the patches it scans past — assert
  any state patches you care about *before* asserting events, or assert the event
  first. `payload` is compared in wire shape (`Musubi.Wire.to_wire/1`), symmetric
  with `dispatch_command/4` (atom keys/values normalize to strings).

  Requires the page's `transport_pid` to be the test process (the `mount/3`
  default).

  ## Example

      page = Musubi.Testing.mount(InboxStore)
      Musubi.Testing.dispatch_command(page, :save, %{})
      Musubi.Testing.assert_push_event(:toast, %{msg: "Saved", level: :info})
  """
  @spec assert_push_event(atom() | String.t(), map(), non_neg_integer()) :: map()
  def assert_push_event(name, payload, timeout \\ 100)
      when (is_atom(name) or is_binary(name)) and is_map(payload) do
    name = to_string(name)
    expected = Wire.to_wire(payload)

    case scan_for_event(name, timeout) do
      %{payload: ^expected} = event ->
        event.payload

      %{payload: other} ->
        ExUnit.Assertions.flunk(
          "push event #{inspect(name)} payload mismatch\n" <>
            "  expected: #{inspect(expected)}\n  got:      #{inspect(other)}"
        )

      nil ->
        ExUnit.Assertions.flunk(
          "expected a push event named #{inspect(name)} within #{timeout}ms"
        )
    end
  end

  @doc """
  Asserts NO push event named `name` is delivered within `timeout`.

  Like `assert_push_event/3`, this consumes the patch messages it scans.
  """
  @spec refute_push_event(atom() | String.t(), non_neg_integer()) :: :ok
  def refute_push_event(name, timeout \\ 100) when is_atom(name) or is_binary(name) do
    name = to_string(name)

    case scan_for_event(name, timeout) do
      nil ->
        :ok

      event ->
        ExUnit.Assertions.flunk(
          "unexpected push event #{inspect(name)}: #{inspect(event.payload)}"
        )
    end
  end

  @spec scan_for_event(String.t(), non_neg_integer()) :: PatchEnvelope.event() | nil
  defp scan_for_event(name, timeout) do
    receive do
      {:patch, %PatchEnvelope{events: events}} ->
        case Enum.find(events, &(&1.name == name)) do
          nil -> scan_for_event(name, timeout)
          event -> event
        end
    after
      timeout -> nil
    end
  end

  @doc """
  Sends an external-mode progress event for an entry on the mounted page.
  """
  @spec simulate_external_progress(t(), atom(), String.t(), non_neg_integer(), Socket.store_id()) ::
          :ok
  def simulate_external_progress(
        %__MODULE__{pid: pid},
        name,
        entry_ref,
        progress,
        store_id \\ []
      )
      when is_atom(name) and is_binary(entry_ref) and is_integer(progress) and is_list(store_id) do
    Server.upload_progress(pid, store_id, name, entry_ref, progress)
    :ok
  end
end
