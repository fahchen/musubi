defmodule Musubi.Page.Server do
  @moduledoc """
  Page-scoped Musubi runtime GenServer. Hosts the store tree for one connected
  client session and runs the command pipeline (routing → `:before_command`
  hooks → `handle_command/3` → `:after_command` hooks → reply) per
  BDR-0007/0009.

  ## Cross-track contract (M3 → M4 → M5)

  Transport adapters call `command/4` to dispatch a single client command. The
  runtime returns the channel reply payload — `{:ok, reply_payload}` — which
  the transport forwards to the client. After the reply is sent, the runtime
  renders the tree, computes a JSON Patch diff against the previously-rendered
  wire root, accumulates stream ops queued during the handler, builds an
  `Musubi.Page.PatchEnvelope`, and pushes it to the bound transport pid via a
  `{:patch, envelope}` message on `handle_continue/2` (so the reply lands
  first per BDR-0009).

  At mount, the same flow runs once with `previous_wire_root: nil` — the
  initial envelope replaces the entire root path (`""`) with the freshly
  rendered wire root and starts the version counter at 1.

  Idle render cycles (no diff ops, no stream ops, no events) emit nothing per
  BDR-0018. A halted command (a `:before_command` hook returned `{:halt, ...}`)
  renders the same way — the push is content-driven, not halt-driven (BDR-0008):
  if the halting hook mutated state or queued stream/event ops those ship in one
  envelope; a pure denial that changed nothing emits nothing.
  """

  use GenServer

  require Logger

  alias Musubi.Async
  alias Musubi.Diff
  alias Musubi.Event
  alias Musubi.Hooks.ValidateCommandSchema
  alias Musubi.Lifecycle
  alias Musubi.Page.Frame
  alias Musubi.Page.PatchEnvelope
  alias Musubi.Page.Server.State
  alias Musubi.Page.StoreTable
  alias Musubi.Page.StoreTable.Entry
  alias Musubi.Reconciler
  alias Musubi.Resolver
  alias Musubi.Socket
  alias Musubi.Stream
  alias Musubi.Telemetry
  alias Musubi.Upload

  @type transport_opts() :: map()
  @type start_arg() ::
          {module(), map(), transport_opts()} | {module(), map(), Socket.t(), transport_opts()}
  @type store_id() :: Socket.store_id()
  @type command_name() :: Musubi.Store.command_name()
  @type command_payload() :: map()
  @type command_reply() :: map()
  @type command_error() :: :unknown_command | :unknown_store

  @doc """
  Starts one page-scoped runtime for the given root store module.

  ## Examples

      Musubi.Page.Server.start_link({MyApp.RootStore, %{"page_id" => "home"}, %{transport_pid: self()}})
      #=> {:ok, pid}
  """
  @spec start_link(start_arg()) :: GenServer.on_start()
  def start_link({root_module, _params, _transport_opts} = arg) when is_atom(root_module) do
    GenServer.start_link(__MODULE__, arg)
  end

  def start_link({root_module, _params, %Socket{}, _transport_opts} = arg)
      when is_atom(root_module) do
    GenServer.start_link(__MODULE__, arg)
  end

  @doc """
  Dispatches a command to the addressed store node and returns the channel
  reply payload.

  This is the public entry point a transport adapter calls to execute one
  client command end-to-end. The pipeline:

    1. Route `store_id` to a mounted store via `Musubi.Page.StoreTable.get/2`.
    2. Run `:before_command` hooks root-first along the chain of sockets.
    3. Dispatch `handle_command/3` on the addressed store module.
    4. Run `:after_command` hooks root-first along the chain.
    5. Return `{:ok, reply_payload}`.
    6. Render → diff → push patch envelope (after the reply lands).

  store_id or command-name resolution failures `raise` (BDR-0003 let-it-crash);
  the GenServer crashes and the supervisor/transport observe the exit.

  ## Examples

      Musubi.Page.Server.command(pid, ["filters"], :change_query, %{"query" => "shirt"})
      #=> {:ok, %{}}
  """
  @spec command(GenServer.server(), store_id(), command_name(), command_payload()) ::
          {:ok, command_reply()}
  def command(server, store_id, command_name, payload)
      when is_list(store_id) and is_atom(command_name) and is_map(payload) do
    GenServer.call(server, {:command, store_id, command_name, payload})
  end

  @doc """
  Dispatches a client command whose name arrived as a string.

  The command name is resolved against the addressed mounted store module's
  declared commands before dispatch, so transports do not create atoms from
  client input.

  ## Examples

      Musubi.Page.Server.command_by_name(pid, ["filters"], "change_query", %{"query" => "shirt"})
      #=> {:ok, %{}}

      Musubi.Page.Server.command_by_name(pid, [], "missing", %{})
      #=> {:error, :unknown_command}
  """
  @spec command_by_name(GenServer.server(), store_id(), String.t(), command_payload()) ::
          {:ok, command_reply()} | {:error, command_error()}
  def command_by_name(server, store_id, command_name, payload)
      when is_list(store_id) and is_binary(command_name) and is_map(payload) do
    GenServer.call(server, {:command_by_name, store_id, command_name, payload})
  end

  @doc false
  # Internal introspection used by `Musubi.Testing`. Returns the addressed
  # store node's current socket and module. Not part of the public runtime
  # surface — production code routes through commands, not direct reads.
  @spec peek(GenServer.server(), store_id()) ::
          {:ok, %{socket: Socket.t(), module: module()}} | {:error, :not_mounted}
  def peek(server, store_id) when is_list(store_id) do
    GenServer.call(server, {:peek, store_id})
  end

  @typedoc "Result of the `allow_upload` preflight."
  @type preflight_reply() :: %{
          required(:ref) => String.t(),
          required(:config) => %{String.t() => term()},
          required(:entries) => %{String.t() => map()},
          required(:errors) => [map()]
        }

  @doc """
  Runs the `allow_upload` preflight for `name` on the store at
  `store_id`. Returns the preflight reply on success.

  Side effects (atomic with the reply): per-entry `{op: add}` ops are
  enqueued and the next render cycle pushes an envelope carrying them.
  """
  @spec allow_upload(GenServer.server(), store_id(), atom(), [map()], module()) ::
          {:ok, preflight_reply()} | {:error, atom()}
  def allow_upload(server, store_id, name, entries, endpoint)
      when is_list(store_id) and is_atom(name) and is_list(entries) and is_atom(endpoint) do
    GenServer.call(server, {:allow_upload, store_id, name, entries, endpoint})
  end

  @doc """
  Cancels a single upload entry by ref. Kills the sub-channel pid if
  one was registered, and emits `{op: cancel}`.
  """
  @spec cancel_upload(GenServer.server(), store_id(), atom(), String.t()) :: :ok
  def cancel_upload(server, store_id, name, ref)
      when is_list(store_id) and is_atom(name) and is_binary(ref) do
    GenServer.call(server, {:cancel_upload, store_id, name, ref})
  end

  @doc """
  Reports external-mode progress for an entry. Enqueues `{op: progress}`
  (and `{op: complete}` when progress hits 100).

  Rejects with `{:error, :wrong_mode}` when the addressed entry is in
  channel mode — channel-mode progress only flows through the
  `Musubi.Transport.UploadChannel` chunk path, never through the main
  channel, so a forged `upload_progress` cannot mark a channel-mode
  upload complete without bytes.
  """
  @spec upload_progress(GenServer.server(), store_id(), atom(), String.t(), non_neg_integer()) ::
          :ok | {:error, :wrong_mode | :unknown_entry | :unknown_store | :unknown_upload}
  def upload_progress(server, store_id, name, ref, progress)
      when is_list(store_id) and is_atom(name) and is_binary(ref) and is_integer(progress) do
    GenServer.call(server, {:upload_progress, store_id, name, ref, progress})
  end

  @doc """
  Reports an external-mode upload failure (e.g. the registered uploader
  rejected the PUT, the network died mid-upload, or a presigned URL
  expired). Marks the entry `:error` and enqueues
  `{op: error, code, message}`.

  Rejects with `{:error, :wrong_mode}` for channel-mode entries — the
  sub-channel is the source of truth for channel-mode failures via
  `:upload_channel_error` and a forged client error must not be allowed
  to short-circuit it.
  """
  @spec upload_client_error(
          GenServer.server(),
          store_id(),
          atom(),
          String.t(),
          Musubi.Upload.Error.t()
        ) :: :ok | {:error, :wrong_mode | :unknown_entry | :unknown_store | :unknown_upload}
  def upload_client_error(server, store_id, name, ref, %Musubi.Upload.Error{} = error)
      when is_list(store_id) and is_atom(name) and is_binary(ref) do
    GenServer.call(server, {:upload_client_error, store_id, name, ref, error})
  end

  @doc """
  Records a channel-mode chunk write: updates the entry's bytes/progress
  and enqueues `{op: progress}` (and `{op: complete}` when the file is
  fully received).
  """
  @spec upload_channel_chunk(
          GenServer.server(),
          store_id(),
          atom(),
          String.t(),
          non_neg_integer(),
          boolean()
        ) :: :ok
  def upload_channel_chunk(server, store_id, name, ref, bytes_written, complete?)
      when is_list(store_id) and is_atom(name) and is_binary(ref) and
             is_integer(bytes_written) and is_boolean(complete?) do
    GenServer.cast(
      server,
      {:upload_channel_chunk, store_id, name, ref, bytes_written, complete?}
    )
  end

  @doc """
  Registers a sub-channel pid and its open temp-file path against the
  entry so chunk progress can be attributed and subsequent
  `consume_uploaded_entries/3` callers see a usable `%{path: path}`.
  """
  @spec register_upload_channel(
          GenServer.server(),
          store_id(),
          atom(),
          String.t(),
          pid(),
          String.t()
        ) :: :ok
  def register_upload_channel(server, store_id, name, ref, channel_pid, path)
      when is_list(store_id) and is_atom(name) and is_binary(ref) and is_pid(channel_pid) and
             is_binary(path) do
    GenServer.cast(server, {:register_upload_channel, store_id, name, ref, channel_pid, path})
  end

  @impl GenServer
  @spec init(start_arg()) ::
          {:ok, State.t(), {:continue, {:push_patch, PatchEnvelope.t() | nil}}}
  def init({root_module, params, transport_opts}) do
    root_socket =
      Socket.assign(
        %Socket{id: "", parent_path: [], module: root_module, assigns: %{}, private: %{}},
        params
      )

    init_root_runtime(root_module, params, root_socket, transport_opts)
  end

  def init({root_module, params, %Socket{} = root_socket, transport_opts}) do
    init_root_runtime(root_module, params, root_socket, transport_opts)
  end

  @spec init_root_runtime(module(), map(), Socket.t(), transport_opts()) ::
          {:ok, State.t(), {:continue, {:push_patch, PatchEnvelope.t() | nil}}}
  defp init_root_runtime(root_module, params, %Socket{} = root_socket, transport_opts) do
    Process.flag(:trap_exit, true)

    transport_pid =
      if is_map(transport_opts), do: Map.get(transport_opts, :transport_pid), else: nil

    root_socket =
      %{root_socket | id: "", parent_path: [], module: root_module, transport_pid: transport_pid}
      |> Socket.put_root_params(params)
      |> Lifecycle.attach_default_hooks()
      |> mount_root_store(params)
      |> normalize_root_assigns()
      |> Reconciler.init_store()

    store_table =
      StoreTable.put(StoreTable.new(), [], %Entry{
        socket: root_socket,
        module: root_module
      })

    {root_socket, store_table} = run_render_cycle(root_socket, store_table)

    {wire_root, events, store_table} = serialize_frames(store_table)
    {stream_ops, store_table} = flush_all_stream_ops(store_table)
    {upload_ops_raw, store_table} = flush_all_upload_ops(store_table)

    {upload_ops, upload_throttle} = throttle_progress(upload_ops_raw, %{})

    envelope = PatchEnvelope.initial(wire_root, stream_ops, upload_ops, events)

    state =
      rebuild_async_index(%State{
        root_module: root_module,
        root_socket: root_socket(store_table, root_socket),
        store_table: store_table,
        version: 1,
        previous_wire_root: wire_root,
        transport: transport_opts,
        upload_progress_last_emitted: upload_throttle
      })

    Telemetry.emit(
      [:musubi, :patch, :stop],
      %{
        count: length(envelope.ops),
        stream_count: length(envelope.stream_ops),
        upload_count: length(envelope.upload_ops)
      },
      %{module: root_module, version: state.version}
    )

    {:ok, state, {:continue, {:push_patch, envelope}}}
  end

  @spec mount_root_store(Socket.t(), map()) :: Socket.t()
  defp mount_root_store(%Socket{module: module} = socket, params)
       when is_atom(module) and is_map(params) do
    result =
      if root_store?(module) do
        module.mount(params, socket)
      else
        {:ok, socket}
      end

    validate_mount_result!(result, module, :mount, 2)
  end

  @spec root_store?(module()) :: boolean()
  defp root_store?(module) when is_atom(module) do
    module_exports?(module, :__musubi__, 1) and module.__musubi__(:root?)
  end

  # Defends against cold-VM races: BEAM lazy-loads modules, so a fresh test
  # VM (or any context where `module` has not yet been referenced) returns
  # `false` from `function_exported?/3` even though the .beam exists on
  # disk. `Code.ensure_loaded?/1` triggers the code server load first, then
  # `function_exported?/3` returns the truthful answer.
  @spec module_exports?(module(), atom(), arity()) :: boolean()
  defp module_exports?(module, fun, arity)
       when is_atom(module) and is_atom(fun) and is_integer(arity) do
    Code.ensure_loaded?(module) and function_exported?(module, fun, arity)
  end

  @spec normalize_root_assigns(Socket.t()) :: Socket.t()
  defp normalize_root_assigns(%Socket{module: module, assigns: assigns} = socket)
       when is_atom(module) and is_map(assigns) do
    %{socket | assigns: Reconciler.normalize_assigns(module, assigns)}
  end

  @spec validate_mount_result!({:ok, Socket.t()} | tuple(), module(), atom(), pos_integer()) ::
          Socket.t()
  defp validate_mount_result!({:ok, %Socket{} = socket}, module, fun, arity)
       when is_atom(module) and is_atom(fun) and is_integer(arity) do
    socket
  end

  defp validate_mount_result!(other, module, fun, arity)
       when is_atom(module) and is_atom(fun) and is_integer(arity) do
    raise ArgumentError,
          "bad callback response from #{inspect(module)}.#{fun}/#{arity}: expected {:ok, %Musubi.Socket{}}, got #{inspect(other)}"
  end

  @impl GenServer
  @spec handle_call(
          {:command, store_id(), command_name(), command_payload()}
          | {:command_by_name, store_id(), String.t(), command_payload()},
          GenServer.from(),
          State.t()
        ) ::
          {:reply, {:ok, command_reply()}, State.t(),
           {:continue, {:push_patch, PatchEnvelope.t() | nil}}}
          | {:reply, {:error, command_error()}, State.t()}
  def handle_call({:command, store_id, command_name, payload}, _from, %State{} = state) do
    handle_command_call(store_id, command_name, payload, state)
  end

  def handle_call({:command_by_name, store_id, command_name, payload}, _from, %State{} = state) do
    case resolve_command_name(state.store_table, store_id, command_name) do
      {:ok, resolved_name} -> handle_command_call(store_id, resolved_name, payload, state)
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:peek, store_id}, _from, %State{store_table: table} = state) do
    case StoreTable.get(table, store_id) do
      %Entry{socket: socket, module: module} ->
        {:reply, {:ok, %{socket: socket, module: module}}, state}

      nil ->
        {:reply, {:error, :not_mounted}, state}
    end
  end

  def handle_call({:allow_upload, store_id, name, entries, endpoint}, _from, %State{} = state) do
    case fetch_upload_target(state, store_id, name) do
      {:ok, %Entry{socket: socket, module: _module} = entry} ->
        result = Musubi.Upload.Preflight.run(socket, name, entries, endpoint, self(), store_id)
        next_state = put_entry_by_store_id(state, store_id, %{entry | socket: result.socket})
        {next_state, envelope} = render_and_envelope(next_state)
        reply = build_preflight_reply(result, name)
        {:reply, {:ok, reply}, next_state, {:continue, {:push_patch, envelope}}}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:upload_progress, store_id, name, ref, progress}, _from, %State{} = state) do
    case fetch_upload_target(state, store_id, name) do
      {:ok, %Entry{socket: socket}} ->
        case Musubi.Upload.fetch_entry(socket, name, ref) do
          {:ok, %Musubi.Upload.Entry{mode: :external}} ->
            {next_state, envelope} = apply_upload_progress(state, store_id, name, ref, progress)
            {:reply, :ok, next_state, {:continue, {:push_patch, envelope}}}

          {:ok, %Musubi.Upload.Entry{}} ->
            # Channel-mode entries can only progress through the
            # sub-channel chunk pipeline; reject any main-channel forge.
            {:reply, {:error, :wrong_mode}, state}

          :error ->
            {:reply, {:error, :unknown_entry}, state}
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:upload_client_error, store_id, name, ref, error}, _from, %State{} = state) do
    case fetch_upload_target(state, store_id, name) do
      {:ok, %Entry{socket: socket}} ->
        case Musubi.Upload.fetch_entry(socket, name, ref) do
          {:ok, %Musubi.Upload.Entry{mode: :external}} ->
            {next_state, envelope} = apply_upload_error(state, store_id, name, ref, error)
            {:reply, :ok, next_state, {:continue, {:push_patch, envelope}}}

          {:ok, %Musubi.Upload.Entry{}} ->
            {:reply, {:error, :wrong_mode}, state}

          :error ->
            {:reply, {:error, :unknown_entry}, state}
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:cancel_upload, store_id, name, ref}, _from, %State{} = state) do
    case fetch_upload_target(state, store_id, name) do
      {:ok, %Entry{socket: socket} = entry} ->
        socket =
          case Musubi.Upload.fetch_entry(socket, name, ref) do
            {:ok, upload_entry} ->
              maybe_kill_upload_channel(upload_entry)
              Musubi.Upload.cancel_upload(socket, name, ref)

            :error ->
              socket
          end

        next_state = put_entry_by_store_id(state, store_id, %{entry | socket: socket})
        {next_state, envelope} = render_and_envelope(next_state)
        {:reply, :ok, next_state, {:continue, {:push_patch, envelope}}}

      {:error, _reason} ->
        {:reply, :ok, state}
    end
  end

  @spec handle_command_call(store_id(), command_name(), command_payload(), State.t()) ::
          {:reply, {:ok, command_reply()}, State.t(),
           {:continue, {:push_patch, PatchEnvelope.t() | nil}}}
  defp handle_command_call(store_id, command_name, payload, %State{} = state)
       when is_list(store_id) and is_atom(command_name) and is_map(payload) do
    base_meta = %{page_id: page_id(state), store_id: store_id, command: command_name}
    started_at = System.monotonic_time()
    Telemetry.emit([:musubi, :command, :start], %{system_time: System.system_time()}, base_meta)

    try do
      {_pipeline_status, reply, next_state, envelope} =
        run_command_with_render(store_id, command_name, payload, state)

      Telemetry.emit(
        [:musubi, :command, :stop],
        %{duration: System.monotonic_time() - started_at},
        Map.put(base_meta, :status, :ok)
      )

      if envelope do
        Telemetry.emit(
          [:musubi, :patch, :stop],
          %{
            count: envelope_op_count(envelope),
            stream_count: envelope_stream_count(envelope)
          },
          Map.put(base_meta, :version, next_state.version)
        )
      end

      {:reply, {:ok, reply}, next_state, {:continue, {:push_patch, envelope}}}
    rescue
      error ->
        Telemetry.emit(
          [:musubi, :command, :exception],
          %{duration: System.monotonic_time() - started_at},
          Map.merge(base_meta, %{
            kind: :error,
            reason: error,
            stacktrace: __STACKTRACE__
          })
        )

        reraise error, __STACKTRACE__
    end
  end

  @spec resolve_command_name(StoreTable.t(), store_id(), String.t()) ::
          {:ok, command_name()} | {:error, command_error()}
  defp resolve_command_name(%StoreTable{} = registry, store_id, command_name)
       when is_list(store_id) and is_binary(command_name) do
    case StoreTable.get(registry, store_id) do
      %Entry{module: module} -> fetch_declared_command_name(module, command_name)
      nil -> {:error, :unknown_store}
    end
  end

  @spec fetch_declared_command_name(module(), String.t()) ::
          {:ok, command_name()} | {:error, :unknown_command}
  defp fetch_declared_command_name(module, command_name)
       when is_atom(module) and is_binary(command_name) do
    case Enum.find(declared_command_names(module), &(Atom.to_string(&1) == command_name)) do
      nil -> {:error, :unknown_command}
      name -> {:ok, name}
    end
  end

  @spec declared_command_names(module()) :: [command_name()]
  defp declared_command_names(module) when is_atom(module) do
    if module_exports?(module, :__musubi__, 1) do
      commands = module.__musubi__(:commands)

      commands
      |> List.wrap()
      |> Enum.map(& &1.name)
    else
      []
    end
  end

  @impl GenServer
  def handle_cast(
        {:upload_channel_chunk, store_id, name, ref, bytes_written, complete?},
        %State{} = state
      ) do
    apply_channel_chunk(state, store_id, name, ref, bytes_written, complete?)
  end

  def handle_cast(
        {:register_upload_channel, store_id, name, ref, channel_pid, path},
        %State{} = state
      ) do
    next_state =
      mutate_upload_socket(state, store_id, fn socket ->
        Musubi.Upload.update_entry(socket, name, ref, fn entry ->
          %{entry | upload_channel_pid: channel_pid, path: path}
        end)
      end)

    {:noreply, next_state}
  end

  def handle_cast(
        {:upload_channel_error, store_id, name, ref, %Musubi.Upload.Error{} = error},
        %State{} = state
      ) do
    next_state =
      mutate_upload_socket(state, store_id, fn socket ->
        socket
        |> Musubi.Upload.update_entry(name, ref, fn entry ->
          %{entry | status: :error, errors: append_error(entry.errors, error)}
        end)
        |> Musubi.Upload.enqueue_error(name, ref, error)
      end)

    {next_state, envelope} = render_and_envelope(next_state)
    {:noreply, next_state, {:continue, {:push_patch, envelope}}}
  end

  @impl GenServer
  @spec handle_continue({:push_patch, PatchEnvelope.t() | nil}, State.t()) ::
          {:noreply, State.t()}
  def handle_continue({:push_patch, nil}, %State{} = state), do: {:noreply, state}

  def handle_continue({:push_patch, %PatchEnvelope{} = envelope}, %State{} = state) do
    case transport_pid(state) do
      nil ->
        :ok

      pid when is_pid(pid) ->
        send(pid, patch_message(state, envelope))
    end

    {:noreply, state}
  end

  @impl GenServer
  def handle_info({ref, {:musubi_async_result, name, kind, classified}}, %State{} = state)
      when is_reference(ref) do
    handle_async_result(ref, classified, {nil, name, kind}, state)
  end

  def handle_info({ref, classified}, %State{} = state) when is_reference(ref) do
    handle_async_result(ref, classified, nil, state)
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %State{} = state) do
    handle_async_down(ref, reason, state)
  end

  def handle_info({:musubi_async_timeout, ref}, %State{} = state) when is_reference(ref) do
    handle_async_timeout(ref, state)
  end

  def handle_info({:EXIT, pid, reason}, %State{} = state) do
    log_linked_process_exit(pid, reason)
    {:stop, reason, state}
  end

  # Server-authoritative targeting primitive (BDR-0030). Delivers `assigns`
  # to the addressed child store's `update/2`, dirtying only that subtree.
  # Placed before the catch-all so it is not swallowed by the root
  # `handle_info` dispatch. A missing target is a no-op + telemetry (LV-aligned).
  def handle_info({:musubi_send_update, store_id, assigns}, %State{} = state)
      when is_list(store_id) and is_map(assigns) do
    case StoreTable.get(state.store_table, store_id) do
      %Entry{socket: socket} = entry ->
        next_socket = Reconciler.update_store(socket, assigns)
        next_state = put_entry(state, store_id, %{entry | socket: next_socket})
        {next_state, envelope} = render_and_envelope(next_state)
        {:noreply, next_state, {:continue, {:push_patch, envelope}}}

      nil ->
        Telemetry.emit(
          [:musubi, :send_update, :no_target],
          %{system_time: System.system_time()},
          %{store_id: store_id, page_id: page_id(state)}
        )

        {:noreply, state, {:continue, {:push_patch, nil}}}
    end
  end

  # Catch-all dispatch path for application messages (typically Phoenix.PubSub
  # broadcasts the root store subscribed to inside `mount/1`). Runs the
  # `:handle_info` hook chain on the root socket, dispatches to the root
  # store's `handle_info/2` callback when present, and re-renders. PubSub is
  # not built in (BDR-0005), so the runtime emits `[:musubi, :pubsub, :receive]`
  # purely for observability.
  def handle_info(message, %State{} = state) do
    Telemetry.emit(
      [:musubi, :pubsub, :receive],
      %{system_time: System.system_time()},
      %{module: state.root_module, page_id: page_id(state)}
    )

    dispatch_root_handle_info(message, state)
  end

  @impl GenServer
  @spec terminate(term(), State.t()) :: :ok
  def terminate(reason, %State{root_module: root_module, root_socket: root_socket}) do
    if module_exports?(root_module, :terminate, 2) do
      root_module.terminate(reason, root_socket)
    end

    log_page_server_terminate(root_module, reason)
    :ok
  end

  # ---------------------------------------------------------------------------
  # Command pipeline + render
  # ---------------------------------------------------------------------------

  # Command replies leave the runtime in native Elixir shape (atom keys,
  # structs, atom values) — symmetric with `render/1` output. The wire-form
  # transformation (`Musubi.Wire.to_wire/1`) happens at the transport egress
  # (`Musubi.Transport.Channel` / `Musubi.Transport.ConnectionChannel`), the
  # same boundary that serializes patch envelopes. So `command/4`,
  # `command_by_name/4`, and `Musubi.Testing.dispatch_command/3` all expose
  # the native reply. Reply-schema validation still runs against the wire
  # form: `Musubi.Hooks.ValidateReplySchema` wires internally for validation
  # only and leaves the returned reply untouched.
  @spec run_command_with_render(store_id(), atom(), command_payload(), State.t()) ::
          {:ok | :halted, command_reply(), State.t(), PatchEnvelope.t() | nil}
  defp run_command_with_render(store_id, command_name, payload, %State{} = state) do
    {pipeline_status, reply, state} =
      run_command_pipeline(store_id, command_name, payload, state)

    # Render in both the :ok and :halted paths: a halting before_command hook may
    # have mutated state or queued stream/push-event ops on the socket, and those
    # ship in one envelope just like a handler's changes. A pure denial (no state
    # change) renders to no envelope, so nothing is pushed.
    {next_state, envelope} = render_and_envelope(state)
    {pipeline_status, reply, next_state, envelope}
  end

  @spec render_and_envelope(State.t()) :: {State.t(), PatchEnvelope.t() | nil}
  defp render_and_envelope(%State{} = state) do
    {next_root_socket, next_registry} =
      run_render_cycle(state.root_socket, state.store_table)

    {wire_root, events, next_registry} = serialize_frames(next_registry)
    {stream_ops, next_registry} = flush_all_stream_ops(next_registry)
    {upload_ops_raw, next_registry} = flush_all_upload_ops(next_registry)

    {upload_ops, next_throttle} =
      throttle_progress(upload_ops_raw, state.upload_progress_last_emitted)

    diff_ops =
      if wire_root == state.previous_wire_root do
        []
      else
        Diff.diff(state.previous_wire_root, wire_root)
      end

    envelope = PatchEnvelope.build(state.version, diff_ops, stream_ops, upload_ops, events)

    next_version = if envelope, do: envelope.version, else: state.version

    next_state =
      rebuild_async_index(%{
        state
        | root_socket: root_socket(next_registry, next_root_socket),
          store_table: next_registry,
          version: next_version,
          previous_wire_root: wire_root,
          upload_progress_last_emitted: next_throttle
      })

    {next_state, envelope}
  end

  @spec run_command_pipeline(store_id(), atom(), command_payload(), State.t()) ::
          {:ok | :halted, command_reply(), State.t()}
  defp run_command_pipeline(store_id, command_name, payload, %State{} = state) do
    addressed = lookup_or_raise!(state.store_table, store_id)
    validate_command_declared!(addressed.module, command_name)

    chain = store_id_chain(store_id)
    state = stamp_command_target(state, chain, addressed.module)

    case run_hook_chain(:before_command, chain, [command_name, payload], state, true) do
      {:halt_reply, reply, state} ->
        # BDR-0008: graceful denial path. Emit `[:musubi, :auth, :deny]` so
        # operators can observe authz hooks halting commands without raising.
        Telemetry.emit(
          [:musubi, :auth, :deny],
          %{system_time: System.system_time()},
          %{
            page_id: page_id(state),
            module: addressed.module,
            path: store_id,
            command: command_name,
            reply: reply
          }
        )

        state = clear_command_target(state, chain)
        {:halted, reply, state}

      {:halt, state} ->
        state = clear_command_target(state, chain)
        {:halted, %{}, state}

      {:cont, state} ->
        {reply, state} = dispatch_handler(store_id, command_name, payload, state)

        case run_hook_chain(:after_command, chain, [command_name, payload, reply], state, false) do
          {:cont, state} ->
            state = clear_command_target(state, chain)
            {:ok, reply, state}

          {:halt, state} ->
            state = clear_command_target(state, chain)
            {:ok, reply, state}
        end
    end
  end

  @spec lookup_or_raise!(StoreTable.t(), store_id()) :: Entry.t()
  defp lookup_or_raise!(registry, store_id) do
    case StoreTable.get(registry, store_id) do
      %Entry{} = entry ->
        entry

      nil ->
        raise ArgumentError, "no store mounted at store_id #{inspect(store_id)}"
    end
  end

  @spec validate_command_declared!(module(), atom()) :: :ok
  defp validate_command_declared!(module, command_name) do
    commands =
      if module_exports?(module, :__musubi__, 1) do
        List.wrap(module.__musubi__(:commands))
      else
        []
      end

    if Enum.any?(commands, &(&1.name == command_name)) do
      :ok
    else
      raise ArgumentError,
            "command #{inspect(command_name)} not declared by #{inspect(module)}"
    end
  end

  # Walk store_id prefixes from root ([]) to the addressed full store_id so
  # hooks attached on ancestor sockets fire before hooks attached on
  # descendants.
  @spec store_id_chain(store_id()) :: [store_id()]
  defp store_id_chain([]), do: [[]]

  defp store_id_chain(store_id) when is_list(store_id) do
    Enum.map(0..length(store_id), &Enum.take(store_id, &1))
  end

  @spec stamp_command_target(State.t(), [store_id()], module()) :: State.t()
  defp stamp_command_target(state, chain, target_module) do
    update_chain_sockets(state, chain, fn socket ->
      Socket.put_private(socket, ValidateCommandSchema.target_private_key(), target_module)
    end)
  end

  @spec clear_command_target(State.t(), [store_id()]) :: State.t()
  defp clear_command_target(state, chain) do
    key = ValidateCommandSchema.target_private_key()

    update_chain_sockets(state, chain, fn socket ->
      %{socket | private: Map.delete(socket.private, key)}
    end)
  end

  @spec update_chain_sockets(State.t(), [store_id()], (Socket.t() -> Socket.t())) :: State.t()
  defp update_chain_sockets(state, chain, fun) do
    Enum.reduce(chain, state, fn chain_id, acc ->
      case StoreTable.get(acc.store_table, chain_id) do
        %Entry{socket: socket} = entry ->
          next_entry = %{entry | socket: fun.(socket)}
          put_entry(acc, chain_id, next_entry)

        nil ->
          acc
      end
    end)
  end

  @spec run_hook_chain(
          Lifecycle.stage(),
          [store_id()],
          [term()],
          State.t(),
          boolean()
        ) ::
          {:cont, State.t()} | {:halt, State.t()} | {:halt_reply, command_reply(), State.t()}
  defp run_hook_chain(stage, chain, hook_args, state, halt_payloads_allowed?) do
    Enum.reduce_while(chain, {:cont, state}, fn chain_id, {:cont, acc} ->
      run_hook_chain_step(chain_id, stage, hook_args, halt_payloads_allowed?, acc)
    end)
  end

  @spec run_hook_chain_step(
          store_id(),
          Lifecycle.stage(),
          [term()],
          boolean(),
          State.t()
        ) ::
          {:cont, {:cont, State.t()}}
          | {:halt, {:halt, State.t()}}
          | {:halt, {:halt_reply, command_reply(), State.t()}}
  defp run_hook_chain_step(chain_id, stage, hook_args, halt_payloads_allowed?, %State{} = acc) do
    case StoreTable.get(acc.store_table, chain_id) do
      %Entry{socket: socket} = entry ->
        socket
        |> Lifecycle.run_hooks(stage, hook_args, halt_payloads_allowed?)
        |> wrap_hook_result(acc, chain_id, entry)

      nil ->
        {:cont, {:cont, acc}}
    end
  end

  @spec wrap_hook_result(Lifecycle.hook_result(), State.t(), store_id(), Entry.t()) ::
          {:cont, {:cont, State.t()}}
          | {:halt, {:halt, State.t()}}
          | {:halt, {:halt_reply, command_reply(), State.t()}}
  defp wrap_hook_result({:cont, %Socket{} = next_socket}, acc, chain_id, entry) do
    {:cont, {:cont, put_entry(acc, chain_id, %{entry | socket: next_socket})}}
  end

  defp wrap_hook_result({:halt, %Socket{} = next_socket}, acc, chain_id, entry) do
    {:halt, {:halt, put_entry(acc, chain_id, %{entry | socket: next_socket})}}
  end

  defp wrap_hook_result({:halt, reply, %Socket{} = next_socket}, acc, chain_id, entry) do
    {:halt, {:halt_reply, reply, put_entry(acc, chain_id, %{entry | socket: next_socket})}}
  end

  @spec dispatch_root_handle_info(term(), State.t()) ::
          {:noreply, State.t(), {:continue, {:push_patch, PatchEnvelope.t() | nil}}}
  defp dispatch_root_handle_info(message, %State{} = state) do
    chain = [[]]

    case run_hook_chain(:handle_info, chain, [message], state, false) do
      {:halt, state} ->
        {:noreply, state, {:continue, {:push_patch, nil}}}

      {:cont, state} ->
        state = invoke_root_handle_info(message, state)
        {next_state, envelope} = render_and_envelope(state)
        {:noreply, next_state, {:continue, {:push_patch, envelope}}}
    end
  end

  @spec invoke_root_handle_info(term(), State.t()) :: State.t()
  defp invoke_root_handle_info(message, %State{root_module: root_module} = state) do
    if module_exports?(root_module, :handle_info, 2) do
      %Entry{socket: socket} = entry = lookup_or_raise!(state.store_table, [])

      case root_module.handle_info(message, socket) do
        {:noreply, %Socket{} = next_socket} ->
          put_entry(state, [], %{entry | socket: next_socket})

        other ->
          raise ArgumentError,
                "bad return from #{inspect(root_module)}.handle_info/2: expected " <>
                  "{:noreply, socket}, got #{inspect(other)}"
      end
    else
      state
    end
  end

  @spec dispatch_handler(store_id(), atom(), command_payload(), State.t()) ::
          {command_reply(), State.t()}
  defp dispatch_handler(store_id, command_name, payload, %State{} = state) do
    %Entry{socket: socket, module: module} =
      entry =
      lookup_or_raise!(state.store_table, store_id)

    target_key = Musubi.Upload.command_target_key()
    handler_socket = Socket.put_private(socket, target_key, true)

    case module.handle_command(command_name, payload, handler_socket) do
      {:noreply, %Socket{} = next_socket} ->
        next_socket = clear_upload_target(next_socket, target_key)
        {%{}, put_entry(state, store_id, %{entry | socket: next_socket})}

      {:reply, reply, %Socket{} = next_socket} when is_map(reply) ->
        next_socket = clear_upload_target(next_socket, target_key)
        {reply, put_entry(state, store_id, %{entry | socket: next_socket})}

      other ->
        raise ArgumentError,
              "bad return from #{inspect(module)}.handle_command/3: expected " <>
                "{:noreply, socket} or {:reply, payload, socket}, got #{inspect(other)}"
    end
  end

  defp clear_upload_target(%Socket{} = socket, target_key) do
    %{socket | private: Map.delete(socket.private, target_key)}
  end

  @spec put_entry(State.t(), store_id(), Entry.t()) :: State.t()
  defp put_entry(%State{store_table: registry} = state, store_id, %Entry{} = entry) do
    next_registry = StoreTable.put(registry, store_id, entry)

    next_root_socket =
      if store_id == [] do
        entry.socket
      else
        state.root_socket
      end

    %{state | store_table: next_registry, root_socket: next_root_socket}
  end

  # ---------------------------------------------------------------------------
  # Render cycle (extracted from `init/1` so the command pipeline can reuse it)
  # ---------------------------------------------------------------------------

  @spec run_render_cycle(Socket.t(), StoreTable.t()) :: {Socket.t(), StoreTable.t()}
  defp run_render_cycle(%Socket{} = root_socket, %StoreTable{} = registry) do
    started_at = System.monotonic_time()

    {:ok, _resolved_root, next_root_socket, next_registry} =
      Resolver.resolve(root_socket, registry)

    Telemetry.emit(
      [:musubi, :render, :stop],
      %{duration: System.monotonic_time() - started_at},
      %{module: root_socket.module}
    )

    {next_root_socket, next_registry}
  end

  @spec root_socket(StoreTable.t(), Socket.t()) :: Socket.t()
  defp root_socket(%StoreTable{} = registry, %Socket{} = fallback) do
    case StoreTable.get(registry, []) do
      %Entry{socket: socket} -> socket
      nil -> fallback
    end
  end

  # Walks every entry in the registry and concatenates their pending stream
  # ops in entry-discovery order (root first, then descendants in registry-key
  # order), clearing the per-socket accumulators along the way. Pending ops
  # do not survive across handlers (see `streams/lifecycle`).
  @spec flush_all_stream_ops(StoreTable.t()) :: {[Stream.op()], StoreTable.t()}
  defp flush_all_stream_ops(%StoreTable{} = registry) do
    sorted_keys =
      registry
      |> StoreTable.keys()
      |> Enum.sort_by(&length/1)

    Enum.reduce(sorted_keys, {[], registry}, fn store_id, {ops_acc, reg_acc} ->
      flush_entry(reg_acc, store_id, ops_acc)
    end)
  end

  @spec flush_entry(StoreTable.t(), StoreTable.key(), [Stream.op()]) ::
          {[Stream.op()], StoreTable.t()}
  defp flush_entry(%StoreTable{} = registry, store_id, ops_acc) do
    case StoreTable.get(registry, store_id) do
      %Entry{socket: socket} = entry ->
        {entry_ops, next_socket} = Stream.flush_pending_ops(socket)
        entry_ops = Enum.map(entry_ops, &Map.put(&1, :store_id, store_id))

        next_registry =
          StoreTable.put(registry, store_id, %{entry | socket: next_socket})

        {ops_acc ++ entry_ops, next_registry}

      nil ->
        {ops_acc, registry}
    end
  end

  # Per-entry outbound aggregation (root before children). For each store socket:
  # drains + wire-encodes its push events (BDR-0032), builds a `Frame` from the
  # entry's wire render + those events, runs the `:after_serialize` transform
  # stage (default `ValidateRender`/`ValidateEvents` hooks, plus app
  # audit/redaction), then stamps each event with the socket's store_id
  # (symmetric with stream/upload ops). Iterating the registry — not the render
  # path — keeps events from memoized sockets flushing. Returns the (possibly
  # hook-rewritten) root wire render, the flattened events, and the registry with
  # hooked sockets + wire_state written back.
  @spec serialize_frames(StoreTable.t()) ::
          {Entry.wire_state() | nil, [PatchEnvelope.event()], StoreTable.t()}
  defp serialize_frames(%StoreTable{} = registry) do
    registry
    |> StoreTable.keys()
    |> Enum.sort_by(&length/1)
    |> Enum.reduce({nil, [], registry}, &serialize_entry/2)
  end

  @spec serialize_entry(store_id(), {Entry.wire_state() | nil, [PatchEnvelope.event()], StoreTable.t()}) ::
          {Entry.wire_state() | nil, [PatchEnvelope.event()], StoreTable.t()}
  defp serialize_entry(store_id, {wire_root, events_acc, reg}) do
    case StoreTable.get(reg, store_id) do
      %Entry{socket: socket, wire_state: wire_state} = entry ->
        {wire_events, socket} = Event.flush_pending(socket)

        {%Frame{render: render, events: events}, socket} =
          Lifecycle.run_transform_hooks(
            socket,
            :after_serialize,
            %Frame{render: wire_state, events: wire_events}
          )

        stamped = Enum.map(events, &Map.put(&1, :store_id, store_id))
        reg = StoreTable.put(reg, store_id, %{entry | socket: socket, wire_state: render})
        {if(store_id == [], do: render, else: wire_root), events_acc ++ stamped, reg}

      nil ->
        {wire_root, events_acc, reg}
    end
  end

  @spec flush_all_upload_ops(StoreTable.t()) :: {[Upload.op()], StoreTable.t()}
  defp flush_all_upload_ops(%StoreTable{} = registry) do
    sorted_keys = registry |> StoreTable.keys() |> Enum.sort_by(&length/1)

    Enum.reduce(sorted_keys, {[], registry}, fn store_id, {acc, reg_acc} ->
      case StoreTable.get(reg_acc, store_id) do
        %Entry{socket: socket} = entry ->
          {ops, next_socket} = Upload.flush_pending_ops(socket)
          stamped = Enum.map(ops, &Map.put(&1, :store_id, store_id))
          next_reg = StoreTable.put(reg_acc, store_id, %{entry | socket: next_socket})
          {acc ++ stamped, next_reg}

        nil ->
          {acc, reg_acc}
      end
    end)
  end

  # BDR-0025: cap progress-op emission rate per `{store_id, upload, ref}` at
  # 10 Hz (one emission every 100 ms). Drops intermediate progress ops that
  # arrive inside the window; non-progress ops bypass the throttle entirely
  # (a `complete`/`error`/`cancel`/`reset` should never be suppressed).
  @progress_throttle_ms 100

  # ---------------------------------------------------------------------------
  # Upload event helpers
  # ---------------------------------------------------------------------------

  @spec fetch_upload_target(State.t(), store_id(), atom()) ::
          {:ok, Entry.t()} | {:error, :unknown_store | :unknown_upload}
  defp fetch_upload_target(%State{store_table: registry}, store_id, name) do
    case StoreTable.get(registry, store_id) do
      %Entry{module: module} = entry ->
        if upload_declared?(module, name) do
          {:ok, entry}
        else
          {:error, :unknown_upload}
        end

      nil ->
        {:error, :unknown_store}
    end
  end

  defp upload_declared?(module, name) when is_atom(module) and is_atom(name) do
    if module_exports?(module, :__musubi__, 1) do
      case module.__musubi__(:upload, name) do
        {:ok, _config} -> true
        :error -> false
      end
    else
      false
    end
  end

  defp build_preflight_reply(%{accepted: accepted, errors: errors, socket: socket}, name) do
    config = preflight_config_payload(socket, name)

    entries =
      accepted
      |> Enum.map(fn {client_ref, accept_entry} ->
        {client_ref, encode_accepted_entry(accept_entry)}
      end)
      |> Map.new()

    errors_wire =
      Enum.map(errors, fn %{client_ref: cref, error: err} ->
        %{"client_ref" => cref, "error" => Musubi.Upload.Error.to_wire(err)}
      end)

    %{
      "ref" => Atom.to_string(name),
      "config" => config,
      "entries" => entries,
      "errors" => errors_wire
    }
  end

  defp preflight_config_payload(socket, name) do
    bucket =
      socket.assigns
      |> Map.get(Musubi.Upload.assigns_key(), %{})
      |> Map.get(name)

    case bucket do
      %{config: %Musubi.Upload.Config{} = config} ->
        Musubi.Upload.Config.to_wire(config)

      _missing ->
        fallback_upload_config(socket, name)
    end
  end

  defp fallback_upload_config(%Socket{module: nil}, _name), do: %{}

  defp fallback_upload_config(%Socket{module: module}, name) do
    case module.__musubi__(:upload, name) do
      {:ok, %Musubi.Upload.Config{} = config} -> Musubi.Upload.Config.to_wire(config)
      _missing -> %{}
    end
  end

  defp encode_accepted_entry(%{type: :channel, entry_ref: ref, token: token}) do
    %{"type" => "channel", "entry_ref" => ref, "token" => token}
  end

  defp encode_accepted_entry(%{type: :external, entry_ref: ref, uploader: uploader, meta: meta}) do
    %{
      "type" => "external",
      "entry_ref" => ref,
      "uploader" => uploader,
      "meta" => Musubi.Wire.to_wire(meta)
    }
  end

  defp apply_upload_progress(state, store_id, name, ref, progress) do
    next_state =
      mutate_upload_socket(state, store_id, fn socket ->
        apply_progress_to_socket(socket, name, ref, progress)
      end)

    next_state = dispatch_handle_progress(next_state, store_id, name, ref)

    render_and_envelope(next_state)
  end

  defp apply_upload_error(state, store_id, name, ref, %Musubi.Upload.Error{} = error) do
    next_state =
      mutate_upload_socket(state, store_id, fn socket ->
        socket
        |> Musubi.Upload.update_entry(name, ref, fn entry ->
          %{entry | status: :error, errors: append_error(entry.errors, error)}
        end)
        |> Musubi.Upload.enqueue_error(name, ref, error)
      end)

    render_and_envelope(next_state)
  end

  defp apply_progress_to_socket(socket, name, ref, progress) do
    socket
    |> Musubi.Upload.update_entry(name, ref, fn entry ->
      status = if progress >= 100, do: :success, else: :uploading
      %{entry | progress: progress, status: status}
    end)
    |> Musubi.Upload.enqueue_progress(name, ref, progress)
    |> maybe_enqueue_complete(name, ref, progress)
  end

  defp apply_channel_chunk(state, store_id, name, ref, bytes_written, complete?) do
    next_state =
      mutate_upload_socket(state, store_id, fn socket ->
        apply_channel_chunk_to_socket(socket, name, ref, bytes_written, complete?)
      end)

    next_state = dispatch_handle_progress(next_state, store_id, name, ref)

    {next_state, envelope} = render_and_envelope(next_state)
    {:noreply, next_state, {:continue, {:push_patch, envelope}}}
  end

  defp apply_channel_chunk_to_socket(socket, name, ref, bytes_written, complete?) do
    case Musubi.Upload.fetch_entry(socket, name, ref) do
      {:ok, entry} ->
        updated = build_chunked_entry(entry, bytes_written, complete?)

        socket
        |> Musubi.Upload.put_entry(name, updated)
        |> Musubi.Upload.enqueue_progress(name, ref, updated.progress)
        |> maybe_enqueue_complete(name, ref, updated.progress)

      :error ->
        socket
    end
  end

  defp build_chunked_entry(entry, bytes_written, complete?) do
    total = max(entry.client_size, bytes_written)
    raw_progress = compute_progress(bytes_written, total)
    progress = if complete?, do: 100, else: raw_progress

    status =
      cond do
        complete? or progress >= 100 -> :success
        progress > 0 -> :uploading
        true -> entry.status
      end

    %{entry | bytes_written: bytes_written, progress: progress, status: status}
  end

  defp dispatch_handle_progress(%State{} = state, store_id, name, ref) do
    with %Entry{socket: socket, module: module} = entry <- fetch_entry(state, store_id),
         true <- module_exports?(module, :handle_progress, 3),
         {:ok, upload_entry} <- Musubi.Upload.fetch_entry(socket, name, ref) do
      case module.handle_progress(name, upload_entry, socket) do
        {:noreply, %Socket{} = next_socket} ->
          put_entry_by_store_id(state, store_id, %{entry | socket: next_socket})

        other ->
          raise ArgumentError,
                "bad return from #{inspect(module)}.handle_progress/3: expected " <>
                  "{:noreply, socket}, got #{inspect(other)}"
      end
    else
      _missing -> state
    end
  end

  defp compute_progress(_bytes, 0), do: 0

  defp compute_progress(bytes, total)
       when is_integer(bytes) and is_integer(total) and total > 0 do
    min(div(bytes * 100, total), 100)
  end

  defp maybe_enqueue_complete(socket, name, ref, 100) do
    Musubi.Upload.enqueue_complete(socket, name, ref)
  end

  defp maybe_enqueue_complete(socket, _name, _ref, _progress), do: socket

  defp mutate_upload_socket(%State{store_table: registry} = state, store_id, fun) do
    case StoreTable.get(registry, store_id) do
      %Entry{socket: socket} = entry ->
        next_entry = %{entry | socket: fun.(socket)}
        put_entry_by_store_id(state, store_id, next_entry)

      nil ->
        state
    end
  end

  defp maybe_kill_upload_channel(%Musubi.Upload.Entry{upload_channel_pid: pid})
       when is_pid(pid) do
    if Process.alive?(pid), do: Process.exit(pid, :shutdown)
    :ok
  end

  defp maybe_kill_upload_channel(_entry), do: :ok

  @spec throttle_progress([Upload.op()], map()) :: {[Upload.op()], map()}
  defp throttle_progress(ops, last_emitted) when is_list(ops) and is_map(last_emitted) do
    now = System.monotonic_time(:millisecond)

    {reversed_kept, next_last} =
      Enum.reduce(ops, {[], last_emitted}, fn op, acc -> throttle_step(op, acc, now) end)

    {Enum.reverse(reversed_kept), next_last}
  end

  defp throttle_step(%{op: "progress"} = op, {kept_acc, last_acc}, now) do
    key = {op.store_id, op.upload, op.ref}

    if progress_due?(Map.get(last_acc, key), now) do
      {[op | kept_acc], Map.put(last_acc, key, now)}
    else
      {kept_acc, last_acc}
    end
  end

  defp throttle_step(op, {kept_acc, last_acc}, _now), do: {[op | kept_acc], last_acc}

  defp progress_due?(nil, _now), do: true
  defp progress_due?(ts, now), do: now - ts >= @progress_throttle_ms

  defp append_error(existing, error) when is_list(existing) do
    List.insert_at(existing, -1, error)
  end

  @spec page_id(State.t()) :: term()
  defp page_id(%State{root_socket: %Socket{assigns: assigns}}) do
    Map.get(assigns, :page_id) || Map.get(assigns, "page_id")
  end

  @spec transport_pid(State.t()) :: pid() | nil
  defp transport_pid(%State{transport: transport}) when is_map(transport) do
    Map.get(transport, :transport_pid)
  end

  defp transport_pid(_state), do: nil

  @spec patch_message(State.t(), PatchEnvelope.t()) ::
          {:patch, PatchEnvelope.t()} | {:musubi_root_patch, String.t(), PatchEnvelope.t()}
  defp patch_message(%State{transport: %{root_id: root_id}}, %PatchEnvelope{} = envelope)
       when is_binary(root_id) do
    {:musubi_root_patch, root_id, envelope}
  end

  defp patch_message(%State{}, %PatchEnvelope{} = envelope), do: {:patch, envelope}

  defp log_linked_process_exit(pid, reason) when is_pid(pid) do
    message = "page server linked process exited: #{inspect(pid)} reason=#{inspect(reason)}"
    log_shutdown_message(message, reason)
  end

  defp log_page_server_terminate(root_module, reason) when is_atom(root_module) do
    message = "page server terminating for #{inspect(root_module)} reason=#{inspect(reason)}"
    log_shutdown_message(message, reason)
  end

  defp log_shutdown_message(message, reason) when is_binary(message) do
    if expected_shutdown_reason?(reason) do
      :ok
    else
      Logger.error(message)
    end
  end

  defp expected_shutdown_reason?(:normal), do: true
  defp expected_shutdown_reason?(:shutdown), do: true
  defp expected_shutdown_reason?({:shutdown, _reason}), do: true
  defp expected_shutdown_reason?(_reason), do: false

  @spec envelope_op_count(PatchEnvelope.t() | nil) :: non_neg_integer()
  defp envelope_op_count(nil), do: 0
  defp envelope_op_count(%PatchEnvelope{ops: ops}), do: length(ops)

  @spec envelope_stream_count(PatchEnvelope.t() | nil) :: non_neg_integer()
  defp envelope_stream_count(nil), do: 0
  defp envelope_stream_count(%PatchEnvelope{stream_ops: ops}), do: length(ops)

  # ---------------------------------------------------------------------------
  # Async message routing
  # ---------------------------------------------------------------------------

  @typedoc "Discard hint plumbed alongside async messages so a stale-ref lazy_discard can still attribute to a `store_id`/`name`/`kind`."
  @type discard_meta() ::
          {store_id() | nil, Async.tracking_name() | nil, Async.kind() | nil} | nil

  @spec handle_async_result(reference(), term(), discard_meta(), State.t()) ::
          {:noreply, State.t(), {:continue, {:push_patch, PatchEnvelope.t() | nil}}}
  defp handle_async_result(ref, classified, discard_meta, %State{} = state) do
    case Map.fetch(state.async_index, ref) do
      {:ok, {store_id, name, kind}} ->
        # Demonitor + flush any pending :DOWN for this ref so it does not
        # also drive a failed write after the success path has run.
        Process.demonitor(ref, [:flush])
        process_async_result(store_id, name, kind, classified, state)

      :error ->
        emit_lazy_discard(state, enrich_discard_meta(discard_meta))
        {:noreply, state, {:continue, {:push_patch, nil}}}
    end
  end

  @spec handle_async_down(reference(), term(), State.t()) ::
          {:noreply, State.t(), {:continue, {:push_patch, PatchEnvelope.t() | nil}}}
  defp handle_async_down(ref, reason, %State{} = state) do
    case Map.fetch(state.async_index, ref) do
      {:ok, {store_id, name, kind}} ->
        process_async_down(store_id, name, kind, reason, state)

      :error ->
        # Either the matching {ref, result} already arrived (success path
        # demonitored + flushed) or the task was for a node that no longer
        # exists. Either way: silent.
        {:noreply, state, {:continue, {:push_patch, nil}}}
    end
  end

  @spec handle_async_timeout(reference(), State.t()) ::
          {:noreply, State.t(), {:continue, {:push_patch, PatchEnvelope.t() | nil}}}
  defp handle_async_timeout(ref, %State{} = state) do
    case Map.fetch(state.async_index, ref) do
      {:ok, {store_id, name, _kind}} ->
        process_async_timeout(store_id, name, state)

      :error ->
        {:noreply, state, {:continue, {:push_patch, nil}}}
    end
  end

  @spec process_async_result(
          store_id(),
          Async.tracking_name(),
          Async.kind(),
          term(),
          State.t()
        ) ::
          {:noreply, State.t(), {:continue, {:push_patch, PatchEnvelope.t() | nil}}}
  defp process_async_result(store_id, name, kind, classified, %State{} = state) do
    with %Entry{} = entry <- fetch_entry(state, store_id),
         {:ok, tracking_entry} <- Async.fetch_tracking(entry.socket, name) do
      next_state =
        apply_async_result_to_entry(state, store_id, entry, name, tracking_entry, classified)

      {next_state, envelope} = render_and_envelope(next_state)
      {:noreply, next_state, {:continue, {:push_patch, envelope}}}
    else
      _missing ->
        emit_lazy_discard(state, {store_id, name, kind})
        {:noreply, state, {:continue, {:push_patch, nil}}}
    end
  end

  defp apply_async_result_to_entry(
         state,
         store_id,
         entry,
         name,
         %{kind: :start} = tracking_entry,
         classified
       ) do
    emit_async_stop(entry.socket, name, tracking_entry.kind, classified)
    dispatch_handle_async(state, store_id, entry, entry.module, name, tracking_entry, classified)
  end

  defp apply_async_result_to_entry(state, store_id, entry, name, tracking_entry, classified) do
    emit_async_stop(entry.socket, name, tracking_entry.kind, classified)
    next_socket = Async.apply_task_result(entry.socket, name, tracking_entry, classified)
    put_entry_by_store_id(state, store_id, %{entry | socket: next_socket})
  end

  @spec process_async_down(store_id(), Async.tracking_name(), Async.kind(), term(), State.t()) ::
          {:noreply, State.t(), {:continue, {:push_patch, PatchEnvelope.t() | nil}}}
  defp process_async_down(store_id, name, _kind, reason, %State{} = state) do
    with %Entry{} = entry <- fetch_entry(state, store_id),
         {:ok, tracking_entry} <- Async.fetch_tracking(entry.socket, name) do
      next_state = apply_async_down_to_entry(state, store_id, entry, name, tracking_entry, reason)
      {next_state, envelope} = render_and_envelope(next_state)
      {:noreply, next_state, {:continue, {:push_patch, envelope}}}
    else
      _missing -> {:noreply, state, {:continue, {:push_patch, nil}}}
    end
  end

  defp apply_async_down_to_entry(state, store_id, entry, name, tracking_entry, reason) do
    classified = {:exit, tracking_entry.cancel_reason || reason}

    emit_async_stop(
      entry.socket,
      name,
      tracking_entry.kind,
      classified
    )

    case tracking_entry.kind do
      :start ->
        dispatch_handle_async(
          state,
          store_id,
          entry,
          entry.module,
          name,
          tracking_entry,
          classified
        )

      :assign ->
        next_socket = Async.apply_task_down(entry.socket, name, tracking_entry, reason)
        put_entry_by_store_id(state, store_id, %{entry | socket: next_socket})

      :stream ->
        next_socket = Async.apply_task_down(entry.socket, name, tracking_entry, reason)
        put_entry_by_store_id(state, store_id, %{entry | socket: next_socket})
    end
  end

  @spec process_async_timeout(store_id(), Async.tracking_name(), State.t()) ::
          {:noreply, State.t(), {:continue, {:push_patch, PatchEnvelope.t() | nil}}}
  defp process_async_timeout(store_id, name, %State{} = state) do
    with %Entry{} = entry <- fetch_entry(state, store_id),
         {next_socket, %{pid: pid}} <- Async.mark_timeout(entry.socket, name) do
      if Process.alive?(pid), do: Process.exit(pid, :kill)
      next_state = put_entry_by_store_id(state, store_id, %{entry | socket: next_socket})
      # No envelope yet — the resulting :DOWN will run render.
      {:noreply, next_state, {:continue, {:push_patch, nil}}}
    else
      _missing -> {:noreply, state, {:continue, {:push_patch, nil}}}
    end
  end

  @spec dispatch_handle_async(
          State.t(),
          store_id(),
          Entry.t(),
          module(),
          Async.tracking_name(),
          Async.tracking_entry(),
          term()
        ) :: State.t()
  defp dispatch_handle_async(state, store_id, entry, module, name, tracking_entry, classified) do
    socket = Async.drop_tracking_only(entry.socket, name)
    cancel_tracked_timer(tracking_entry)

    delivered = unwrap_for_handle_async(classified)
    chain = store_id_chain(store_id)

    state = put_entry_by_store_id(state, store_id, %{entry | socket: socket})

    case run_hook_chain(:handle_async, chain, [name, delivered], state, false) do
      {:cont, state} ->
        invoke_handle_async(state, store_id, module, name, delivered)

      {:halt, state} ->
        state
    end
  end

  @spec invoke_handle_async(State.t(), store_id(), module(), Async.tracking_name(), term()) ::
          State.t()
  defp invoke_handle_async(state, store_id, module, name, delivered) do
    %Entry{socket: socket} = entry = fetch_entry(state, store_id)

    if module_exports?(module, :handle_async, 3) do
      try do
        case module.handle_async(name, delivered, socket) do
          {:noreply, %Socket{} = next_socket} ->
            put_entry_by_store_id(state, store_id, %{entry | socket: next_socket})

          other ->
            raise ArgumentError,
                  "bad return from #{inspect(module)}.handle_async/3: expected " <>
                    "{:noreply, socket}, got #{inspect(other)}"
        end
      rescue
        error ->
          # BDR-0020: handle_async/3 exceptions are caught; runtime survives.
          Musubi.Async.Telemetry.exception(socket, name, :start, :error, error, __STACKTRACE__)

          Logger.error(
            "handle_async/3 raised on #{inspect(module)} for #{inspect(name)}: " <>
              Exception.format(:error, error, __STACKTRACE__)
          )

          state
      end
    else
      state
    end
  end

  defp unwrap_for_handle_async({:ok, value}), do: {:ok, value}
  defp unwrap_for_handle_async({:exit, reason_class}), do: {:exit, reason_class}

  defp cancel_tracked_timer(%{timer_ref: nil}), do: :ok

  defp cancel_tracked_timer(%{timer_ref: ref}) when is_reference(ref) do
    _cancel = Process.cancel_timer(ref)
    :ok
  end

  @spec fetch_entry(State.t(), store_id()) :: Entry.t() | nil
  defp fetch_entry(%State{store_table: registry}, store_id) when is_list(store_id) do
    StoreTable.get(registry, store_id)
  end

  @spec put_entry_by_store_id(State.t(), store_id(), Entry.t()) :: State.t()
  defp put_entry_by_store_id(%State{store_table: registry} = state, store_id, %Entry{} = entry)
       when is_list(store_id) do
    next_registry = StoreTable.put(registry, store_id, entry)

    next_root_socket =
      if store_id == [] do
        entry.socket
      else
        state.root_socket
      end

    %{state | store_table: next_registry, root_socket: next_root_socket}
  end

  @spec rebuild_async_index(State.t()) :: State.t()
  defp rebuild_async_index(%State{store_table: registry} = state) do
    index =
      Enum.reduce(StoreTable.keys(registry), %{}, &collect_entry_refs(registry, &1, &2))

    %{state | async_index: index}
  end

  defp collect_entry_refs(registry, store_id, acc) do
    case StoreTable.get(registry, store_id) do
      %Entry{socket: socket} ->
        Enum.reduce(Async.tracking(socket), acc, &put_ref(&1, store_id, &2))

      nil ->
        acc
    end
  end

  defp put_ref({name, %{ref: ref, kind: kind}}, store_id, acc) do
    Map.put(acc, ref, {store_id, name, kind})
  end

  @spec emit_async_stop(Socket.t(), Async.tracking_name(), Async.kind(), term()) :: :ok
  defp emit_async_stop(socket, name, kind, classified) do
    status =
      case classified do
        {:ok, {:ok, _value}} -> :ok
        {:ok, {:ok, _value, _opts}} -> :ok
        _other -> :failed
      end

    Musubi.Async.Telemetry.stop(socket, name, kind, status)
  end

  @spec enrich_discard_meta(discard_meta()) :: discard_meta()
  defp enrich_discard_meta(nil), do: nil
  defp enrich_discard_meta({_store_id, _name, _kind} = meta), do: meta

  @spec emit_lazy_discard(State.t(), discard_meta()) :: :ok
  defp emit_lazy_discard(%State{} = state, discard_meta) do
    {store_id, name, kind} =
      case discard_meta do
        {sid, n, k} -> {sid, n, k}
        nil -> {nil, nil, nil}
      end

    Musubi.Async.Telemetry.lazy_discard(
      %{
        page_id: page_id(state),
        module: state.root_module,
        store_id: store_id
      },
      name,
      kind
    )
  end
end
