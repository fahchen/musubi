defmodule Musubi.Page.ServerTest do
  use ExUnit.Case, async: true

  alias Musubi.Page.Server
  alias Musubi.Page.Server.State
  alias Musubi.Page.StoreTable

  setup do
    Process.flag(:trap_exit, true)
    :ok
  end

  defmodule RootStore do
    use Musubi.Store

    state do
      field :status, String.t()
    end

    @impl Musubi.Store
    def mount(socket) do
      {:ok, Musubi.Socket.assign(socket, :status, "mounted")}
    end

    @impl Musubi.Store
    def render(socket) do
      %{status: socket.assigns.status}
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defmodule TerminatesRootStore do
    use Musubi.Store

    state do
      field :status, String.t()
    end

    @impl Musubi.Store
    def mount(socket) do
      {:ok, Musubi.Socket.assign(socket, status: "mounted")}
    end

    @impl Musubi.Store
    def render(socket) do
      %{status: socket.assigns.status}
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}

    @impl Musubi.Store
    def terminate(reason, socket) do
      send(socket.assigns.test_pid, {:root_terminate, reason, socket.assigns.status})
      :ok
    end
  end

  test "page server init builds a root socket and inserts it into the registry" do
    pid = start_supervised!({Server, {RootStore, %{}, %{transport_pid: self()}}})
    assert %State{} = :sys.get_state(pid)

    %State{
      root_module: RootStore,
      root_socket: root_socket,
      store_table: store_table,
      version: version,
      transport: transport
    } = :sys.get_state(pid)

    assert root_socket.id == ""
    assert root_socket.parent_path == []
    assert root_socket.module == RootStore
    assert root_socket.assigns == %{__changed__: %{}, status: "mounted"}

    assert %{
             before_command: [%{id: Musubi.Hooks.ValidateCommandSchema}],
             after_command: [%{id: Musubi.Hooks.ValidateReplySchema}]
           } = Musubi.Socket.get_private(root_socket, :hooks)

    # M4: initial render emits the bootstrap envelope (version 1).
    assert version == 1
    assert transport == %{transport_pid: self()}

    assert_receive {:patch, %Musubi.Page.PatchEnvelope{base_version: 0, version: 1}}

    assert StoreTable.keys(store_table) == [[]]

    assert registry_entry = StoreTable.get(store_table, [])
    assert registry_entry.module == RootStore
    assert registry_entry.socket == root_socket
    assert registry_entry.resolved_state == %{status: "mounted", __musubi_store_id__: []}

    assert registry_entry.wire_state == %{
             "status" => "mounted",
             "__musubi_store_id__" => []
           }
  end

  test "default hooks include ValidateCommandSchema everywhere and ValidateRender in dev/test" do
    default_hooks = Application.get_env(:musubi, :default_hooks, [])

    assert Enum.any?(default_hooks, fn
             {Musubi.Hooks.ValidateCommandSchema, :before_command, _fun} -> true
             _other -> false
           end)

    assert Enum.any?(default_hooks, fn
             {Musubi.Hooks.ValidateReplySchema, :after_command, _fun} -> true
             _other -> false
           end)

    if Mix.env() in [:dev, :test] do
      assert Enum.any?(default_hooks, fn
               {Musubi.Hooks.ValidateRender, :after_serialize, _fun} -> true
               _other -> false
             end)
    else
      refute Enum.any?(default_hooks, fn
               {Musubi.Hooks.ValidateRender, :after_serialize, _fun} -> true
               _other -> false
             end)
    end
  end

  test "root terminate fires on runtime exit" do
    pid =
      start_supervised!(
        {Server, {TerminatesRootStore, %{test_pid: self()}, %{transport_pid: self()}}}
      )

    GenServer.stop(pid, :shutdown)

    assert_receive {:root_terminate, :shutdown, "mounted"}
  end

  defmodule HandleInfoStore do
    use Musubi.Store

    state do
      field :counter, integer()
    end

    @impl Musubi.Store
    def mount(socket) do
      {:ok, Musubi.Socket.assign(socket, :counter, 0)}
    end

    @impl Musubi.Store
    def render(socket) do
      %{counter: socket.assigns.counter}
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}

    @impl Musubi.Store
    def handle_info(:bump, socket) do
      {:noreply, Musubi.Socket.update(socket, :counter, &(&1 + 1))}
    end
  end

  test "catch-all handle_info dispatches to root store and emits [:musubi, :pubsub, :receive]" do
    ref = :telemetry_test.attach_event_handlers(self(), [[:musubi, :pubsub, :receive]])
    on_exit(fn -> :telemetry.detach(ref) end)

    assert {:ok, pid} =
             Server.start_link({HandleInfoStore, %{}, %{transport_pid: self()}})

    # consume initial bootstrap envelope
    assert_receive {:patch, _envelope}

    send(pid, :bump)

    assert_receive {[:musubi, :pubsub, :receive], ^ref, _measurements, %{module: HandleInfoStore}}
    assert_receive {:patch, %{ops: ops}}
    assert Enum.any?(ops, fn op -> op[:path] == "/counter" and op[:value] == 1 end)
  end

  defmodule DenyStore do
    use Musubi.Store

    state do
      field :status, String.t()
    end

    command(:do_thing)

    @impl Musubi.Store
    def mount(socket) do
      socket = Musubi.Socket.assign(socket, :status, "ready")

      socket =
        Musubi.Lifecycle.attach_hook(socket, :authz, :before_command, fn _name, _payload, s ->
          {:halt, %{"error" => "forbidden"}, s}
        end)

      {:ok, socket}
    end

    @impl Musubi.Store
    def render(socket) do
      %{status: socket.assigns.status}
    end

    @impl Musubi.Store
    def handle_command(:do_thing, _payload, socket) do
      {:noreply, socket}
    end
  end

  test "graceful denial via :before_command halt-with-reply emits [:musubi, :auth, :deny]" do
    ref = :telemetry_test.attach_event_handlers(self(), [[:musubi, :auth, :deny]])
    on_exit(fn -> :telemetry.detach(ref) end)

    assert {:ok, pid} = Server.start_link({DenyStore, %{}, %{transport_pid: self()}})
    assert_receive {:patch, _envelope}

    assert {:ok, %{"error" => "forbidden"}} = Server.command(pid, [], :do_thing, %{})

    assert_receive {[:musubi, :auth, :deny], ^ref, _measurements,
                    %{command: :do_thing, module: DenyStore, path: []}}
  end

  test "mount/2 runs even when the store module has been purged before init" do
    # Simulate the cold-VM race: `function_exported?/3` returns `false` for
    # an unloaded module, which previously caused `root_store?/1` to skip
    # `mount/2` entirely. With `Code.ensure_loaded?/1` in front, the BEAM
    # code server reloads the .beam before the predicate runs.
    store = Musubi.Test.Fixtures.ColdVMStore
    :code.purge(store)
    :code.delete(store)
    refute :erlang.module_loaded(store)

    pid = start_supervised!({Server, {store, %{}, %{transport_pid: self()}}})
    %State{root_socket: root_socket} = :sys.get_state(pid)

    assert root_socket.assigns.status == "mounted"
  end

  test "a child store's init/1 runs even when its module has been purged" do
    # The child-store half of the cold-VM race: a child is only ever named by
    # the `child(Module, ...)` atom in its parent's render, so nothing loads it
    # before `Musubi.Reconciler.init_store/1` probes for `init/1`. Without
    # `Code.ensure_loaded?/1` the callback was silently skipped and the child
    # rendered from unmounted assigns.
    child = Musubi.Test.Fixtures.ColdVMStore
    :code.purge(child)
    :code.delete(child)
    refute :erlang.module_loaded(child)

    parent = Musubi.Test.Fixtures.ColdVMParentStore
    pid = start_supervised!({Server, {parent, %{}, %{transport_pid: self()}}})

    assert {:ok, %{socket: child_socket}} = Server.peek(pid, ["cold"])
    assert child_socket.assigns.status == "mounted"
  end
end
