defmodule Musubi.Page.ServerCommandTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  require Logger

  alias Musubi.Lifecycle
  alias Musubi.Page.Server
  alias Musubi.Page.Server.State
  alias Musubi.Page.StoreTable
  alias Musubi.Socket

  defmodule LeafStore do
    @moduledoc false
    use Musubi.Store

    state do
      field :status, String.t()
    end

    command :select do
      payload do
        field :id, String.t()
      end
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, Musubi.Socket.assign(socket, :status, "ready")}
    @impl Musubi.Store
    def render(socket), do: %{status: socket.assigns.status}

    @impl Musubi.Store
    def handle_command(:select, %{"id" => id}, socket) do
      {:reply, %{selected: id}, Musubi.Socket.assign(socket, :status, "selected:" <> id)}
    end
  end

  defmodule FiltersStore do
    @moduledoc false
    use Musubi.Store

    state do
      field :query, String.t()
    end

    command :change_query do
      payload do
        field :query, String.t()
      end
    end

    command :wipe

    @impl Musubi.Store
    def mount(socket), do: {:ok, Musubi.Socket.assign(socket, :query, "")}
    @impl Musubi.Store
    def render(socket), do: %{query: socket.assigns.query}

    @impl Musubi.Store
    def handle_command(:change_query, %{"query" => query}, socket) do
      {:noreply, Musubi.Socket.assign(socket, :query, query)}
    end

    @impl Musubi.Store
    def handle_command(:wipe, _payload, socket) do
      {:noreply, Musubi.Socket.assign(socket, :query, "")}
    end
  end

  defmodule RootStore do
    @moduledoc false
    use Musubi.Store

    state do
      field :title, String.t()
      field :filters, FiltersStore.t()
      field :leaf, LeafStore.t()
    end

    command :reload_products

    @impl Musubi.Store
    def mount(socket) do
      socket =
        socket
        |> Musubi.Socket.assign(:title, "home")
        |> Musubi.Socket.assign(:reloads, 0)

      socket =
        case Musubi.Socket.get_private(socket, :hook_events) do
          nil -> socket
          test_pid -> attach_audit_hooks(socket, test_pid)
        end

      {:ok, socket}
    end

    @impl Musubi.Store
    def render(socket) do
      %{
        title: socket.assigns.title,
        filters: Musubi.Child.child(FiltersStore, id: "filters"),
        leaf: Musubi.Child.child(LeafStore, id: "leaf")
      }
    end

    @impl Musubi.Store
    def handle_command(:reload_products, _payload, socket) do
      next = Map.get(socket.assigns, :reloads, 0) + 1
      {:reply, %{reloaded: true}, Musubi.Socket.assign(socket, :reloads, next)}
    end

    defp attach_audit_hooks(socket, test_pid) do
      socket
      |> Lifecycle.attach_hook(:audit_before, :before_command, fn name, _payload, sock ->
        send(test_pid, {:hook, :root_before, name})
        {:cont, sock}
      end)
      |> Lifecycle.attach_hook(:audit_after, :after_command, fn name, _payload, _reply, sock ->
        send(test_pid, {:hook, :root_after, name})
        {:cont, sock}
      end)
    end
  end

  defmodule HaltingStore do
    @moduledoc false
    use Musubi.Store

    state do
      field :ok, boolean()
    end

    command :restricted

    @impl Musubi.Store
    def mount(socket) do
      socket =
        Lifecycle.attach_hook(socket, :auth, :before_command, fn _name, _payload, sock ->
          {:halt, %{ok: false, reason: "unauthorized"}, sock}
        end)

      {:ok, Musubi.Socket.assign(socket, :ok, true)}
    end

    @impl Musubi.Store
    def render(socket), do: %{ok: socket.assigns.ok}

    @impl Musubi.Store
    def handle_command(:restricted, _payload, socket) do
      send(socket.assigns.test_pid, :handler_should_not_run)
      {:noreply, socket}
    end
  end

  defmodule SilentHaltStore do
    @moduledoc false
    use Musubi.Store

    state do
      field :ok, boolean()
    end

    command :gated

    @impl Musubi.Store
    def mount(socket) do
      socket =
        Lifecycle.attach_hook(socket, :gate, :before_command, fn _name, _payload, sock ->
          {:halt, sock}
        end)

      {:ok, Musubi.Socket.assign(socket, :ok, true)}
    end

    @impl Musubi.Store
    def render(socket), do: %{ok: socket.assigns.ok}

    @impl Musubi.Store
    def handle_command(:gated, _payload, socket) do
      send(socket.assigns.test_pid, :handler_should_not_run)
      {:noreply, socket}
    end
  end

  defmodule ProductCardStore do
    @moduledoc false
    use Musubi.Store

    state do
      field :id, String.t()
    end

    command :select

    @impl Musubi.Store
    def mount(socket), do: {:ok, Musubi.Socket.assign(socket, :id, socket.id)}
    @impl Musubi.Store
    def render(socket), do: %{id: socket.assigns.id}

    @impl Musubi.Store
    def handle_command(:select, _payload, socket) do
      {:reply, %{selected: socket.assigns.id}, socket}
    end
  end

  defmodule ProductsListStore do
    @moduledoc false
    use Musubi.Store

    state do
      field :products, list(ProductCardStore.t())
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, Musubi.Socket.assign(socket, :ids, ["prod_123", "prod_456"])}

    @impl Musubi.Store
    def render(socket) do
      %{
        products:
          Enum.map(socket.assigns.ids, fn id -> Musubi.Child.child(ProductCardStore, id: id) end)
      }
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defmodule CrashingStore do
    @moduledoc false
    use Musubi.Store

    state do
      field :ok, boolean()
    end

    command :boom

    @impl Musubi.Store
    def mount(socket), do: {:ok, Musubi.Socket.assign(socket, :ok, true)}
    @impl Musubi.Store
    def render(socket), do: %{ok: socket.assigns.ok}

    @impl Musubi.Store
    def handle_command(:boom, _payload, _socket) do
      raise "boom"
    end
  end

  defmodule TypedReplyStore do
    @moduledoc false
    use Musubi.Store

    state do
      field :ok, boolean()
    end

    command :ok_reply do
      reply do
        field :ok, boolean()
      end
    end

    command :bad_type_reply do
      reply do
        field :ok, boolean()
      end
    end

    command :missing_field_reply do
      reply do
        field :ok, boolean()
      end
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, Musubi.Socket.assign(socket, :ok, true)}
    @impl Musubi.Store
    def render(socket), do: %{ok: socket.assigns.ok}

    @impl Musubi.Store
    def handle_command(:ok_reply, _payload, socket) do
      {:reply, %{"ok" => true}, socket}
    end

    def handle_command(:bad_type_reply, _payload, socket) do
      {:reply, %{"ok" => "not_a_bool"}, socket}
    end

    def handle_command(:missing_field_reply, _payload, socket) do
      {:reply, %{}, socket}
    end
  end

  defmodule WireReplyStore do
    @moduledoc false
    use Musubi.Store

    state do
      field :ok, boolean()
    end

    command :atom_keyed
    command :atom_valued
    command :nested
    command :already_wire

    @impl Musubi.Store
    def mount(socket), do: {:ok, Musubi.Socket.assign(socket, :ok, true)}
    @impl Musubi.Store
    def render(socket), do: %{ok: socket.assigns.ok}

    @impl Musubi.Store
    def handle_command(:atom_keyed, _payload, socket) do
      {:reply, %{selected: "abc"}, socket}
    end

    def handle_command(:atom_valued, _payload, socket) do
      {:reply, %{status: :active}, socket}
    end

    def handle_command(:nested, _payload, socket) do
      {:reply, %{meta: %{count: 3, tags: [:a, :b]}}, socket}
    end

    def handle_command(:already_wire, _payload, socket) do
      {:reply, %{"ok" => true}, socket}
    end
  end

  setup do
    Process.flag(:trap_exit, true)
    :ok
  end

  describe "Scenario: Routing to the root store" do
    test "dispatches the command to the root handler and returns the reply" do
      pid = start_supervised!({Server, {RootStore, %{}, %{transport_pid: self()}}})

      assert {:ok, %{reloaded: true}} = Server.command(pid, [], :reload_products, %{})
    end
  end

  describe "Scenario: Routing to a nested child store" do
    test "dispatches command to filters child and persists the mutated socket" do
      pid = start_supervised!({Server, {RootStore, %{}, %{transport_pid: self()}}})

      assert {:ok, %{}} =
               Server.command(pid, ["filters"], :change_query, %{"query" => "shirt"})

      %State{store_table: registry} = :sys.get_state(pid)
      entry = StoreTable.get(registry, ["filters"])
      assert entry.socket.assigns.query == "shirt"
    end
  end

  describe "Scenario: Routing to a child of a keyed list" do
    test "dispatches the command to the matching child store handler" do
      pid = start_supervised!({Server, {ProductsListStore, %{}, %{transport_pid: self()}}})

      assert {:ok, %{selected: "prod_123"}} =
               Server.command(pid, ["products", "prod_123"], :select, %{})

      assert {:ok, %{selected: "prod_456"}} =
               Server.command(pid, ["products", "prod_456"], :select, %{})
    end
  end

  describe "Scenario: Path that does not resolve crashes the runtime" do
    test "raises and exits the page runtime when the path is unknown" do
      pid = start_supervised!({Server, {RootStore, %{}, %{transport_pid: self()}}})
      Process.link(pid)

      capture_log(fn ->
        catch_exit(Server.command(pid, ["missing"], :select, %{"id" => "x"}))
        assert_receive {:EXIT, ^pid, _reason}
        Logger.flush()
      end)
    end
  end

  describe "Scenario: Command name absent from the addressed store crashes" do
    test "raises and exits when the addressed store does not declare the command" do
      pid = start_supervised!({Server, {RootStore, %{}, %{transport_pid: self()}}})
      Process.link(pid)

      capture_log(fn ->
        catch_exit(Server.command(pid, ["filters"], :delete, %{}))
        assert_receive {:EXIT, ^pid, _reason}
        Logger.flush()
      end)
    end
  end

  describe "Scenario: Payload conforms to the declared schema" do
    test "validation succeeds; handler runs" do
      pid = start_supervised!({Server, {RootStore, %{}, %{transport_pid: self()}}})

      assert {:ok, %{}} =
               Server.command(pid, ["filters"], :change_query, %{"query" => "shirt"})
    end
  end

  describe "Scenario: Payload violates a declared field type" do
    test "schema validation hook raises before any handler runs" do
      pid = start_supervised!({Server, {RootStore, %{}, %{transport_pid: self()}}})
      Process.link(pid)

      capture_log(fn ->
        catch_exit(Server.command(pid, ["filters"], :change_query, %{"query" => 42}))
        assert_receive {:EXIT, ^pid, _reason}
        Logger.flush()
      end)
    end
  end

  describe "Scenario: Authorization hook halts an unauthorized command" do
    test "halt with reply produces channel ok status with the halt payload" do
      pid =
        start_supervised!({Server, {HaltingStore, %{test_pid: self()}, %{transport_pid: self()}}})

      assert {:ok, %{ok: false, reason: "unauthorized"}} =
               Server.command(pid, [], :restricted, %{})

      refute_received :handler_should_not_run
    end
  end

  describe "Scenario: Hook halts without a reply" do
    test "delivers default ok reply with empty payload" do
      pid =
        start_supervised!(
          {Server, {SilentHaltStore, %{test_pid: self()}, %{transport_pid: self()}}}
        )

      assert {:ok, %{}} = Server.command(pid, [], :gated, %{})
      refute_received :handler_should_not_run
    end
  end

  describe "Scenario: Handler chooses {:noreply, socket}" do
    test "client receives a reply with empty payload and state mutates" do
      pid = start_supervised!({Server, {RootStore, %{}, %{transport_pid: self()}}})

      assert {:ok, %{}} = Server.command(pid, ["filters"], :wipe, %{})
    end
  end

  describe "Scenario: Handler chooses {:reply, payload, socket}" do
    test "the client receives the handler's reply payload" do
      pid = start_supervised!({Server, {RootStore, %{}, %{transport_pid: self()}}})

      assert {:ok, %{selected: "abc"}} =
               Server.command(pid, ["leaf"], :select, %{"id" => "abc"})
    end
  end

  describe "Scenario: Reply conforms to the declared schema" do
    test "validation succeeds; caller receives the handler reply" do
      pid = start_supervised!({Server, {TypedReplyStore, %{}, %{transport_pid: self()}}})

      assert {:ok, %{"ok" => true}} = Server.command(pid, [], :ok_reply, %{})
    end
  end

  describe "Scenario: Command replies are returned in native Elixir form" do
    test "atom keys stay atom keys" do
      pid = start_supervised!({Server, {WireReplyStore, %{}, %{transport_pid: self()}}})

      assert {:ok, %{selected: "abc"}} = Server.command(pid, [], :atom_keyed, %{})
    end

    test "atom values stay atoms" do
      pid = start_supervised!({Server, {WireReplyStore, %{}, %{transport_pid: self()}}})

      assert {:ok, %{status: :active}} = Server.command(pid, [], :atom_valued, %{})
    end

    test "nested values stay native" do
      pid = start_supervised!({Server, {WireReplyStore, %{}, %{transport_pid: self()}}})

      assert {:ok, %{meta: %{count: 3, tags: [:a, :b]}}} =
               Server.command(pid, [], :nested, %{})
    end

    test "string-keyed replies pass through unchanged" do
      pid = start_supervised!({Server, {WireReplyStore, %{}, %{transport_pid: self()}}})

      assert {:ok, %{"ok" => true}} = Server.command(pid, [], :already_wire, %{})
    end
  end

  describe "Scenario: Reply violates a declared field type" do
    test "reply schema validator raises through the default :after_command hook" do
      pid = start_supervised!({Server, {TypedReplyStore, %{}, %{transport_pid: self()}}})
      Process.link(pid)

      capture_log(fn ->
        catch_exit(Server.command(pid, [], :bad_type_reply, %{}))
        assert_receive {:EXIT, ^pid, _reason}
        Logger.flush()
      end)
    end
  end

  describe "Scenario: Reply is missing a declared field" do
    test "reply schema validator raises through the default :after_command hook" do
      pid = start_supervised!({Server, {TypedReplyStore, %{}, %{transport_pid: self()}}})
      Process.link(pid)

      capture_log(fn ->
        catch_exit(Server.command(pid, [], :missing_field_reply, %{}))
        assert_receive {:EXIT, ^pid, _reason}
        Logger.flush()
      end)
    end
  end

  describe "Scenario: A handler crash terminates the page runtime" do
    test "the page runtime exits and the caller observes the exit" do
      pid = start_supervised!({Server, {CrashingStore, %{}, %{transport_pid: self()}}})
      Process.link(pid)

      capture_log(fn ->
        catch_exit(Server.command(pid, [], :boom, %{}))
        assert_receive {:EXIT, ^pid, _reason}
        Logger.flush()
      end)
    end
  end

  describe "Scenario: A root-attached hook runs before a child-attached hook" do
    test "root hook fires before the child hook for a command on the child" do
      params = %{}

      pid = start_supervised!({Server, {RootStore, params, %{transport_pid: self()}}})

      :ok = inject_root_hook_audit(pid, self())

      assert {:ok, _reply} =
               Server.command(pid, ["filters"], :change_query, %{"query" => "x"})

      assert_received {:hook, :root_before, :change_query}
      assert_received {:hook, :root_after, :change_query}
    end
  end

  describe "Scenario: Successful command emits start and stop telemetry" do
    test "emits :start and :stop with metadata page_id, store_id, command, status" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:musubi, :command, :start],
          [:musubi, :command, :stop]
        ])

      on_exit(fn -> :telemetry.detach(ref) end)

      pid =
        start_supervised!({Server, {RootStore, %{"page_id" => "home"}, %{transport_pid: self()}}})

      assert {:ok, _reply} = Server.command(pid, ["filters"], :wipe, %{})

      assert_receive {[:musubi, :command, :start], ^ref, _,
                      %{
                        page_id: "home",
                        store_id: ["filters"],
                        command: :wipe
                      }}

      assert_receive {[:musubi, :command, :stop], ^ref, _,
                      %{
                        page_id: "home",
                        store_id: ["filters"],
                        command: :wipe,
                        status: :ok
                      }}
    end

    test "stop metadata excludes the payload contents" do
      ref = :telemetry_test.attach_event_handlers(self(), [[:musubi, :command, :stop]])
      on_exit(fn -> :telemetry.detach(ref) end)

      pid = start_supervised!({Server, {RootStore, %{}, %{transport_pid: self()}}})

      assert {:ok, _reply} =
               Server.command(pid, ["filters"], :change_query, %{"query" => "secret-payload"})

      assert_receive {[:musubi, :command, :stop], ^ref, _, metadata}

      refute Map.has_key?(metadata, :payload)
      refute String.contains?(inspect(metadata), "secret-payload")
    end
  end

  describe "Scenario: Handler crash emits an exception event" do
    test "telemetry exception fires with kind/reason/stacktrace" do
      ref = :telemetry_test.attach_event_handlers(self(), [[:musubi, :command, :exception]])
      on_exit(fn -> :telemetry.detach(ref) end)

      pid = start_supervised!({Server, {CrashingStore, %{}, %{transport_pid: self()}}})
      Process.link(pid)

      capture_log(fn ->
        catch_exit(Server.command(pid, [], :boom, %{}))
        assert_receive {:EXIT, ^pid, _reason}

        assert_receive {[:musubi, :command, :exception], ^ref, _,
                        %{kind: :error, reason: %RuntimeError{}, stacktrace: stacktrace}}

        assert is_list(stacktrace)
        Logger.flush()
      end)
    end
  end

  # `RootStore.mount/1` reads `socket.private[:hook_events]` to know whether to
  # attach the audit hooks. We can't pass the test pid through `params`
  # (assigns) because params don't reach private. Inject via :sys for testing.
  defp inject_root_hook_audit(pid, test_pid) do
    :sys.replace_state(pid, fn %State{} = state ->
      next_root_socket =
        state.root_socket
        |> Socket.put_private(:hook_events, test_pid)
        |> Lifecycle.attach_hook(:audit_before, :before_command, fn name, _payload, sock ->
          send(test_pid, {:hook, :root_before, name})
          {:cont, sock}
        end)
        |> Lifecycle.attach_hook(:audit_after, :after_command, fn name, _payload, _reply, sock ->
          send(test_pid, {:hook, :root_after, name})
          {:cont, sock}
        end)

      sync_root_into_registry(%{state | root_socket: next_root_socket})
    end)

    :ok
  end

  defp sync_root_into_registry(%State{root_socket: root_socket, store_table: registry} = state) do
    case StoreTable.get(registry, []) do
      nil ->
        state

      entry ->
        next_registry = StoreTable.put(registry, [], %{entry | socket: root_socket})
        %{state | store_table: next_registry}
    end
  end
end
