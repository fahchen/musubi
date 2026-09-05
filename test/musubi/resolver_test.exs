defmodule Musubi.ResolverTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  require Logger

  alias Musubi.AsyncResult
  alias Musubi.Lifecycle
  alias Musubi.Page.StoreTable
  alias Musubi.Page.StoreTable.Entry
  alias Musubi.Reconciler
  alias Musubi.Resolver
  alias Musubi.Socket

  defmodule HeaderStore do
    use Musubi.Store

    state do
      field :user_name, String.t()
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, socket}

    @impl Musubi.Store
    def render(socket) do
      %{user_name: socket.assigns.user_name}
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defmodule RawMapRootStore do
    use Musubi.Store

    state do
      field :header, HeaderStore.state()
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, socket}

    @impl Musubi.Store
    def render(_socket) do
      %{header: %{user_name: "Alice"}}
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defmodule PlaceholderRootStore do
    use Musubi.Store

    state do
      field :header, HeaderStore.state()
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, socket}

    @impl Musubi.Store
    def render(socket) do
      %{header: child(HeaderStore, id: "header", user_name: socket.assigns.user_name)}
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defmodule MountInertRootStore do
    use Musubi.Store

    state do
      field :title, String.t()
    end

    @impl Musubi.Store
    def mount(socket) do
      {:ok, Musubi.Socket.assign(socket, :tmp, child(HeaderStore, id: "x", user_name: "tmp"))}
    end

    @impl Musubi.Store
    def render(_socket) do
      %{title: "ready"}
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defmodule NestedStreamRootStore do
    use Musubi.Store

    state do
      field :feed do
        stream :messages do
          field :body, String.t()
        end
      end

      stream :users, %{id: String.t(), name: String.t()}
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, socket}

    @impl Musubi.Store
    def render(_socket) do
      %{feed: %{messages: stream(:messages)}, users: stream(:users)}
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defmodule AsyncStreamRootStore do
    use Musubi.Store

    state do
      stream_async :messages, %{id: String.t(), body: String.t()}
    end

    @impl Musubi.Store
    def mount(socket),
      do: {:ok, Musubi.Socket.assign(socket, :messages, Musubi.AsyncResult.loading())}

    @impl Musubi.Store
    def render(_socket) do
      %{messages: async_stream(:messages)}
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defmodule MissingStreamRootStore do
    use Musubi.Store

    state do
      stream :messages, String.t()
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, socket}
    @impl Musubi.Store
    def render(_socket), do: %{}
    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defmodule WrongStreamPathRootStore do
    use Musubi.Store

    state do
      field :feed do
        stream :messages, String.t()
      end
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, socket}
    @impl Musubi.Store
    def render(_socket), do: %{messages: stream(:messages)}
    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defmodule DuplicateStreamRootStore do
    use Musubi.Store

    state do
      field :feed do
        stream :messages, String.t()
      end
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, socket}

    @impl Musubi.Store
    def render(_socket) do
      %{feed: %{"messages" => stream(:messages), :messages => stream(:messages)}}
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defmodule ListStreamRootStore do
    use Musubi.Store

    state do
      stream :messages, String.t()
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, socket}
    @impl Musubi.Store
    def render(_socket), do: %{messages: []}
    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defmodule HandWrittenStreamMarkerRootStore do
    use Musubi.Store

    state do
      stream :messages, String.t()
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, socket}
    @impl Musubi.Store
    def render(_socket), do: %{messages: %{__musubi_stream__: "messages"}}
    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defmodule ListChildStore do
    use Musubi.Store

    state do
      field :label, String.t()
      field :preserved, String.t()
    end

    @impl Musubi.Store
    def mount(socket) do
      send(socket.assigns.test_pid, {:mount, socket.id})
      {:ok, Musubi.Socket.assign(socket, :preserved, "mounted-#{socket.id}")}
    end

    @impl Musubi.Store
    def render(socket) do
      %{label: socket.assigns.label, preserved: socket.assigns.preserved}
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defmodule ListRootStore do
    use Musubi.Store

    state do
      field :items, list(ListChildStore.state())
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, socket}

    @impl Musubi.Store
    def render(socket) do
      %{
        items:
          Enum.map(socket.assigns.rows, fn %{id: id, label: label} ->
            child(ListChildStore, id: id, label: label, test_pid: socket.assigns.test_pid)
          end)
      }
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defmodule DerivedLineChildStore do
    use Musubi.Store

    attr :line, map(), required: true

    state do
      field :sku, String.t()
      field :qty, integer()
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, mirror_line(socket, socket.assigns.line)}

    @impl Musubi.Store
    def render(socket) do
      %{sku: socket.assigns.sku, qty: socket.assigns.qty}
    end

    @impl Musubi.Store
    def update(params, socket), do: {:ok, mirror_line(socket, params.line)}

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}

    defp mirror_line(socket, line) do
      socket
      |> Socket.assign(:sku, line.sku)
      |> Socket.assign(:qty, line.qty)
    end
  end

  defmodule DerivedLineRootStore do
    use Musubi.Store

    state do
      field :lines, list(DerivedLineChildStore.state())
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, socket}

    @impl Musubi.Store
    def render(socket) do
      %{
        lines:
          Enum.map(socket.assigns.lines, fn line ->
            child(DerivedLineChildStore, id: line.id, line: line)
          end)
      }
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defmodule FilterStoreV1 do
    use Musubi.Store

    state do
      field :version, String.t()
    end

    @impl Musubi.Store
    def mount(socket) do
      send(socket.assigns.test_pid, :mounted_v1)
      {:ok, Musubi.Socket.assign(socket, :version, "v1")}
    end

    @impl Musubi.Store
    def render(socket) do
      %{version: socket.assigns.version}
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defmodule FilterStoreV2 do
    use Musubi.Store

    state do
      field :version, String.t()
    end

    @impl Musubi.Store
    def mount(socket) do
      send(socket.assigns.test_pid, :mounted_v2)
      {:ok, Musubi.Socket.assign(socket, :version, "v2")}
    end

    @impl Musubi.Store
    def render(socket) do
      %{version: socket.assigns.version}
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defmodule ModuleSwapRootStore do
    use Musubi.Store

    state do
      field :filters, map()
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, socket}

    @impl Musubi.Store
    def render(socket) do
      %{
        filters:
          child(socket.assigns.filters_module, id: "filters", test_pid: socket.assigns.test_pid)
      }
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defmodule DuplicateChildStore do
    use Musubi.Store

    state do
      field :value, String.t()
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, socket}

    @impl Musubi.Store
    def render(socket) do
      %{value: socket.assigns.value}
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defmodule DuplicateRootStore do
    use Musubi.Store

    state do
      field :items, list(DuplicateChildStore.state())
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, socket}

    @impl Musubi.Store
    def render(_socket) do
      %{
        items: [
          child(DuplicateChildStore, id: "static", value: "a"),
          child(DuplicateChildStore, id: "static", value: "b")
        ]
      }
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defmodule DefaultUpdateChildStore do
    use Musubi.Store

    state do
      field :title, String.t()
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, socket}

    @impl Musubi.Store
    def render(socket) do
      %{title: socket.assigns.title}
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defmodule DefaultUpdateRootStore do
    use Musubi.Store

    state do
      field :child, DefaultUpdateChildStore.state()
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, socket}

    @impl Musubi.Store
    def render(socket) do
      %{child: child(DefaultUpdateChildStore, id: "child", title: socket.assigns.title)}
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defmodule AssignedChildRootStore do
    use Musubi.Store

    state do
      field :child, map() | nil
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, socket}

    @impl Musubi.Store
    def render(socket) do
      %{child: socket.assigns.child}
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defmodule ToggleChildRootStore do
    use Musubi.Store

    state do
      field :child, map() | nil
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, socket}

    @impl Musubi.Store
    def render(socket) do
      if socket.assigns.show? do
        %{child: socket.assigns.child}
      else
        %{child: nil}
      end
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defmodule MemoChildStore do
    use Musubi.Store

    state do
      field :title, String.t()
    end

    @impl Musubi.Store
    def mount(socket) do
      send(socket.assigns.test_pid, :memo_mount)
      {:ok, socket}
    end

    @impl Musubi.Store
    def render(socket) do
      send(socket.assigns.test_pid, :memo_to_state)
      %{title: socket.assigns.title}
    end

    @impl Musubi.Store
    def update(_new_assigns, socket) do
      send(socket.assigns.test_pid, :memo_update)
      {:ok, socket}
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defmodule MemoRootStore do
    use Musubi.Store

    state do
      field :child, MemoChildStore.state()
      field :sibling_field, integer()
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, socket}

    @impl Musubi.Store
    def render(socket) do
      %{
        child:
          child(MemoChildStore,
            id: "child",
            title: socket.assigns.title,
            test_pid: socket.assigns.test_pid
          ),
        sibling_field: socket.assigns.sibling_field
      }
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defmodule BadMountChildStore do
    use Musubi.Store

    state do
      field :value, String.t()
    end

    @impl Musubi.Store
    def mount(_socket) do
      {:error, :db_unavailable}
    end

    @impl Musubi.Store
    def render(_socket) do
      %{value: "never"}
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defmodule BadUpdateChildStore do
    use Musubi.Store

    state do
      field :value, String.t()
    end

    @impl Musubi.Store
    def mount(socket) do
      {:ok, socket}
    end

    @impl Musubi.Store
    def render(socket) do
      %{value: socket.assigns.value}
    end

    @impl Musubi.Store
    def update(_new_assigns, _socket) do
      :bad
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defmodule BadLifecycleRootStore do
    use Musubi.Store

    state do
      field :child, map()
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, socket}

    @impl Musubi.Store
    def render(socket) do
      %{child: child(socket.assigns.child_module, id: "child", value: socket.assigns.value)}
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defmodule RaisingChildStore do
    use Musubi.Store

    state do
      field :value, String.t()
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, socket}

    @impl Musubi.Store
    def render(_socket) do
      raise KeyError, key: :value, term: %{}
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defmodule RaisingRootStore do
    use Musubi.Store

    state do
      field :child, map()
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, socket}

    @impl Musubi.Store
    def render(_socket) do
      raise KeyError, key: :boom, term: %{}
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  describe "Render Contract" do
    test "Render output uses child placeholders for nested store fields" do
      socket = root_socket(PlaceholderRootStore, %{user_name: "Alice"})
      registry = registry(socket)

      assert {:ok, %{header: %{user_name: "Alice"}}, _socket, resolved_registry} =
               Resolver.resolve(socket, registry)

      assert StoreTable.get(resolved_registry, ["header"])
    end

    test "Render output uses raw maps for nested store types" do
      socket = root_socket(RawMapRootStore)
      registry = registry(socket)

      assert {:ok, %{header: %{user_name: "Alice"}}, _socket, resolved_registry} =
               Resolver.resolve(socket, registry)

      assert StoreTable.keys(resolved_registry) == [[]]
    end

    test "Resolver evaluates child placeholders before the parent's output is finalized" do
      socket = root_socket(PlaceholderRootStore, %{user_name: "Alice"})
      registry = registry(socket)

      assert {:ok, resolved_root, _socket, _registry} = Resolver.resolve(socket, registry)

      assert resolved_root == %{
               header: %{user_name: "Alice", __musubi_store_id__: ["header"]},
               __musubi_store_id__: []
             }
    end

    test "stream placeholders render wire markers at nested declared paths" do
      socket = root_socket(NestedStreamRootStore)
      registry = registry(socket)

      assert {:ok, resolved_root, _socket, resolved_registry} = Resolver.resolve(socket, registry)

      assert %{
               feed: %{messages: %{__musubi_stream__: "messages"}},
               users: %{__musubi_stream__: "users"},
               __musubi_store_id__: []
             } = resolved_root

      assert %Entry{wire_state: wire_state} = StoreTable.get(resolved_registry, [])
      assert %{"feed" => %{"messages" => %{"__musubi_stream__" => "messages"}}} = wire_state
      assert %{"users" => %{"__musubi_stream__" => "users"}} = wire_state
    end

    test "async stream placeholders render markers inside AsyncResult.result" do
      socket = root_socket(AsyncStreamRootStore)
      registry = registry(socket)

      assert {:ok, resolved_root, _socket, resolved_registry} = Resolver.resolve(socket, registry)

      assert %{
               messages: %AsyncResult{
                 status: :loading,
                 result: %{__musubi_stream__: "messages"},
                 reason: nil
               },
               __musubi_store_id__: []
             } = resolved_root

      assert %Entry{wire_state: wire_state} = StoreTable.get(resolved_registry, [])

      assert %{
               "messages" => %{
                 "__musubi_async__" => true,
                 "status" => "loading",
                 "result" => %{"__musubi_stream__" => "messages"},
                 "reason" => nil
               }
             } = wire_state
    end

    test "declared streams must be rendered with stream/1" do
      socket = root_socket(MissingStreamRootStore)

      assert_raise ArgumentError, ~r/declared stream :messages was not rendered/, fn ->
        Resolver.resolve(socket, registry(socket))
      end
    end

    test "stream placeholders must match their declared schema path" do
      socket = root_socket(WrongStreamPathRootStore)

      assert_raise ArgumentError, ~r/rendered at \/messages.*declared at \/feed\/messages/, fn ->
        Resolver.resolve(socket, registry(socket))
      end
    end

    test "a stream may be placed only once" do
      socket = root_socket(DuplicateStreamRootStore)

      assert_raise ArgumentError, ~r/stream :messages rendered more than once/, fn ->
        Resolver.resolve(socket, registry(socket))
      end
    end

    test "plain lists are not valid stream placeholders" do
      socket = root_socket(ListStreamRootStore)

      assert_raise ArgumentError, ~r/declared stream :messages was not rendered/, fn ->
        Resolver.resolve(socket, registry(socket))
      end
    end

    test "user-authored stream marker maps are rejected" do
      socket = root_socket(HandWrittenStreamMarkerRootStore)

      assert_raise ArgumentError, ~r/was not produced by stream\(:name\)/, fn ->
        Resolver.resolve(socket, registry(socket))
      end
    end

    test "Reordering a keyed list preserves child assigns" do
      rows = [%{id: "a", label: "A"}, %{id: "b", label: "B"}]
      socket = root_socket(ListRootStore, %{rows: rows, test_pid: self()})
      registry = registry(socket)

      assert {:ok, %{items: [%{preserved: "mounted-a"}, %{preserved: "mounted-b"}]}, socket,
              registry} =
               Resolver.resolve(socket, registry)

      assert_receive {:mount, "a"}
      assert_receive {:mount, "b"}

      reordered_socket =
        socket
        |> Socket.assign(:rows, Enum.reverse(rows))
        |> Socket.assign(:unrelated, true)

      assert {:ok, %{items: [%{preserved: "mounted-b"}, %{preserved: "mounted-a"}]}, _socket,
              _registry} =
               Resolver.resolve(reordered_socket, registry)

      refute_receive {:mount, _id}
    end

    test "derived child assigns update when their parent source assign changes" do
      lines = [%{id: "mug", sku: "mug", qty: 1}]
      socket = root_socket(DerivedLineRootStore, %{lines: lines})
      registry = registry(socket)

      assert {:ok, %{lines: [%{sku: "mug", qty: 1}]}, socket, registry} =
               Resolver.resolve(socket, registry)

      next_lines = [%{id: "mug", sku: "mug", qty: 2}]
      next_socket = Socket.assign(socket, :lines, next_lines)

      assert {:ok, %{lines: [%{sku: "mug", qty: 2}]}, _socket, _registry} =
               Resolver.resolve(next_socket, registry)
    end

    test "Changing a child's module remounts a fresh node" do
      socket =
        root_socket(ModuleSwapRootStore, %{filters_module: FilterStoreV1, test_pid: self()})

      registry = registry(socket)

      assert {:ok, %{filters: %{version: "v1"}}, socket, registry} =
               Resolver.resolve(socket, registry)

      assert_receive :mounted_v1

      next_socket = Socket.assign(socket, :filters_module, FilterStoreV2)

      assert {:ok, %{filters: %{version: "v2"}}, _socket, next_registry} =
               Resolver.resolve(next_socket, registry)

      assert_receive :mounted_v2
      assert %Entry{module: FilterStoreV2} = StoreTable.get(next_registry, ["filters"])
    end

    test "Duplicate ids in a list reconcile to a hard runtime error" do
      socket = root_socket(DuplicateRootStore)

      assert_raise ArgumentError, ~r/duplicate child store_id/, fn ->
        Resolver.resolve(socket, registry(socket))
      end
    end

    test "Missing id is rejected" do
      child = Musubi.Child.child(DuplicateChildStore, value: "missing")
      socket = root_socket(AssignedChildRootStore, %{child: child})

      assert_raise ArgumentError, ~r/missing required :id/, fn ->
        Resolver.resolve(socket, registry(socket))
      end
    end

    test "Non-string id is rejected" do
      child = Musubi.Child.child(DuplicateChildStore, id: 42, value: "bad")
      socket = root_socket(AssignedChildRootStore, %{child: child})

      assert_raise ArgumentError, ~r/id must be a binary string/, fn ->
        Resolver.resolve(socket, registry(socket))
      end
    end

    test "First appearance triggers mount and render" do
      socket = root_socket(PlaceholderRootStore, %{user_name: "Alice"})

      assert {:ok, %{header: %{user_name: "Alice"}}, _socket, _registry} =
               Resolver.resolve(socket, registry(socket))
    end

    test "Disappearance silently discards the node" do
      child = Musubi.Child.child(HeaderStore, id: "header", user_name: "Alice")
      socket = root_socket(ToggleChildRootStore, %{show?: true, child: child})
      registry = registry(socket)

      assert {:ok, _resolved, socket, registry} = Resolver.resolve(socket, registry)
      assert StoreTable.get(registry, ["header"])

      next_socket = Socket.assign(socket, :show?, false)

      assert {:ok, %{child: nil}, _socket, next_registry} =
               Resolver.resolve(next_socket, registry)

      refute StoreTable.get(next_registry, ["header"])
    end

    test "A store may omit update/2; the default merges new_assigns into socket.assigns" do
      socket = root_socket(DefaultUpdateRootStore, %{title: "Inbox"})
      registry = registry(socket)

      assert {:ok, %{child: %{title: "Inbox"}}, socket, registry} =
               Resolver.resolve(socket, registry)

      next_socket = Socket.assign(socket, :title, "Archive")

      assert {:ok, %{child: %{title: "Archive"}}, _socket, _registry} =
               Resolver.resolve(next_socket, registry)
    end

    test "Unrelated sibling mutates assigns this child does not consume" do
      socket = root_socket(MemoRootStore, %{title: "Inbox", sibling_field: 1, test_pid: self()})
      registry = registry(socket)

      assert {:ok, %{child: %{title: "Inbox"}, sibling_field: 1}, socket, registry} =
               Resolver.resolve(socket, registry)

      assert_receive :memo_mount
      assert_receive :memo_to_state

      next_socket = Socket.assign(socket, :sibling_field, 2)

      assert {:ok, %{child: %{title: "Inbox"}, sibling_field: 2}, _socket, _registry} =
               Resolver.resolve(next_socket, registry)

      refute_receive :memo_update
      refute_receive :memo_to_state
    end

    test "assign/3 with the same value is a no-op (no entry recorded in __changed__)" do
      socket = root_socket(MemoRootStore, %{title: "Inbox", sibling_field: 1, test_pid: self()})
      registry = registry(socket)

      assert {:ok, _resolved, socket, registry} = Resolver.resolve(socket, registry)
      assert_receive :memo_mount
      assert_receive :memo_to_state

      next_socket = Socket.assign(socket, :title, socket.assigns.title)

      assert {:ok, _resolved, _socket, _registry} = Resolver.resolve(next_socket, registry)
      refute_receive :memo_update
      refute_receive :memo_to_state
    end

    test "__changed__ is reset after each render cycle" do
      socket = root_socket(DefaultUpdateRootStore, %{title: "Inbox"})

      assert {:ok, _resolved, resolved_socket, _registry} =
               Resolver.resolve(socket, registry(socket))

      assert resolved_socket.assigns.__changed__ == %{}
    end

    test "Toggling :if=false then :if=true on the same identity remounts" do
      child = Musubi.Child.child(ListChildStore, id: "n", label: "Notice", test_pid: self())
      socket = root_socket(ToggleChildRootStore, %{show?: true, child: child})
      registry = registry(socket)

      assert {:ok, _resolved, socket, registry} = Resolver.resolve(socket, registry)
      assert_receive {:mount, "n"}

      dropped_socket = Socket.assign(socket, :show?, false)

      assert {:ok, _resolved, dropped_socket, dropped_registry} =
               Resolver.resolve(dropped_socket, registry)

      refute StoreTable.get(dropped_registry, ["n"])

      remount_socket = Socket.assign(dropped_socket, :show?, true)

      assert {:ok, _resolved, _socket, _registry} =
               Resolver.resolve(remount_socket, dropped_registry)

      assert_receive {:mount, "n"}
    end

    test "child(...) called inside mount has no effect" do
      socket = root_socket(MountInertRootStore)

      assert {:ok, %{title: "ready"}, _socket, resolved_registry} =
               Resolver.resolve(Reconciler.init_store(socket), registry(socket))

      assert StoreTable.keys(resolved_registry) == [[]]
    end
  end

  describe "let-it-crash lifecycle failures" do
    setup do
      Process.flag(:trap_exit, true)
      :ok
    end

    test "Render raise crashes the runtime" do
      capture_log(fn ->
        pid =
          spawn_link(fn ->
            try do
              socket = root_socket(RaisingRootStore)
              Resolver.resolve(socket, registry(socket))
            rescue
              error -> exit({error, __STACKTRACE__})
            end
          end)

        ref = Process.monitor(pid)
        assert_receive {:EXIT, ^pid, {%KeyError{}, _stacktrace}}
        assert_receive {:DOWN, ^ref, :process, ^pid, _reason}
        Logger.flush()
      end)
    end

    test "Returning {:error, reason} from mount raises" do
      child = Musubi.Child.child(BadMountChildStore, id: "child")

      capture_log(fn ->
        pid =
          spawn_link(fn ->
            try do
              socket = root_socket(AssignedChildRootStore, %{child: child})
              Resolver.resolve(socket, registry(socket))
            rescue
              error -> exit({error, __STACKTRACE__})
            end
          end)

        ref = Process.monitor(pid)
        assert_receive {:EXIT, ^pid, {%ArgumentError{}, _stacktrace}}
        assert_receive {:DOWN, ^ref, :process, ^pid, _reason}
        Logger.flush()
      end)
    end

    test "update non-conforming returns raise" do
      socket =
        root_socket(BadLifecycleRootStore, %{child_module: BadUpdateChildStore, value: "one"})

      registry = registry(socket)

      assert {:ok, _resolved, socket, registry} = Resolver.resolve(socket, registry)

      capture_log(fn ->
        pid =
          spawn_link(fn ->
            try do
              Resolver.resolve(Socket.assign(socket, :value, "two"), registry)
            rescue
              error -> exit({error, __STACKTRACE__})
            end
          end)

        ref = Process.monitor(pid)
        assert_receive {:EXIT, ^pid, {%ArgumentError{}, _stacktrace}}
        assert_receive {:DOWN, ^ref, :process, ^pid, _reason}
        Logger.flush()
      end)
    end
  end

  describe "lifecycle pipeline" do
    test ":after_render receives the Elixir-form frame during resolve" do
      test_pid = self()
      socket = root_socket(RawMapRootStore)

      # `:after_serialize` no longer runs in resolve — it moved to the page
      # server's aggregation phase (over the wire frame + drained events).
      socket =
        Lifecycle.attach_hook(socket, :elixir, :after_render, fn frame, current_socket ->
          send(test_pid, {:after_render, frame})
          {:cont, frame, current_socket}
        end)

      assert {:ok, _resolved, _socket, registry} = Resolver.resolve(socket, registry(socket))

      assert_receive {:after_render, %Musubi.Page.Frame{render: %{header: %{user_name: "Alice"}}}}

      assert %Entry{
               resolved_state: %{header: %{user_name: "Alice"}},
               wire_state: %{"header" => %{"user_name" => "Alice"}}
             } = StoreTable.get(registry, [])
    end
  end

  defp registry(%Socket{} = socket) do
    StoreTable.put(
      StoreTable.new(),
      Socket.store_id(socket),
      %Entry{
        socket: socket,
        module: socket.module
      }
    )
  end

  defp root_socket(module, assigns \\ %{}) when is_atom(module) and is_map(assigns) do
    Socket.assign(
      %Socket{id: "", parent_path: [], module: module, assigns: %{}, private: %{}},
      assigns
    )
  end
end
