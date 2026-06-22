defmodule Musubi.ReconcilerFalsePositiveTest do
  use ExUnit.Case, async: true

  alias Musubi.Page.StoreTable
  alias Musubi.Page.StoreTable.Entry
  alias Musubi.Resolver
  alias Musubi.Socket

  defmodule ChildStore do
    use Musubi.Store

    attr :title, String.t(), required: true
    attr :test_pid, pid(), required: true

    state do
      field :title, String.t()
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, socket}

    @impl Musubi.Store
    def update(assigns, socket) do
      send(socket.assigns.test_pid, :child_update)
      {:ok, Socket.assign(socket, assigns)}
    end

    @impl Musubi.Store
    def render(socket) do
      send(socket.assigns.test_pid, :child_render)
      %{title: socket.assigns.title}
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defmodule ParentStore do
    use Musubi.Store

    state do
      field :child, ChildStore.state()
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, socket}

    @impl Musubi.Store
    def render(socket) do
      %{
        child: child(ChildStore, id: "c", title: "static", test_pid: socket.assigns.test_pid)
      }
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defmodule SelfMutatingChild do
    use Musubi.Store

    attr :title, String.t(), required: true
    attr :test_pid, pid(), required: true

    state do
      field :title, String.t()
    end

    @impl Musubi.Store
    # Overwrites a consumed-key-named assign in its own lifecycle, so the
    # child's live assigns diverge from the props the parent passed.
    def mount(socket), do: {:ok, Socket.assign(socket, :title, "mutated")}

    @impl Musubi.Store
    def update(assigns, socket) do
      send(socket.assigns.test_pid, :child_update)
      {:ok, socket |> Socket.assign(assigns) |> Socket.assign(:title, "mutated")}
    end

    @impl Musubi.Store
    def render(socket) do
      send(socket.assigns.test_pid, :child_render)
      %{title: socket.assigns.title}
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defmodule SnapshotParent do
    use Musubi.Store

    state do
      field :child, SelfMutatingChild.state()
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, socket}

    @impl Musubi.Store
    def render(socket) do
      %{
        child:
          child(SelfMutatingChild,
            id: "c",
            title: socket.assigns.title_prop,
            test_pid: socket.assigns.test_pid
          )
      }
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  test "no phantom update/2 when child overwrote a consumed-key assign and parent props are unchanged" do
    socket = root_socket(SnapshotParent, %{title_prop: "static", test_pid: self()})
    registry = registry(socket)

    assert {:ok, _resolved, root, registry} = Resolver.resolve(socket, registry)
    assert_receive :child_render

    # Dirty the parent via a key NOT passed to the child; child props unchanged.
    next = Socket.assign(root, :nonce, 1)

    assert {:ok, _resolved, _root, _registry} = Resolver.resolve(next, registry)

    refute_receive :child_update, 50
    refute_receive :child_render, 50
  end

  test "child re-runs update/2 when a parent prop value genuinely changes" do
    socket = root_socket(SnapshotParent, %{title_prop: "static", test_pid: self()})
    registry = registry(socket)

    assert {:ok, _resolved, root, registry} = Resolver.resolve(socket, registry)
    assert_receive :child_render

    next = Socket.assign(root, :title_prop, "changed")

    assert {:ok, _resolved, _root, _registry} = Resolver.resolve(next, registry)

    assert_receive :child_update
  end

  test "child internally dirty re-renders via subtree_dirty? without re-running update/2" do
    socket = root_socket(SnapshotParent, %{title_prop: "static", test_pid: self()})
    registry = registry(socket)

    assert {:ok, _resolved, root, registry} = Resolver.resolve(socket, registry)
    assert_receive :child_render

    %Entry{socket: child_socket} = entry = StoreTable.get(registry, ["c"])
    dirtied = Socket.assign(child_socket, :tick, 1)
    registry = StoreTable.put(registry, ["c"], %{entry | socket: dirtied})

    assert {:ok, _resolved, _root, _registry} = Resolver.resolve(root, registry)

    assert_receive :child_render
    refute_receive :child_update, 50
  end

  test "child reuses when parent dirty key has the same existing child assign value" do
    socket = root_socket(ParentStore, %{title: "Inbox", test_pid: self()})
    registry = registry(socket)

    assert {:ok, _resolved_root, root_socket, registry} = Resolver.resolve(socket, registry)
    assert_receive :child_render

    next_socket = Socket.assign(root_socket, :title, "Outbox")

    assert {:ok, _resolved_root, _next_root_socket, _registry} =
             Resolver.resolve(next_socket, registry)

    refute_receive :child_update, 50
    refute_receive :child_render, 50
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

  defp root_socket(module, assigns) when is_atom(module) and is_map(assigns) do
    Socket.assign(
      %Socket{id: "", parent_path: [], module: module, assigns: %{}, private: %{}},
      assigns
    )
  end
end
