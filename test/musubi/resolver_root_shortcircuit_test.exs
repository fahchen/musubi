defmodule Musubi.ResolverRootShortCircuitTest do
  use ExUnit.Case, async: true

  alias Musubi.Lifecycle
  alias Musubi.Page.StoreTable
  alias Musubi.Page.StoreTable.Entry
  alias Musubi.Resolver
  alias Musubi.Socket
  alias Musubi.Stream

  defmodule ShortCircuitChildStore do
    use Musubi.Store

    attr :test_pid, pid(), required: true

    state do
      field :count, integer()
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, Socket.assign(socket, :count, 0)}

    @impl Musubi.Store
    def render(socket) do
      %{count: socket.assigns.count}
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defmodule ShortCircuitRootStore do
    use Musubi.Store

    state do
      field :child, ShortCircuitChildStore.state()
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, socket}

    @impl Musubi.Store
    def render(socket) do
      send(socket.assigns.test_pid, :root_render_called)
      %{child: child(ShortCircuitChildStore, id: "child", test_pid: socket.assigns.test_pid)}
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defmodule RootStreamDirtyStore do
    use Musubi.Store

    state do
      field :child, ShortCircuitChildStore.state()
      stream :messages, %{id: String.t()}
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, socket}

    @impl Musubi.Store
    def render(socket) do
      send(socket.assigns.test_pid, :root_render_called)

      %{
        child: child(ShortCircuitChildStore, id: "child", test_pid: socket.assigns.test_pid),
        messages: stream(:messages)
      }
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  test "root render/1 is not invoked when root __changed__ is empty and only a child is dirty" do
    test_pid = self()
    root_socket = root_socket(ShortCircuitRootStore, %{test_pid: test_pid})

    socket =
      Lifecycle.attach_hook(root_socket, :root_after_render, :after_render, fn frame,
                                                                              current_socket ->
        send(test_pid, :root_after_render_called)
        {:cont, frame, current_socket}
      end)

    assert {:ok, %{child: %{count: 0}}, root_socket, registry} =
             Resolver.resolve(socket, registry(socket))

    assert_receive :root_render_called
    assert_receive :root_after_render_called
    refute_receive :root_render_called, 50
    refute_receive :root_after_render_called, 50

    assert %Entry{} = child_entry = StoreTable.get(registry, ["child"])
    assert %Socket{} = child_socket = child_entry.socket

    dirty_child_socket = Socket.assign(child_socket, :count, 1)

    dirty_registry =
      StoreTable.put(registry, ["child"], %Entry{child_entry | socket: dirty_child_socket})

    assert {:ok, %{child: %{count: 1}}, _next_root_socket, _next_registry} =
             Resolver.resolve(root_socket, dirty_registry)

    refute_receive :root_render_called, 50
    assert_receive :root_after_render_called
    refute_receive :root_after_render_called, 50
  end

  test "root render/1 still runs when the root has pending changed streams" do
    test_pid = self()
    root_socket = root_socket(RootStreamDirtyStore, %{test_pid: test_pid})

    socket =
      Lifecycle.attach_hook(root_socket, :root_after_render, :after_render, fn frame,
                                                                              current_socket ->
        send(test_pid, :root_after_render_called)
        {:cont, frame, current_socket}
      end)

    assert {:ok,
            %{
              child: %{count: 0, __musubi_store_id__: ["child"]},
              messages: %{__musubi_stream__: "messages"},
              __musubi_store_id__: []
            }, root_socket, registry} =
             Resolver.resolve(socket, registry(socket))

    assert_receive :root_render_called
    assert_receive :root_after_render_called
    refute_receive :root_render_called, 50
    refute_receive :root_after_render_called, 50

    dirty_root_socket = Stream.stream_insert(root_socket, :messages, %{id: "m1"})

    assert {:ok,
            %{
              child: %{count: 0, __musubi_store_id__: ["child"]},
              messages: %{__musubi_stream__: "messages"},
              __musubi_store_id__: []
            }, _next_root_socket, _next_registry} =
             Resolver.resolve(dirty_root_socket, registry)

    assert_receive :root_render_called
    assert_receive :root_after_render_called
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
