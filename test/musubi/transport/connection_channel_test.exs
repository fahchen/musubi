defmodule Musubi.Transport.ConnectionChannelTest do
  use ExUnit.Case, async: true

  defmodule TestEndpoint do
    @moduledoc false
    use Phoenix.Endpoint, otp_app: :musubi

    socket("/musubi", Musubi.Transport.ConnectionChannelTest.MusubiSocket,
      websocket: false,
      longpoll: false
    )
  end

  defmodule ChildStore do
    @moduledoc false
    use Musubi.Store

    state do
      field :label, String.t()
    end

    @impl Musubi.Store
    def init(socket) do
      socket
      |> Musubi.Socket.session()
      |> Map.fetch!("test_pid")
      |> send({:child_init, Musubi.Socket.session(socket), Musubi.Socket.connect_info(socket)})

      {:ok, socket}
    end

    @impl Musubi.Store
    def render(socket), do: %{label: socket.assigns.label}

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defmodule AlphaRootStore do
    @moduledoc false
    use Musubi.Store, root: true

    attr :room_id, String.t(), required: true

    state do
      field :room_id, String.t()
      field :current_user, String.t()
      field :child, ChildStore.state()
    end

    @impl Musubi.Store
    def mount(params, socket) do
      session = Musubi.Socket.session(socket)
      test_pid = Map.fetch!(session, "test_pid")

      send(test_pid, {:alpha_mount, self(), params, socket.assigns.current_user})

      socket = Musubi.Socket.assign(socket, :room_id, Map.fetch!(params, "room_id"))

      {:ok, socket}
    end

    @impl Musubi.Store
    def init(socket) do
      test_pid = Map.fetch!(Musubi.Socket.session(socket), "test_pid")
      send(test_pid, {:alpha_init, socket.assigns.room_id})
      {:ok, socket}
    end

    @impl Musubi.Store
    def render(socket) do
      %{
        room_id: socket.assigns.room_id,
        current_user: socket.assigns.current_user,
        child: child(ChildStore, id: "child", label: socket.assigns.room_id)
      }
    end

    command :rename do
      payload do
        field :room_id, String.t()
      end
    end

    @impl Musubi.Store
    def handle_command(:rename, %{"room_id" => room_id}, socket) do
      {:noreply, Musubi.Socket.assign(socket, :room_id, room_id)}
    end
  end

  defmodule BetaRootStore do
    @moduledoc false
    use Musubi.Store, root: true

    state do
      field :label, String.t()
      field :current_user, String.t()
    end

    @impl Musubi.Store
    def mount(params, socket) do
      test_pid = Map.fetch!(Musubi.Socket.session(socket), "test_pid")
      send(test_pid, {:beta_mount, self(), params, socket.assigns.current_user})
      {:ok, Musubi.Socket.assign(socket, :label, Map.fetch!(params, "label"))}
    end

    @impl Musubi.Store
    def render(socket) do
      %{label: socket.assigns.label, current_user: socket.assigns.current_user}
    end

    command :rename do
      payload do
        field :label, String.t()
      end
    end

    @impl Musubi.Store
    def handle_command(:rename, %{"label" => label}, socket) do
      {:noreply, Musubi.Socket.assign(socket, :label, label)}
    end
  end

  defmodule MusubiSocket do
    @moduledoc false
    use Musubi.Socket,
      roots: [
        Musubi.Transport.ConnectionChannelTest.AlphaRootStore,
        Musubi.Transport.ConnectionChannelTest.BetaRootStore
      ]

    @impl Musubi.Socket
    def handle_connect(%{"current_user" => current_user}, socket) do
      {:ok, Musubi.Socket.assign(socket, :current_user, current_user)}
    end

    @impl Musubi.Socket
    def handle_join(params, socket) do
      test_pid = socket |> Musubi.Socket.session() |> Map.fetch!("test_pid")
      send(test_pid, {:connection_join, params, socket.assigns.current_user})

      {:ok, socket}
    end
  end

  import Phoenix.ChannelTest

  @endpoint TestEndpoint
  @alpha_module_str "Musubi.Transport.ConnectionChannelTest.AlphaRootStore"
  @beta_module_str "Musubi.Transport.ConnectionChannelTest.BetaRootStore"

  setup_all do
    start_supervised!({Phoenix.PubSub, name: Musubi.Transport.ConnectionChannelTest.PubSub})
    start_supervised!(TestEndpoint)
    :ok
  end

  setup do
    Process.flag(:trap_exit, true)
    :ok
  end

  test "join mounts the root and shares connect assigns/session with the root and its children" do
    {:ok, %{"root_id" => alpha_root}, _socket} =
      join_root(@alpha_module_str, "alpha-1", %{"room_id" => "general"})

    assert alpha_root == "#{@alpha_module_str}:alpha-1"

    # handle_join runs per channel and now sees the mount params on the join.
    assert_receive {:connection_join,
                    %{"module" => @alpha_module_str, "id" => "alpha-1", "params" => %{"room_id" => "general"}},
                    "connect-user"}

    assert_receive {:alpha_mount, alpha_pid, %{"room_id" => "general"}, "connect-user"}
    assert_receive {:alpha_init, "general"}

    assert_receive {:child_init, %{"test_pid" => _test_pid, "user_id" => "u1"},
                    %{peer_data: %{address: {127, 0, 0, 1}}}}

    assert is_pid(alpha_pid)

    assert_push("patch", %{
      "root_id" => ^alpha_root,
      "ops" => [
        %{
          op: "replace",
          path: "",
          value: %{
            "room_id" => "general",
            "current_user" => "connect-user",
            "child" => %{"label" => "general"}
          }
        }
      ]
    })
  end

  test "command routes to the channel's root and patches it" do
    {:ok, %{"root_id" => alpha_root}, socket} =
      join_root(@alpha_module_str, "alpha-1", %{"room_id" => "general"})

    assert_receive {:alpha_mount, _pid, _params, _current_user}
    assert_receive {:alpha_init, "general"}
    assert_receive {:child_init, _session, _connect_info}
    assert_push("patch", %{"root_id" => ^alpha_root, "version" => 1})

    command_ref =
      push(socket, "command", %{
        "store_id" => [],
        "name" => "rename",
        "payload" => %{"room_id" => "random"}
      })

    assert_reply(command_ref, :ok, %{})

    assert_push("patch", %{
      "root_id" => ^alpha_root,
      "version" => 2,
      "ops" => ops
    })

    assert %{op: "replace", path: "/child/label", value: "random"} in ops
    assert %{op: "replace", path: "/room_id", value: "random"} in ops
  end

  test "malformed command payload replies with an error without stopping the root" do
    {:ok, %{"root_id" => beta_root}, socket} =
      join_root(@beta_module_str, "beta-1", %{"label" => "secondary"})

    assert_receive {:beta_mount, beta_pid, _params, _current_user}
    assert_push("patch", %{"root_id" => ^beta_root})

    beta_down = Process.monitor(beta_pid)

    missing_name_ref =
      push(socket, "command", %{
        "store_id" => [],
        "payload" => %{"label" => "bad"}
      })

    assert_reply(missing_name_ref, :error, %{reason: "missing required field"})
    refute_receive {:DOWN, ^beta_down, :process, ^beta_pid, _reason}

    command_ref =
      push(socket, "command", %{
        "store_id" => [],
        "name" => "rename",
        "payload" => %{"label" => "still-mounted"}
      })

    assert_reply(command_ref, :ok, %{})

    assert_push("patch", %{
      "root_id" => ^beta_root,
      "ops" => [%{path: "/label"}]
    })
  end

  test "unknown command replies with an error without stopping the root" do
    {:ok, %{"root_id" => alpha_root}, socket} =
      join_root(@alpha_module_str, "alpha-1", %{"room_id" => "general"})

    assert_receive {:alpha_mount, alpha_pid, _params, _current_user}
    assert_receive {:alpha_init, "general"}
    assert_receive {:child_init, _session, _connect_info}
    assert_push("patch", %{"root_id" => ^alpha_root})

    alpha_down = Process.monitor(alpha_pid)

    unknown_ref =
      push(socket, "command", %{
        "store_id" => ["child"],
        "name" => "missing",
        "payload" => %{}
      })

    assert_reply(unknown_ref, :error, %{reason: "unknown command"})
    refute_receive {:DOWN, ^alpha_down, :process, ^alpha_pid, _reason}

    command_ref =
      push(socket, "command", %{
        "store_id" => [],
        "name" => "rename",
        "payload" => %{"room_id" => "still-mounted"}
      })

    assert_reply(command_ref, :ok, %{})

    assert_push("patch", %{"root_id" => ^alpha_root, "ops" => ops})

    assert %{op: "replace", path: "/room_id", value: "still-mounted"} in ops
  end

  test "join rejects undeclared roots" do
    assert {:error, %{reason: "unknown root"}} =
             join_root("Unknown.RootStore", "unknown", %{})
  end

  test "join requires an id field" do
    result =
      subscribe_and_join(
        connected_socket(),
        Musubi.Transport.ConnectionChannel,
        "musubi:connection:legacy",
        %{
          "module" => @alpha_module_str,
          "root_id" => "legacy-root",
          "params" => %{"room_id" => "general"}
        }
      )

    assert {:error, %{reason: "missing root id"}} = result
    refute_receive {:alpha_mount, _pid, _params, _current_user}
  end

  test "the same caller id mounts independently under different modules" do
    {:ok, %{"root_id" => alpha_root}, _alpha_socket} =
      join_root(@alpha_module_str, "shared", %{"room_id" => "general"})

    assert alpha_root == "#{@alpha_module_str}:shared"
    assert_receive {:alpha_mount, _pid, _params, _current_user}
    assert_push("patch", %{"root_id" => ^alpha_root})

    {:ok, %{"root_id" => beta_root}, _beta_socket} =
      join_root(@beta_module_str, "shared", %{"label" => "secondary"})

    assert beta_root == "#{@beta_module_str}:shared"
    assert_receive {:beta_mount, _pid, _params, _current_user}
    assert_push("patch", %{"root_id" => ^beta_root})
  end

  test "leaving a root channel stops its root without affecting others" do
    {:ok, %{"root_id" => alpha_root}, alpha_socket} =
      join_root(@alpha_module_str, "alpha-1", %{"room_id" => "general"})

    assert_receive {:alpha_mount, alpha_pid, _params, _current_user}
    assert_receive {:alpha_init, "general"}
    assert_receive {:child_init, _session, _connect_info}
    assert_push("patch", %{"root_id" => ^alpha_root})

    {:ok, %{"root_id" => beta_root}, beta_socket} =
      join_root(@beta_module_str, "beta-1", %{"label" => "secondary"})

    assert_receive {:beta_mount, beta_pid, _params, _current_user}
    assert_push("patch", %{"root_id" => ^beta_root})

    alpha_down = Process.monitor(alpha_pid)
    beta_down = Process.monitor(beta_pid)

    leave_ref = leave(alpha_socket)
    assert_reply(leave_ref, :ok)

    assert_receive {:DOWN, ^alpha_down, :process, ^alpha_pid, _reason}
    refute_receive {:DOWN, ^beta_down, :process, ^beta_pid, _reason}

    command_ref =
      push(beta_socket, "command", %{
        "store_id" => [],
        "name" => "rename",
        "payload" => %{"label" => "still-mounted"}
      })

    assert_reply(command_ref, :ok, %{})

    assert_push("patch", %{
      "root_id" => ^beta_root,
      "ops" => [%{path: "/label"}]
    })
  end

  # Connect a socket and join this root's own channel; the join payload carries
  # the mount params (join IS the mount). Returns the raw `subscribe_and_join`
  # result so error cases can assert on it.
  defp join_root(module_str, id, params) do
    root_id = module_str <> ":" <> id
    topic = "musubi:connection:" <> root_id

    subscribe_and_join(
      connected_socket(socket_id: root_id),
      Musubi.Transport.ConnectionChannel,
      topic,
      %{"module" => module_str, "id" => id, "params" => params}
    )
  end

  defp connected_socket(opts \\ []) do
    socket_id = Keyword.get(opts, :socket_id, "user_id")
    session = %{"test_pid" => self(), "user_id" => "u1"}
    connect_info = %{session: session, peer_data: %{address: {127, 0, 0, 1}}}

    phoenix_socket = socket(MusubiSocket, socket_id, %{})

    {:ok, connected_socket} =
      MusubiSocket.connect(%{"current_user" => "connect-user"}, phoenix_socket, connect_info)

    connected_socket
  end
end
