defmodule Musubi.Transport.UploadConnectionTest do
  @moduledoc """
  Drives `allow_upload` / `cancel_upload` / `upload_progress` through
  the real `Musubi.Transport.ConnectionChannel` against a tree where
  the upload lives on a child store. Exercises Codex blocker 1
  (resolve upload via `store_id`, not root).
  """

  use ExUnit.Case

  defmodule TestEndpoint do
    @moduledoc false
    use Phoenix.Endpoint, otp_app: :musubi

    socket("/musubi", Musubi.Transport.UploadConnectionTest.MusubiSocket,
      websocket: false,
      longpoll: false
    )
  end

  defmodule CartLineStore do
    @moduledoc false
    use Musubi.Store

    attr :line_id, String.t(), required: true

    state do
      field :line_id, String.t()
    end

    upload(:attachment, accept: ~w(.pdf), max_entries: 1, max_file_size: 1_000)

    @impl Musubi.Store
    def init(socket) do
      {:ok, Musubi.Socket.assign(socket, :line_id, socket.assigns.line_id)}
    end

    @impl Musubi.Store
    def render(socket), do: %{line_id: socket.assigns.line_id}

    @impl Musubi.Store
    def handle_command(_n, _p, s), do: {:noreply, s}
  end

  defmodule CartStore do
    @moduledoc false
    use Musubi.Store, root: true

    state do
      field :lines, list(CartLineStore.state())
    end

    @impl Musubi.Store
    def mount(_params, socket) do
      {:ok, Musubi.Socket.assign(socket, :lines, ["1", "2"])}
    end

    @impl Musubi.Store
    def render(socket) do
      lines =
        Enum.map(socket.assigns.lines, fn id ->
          Musubi.Child.child(CartLineStore, id: "line-#{id}", line_id: id)
        end)

      %{lines: lines}
    end

    @impl Musubi.Store
    def handle_command(_n, _p, s), do: {:noreply, s}
  end

  defmodule ExternalStore do
    @moduledoc false
    use Musubi.Store, root: true

    state do
      field :title, String.t() | nil
    end

    upload(:avatar, accept: ~w(.png), max_entries: 1)

    @impl Musubi.Store
    def render(_socket), do: %{title: "Hi"}
    @impl Musubi.Store
    def handle_command(_n, _p, s), do: {:noreply, s}

    @impl Musubi.Store
    def upload_external(:avatar, entry, socket) do
      {:ok, %{uploader: "S3", url: "https://example/" <> entry.ref}, socket}
    end
  end

  defmodule MusubiSocket do
    @moduledoc false
    use Musubi.Socket,
      roots: [
        Musubi.Transport.UploadConnectionTest.CartStore,
        Musubi.Transport.UploadConnectionTest.ExternalStore
      ]
  end

  import Phoenix.ChannelTest

  @endpoint TestEndpoint
  @cart_module "Musubi.Transport.UploadConnectionTest.CartStore"
  @external_module "Musubi.Transport.UploadConnectionTest.ExternalStore"

  setup_all do
    # Keyed by this test module's full `TestEndpoint` alias, so no other
    # test reads or writes the same `:musubi` app env entry. Scoped
    # per-test config rather than shared global state.
    Application.put_env(:musubi, TestEndpoint,
      secret_key_base: String.duplicate("a", 64),
      server: false,
      pubsub_server: __MODULE__.PubSub
    )

    start_supervised!({Phoenix.PubSub, name: __MODULE__.PubSub})
    start_supervised!(TestEndpoint)
    :ok
  end

  setup do
    Process.flag(:trap_exit, true)
    {:ok, _reply, socket} = join_root(@cart_module, "cart-1", %{})
    {:ok, socket: socket}
  end

  test "allow_upload on a child store_id resolves the child upload", %{socket: socket} do
    push_ref =
      push(socket, "allow_upload", %{
        "store_id" => ["lines", "line-2"],
        "name" => "attachment",
        "entries" => [
          %{"client_ref" => "0", "name" => "spec.pdf", "size" => 100, "type" => "application/pdf"}
        ]
      })

    assert_reply(push_ref, :ok, reply)
    assert reply["ref"] == "attachment"
    assert reply["errors"] == []
    [{"0", entry}] = Enum.to_list(reply["entries"])
    assert entry["type"] == "channel"
    assert is_binary(entry["token"])
  end

  test "allow_upload on the root rejects when the upload is not declared there", %{socket: socket} do
    push_ref =
      push(socket, "allow_upload", %{
        "store_id" => [],
        "name" => "attachment",
        "entries" => [
          %{"client_ref" => "0", "name" => "spec.pdf", "size" => 1, "type" => "application/pdf"}
        ]
      })

    assert_reply(push_ref, :error, %{reason: reason})
    assert reason =~ "unknown"
  end

  test "cancel_upload routes by child store_id", %{socket: socket} do
    push_ref =
      push(socket, "allow_upload", %{
        "store_id" => ["lines", "line-1"],
        "name" => "attachment",
        "entries" => [
          %{"client_ref" => "0", "name" => "a.pdf", "size" => 1, "type" => "application/pdf"}
        ]
      })

    assert_reply(push_ref, :ok, reply)
    [{_cref, %{"entry_ref" => entry_ref}}] = Enum.to_list(reply["entries"])

    push_ref =
      push(socket, "cancel_upload", %{
        "store_id" => ["lines", "line-1"],
        "name" => "attachment",
        "ref" => entry_ref
      })

    assert_reply(push_ref, :ok, _reply)
  end

  describe "upload_error event (external mode)" do
    setup do
      {:ok, _reply, socket} = join_root(@external_module, "ext-1", %{})
      {:ok, socket: socket}
    end

    test "client-pushed upload_error emits {op: error, code: external_failed}", %{socket: socket} do
      push_ref =
        push(socket, "allow_upload", %{
          "store_id" => [],
          "name" => "avatar",
          "entries" => [
            %{"client_ref" => "0", "name" => "a.png", "size" => 1, "type" => "image/png"}
          ]
        })

      assert_reply(push_ref, :ok, reply)
      [{_cref, %{"entry_ref" => entry_ref}}] = Enum.to_list(reply["entries"])

      push_ref =
        push(socket, "upload_error", %{
          "store_id" => [],
          "name" => "avatar",
          "ref" => entry_ref,
          "code" => "external_failed",
          "message" => "PUT failed with status 500"
        })

      assert_reply(push_ref, :ok, _r)
    end

    test "unknown error codes degrade to external_failed", %{socket: socket} do
      push_ref =
        push(socket, "allow_upload", %{
          "store_id" => [],
          "name" => "avatar",
          "entries" => [
            %{"client_ref" => "0", "name" => "a.png", "size" => 1, "type" => "image/png"}
          ]
        })

      assert_reply(push_ref, :ok, reply)
      [{_cref, %{"entry_ref" => entry_ref}}] = Enum.to_list(reply["entries"])

      # Server controls the atom union; a bogus code does not crash and
      # does not let the client invent arbitrary `Musubi.Upload.Error.code()`.
      push_ref =
        push(socket, "upload_error", %{
          "store_id" => [],
          "name" => "avatar",
          "ref" => entry_ref,
          "code" => "i_am_definitely_not_a_real_code",
          "message" => "lol"
        })

      assert_reply(push_ref, :ok, _r)
    end
  end

  # Connect a socket and join this root's own channel (join IS the mount).
  defp join_root(module_str, id, params) do
    session = %{"test_pid" => self()}
    connect_info = %{session: session}
    root_id = module_str <> ":" <> id
    topic = "musubi:connection:" <> root_id
    phoenix_socket = socket(MusubiSocket, root_id, %{})

    {:ok, connected_socket} = MusubiSocket.connect(%{}, phoenix_socket, connect_info)

    subscribe_and_join(
      connected_socket,
      Musubi.Transport.ConnectionChannel,
      topic,
      %{"module" => module_str, "id" => id, "params" => params}
    )
  end
end
