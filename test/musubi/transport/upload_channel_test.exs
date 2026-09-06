defmodule Musubi.Transport.UploadChannelTest do
  @moduledoc """
  Integration coverage for `Musubi.Transport.UploadChannel`:
  join verification, binary `"chunk"` frames, completion on the final
  chunk, temp-file lifecycle on success and disconnect, chunk_timeout
  watchdog, and the "wrong mode" forge rejection for `upload_progress`
  on channel-mode entries.
  """

  use ExUnit.Case

  defmodule TestEndpoint do
    @moduledoc false
    use Phoenix.Endpoint, otp_app: :musubi

    socket("/musubi", Musubi.Transport.UploadChannelTest.MusubiSocket,
      websocket: false,
      longpoll: false
    )
  end

  defmodule AvatarStore do
    @moduledoc false
    use Musubi.Store, root: true

    state do
      field :title, String.t() | nil
    end

    upload(:avatar,
      accept: ~w(.png),
      max_entries: 2,
      max_file_size: 1_000_000,
      chunk_size: 1_024,
      chunk_timeout: 200
    )

    @impl Musubi.Store
    def render(socket), do: %{title: socket.assigns[:title]}

    @impl Musubi.Store
    def handle_command(_n, _p, s), do: {:noreply, s}
  end

  defmodule MusubiSocket do
    @moduledoc false
    use Musubi.Socket,
      roots: [Musubi.Transport.UploadChannelTest.AvatarStore]
  end

  import Phoenix.ChannelTest

  @endpoint TestEndpoint

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
    page = Musubi.Testing.mount(AvatarStore)
    assert_receive {:patch, _initial_env}, 500
    {:ok, page: page}
  end

  describe "join/3" do
    test "rejects unknown topic format", _ctx do
      assert {:error, %{reason: "unauthorized"}} =
               subscribe_and_join(
                 socket(MusubiSocket, "x", %{}),
                 Musubi.Transport.UploadChannel,
                 "musubi_upload:nonsense",
                 %{"token" => "anything"}
               )
    end

    test "rejects forged token", _ctx do
      assert {:error, %{reason: "unauthorized"}} =
               subscribe_and_join(
                 socket(MusubiSocket, "x", %{}),
                 Musubi.Transport.UploadChannel,
                 "musubi_upload:e_001",
                 %{"token" => "forged.token.value"}
               )
    end

    test "rejects when owning store pid is dead", %{page: page} do
      entry = preflight_one_entry(page, 64)
      stop_page(page)

      assert {:error, %{reason: "unauthorized"}} =
               subscribe_and_join(
                 socket(MusubiSocket, "x", %{}),
                 Musubi.Transport.UploadChannel,
                 "musubi_upload:" <> entry["entry_ref"],
                 %{"token" => entry["token"]}
               )
    end

    test "valid token opens a temp file and registers the channel pid", %{page: page} do
      entry = preflight_one_entry(page, 64)

      {:ok, _reply, _channel_socket} =
        subscribe_and_join(
          socket(MusubiSocket, "x", %{}),
          Musubi.Transport.UploadChannel,
          "musubi_upload:" <> entry["entry_ref"],
          %{"token" => entry["token"]}
        )

      # Give the register cast time to land.
      Process.sleep(20)

      {:ok, %{socket: page_socket}} = Musubi.Page.Server.peek(page.pid, [])

      {:ok, %Musubi.Upload.Entry{path: path, upload_channel_pid: pid}} =
        Musubi.Upload.fetch_entry(page_socket, :avatar, entry["entry_ref"])

      assert is_binary(path)
      assert File.exists?(path)
      assert is_pid(pid)
    end
  end

  describe "chunk frames" do
    test "writes bytes and replies with progress 0..100 against client_size", %{page: page} do
      entry = preflight_one_entry(page, 256)

      {:ok, _r, channel_socket} =
        subscribe_and_join(
          socket(MusubiSocket, "x", %{}),
          Musubi.Transport.UploadChannel,
          "musubi_upload:" <> entry["entry_ref"],
          %{"token" => entry["token"]}
        )

      ref = push(channel_socket, "chunk", :binary.copy(<<0>>, 128))
      assert_reply(ref, :ok, %{progress: 50})

      ref = push(channel_socket, "chunk", :binary.copy(<<0>>, 128))
      assert_reply(ref, :ok, %{progress: 100})
    end

    test "accepts the serializer's {:binary, data} payload shape", %{page: page} do
      entry = preflight_one_entry(page, 128)

      {:ok, _r, channel_socket} =
        subscribe_and_join(
          socket(MusubiSocket, "x", %{}),
          Musubi.Transport.UploadChannel,
          "musubi_upload:" <> entry["entry_ref"],
          %{"token" => entry["token"]}
        )

      # What a websocket transport actually delivers: `push/3` skips the
      # serializer, but `Phoenix.Socket.V2.JSONSerializer.decode_binary/1`
      # tags every binary frame this way before the channel sees it.
      ref = push(channel_socket, "chunk", {:binary, :binary.copy(<<3>>, 64)})
      assert_reply(ref, :ok, %{progress: 50})

      ref = push(channel_socket, "chunk", {:binary, :binary.copy(<<3>>, 64)})
      assert_reply(ref, :ok, %{progress: 100})
    end

    test "final chunk completes the upload without any close event", %{page: page} do
      entry = preflight_one_entry(page, 64)
      assert_receive {:patch, _add_env}, 500

      {:ok, _r, channel_socket} =
        subscribe_and_join(
          socket(MusubiSocket, "x", %{}),
          Musubi.Transport.UploadChannel,
          "musubi_upload:" <> entry["entry_ref"],
          %{"token" => entry["token"]}
        )

      ref = push(channel_socket, "chunk", :binary.copy(<<7>>, 64))
      assert_reply(ref, :ok, %{progress: 100})

      assert_receive {:patch, envelope}, 500

      assert Enum.any?(
               envelope.upload_ops,
               &(&1.op == "complete" and &1.ref == entry["entry_ref"])
             )

      # Channel terminates cleanly with the file still on disk; the entry
      # owns it and consume will move/delete it.
      Process.sleep(30)
      {:ok, %{socket: page_socket}} = Musubi.Page.Server.peek(page.pid, [])

      {:ok, %Musubi.Upload.Entry{path: path, status: status}} =
        Musubi.Upload.fetch_entry(page_socket, :avatar, entry["entry_ref"])

      assert status == :success
      assert is_binary(path)
      assert File.exists?(path)
    end

    test "client disconnect before completion deletes the file and emits cancel", %{page: page} do
      entry = preflight_one_entry(page, 256)
      assert_receive {:patch, _add_env}, 500

      {:ok, _r, channel_socket} =
        subscribe_and_join(
          socket(MusubiSocket, "x", %{}),
          Musubi.Transport.UploadChannel,
          "musubi_upload:" <> entry["entry_ref"],
          %{"token" => entry["token"]}
        )

      Process.sleep(20)
      {:ok, %{socket: page_socket}} = Musubi.Page.Server.peek(page.pid, [])

      {:ok, %Musubi.Upload.Entry{path: path}} =
        Musubi.Upload.fetch_entry(page_socket, :avatar, entry["entry_ref"])

      assert File.exists?(path)

      _leave = leave(channel_socket)

      # Wait for the page server to process the cancel.
      assert_receive {:patch, envelope}, 500

      assert Enum.any?(envelope.upload_ops, fn op ->
               op.op == "cancel" and op.ref == entry["entry_ref"]
             end)

      refute File.exists?(path)
    end
  end

  describe "chunk_timeout watchdog" do
    test "emits a scrubbed chunk_timeout error and terminates", %{page: page} do
      entry = preflight_one_entry(page, 1_024)
      assert_receive {:patch, _add}, 500

      {:ok, _r, _channel_socket} =
        subscribe_and_join(
          socket(MusubiSocket, "x", %{}),
          Musubi.Transport.UploadChannel,
          "musubi_upload:" <> entry["entry_ref"],
          %{"token" => entry["token"]}
        )

      # No chunks sent. chunk_timeout is 200 ms on the AvatarStore upload.
      assert_receive {:patch, envelope}, 1_500

      error_op =
        Enum.find(envelope.upload_ops, fn op ->
          op.op == "error" and Map.get(op, :ref) == entry["entry_ref"]
        end)

      assert error_op

      assert error_op.error == %{
               "code" => "chunk_timeout",
               "message" => "upload timed out between chunks"
             }
    end
  end

  describe "wrong-mode upload_progress" do
    test "main-channel upload_progress for a channel-mode entry is rejected", %{page: page} do
      entry = preflight_one_entry(page, 64)
      assert_receive {:patch, _add}, 500

      assert {:error, :wrong_mode} =
               Musubi.Page.Server.upload_progress(page.pid, [], :avatar, entry["entry_ref"], 100)
    end
  end

  defp preflight_one_entry(page, size) do
    {:ok, reply} =
      Musubi.Testing.allow_upload(
        page,
        :avatar,
        [%{"client_ref" => "0", "name" => "a.png", "size" => size, "type" => "image/png"}],
        endpoint: TestEndpoint
      )

    [{_cref, entry}] = Enum.to_list(reply["entries"])
    entry
  end

  defp stop_page(%{pid: pid}) do
    if Process.alive?(pid), do: GenServer.stop(pid, :normal, 1_000)
    :ok
  end
end
