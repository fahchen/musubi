defmodule ChatRoom.Stores.ChatRoomStore do
  @moduledoc """
  Single-store chat-room page. Demonstrates Musubi's full async + stream +
  PubSub surface in one module:

    * `stream_async :messages, MessageState.t()` slot — async-seeded stream.
      `socket.assigns.messages` carries a `loading | ok | failed`
      `AsyncResult`; items live in the stream slot. After the initial
      seed completes, `handle_info({:message_received, ...})` continues
      to drive incremental inserts via `stream_insert/4`.
    * `stream_async/3` on root `mount/2` to seed the latest stored
      messages off the mount path (mount returns immediately; messages
      flip to `:ok` once the background task settles).
    * `assign_async/3` for the `:online_users` AsyncResult field
    * `set_name` command backed by the application-owned presence registry
    * `start_async/3` + `handle_async/3` for the optimistic `:send_message`
      flow with delivery receipts and the BDR-0020 caught-exception path
    * `cancel_async/2` from `terminate/2` to abandon in-flight tasks
    * `Phoenix.PubSub.subscribe/2` inside root `mount/2` (BDR-0005:
      application-owned PubSub) and `handle_info/2` dispatch
    * `upload :attachment` in channel mode plus the `attach` command that
      consumes it (`docs/uploads.md`): the entry's temp file is moved into an
      application-owned agent and announced as an ordinary chat message, so
      the upload leaves the transfer plane entirely once it lands
  """

  use Musubi.Store, root: true

  alias ChatRoom.AttachmentState
  alias ChatRoom.Attachments
  alias ChatRoom.Chat
  alias ChatRoom.MessageState
  alias ChatRoom.OnlineUser
  alias ChatRoom.Presence

  attr(:room_id, String.t(), required: true)

  # The chat example keeps and streams only the newest 100 messages.
  @message_limit 100

  # Negative stream limits trim from the tail after inserting at the head,
  # keeping the client-side materialized stream to the newest messages.
  @stream_limit -100

  # Artificial latency so the `:messages` AsyncResult loading -> ok
  # transition is visible in the UI on first mount/reconnect.
  @history_load_delay_ms 1_500

  state do
    stream_async(:messages, MessageState.t(), item_key: &"msg-#{&1.id}", limit: @stream_limit)
    field(:current_user, OnlineUser.t())
    field(:online_users, Musubi.AsyncResult.of(list(OnlineUser.t())))

    field(
      :last_send_status,
      %{type: :idle} | %{type: :ok, id: String.t()} | %{type: :failed, reason: String.t()}
    )
  end

  command :set_name do
    payload do
      field :name, String.t()
    end

    reply do
      field :ok, boolean()
      field :name, String.t()
    end
  end

  command :send_message do
    payload do
      field :body, String.t()
    end

    reply do
      field :queued, boolean()
    end
  end

  # Declared outside `state do` (docs/uploads.md): the framework injects the
  # `{"__musubi_upload__": "attachment"}` marker into the render output and
  # drives the handle over `upload_ops`, so no field here composes upload
  # state by hand. No `upload_external/3` callback is defined, which is what
  # keeps the entry in channel mode — chunks ride `musubi_upload:<ref>`.
  upload(:attachment,
    accept: ~w(.png .jpg .jpeg .gif .txt .md),
    max_entries: 1,
    max_file_size: 2_000_000
  )

  command :attach do
    reply do
      field :attached, boolean()
      field :name, String.t() | nil
    end
  end

  @impl Musubi.Store
  def mount(params, socket) do
    room_id = Map.fetch!(params, "room_id")
    user_id = new_user_id()
    current_user = Presence.join(room_id, user_id, default_name(user_id))
    Phoenix.PubSub.subscribe(ChatRoom.PubSub, "room:" <> room_id)

    socket =
      socket
      |> assign(:room_id, room_id)
      |> assign(:user_id, user_id)
      |> assign(:current_user, current_user)
      |> assign(:last_send_status, %{type: :idle})
      |> stream_async(:messages, fn ->
        # Simulated history-load latency so the AsyncResult loading->ok
        # transition is visible client-side.
        Process.sleep(@history_load_delay_ms)
        {:ok, Chat.recent(room_id, @message_limit), reset: true}
      end)
      |> assign_async(:online_users, fn -> {:ok, Presence.list(room_id)} end)

    {:ok, socket}
  end

  # ---------------------------------------------------------------------------
  # Render output
  # ---------------------------------------------------------------------------

  @impl Musubi.Store
  def render(socket) do
    %{
      messages: async_stream(:messages),
      current_user: socket.assigns.current_user,
      online_users: socket.assigns.online_users,
      last_send_status: socket.assigns.last_send_status
    }
  end

  @spec new_user_id() :: String.t()
  defp new_user_id do
    "user-" <> Integer.to_string(System.unique_integer([:positive]))
  end

  @spec default_name(String.t()) :: String.t()
  defp default_name(user_id), do: "Guest " <> String.replace_prefix(user_id, "user-", "")

  # ---------------------------------------------------------------------------
  # Commands
  # ---------------------------------------------------------------------------

  @impl Musubi.Store
  def handle_command(:set_name, %{"name" => name}, socket) do
    room_id = socket.assigns.room_id
    user_id = socket.assigns.user_id
    current_user = Presence.update_name(room_id, user_id, normalize_name(name, user_id))

    socket =
      socket
      |> assign(:current_user, current_user)
      |> put_online_users(Presence.list(room_id))

    {:reply, %{"ok" => true, "name" => current_user.name}, socket}
  end

  # `start_async/3` kicks off a fire-and-forget send. The result routes to
  # `handle_async/3` below — `socket.assigns` is NOT mutated by start_async
  # itself, only by what handle_async writes.
  @impl Musubi.Store
  def handle_command(:send_message, %{"body" => body}, socket) do
    room_id = socket.assigns.room_id
    sender = socket.assigns.current_user.name

    {:reply, %{"queued" => true},
     start_async(socket, :send_message, fn ->
       Chat.send_message(room_id, sender, body)
     end)}
  end

  # The `docs/uploads.md` completion path. `consume_uploaded_entries/3` may
  # only run inside a command handler; in channel mode it hands over
  # `%{path: ...}` for a temp file the runtime deletes as soon as this returns,
  # so the bytes move into the application-owned agent *here*. Consuming the
  # last completed entry empties the index, which emits `{op: reset}` and
  # returns every client's handle to idle.
  @impl Musubi.Store
  def handle_command(:attach, _payload, socket) do
    room_id = socket.assigns.room_id
    sender = socket.assigns.current_user.name

    {socket, attachments} =
      consume_uploaded_entries(socket, :attachment, fn %{path: path}, entry ->
        {:ok, Attachments.put(entry.client_name, entry.client_type, File.read!(path))}
      end)

    # The row reaches every client — this one included — over the same PubSub
    # broadcast a typed message takes, so `handle_info/2` does the
    # `stream_insert/4` and the reply carries no state (BDR-0009).
    Enum.each(attachments, fn %AttachmentState{name: name} = attachment ->
      Chat.send_message(room_id, sender, "shared " <> name, attachment)
    end)

    reply =
      case attachments do
        [] -> %{"attached" => false, "name" => nil}
        [%AttachmentState{name: name} | _rest] -> %{"attached" => true, "name" => name}
      end

    {:reply, reply, socket}
  end

  @spec normalize_name(String.t(), String.t()) :: String.t()
  defp normalize_name(name, user_id) do
    case String.trim(name) do
      "" -> default_name(user_id)
      trimmed -> String.slice(trimmed, 0, 40)
    end
  end

  @spec put_online_users(Musubi.Socket.t(), [OnlineUser.t()]) :: Musubi.Socket.t()
  defp put_online_users(socket, users) when is_list(users) do
    assign(socket, :online_users, Musubi.AsyncResult.ok(socket.assigns.online_users, users))
  end

  # ---------------------------------------------------------------------------
  # Async result routing
  # ---------------------------------------------------------------------------

  # `:ok` and `:error` are application-level outcomes the task fun returned.
  # `:exit` is the task-exit path (task crashed / killed) — the runtime
  # delivers it to `handle_async/3` and emits `[:musubi, :async, :stop]` with
  # `status: :failed`. BDR-0020 (`[:musubi, :async, :exception]`) covers a
  # different case: the `handle_async/3` clause itself raising — the runtime
  # catches that and the page survives. This example does not raise from
  # `handle_async/3`.
  @impl Musubi.Store
  def handle_async(:send_message, {:ok, {:ok, %MessageState{id: id}}}, socket) do
    {:noreply, assign(socket, :last_send_status, %{type: :ok, id: id})}
  end

  @impl Musubi.Store
  def handle_async(:send_message, {:ok, {:error, reason}}, socket) do
    status = %{type: :failed, reason: Atom.to_string(reason)}
    {:noreply, assign(socket, :last_send_status, status)}
  end

  @impl Musubi.Store
  def handle_async(:send_message, {:exit, reason}, socket) do
    status = %{type: :failed, reason: inspect(reason)}
    {:noreply, assign(socket, :last_send_status, status)}
  end

  # ---------------------------------------------------------------------------
  # PubSub messages — application-owned (BDR-0005)
  # ---------------------------------------------------------------------------

  @impl Musubi.Store
  def handle_info({:message_received, %MessageState{} = msg}, socket) do
    {:noreply, stream_insert(socket, :messages, msg, at: 0, limit: @stream_limit)}
  end

  @impl Musubi.Store
  def handle_info({:presence_changed, users}, socket) when is_list(users) do
    {:noreply, put_online_users(socket, users)}
  end

  # Cancel any in-flight send when the page goes down so the task does not
  # outlive the runtime. `cancel_async/2` is a no-op when the name is not
  # tracked.
  @impl Musubi.Store
  def terminate(_reason, socket) do
    Presence.leave(socket.assigns.room_id, socket.assigns.user_id)

    socket
    |> cancel_async(:send_message)
    |> cancel_async(:messages)
    |> cancel_async(:online_users)

    :ok
  end
end
