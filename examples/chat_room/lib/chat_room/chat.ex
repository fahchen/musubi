defmodule ChatRoom.Chat do
  @moduledoc """
  Agent-backed in-memory chat backend for the example.

  Messages are stored by room and capped to the most recent 100 entries. This
  keeps reconnect/bootstrap behavior realistic without introducing a database.
  """

  use Agent

  alias ChatRoom.AttachmentState
  alias ChatRoom.MessageState

  # Keep the example bounded: each room stores and sends at most the latest
  # 100 messages.
  @max_messages 100

  @typep room_id() :: String.t()
  @typep room_messages() :: %{room_id() => [MessageState.t()]}

  @doc """
  Starts the example chat message store.

  ## Examples

      children = [ChatRoom.Chat]
      Supervisor.start_link(children, strategy: :one_for_one)
  """
  @spec start_link(keyword()) :: Agent.on_start()
  def start_link(_opts), do: Agent.start_link(fn -> %{} end, name: __MODULE__)

  @doc """
  Returns the most recent messages for `room_id`, newest first.

  The returned list is capped to 100 messages even when a larger limit is
  requested.

  ## Examples

      ChatRoom.Chat.recent("general", 100)
      #=> []
  """
  @spec recent(String.t(), pos_integer()) :: [MessageState.t()]
  def recent(room_id, limit) when is_binary(room_id) and is_integer(limit) and limit > 0 do
    count = min(limit, @max_messages)

    Agent.get(__MODULE__, fn messages_by_room ->
      messages_by_room
      |> Map.get(room_id, [])
      |> Enum.take(count)
    end)
  end

  @doc """
  Stores and broadcasts a message for `room_id`.

  `attachment` is `nil` for a typed message and an `AttachmentState` for the
  row the `attach` command appends after consuming an upload entry.

  ## Examples

      ChatRoom.Chat.send_message("general", "Ada", "hello")
      #=> {:ok, %ChatRoom.MessageState{}}
  """
  @spec send_message(String.t(), String.t(), String.t(), AttachmentState.t() | nil) ::
          {:ok, MessageState.t()}
  def send_message(room_id, sender, body, attachment \\ nil)
      when is_binary(room_id) and is_binary(sender) and is_binary(body) do
    msg = %MessageState{
      id: "msg-" <> Integer.to_string(System.unique_integer([:positive])),
      body: body,
      sender: sender,
      attachment: attachment
    }

    Agent.update(__MODULE__, &store_message(&1, room_id, msg))
    Phoenix.PubSub.broadcast(ChatRoom.PubSub, "room:" <> room_id, {:message_received, msg})

    {:ok, msg}
  end

  @spec store_message(room_messages(), room_id(), MessageState.t()) :: room_messages()
  defp store_message(messages_by_room, room_id, %MessageState{} = msg) do
    room_messages =
      messages_by_room
      |> Map.get(room_id, [])
      |> then(&[msg | &1])
      |> Enum.take(@max_messages)

    Map.put(messages_by_room, room_id, room_messages)
  end
end
