defmodule ChatRoom.Application do
  @moduledoc false

  use Application

  @impl Application
  def start(_type, _args) do
    children = [
      {Phoenix.PubSub, name: ChatRoom.PubSub},
      ChatRoom.Chat,
      ChatRoom.Attachments,
      ChatRoom.Presence,
      ChatRoomWeb.Endpoint
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: ChatRoom.Supervisor)
  end
end
