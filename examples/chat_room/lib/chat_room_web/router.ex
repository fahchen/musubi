defmodule ChatRoomWeb.Router do
  @moduledoc false

  use Plug.Router

  alias ChatRoom.AttachmentState
  alias ChatRoom.Attachments

  plug(:match)
  plug(:dispatch)

  get "/" do
    send_index(conn)
  end

  # Serves what the `attach` command consumed out of the upload entry. The
  # message row references this URL, so an uploaded image renders in the
  # browser client and the desktop client can hand the URL to a browser.
  get "/attachments/:id" do
    case Attachments.fetch(id) do
      {:ok, {%AttachmentState{} = attachment, contents}} ->
        conn
        |> Plug.Conn.put_resp_content_type(attachment.content_type)
        |> Plug.Conn.send_resp(200, contents)

      :error ->
        Plug.Conn.send_resp(conn, 404, "attachment not found")
    end
  end

  match _ do
    send_index(conn)
  end

  defp send_index(conn) do
    index_path = Path.join(:code.priv_dir(:chat_room), "static/index.html")

    conn
    |> Plug.Conn.put_resp_content_type("text/html")
    |> Plug.Conn.send_file(200, index_path)
  end
end
