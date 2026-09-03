defmodule ChatRoom.Attachments do
  @moduledoc """
  Agent-backed in-memory blob store for consumed upload entries.

  `Musubi.Upload.consume_uploaded_entries/3` hands the store a temp file that
  the runtime deletes as soon as the command returns, so the application has
  to move the bytes somewhere it owns before then. This example moves them
  into an Agent — the same "application owns the destination" shape a real app
  would satisfy with S3 or a `priv/uploads` directory.

  Entries are capped at the newest 20 blobs; the oldest are dropped, and a
  message row that points at a dropped id renders a dead link. That is the
  honest trade for an example that keeps every byte in memory.
  """

  use Agent

  alias ChatRoom.AttachmentState

  # Keep the example bounded: at most 20 blobs live in the Agent at once.
  @max_attachments 20

  # Sent for a blob whose declared MIME type is missing or outside the
  # allowlist. Browsers need a concrete type on the response, and
  # `Plug.Conn.put_resp_content_type/2` rejects an empty one.
  @fallback_content_type "application/octet-stream"

  # `client_type` is whatever the client said it was — the server validates
  # `accept` against the *extension* and never against the MIME type
  # (BDR-0026). Echoing an unvetted one back as the `Content-Type` of a
  # same-origin response is how a stored file becomes stored XSS, so only these
  # render as themselves; anything else downloads as a blob.
  @served_content_types ~w(image/png image/jpeg image/gif text/plain text/markdown)

  # The Agent holds `[{id, {attachment, contents}}]`, newest first, so the cap
  # is one `Enum.take/2` on insert.

  @doc """
  Starts the example attachment store.

  ## Examples

      children = [ChatRoom.Attachments]
      Supervisor.start_link(children, strategy: :one_for_one)
  """
  @spec start_link(keyword()) :: Agent.on_start()
  def start_link(_opts), do: Agent.start_link(fn -> [] end, name: __MODULE__)

  @doc """
  Stores `contents` under a fresh id and returns the state describing it.

  ## Examples

      ChatRoom.Attachments.put("notes.md", "text/markdown", "# hi")
      #=> %ChatRoom.AttachmentState{name: "notes.md", url: "/attachments/att-1"}
  """
  @spec put(String.t(), String.t(), binary()) :: AttachmentState.t()
  def put(name, content_type, contents)
      when is_binary(name) and is_binary(content_type) and is_binary(contents) do
    id = "att-" <> Integer.to_string(System.unique_integer([:positive]))

    attachment = %AttachmentState{
      name: name,
      content_type: normalize_content_type(content_type),
      size: byte_size(contents),
      url: "/attachments/" <> id
    }

    Agent.update(__MODULE__, fn blobs ->
      Enum.take([{id, {attachment, contents}} | blobs], @max_attachments)
    end)

    attachment
  end

  @spec normalize_content_type(String.t()) :: String.t()
  defp normalize_content_type(content_type) when content_type in @served_content_types,
    do: content_type

  defp normalize_content_type(_content_type), do: @fallback_content_type

  @doc """
  Fetches one stored blob: the state and the bytes served under its URL.

  ## Examples

      ChatRoom.Attachments.fetch("att-1")
      #=> {:ok, {%ChatRoom.AttachmentState{}, "# hi"}}
  """
  @spec fetch(String.t()) :: {:ok, {AttachmentState.t(), binary()}} | :error
  def fetch(id) when is_binary(id) do
    Agent.get(__MODULE__, fn blobs ->
      case List.keyfind(blobs, id, 0) do
        {^id, blob} -> {:ok, blob}
        nil -> :error
      end
    end)
  end
end
