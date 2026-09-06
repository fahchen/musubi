defmodule ChatRoom.AttachmentState do
  @moduledoc """
  Reusable Musubi.State module describing one consumed upload entry. Carried
  by `ChatRoom.MessageState` so an attachment travels with the message row
  that announced it, and never as upload state — the upload handle is a
  transfer-plane object that resets the moment the entry is consumed.
  """

  use Musubi.State

  state do
    field(:name, String.t())
    field(:content_type, String.t())
    field(:size, integer())
    field(:url, String.t())
  end
end
