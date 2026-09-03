defmodule ChatRoom.MessageState do
  @moduledoc """
  Reusable Musubi.State module describing one chat message. Used as the
  per-item type of the `:messages` stream slot.

  `attachment` is `nil` for a typed message and set for the row the `attach`
  command appends after it consumes an upload entry.
  """

  use Musubi.State

  alias ChatRoom.AttachmentState

  state do
    field(:id, String.t())
    field(:body, String.t())
    field(:sender, String.t())
    field(:attachment, AttachmentState.t() | nil)
  end
end
