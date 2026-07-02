defmodule Musubi.Page.Frame do
  @moduledoc """
  Per-socket outbound bundle threaded through the `:after_render` and
  `:after_serialize` transform stages (`Musubi.Lifecycle`).

  A frame groups one store socket's render output with the push events
  (`Musubi.Event`, BDR-0032) it queued this cycle. `:after_render` sees the
  Elixir-form frame (`render` native, `events` empty — events are drained at the
  server aggregation phase); `:after_serialize` sees the wire-form frame
  (`render` serialized, `events` wire-encoded) and is where render / event
  validation runs. A hook returns `{:cont | :halt, frame, socket}` and may
  rewrite `render` or `events`.
  """

  use TypedStructor

  alias Musubi.Event
  alias Musubi.Page.StoreTable.Entry

  typed_structor do
    field :render, Entry.resolved_state() | Entry.wire_state() | nil,
      default: nil,
      doc:
        "This socket's render output — Elixir form at `:after_render`, wire form at `:after_serialize`."

    field :events, [Event.event()],
      default: [],
      doc:
        "Push events this socket queued (BDR-0032). Empty at `:after_render`; drained/wire-encoded at `:after_serialize`."
  end
end
