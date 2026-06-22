defmodule Musubi.Page.StoreTable.Entry do
  @moduledoc "Logical store node entry tracked inside the page server."

  use TypedStructor

  alias Musubi.Child
  alias Musubi.Socket
  alias Musubi.Stream.AsyncPlaceholder
  alias Musubi.Stream.Marker
  alias Musubi.Stream.Placeholder

  @type resolved_state() ::
          nil
          | boolean()
          | number()
          | String.t()
          | atom()
          | [resolved_state()]
          | %{optional(term()) => resolved_state()}

  @type raw_state_value() ::
          nil
          | boolean()
          | number()
          | String.t()
          | atom()
          | Child.t()
          | Placeholder.t()
          | AsyncPlaceholder.t()
          | Marker.marker()
          | [raw_state_value()]
          | %{optional(term()) => raw_state_value()}

  @type raw_state() :: :not_rendered | raw_state_value()

  @type wire_state() ::
          nil
          | boolean()
          | number()
          | String.t()
          | [wire_state()]
          | %{optional(String.t()) => wire_state()}

  typed_structor do
    field :socket, Socket.t(),
      enforce: true,
      doc:
        "Socket for this store node — carries assigns, hook table, identity. Preserved across identity-stable re-renders."

    field :module, module(),
      enforce: true,
      doc: "Store module backing this node. Changing the module forces a fresh mount (BDR-0011)."

    field :resolved_state, resolved_state(),
      default: nil,
      doc:
        "Last resolved render output (Elixir form) for this node. Reused when memoization skips `update/2` and `render/1` (BDR-0013)."

    field :raw_state, raw_state(),
      default: :not_rendered,
      doc:
        "Last pre-resolution `render/1` return value (Elixir form before child resolution, stream normalization, store-id injection, or serialization). Reused by the root render short-circuit to re-walk descendants without re-invoking the root render callback."

    field :wire_state, wire_state() | nil,
      default: nil,
      doc:
        "Last serialized render output (wire form, after `Musubi.Wire.to_wire/1`). Stored alongside `resolved_state` so the M4 diff engine can compare wire-form trees without re-serializing."

    field :consumed_assigns, %{optional(Socket.assign_key()) => term()},
      default: %{},
      doc:
        "Snapshot of the normalized props the parent passed to this child on the previous render (via `child(Module, id: ..., key: value, ...)`). The reconciler compares the next render's props against this snapshot to decide whether to re-run `update/2` — independent of the child's live `socket.assigns`, which the child mutates itself."
  end
end
