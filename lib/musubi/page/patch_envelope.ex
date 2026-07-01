defmodule Musubi.Page.PatchEnvelope do
  @moduledoc """
  Wire-shape of one Musubi patch update.

  An envelope groups one render cycle's RFC 6902 ops with the matching
  `stream_ops` accumulated by the stream API. Per BDR-0014/0018:

    * `ops` are the post-filtered Musubi JSON Patch ops (`add`/`remove`/`replace` only).
    * `stream_ops` carry stream-typed content (the wire tree carries stable
      `%{"__musubi_stream__" => name}` markers at stream placement paths).
    * The page runtime emits an envelope when `ops`, `stream_ops`, `upload_ops`,
      or `events` is non-empty; an idle render cycle produces no envelope. The
      skip is content-driven: `build/5` returns `nil` when all four are empty.

  ## Cross-track contract

  This struct is the canonical wire-shape M5/M6 transport adapters serialize.
  Adding or removing fields is breaking — coordinate via spec change first.

  ## Examples

      iex> %Musubi.Page.PatchEnvelope{base_version: 0, version: 1, ops: [%{op: "replace", path: "", value: %{}}], stream_ops: []}
      ...> |> Map.fetch!(:type)
      "patch"
  """

  use TypedStructor

  @typedoc "Op shape — see `Musubi.Diff` for `ops` and `Musubi.Stream` for `stream_ops`."
  @type op() :: %{
          required(:op) => String.t(),
          required(:path) => String.t(),
          optional(:value) => term()
        }

  @typedoc "Wire stream-op shape produced by `Musubi.Page.Server` from the `Musubi.Stream` accumulator."
  @type stream_op() :: map()

  @typedoc "Wire upload-op shape produced by `Musubi.Page.Server` from the `Musubi.Upload` accumulator."
  @type upload_op() :: map()

  @typedoc "Wire push-event shape (BDR-0032) produced by `Musubi.Page.Server` from the `Musubi.Event` accumulator."
  @type event() :: %{required(:name) => String.t(), required(:payload) => term()}

  typed_structor do
    field :type, String.t(),
      default: "patch",
      doc: "Envelope discriminator. Always the literal string `\"patch\"` (per spec)."

    field :base_version, non_neg_integer(),
      enforce: true,
      doc: "Version the envelope was computed against — equals the previous emitted `version`."

    field :version, non_neg_integer(),
      enforce: true,
      doc:
        "New version after this envelope is applied — equals `base_version + 1`. Monotonic per page runtime; resets to 0 on reconnect (fresh page server)."

    field :ops, [op()],
      default: [],
      doc:
        "RFC 6902 ops describing the wire-form delta. Only `add`/`remove`/`replace` (BDR-0014). Stream item content never appears here."

    field :stream_ops, [stream_op()],
      default: [],
      doc:
        "Ordered wire ops for stream-typed slots (reset/insert/delete, each tagged with `store_id`). Applied after `ops` in array order on the client (BDR-0018)."

    field :upload_ops, [upload_op()],
      default: [],
      doc:
        "Ordered wire ops for upload-tracked entries (config/add/progress/complete/error/cancel/reset, each tagged with `store_id`). Independent of `stream_ops`."

    field :events, [event()],
      default: [],
      doc:
        "Transient push events (BDR-0032), each `%{name, payload}`. Fire-and-forget: dispatched once on the client after `ops`, owns no version/recovery semantics, not replayed on reconnect. An event-only cycle still emits an envelope and bumps `version`."
  end

  @doc """
  Builds the bootstrap envelope for a fresh page runtime.

  The first envelope after mount carries `base_version: 0`, `version: 1`, a
  single `replace` op at the root path with the full wire-form root value, and
  whatever stream ops the application queued during `mount`.

  ## Examples

      iex> envelope = Musubi.Page.PatchEnvelope.initial(%{"title" => "Inbox"}, [], [])
      iex> envelope.base_version
      0
      iex> envelope.version
      1
      iex> envelope.ops
      [%{op: "replace", path: "", value: %{"title" => "Inbox"}}]
  """
  @spec initial(term(), [stream_op()], [upload_op()], [event()]) :: t()
  def initial(wire_root, stream_ops, upload_ops \\ [], events \\ [])
      when is_list(stream_ops) and is_list(upload_ops) and is_list(events) do
    %__MODULE__{
      type: "patch",
      base_version: 0,
      version: 1,
      ops: [%{op: "replace", path: "", value: wire_root}],
      stream_ops: stream_ops,
      upload_ops: upload_ops,
      events: events
    }
  end

  @doc """
  Builds an envelope for a subsequent render cycle.

  Returns `nil` only when `ops`, `stream_ops`, `upload_ops`, and `events` are all
  empty (BDR-0018 — idle cycles emit nothing). An events-only cycle still emits
  an envelope and bumps `version` (BDR-0032).

  ## Examples

      iex> Musubi.Page.PatchEnvelope.build(0, [%{op: "replace", path: "/a", value: 1}], [])
      %Musubi.Page.PatchEnvelope{type: "patch", base_version: 0, version: 1, ops: [%{op: "replace", path: "/a", value: 1}], stream_ops: [], upload_ops: [], events: []}

      iex> Musubi.Page.PatchEnvelope.build(3, [], [], [], [])
      nil

      iex> Musubi.Page.PatchEnvelope.build(3, [], [], [], [%{name: "toast", payload: %{}}])
      %Musubi.Page.PatchEnvelope{type: "patch", base_version: 3, version: 4, ops: [], stream_ops: [], upload_ops: [], events: [%{name: "toast", payload: %{}}]}
  """
  @spec build(non_neg_integer(), [op()], [stream_op()], [upload_op()], [event()]) :: t() | nil
  def build(base_version, ops, stream_ops, upload_ops \\ [], events \\ [])

  def build(_base_version, [], [], [], []), do: nil

  def build(base_version, ops, stream_ops, upload_ops, events)
      when is_integer(base_version) and base_version >= 0 and is_list(ops) and
             is_list(stream_ops) and is_list(upload_ops) and is_list(events) do
    %__MODULE__{
      type: "patch",
      base_version: base_version,
      version: base_version + 1,
      ops: ops,
      stream_ops: stream_ops,
      upload_ops: upload_ops,
      events: events
    }
  end

  @doc """
  Returns the envelope as a JSON-encodable map with string keys.

  Use this at the transport boundary when the channel serializer expects a
  plain map (e.g. handing the envelope to `Phoenix.Channel.push/3`).

  ## Examples

      iex> envelope = %Musubi.Page.PatchEnvelope{base_version: 0, version: 1, ops: [], stream_ops: [], upload_ops: [], events: []}
      iex> Musubi.Page.PatchEnvelope.to_wire(envelope)
      %{"type" => "patch", "base_version" => 0, "version" => 1, "ops" => [], "stream_ops" => [], "upload_ops" => [], "events" => []}
  """
  @spec to_wire(t()) :: %{String.t() => term()}
  def to_wire(%__MODULE__{} = envelope) do
    %{
      "type" => envelope.type,
      "base_version" => envelope.base_version,
      "version" => envelope.version,
      "ops" => envelope.ops,
      "stream_ops" => envelope.stream_ops,
      "upload_ops" => envelope.upload_ops,
      "events" => envelope.events
    }
  end
end
