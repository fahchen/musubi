defmodule Musubi.Stream do
  @moduledoc """
  Stream API for Musubi stores.

  Stream-typed slots (declared via `stream/2,3` inside `state do`) carry
  collections whose materialization is **owned by the client**. The server
  queues raw delta ops (`reset`/`insert`/`delete`) on a per-stream
  `Musubi.Stream.Slot` struct stored under `socket.assigns.__streams__`. Each
  cycle's queued ops drain into the patch envelope's `stream_ops`, where the
  page runtime stamps each op with its owning `store_id`, and the struct is
  pruned.

  The runtime does not keep an ordered `item_keys` list, does not decide
  upsert-vs-insert, and does not trim for `:limit` server-side. The client
  materializes the stream and applies the per-op `:limit` field.

  ## Public API surface (frozen for M5+)

    * `stream/3,4`
    * `stream_configure/3`
    * `stream_insert/3,4`
    * `stream_delete/3`
    * `stream_delete_by_item_key/3`

  ## Reserved socket-assigns shape

  `socket.assigns.__streams__` is a map with reserved sub-keys:

    * `__ref__` — monotonic per-page ref counter used to stamp each new
      `Musubi.Stream.Slot`.
    * `__changed__` — `MapSet` of stream names mutated since the last prune.
      Used by `Musubi.Resolver` to know which structs to prune.
    * `__configured__` — pre-init configure opts keyed by stream name.
      Applied when the matching `Musubi.Stream.Slot` is first initialized.
    * `<atom_name>` — the per-stream `Musubi.Stream.Slot` struct.

  Sub-keys are runtime-internal; do not read or write directly.

  ## Examples

      Musubi.Stream.stream_insert(socket, :messages, %{id: "1", body: "hi"})
      #=> %Musubi.Socket{...}
  """

  alias Musubi.Socket
  alias Musubi.Stream.Slot
  alias Musubi.Telemetry
  alias Musubi.Wire

  @typedoc "Public stream identifier — atom matching a `stream :name, T, ...` declaration."
  @type stream_name() :: atom()

  @typedoc "Computed item_key returned by the configured `:item_key` capture. Always a binary."
  @type item_key() :: String.t()

  @typedoc "Per-stream config — runtime-merged from compile-time reflection + `stream_configure/3`."
  @type config() :: %{
          required(:item_key) => (term() -> item_key()),
          required(:limit) => integer() | nil
        }

  @typedoc "Wire stream op pushed in the envelope's `stream_ops` array."
  @type op() :: %{
          required(:op) => String.t(),
          required(:stream) => String.t(),
          required(:ref) => String.t(),
          required(:store_id) => [String.t()],
          optional(any()) => any()
        }

  @typep raw_op() :: %{
           required(:op) => String.t(),
           required(:stream) => String.t(),
           required(:ref) => String.t(),
           optional(any()) => any()
         }

  @assigns_key :__streams__
  @ref_key :__ref__
  @changed_key :__changed__
  @configured_key :__configured__
  @drained_key :__musubi_drained_stream_ops__

  @doc """
  Returns the reserved socket-assigns key holding the per-page stream index.

  Exposed so tests and hooks can introspect without hard-coding the literal.
  """
  @spec assigns_key() :: :__streams__
  def assigns_key, do: @assigns_key

  @doc """
  Returns the reserved socket-private key holding the drained stream ops
  (populated by `Musubi.Resolver` and consumed by the page server).
  """
  @spec drained_key() :: :__musubi_drained_stream_ops__
  def drained_key, do: @drained_key

  @doc """
  Sets configure-only options for `name`. Raises if `name` has already been
  initialized via `stream/3,4` or `stream_insert/3,4`.

  Accepts `:item_key` (arity-1 function returning a binary) and `:limit`
  (integer or `nil`). The configuration takes effect when the stream is
  next initialized; re-configuring the same stream after init is a
  lifetime error.

  Configure is purely server-side state — it does not produce a wire op
  (the `item_key` capture is not transferable, and per-insert ops carry
  the `:limit` the client needs). Documented Musubi divergence vs. the
  abstract spec in the realignment notes.

  ## Examples

      socket = Musubi.Stream.stream_configure(socket, :messages, item_key: &("custom-" <> &1.id))
  """
  @spec stream_configure(Socket.t(), stream_name(), keyword()) :: Socket.t()
  def stream_configure(%Socket{} = socket, name, opts) when is_atom(name) and is_list(opts) do
    if initialized?(socket, name) do
      raise ArgumentError,
            "stream_configure(:#{name}, ...) is only valid before the stream is initialized; " <>
              "stream_configure must precede `stream/3,4` or `stream_insert/3,4` for the same name."
    end

    overrides = build_config_overrides(opts, name)

    update_streams_index(socket, fn index ->
      configured = Map.get(index, @configured_key, %{})

      next_configured =
        Map.put(configured, name, Map.merge(Map.get(configured, name, %{}), overrides))

      Map.put(index, @configured_key, next_configured)
    end)
  end

  defp initialized?(%Socket{} = socket, name) do
    case Map.get(streams_index(socket), name) do
      %Slot{} -> true
      _other -> false
    end
  end

  @doc """
  Bulk seeds or refreshes a stream slot.

  With `reset: true`, marks the stream's slot so the flushed wire
  ops include a `reset` ahead of the inserts and the client clears its
  local stream before applying them.

  ## Examples

      socket = Musubi.Stream.stream(socket, :messages, [%{id: "1", body: "hi"}])
      socket = Musubi.Stream.stream(socket, :messages, fresh_items, reset: true)
  """
  @spec stream(Socket.t(), stream_name(), [term()], keyword()) :: Socket.t()
  def stream(socket, name, items, opts \\ [])

  def stream(%Socket{} = socket, name, items, opts)
      when is_atom(name) and is_list(items) and is_list(opts) do
    {reset?, opts} = Keyword.pop(opts, :reset, false)

    socket = ensure_stream_initialized(socket, name)
    socket = if reset?, do: mark_reset(socket, name), else: socket

    Enum.reduce(items, socket, fn item, acc ->
      stream_insert(acc, name, item, opts)
    end)
  end

  @doc """
  Queues an insert for one item in a stream slot.

  The default position is `:at` `-1` (append). The runtime does **not**
  decide whether the insert is an upsert or new — that is the client's
  responsibility. The `:limit` field is passed through verbatim
  on the wire op; the client trims if the limit is exceeded after applying
  the insert.

  ## Examples

      socket = Musubi.Stream.stream_insert(socket, :messages, %{id: "1", body: "hi"})
      socket = Musubi.Stream.stream_insert(socket, :messages, item, at: 0, limit: -100)
  """
  @spec stream_insert(Socket.t(), stream_name(), term(), keyword()) :: Socket.t()
  def stream_insert(socket, name, item, opts \\ [])

  def stream_insert(%Socket{} = socket, name, item, opts)
      when is_atom(name) and is_list(opts) do
    socket = ensure_stream_initialized(socket, name)
    slot = fetch_slot!(socket, name)

    item_key = compute_item_key(opts, slot.item_key_fun, item, socket.module, name)
    position = validate_position!(Keyword.get(opts, :at, -1))
    limit = validate_limit!(Keyword.get(opts, :limit, nil))

    update_slot(socket, name, fn ls ->
      %{ls | inserts: [{item_key, position, item, limit} | ls.inserts]}
    end)
  end

  @doc """
  Queues a delete for one item in a stream slot, deriving its `item_key`
  from the item via the stream's configured key function.

  ## Examples

      socket = Musubi.Stream.stream_delete(socket, :messages, %{id: "1"})
  """
  @spec stream_delete(Socket.t(), stream_name(), term()) :: Socket.t()
  def stream_delete(%Socket{} = socket, name, item) when is_atom(name) do
    socket = ensure_stream_initialized(socket, name)
    slot = fetch_slot!(socket, name)
    item_key = compute_item_key([], slot.item_key_fun, item, socket.module, name)
    stream_delete_by_item_key(socket, name, item_key)
  end

  @doc """
  Queues a delete for one item in a stream slot directly by `item_key`.

  ## Examples

      socket = Musubi.Stream.stream_delete_by_item_key(socket, :messages, "messages-1")
  """
  @spec stream_delete_by_item_key(Socket.t(), stream_name(), item_key()) :: Socket.t()
  def stream_delete_by_item_key(%Socket{} = socket, name, item_key)
      when is_atom(name) and is_binary(item_key) do
    socket = ensure_stream_initialized(socket, name)

    update_slot(socket, name, fn ls ->
      %{ls | deletes: [item_key | ls.deletes]}
    end)
  end

  @doc """
  Drains the per-socket accumulator populated by `Musubi.Resolver`
  and returns the wire ops for this cycle.

  Called by the page runtime once per render cycle, immediately after the
  resolver finishes (the prune hook has already run by then). After the
  call, the accumulator is empty.

  ## Examples

      {ops, socket} = Musubi.Stream.flush_pending_ops(socket)
      ops
      #=> [%{op: "insert", stream: "messages", ref: "0", item_key: "messages-1", item: %{...}, at: -1, limit: nil}]
  """
  @spec flush_pending_ops(Socket.t()) :: {[raw_op()], Socket.t()}
  def flush_pending_ops(%Socket{} = socket) do
    socket = drain_and_prune(socket)
    drained = Socket.get_private(socket, @drained_key, [])

    Telemetry.emit(
      [:musubi, :stream, :flush],
      %{count: length(drained)},
      %{module: socket.module}
    )

    {drained, Socket.put_private(socket, @drained_key, [])}
  end

  @doc """
  Returns the queued stream ops for the current cycle (not yet flushed).

  Reads pending fields from each `Musubi.Stream.Slot` plus any already-drained
  ops on the socket-private accumulator. Useful for tests and debugging.

  ## Examples

      socket = Musubi.Stream.stream_insert(%Musubi.Socket{module: M}, :messages, %{id: "1"})
      length(Musubi.Stream.pending_ops(socket))
      #=> 1
  """
  @spec pending_ops(Socket.t()) :: [raw_op()]
  def pending_ops(%Socket{} = socket) do
    drained = Socket.get_private(socket, @drained_key, [])

    live =
      socket
      |> streams_index()
      |> stream_entries()
      |> Enum.flat_map(fn {_name, %Slot{} = ls} -> build_ops(ls) end)

    drained ++ live
  end

  @doc """
  Drains pending ops from every `Musubi.Stream.Slot` marked changed, appends
  them to the socket-private accumulator, prunes each struct, and clears
  the `__changed__` set. Invoked by `Musubi.Resolver`.

  Streams not in `__changed__` are left untouched.
  """
  @spec drain_and_prune(Socket.t()) :: Socket.t()
  def drain_and_prune(%Socket{} = socket) do
    case Map.get(socket.assigns, @assigns_key) do
      nil ->
        socket

      index ->
        do_drain_and_prune(socket, index)
    end
  end

  defp do_drain_and_prune(%Socket{} = socket, index) do
    changed = Map.get(index, @changed_key, MapSet.new())

    {drained_ops, next_index} =
      changed
      |> Enum.sort_by(fn name -> Map.fetch!(index, name).ref end)
      |> Enum.reduce({[], index}, fn name, {ops_acc, idx_acc} ->
        case Map.get(idx_acc, name) do
          %Slot{} = ls ->
            ops = build_ops(ls)
            {ops_acc ++ ops, Map.put(idx_acc, name, Slot.prune(ls))}

          nil ->
            {ops_acc, idx_acc}
        end
      end)

    next_index = Map.put(next_index, @changed_key, MapSet.new())

    prior = Socket.get_private(socket, @drained_key, [])
    next_drained = prior ++ drained_ops

    socket = put_streams_index(socket, next_index)

    if next_drained == [] and prior == [] do
      socket
    else
      Socket.put_private(socket, @drained_key, next_drained)
    end
  end

  @doc """
  Returns the names of streams marked changed (mutated since the last prune).

  Exposed for `Musubi.Resolver` and tests.

  ## Examples

      Musubi.Stream.changed_streams(socket)
      #=> MapSet.new([:messages])
  """
  @spec changed_streams(Socket.t()) :: MapSet.t(stream_name())
  def changed_streams(%Socket{} = socket) do
    socket |> streams_index() |> Map.get(@changed_key, MapSet.new())
  end

  # ---------------------------------------------------------------------------
  # Internal: streams index shape helpers
  # ---------------------------------------------------------------------------

  defp streams_index(%Socket{assigns: assigns}), do: Map.get(assigns, @assigns_key, %{})

  defp put_streams_index(%Socket{} = socket, index) do
    %{socket | assigns: Map.put(socket.assigns, @assigns_key, index)}
  end

  defp update_streams_index(%Socket{} = socket, fun) when is_function(fun, 1) do
    put_streams_index(socket, fun.(streams_index(socket)))
  end

  defp stream_entries(index) do
    index
    |> Enum.filter(fn
      {key, %Slot{}} when is_atom(key) -> not reserved_key?(key)
      _other -> false
    end)
    |> Enum.sort_by(fn {_name, %Slot{ref: ref}} -> ref end)
  end

  defp reserved_key?(@ref_key), do: true
  defp reserved_key?(@changed_key), do: true
  defp reserved_key?(@configured_key), do: true
  defp reserved_key?(_other), do: false

  defp ensure_stream_initialized(%Socket{} = socket, name) do
    case Map.get(streams_index(socket), name) do
      %Slot{} ->
        socket

      nil ->
        compile_config = compile_config!(socket, name)
        index = streams_index(socket)
        configured = Map.get(index, @configured_key, %{})
        runtime_overrides = Map.get(configured, name, %{})
        merged = Map.merge(compile_config, runtime_overrides)

        {ref, index} = next_ref(index)

        slot = %Slot{
          name: name,
          item_key_fun: Map.fetch!(merged, :item_key),
          ref: ref,
          inserts: [],
          deletes: [],
          reset?: false
        }

        index = stamp_limit_default(index, name, Map.get(merged, :limit))

        next_index = Map.put(index, name, slot)

        put_streams_index(socket, next_index)
    end
  end

  # Tracks the compile-time/configured `:limit` so each insert can default to
  # it without re-reading reflection. Stored under the configured map.
  defp stamp_limit_default(index, name, limit) do
    configured = Map.get(index, @configured_key, %{})
    cfg = Map.put(Map.get(configured, name, %{}), :limit, limit)
    Map.put(index, @configured_key, Map.put(configured, name, cfg))
  end

  defp next_ref(index) do
    counter = Map.get(index, @ref_key, 0)
    {counter, Map.put(index, @ref_key, counter + 1)}
  end

  defp compile_config!(%Socket{module: module}, name) do
    if module && function_exported?(module, :__musubi_stream_config__, 1) do
      module.__musubi_stream_config__(name)
    else
      raise ArgumentError,
            "stream :#{name} not declared on module #{inspect(module)} — declare it inside state do."
    end
  end

  defp fetch_slot!(%Socket{} = socket, name) do
    Map.fetch!(streams_index(socket), name)
  end

  defp update_slot(%Socket{} = socket, name, fun) when is_function(fun, 1) do
    update_streams_index(socket, fn index ->
      slot = Map.fetch!(index, name)
      next_slot = fun.(slot)
      changed = Map.get(index, @changed_key, MapSet.new())

      index
      |> Map.put(name, next_slot)
      |> Map.put(@changed_key, MapSet.put(changed, name))
    end)
  end

  defp mark_reset(%Socket{} = socket, name) do
    update_slot(socket, name, fn ls -> %{ls | reset?: true} end)
  end

  # ---------------------------------------------------------------------------
  # Internal: item_key + position + limit
  # ---------------------------------------------------------------------------

  defp compute_item_key(opts, default_fun, item, module, name) do
    fun =
      case Keyword.fetch(opts, :item_key) do
        {:ok, override} -> validate_item_key_override!(override, name, :stream_insert)
        :error -> default_fun
      end

    invoke_item_key!(fun, item, module, name)
  end

  defp validate_item_key_override!(fun, _name, _call_site) when is_function(fun, 1), do: fun

  defp validate_item_key_override!(other, name, call_site) do
    raise ArgumentError,
          "#{call_site}(:#{name}, ...) :item_key must be an arity-1 function, got: #{inspect(other)}"
  end

  defp invoke_item_key!(fun, item, module, name) do
    result = fun.(item)

    if is_binary(result) do
      result
    else
      raise ArgumentError,
            "stream :#{name} item_key on #{inspect(module)} must return a binary, got: #{inspect(result)}"
    end
  rescue
    error in [KeyError] ->
      reraise ArgumentError,
              "stream :#{name} on #{inspect(module)}: item is missing the `:id` field required " <>
                "by the default item_key — got #{inspect(item)} (KeyError: #{Exception.message(error)})",
              __STACKTRACE__

    error in [ArgumentError] ->
      reraise error, __STACKTRACE__
  end

  defp validate_position!(-1), do: -1
  defp validate_position!(0), do: 0
  defp validate_position!(index) when is_integer(index) and index > 0, do: index

  defp validate_position!(other) do
    raise ArgumentError,
          "stream_insert :at expects -1, 0, or a positive integer, got: #{inspect(other)}"
  end

  defp validate_limit!(nil), do: nil
  defp validate_limit!(limit) when is_integer(limit), do: limit

  defp validate_limit!(other) do
    raise ArgumentError,
          "stream_insert :limit expects an integer or nil, got: #{inspect(other)}"
  end

  # ---------------------------------------------------------------------------
  # Internal: op builders
  # ---------------------------------------------------------------------------

  defp build_ops(%Slot{} = ls) do
    name_str = Atom.to_string(ls.name)
    ref = Integer.to_string(ls.ref)

    reset_ops = if ls.reset?, do: [%{op: "reset", stream: name_str, ref: ref}], else: []

    insert_ops =
      ls.inserts
      |> Enum.reverse()
      |> Enum.map(fn {item_key, at, item, limit} ->
        %{
          op: "insert",
          stream: name_str,
          ref: ref,
          item_key: item_key,
          at: at,
          item: Wire.to_wire(item),
          limit: limit
        }
      end)

    delete_ops =
      ls.deletes
      |> Enum.reverse()
      |> Enum.map(fn item_key ->
        %{op: "delete", stream: name_str, ref: ref, item_key: item_key}
      end)

    reset_ops ++ insert_ops ++ delete_ops
  end

  defp build_config_overrides(opts, name) do
    Enum.reduce(opts, %{}, fn
      {:item_key, fun}, acc ->
        Map.put(acc, :item_key, validate_item_key_override!(fun, name, :stream_configure))

      {:limit, limit}, acc when is_integer(limit) or is_nil(limit) ->
        Map.put(acc, :limit, limit)

      {key, value}, _acc ->
        raise ArgumentError,
              "stream_configure(:#{name}, ...) does not accept option #{inspect({key, value})}"
    end)
  end
end
