defmodule Musubi.Resolver do
  @moduledoc """
  Public render resolver for Musubi store trees.

  `resolve/2` renders the given store socket, resolves any `child(...)`
  placeholders bottom-up, then for each rendered store runs the lifecycle
  pipeline:

    1. `:after_render` hooks — receive the Elixir-form `Musubi.Page.Frame` and may
       rewrite its `render`.
    2. Wire serialization — converts the (possibly hook-rewritten) resolved
       Elixir term to wire form while stitching cached child `wire_state` at
       reused store boundaries.

  The `:after_serialize` stage runs later, at the page server's aggregation phase
  (over the wire frame, including drained push events), not here.

  Each rendered store node's resolved state map carries
  `__musubi_store_id__: store_id_array`, the array runtime identity the client
  echoes verbatim when issuing commands.

  After the pipeline, `socket.assigns.__changed__` is reset and the registry
  entry stores `raw_state` (pre-resolution Elixir form), `resolved_state`
  (resolved Elixir form, used for memoization), and `wire_state` (wire form,
  consumed by the M4 diff engine).

  Return shape:

      {:ok, resolved_root, updated_socket, updated_store_table}

  `resolved_root` is the Elixir-form output of the root render. The matching
  wire-form root is available via the registry root entry's `:wire_state`.
  """

  alias Musubi.AsyncResult
  alias Musubi.Child
  alias Musubi.Lifecycle
  alias Musubi.Page.Frame
  alias Musubi.Page.StoreTable
  alias Musubi.Page.StoreTable.Entry
  alias Musubi.Reconciler
  alias Musubi.Socket
  alias Musubi.Stream
  alias Musubi.Stream.AsyncPlaceholder
  alias Musubi.Stream.Marker
  alias Musubi.Stream.Placeholder
  alias Musubi.Telemetry
  alias Musubi.Upload.Marker, as: UploadMarker
  alias Musubi.Wire

  @store_id_key :__musubi_store_id__

  @type resolved_scalar() :: nil | boolean() | number() | String.t() | atom()
  @type resolved_value() ::
          resolved_scalar() | [resolved_value()] | %{optional(term()) => resolved_value()}
  @typep stitchable_value() ::
           resolved_scalar()
           | struct()
           | [stitchable_value()]
           | %{optional(atom() | String.t()) => stitchable_value()}
  @type resolve_result() :: {:ok, resolved_value(), Socket.t(), StoreTable.t()}

  @doc """
  Returns the reserved key name carried on every resolved store-node render output.

  ## Examples

      iex> Musubi.Resolver.store_id_key()
      :__musubi_store_id__
  """
  @spec store_id_key() :: :__musubi_store_id__
  def store_id_key, do: @store_id_key

  @doc """
  Renders one store tree and resolves child placeholders bottom-up.

  ## Examples

      iex> defmodule ResolverDocChild do
      ...>   use Musubi.Store
      ...>   state do
      ...>     field :title, String.t()
      ...>   end
      ...>   def render(socket), do: %{title: socket.assigns.title}
      ...> end
      iex> defmodule ResolverDocRoot do
      ...>   use Musubi.Store
      ...>   state do
      ...>     field :child, map()
      ...>   end
      ...>   def render(_socket), do: %{child: Musubi.Child.child(ResolverDocChild, id: "child", title: "Inbox")}
      ...> end
      iex> socket = %Musubi.Socket{id: "", parent_path: [], module: ResolverDocRoot, assigns: %{}, private: %{}}
      iex> registry =
      ...>   Musubi.Page.StoreTable.put(
      ...>     Musubi.Page.StoreTable.new(),
      ...>     [],
      ...>     %Musubi.Page.StoreTable.Entry{socket: socket, module: ResolverDocRoot}
      ...>   )
      iex> {:ok, %{child: %{title: "Inbox", __musubi_store_id__: ["child"]}, __musubi_store_id__: []}, _socket, _registry} = Musubi.Resolver.resolve(socket, registry)
  """
  @spec resolve(Socket.t(), StoreTable.t()) :: resolve_result()
  def resolve(%Socket{} = socket, %StoreTable{} = registry) do
    resolve_started_at = System.monotonic_time()

    {resolved_root, updated_socket, updated_registry, live_identities} =
      render_store(socket, registry, %{})

    final_registry = Reconciler.prune_stale_entries(updated_registry, live_identities)

    Telemetry.emit(
      [:musubi, :resolve, :stop],
      %{duration: System.monotonic_time() - resolve_started_at},
      %{module: socket.module, store_id: Socket.store_id(socket)}
    )

    {:ok, resolved_root, updated_socket, final_registry}
  end

  defp render_store(%Socket{} = socket, %StoreTable{} = registry, live_identities)
       when is_map(live_identities) do
    store_id = Socket.store_id(socket)
    raw_state = render_input(socket, registry, store_id)

    {resolved_state, resolved_registry, resolved_live_identities} =
      resolve_value(raw_state, socket, registry, store_id, live_identities)

    resolved_state = normalize_stream_placeholders!(resolved_state, socket)
    resolved_state = validate_and_inject_upload_markers!(resolved_state, socket)
    resolved_state = inject_store_id(resolved_state, store_id)

    {resolved_state, next_socket} = finalize_socket(socket, resolved_state)

    wire_state = stitch_wire(resolved_state, resolved_registry, store_id)

    next_registry =
      StoreTable.put(
        resolved_registry,
        store_id,
        %Entry{
          socket: next_socket,
          module: next_socket.module,
          raw_state: raw_state,
          resolved_state: resolved_state,
          wire_state: wire_state,
          consumed_assigns: entry_consumed_assigns(registry, store_id)
        }
      )

    next_live_identities = Map.put(resolved_live_identities, store_id, true)

    {resolved_state, next_socket, next_registry, next_live_identities}
  end

  @spec render_input(Socket.t(), StoreTable.t(), StoreTable.key()) :: Entry.raw_state()
  defp render_input(%Socket{} = socket, %StoreTable{} = registry, []) do
    case StoreTable.get(registry, []) do
      %Entry{raw_state: raw_state} ->
        if not Socket.any_changed?(socket) and raw_state != :not_rendered and
             not has_changed_streams?(socket) do
          raw_state
        else
          socket.module.render(socket)
        end

      _entry ->
        socket.module.render(socket)
    end
  end

  defp render_input(%Socket{} = socket, %StoreTable{}, _store_id),
    do: socket.module.render(socket)

  # Runs the `:after_render` transform stage over the Elixir-form frame, then
  # prunes streams and resets change tracking. `:after_serialize` runs later at
  # the page server's aggregation phase. Returns the (possibly hook-rewritten)
  # resolved render term plus the updated socket.
  @spec finalize_socket(Socket.t(), resolved_value()) :: {resolved_value(), Socket.t()}
  defp finalize_socket(%Socket{} = socket, resolved_state) do
    {%Frame{render: render}, hooked_socket} =
      Lifecycle.run_transform_hooks(socket, :after_render, %Frame{render: resolved_state})

    {render, hooked_socket |> Stream.drain_and_prune() |> Socket.reset_changed()}
  end

  @spec has_changed_streams?(Socket.t()) :: boolean()
  defp has_changed_streams?(%Socket{} = socket) do
    socket
    |> Stream.changed_streams()
    |> MapSet.size() > 0
  end

  defp inject_store_id(resolved_state, store_id) when is_map(resolved_state) do
    Map.put(resolved_state, @store_id_key, store_id)
  end

  defp inject_store_id(resolved_state, _store_id), do: resolved_state

  @spec stitch_wire(stitchable_value(), StoreTable.t(), StoreTable.key()) :: Entry.wire_state()
  defp stitch_wire(list, %StoreTable{} = registry, own_store_id) when is_list(list) do
    Enum.map(list, &stitch_wire(&1, registry, own_store_id))
  end

  defp stitch_wire(value, _unused_registry, _unused_own_store_id) when is_struct(value),
    do: Wire.to_wire(value)

  defp stitch_wire(%{@store_id_key => store_id} = map, %StoreTable{} = registry, own_store_id)
       when is_list(store_id) and store_id != own_store_id do
    cached_child_wire(registry, store_id) || Wire.to_wire(map)
  end

  defp stitch_wire(map, %StoreTable{} = registry, own_store_id) when is_map(map) do
    Map.new(map, fn {key, value} ->
      {Wire.Encoder.key_to_wire(key), stitch_wire(value, registry, own_store_id)}
    end)
  end

  defp stitch_wire(value, _registry, _own_store_id), do: Wire.to_wire(value)

  @spec cached_child_wire(StoreTable.t(), StoreTable.key()) :: Entry.wire_state() | nil
  defp cached_child_wire(%StoreTable{} = registry, store_id) do
    case StoreTable.get(registry, store_id) do
      %Entry{} = entry ->
        entry.wire_state

      nil ->
        nil
    end
  end

  @spec normalize_stream_placeholders!(resolved_value(), Socket.t()) :: resolved_value()
  defp normalize_stream_placeholders!(resolved_state, %Socket{} = socket) do
    streams_by_name = declared_streams_by_name(socket.module)

    {normalized, placements} =
      replace_stream_placeholders!(resolved_state, [], %{}, streams_by_name, socket)

    ensure_all_streams_placed!(streams_by_name, placements)
    normalized
  end

  @spec declared_streams_by_name(module()) :: %{optional(atom()) => map()}
  defp declared_streams_by_name(module) do
    if function_exported?(module, :__musubi__, 1) do
      streams = module.__musubi__(:streams)

      streams
      |> List.wrap()
      |> Map.new(fn %{name: name} = stream -> {name, stream} end)
    else
      %{}
    end
  end

  @spec replace_stream_placeholders!(
          resolved_value(),
          [String.t()],
          %{optional(atom()) => [String.t()]},
          %{
            optional(atom()) => map()
          },
          Socket.t()
        ) ::
          {resolved_value(), %{optional(atom()) => [String.t()]}}
  defp replace_stream_placeholders!(
         %Placeholder{name: name},
         path,
         placements,
         streams_by_name,
         _socket
       ) do
    current_path = Enum.reverse(path)

    case Map.fetch(streams_by_name, name) do
      {:ok, %{path: ^current_path}} ->
        if Map.has_key?(placements, name) do
          raise ArgumentError,
                "stream #{inspect(name)} rendered more than once"
        end

        {Marker.new(name), Map.put(placements, name, current_path)}

      {:ok, %{path: expected_path}} ->
        raise ArgumentError,
              "stream #{inspect(name)} rendered at #{format_stream_path(current_path)}, " <>
                "but it is declared at #{format_stream_path(expected_path)}"

      :error ->
        raise ArgumentError, "stream #{inspect(name)} is not declared"
    end
  end

  defp replace_stream_placeholders!(
         %AsyncPlaceholder{name: name},
         path,
         placements,
         streams_by_name,
         %Socket{} = socket
       ) do
    current_path = Enum.reverse(path)

    case Map.fetch(streams_by_name, name) do
      {:ok, %{path: expected_path}} ->
        expected_parent_path = async_stream_parent_path!(name, expected_path)

        cond do
          Map.has_key?(placements, name) ->
            raise ArgumentError,
                  "stream #{inspect(name)} rendered more than once"

          current_path != expected_parent_path ->
            raise ArgumentError,
                  "async stream #{inspect(name)} rendered at #{format_stream_path(current_path)}, " <>
                    "but it is declared at #{format_stream_path(expected_parent_path)}"

          true ->
            async = async_stream_assign!(socket, name)
            {%{async | result: Marker.new(name)}, Map.put(placements, name, expected_path)}
        end

      :error ->
        raise ArgumentError, "async stream #{inspect(name)} is not declared"
    end
  end

  defp replace_stream_placeholders!(
         %AsyncResult{} = async,
         path,
         placements,
         streams_by_name,
         socket
       ) do
    {resolved_result, next_placements} =
      replace_stream_placeholders!(
        async.result,
        ["result" | path],
        placements,
        streams_by_name,
        socket
      )

    {%{async | result: resolved_result}, next_placements}
  end

  defp replace_stream_placeholders!(value, path, placements, streams_by_name, socket)
       when is_map(value) and not is_struct(value) do
    cond do
      Map.has_key?(value, @store_id_key) ->
        {value, placements}

      Marker.marker?(value) ->
        raise ArgumentError,
              "stream marker at #{format_stream_path(Enum.reverse(path))} was not produced by stream(:name)"

      true ->
        Enum.reduce(value, {%{}, placements}, fn {key, child}, {acc, current_placements} ->
          {resolved_child, next_placements} =
            replace_stream_placeholders!(
              child,
              [to_string(key) | path],
              current_placements,
              streams_by_name,
              socket
            )

          {Map.put(acc, key, resolved_child), next_placements}
        end)
    end
  end

  defp replace_stream_placeholders!(value, path, placements, streams_by_name, socket)
       when is_list(value) do
    {resolved_list, next_placements} =
      value
      |> Enum.with_index()
      |> Enum.map_reduce(placements, fn {element, index}, current_placements ->
        {resolved_element, next_placements} =
          replace_stream_placeholders!(
            element,
            [Integer.to_string(index) | path],
            current_placements,
            streams_by_name,
            socket
          )

        {resolved_element, next_placements}
      end)

    {resolved_list, next_placements}
  end

  defp replace_stream_placeholders!(value, _path, placements, _streams_by_name, _socket) do
    {value, placements}
  end

  defp async_stream_parent_path!(name, expected_path) do
    case Enum.reverse(expected_path) do
      ["result" | reversed_parent_path] ->
        Enum.reverse(reversed_parent_path)

      _other ->
        raise ArgumentError,
              "async_stream(#{inspect(name)}) requires an AsyncResult.of(stream(...)) " <>
                "state declaration"
    end
  end

  defp async_stream_assign!(%Socket{} = socket, name) when is_atom(name) do
    case Map.fetch(socket.assigns, name) do
      {:ok, %AsyncResult{} = async} ->
        async

      {:ok, other} ->
        raise ArgumentError,
              "async_stream(#{inspect(name)}) expects socket.assigns.#{name} to be " <>
                "a Musubi.AsyncResult, got: #{inspect(other)}"

      :error ->
        AsyncResult.loading()
    end
  end

  @spec ensure_all_streams_placed!(%{optional(atom()) => map()}, %{
          optional(atom()) => [String.t()]
        }) ::
          :ok
  defp ensure_all_streams_placed!(streams_by_name, placements) do
    missing =
      streams_by_name
      |> Map.keys()
      |> Enum.reject(&Map.has_key?(placements, &1))

    case missing do
      [] ->
        :ok

      [name | _rest] ->
        raise ArgumentError,
              "declared stream #{inspect(name)} was not rendered with stream(#{inspect(name)})"
    end
  end

  @spec format_stream_path([String.t()]) :: String.t()
  defp format_stream_path([]), do: "/"
  defp format_stream_path(path), do: "/" <> Enum.join(path, "/")

  defp resolve_value(
         %Child{} = child,
         %Socket{} = parent_socket,
         %StoreTable{} = registry,
         path,
         live
       )
       when is_list(path) do
    resolve_child(child, parent_socket, registry, path, live)
  end

  defp resolve_value(value, %Socket{} = parent_socket, %StoreTable{} = registry, path, live)
       when is_map(value) and not is_struct(value) do
    Enum.reduce(value, {%{}, registry, live}, fn {key, child_or_value},
                                                 {acc, current_registry, current_live} ->
      if match?(%Child{}, child_or_value) do
        {resolved_child, next_registry, next_live} =
          resolve_child(child_or_value, parent_socket, current_registry, path, current_live)

        {Map.put(acc, key, resolved_child), next_registry, next_live}
      else
        next_path = append_path_segment(path, to_string(key))

        {resolved_child, next_registry, next_live} =
          resolve_value(child_or_value, parent_socket, current_registry, next_path, current_live)

        {Map.put(acc, key, resolved_child), next_registry, next_live}
      end
    end)
  end

  defp resolve_value(value, %Socket{} = parent_socket, %StoreTable{} = registry, path, live)
       when is_list(value) do
    {resolved_list, {next_registry, next_live}} =
      Enum.map_reduce(value, {registry, live}, fn element, {current_registry, current_live} ->
        {resolved_element, next_registry, next_live} =
          resolve_value(element, parent_socket, current_registry, path, current_live)

        {resolved_element, {next_registry, next_live}}
      end)

    {resolved_list, next_registry, next_live}
  end

  defp resolve_value(value, _parent_socket, registry, _path, live) do
    {value, registry, live}
  end

  defp resolve_child(
         %Child{} = child,
         %Socket{} = parent_socket,
         %StoreTable{} = registry,
         path,
         live
       )
       when is_list(path) do
    case Reconciler.reconcile_child(child, parent_socket, path, registry) do
      {:reuse, store_id, %Entry{} = entry, consumed_assigns} ->
        ensure_unique_identity!(store_id, live)

        next_registry =
          StoreTable.put(registry, store_id, %{entry | consumed_assigns: consumed_assigns})

        {entry.resolved_state, next_registry, mark_subtree_live(registry, store_id, live)}

      {:mount, store_id, %Socket{} = child_socket, consumed_assigns} ->
        ensure_unique_identity!(store_id, live)
        mounted_socket = Reconciler.mount_store(child_socket)

        {resolved_state, next_socket, next_registry, next_live} =
          render_store(mounted_socket, registry, live)

        next_registry = put_consumed_assigns(next_registry, store_id, consumed_assigns)

        {resolved_state, next_socket_registry_socket(next_registry, store_id, next_socket),
         Map.put(next_live, store_id, true)}

      {:update, store_id, %Socket{} = child_socket, consumed_assigns} ->
        ensure_unique_identity!(store_id, live)

        {resolved_state, next_socket, next_registry, next_live} =
          render_store(child_socket, registry, live)

        next_registry = put_consumed_assigns(next_registry, store_id, consumed_assigns)

        {resolved_state, next_socket_registry_socket(next_registry, store_id, next_socket),
         Map.put(next_live, store_id, true)}
    end
  end

  @spec entry_consumed_assigns(StoreTable.t(), StoreTable.key()) ::
          %{optional(Socket.assign_key()) => term()}
  defp entry_consumed_assigns(%StoreTable{} = registry, store_id) do
    case StoreTable.get(registry, store_id) do
      %Entry{consumed_assigns: consumed_assigns} -> consumed_assigns
      nil -> %{}
    end
  end

  defp ensure_unique_identity!(store_id, live_identities) do
    if Map.has_key?(live_identities, store_id) do
      raise ArgumentError,
            "duplicate child store_id encountered during reconcile: #{inspect(store_id)} " <>
              "(two children share the same parent and id; ids must be unique among siblings " <>
              "regardless of module)"
    end

    :ok
  end

  @spec put_consumed_assigns(StoreTable.t(), StoreTable.key(), %{
          optional(Socket.assign_key()) => term()
        }) ::
          StoreTable.t()
  defp put_consumed_assigns(%StoreTable{} = registry, store_id, consumed_assigns) do
    case StoreTable.get(registry, store_id) do
      %Entry{} = entry ->
        StoreTable.put(registry, store_id, %{entry | consumed_assigns: consumed_assigns})

      nil ->
        registry
    end
  end

  @spec mark_subtree_live(StoreTable.t(), StoreTable.key(), %{optional(StoreTable.key()) => true}) ::
          %{optional(StoreTable.key()) => true}
  defp mark_subtree_live(%StoreTable{} = registry, store_id, live_identities)
       when is_list(store_id) and is_map(live_identities) do
    Enum.reduce(StoreTable.subtree_keys(registry, store_id), live_identities, fn subtree_store_id,
                                                                                 acc ->
      Map.put(acc, subtree_store_id, true)
    end)
  end

  @spec next_socket_registry_socket(StoreTable.t(), StoreTable.key(), Socket.t()) ::
          StoreTable.t()
  defp next_socket_registry_socket(%StoreTable{} = registry, store_id, socket) do
    case StoreTable.get(registry, store_id) do
      %Entry{} = entry ->
        StoreTable.put(registry, store_id, %{entry | socket: socket})

      nil ->
        registry
    end
  end

  @spec append_path_segment([String.t()], String.t()) :: [String.t()]
  defp append_path_segment(path, segment) when is_list(path) and is_binary(segment) do
    List.insert_at(path, -1, segment)
  end

  @spec validate_and_inject_upload_markers!(resolved_value(), Socket.t()) :: resolved_value()
  defp validate_and_inject_upload_markers!(resolved_state, %Socket{} = socket) do
    declared = declared_uploads_by_name(socket.module)

    walk_for_upload_markers!(resolved_state, [], declared)

    inject_upload_markers(resolved_state, declared, socket)
  end

  @spec declared_uploads_by_name(module() | nil) :: %{optional(atom()) => map()}
  defp declared_uploads_by_name(nil), do: %{}

  defp declared_uploads_by_name(module) when is_atom(module) do
    if function_exported?(module, :__musubi__, 1) do
      uploads = List.wrap(module.__musubi__(:uploads))
      Map.new(uploads, fn %{name: name} = config -> {name, config} end)
    else
      %{}
    end
  end

  defp upload_declared?(declared, name) when is_binary(name) do
    Enum.any?(declared, fn {declared_name, _config} -> Atom.to_string(declared_name) == name end)
  end

  defp walk_for_upload_markers!(value, path, declared)
       when is_map(value) and not is_struct(value) do
    cond do
      Map.has_key?(value, @store_id_key) ->
        :ok

      UploadMarker.marker?(value) ->
        name = UploadMarker.marker_name(value)
        formatted = format_stream_path(Enum.reverse(path))

        if upload_declared?(declared, name) do
          raise ArgumentError,
                "hand-written upload marker at #{formatted}; remove it — the framework " <>
                  "injects upload markers automatically"
        else
          raise ArgumentError,
                "unknown upload #{inspect(name)} referenced at #{formatted}; declare it " <>
                  "with `upload :#{name}, ...` at the top level of the store module"
        end

      true ->
        Enum.each(value, fn {key, child} ->
          walk_for_upload_markers!(child, [to_string(key) | path], declared)
        end)
    end
  end

  defp walk_for_upload_markers!(value, path, declared) when is_list(value) do
    value
    |> Enum.with_index()
    |> Enum.each(fn {element, index} ->
      walk_for_upload_markers!(element, [Integer.to_string(index) | path], declared)
    end)
  end

  defp walk_for_upload_markers!(_value, _path, _declared), do: :ok

  defp inject_upload_markers(resolved_state, declared, %Socket{} = _socket)
       when map_size(declared) == 0 do
    resolved_state
  end

  defp inject_upload_markers(resolved_state, declared, %Socket{module: module})
       when is_map(resolved_state) and not is_struct(resolved_state) do
    Enum.reduce(declared, resolved_state, fn {name, _config}, acc ->
      if Map.has_key?(acc, name) or Map.has_key?(acc, Atom.to_string(name)) do
        raise ArgumentError,
              "upload :#{name} on #{inspect(module)} collides with a key returned by " <>
                "render/1; rename either the upload or the render-output key"
      else
        Map.put(acc, name, UploadMarker.new(name))
      end
    end)
  end

  defp inject_upload_markers(_resolved_state, _declared, %Socket{module: module}) do
    raise ArgumentError,
          "uploads are declared on #{inspect(module)} but render/1 did not return a map; " <>
            "upload markers can only be injected into a map-shaped render output"
  end
end
