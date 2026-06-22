defmodule Musubi.Reconciler do
  @moduledoc "Child identity, lifecycle, attr-defaulting, and disappearance handling for Musubi render resolution."

  alias Musubi.Child
  alias Musubi.DSL.Attr
  alias Musubi.Page.StoreTable
  alias Musubi.Page.StoreTable.Entry
  alias Musubi.Socket
  alias Musubi.Stream
  alias Musubi.Telemetry

  @type identity_key() :: StoreTable.key()

  @type reconcile_result() ::
          {:mount, identity_key(), Socket.t(), map()}
          | {:update, identity_key(), Socket.t(), map()}
          | {:reuse, identity_key(), Entry.t(), map()}

  @doc """
  Reconciles one child placeholder against the existing registry entry.

  Returns a tagged action describing whether the child must mount, update, or
  can reuse the previously resolved output. The returned `identity_key` is the
  child's `store_id` (parent's `store_id ++ [local id]`).

  ## Examples

      iex> parent_socket = Musubi.Socket.assign(%Musubi.Socket{}, :title, "Inbox")
      iex> child = Musubi.Child.child(ExampleChild, id: "child", title: "Inbox")
      iex> {:mount, ["child"], %Musubi.Socket{}, %{title: "Inbox"}} =
      ...>   Musubi.Reconciler.reconcile_child(child, parent_socket, [], Musubi.Page.StoreTable.new())
  """
  @spec reconcile_child(Child.t(), Socket.t(), [String.t()], StoreTable.t()) ::
          reconcile_result()
  def reconcile_child(
        %Child{} = child,
        %Socket{} = parent_socket,
        parent_path,
        %StoreTable{} = registry
      )
      when is_list(parent_path) do
    id = validate_id!(child)
    assigns = normalize_assigns(child.module, child.assigns)
    store_id = List.insert_at(parent_path, -1, id)

    case StoreTable.get(registry, store_id) do
      %Entry{module: existing_module} = entry when existing_module == child.module ->
        cond do
          # Did the parent pass different props than last render? Compared
          # against a snapshot of the previously-passed props — NOT the child's
          # live `socket.assigns`, which the child mutates itself (reload,
          # command, async write). Reading live assigns would report a phantom
          # change whenever the child overwrote a consumed-key-named assign,
          # re-firing `update/2` every render (an infinite loop when `update/2`
          # calls `send_update`).
          entry.consumed_assigns !== assigns ->
            next_socket = update_store(entry.socket, assigns)
            {:update, store_id, next_socket, assigns}

          # Child has internal mutations queued (from a command handler, an
          # async result write, or a stream insert) since the last render. The
          # parent did not change so `update/2` does not run, but the child
          # still needs to re-render so its new state surfaces in the wire diff.
          subtree_dirty?(registry, store_id) ->
            {:update, store_id, entry.socket, assigns}

          true ->
            {:reuse, store_id, %{entry | consumed_assigns: assigns}, assigns}
        end

      _missing_or_module_change ->
        {:mount, store_id,
         new_child_socket(parent_socket, parent_path, child.module, id, assigns), assigns}
    end
  end

  @spec subtree_dirty?(StoreTable.t(), identity_key()) :: boolean()
  defp subtree_dirty?(%StoreTable{} = registry, store_id) when is_list(store_id) do
    registry
    |> StoreTable.subtree_keys(store_id)
    |> Enum.any?(fn subtree_store_id ->
      case StoreTable.get(registry, subtree_store_id) do
        %Entry{socket: socket} -> Socket.any_changed?(socket) or stream_changed?(socket)
        nil -> false
      end
    end)
  end

  @spec stream_changed?(Socket.t()) :: boolean()
  defp stream_changed?(%Socket{} = socket) do
    socket
    |> Stream.changed_streams()
    |> MapSet.size() > 0
  end

  @doc """
  Runs the store initialization callback and returns the initialized socket.

  ## Examples

      iex> defmodule ReconcilerMountDocStore do
      ...>   def init(socket), do: {:ok, Musubi.Socket.assign(socket, :mounted?, true)}
      ...> end
      iex> socket = %Musubi.Socket{module: ReconcilerMountDocStore}
      iex> Musubi.Reconciler.init_store(socket).assigns.mounted?
      true
  """
  @spec init_store(Socket.t()) :: Socket.t()
  def init_store(%Socket{module: module} = socket) when is_atom(module) do
    {result, fun, arity} =
      cond do
        function_exported?(module, :init, 1) ->
          {module.init(socket), :init, 1}

        function_exported?(module, :mount, 1) ->
          {module.mount(socket), :mount, 1}

        true ->
          {{:ok, socket}, :init, 1}
      end

    socket = validate_callback_result!(result, module, fun, arity)

    # Per BDR-0025 the first envelope after a store's mount carries a
    # `{op: config}` upload op for every declared upload. Running this
    # here (instead of only at root init) ensures child stores that
    # declare uploads also emit their config when they first mount —
    # required for the per-item child-store pattern (BDR-0028).
    Musubi.Upload.ensure_configs(socket)
  end

  @doc """
  Runs the legacy store mount callback.

  This function remains as a compatibility wrapper for callers using the old
  `mount/1` naming. New code should call `init_store/1`.

  ## Examples

      iex> defmodule ReconcilerLegacyMountDocStore do
      ...>   def mount(socket), do: {:ok, Musubi.Socket.assign(socket, :mounted?, true)}
      ...> end
      iex> socket = %Musubi.Socket{module: ReconcilerLegacyMountDocStore}
      iex> Musubi.Reconciler.mount_store(socket).assigns.mounted?
      true
  """
  @spec mount_store(Socket.t()) :: Socket.t()
  def mount_store(%Socket{} = socket) do
    init_store(socket)
  end

  @doc """
  Runs `update/2` when present; otherwise merges the new assigns into the socket.

  ## Examples

      iex> defmodule ReconcilerUpdateDocStore do
      ...>   def update(assigns, socket), do: {:ok, Musubi.Socket.assign(socket, assigns)}
      ...> end
      iex> socket = %Musubi.Socket{module: ReconcilerUpdateDocStore, assigns: %{}, private: %{}}
      iex> Musubi.Reconciler.update_store(socket, %{title: "Inbox"}).assigns.title
      "Inbox"
  """
  @spec update_store(Socket.t(), map()) :: Socket.t()
  def update_store(%Socket{module: module} = socket, new_assigns)
      when is_atom(module) and is_map(new_assigns) do
    result =
      if function_exported?(module, :update, 2) do
        module.update(new_assigns, socket)
      else
        {:ok, Socket.assign(socket, new_assigns)}
      end

    validate_callback_result!(result, module, :update, 2)
  end

  @doc """
  Drops any registry entries not observed in the latest render tree.

  A dropped child emits a skeleton lazy-discard telemetry event so M5 can hook
  into the same path when async delivery arrives after disappearance.

  ## Examples

      iex> entry = %Musubi.Page.StoreTable.Entry{socket: %Musubi.Socket{}, module: Example}
      iex> registry =
      ...>   Musubi.Page.StoreTable.put(
      ...>     Musubi.Page.StoreTable.new(),
      ...>     ["root"],
      ...>     entry
      ...>   )
      iex> Musubi.Reconciler.prune_stale_entries(registry, %{})
      %Musubi.Page.StoreTable{entries: %{}}
  """
  @spec prune_stale_entries(StoreTable.t(), map()) :: StoreTable.t()
  def prune_stale_entries(%StoreTable{} = registry, live_identities)
      when is_map(live_identities) do
    Enum.reduce(StoreTable.keys(registry), registry, fn store_id, acc ->
      if Map.has_key?(live_identities, store_id) do
        acc
      else
        emit_lazy_discard(store_id, registry)
        StoreTable.delete(acc, store_id)
      end
    end)
  end

  @doc false
  @spec normalize_assigns(module(), map()) :: map()
  def normalize_assigns(module, assigns) when is_atom(module) and is_map(assigns) do
    attrs =
      if function_exported?(module, :__musubi__, 1) do
        module.__musubi__(:attrs)
      else
        []
      end

    Enum.reduce(attrs, assigns, fn %{name: name, required: required, default: default}, acc ->
      cond do
        Map.has_key?(acc, name) ->
          acc

        default != Attr.no_default() ->
          Map.put(acc, name, default)

        required ->
          raise ArgumentError,
                "missing required attr #{inspect(name)} for child #{inspect(module)}"

        true ->
          acc
      end
    end)
  end

  @spec emit_lazy_discard(identity_key(), StoreTable.t()) :: :ok
  defp emit_lazy_discard(store_id, %StoreTable{} = registry) do
    module =
      case StoreTable.get(registry, store_id) do
        %Entry{module: module} -> module
        nil -> nil
      end

    Telemetry.emit(
      [:musubi, :async, :lazy_discard],
      %{count: 1},
      %{store_id: store_id, module: module}
    )
  end

  @spec new_child_socket(Socket.t(), [String.t()], module(), String.t(), map()) :: Socket.t()
  defp new_child_socket(%Socket{} = parent_socket, parent_path, module, id, assigns)
       when is_list(parent_path) and is_atom(module) and is_binary(id) and is_map(assigns) do
    Socket.assign(
      Socket.inherit_context(parent_socket, %Socket{
        id: id,
        parent_path: parent_path,
        module: module,
        assigns: %{},
        private: %{}
      }),
      assigns
    )
  end

  @spec validate_callback_result!({:ok, Socket.t()} | tuple(), module(), atom(), pos_integer()) ::
          Socket.t()
  defp validate_callback_result!({:ok, %Socket{} = socket}, module, fun, arity)
       when is_atom(module) and is_atom(fun) and is_integer(arity) do
    socket
  end

  defp validate_callback_result!(other, module, fun, arity)
       when is_atom(module) and is_atom(fun) and is_integer(arity) do
    raise ArgumentError,
          "bad callback response from #{inspect(module)}.#{fun}/#{arity}: expected {:ok, %Musubi.Socket{}}, got #{inspect(other)}"
  end

  @spec validate_id!(Child.t()) :: String.t()
  defp validate_id!(%Child{id: id, module: _module}) when is_binary(id), do: id

  defp validate_id!(%Child{id: nil, module: module}) do
    raise ArgumentError, "child #{inspect(module)} is missing required :id"
  end

  defp validate_id!(%Child{id: id, module: module}) do
    raise ArgumentError,
          "child #{inspect(module)} id must be a binary string, got: #{inspect(id)}"
  end
end
