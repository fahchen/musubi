defmodule Musubi.Type do
  @moduledoc """
  Type-AST utilities for Musubi's `state do` reflection.

  Three responsibilities:

    * `valid_type?/1` — compile-time predicate over the AST shape of a field
      type. Used by `Musubi.Plugin.StateField` to reject typos and unsupported
      constructors before reflection is recorded.
    * `verify_module!/1` — `@after_verify` callback that walks every reflected
      field type on the host module and ensures cross-module references
      (`OtherModule.t()` / `OtherModule.state()`) resolve to a loaded module
      that opts into the Musubi runtime contract.
    * `valid?/2` — runtime predicate that checks a wire-form value against a
      single field type AST. The compile-time-generated
      `__musubi_validate_state__/1` walks fields and calls this per leaf.

  ## Wire-form expectations

  `valid?/2` operates on the wire-form output produced by `Musubi.Wire.to_wire/1`.
  Atom keys become string keys, atoms (other than `nil`/`true`/`false`) become
  strings, and structs are recursively converted. Type predicates check the
  wire shape, not the original Elixir form — for example, an `:active` literal
  matches the wire string `"active"`.
  """

  @typedoc "AST node returned by `Macro.expand/2`-friendly `state do` field declarations."
  @type type_ast() :: Macro.t()

  # Stdlib `Module.t()` references that Musubi accepts without an
  # `__musubi_runtime_module__/0` callback.
  @primitive_remote_t %{
    String => :string
  }

  @doc """
  Returns whether `type_ast` is a supported Musubi field-type AST.

  ## Examples

      iex> Musubi.Type.valid_type?(quote do: String.t())
      true
      iex> Musubi.Type.valid_type?(quote do: list(integer()))
      true
      iex> Musubi.Type.valid_type?(quote do: %{title: String.t()})
      true
      iex> Musubi.Type.valid_type?(quote do: bogus_constructor())
      false
  """
  @spec valid_type?(type_ast()) :: boolean()
  def valid_type?(type_ast), do: do_valid_type?(type_ast)

  @doc """
  Walks every reflected field type on `module` and verifies cross-module
  references resolve to loaded modules that opt into the Musubi runtime contract.

  Wired via `@after_verify` in `Musubi.State.__using__/1` and
  `Musubi.Store.__using__/1`. Raises `CompileError` when a reference is unknown
  or the referenced module does not export `__musubi_runtime_module__/0`.

  ## Examples

      iex> defmodule TypeVerifyExample do
      ...>   use Musubi.State
      ...>   state do
      ...>     field :title, String.t()
      ...>   end
      ...> end
      iex> Musubi.Type.verify_module!(TypeVerifyExample)
      :ok
  """
  @spec verify_module!(module()) :: :ok
  def verify_module!(module) when is_atom(module) do
    fields =
      if function_exported?(module, :__musubi__, 1) do
        module.__musubi__(:fields)
      else
        []
      end

    for %{name: name, type: type_ast} <- fields do
      walk_remote_refs(type_ast, module, fn ref_module, kind ->
        verify_remote_ref!(module, name, type_ast, ref_module, kind)
      end)
    end

    :ok
  end

  @doc """
  Returns whether the wire-form `value` matches `type_ast`.

  Single-AST-node predicate. Recurses through unions, lists, literal-keyed
  maps, stream marker fields, and `Musubi.AsyncResult.of(T)`. For `Module.t()` and
  `Module.state()` references it dispatches to
  `Module.__musubi_validate_state__/1` and reduces its result to a boolean.

  ## Examples

      iex> Musubi.Type.valid?("Inbox", quote(do: String.t()))
      true
      iex> Musubi.Type.valid?(42, quote(do: String.t()))
      false
      iex> Musubi.Type.valid?("active", quote(do: :active))
      true
      iex> Musubi.Type.valid?(["a", "b"], quote(do: list(String.t())))
      true
  """
  @spec valid?(term(), type_ast()) :: boolean()
  def valid?(value, type_ast), do: do_valid?(value, type_ast, nil)

  @doc """
  Variant of `valid?/2` that resolves bare aliases against `host_module`'s
  parent namespaces. Used by the compile-time-generated
  `__musubi_validate_state__/1` so nested-module references inside a host
  module's `state do` resolve the way Elixir resolves them at compile time.

  ## Examples

      iex> Musubi.Type.valid?("Inbox", quote(do: String.t()), Musubi.State)
      true
  """
  @spec valid?(term(), type_ast(), module() | nil) :: boolean()
  def valid?(value, type_ast, host_module) when is_atom(host_module) or is_nil(host_module),
    do: do_valid?(value, type_ast, host_module)

  # ---------------------------------------------------------------------------
  # valid_type?/1
  # ---------------------------------------------------------------------------

  defp do_valid_type?({:|, _meta, [left, right]}),
    do: do_valid_type?(left) and do_valid_type?(right)

  defp do_valid_type?({:list, _meta, [inner]}), do: do_valid_type?(inner)

  defp do_valid_type?({:stream, _meta, [inner]}), do: do_valid_type?(inner)

  defp do_valid_type?({:map, _meta, []}), do: true

  defp do_valid_type?({:%{}, _meta, pairs}) when is_list(pairs) do
    Enum.all?(pairs, fn
      {key, value} -> literal_key?(key) and do_valid_type?(value)
      _other -> false
    end)
  end

  defp do_valid_type?({:integer, _meta, []}), do: true
  defp do_valid_type?({:boolean, _meta, []}), do: true
  defp do_valid_type?({:atom, _meta, []}), do: true
  defp do_valid_type?(nil), do: true
  defp do_valid_type?(literal) when is_atom(literal), do: true
  defp do_valid_type?(literal) when is_binary(literal), do: true
  defp do_valid_type?(literal) when is_integer(literal), do: true
  defp do_valid_type?(literal) when is_float(literal), do: true

  # `Module.t()` / `Module.state()` cross-module reference.
  defp do_valid_type?({{:., _dot, [aliased, kind]}, _call, []})
       when kind in [:t, :state] do
    alias?(aliased)
  end

  # `Musubi.AsyncResult.of(T)` — the only `.of(T)` we accept.
  defp do_valid_type?({{:., _dot, [aliased, :of]}, _call, [inner]}) do
    async_result_alias?(aliased) and do_valid_type?(inner)
  end

  defp do_valid_type?(_other), do: false

  defp literal_key?(key) when is_atom(key) and not is_nil(key), do: true
  defp literal_key?(_other), do: false

  defp alias?({:__aliases__, _meta, parts}) when is_list(parts), do: true
  defp alias?(module) when is_atom(module), do: true
  defp alias?(_other), do: false

  defp async_result_alias?({:__aliases__, _meta, [:Musubi, :AsyncResult]}), do: true
  defp async_result_alias?(Musubi.AsyncResult), do: true
  defp async_result_alias?(_other), do: false

  # ---------------------------------------------------------------------------
  # verify_module!/1
  # ---------------------------------------------------------------------------

  defp walk_remote_refs({:|, _meta, [left, right]}, host_module, fun) do
    walk_remote_refs(left, host_module, fun)
    walk_remote_refs(right, host_module, fun)
  end

  defp walk_remote_refs({:list, _meta, [inner]}, host_module, fun),
    do: walk_remote_refs(inner, host_module, fun)

  defp walk_remote_refs({:stream, _meta, [inner]}, host_module, fun),
    do: walk_remote_refs(inner, host_module, fun)

  defp walk_remote_refs({:%{}, _meta, pairs}, host_module, fun) when is_list(pairs) do
    Enum.each(pairs, fn {_key, value} -> walk_remote_refs(value, host_module, fun) end)
  end

  defp walk_remote_refs({{:., _dot, [aliased, kind]}, _call, []}, host_module, fun)
       when kind in [:t, :state] do
    case resolve_alias(aliased, host_module) do
      {:ok, module} -> fun.(module, kind)
      :error -> fun.(unresolved_module(aliased), kind)
    end
  end

  defp walk_remote_refs({{:., _dot, [_aliased, :of]}, _call, [inner]}, host_module, fun),
    do: walk_remote_refs(inner, host_module, fun)

  defp walk_remote_refs(_other, _host_module, _fun), do: :ok

  defp resolve_alias({:__aliases__, _meta, parts}, host_module) when is_list(parts) do
    candidates = candidate_modules(host_module, parts)

    case Enum.find(candidates, &Code.ensure_loaded?/1) do
      nil -> :error
      module -> {:ok, module}
    end
  end

  defp resolve_alias(module, _host_module) when is_atom(module), do: {:ok, module}
  defp resolve_alias(_other, _host_module), do: :error

  # Only reached when `resolve_alias/2` returns `:error`, which never happens for
  # a bare atom module — that clause resolves to `{:ok, module}`.
  defp unresolved_module({:__aliases__, _meta, parts}) when is_list(parts),
    do: concat_module(parts)

  defp candidate_modules(nil, parts), do: [concat_module(parts)]

  defp candidate_modules(host_module, parts) when is_atom(host_module) do
    namespace_parts =
      host_module
      |> Module.split()
      |> Enum.drop(-1)

    namespaced =
      for count <- length(namespace_parts)..1//-1,
          prefix = Enum.take(namespace_parts, count),
          do: concat_module(prefix ++ parts)

    Enum.uniq([concat_module(parts) | namespaced])
  end

  # Module.concat/1 creates atoms at runtime, but our candidate list only
  # contains module references the host module's `state do` block was already
  # compiled to reference. Module.safe_concat/1 would refuse modules that
  # haven't been loaded yet, which defeats the namespace-walk lookup.
  # credo:disable-for-next-line Credo.Check.Warning.UnsafeToAtom
  defp concat_module(parts), do: Module.concat(parts)

  defp verify_remote_ref!(host_module, field_name, type_ast, ref_module, kind) do
    cond do
      Map.has_key?(@primitive_remote_t, ref_module) and kind == :t ->
        :ok

      not musubi_runtime_loaded?(ref_module) ->
        raise CompileError,
          description:
            "#{inspect(host_module)}.#{field_name}: type #{Macro.to_string(type_ast)} references " <>
              "#{inspect(ref_module)}.#{kind}() but #{inspect(ref_module)} is not a loadable Musubi runtime module"

      kind == :state and musubi_kind(ref_module) != :store ->
        raise CompileError,
          description:
            "#{inspect(host_module)}.#{field_name}: type #{Macro.to_string(type_ast)} uses " <>
              "#{inspect(ref_module)}.state() but #{inspect(ref_module)} is not a Musubi.Store. " <>
              "Use #{inspect(ref_module)}.t() to reference a Musubi.State, or change " <>
              "#{inspect(ref_module)} to `use Musubi.Store` if it should mount as a child."

      true ->
        :ok
    end
  end

  defp musubi_runtime_loaded?(module) do
    case Code.ensure_loaded(module) do
      {:module, ^module} -> function_exported?(module, :__musubi_runtime_module__, 0)
      _other -> false
    end
  end

  defp musubi_kind(module) do
    if function_exported?(module, :__musubi_kind__, 0) do
      module.__musubi_kind__()
    end
  end

  # ---------------------------------------------------------------------------
  # valid?/2
  # ---------------------------------------------------------------------------

  defp do_valid?(value, {:|, _meta, [left, right]}, host_module),
    do: do_valid?(value, left, host_module) or do_valid?(value, right, host_module)

  defp do_valid?(value, {:list, _meta, [inner]}, host_module) when is_list(value),
    do: Enum.all?(value, &do_valid?(&1, inner, host_module))

  defp do_valid?(_value, {:list, _meta, [_inner]}, _host_module), do: false

  defp do_valid?(value, {:stream, _meta, [_inner]}, _host_module),
    do: Musubi.Stream.Marker.marker?(value)

  defp do_valid?(value, {:map, _meta, []}, _host_module), do: is_map(value)

  defp do_valid?(value, {:%{}, _meta, pairs}, host_module)
       when is_map(value) and is_list(pairs) do
    expected_keys = MapSet.new(pairs, fn {key, _value} -> Atom.to_string(key) end)
    actual_keys = MapSet.new(Map.keys(value), &to_key_string/1)

    MapSet.equal?(expected_keys, actual_keys) and
      Enum.all?(pairs, fn {key, value_type} ->
        case Map.fetch(value, Atom.to_string(key)) do
          {:ok, nested} -> do_valid?(nested, value_type, host_module)
          :error -> false
        end
      end)
  end

  defp do_valid?(_value, {:%{}, _meta, _pairs}, _host_module), do: false

  defp do_valid?(value, {:integer, _meta, []}, _host_module), do: is_integer(value)
  defp do_valid?(value, {:boolean, _meta, []}, _host_module), do: is_boolean(value)

  # `atom()` after wire serialization: nil/true/false stay, every other atom
  # serializes to a binary.
  defp do_valid?(value, {:atom, _meta, []}, _host_module),
    do: is_binary(value) or is_boolean(value) or is_nil(value)

  defp do_valid?(value, nil, _host_module), do: value === nil

  # Literal atom in type AST: matches its wire-form string, except for the
  # JSON-native trio.
  defp do_valid?(value, true, _host_module), do: value === true
  defp do_valid?(value, false, _host_module), do: value === false

  defp do_valid?(value, literal, _host_module) when is_atom(literal),
    do: value === Atom.to_string(literal)

  defp do_valid?(value, literal, _host_module) when is_binary(literal), do: value === literal
  defp do_valid?(value, literal, _host_module) when is_integer(literal), do: value === literal
  defp do_valid?(value, literal, _host_module) when is_float(literal), do: value === literal

  defp do_valid?(value, {{:., _dot, [aliased, kind]}, _call, []}, host_module)
       when kind in [:t, :state] do
    case resolve_alias(aliased, host_module) do
      {:ok, String} -> is_binary(value)
      {:ok, module} -> validate_via_module(module, value)
      :error -> false
    end
  end

  defp do_valid?(value, {{:., _dot, [aliased, :of]}, _call, [inner]}, host_module) do
    async_result_alias?(aliased) and valid_async_result?(value, inner, host_module)
  end

  defp do_valid?(_value, _type_ast, _host_module), do: false

  defp valid_async_result?(
         %{
           "__musubi_async__" => true,
           "status" => status,
           "result" => result,
           "reason" => reason
         },
         inner,
         host_module
       ) do
    case status do
      "loading" ->
        is_nil(reason) and valid_async_result_value?(result, inner, host_module)

      "ok" ->
        is_nil(reason) and do_valid?(result, inner, host_module)

      "failed" ->
        not is_nil(reason) and valid_async_result_value?(result, inner, host_module)

      _other ->
        false
    end
  end

  defp valid_async_result?(_value, _inner, _host_module), do: false

  defp valid_async_result_value?(nil, _inner, _host_module), do: true

  defp valid_async_result_value?(result, inner, host_module),
    do: do_valid?(result, inner, host_module)

  defp validate_via_module(module, value) do
    cond do
      function_exported?(module, :__musubi_validate_state__, 1) ->
        module.__musubi_validate_state__(value) == :ok

      function_exported?(module, :__musubi_validate_input__, 1) ->
        module.__musubi_validate_input__(value) == :ok

      true ->
        false
    end
  end

  defp to_key_string(key) when is_binary(key), do: key
  defp to_key_string(key) when is_atom(key), do: Atom.to_string(key)
  defp to_key_string(key), do: inspect(key)
end
