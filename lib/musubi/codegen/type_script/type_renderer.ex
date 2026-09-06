defmodule Musubi.Codegen.TypeScript.TypeRenderer do
  @moduledoc """
  Pure converter from a single Musubi field-type AST node to its TypeScript
  string form. Lives separately from `Musubi.Codegen.TypeScript` so the
  conversion table can be exercised one AST shape at a time, with no bundle
  assembly, manifest discovery, or alias-expansion concerns mixed in.

  All `{:__aliases__, _, parts}` nodes are expected to be already-fully-
  qualified — `Musubi.Codegen.Manifest` resolves consumer aliases
  at `@after_compile` time using the captured `Macro.Env` before serializing.

  ## Type mapping

  | Musubi field type AST            | TypeScript                              |
  | :------------------------------ | :-------------------------------------- |
  | `String.t()` / `binary()`       | `string`                                |
  | `integer()` / `float()`         | `number`                                |
  | `boolean()`                     | `boolean`                               |
  | `atom()`                        | `string` (atoms serialize as strings)   |
  | `:literal` (atom literal)       | `"literal"`                             |
  | `nil`                           | `null`                                  |
  | `map()`                         | `Record<string, unknown>`               |
  | `%{key: T}`                     | `{ key: T }` (literal-keyed map)        |
  | `list(T)` / `[T]`               | `T[]`                                   |
  | `stream(T)`                     | `Musubi.StreamField<T>`                  |
  | `T \\| U`                       | `T \| U`                                |
  | `Module.t()`                    | full Elixir alias path                  |
  | `Module.state()`                | `Musubi.StoreField<"Full.Module">`       |
  | `Musubi.AsyncResult.of(T)`       | `Musubi.AsyncField<T>`                   |
  """

  @doc """
  Renders a single Musubi field-type AST node as TypeScript.

  Options:
    * `:root_namespace` — namespace prefix used for marker types
      (`StoreField`, `StreamField`, `AsyncField`). Defaults to `"Musubi"`.

  ## Examples

      iex> Musubi.Codegen.TypeScript.TypeRenderer.render(quote(do: String.t()))
      "string"
      iex> Musubi.Codegen.TypeScript.TypeRenderer.render(quote(do: list(String.t())))
      "string[]"
      iex> Musubi.Codegen.TypeScript.TypeRenderer.render(quote(do: stream(String.t())))
      "Musubi.StreamField<string>"
      iex> Musubi.Codegen.TypeScript.TypeRenderer.render(quote(do: String.t() | nil))
      "string | null"
  """
  @spec render(Macro.t()) :: String.t()
  @spec render(Macro.t(), keyword()) :: String.t()
  def render(type_ast, opts \\ []) do
    root = Keyword.get(opts, :root_namespace, "Musubi")
    do_render(type_ast, root)
  end

  defp do_render({:|, _meta, [left, right]}, root) do
    do_render(left, root) <> " | " <> do_render(right, root)
  end

  defp do_render({:list, _meta, [inner]}, root), do: wrap_array(do_render(inner, root))

  defp do_render({:stream, _meta, [inner]}, root),
    do: "#{root}.StreamField<#{do_render(inner, root)}>"

  defp do_render({:map, _meta, []}, _root), do: "Record<string, unknown>"

  defp do_render({:string, _meta, []}, _root), do: "string"
  defp do_render({:binary, _meta, []}, _root), do: "string"
  defp do_render({:integer, _meta, []}, _root), do: "number"
  defp do_render({:float, _meta, []}, _root), do: "number"
  defp do_render({:boolean, _meta, []}, _root), do: "boolean"
  defp do_render({:atom, _meta, []}, _root), do: "string"

  # `String.t()` shortcut — must precede the generic literal-map clause whose
  # 3-tuple shape would otherwise capture this AST with `pairs=[]`.
  defp do_render({{:., _dot, [{:__aliases__, _meta, [:String]}, :t]}, _call, []}, _root),
    do: "string"

  # `Musubi.AsyncResult.of(T)` — resolves the inner T recursively.
  defp do_render({{:., _dot, [aliased, :of]}, _call, [inner]}, root) do
    if async_result_alias?(aliased) do
      "#{root}.AsyncField<#{do_render(inner, root)}>"
    else
      "unknown"
    end
  end

  # `Module.state()` — mounted child store marker.
  defp do_render({{:., _dot, [aliased, :state]}, _call, []}, root) do
    "#{root}.StoreField<#{inspect(full_alias_path(aliased))}>"
  end

  # `Module.t()` — bare alias path. TS namespace lookup resolves it from the
  # call site's namespace.
  defp do_render({{:., _dot, [aliased, :t]}, _call, []}, _root) do
    full_alias_path(aliased)
  end

  defp do_render({:%{}, _meta, pairs}, root) when is_list(pairs) do
    body =
      Enum.map_join(pairs, "; ", fn {key, value} ->
        "#{render_key(key)}: #{do_render(value, root)}"
      end)

    "{ " <> body <> " }"
  end

  defp do_render(nil, _root), do: "null"
  defp do_render(true, _root), do: "true"
  defp do_render(false, _root), do: "false"

  defp do_render(literal, _root) when is_atom(literal), do: inspect(Atom.to_string(literal))
  defp do_render(literal, _root) when is_binary(literal), do: inspect(literal)
  defp do_render(literal, _root) when is_integer(literal), do: Integer.to_string(literal)
  defp do_render(literal, _root) when is_float(literal), do: Float.to_string(literal)

  defp do_render(_other, _root), do: "unknown"

  defp wrap_array(rendered) do
    if String.contains?(rendered, " | ") or String.contains?(rendered, " & ") do
      "Array<#{rendered}>"
    else
      rendered <> "[]"
    end
  end

  defp render_key(key) when is_atom(key), do: Atom.to_string(key)
  defp render_key(key) when is_binary(key), do: inspect(key)

  defp full_alias_path({:__aliases__, _meta, parts}) when is_list(parts) do
    Enum.map_join(parts, ".", &Atom.to_string/1)
  end

  defp full_alias_path(module) when is_atom(module) do
    module |> Module.split() |> Enum.join(".")
  end

  defp async_result_alias?({:__aliases__, _meta, [:Musubi, :AsyncResult]}), do: true
  defp async_result_alias?(Musubi.AsyncResult), do: true
  defp async_result_alias?(_other), do: false
end
