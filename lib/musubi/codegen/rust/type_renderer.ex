defmodule Musubi.Codegen.Rust.TypeRenderer do
  @moduledoc """
  Pure converter from a single Musubi field-type AST node to its Rust type
  string. Lives separately from `Musubi.Codegen.Rust` so the conversion table
  can be exercised one AST shape at a time, with no bundle assembly, manifest
  discovery, or alias-expansion concerns mixed in.

  All `{:__aliases__, _, parts}` nodes are expected to be already-fully-
  qualified — `Musubi.Codegen.Manifest` resolves consumer aliases at
  `@after_compile` time using the captured `Macro.Env` before serializing.

  Unlike the TypeScript renderer this is not a plain `AST -> String` function:
  Rust is nominal, so every anonymous map and every non-trivial union must
  become a **named** `struct` or `enum` declared somewhere. `render/2`
  therefore threads a context (`t:ctx/0`) that accumulates those hoisted
  declarations plus the per-Rust-module name table, and returns
  `{rendered_reference, ctx}`. Use `render!/2` for the shapes that cannot
  hoist.

  ## Type mapping

  The prelude module is written `musubi` below (the `:rust_codegen_root_module`
  default); it is reached through the depth-correct `super::` prefix, so at
  depth 2 the rendered path is `super::super::musubi::…`.

  | Musubi field type AST            | Rust                                      |
  | :------------------------------ | :---------------------------------------- |
  | `String.t()` / `binary()` / `string()` | `String`                             |
  | `integer()`                     | `i64`                                      |
  | `float()`                       | `f64`                                      |
  | `boolean()` / `true` / `false`  | `bool`                                     |
  | `atom()`                        | `String` (atoms serialize as strings)      |
  | `:literal` (atom literal)       | hoisted single-variant enum                |
  | `"str"` / `1` / `1.0`           | `String` / `i64` / `f64` (value recorded in a `///` note) |
  | `nil`                           | `()`                                       |
  | `map()`                         | `serde_json::Map<String, serde_json::Value>` |
  | `%{key: T}`                     | hoisted struct                             |
  | `list(T)`                       | `Vec<T>`                                   |
  | `stream(T)`                     | `Vec<T>` (the client hydrates the marker)  |
  | `T \\| nil`                     | `Option<T>`                                |
  | `T \\| U`                       | hoisted enum, or `serde_json::Value`       |
  | `Module.t()`                    | `my_app::states::CartState`, or `…::cart_store::State` when the module is a store |
  | `Module.state()`                | `musubi::StoreField<my_app::stores::cart_store::State>` |
  | `Musubi.AsyncResult.of(T)`      | `musubi::AsyncResult<T>`                   |
  | anything unrecognized           | `serde_json::Value`                        |

  ## Navigation types

  Every snapshot type above has a second rendering: the handle a generated
  `Ext` accessor hands back (`docs/rust-reactive-state.md` §4.3, mirrored in
  `docs/rust-codegen.md` §3.2). `nav_type/3` derives it from the same AST node
  plus the already-rendered snapshot type, so the two columns cannot drift:

  | Musubi field type AST      | `Ext` accessor returns    |
  | :------------------------- | :------------------------ |
  | `stream(T)`                | `musubi::StreamState<T'>` |
  | `Module.state()`           | `musubi::StoreState<S>`   |
  | `Musubi.AsyncResult.of(T)` | `musubi::AsyncState<T'>`  |
  | everything else            | `musubi::State<snapshot>` |

  Declared uploads are the fifth shape (`musubi::UploadSlotState`); they are
  not field types, so `Musubi.Codegen.Rust` builds that spec directly.
  """

  alias Musubi.Codegen.Rust.Names

  # Emitted verbatim on every generated struct and enum. `Eq`/`Hash` are
  # omitted because `f64` fields make them impossible in general, and deriving
  # them conditionally would make the output non-uniform.
  @derives "#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]"

  # One level of Rust indentation. The generator emits already-formatted
  # output; `cargo fmt` is never invoked.
  @indent "    "

  # rustfmt's `chain_width` under the default `use_small_heuristics`: 60% of
  # `max_width`. A method chain wider than this goes vertical even when the
  # line itself would fit, so the accessor bodies have to measure against both.
  @chain_width 60

  # Emitted above every generated navigation trait.
  @ext_doc """
  /// Typed navigation for the shape above: one accessor per declared field,
  /// each handing back a handle rather than a value
  /// (`docs/rust-reactive-state.md` §4.2). Reach it through `nav`.
  """

  # rustfmt's `max_width`. A field line that would exceed it is emitted with its
  # outermost generic argument list wrapped, which is what rustfmt does — the
  # bundle has to survive a consumer's `cargo fmt --all --check`
  # (docs/rust-codegen.md §4.1).
  @max_width 100

  # Name of the generated prelude module holding the runtime re-exports.
  @default_root_module "musubi"

  # Total fallback, mirroring the TypeScript renderer's `unknown`: a field type
  # AST can carry operators, locals or `unquote` artifacts that alias expansion
  # deliberately preserves verbatim, so rendering never raises.
  @fallback "serde_json::Value"

  @typedoc """
  One hoisted `struct` / `enum` declaration: its allocated name, its
  already-formatted, unindented Rust source (trailing newline included), and —
  for a struct hoisted out of a *state* field — the navigation trait emitted
  beside it (`nil` for enums, and for every shape that never reaches the state
  tree).
  """
  @type declaration() :: %{
          name: String.t(),
          code: String.t(),
          ext: %{trait: String.t(), blocks: [String.t()]} | nil
        }

  @typedoc """
  One rendered struct field: its Rust ident, the `#[serde(rename)]` it needs (or
  `nil`), its rendered type, and the `///` doc lines above it. State fields also
  carry the navigation half: the wire key the accessor addresses, the handle
  type it returns, and whether reaching that handle needs an `.into()`.
  """
  @type field_spec() :: %{
          :ident => String.t(),
          :rename => String.t() | nil,
          :type => String.t(),
          :docs => [String.t()],
          optional(:skip_none) => boolean(),
          optional(:key) => String.t(),
          optional(:nav) => String.t(),
          optional(:into) => boolean()
        }

  @typedoc """
  Render context. Plain map, so `render/2` stays pure and table-testable.

    * `:root_module` — name of the prelude module (`:rust_codegen_root_module`)
    * `:depth` — `pub mod` nesting of the item being rendered, for `super::`
    * `:prefix` — base name the next hoisted type claims: the enclosing
      generated item's name followed by every path segment walked so far
    * `:claimed` — names already taken in the enclosing Rust module
    * `:hoists` — accumulated declarations, most recent first
    * `:notes` — doc-comment lines the caller must attach to the current field
    * `:nav` — whether what is being rendered reaches the state tree, and so
      whether hoisted structs get a navigation trait beside them
  """
  @type ctx() :: %{
          root_module: String.t(),
          depth: non_neg_integer(),
          prefix: String.t(),
          claimed: MapSet.t(String.t()),
          stores: MapSet.t([String.t()]),
          hoists: [declaration()],
          notes: [String.t()],
          nav: boolean()
        }

  @doc """
  Builds a render context.

  Options:

    * `:root_module` — prelude module name. Defaults to `"musubi"`.
    * `:depth` — how many `pub mod` levels enclose the referencing item.
      Defaults to `0`.
    * `:prefix` — name of the enclosing generated item, used as the hoisted
      name prefix. Defaults to `""`.
    * `:claimed` — names already taken in the enclosing Rust module. Defaults
      to `MapSet.new()`.
    * `:stores` — `Module.split/1` segments (strings) of every `kind: :store` module in
      the bundle. A store is never a bare struct (`docs/rust-codegen.md` §4.2),
      so `Module.t()` on one resolves to its `State` shape instead. Defaults to
      `MapSet.new()`.
    * `:nav` — `true` while rendering *state* fields, so that every struct
      hoisted out of them gets its `Ext` navigation trait
      (`docs/rust-reactive-state.md` §4.3). `Params`, command payloads/replies
      and event payloads never reach the state tree and pass `false`, the
      default.

  ## Examples

      iex> alias Musubi.Codegen.Rust.TypeRenderer
      iex> ctx = TypeRenderer.new(prefix: "CartStateAddress")
      iex> {rendered, ctx} = TypeRenderer.render(quote(do: %{street: String.t()}), ctx)
      iex> rendered
      "CartStateAddress"
      iex> Enum.map(TypeRenderer.declarations(ctx), & &1.name)
      ["CartStateAddress"]
  """
  @spec new() :: ctx()
  @spec new(keyword()) :: ctx()
  def new(opts \\ []) do
    %{
      root_module: Keyword.get(opts, :root_module, @default_root_module),
      depth: Keyword.get(opts, :depth, 0),
      prefix: Keyword.get(opts, :prefix, ""),
      claimed: Keyword.get(opts, :claimed, MapSet.new()),
      stores: Keyword.get(opts, :stores, MapSet.new()),
      hoists: [],
      notes: [],
      nav: Keyword.get(opts, :nav, false)
    }
  end

  @doc """
  Renders a single Musubi field-type AST node as Rust, returning the rendered
  type reference and the context carrying every declaration the reference
  needs.

  ## Examples

      iex> alias Musubi.Codegen.Rust.TypeRenderer
      iex> {rendered, ctx} = TypeRenderer.render(quote(do: list(String.t())), TypeRenderer.new())
      iex> {rendered, TypeRenderer.declarations(ctx)}
      {"Vec<String>", []}
  """
  @spec render(Macro.t(), ctx()) :: {String.t(), ctx()}
  def render(type_ast, ctx), do: do_render(type_ast, ctx)

  @doc """
  Renders `type_ast` and discards the context. Convenience for the shapes that
  hoist nothing — an anonymous map or union rendered this way still returns its
  reference name, but the declaration it needs is dropped.

  ## Examples

      iex> Musubi.Codegen.Rust.TypeRenderer.render!(quote(do: String.t()))
      "String"
      iex> Musubi.Codegen.Rust.TypeRenderer.render!(quote(do: list(String.t())))
      "Vec<String>"
      iex> Musubi.Codegen.Rust.TypeRenderer.render!(quote(do: String.t() | nil))
      "Option<String>"
      iex> Musubi.Codegen.Rust.TypeRenderer.render!(quote(do: stream(String.t())))
      "Vec<String>"
  """
  @spec render!(Macro.t()) :: String.t()
  @spec render!(Macro.t(), ctx()) :: String.t()
  def render!(type_ast, ctx \\ new()) do
    {rendered, _ctx} = do_render(type_ast, ctx)

    rendered
  end

  @doc """
  Returns the handle type a generated `Ext` accessor hands back for one field,
  and whether reaching it needs an `.into()` conversion from the plain
  `State<snapshot>` the `field` primitive yields.

  Derived from the field-type AST plus its already-rendered snapshot type, so
  the navigation column of `docs/rust-reactive-state.md` §4.3 cannot drift from
  the snapshot column. Re-rendering the inner type instead would allocate a
  second hoisted name for the same shape.

  ## Examples

      iex> alias Musubi.Codegen.Rust.TypeRenderer
      iex> ctx = TypeRenderer.new()
      iex> TypeRenderer.nav_type(quote(do: String.t()), "String", ctx)
      {"musubi::State<String>", false}
      iex> TypeRenderer.nav_type(quote(do: stream(String.t())), "Vec<String>", ctx)
      {"musubi::StreamState<String>", true}
  """
  @spec nav_type(Macro.t(), String.t(), ctx()) :: {String.t(), boolean()}
  def nav_type({:stream, _meta, [_inner]}, rendered, ctx),
    do: handle_type("StreamState", rendered, ctx)

  def nav_type({{:., _dot, [aliased, :state]}, _call, []}, rendered, ctx) do
    if alias_segments(aliased),
      do: handle_type("StoreState", rendered, ctx),
      else: {plain_type(rendered, ctx), false}
  end

  def nav_type({{:., _dot, [aliased, :of]}, _call, [_inner]}, rendered, ctx) do
    if async_result_alias?(aliased),
      do: handle_type("AsyncState", rendered, ctx),
      else: {plain_type(rendered, ctx), false}
  end

  def nav_type(_ast, rendered, ctx), do: {plain_type(rendered, ctx), false}

  # The three wrapper shapes all render as `Outer<Inner>`; the handle keeps the
  # inner type and swaps the wrapper. A shape that lost its wrapper on the way
  # to Rust (an unresolvable alias falling back to `serde_json::Value`) has no
  # inner type to lift, so it navigates as an opaque leaf.
  defp handle_type(handle, rendered, ctx) do
    case split_generic(rendered) do
      {_outer, [inner]} -> {"#{prelude(ctx)}::#{handle}<#{inner}>", true}
      _other -> {plain_type(rendered, ctx), false}
    end
  end

  defp plain_type(rendered, ctx), do: "#{prelude(ctx)}::State<#{rendered}>"

  @doc """
  Returns the declarations hoisted into `ctx`, in allocation order (nested
  shapes before the shape that references them). The bundle assembler emits
  them in the enclosing item's module, sorted by name.

  ## Examples

      iex> alias Musubi.Codegen.Rust.TypeRenderer
      iex> {_rendered, ctx} = TypeRenderer.render(quote(do: String.t()), TypeRenderer.new())
      iex> TypeRenderer.declarations(ctx)
      []
  """
  @spec declarations(ctx()) :: [declaration()]
  def declarations(ctx), do: Enum.reverse(ctx.hoists)

  @doc """
  Returns the doc-comment lines the caller must attach to the field it just
  rendered, in emission order. Populated for the union shapes that lose
  information on the way to Rust — an undiscriminated union collapsing to
  `serde_json::Value`, or a union of non-atom literals collapsing to its
  underlying scalar.

  ## Examples

      iex> alias Musubi.Codegen.Rust.TypeRenderer
      iex> {rendered, ctx} = TypeRenderer.render(quote(do: "a" | "b"), TypeRenderer.new())
      iex> {rendered, TypeRenderer.notes(ctx)}
      {"String", [~s(Allowed values: "a" | "b")]}
  """
  @spec notes(ctx()) :: [String.t()]
  def notes(ctx), do: Enum.reverse(ctx.notes)

  # Renders the body of a struct (or of a struct enum variant) from
  # already-built field specs: the `///` doc lines, the `#[serde(rename)]`
  # attribute and the field itself, each prefixed with `indent` and
  # `visibility`.
  #
  # A field whose line would exceed rustfmt's 100-column `max_width` has its
  # outermost generic argument list wrapped the way rustfmt wraps it, so the
  # bundle survives a consumer's `cargo fmt --all --check`
  # (`docs/rust-codegen.md` §4.1). `offset` is the number of columns the bundle
  # assembler will prepend afterwards — every generated item is rendered
  # unindented and shifted once per enclosing `pub mod` — so it counts towards
  # the width without appearing in the emitted prefix.
  @spec field_lines([field_spec()], String.t(), String.t(), non_neg_integer()) :: String.t()
  defp field_lines(specs, indent, visibility, offset) do
    Enum.map_join(specs, "", &field_line(&1, indent, visibility, offset))
  end

  @doc """
  Emits one `#[derive(...)]`-prefixed `pub struct` from already-built field
  specs. `depth` is the enclosing `pub mod` nesting — the block is rendered
  unindented and shifted once per level by the bundle assembler, so the field
  renderer needs it to measure the final line width against rustfmt's
  `max_width`.

  ## Examples

      iex> spec = %{ident: "title", rename: nil, type: "String", docs: []}
      iex> block = Musubi.Codegen.Rust.TypeRenderer.struct_block("State", [spec], 0)
      iex> String.contains?(block, "pub struct State {\\n    pub title: String,\\n}")
      true
  """
  @spec struct_block(String.t(), [field_spec()], non_neg_integer()) :: String.t()
  def struct_block(name, specs, depth) do
    @derives <> "\n" <> struct_body(name, specs, depth * String.length(@indent))
  end

  @doc """
  Emits the navigation surface for one shape: the `pub trait <Name>Ext`
  carrying one accessor per field, followed by one `impl` per target.

  `targets` are `{rust_type, body}` pairs. `:field` bodies navigate through the
  `State::child` primitive (`docs/rust-reactive-state.md` §2.4); `:forward`
  bodies are the `StoreState::fields` forwarding that gives a child store's
  shape its second impl (§4.2). `depth` is the enclosing `pub mod` nesting,
  needed to measure the emitted lines against rustfmt's `max_width`.

  ## Examples

      iex> spec = %{ident: "title", key: "title", nav: "musubi::State<String>", into: false}
      iex> [_trait, impl_block] =
      ...>   Musubi.Codegen.Rust.TypeRenderer.ext_blocks(
      ...>     "CartStateExt",
      ...>     [{"musubi::State<CartState>", :field}],
      ...>     [spec],
      ...>     0
      ...>   )
      iex> String.contains?(impl_block, ~s|self.child("title")|)
      true
  """
  @spec ext_blocks(
          String.t(),
          [{String.t(), :field | :forward}],
          [field_spec()],
          non_neg_integer()
        ) ::
          [String.t()]
  def ext_blocks(trait, targets, specs, depth) do
    offset = depth * String.length(@indent)

    [
      @ext_doc <> block("pub trait #{trait}", trait_body(specs, offset))
      | Enum.map(targets, fn {target, body} ->
          block(impl_header(trait, target, offset), impl_body(specs, {body, trait}, offset))
        end)
    ]
  end

  # rustfmt's two shapes for an `impl` header: all on one line, or the trait
  # alone with `for <type>` indented under it and the brace on its own line.
  defp impl_header(trait, target, offset) do
    one_line = "impl #{trait} for #{target}"

    if line_fits?(one_line <> " {", offset),
      do: one_line,
      else: "impl #{trait}\n#{@indent}for #{target}\n"
  end

  # rustfmt collapses an empty braced item onto one line — unless the header
  # already broke, where the brace pair keeps its own lines.
  defp block(header, body) do
    if String.ends_with?(header, "\n"),
      do: header <> "{\n" <> body <> "}\n",
      else: braced(header, body)
  end

  defp braced(header, ""), do: header <> " {}\n"
  defp braced(header, body), do: header <> " {\n" <> body <> "}\n"

  defp trait_body(specs, offset),
    do: Enum.map_join(specs, "", &signature(&1, @indent, ";", offset))

  defp impl_body(specs, body, offset) do
    Enum.map_join(specs, "", fn spec ->
      signature(spec, @indent, " {", offset) <>
        accessor_body(spec, body, @indent <> @indent, offset) <> @indent <> "}\n"
    end)
  end

  # rustfmt's three shapes for a `fn` header, in the order it tries them: all on
  # one line (the trailing `{` or `;` counted); then the argument list broken
  # with the return type still on the `) -> ` line (the terminator *not*
  # counted — the brace moves rather than the type wrapping); then the return
  # type wrapped by the same generic-argument rule `declaration/4` uses for a
  # struct field.
  defp signature(spec, indent, tail, offset) do
    head = "#{indent}fn #{spec.ident}(&self) -> "
    returns = "#{indent}) -> "

    cond do
      line_fits?(head <> spec.nav <> tail, offset) ->
        head <> spec.nav <> tail <> "\n"

      line_fits?(returns <> spec.nav, offset) ->
        broken_signature(spec, indent) <> returns <> spec.nav <> broken_tail(tail, indent) <> "\n"

      true ->
        broken_signature(spec, indent) <>
          returns <> wrap_generic(spec.nav, indent, offset) <> tail <> "\n"
    end
  end

  # A broken signature whose return type stayed on one line puts the opening
  # brace on a line of its own — unconditionally, not only when the brace would
  # overflow. A wrapped return type does not: its closing `>` is already back at
  # the header's indent, so the brace follows it as it would any other block.
  # A trait declaration's `;` never moves.
  defp broken_tail(" {", indent), do: "\n#{indent}{"
  defp broken_tail(tail, _indent), do: tail

  defp broken_signature(spec, indent),
    do: "#{indent}fn #{spec.ident}(\n#{indent}#{@indent}&self,\n"

  # One accessor body, always a method chain: rustfmt keeps a chain on one line
  # only while it fits `chain_width` (60 under the default
  # `use_small_heuristics`) *and* `max_width`; otherwise every link but the
  # first goes on its own line.
  defp accessor_body(spec, body, indent, offset) do
    [head | rest] = chain(spec, body)
    width = offset + String.length(indent)

    if String.length(Enum.join([head | rest])) <= min(@max_width - width, @chain_width) do
      indent <> Enum.join([head | rest]) <> "\n"
    else
      indent <> head <> "\n" <> Enum.map_join(rest, "", &"#{indent}#{@indent}#{&1}\n")
    end
  end

  # Called through the trait by name, not as a method: a declared field may be
  # named after one of `State`'s own inherent methods (`child`, `value`, `at`,
  # `node`, ...), and an inherent method wins method resolution outright — the
  # forwarding body would then call the primitive instead of the accessor it is
  # forwarding to, and fail on arity.
  defp chain(spec, {:forward, trait}), do: ["#{trait}::#{spec.ident}(&self.fields())"]

  # `State::child` is infallible: a key the render does not carry — a root that
  # is still `Null` before the first patch, or one teardown has emptied — yields
  # a handle that reads as gone, never a panic. Navigation is the zero-cost half
  # of the handle/value split (docs/rust-reactive-state.md §2.4); `value()` is
  # where a contract violation is allowed to be loud.
  defp chain(spec, {:field, _trait}) do
    ["self.child(\"#{spec.key}\")"] ++ if spec.into, do: [".into()"], else: []
  end

  @doc """
  One level of Rust indentation. The generator emits already-formatted output;
  `cargo fmt` is never invoked.

  ## Examples

      iex> Musubi.Codegen.Rust.TypeRenderer.indent()
      "    "
  """
  @spec indent() :: String.t()
  def indent, do: @indent

  @doc """
  rustfmt's `max_width`. Owned here so the bundle assembler measures its own
  lines — the prelude's `use` tree — against the same number the field renderer
  uses.

  ## Examples

      iex> Musubi.Codegen.Rust.TypeRenderer.max_width()
      100
  """
  @spec max_width() :: pos_integer()
  def max_width, do: @max_width

  defp do_render({:|, _meta, [_left, _right]} = union, ctx), do: render_union(union, ctx)

  defp do_render({:list, _meta, [inner]}, ctx), do: wrap("Vec", inner, ctx)

  # `stream(T)` is a plain `Vec<T>`: the client runtime's hydration pass
  # substitutes the materialized array for the `{"__musubi_stream__": …}`
  # marker before serde runs, so no marker type reaches the generated struct.
  defp do_render({:stream, _meta, [inner]}, ctx), do: wrap("Vec", inner, ctx)

  # `serde_json::Map` rather than `HashMap` so key order round-trips under
  # `serde_json/preserve_order`, and so the type reads as "arbitrary JSON
  # object".
  defp do_render({:map, _meta, []}, ctx), do: {"serde_json::Map<String, serde_json::Value>", ctx}

  defp do_render({:string, _meta, []}, ctx), do: {"String", ctx}
  defp do_render({:binary, _meta, []}, ctx), do: {"String", ctx}
  defp do_render({:integer, _meta, []}, ctx), do: {"i64", ctx}
  defp do_render({:float, _meta, []}, ctx), do: {"f64", ctx}
  defp do_render({:boolean, _meta, []}, ctx), do: {"bool", ctx}
  defp do_render({:atom, _meta, []}, ctx), do: {"String", ctx}

  # `String.t()` shortcut — must precede both the generic `Module.t()` clause
  # and the `%{}` literal-map clause, whose 3-tuple shape would otherwise
  # capture this AST with `pairs = []`.
  defp do_render({{:., _dot, [{:__aliases__, _meta, [:String]}, :t]}, _call, []}, ctx),
    do: {"String", ctx}

  # `Musubi.AsyncResult.of(T)` — resolves the inner T recursively. The wrapper
  # is name-transparent, so it contributes no hoisted-name segment.
  defp do_render({{:., _dot, [aliased, :of]}, _call, [inner]}, ctx) do
    if async_result_alias?(aliased) do
      wrap(prelude(ctx) <> "::AsyncResult", inner, ctx)
    else
      {@fallback, ctx}
    end
  end

  # `Module.state()` — mounted child store. `StoreField<S>` carries
  # `__musubi_store_id__` and flattens the child's own fields.
  defp do_render({{:., _dot, [aliased, :state]}, _call, []}, ctx) do
    case alias_segments(aliased) do
      nil ->
        {@fallback, ctx}

      segments ->
        store_state = qualify(ctx, Names.module_path(segments)) <> "::State"

        {"#{prelude(ctx)}::StoreField<#{store_state}>", ctx}
    end
  end

  # `Module.t()` — a sibling module's generated shape. A `kind: :state` module
  # is a bare struct in its parent module; a `kind: :store` module is a
  # `pub mod` whose shape lives at `::State` (§4.2), so there is no bare struct
  # to point at.
  defp do_render({{:., _dot, [aliased, :t]}, _call, []}, ctx) do
    case alias_segments(aliased) do
      nil -> {@fallback, ctx}
      segments -> {qualify(ctx, shape_path(segments, ctx)), ctx}
    end
  end

  defp do_render({:%{}, _meta, pairs}, ctx) when is_list(pairs), do: hoist_struct(pairs, ctx)

  defp do_render(nil, ctx), do: {"()", ctx}
  defp do_render(true, ctx), do: {"bool", ctx}
  defp do_render(false, ctx), do: {"bool", ctx}

  defp do_render(literal, ctx) when is_atom(literal), do: hoist_atom_enum([literal], ctx)

  defp do_render(literal, ctx) when is_binary(literal),
    do: {"String", note_values(ctx, [literal])}

  defp do_render(literal, ctx) when is_integer(literal), do: {"i64", note_values(ctx, [literal])}
  defp do_render(literal, ctx) when is_float(literal), do: {"f64", note_values(ctx, [literal])}

  defp do_render(_other, ctx), do: {@fallback, ctx}

  defp wrap(outer, inner, ctx) do
    {rendered, ctx} = do_render(inner, ctx)

    {"#{outer}<#{rendered}>", ctx}
  end

  # §3.4, in order: flatten, strip `nil` into `Option`, then classify what is
  # left.
  defp render_union(union, ctx) do
    {nilable?, arms} = union |> flatten_union() |> strip_nil()
    {rendered, ctx} = render_arms(arms, ctx)

    cond do
      arms == [] -> {"()", ctx}
      nilable? -> {"Option<#{rendered}>", ctx}
      true -> {rendered, ctx}
    end
  end

  defp flatten_union({:|, _meta, [left, right]}), do: flatten_union(left) ++ flatten_union(right)
  defp flatten_union(arm), do: [arm]

  defp strip_nil(arms), do: {Enum.any?(arms, &is_nil/1), Enum.reject(arms, &is_nil/1)}

  defp render_arms([], ctx), do: {"()", ctx}
  defp render_arms([arm], ctx), do: do_render(arm, ctx)

  defp render_arms(arms, ctx) do
    if Enum.all?(arms, &atom_literal?/1) do
      hoist_atom_enum(arms, ctx)
    else
      render_wide_arms(arms, discriminant(arms), ctx)
    end
  end

  defp render_wide_arms(arms, nil, ctx) do
    cond do
      Enum.all?(arms, &is_binary/1) -> {"String", note_values(ctx, arms)}
      Enum.all?(arms, &is_integer/1) -> {"i64", note_values(ctx, arms)}
      Enum.all?(arms, &is_float/1) -> {"f64", note_values(ctx, arms)}
      true -> {@fallback, note(ctx, "Declared arms: " <> arms_source(arms))}
    end
  end

  defp render_wide_arms(arms, tag, ctx), do: hoist_tagged_enum(arms, tag, ctx)

  defp atom_literal?(arm), do: is_atom(arm) and arm not in [nil, true, false]

  # The discriminant is the first key of the first arm that is present in every
  # arm with a distinct atom-literal value. `nil` when the arms are not all
  # maps, or no such key exists.
  defp discriminant(arms) do
    arm_pairs = Enum.map(arms, &map_pairs/1)

    if Enum.all?(arm_pairs, &is_list/1), do: first_discriminant(arm_pairs)
  end

  defp map_pairs({:%{}, _meta, pairs}) when is_list(pairs), do: pairs
  defp map_pairs(_other), do: nil

  defp first_discriminant([first | _rest] = arm_pairs) do
    Enum.find_value(first, fn {key, _value} -> if discriminant?(arm_pairs, key), do: key end)
  end

  defp discriminant?(arm_pairs, key) do
    values = Enum.map(arm_pairs, &pair_value(&1, key))

    Enum.all?(values, &atom_literal?/1) and Enum.uniq(values) == values
  end

  defp pair_value(pairs, key) do
    case List.keyfind(pairs, key, 0) do
      {^key, value} -> value
      nil -> nil
    end
  end

  defp note_values(ctx, arms), do: note(ctx, "Allowed values: " <> arms_source(arms))

  defp arms_source(arms), do: Enum.map_join(arms, " | ", &Macro.to_string/1)

  defp note(ctx, text), do: %{ctx | notes: [text | ctx.notes]}

  defp hoist_struct(pairs, ctx) do
    {name, ctx} = claim(ctx)
    {specs, ctx} = render_field_specs(pairs, ctx)
    code = @derives <> "\n" <> struct_body(name, specs, offset(ctx))
    {ext, ctx} = hoist_ext(name, specs, ctx)

    {name, push_hoist(ctx, name, code, ext)}
  end

  # A struct hoisted out of a state field is a node of the state tree, so it
  # gets the same `<Name>Ext` navigation trait a named shape does
  # (`docs/rust-reactive-state.md` §4.3). The trait name is allocated from the
  # same table as the struct's, so the append-`2` strategy covers the
  # (unreachable) case of a shape already holding the name.
  defp hoist_ext(_name, _specs, %{nav: false} = ctx), do: {nil, ctx}

  defp hoist_ext(name, specs, ctx) do
    {trait, claimed} = Names.allocate(name <> "Ext", ctx.claimed)
    target = "#{prelude(ctx)}::State<#{name}>"
    blocks = ext_blocks(trait, [{target, :field}], specs, ctx.depth)

    {%{trait: trait, blocks: blocks}, %{ctx | claimed: claimed}}
  end

  defp struct_body(name, [], _offset), do: "pub struct #{name} {}\n"

  defp struct_body(name, specs, offset) do
    "pub struct #{name} {\n" <> field_lines(specs, @indent, "pub ", offset) <> "}\n"
  end

  # Unions are leaves (`docs/rust-reactive-state.md` §4.3): Rust cannot
  # navigate reactively *into* a variant, so a hoisted enum gets no `Ext`.
  defp hoist_atom_enum(atoms, ctx) do
    {name, ctx} = claim(ctx)

    variants =
      Enum.map_join(atoms, "", fn atom ->
        {variant, wire} = Names.variant_ident(atom)

        rename_line(wire, @indent) <> "#{@indent}#{variant},\n"
      end)

    code = @derives <> "\npub enum #{name} {\n" <> variants <> "}\n"

    {name, push_hoist(ctx, name, code)}
  end

  # Internally tagged (`#[serde(tag = ...)]`), never adjacently tagged: the
  # wire shape is `{"type": "paused", "value": 3}`, the tag a sibling key of
  # the payload. Struct variants are inlined so consumers can
  # `match st { …::Paused { value } => … }`.
  defp hoist_tagged_enum(arms, tag, ctx) do
    {name, ctx} = claim(ctx)
    {variants, ctx} = Enum.map_reduce(arms, ctx, &variant(&1, tag, &2))

    code =
      @derives <>
        "\n#[serde(tag = \"#{tag}\")]\npub enum #{name} {\n" <> Enum.join(variants) <> "}\n"

    {name, push_hoist(ctx, name, code)}
  end

  # A variant's payload is inside the leaf, so nothing under it is navigable —
  # `nav: false` keeps a struct hoisted out of an arm from claiming an `Ext`
  # trait no accessor could ever reach.
  defp variant({:%{}, _meta, pairs}, tag, ctx) do
    {variant, wire} = pairs |> pair_value(tag) |> Names.variant_ident()
    payload = List.keydelete(pairs, tag, 0)
    {specs, inner_ctx} = render_field_specs(payload, %{descend(ctx, wire) | nav: false})
    line = rename_line(wire, @indent) <> "#{@indent}#{variant}#{variant_body(specs, ctx)},\n"

    {line, %{inner_ctx | prefix: ctx.prefix, nav: ctx.nav}}
  end

  defp variant_body([], _ctx), do: ""

  defp variant_body(specs, ctx) do
    if Enum.any?(specs, &attributed?/1) do
      " {\n" <>
        field_lines(specs, @indent <> @indent, "", offset(ctx)) <> @indent <> "}"
    else
      " { " <> Enum.map_join(specs, ", ", &"#{&1.ident}: #{&1.type}") <> " }"
    end
  end

  # Columns the bundle assembler prepends to every line of a hoisted
  # declaration: one indent level per enclosing `pub mod`.
  defp offset(ctx), do: ctx.depth * String.length(@indent)

  defp attributed?(spec),
    do: spec.rename != nil or spec.docs != [] or Map.get(spec, :skip_none, false)

  # Every field descends from the *enclosing* prefix, so a sibling field never
  # leaks into the next one's hoisted name; only `:claimed` and `:hoists` carry
  # across.
  defp render_field_specs(pairs, ctx) do
    Enum.map_reduce(pairs, ctx, fn {key, value}, acc ->
      {ident, rename} = Names.field_ident(key)
      {rendered, field_ctx} = do_render(value, %{descend(acc, key) | notes: []})
      {nav, into?} = nav_type(value, rendered, acc)

      spec = %{
        ident: ident,
        rename: rename,
        type: rendered,
        docs: notes(field_ctx),
        key: to_string(key),
        nav: nav,
        into: into?
      }

      {spec, %{field_ctx | notes: acc.notes, prefix: acc.prefix}}
    end)
  end

  # Walks one segment deeper into the hoisted-name scheme. Wrappers never call
  # this — `list`, `stream`, `AsyncResult.of` and `Option` are name-transparent.
  defp descend(ctx, segment), do: %{ctx | prefix: ctx.prefix <> Names.pascal_case(segment)}

  defp field_line(spec, indent, visibility, offset) do
    doc_lines(spec.docs, indent) <>
      rename_line(spec.rename, indent) <>
      skip_none_line(spec, indent) <> declaration(spec, indent, visibility, offset)
  end

  # `Musubi.Codegen.Rust`'s optional mount attrs must serialize as an absent
  # key rather than an explicit `null`.
  defp skip_none_line(%{skip_none: true}, indent),
    do: "#{indent}#[serde(skip_serializing_if = \"Option::is_none\")]\n"

  defp skip_none_line(_spec, _indent), do: ""

  # rustfmt's three shapes for one field, in the order it tries them: all on one
  # line; the type alone on the next line at `indent + 4`; the outermost generic
  # argument list wrapped.
  defp declaration(spec, indent, visibility, offset) do
    head = "#{indent}#{visibility}#{spec.ident}: "
    inner = indent <> @indent

    cond do
      fits?(spec.type, offset + String.length(head)) ->
        head <> spec.type <> ",\n"

      fits?(spec.type, offset + String.length(inner)) ->
        String.trim_trailing(head) <> "\n" <> inner <> spec.type <> ",\n"

      true ->
        head <> wrap_generic(spec.type, indent, offset) <> ",\n"
    end
  end

  defp fits?(type, prefix_width), do: prefix_width + String.length(type) + 1 <= @max_width

  # The same measurement for a line that is already complete — a `fn` header
  # carries its own terminator, so nothing is added for a trailing comma.
  defp line_fits?(line, offset), do: offset + String.length(line) <= @max_width

  # Reproduces rustfmt's wrapping of a too-wide generic argument list:
  # `Outer<` on the field line, one argument per line at `indent + 4` with a
  # trailing comma, `>` back at `indent`. Recurses into an argument that still
  # does not fit; a type with no generic arguments is left alone, exactly as
  # rustfmt leaves a long plain path.
  defp wrap_generic(type, indent, offset) do
    case split_generic(type) do
      nil -> type
      {outer, args} -> outer <> "<\n" <> wrapped_args(args, indent, offset) <> indent <> ">"
    end
  end

  defp wrapped_args(args, indent, offset) do
    inner = indent <> @indent
    width = offset + String.length(inner)

    Enum.map_join(args, "", fn arg ->
      inner <> wrap_arg(arg, inner, width, offset) <> ",\n"
    end)
  end

  defp wrap_arg(arg, indent, width, offset) do
    if fits?(arg, width), do: arg, else: wrap_generic(arg, indent, offset)
  end

  defp split_generic(type) do
    with true <- String.ends_with?(type, ">"),
         [outer, rest] <- String.split(type, "<", parts: 2) do
      {outer, rest |> String.slice(0..-2//1) |> split_args()}
    else
      _other -> nil
    end
  end

  # Splits a generic argument list on its top-level commas only, so
  # `Map<String, Vec<A, B>>` yields `["String", "Vec<A, B>"]`.
  defp split_args(inner) do
    {args, last, _depth} =
      inner
      |> String.graphemes()
      |> Enum.reduce({[], "", 0}, fn
        ",", {args, current, 0} -> {[current | args], "", 0}
        "<", {args, current, depth} -> {args, current <> "<", depth + 1}
        ">", {args, current, depth} -> {args, current <> ">", depth - 1}
        char, {args, current, depth} -> {args, current <> char, depth}
      end)

    [last | args] |> Enum.reverse() |> Enum.map(&String.trim/1)
  end

  defp doc_lines(docs, indent), do: Enum.map_join(docs, "", &"#{indent}/// #{&1}\n")

  defp rename_line(nil, _indent), do: ""
  defp rename_line(wire, indent), do: "#{indent}#[serde(rename = \"#{wire}\")]\n"

  defp claim(ctx) do
    {name, claimed} = Names.allocate(ctx.prefix, ctx.claimed)

    {name, %{ctx | claimed: claimed}}
  end

  defp push_hoist(ctx, name, code, ext \\ nil),
    do: %{ctx | hoists: [%{name: name, code: code, ext: ext} | ctx.hoists]}

  # Cross-module references are `super::`-chained from the referencing module
  # up to the file root, prost-style, so the bundle never names its own crate.
  # `alias_segments/1` yields atoms for an `{:__aliases__, _, _}` node and
  # strings for a resolved module atom, so both sides are stringified before
  # the lookup.
  defp shape_path(segments, ctx) do
    if MapSet.member?(ctx.stores, Enum.map(segments, &to_string/1)) do
      Names.module_path(segments) <> "::State"
    else
      Names.struct_path(segments)
    end
  end

  defp qualify(ctx, path), do: String.duplicate("super::", ctx.depth) <> path

  defp prelude(ctx), do: qualify(ctx, ctx.root_module)

  defp alias_segments({:__aliases__, _meta, parts}) when is_list(parts) do
    if Enum.all?(parts, &is_atom/1), do: parts
  end

  # `Module.split/1` raises on an Erlang module (`:queue.t()`), and §3.2's
  # fallback is total, so anything that is not an `Elixir.`-prefixed atom falls
  # through to `serde_json::Value`.
  defp alias_segments(module) when is_atom(module) and not is_nil(module) do
    if match?("Elixir." <> _rest, Atom.to_string(module)), do: Module.split(module)
  end

  defp alias_segments(_other), do: nil

  defp async_result_alias?({:__aliases__, _meta, [:Musubi, :AsyncResult]}), do: true
  defp async_result_alias?(Musubi.AsyncResult), do: true
  defp async_result_alias?(_other), do: false
end
