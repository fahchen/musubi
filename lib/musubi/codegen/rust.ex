defmodule Musubi.Codegen.Rust do
  @moduledoc """
  Rust codegen for every Musubi `state do` module exposed by the current Mix
  project. Emits a single `.rs` bundle from the same per-module manifest the
  TypeScript target consumes.

  ## Output shape

  One file (`priv/codegen/rust/musubi.rs` by default) containing:

    * a header comment plus the `#![allow(...)]` inner attribute, so the
      bundle must be included as a module file (`mod generated;`) or through
      `include!`
    * a prelude `pub mod musubi` (name configurable via
      `config :musubi, :rust_codegen_root_module`) holding nothing but
      `pub use` re-exports of the client crate's shared runtime types
      (`config :musubi, :rust_codegen_runtime_path`)
    * the module tree, sorted by Elixir module segment. A `kind: :state`
      module becomes a `pub struct` in its parent `pub mod`; a `kind: :store`
      module always gets its own `pub mod` holding the zero-sized marker
      struct, its `Store` impl, the `State` shape struct, the `Params` mount
      struct, one struct per command payload / reply, one payload struct per
      push event, and every type hoisted out of those
    * beside every shape that reaches the state tree, its navigation trait —
      `pub trait <Name>Ext` plus the `impl` on `State<Shape>` (and, for a
      store's shape, a second `impl` on `StoreState<Shape>`)
      (`docs/rust-reactive-state.md` §4.2)
    * a trailing `pub mod nav` re-exporting every one of those traits flat, so
      a consumer writes `use <bundle>::nav::*;` once per file

  Rust is nominal, so anonymous maps and non-trivial unions cannot be written
  inline: `Musubi.Codegen.Rust.TypeRenderer` hoists them into named `struct` /
  `enum` declarations which this module places in the enclosing item's Rust
  module, immediately after that item, sorted by name.

  Cross-module references are `super::`-chained from the referencing module
  (prost-style) so the bundle never has to know the crate it is mounted in.
  """

  alias Musubi.Codegen.Manifest
  alias Musubi.Codegen.Rust.Names
  alias Musubi.Codegen.Rust.TypeRenderer

  # Name of the generated prelude module — a sibling of the module tree, not a
  # wrapper around it.
  @default_root_module "musubi"

  # Rust path of the client crate owning the shared runtime types. Retargetable
  # so a consumer re-exporting the crate under another name can point at it.
  @default_runtime_path "musubi_client"

  # The re-export list is normative (docs/rust-codegen.md §4.5, extended by
  # docs/rust-reactive-state.md §4.1 with the seven state-tree names) and
  # mirrored verbatim in docs/rust-client.md §8.2. Emitted as one `use` tree
  # filled to rustfmt's `max_width` at the one indent level it always lands on.
  @runtime_types ~w(
    AsyncError AsyncResult AsyncState Command Event NoReply State StateTree
    Store StoreField StoreId StoreState StreamState Subscription UploadSlot
    UploadSlotState
  )

  # Bundle-level module re-exporting every generated navigation trait, so a
  # consumer writes `use generated::nav::*;` once per file rather than
  # importing one trait per shape (docs/rust-reactive-state.md §4.2).
  @nav_module "nav"

  @nav_comment """
  // Navigation traits, flat. One `use <bundle>::nav::*;` per consumer file
  // brings every generated accessor into scope.
  """

  # One level of Rust indentation, owned by the type renderer so the two
  # emitters cannot drift.
  @indent TypeRenderer.indent()

  # rustfmt's `max_width`, owned by the type renderer for the same reason.
  @max_width TypeRenderer.max_width()

  @prelude_comment """
  // Prelude: re-exports only. The shared runtime types are owned by the
  // client crate (`:rust_codegen_runtime_path`, default `musubi_client`).
  """

  @typedoc """
  An entry produced by `Musubi.Codegen.Manifest.list/1` and consumed by
  `render/1`. Pre-loaded reflection data — `render/1` performs no module
  callback lookups and no alias resolution itself.
  """
  @type entry() :: Manifest.entry()

  # Rendering options for one generated item: the `pub mod` depth of the module
  # holding it, the prelude module name, whether `doc:` field options render as
  # `///` comments (command and event fields only, mirroring the TS target's
  # asymmetry), and whether the item reaches the state tree and so takes a
  # navigation surface.
  @typep item_opts() :: %{
           depth: non_neg_integer(),
           root_module: String.t(),
           stores: MapSet.t([String.t()]),
           docs?: boolean(),
           nav?: boolean()
         }

  @doc """
  Renders one Rust bundle covering every `{module, data}` entry in `entries`.
  Returns the rendered source string. Raises `ArgumentError` when two modules
  underscore to the same Rust module path, when two siblings of the emitted
  module tree do, when a module segment underscores to a Rust keyword that
  cannot be a raw identifier, when a top-level module (or the configured
  prelude) takes the `nav` module's name, or when an upload name collides with
  a state field.

  Options:

    * `:root_module` — prelude module name. Defaults to
      `configured_root_module/0`.
    * `:runtime_path` — Rust path of the crate owning the runtime types.
      Defaults to `configured_runtime_path/0`.

  ## Examples

      entries = Musubi.Codegen.Manifest.list()
      Musubi.Codegen.Rust.render(entries, root_module: "musubi")
      #=> "// Generated by `mix compile.musubi_rust`. Do not edit by hand.\\n..."
  """
  @spec render([entry()]) :: String.t()
  @spec render([entry()], keyword()) :: String.t()
  def render(entries, opts \\ []) when is_list(entries) do
    root = Keyword.get(opts, :root_module, configured_root_module())
    runtime = Keyword.get(opts, :runtime_path, configured_runtime_path())

    entries =
      entries
      |> Enum.uniq_by(fn {module, _data} -> module end)
      |> Enum.sort_by(fn {module, _data} -> Module.split(module) end)
      |> Enum.map(fn {module, data} -> {module, normalize(data)} end)
      |> validate_no_module_path_collisions!()

    tree =
      entries
      |> tree()
      |> validate_no_sibling_module_collisions!("")
      |> validate_no_nav_module_collision!(root)

    bundle = %{
      root: root,
      stores: store_segments(entries),
      prelude_segment: prelude_segment(tree, root),
      prelude_item: prelude_item(runtime)
    }

    {blocks, nav} = prelude_and_tree(tree, bundle)

    Enum.join([header() | blocks] ++ nav_block(nav), "\n")
  end

  @doc """
  Returns the configured prelude module name, falling back to `"musubi"`.

  ## Examples

      iex> Musubi.Codegen.Rust.configured_root_module()
      "musubi"
  """
  @spec configured_root_module() :: String.t()
  def configured_root_module do
    Application.get_env(:musubi, :rust_codegen_root_module, @default_root_module)
  end

  @doc """
  Returns the configured Rust path of the crate owning the shared runtime
  types, falling back to `"musubi_client"`.

  ## Examples

      iex> Musubi.Codegen.Rust.configured_runtime_path()
      "musubi_client"
  """
  @spec configured_runtime_path() :: String.t()
  def configured_runtime_path do
    Application.get_env(:musubi, :rust_codegen_runtime_path, @default_runtime_path)
  end

  defp validate_no_module_path_collisions!(entries) do
    collisions =
      entries
      |> Enum.group_by(fn {module, _data} -> Names.module_path(module) end, &elem(&1, 0))
      |> Enum.filter(fn {_path, modules} -> length(modules) > 1 end)

    case collisions do
      [] ->
        entries

      [{path, modules} | _rest] ->
        raise ArgumentError,
              "Musubi Rust codegen: #{inspect(modules)} collide on the rust module path " <>
                "#{inspect(path)}; rename one before the generator can emit the bundle"
    end
  end

  # The full-path check above only sees the *entry* modules. The emitted
  # structure is a per-level `pub mod` tree, so a collision between one
  # module's intermediate namespace segment and another module's own segment
  # (`MyApp.Foo.Bar` vs `MyApp.FOO`) never appears as a full-path duplicate and
  # would emit two `pub mod foo` items at one level — `error[E0428]` in a file
  # the consumer did not write (docs/rust-codegen.md §4.2).
  defp validate_no_sibling_module_collisions!(tree, prefix) do
    tree
    |> Enum.filter(fn {_segment, {children, leaf}} -> emits_module?(children, leaf) end)
    |> Enum.group_by(fn {segment, _node} -> Names.mod_ident(segment) end, &elem(&1, 0))
    |> Enum.sort()
    |> Enum.each(&raise_on_collision!(&1, prefix))

    tree
    |> Enum.filter(fn {_segment, {children, _leaf}} -> map_size(children) > 0 end)
    |> Enum.each(fn {segment, {children, _leaf}} ->
      validate_no_sibling_module_collisions!(children, prefix <> Names.mod_ident(segment) <> "::")
    end)

    tree
  end

  # The `nav` module is a third top-level item the bundle owns outright, next
  # to the prelude and the module tree. Unlike the prelude it cannot be merged
  # into a generated module of the same name — its contents are re-exports of
  # items discovered *during* the tree walk — so a collision is a hard error,
  # like the reserved module names in `Names.mod_ident/1`.
  defp validate_no_nav_module_collision!(_tree, @nav_module) do
    raise ArgumentError,
          "Musubi Rust codegen: :rust_codegen_root_module is #{inspect(@nav_module)}, the " <>
            "name the bundle's navigation re-export module already takes; configure another"
  end

  defp validate_no_nav_module_collision!(tree, _root) do
    colliding =
      Enum.find(tree, fn {segment, {children, leaf}} ->
        emits_module?(children, leaf) and Names.mod_ident(segment) == @nav_module
      end)

    case colliding do
      nil ->
        tree

      {segment, _node} ->
        raise ArgumentError,
              "Musubi Rust codegen: top-level module #{inspect(to_string(segment))} becomes " <>
                "#{inspect(@nav_module)}, the bundle's navigation re-export module; rename " <>
                "the Elixir module"
    end
  end

  defp raise_on_collision!({_ident, [_only]}, _prefix), do: :ok

  defp raise_on_collision!({ident, segments}, prefix) do
    raise ArgumentError,
          "Musubi Rust codegen: #{inspect(Enum.sort(segments))} collide on the rust module " <>
            "path #{inspect(prefix <> ident)}; rename one before the generator can emit " <>
            "the bundle"
  end

  defp normalize(data) do
    %{
      kind: Map.get(data, :kind) || :state,
      fields: data |> Map.get(:fields, []) |> List.wrap() |> Manifest.renderable_fields(),
      commands: data |> Map.get(:commands, []) |> List.wrap(),
      events: data |> Map.get(:events, []) |> List.wrap(),
      attrs: data |> Map.get(:attrs, []) |> List.wrap(),
      uploads: data |> Map.get(:uploads, []) |> List.wrap()
    }
  end

  defp header do
    """
    // Generated by `mix compile.musubi_rust`. Do not edit by hand.
    // Include as a module file (`mod generated;`) or via `include!`.

    #![allow(clippy::all, dead_code, unused_imports)]
    """
  end

  # The prelude's sole item, unindented. Emitted verbatim and unconditionally
  # (§4.5) — as the body of a standalone `pub mod <root>`, or, when a top-level
  # Elixir namespace already owns that Rust name, as the first item of *that*
  # module (`prelude_segment/2`). Merging rather than emitting a second
  # `pub mod <root>` keeps the bundle valid Rust: two `pub mod` items of the
  # same name at one level are `error[E0428]`.
  defp prelude_item(runtime) do
    """
    pub use ::#{runtime}::generated::{
    #{fill(@runtime_types, @indent, String.length(@indent))}};
    """
  end

  # rustfmt's `Mixed` import layout: greedily fill each line up to `max_width`,
  # trailing comma included. `offset` is what the bundle assembler prepends —
  # the prelude item is always shifted exactly one level, and its continuation
  # lines carry one more.
  defp fill(names, indent, offset) do
    names
    |> Enum.reduce([], &fill_line(&1, &2, indent, offset))
    |> Enum.reverse()
    |> Enum.map_join("", &(&1 <> "\n"))
  end

  defp fill_line(name, [line | rest] = lines, indent, offset) do
    if offset + String.length(line) + 2 + String.length(name) <= @max_width,
      do: [line <> " " <> name <> "," | rest],
      else: [indent <> name <> "," | lines]
  end

  defp fill_line(name, [], indent, _offset), do: [indent <> name <> ","]

  defp prelude_block(bundle) do
    @prelude_comment <> "pub mod #{bundle.root} {\n" <> indent(bundle.prelude_item) <> "}\n"
  end

  # ---------------------------------------------------------------------------
  # Module tree
  # ---------------------------------------------------------------------------

  defp store_segments(entries) do
    for {module, %{kind: :store}} <- entries, into: MapSet.new(), do: Module.split(module)
  end

  defp tree(entries) do
    Enum.reduce(entries, %{}, fn {module, _data} = entry, tree ->
      insert_entry(tree, Module.split(module), entry)
    end)
  end

  # The top-level Elixir segment whose `pub mod` would carry the prelude's Rust
  # name, or `nil` when the prelude can stand alone. A leaf `kind: :state`
  # module emits a bare struct and no `pub mod`, so it never collides — struct
  # and module names live in different Rust namespaces.
  defp prelude_segment(tree, root) do
    Enum.find_value(tree, fn {segment, {children, leaf}} ->
      if Names.mod_ident(segment) == root and emits_module?(children, leaf), do: segment
    end)
  end

  defp emits_module?(children, leaf),
    do: map_size(children) > 0 or match?({_module, %{kind: :store}}, leaf)

  defp prelude_and_tree(tree, %{prelude_segment: nil} = bundle) do
    {blocks, nav} = emit_blocks(tree, 0, bundle, MapSet.new(), [])

    {[prelude_block(bundle) | blocks], nav}
  end

  defp prelude_and_tree(tree, bundle), do: emit_blocks(tree, 0, bundle, MapSet.new(), [])

  defp insert_entry(tree, [last], entry) do
    Map.update(tree, last, {%{}, entry}, fn {children, _leaf} -> {children, entry} end)
  end

  defp insert_entry(tree, [head | rest], entry) do
    Map.update(tree, head, {insert_entry(%{}, rest, entry), nil}, fn {children, leaf} ->
      {insert_entry(children, rest, entry), leaf}
    end)
  end

  # Emits the body of one Rust module as a list of blocks, plus every
  # navigation trait declared inside it (for the bundle's `nav` module).
  # `claimed` seeds the per-module name table with the names already taken
  # inside it, so a hoisted type can never shadow a generated item; `path` is
  # the Rust module path reached so far, which the `nav` re-exports name.
  defp emit_blocks(tree, depth, bundle, claimed, path) do
    nodes = Enum.sort_by(tree, fn {segment, _node} -> segment end)
    claimed = Enum.reduce(nodes, claimed, &claim_struct_name/2)

    {emitted, _claimed} =
      Enum.map_reduce(nodes, claimed, fn node, acc ->
        {blocks, acc, nav} = emit_node(node, depth, bundle, acc, path)

        {{blocks, nav}, acc}
      end)

    {emitted |> Enum.flat_map(&elem(&1, 0)) |> List.flatten(),
     Enum.flat_map(emitted, &elem(&1, 1))}
  end

  # A shape's `<Name>Ext` takes its place in the module's name table alongside
  # the shape itself, before any hoisted type is allocated, so a hoisted type
  # can never shadow a navigation trait (docs/rust-reactive-state.md §4.2).
  defp claim_struct_name({segment, {_children, {_module, %{kind: :state}}}}, claimed),
    do: claimed |> MapSet.put(segment) |> MapSet.put(ext_name(segment))

  defp claim_struct_name(_node, claimed), do: claimed

  defp ext_name(name), do: to_string(name) <> "Ext"

  # A node with no leaf is a pure namespace: only the `pub mod` is emitted.
  defp emit_node({segment, {children, nil}}, depth, bundle, claimed, path) do
    {blocks, nav} = child_module(segment, children, depth, bundle, path)

    {blocks, claimed, nav}
  end

  defp emit_node(
         {segment, {children, {_module, %{kind: :state} = data}}},
         depth,
         bundle,
         claimed,
         path
       ) do
    opts = item_opts(depth, bundle)
    shape = Names.struct_ident(segment)
    {specs, hoists, claimed} = render_fields(data.fields, segment, claimed, %{opts | nav?: true})
    {hoisted, hoisted_nav} = hoisted_blocks(hoists, path)

    blocks =
      [TypeRenderer.struct_block(shape, specs, depth)] ++
        ext_blocks(shape, [{state_target(shape, opts), :field}], specs, opts) ++ hoisted

    {child_blocks, child_nav} = child_module(segment, children, depth, bundle, path)

    {blocks ++ child_blocks, claimed,
     [nav_entry(ext_name(shape), path) | hoisted_nav] ++ child_nav}
  end

  # A store's own items and any module nested under it share one Rust module,
  # so the nested structs continue the store's name table.
  defp emit_node(
         {segment, {children, {module, %{kind: :store} = data}}},
         depth,
         bundle,
         claimed,
         path
       ) do
    inner_path = [Names.mod_ident(segment) | path]
    {items, inner_claimed, nav} = store_items(module, data, depth + 1, bundle, inner_path)
    {nested, nested_nav} = emit_blocks(children, depth + 1, bundle, inner_claimed, inner_path)

    body = prelude_prefix(segment, depth, bundle) <> Enum.join(items ++ nested, "\n")

    {[mod_block(segment, body)], claimed, nav ++ nested_nav}
  end

  defp child_module(_segment, children, _depth, _bundle, _path) when map_size(children) == 0,
    do: {[], []}

  defp child_module(segment, children, depth, bundle, path) do
    inner_path = [Names.mod_ident(segment) | path]
    {blocks, nav} = emit_blocks(children, depth + 1, bundle, MapSet.new(), inner_path)
    body = Enum.join(blocks, "\n")

    {[mod_block(segment, prelude_prefix(segment, depth, bundle) <> body)], nav}
  end

  defp prelude_prefix(segment, 0, %{prelude_segment: segment} = bundle),
    do: @prelude_comment <> bundle.prelude_item <> "\n"

  defp prelude_prefix(_segment, _depth, _bundle), do: ""

  defp mod_block(segment, body) do
    "pub mod #{Names.mod_ident(segment)} {\n" <> indent(body) <> "}\n"
  end

  # ---------------------------------------------------------------------------
  # Store items
  # ---------------------------------------------------------------------------

  defp store_items(module, data, depth, bundle, path) do
    ensure_no_state_upload_collision!(module, data.fields, data.uploads)

    marker = module |> Module.split() |> List.last() |> Names.struct_ident()
    prelude = prelude_path(depth, bundle.root)
    opts = item_opts(depth, bundle)

    claimed =
      MapSet.new(
        [marker, ext_name(marker), "State", "Params"] ++
          Enum.flat_map(data.commands, &command_names/1) ++
          Enum.map(data.events, &event_name/1)
      )

    {specs, hoists, claimed} =
      render_fields(data.fields, marker, claimed, %{opts | nav?: true})

    upload_specs = Enum.map(data.uploads, &upload_spec(&1, prelude))

    {param_specs, param_hoists, claimed} =
      render_attrs(data.attrs, marker <> "Params", claimed, opts)

    {commands, claimed} =
      Enum.flat_map_reduce(data.commands, claimed, &command_blocks(&1, marker, prelude, opts, &2))

    {events, claimed} =
      Enum.flat_map_reduce(data.events, claimed, &event_blocks(&1, marker, prelude, opts, &2))

    {hoisted, hoisted_nav} = hoisted_blocks(hoists ++ param_hoists, path)

    blocks =
      [marker_block(marker), store_impl_block(module, marker, prelude)] ++
        [state_block(marker, specs ++ upload_specs, depth)] ++
        ext_blocks(marker, store_targets(opts), specs ++ upload_specs, opts) ++
        [params_block(param_specs, depth)] ++ hoisted ++ commands ++ events

    {blocks, claimed, [nav_entry(ext_name(marker), path) | hoisted_nav]}
  end

  # A store's shape carries two impls (docs/rust-reactive-state.md §4.2): one on
  # `State<State>` for a shape reached as an ordinary node, one forwarding
  # through `StoreState::fields` so a child store's own handle navigates
  # directly — `snap.checkout_panel().total()` next to
  # `snap.checkout_panel().store_id()`.
  defp store_targets(opts) do
    prelude = prelude_path(opts.depth, opts.root_module)

    [{"#{prelude}::State<State>", :field}, {"#{prelude}::StoreState<State>", :forward}]
  end

  defp state_target(shape, opts),
    do: "#{prelude_path(opts.depth, opts.root_module)}::State<#{shape}>"

  defp ext_blocks(shape, targets, specs, opts),
    do: TypeRenderer.ext_blocks(ext_name(shape), targets, specs, opts.depth)

  # `path` is the enclosing Rust module path, innermost segment first; the
  # `nav` re-export needs it outermost first.
  defp nav_entry(trait, path), do: %{trait: trait, segments: Enum.reverse([trait | path])}

  # The bundle's last top-level item: every navigation trait re-exported flat,
  # so one `use <bundle>::nav::*;` per consumer file is enough. Sorted the way
  # rustfmt sorts a `use` group — segment by segment, uppercase before
  # lowercase — because the bundle has to survive `cargo fmt --check` (§4.1),
  # and that ordering is by trait name wherever two traits share a module.
  # Two shapes in different modules can carry the same trait name, so the names
  # are allocated through the same append-`2` strategy hoisted types use.
  defp nav_block([]), do: []

  defp nav_block(entries) do
    {uses, _claimed} =
      entries
      |> Enum.sort_by(& &1.segments)
      |> Enum.map_reduce(MapSet.new(), fn entry, claimed ->
        {name, claimed} = Names.allocate(entry.trait, claimed)
        path = Enum.join(entry.segments, "::")

        {@indent <> "pub use super::#{path}#{rename_use(entry.trait, name)};\n", claimed}
      end)

    [@nav_comment <> "pub mod #{@nav_module} {\n" <> Enum.join(uses) <> "}\n"]
  end

  defp rename_use(trait, trait), do: ""
  defp rename_use(_trait, name), do: " as #{name}"

  defp ensure_no_state_upload_collision!(module, fields, uploads) do
    field_names = MapSet.new(fields, & &1.name)

    case Enum.find(uploads, fn upload -> MapSet.member?(field_names, upload.name) end) do
      nil ->
        :ok

      %{name: name} ->
        raise ArgumentError,
              "Musubi Rust codegen: upload :#{name} on #{inspect(module)} " <>
                "collides with a state field of the same name; rename one before " <>
                "the generator can emit a merged shape"
    end
  end

  defp command_names(command), do: [command_name(command), command_name(command) <> "Reply"]

  defp command_name(%{name: name}), do: Names.pascal_case(name)

  defp event_name(%{name: name}), do: Names.pascal_case(name) <> "Payload"

  defp marker_block(marker) do
    """
    /// Zero-sized marker type implementing `Store`. Distinct from `State`.
    pub struct #{marker};
    """
  end

  defp store_impl_block(module, marker, prelude) do
    """
    impl #{prelude}::Store for #{marker} {
    #{@indent}const MODULE: &'static str = "#{full_module_name(module)}";
    #{@indent}type State = State;
    #{@indent}type Params = Params;
    }
    """
  end

  defp state_block(marker, specs, depth) do
    """
    /// The store's rendered shape: state fields plus one `UploadSlot` per
    /// declared upload. Reached as `<#{marker} as Store>::State`.
    """ <> TypeRenderer.struct_block("State", specs, depth)
  end

  defp params_block(specs, depth) do
    """
    /// The mount params object, one field per `attr/3` declaration: required
    /// attrs are plain fields, optional ones `Option` that serialize to an
    /// absent key rather than an explicit `null`. A store declaring no `attr`
    /// gets an empty struct, which serializes to `{}`.
    """ <> TypeRenderer.struct_block("Params", specs, depth)
  end

  # `attr/3` metadata is field-shaped (`%{name, type, required, default}`) but
  # carries no `:opts`, so it renders through `render_fields/4` unchanged; only
  # the optionality wrapper is attr-specific. Declared defaults stay
  # server-side (`Musubi.Reconciler.normalize_assigns/2` applies them), so an
  # optional attr is `Option<T>` whether or not it declared one.
  defp render_attrs(attrs, item_name, claimed, opts) do
    {specs, hoists, claimed} = render_fields(attrs, item_name, claimed, opts)

    {Enum.zip_with(attrs, specs, &optional_spec/2), hoists, claimed}
  end

  # An unset optional attr must serialize to an *absent* key, the way the
  # TypeScript client omits it: `normalize_assigns/2` gates a declared default
  # on `Map.has_key?/2`, so a present-but-nil key would suppress the default,
  # and `Musubi.Codegen.Rust`'s params also feed `cache_key/3`, which has to
  # agree byte-for-byte with `storeCacheKey`.
  defp optional_spec(%{required: true}, spec), do: spec

  defp optional_spec(_attr, spec) do
    spec
    |> Map.put(:type, optionalize(spec.type))
    |> Map.put(:skip_none, true)
  end

  # `String.t() | nil` already renders as `Option<String>`; wrapping again
  # would emit `Option<Option<String>>`, which serde only accepts a doubly
  # nested null for.
  defp optionalize("Option<" <> _rest = type), do: type
  defp optionalize(type), do: "Option<" <> type <> ">"

  # The fifth handle shape (docs/rust-reactive-state.md §4.3): an upload slot
  # is an inert leaf on the tree whose accessor hands back the two-halves key
  # `Mounted::upload_at/1` takes.
  defp upload_spec(%{name: name}, prelude) do
    {ident, rename} = Names.field_ident(name)

    %{
      ident: ident,
      rename: rename,
      type: prelude <> "::UploadSlot",
      docs: [],
      key: to_string(name),
      nav: prelude <> "::UploadSlotState",
      into: true
    }
  end

  defp command_blocks(command, marker, prelude, opts, claimed) do
    name = command_name(command)
    opts = %{opts | docs?: true}
    {specs, hoists, claimed} = render_fields(command.payload_fields, name, claimed, opts)

    {reply_blocks, reply_type, claimed} =
      command_reply(Map.get(command, :reply_fields, []), name, prelude, opts, claimed)

    impl = """
    impl #{prelude}::Command<#{marker}> for #{name} {
    #{@indent}const NAME: &'static str = "#{command.name}";
    #{@indent}type Reply = #{reply_type};
    }
    """

    blocks =
      Enum.concat([
        [TypeRenderer.struct_block(name, specs, opts.depth)],
        hoisted_blocks(hoists),
        reply_blocks,
        [impl]
      ])

    {blocks, claimed}
  end

  # `{:noreply, socket}` replies `{}` on the wire, which the crate's permissive
  # `NoReply` struct deserializes — a deliberate divergence from TS's `never`.
  defp command_reply([], _name, prelude, _opts, claimed),
    do: {[], prelude <> "::NoReply", claimed}

  defp command_reply(fields, name, _prelude, opts, claimed) do
    reply_name = name <> "Reply"
    {specs, hoists, claimed} = render_fields(fields, reply_name, claimed, opts)

    {[TypeRenderer.struct_block(reply_name, specs, opts.depth) | hoisted_blocks(hoists)],
     reply_name, claimed}
  end

  defp event_blocks(event, marker, prelude, opts, claimed) do
    name = event_name(event)
    opts = %{opts | docs?: true}
    {specs, hoists, claimed} = render_fields(event.payload_fields, name, claimed, opts)

    payload =
      "/// Push event payload (BDR-0032).\n" <> TypeRenderer.struct_block(name, specs, opts.depth)

    impl = """
    impl #{prelude}::Event<#{marker}> for #{name} {
    #{@indent}const NAME: &'static str = "#{event.name}";
    }
    """

    {Enum.concat([[payload], hoisted_blocks(hoists), [impl]]), claimed}
  end

  # ---------------------------------------------------------------------------
  # Fields and declarations
  # ---------------------------------------------------------------------------

  defp item_opts(depth, bundle) do
    %{
      depth: depth,
      root_module: bundle.root,
      stores: bundle.stores,
      docs?: false,
      nav?: false
    }
  end

  @spec render_fields([map()], String.t(), MapSet.t(String.t()), item_opts()) ::
          {[TypeRenderer.field_spec()], [TypeRenderer.declaration()], MapSet.t(String.t())}
  defp render_fields(fields, item_name, claimed, opts) do
    ctx =
      TypeRenderer.new(
        root_module: opts.root_module,
        depth: opts.depth,
        claimed: claimed,
        stores: opts.stores,
        nav: opts.nav?
      )

    {specs, ctx} =
      Enum.map_reduce(fields, ctx, fn field, acc ->
        {ident, rename} = Names.field_ident(field.name)
        prefix = Names.hoisted_name(item_name, [field.name])
        {type, acc} = TypeRenderer.render(field.type, %{acc | prefix: prefix, notes: []})
        docs = field_docs(field, opts.docs?) ++ TypeRenderer.notes(acc)
        {nav, into?} = TypeRenderer.nav_type(field.type, type, acc)

        spec = %{
          ident: ident,
          rename: rename,
          type: type,
          docs: docs,
          key: to_string(field.name),
          nav: nav,
          into: into?
        }

        {spec, acc}
      end)

    {specs, TypeRenderer.declarations(ctx), ctx.claimed}
  end

  # TS renders `/** doc */` for command and event fields but not for state
  # fields; the asymmetry is mirrored deliberately so the two targets stay
  # diffable. The union notes the renderer accumulates are emitted regardless —
  # they record information Rust cannot express in the type.
  defp field_docs(%{opts: opts}, true) when is_list(opts) do
    case Keyword.get(opts, :doc) do
      nil -> []
      doc -> [doc]
    end
  end

  defp field_docs(_field, _docs?), do: []

  # A hoisted struct's navigation trait follows the struct it belongs to, so
  # the module stays readable top-down; enums hoist no trait (they are leaves).
  defp hoisted_blocks(declarations, path) do
    sorted = Enum.sort_by(declarations, & &1.name)

    nav = for %{ext: %{trait: trait}} <- sorted, do: nav_entry(trait, path)

    {Enum.flat_map(sorted, &[&1.code | ext_code(&1)]), nav}
  end

  defp ext_code(%{ext: nil}), do: []
  defp ext_code(%{ext: %{blocks: blocks}}), do: blocks

  # Command payloads, command replies and event payloads never reach the state
  # tree, so nothing hoisted out of them carries a navigation trait; the empty
  # match is the assertion (docs/rust-reactive-state.md §4.3).
  defp hoisted_blocks(declarations) do
    {blocks, []} = hoisted_blocks(declarations, [])

    blocks
  end

  defp prelude_path(depth, root), do: String.duplicate("super::", depth) <> root

  defp full_module_name(module), do: module |> Module.split() |> Enum.join(".")

  defp indent(body) do
    body
    |> String.split("\n")
    |> Enum.map_join("\n", fn
      "" -> ""
      line -> @indent <> line
    end)
  end
end
