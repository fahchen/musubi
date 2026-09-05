defmodule Musubi.Codegen.Rust.TypeRendererTest do
  use ExUnit.Case, async: true

  alias Musubi.Codegen.Rust.Names
  alias Musubi.Codegen.Rust.TypeRenderer

  doctest Musubi.Codegen.Rust.TypeRenderer

  @ext_doc """
  /// Typed navigation for the shape above: one accessor per declared field,
  /// each handing back a handle rather than a value
  /// (`docs/rust-reactive-state.md` §4.2). Reach it through `nav`.
  """

  describe "primitives" do
    test "String.t()" do
      assert TypeRenderer.render!(quote(do: String.t())) == "String"
    end

    test "binary()" do
      assert TypeRenderer.render!(quote(do: binary())) == "String"
    end

    test "string()" do
      assert TypeRenderer.render!(quote(do: string())) == "String"
    end

    test "integer()" do
      assert TypeRenderer.render!(quote(do: integer())) == "i64"
    end

    test "float()" do
      assert TypeRenderer.render!(quote(do: float())) == "f64"
    end

    test "boolean()" do
      assert TypeRenderer.render!(quote(do: boolean())) == "bool"
    end

    test "atom() (atoms serialize as strings)" do
      assert TypeRenderer.render!(quote(do: atom())) == "String"
    end

    test "map() (untyped) keeps JSON object semantics" do
      assert TypeRenderer.render!(quote(do: map())) ==
               "serde_json::Map<String, serde_json::Value>"
    end
  end

  describe "literals" do
    test "nil alone deserializes only from JSON null" do
      assert TypeRenderer.render!(nil) == "()"
    end

    test "true" do
      assert TypeRenderer.render!(true) == "bool"
    end

    test "false" do
      assert TypeRenderer.render!(false) == "bool"
    end

    test "binary literal widens to String and records the lost value constraint" do
      {rendered, ctx} = TypeRenderer.render("hello", TypeRenderer.new())

      assert rendered == "String"
      assert [~s(Allowed values: "hello")] = TypeRenderer.notes(ctx)
    end

    test "integer literal widens to i64 and records the lost value constraint" do
      {rendered, ctx} = TypeRenderer.render(42, TypeRenderer.new())

      assert rendered == "i64"
      assert ["Allowed values: 42"] = TypeRenderer.notes(ctx)
    end

    test "float literal widens to f64 and records the lost value constraint" do
      {rendered, ctx} = TypeRenderer.render(3.14, TypeRenderer.new())

      assert rendered == "f64"
      assert ["Allowed values: 3.14"] = TypeRenderer.notes(ctx)
    end

    test "lone atom literal hoists a single-variant enum" do
      {rendered, ctx} = TypeRenderer.render(:fixed, ctx("CartState", [:mode]))

      assert rendered == "CartStateMode"
      assert [%{name: "CartStateMode", code: code}] = TypeRenderer.declarations(ctx)
      assert code =~ "pub enum CartStateMode {"
      assert code =~ ~s|#[serde(rename = "fixed")]|
    end
  end

  describe "containers" do
    test "list(T)" do
      assert TypeRenderer.render!(quote(do: list(String.t()))) == "Vec<String>"
    end

    test "stream(T) is a plain Vec — the client hydrates the marker" do
      assert TypeRenderer.render!(quote(do: stream(String.t()))) == "Vec<String>"
    end

    test "list of nested list" do
      assert TypeRenderer.render!(quote(do: list(list(integer())))) == "Vec<Vec<i64>>"
    end

    test "list of nilable inner type" do
      assert TypeRenderer.render!(quote(do: list(String.t() | nil))) == "Vec<Option<String>>"
    end
  end

  describe "unions" do
    test "T | nil becomes Option<T> with no enum" do
      assert TypeRenderer.render!(quote(do: String.t() | nil)) == "Option<String>"
    end

    test "nil on the left is stripped just the same" do
      assert TypeRenderer.render!(quote(do: nil | integer())) == "Option<i64>"
    end

    test "atom-literal union hoists a C-like enum with per-variant renames" do
      ast = quote(do: :idle | :running | :"needs-review")
      {rendered, ctx} = TypeRenderer.render(ast, ctx("CartState", [:status]))

      assert rendered == "CartStateStatus"
      assert [%{name: "CartStateStatus", code: code}] = TypeRenderer.declarations(ctx)

      assert code == """
             #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
             pub enum CartStateStatus {
                 #[serde(rename = "idle")]
                 Idle,
                 #[serde(rename = "running")]
                 Running,
                 #[serde(rename = "needs-review")]
                 NeedsReview,
             }
             """
    end

    test "atom-literal union including nil wraps the hoisted enum in Option" do
      {rendered, ctx} = TypeRenderer.render(quote(do: :a | :b | nil), ctx("P", [:mode]))

      assert rendered == "Option<PMode>"
      assert [%{name: "PMode"}] = TypeRenderer.declarations(ctx)
    end

    test "union of three arms where nil-stripping leaves one renders that arm directly" do
      assert TypeRenderer.render!(quote(do: String.t() | nil | nil)) == "Option<String>"
    end

    test "maps sharing a distinct-atom discriminant hoist an internally tagged enum" do
      ast = quote(do: %{type: :active} | %{type: :paused, value: integer()})
      {rendered, ctx} = TypeRenderer.render(ast, ctx("Probe", [:status]))

      assert rendered == "ProbeStatus"
      assert [%{name: "ProbeStatus", code: code}] = TypeRenderer.declarations(ctx)

      assert code == """
             #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
             #[serde(tag = "type")]
             pub enum ProbeStatus {
                 #[serde(rename = "active")]
                 Active,
                 #[serde(rename = "paused")]
                 Paused { value: i64 },
             }
             """
    end

    test "the first qualifying key of the first arm wins as discriminant" do
      ast = quote(do: %{kind: :a, other: :x} | %{kind: :b, other: :y})
      {_rendered, ctx} = TypeRenderer.render(ast, ctx("Probe", [:status]))

      assert [_a_other, _b_other, %{name: "ProbeStatus", code: code}] =
               TypeRenderer.declarations(ctx)

      assert code =~ ~s|#[serde(tag = "kind")]|
      assert code =~ "A { other: ProbeStatusAOther }"
    end

    test "a variant field needing an attribute forces a multi-line struct variant" do
      ast = quote(do: %{type: :active} | %{"kebab-x" => String.t(), type: :paused})
      {_rendered, ctx} = TypeRenderer.render(ast, ctx("Probe", [:status]))

      assert [%{code: code}] = TypeRenderer.declarations(ctx)

      assert code =~ """
                 Paused {
                     #[serde(rename = "kebab-x")]
                     kebab_x: String,
                 },
             """
    end

    test "a nested shape inside a struct variant hoists as <EnumName><VariantName><Field>" do
      ast = quote(do: %{type: :active} | %{type: :paused, value: %{a: integer()}})
      {_rendered, ctx} = TypeRenderer.render(ast, ctx("Probe", [:status]))

      assert ["ProbeStatusPausedValue", "ProbeStatus"] =
               Enum.map(TypeRenderer.declarations(ctx), & &1.name)
    end

    test "binary-literal union collapses to String and records the values" do
      {rendered, ctx} = TypeRenderer.render(quote(do: "a" | "b"), ctx("P", [:x]))

      assert rendered == "String"
      assert TypeRenderer.declarations(ctx) == []
      assert [~s(Allowed values: "a" | "b")] = TypeRenderer.notes(ctx)
    end

    test "integer-literal union collapses to i64" do
      {rendered, ctx} = TypeRenderer.render(quote(do: 1 | 2), ctx("P", [:x]))

      assert rendered == "i64"
      assert ["Allowed values: 1 | 2"] = TypeRenderer.notes(ctx)
    end

    test "float-literal union collapses to f64" do
      {rendered, _ctx} = TypeRenderer.render(quote(do: 1.0 | 2.0), ctx("P", [:x]))

      assert rendered == "f64"
    end

    test "an undiscriminated heterogeneous union falls back to serde_json::Value" do
      {rendered, ctx} = TypeRenderer.render(quote(do: integer() | String.t()), ctx("P", [:x]))

      assert rendered == "serde_json::Value"
      assert TypeRenderer.declarations(ctx) == []
      assert ["Declared arms: integer() | String.t()"] = TypeRenderer.notes(ctx)
    end

    test "maps whose candidate key repeats a value are not a discriminant" do
      ast = quote(do: %{type: :same} | %{type: :same, value: integer()})
      {rendered, ctx} = TypeRenderer.render(ast, ctx("P", [:x]))

      assert rendered == "serde_json::Value"
      assert TypeRenderer.declarations(ctx) == []
    end
  end

  describe "module references" do
    test "Module.t() emits the generated state struct path" do
      ast = quote(do: Musubi.TestSupport.TypespecProbeChild.t())

      assert TypeRenderer.render!(ast) == "musubi::test_support::TypespecProbeChild"
    end

    test "Module.t() on a known store emits its State path, not a bare struct" do
      ast = quote(do: Musubi.TestSupport.TypespecProbeChild.t())
      ctx = TypeRenderer.new(stores: MapSet.new([~w(Musubi TestSupport TypespecProbeChild)]))

      assert TypeRenderer.render!(ast, ctx) ==
               "musubi::test_support::typespec_probe_child::State"
    end

    test "Module.state() emits StoreField over the store's State" do
      ast = quote(do: Musubi.TestSupport.TypespecProbeChild.state())

      assert TypeRenderer.render!(ast) ==
               "musubi::StoreField<musubi::test_support::typespec_probe_child::State>"
    end

    test "Musubi.AsyncResult.of(T) renders as the prelude AsyncResult" do
      ast = quote(do: Musubi.AsyncResult.of(String.t()))

      assert TypeRenderer.render!(ast) == "musubi::AsyncResult<String>"
    end

    test "AsyncResult.of(stream(T)) renders as AsyncResult<Vec<T>>" do
      ast = quote(do: Musubi.AsyncResult.of(stream(String.t())))

      assert TypeRenderer.render!(ast) == "musubi::AsyncResult<Vec<String>>"
    end

    test "non-AsyncResult `.of/1` falls back to serde_json::Value" do
      ast = quote(do: Some.Other.Module.of(String.t()))

      assert TypeRenderer.render!(ast) == "serde_json::Value"
    end

    test "depth prefixes every cross-module path with a super:: chain" do
      ctx = TypeRenderer.new(depth: 3)
      ast = quote(do: Musubi.AsyncResult.of(Demo.LineItem.t()))

      assert TypeRenderer.render!(ast, ctx) ==
               "super::super::super::musubi::AsyncResult<super::super::super::demo::LineItem>"
    end

    test ":root_module retargets the prelude path" do
      ctx = TypeRenderer.new(root_module: "rt")

      assert TypeRenderer.render!(quote(do: Musubi.AsyncResult.of(integer())), ctx) ==
               "rt::AsyncResult<i64>"
    end
  end

  describe "hoisting" do
    test "an anonymous map hoists a struct named after the enclosing item and path" do
      {rendered, ctx} =
        TypeRenderer.render(quote(do: %{street: String.t()}), ctx("CartState", [:address]))

      assert rendered == "CartStateAddress"
      assert [%{name: "CartStateAddress", code: code}] = TypeRenderer.declarations(ctx)

      assert code == """
             #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
             pub struct CartStateAddress {
                 pub street: String,
             }
             """
    end

    test "an empty map hoists a braced unit struct" do
      {rendered, ctx} = TypeRenderer.render(quote(do: %{}), ctx("P", [:empty]))

      assert rendered == "PEmpty"
      assert [%{code: "#" <> _rest = code}] = TypeRenderer.declarations(ctx)
      assert code =~ "pub struct PEmpty {}"
    end

    test "nested maps hoist depth-first, outermost name last" do
      ast = quote(do: %{tags: %{a: integer()}})
      {rendered, ctx} = TypeRenderer.render(ast, ctx("CartState", [:meta]))

      assert rendered == "CartStateMeta"

      assert ["CartStateMetaTags", "CartStateMeta"] =
               Enum.map(TypeRenderer.declarations(ctx), & &1.name)
    end

    test "sibling fields each descend from the enclosing name, never from each other" do
      ast = quote(do: %{a: %{x: integer()}, b: %{y: integer()}})
      {rendered, ctx} = TypeRenderer.render(ast, ctx("P", [:node]))

      assert rendered == "PNode"

      assert ["PNodeA", "PNodeB", "PNode"] =
               Enum.map(TypeRenderer.declarations(ctx), & &1.name)
    end

    test "sibling struct-variant fields each descend from the variant name" do
      ast =
        quote(do: %{type: :a} | %{type: :b, one: %{x: integer()}, two: %{y: integer()}})

      {_rendered, ctx} = TypeRenderer.render(ast, ctx("P", [:node]))

      assert ["PNodeBOne", "PNodeBTwo", "PNode"] =
               Enum.map(TypeRenderer.declarations(ctx), & &1.name)
    end

    test "wrappers are name-transparent" do
      ast = quote(do: list(list(%{sku: String.t()})))
      {rendered, ctx} = TypeRenderer.render(ast, ctx("CartState", [:grid]))

      assert rendered == "Vec<Vec<CartStateGrid>>"
      assert [%{name: "CartStateGrid"}] = TypeRenderer.declarations(ctx)
    end

    test "stream and AsyncResult wrappers add no segment either" do
      ast = quote(do: Musubi.AsyncResult.of(stream(%{id: String.t()})))
      {rendered, ctx} = TypeRenderer.render(ast, ctx("CartStore", [:suggestions]))

      assert rendered == "musubi::AsyncResult<Vec<CartStoreSuggestions>>"
      assert [%{name: "CartStoreSuggestions"}] = TypeRenderer.declarations(ctx)
    end

    test "a name already claimed by a generated item gets a numeric suffix" do
      ctx =
        TypeRenderer.new(
          prefix: Names.hoisted_name("CartState", [:address]),
          claimed: claimed(["CartStateAddress"])
        )

      {rendered, ctx} = TypeRenderer.render(quote(do: %{street: String.t()}), ctx)

      assert rendered == "CartStateAddress2"
      assert [%{name: "CartStateAddress2", code: code}] = TypeRenderer.declarations(ctx)
      assert code =~ "pub struct CartStateAddress2 {"
    end

    test "two distinct shapes claiming the same base name are suffixed in allocation order" do
      {first, ctx} = TypeRenderer.render(quote(do: %{a: integer()}), ctx("P", [:meta, :tags]))

      {second, ctx} =
        TypeRenderer.render(quote(do: %{b: integer()}), rename(ctx, "P", [:meta_tags]))

      assert {"PMetaTags", "PMetaTags2"} = {first, second}
      assert ["PMetaTags", "PMetaTags2"] = Enum.map(TypeRenderer.declarations(ctx), & &1.name)
    end

    test "hoists accumulate across successive renders sharing one context" do
      {_rendered, ctx} = TypeRenderer.render(quote(do: %{a: integer()}), ctx("P", [:one]))
      {_rendered, ctx} = TypeRenderer.render(quote(do: :x | :y), rename(ctx, "P", [:two]))

      assert ["POne", "PTwo"] = Enum.map(TypeRenderer.declarations(ctx), & &1.name)
    end

    test "render!/2 drops the hoisted declaration but still names the reference" do
      assert TypeRenderer.render!(quote(do: %{a: integer()}), ctx("P", [:one])) == "POne"
    end
  end

  describe "identifiers" do
    test "a keyword field name becomes a raw ident with no rename" do
      ast = quote(do: %{type: String.t()})
      {_rendered, ctx} = TypeRenderer.render(ast, ctx("P", [:node]))

      assert [%{code: code}] = TypeRenderer.declarations(ctx)
      assert code =~ "pub r#type: String,"
      refute code =~ "rename"
    end

    test "a non-ident binary key is sanitized and renamed" do
      ast = quote(do: %{"kebab-key" => String.t()})
      {_rendered, ctx} = TypeRenderer.render(ast, ctx("P", [:node]))

      assert [%{code: code}] = TypeRenderer.declarations(ctx)

      assert code =~ """
                 #[serde(rename = "kebab-key")]
                 pub kebab_key: String,
             """
    end

    test "a field whose union collapses carries the note for its own doc comment" do
      ast = quote(do: %{choice: "a" | "b"})
      {_rendered, ctx} = TypeRenderer.render(ast, ctx("P", [:node]))

      assert [%{code: code}] = TypeRenderer.declarations(ctx)
      assert code =~ ~s(    /// Allowed values: "a" | "b"\n    pub choice: String,)
      assert TypeRenderer.notes(ctx) == []
    end
  end

  describe "fallback" do
    test "unknown AST shape renders as serde_json::Value" do
      assert TypeRenderer.render!({:weird_node, [], [:nope]}) == "serde_json::Value"
    end

    test "an alias node the manifest could not expand falls back" do
      ast = {{:., [], [{:__aliases__, [], [{:unquote, [], []}]}, :t]}, [], []}

      assert TypeRenderer.render!(ast) == "serde_json::Value"
    end

    # `Module.split/1` raises on an Erlang module, and §3.2's fallback is total.
    test "an Erlang module type falls back instead of raising" do
      assert TypeRenderer.render!(quote(do: :queue.t())) == "serde_json::Value"
      assert TypeRenderer.render!(quote(do: :queue.state())) == "serde_json::Value"
    end
  end

  # The navigation column of `docs/rust-reactive-state.md` §4.3, one AST shape
  # per row. `nav_type/3` takes the already-rendered snapshot type so the two
  # columns are derived from one render, never from two.
  describe "navigation types" do
    test "the four wrapper shapes lift into their own handle" do
      assert nav(quote(do: stream(String.t()))) == {"musubi::StreamState<String>", true}

      assert nav(quote(do: MyApp.CartStore.state())) ==
               {"musubi::StoreState<my_app::cart_store::State>", true}

      assert nav(quote(do: Musubi.AsyncResult.of(integer()))) ==
               {"musubi::AsyncState<i64>", true}

      # `stream_async`: the async node wraps the stream, so the handle keeps
      # `Vec<T>` — the shape `AsyncState::ok_stream/1` is defined on.
      assert nav(quote(do: Musubi.AsyncResult.of(stream(String.t())))) ==
               {"musubi::AsyncState<Vec<String>>", true}
    end

    test "everything else navigates as a plain State over its snapshot type" do
      assert nav(quote(do: String.t())) == {"musubi::State<String>", false}
      assert nav(quote(do: list(String.t()))) == {"musubi::State<Vec<String>>", false}
      assert nav(quote(do: String.t() | nil)) == {"musubi::State<Option<String>>", false}
      assert nav(quote(do: MyApp.LineItem.t())) == {"musubi::State<my_app::LineItem>", false}

      assert nav(quote(do: map())) ==
               {"musubi::State<serde_json::Map<String, serde_json::Value>>", false}
    end

    test "a wrapper whose alias could not be expanded navigates as an opaque leaf" do
      assert nav(quote(do: :queue.state())) == {"musubi::State<serde_json::Value>", false}

      assert nav(quote(do: Other.Thing.of(integer()))) ==
               {"musubi::State<serde_json::Value>", false}
    end

    test "the prelude path is depth-correct, like the snapshot column's" do
      ctx = TypeRenderer.new(depth: 2, root_module: "rt")

      assert TypeRenderer.nav_type(quote(do: stream(String.t())), "Vec<String>", ctx) ==
               {"super::super::rt::StreamState<String>", true}
    end
  end

  describe "navigation traits" do
    test "a struct hoisted out of a state field carries its Ext trait" do
      {_rendered, ctx} =
        TypeRenderer.render(
          quote(do: %{street: String.t()}),
          %{ctx("CartState", [:address]) | nav: true}
        )

      assert [%{name: "CartStateAddress", ext: %{trait: "CartStateAddressExt", blocks: blocks}}] =
               TypeRenderer.declarations(ctx)

      assert Enum.join(blocks) =~
               """
               pub trait CartStateAddressExt {
                   fn street(&self) -> musubi::State<String>;
               }
               """
    end

    test "a struct that never reaches the state tree carries none" do
      {_rendered, ctx} =
        TypeRenderer.render(quote(do: %{street: String.t()}), ctx("CartStateParams", [:address]))

      assert [%{ext: nil}] = TypeRenderer.declarations(ctx)
    end

    test "a struct hoisted inside a union arm carries none — unions are leaves" do
      ast = quote(do: %{type: :a} | %{type: :b, payload: %{x: integer()}})
      {_rendered, ctx} = TypeRenderer.render(ast, %{ctx("P", [:node]) | nav: true})

      assert Enum.map(TypeRenderer.declarations(ctx), & &1.ext) == [nil, nil]
    end

    test "the second impl of a store shape forwards through StoreState::fields" do
      specs = [%{ident: "amount", key: "amount", nav: "musubi::State<i64>", into: false}]

      [_trait, _direct, forwarding] =
        TypeRenderer.ext_blocks(
          "CartStoreExt",
          [{"musubi::State<State>", :field}, {"musubi::StoreState<State>", :forward}],
          specs,
          0
        )

      assert forwarding == """
             impl CartStoreExt for musubi::StoreState<State> {
                 fn amount(&self) -> musubi::State<i64> {
                     CartStoreExt::amount(&self.fields())
                 }
             }
             """
    end

    test "a shape with no fields emits an empty trait and an empty impl" do
      assert TypeRenderer.ext_blocks("EmptyExt", [{"musubi::State<Empty>", :field}], [], 0) == [
               @ext_doc <> "pub trait EmptyExt {}\n",
               "impl EmptyExt for musubi::State<Empty> {}\n"
             ]
    end
  end

  defp nav(ast) do
    ctx = TypeRenderer.new()
    {rendered, ctx} = TypeRenderer.render(ast, ctx)

    TypeRenderer.nav_type(ast, rendered, ctx)
  end

  defp ctx(prefix, path), do: TypeRenderer.new(prefix: Names.hoisted_name(prefix, path))

  # Retargets an existing context at the next field, keeping its hoists and
  # name table — what the bundle assembler does between two fields of one item.
  defp rename(ctx, prefix, path), do: %{ctx | prefix: Names.hoisted_name(prefix, path)}

  defp claimed(names), do: MapSet.new(names)
end
