# Rust Codegen (`:musubi_rust`)

Design document for a second codegen target that mirrors `:musubi_ts`: one Mix
compiler that renders a single `.rs` bundle of typed store/state definitions
from the same per-module compile-time manifest the TypeScript target already
consumes.

Status: **implemented**. `Musubi.Codegen.Rust`, `Musubi.Codegen.Rust.Names`,
`Musubi.Codegen.Rust.TypeRenderer` and `Mix.Tasks.Compile.MusubiRust` ship the
behaviour described here; `mix compile.musubi_rust --check` runs in
`mix precommit`.

Scope note: this document covers the **generator**, and it is the **normative**
source for everything the generator emits — compiler and config names, type
mapping, hoisting and naming, and the shape of the generated bundle.
`docs/rust-client.md` §8 defers to it.

Building the Rust client *runtime* (transport, patch application, stream
materialization, uploads, reconnect) is a separate project, specified in
`docs/rust-client.md`. The generator does not implement it — but it does
**depend on it**: the shared runtime types (`AsyncResult`, `Store`,
`Command<S>`, `Event<S>`, `StoreId`, `StoreField<S>`, `NoReply`, `UploadSlot`)
are owned by the `musubi-client` crate and re-exported by the bundle (§4.5), so
that `Connection::mount::<CartStore>()` type-checks. Exactly as the TS bundle
emits types that `packages/client` consumes.

---

## 1. Manifest strategy

### 1.1 Decision: reuse the existing manifest, renamed to a target-neutral name

The persisted `state.term` payload is already target-agnostic. It contains raw
Musubi reflection and nothing else:

| key | value |
| :-- | :---- |
| `:module` | `env.module` atom |
| `:kind` | `:state` or `:store` (`:input` never stamped — `Musubi.DSL.Input.input/1` does not attach the plugin) |
| `:fields` | `[%{name: atom, type: Macro.t(), opts: keyword}]`, `:type` alias-expanded |
| `:commands` | `[%{name, payload_fields, reply_fields, opts}]`, both field lists alias-expanded |
| `:events` | `[%{name, payload_fields}]`, payload alias-expanded |
| `:uploads` | `[%Musubi.Upload.Config{name, accept, max_entries, max_file_size, chunk_size, chunk_timeout}]` (no AST, not expanded) |
| `:source` | `env.file` (stored, never read back) |

No TypeScript string, marker name, or output path is encoded. Only three
*names* are TS-coupled: the module names, the `@subdir "musubi-codegen-ts"`
build directory, and the `:__musubi_ts_target_dir__` test-isolation process key.

**Decision: rename to target-neutral names and stamp exactly once.** A second
`@after_compile` hook stamping a parallel `musubi-codegen-rust/` tree would
double the compile-time file writes, double the `mix clean` surface, and create
two sources of truth that can drift when one plugin is attached and the other
is not.

| Today | After |
| :---- | :---- |
| `Musubi.Plugin.TypeScript` | `Musubi.Plugin.Codegen` |
| `Musubi.Codegen.TypeScript.Manifest` | `Musubi.Codegen.Manifest` |
| `@subdir "musubi-codegen-ts"` | `@subdir "musubi-codegen"` |
| `:__musubi_ts_target_dir__` | `:__musubi_codegen_target_dir__` |
| `plugin(Musubi.Plugin.TypeScript)` in `Musubi.DSL.State.state/1` | `plugin(Musubi.Plugin.Codegen)` |

`Musubi.Codegen.TypeScript` (the bundle assembler) and
`Musubi.Codegen.TypeScript.TypeRenderer` keep their names — they are genuinely
TS-specific. The new siblings are `Musubi.Codegen.Rust` and
`Musubi.Codegen.Rust.TypeRenderer`.

### 1.2 What the rename changed

The rename landed as one standalone preparatory commit ahead of the Rust
renderer, with **no deprecation shim** — both renamed modules are internal and
pre-1.0. `Musubi.Codegen.TypeScript.Manifest` was `@moduledoc false` and listed
in `mix.exs` `skipped_doc_references/0`; `Musubi.Plugin.TypeScript` was covered
by the `"Musubi.Plugin."` prefix in the same list, absent from `docs_modules/0`,
and referenced only from `Musubi.DSL.State.state/1`, so no consumer ever named
either.

Beyond the two module names and the `@subdir` / process-key constants, the
commit touched four non-test sites a naive grep misses, and they are the places
to check first if the naming ever drifts again:

- `lib/musubi/plugin/type_script.ex` moved to `lib/musubi/plugin/codegen.ex`,
  carrying its module name, moduledoc, and the `@after_compile` literal.
- The "Discovery" section of the `Mix.Tasks.Compile.MusubiTs` moduledoc, which
  spells the manifest path and the plugin module out in prose.
- The `@typedoc` on the manifest entry type in
  `lib/musubi/codegen/type_script.ex`.
- `.github/copilot-instructions.md`.

`mix.exs` `skipped_doc_references/0` moved to `"Musubi.Codegen.Manifest"`, the
two `AGENTS.md` codegen bullets were rewritten onto the current `StoreDef`
output shape and the `priv/codegen/ts/musubi.d.ts` default, and the two stale
`@type entry()` definitions (`manifest.ex`, `type_script.ex`) that omitted
`:events` were fixed in the same pass.

Consumer impact: the old `_build/<env>/musubi-codegen-ts/` directory became an
orphan. Nothing reads it and the TypeScript compiler's `manifests/0` callback
no longer points at it, but `mix clean` does **not** remove it — `clean/0`
`rm_rf`s `Manifest.target_dir()`, which after the rename is the *new*
directory. The orphan survives until the next `_build` wipe. No consumer app
needed a code change; the next `mix compile` restamps into `musubi-codegen/`
because `@after_compile` fires on recompilation of every `state do` module
(which the plugin change itself forces).

Two target-agnostic generalizations landed in the same commit, and both are
normative for any future renderer:

- **`:__streams__` filtering** lives on the shared layer as
  `Musubi.Codegen.Manifest.renderable_fields/1`, not in a renderer. Every
  renderer calls it rather than re-deriving the exclusion list.
- **`stamp/3` performs no alias expansion.** It builds the same 7-key map from
  module reflection alone, having no `Macro.Env`, so entries it writes can
  carry single-segment `{:__aliases__, _, [:Child]}` nodes the real compile
  path never produces. Its `@doc false` says so; renderers are written against
  the expanded form `collect/1` emits, never against what `stamp/3` happens to
  persist.

### 1.3 What the Rust target consumes

`Musubi.Codegen.Manifest.list/1` verbatim: a list of
`{module, %{kind, fields, commands, events, uploads}}` sorted by
`Module.split/1`, with every `{:__aliases__, _, parts}` node already
fully-qualified. The Rust renderer therefore needs **no** `Macro.Env`, alias
table, or heuristic module resolution — exactly like the TS renderer. This is
the single invariant that makes a second target cheap; preserve it.

---

## 2. Module layout and Mix task contract

### 2.1 Modules

| Module | Role | TS counterpart |
| :----- | :--- | :------------- |
| `Musubi.Codegen.Rust` | Bundle assembly: prelude, `Store` trait + impls, module tree, hoisted-type emission | `Musubi.Codegen.TypeScript` |
| `Musubi.Codegen.Rust.TypeRenderer` | Single field-type AST → Rust type string (+ hoist requests) | `Musubi.Codegen.TypeScript.TypeRenderer` |
| `Musubi.Codegen.Rust.Names` | Pure naming helpers: `pascal_case/1`, module/struct paths, raw-ident escaping, hoisted-name allocation | — (new; TS needs none) |
| `Mix.Tasks.Compile.MusubiRust` | Mix compiler; the `--check`/drift/`:noop` body is shared with the TS task in `Musubi.Codegen.Compiler` | `Mix.Tasks.Compile.MusubiTs` |

`Musubi.Codegen.Rust.Names` is split out because it is the one piece with
non-obvious rules (keyword escaping, collision suffixes) and it deserves a pure
table test of its own.

### 2.2 Compiler contract — identical decision table

`Mix.Tasks.Compile.MusubiRust`, `use Mix.Task.Compiler`, compiler atom
`:musubi_rust`, diagnostics stamped `compiler_name: "musubi_rust"`. The shipped
task delegates this body to `Musubi.Codegen.Compiler`, which both targets share;
the equivalent inline form is:

```elixir
def run(argv) do
  {opts, _rest, _invalid} = OptionParser.parse(argv, strict: [check: :boolean])

  Manifest.clean_outdated()

  entries = Manifest.list()
  output_path = configured_output_path()
  contents = Musubi.Codegen.Rust.render(entries)
  existing = File.read(output_path)
  check? = opts[:check] == true

  cond do
    existing == {:ok, contents} -> :noop
    entries == [] and existing == {:error, :enoent} -> :noop
    check? -> {:error, [drift_diagnostic(output_path)]}
    entries == [] and match?({:ok, _bundle}, existing) -> warn_and_keep(...); {:ok, []}
    true -> write_bundle!(contents, output_path); {:ok, []}
  end
end
```

Byte-for-byte the same shape as the TS task, including clause order:

1. Byte-identical output ⇒ `:noop` (`--check` irrelevant).
2. Empty manifest **and** no existing file ⇒ `:noop`. This is why musubi's own
   repo emits no bundle (its only `state do` modules live under `test/`, which
   `eligible_source?/1` skips) and why `--check` passes here.
3. `--check` with any difference ⇒ `{:error, [diagnostic]}`, no write.
4. Empty manifest **and** an existing bundle ⇒ keep the file, warn, `{:ok, []}`.
   See the empty-manifest guard below.
5. Otherwise write.

`drift_diagnostic/1`:

```elixir
%Mix.Task.Compiler.Diagnostic{
  compiler_name: "musubi_rust",
  file: output_path,
  message:
    "Musubi Rust bundle is out of date. Run `mix compile.musubi_rust` and commit the result.",
  position: nil,
  severity: :error
}
```

`manifests/0` ⇒ `[Manifest.target_dir()]`. `clean/0` ⇒
`File.rm_rf(Manifest.target_dir())` then `:ok`.

**Shared-manifest caveat:** with one manifest directory and two compilers, both
`manifests/0` return the same path and both `clean/0` delete it. That is
harmless (`rm_rf` on a missing dir is `:ok`, and either compiler restamps
nothing — stamping is owned by `@after_compile`, not by the compilers), but it
does mean a run after `mix clean` (or after any `_build` wipe) without a
recompile sees an **empty manifest** while a committed bundle still sits on
disk. Both compilers handle that case identically:

- `--check` reports **drift** (clause 3): `existing == {:ok, contents}` is false
  because `render([])` is not the full bundle, and clause 2 does not apply
  because the file exists. Only the no-bundle case (Musubi's own repo) returns
  `:noop`. So the ordering requirement stands — `--check` must run after a
  compile, not after a clean, or CI fails spuriously.
- A plain run (clause 4) **refuses to write**. Without the guard, `mix compile`
  after a manifest wipe would silently replace both committed bundles with their
  empty renders (`interface Stores {}` / prelude-only), because no `state do`
  module recompiles, so nothing restamps and `Manifest.list/0` legitimately
  returns `[]`. The empty manifest is indistinguishable from "every store was
  deleted" at this layer, and only one of the two readings is recoverable from,
  so the compiler keeps the file, emits a `Mix.shell/0` warning naming the cause
  and the remedy (`mix compile --force` restamps), and returns `{:ok, []}` —
  **not** `:error`: a missing manifest is not a reason to fail a consumer's
  build. The genuine all-stores-deleted case still converges, one step later:
  deleting a store recompiles the project, which restamps, and the next run
  writes the empty bundle truthfully.

### 2.3 Configuration

| Key (under `:musubi`) | Default | Meaning |
| :-------------------- | :------ | :------ |
| `:rust_codegen_output_path` | `"priv/codegen/rust/musubi.rs"` | Bundle destination |
| `:rust_codegen_root_module` | `"musubi"` | Name of the generated **prelude module** — a sibling `pub mod musubi` next to `pub mod my_app`, holding nothing but `pub use` re-exports of the runtime crate's shared types (§4.5). Mirrors `:ts_codegen_root_namespace`. Must be a valid snake_case Rust ident |
| `:rust_codegen_runtime_path` | `"musubi_client"` | Rust path of the client crate that **owns** the shared runtime types (`AsyncResult`, `Store`, `Command`, …). Emitted as `::<path>::generated::…`. Retargetable so a consumer that re-exports the crate under another name can point at it |

Read the root module through `Musubi.Codegen.Rust.configured_root_module/0`
(mirroring `TypeScript.configured_root_namespace/0`) and the runtime path
through `configured_runtime_path/0`; `render/2` accepts `:root_module` and
`:runtime_path` as overrides so tests can pass them explicitly, and the Mix task
never passes them.

Note the layout this pins: the root module is a **sibling** prelude module, not
a wrapper around the module tree. Generated store types are addressed as
`my_app::stores::cart_store::CartStore`, not
`musubi::my_app::stores::cart_store::CartStore`.

Deliberately **not** added in v1: crate name, edition, `Cargo.toml` generation,
`rustfmt` invocation, feature flags. See [Out of scope](#8-out-of-scope-for-v1).

### 2.4 Wiring

Consumers:

```elixir
compilers: Mix.compilers() ++ [:musubi_ts, :musubi_rust]
```

Either target can be enabled alone; both read the same manifest.

Musubi's own repo:

- `mix.exs` `aliases/0` `precommit:` gains `"compile.musubi_rust --check"`
  directly after `"compile.musubi_ts --check"`.
- `docs` `groups_for_modules` `Codegen:` gains `Mix.Tasks.Compile.MusubiRust`.
- `docs_modules/0` gains the same module.
- `skipped_doc_references/0` — no change needed beyond the manifest rename;
  `Musubi.Codegen.Rust` and `Musubi.Codegen.Rust.TypeRenderer` are
  `@moduledoc false` internals reached only through the Mix task, so add
  `"Musubi.Codegen.Rust"` alongside the existing entries if they end up
  cross-referenced.
- `config/test.exs` gains
  `config :musubi, :rust_codegen_output_path, "test/tmp/musubi_rust_bundle.rs"`
  with the same explanatory comment as the TS key.

---

## 3. Type mapping

### 3.1 The nominal-typing problem

TypeScript is structural: `{ street: string }` and `"a" | "b"` are types that
can be written inline anywhere. Rust is nominal — every anonymous map and every
non-trivial union must become a **named** `struct` or `enum` declared somewhere.
The Rust renderer therefore cannot be a pure `AST -> String` function like the
TS one. It is:

```elixir
@spec render(Macro.t(), ctx()) :: {String.t(), ctx()}
```

where `ctx()` carries the root module name, the current module depth (for
`super::` path prefixes), the hoist prefix (the name of the enclosing generated
item), and the accumulator of hoisted declarations plus the per-module name
table. `ctx()` is a plain map; the function stays pure and table-testable.

A convenience `render!/2` returning just the string is provided for the
non-hoisting cases so the primitive table test reads like the TS one.

### 3.2 Complete mapping table

`root` below is the configured root module reached through the depth-correct
prefix (see [4.3](#43-cross-module-path-resolution)); written `musubi::` here
for readability.

| Musubi field-type AST | TypeScript (today) | Rust |
| :-------------------- | :----------------- | :--- |
| `String.t()` / `binary()` / `string()` | `string` | `String` |
| `integer()` | `number` | `i64` |
| `float()` | `number` | `f64` — but `Musubi.Type` does not accept `float()` in a `state do` block today, so this row is reachable only through float literals |
| `boolean()` | `boolean` | `bool` |
| `atom()` | `string` | `String` |
| `:literal` (lone atom literal) | `"literal"` | hoisted single-variant enum (§3.4) |
| `true` / `false` | `true` / `false` | `bool` |
| `"str"` literal | `"str"` | `String` (value constraint lost; doc comment records it) |
| `1` / `1.0` literal | `1` / `1.0` | `i64` / `f64` (same) |
| `nil` (alone) | `null` | `()` (serde: deserializes only from JSON `null`) |
| `map()` | `Record<string, unknown>` | `serde_json::Map<String, serde_json::Value>` |
| `%{key: T, ...}` | `{ key: T }` | hoisted struct (§3.3) |
| `list(T)` | `T[]` | `Vec<T>` |
| `stream(T)` | `Musubi.StreamField<T>` | `Vec<T>` (see note below) |
| `T \| nil` (any arity, `nil` present) | `T \| null` | `Option<T'>` where `T'` is the remaining union (§3.4) |
| `T \| U` (no `nil`) | `T \| U` | hoisted enum (§3.4) |
| `Module.t()` | full alias path | path to the generated struct, e.g. `my_app::states::LineItemState` |
| `Module.state()` | `Musubi.StoreField<"Full.Module">` | `musubi::StoreField<my_app::stores::cart_store::State>` |
| `Musubi.AsyncResult.of(T)` | `Musubi.AsyncField<T>` | `musubi::AsyncResult<T>` |
| any other `X.of(T)` | `unknown` | `serde_json::Value` |
| anything unrecognized | `unknown` | `serde_json::Value` |

Notes on the scalar choices:

- **`stream(T)` renders as `Vec<T>`, not a marker type.** The wire node is
  `{"__musubi_stream__": "<name>"}`, but the client runtime's hydration pass
  (`docs/rust-client.md` §4.6) substitutes the materialized JSON array for that
  marker *before* deserialization, so the marker never reaches serde. There is
  no `StreamField<T>` type in either the prelude or the crate; a generated field
  declared `stream(MessageState.t())` is
  `pub messages: Vec<super::MessageState>`. `stream_async` therefore renders
  `AsyncResult<Vec<T>>`, and the hydration pass resolves the marker nested
  inside the async node's `result`.
- **`i64` / `f64` are fixed in v1.** Elixir integers are arbitrary precision, so
  a value above `2^63-1` fails to deserialize. Accepted: the JSON wire is
  already consumed by JS clients where `Number.MAX_SAFE_INTEGER` bites first.
  If a consumer needs bignums, they declare the field `String.t()`.
- **`atom()` → `String`** for the same reason TS maps it to `string`: atoms
  serialize as strings at wire egress (`Musubi.Wire`, BDR-0029).
- **`map()` → `serde_json::Map<String, Value>`** rather than
  `HashMap<String, Value>` so key order round-trips when the consumer enables
  `serde_json/preserve_order`, and so the type reads as "arbitrary JSON object".
- **Fallback is total.** `render/2` never raises — mirroring the TS renderer's
  `unknown` catch-all — because a field type AST can contain operators, locals,
  or `unquote` artifacts that alias expansion deliberately preserves verbatim.
- **Clause ordering pitfall carries over verbatim:** the `String.t()` shortcut
  clause must precede the `%{}` literal-map clause, whose 3-tuple shape would
  otherwise capture it with `pairs = []`.

### 3.3 Hoisting anonymous maps

Every `{:%{}, _, pairs}` node becomes a named struct. This is the common case:
`Musubi.DSL.Schema.type_from_block/1` turns every inline `field :x do ... end`,
`stream :x do ... end`, and `stream_async :x do ... end` block into a `%{}` AST
node, so nested inline blocks hoist rather than being written inline as TS does.

```elixir
state do
  field :address do
    field :street, String.t()
    field :zip, String.t()
  end
end
```

```rust
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CartStateAddress {
    pub street: String,
    pub zip: String,
}
```

and `CartState { pub address: CartStateAddress }`.

Binary map keys (`%{"kebab-key" => T}`) become snake_case idents with an
explicit `#[serde(rename = "kebab-key")]`.

### 3.4 Hoisting unions

Processed in this order:

1. **Flatten.** `{:|, _, [l, r]}` is left-nested; flatten to a list of arms.
2. **Strip `nil`.** If any arm is `nil`, remove it and wrap the result in
   `Option<_>`. `String.t() | nil` ⇒ `Option<String>` — no enum. A union of
   three arms including `nil` ⇒ `Option<HoistedEnum>` over the other two.
   Plain `Option<T>` is correct without `#[serde(default)]` because the server
   always renders every declared key; a missing key is a server bug and should
   fail loudly.
3. **Single arm left** ⇒ render it directly (possibly inside `Option`).
4. **All remaining arms are atom literals** ⇒ **C-like enum**:

   ```elixir
   field :status, :idle | :running | :"needs-review"
   ```

   ```rust
   #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
   pub enum CartStateStatus {
       #[serde(rename = "idle")]
       Idle,
       #[serde(rename = "running")]
       Running,
       #[serde(rename = "needs-review")]
       NeedsReview,
   }
   ```

   Always emit an explicit per-variant `#[serde(rename = "...")]` rather than a
   container `rename_all`. Atoms carry arbitrary characters (`:"needs-review"`,
   `:"with space"`) and `rename_all = "snake_case"` would silently mis-map them.
   Explicit renames are also self-documenting in the generated file.

5. **All remaining arms are `%{}` maps sharing a discriminant key** — one key
   `K` present in every arm whose value in every arm is a *distinct atom
   literal* ⇒ **internally tagged enum**:

   ```elixir
   field :status, %{type: :active} | %{type: :paused, value: integer()}
   ```

   ```rust
   #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
   #[serde(tag = "type")]
   pub enum ProbeStatus {
       #[serde(rename = "active")]
       Active,
       #[serde(rename = "paused")]
       Paused { value: i64 },
   }
   ```

   Internally tagged, **not** adjacently tagged: the wire shape is
   `{"type": "paused", "value": 3}` — the tag is a sibling key of the payload,
   not `{"t": ..., "c": {...}}`. If more than one key qualifies as a
   discriminant, pick the first in declaration order of the *first* arm
   (deterministic).

   Serde restriction to respect: internally tagged enums cannot deserialize from
   non-self-describing formats and cannot have newtype variants wrapping
   non-struct types. Both hold here — every variant is a struct or unit variant.

6. **Otherwise** ⇒ **`serde_json::Value`**, the same total fallback the §3.2
   table already uses for unrecognized ASTs (and the analogue of the TS target's
   `unknown`). A heterogeneous, undiscriminated union has no faithful nominal
   Rust encoding: `#[serde(untagged)]` is first-match-wins, so structurally
   overlapping arms silently mis-resolve, and the variant-naming rules needed to
   make it deterministic are the most fragile part of the renderer for a case
   the DSL barely produces (every union in this repo's examples and probe
   fixtures is case 4 or case 5). Emit a doc comment on the field recording the
   declared arms so the information is not lost. Revisit if a real consumer
   declares such a union.

7. **Literal unions of non-atoms** collapse: a union whose remaining arms are
   all binary literals ⇒ `String`; all integer literals ⇒ `i64`; all float
   literals ⇒ `f64`. Rust has no literal types and a `#[serde(untagged)]` enum
   of identically-typed newtype variants is degenerate. Emit a doc comment on
   the field recording the allowed values.

Arms that are themselves anonymous maps hoist as nested types named
`<EnumName><VariantName>` when they need their own struct — which they do not in
case 5, where **struct variants are inlined into the enum**
(`Paused { value: i64 }`, never `Paused(PausedPayload)`). Inline struct variants
are load-bearing: they are what lets a consumer write
`match st { …::Ok { id } => … }`.

### 3.5 Deterministic hoisted-name scheme

**Rule.** `hoisted_name = <EnclosingItemName> <> PascalCase(path segments)`.

- **Enclosing item name** is the name of the generated Rust item currently being
  rendered — `CartState` for a state module, `Checkout` for a command payload
  struct, `CheckoutReply` for its reply, `ToastPayload` for an event payload.
  **One exception:** a store's shape struct is named `State` (§4.6), which is
  useless as a prefix and would collide across every store in the bundle, so a
  store's hoisted types are prefixed with the **store marker name** instead —
  `MyApp.Stores.CartStore` ⇒ `CartStoreAddress`, not `StateAddress`.
- **Path segments** are field names and map keys, PascalCased and concatenated,
  outermost first.
- **Wrappers are transparent.** `list(T)`, `stream(T)`, `Musubi.AsyncResult.of(T)`,
  and the `Option` produced by `nil`-stripping contribute no segment, because
  each has exactly one type argument — the enclosing field name already
  identifies it uniquely. `list(list(%{...}))` under `field :grid` still yields
  `CartStateGrid`.
- **Union arms are not transparent.** A hoisted arm is `<EnumName><VariantName>`.

Examples:

| Declaration site | Hoisted name |
| :--------------- | :----------- |
| `CartState`, `field :address do ... end` | `CartStateAddress` |
| `CartState`, `field :lines, list(%{sku: String.t()})` | `CartStateLines` |
| `CartState`, `field :status, :idle \| :busy` | `CartStateStatus` |
| `CartState`, `field :meta do field :tags do ... end end` | `CartStateMetaTags` |
| store `MyApp.Stores.CartStore`, `field :address do ... end` | `CartStoreAddress` |
| command `:checkout`, payload `field :mode, :fast \| :slow` | `CheckoutMode` |
| command `:checkout`, reply `field :bucket do ... end` | `CheckoutReplyBucket` |
| event `:toast`, payload `field :level, :info \| :warn` | `ToastPayloadLevel` |

**Placement.** A hoisted type is emitted in the same Rust module as its
enclosing item, immediately after it, sorted by name. So `CartStateAddress`
sits next to `CartState` in `my_app::states`; `CartStoreAddress` and
`CheckoutMode` sit inside `my_app::stores::cart_store`.

**Collision rule (single policy, no hard error).** The name table is
per-Rust-module. Allocation order is fully deterministic: entries sorted by
`Module.split/1`, items within a module in a fixed order (marker, shape struct,
commands in declaration order, events in declaration order), fields in
declaration order, depth-first. If a name is already claimed by a *different*
type, append `2`, then `3`, and so on. Generated item names (`CartState`,
`CartStore`, `State`, `Checkout`, …) claim their slots first, so a hoisted type
can never shadow a top-level generated one, and silent shadowing is therefore
impossible. This is the **only** collision policy in the design: hoisted-name
collisions are never an `ArgumentError`. (The two structural conflicts that
*do* raise are unrelated: upload/state field-name collision and the §4.2 Rust
module-path collision.)

**No structural dedupe in v1.** Two identical `%{a: integer()}` shapes under two
different fields produce two distinct structs. Dedupe would be nicer output but
introduces a global structural-equality index whose naming ("which field wins
the name?") is not obviously deterministic across manifest ordering. Revisit
after the first real consumer.

### 3.6 Rust identifiers

Field names come from Elixir atoms and are already snake_case, so the generated
ident is normally the atom verbatim and **no `#[serde(rename)]` is needed**.
Three exceptions:

1. **Raw-identifier keywords.** `type`, `move`, `ref`, `match`, `fn`, `use`,
   `mod`, `impl`, `where`, `loop`, `box`, `const`, `static`, `struct`, `enum`,
   `trait`, `let`, `if`, `else`, `for`, `while`, `return`, `break`, `continue`,
   `pub`, `unsafe`, `extern`, `as`, `in`, `true`, `false`, plus the
   edition-reserved `async`, `await`, `dyn`, `try`, `gen`, `become`, `do`,
   `final`, `macro`, `override`, `priv`, `typeof`, `unsized`, `virtual`, `yield`
   ⇒ emit `r#type`, `r#match`, … Serde's derive strips the `r#` prefix when
   deriving the wire name, so `r#type` serializes as `"type"` with **no**
   `rename` attribute. (`field :type, :a | :b` is common in discriminant maps —
   this path will be exercised immediately.)
2. **Keywords that cannot be raw:** `self`, `Self`, `super`, `crate`, and
   `_`. Emit `self_`, `super_`, `crate_`, … with an explicit
   `#[serde(rename = "self")]`.
3. **Non-ident keys** — only reachable through binary map keys
   (`%{"kebab-key" => T}`). Emit `Macro.underscore`-style sanitization
   (non-alphanumerics → `_`, leading digit prefixed with `_`) plus an explicit
   `#[serde(rename = "kebab-key")]`.

Type names (module segments) are already PascalCase in Elixir and are used
verbatim, with one guard: `Self` cannot name a type, so a leaf segment that
would produce `pub struct Self` raises `ArgumentError`.

Module path segments are `Macro.underscore/1` of each segment **plus the same
escaping**, since `Macro.underscore("Match")` is the Rust keyword `match`:

- a segment underscoring to a raw-able keyword is emitted as `r#match`,
  `r#type`, … — both in its own `pub mod` item and in every `super::`-chained
  cross-reference (§4.3), so `MyApp.Match.CartStore` becomes
  `pub mod my_app { pub mod r#match { pub mod cart_store { … } } }`;
- a segment underscoring to one of the four keywords that cannot be raw
  (`self`, `Self`, `super`, `crate`) or to `_` raises `ArgumentError`, next to
  the path-collision guard of §4.2. Silently rewriting a module path is worse
  than asking for the Elixir module to be renamed, because the path is
  consumer-visible.

---

## 4. Emission shape

### 4.1 File skeleton

One file, deterministic byte-for-byte, rustfmt-stable by construction (the
generator emits already-formatted output; `cargo fmt` is never invoked).

```rust
// Generated by `mix compile.musubi_rust`. Do not edit by hand.
// Include as a module file (`mod generated;`) or via `include!`.

#![allow(clippy::all, dead_code, unused_imports)]

// Prelude: re-exports only. The shared runtime types are owned by the
// client crate (`:rust_codegen_runtime_path`, default `musubi_client`).
pub mod musubi {
    pub use ::musubi_client::generated::{
        AsyncError, AsyncResult, Command, Event, NoReply, Store, StoreField, StoreId, UploadSlot,
    };
}

pub mod my_app {
    pub mod states {
        pub struct CartState { ... }
        pub struct CartStateAddress { ... }
    }
    pub mod stores {
        pub mod cart_store { ... }
    }
}
```

The `#![...]` inner attribute means the bundle must be included as a module file
(`src/generated.rs` + `mod generated;`) or via `include!`, which is what the
second header line records. The prelude module exists so the rest of the bundle
can name the shared runtime types through one depth-correct `super::`-chained
path (§4.5).

Order: header comment, inner attributes, prelude module, then the module tree
sorted by segment (`Enum.sort_by/2` on segment strings, same as
`emit_state_tree/3`). No `use` statements at file scope.

**Prelude merge.** When a top-level Elixir segment snake_cases to the configured
`:rust_codegen_root_module` **and** itself emits a `pub mod` — the case Musubi's
own `Musubi.*` fixtures hit — the re-export is emitted as the **first item of
that module** and no sibling prelude module is written. Two `pub mod musubi`
items at one level are `error[E0428]`, so the merge is not optional. The trigger
is exactly `snake_case(top_segment) == root_module and emits_module?`; a leaf
`kind: :state` module emits only a struct, and struct and module names live in
different Rust namespaces, so it never triggers. Resolved paths are unchanged
either way: cross-references stay `super::…::musubi::AsyncResult`, because the
prelude sits at the same depth in both shapes. §4.5's "exactly one item, emitted
verbatim and unconditionally" describes the re-export, not the wrapper.

**Line width.** "Already formatted" means formatted the way rustfmt would: a
field whose line would exceed rustfmt's 100-column `max_width` is emitted in
rustfmt's next shape — the type alone on the following line at `indent + 4` if
it fits there, otherwise the outermost generic argument list wrapped as
`Outer<\n<indent+4>Arg,\n<indent>>`. Without this the two gates fight: a
consumer running `cargo fmt` reformats the bundle, and
`mix compile.musubi_rust --check` then reports drift.

### 4.2 Module tree

Two rules, by manifest `:kind`:

- **`kind: :state`** — mirrors the TS namespace tree exactly, one substitution:
  TS namespace ⇒ Rust `pub mod` with a snake_case name; TS
  `interface <LastSegment>` ⇒ Rust `pub struct <LastSegment>` **in the parent
  module**. `MyApp.States.CartState` ⇒ `my_app::states::CartState`.
- **`kind: :store`** — the module always gets its own `pub mod`, because a store
  emits several items that need a namespace of their own.
  `MyApp.Stores.CartStore` ⇒ `pub mod my_app::stores::cart_store` containing
  the marker `pub struct CartStore;`, the shape `pub struct State`, the command
  payload/reply structs, the event payload structs, and every hoisted type
  those need (§4.6). A store is therefore **never** a bare struct at
  `my_app::stores::CartStore`.

The `:state` table below:

| Elixir module | TS | Rust |
| :------------ | :- | :--- |
| `MyApp.States.CartState` (leaf) | `declare namespace MyApp { namespace States { interface CartState {...} } }` | `pub mod my_app { pub mod states { pub struct CartState {...} } }` |
| `MyApp.States.Cart` **and** `MyApp.States.Cart.Item` | `interface Cart` + `namespace Cart { interface Item }` | `pub struct Cart` + `pub mod cart { pub struct Item }` in `my_app::states` |

Rust's separate type and value/module namespaces plus the casing difference mean
`struct Cart` and `mod cart` coexist without conflict — the "leaf with children"
case needs no special handling beyond emitting both.

`namespace_keyword/1`'s depth-0 special case (`declare namespace` vs
`namespace`) has no Rust analogue: every level is `pub mod`.

**New validation.** Two checks, because two different things can collide:

1. `validate_no_module_path_collisions!/1` raises `ArgumentError` when two
   Elixir **entry** modules underscore to the same Rust path (`MyApp.FOO` and
   `MyApp.Foo` both → `my_app::foo`... and the leaf structs `FOO` and `Foo` also
   collide only by case). Mirrors the existing `validate_no_duplicates!/1`.
2. `validate_no_sibling_module_collisions!/2` walks the built tree and raises
   when two **siblings that each emit a `pub mod`** — an intermediate namespace,
   a `kind: :store` leaf, or a `kind: :state` leaf with children — share a Rust
   module name. The full-path check cannot see this case: with
   `MyApp.Foo.Bar` (state) and `MyApp.FOO` (store) no two entry paths are equal,
   yet the bundle carries two `pub mod foo` items inside `pub mod my_app`.

Both are defensive and unreachable with idiomatic module names, but the failure
mode otherwise is a `cargo build` error in a generated file the consumer did not
write.

### 4.3 Cross-module path resolution

A struct nested `d` modules deep that references the prelude or another
generated struct emits a `super::`-chained path:

```elixir
def root_path(depth, root), do: String.duplicate("super::", depth) <> root
```

so `my_app::states::CartState` (depth 2) writes `super::super::musubi::AsyncResult<T>`
and `super::super::my_app::states::LineItemState`. This is the `prost` approach:
fully deterministic and independent of where the consumer mounts the bundle in
their crate.

Alternative considered and deferred: a `:rust_codegen_absolute_path` config
(e.g. `"crate::musubi_gen"`) producing absolute paths instead of `super::`
chains. Nicer to read, but requires the consumer to keep the config in sync with
their file layout. Not in v1.

### 4.4 Derives

Every generated struct and enum:

```rust
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
```

- **`Eq` / `Hash` are omitted** — `f64` fields make them impossible in general,
  and deriving conditionally would make the output non-uniform.
- **No `rename_all`.** Wire keys are already snake_case because they come from
  Elixir atoms. The only renames are the three ident exceptions in §3.6 and the
  atom-literal variant renames in §3.4.
- **No `deny_unknown_fields`.** Store nodes carry `__musubi_store_id__` and
  future wire keys; strict structs would break on the very first store node.
- **No `#[serde(default)]` in v1.** The server renders every declared key; a
  missing key should fail loudly rather than silently produce a zero value.
- Fully-qualified `serde::Serialize` paths avoid requiring a `use serde::...;`
  in the consumer's including module.

### 4.5 Prelude — re-exports, not definitions

**Decision: the client crate owns every shared runtime type; the bundle
re-exports them.** The alternative (a self-contained prelude defining its own
`StoreId`, `AsyncResult<T>`, `Store`, …) cannot work: a bundle-local
`trait Store` is a *different* trait from `musubi_client::generated::Store`, so
`Connection::mount::<CartStore>()` would not compile. The same argument rules
out a crate-side *sealed* `Store` trait — a sealed trait cannot be implemented
by a file generated into a consumer crate — so there is no `sealed` module.

`pub mod <root_module>` therefore contains exactly one item, emitted verbatim
and unconditionally, with `<runtime_path>` from `:rust_codegen_runtime_path`
(default `musubi_client`). "Unconditionally" is about the item: the *wrapper* is
the generated top-level module of the same Rust name when there is one, per the
prelude-merge rule in §4.1.

```rust
pub use ::musubi_client::generated::{
    AsyncError, AsyncResult, Command, Event, NoReply, Store, StoreField, StoreId, UploadSlot,
};
```

That list is normative and is mirrored verbatim in `docs/rust-client.md` §8.2.
The crate-side definitions (`docs/rust-client.md` §6.1, §7) are the single
source of truth for their shapes; reproduced here only for the reader:

```rust
/// Server-authored store path (root = empty). Newtype, `#[serde(transparent)]`;
/// `StoreId::root()` constructs the root path.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct StoreId(Vec<String>);

/// A mounted child store: the wire node carries `__musubi_store_id__`
/// alongside the child's own rendered fields.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StoreField<S> {
    #[serde(rename = "__musubi_store_id__")]
    pub store_id: StoreId,
    #[serde(flatten)]
    pub state: S,
}

/// An upload slot. The wire node is `{"__musubi_upload__": "<name>"}`.
/// Inert in v1 — see §8.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct UploadSlot {
    #[serde(rename = "__musubi_upload__")]
    pub name: String,
}

/// The reply type generated for a command that declares no `reply do` block.
/// `{:noreply, socket}` replies `{}` on the wire, so this deserializes from
/// any object and carries nothing.
#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub struct NoReply {}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum AsyncResult<T> {
    Loading { result: Option<T>, reason: Option<AsyncError> },
    Ok { result: T, reason: Option<AsyncError> },
    Failed { result: Option<T>, reason: Option<AsyncError> },
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum AsyncError {
    Structured { kind: AsyncErrorKind, value: serde_json::Value },
    Opaque(serde_json::Value),
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AsyncErrorKind { Error, Exit }
```

Three concrete Rust details this pins down:

- **`AsyncResult` keeps the wire field names `result` / `reason`**, rather than
  the TS client's app-facing `data` / `error` normalization. Keeping them means
  the derive works as-is with no hand-written `Deserialize`, and the three
  variants line up 1:1 with `lib/musubi/async_result.ex`'s
  `%AsyncResult{status, result, reason}`. Every variant carries `reason`,
  including `Ok`, because the wire always renders the key (as `null`); consumers
  matching only on `result` write `Ok { result, .. }`.
- **`AsyncResult` drops `__musubi_async__`.** The wire carries
  `{"__musubi_async__": true, "status": ..., "result": ..., "reason": ...}`.
  An internally-tagged enum on `status` ignores the extra key on deserialize.
  The marker exists so a *dynamically typed* client can detect async nodes; a
  Rust client knows statically from the field's declared type. Consequence:
  serializing an `AsyncResult` back out omits `__musubi_async__` — acceptable,
  because state never travels client→server (only command payloads do, and
  those are separate types).
- **`StoreField<S>` uses `#[serde(flatten)]`**, which is strictly better than
  the TS phantom-type encoding: TS carries `StoreField<"MyApp.CartStore">` and
  looks the shape up in the `Stores` interface at type level, whereas Rust
  actually deserializes the child's fields in place. `store_id` therefore lives
  on the wrapper, never as a hand-declared field on a generated `State` struct —
  and it cannot collide with a declared field anyway, because
  `Musubi.DSL.Field.validate_reserved!/1` already raises at `state do` expansion
  time for any field name starting with `__musubi_`.

**Uploads: only the slot type.** The full TS upload family (`UploadConfig`,
`UploadEntryStatus`, `UploadStatus`, `UploadError`, `UploadEntry`,
`UploadHandle`) is **not** emitted in v1. The client crate defers the upload
engine wholesale (`docs/rust-client.md` §10), so nothing would ever deserialize
those types. An upload field renders as `musubi::UploadSlot`; only the upload
*name* reaches the bundle. See §8.

### 4.6 Store registry — the crate's `Store` / `Command` / `Event` traits

TS's `interface Stores` keyed by module-name string literals, with phantom
`StoreDef<Module, Shape, Commands, Events>`, is a type-level lookup table with
no Rust equivalent. Rust gets traits instead — and per §4.5 those traits are
**defined in the client crate**, not in the bundle. `docs/rust-client.md` §7 is
the source of truth; reproduced verbatim:

```rust
pub trait Store: Send + Sync + 'static {
    const MODULE: &'static str;
    type State: serde::de::DeserializeOwned + Send + Sync + 'static;
}

/// One implementation per declared command, generic over the owning store so
/// `Mounted::<St>::command::<C: Command<St>>` type-checks the pairing.
pub trait Command<S: Store>: serde::Serialize + Send + 'static {
    const NAME: &'static str;
    type Reply: serde::de::DeserializeOwned + Send + 'static;
}

/// One implementation per declared push event (BDR-0032), on the payload type.
pub trait Event<S: Store>: serde::de::DeserializeOwned + Send + 'static {
    const NAME: &'static str;
}
```

There is no `type Params`, no `type Commands`, no `type Events`, and no
`STORES` const:

- **`type Params` is not generable.** Mount params are declared with `attr/3`
  and reflected through `__musubi__(:attrs)`, which the shared manifest does not
  carry (`:module, :kind, :fields, :commands, :events, :uploads, :source`).
  `Connection::mount` therefore takes an untyped
  `serde_json::Map<String, serde_json::Value>`, matching the TS target, which
  has no params typing either. Adding `:attrs` to the manifest and generating a
  params struct is recorded as future work in §8.
- **No `Command` / `Event` sum enums.** Nothing consumes them: the client
  dispatches typed payload structs (`mounted.command(Checkout { .. })`) and
  routes events by `(store_id, name)` into a per-event payload type. A sum enum
  would be emitted and never deserialized.
- **No `pub const STORES`.** It was justified as a mount allowlist, but §5 says
  root-ness is deliberately not inferred, so it lists roots and children alike
  and cannot validate anything. The server already rejects non-roots with
  `"declared store is not a root store"`.

Per store module `MyApp.Stores.CartStore` the generator emits into
`my_app::stores::cart_store` (`R = super::super::super::musubi`, the
depth-correct prelude path from §4.3):

```rust
/// Zero-sized marker type implementing `Store`. Distinct from `State`.
pub struct CartStore;

impl R::Store for CartStore {
    const MODULE: &'static str = "MyApp.Stores.CartStore";
    type State = State;
}

/// The store's rendered shape: state fields plus one `UploadSlot` per
/// declared upload. Reached as `<CartStore as Store>::State`.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct State {
    pub title: String,
    pub avatar: R::UploadSlot,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Checkout { pub coupon: Option<String> }

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CheckoutReply { pub order_id: String }

impl R::Command<CartStore> for Checkout {
    const NAME: &'static str = "checkout";
    type Reply = CheckoutReply;
}

/// Push event payload (BDR-0032).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ToastPayload { pub message: String }

impl R::Event<CartStore> for ToastPayload {
    const NAME: &'static str = "toast";
}
```

Design points:

- **Marker and shape are two types.** `CartStore` is the `St: Store` parameter
  (`Mounted<CartStore>`); `State` is what a snapshot holds
  (`Arc<<CartStore as Store>::State>`). They are never the same type.
- **`store_id` is not part of any generated struct or command payload.** For
  commands it is transport framing, filled by the client runtime from the store
  handle the command was dispatched on. For child stores it lives on
  `musubi::StoreField<S>` (§4.5), which is how
  `mounted.command_on(&snap.checkout_panel.store_id, Pay { .. })` reaches it.
- **Empty command payload** (`command :refresh` with no `payload do` block) ⇒
  `pub struct Refresh {}` — a unit-like struct with braces, so it serializes as
  `{}`, matching TS's `payload: {}`.
- **Empty reply** ⇒ TS emits `reply: never`. Rust emits
  `type Reply = musubi::NoReply;` — the crate-provided permissive struct that
  deserializes from `{}`, which is what `{:noreply, socket}` actually replies.
  A deliberate divergence from TS's `never`, and more accurate to the wire.
- **Event payload naming.** The struct is `<PascalCase(name)>Payload`
  (`ToastPayload`), matching the hoist prefix in §3.5, and the trait impl is on
  that struct. `mounted.events::<ToastPayload, _>(&store_id)` is the call.
- **Store shape = state fields ++ upload fields**, with the same
  `ensure_no_state_upload_collision!/3` guard raising `ArgumentError` on a name
  clash. Only the upload *name* reaches the bundle; `accept` / `max_entries` /
  `max_file_size` / `chunk_size` / `chunk_timeout` arrive at runtime via the
  `config` upload op.
- **Store modules appear in the module tree**, unlike TS where a store's shape
  is inlined into the `Stores` interface and no namespace is emitted. Rust needs
  nominal types anyway, so stores and states share one tree; the `Store` impls
  reference them. Documented divergence.
- **Field doc comments.** TS renders `/** doc */` for command and event fields
  but not for state fields. Mirror that asymmetry exactly (`///` doc comments on
  command/event payload/reply fields from `Keyword.get(opts, :doc)`, nothing on
  state fields) so the two targets stay diffable; fixing the asymmetry is a
  separate change that should land in both.

### 4.7 Wire contract the generated types must match

The generator is only correct if the emitted types deserialize the actual wire.
Anchors (see `docs/client-contract.md`, `packages/client/src/types.ts`,
`lib/musubi/page/patch_envelope.ex`):

- Store node ⇒ any object with `"__musubi_store_id__": [String]`; root is `[]`
  (`StoreField<S>` + flatten).
- Stream slot ⇒ `{"__musubi_stream__": "<name>"}`, exactly one key. Resolved by
  the client runtime's hydration pass, **not** by serde: the marker is replaced
  with the materialized array before deserialization, so the generated field
  type is `Vec<T>` (§3.2). Contents arrive out-of-band in `stream_ops`, never in
  the JSON Patch `ops` (BDR-0014 restricts `ops` to add/remove/replace over the
  state tree).
- Upload slot ⇒ `{"__musubi_upload__": "<name>"}`, auto-injected at the store's
  render root (BDR-0024), deserialized into the inert `musubi::UploadSlot`.
  Contents arrive in `upload_ops` (BDR-0025) and are discarded in v1.
- Async node ⇒ `{"__musubi_async__": true, "status", "result", "reason"}`
  (`AsyncResult<T>`), with `result` resolved recursively — it may itself be a
  stream marker, a store node, an array, or a plain object.
- Envelope framing (`type`, `base_version`, `version`, `ops`, `stream_ops`,
  `upload_ops`, `events`), reply-before-patch ordering (BDR-0009),
  idle-cycle-emits-nothing (BDR-0018), reconnect-only recovery (BDR-0015),
  the upload sub-channel and stateless token (BDR-0026) are **runtime**
  concerns. The generator emits none of them; a Rust client runtime owns them.
  Listing them here only to draw the boundary.

---

## 5. What the generator does *not* infer

- `:input` modules. `Musubi.DSL.Input.input/1` does not attach the codegen
  plugin, so `kind: :input` is never stamped. Unchanged.
- Stream options (`item_key`, `limit`). They live in the `stream` node's AST
  metadata and drive server-side flushing plus client-side trimming; the
  generated type is just `Vec<T>`. Same as TS.
- Upload config values (§4.6).
- Root-ness (`root: true`). The mount allowlist is a socket-level server concern
  and the server already rejects non-roots with
  `"declared store is not a root store"`. Nothing in the bundle enumerates or
  gates on it (there is no `STORES` const).
- Mount params (`attr/3`). Not in the shared manifest; `mount` takes an untyped
  JSON object (§4.6).

---

## 6. Test coverage

Four suites, mirroring the TS ones file-for-file.

### 6.1 `test/musubi/codegen/rust/type_renderer_test.exs` (`async: true`)

A pure table test over `Musubi.Codegen.Rust.TypeRenderer`, grouped
`primitives / literals / containers / unions / module references / hoisting /
identifiers` — the TS renderer's groups plus the two Rust needs. Simple rows
assert the rendered string:

```elixir
assert render!(quote(do: String.t())) == "String"
assert render!(quote(do: list(String.t()))) == "Vec<String>"
assert render!(quote(do: String.t() | nil)) == "Option<String>"
assert render!(quote(do: stream(String.t()))) == "Vec<String>"
```

Hoisting rows assert the returned pair — the rendered reference *and* the
accumulated declarations — and cover depth-first nesting, name-transparent
wrappers (`stream`, `AsyncResult`), siblings descending from the enclosing name
rather than from each other, and the §3.5 numeric-suffix collision policy in
allocation order. Every §3.4 union branch has a row: nil-stripping, atom enums
with per-variant renames, internally tagged maps (including which key wins as
discriminant and which candidate keys disqualify), literal collapse, and the
`serde_json::Value` total fallback. `depth` and the `:root_module` override are
exercised on the cross-module `super::`-chain rows.

`test/musubi/codegen/rust/names_test.exs` is the companion table test for
`Musubi.Codegen.Rust.Names`: raw-ident keyword escaping (`type` ⇒ `r#type`, no
rename; non-raw-able keywords ⇒ trailing underscore plus
`#[serde(rename = ...)]`), variant PascalCasing, module/struct path derivation,
`hoisted_name/2`, and `allocate/2`. Module `@doc` examples are doctested per
AGENTS.md.

### 6.2 `test/musubi/codegen/rust_test.exs` (`async: true`)

The golden-string bundle test. Expected output is built from a `@preamble`
heredoc (plus a `@merged_preamble` variant for the `Musubi.*` probe fixtures,
whose own top-level `pub mod musubi` merges with the prelude) and compared
through a `normalize/1` that strips leading whitespace per line, so assertions
bind content and ordering rather than indentation. Entries come from the
`__env__/0` fixture trick — `Manifest.collect(module.__env__())` — so real alias
expansion is exercised with zero disk I/O.

Covered: the empty render (header + prelude only), a `Musubi.State` entry as a
bare struct with hoisted types sorted by name, a leaf state module that is both
a struct and a same-named module, a `Musubi.Store` entry as its own module
carrying marker + `Store` impl + `State`, stream/async/child-store projection,
`Module.t()` on a store resolving to its `State` rather than the marker,
internally tagged enums, commands (empty payload as a braced struct, empty reply
as `musubi::NoReply`, and a declared `reply do` wired through `type Reply`),
push events (a `<Name>Payload` struct per event implementing `Event`), and
uploads (inert `UploadSlot` fields, plus `assert_raise ArgumentError` on a name
colliding with a state field).

Bundle invariants have their own group: input order doesn't change output,
duplicate entries for one module render once, the only `use` is the prelude
re-export, no `crate::`-absolute paths, no container renames / strict structs /
TS-only markers, and every atom-literal variant carries an explicit
`#[serde(rename = "...")]` (§3.4 — there is no `rename_all` anywhere). The
`render/2` options group pins `:root_module` and `:runtime_path` retargeting,
and a module-path-collision group pins the §4.2 guard (two modules underscoring
to one path, an intermediate segment colliding with another module's own
segment, a keyword segment emitted as a raw identifier, and a keyword that
cannot be raw ⇒ raise).

Two formatting assertions back §4.1's "rustfmt-stable by construction" claim:
no emitted `pub` line exceeds rustfmt's 100-column `max_width`, and — where
`rustfmt` is on `PATH` — the rendered bundle survives `rustfmt --edition 2024
--check` with no diff.

The fixtures in `test/support/typespec_probe.ex` carry the hard cases:
`stream/2` with a module item, an inline `AsyncResult.of(stream(...))`, a union
of two discriminated maps, `Child.state()`, `list(String.t())`, a single-segment
alias exercising expansion, an upload, a `:type`-keyword field name with an
inline nested block, and command fixtures with and without a `reply do` block —
the last of these being one of the two places Rust deliberately diverges from TS
(`NoReply` vs `never`).

No fixture uses the `stream_async` macro and none is needed:
`lib/musubi/dsl/schema.ex` `async_stream_type/2` expands it to the same
`Musubi.AsyncResult.of(stream(...))` AST the existing fixture writes by hand, so
the path is covered by AST equivalence.

### 6.3 `test/mix/tasks/compile/musubi_rust_test.exs` (`async: false`)

The Mix-task integration test: a fresh write covering every stamped module,
`--check` drift under the `musubi_rust` compiler name (asserting `severity`,
`compiler_name`, `file`, and that the stale file is untouched), and the
empty-manifest guard of §2.2. The `:noop` / drift / `manifests/0` / `clean/0`
plumbing itself lives in `Musubi.Codegen.Compiler` and is covered once, in
`test/mix/tasks/compile/musubi_ts_test.exs`.

`async: false` is required only because the manifest target dir is a process-dict
override and the output path is app env — both global. Per the global rule on
runtime config, neither task test calls `Application.put_env`: both read
`Application.fetch_env!(:musubi, :<target>_codegen_output_path)`, whose value
lives in `config/test.exs` (§2.4). **Keep it that way** — a test that mutates
those keys makes every sibling suite order-dependent.

### 6.4 Manifest reuse test

`test/musubi/codegen/manifest_test.exs` (renamed from
`type_script/manifest_test.exs`) carries the single-stamp regression: drive
`__after_compile__/2` once against a tmp target, then assert that both
`Musubi.Codegen.TypeScript.render(Manifest.list(target))` and
`Musubi.Codegen.Rust.render(Manifest.list(target))` produce non-empty output
from that one `state.term`. That is what catches anyone re-introducing a
per-target stamp.

The rename left the rest of the manifest suite's coverage intact: idempotent
stamping, sorted `list/1`, corrupt-term skipping, missing-dir `[]` / `:ok`,
orphan-module sweeping, both `test/` skip variants, `renderable_fields/1`, and
alias expansion.

### 6.5 Compilation smoke test (still deferred)

The strongest test would be "does `cargo build` accept the bundle", against a
fixture crate carrying `serde` / `serde_json`. It is **not** implemented: it
would need `cargo` on the test machine, so it belongs behind a `@tag :rust`
excluded by default in `test_helper.exs`, and it is not part of CI. The
rustfmt check in §6.2 and the crate-side
`crates/musubi-client/tests/generated.rs` (which exercises the re-exported
runtime types the bundle depends on) are partial substitutes, not replacements —
neither type-checks generated code.

---

## 7. Divergences from the TS target (summary)

| Aspect | TS | Rust | Why |
| :----- | :- | :--- | :-- |
| Anonymous maps | inline `{ k: T }` | hoisted named struct | Rust is nominal |
| Unions | inline `A \| B` | hoisted named enum | Rust is nominal |
| `T \| nil` | `T \| null` | `Option<T>` | idiomatic; `null` is not a type |
| Store shape | inlined into `interface Stores` | real struct in the module tree | needed as a nominal type anyway |
| Store registry | `interface Stores` keyed by module string | `Store` / `Command<S>` / `Event<S>` traits owned by `musubi-client` | no type-level string-keyed lookup |
| Child store | phantom `StoreField<"Mod">` | `StoreField<S>` with `#[serde(flatten)]` | Rust actually deserializes it |
| Streams | `Musubi.StreamField<T>` phantom | `Vec<T>` | the client hydrates the marker before deserializing |
| Empty reply | `never` | `musubi::NoReply` | `{:noreply}` replies `{}` on the wire |
| Shared runtime types | emitted into the bundle | re-exported from `musubi_client::generated` | one trait, one `AsyncResult`, across bundle and crate |
| Async marker | `__musubi_async__` in the type | dropped (ignored on deserialize) | static typing makes detection unnecessary |
| Cross-refs | namespace lookup | `super::`-chained paths | no ambient namespace merging |
| Upload key casing | camelCase via renames | snake_case verbatim | wire is already snake_case |

---

## 8. Out of scope for v1

Scope is deliberately capped at "what `:musubi_ts` does, for Rust".

- **Any Rust client runtime.** No socket/channel layer, no JSON Patch
  application, no stream materialization, no upload transfer, no reconnect or
  version-mismatch recovery, no snapshot layer, no cache. That is
  `docs/rust-client.md`\'s subject, shipped as the separate `musubi-client`
  crate. This document only fixes the *type surface* that runtime deserializes
  into, plus the re-export seam to it (§4.5).
- **Crate scaffolding.** No `Cargo.toml`, no `build.rs`, no workspace wiring,
  no `cargo fmt` / `cargo check` invocation from the Mix task.
- **`no_std`, non-serde codecs, alternative JSON crates.** `serde` +
  `serde_json` only.
- **Structural dedupe of hoisted types** (§3.5).
- **Upload handle/config types.** The bundle emits only `musubi::UploadSlot`.
  `UploadConfig`, `UploadAccept`, `UploadEntry`, `UploadEntryStatus`,
  `UploadStatus`, `UploadError`, and the `UploadHandle` state machine are
  deferred to land with the client crate's upload engine
  (`docs/rust-client.md` §10), which v1 defers wholesale. Emitting them now
  would ship seven types nothing deserializes.
- **Typed mount params.** `attr/3` declarations are not in the shared manifest,
  so no `Params` struct is generated and `mount` takes an untyped JSON object
  (§4.6). Adding `:attrs` to `Manifest.collect/1` (and to the `@type entry()`)
  plus a generated params struct is the follow-up if typed mounts are wanted;
  the TS target has no params typing either, so it is also beyond parity.
- **Per-store `Command` / `Event` sum enums** and any `AnyStore` enum. Re-add
  only if a consumer needs exhaustive matching.
- **`:input` modules**, stream options, upload config values, root-ness (§5).
- **Literal-type fidelity** for binary/integer/float literals and lone atom
  literals outside enums (§3.2, §3.4 case 7).
- **Doc comments on state fields** — preserved as an intentional asymmetry with
  TS (§4.6).
- **Versioning / compatibility shims** between a generated bundle and a running
  server. Drift is caught by `--check` at build time, not at runtime.

---

## 9. Landing order (historical)

The feature shipped in seven steps, recorded here only because other documents
cite them by number (`docs/rust-gpui-example.md` §7 keys its D2 milestone to
steps 2–6). Nothing here is outstanding.

1. Rename commit (§1.2): `Musubi.Plugin.Codegen`, `Musubi.Codegen.Manifest`,
   subdir, process key, `renderable_fields/1` hoist, stale `@type entry()`
   fixes, `AGENTS.md` codegen bullets. TS behavior unchanged; TS tests updated
   in place.
2. `Musubi.Codegen.Rust.Names` + its table test.
3. `Musubi.Codegen.Rust.TypeRenderer` + its table test (hoisting context, all
   §3 rows).
4. `Musubi.Codegen.Rust` prelude + module tree + `Store` trait emission +
   golden bundle test.
5. `Mix.Tasks.Compile.MusubiRust` + integration test + `config/test.exs` key.
6. `mix.exs` wiring (precommit, docs groups, `docs_modules/0`).
7. Manifest reuse regression test.
