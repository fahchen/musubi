# AGENTS.md

Server-authoritative, page-scoped runtime library for Elixir/Phoenix. One BEAM process per connected page owns a tree of stores; pushes RFC 6902 JSON Patch updates to the client.

## Stack

- Elixir 1.18+ (CI matrix tests 1.18 + 1.19; local dev uses 1.19 via `mise`)
- `{:typed_structor, "~> 0.6.1"}` — struct definitions and plugin-based metadata
- `{:jsonpatch, "~> 2.2"}` (corka149/jsonpatch) — RFC 6902 diff/apply
- `Phoenix.Channel` (transport, M4); `Phoenix.PubSub` is used by application code, not wrapped (BDR-0005)

## Source of truth

`spec/` wins when PRD and spec disagree. Order of authority:

1. `spec/domains/**/*.feature`
2. `spec/decisions/BDR-*.md`
3. `spec/glossary.md`
4. `docs/PRD.md` (informational)

If the spec feels wrong, flag the discrepancy in the PR — never silently edit `spec/` files.

## Rules

- **Never** add functions, modules, or delegations that are not yet used by any caller — introduce them when the first caller needs them
- Run `mix precommit` when done with all changes and fix any pending issues
- **Always** use `TypedStructor` for structs — never bare `defstruct`
- **Always** add `@doc` with examples to public functions. Use `iex>` for doctestable examples, otherwise plain code with `#=>` for return values
- **Always** use concrete types in specs — never `term()`, `any()`, or bare `atom()`. Error reasons should be specific atom unions, return values should name the actual struct/type
- **Always** add parentheses to `@type`/`@typep` definitions — `@type name() ::` not `@type name ::`
- **Always** use `@typep` for types only used within the module — never expose types without external callers
- **Always** add explanatory comments to module attributes, especially magic numbers and non-obvious constants
- **Always** use `params` as the parameter name for changeset/function input — never `attrs`
- **Always** use `JSON` module (Elixir 1.18+ stdlib) — never `Jason` for encode/decode
- **Always** use `System.fetch_env!` for required environment variables — never `System.get_env` with empty default for credentials
- **Lookup naming:** `get_` returns `Schema.t() | nil`, `fetch_` returns `{:ok, Schema.t()} | :error` — never mix the two
- **Function ordering:** public functions first, each followed immediately by its private helpers. If a private function serves multiple public functions, place it below all of them. Private functions ordered by call sequence
- Sibling files in a directory are named by responsibility — never by CRUD (`commands.ex`/`finders.ex`)
- **Always** use `async: true` in test modules — design code for concurrency. Never use `async: false` as a workaround for shared state
- **Never** modify implementation code solely to make tests pass. If a failure is confined to tests or test infrastructure, fix it in the test layer unless the user explicitly asks for a production behavior change
- **Never** use `Process.sleep` in tests — use built-in wait/retry mechanisms or assertions with timeouts
- **Test ordering:** test cases (`describe`/`test`) at the top, helper functions and setup at the bottom. Use `setup` and `@tag` to organize test preparation — avoid inline helper calls
- **Always** use pattern matching in test assertions — never `assert x.field == value`
- **Test final socket state, not intermediate process.** Drive the command, sync on a barrier (terminal patch envelope or `[:musubi, :async, :stop | :exception]` telemetry), then assert directly on `socket.assigns.<field>` by reading `:sys.get_state(pid)` and walking the store registry. Don't pin patch-envelope sequencing, telemetry interleaving, or tracking-private-key shape mid-flight — those couple the suite to runtime plumbing. Lifecycle assertions (loading visibility, demonitor-before-kill) belong in small focused tests whose stated subject IS the lifecycle, not as asides in happy-path tests.
- **Never** seed global/shared data in tests or `test/test_helper.exs` — each test must insert the rows it needs explicitly
- **Never** mutate runtime config keys that other tests read. `Application.put_env` IS allowed inside a single test's `setup_all` when the key is uniquely scoped to that test (e.g. `:musubi, TestModule.TestEndpoint`), since no sibling reads it; add a one-line comment at the call site noting the scoping. Shared / cross-test keys belong in `config/test.exs`
- **Never** force-push (including `--force-with-lease`). To address review feedback, stack a new commit on the branch tip — do not amend and force
- **Never** `git commit --amend` on a published commit — add a follow-up commit instead

## Musubi-specific contracts

- `%Musubi.Socket{}` field shape: `assigns`, `id`, `parent_path`, `module`, `endpoint`, `topic`, `transport_pid`, `private`. Mirrors `Phoenix.Socket`
- `socket.assigns.__changed__` records mutations as `%{key => true}`; `===` short-circuits no-op writes (BDR-0013)
- `socket.private[:hooks]` is reserved for `Musubi.Lifecycle` — write via `Musubi.Socket.put_private/3`
- Hook stages: `:before_command | :after_command | :handle_async | :handle_info | :after_render | :after_serialize` (BDR-0004)
- Hook fun arity is stage-dependent: `:before_command`/`:after_command`/`:handle_async` are arity 3, `:handle_info`/`:after_render`/`:after_serialize` are arity 2
- `:after_render` runs on the resolved Elixir term (atom keys, structs, atom values); `:after_serialize` runs on the wire term (string keys, plain maps, atoms-as-strings) produced by `Musubi.Wire.to_wire/1`
- Stores `use Musubi.Store` and implement `render/1`, `mount/1`, `handle_command/3`, optionally `handle_async/3` and `terminate/2`. The `@behaviour Musubi.Store` declaration gives compile-time enforcement. `render/1` returns the Elixir-shaped resolved term; wire conversion happens separately via `Musubi.Wire.to_wire/1`
- Hook return: `{:cont, socket} | {:halt, socket} | {:halt, reply, socket}`
- Child identity is `{parent_path, module, id}`; `id` must be a binary (BDR-0011)
- `Module.__musubi__/1` reflection keys: `:fields`, `:commands`, `:streams`, `:attrs`, `{:type, name}`. Singular variants `__musubi__/2` accept `:field | :command | :stream | :attr` plus a name and return `{:ok, def} | :error`
- Stream runtime helpers (compile-time generated alongside `__musubi__/1`): `Module.__musubi_stream_config__(name) :: %{item_key: fun, limit: integer | nil}` and `Module.__musubi_stream_item_key__(name, item) :: binary` — used by `Musubi.Stream` so callers never `Code.eval_quoted/3` the AST stored on `:streams`
- typed_structor evaluates field opts eagerly — `Musubi.DSL.State.field/3` wraps opts in `Macro.escape/1` so `item_key: &…` captures survive into reflection. Don't strip this
- Module-kind contract:

  | use directive    | role                   | DSL block | reflection callback           |
  | ---------------- | ---------------------- | --------- | ----------------------------- |
  | use Musubi.Store  | store + lifecycle      | state do  | __musubi_validate_state__/1    |
  | use Musubi.State  | render output type     | state do  | __musubi_validate_state__/1    |
  | use Musubi.Input  | input data type        | input do  | __musubi_validate_input__/1    |
- Stream API (LV-aligned, frozen for M4+): `Musubi.Stream.stream/3,4`, `stream_configure/3`, `stream_insert/3,4`, `stream_delete/3`, `stream_delete_by_item_key/3`. Note Musubi uses `_by_item_key` where LV uses `_by_dom_id`. The runtime queues raw delta ops on a per-stream `Musubi.Stream.Slot` struct — it does **not** maintain an ordered `item_keys` list, decide upsert-vs-insert, or trim for `:limit` (the client owns materialization). `stream_configure/3` is a lifetime gate: raises if called after the stream is initialized
- Reserved socket-assigns shape: `socket.assigns.__streams__ = %{__ref__: int, __changed__: MapSet, __configured__: %{name => opts}, <name> => %Musubi.Stream.Slot{}}`. The drained-ops accumulator lives at `socket.private[:__musubi_drained_stream_ops__]` (populated by the runtime drain step in `Musubi.Resolver.resolve/2`, consumed by `Musubi.Page.Server`). Do not read or write directly
- `Musubi.Stream.Slot` (typed_structor) is the per-stream pending struct: `name`, `item_key_fun`, `ref`, `inserts`, `deletes`, `reset?`. `Musubi.Stream.Slot.prune/1` clears `inserts`/`deletes`/`reset?` and is called once per cycle by the runtime drain step
- `Musubi.Stream.drain_and_prune/1` is a **runtime invariant** called by `Musubi.Resolver.resolve/2` after the `:after_serialize` lifecycle hooks fire. It drains pending ops from each stream slot marked changed into the per-socket accumulator, prunes the structs, and clears `__changed__`. **Not a removable hook** — required for every render cycle
- Stream wire-op shape (carried in `PatchEnvelope.stream_ops`): `%{op: "insert", stream: name_str, ref: ref_str, store_id, item_key, at, item, limit}`, `%{op: "delete", stream: name_str, ref: ref_str, store_id, item_key}`, `%{op: "reset", stream: name_str, ref: ref_str, store_id}`. `store_id` is the owning store path (`[]` for the root, child paths otherwise). `stream_configure/3` is server-side only — no configure op appears on the wire (the `item_key` capture is not transferable; per-insert `limit` carries what the client needs)
- `Musubi.Page.PatchEnvelope.t` shape: `type: "patch"`, `base_version`, `version`, `ops`, `stream_ops`. `version` is a monotonic per-page counter starting at 1 (initial bootstrap envelope) and resetting on reconnect (fresh page server). `ops` only carries `add`/`remove`/`replace` (BDR-0014); `move`/`copy`/`test` are filtered. Stream-typed paths never appear in `ops` — content flows entirely via `stream_ops`. Idle render cycles emit no envelope (BDR-0018)
- Telemetry events emitted by the runtime: `[:musubi, :command, :start | :stop | :exception]`, `[:musubi, :render, :stop]`, `[:musubi, :resolve, :stop]`, `[:musubi, :validate, :stop | :exception]`, `[:musubi, :diff, :stop]`, `[:musubi, :patch, :stop]`, `[:musubi, :stream, :flush]`, `[:musubi, :async, :start | :stop | :exception | :cancel | :lazy_discard]`
- Async API (LV-parity, frozen for M5+): `Musubi.Async.assign_async/3,4`, `Musubi.Async.start_async/3,4`, `Musubi.Async.cancel_async/2,3`, `Musubi.Async.stream_async/3,4`. `assign_async`/`stream_async` write `Musubi.AsyncResult` values into `socket.assigns` synchronously (loading) and on completion (ok/failed). `start_async` results route to the store's `handle_async/3` callback; `socket.assigns` is not mutated by the call itself
- `Musubi.AsyncResult` is a runtime struct `%{status: :loading | :ok | :failed, result: term, reason: nil | {:error, term} | {:exit, term}}`. `loading/0,1`, `ok/2`, `failed/2` are the canonical constructors; `loading/1`/`failed/2` preserve the prior result for stale-while-loading/failed UX. The struct serializes via a custom `Musubi.Wire` impl: status atom becomes a string, result recurses, reason becomes `nil` or `%{"kind" => "error" | "exit", "value" => ...}`
- `Musubi.AsyncResult.of(t)` is BOTH the compile-time field-type marker (used inside `state do`) and the runtime struct typespec
- Reserved socket-private key: `socket.private[:__musubi_async_refs__]` holds per-task tracking entries (`%{name => %{ref, pid, kind, keys, prior, timer_ref, cancel_reason, supervisor}}`). Runtime-internal; introspect via `Musubi.Async.tracking/1`
- `Musubi.AsyncSupervisor` is a `Task.Supervisor` started by `Musubi.Application`. Override per call with the `:supervisor` option to any of the async entry points
- BDR-0019: a second `start_async/3,4` with the same name silently overwrites the prior tracked ref. The older task continues running but its result is lazy-discarded on arrival (`[:musubi, :async, :lazy_discard]`)
- BDR-0020: `handle_async/3` exceptions are caught — diverges from BDR-0003 let-it-crash for command/render handlers. Failures emit `[:musubi, :async, :exception]` with `kind/reason/stacktrace` and the runtime continues. `socket.assigns` is not modified for that cycle
- `:timeout` (Musubi extension) terminates an overdue task with `failed(prior, {:exit, :timeout})`. `:reset` (`true` or a key subset) cancels the prior task and re-emits `loading()` for the listed keys
- `stream_async/3,4` requires a previously-declared `stream :name, ...` slot inside `state do` and raises `ArgumentError` otherwise. On success it atomically writes `AsyncResult.ok(prior, true)` AND seeds the stream slot in the same envelope; on `{:error, reason}` it writes `failed/2` and leaves stream contents untouched
- Compile-time lint: `Musubi.Async.Macros` wraps `assign_async`/`start_async` with `IO.warn` on socket capture inside the task fun. The walk only inspects literal `fn …` / `&…` AST so `start_async(socket, :foo, build_fn(socket))` (where `socket` flows through a helper) does not false-warn (LV-aligned). `cancel_async` is wrapped only as a delegate — no capture check, mirroring LV. Auto-imported by `Musubi.Store.__using__/1`. `stream_async` is intentionally not wrapped at the macro layer (the `state do` DSL exposes a same-named macro for declaring async-wrapped stream fields and Elixir cannot disambiguate same-name imports by argument count) — call the runtime form via `Musubi.Async.stream_async/3,4`
- Telemetry catalog: `Musubi.Telemetry.events/0` returns the canonical list of runtime telemetry event names. Adding a new runtime event MUST update the catalog (or the events test in `test/musubi/telemetry_test.exs` blocks the build). Adapter-scoped events (`[:musubi, :channel, :*]`) live on the adapter and are documented at the adapter's `@moduledoc`, not in the catalog
- Catch-all `handle_info/2` on `Musubi.Page.Server` dispatches to the root store's `handle_info/2` callback after running the `:handle_info` hook chain; the runtime emits `[:musubi, :pubsub, :receive]` for every dispatch (BDR-0005). PubSub is application-owned: stores call `Phoenix.PubSub.subscribe/2` directly inside `mount/1`
- Graceful denial via a `:before_command` hook returning `{:halt, reply, socket}` emits `[:musubi, :auth, :deny]` with `%{module, path, command, reply}` metadata (BDR-0008)
- Command replies leave the runtime (`Musubi.Page.Server.command/4`, `command_by_name/4`, `Musubi.Testing.dispatch_command/3`) in **native Elixir shape** (atom keys, structs, atom values) — symmetric with `render/1`. `Musubi.Wire.to_wire/1` is applied at **transport egress** (`Musubi.Transport.Channel` / `Musubi.Transport.ConnectionChannel`), the same boundary that serializes patch envelopes — mirrors render's `:after_serialize`. Client wire contract is unchanged. Reply-schema validation still runs against the wire form: `Musubi.Hooks.ValidateReplySchema` wires internally for validation only and leaves the returned reply native (BDR-0029)
- Codegen: the `:musubi_ts` Mix compiler (`Mix.Tasks.Compile.MusubiTs`) writes one TypeScript declaration bundle (`priv/codegen/ts/musubi.d.ts` by default, configurable via `config :musubi, :ts_codegen_output_path`) covering every Musubi `state do` module exposed by the current Mix project. Consumer apps wire it via `compilers: Mix.compilers() ++ [:musubi_ts]` so `mix compile` keeps the bundle in sync. The bundle is one ambient `declare namespace Musubi { ... }` (root name configurable via `config :musubi, :ts_codegen_root_namespace`) holding the shared runtime types (`StoreId`, `AsyncResult<T>`, `StoreDef`, `StoreField`, `StreamField`, `AsyncField`, `UploadField`, the upload types) plus a single `interface Stores` whose keys are fully-qualified Elixir module names mapping to `StoreDef<Module, Shape, Commands, Events>`. Discovery is via per-module manifest entries — `Mix.Tasks.Compile.MusubiTs` lists `Mix.Project.build_path()/musubi-codegen/<inspect(module)>/state.term` files; no beam scan, no `:application.get_key/2` walk. Modules whose source lives under `test/` are skipped at stamp time; musubi itself emits no bundle. `mix compile.musubi_ts --check` is wired into `mix precommit` and returns a `Mix.Task.Compiler.Diagnostic` (non-zero exit) on drift
- Codegen: the `:musubi_rust` Mix compiler (`Mix.Tasks.Compile.MusubiRust`) writes one Rust bundle (`priv/codegen/rust/musubi.rs` by default, configurable via `config :musubi, :rust_codegen_output_path`) from the same manifest. `config :musubi, :rust_codegen_root_module` (default `"musubi"`) names the prelude module and `config :musubi, :rust_codegen_runtime_path` (default `"musubi_client"`) the crate owning the shared runtime types. The bundle is one file: header + `#![allow(...)]`, a prelude `pub mod musubi` holding only `pub use ::musubi_client::generated::{...}` re-exports, then the module tree — a `kind: :state` module is a `pub struct` in its parent `pub mod`, a `kind: :store` module is its own `pub mod` holding the marker struct, its `Store` impl, `pub struct State`, the command payload/reply and event payload structs, and every hoisted type. Cross-references are root-relative `super::`-chains (prost-style), module segments are snake_cased with `r#` keyword escaping, and the emitted source is rustfmt-stable by construction (`cargo fmt` is never invoked). `mix compile.musubi_rust --check` is wired into `mix precommit` alongside the TS one. Normative design: `docs/rust-codegen.md`
- The `Musubi.Plugin.Codegen` plugin (auto-applied by `Musubi.DSL.State.state/1`) injects an `@after_compile {Musubi.Codegen.Manifest, :__after_compile__}` callback on every `state do` module. There is exactly **one** stamp shared by every codegen target — the persisted payload (`%{module, kind, fields, commands, events, uploads, source}`) is target-neutral raw reflection with quoted Elixir type ASTs, so adding a renderer must not add a second `@after_compile`. The callback uses `Macro.expand/2` against `env.aliases` to fully-qualify every `{:__aliases__, _, _}` AST node (so renderers never re-run alias resolution). Target-agnostic field policy lives on the shared layer too: `Musubi.Codegen.Manifest.renderable_fields/1` drops DSL-internal fields (`:__streams__`) and every renderer calls it. Removing the plugin from the chain breaks codegen — there is no persisted `:__musubi_ts__` attribute or beam-scan fallback
- Reference Phoenix Channel adapter (`Musubi.Transport.Channel`) emits `[:musubi, :channel, :join]` and `[:musubi, :channel, :terminate]`. Adapter `terminate/2` unlinks the linked page server and calls `GenServer.stop/3` with the channel's terminate reason (`:normal`, `:shutdown`, `{:shutdown, :left}`, etc) so the page server's own `terminate/2` runs with the actual context. Reconnect is recovery (BDR-0015): each join builds a fresh page server with `version: 1`
- The Rust client crates live under `crates/<name>/` in a Cargo workspace rooted at `Cargo.toml`: `phoenix-channel` (transport), `musubi-client` (runtime-free core — tokio must never reach it), `musubi-client-tokio` (the tokio seams). Edition 2024, MSRV 1.85, `publish = false`, and `version`/`edition`/dependency versions inherited from `[workspace.package]`. The crates ride the Hex tarball via `package/0` `:files` (with the root `Cargo.toml`) so a consumer path-depends on `../deps/musubi/crates/<name>`. Gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace` on stable and on 1.85 — all in the `rust` CI job, none in `mix precommit`. Normative design: `docs/rust-client.md`
- Examples live under `examples/<name>/` as standalone mini-apps that depend on `musubi` via `path: "../.."`. They are documentation, not test deps — do not add them to the main project's `deps/0`
- Application-level persistence pattern lives in `docs/persistence-pattern.md`: load inside `mount/1`, save via `attach_hook(:persist, :after_command, fun)`. Musubi ships no `Musubi.Persistence` module per `spec/backlog.md`

## Workflow

1. **Code** — make changes
2. **Precommit** — run `mix precommit` (compile → deps.unlock --unused → format → credo --strict → compile.musubi_ts --check → compile.musubi_rust --check → dialyzer → test). When `crates/` changed, also run `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings` and `cargo test --workspace`
3. **Fix** — fix all issues until precommit passes clean
4. **Commit** — descriptive message, conventional-commits prefix (`feat(m1):`, `fix:`, `docs:`)
5. **Push** — `git push origin <branch>` (no flags)
6. **PR** — `gh pr create --base main`. Title is the squash-merge commit message; PRs target `main`; repo is squash-merge only

## Worktree handoffs (paseo)

When a worktree receives a handoff:

1. **Plan first** — invoke `planning-with-files:plan`. Track phases/decisions/errors in `task_plan.md`/`findings.md`/`progress.md`
2. **Delegate implementation to sub-agents** — coordinate from the main agent; let sub-agents do the writing
3. **Merge `main` into your branch** (do not rebase) when a sibling lands on `main`. Resolve `mix.lock` conflicts by re-running `mix deps.get`
4. **Then** push + open PR
