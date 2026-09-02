# Copilot code review playbook

Conventions, contracts, and Elixir style live in [`AGENTS.md`](../AGENTS.md). **Do not restate them here.** This file describes only what the reviewer should additionally look for when reading a diff.

The repo has a real BDD spec under `spec/`. **Spec is authoritative**: code that contradicts the spec is a bug regardless of test pass/fail. Cite the exact `Scenario:` line or `BDR-NNNN` slug when flagging a violation.

## Where the spec lives

- `spec/domains/<domain>/features/*.feature` — Gherkin scenarios. Domains: `runtime`, `replication`, `streams`, `async`.
- `spec/decisions/BDR-NNNN-*.md` — accepted Behavioral Decision Records. Filenames are self-describing; read the BDR a diff touches, don't pattern-match on the number.
- `spec/glossary.md` — domain terms.
- `spec/backlog.md` — out-of-scope or deferred work.

When a diff touches a domain, **read the relevant `.feature` and any matching BDR before reviewing**. Don't rely on memory.

## Spec-violation checklist

1. **Behavior changed without spec changing.** A feature scenario fixes the wire shape, the hook order, the diff op set, the identity tuple, etc. If the diff alters one of those and `spec/` doesn't change in the same PR, block.
2. **New behavior with no scenario.** New socket helper, new envelope field, new hook stage, new reflection key, new wire op kind: needs a scenario in the matching `.feature` file before merge.
3. **BDR contradiction.** If the diff implements something a BDR rules out, block and link the BDR.
4. **Glossary drift.** If naming conflicts with `spec/glossary.md`, flag.

## What to look for in this codebase

The high-leverage review questions, beyond the conventions in `AGENTS.md`:

- **Reflection surface stability.** `Module.__musubi__/1` keys are a public contract. Renaming a key is breaking; adding one needs a deliberate decision (preferably surfaced in the PR description, not buried in a plugin file).
- **`Macro.escape(opts)` invariants.** Stream metadata (`item_key: &…`) and any other AST that survives into reflection must stay quoted. Diffs that drop `Macro.escape/1` from a DSL macro need a spelled-out reason.
- **`socket.private` discipline.** Writes to `socket.private` should go through `Musubi.Socket.put_private/3` rather than direct struct updates, even though both compile.
- **Hook return shapes.** `{:cont, socket} | {:halt, socket} | {:halt, reply, socket}`. The third form is only legal when the caller passed `halt_payloads_allowed?: true`. Diffs that broaden this must change `Musubi.Hook.run_hooks/4`'s contract and the matching scenarios.
- **Hook arity is stage-dependent.** `:before_command`/`:after_command`/`:handle_async` hooks take three arguments, `:handle_info`/`:after_render`/`:after_serialize` take two. Diffs that flatten hook arity back to one generic shape should be questioned.
- **Wire-form vs Elixir-form distinction.** `:after_render` hooks see the Elixir-shaped resolved term (atom keys, structs, atom values). `:after_serialize` hooks see the wire-shaped term produced by `Musubi.Wire.to_wire/1` (string keys, plain maps, atoms-as-strings). Diffs that read `value.field` (atom-key access) inside an `:after_serialize` hook, or that pattern-match string keys inside `:after_render`, are confusing the stages.
- **Store callback contract is explicit.** `use Musubi.Store` injects `@behaviour Musubi.Store`, so `mount/1`, `render/1`, and `handle_command/3` are compile-time-required, while `handle_async/3` and `terminate/2` remain optional. Diffs that reintroduce duck-typed `function_exported?/3` checks for required callbacks should be questioned.
- **Diff engine purity.** Only `add | remove | replace` JSON Patch ops. No `move`, `copy`, `test`. No subtree-replace fallback. No size threshold.
- **Let-it-crash vs caught.** Command and render handlers crash the page runtime; `handle_async/3` exceptions are caught. Diffs that flip either side need a BDR-level discussion.
- **typed_structor `command` DSL boundary.** `command :name do payload …end` deliberately does **not** use a typed_structor block (per-command sub-modules + credo's `UnsafeToAtom` rule are irreconcilable). Re-introducing typed_structor for commands needs a concrete plan for both `@type t/0` collisions and dynamic-atom warnings.
- **LV-parity surfaces.** `stream/4`, `stream_configure/3`, `stream_insert/4`, `stream_delete/3`, `stream_delete_by_item_key/3`, `assign_async/3,4`, `start_async/3,4`, `cancel_async/2,3`, `handle_async/3`, `stream_async/4` (Phoenix.LiveView 1.1+ also has it). Diffs that change semantics from LV without a corresponding scenario should be questioned.
- **Stream pending-ops invariants.** `Musubi.Stream` queues deltas on a per-stream `%Musubi.Stream.Slot{}` held under `socket.assigns.__streams__[<name>]`. The runtime does **not** maintain an ordered `item_keys` list, decide upsert-vs-insert, or trim for `:limit` server-side — that's the client's job. `stream_configure/3` is a lifetime gate; raises if called after the stream's first init. `Musubi.Resolver.resolve/2` calls `Musubi.Stream.drain_and_prune/1` directly after the `:after_serialize` hooks run — drain+prune is a runtime invariant, **not a removable hook**. The page server collects from `socket.private[:__musubi_drained_stream_ops__]` after the resolver returns. Diffs that re-introduce server-side `item_keys`, server-side trim, an `update_only` option, or that move drain+prune back into a hook are spec violations.
- **PatchEnvelope shape.** Initial envelope after mount is `base_version: 0, version: 1, ops: [{op: "replace", path: "", value: <wire root>}], stream_ops: [<mount-time stream ops>]`. Subsequent envelopes increment `version` by 1 per emitted patch. Each wire stream op carries the owning `store_id` path alongside its `stream` name and `ref`. Stream-typed field values must surface as `[]` inside `ops` and never as a populated array — content flows via `stream_ops`. Diffs that introduce a separate `"snapshot"` envelope, a `request_resync` command, or a coalescing layer across cycles violate BDR-0014/0015/0018.
- **Diff engine purity (extended).** Beyond the op-set restriction, `Musubi.Diff.diff/2` is the *only* path from wire-form trees to `ops`. Any code that hand-builds `%{op: ..., path: ...}` maps and bypasses `Musubi.Diff` should be questioned — manual JSON Pointer assembly defeats the `jsonpatch` library's RFC 6901 escaping.
- **Async tracking discipline.** `socket.private[:__musubi_async_refs__]` is the single source of truth for in-flight tasks. `Musubi.Page.Server` rebuilds `state.async_index` from socket trackings after every handler — diffs that bypass `Musubi.Async.put_tracking`/`drop_tracking_only` (or write the private key directly) will desynchronize the index and surface as silent lazy-discards.
- **`Musubi.AsyncResult` semantics.** `loading/1`/`failed/2` MUST preserve the prior `result` for stale-while-loading/failed UX (BDR-0019/0020 reference). Diffs that drop the prior, or that introduce statuses outside `:loading | :ok | :failed`, are spec violations. The wire impl must keep the status atom serialized as a string.
- **`handle_async/3` exception caught (BDR-0020).** A try/rescue wraps the dispatch; failure emits `[:musubi, :async, :exception]` and the runtime survives. Diffs that re-raise, that swallow without telemetry, or that catch `assign_async`/`stream_async` task crashes (which must surface as `failed/2` writes) are spec violations.
- **Same-name `start_async` (BDR-0019).** A second call silently overwrites the prior tracked ref; the old task's result lazy-discards on arrival. Diffs that auto-cancel the prior task or that surface a `:cancel` event for the overwrite are wrong.
- **Cancel semantics.** `cancel_async/2,3` by name kills the pid; the `:DOWN` drives the failed write. Cancel by `%AsyncResult{}` pre-writes `failed/2` AND drops tracking before killing (so the eventual `:DOWN` is a no-op). The runtime stamps `cancel_reason` on the tracking entry so a kill via `Process.exit/2` surfaces the operator-visible reason rather than the raw `:DOWN` reason.
- **`stream_async` declaration check.** Calling `stream_async/3,4` with an undeclared `name` MUST raise `ArgumentError` BEFORE the task is spawned (no orphan tasks). Success writes `AsyncResult.ok(prior, true)` — never the items themselves — and seeds the stream slot in the same envelope. Failure leaves stream contents untouched.
- **Codegen drift.** When a diff edits a `state do` block on a Musubi module, the TypeScript bundle at `priv/codegen/ts/musubi.d.ts` (or the consumer app's equivalent path under `config :musubi, :ts_codegen_output_path`) must be regenerated and committed. The `:musubi_ts` Mix compiler (`Mix.Tasks.Compile.MusubiTs`) keeps it in sync on every `mix compile` once added to `compilers:`; `mix compile.musubi_ts --check` runs in `mix precommit` and returns a `Mix.Task.Compiler.Diagnostic` on drift. A CI failure there means the diff missed regenerating the bundle. Adding a new field-type AST shape (e.g. a non-trivial union, a new `stream(T)` site, a new cross-module reference) should also surface a `TypeRenderer` test in `test/musubi/codegen/type_script/type_renderer_test.exs`. Discovery is via per-module manifest entries written by `Musubi.Plugin.Codegen`'s injected `@after_compile {Musubi.Codegen.Manifest, :__after_compile__}` callback (the callback uses `Macro.expand/2` against `env.aliases` to fully-qualify cross-module references before serializing). There is no persisted `:__musubi_ts__` attribute and no beam scan — diffs that drop the plugin from the chain will silently empty the codegen output.
- **Telemetry catalog.** Every event the runtime emits must appear in `Musubi.Telemetry.events/0`. Diffs that emit a new `[:musubi, …]` event without updating `events/0` violate the catalog contract enforced by `test/musubi/telemetry_test.exs`. Adapter-scoped events (e.g. `[:musubi, :channel, :*]`) stay on the adapter's moduledoc, not the catalog.
- **handle_info dispatch.** The page server's catch-all `handle_info/2` clause dispatches application messages to the root store's `handle_info/2` after running the `:handle_info` hook chain and emits `[:musubi, :pubsub, :receive]` (BDR-0005). Diffs that bypass the hook chain, swallow the dispatch, or drop the telemetry are spec violations.
- **Auth deny telemetry.** `:before_command` halt-with-reply is the documented graceful-denial pattern (BDR-0008). The runtime emits `[:musubi, :auth, :deny]` with `%{module, path, command, reply}`. Diffs that omit the emission or attach it to a different stage are wrong.
- **Channel adapter cleanup.** `Musubi.Transport.Channel.terminate/2` must unlink the linked page server, call `GenServer.stop/3` with the channel's terminate reason (so the page server's `terminate/2` runs with the actual context — `:normal`, `:shutdown`, `{:shutdown, :left}`, etc), and emit `[:musubi, :channel, :terminate]`. Diffs that hardcode `:shutdown` (or rely solely on the link, which would deliver a noproc-style EXIT and skip the page server's `terminate/2`) lose the operator-visible reason.
- **Examples are not test deps.** `examples/<name>/` mini-apps depend on `musubi` via `path: "../.."`. Diffs that add them to the main project's `deps/0` or its test suite mistake documentation for runtime code. Each example owns its own `.gitignore` for `_build/`, `deps/`, `cover/`, `priv/codegen/`; the root `.gitignore` does not whitelist them.

## Test review

Same standards as production code:

- **Happy path + edge case + error path.** Every public function or new behavior needs at least the happy path. Each `{:error, …}` branch and each raise must be covered.
- **Spec-traced.** Tests for user-visible behavior should map to a Gherkin scenario. Test names should echo `Scenario:` titles when applicable. Behavior with no scenario in the spec is itself a flag (either add the scenario or justify the gap).
- **Compile-time DSL tests.** Use `Code.compile_string/1` + `assert_raise` for compile errors; don't try to assert at runtime what is a compile error.
- **No redundancy.** Two tests that fail or pass together for the same reason → one is dead weight. Test names that lie about what they assert are worse than no test name.

## Severity hints

- **Block** (don't approve): spec violation without spec update; BDR contradiction; reflection-key rename; new JSON Patch op kind; let-it-crash bypass on command/render handlers; force-push or `--amend` on a published commit; new behavior without happy-path test.
- **Comment** (request fix before merge): missing edge-case or error-path tests; redundant or mis-named tests; `Macro.escape(opts)` removed without reason; `socket.private` writes that bypass `put_private/3`; telemetry-event name churn.
- **Suggestion**: typo, minor refactor, naming nit.

## What not to flag

- Anything already covered by `AGENTS.md`. Trust that the contributor read it.
- Style preferences when the existing codebase has chosen a different style consistently. Defer to surrounding code.
- Hypothetical future scenarios the spec doesn't cover.
