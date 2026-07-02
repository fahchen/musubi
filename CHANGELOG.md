# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

The Elixir package (`musubi`) and the JS packages (`@musubi/client`,
`@musubi/react`) share this changelog. Per-package version numbers are
not in lockstep yet; entries note which surface they affect.

## [Unreleased]

### Added

- **`musubi` / `@musubi/client` / `@musubi/react`** — **Push events** (BDR-0032):
  transient, fire-and-forget server-to-client signals (a toast, "scroll to
  bottom"), the analog of LiveView `push_event` + `handleEvent`. The server
  queues them with the bare `push_event(socket, name, payload)` helper from any
  store callback; they are folded into the cycle's patch envelope (a new `events`
  field), so one push carries the diff and its events together. An events-only
  cycle still emits an envelope and bumps `version`. Events own no ack, retry, or
  reconnect replay — a handler only sees events fired after it registers, so data
  that must exist at mount belongs in state. The client consumes them via
  `store.handleEvent(name, cb)` (returns an unsubscribe thunk; registry survives
  reconnect), and React via `useMusubiEvent(store, name, cb)`. Backend tests
  assert delivery with `Musubi.Testing.assert_push_event/3` /
  `refute_push_event/2`.
- **`musubi` / `@musubi/client` / `@musubi/react`** — **Typed, per-store event
  declarations.** Declare events in any store with the payload-only `event` DSL
  (`event :toast do field :msg, String.t() end`). Events are per-store and tagged
  on the wire with the emitting store's `store_id`, dispatched on the client per
  `(store_id, name)` (symmetric with stream/upload ops), so the same name on two
  stores never collides. The declaration drives codegen — `StoreDef` gains a 4th
  `Events` type param (default `{}`, so existing references stay valid) — yielding
  typed `handleEvent` / `useMusubiEvent` per store.
- **`musubi`** — **`:after_render` / `:after_serialize` are transform stages.**
  Both carry a per-socket `Musubi.Page.Frame` (`%{render, events}`) and return
  `{:cont | :halt, frame, socket}` (`Lifecycle.run_transform_hooks/3`), so apps
  can rewrite outbound render/events (audit, redact, enrich, drop). `:after_serialize`
  runs at the page server's aggregation phase, iterating the registry so events
  on memoized sockets still flush. Declared-event payload validation runs there,
  gated by `config :musubi, :validate_push_events` (dev/test default) — dev-correctness,
  not a security boundary, never raises in prod.

## [0.12.0] — 2026-06-27

### Fixed

- **`musubi` / `@musubi/client`** — A WebSocket reconnect no longer leaves a
  root store permanently unrecoverable (`Command "…" failed: Store is not
  connected` until a full page reload). The whole connection multiplexed every
  root over one `musubi:connection` topic/channel; on a drop, Phoenix's
  automatic rejoin of the (never-`leave()`d) channel collided on that single
  topic with the new channel the recovery path opened, Phoenix closed the
  duplicate join, the state machine derailed, `mount` was never re-sent, and the
  server's `terminate/2` `stop_root` was never reversed. Each root store now
  owns its own channel, so recovery rides Phoenix's per-channel rejoin with no
  collision (see Changed).

### Changed

- **`musubi` / `@musubi/client`** — **Breaking wire protocol.** Each root store
  now gets its own channel on topic `musubi:connection:<root_id>` instead of one
  shared `musubi:connection` channel multiplexing all roots by `root_id`. Join
  carries `{module, id, params}` and **is** the mount (the join reply returns the
  canonical `root_id`); leaving the channel (client `leave()` or a transport
  drop) **is** the unmount and stops that one root via `terminate/2`. Reconnect
  recovery now reuses Phoenix's built-in per-channel rejoin: the
  `join().receive("ok")` hook re-fires on every rejoin and rebuilds the root,
  keeping the last-good snapshot until the server's fresh initial patch swaps it
  in. The `command` / `allow_upload` / `cancel_upload` / `upload_progress` /
  `upload_error` payloads no longer carry a `root_id` (one root per channel).
  Client and server must be upgraded together.

### Removed

- **`@musubi/client`** — `SocketLike.onOpen` is gone (added in 0.11.0 to drive
  the socket-level reopen handler). Recovery is now per-channel, so the
  socket-level `onOpen`/`handleSocketReopen`/`reestablishConnectionRoots`
  machinery — and the `mount` / `unmount` / `already_mounted` channel messages —
  were removed. Custom `SocketLike` implementations need only `connect` and
  `channel`.

## [0.11.1] — 2026-06-25

### Fixed

- **`@musubi/client`** — A silent WebSocket drop (clean `socket.disconnect()`
  swallowed by an iOS Safari bfcache freeze) no longer leaves the client with no
  live data after resume. `handleSocketReopen` bailed whenever
  `connectionState.channel` was set, but `socket.onOpen` fires only on a *fresh*
  transport — a channel still held at that point is a zombie bound to the dead
  prior transport. When the WS `onclose` is never delivered,
  `handleConnectionDisconnect` never runs, so `connectionState.channel` and every
  live `root.channel` stay stale and the bail skipped the remount entirely. The
  reopen guard now keys on `connectPromise` (the real "connect in flight" signal)
  and, when a stale channel is present, runs `handleConnectionDisconnect` to
  normalize state (`version` → 0, channels cleared) before re-establishing, so
  the live roots are actually re-mounted. Completes the reconnect story from
  0.11.0, which only covered the drop path where the channel `onClose`/`onError`
  fires.

## [0.11.0] — 2026-06-24

### Fixed

- **`@musubi/client`** — A hard WebSocket drop (iOS Safari backgrounded then
  resumed, network loss, server restart) no longer blanks downstream consumers.
  `handleConnectionDisconnect` now keeps the last-good root/index/streams/
  snapshots for live roots (only resetting `version` to 0) instead of wiping
  them and clearing the roots map, so mounted `proxy.snapshot()` keeps returning
  complete stale data through the reconnect window rather than collapsing to a
  missing-snapshot stub. On socket reopen, a new `onOpen` hook re-joins the
  connection channel and re-mounts each live root; the server's initial patch
  (whole-root `replace ""`) atomically swaps fresh state in, so consumers
  refresh automatically with no navigation or manual reload. Completes the
  reconnect story from 0.10.0, which only covered the version-mismatch recovery
  path (`The terminal disconnect path still resets`).

### Changed

- **`@musubi/client`** — `SocketLike` gains a required `onOpen(callback)` method,
  used to drive reconnect recovery. `Phoenix.Socket` already implements it;
  custom `SocketLike` implementations must add it.

## [0.10.0] — 2026-06-24

### Fixed

- **`@musubi/client` / `@musubi/react`** — Snapshotting a store whose node is
  absent from the index now returns `undefined` instead of a stub object cast to
  the fully-hydrated snapshot type. The stub typechecked as complete, so a
  consumer dereferencing a "guaranteed" field (`snap.artifact.id`) crashed at
  runtime — most visibly on a websocket reconnect (e.g. iOS Safari backgrounded
  then resumed) when a re-render hit the reset index. `StoreProxy.snapshot()`
  and `useMusubiSnapshot` now return `StoreSnapshot<M, R> | undefined`.
  **Breaking (types):** unguarded `.field` access on a snapshot is now a `tsc`
  error — guard with `if (!snap) …` (the same skeleton you render on cold mount).
- **`@musubi/client`** — Version-mismatch recovery no longer empties the store
  index before re-mounting. It keeps serving the last-good (stale-but-complete)
  snapshot through the remount window and lets the remount's initial patch
  (whole-root `replace ""`) swap in fresh state atomically, closing the
  reconnect-window stub on the self-healing path. The terminal disconnect path
  still resets; the type guard above covers it.

## [0.9.2] — 2026-06-23

### Fixed

- **`musubi`** — `Reconciler.reconcile_child/4` no longer re-runs a child's
  `update/2` on every render when the parent passes byte-for-byte identical
  props. The parent-prop change gate now compares incoming props against a
  snapshot of the previously-passed props (`Entry.consumed_assigns`) instead of
  the child's live `socket.assigns`, which the child mutates itself (reload,
  command, async write). Previously a child that overwrote a consumed-key-named
  assign looked like a parent change every cycle, re-firing `update/2` — an
  infinite render loop when a store called `Musubi.send_update` from its own
  `update/2`. **Behavior change (BDR-0030):** a `send_update` write to a key the
  parent also controls now persists until the parent prop actually changes,
  instead of reverting each cycle — the real `Phoenix.LiveView` change-tracking
  rule (#81).

## [0.9.1] — 2026-06-21

### Fixed

- **`musubi`** — `assign_async`/`stream_async` re-assigning a name already in
  flight no longer kills the prior task (including with `:reset`). The runtime
  drops its tracking instead; the prior task runs to completion and its result /
  `:DOWN` lazy-discards by ref. This matches `Phoenix.LiveView` (which never
  exits a producer on re-assign) and extends the `start_async` rule (BDR-0019)
  to all three async APIs. Only `cancel_async/2,3` and `:timeout` actively kill.
  Previously the unconditional kill could terminate a task mid-DB-call and tear
  down a shared Ecto sandbox connection (`:CONNECTION_DEAD`) — see
  `docs/review-store-async-sqlite-problem.md` (BDR-0031).

## [0.9.0] — 2026-06-17

### Added

- **`musubi`** — `Musubi.send_update/2,3`, aligned with
  `Phoenix.LiveView.send_update`, lets the server target one mounted child store
  by `store_id` with new assigns. The map is delivered to the store's `update/2`;
  only that subtree re-renders and one scoped JSON Patch envelope ships (the
  clean root short-circuits its own `render/1`). It is the intra-page last hop
  for cross-connection fan-out coordinated over `Phoenix.PubSub` — Musubi owns
  the targeting, the application owns the broadcast (no built-in PubSub
  abstraction). The two-arity form sends to `self()` (call it from the root's
  `handle_info/2`); the three-arity form targets an explicit page pid.
  Addressing the root (`[]`) is allowed. A `store_id` that no longer resolves to
  a mounted store is a no-op and emits `[:musubi, :send_update, :no_target]`
  telemetry (BDR-0030, #76).

## [0.8.0] — 2026-06-13

### Added

- **`@musubi/client` / `@musubi/react`** — Opt-in, TanStack-Query-style
  stale-while-revalidate store cache. Mounting a store whose identity was
  seen before seeds last-known state immediately (`fromCache: true`) while the
  live mount revalidates in the background and swaps in fresh state when the
  server's initial patch lands. Enabled per call via `MountStoreOptions.cache`
  (`{ gcTime?, persister?, buster?, initialData? }`). Storage is pluggable
  through `MusubiCachePersister` — the default is a connection-scoped in-memory
  Map (cleared on `disconnect`); `createStorageCachePersister` adapts
  `localStorage` / `sessionStorage`. Accepted patches write through to the
  cache on a per-key trailing throttle and flush on teardown / disconnect.
  `gcTime` (default 5 min) is measured from the entry's last update and
  enforced at read so it survives reloads. A `buster` string discards stale
  data shapes across deploys, with a dev warning when a durable persister is
  used without one. Commands dispatched during the stale window are queued
  behind the live initial patch instead of rejecting. New surface:
  `MountedStore.{fromCache, isFetching, revalidated}`,
  `MusubiConnection.clearStoreCache(target?)`, and the
  `createMemoryPersister` / `createStorageCachePersister` / `storeCacheKey`
  exports. In `@musubi/react`, `useMusubiRoot`'s result gains `isFetching` and
  `revalidationError`, plus a `keepPreviousData` option that keeps the prior
  store visible across an `id` / `params` change until the new mount resolves.
  Non-cached mounts are unchanged (#74).

## [0.7.2] — 2026-06-05

### Fixed

- **`@musubi/react`** — `useMusubiRootSuspense` no longer double-mounts a
  root over the wire on react-router v7 SPA navigation. When the last
  render-phase claimer dropped while the mount was still in flight, the
  orphan sweep chained teardown inline on the mount promise; that `.then`
  ran in the same microtask flush as the mount settle — ahead of React's
  MessageChannel-scheduled resumed Suspense render — so the resumed render
  found no shared entry and allocated a fresh mount, producing a spurious
  `unmount` + `mount` burst right after the first patch. Teardown is now
  deferred two macrotask hops past the settle (React's render-phase then
  commit-phase) so the resumed render re-claims the entry first and the
  existing ref / claimer guards bail. Also tidied the shared-mount
  bookkeeping (`key` as a real `SharedRootMount` field, `cancelCleanupTimer`
  / `buildMountOptions` helpers) with no behavior change (#70).

## [0.7.1] — 2026-05-31

### Fixed

- **Transport / `@musubi/client`** — Restored multi-observer ergonomics
  regressed in 0.7.0. The server's `:already_mounted` reply on duplicate
  `(module, id)` now carries the existing `root_id`; the client aliases
  to its local `RootConnection`, bumps a local refCount, and shares one
  `StoreProxy` across all consumers. The last `unmount` defers the server
  push via a brief grace timer so a route-swap remount within the same
  React commit batch cancels the teardown. Out-of-sync state (server
  reports mounted, client has no record) surfaces as
  `MusubiInconsistencyError` instead of being swallowed. Dev-mode warns
  when an alias has different `params` than the original mount. Wire
  protocol additions: `:already_mounted` `:error` reply payload now
  carries `"root_id"`. The 0.7.0 cross-module isolation
  (`"<module>:<caller-id>"` composite root_id) is unchanged (#67).
- **`@musubi/client`** — Hardened the mount / unmount / disconnect
  interplay against several real edge cases (#68). Mid-mount disconnect
  no longer surfaces as an unhandled rejection: the in-flight
  tentative's initial-patch waiter is now shielded by a pre-attached
  `.catch` so rejecting before any awaiter is observing is safe, and
  the mount push is settled synchronously via a `cancelMountPush` hook
  so the caller doesn't wait for Phoenix's push timeout. Version-mismatch
  recovery that hits a stale `:already_mounted` reply (server still
  has our entry after our recovery `unmount` push failed to land) no
  longer hangs forever waiting for an initial patch the server won't
  re-emit — it logs and force-cascades a full
  `disconnectConnectionState` (channel left + runtime entry removed)
  so consumers see a clean tear-down. Grace-timer cancellation
  (alias-remount, disconnect) now settles the awaiting `unmount()`
  caller through a `pendingUnmountResolver` rather than hanging it
  forever. The grace timer skips teardown when a concurrent mount for
  the same `(module, callerId)` is in flight, and
  `mountConnectionRoot`'s `finally` re-arms teardown for any root left
  orphaned because that pending mount then settled `:error` instead of
  aliasing. `channel.leave()` is now called with `connectionState.channel`
  pre-cleared and inside a `try/finally` that guarantees `roots` and
  the runtime entry are dropped even if `leave()` throws synchronously.
  `handleConnectionDisconnect` now clears `connectionState.roots` (was
  only `disconnectConnectionState`) so a subsequent mount on the
  reconnecting state can't alias to a disconnected entry. Server-side
  unmount-push failures are now logged via `console.warn` instead of
  bubbling to the consumer's `await mounted.unmount()` — local state
  is already torn down by then; the release promise resolves cleanly.

## [0.7.0] — 2026-05-31

### Changed (breaking — wire protocol)

- **Transport / `@musubi/client`** — Connection roots are now identified
  by `(module, caller id)`; the server composes and assigns the wire
  `root_id`, which the client treats as opaque. Fixes silent state
  corruption when two roots shared a caller id. Duplicate `(module, id)`
  on one connection is rejected with `:already_mounted`. Client-side
  dedup removed. Tooling that pinned literal `root_id` values must
  update. See `spec/domains/runtime/features/connection-root-identity.feature` (#65).

## [0.6.1] — 2026-05-30

### Fixed

- **Transport** — `Musubi.Transport.Socket.build_connect_socket/2` no
  longer crashes the WebSocket handshake with `FunctionClauseError` when
  Phoenix's cookie session store delivers `connect_info = %{session:
  nil}` (the shape it produces on a cookieless first visit). The handler
  now normalizes `nil` to `%{}` before passing the session through to
  `Musubi.Socket.put_session/2` (#63).
- **`@musubi/react`** — Drop the `react ^18.3.0` / `react-dom ^18.3.0`
  devDependencies that were causing pnpm-workspace consumers on React
  19 to ship two React copies in their production bundle and crash
  with minified React error `#525` on the first Suspense render. React
  is now hoisted at the repo root and pinned via `pnpm.overrides`; the
  package's public `peerDependencies` (`react ^18.2.0 || ^19.0.0`) is
  unchanged (#63).
- **`@musubi/react`** — `useMusubiRootSuspense` no longer wedges
  Suspense in an infinite mount/unmount loop under React 19. The
  previous timer-based orphan sweep raced React 19's
  MessageChannel-scheduled commit and tore the mount entry down before
  any consumer could claim it. The cleanup path is now a
  `FinalizationRegistry`-backed safety net: each render-phase mount
  allocates a fresh unregister token and adds the fiber's `useId`
  claim to a `Set<claimerId>` on the shared entry. The finalizer
  fires only after React releases the discarded fiber, drops this
  fiber's claim, and bails while the set is non-empty (other sibling
  consumers still hold the entry) or while `refs > 0` (a committed
  consumer owns the lifecycle). Falls back to "cleanup on channel
  termination" on hosts that lack `FinalizationRegistry`. (#63).

## [0.6.0] — 2026-05-28

### Added

- `Musubi.Testing.dispatch_command/4` now accepts a native (atom-keyed,
  atom-valued) payload and wire-encodes it via `Musubi.Wire.to_wire/1`
  before dispatch, so `handle_command/3` receives the same string-keyed
  map a real client delivers (#61). Tests can write `%{by: 3}` instead of
  `%{"by" => 3}`; the encode is idempotent on existing string-keyed
  payloads, so this is non-breaking. Symmetric with the egress `to_wire`
  encoding of command replies (#59).

## [0.5.0] — 2026-05-27

### Changed

- Command replies are now returned in native Elixir shape (atom keys,
  structs, atom values), symmetric with `render/1`; `Musubi.Wire.to_wire/1`
  moves to the transport egress (#59). Revises #57. Client wire contract
  unchanged. **Breaking** (Elixir API): tests asserting wire-shaped replies
  from `dispatch_command/3` / `command/4` must switch to native shape.

## [0.4.0] — 2026-05-26

### Changed

- Command replies now serialize through `Musubi.Wire` (#57). Replies match
  the wire shape the client receives (string keys, stringified atoms), and
  schema validation runs against that form — fixing atom-valued and nested
  reply-field validation. `:after_command` hooks and `[:musubi, :auth,
  :deny]` telemetry still see the raw reply (atom keys/values).

### Added

- `Musubi.Wire` support for `DateTime`/`NaiveDateTime`/`Date`/`Time`
  (ISO8601) and `URI` (string) (#57). `MapSet`, `Decimal`, and tuples stay
  unhandled and raise `Protocol.UndefinedError` — convert first.

## [0.3.0] — 2026-05-20

### Added

- **File uploads** (#54). Top-level `upload :name, opts` DSL declared
  per store, outside `state do`. The framework auto-injects
  `{"__musubi_upload__": "<name>"}` markers into render output. Upload
  state ships through an independent `upload_ops` envelope stream
  (`config / add / progress / complete / error / cancel / reset`),
  parallel to `stream_ops`; progress mutation does not pollute
  `__changed__` or trigger `render/1`. Authorization uses a per-entry
  `musubi_upload:<entry_ref>` sub-channel joined with a `Phoenix.Token`
  (HMAC, `max_age: 600`). External (S3/R2 direct) mode ships in v1
  via the optional `upload_external/3` callback. Store facade:
  `consume_uploaded_entries/3`, `cancel_upload/3`,
  `uploaded_entries/2`. New optional callback: `handle_progress/3`.
  Client surface exposes `page.<name>` as a stable reactive
  `UploadHandle` with TanStack-style `status` enum and `isXxx`
  mirrors; no separate React hook. Full reference in
  `docs/uploads.md`; design decisions in
  `spec/decisions/BDR-0024..0028`.

### Changed

- **BREAKING (DSL)** — `command :name, ...` is replaced by the
  block-form `command :name do ... end`, with explicit `payload do
  ... end` and `reply do ... end` sub-blocks for schema declaration.
  Reply validation is now mandatory when a `reply do` block is
  declared. Migration: rewrite each `command :name, payload: ...,
  reply: ...` call as the block form (#53).
- README documents how to wire a Phoenix endpoint socket for Musubi
  (#52).

### Fixed

- `cart_page` example: declare command reply types so the example
  compiles under the strict reply validation (#51).

## [0.2.0] — 2026-05-18

### Added

- `Musubi.Testing` test harness — `mount/3`, `dispatch_command/4`,
  `render/2`, and the `assigns/2` escape hatch for asserting on store
  state from ExUnit.
- `createMusubi` client factory — bind a store type once and reuse the
  resulting page/command/subscribe API across an application.
- React Suspense integration and an `<MusubiProvider>` that accepts a
  raw `Phoenix.Socket` directly.
- Structured command errors. `useMusubiCommand` now returns a
  mutation-shaped value (`mutate`, `isPending`, `data`, `error`, …).
- Phoenix matrix in CI; publish workflow; MIT LICENSE and README
  badges.

### Changed

- **BREAKING (rename)** — Package renamed from `Arbor` to `Musubi`
  throughout the codebase, docs, and configuration.
- `Arbor.Store` facade reshaped to mirror LiveView's call surface,
  including `assign_new/3` and `update/3`.

### Performance

- Resolver short-circuits `render/1` when the root socket is
  unchanged; cached child `wire_state` stitches into the parent wire
  output without re-walk.
- Reconciler checks parent assign value equality before computing
  changed-key intersections; deep-tree leaf dirty detection is
  prune-safe.
- Page server skips `Jsonpatch` diffing when the wire root is
  structurally equal between cycles.
- Client invalidates `snapshotCache` by op path instead of clearing
  the entire cache.

### Fixed

- Reconciler deep-tree leaf dirty detection now survives prune cycles
  without losing references.

## [0.1.0] — 2026-05-17

Initial public release of the Musubi runtime (then `Arbor`):

- Server-authoritative, page-scoped runtime over `Phoenix.Channel`.
- Stores declared via `use Musubi.Store` with `state do … end`,
  command handlers, async helpers, and a `render/1` callback.
- Per-page diff pipeline emitting RFC 6902 JSON Patch envelopes.
- LV-aligned change tracking via per-key `__changed__` flags.
- Streams with stable wire markers and an independent `stream_ops`
  delta channel; LiveView-aligned semantics.
- Async helpers: `assign_async/3,4`, `start_async/3,4`,
  `cancel_async/2,3`, `stream_async/3,4`.
- TypeScript client and React adapter that materialize the diff stream
  into immutable snapshots.

[Unreleased]: https://github.com/fahchen/musubi/compare/v0.12.0...HEAD
[0.12.0]: https://github.com/fahchen/musubi/compare/v0.11.1...v0.12.0
[0.11.1]: https://github.com/fahchen/musubi/compare/v0.11.0...v0.11.1
[0.11.0]: https://github.com/fahchen/musubi/compare/v0.10.0...v0.11.0
[0.10.0]: https://github.com/fahchen/musubi/compare/v0.9.2...v0.10.0
[0.9.2]: https://github.com/fahchen/musubi/compare/v0.9.1...v0.9.2
[0.9.1]: https://github.com/fahchen/musubi/compare/v0.9.0...v0.9.1
[0.9.0]: https://github.com/fahchen/musubi/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/fahchen/musubi/compare/v0.7.2...v0.8.0
[0.7.2]: https://github.com/fahchen/musubi/compare/v0.7.1...v0.7.2
[0.7.1]: https://github.com/fahchen/musubi/compare/v0.7.0...v0.7.1
[0.7.0]: https://github.com/fahchen/musubi/compare/v0.6.1...v0.7.0
[0.6.1]: https://github.com/fahchen/musubi/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/fahchen/musubi/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/fahchen/musubi/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/fahchen/musubi/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/fahchen/musubi/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/fahchen/musubi/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/fahchen/musubi/releases/tag/v0.1.0
