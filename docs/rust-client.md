# Musubi Rust client — design

Status: **implemented** (v1 scope; uploads still deferred per §10, wire
fixtures still deferred per §12). This document specifies a Rust client crate
that
implements the Musubi client contract as a peer of `packages/client`
(TypeScript). It is a design record, not an implementation guide for a
specific milestone: every normative statement is derived from
`docs/client-contract.md`, `docs/streams.md`, `docs/push-events.md`,
`docs/uploads.md`, the `spec/decisions/BDR-*` records, and the current
`packages/client/src/*.ts` behavior. Where those disagree, the runtime and
`packages/client/src/types.ts` win (see "Known contract discrepancies").

The Rust client is a **second consumer of the same wire contract**, not a port
of the TypeScript runtime. The TS client is dynamically typed at the edges
(proxies, structural marker types, `unknown` fallbacks); Rust is nominal and
deserialization-driven. That single difference drives most of the decisions
below.

---

## 1. Crate identity and placement

### 1.1 Names

| Artifact | Name |
|---|---|
| Core crate (runtime-agnostic) | `musubi-client` (lib `musubi_client`) |
| Tokio transport crate | `musubi-client-tokio` (lib `musubi_client_tokio`) |
| Phoenix Channel protocol crate | `phoenix-channel` (lib `phoenix_channel`) — not Musubi-aware, see §3 |
| Generated code crate-side helper module | `musubi_client::generated` (the shared runtime types the generated file re-exports; see §8.5) |
| Elixir compiler task | `mix compile.musubi_rust` (`Mix.Tasks.Compile.MusubiRust`), compiler atom `:musubi_rust` |
| Elixir renderer | `Musubi.Codegen.Rust` + `Musubi.Codegen.Rust.TypeRenderer` |

### 1.2 Placement

Decision: a **Cargo workspace at the repo root** with three crates under
`crates/`.

```
/Cargo.toml                            # [workspace] members = ["crates/*"], resolver = "3"
/crates/phoenix-channel/               # Phoenix Channel protocol, not Musubi-aware (§3)
/crates/musubi-client/                 # runtime-agnostic Musubi core
/crates/musubi-client/LICENSE          # copy of /LICENSE (each crate carries its own)
/crates/musubi-client/tests/fixtures/*.json  # wire fixtures — deferred to R9 (§12)
/crates/musubi-client-tokio/           # tokio Spawner/Timer/Connector impls (§2.3)
```

Rejected alternatives:

- `packages/rust/` — `packages/` in this repo means "npm workspace member"
  (`pnpm-workspace.yaml` globs `packages/*`). Putting a Cargo crate there
  makes the pnpm workspace root ambiguous for humans even though pnpm itself
  only picks up directories containing `package.json`.
- A separate git repository — the wire contract, the BDRs, and the codegen
  renderer live here; splitting the repo means the Rust client can no longer
  be validated against captured fixtures in the same CI run (see §12).

### 1.3 Hex-tarball implications (asymmetry with the TS packages)

`mix.exs` `package/0` ships `packages/client/src` and `packages/react/src`
**inside the Hex tarball** so that consuming Phoenix apps can depend on them
via `file:../deps/musubi/packages/client` from their `package.json`. That
mechanism exists because npm has no per-language registry story for
"library-adjacent JS shipped by an Elixir dep".

The Rust crates follow the **same pattern**:

- `crates/` **is** added to `package/0` `:files`, next to `packages/`.
  Consumers depend by path from the fetched Hex dep, the Cargo mirror of the
  npm `file:` reference:

  ```toml
  musubi-client       = { path = "../deps/musubi/crates/musubi-client" }
  musubi-client-tokio = { path = "../deps/musubi/crates/musubi-client-tokio" }
  ```

- **Nothing is published to crates.io at this time.** The Hex tarball is the
  only distribution channel, so the crate version is the Hex `musubi` version —
  a single version stream, no skew between the generated file and the crate.
- Accepted limitation: a Rust consumer with no Elixir dep tree (a native app
  talking to a remote Musubi server) must vendor the crates via a git
  dependency on this repo. Publishing to crates.io is deferred until that
  consumer exists; revisit then.
- Only the *generated* file crosses the boundary through the consumer's own
  repo: `mix compile.musubi_rust` writes a `.rs` file that the consumer's Rust
  crate `include!`s or checks in as a module. Same shape as the TS
  `musubi.d.ts` bundle.

### 1.4 MSRV, edition, license

- Edition **2024**, MSRV **1.85**, pinned in `Cargo.toml`
  (`rust-version = "1.85"`) and verified by a CI job on exactly that toolchain.
  Rationale: `gpui`-based consumers (the motivating non-tokio embedder) already
  track recent stable; 1.85 is the first edition-2024 stable.
- License **MIT**, matching `/LICENSE` (Copyright (c) 2026 Phil Chen).
  `license = "MIT"` in each `Cargo.toml` plus a verbatim `LICENSE` copy per
  crate directory (the copies ride the Hex tarball with the sources).

### 1.5 Dependencies

Per crate; no optional dependencies, no feature flags — runtime choice is a
crate choice (§2.3).

| Crate | Dependency | Version | Why |
|---|---|---|---|
| `phoenix-channel` | `serde`, `serde_json` | `1` | frame (de)serialization |
| `phoenix-channel` | `futures-core` / `futures-sink` | `0.3` | `Stream`/`Sink` in the `Socket` trait (§2.2) |
| `phoenix-channel` | `futures-channel` / `futures-util` | `0.3` | inbox, oneshot replies, `select!`, `BoxFuture` |
| `phoenix-channel` | `tracing` | `0.1` | protocol-layer diagnostics |
| `musubi-client` | `phoenix-channel` | path | protocol layer (§3) |
| `musubi-client` | `serde` (derive), `serde_json` | `1` | wire types, shadow document (§4.2) |
| `musubi-client` | `json-patch` | `4` | RFC 6902 application (§4.1) |
| `musubi-client` | `futures-*` | `0.3` | as above |
| `musubi-client` | `tracing` | `0.1` | `warn!`/`debug!` in §7/§10/§11 |
| both | `thiserror` | `2` | `Display`/`Error` derives for §11 (idiomatic error types without hand-written impls) |
| `musubi-client-tokio` | `musubi-client` | path | re-exports the core |
| `musubi-client-tokio` | `tokio` | `1` (rt, time, net) | `TokioSpawner`/`TokioTimer` |
| `musubi-client-tokio` | `tokio-tungstenite` | `0.24` (rustls-tls-webpki-roots) | `TungsteniteConnector` |

`arc-swap` is deliberately **not** a dependency — see §2.4. `musubi-client`'s
tree contains no runtime, which is what the gpui embedder needs
(`docs/rust-gpui-example.md` §5.4); tokio embedders add `musubi-client-tokio`.

---

## 2. Runtime model

### 2.1 Constraint

The primary embedder target is **not** a tokio server. `gpui` runs its own
executor (`BackgroundExecutor` / `ForegroundExecutor`) and does not host a
tokio reactor; spawning tokio-dependent futures from gpui requires dragging a
whole runtime in and bridging it. Meanwhile the obvious default transport,
`tokio-tungstenite`, *is* tokio-bound.

Decision: **runtime-agnostic core, pluggable transport + spawner + timer;
tokio support ships as its own crate** (`musubi-client-tokio`, §2.3). No
`tokio::` type appears in any `musubi-client` signature. The core is a
single-owner actor driven by whatever executor the embedder hands it.

### 2.2 The three seams

```rust
/// One WebSocket frame as Phoenix sees it.
pub enum Frame {
    Text(String),
    Binary(Vec<u8>),   // required for upload chunks (deferred, §10)
}

/// A connected socket: a Sink of outbound frames + a Stream of inbound frames.
pub trait Socket:
    Sink<Frame, Error = TransportError>
    + Stream<Item = Result<Frame, TransportError>>
    + Send
    + Unpin
    + 'static
{
}

/// How to (re)open a socket. Called once per connect and once per reconnect
/// attempt; the crate owns backoff, not the impl.
pub trait Connector: Send + Sync + 'static {
    fn connect(&self, url: &str) -> BoxFuture<'static, Result<Box<dyn Socket>, TransportError>>;
}

/// Detached task spawning. `gpui::BackgroundExecutor`, `tokio::spawn`,
/// `async_std::task::spawn`, or a test's manual pump all satisfy this.
pub trait Spawner: Send + Sync + 'static {
    fn spawn(&self, fut: BoxFuture<'static, ()>);
}

/// Time. Needed for heartbeats (30s), push timeouts, and reconnect backoff.
/// Must be injectable so tests are deterministic.
pub trait Timer: Send + Sync + 'static {
    fn sleep(&self, dur: Duration) -> BoxFuture<'static, ()>;
}
```

`Sink`/`Stream` come from `futures-core`/`futures-sink`, which are
runtime-agnostic. Using them instead of an `async_trait` transport avoids both
a `dyn`-dispatch-per-frame `Box<dyn Future>` allocation and an `async-trait`
dependency in the public API.

### 2.3 Provided implementations

| Crate | Contents |
|---|---|
| `musubi-client-tokio` | `TokioSpawner`, `TokioTimer`, `TungsteniteConnector` (tokio-tungstenite + rustls); re-exports `musubi_client::*` |
| gpui | *no crate — deliberately* |

Not a `tokio` cargo feature on the core: a separate crate keeps the core's
dependency tree runtime-free by construction (no `default-features = false`
discipline to enforce) and lets the tokio adapter rev independently.
`musubi-client-tokio` also ships the one-liner convenience
`musubi_client_tokio::builder(url) -> ConnectionBuilder` — the core builder
pre-filled with `TokioSpawner`/`TokioTimer`/`TungsteniteConnector`.

There is no `gpui` crate. gpui embedders implement `Spawner`/`Timer` in three
lines each against their own executor and supply a `Connector` over whatever
WS client they already link (or `tokio-tungstenite` driven on a dedicated
thread). A `gpui` adapter crate would put a fast-moving, unpublished-ABI
dependency in the workspace for no API benefit. The reference adapter lives in
the gpui example (`docs/rust-gpui-example.md`).

### 2.4 Concurrency shape: one actor, no locks

The connection is a **single owned task**:

```
Actor {
    socket: PhoenixSocket,             // phoenix-channel owns the `Box<dyn Socket>`
    roots: HashMap<Arc<str>, Root>,    // one entry per mounted root, keyed by root id
    rx: UnboundedReceiver<ActorMsg>,   // futures-channel, runtime agnostic
    ...
}
```

Public handles (`Connection`, `Mounted<S>`) are cheap `Clone` values holding an
`mpsc::Sender<ActorMsg>`; every request carries a `oneshot::Sender` for its
reply. The actor `select!`s over `{ inbound frames, inbox, heartbeat tick,
timers }`.

Why: it removes all shared-mutable state (no `Mutex<Tree>` contended between a
UI thread and a socket thread), it keeps frame order intact **up to the socket
actor**, and it makes the whole protocol layer testable by feeding a scripted
`Socket` impl.

Note what that does *not* buy: reply-before-patch ordering (BDR-0009) is **not**
a client guarantee. A `phx_reply` reaches the Musubi actor through the per-push
task that awaits the push's oneshot, while a `"patch"` push reaches it through
the per-channel forwarding task; two independently woken tasks feeding one inbox
means inbox order is executor-scheduling dependent, not frame order. §6.2 and
`Mounted::command`'s `# Ordering` section are the contract: a resolved reply
implies nothing about applied state. Read state from `snapshot()`/`updates()`.

State delivery to the embedder is **not** through the actor's inbox. Each
mounted root owns a snapshot cell:

```rust
pub(crate) struct RootCell<St: Store> {
    snapshot: Mutex<Option<Arc<St::State>>>,
    ...
}
```

`std::sync::Mutex<Option<Arc<_>>>`, not `arc-swap`: a snapshot read happens once
per render, not in a hot loop, and the write happens once per accepted envelope,
so the uncontended-mutex cost is irrelevant and it drops a dependency. The
`Option` is the pre-initial-patch / mid-reconnect hole `snapshot()` surfaces
(§7). Swapping in `ArcSwap` later is a private change if profiling ever asks
for it.

plus, per `updates()`/`events()` subscription, a `futures_channel::mpsc`
sender on the same cell, driven from the actor task. There is **no callback
registry**: the only subscription surface is `Stream`s (§7), each backed by one
sender; a closed receiver (dropped stream) is pruned at the next send. Sends
happen on the actor task; embedders that need thread affinity (gpui) hop inside
their own consuming task (`cx.spawn` + `while let`). No `tokio::sync::watch`.

---

## 3. Phoenix Channel protocol layer

Musubi adds no framing below the channel layer, so this layer is a small,
self-contained reimplementation of the parts of `phoenix.js` the contract
depends on. It is its **own crate**, `phoenix-channel`
(`crates/phoenix-channel/`), and is not Musubi-aware — `musubi-client` is one
consumer of it. The `Socket`/`Connector`/`Spawner`/`Timer` traits of §2.2 are
defined here and re-exported by `musubi_client`. Unpublished like the rest of
the workspace (§1.3); the crates.io name is a question for publishing time.

### 3.1 Wire framing (serializer v2)

- Endpoint: caller-supplied base (e.g. `wss://host/socket`); the crate appends
  `/websocket` and `vsn=2.0.0` plus the caller's connect params as query
  string. **Auth goes in socket connect params, never in join params.**
- Every text frame is a 5-tuple array:
  `[join_ref, ref, topic, event, payload]`, with `join_ref`/`ref` nullable
  strings and `payload` an arbitrary JSON object.
- Replies arrive as event `"phx_reply"` with
  `payload = {"status": "ok" | "error", "response": {...}}` and `ref` equal to
  the originating push's ref.
- Lifecycle events: `"phx_join"`, `"phx_leave"`, `"phx_close"`, `"phx_error"`.
- Heartbeat: push `{event: "heartbeat", topic: "phoenix", payload: {}}` every
  30s (configurable). Missing a heartbeat reply before the next tick ⇒ treat
  the socket as dead and tear down for reconnect, matching `phoenix.js`.
- Refs are a monotonically increasing `u64` stringified; `join_ref` is the ref
  of the channel's current `phx_join` and must be echoed on every push for that
  channel.

### 3.2 Channels

- **One channel per mounted root.** Topic `"<base_topic>:<root_id>"` where
  `base_topic` defaults to `"musubi:connection"` and
  `root_id = format!("{module}:{id}")`.
- **Join is mount; leave is unmount.** There are no `mount`/`unmount` messages.
  Join payload, exact keys:
  ```json
  {"module": "MyApp.Stores.CartPageStore", "id": "cart:page", "params": {}}
  ```
  `id` must be a non-empty string; `params` must be a JSON object (default
  `{}`).
- Join ok reply `{"root_id": "..."}` — the client recomposes `root_id` locally
  and **fails the mount on mismatch**. Join error reply `{"reason": "..."}` is
  surfaced as `MusubiError::Join { reason }`. Join timeout is a join failure.
- **Rejoin.** On socket reopen, every channel still in the registry is
  re-joined with its original params, and the join-ok handling runs again
  (Phoenix `Push.resend` semantics). This is load-bearing for §9; a Rust
  channel that only fires its ok-hook once is wrong.
- Per-push timeout (default 10s, configurable) yields a `Timeout` outcome
  rather than hanging the caller's future.
- **Generation counter.** Each `attach_and_join` for a root bumps a `u64`
  generation stamped into every callback/inflight push; anything arriving with
  a stale generation is dropped. Deliberate leaves set `suppress_close` so the
  resulting `phx_close` does not re-enter reconnect handling.

### 3.3 Reconnect (socket level)

Exponential backoff with jitter (`[10ms, 50ms, 100ms, 150ms, 200ms, 250ms,
500ms, 1s, 2s, 5s]`, then 5s steady — matching `phoenix.js`'s default ladder
closely enough), reset on a successful open. The jitter (up to +25%) is seeded
from `std::collections::hash_map::RandomState`, so no PRNG dependency is needed
— a reconnecting fleet needs spread, not randomness. The socket layer is responsible
for reconnecting and rejoining; the Musubi layer only reacts to
join-ok/close/error (§9).

---

## 4. Patch engine

### 4.1 RFC 6902 subset

Application is delegated to the **`json-patch` crate** (serde ecosystem,
RFC 6902 + 6901 complete) — no in-house pointer/patch implementation. The
Musubi layer adds exactly two things on top:

- **Op allowlist before applying.** Only `add` / `remove` / `replace` are legal
  (BDR-0014: pure minimal structural diff — the server never emits
  `move`/`copy`/`test`, and never falls back to a subtree replace). Any other
  `op` is a protocol violation ⇒ `PatchError::UnsupportedOp` ⇒ treated as a
  version-mismatch-class failure (§9). The allowlist is enforced at envelope
  decode (`PatchOp` is a three-variant enum), so `json_patch::patch` never even
  sees a disallowed op.
- **Error mapping.** `json_patch::PatchError` (bad pointer, index out of
  bounds, traversal into a non-container, ...) maps to `PatchError::Apply` and
  aborts the envelope; the previous tree must remain intact — see 4.3.
  `json_patch::patch` is atomic on failure, which is exactly the §4.3
  requirement.

Pointer unescaping, array-index rules (`-`, `index == len`, leading-zero
rejection) and sequential left-to-right application are the crate's contract,
verified end-to-end by the wire fixtures (§12) rather than re-specified here.

### 4.2 Decision: shadow `serde_json::Value` document, re-deserialize per accepted envelope

Two candidate architectures:

| | Shadow-doc (**chosen for v1**) | Typed in-place |
|---|---|---|
| Model | Keep the authoritative wire tree as `serde_json::Value`; apply ops to it; then deserialize the (hydrated) value into `Arc<S::State>` | Generate per-store code that resolves a JSON Pointer to a typed field and mutates it |
| Correctness risk | Low — one pointer implementation, exercised by fixtures | High — every generated struct needs a pointer-walk arm; enum/`Option`/`Vec` boundaries multiply cases; `add` on a struct field is meaningless |
| Cost per envelope | O(state size) deserialize, regardless of diff size | O(diff size) |
| Interaction with §5/§7 hydration | Natural: hydrate the `Value` before deserializing | Awkward: streams live outside the typed tree |
| `Arc` snapshot semantics | Free — each cycle produces a fresh owned `S::State` | Needs interior mutability or full clone anyway |

Choose the shadow document. The tradeoff is explicit and accepted: **a full
deserialize of the root state on every accepted envelope**, even for a
one-field `replace`. Musubi pages are page-scoped, human-sized trees and the
server already re-renders the whole page per cycle; a per-cycle deserialize of
a few KB is not the bottleneck. The escape hatch, if profiling ever says
otherwise, is per-store-node memoization: cache `store_id -> Arc<ChildState>`
and skip re-deserializing subtrees whose `Value` pointer is untouched by this
envelope's ops (the same invalidation set already computed for snapshot
invalidation). That optimization is additive and does not change the API.

### 4.3 Application order and atomicity

Per accepted envelope, in order:

1. Validate the envelope (§4.4) and the version (§4.5).
2. Apply `ops` to the shadow doc via `json_patch::patch`, which is atomic:
   on any error the document is left untouched — the previous tree stays
   authoritative — and the client enters version-mismatch recovery (§9).
3. Apply `stream_ops` in array order (§5).
4. Apply `upload_ops` (§10 — v1: parsed and discarded).
5. Rebuild derived indices: `store_id -> pointer`, prune stream/upload state
   for vanished `store_id`s (BDR-0011).
6. Hydrate + deserialize (§4.6), publish the new `Arc<S::State>` to the
   `updates()` senders.
7. Dispatch `events` (§8) into the `events()` senders — after state
   publication.
8. Set `version = envelope.version`.

### 4.4 Envelope validation

```rust
struct PatchEnvelope {
    r#type: String,               // must be "patch"
    root_id: String,              // present; validated then ignored
    base_version: u64,
    version: u64,
    #[serde(default)] ops: Vec<PatchOp>,
    #[serde(default)] stream_ops: Vec<StreamOp>,
    #[serde(default)] upload_ops: Vec<UploadOp>,   // optional for tolerance
    #[serde(default)] events: Vec<PushEvent>,      // optional for tolerance
}
```

`ops` and `stream_ops` are required by the TS predicate but `#[serde(default)]`
costs nothing and matches the "forward/backward tolerant" posture the TS client
takes for `upload_ops`/`events`. Unknown fields are ignored (no
`deny_unknown_fields` anywhere on wire types — the server is allowed to add
fields).

### 4.5 Version discipline

`version: u64` per root, `0` meaning "not connected / awaiting initial".

- Initial envelope must be exactly `base_version == 0 && version == 1`.
  Anything else fails the pending mount with
  `MusubiError::Protocol("Initial patch envelope must start at version 1")`.
- Subsequent: `envelope.base_version == version && envelope.version == version + 1`.
  Otherwise ⇒ §9 recovery.
- `version` is a **message sequence**, not a state version: event-only and
  stream-only cycles bump it (BDR-0018). Idle cycles emit nothing, so the
  sequence is gapless for the life of one page runtime.
- The initial envelope's `ops` is always
  `[{"op":"replace","path":"","value":<full wire root>}]`, plus whatever
  stream/upload/event ops `mount` queued.

### 4.6 Wire-tree markers and the hydration pass

The wire tree carries four marker shapes:

| Marker | Shape |
|---|---|
| Store node | any object containing `"__musubi_store_id__": ["seg", ...]` (root `[]`) |
| Stream slot | `{"__musubi_stream__": "<name>"}` — exactly one key |
| Upload slot | `{"__musubi_upload__": "<name>"}` — exactly one key |
| Async value | `{"__musubi_async__": true, "status": ..., "result": ..., "reason": ...}` |

Because Rust is nominal, the generated `State` structs cannot resolve a stream
marker to a `Vec<Item>` on their own — the marker does not carry a `store_id`,
and serde derive has no ambient context. Therefore, before deserializing, the
engine runs one **hydration walk** over the patched shadow doc, tracking the
nearest enclosing `__musubi_store_id__`, and rewrites:

- `{"__musubi_stream__": name}` → the materialized JSON array for
  `(store_id, name)` (§5).
- `{"__musubi_upload__": name}` → **left untouched in v1**; the generated field
  type is the inert `UploadSlot { name }`, which deserializes from the marker
  as-is (§10).
- Async nodes are left alone: `AsyncResult<T>` derives `Deserialize` from the
  wire shape directly (§6.1), so no rewriting is needed. Markers *inside* an
  async node's `result` are still rewritten by the same walk, which is what
  makes `stream_async` render as `AsyncResult<Vec<Item>>`.

v1 hydrates into an owned copy (one extra deep copy per cycle, on top of the
deserialize copy). The known optimization, deferred: implement hydration as a
`serde::Deserializer` adapter wrapping `&Value` that substitutes stream arrays
in place — zero extra copies, but ~300 lines of `Deserializer` plumbing. Not
worth it before there is a profile.

The shadow doc itself is **never** hydrated in place: patch pointers address
the wire tree, so the wire tree must stay pristine across cycles.

---

## 5. Streams: client-owned materialization

Per `docs/streams.md` and BDR-0018, the server keeps no ordered key list, makes
no upsert decision, and does no limit trimming. All of it is the client's job.
This is the single most behavior-sensitive part of the port; it must match
`packages/client/src/streams.ts` op-for-op.

State: `HashMap<(StoreId, StreamName), Vec<StreamEntry>>` where
`StreamEntry { item_key: String, item: Value }`. (`StoreId` is the newtype
over `Vec<String>` from §7, `Eq + Hash` — hash the tuple directly; the TS
`json(store_id) + "\0" + name` string key is an implementation detail of a JS
`Map`, not a wire format.)

Wire ops, each stamped with `store_id` by the page server and flushed
parent-first by `store_id` length:

```json
{"op":"reset",  "stream":"messages","ref":"0","store_id":[]}
{"op":"insert", "stream":"messages","ref":"0","store_id":[],"item_key":"msg-1","at":-1,"item":{...},"limit":-100}
{"op":"delete", "stream":"messages","ref":"0","store_id":[],"item_key":"msg-1"}
```

`ref` is the per-store slot ref; the client ignores it and keys by
`(store_id, stream)`. Within one store's flush the order is always
`[reset?] ++ inserts ++ deletes`.

Semantics:

- `reset` ⇒ the list becomes empty.
- `delete` ⇒ retain entries whose `item_key != op.item_key`.
- `insert` (**upsert-then-position**, in this exact order):
  1. If an entry with the same `item_key` exists, **remove it first**. The item
     is repositioned, not updated in place.
  2. Resolve the index against the **post-removal** length `len`:
     `at == -1` ⇒ `len` (append); `at <= 0` (0 or any other negative) ⇒ `0`
     (prepend); `at > 0` ⇒ `min(at, len)`.
  3. Insert.
  4. Trim by `limit`.
- `limit` trimming (per-op, `null` = no limit): `size = limit.abs()`;
  `size == 0` ⇒ empty list; `len <= size` ⇒ no trim; else
  `overflow = len - size` and — **direction is chosen by `at`, not by the sign
  of `limit`** — if `at == 0` drop `overflow` from the **end**, otherwise
  (including `at == -1` and `at > 0`) drop `overflow` from the **front**.
  The server-side convention writes negative limits (`-100`); the client does
  not read that sign.
- **Owner disappearance**: no `reset` is emitted when a store unmounts. After
  every envelope, drop every stream key whose `store_id` is absent from the
  freshly rebuilt store index (BDR-0011 fresh-mount semantics: reappearance
  starts empty).
- Async streams: an async wire `result` may itself be a stream marker;
  materialize to `AsyncResult<Vec<Item>>` by hydrating inside the async node.

Change notification: a store counts as changed if its indexed node identity
changed, **or** its `store_id` appears in this envelope's `stream_ops`
(`touched_store_keys`), **or** it had stream keys before and none after
(prune), **or** its `store_id` appears in `upload_ops`.

**Deferred.** v1 has no per-store subscription — `Mounted::updates` publishes
one whole-root snapshot per envelope — so the engine computes no change set and
this rule is unimplemented. It lands with the per-store snapshot cache; see the
`ponytail:` note on `PatchEngine::apply`.

Exposure to the app: the hydration pass substitutes the materialized item
values (in list order) for the stream marker, so the generated field type is a
plain `Vec<Item>` on the state struct. Item deserialization failures are
per-stream fatal for the cycle (§11) — the server is authoritative and a
mismatch means codegen drift.

---

## 6. AsyncResult, commands, events

### 6.1 `AsyncResult<T>`

Wire (`lib/musubi/async_result.ex`):

```json
{"__musubi_async__": true, "status": "loading"|"ok"|"failed",
 "result": <T|null|marker>, "reason": null | {"kind":"error"|"exit","value":<any>} | <any>}
```

Detection predicate: object, `__musubi_async__ == true`, `status` in
`{loading, ok, failed}`, both `result` and `reason` keys present.

**This is the single definition.** It lives in `musubi_client::generated` and is
re-exported by the generated bundle (`docs/rust-codegen.md` §4.5); no other
document defines it.

```rust
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
    Structured { kind: AsyncErrorKind, value: Value },
    Opaque(Value),      // server falls back to inspect/1 strings
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AsyncErrorKind { Error, Exit }
```

Field naming: **`result` / `reason`, the wire names**, not the TS client's
app-facing `data` / `error` normalization. Rationale — the derive then works
with no hand-written `Deserialize`, and the three variants line up 1:1 with
`lib/musubi/async_result.ex`'s `%AsyncResult{status, result, reason}`. The
divergence from `packages/client`'s `{status, data, error}` is deliberate and
is explained in `crates/musubi-client/README.md`.

Every variant carries `reason`, including `Ok`, because the server always
renders the key (as `null` when not failed). Consumers matching only on the
payload write `AsyncResult::Ok { result, .. }`.

`__musubi_async__` needs no handling: an internally-tagged enum on `status`
ignores unknown sibling keys. The detection predicate above is still what the
*hydration* walk uses to recognize an async node.

`result` is resolved **recursively** through the same marker rules — it can be a
stream marker, a store node, an array, or a plain object — which is handled
naturally because hydration runs before deserialization.

Deliberately no `Default`/`unwrap_or_default` conveniences: `Loading` with
`result: None` and `Ok` are semantically different states and the app must
branch.

### 6.2 Commands

Push event `"command"` on the root's channel, payload exactly:

```json
{"store_id": ["child","0"], "name": "checkout", "payload": {...}}
```

`store_id` is the server-authored path, echoed verbatim (root = `[]`). No
`root_id` — one root per channel.

Typed surface, generated per command:

```rust
pub trait Command<S: Store>: Serialize + Send + 'static {
    const NAME: &'static str;
    type Reply: DeserializeOwned + Send + 'static;
}

// generated into `my_app::stores::cart_store` (docs/rust-codegen.md §4.6):
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Checkout { pub coupon: Option<String> }
impl Command<CartStore> for Checkout {
    const NAME: &'static str = "checkout";
    type Reply = CheckoutReply;
}
```

`CartStore` here is the zero-sized **marker** type, not the state struct; the
state struct is `my_app::stores::cart_store::State`
(`<CartStore as Store>::State`).

Outcomes are transport `phx_reply` (BDR-0001: no application-level ack, no
`client_seq`):

- `status: "ok"` ⇒ `response` is the command reply map. `{:reply, payload,
  socket}` wire-serializes the payload (string keys, atoms stringified —
  BDR-0029, applied at transport egress); `{:noreply, socket}` ⇒ **`{}`**, not
  `null`. Commands declared with no reply fields generate
  `type Reply = NoReply`, a permissive struct that deserializes from `{}`.
- `status: "error"` ⇒ `response = {"reason": "<string>"}` ⇒
  `CommandError::Failed`. Known reasons include `"unknown command"`,
  `"unknown root"`, `"unknown store"`, `"missing required field"`, plus
  authorization halts (BDR-0008).
- Push timeout ⇒ `CommandError::Timeout`.
- `code` extraction from the error response: the first **string-valued** field
  among `"code"`, `"error"`, `"reason"`, in that order; else `None`.

Preconditions: **no channel, or `version == 0` (mid-reconnect) ⇒
`MusubiError::NotConnected`.** No queueing, no retry — a dispatch is either
sendable now or rejected. (Queueing a dispatch behind an in-flight initial patch
only becomes meaningful with the deferred SWR cache; it is recorded there as
future work, §10, so that no one builds a retry/timeout state machine that
nothing can reach.)

**Ordering (BDR-0009): reply, then the `"patch"` push, then server-side
effects.** The Rust API must not let callers mistake a resolved reply for
applied state. Concretely: `Mounted::command(...).await` returns `Reply` and
the docs state, at the method's `# Ordering` heading, that the corresponding
patch has *not* been applied yet. There is deliberately **no**
`command_and_wait_for_patch` helper in v1 — a `{:noreply}` command still
patches out of band and there is no correlation id to wait on, so any such
helper would be a race dressed as an API. Apps that need "state settled" watch
the snapshot stream for the condition they care about.

Bulk rejection of pending commands: `Disconnected` on channel close/error,
`Unmounted` on teardown, `VersionMismatch` on recovery, and the join failure
reason on a failed (re)join.

BDR-0030 (`send_update`) is server-internal and produces ordinary envelopes —
no client work.

### 6.3 Push events (BDR-0032)

Events ride in `PatchEnvelope.events`; there is **no** `"event"` channel frame.
Shape: `{"store_id": [...], "name": "toast", "payload": <wire term>}`.

- The registry lives on the **root connection**, keyed by `(store_id, name)`,
  and **survives reconnect** — it is cleared only on unmount/disconnect.
- Multiple `events()` streams per key; events with no live stream are silently
  dropped.
- Dispatched exactly once per event, **after** ops/stream_ops/upload_ops are
  applied and the state publication in §4.3. Dispatch is a send into each live
  stream's sender; closed receivers are pruned on the way.
- No ack, no retry, no replay. Events inside an envelope rejected for a version
  gap are discarded with it. A cold client can miss mount-time events —
  documented and accepted upstream.

Typed surface. The wire name has to come from the type, because the dispatch key
is `(store_id, name)`; so the crate defines an `Event` trait mirroring `Command`,
and the generated bundle implements it **on the payload struct**:

```rust
pub trait Event<S: Store>: DeserializeOwned + Send + 'static {
    const NAME: &'static str;
}

// generated into `my_app::stores::cart_store` (docs/rust-codegen.md §4.6):
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ToastPayload { pub message: String }
impl Event<CartStore> for ToastPayload {
    const NAME: &'static str = "toast";
}
```

API:

```rust
let mut toasts = mounted.events::<ToastPayload, _>(&StoreId::root());
while let Some(toast) = toasts.next().await { show(&toast.message); }
// dropping `toasts` unsubscribes
```

The stream is the subscription: dropping it unregisters (no separate
`Subscription` guard type). Payload deserialization failure logs and drops
that event rather than failing the envelope (an event is not state).

---

## 7. Public API sketch

```rust
// ---- conventions -------------------------------------------------------
/// Crate-wide alias, the std convention (`std::io::Result`-style).
pub type Result<T, E = MusubiError> = std::result::Result<T, E>;

/// Server-authored store path (root = empty). A newtype, not a `Vec` alias,
/// so ids cannot be confused with arbitrary string vectors.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct StoreId(Vec<String>);

impl StoreId {
    pub fn root() -> Self;
    pub fn as_slice(&self) -> &[String];
}

// ---- entry point: builder, not a 4-positional free function ------------
impl Connection {
    pub fn builder() -> ConnectionBuilder;
}

impl ConnectionBuilder {
    pub fn url(self, url: impl Into<String>) -> Self;          // required
    pub fn connector(self, c: impl Connector) -> Self;         // required seam
    pub fn spawner(self, s: impl Spawner) -> Self;             // required seam
    pub fn timer(self, t: impl Timer) -> Self;                 // required seam
    pub fn topic(self, topic: impl Into<String>) -> Self;      // default "musubi:connection"
    pub fn heartbeat(self, d: Duration) -> Self;               // default 30s
    pub fn join_timeout(self, d: Duration) -> Self;            // default 10s
    pub fn push_timeout(self, d: Duration) -> Self;            // default 10s
    /// Spawns the actor; the socket opens lazily on first use. The only
    /// build-time error is a missing required seam.
    pub fn build(self) -> Result<Connection, BuildError>;
}

impl Connection {
    /// `params` is the **mount** params object (the channel join payload's
    /// `params` key), not the socket connect params. Accepts anything that
    /// serializes to a JSON object (`serde_json::json!({...})`, a map, a
    /// struct); a non-object value is rejected with `MusubiError::Protocol`
    /// before anything is sent. Untyped: see below.
    pub async fn mount<St: Store>(&self, id: &str, params: impl Serialize)
        -> Result<Mounted<St>>;
    pub async fn disconnect(self) -> Result<()>;
}

// ---- the three traits the generated bundle implements ------------------
// Defined here, in `musubi_client::generated`; the bundle re-exports them
// (docs/rust-codegen.md §4.5) rather than declaring its own.
pub trait Store: Send + Sync + 'static {
    const MODULE: &'static str;                       // "MyApp.Stores.CartStore"
    type State: DeserializeOwned + Send + Sync + 'static;
}

pub trait Command<S: Store>: Serialize + Send + 'static {
    const NAME: &'static str;
    type Reply: DeserializeOwned + Send + 'static;
}

pub trait Event<S: Store>: DeserializeOwned + Send + 'static {
    const NAME: &'static str;
}

// ---- mounted root ------------------------------------------------------
impl<St: Store> Mounted<St> {
    pub fn snapshot(&self) -> Option<Arc<St::State>>;          // None while version == 0 mid-reconnect

    /// One item per accepted envelope. The subscription surface is a
    /// `Stream`, not a callback: dropping the stream unsubscribes.
    #[must_use]
    pub fn updates(&self) -> impl Stream<Item = Arc<St::State>> + Send + 'static;

    pub async fn command<C: Command<St>>(&self, cmd: C) -> Result<C::Reply>;

    /// Child-store dispatch. `T` is inferred from `cmd`'s `Command<T>` impl —
    /// no turbofish at the call site.
    pub async fn command_on<C, T>(&self, target: &StoreId, cmd: C) -> Result<C::Reply>
    where T: Store, C: Command<T>;

    /// Push events (BDR-0032) as a typed `Stream`. Dropping the stream
    /// unregisters. `mounted.events::<ToastPayload, _>(&StoreId::root())`.
    #[must_use]
    pub fn events<E, T>(&self, store_id: &StoreId) -> impl Stream<Item = E> + Send + 'static
    where T: Store, E: Event<T>;

    // No unmount method: unmounting is automatic. Dropping the last clone of
    // this handle leaves the channel (RAII) — see the "Unmount" note below.
}
```

Notes on the shape:

- **Idiom baseline.** Builder for construction (reqwest-style) instead of a
  positional free function; a crate `Result` alias; `Stream`s instead of
  callback registration (`Subscription` guards are gone — the stream itself is
  the RAII guard); `#[must_use]` on streams and command futures; generics
  ordered so the inferable parameter comes last (call sites never need a bare
  `_` turbofish except `events::<Payload, _>`). Embedders that need thread
  affinity (gpui) hop inside their own consuming task (`cx.spawn` +
  `while let`) — a callback API would force the same hop anyway.
- **`snapshot()` returns `Option`.** It is `None` before the initial patch and
  whenever the node is absent from the index mid-reconnect — same guard the TS
  client requires. Callers must handle it; there is no panicking accessor.
- **No typed mount params.** `Store` has no `type Params`, because there is no
  source of truth for it: params are declared with `attr/3` and reflected via
  `__musubi__(:attrs)`, which the shared manifest does not carry
  (`{module, kind, fields, commands, events, uploads, source}` — see
  `Musubi.Codegen.Manifest.collect/1`). `mount` therefore takes
  `impl Serialize` (validated to be a JSON object at runtime), matching the TS
  target, which has no params typing either
  (`StoreDef<Module, Shape, Commands, Events>`). Adding `:attrs` to the manifest
  and generating a params struct is future work, recorded in
  `docs/rust-codegen.md` §8. Note this is **not** optional data: a store like
  `ChatRoom.Stores.ChatRoomStore` declares `attr(:room_id, required: true)` and
  its `mount/2` does `Map.fetch!(params, "room_id")`, so callers must pass it.
- **No `type Commands` / `type Events` on `Store`.** Nothing consumes a sum
  enum; dispatch is per-payload-type via `Command<S>` / `Event<S>`.
- **No proxy, no dynamic field access, no `keyOf`.** Nominal Rust replaces the
  TS proxy layer: `snapshot.header.title` is a struct field. Reserved runtime
  member names (`dispatchCommand`, `subscribe`, `handleEvent`, `snapshot`)
  therefore have no collision risk on the state struct, and a declared state
  field cannot be named `__musubi_store_id__` in the first place —
  `Musubi.DSL.Field.validate_reserved!/1` (`lib/musubi/dsl/field.ex`) already
  raises `ArgumentError` at `state do` expansion time for any name starting with
  `__musubi_`. No new codegen guard is needed.
- **Child store dispatch.** A `Module.state()` field renders as
  `musubi::StoreField<ChildState>` — `{ store_id, #[serde(flatten)] state }`
  (`docs/rust-codegen.md` §4.5) — so
  `mounted.command_on(&snap.checkout_panel.store_id, Pay { .. })` is the
  idiomatic child-command call and `snap.checkout_panel.state.total` reads the
  child's fields. Store ids are **server-authored**; the client echoes them
  verbatim and never constructs or parses them.
- **Duplicate mounts.** Two `mount::<St>("cart:page", ..)` calls for the same
  `(module, id)` alias one root: the second bumps a refcount and returns a
  second `Mounted` handle over the same channel. The registry insert happens
  **synchronously before any await** in the mount path so concurrent mounts
  cannot open two channels on one topic. First-mount params win; later params
  are ignored, with a `tracing::warn!`. If the existing root's initial patch is
  still in flight, the aliasing caller awaits it and, on failure, decrements the
  refcount and propagates the error.
- **Unmount is `Drop`, not a method.** There is no explicit `unmount()`.
  `Mounted` is `Clone` over a refcount; `Drop` decrements it, and at zero sends
  a non-blocking `Leave` message into the actor inbox (an unbounded
  `mpsc::Sender::unbounded_send` — safe from a sync `Drop`). The actor then
  rejects pending mount/commands with `Unmounted`, resets state, drops the root
  from the registry, and leaves the channel (the server's `terminate/2` stops
  the root). If the actor is already gone (connection dropped), the send fails
  silently — the server side is torn down with the socket anyway. Apps that
  need to *observe* teardown completion use `Connection::disconnect()`.

  There is **no unmount grace window and no cancel-by-remount**. The TS client
  has one because React StrictMode double-invokes effects; a Rust embedder has
  no such double-mount, and the TS window is `0` anyway. Refcounted aliasing is
  kept (`Mounted` is `Clone`; the last drop leaves the channel); a configurable
  grace can be added later if a real embedder needs it.
- **Streams as views.** Materialized streams appear as ordinary `Vec<Item>`
  fields on the snapshot (§4.6), so there is no separate stream API surface.

---

## 8. Codegen

`docs/rust-codegen.md` is the **normative** specification of the generator:
compiler and config names, the Elixir → Rust type mapping, hoisting/naming
rules, the module tree, and the exact emission shape. This section carries only
what the *client crate* owes the generator, plus the one prerequisite refactor
that is shared between the two.

Names, fixed once and used in both documents: compiler atom `:musubi_rust`,
task `mix compile.musubi_rust` (`Mix.Tasks.Compile.MusubiRust`), config keys
`:rust_codegen_output_path` (default `"priv/codegen/rust/musubi.rs"`),
`:rust_codegen_root_module` (default `"musubi"`, a **sibling** prelude module),
and `:rust_codegen_runtime_path` (default `"musubi_client"`).

### 8.1 Prerequisite refactor (shared manifest)

Today `Musubi.Codegen.TypeScript.Manifest` stamps
`_build/<env>/musubi-codegen-ts/<inspect(module)>/state.term` with
`%{module, kind, fields, commands, events, uploads, source}` — a payload that
is already **fully target-agnostic** (raw Musubi reflection with quoted Elixir
type ASTs; no TS strings, no marker names, no output path). Only the naming is
TS-coupled.

This landed ahead of the Rust renderer, hoisting:

| From | To |
|---|---|
| `Musubi.Plugin.TypeScript` | `Musubi.Plugin.Codegen` |
| `Musubi.Codegen.TypeScript.Manifest` | `Musubi.Codegen.Manifest` |
| `@subdir "musubi-codegen-ts"` | `"musubi-codegen"` |
| `:__musubi_ts_target_dir__` process key | `:__musubi_codegen_target_dir__` |
| `@after_compile {TypeScript.Manifest, ...}` | the shared manifest — **one** `@after_compile`, N renderers |

One stamp, two renderers. Adding a second `@after_compile` per target was
explicitly rejected: it doubles compile-time IO for identical data. The same
commit fixed the stale `@type entry()` definitions (both omitted `:events`) and
hoisted the `:__streams__` field filter to `Manifest.renderable_fields/1` so
Rust does not re-derive a target-agnostic policy. The full file-by-file
checklist is `docs/rust-codegen.md` §1.2.

Note what this refactor does **not** add: `:attrs`. Mount params therefore stay
untyped in v1 (§7).

### 8.5 What the generated file depends on

The generated bundle is type-only — no runtime logic, no registries, no store
objects (same rule as the TS bundle). Its entire dependency surface is `serde`,
`serde_json`, and these nine items, which it re-exports into its own prelude
module (`docs/rust-codegen.md` §4.5):

```rust
musubi_client::generated::{
    AsyncError, AsyncResult, Command, Event, NoReply, Store, StoreField, StoreId, UploadSlot,
}
```

That list is normative and must match `docs/rust-codegen.md` §4.5 verbatim.
`AsyncErrorKind` is reachable through `AsyncError` and is exported too, but no
generated item names it directly.

Points worth stating because they are easy to get wrong:

- The bundle emits `impl ::musubi_client::generated::Store for CartStore`. The
  traits are **not** sealed — a sealed trait could not be implemented from a
  file generated into a consumer crate.
- `stream(T)` renders as `Vec<T>`, not a marker type: hydration (§4.6)
  substitutes the array before serde runs. There is no `StreamField`.
- Uploads render as the inert `UploadSlot` only; the `UploadHandle` family is
  deferred with the upload engine (§10).

## 9. Reconnect and recovery (BDR-0015: reconnect-only, no resync)

There is **no** application-level resync command. Loss recovery *is* the
reconnect path.

**Transport drop / server-initiated close, with live consumers:**
keep the last-good tree, index, streams, and last published snapshot rendering;
set `version = 0`; clear the pending-initial-patch waiter; reject pending
commands with `Disconnected`; and **keep the channel registered so the socket
layer rejoins it**. On rejoin the server re-runs `mount` (fresh page server,
fresh version sequence from 0) and pushes a fresh initial patch
(`replace ""`) that atomically swaps the state in. No client-driven resync
push, no delta replay, no event replay.

**Rejoin handling.** The join-ok hook fires on *every* rejoin. On each fire:
verify `root_id`, set `version = 0`, and re-arm the initial-patch waiter — but
only when no waiter is already pending (i.e. this is a reconnect, not the first
join).

**Close with refcount 0**: leave and drop the root so nothing rejoins an orphan.

**Version mismatch on a still-live channel.** Guard with a `recovering` flag,
then: reject pending commands with `VersionMismatch`; **soft reset** (keep
last-good tree/index/streams/snapshots; set `version = 0`); leave the channel
(stopping the server-side root) and re-create + re-join it. If that re-join
fails, **do not disconnect** — log, keep the last-good rendering, and rely on
the transport's continued rejoin attempts.

**Generation guarding.** As in §3.2: every `patch` / `on_close` / `on_error` /
join callback carries the generation captured at `attach_and_join` time and is
ignored if stale. Deliberate leaves set `suppress_close`.

Consequences the embedder must be told about, in rustdoc: reconnect re-runs
server `mount`, so mount-time push events re-fire and stream contents are
rebuilt from whatever `mount` re-seeds (`stream(..., reset: true)` /
`stream_async(..., reset: true)`, BDR-0022). Uploads in flight are lost —
uploads are not resumable.

---

## 10. v1 scope cut: uploads deferred

**Uploads are not implemented in v1.** Stated explicitly so it is not mistaken
for an oversight. What v1 does and does not do:

Does:
- Parse `upload_ops` off the envelope and **discard** them (with a
  `tracing::debug!`), so an upload-carrying app does not break the Rust client.
- Deserialize `{"__musubi_upload__": name}` slots into an inert
  `UploadSlot { name: String }` so generated types compile against real stores.
- Reserve the binary `Frame::Binary` variant in the transport trait (§2.2) —
  channel-mode chunks need raw binary frames, and retrofitting them into a
  text-only transport trait would be a breaking change.

Does not:
- The preflight round trip (`"allow_upload"` / `"cancel_upload"` /
  `"upload_progress"` / `"upload_error"` pushes).
- Channel-mode transfer (BDR-0026): joining `"musubi_upload:<entry_ref>"` with
  the stateless `Phoenix.Token` from preflight (`max_age: 600s`), pushing
  `config.chunk_size` binary chunks sequentially awaiting `{"progress": n}`,
  and relying on the server's `bytes_written >= client_size` completion detect
  (there is no `"close"` event).
- External mode (BDR-0027): the named-uploader registry (a future
  `ConnectionBuilder::uploader(name, impl Uploader)` setter), `{entry, file, meta, on_progress,
  cancel_signal}` invocation, the direct PUT, progress relay, and
  `code: "external_failed"` reporting. **External uploaders are deferred
  wholesale**, including the `Uploader` trait.
- The `UploadHandle` state machine: `config`/`add`/`progress`/`complete`/
  `error`/`cancel`/`reset` op application, mean-of-entries handle progress,
  `idle|selecting|uploading|success|error|cancelled` handle status,
  `pending|uploading|success|error|cancelled` entry status, and the open error
  union `too_large|too_many_files|not_accepted|chunk_timeout|external_failed|
  preflight_rejected`.

No `UploadEngine` trait and no `NoopUploadEngine` ship in v1. There is nothing
to abstract over: the hydration pass leaves `{"__musubi_upload__": name}`
markers in the shadow doc untouched (the generated field type is the inert
`UploadSlot`, so deserialization still works), and `upload_ops` are discarded.
Introducing the seam when uploads are actually implemented is not a breaking
change — it would be a private trait behind feature `uploads`.

`touched_store_keys` **does** already union `upload_ops` store ids into the
change set (§5); that is one line and it keeps change notification correct for
an upload-carrying app even while the ops themselves are dropped.

Also deferred from v1: the stale-while-revalidate cache
(`packages/client/src/cache.ts` — `{data, updated_at, buster}` entries keyed by
`{module, id, params}`, cache-seeded trees before the live initial patch,
throttled writes, flush+evict on teardown) and any React-equivalent binding
layer. Queueing a command dispatch behind an in-flight initial patch (rejected
outright in v1, §6.2) belongs to that milestone: it only has meaning once a
cache seed can make a root renderable before `version == 1`.

---

## 11. Error taxonomy

```rust
pub enum MusubiError {
    /// Socket/IO level: connect failed, frame decode failed, socket closed.
    Transport(TransportError),
    /// Channel join rejected by the server. `reason` is the server string:
    /// "unauthorized", "params must be a map", "missing required field",
    /// "missing root id", "missing Musubi connection socket",
    /// "missing Musubi socket", "declared store is not a root store",
    /// "unknown root", "internal error".
    Join { topic: String, reason: String },
    /// Join or push exceeded its timeout.
    Timeout,
    /// No channel, or version == 0 (mid-reconnect) at dispatch time.
    NotConnected,
    /// Envelope failed version continuity; recovery has been initiated.
    VersionMismatch,
    /// Root was unmounted (or dropped) with work in flight.
    Unmounted,
    /// disconnect() was called with work in flight.
    Disconnected,
    /// Envelope violated the contract: bad discriminator, root_id mismatch,
    /// unsupported op, bad pointer, initial version != 1.
    Protocol(&'static str),
    /// RFC 6902 application failure.
    Patch(PatchError),
    /// The wire tree did not match the generated types — i.e. codegen drift.
    Decode { store_id: StoreId, source: serde_json::Error },
    /// Command outcome.
    Command(CommandError),
}

pub enum CommandError {
    Failed { command: &'static str, store_id: StoreId, reply: Value, code: Option<String> },
    Timeout { command: &'static str, store_id: StoreId },
}
```

Rules:

- `MusubiError` implements `std::error::Error` and is `#[non_exhaustive]`;
  `Display`/`Error`/`From` impls come from `thiserror` derives, not
  hand-written boilerplate.
- Error identity is by variant, not by string matching (the TS client matches
  on `name == "MusubiCommandError"` only because of cross-module realm issues;
  Rust has no such problem).
- `Decode` is the one variant that almost always means "the generated file and
  the server disagree". It carries the offending `store_id` and is logged at
  `error!` with the pointer, because a silent partial state is worse than a
  loud stall. A decode failure of the **root** state fails the envelope and
  enters recovery (§9); the tree is not advanced.
- Reason strings from the server are propagated verbatim and never parsed into
  variants — the server's reason list is not a stability contract.

---

## 12. Test strategy

No live-server tests in v1. Three layers:

1. **Wire fixtures captured from the Elixir suite.** One mechanism, not two:
   a `mix musubi.capture_wire` task that drives the existing harness in
   `test/musubi/transport/connection_channel_test.exs` and writes one JSON file
   per scenario to `crates/musubi-client/tests/fixtures/<scenario>.json`:

   ```json
   {
     "scenario": "stream_insert_limit",
     "frames": [{"dir": "in" | "out", "event": "phx_reply", "payload": { }}],
     "expected_state": { }
   }
   ```

   `dir` is relative to the client (`in` = server→client). Fixtures are checked
   in; the regenerate-and-diff CI job in §12.4 fails the build when a
   server-side wire change makes them stale.

   **Deferred to R9.** Neither `mix musubi.capture_wire` nor
   `crates/musubi-client/tests/fixtures/` exists yet: layers 2 and 3 shipped
   with v1, layer 1 did not, and it is R9 — not R2 — that this layer is the
   exit criterion for (see §15). Until it lands, nothing in CI catches the
   Rust engine drifting from what `Musubi.Page.Server` actually emits — the
   `fixture drift` row of §12.4 is not wired, and the Rust tests assert against
   hand-authored JSON derived from this document rather than from captured
   bytes. Scenarios to capture at minimum: initial mount, incremental
   ops of all three kinds, root `replace ""`, stream reset/insert/delete with
   `at`/`limit` variants, child mount/unmount (BDR-0011 prune), async
   loading→ok→failed, event-only cycle, command ok/`{:noreply}`/error,
   version-gap injection.
2. **Pure-unit golden tests**, table-driven, mirroring
   `test/musubi/codegen/type_script/type_renderer_test.exs` in style:
   - Patch layer: op allowlist rejection (`move`/`copy`/`test` ⇒
     `UnsupportedOp`), `json_patch` error mapping, atomicity on mid-envelope
     failure. (Pointer semantics themselves are the `json-patch` crate's
     responsibility — covered indirectly by the wire fixtures.)
   - Stream materialization: the full `at` × `limit` matrix, including the
     upsert-then-position ordering, `at == 0` trims from the end vs everything
     else trims from the front, `limit == 0`, and `limit == null`.
   - `AsyncResult` deserialization incl. `Opaque` reasons and nested markers.
   - Hydration: markers at every nesting depth, inside arrays, inside async
     results, and marker-lookalikes (an object with `__musubi_stream__` **plus**
     another key is *not* a stream slot).
3. **Protocol tests over a scripted transport.** A `MockSocket` implementing
   `Socket` plus a `ManualTimer` implementing `Timer` — both in the shared rig
   at `crates/phoenix-channel/tests/common/mod.rs`, which the `musubi-client`
   suite includes by path — give deterministic
   coverage of: join/rejoin, join failure reasons, generation guarding,
   duplicate mount aliasing, drop-at-refcount-0 teardown,
   version-mismatch recovery (including re-join failure ⇒ keep last good),
   bulk command rejection on each teardown path, reply-before-patch ordering,
   heartbeat timeout, and refcount-0 close.

Elixir side: a `Musubi.Codegen.Rust.TypeRenderer` table test cloned from the TS
one, plus a golden-bundle test over the existing `test/support/typespec_probe.ex`
fixtures (which already cover streams, `AsyncResult.of(stream(...))`,
union-of-maps, `Child.state()`, `list(String.t())`, uploads, commands with and
without payloads, and events).

Deferred to a later milestone: an `#[ignore]`d integration test booting the
Elixir example app and driving a real socket, run on demand rather than in the
default `cargo test`.

### 12.4 CI jobs

`.github/workflows/ci.yml` gains a `rust` job (the gpui example stays out of
CI — `docs/rust-gpui-example.md` §8):

| Job | Command |
|---|---|
| test (stable) | `cargo test --workspace` |
| test (MSRV) | the same on toolchain `1.85` — this is the check §1.4 refers to |
| format | `cargo fmt --all --check` |
| lint | `cargo clippy --workspace --all-targets -- -D warnings` |
| runtime-free core | `cargo check -p musubi-client` + `cargo tree -p musubi-client -i tokio` matching nothing (the gpui embedder\'s configuration) |
| fixture drift | `mix musubi.capture_wire && git diff --exit-code crates/musubi-client/tests/fixtures` — **not wired**, see the §12 layer-1 deferral |

---

## 13. Versioning and compatibility

- Pre-1.0, unpublished (§1.3). The crates ship inside the Hex tarball, so
  **their version is the Hex `musubi` version** — one stream, no pairing table
  needed. Each crate's `Cargo.toml` `version` is bumped with the Hex release.
- Any change to the frames in §3–§8 is a breaking change to this crate,
  regardless of Rust-level semver, and gets a `CHANGELOG` "Wire" heading.
- The generated file is **not** versioned independently: it is regenerated by
  the consumer's `mix compile.musubi_rust`, and `--check` in the consumer's CI
  is what catches drift — exactly the TS story. Since the generated file and
  the crate both come from the same fetched `deps/musubi`, they cannot skew.

Version-skew enforcement (`MUSUBI_PROTOCOL` consts, wire protocol versions)
becomes relevant only if the crates are ever published to crates.io — deferred
with that decision.

---

## 14. Known contract discrepancies (fix upstream)

`docs/client-contract.md` is out of date on three points; `packages/client/src/types.ts`,
`lib/musubi/page/patch_envelope.ex`, and `lib/musubi/async_result.ex` are
correct and are what this design implements:

1. The contract doc omits `upload_ops` from `PatchEnvelope`. The struct has it
   (`lib/musubi/page/patch_envelope.ex`), and the emission rule is "any of
   `ops`, `stream_ops`, `upload_ops`, `events` non-empty".
2. The contract doc omits the `__musubi_async__: true` discriminator from the
   wire async shape. It exists and is part of the detection predicate.
3. The contract doc's "Generated TypeScript" section documents
   `interface StoreDef<Module extends string, Shape, Commands>` (three
   parameters), but `lib/musubi/codegen/type_script.ex:161` emits
   `StoreDef<Module extends string, Shape, Commands, Events = {}>`. The Rust
   design assumes the four-parameter form (`docs/rust-codegen.md` §4.6), so this
   one must be fixed with the rest.

One smaller item is still open: the `Mix.Tasks.Compile.MusubiTs` moduledoc
documents a default output path of `musubi.ts` while `@default_output_path` is
`musubi.d.ts`. (`AGENTS.md` and the two `@type entry()` definitions were listed
here as well and were fixed with the §8.1 refactor.)

---

## 15. Milestones

R0–R7 have landed. R8 and R9 are post-v1 and unimplemented.

| M | Contents | Exit criterion |
|---|---|---|
| R0 | §8.1 shared-manifest refactor (Elixir only) | TS bundle byte-identical before/after; `mix precommit` green |
| R1 | `phoenix-channel` crate: framing, refs, join/leave/push, heartbeat, backoff, rejoin | Protocol tests over `MockSocket` |
| R2 | Patch engine + hydration + index + `AsyncResult` | §12 layer-2/3 golden tests pass (the layer-1 wire fixtures are R9's criterion, not this one) |
| R3 | Stream materialization | Full `at` × `limit` matrix passes |
| R4 | Connection/mount lifecycle, refcounting, `Drop`-based unmount, recovery (§9) | Scripted reconnect + version-gap tests pass |
| R5 | Commands + push events | Reply-before-patch ordering test passes |
| R6 | `Musubi.Codegen.Rust` + `mix compile.musubi_rust`, `--check` (per `docs/rust-codegen.md`) | Generated bundle compiles against R2–R5 and round-trips the probe fixtures |
| R7 | Ship `crates/` in the Hex package (`package/0` `:files`) + crate READMEs | A consumer app path-depends on `../deps/musubi/crates/*` and compiles |
| R8 (post-v1) | Uploads (channel mode, then external), SWR cache | — |
| R9 (post-v1) | `mix musubi.capture_wire` + checked-in `crates/musubi-client/tests/fixtures/*.json` + the §12.4 fixture-drift CI row | R2's fixture golden tests pass |
