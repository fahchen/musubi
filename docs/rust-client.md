# Musubi Rust client — design

This document specifies the Rust client crates, which implement the Musubi
client contract as a peer of `packages/client` (TypeScript). Every normative
statement here is derived from `docs/client-contract.md`, `docs/streams.md`,
`docs/push-events.md`, `docs/uploads.md`, the `spec/decisions/BDR-*` records,
and `packages/client/src/*.ts`. Where those disagree, the runtime and
`packages/client/src/types.ts` win.

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
| Generated code crate-side helper module | `musubi_client::generated` (the shared runtime types the generated file re-exports; see §8.2) |
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
/crates/musubi-client/tests/fixtures/*.json  # wire fixtures, `mix musubi.capture_wire` (§12)
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
tree contains no runtime, which is what a GUI embedder needs; tokio embedders
add `musubi-client-tokio`.

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
    Binary(Vec<u8>),   // upload chunks ride these (§10.2)
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
dependency in the workspace for no API benefit. A reference adapter — the three
seams over gpui's executor, meant to be copied verbatim — ships as
`examples/chat_room/desktop/src/transport.rs`.

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
4. Apply `upload_ops` into the root's upload registry, in array order (§10).
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
- `{"__musubi_upload__": name}` → **left untouched**; the generated field type
  is the inert `UploadSlot { name }`, which deserializes from the marker as-is.
  Live upload state is folded into the registry instead and read through
  `Mounted::upload(&store_id, name)` (§10).
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

**Not implemented.** There is no per-store subscription: `Mounted::updates`
publishes one whole-root snapshot per envelope, so an upload-only cycle already
wakes every root subscriber, and the engine computes no change set. The rule is
specified here because a per-store snapshot cache would need it. Per-**upload**
notification *is* implemented: an `Upload`'s `updates()` stream fires for
exactly the ops that touched it (§10).

Exposure to the app: the hydration pass substitutes the materialized item
values (in list order) for the stream marker, so the generated field type is a
plain `Vec<Item>` on the state struct. Item deserialization failures are
per-stream fatal for the cycle (§11) — the server is authoritative and a
mismatch means codegen drift.

---

## 6. AsyncResult, commands, events, mount cache

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
`MusubiError::NotConnected`.** No retry, ever — a dispatch is either sendable
now or rejected.

The one exception is a root a **cache seed** made renderable before its live
initial patch (§6.4). There the caller is looking at state, so `NotConnected`
would be a lie; the dispatch is held instead and flushed, in queue order, the
moment the initial patch is published and `version` reaches `1`. "In order"
is the order the queue is drained and the pushes are issued in; each one is
handed to the spawner as its own task, so which reaches the socket first is
ultimately the executor's choice, exactly as for two concurrent `command`
calls. The queue is a bridge
across exactly one revalidation, not a retry buffer:

- It is bounded (32 dispatches per root). Past the bound a dispatch gets the
  same `NotConnected` an unseeded root gives.
- Every bulk rejection empties it and clears the seeded flag with it —
  `VersionMismatch` on recovery (a revalidation that produced a version gap),
  `Disconnected` on channel close, `Unmounted` on teardown, the join reason on a
  failed re-join. After any of those the root is back to the plain contract
  above, so nothing queues behind a revalidation that is not coming.
- A root that reached `version == 1` on its own and then reconnected is **not**
  seeded, so it rejects as before. Queueing is a property of the seed, not of
  the cache being enabled.

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

### 6.4 Mount cache (stale-while-revalidate)

The normative cache contract is `docs/client-contract.md` § Store Cache
(Stale-While-Revalidate); this section records the Rust shape and the three
divergences from it.

Opt-in and **connection-wide**, unlike the TypeScript client's per-mount `cache`
option:

```rust
let connection = Connection::builder()
    .url(url).connector(connector).spawner(spawner).timer(timer)
    .cache(MemoryCacheStore::new())          // any `CacheStore`
    .cache_buster(env!("CARGO_PKG_VERSION")) // default ""
    .cache_gc_time(Duration::from_secs(300)) // default 5min
    .build()?;
```

**Entry.** `CacheEntry { data: Value, updated_at: u64, buster: String }`, keyed
by `cache_key(module, id, params)` = `"<id>|<module>|<canonical params>"`,
params canonicalized with sorted keys so field order cannot fork one store into
two slots. It matches `storeCacheKey` in `packages/client/src/cache.ts` for
object-valued params over non-float scalars — which is every generated `Params`
struct — and deliberately not beyond that: TypeScript renders *omitted* params
as `null` where Rust always has an object (`Params {}` ⇒ `{}`), and
`serde_json`'s float rendering is not `JSON.stringify`'s (`1.0` vs `1`). Point
two clients at one durable store only under those terms. `data` is the **wire tree** (the shadow document, `__musubi_store_id__`
and `__musubi_stream__` markers intact), so seeding is the same marker
substitution the engine already does and there is no second decoding path.
`updated_at` is `now_ms()`, wall-clock milliseconds since the Unix epoch.

**Store.** `trait CacheStore { get, put, evict }`, `Send + Sync + 'static`,
each returning a `BoxFuture`. Every method is fallible in practice and
infallible in the signature: an implementation that cannot read returns `None`
and one that cannot write does nothing, so a broken cache degrades to a cold
mount instead of failing one. The crate ships `MemoryCacheStore` only — a
durable store is the embedder's, because the file system and the platform's
storage are runtime decisions this crate does not make.

**Mount.** The registry insert and the join happen first, then the read is
*spawned*, so a slow store delays the seed and never the revalidation. When the
read produces an entry whose `buster` matches and whose age is within
`cache_gc_time`, the actor adopts it with `PatchEngine::seed` — document, index
and prune, **version stays 0** — publishes it, and resolves every mount waiting
on the root. The live initial patch is still required to be
`base_version: 0, version: 1`, and its whole-root `replace ""` swaps the seed
out in one op.

Five things drop a seed rather than showing it:

- A stale or wrong-`buster` entry is evicted by the reading task; the mount is
  cold.
- A read that suspends past the live initial patch loses: `published || version
  != 0` means the server's state stays and the seed is discarded (the same race
  guard `trySeedFromCache` has).
- A read that suspends past its own *mount* loses. A root is addressed by
  `"<module>:<id>"` but its slot also keys on the params, so a failed join
  followed by a re-mount of the same id under different params would otherwise
  be seeded from the first mount's slot. `ActorMsg::CacheSeed` carries the key
  it was issued for and is dropped when that is no longer the root's.
- A tree the generated types reject — a shape an older build wrote —
  is discarded via `PatchEngine::discard_seed`, the slot is evicted, and the
  mount goes on waiting for the cold path. This is deliberately **not**
  `MusubiError::Decode`: nothing is diverged, the live patch is still coming.
- Streams are not cached (`stream_ops` are not part of the tree), so a seeded
  stream slot hydrates to `[]` until the live envelope refills it — exactly what
  the TypeScript client does, which seeds `root` without seeding `streams`.

**Writes.** After every accepted envelope is published, the root's document is
queued for its slot under a trailing throttle (`CACHE_WRITE_THROTTLE`, 1s): a
burst of envelopes costs at most one write per interval, always the latest tree,
fire-and-forget.

**Teardown.** Unmount flushes the pending write, then arms the gc timer with the
remainder of `cache_gc_time` measured from the entry's own `updated_at`, so a
slot that was already half-expired is not given a fresh lifetime. A re-mount of
the same slot cancels that eviction. `disconnect()` flushes but does **not**
evict: the entry ages out on its own and a reconnecting app can seed from it
again — the one place this diverges from `disconnectConnectionState`, which
clears the runtime-owned memory persister. Here the store is the embedder's, so
wiping it on disconnect would be a surprise.

Not carried over from the TypeScript layer: `initialData` (a per-mount option,
and this cache is connection-wide) and the durable-persister-without-`buster`
warning (a `CacheStore` does not declare whether it is durable).

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
    pub fn uploader(self, name: impl Into<String>, u: impl Uploader) -> Self;
    pub fn cache(self, store: impl CacheStore) -> Self;        // §6.4, off by default
    pub fn cache_buster(self, buster: impl Into<String>) -> Self; // default ""
    pub fn cache_gc_time(self, d: Duration) -> Self;           // default 5min
    /// Spawns the actor; the socket opens lazily on first use. The only
    /// build-time error is a missing required seam.
    pub fn build(self) -> Result<Connection, BuildError>;
}

impl Connection {
    /// `params` is the **mount** params object (the channel join payload's
    /// `params` key), not the socket connect params. It is the store's
    /// generated `Params` struct — one field per `attr/3` declaration — so a
    /// required attr cannot be forgotten at the call site. A hand-written
    /// `Store` impl whose `Params` serializes to a non-object is rejected with
    /// `MusubiError::Protocol` before anything is sent.
    pub async fn mount<St: Store>(&self, id: &str, params: St::Params)
        -> Result<Mounted<St>>;

    /// The escape hatch: `attr/3` is the child-store assign contract, and the
    /// page server hands the join payload's `params` map to `mount/2`
    /// unvalidated (`mount_root_store/2`), so a root that reads a key it never
    /// declared as an attr is legal and unreachable through `St::Params`.
    /// Same object guard, no attr typing.
    pub async fn mount_with_params<St: Store>(&self, id: &str, params: impl Serialize)
        -> Result<Mounted<St>>;
    pub async fn disconnect(self) -> Result<()>;
}

// ---- the three traits the generated bundle implements ------------------
// Defined here, in `musubi_client::generated`; the bundle re-exports them
// (docs/rust-codegen.md §4.5) rather than declaring its own.
pub trait Store: Send + Sync + 'static {
    const MODULE: &'static str;                       // "MyApp.Stores.CartStore"
    type State: DeserializeOwned + Send + Sync + 'static;
    type Params: Serialize + Send + 'static;          // from `attr/3`
}

pub trait Command<S: Store>: Serialize + Send + 'static {
    const NAME: &'static str;
    type Reply: DeserializeOwned + Send + 'static;
}

pub trait Event<S: Store>: DeserializeOwned + Send + 'static {
    const NAME: &'static str;
}

// ---- mounted root ------------------------------------------------------
/// Client-local liveness projection (BDR-0033). No wire message carries it.
pub enum MountStatus { Connecting, Live, Reconnecting }

impl<St: Store> Mounted<St> {
    pub fn snapshot(&self) -> Option<Arc<St::State>>;          // None while version == 0 mid-reconnect

    /// One item per accepted envelope. The subscription surface is a
    /// `Stream`, not a callback: dropping the stream unsubscribes.
    #[must_use]
    pub fn updates(&self) -> impl Stream<Item = Arc<St::State>> + Send + 'static;

    /// BDR-0033: `Connecting` until the first *accepted* initial patch (a
    /// cache seed does not count), `Live` after, `Reconnecting` from a socket
    /// drop / heartbeat timeout / version-gap recovery until the rejoin's
    /// fresh initial patch lands. Terminal outcomes (rejected join, unmount,
    /// disconnect) stay on the mount error path — no error arm here.
    pub fn status(&self) -> MountStatus;

    /// One item per status transition; no replay — read `status()` first.
    /// Same contract as `updates()`: dropping the stream unsubscribes, and it
    /// ends when the root is unmounted or the connection disconnected.
    #[must_use]
    pub fn status_updates(&self) -> impl Stream<Item = MountStatus> + Send + 'static;

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

    /// The live upload handle for `(store_id, name)` — the name is read off
    /// the state struct's inert `UploadSlot` field. See §10.
    pub fn upload(&self, store_id: &StoreId, name: &str) -> Upload;

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
- **`status()` answers "am I current", `snapshot()` answers "have I loaded".**
  The two are deliberately separate (BDR-0033): a reconnect never clears the
  snapshot, so an idle disconnect is observable only on the status surface.
  The socket layer underneath exposes the connection-wide analogue
  (`PhoenixSocket::status` / `status_updates`,
  `SocketStatus { Connecting, Connected, Reconnecting, Closed }`); this crate
  folds the per-topic projection of the same signal — the `ChannelEvent`s the
  socket actor emits from the identical transitions — into a per-root
  `MountStatus`. While `Reconnecting`, the embedder MUST keep rendering the
  last-good tree; the status exists to annotate stale rendering, never to
  blank it.
- **Typed mount params.** `Store::Params` is the struct the generator emits
  from `__musubi__(:attrs)`, which the shared manifest carries as `:attrs`
  (`{module, kind, fields, commands, events, attrs, uploads, source}` — see
  `Musubi.Codegen.Manifest.collect/1`). A `required: true` attr is a plain
  field, every other attr an `Option<T>`; a store declaring no `attr` gets
  `pub struct Params {}`. That matters because params are **not** optional
  data: `ChatRoom.Stores.ChatRoomStore` declares
  `attr(:room_id, String.t(), required: true)` and its `mount/2` does
  `Map.fetch!(params, "room_id")`, so a `json!({})` mount used to fail only at
  the server. `mount` still validates that the value serialized to a JSON
  object, because `Store` is unsealed and `Params` is only bound by
  `Serialize`. The TS target has no params typing
  (`StoreDef<Module, Shape, Commands, Events>`); that parity gap is recorded in
  `docs/rust-codegen.md` §8.
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
- **No UI binding layer.** There is no Rust equivalent of `@musubi/react`. A UI
  integrates against `snapshot()` and `updates()` directly, which is what makes
  the surface portable across GUI frameworks.

---

## 8. Codegen

`docs/rust-codegen.md` is the **normative** specification of the generator:
compiler and config names, the Elixir → Rust type mapping, hoisting/naming
rules, the module tree, and the exact emission shape. This section carries only
what the *client crate* owes the generator, plus the manifest layer the two
targets share.

Names, fixed once and used in both documents: compiler atom `:musubi_rust`,
task `mix compile.musubi_rust` (`Mix.Tasks.Compile.MusubiRust`), config keys
`:rust_codegen_output_path` (default `"priv/codegen/rust/musubi.rs"`),
`:rust_codegen_root_module` (default `"musubi"`, a **sibling** prelude module),
and `:rust_codegen_runtime_path` (default `"musubi_client"`).

### 8.1 The shared manifest

`Musubi.Plugin.Codegen` stamps
`_build/<env>/musubi-codegen/<inspect(module)>/state.term` with
`%{module, kind, fields, commands, events, attrs, uploads, source}`, and
`Musubi.Codegen.Manifest` reads it back. The payload is **fully
target-agnostic**: raw Musubi reflection with quoted Elixir type ASTs, no TS
strings, no marker names, no output path.

One stamp, N renderers. A second `@after_compile` per target is explicitly
rejected — it doubles compile-time IO for identical data. For the same reason
the `:__streams__` field filter lives on the shared layer as
`Manifest.renderable_fields/1` rather than being re-derived per renderer; see
`docs/rust-codegen.md` §1.1–§1.2 for the full manifest contract.

`:attrs` is what makes typed mount params possible: the Rust target generates a
`Params` struct per store from it (§7, `docs/rust-codegen.md` §4.6), and the TS
renderer ignores the key.

### 8.2 What the generated file depends on

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
  hand-written in `musubi-client` and keyed by `(store_id, name)`, so codegen
  emits nothing for it (§10).

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

**Status surface (BDR-0033).** Every path above is observable without a failed
command: transport drop, heartbeat timeout and version-gap recovery each flip
`Mounted::status()` to `Reconnecting` the moment the client notices (bounded
by the heartbeat interval for a silent death), and the rejoin's fresh initial
patch — not the rejoin itself — flips it back to `Live`. A root that never
reached `Live` stays `Connecting` through socket churn; terminal outcomes stay
on the mount error path. The status is a client-local projection of the
signals in this section — no wire message carries it, and it never modifies
the recovery behavior it reports on.

Consequences the embedder must be told about, in rustdoc: reconnect re-runs
server `mount`, so mount-time push events re-fire and stream contents are
rebuilt from whatever `mount` re-seeds (`stream(..., reset: true)` /
`stream_async(..., reset: true)`, BDR-0022). Uploads in flight are lost —
uploads are not resumable. The reconnect window itself is renderable state:
`status()`/`status_updates()` report it while `snapshot()` keeps serving the
last-good tree.

---

## 10. Uploads

Both halves are implemented. The **data plane** — everything the server drives
over `upload_ops` — matches `packages/client/src/uploads.ts` op-for-op, exactly
like streams (§5). The **control plane** — selecting files, preflight, and
moving bytes — is the client's own API, and it is the only thing that ever
writes `UploadHandle::status`.

### 10.1 Data plane

`crates/musubi-client/src/uploads.rs`. `PatchEngine` folds `upload_ops` into a
per-root registry (`Uploads`) keyed by `(StoreId, upload_name)` — uploads are
singletons per store, so that pair is the identity (BDR-0028). The pair is
hashed directly; the TS `json(store_id) + "\0" + name` string key is an
implementation detail of a JS `Map`, not a wire format.

The state slot stays inert: hydration leaves `{"__musubi_upload__": name}`
alone and the generated field type is still `UploadSlot { name: String }`,
which is what an app reads the handle's key off. Live upload state is reached
through the handle, never through the state struct:

```rust
let avatar = cart.upload(&StoreId::root(), &cart.snapshot()?.avatar.name);

let handle = avatar.snapshot();        // UploadHandle, always available
let mut updates = avatar.updates();    // one item per envelope that touched it
```

`Upload` is a cheap `Clone` over the live cell, and `snapshot()`/`updates()`
mirror the `Mounted` surface. A handle is created on first access — before any
op it reads as idle with the framework defaults — and the same key always
resolves to the same handle, so it can be taken as soon as the marker appears.

Op application (`UploadHandle`, mirroring `applyOps`):

| op | effect |
|---|---|
| `config` | replace the handle's `UploadConfig` (`chunk_timeout` is not on the wire) |
| `add` | upsert by `ref`, keeping the entry's position; the wire `progress`/`status`/`errors` win |
| `progress` | `entry.progress = op.progress`; status `success` at `>= 100`, else `uploading`; unknown ref ignored |
| `complete` | `progress = 100`, status `success` — the 10 Hz progress throttle can swallow the final `100`, `complete` is never dropped; unknown ref ignored |
| `error` | with `ref`: status `error` and **append** to the entry's errors; without one: append to the handle's errors |
| `cancel` | **delete** the entry — cancellation is a deletion, never a status |
| `reset` | clear every entry and the handle's errors; the handle's own status is untouched |

`UploadHandle::progress()` is the plain mean over **all** entries — pending and
failed included — rounded half-up, `0` with no entries. Entries keep insertion
order (a `Vec`, not a `HashMap`), which is what the TS `Map` iteration order
gives.

Each touched handle publishes exactly **one** snapshot per envelope, not one
per op, and an envelope that changes nothing publishes nothing. Handles whose
store leaves the freshly rebuilt index are pruned alongside streams, which ends
their `updates()` streams (BDR-0011 fresh-mount semantics; uploads are not
resumable per BDR-0003). Unmounting the root clears the whole registry.

Types are the wire types: `UploadOp` is a `#[serde(tag = "op")]` enum over the
seven variants with `error.ref` optional, `UploadAccept` is
`Any | Extensions(Vec<String>)`, and `UploadErrorCode` is an **open** enum —
`too_large | too_many_files | not_accepted | chunk_timeout | chunk_too_large |
external_failed | preflight_rejected | internal`, with `Other(String)` for
anything a newer server adds — the same union `docs/uploads.md` documents, over
a TS type that is open too (`(string & {})`).

`upload_ops` decodes **element by element**: an op whose `op` tag (or whose
`entry.status`) this build does not know is logged and skipped, not failed. One
unknown upload delta must not take the state `ops`, `stream_ops` and `events`
travelling in the same envelope with it and gap the root's version;
`applyOps` in `packages/client/src/uploads.ts` is a `switch` with no `default`,
so it already drops exactly these ops and applies the rest.

`UploadStatus` (`idle|selecting|uploading|success|error|cancelled`) is
**never written by an op** — it is driven by the client's own
`select`/`start`/`cancel`/`reset` (§10.2). `cancelled` is reserved on both
clients: neither ever assigns it to a handle or an entry, because `cancel`
deletes the entry.

### 10.2 Control plane

`crates/musubi-client/src/transfer.rs`. Four `async` methods on the same
`Upload` the data plane hands out, so an app never holds two objects for one
upload:

```rust,ignore
let entries = avatar.select(vec![UploadFile::new("me.png", "image/png", bytes)]).await?;
avatar.start().await?;                  // every entry, concurrently
avatar.cancel(Some(&entries[0].r#ref)).await?;
avatar.reset().await?;
```

`UploadFile` is bytes plus client metadata (`name`, `content_type`); the crate
is runtime-free, so **the embedder reads the file** and `client_size` is
`bytes.len()` — a size disagreeing with the bytes would strand the transfer,
since channel-mode completion is `bytes_written >= client_size`. A streaming
chunk provider is a later addition; nothing in the wire contract depends on the
whole file being resident.

**Where the pushes go.** Main-channel pushes (`allow_upload`, `cancel_upload`,
`upload_progress`, `upload_error`) are routed through the connection actor,
which owns the current channel incarnation — a handle pinning a `Channel` would
push into one that recovery has replaced. Chunk sub-channels are opened
straight on the `PhoenixSocket`: they are per-entry, short-lived, and the actor
has no business tracking them. The two external-mode relays are *detached*
pushes (no reply awaited), matching the TS client.

**Preflight.** `select` sets `status = selecting`, clears the handle's errors,
and pushes `allow_upload` with one offered entry per file, `client_ref` being
the file's index. The reply carries the live `config`, the accepted entries
keyed by `client_ref`, and one error per rejected file; rejections become
handle-level errors and produce no entry and no op at all, so a partially
rejected selection still ends in `status = error`. Entries are seeded in
selection order (the reply's map is sorted by `client_ref`) and merge with the
`{op: add}`s that arrive **after** the reply (BDR-0009) — whichever lands
first, there is exactly one entry.

**Channel mode (BDR-0026).** `phoenix-channel` gained serializer v2 binary
framing for this: `BinaryPush` (kind `0`, four length-prefixed header fields,
then the payload verbatim) and `Channel::push_binary`. Only the client→server
push layout is modelled — the three server→client binary layouts have different
headers and a Musubi server never sends one, since even a chunk's reply is a
text `phx_reply`. Per entry: join `musubi_upload:<entry_ref>` with the
stateless preflight token, then push `config.chunk_size` slices
**sequentially**, each awaiting its `{"progress": n}` ack. Entries run
concurrently with each other (`join_all` on the caller's task — no spawner
involved). There is no `"close"` event: the server completes on
`bytes_written >= client_size`, replies `100`, and stops the channel; the
authoritative signal is the `{op: complete}` on the main channel, and the
per-chunk reply is only an ack. The sub-channel is **always** left afterwards —
success, rejection or cancellation — because a channel left registered would be
rejoined by the socket's own recovery, and with a token still inside its 600s
window that would open a second upload of the same entry.

One deliberate divergence: an **empty file** is sent as one empty chunk. The TS
client's `offset < size` loop sends nothing at all, and the server then waits
for the chunk-timeout watchdog; one empty chunk completes it immediately, and
the server accepts it (`0 >= 0`).

**External mode (BDR-0027).** `ConnectionBuilder::uploader(name, impl Uploader)`
builds a registry the server's `uploader` string dispatches against; a name this
connection never registered fails the entry with `TransferError::NoUploader`
rather than falling back to channel mode. The trait is runtime-agnostic:

```rust,ignore
pub trait Uploader: Send + Sync + 'static {
    fn upload(&self, request: UploadRequest) -> BoxFuture<'static, Result<(), UploaderError>>;
}
```

`UploadRequest` carries the entry snapshot, the bytes, the opaque `meta` from
`upload_external/3`, an `UploadProgress` sink and a `CancelSignal` (pollable
with `is_cancelled()`, or `select!`-able on `cancelled()`). The app does the PUT
itself — the crate ships no HTTP client. On success the client reports
`progress: 100`, which is what makes the server emit `{op: complete}`; on
failure it pushes `upload_error` with `code: "external_failed"` and the
uploader's message, then returns `TransferError::Uploader`.

**Cancellation.** `cancel(Some(ref) | None)` raises the entry's `CancelSignal`,
leaves its sub-channel — which is what makes the server delete the partial file
— and pushes `cancel_upload` per entry, sequentially. The handle's own status is
untouched: the server answers with `{op: cancel}`, which *deletes* the entry, so
there is no cancelled state to observe. `reset` cancels everything, clears the
entries and errors, and returns the handle to `idle`.

**Failures.** Upload-specific ones are `TransferError` (`Rejected`, `Chunk`,
`Cancelled`, `NoUploader`, `Uploader`), reached through `MusubiError::Transfer`;
everything shared with the rest of the client stays on `MusubiError` (`Join`,
`Timeout`, `NotConnected`, `Disconnected`). `start` returns the first failure
and ends the handle in `status = error` — which also covers an entry the
*server* failed with `{op: error}`, where no transfer here returned anything.

**Recovery.** On `soft_reset`, a rejoin or a version gap the handles are
**kept**, matching the TS client. Uploads are not resumable (BDR-0003): an
in-flight entry the server dropped is only cleared once its store leaves the
index or the server emits a `reset`, and a transfer that was running fails on
its own push — disconnected, or the push timeout — rather than being retried.

**Not supported.** The crate reads no files and streams nothing off disk: you
hand it an `UploadFile`, so entry bytes are held in memory for the duration of
the transfer. Size is bounded by the store's `max_file_size` declaration, which
the server enforces at preflight.

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
   `mix musubi.capture_wire` drives the connection-channel harness and writes
   one JSON file per scenario to
   `crates/musubi-client/tests/fixtures/<scenario>.json`:

   ```json
   {
     "scenario": "stream_limit_variants",
     "frames": [{"dir": "in" | "out", "event": "phx_reply", "payload": { }}],
     "expected_state": { }
   }
   ```

   `dir` is relative to the client (`in` = server→client). Fixtures are checked
   in; the regenerate-and-diff CI job in §12.4 fails the build when a
   server-side wire change makes them stale.

   The harness — endpoint, `Musubi.Socket`, fixture root stores, recorder,
   scenario list — lives in `test/support/wire_capture/`, and so does the task
   itself (`test/support/mix/tasks/musubi.capture_wire.ex`): the
   stores it drives are test-only and must not ship in the Hex tarball. The
   same modules back `test/musubi/transport/connection_channel_test.exs`, which
   is what "one mechanism" means here. `test/musubi/wire_capture_test.exs`
   covers the frame schema, the canonical encoding, and that the checked-in
   files match what the current server emits.

   **Replay.** `crates/musubi-client/tests/fixtures.rs` drives every fixture
   back through a real `Connection` over the layer-3 `MockSocket`, so the two
   halves meet: capture is the server's story, replay is the client's. Per
   fixture, in recorded order —

   - an `out` frame is **not** injected. It names the call the client is
     expected to make — `mount`, `command_on`, `Upload::select` / `start` /
     `cancel`, or dropping the last `Mounted` — and the frame the client
     actually writes must equal it: event, payload and topic, in that position
     of the sequence. A mount takes the store marker whose `MODULE` matches the
     recorded join, and a command the payload marker whose `NAME` matches, i.e.
     exactly the types `mix compile.musubi_rust` emits; `State` is a
     `serde_json::Value`, because layer 1's subject is the wire tree and not
     one generated struct's field set.
   - an `in` frame is fed in verbatim: a `phx_reply` answers the oldest
     unanswered push, a `"patch"` rides the join.
   - afterwards the root's snapshot must equal `expected_state`. That is what
     makes the pass non-circular: the document is the **server's**, authored by
     `Musubi.Page.Server`, and the client has to reach it by applying only what
     the fixture delivered.

   The fixture directory is enumerated at run time rather than listed in the
   suite, so a scenario added to `Scenarios.all/0` is replayed the moment it is
   captured.

   Contract details worth pinning:

   - **`expected_state` is the server's own wire root** after the scenario's
     last *delivered* envelope — `Musubi.Page.Server.State.previous_wire_root`,
     read off the page server rather than replayed from the very ops under
     test. It is therefore the document a client's patch engine must hold
     **before hydration**: stream slots are still `{"__musubi_stream__": …}`
     markers and upload slots still `{"__musubi_upload__": …}`, because stream
     and upload contents arrive out of band in `stream_ops` / `upload_ops`.
     Materialization is layer 2's subject.
   - **Determinism.** Output is canonical JSON (sorted keys at every depth,
     two-space indent, trailing newline). Server-issued upload entry refs are
     minted from `:crypto.strong_rand_bytes/1`, so they are renumbered
     `u_0001`, `u_0002`, … in first-appearance order. Phoenix's `ref` /
     `join_ref` counters are not recorded at all. The fixture stores render no
     timestamps, pids or random ids. Two consecutive captures are byte-identical,
     which is what makes §12.4's `git diff --exit-code` gate meaningful.
   - **No binary frames.** The upload *control* plane (`allow_upload`,
     `upload_progress`, `cancel_upload`) rides the connection channel as JSON
     and is captured. Channel-mode chunk transfer rides `musubi_upload:<ref>`
     with raw binary `"chunk"` payloads, which this frame schema cannot
     express; the upload fixtures therefore use **external-uploader** mode,
     which also avoids capturing a signed (and so per-run) token. Extending the
     schema to carry binaries is a separate decision, not made here.
   - **`version_gap` is captured, not synthesized.** Every frame in it is
     server-authored; the recorder simply drops one delivered patch, which is
     exactly what a lost push looks like on the wire. Its `expected_state` is
     pinned to the state before the gap, because the client must reject the
     gapped envelope and keep its last good document.
   - **The replay hydrates before comparing.** `expected_state` is
     pre-hydration, the snapshot is post-hydration, and the only difference is
     the stream slots. The suite therefore substitutes each marker with the
     array that scenario's `stream_ops` materialize to, hand-derived from
     `packages/client/src/streams.ts` — the behavioural reference — rather than
     from `streams.rs`, so the two are still being compared and not merely
     restated. Upload slots are compared as the inert markers they stay.
   - **Two documented asymmetries**, both about frames that are not
     server-authored state:
     - `command_errors` contains one `command` frame with **no `name`**, pushed
       by hand to record the server's malformed-frame reply. No typed client can
       write it — the name is a `Command::NAME` const — so the replay skips that
       frame and its reply, and every other frame still has to match.
     - two scenarios end with client-side teardown the capture cannot contain:
       a rejected join leaves nobody holding the root, so the root is torn down
       and its channel left (nothing may rejoin an orphan, §9), and a version
       gap leaves and re-joins. The replay pins those trailing frames per
       scenario; every other scenario must write exactly what was recorded and
       nothing more.

   The 21 scenarios: `initial_mount`, `root_replace_on_rejoin`,
   `mount_rejected_unknown_root`, `child_mount_unmount`, `version_gap`,
   `command_noreply_replace`, `command_reply_no_patch`,
   `command_add_remove_ops`, `command_errors`, `stream_reset`,
   `stream_insert`, `stream_delete`, `stream_at_variants`,
   `stream_limit_variants`, `async_loading_ok`, `async_loading_failed`,
   `event_only_cycle`, `upload_preflight_ok`, `upload_preflight_rejected`,
   `upload_progress_complete`, `upload_cancel`.
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

Not covered: an integration test booting the Elixir example app and driving a
real socket. The captured fixtures stand in for it, and they are regenerated
from the live server rather than hand-written.

### 12.4 CI jobs

`.github/workflows/ci.yml` carries a `rust` job (the gpui example stays out of
CI):

| Job | Command |
|---|---|
| test (stable) | `cargo test --workspace` — which includes the layer-1 fixture replay, so a checked-in fixture the client can no longer satisfy fails here |
| test (MSRV) | the same on toolchain `1.85` — this is the check §1.4 refers to |
| format | `cargo fmt --all --check` |
| lint | `cargo clippy --workspace --all-targets -- -D warnings` |
| runtime-free core | `cargo check -p musubi-client` + `cargo tree -p musubi-client -i tokio` matching nothing (the gpui embedder\'s configuration) |
| codegen smoke test | `mix test --only rust` after the cargo tests, on both toolchain legs — the `docs/rust-codegen.md` §6.5 `cargo check` over the rendered probe bundle, which needs both the BEAM and a Rust toolchain and therefore lives here rather than in the Elixir job |
| fixture drift | `mix musubi.capture_wire`, then `git add --intent-to-add` + `git diff --exit-code` over `crates/musubi-client/tests/fixtures` — a step of the **Elixir** `test` job, not this one, since it needs the BEAM. The `--intent-to-add` is what makes a brand-new scenario's untracked file count as drift. `mix test` asserts the same gate |

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
becomes relevant only if the crates are ever published to crates.io, and is not
implemented.
