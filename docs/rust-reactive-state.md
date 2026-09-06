# Musubi Rust Reactive State — Design

> This design record is the formal version of `docs/rust-reactive-handoff.md` (the owner handoff). It is written in Simplified Technical English.

This document specifies the **retained reactive state tree**. The tree replaces
the data plane of the Rust client, which works on whole-root snapshots. It
succeeds `docs/rust-client.md` §2.4 (state delivery), §4.2 (the shadow
document), §4.6 (hydration) and §5 (stream materialization). The rest of that
document — the seams, the channel layer, mount, reconnection, commands, events,
uploads, status and the cache — stays normative **in semantics** and is not
touched. The **method names** of two planes, uploads and connection status,
change to the common convention of §2.4, and that change adds no capability and
removes none. If the two documents conflict on the data plane, this document
wins. If they conflict on any other point, this document is wrong.

The wire contract does not change. `docs/client-contract.md`,
`docs/streams.md`, `docs/uploads.md` and `docs/push-events.md` stay as they are,
and the server code does not change. This is a rearchitecture on the client
side. It touches only what happens *after* `PatchEnvelope::decode`.

The normative input is the handoff design of the project owner (the retained
tree, recursive semantic equality, per-node subscription, RAII subscription
tokens, GPUI as an adapter only). Where this document deviates from that
design, or must resolve an ambiguity in it, it marks the place **Deviation** or
**Interpretation** and gives the reason.

---

## 1. What this design replaces

### 1.1 The v1 data plane

Today, for each envelope that the client accepts, the cost per root is:

```
clone the shadow Value  ->  json_patch::patch  ->  rebuild store_id -> pointer
  ->  fold stream_ops over copies  ->  hydration walk (rewrites markers in place)
  ->  St::State::deserialize (whole root)  ->  Latest::set (whole root)
  ->  every updates() subscriber wakes
```

Four properties of this shape are the reason for this work:

| Property | Cost |
|---|---|
| One whole-root deserialization per envelope | A single-field `replace` also costs O(state size) (`docs/rust-client.md` §4.2 accepts this as a v1 trade-off) |
| One whole-root publication per envelope | Any change wakes all the subscribers of all the fields, including cycles that carry only uploads or only events |
| No change set | The change notification rules of §5 have a specification but **no implementation**. A downstream consumer cannot ask which part changed |
| The snapshot identity depends on the cycle | A UI that holds `Arc<St::State>` must derive everything again each time. Nothing survives across a cycle, so nothing can be memoized on it |

The fourth property is the one that costs the native UI the most.
`examples/chat_room/desktop` calls `ListState::reset(count)` each time the
length of the message list changes, and that call discards the cached height of
every row. A new `Arc<State>` does not state *which* rows changed.

### 1.2 The model

```
PatchEnvelope
  ->  one transaction against the retained tree
        ops        -> pointer-addressed reconciliation
        stream_ops -> key-addressed collection reconciliation
  ->  recursive semantic equality, bottom-up over the dirty set
  ->  ChangeSet<NodeId> (+ per-collection keyed edits)
  ->  the subscribers of exactly the changed nodes
  ->  RAII-managed callbacks
```

The tree is retained: the `NodeId` of a node is a client-local identity and
lives longer than any envelope. A `State<T>` binds to a `NodeId`, never to a
JSON pointer. A patch is only an *input*. The decision to notify comes from a
comparison of the semantic value of each node before and after the whole
transaction. The structure of the tree **is** the dependency graph. There is no
signal graph, no thread-local current subscriber and no VDOM, and `value()`
never subscribes implicitly.

### 1.3 The crate layout

**Decision: add exactly two crates, no more and no less.** The five-crate ideal
of the handoff maps to this repository as follows.

| Handoff §29 | The reality of this repository | Reason |
|---|---|---|
| `musubi-protocol` (the wire model) | **Not created** | Below `musubi-client`, the only crate that can consume the wire types is `musubi-state`, and the API of `musubi-state` mentions exactly `StoreId`, `PatchOp` and `StreamOp` — so these three types *move into* it. No other item has more than one consumer, so a protocol crate is not needed to hold them. A crate whose only purpose is to be a dependency of one other crate is a layer, not a boundary. |
| `musubi-state` | `crates/musubi-state` (**new**) | `StateTree`, `Node`, `NodeId`, `NodeKind`, `SemanticValue`, `State<T>`, the navigation views, `Subscription`, `ChangeSet`, `Notify`, the equality test and reconciliation. No network, no UI, no runtime. |
| `musubi-client` | The crate itself does not change. It adds one path dependency on `musubi-state` and **removes `json-patch`** (a reversal of the decision in `docs/rust-client.md` §4.1; see §1.4 of this document) | The actor, the transport, mount, reconnection, commands, events, uploads, the cache and the error taxonomy do not change. |
| `musubi-gpui` | `crates/musubi-gpui` (**new**, `exclude` in the workspace) | It reverses `docs/rust-client.md` §2.3 — see §5.1. |
| `musubi-codegen` | **`Musubi.Codegen.Rust`, an Elixir module** | There is no Rust code generation crate, and there will be none: the generator is a Mix compiler (`mix compile.musubi_rust`) that writes one `.rs` bundle directly into the crate of the consumer. This name in the handoff means a capability that this repository already has. |

The dependency direction, from the top down:

```
musubi-client-tokio ──> musubi-client ──> phoenix-channel
                              │
                              └────────> musubi-state   <── musubi-gpui
                                              │
                                              └──> serde, serde_json, slotmap
```

`musubi-gpui` depends **only** on `musubi-state`. It adapts `State<T>` and
`Subscription` to a gpui entity; it never sees an envelope, a socket or
`Mounted`. This keeps gpui out of the dependency graph of `musubi-client`,
together with the tokio that gpui pulls in transitively through
`gpui_http_client`. The CI gate `! cargo tree -p musubi-client -i tokio`
enforces this line.

**The re-exports keep every existing path valid.** `StoreId`, `PatchOp` and
`StreamOp` move to `musubi-state`, and `musubi-client` re-exports them as
`musubi_client::generated::StoreId` and `musubi_client::{PatchOp, StreamOp}`,
so no consumer path changes and the normative re-export list of the generated
bundle (`docs/rust-codegen.md` §4.5) still resolves. `UploadSlot` gets the same
treatment. It is the `{ name }` snapshot struct that the generator renders when
you declare an upload. It is the projection value of `NodeKind::UploadSlot`, so
it moves down to `musubi-state` with that node kind, and `musubi-client`
re-exports it unchanged as `musubi_client::generated::UploadSlot` (§2.4).

**Deviation (the set of types that move down grew during implementation).** The
same reason moved four more value types: `StoreField<S>`, `AsyncResult<T>`,
`AsyncError` and `AsyncErrorKind` (§3.3 first wrote that the last three do not
move down). The reason is mechanical, and the first count missed it: §2.4
signed off `StoreState::<S>::value() -> StoreField<S>` and
`AsyncState::<T>::value() -> AsyncResult<T>`, and the handle lives in
`musubi-state` — a handle cannot name a return type that lives in a crate which
depends on it, because that is a cycle. `musubi-client` re-exports all four
unchanged from `musubi_client::generated`, the normative list in
`docs/rust-codegen.md` §4.5 stays word for word the same, and no consumer path
changes. The four types go in `crates/musubi-state/src/wire.rs`, under the rule
of item 5 in §1.3.1: this crate adds no inherent method and no local trait impl
to them, so the cost of a split stays "one move plus a set of re-exports".
`PatchEnvelope`, `UploadOp` and `PushEvent` stay in `musubi-client`: they belong
to the envelope wrapper, the upload plane and the event plane, and the tree says
nothing about them. (§5.5 narrows this promise by one step: the re-export of
`StoreId` stays, and the re-exports of `PatchOp` and `StreamOp` become internal
`pub(crate)` paths together with `PatchEnvelope`, because no public signature
mentions them.)

**The dependencies of `musubi-state`.** They are `serde` and `serde_json` (the
tree is built from a `Value` and also projects back to a `Value`, and
`NodeKind::Number` is a `serde_json::Number`), `slotmap`, and `thiserror` —
`TreeError` and `ReadError` are the two public error enums that this document
signs off, the existing error taxonomy of `musubi-client` uses `thiserror`
everywhere, and two hand-written `Display` impls would only make the dependency
list one line shorter and would give nothing in return. That is all. There is
no `futures`, no `tracing` and no runtime.

The absence of `tracing` has one cost. §3.2 accounts for it.

*Interpretation.* The handoff calls `musubi-state` "zero dependency". Read that
here as "no network, no UI, no runtime", because the same handoff writes
`serde_json::Number` and `SlotMap` in its own type definitions. This design
keeps `slotmap` instead of a hand-written version: `latest.rs` replaced
`tokio::sync::watch` because that dependency pulls in a *runtime*, and
`slotmap` pulls in nothing. A generational index is also the one invariant that
you must not write by hand. It makes a `State<T>` that holds a released
`NodeId` detectable, instead of an alias to a recycled node that reports no
error.

#### 1.3.1 Two options compared: the five-crate ideal against the minimal increment

The table above is the conclusion; this subsection states what it costs. The
two options differ in one point only — where the three wire types (`StoreId`,
`PatchOp`, `StreamOp`) live.

First, remove one option that does not exist: **the three types cannot stay in
`musubi-client`**. The `apply(&[PatchOp], &[StreamOp])` function of
`musubi-state` must name them, and `musubi-client` depends on `musubi-state` —
to keep them where they are is a cycle. So there are only two places: a new
`musubi-protocol`, or the inside of `musubi-state`.

| | A: the five crates of handoff §29 | B: the minimal increment (adopted here) |
|---|---|---|
| New crates | `musubi-protocol`, `musubi-state`, `musubi-gpui` | `musubi-state`, `musubi-gpui` |
| Home of the three wire types | `musubi-protocol` | `musubi-state`, re-exported through `musubi-client` |
| Dependencies of `musubi-state` | `musubi-protocol` + serde/serde_json/slotmap | serde/serde_json/slotmap |
| Edges in the dependency graph | 6 | 4 |
| A consumer that wants only the wire types | Gets a crate of 100 lines | Gets the whole retained tree plus `slotmap` |
| Items to maintain at each landing | 3 copies of Cargo.toml, README, lint header, CI path, semver promise | 2 copies |

**The cost of the merge, listed honestly:**

1. **The compilation unit is larger.** A change to one variant of `PatchOp`
   recompiles all of `musubi-state` (the tree, the equality test,
   reconciliation, projection) and its downstream crates `musubi-client` and
   `musubi-gpui`. In option A, the same change recompiles one crate with almost
   no code, plus the downstream crates. The absolute numbers do not matter,
   because `musubi-state` is a medium-size crate of pure logic, not `syn`. The
   direction is still real, and it gets worse as the tree grows.
2. **The dependency direction is fixed, and it is fixed asymmetrically.** After
   the merge, "I want the wire types" implies "I want the whole tree", and the
   opposite statement, "I want the tree but not the wire types", cannot even be
   expressed, because both live in one crate. No consumer suffers from this
   today. The first tool that only wants to decode envelopes will suffer
   tomorrow — a session recorder and replayer, a fixture checker against
   `test/support/wire_capture`, or an adapter on a transport that this
   repository does not own — because it must link `slotmap` and the whole
   reconciliation logic.
3. **The re-exports become a compatibility layer that needs maintenance.**
   `musubi-client` must grow a set of pure forwarding `pub use` statements, so
   that `musubi_client::{PatchOp, StreamOp}` and
   `musubi_client::generated::StoreId` still resolve. This is the tax of the
   merge: one name now has two valid paths, and the normative list in
   `docs/rust-codegen.md` §4.5 names one of them.
4. **`musubi-gpui` sees the wire types transitively.** It needs only
   `State<T>`, `Subscription` and `ChangeSet`, but it also gets `PatchOp`
   through its dependency on `musubi-state`. The compilation cost is
   negligible, but the sentence "the gpui adapter never touches the wire" is
   now only a rule, not a fact about type visibility.
5. **A later split is not a pure move — unless the project keeps one rule from
   now on.** To move the three types into a new crate is a mechanical
   operation, but if `musubi-state` writes an inherent method or a local trait
   impl on them (for example, a tree lookup helper on `StoreId`), the orphan
   rule pins those impls to `musubi-state`, and the split then becomes a real
   API change instead of a move. **This design therefore sets one rule:
   `musubi-state` adds no inherent method and no local trait impl to the three
   wire types, and every helper that the tree needs is a free function of the
   tree or a method on a private type.** Keep this rule, and the cost of the
   split stays "one move plus a set of re-exports".

**The benefits of the merge:**

1. **Two fewer crate boundaries to maintain.** Each crate is one manifest, one
   README, one `#![forbid(unsafe_code)]`/`#![warn(missing_docs)]` header, one
   CI path, and one semver promise that a review will question. The
   `musubi-protocol` of option A would be three type definitions plus one
   document, and the first thing that document must explain is why the crate
   exists.
2. **The dependency graph is simpler.** There are 4 edges instead of 6, and the
   CI gate `cargo tree -p musubi-client -i tokio` has fewer layers to examine.
3. **There is no second consumer today, so a split is speculation.** A crate
   whose only purpose is to be a dependency of one other crate is a layer, not
   a boundary (see the `musubi-protocol` row in the table above), and
   AGENTS.md forbids an abstraction that has no second caller.
4. **The direction is asymmetric, and the asymmetry favors the merge.** To move
   three types out of one crate is a move; to merge two published crates that
   each grew an API means name conflicts, duplicate re-exports and the merge of
   two documents. To merge first and split later is cheaper than to split first
   and merge later.

**When to split (triggers; split when any one holds, they need not all hold):**

- The **first** consumer appears that wants only the wire types and not the
  retained tree (record and replay, fixture check, server simulation, external
  transport adapter). One consumer is enough — it is then the second consumer,
  and the abstraction is justified.
- `PatchOp`/`StreamOp`/`StoreId` change much more often than the tree logic, so
  that work on the wire side waits for `musubi-state` to recompile.
- The wire types need a version rhythm that is independent of the tree API (for
  example, the generator must pin a dependency to a protocol version).
- The rule above, "add no inherent method to the wire types", fails to hold
  back a requirement for the first time — that shows that the wire types have
  started to carry tree semantics, and they must move out.

**Not a trigger:** "the layering looks cleaner", and "the handoff drew five".

### 1.4 One reversal: the client owns the pointer traversal

`docs/rust-client.md` §4.1 delegates RFC 6902 and RFC 6901 to the `json-patch`
crate and states "build no own pointer or patch implementation". This is no
longer possible: `json_patch::patch` works on a `serde_json::Value`, and no
such `Value` exists any more. To keep a shadow `Value` beside the tree only to
host that crate would bring back the whole-tree clone, and the removal of that
clone is the purpose of this design.

**Decision: `musubi-state` owns the pointer resolution.** It is about 80 lines
— token unescaping (`~1` → `/`, `~0` → `~`, in that order, which you must not
reverse), the array index rules (`-` means append, `add` allows
`index == len`, a leading zero is rejected), and application from left to right
in order. The op allowlist stays where it is, at envelope decode time
(`PatchOp` is an enum with three variants), so a downstream consumer never sees
`move`/`copy`/`test`.

The test bed that §4.1 relied on for the delegation does not change, and it now
carries the load: 21 wire fixtures replay every pointer shape that a real
server can send, and `musubi-state` also has its own unit tests for the escape
rules and the index rules.

---

## 2. The five interfaces

All of this section belongs to `musubi-state`. That crate uses
`#![forbid(unsafe_code)]` and `#![warn(missing_docs)]`, as `musubi-client` does.

### 2.1 `NodeId`, `Node`, `NodeKind`, `SemanticValue`

```rust
/// Client-local identity of one retained node.
///
/// Stable for the node's lifetime and **never** reused after the node is freed:
/// the generation half of the index is what makes a `State<T>` that outlived
/// its node read as dead rather than as some later node that took its slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(/* private: slotmap key */);

/// A copy of one node's metadata, as of the moment it was read.
///
/// Nodes are not handed out by reference. A `&Node` would either escape the
/// tree lock or hold it across caller code, and caller code is allowed to call
/// `subscribe()` — so this is an owned copy, produced by `StateTree::node`.
/// It is a diagnostics and adapter surface, not the read path: `State::value`
/// does not go through it.
#[derive(Debug, Clone)]
pub struct Node {
    /// `None` for the root, which is the only parentless node.
    pub parent: Option<NodeId>,
    pub kind: NodeKind,
    /// Bumped only by a transaction that changed this node's semantic value.
    /// `0` means no transaction has ever touched it.
    pub revision: u64,
    pub semantic: SemanticValue,
    /// Live subscriptions on this node. Diagnostics only.
    pub subscribers: usize,
}

/// What a node is, and where its children live.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum NodeKind {
    Null,
    Bool(bool),
    Number(serde_json::Number),
    String(Arc<str>),

    /// A plain JSON array. Children are **index**-identified (handoff §19).
    Array(Vec<NodeId>),

    /// A plain JSON object. Children are key-identified; key order is not
    /// semantic, which is why this is a `BTreeMap`.
    Object(BTreeMap<Arc<str>, NodeId>),

    /// An object that also carries `__musubi_store_id__`. Reconciled by
    /// **store id**, not by position (§3.2).
    Store {
        store_id: StoreId,
        fields: BTreeMap<Arc<str>, NodeId>,
    },

    /// A stream slot: an **ordered, keyed** collection whose contents arrive in
    /// `stream_ops` and never in `ops` (§3.1).
    Collection {
        /// The declared stream name, from the wire marker.
        name: Arc<str>,
        /// The nearest enclosing store, resolved once at node creation.
        owner: StoreId,
        /// Item key -> child, in list order.
        items: Vec<(Arc<str>, NodeId)>,
    },

    /// `{"__musubi_async__": true, "status", "result", "reason"}` (§3.3).
    Async {
        status: AsyncStatus,
        result: NodeId,
        reason: NodeId,
    },

    /// `{"__musubi_upload__": "<name>"}`. Inert: live upload state lives on the
    /// `Upload` plane, never in the tree (§3.4).
    UploadSlot {
        /// The declared slot name, from the wire marker.
        name: Arc<str>,
        /// The nearest enclosing store, resolved once at node creation —
        /// exactly as `Collection` does it. This is the half of the
        /// `(store_id, name)` upload key that used to be spelled by hand at the
        /// call site, and it is what lets the client bridge from the tree to
        /// the upload plane in one step (§3.4).
        owner: StoreId,
    },
}

/// The three wire statuses of an async node. The typed `AsyncResult<T>` an app
/// matches on stays in `musubi_client::generated`; this is only what the tree
/// needs to decide equality.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AsyncStatus { Loading, Ok, Failed }
```

```rust
/// A node's value as equality sees it.
///
/// Cheap to clone (one `Arc` bump), cheap to compare (pointer equality is the
/// fast path), and **structurally shared**: a child that a transaction did not
/// change keeps the exact `Arc` it had, so its parent's comparison stops at the
/// pointer. That sharing is what makes recursive equality operationally
/// incremental rather than a full-tree DFS.
#[derive(Debug, Clone)]
pub struct SemanticValue(Arc<Semantic>);

impl PartialEq for SemanticValue {
    /// `Arc::ptr_eq` first, structural comparison second. Pointer equality is
    /// an **optimization, not the definition**: two distinct allocations
    /// holding equal contents are equal.
    fn eq(&self, other: &Self) -> bool { ... }
}

impl SemanticValue {
    /// The hydrated projection of this value (§3.5).
    pub fn to_hydrated(&self) -> Value;
    /// The wire projection of this value: markers back in place (§3.5).
    pub fn to_wire(&self) -> Value;
}
```

*Interpretation.* The `NodeKind` of the handoff has six variants (`Null`,
`Bool`, `Number`, `String`, `Array`, `Object`). This design adds four variants
that only Musubi has: `Store`, `Collection`, `Async` and `UploadSlot`. The
handoff's own §19 lists a separate capability layer: "a future specialized
KEYED collection reconciliation (e.g. Musubi child stores with stable
store_id)". This design builds that layer now instead of later. The variants
are deliberate: a classifier trait from the host would get exactly one
implementation, and AGENTS.md forbids an abstraction with no second caller. The
generality the handoff wants stays where it is important. The marker *strings*,
the envelope wrapper, the upload plane, the event plane, and every `__musubi_`
constant outside the type definitions of the tree stay in `musubi-client`.

### 2.2 `StateTree`

```rust
/// The retained tree of one mounted root.
///
/// Cheap to clone; every clone addresses the same tree. `Send + Sync`, with the
/// whole node arena behind one `std::sync::Mutex` (§2.6).
#[derive(Clone)]
pub struct StateTree {
    inner: Arc<StateTreeInner>,
}

impl StateTree {
    /// A tree holding one root node, `Null`, revision `0`.
    ///
    /// The root's `NodeId` is allocated here and **never changes** — not on a
    /// `replace ""`, not on a rejoin, not on a cache seed. That is what makes
    /// `Mounted::state()` a value an embedder can hold across a reconnect.
    pub fn new() -> Self;

    /// The root as a typed reactive view. `T` is unchecked here; see §4.4.
    pub fn root<T>(&self) -> State<T>;

    /// The root's `NodeId`.
    pub fn root_id(&self) -> NodeId;

    /// One transaction, applied and committed. `ops` land before `stream_ops`,
    /// which is the only order in which every op's target exists (§3.6).
    ///
    /// Atomic: on any error every mutation is rolled back and the tree is
    /// exactly as it was. Subscribers are **not** invoked here — the returned
    /// guard owes them (§2.3).
    pub fn apply(&self, ops: &[PatchOp], stream_ops: &[StreamOp])
        -> Result<Notify, TreeError>;

    /// A transaction the caller drives, for the one case that needs to inspect
    /// the result before deciding: drift validation (§4.4).
    pub fn begin(&self) -> Transaction<'_>;

    /// Ends the tree: empties the root to `Null`, notifies, and refuses every
    /// later transaction. Terminal — the analogue of `Latest::close`, and what
    /// `RootSink::clear` calls at teardown.
    pub fn close(&self) -> Notify;

    /// A copy of one node's metadata, or `None` if it has been freed.
    pub fn node(&self, id: NodeId) -> Option<Node>;

    /// The hydrated projection of a subtree: stream slots as arrays, store
    /// nodes carrying `__musubi_store_id__`, upload slots as their marker,
    /// async nodes as their wire shape (§3.5). What `State::value` reads.
    pub fn to_hydrated(&self, id: NodeId) -> Option<Value>;

    /// The wire projection of a subtree: stream slots back to
    /// `{"__musubi_stream__": name}`, everything else as above. What the mount
    /// cache stores (§7).
    pub fn to_wire(&self, id: NodeId) -> Option<Value>;

    /// Every live store id. Replaces the pruning half of `index.rs` (§3.5).
    pub fn store_ids(&self) -> Vec<StoreId>;

    /// The node a store id resolves to, or `None` if that store is not mounted.
    pub fn store_node(&self, store_id: &StoreId) -> Option<NodeId>;

    /// Node count. Tests and diagnostics.
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;

    /// Whether `close` has ended this tree. `pub(crate)`: §5.5's read half does
    /// not carry it, and a consumer asks `State::is_live`, which folds it
    /// together with "the node is still there".
    pub(crate) fn is_closed(&self) -> bool;
}
```

**Deviation.** The handoff writes `apply(&mut self, ...)`. The `&self` here is
not a preference. It is necessary: the handoff's own §5 defines
`State<T> { tree: Arc<StateTreeInner>, .. }`, and an `Arc` cannot give a
`&mut`. Interior mutability is already implicit in §5. This text only makes it
explicit.

### 2.3 `apply()`, `Transaction`, `ChangeSet`, `Notify`

```rust
/// An open transaction. Holds the tree's lock; `!Send`, and lives on whichever
/// task drives the envelope (the actor task).
///
/// Dropping it **rolls back**. `commit` is the only way to keep the work — the
/// journal is a drop guard, so a panic mid-transaction unwinds through the
/// rollback and leaves the tree consistent rather than half-applied.
pub struct Transaction<'a> { ... }

impl Transaction<'_> {
    /// Applies one batch. May be called more than once; every call joins the
    /// same transaction, so `1 -> 2 -> 1` across two calls still notifies
    /// nobody.
    pub fn apply(&mut self, ops: &[PatchOp], stream_ops: &[StreamOp])
        -> Result<(), TreeError>;

    /// The hydrated projection of a node **as this transaction has it**, before
    /// it is committed. The one thing a caller inspects mid-transaction, and
    /// only for drift validation (§4.4).
    pub fn to_hydrated(&self, id: NodeId) -> Option<Value>;

    /// Settle the dirty set bottom-up, diff, bump revisions, collect
    /// subscribers, release the lock. Nothing here can fail.
    #[must_use = "dropping the Notify is what runs the subscribers"]
    pub fn commit(self) -> Notify;
}

impl Drop for Transaction<'_> {
    /// Replays the journal backwards: restores every mutated node's kind,
    /// semantic value and revision, and frees every node the transaction
    /// allocated. O(diff), not O(tree) — which makes atomicity **cheaper** than
    /// v1's, where it cost one whole-tree clone per envelope.
    fn drop(&mut self) { ... }
}
```

```rust
/// What one transaction changed.
#[derive(Debug, Clone, Default)]
pub struct ChangeSet { ... }

impl ChangeSet {
    /// Every node whose semantic value changed, children before parents.
    pub fn changed(&self) -> &[NodeId];
    pub fn contains(&self, id: NodeId) -> bool;
    pub fn is_empty(&self) -> bool;

    /// The keyed edits one collection node took, in application order. Empty
    /// for every node that is not a `Collection`, and for a collection whose
    /// change was confined to an item's own fields.
    ///
    /// Also empty for a node that is not in this change set at all — a
    /// transaction that rewrote a list into exactly what it already was
    /// changed nothing and edited nothing.
    ///
    /// This is the surface an incremental list adapter consumes (§5.1); it
    /// reaches that adapter as the second argument of
    /// [`StreamState::subscribe`](StreamState::subscribe) (§6.3).
    pub fn collection_edits(&self, id: NodeId) -> &[CollectionEdit];
}

#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum CollectionEdit {
    Inserted { item_key: Arc<str>, index: usize, node: NodeId },
    Removed  { item_key: Arc<str>, index: usize },
    Moved    { item_key: Arc<str>, from: usize, to: usize },
    /// Everything before this edit is gone; what follows rebuilds the list.
    Reset,
}
```

```rust
/// The callbacks a committed transaction owes, and the change set that
/// produced them.
///
/// **The tree lock is already released when this exists.** There is no API that
/// hands a caller a callback while the lock is held; that is the handoff's
/// never-notify-under-the-lock rule made structural rather than conventional.
///
/// Dropping it invokes every owed callback exactly once, on the dropping
/// thread. Holding it is how a caller sequences notification against the rest
/// of its own commit (§3.6).
#[must_use = "dropping this is what notifies subscribers"]
pub struct Notify { ... }

impl Notify {
    /// What the transaction changed. Readable before the callbacks run.
    pub fn changes(&self) -> &ChangeSet;
}

impl Drop for Notify { ... }

/// What a subscriber is told. No old/new value: the callback re-reads through
/// its own `State<T>` (handoff §24–25).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct Change {
    /// The node's revision after the transaction.
    pub revision: u64,
}
```

```rust
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TreeError {
    /// The pointer did not resolve, or resolved into a non-container.
    Pointer { path: String, reason: &'static str },
    /// An array index was out of bounds, or not a valid RFC 6901 index token.
    Index { path: String },
    /// This value would nest a node past the depth cap of the tree (256
    /// levels). Depth accumulates across ops and across envelopes, and the
    /// recursion that walks a subtree uses the caller's stack. The tree
    /// therefore refuses the value at the write boundary, where the transaction
    /// can still roll back cleanly, instead of waiting for a stack overflow
    /// that aborts the process.
    Depth { limit: usize },
    /// The transaction was applied to a tree that `close` had already ended.
    Closed,
}
```

`musubi-client` maps `TreeError` onto its existing taxonomy. `Pointer` and
`Index` become `MusubiError::Patch(PatchError::Apply)`, the same class of
version mismatch failure that `json_patch::PatchError` produced before.
`Closed` is not reachable from the actor, because the actor always discards a
root before it closes the tree of that root.

Inside `commit`, the steps are in this order:

1. **Settle.** Recompute the `SemanticValue` for the dirty set bottom-up, then
   for the ancestors of each dirty node. An unchanged child contributes the
   `Arc` it already had, so the recomputation of a parent is only a sequence of
   pointer copies.
2. **Diff.** Compare each recomputed value with the value recorded when the
   node was first touched. Unchanged ⇒ restore the *old* `Arc`, so that the
   pointer fast path of an ancestor still hits, and leave the revision alone.
   Changed ⇒ increment the revision, and record the node in the `ChangeSet`.
3. **Collect.** Walk the change set, and clone the subscriber handle of each
   node into `Notify`.
4. **Release** the lock and return.
### 2.4 `State<T>` and the navigation views

```rust
/// A typed reactive view rooted at one node of a shared retained tree.
///
/// `State<AppState>`, `State<Vec<Item>>`, `State<Item>` and `State<String>` are
/// the same thing; they differ only in typed navigation. Any subtree is a full
/// reactive state — `value()`, `subscribe()`, `revision()` — and is passable to
/// a component that knows nothing about the root.
pub struct State<T> {
    tree: StateTree,
    node: NodeId,
    _marker: PhantomData<fn() -> T>,
}

impl<T> Clone for State<T> { ... }   // hand-written: `T: Clone` is not required
```

`PhantomData<fn() -> T>` makes `State<T>` **`Send + Sync` for every `T`, including
a `T` that is `!Send`**, and covariant over `T`. This is essential: it lets
`State<Item>` move to the UI thread without `Item: Send`. It is also the reason
the marker uses a function pointer and not `PhantomData<T>`.

```rust
impl<T> State<T> {
    /// The node this view is rooted at.
    pub fn node(&self) -> NodeId;
    /// The tree it belongs to.
    pub fn tree(&self) -> &StateTree;

    /// The node's revision. `0` means no transaction has ever touched it —
    /// which for a root is exactly "the initial patch has not landed" (§5.3).
    pub fn revision(&self) -> u64;

    /// Whether the node is still in an open tree. `false` once the node was
    /// removed, or once the tree was closed by teardown.
    pub fn is_live(&self) -> bool;

    /// Re-type this view in place. The escape hatch codegen and hand-written
    /// navigation both use; no data moves.
    pub fn cast<U>(&self) -> State<U>;

    /// The child at `key` — the primitive every generated field accessor is
    /// built from, and **infallible**, as the handle law below requires:
    /// `x.prop()` costs nothing, reads no value and cannot fail. A key this
    /// node does not hold yields a handle rooted at a null `NodeId`, which
    /// reads `is_live() == false` and `try_value() == Err(ReadError::Gone)`.
    pub fn child<U>(&self, key: &str) -> State<U>;

    /// `child`, with an absent key reported instead of handed back as a dead
    /// handle — for the places where absence is a branch rather than a state
    /// (`AsyncState::result`).
    pub fn field<U>(&self, key: &str) -> Option<State<U>>;

    /// Subscribe. RAII: dropping the returned guard unsubscribes.
    ///
    /// `value()` never subscribes implicitly — there is no thread-local current
    /// subscriber and no automatic dependency tracking (handoff §11, §32).
    #[must_use = "dropping the subscription unsubscribes"]
    pub fn subscribe(&self, on_change: impl Fn(Change) + Send + Sync + 'static)
        -> Subscription;
}

impl<T: DeserializeOwned> State<T> {
    /// This subtree's value: one detached, non-reactive snapshot of it.
    ///
    /// The single materialization point. What comes back is a plain `T` with no
    /// tie to the tree — not a handle, not a view, not a guard (§2.4).
    ///
    /// # Panics
    ///
    /// If the node was removed, or if its shape does not match `T`. Both are
    /// contract violations the caller can rule out; see §4.4 for the honest
    /// accounting and `try_value` for the checked form.
    #[track_caller]
    pub fn value(&self) -> T;

    /// The same read, with the failure reported instead of raised.
    pub fn try_value(&self) -> Result<T, ReadError>;
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ReadError {
    /// The node has been removed, or the tree was closed.
    Gone,
    /// The node's shape does not match the requested type — codegen drift.
    Shape(#[from] serde_json::Error),
}
```

#### The uniform convention: a property is a handle

**Define the words first.** This document uses only four nouns. The whole
document uses them exactly as the table below gives them. A mention of one of
them means that row, and no other row:

| Term | What it is | What gives it | One-sentence test |
|---|---|---|---|
| **handle** | The client-side form of one property: it has an identity, you can store it in a struct, you can pass it to a component that does not know the root exists, and you can subscribe to it | `x.prop()` | Zero cost, reads no value, cannot fail |
| **value** | One snapshot detached from the reactive system: plain Rust data with no further relation to the tree | `handle.value()` | The single materialization point; you pay for what you read |
| **subscription** | One live observation, RAII; a drop unsubscribes | `handle.subscribe(cb)` | The receipt is always `Subscription`, so one `Vec` holds all of them |
| **stream form** | The `await` shape of the same subscription, for a consumer that writes a loop | `handle.into_stream()` — only the two handles outside the tree have it (next table) | Not a handle and not a getter; `into_` is a shape conversion |

These four words fix the three roles that readers confuse most often:
**`x.prop()` gives a handle, `value()` gives a value, and `into_stream()` gives
the other shape of one subscription.** A name that contains `value` returns only
a value. A name that contains `stream` returns only a stream. Every other
property accessor returns a handle. There is no exception and no second set of
words.

On top of that, this is a first-class rule. It applies to the **whole API
surface**, not only to the tree:

> **Every observable property gives a handle through `x.prop()`. Every handle
> has `.value()` to read the current value, and `.subscribe(cb) -> Subscription`
> to install one RAII subscription.**

Four actions, four fixed forms, and no fifth form:

| What you want to do | The form |
|---|---|
| Access a property | `x.prop()` — zero cost, gives a **handle** |
| Read the current value | `handle.value()` — the single materialization point, gives a **value** |
| Observe a change | `handle.subscribe(cb)` — the single subscription entry point, the receipt is `Subscription` |
| Cancel an observation | `drop(subscription)` — there is no `unsubscribe()` (§2.5) |

**The rule also holds on the two surfaces outside the tree, although they are
not nodes.** The five views on the tree have this shape by construction. The two
surfaces outside the tree take this shape by their signatures:

| Surface | Property accessor | The three actions on the handle |
|---|---|---|
| Connection status | `Mounted::status() -> StatusState` | `.value()` / `.subscribe()` / `.into_stream()` (§5.4) |
| Upload | `Mounted::upload_at(&slot) -> Option<Upload>` | `.value()` / `.subscribe()` / `.into_stream()` (§6.4) |

The strength of the rule is that it excludes two shapes. The first shape is **a
read that takes the name of the property itself, with a parallel method name for
the subscription**. In that shape, whether `status()` gives a value or a handle
depends on whether the reader remembers that the other name exists. The second
shape is **a second set of verbs for the same pair of actions inside one crate**.
The tree would call them `value`/`subscribe`, the surfaces outside the tree would
use other words, and the reader must first know which plane he is on. Neither
shape exists. `Mounted` has **no second way to read**: the two property accessors
`state()` and `status()`, plus `upload_at(&slot)` which takes a handle by slot
(§3.4), all give a handle, and each handle continues with the same `.value()` /
`.subscribe()`.

**A comparison table for the whole API surface.** The seven handles set out in
full — which one has `revision()`, which one has `into_stream()`, and why:

| Handle | What gives it | Property access (child handles) | `value()` gives | The `subscribe` callback | `revision()` | `into_stream()` | What is behind it |
|---|---|---|---|---|---|---|---|
| `State<T>` | `Mounted::state()`, the generated field accessors | the generated field accessors, `at`/`first`/`last`/`iter`, `as_some` | `T` | `Fn(Change)` | Yes | No | A tree node |
| `StreamState<T>` | the generated `stream` field accessor | `at` / `by_key` / `iter` | `Vec<T>` | `Fn(Change, &[CollectionEdit])` | Yes | No | A tree node (`Collection`) |
| `StoreState<S>` | the generated field accessor for a child store | `fields()` | `StoreField<S>` | `Fn(Change)` | Yes | No | A tree node (`Store`) |
| `AsyncState<T>` | the generated field accessor for an async field | `result()`, `reason()`, `ok_stream()` | `AsyncResult<T>` | `Fn(Change)` | Yes | No | A tree node (`Async`) |
| `UploadSlotState` | the generated field accessor for an upload slot | None — a leaf | `UploadSlot` | `Fn(Change)` — **never fires** (§3.4) | Yes, but it does not increment after creation | No | A tree node (`UploadSlot`) |
| `StatusState` | `Mounted::status()` | None — a leaf | `MountStatus` | `Fn(MountStatus)` | **No** | **Yes** (latest-value, replayed on the first poll) | A `Latest<MountStatus>` cell (§5.4) |
| `Upload` | `Mounted::upload_at(&slot)` (the primitive is `Mounted::upload(&store_id, name)`, §3.4) | None — a leaf | `UploadHandle` | `Fn(&UploadHandle)` | **No** | **Yes** (a queue, no replay) | A cell of the `Uploads` registry (§6.4) |

Three columns differ. Each difference has its reason below, so that no
difference looks like an oversight:

**Only the tree has `revision()`.** The revision is a counter of
**transactions**. It is monotonic per node, and it increments only when one
transaction really changes the semantic value of that node. It can therefore
answer two questions that hold only for the tree: "did this notification carry a
real change or a rewrite with the same value" (§9.3), and "has the initial patch
landed" (`revision() == 0`, §5.3). The two cells outside the tree have no
transaction. Their writes come from the socket lifecycle and from the upload
control plane, and they take part in the semantic settlement of no envelope. A
counter on them would promise a set of properties that only the tree has: at most
one notification per transaction, and no notification from a transaction that
restores the previous value. The upload cell has queue semantics and does not
merge. **A number must come from settlement, and not from the wish to add one
more counter.**

**Only the surfaces outside the tree have `into_stream()`, which is the opposite
direction.** The tree does not give it. `musubi-state` has no async surface
(§1.3), and there are only two ways to create a stream on a node. One is a queue
per node, which is unbounded and is exactly the thing §5.2 excludes. The other is
a latest cell per node, which materializes once per node per envelope and is
worse than one cell for the whole root. A consumer that needs a `Future` or a
`Stream` connects one itself: the ten lines of `oneshot` in §6.1 do this, and an
mpsc gives a stream. The two cells outside the tree **are already streams**.
`into_stream()` is not a new mechanism; it keeps the existing stream on the
handle. A consumer that lives inside an async block and must `await` one
condition — the place in §6.5.1 that waits for `Live` — needs the stream form and
not a callback. **Two shapes, one property: `subscribe` is for a consumer that
puts the observation into a struct, and `into_stream` is for a consumer that
writes a loop.**

**Why this method is named `into_stream()` and not `updates()`.** During the
review of the line `let mut statuses = chat.status().updates();` the owner asked:
"does this `updates` get a handle?" **The need to ask that question is the proof
that the name is wrong.** On the unified API surface, the property accessor is
what gives a handle, and `updates` reads like a property accessor, because it is
a noun. It therefore sits in the same position as `status()` and appears to do
the same thing, so the reader cannot tell whether he holds a handle, a value or
something else. The one word `into_stream` answers three things at once:

- **`into_` is the Rust idiom for a shape conversion** (`into_iter`,
  `into_inner`, `into_bytes`). A reader who sees it expects the same thing in
  another shape, with the original consumed. He does not expect a sub-object or a
  value. That is exactly what the method does: one subscription becomes a
  `Stream`.
- **It takes `self` by value, so the signature states the consuming semantics by
  itself** — see below. `updates(&self)` only borrows and gives the borrow back,
  which looks like a getter and therefore like a property accessor.
- **It has its own row next to `value()` in the same table of terms**, so the two
  do not mix: a name with `value` returns a value, a name with `stream` returns a
  stream, and every other property accessor returns a handle.

The signatures are fixed as follows, and they are the same in both places:

```rust
impl StatusState {
    /// Consumes one handle, hands back the same subscription in `await` shape.
    fn into_stream(self) -> impl Stream<Item = MountStatus> + Send + 'static;
}

impl Upload {
    fn into_stream(self) -> impl Stream<Item = UploadHandle> + Send + 'static;
}
```

**Taking `self` is not a restriction, because a handle is `Clone`.** The common
form `chat.status().into_stream()` costs nothing: `status()` creates a new handle
anyway, and that handle is the one the call consumes. A consumer that already
holds a handle and needs it later consumes a clone — `upload.clone().into_stream()`,
which is the shape used in §6.5.1. If the signature took `&self`, the
documentation alone would have to state that the stream is the subscription.
Taking `self` states it in the type system: **you give one handle and you get one
stream; clone the handle if you need both.**

**A callback outside the tree carries a value; a callback on the tree carries
only `Change`.** The handoff §24 states that a callback receives only the
revision, and that the callback re-reads the value itself. That rule holds only
if **the re-read can recover the information**, because the value of a node stays
on the tree. The two cells outside the tree do not meet that condition. The
status cell merges, so a callback that re-reads `value()` can read a value that is
**later** than the transition that woke it. The upload cell is a queue, so "which
item woke me" is not in the current value at all. This is the same criterion that
makes `StreamState` carry `&[CollectionEdit]` (see the Deviation later in this
section): **information that a re-read cannot recover must arrive with the
notification.** The cost here is exactly zero: `MountStatus` is a one-byte `Copy`
enum, and `UploadHandle` is given by reference, so a consumer that must keep it
calls `clone()` itself.

**What does not take part in this unification, and why.**

**An event does not take part (§6.2).** An event is a **discrete occurrence**,
not a property. None of the three actions holds. An event has no current value,
so `value()` has no definition: "the current toast" is not a meaningful phrase.
An event cannot merge: two `MessagePosted` events are two occurrences, and not
two versions of one occurrence. A late subscription misses an event (BDR-0032),
while a late property subscription still gets the value from `value()`. To force
an event into the `value()`/`subscribe()` shape, you must do one of two things.
You must invent a current value that holds the most recent event, which makes a
slow consumer drop events silently and cancels the delivery promise of BDR-0032.
Or you must make `value()` give something different on every call, which means it
is not a property. A queue is the correct semantics for an event, and the
comparison table in §6.2 argues this row by row. Only one point is added here
about the relation to this section: **the unification covers properties, and not
everything that changes.**

**A command and its receipt do not take part (§6.1).** A command is one action,
and the receipt is the one-time result of that action: `command(..).await`
returns a value, not something you can observe again. The example in §6.1
therefore needs no change; it already has the unified shape. `reply.ok` and
`reply.message` are **ordinary field accesses after materialization** (see the
next section, "Field access has not disappeared; it happens after
materialization"). The truly observable question, "has this command landed", is
answered in §6.1 by the handle `state.total()`, and not by the receipt.
`command_on(&panel.store_id(), ..)` follows the same rule: `store_id()` is an
identity, not a property.

**The metadata of a handle itself is not a property.** `node()`, `revision()`,
`is_live()`, `store_id()`, `len()`/`is_empty()`/`keys()`, and
`AsyncState::status()` return a value directly, not a handle. There is one
criterion, and it is decidable:

> **Does this read have its own subscribable identity?** If yes, it is a
> property and it gives a handle. If no, it is one projection of the value of the
> handle itself, and it stays an ordinary method.

`store_id()` is an identity and not a value: it is constant, and a change of it
means another store. `len()` is one projection of the semantic value of the
collection node itself; to subscribe to the length is to subscribe to that
collection, and there is no second thing to subscribe to. `revision()`,
`is_live()` and `node()` describe the handle, and not the value under
observation.

**One difference that must be named: `AsyncState::status()` and
`Mounted::status()` have the same name and a different shape.** The first returns
an `AsyncStatus` value; the second returns a `StatusState` handle. This is not a
missed edit. It is the criterion above, applied in two places, with two different
answers. The status of an async node is **part of the semantic value of that node
itself** (§3.3 — for this reason a change from `loading` to `ok` notifies this
node even when the result did not change). It has no node of its own, no revision
of its own and no subscriber list of its own: `feed.status()` and
`feed.subscribe(..)` already refer to the same thing, and a handle for it would
only be a second name for the same node. `MountStatus` has its own cell and its
own subscriber list, and it is synchronized with no node on the tree (§5.4), so
it is a separately observable thing. A consumer that wants to act only when the
status changes subscribes to `feed` and compares in the callback. That is a
filter, and the consumer writes it in three lines. For the framework to write it,
the framework must first invent a node that the server never renders on its own.

**The other half of the unification: there is only one receipt type.** The
`subscribe` method of all seven handles returns the same `Subscription` (§2.5),
and the two handles outside the tree do the same. This is not tidy naming; it is
**what the unification really buys**: one view can put all of its observations
into one `Vec<Subscription>`, where they live together, end together, and are
watched together by `#[must_use]`. The gpui view in §6.5.2 therefore has only one
such field. The three observations — state, connection status and upload — are in
the same `Vec`, and not in one `Task<()>` each.

**Back to the earlier question from the owner: "can we drop the `get` function
and access the property directly?"** (At that time the read method was still
named `get()`. The next section explains why it is now named `value()`.)

Half of the answer is that it already is: **`state.count()` is the property
access itself.** It reads no value, takes no lock and cannot fail. The handle it
gives **is** the client-side form of the property `count`: it has an identity
(`NodeId`), it has a revision (`revision()`), you can store it in a struct, you
can subscribe to it alone, and you can pass it to a component that does not know
the root exists. This does much more than a plain field, and the call is just as
short. At this level the request of the owner is **fully met**: on the whole API
surface, every observable property is `x.prop()`, with no parallel second method
name and no verb that belongs to one surface only.

The other half is this: to get the **value** of that property, you need an
explicit point, and in Rust that point can only be one method call. This is not
ceremony that you can compress further. It is the meeting of one language
constraint and one design property. The section after next writes it out together
with the three rejected ways to approximate property syntax. The two halves
together are the complete answer: **the access is a property, and the
materialization is explicit.**

#### Why that method is named `value()`, and not `get()`, and certainly not `handler()`

During the review of the line `let slot = state.attachment().get();` the owner
proposed: "can `get` take another name, such as `handler`?" **The direction of
the proposal is right, and the word is the wrong one.** The way the word is wrong
shows the most important fact about this API surface, so it is recorded here
directly.

**Why the name cannot be `handler` or `handle`.** In this convention, **the
handle is what `x.prop()` returns**: the thing `state.attachment()` gives is the
handle. The read method on a handle returns the **opposite** of a handle: one
value snapshot detached from the reactive system. A name such as `handle()` or
`handler()` makes `state.attachment().handle()` read as "take a handle out of a
handle", which states the two roles backwards. The real handle then has no name,
because `attachment()` looks like one step of a path, and the value is called the
handle instead. The two comments from the owner point to the same cause: the
names of the three roles — handle, value and stream form — are not distinct
enough. The correction must therefore make **every name state which role it
returns**, instead of moving the most confusing word to another position.

**Why `value()` — the option next to the one the owner proposed.** The real
request in the proposal is that the word `get` does not state what it gives, and
that request is fully correct. `get` is a general verb. In the standard library
it can give `Option<&T>` (`Vec::get`) and it can give an owned `T` (`Cell::get`
gives a copy), so the reader must remember the context to know what he received.
`value()` is the strongest word for that position:

1. **It states directly what it returns** — a value, not a view, not a guard, not
   a handle. The row "value" in the table of terms is its definition, and the row
   "handle" is the definition of `x.prop()`. Each role has its own name, so the
   reader does not need memory to tell them apart.
2. **It removes the semantic conflict with index addressing on a collection.**
   The handoff wrote `State<Vec<T>>::get(&self, index) -> Option<State<T>>`,
   which has the same name as the read method that every handle has, and the
   opposite meaning: one navigates and one materializes. This design renames it
   to `at` (see the Deviation below). After the rename, the word `get` is free on
   the whole API surface — and **free is the state it must keep**. If the Rust
   idiom `Vec::get(i)` came back, `x.get(3)` would return a handle and `x.get()`
   would return a value, one word would again carry two roles, and the confusion
   in comment 1 would return at once. With `value()` and `at()`, the two roles
   never share one verb.
3. **It aligns this read with its cost.** Every occurrence of `.value()` is one
   materialization. Every occurrence of `.subscribe()` is one subscription. Every
   occurrence of `.into_stream()` is one shape conversion. Three actions, three
   words, and no word does two jobs.

**The word `handler` is not taken, but the request behind it is taken.** For the
record: what is rejected is **the word**, not the comment. The comment is that
`get` is not clear. This design accepts it, and it extends the correction from
one method to three roles. That is why the table of terms at the start of this
section exists.

*The scope, stated exactly.* The seven handles use the same name for the read
method (`State`, `StreamState`, `StoreState`, `AsyncState`, `UploadSlotState`,
`StatusState`, `Upload`), and `try_value()` is the variant that does not panic.
The whole handle family has no second spelling.

#### Why a read is written as `value()`, and not as a direct property access

The two sections above answered half of the question: the property access is
already `x.prop()`, the seven handles are treated alike, and the read method is
named `value()`. This section answers the other half: **why the materialization
step must be one method call**, and why the form `state.count` cannot buy the
same thing in Rust. The reason is at the language level, not a matter of style.
The fatal problem of each of the three ways to approximate property syntax is
written here too, because this `value()` carries the most central property of
this design.

**Rust has no computed property.** In Rust the syntax `state.count` has only one
meaning: it reads a **memory field that already exists**. It runs no code, takes
no lock, reports no error and constructs nothing on demand. `State<T>` has
exactly three fields: `tree`, `node` and `_marker`. The value is not in `State`
at all. It is on some node of the shared arena, and to take it out you must take
the lock, walk the subtree and deserialize. That is a **computation**, and field
access syntax cannot express a computation. The only way to make `state.count`
work is to materialize `count` in advance and put it into the struct, which is
exactly the whole-root snapshot of v1 that this design removes.

The three ways to approximate property syntax, and the fatal problem of each:

**(a) `Deref` to a snapshot guard.** Write
`impl Deref for State<ChatState> { type Target = ChatState }` and
`state.current_user` compiles. Two problems:

- *The signature does not work at all.* `fn deref(&self) -> &Self::Target` must
  return a reference borrowed from `self`, and `self` holds no `ChatState` to
  borrow. The only way to produce that reference is to make `State` cache one
  materialized result itself: one `OnceCell<T>` per node, invalidated after every
  transaction. That replaces the whole-root snapshot of v1 with a per-node
  snapshot, and both the memory use and the invalidation logic get worse.
- *Even the weaker form with an explicit guard (`state.read().current_user`)
  hides the cost.* The guard either holds the lock, and then user code runs under
  the lock. That breaks §2.6, "the API runs caller code under the lock in exactly
  one place", and one long borrow during a render blocks the actor from landing
  the next envelope. Or the guard holds one clone, and then every "property
  access" is in fact one materialization of a whole subtree, although it looks
  like one field read. Both make "materialize as much as you read" invisible,
  against the principle of handoff §11: a read must not subscribe implicitly, and
  an implicit cost must be made explicit. There is one more practical
  consequence: **the lifetime of the guard leaks into the caller.** It is not
  `Send`, it cannot cross an `await`, and it cannot go into a gpui view field,
  and a consumer does these three things every day.

**(b) A navigation method that returns the value directly
(`state.current_user() -> OnlineUser`).** This does remove `.value()`, but it
removes reactive navigation with it. The handoff §7 states that navigation must
stay reactive (`state.items() -> State<Vec<Item>>`, and **only** `.value()`
materializes). Once navigation returns a value, there is no node to `subscribe`
to, and `state.current_user().name()` degrades into "materialize the whole user
first, then take one field out of it". Every step down costs one subtree
materialization, and per-node subscription loses its basis. This does not save
one `.value()`; it replaces the design.

**(c) The unstable `Fn` traits (`state.count()` gives the value directly).**
`impl FnOnce for State<i64>` lets `state.count()` evaluate to `i64`. There are
two reasons to reject it. It needs `#![feature(unboxed_closures, fn_traits)]`,
which is nightly-only, while these crates are fixed to the stable MSRV 1.85. And
it meets the same conflict as (b): the form `state.count()` is already taken by
navigation that returns a `State`, and one piece of syntax cannot be both
navigation and materialization.

**Conclusion: materialization stays one method call, and that method is named
`value()`.** It is not ceremony that you can drop. It is where the design
property "an explicit materialization point" lands in the syntax. Every place in
a line of code where `.value()` appears is a place where a reactive view becomes
one value detached from the tree, which is the place where this read **costs
something**. Every place without `.value()` is navigation at zero cost. §10.1
discusses **how** to implement this materialization point (two paths, with the
preference already fixed), and never **whether** it must exist.

**Field access has not disappeared; it happens after materialization.** The
reactive property access is `x.prop()`, as stated above. The TypeScript client
can write `state.count` because the state there **is** an ordinary object. On the
Rust side, the thing that corresponds to that ordinary object is the generated
snapshot struct, and it is one step to the right of `value()`:

```rust
let user = state.current_user().value();   // one materialization, one explicit point
user.id;                                   // after that, ordinary field access, zero cost
user.name;
```

After one `value()` you can read as many fields as you need, and you read one
**consistent** value. It is a snapshot detached from the tree, and the next
envelope cannot change it between two field reads. The misuse is the opposite:
one `.value()` per field. `state.current_user().id().value()` and
`state.current_user().name().value()` are two materializations, two locks, and
two values that can come from different transactions. There is only one valid
reason to write them apart: you really must subscribe to the two fields
separately.

**Do we add one layer of sugar (`Display` / `PartialEq<T>` on a scalar)?
Decision: do not add it.** The proposal is to let `format!("{}", state.title())`
and `assert_eq!(state.title(), "Cart".to_owned())` omit one `.value()`. There are
three concrete reasons, and none of them is about taste:

1. **It moves a panic into the formatter.** `value()` panics when the shape does
   not match or when the node was removed (§4.4 argues for this choice and gives
   it `#[track_caller]`). A panic inside `Display::fmt` breaks the caller frame
   on one log statement, and the author of that log statement believes he is
   doing something that cannot fail. That is a fault created inside diagnostic
   code.
2. **`PartialEq<T>` turns a failure into a silent `false`.** A node that was
   removed is equal to no value, so the assertion reports a wrong value while the
   truth is that the node is gone. `try_value()` and `is_live()` keep these two
   cases fully apart today, and this sugar mixes them again. In a test that is
   worse than a panic.
3. **It buys almost nothing.** The about 25 assertions in `tests/connection.rs`
   already work when written as `cart.state().title().value()`. The sugar saves
   eight characters, and the cost is that "one materialization happens here"
   becomes hard to see in the two places where it must be most visible: logs and
   assertions.

**The one that the same reasoning finds harmless, and that this design therefore
provides: `Debug`.** `State<T>`, `StreamState<T>`, `StoreState<S>` and
`AsyncState<T>` all implement `Debug` (hand-written, for the same reason as
`Clone`: it does not require `T: Debug`), and it **prints the identity of the
view, not the value** — `State { node: NodeId(7), revision: 3, live: true }`. It
does not materialize, does not extend how long the lock is held, cannot panic,
and implies no subscription semantics. It lets `dbg!(&rows)` or one log line
answer "which node is this, and what is its current revision" without a
materialization first. To see the value, write `.value()` as before. That is the
line this section keeps.

An ordinary JSON array:

```rust
impl<T> State<Vec<T>> {
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
    pub fn at(&self, index: usize) -> Option<State<T>>;
    pub fn first(&self) -> Option<State<T>>;
    pub fn last(&self) -> Option<State<T>>;
    /// Snapshots the child ids under the lock, then yields views. The
    /// iterator holds no lock, so a consumer may `subscribe()` while iterating.
    pub fn iter(&self) -> impl Iterator<Item = State<T>>;
}
```

**Deviation.** The handoff wrote
`State<Vec<T>>::get(&self, index) -> Option<State<T>>`. It is renamed to `at`.

*The reason changed after the read method was renamed to `value()`, and it became
stronger.* The first reason was a name collision: `get` was already taken by the
read method of every `State<T>`, and the two meanings are opposite (one
materializes, one navigates). After the rename of the read method the collision is
gone, and `get(index)` could even align with `Vec::get`. This design still does
**not** use it, because it would bring back exactly the confusion that comment 1
exposed: `x.get(3)` returns a handle and `x.get()` returns a value, so one verb
again carries two different roles from the table of terms. `at` and `value` each
state one thing. **Navigation and materialization never share one verb.**

Three navigation newtypes carry the wire shapes that an index cannot express,
plus one leaf newtype (`UploadSlotState`, §3.4 — it does not navigate; it exists
to connect the state tree to the upload plane). All four are thin wrappers over
`State<_>`, and they carry the same four common methods — `value`, `subscribe`,
`revision` and `node` — through an inherent impl and not through `Deref`, because
the use of `Deref` as inheritance is an anti-pattern in Rust, and the surface
here is only four methods. The only difference in shape is in `StreamState`: its
`subscribe` callback takes one more argument, the edits of this transaction to
this collection, and `as_state()` is the path down for a consumer that does not
need that argument. The reasons for both, together with "why the method is named
`subscribe`", are below.

```rust
/// A stream slot: ordered **and** keyed. `value()` still yields `Vec<T>`, so the
/// snapshot type on a generated `State` struct is unchanged (§4.3).
pub struct StreamState<T> { ... }

impl<T> StreamState<T> {
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
    /// Item keys in list order.
    pub fn keys(&self) -> Vec<Arc<str>>;
    /// The item with this key. Identity survives repositioning (§3.1).
    pub fn by_key(&self, item_key: &str) -> Option<State<T>>;
    pub fn at(&self, index: usize) -> Option<State<T>>;
    /// `(item_key, item)` in list order — what a keyed list adapter renders.
    pub fn iter(&self) -> impl Iterator<Item = (Arc<str>, State<T>)>;

    pub fn value(&self) -> Vec<T> where T: DeserializeOwned;
    pub fn try_value(&self) -> Result<Vec<T>, ReadError> where T: DeserializeOwned;

    /// Subscribe, and be handed **this transaction's edits to this collection**
    /// along with the change.
    ///
    /// Called on exactly the occasions a plain node subscription is called; the
    /// slice is empty when the change was confined to items' own fields. The
    /// edits are not state — they are the difference between two states, which
    /// a callback cannot recompute by re-reading the tree — so they have to
    /// arrive with the notification (§6.3).
    #[must_use = "dropping the subscription unsubscribes"]
    pub fn subscribe(
        &self,
        on_change: impl Fn(Change, &[CollectionEdit]) + Send + Sync + 'static,
    ) -> Subscription;

    /// The same node seen as an ordinary `State<Vec<T>>`: a one-argument
    /// `subscribe`, index addressing, and whatever else generic code expects.
    /// It changes what a callback is *shown*, never when it is called.
    pub fn as_state(&self) -> State<Vec<T>>;

    pub fn revision(&self) -> u64;
    pub fn node(&self) -> NodeId;
}

/// A mounted child store. `store_id()` is what `Mounted::command_on` takes.
///
/// **Deviation.** Its signature is `-> Option<StoreId>`, not `-> StoreId`. The
/// id is read once when the handle is created and is never read again (it is an
/// identity, not a property). Because navigation cannot fail, the state "there
/// was never a store node under this handle" exists, and in that state there is
/// no honest `StoreId` to give — `StoreId::root()` is exactly the kind of value
/// that silently addresses the wrong store, which §3.4 removes. `None` has the
/// same shape and the same reason as the neighbouring `UploadSlotState::key()`.
pub struct StoreState<S> { ... }

impl<S> StoreState<S> {
    pub fn store_id(&self) -> Option<StoreId>;
    /// The child's own shape, for the generated `Ext` trait to navigate.
    pub fn fields(&self) -> State<S>;

    pub fn value(&self) -> StoreField<S> where S: DeserializeOwned;
    pub fn try_value(&self) -> Result<StoreField<S>, ReadError> where S: DeserializeOwned;
    pub fn subscribe(&self, ...) -> Subscription;
    pub fn revision(&self) -> u64;
    pub fn node(&self) -> NodeId;
}

/// An async node. The status is part of *this* node's semantics; the result is
/// a subtree that reconciles on its own (§3.3).
pub struct AsyncState<T> { ... }

impl<T> AsyncState<T> {
    pub fn status(&self) -> AsyncStatus;
    /// The `result` subtree. `None` when the wire `result` is `null`.
    pub fn result(&self) -> Option<State<T>>;
    pub fn reason(&self) -> State<Option<AsyncError>>;

    pub fn value(&self) -> AsyncResult<T> where T: DeserializeOwned;
    pub fn try_value(&self) -> Result<AsyncResult<T>, ReadError> where T: DeserializeOwned;
    pub fn subscribe(&self, ...) -> Subscription;
    pub fn revision(&self) -> u64;
    pub fn node(&self) -> NodeId;
}

impl<T> AsyncState<Vec<T>> {
    /// The `result` subtree of a `stream_async` field, as a keyed collection.
    /// `None` while the result is `null`.
    pub fn ok_stream(&self) -> Option<StreamState<T>>;
}
```

```rust
/// An upload slot — the tree's inert half of the upload plane (§3.4).
///
/// A leaf: there is nothing under it to navigate to, and it never notifies,
/// because the server re-renders the same marker every cycle. It is a distinct
/// type rather than a plain `State<UploadSlot>` for exactly one reason: it
/// knows **both** halves of the `(store_id, name)` upload key, which is what
/// turns "walk from the state tree to the live upload handle" into one step
/// with no bare strings (§3.4).
pub struct UploadSlotState { ... }

impl UploadSlotState {
    /// Both halves of the upload key, or `None` if the slot node is gone
    /// (its store was unmounted, or the tree was closed).
    ///
    /// The owner is the nearest enclosing store, resolved once at node creation
    /// (§2.1) — the half that used to be typed out as a literal
    /// `StoreId::root()` at every call site, correct only by accident for a
    /// slot declared inside a child store.
    pub fn key(&self) -> Option<(StoreId, Arc<str>)>;

    pub fn value(&self) -> UploadSlot;
    pub fn try_value(&self) -> Result<UploadSlot, ReadError>;

    /// Registers, and never fires (§3.4). Present so the handle family has no
    /// exceptions, not because anything is expected to call it.
    pub fn subscribe(&self, on_change: impl Fn(Change) + Send + Sync + 'static)
        -> Subscription;

    pub fn revision(&self) -> u64;
    pub fn node(&self) -> NodeId;
}
```

*Interpretation — the **value** type `UploadSlot` moves down together with its
node kind.* `UploadSlotState` must be able to name the return type of `value()`,
and `NodeKind::UploadSlot` is already in `musubi-state`. `UploadSlot` (the
`{ name }` snapshot struct) is therefore treated like `StoreId`: it moves into
`musubi-state`, and it is re-exported unchanged from `musubi_client::generated`
(§1.3). The prelude list of the generated bundle already names `UploadSlot`, so
**no consumer path changes**. The statement in §4.1, "an upload still renders as
`musubi::UploadSlot`", also holds word for word.

**Deviation — a collection edit is the only thing carried besides `Change`.**
The handoff §24 states that a callback receives only the revision, and that the
callback re-reads the value itself. A collection edit does not break this rule:
it carries the `item_key` and the index (the `NodeId` in `Inserted` is a handle,
not a copy of a value), and not the value of any node. It must arrive with the
notification, because a re-read cannot recover it. "Which items were inserted,
removed or moved" is the difference between two states, and the tree the callback
sees holds only the *present* state. Without it, an incremental list adapter must
go back to diffing the list itself, and `ChangeSet` exists exactly so that it
does not have to (§5.1 capability 2, §6.3). `Notify` already holds the
`ChangeSet` outside the lock and calls the callbacks one by one, so passing the
edit slice of that node as well adds no mechanism.

**Why the method is named `subscribe` and not `subscribe_edits`.** An earlier
draft of this design gave `StreamState` two subscription methods: `subscribe`
with one argument, and `subscribe_edits` with two arguments. They are merged here
into one, and the name is `subscribe`.

*The merge is possible because `StreamState<T>` is a separate view type, and not
an alias for `State<Vec<T>>`.* It does not `Deref` to `State<Vec<T>>` (the rule
above: all three newtypes forward through an inherent impl and do not use `Deref`
as inheritance), so `rows.subscribe(..)` has only one candidate — the one on
`StreamState` itself. The generic `State<T>::subscribe(impl Fn(Change))` is not
on the method resolution path of `StreamState` at all, so there is no competition
between overloads, and no doubt about which method was called. Two methods with
the same name and different signatures on two different types are completely
ordinary in Rust.

*The condition under which that problem is real.* If `StreamState` were designed
as `Deref<Target = State<Vec<T>>>`, the merge would meet method shadowing: an
inherent method takes priority over a method of the deref target, so the
previously valid `rows.subscribe(|change| ..)` would suddenly report the wrong
number of arguments, and the error message would point at `StreamState` while the
reader believes he calls `State`. **Under that design, keeping the second name
`subscribe_edits` is the right choice**, because it writes "this is a
subscription to the collection, not to the node" at the call site. This design
does not use `Deref` (see above in §2.4), so that condition does not hold. The
only reason left for two names is a possible future `Deref`, and this design
clearly adds none. To add it is to take inheritance semantics in exchange for
four forwarding methods, which is exactly what is already rejected here.

*The costs, stated exactly.* There are two, and both are small:

1. **A subscriber that does not need the edits must still write a closure with
   two arguments**: `rows.subscribe(|_change, _edits| ..)`. The cost is a few
   characters. In exchange, a collection subscription has only one entry point:
   you do not choose between two names, and the misuse "I called `subscribe`, so
   why does the list not move" cannot happen. That misuse is the easiest error in
   the two-name design: the subscriber silently receives less than the only
   useful half of the information.
2. **The four common methods no longer have exactly the same shape**: the arity
   of `StreamState::subscribe` differs from the other two newtypes. This affects
   nothing: these four methods were never a trait, no generic code is written
   over "anything that has `subscribe`", and an inherent method cannot express
   such a generic.

*A consumer that needs a callback with one argument uses `as_state()`.* It
returns the `State<Vec<T>>` view of the same node, so the one-argument
`subscribe` and index addressing both come back. It changes only **what the
callback sees**, and never **when it is called** — the same node, the same
transaction and the same notification, because the time of the notification comes
from the semantic value of the node and not from the view type used to subscribe
(§9.1: the semantic value of a collection contains the semantic value of every
item). Its real use is not to save one argument, but to pass a collection node to
a generic helper function whose signature is written over `State<T>`. The saved
argument is only a side effect.

The other three newtypes (`StoreState`, `AsyncState`, `UploadSlotState`) need no
`as_state()`: the arity of their `subscribe` did not change, so there is nothing
to go down to. If a generic helper function needs it later, it is one line of
forwarding as well. Add it when the first caller exists (AGENTS.md: without a
second caller, promise nothing).

`State<Option<T>>` needs no newtype:

```rust
impl<T> State<Option<T>> {
    /// The value view when the node is not `Null`.
    pub fn as_some(&self) -> Option<State<T>>;
    pub fn is_none(&self) -> bool;
}
```

### 2.5 `Subscription`

```rust
/// One RAII subscription. Dropping it unsubscribes.
///
/// **One token for the whole API** (§2.4): a node subscription, a
/// `StatusState` subscription and an `Upload` subscription are all this type,
/// so one `Vec<Subscription>` holds every observation a view has.
///
/// Holds a `Weak` to whatever it was registered on, so a subscription never
/// keeps that thing alive, and dropping one against an already-dropped target
/// is a no-op.
#[must_use = "dropping the subscription unsubscribes"]
pub struct Subscription(Target);

enum Target {
    /// A node of a retained tree. A `Weak` and two ids; no allocation.
    Node { tree: Weak<StateTreeInner>, node: NodeId, id: SubscriberId },
    /// A cell outside any tree — `musubi-client`'s status and upload planes.
    Cell { cell: Weak<dyn Unsubscribe>, id: SubscriberId },
}

/// What a non-tree subscription target must be able to do.
///
/// Implemented in `musubi-client` by the two cells (§5.4, §6.4);
/// `musubi-state` never names them. The lower crate states the contract, the
/// upper one satisfies it.
pub trait Unsubscribe: Send + Sync {
    fn unsubscribe(&self, id: SubscriberId);
}

/// One subscriber's identity within one target. Opaque.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubscriberId(/* private */);

impl Drop for Subscription { ... }
```

That is the whole type. There is no `unsubscribe()` (it is `drop` with a longer
name), no `forget()` and no bundle type. A consumer that holds more than one
subscription keeps its own `Vec<Subscription>`. The gpui example does this today
for its gpui `Task`.

**Why one type with two variants, and not two types.** Two types give no
unification. The consumer must then keep one field for a subscription in the
tree and one field for a subscription outside the tree, and the rule in §2.4
buys exactly "one `Vec` holds all of them". The type is also not a
`Box<dyn FnOnce()>`. That form costs one heap allocation per subscription, and a
subscription per node is the most frequent thing in this design (one per row,
§6.5.1). To add one allocation to thousands of subscriptions inside the tree for
the two planes outside the tree is the wrong direction. The present shape
allocates nothing: one tag, one `Weak` (a thin pointer for the tree variant, a
fat pointer for the cell variant) and two ids.

**The bound on the callback is `Fn(Change) + Send + Sync + 'static`.** The bound
needs `Sync` and not only `Send`, because the tree itself is `Sync` and holds the
callback in an `Arc`. An `Arc<F>` is `Send` only when `F: Send + Sync`.

**One stated risk: a callback can still run one time after its `Subscription` is
dropped.** `Notify` clones the owed callbacks under the tree lock and calls them
only after it releases the lock. A drop inside this window is too late to cancel
the call. To close this race, you must hold the lock across user code (any code
that drops its own subscription inside the callback then deadlocks), or add a
two-phase protocol; both cost more than they give. The contract is therefore:
**a callback runs at most one time per transaction, and it can run one more time
after its subscription is dropped; a callback must tolerate one stale call.**
Every real consumer can already do this — a call to gpui `Entity::update` on a
dropped entity returns `Err`, and the present loop already handles that branch —
and there is a precedent inside the crate: `RootSink::dispatch_event` also
removes a closed sender at send time, not at drop time. **This contract is
word-for-word the same for the two cells outside the tree**: they use the same
discipline of "clone the owed callbacks under their own lock, then call them
after the lock is released" (§5.4, §6.4), so the consumer writes the tolerant
code one time, not one time per plane — this is also where the unification of
§2.4 holds in semantics, and not only in naming.

**A removed node notifies one time, and after that it reads as dead.** A node
that leaves a collection (one `delete`, one `limit` trim) or leaves an object
(one `remove` op) is recorded as changed, its subscribers get a notification, and
then its slot is released. A `State<T>` that still points to it reads
`is_live() == false` and `try_value() == Err(ReadError::Gone)`, and `value()`
will panic.
The rule for the consumer follows: **a view that is bound to a subtree must check
`is_live()` in its own change callback and remove itself.** `StreamState::iter`
yields only live items, and the incremental list adapter also drops the matching
row view on the `CollectionEdit::Removed` edit that announces the removal. To hit
the panic, you must first ignore the notification that reports the dead node.

### 2.6 Ownership, `Send`/`Sync` and lock discipline

| Type | `Send` | `Sync` | Notes |
|---|---|---|---|
| `StateTree` | Yes | Yes | `Arc<StateTreeInner>`; `Clone` is one increment of the reference count |
| `State<T>` | Yes, for any `T` | Yes, for any `T` | `PhantomData<fn() -> T>` |
| `StreamState<T>` / `StoreState<S>` / `AsyncState<T>` | Yes | Yes | The same marker |
| `Subscription` | Yes | Yes | One `Weak` and two ids; the `Weak<dyn Unsubscribe>` of the cell variant is also `Send + Sync`, because `Unsubscribe: Send + Sync` (§2.5) |
| `Notify` | Yes | No | Holds `Arc<dyn Fn + Send + Sync>` callbacks |
| `Transaction<'_>` | **No** | No | Holds a `MutexGuard`; it lives on the task that drives it |
| `Node` / `ChangeSet` / `SemanticValue` / `Change` | Yes | Yes | owned values |

**One lock, not one lock per node.** The whole node arena sits behind a single
`std::sync::Mutex`. A write happens one time per accepted envelope, on the actor
task; a read happens one time per `value()`/`revision()`/`subscribe()` call. A
lock per node buys nothing, and it makes a transaction non-atomic by
construction. Lock poisoning is always ignored, as `musubi_client::lock` does.
Here the ignore is *safe*, and not only tolerable: the journal is a drop guard,
and a panic inside a transaction unwinds through the rollback, so the arena that
the poisoned lock protects is consistent.

**The lock is held across caller code in exactly one place.** The drift check
(`Transaction::to_hydrated` followed by the `deserialize` of the caller) runs
inside an open transaction, so the lock is held across one full deserialization
of the root — this happens only on a root replace, that is one time per mount
plus one time per rejoin, plus one time per transaction in `debug_assertions`
builds (§4.4). No other place in the API runs caller code under the lock:

- `subscribe()` registers the callback and returns; it never calls the callback.
- `apply()`/`commit()` collect the callbacks under the lock and call none of
  them.
- `Notify::drop` calls them after the lock is released, so a callback can freely
  call `value()`, `revision()`, `subscribe()` and even `apply()` without a
  deadlock. (A nested `apply()` call from a callback is legal; it makes a nested
  `Notify` that is dropped inside the outer notification. The client never does
  this, and the tree does not forbid it.)
- `State::iter` takes a snapshot of the child node ids under the lock, and yields
  the views only after that.
- `Node`, `ChangeSet` and `SemanticValue` are all owned copies, so no guard
  escapes.
- The two cells outside the tree (status in §5.4, upload in §6.4) keep the same
  discipline: clone the owed callbacks under their own lock, then call them after
  the lock is released. The statement "the API runs caller code under the lock in
  exactly one place" therefore stays true word-for-word after the unification of
  §2.4.

**The callbacks run on the actor task.** The actor drops the `Notify`, so a slow
callback holds up the inbox — this is the head-of-line blocking cost that
`docs/rust-client.md` §2.4 already defines. This is intentional: a notification
that moves to a spawned task breaks the relative order of state notifications and
event dispatch (it breaks step 9 of §4.3), and it brings back the unbounded queue
that the latest-value cell exists to avoid. The contract is the same one that
`dispatch_event` always keeps: **a callback only schedules work; it does not
compute.** The callback of the gpui adapter is one `Entity::update` plus one
`cx.notify()`, that is one enqueue.

The status callbacks also run on the actor task (the only writers are
`RootSink::set_status` and `RootSink::clear`, §5.4); the upload callbacks run in
**two places** — the `upload_ops` fold comes from the actor task, and the state
change of the control plane comes from the task that calls `select`/`start`
(§6.4). The contract is the same in all three places, and the consumer does not
need to separate the planes.

**The timers and the RAII patterns elsewhere do not change.** This design adds no
timer; the cache write throttle and the "fenced-not-cancelled" discipline of the
socket layer stay as they are.

---

## 3. Wire integration

### 3.1 Streams are keyed, not arrays

**`stream_ops` drive the keyed reconciliation directly, and they never pass
through a JSON pointer.** The index identity rule of handoff §19 applies only to
`NodeKind::Array` — the plain JSON arrays that appear inside a state value — and
never to a stream.

The wire forces this; it is not a choice. `docs/streams.md` states clearly: "JSON
Patch ops never carry stream item content. Stream item content flows through
`stream_ops` only". Each op carries `store_id`, `stream` and `item_key`, but the
slot in the tree holds only `{"__musubi_stream__": "<name>"}`. No pointer can
address a stream item, so the index identity is not available in principle.

An op resolves to a node as `(store_id, stream)` → `Collection` node, through the
store map of the tree plus the slot table of that store node. It happens
**inside** the transaction, after the `ops` land, because the `replace ""` of the
first envelope is what creates that slot, and the insert of the same envelope
fills it.

The semantics per op stay byte-for-byte equal to
`packages/client/src/streams.ts` and to the present `streams.rs` — the two
clients must materialize the same list, or the same page renders differently on
each side:

| Op | Effect on the list | Effect on the node |
|---|---|---|
| `reset` | The list becomes empty | Each item node enters the carry-over table of this transaction (see below) |
| `delete` | Drops each item that matches the `item_key` | That item node enters the carry-over table |
| `insert` | **Upsert first, then locate, then trim**, strictly in this order | See below |

The details of `insert` are the same as in `docs/rust-client.md` §5: if an item
with the same `item_key` exists, remove it **first**; resolve the index against
the length **after** the removal (`at == -1` ⇒ append; `at <= 0` ⇒ prepend;
`at > 0` ⇒ `min(at, len)`); insert; then trim by `limit` (`size = limit.abs()`;
`0` ⇒ clear; `len <= size` ⇒ no trim; otherwise drop the overflow from the
**tail** when `at == 0`, and from the **head** in every other case — the
direction comes from `at`, never from the sign of `limit`).

**An insert that upserts keeps the `NodeId`.** The removal and the re-insertion
apply to the *list*, not to the *node*. An insert on an existing `item_key`
reuses the node of that item and reconciles the new item value into it, so
`{id: "a", body: "hi"}` → `{id: "a", body: "edited"}` moves only the `body` child
node, and `id` stays as it is. The `State<MessageState>` that a view holds for
row `a` stays valid, keeps its subscribers, and gets a notification for `body`
only and for nothing else. This is strictly better than the TypeScript client,
which rebuilds the whole object. There is one exception: the store id of the row
value changed — an upsert also obeys the identity rule of §3.2, and a row that is
no longer the same store does not keep the old node.

**Interpretation — the carry-over table per transaction.** A node that is removed
from a collection during a transaction goes into a table keyed by `item_key`,
until the transaction settles; an insert for a carried-over key **reuses that
node**. Every node that is still in the table at settle time is released. Without
this rule, the most common refresh on the wire —
`stream(socket, name, fresh_items, reset: true)`, which flushes as
`[reset] ++ inserts` in one envelope
(`docs/rust-client.md` §5) — destroys and rebuilds every row node, notifies the
subscribers of every row, and takes away all the value of the keyed identity. The
handoff does not cover this, because it does not cover a keyed collection at all;
this rule is the minimum rule that makes `reset: true` behave as the keyed diff
that it already is.

The internal order of one flush for a single store is always
`[reset?] ++ inserts ++ deletes`, so for the same key a delete never comes before
an insert. The carry-over table therefore covers an insert after a `reset` and an
insert back after a `limit` trim; it does not revive an intentional delete.

**A row that is adopted away also records a `Removed` edit.** The `ops` come
before the `stream_ops` (§3.6), so "a render moves a store out of a stream row,
and the same envelope deletes that row" is a real shape: `detach` takes the row
out of the item table, the delete that follows does not find the key, neither
side records an edit, and the list adapter replays only `collection_edits`
(§6.3) — so the row stays on the screen forever. For this reason, a `detach` that
takes an item out of a `Collection` records `CollectionEdit::Removed` at the
index of that moment, with the same byte-for-byte shape as a delete. The node
does **not** enter the carry-over table: the carry-over table exists for "this
transaction took the node out of the *list*", but this node left on purpose and
is now mounted elsewhere in the tree, and to let an insert claim it again would
give it two parent nodes.

**An insert that adopts a row of this same collection locates against the list
after the adoption.** The flush order is `[reset?] ++ inserts ++ deletes`, so
when "the value of row `b` embeds the store that was row `a`" arrives, this
insert itself adopts `a` while it builds the item — `detach` already took `a` out
of the item table and recorded a `Removed` edit. In "upsert first, then locate",
the **locate step therefore happens after the item is built**: at that moment,
read the item table again, and resolve the "length after the removal" for `at`,
the `from` of `Moved` and the index of `Inserted` against this table; they then
align item by item with the list that the adapter holds at this step of the
replay (§6.3). To write back a snapshot from **before** the reconciliation puts
`a` back: one node is then mounted under two parent nodes, the delete that
follows carries it over and releases it at settle time, and row `b` still points
to it.

**Order: a pure reorder notifies the collection, not the items.**

*Decision.* The order **is** part of the semantic value of the collection.
`SemanticValue::Keyed` is an ordered vector of `(item_key, item_semantic)` pairs,
so a move of one row changes the value of the collection, even when no item value
changes. The collection node and its ancestors get a notification; the item nodes
do not.

*Reason, in two halves.* The notification of the collection is necessary: a
`stream_insert` with `at: 0` moves an existing row to the top, which is a visible
UI change with no item change at all; if the collection stays silent, nothing
repaints — the list of the chat example has exactly this shape (`at: 0`,
`limit: -100`, index 0 is the newest item). The absence of a notification on the
items is necessary too: `ItemView(State<Item>)` renders the fields of the item
itself, and its position is the business of the parent node — an item that
repaints on each arrival of a neighbour makes the subscription per node worthless
at the place where it gives the most value. This is the same line that the
handoff draws for a plain array (index identity: a moved element is a changed
element *of that array*), only transposed onto the key identity.

The `ChangeSet` carries these edits, so an adapter never has to derive a list
diff from the changes: a reorder gives
`CollectionEdit::Moved { item_key, from, to }`, the other cases give
`Inserted`/`Removed`, and a reset gives `Reset`. This is the second of the two
capabilities that support `musubi-gpui` (§5.1).

**A stream slot is adopted by `(owner, name)`, in the same shape as a store that
is adopted by id.** When a store is rendered a second time inside the same
envelope (the duplicate rule of §3.2 gives the second sighting a new node), and
the stream marker in the new node creates an empty `Collection`, that empty
collection displaces the live node in the `(store_id, stream)` index that still
holds the items — the original node then unmounts, the items are released with
it, the store never really unmounted, and the clear of BDR-0011 does not apply.
For this reason, the reconciliation of a stream marker first looks up the live
collection node by `(owner, name)`, adopts it on a hit (and rejects two parents
and a cycle in the same way), and creates a new node only on a miss. The
re-render of a plain marker that arrives every cycle still takes the "unchanged"
fast path and pays no index lookup.

**One op places a collection one time only.** The tree records each collection
node that an op puts somewhere. An op puts a collection somewhere in three ways:
it keeps the node in the slot that already holds it, it adopts the node, or it
builds a new node. A second marker for the same stream in the same op does not
adopt a recorded node. The first sighting keeps the collection node and its
items. The later sighting gets a new, empty collection node, and the
`(owner, name)` index then points to that new node. This is the rule that §3.2
gives to a duplicate store id. The two rules are now one rule.

Without this rule, the last sighting wins. A first sighting that moves the
collection one level down puts the node out of reach of the "already a child of
this parent" test, and a later key adopts the node back. A first sighting that
keeps the collection in place is worse: the parent holds that node in its new
field map, a later nested key adopts the node away, and the write-back of the
map makes the collection reachable from two parents.

The record holds node ids, not index keys. A marker that finds its collection
already in the slot is the re-render that arrives every cycle. A node id is a
copy type, so the record costs one hash insert for each marker and no
allocation. An index key costs one store id clone and one string for each
marker.

The record is scoped to one op. The tree clears it at the start of each patch op
and at the start of each stream op, together with the record of store ids of
§3.2. Two ops in one envelope can move the same collection two times. That is
legal.

**An op that finds no slot is discarded.** The resolution uses the
`(store_id, stream) -> NodeId` index of the tree; if the key is absent from the
index, or points to a node that is no longer present, the op does nothing. **No
log entry is written**: `musubi-state` has no `tracing` dependency (§1.3), and to
bring back a dependency for one log line would make an exception to the first
promise of the crate, which is that it has no runtime (the same reason as the
Deviation in §3.2). This is not observable: Musubi rejects a render that has no
stream placeholder (`docs/streams.md`), so every declared stream has a marker in
every render; the one window that remains — a store unmounts in the same cycle as
an insert — releases its subtree together with its `Collection` child nodes at
the end of the same envelope (§3.2). The two clients stay consistent.

### 3.2 Child stores

**A `__musubi_store_id__` node is keyed by its `store_id`.** A store node
reconciles by identity, not by position. A child store that *moved* — from
`/panel` to `/rows/0`, or from one parent node to another — keeps its `NodeId`,
its subtree, its stream collection and its subscribers.

The mechanism: the tree keeps one `HashMap<StoreId, NodeId>` and updates it when
it creates or releases a `Store` node. To reconcile an incoming value that
carries `__musubi_store_id__: X`, the tree looks up `X` first. On a hit it
attaches the existing node under the new parent node and reconciles into it; on
a miss it creates a new node. The server writes each store id, and the id is
unique inside one root, so the lookup is never ambiguous. A duplicate is a
server bug; the tree treats the second occurrence as a new node.

**The identity is the node, and the reverse is also true.** Adoption occurs only
when the same id comes back; the tree never rewrites a store node in a slot into
a *different* store or into a plain value. If the incoming value carries a
different id, or is no longer a store, the tree unmounts the old node completely
(its handles then read as dead, `is_live() == false`) and the slot gets a new
node. Without this rule, `replace /panel {store b}` reuses the `NodeId` of store
a: a live `StoreState` caches the id of a, but the node renders the fields of b.
`command_on` then sends the command to the wrong store and gives no error, which
is the class of result that §3.4 removes. This rule holds for every write path:
the path level `add`/`replace`, an array shift, the upsert of `stream_insert`
(the exception in §3.1) and the marker op below all pass through the same
per-parent reconcile entry point.

**Adoption has a third case that the tree treats as a new node: the ancestor
case.** When a store is rendered into its own subtree (`add /a/inner/self
{store X}`, where X is `/a`), adoption attaches the node under itself and makes
a cycle in the parent chain. `mark_dirty` walks the parent chain to the root, so
a cycle means an endless spin that holds the lock. Therefore the tree walks the
ancestor chain of the new parent node once before adoption (O(depth)). A hit
gets the same structural treatment as a duplicate id: the new key gets a new
node, and the existing node does not move. Related guards: `mark_dirty`,
`owner_of`, `depth_of` and `subtree_post_order` all carry a step limit, so any
future break of an invariant degrades to an error instead of a wedged process.
The tree depth is capped at the write boundary (`TreeError::Depth`, limit 256),
which makes all recursive read paths and the destructor bounded.

**The depth cap also applies to adoption.** Creation measures node by node,
which is sufficient only for a subtree that grows out of the wire. Adoption
moves a subtree that already stands, as one unit, and every descendant that
matches the incoming value returns from the "unchanged" fast path of the
reconcile and never reaches creation. The depth of the destination and the
height of the subtree **combine**: 100 + 200 crosses a cap that neither side
crosses alone. The adoption point therefore measures this combination:
`depth(new parent node) + height(subtree) ≤ 256`. If the check fails, the tree
rejects the whole envelope with `TreeError::Depth` (creation instead of adoption
is not an option: a `Collection` child node of a store projects as a bare
marker, and a rebuild loses all the stream items with no error, §3.1). The
invariant therefore reads: **no live node is more than 256 levels from its root,
and this also holds in the middle of a transaction** — the height changes inside
one envelope with each earlier creation and adoption. With this invariant, most
moves get the answer for free: if the node lands in a slot that is not deeper
than now, no node in the subtree becomes deeper than now, and a reorder, a
prepend and the return of a carry-over row to its original collection are all of
this kind. Only a real move to a deeper position pays one O(subtree) probe, and
the probe stops at the first level that is over the budget. The return of a
carry-over row to the collection through `stream_insert` is a third gate, and it
measures the same value.

**A detach leaves an addressable placeholder.** For a store that moves between
two existing keys, `Musubi.Diff` emits the destination first and the clearing of
the source second: `[replace /b {"w": {store p}}, replace /a nil]`. If adoption
also deleted the key when it detaches the node from `/a`, the next op in the
same envelope would fail to resolve and the whole envelope would roll back, but
a legal server frame must not be rejected. Therefore, when the tree detaches a
node from an object field, the source key points to a new plain Null node; the
server always clears the source later in the envelope, so the final state
agrees, and if it does not, the drift check catches it. A shift inside the same
parent node is an *exchange*: the displaced node takes the slot that the
adoption frees. This is the mechanism that keeps both nodes through one reorder.
The tree releases every placeholder node that no one claims at the end of the
envelope.

**A pointer op that points inside a marker is legal, and the tree treats it as a
change of identity.** The server diff treats the rendered JSON document as a
plain document, so a reorder of a plain list of child stores emits
`replace /rows/0/__musubi_store_id__/0 "b"`, and a prepend emits
`remove /rows/0/__musubi_store_id__`. The TypeScript reference client applies
these ops directly to the flat document, so these shapes are legal under the
contract. In the tree this key is absorbed into `NodeKind::Store`, so the walk
has a special case: when an op addresses the `__musubi_store_id__` of a store
node, a change to the id vector is a change of identity, and it uses exactly the
same index, claim and adoption mechanism as above — to replace an element is to
change the id; a `remove` of the whole key makes the node a plain value; an
`add` of the whole key mounts a child store.

**When a marker is released, the tree does one exchange of identity.** For a
plain row that is prepended before a store row, `Musubi.Diff` emits
`[add /rows/1 {store a}, remove /rows/0/n,
remove /rows/0/__musubi_store_id__, add /rows/0/kind "banner"]`: the copy
**lands first**, and the marker is removed from the original node after that.
At the moment the copy lands, the render truly carries the same id two times,
and the duplicate rule is correct to give the second sighting a new node. The op
that decides the result is that removal of the marker — it says that the node
that was store a is now a plain object, and at this moment that id sits on a
node that this transaction created from nothing. Store a is rendered in both
frames and is never unmounted, so §3.2 owes it its `NodeId`, its subtree, its
stream collection and its subscribers. Under a literal reading of "unmount when
the render no longer puts the store here", all of these die with the original
node while the copy inherits the id: the final JSON and the store index are both
correct, but every handle reads as dead. Therefore the two nodes **exchange
slots**: the original node moves into the slot of the copy and reconciles to the
value of the copy (on the way it adopts back, as a bare marker, everything that
the copy adopted from it when the copy was built — first of all the stream
collection), and the copy takes the slot of the original node and is rebuilt as
the plain value that this op writes; the copy exists only since the start of
this transaction, so no one holds it. This is the same operation as the exchange
in a reorder inside one parent node, but it arrives from the other end. The
exchange rejects the same two conditions as adoption (the node is not in the
slot that it claims; the move closes a cycle in the parent chain), and it
rejects one more: the move takes the subtree past the depth cap. A rejection
only makes one store lose its `NodeId`; it does not make the tree lose its
shape.

**A patch op positions its write against the children that the reconcile left,
not against the children that it read.** To build an incoming value can adopt a
store out of a sibling slot of the same parent node. `detach` then takes that
node out of the live children of the parent and puts an addressable null in its
place. A write that rebuilt the parent from a snapshot taken before the
reconcile put the adopted node back, and one node then stood in two slots. This
rule holds for a positional write, for a whole-object rewrite and for a
whole-list rewrite alike. The test that asks whether a rebuilt child list has
changed asks it against the live children for the same reason: a list that
matches the snapshot can still fail to match what the adoption left standing.

The exchange that a reorder gets applies only while the parent still holds the
node that the write displaces. One value can adopt two times, and the node that
the exchange would move into the vacated key can already be a child of the value
that the op writes.

**Deviation (the record method, not the behaviour).** The original text said
that the tree logs with `warn!`. `musubi-state` has no `tracing` dependency
(§1.3), and to bring back a dependency for one log line buys an exception to the
first promise of the crate, which is "no runtime". Therefore the tree handles
the second occurrence **structurally**: it does not adopt a store id a second
time inside one op, once that id is placed. The second key then gets a new node,
instead of one node that hangs under two parent nodes. The second result breaks
the reason why `detach` exists ("no node is reachable from two parent nodes"),
and it lets a later `remove` release a node that the other key still points to.
On the server side, `spec/domains/runtime/features/render-contract.feature`
raises directly on such a render, so against a correct server this path is
unreachable; if it is reached, the tree still does not damage itself.

If the incoming tree no longer has the id of a store node, the tree releases
that node together with its whole subtree. This is the structural form of the
fresh mount semantics of BDR-0011: the `Collection` child nodes of that store go
with it, so a store that appears again starts empty, and no trim pass is
necessary.

`StoreState<S>::store_id()` is how `Mounted::command_on` gets that id, and it
has exactly the same function as `snapshot.checkout_panel.store_id` today. The
handle reads the id one time at creation and keeps it: a handle that outlives
the unmount of its own store therefore still reports **its own** id, and
`command_on` fails against a store that the server no longer has — a visible
result. If the handle read the node again, it would change with no error to the
id of the root store. The return type is `Option<StoreId>`; the Deviation in
§2.4 gives the reason.

### 3.3 Async nodes

**`__musubi_async__` becomes node semantics, not a rewrite during hydration.**
Today `AsyncResult<T>` is an internally tagged enum. It deserializes the wire
shape directly, and the hydration pass does not touch this node. Under the tree
it is `NodeKind::Async { status, result, reason }`:

- **The status is part of the semantic value of the async node itself**, so a
  `loading -> ok` change notifies that async node even if the result does not
  change; and a `ok -> loading` change that keeps the previous payload (the
  shape from `Musubi.AsyncResult.loading/1`, which still shows the old value
  while it loads) notifies the async node only, and does **not** notify the
  result subtree.
- **The result subtree reconciles under it in the normal way.** It can be
  anything that the wire permits: a scalar, an object, a store node, a plain
  array, or a stream slot — the last shape is how `stream_async` renders as
  `AsyncResult<Vec<Item>>`. It is a plain child node with a plain identity, so a
  row in `async_stream(:messages)` keeps its `NodeId` between refreshes, exactly
  like a row of a plain stream.
- `reason` is also a child node, so a failure that changes only the reason
  notifies the async node and does not touch the result.

The concrete gain is in the shape that `docs/rust-gpui-example.md` §4.3 already
renders: on a reconnect the async value goes back to `loading` and still carries
the previous rows. The header view that subscribes to `AsyncState` redraws (it
now dims the list); the row view that subscribes to a single item does not
redraw at all. Today both redraw, because the whole root has only one
notification.

**Deviation.** This section first said that `AsyncResult<T>`, `AsyncError` and
`AsyncErrorKind` do **not** move down. In the implementation they moved down,
together with `StoreField<S>`, and §1.3 records the reason: §2.4 signs
`AsyncState::<T>::value() -> AsyncResult<T>`, and `AsyncState` lives in
`musubi-state` — to keep the three types in the crate above makes a cycle.
`musubi_client::generated` re-exports the three types unchanged, the prelude
list of the specification (`docs/rust-codegen.md` §4.5) is unchanged word for
word, and no consumer path changes. The tree still uses only `AsyncStatus` to
decide equivalence; what moved is three pure value types, not semantics.

**A write into an async slot lands in the slot that the pointer names.** The two
slots are not a child list, so there is no position to write into. The node that
a write displaces is not a reliable name for its slot either: to build the
incoming value can adopt that node into the other slot, or out of the async node
altogether. A write that matched on the displaced node then wrote into the wrong
slot, or into no slot at all.

### 3.4 Upload slots

**An inert leaf. Nothing changes in the semantics of the upload plane.**
`{"__musubi_upload__": "<name>"}` becomes `NodeKind::UploadSlot { name, owner }`,
and its semantic value is that name plus its owner
(§2.1). The server renders the same marker in every cycle, and the owner
resolves one time at node creation and is then fixed, so an upload slot node
never changes and never notifies.

The live upload state does not move: `upload_ops` fold into the `Uploads`
registry of the root, keyed by `(store_id, name)`. That plane — the data plane
and the control plane, the preflight, the chunked binary transfer, the external
`Uploader` — is orthogonal to the tree; the three actions on the handle use the
common naming convention of §2.4 (`value()`/`subscribe()`, and `into_stream()`
for the stream form); see §6.4.

#### From the state tree to the upload handle: one step, no bare strings

**Problem.** `NodeKind::UploadSlot` is inert, but that does not mean that the
path to reach it can be poor. If the slot is a plain leaf
(`State<UploadSlot>`), the consumer must walk these two steps:

```rust
// Before: read a name, then use it to look up the registry by string.
let slot = state.attachment().value();                       // one materialization, only to get a name
let upload = chat.upload(&StoreId::root(), &slot.name);      // the other half of the key is hand-written
```

There are three problems, and each is more severe than the one before:

1. **The code pays one materialization to get a name.** The semantics of
   `value()` are "give me a snapshot of this attribute outside the reactive
   system", but what the caller wants here is "give me the handle of that
   upload" — the value in the middle is only an intermediate step.
2. **It is a two-step jump through a string.** `&slot.name` is a bare string
   that the code takes out of a value and gives to another plane; a typo, a
   wrong spelling, or the name of a wrong slot: the compiler catches none of
   them.
3. **`StoreId::root()` is hand-written, and it is often wrong.** The upload key
   has two halves, the slot node knows both halves, but the consumer takes only
   one half from it and guesses the other. For a slot that is declared in a
   **child store**, `StoreId::root()` is simply wrong — it finds an empty
   registry entry and then uploads nothing, with no error. This is not a
   question of style; it is a class of bug that the shape of the API creates.

**Decision: the generated accessor for an upload slot field returns
`UploadSlotState` (§2.4), and `Mounted::upload_at(&slot)` provides the bridge.**

```rust
impl<St: Store> Mounted<St> {
    /// The live upload handle for a slot in this mount's tree.
    ///
    /// `None` exactly when the slot node is gone — its store was unmounted, or
    /// the root was torn down. Both halves of the `(store_id, name)` key come
    /// from the node (§2.4 `UploadSlotState::key`), so there is nothing for the
    /// caller to spell.
    pub fn upload_at(&self, slot: &UploadSlotState) -> Option<Upload>;

    /// The same handle by raw key. Kept as the primitive — a handful of
    /// hand-written embedders address a slot they never navigated to — but no
    /// longer the way a consumer walks from the tree.
    pub fn upload(&self, store_id: &StoreId, name: &str) -> Upload;
}
```

```rust
// After: one step, and both halves of the key come from the node.
let upload = chat.upload_at(&state.attachment());            // Option<Upload>
```

**Why `Mounted::upload_at(&slot)` and not `slot.upload(&mounted)`.** Both forms
read well; the dependency direction decides, and it has only one answer.
`UploadSlotState` is a leaf handle inside the tree and lives in `musubi-state`;
`Mounted<St>`, `Upload` and the `Store` trait all live in `musubi-client`, and
the dependency direction is `musubi-client -> musubi-state` (§1.3). With
`slot.upload(&mounted)`, `musubi-state` must name `Mounted<St: Store>` — which
needs either a reversal of this edge (not possible), or a move of the whole
`UploadSlotState` up into `musubi-client` (after which it cannot enter the
prelude of the generated bundle, because that list holds only the vocabulary of
the tree, §4.1; `StatusState` is absent from the list at the same boundary).
`upload_at` lets the upper layer fetch downwards: **the lower layer gives out a
pure tree handle, and the upper layer recognizes it and translates it into an
object of its own plane.** This shape already holds in the crate —
`command_on(&panel.store_id(), ..)` is the same path, but there the caller
passes an id and here the caller passes a handle.

**`upload()` stays and is not deprecated.** It is still the primitive of the
registry, for a hand-written embedder that does not use tree navigation (the
survival table in §7). What changes is **the correct form when you start from
the state tree**: that path has no bare string, and no hand-written `StoreId`.

#### One pure gain of the inert slot

Return to the slot itself. One result is worth a statement, because it is a pure
gain: the change notification rule in `docs/rust-client.md` §5 has the clause
"or its `store_id` appears in `upload_ops`". That clause is **deleted**. A cycle
with only uploads
changes no state node, so it wakes nobody on the state plane; today such a cycle
wakes every root subscriber. Notification per upload is strictly finer, and it
is already implemented.

### 3.5 Hydration disappears as a phase

`hydrate.rs`, `index.rs` and `streams.rs` are deleted from `musubi-client`.
Every responsibility that they carry has a destination:

| Responsibility of the three modules | Destination |
|---|---|
| Replace `{"__musubi_stream__": name}` with the materialized array before serde | `NodeKind::Collection` **is** that materialized list. `to_hydrated` projects it as a JSON array; this pass does not exist. |
| Track the nearest enclosing `__musubi_store_id__`, to resolve a marker | The tree resolves it **one time** at the creation of the `Collection` node and keeps it in `NodeKind::Collection::owner`. A marker is never resolved again. |
| `build_store_index` — rebuild `StoreId -> pointer` for every envelope | `StateTree` keeps `HashMap<StoreId, NodeId>` incremental: one insert for each store node that it creates, one remove for each store node that it releases. O(store changes) instead of O(tree). |
| The RFC 6901 pointer string of a store node | Gone. Nothing addresses a store by a pointer any more. |
| `prune_to_index` — discard the streams of a store that is gone | Solved structurally: a `Collection` is released together with the subtree of its store. **The upload trim still exists**, but it now runs against `StateTree::store_ids()` instead of the index. |
| `StreamStore::stage` / `commit` — the two-phase fold | Gone. The transaction journal is the staging area, and it covers the tree and the streams with one mechanism instead of two. |
| `StreamsView` — a staged fold on top of the committed streams | Gone together with the two-phase fold. |
| The shadow `serde_json::Value` document | Gone. The tree is authoritative; `to_wire(root)` projects the wire document again for the mount cache. |
| The marker shape rules (a single key `__musubi_stream__` with a string value; `__musubi_store_id__` only with an array of strings) | Unchanged. They move into the classifier of `musubi-state`, and the unit tests stay the same. A state field must not have a name of the form `__musubi_*` — `Musubi.DSL.Field.validate_reserved!/1` raises when `state do` expands — so a value that looks like a marker can only be data. |

Two projections replace the single hydration pass, and both run on demand
instead of one time per envelope:

- **hydrated** (`State::value`, `Transaction::to_hydrated`) — a collection
  projects as a JSON array, a store node carries `__musubi_store_id__`, an
  upload slot projects as its marker, and an async node projects as
  `{"__musubi_async__": true, status, result, reason}`. This is the shape that
  the generated types deserialize, and it is also why the `expected_state`
  comparison in the replay of the wire fixtures works unchanged.
- **wire** (`StateTree::to_wire`) — the same as above, but a collection projects
  back to `{"__musubi_stream__": name}`. This is the shape that
  `CacheEntry::data` holds, and it is also why the mount cache needs no change
  at all (§7).

The keys of an object project in sorted order, because `NodeKind::Object` is a
`BTreeMap` (handoff §18: the order of the keys must not affect equivalence).
This is not visible: without the `preserve_order` feature `serde_json::Map` is
itself a `BTreeMap`, and in every case the `PartialEq` of `Value` compares a map
as a map.

### 3.6 The order of envelope processing

**`apply()` is the transaction, and one envelope is one transaction.** The three
phases of `docs/rust-client.md` §4.3 collapse into two, because the journal
replaces the working copy that made the middle phase possible.

`ops`, `stream_ops` and `upload_ops` combine into **one** `ChangeSet` as the
table below shows, and this combination is not symmetric — `upload_ops`
contribute nothing to it, because an upload slot is inert (§3.4).

| # | Step | Can it fail? | Note |
|---|---|---|---|
| 1 | Validate the envelope (§4.4) and the version (§4.5) | Yes | Unchanged |
| 2 | `let mut txn = tree.begin()` | No | Takes the lock |
| 3 | `txn.apply(&envelope.ops, &envelope.stream_ops)` | Yes | `ops` land first — this is the only order that makes the slot of a stream op exist |
| 4 | If the envelope carries a root `replace ""` (or under `debug_assertions`): `sink.validate(txn.to_hydrated(root))` | Yes | The only whole-root deserialization that remains (§4.4) |
| 5 | `let notify = txn.commit()` | No | Settle, compare, collect, **and release the lock** |
| 6 | `uploads.apply_ops(&envelope.upload_ops)` | No | Unchanged; this is the first thing outside the tree that learns that the envelope is accepted |
| 7 | `uploads.prune(tree.store_ids())` | No | BDR-0011; the streams are trimmed structurally |
| 8 | `version = envelope.version` | No | Unchanged |
| 9 | `drop(notify)` | No | **The state subscribers run here** |
| 10 | `sink.set_status(MountStatus::Live)` | No | Unchanged |
| 11 | Dispatch `envelope.events` | No | Unchanged — after the state, as step 9 of §4.3 requires |
| 12 | Resolve the pending mount, flush the queued dispatches | No | Unchanged |
| 13 | `cache.on_publish(key, \|\| tree.to_wire(root))` | No | The shape is unchanged (§7). The projection is **lazy**: a whole-root `to_wire` is a full materialization of the tree, and a connection with no configured cache (the default) must never pay it once, so the code passes in a thunk and gets out an owned `Value` — the coordinator no longer needs to `clone` a tree that it should own |

A failure in step 1, 3 or 4 drops the `Transaction` and rolls the tree back
exactly to its earlier state, and step 5 and all later steps do not run: the
version does not advance, no upload subscriber hears about this envelope, no
state subscriber is notified, the last good tree continues to render, and the
recovery procedure of `docs/rust-client.md` §9 restarts that root. This is
exactly the atomicity that §4.3 states today, but the journal
achieves it in O(diff) instead of an O(tree) clone.

The relative order of steps 9, 10 and 11 keeps the present contract exactly: the
state becomes current before the status reports `Live`, and both come before the
dispatch of the events.

`PatchEngine::prepare`/`commit` disappear. They existed to let the caller do one
deserialization *between* the change of the copy and the acceptance of the copy;
the journal makes `apply` itself atomic, and the work that once ran between the
two steps now runs inside the transaction as step 4.

---

## 4. Code generation: the two surfaces

The generator still uses `docs/rust-codegen.md` as its specification. This section
adds one column to that document's §3.2 table, and one item type to the output of
its §4.6.

### 4.1 What stays unchanged

The plain snapshot structs stay exactly as they are, and they are exactly what
`value()` returns. `State`, `Params`, the command payload and reply structs, the
event payload structs, the promoted structs and enums, the promotion and naming
rules (§3.3–§3.6), the module tree (§4.2), the cross-module `super::` chain
(§4.3), the derives (§4.4), the store registry trait (§4.6) and the wire contract
(§4.7) all stay unchanged. `stream(T)` still renders as `Vec<T>`,
`Module.state()` still renders as `musubi::StoreField<S>`, and one upload still
renders as `musubi::UploadSlot`.

The prelude re-export list (`docs/rust-codegen.md` §4.5) gets seven more names
(`AsyncState`, `State`, `StateTree`, `StoreState`, `StreamState`, `Subscription`,
`UploadSlotState`). This is a normative change to that list. `Store` and
`UploadSlot` in the current list stay unchanged. The generated bundle still needs
the first one for `impl musubi::Store for XStore`. The second one is the snapshot
type of the upload slot; only the crate that defines it changes (§2.4, §1.3). The
path is unchanged word for word:

```rust
pub use ::musubi_client::generated::{
    AsyncError, AsyncResult, AsyncState, Command, Event, NoReply, State, StateTree,
    Store, StoreField, StoreId, StoreState, StreamState, Subscription, UploadSlot,
    UploadSlotState,
};
```

`State`, `StateTree`, `StreamState`, `StoreState`, `AsyncState`,
`UploadSlotState` and `Subscription` are re-exports of `musubi-state` types.
`musubi_client::generated` then re-exports them a *second* time, so that the
bundle always names only one crate (`:rust_codegen_runtime_path`).
`StateTree` enters the prelude only to make the type chain of `State::tree()`
nameable: it promises read-only methods only, and the write half is not on the
public surface (§5.5).

**`StatusState` does not enter this list.** It is a type of `musubi-client`
itself (§5.4), not of `musubi-state`. Therefore it is exported from the
`musubi_client` root, like `Mounted`, `Upload`, `UploadHandle` and `MountStatus`.
The prelude of the generated bundle re-exports only the vocabulary of the tree.
This does not affect the unification in §2.4: the unification applies to the
**shape**, not to the module they live in. That boundary is also already
established (`Upload` is not in the prelude today either).

### 4.2 The navigation surface and the orphan rule

The handoff writes `impl State<AppState> { pub fn count(&self) -> State<i64>; }`.
**This does not compile in the generated bundle.** `State<T>` is defined in
`musubi-state`. You can write an inherent impl only in the crate that defines the
type, and the orphan rule has no escape for inherent impls.

*Decision: generate an extension trait.* For each generated shape struct `X`, the
bundle produces `pub trait XExt` and implements it for `State<X>`:

```rust
pub trait CartStateExt {
    fn title(&self) -> State<String>;
    fn lines(&self) -> State<Vec<CartStateLines>>;
    fn messages(&self) -> StreamState<super::MessageState>;
    fn feed(&self) -> AsyncState<Vec<super::MessageState>>;
    fn checkout_panel(&self) -> StoreState<super::stores::panel_store::State>;
    fn avatar(&self) -> UploadSlotState;
}

impl CartStateExt for State<CartState> { ... }
```

The rejected alternatives:

- **One view newtype per shape** (`pub struct CartStateView(State<CartState>)`)
  — this can use inherent methods and needs no trait import, but each boundary
  needs one conversion, and each shape gets a second type that competes in the
  name table of §3.5. It exchanges one import for one type plus one conversion.
- **One general `Navigable` trait with an associated type `View`** — this reduces
  to the newtype alternative above, plus one more trait.

Two details make the trait alternative acceptable:

- **A bundle-level `nav` module.** The generator produces
  `pub mod nav { pub use ...::{CartStateExt, MessageStateExt, ...}; }` as the
  last top-level item, sorted by name. Therefore the consumer writes
  `use generated::nav::*;` once per file, instead of one import per shape. This
  is the pattern of `itertools::Itertools` / `futures::StreamExt`, and Rust
  consumers already expect it.
- **The shape of a store has two impls.** `XExt` is implemented for `State<X>`
  *and* for `StoreState<X>`. Therefore `snap.checkout_panel().total()` reads
  directly, and `snap.checkout_panel().store_id()` is next to it. The second impl
  forwards through `StoreState::fields`, and it calls the trait method **by
  name** (`XExt::total(&self.fields())`) instead of `self.fields().total()`. A
  declared field can be named `child`, `value`, `at` or `node` — these are
  inherent method names of `State<T>` — and an inherent method always wins in
  method resolution. Therefore the dot form would call the primitive and fail to
  compile, because the number of arguments does not agree. The named call removes
  this whole class of name collisions in the forwarding direction.

Naming and conflicts: `<ItemName>Ext` takes its place in the name table of each
Rust module together with the item itself, before any promoted type is allocated
(§3.5). Therefore a promoted type can never shadow an `Ext` trait, and the
existing "append `2`, then append `3`" strategy covers that (unreachable)
conflict.

### 4.3 The output per manifest field type

This is the table of `docs/rust-codegen.md` §3.2, with the navigation column
added. Each row of the snapshot column is unchanged.

| Musubi field type AST | Snapshot Rust (unchanged) | The `Ext` accessor returns |
| :--- | :--- | :--- |
| `String.t()` / `binary()` / `string()` / `atom()` | `String` | `State<String>` |
| `integer()` | `i64` | `State<i64>` |
| `float()` | `f64` | `State<f64>` |
| `boolean()` / `true` / `false` | `bool` | `State<bool>` |
| `"str"` / `1` / `1.0` literal | `String` / `i64` / `f64` | `State<String>` / `State<i64>` / `State<f64>` |
| `:literal` (an atom alone) | promoted single-variant enum `E` | `State<E>` |
| `nil` (alone) | `()` | `State<()>` |
| `map()` | `serde_json::Map<String, Value>` | `State<serde_json::Map<String, Value>>` — an opaque leaf; no navigation is generated |
| `%{key: T, ...}` | promoted struct `X` | `State<X>`, navigated through `XExt` |
| `list(T)` | `Vec<T'>` | `State<Vec<T'>>` — addressed by index (`at`, `first`, `last`, `iter`) |
| `stream(T)` | `Vec<T'>` | **`StreamState<T'>`** — addressed by key (`by_key`, `keys`, `at`, `iter`) |
| `T \| nil` | `Option<T'>` | `State<Option<T'>>`, plus `as_some() -> Option<State<T'>>` |
| `T \| U`, all atom literals | promoted C-style enum `E` | `State<E>` — a leaf; match on `value()` |
| `T \| U`, tagged maps | promoted internally tagged enum `E` | `State<E>` — a leaf; match on `value()` |
| `T \| U`, any other case | `serde_json::Value` | `State<Value>` — a leaf |
| `Module.t()` | `path::XState` | `State<path::XState>`, navigated through `path::XStateExt` |
| `Module.state()` | `musubi::StoreField<S>` | **`StoreState<S>`**, navigated through the child store's own `Ext` |
| `Musubi.AsyncResult.of(T)` | `musubi::AsyncResult<T'>` | **`AsyncState<T'>`** |
| `stream_async` (`AsyncResult.of(stream(T))`) | `musubi::AsyncResult<Vec<T'>>` | `AsyncState<Vec<T'>>`, plus `ok_stream() -> Option<StreamState<T'>>` |
| a declared upload | `musubi::UploadSlot` | **`UploadSlotState`** — a lazy leaf, plus `key()` and the one-step bridge `Mounted::upload_at(&slot)` (§3.4) |
| any other `X.of(T)` / not recognized | `serde_json::Value` | `State<Value>` — a leaf |

Notes:

- **Union enums are leaves.** Rust cannot navigate reactively *into* a variant
  unless each variant of each union has a view type, and the promotion rules of
  §3.4 must then also give them names. In practice a union changes as a whole
  (the server re-renders the discriminant tag together with the payload).
  Therefore `State<E>` plus `value()` is both simpler and more honest.
- **`ok_stream` is produced only for `stream_async` fields**, where the manifest
  already knows that the result is a stream. It has a caller on the day it lands:
  the `messages` field of the chat example is `stream_async`.
- **Promoted types also get an `Ext` trait**, under the same `<Name>Ext` rule.
  Therefore, for an inline `field :address do ... end`,
  `state.address().street()` is available.
- `Params`, command payloads, command replies and event payloads do **not** get
  navigation: they never appear in the state tree.

### 4.4 Where the drift check belongs

The whole-root `Decode` at publish time, as one step per envelope, no longer
exists. Something must continue to make "the generated file does not agree with
the server" a loud failure, and not a silent partial render. This is exactly the
class of failure that `MusubiError::Decode` exists for, and exactly the class
that §11 calls "worse than one loud stop".

*Decision: a layered drift check, with the root replace as the main layer.*

| Layer | When it runs | Cost | What it catches |
|---|---|---|---|
| **Root replace** (always on, release included) | each envelope that carries `replace ""` — that is, once per mount and once per rejoin | one `St::State` deserialization, and it discards the result | at the one moment when the whole tree appears completely on the wire, it checks that the shape of the whole tree agrees with the generated types |
| **Per transaction** (only `debug_assertions`) | each accepted envelope in a debug or test build | one `St::State` deserialization per envelope — the cost of v1 today, kept where it is free | any mid-session op that moves a field out of its declared type |
| **`value()` / `try_value()`** | each read, on the subtree that is read | only that subtree | everything the first two layers do not cover, caught at the point of use |

Mechanically, layer 1 and layer 2 are the same code as step 4 of §3.6, only with
a different guard condition. The error handling of the actor does not change at
all: the check runs through the existing dyn-erased sink hook. On a failure the
transaction is dropped, the mount fails with
`MusubiError::Decode { store_id: StoreId::root(), source }`, that root enters the
recovery of `docs/rust-client.md` §9, and the last good tree continues to render.
`RootSink` loses `publish` and gets:

```rust
/// Deserializes a whole hydrated wire root into `St::State` and throws the
/// result away. Validation only — the tree is built from the wire value, not
/// from this. The dyn-erasure that keeps the actor non-generic over `Store`.
fn validate(&self, hydrated: &Value) -> std::result::Result<(), serde_json::Error>;

/// The retained tree this root publishes into.
fn tree(&self) -> &StateTree;
```

*Why not "`value()` returns a `Result`, and the story ends there".* With
`value()` alone, the drift check falls on the field that the UI happens to read
first, and at the moment it happens to render. That is a late and partial
diagnosis. It also happens on the consumer thread and not on the actor thread,
with no `store_id` and no recovery. The root replace check keeps the failure
where the crate already reports failures, and it also keeps `MusubiError::Decode`
with the meaning it always had.

*An honest cost calculation.* Layer 1 is strictly cheaper than v1: one whole-root
deserialization per **mount plus rejoin**, against one per **accepted envelope**
in v1. For a page that receives ten envelopes per second, this is two
deserializations per session, and not twenty per second. It is also the only
whole-root deserialization left in the client. What it does not catch in a
release build is a non-root `replace` that changes a field type in mid-session.
That is a server/codegen contract violation, of the same class that the crate
already handles with `error!` plus recovery, and layer 2 catches it in every test
run, including all 21 wire fixtures.

*Also the panic.* `State::value` cannot fail in its signature. It panics on a
shape mismatch, and when the node is already removed. The reason is the layering
above: any `T` that a generated accessor can reach was checked once at mount time
against this exact server. Therefore, to hit the panic, either a mid-session type
change happened (see above), or someone ignored the change notification that
announced the removal of the node (§2.5). Two facts keep this honest and not
careless: `value()` carries `#[track_caller]`, so the panic points at the call
site and not at the crate; and `value()` is **never called on the actor task**,
so a panicking read destroys a frame or a task of the consumer, and never the
connection. `try_value()` is next to it, for embedders that navigate by hand, and
for anyone who still holds a `State<T>` across a reconnect that crosses a shape
change.

**The budget for a panic belongs to `value()` only, not to navigation.**
`x.prop()` cannot fail (the vocabulary of §2.4). Therefore the generated accessor
chain lands as `self.child("<wire key>")`, and not as an `.expect(..)`. A root
that no patch reached yet, and a root that teardown emptied, are both states that
`is_live()`/`try_value()` must answer. They are not conditions that must stop the
consumer during navigation. This is also why `examples/chat_room/desktop` can
delete the hand-written `tree()` helper that all reads went through. The check
moves back to where it belongs: the line that reads.

**A panic in a subscriber costs only its own notification.** The words "never the
connection" above are true for reads, but they were not true for subscription
callbacks. The `Drop` of `Notify` runs on the actor task (§3.6 step 9), and one
callback that unwinds would skip every later callback in the same transaction and
take the connection with it. Therefore each callback is wrapped separately in
`catch_unwind`: the panic hook already reported it, only its own notification is
lost, and the other subscribers get their notification as usual. This is the
implementation that makes those words true. It is not a relaxation of them.

---

## 5. Decisions carried forward from the owner

### 5.1 `musubi-gpui` exists — it reverses `docs/rust-client.md` §2.3

§2.3 says: "There is no `gpui` crate. gpui embedders implement `Spawner`/`Timer`
in three lines each ... A `gpui` adapter crate would put a fast-moving,
unpublished-ABI dependency in the workspace for no API benefit."

*That reasoning is correct for the v1 surface and wrong for this surface.* It
stands on "no API benefit", and that holds when the whole integration is only
"poll a `Stream` of whole-root snapshots in a `cx.spawn` loop". It is truly three
lines, and truly not worth a crate. Fine-grained subscription rewrites both
halves of that sentence.

**The two capabilities that support this crate:**

1. **The `!Send` hop becomes boilerplate per view and per subscription.** The
   subscription callback is `Fn(Change) + Send + Sync`, and a gpui entity is
   `!Send` and thread-affine. Therefore each subscription needs the same hop:
   capture a `WeakEntity` and an `AsyncApp`, schedule one update on the
   foreground executor, call `cx.notify()`, and then branch on the case where the
   entity is gone. On the v1 surface, you write this hop **once per window**.
   With per-node subscription, you write it once per view and per field. This is
   exactly the repeated glue code that gets subtle errors very easily, and an
   adapter exists to take it over. The unification of §2.4 makes this argument
   one step wider: after the unification, **the seven handles have the same
   callback shape**, so the hop is the same hop, and the adapter absorbs all of
   it and not a part of it.
2. **A keyed `ChangeSet` makes incremental list updates possible.**
   `ChangeSet::collection_edits` names the item keys that are inserted, removed
   and moved, and their positions. This is exactly the input a virtualized list
   needs: it updates only the affected row range, instead of erasing every cached
   row height with `ListState::reset(count)`. The second form is what
   `examples/chat_room/desktop` does today, and what
   `docs/rust-gpui-example.md` §4.2 records as a "cost". To translate a keyed
   `ChangeSet` into a list update is adapter code by definition: this is the only
   place where the vocabulary of `musubi-state` and the vocabulary of gpui meet.

**The constraints on that crate are all deliberate:**

- **Thin.** One `observe(state, entity, cx)` that returns a `Subscription`, one
  of the same for each of the three navigation views (`StreamState`,
  `StoreState`, `AsyncState`), one `to_view(window, cx, apply)` that isolates
  that hop, and one list driver that is based on `collection_edits`. Nothing
  else. No widgets, no themes, no rendering. `UploadSlotState` does **not** get
  this one: its subscription never fires (§3.4), and to give it an `observe`
  hands out a token for something that can never deliver a notification.
- **It depends on `musubi-state` only.** It never depends on `musubi-client`, so
  gpui cannot reach the dependency graph of the client, not even transitively.
- **`gpui = "0.2.2"`**, the same pinned version that
  `examples/chat_room/desktop` and `gpui-component 0.5.1` already agree on. It
  enables the default features for the same feature unification reason that
  `docs/rust-gpui-example.md` records.
- **`publish = false`**, like every other crate here.
- **Excluded from the workspace.** `crates/musubi-gpui/Cargo.toml` carries an
  empty `[workspace]` table, and the root manifest adds
  `exclude = ["crates/musubi-gpui"]` next to `members = ["crates/*"]`. If one of
  the two is missing, gpui enters the root `Cargo.lock`,
  `cargo test --workspace` starts to build it, and the tokio isolation gate gets
  one more thing to examine. This is the precedent that
  `examples/chat_room/desktop` already set, moved one directory across and used
  again.
- **Its own CI job**, or none at all when it first lands — the same position that
  the example has today.

That isolated hop is what makes two things true at the same time: "it depends on
`musubi-state` only", and "handles off the tree can also use it":

```rust
/// The hop, on its own: takes a callback body written against the view, hands
/// back the `Send + Sync` closure every `subscribe` in the API asks for.
///
/// Generic over the notified **value**, never over the handle — which is
/// exactly what lets it serve `musubi-client`'s `StatusState` and `Upload`
/// (§2.4) without this crate depending on `musubi-client`.
pub fn to_view<E, V, A>(
    window: &Window,
    cx: &mut Context<V>,
    apply: A,
) -> impl Fn(E) + Send + Sync + 'static + use<E, V, A>
where
    E: Send + 'static,
    V: 'static,
    A: Fn(&mut V, E, &mut Window, &mut Context<V>) + Send + Sync + 'static;
```

**Two deviations, both facts of gpui 0.2.2, not taste.**

1. **One more `&Window` parameter**, one for `to_view` and one for
   `observe_with`. `apply` receives a `&mut Window`, and in 0.2.2 the only path
   from a background notification to a `&mut Window` is
   `Context::spawn_in(window, ..)` → `AsyncWindowContext` → `WeakEntity::update_in`.
   `AsyncWindowContext::new_context` is `pub(crate)`, and `Context<V>` itself
   does not carry a window handle. Therefore the window becomes a parameter, in
   the position where gpui itself puts it (immediately before `cx`), and every
   call site of §6.5.2 already has `window` in scope. The bodies of `observe` and
   `drive_list` do not need the window, and their signatures do not change at
   all.
   (`apply` is written as a named type parameter and not as `impl Trait`, only
   because the `use<..>` of edition 2024 — which stops the returned closure from
   capturing the lifetimes of `window` and `cx` — must name every type parameter
   in scope. The call sites do not change.)
2. **The hop is a channel, not a captured context.** The sketch in this section
   and in §6.3 clones `cx.to_async()` into the callback. This does not compile on
   0.2.2: `AsyncApp` holds an `rc::Weak<AppCell>` and a `ForegroundExecutor` that
   an explicit marker field pins as `!Send`, and a `Send + Sync` closure cannot
   hold it. Therefore a **value** crosses the thread: the returned closure holds
   an `UnboundedSender<E>` (which is `Send + Sync` for `E: Send`), and a
   foreground task spawned here drains the receiver and runs `apply` on the
   entity's own thread. The order is the order of the channel, and therefore the
   order in which the transactions produced them. The queue is unbounded, because
   the loss of one state notification puts the view out of sync, and the task
   that drains it is scheduled by the same executor that redraws — a backlog is
   one busy frame, not a leak. The RAII lifetime does not change: when the
   closure is dropped, the sender is gone, the receiver ends, and the task
   finishes.

`observe` and `observe_with` are built on it: the first is the special case where
`apply` does one `cx.notify()` only, and the second adds one more layer around it
that feeds the handle itself to the callback body. The call site therefore has
the same shape on the tree and off it — `state.subscribe(to_view(..))` and
`chat.status().subscribe(to_view(..))` correspond word for word (§6.5.2).

### 5.2 There is no second read path

**`Mounted::state() -> State<St::State>` is the one entry point that reads the
state.** There is no whole-root snapshot method, and no whole-root update stream.

Both are absent for the same reason: each one needs one whole-root
`Latest<Arc<St::State>>` cell, that is, one whole-root deserialization per
envelope. This design exists to remove that cost, and to keep it as sugar that
"the caller can choose to pay" is to keep it exactly as it is. There is also no
cheap whole-root update stream on top of the tree: that is one complete
materialization per envelope plus a queue, that is, a second data plane that runs
beside the tree.

The same discipline applies to the connection status and to uploads: each has one
name only that hands out a handle, and the three forms — read, watch and stream —
grow on the handle (§5.4, §6.4), and not as three parallel methods.

### 5.3 The surface of `Mounted`

| Method | What it hands out |
|---|---|
| `state() -> State<St::State>` | The root view of the retained tree. Not an `Option` — the root node exists when `mount` returns |
| `status() -> StatusState` | The liveness handle of BDR-0033; the current value is `status().value()` (§2.4, §5.4) |
| `command()`, `command_on()` | Commands; §6.1 shows how they combine with the tree |
| `events()` | The event plane; §6.2 shows why it does not join the unification of §2.4 |
| `upload(&store_id, name)` | The primitive of the upload registry — the regular form that starts from the state tree is the next one (§3.4) |
| `upload_at(&slot) -> Option<Upload>` | Gets the upload handle from an `UploadSlotState` in one step; both halves of the key come from the node (§3.4); §6.4 shows the boundary of the two planes |
| `Clone`, and `Drop` as unmount | The mount lifecycle; this design does not touch it |

State, liveness and upload each have one name only, and under the name is the
unification agreement of §2.4: to read is `value()`, to watch is `subscribe()`,
and the loop form is `into_stream()`.

`state()` is not an `Option`, so the view itself answers two lifetime questions:

| Question | How to read it |
|---|---|
| Nothing has landed yet | `state().revision() == 0` |
| Read one field | `state().title().value()` |
| Read the whole thing | `state().value()` / `state().try_value()` |
| The tree is closed after `disconnect()` | `!state().is_live()` |

When the consumer wants to observe a change, it puts the `Subscription` on the
node that it really cares about, and not on the root to wait for a whole root.

### 5.4 `latest.rs`: one cell, and it holds the status

**`RootCell` holds one `Latest` cell, which holds a `MountStatus`.** The state is
not in the cell — it is on the tree (§5.2); the cell moves behind a handle
(§2.4).

`MountStatus` is not state. It is a client-local liveness projection. No wire
message carries it, the server does not take part, and no node in the wire tree
can hold it (BDR-0033, `docs/client-contract.md`). To put it in the tree means to
invent a node that the server never renders, and then to exclude it from
`to_wire`, so that the mount cache does not persist it; to exclude it from
`to_hydrated`, so that `St::State` does not have to declare it; and to exclude it
from the drift check as well. Three exclusions to save one small cell is a bad
exchange.

The semantics of that cell are therefore: latest-value, edges only, the first
poll replays, close is a final state, and "a handle held across a disconnect will
read `Connecting` forever". That module holds the two types `Latest`/`Updates`,
their tests and their reason documents, plus one callback list next to the
sender/waker list (implementation item 1 below).

**The path that reaches it is one property, not two methods** (the unification
agreement of §2.4).

**A direct answer to the owner's annotation on the line
`chat.status().into_stream()`: "Is this how you get the handle?" — No.** The
handle is the `StatusState` that `status()` returns. `into_stream()` consumes it
and returns the `await` form of the same subscription. The relation of the three
flattens into one line:

```
mounted.status()                  -> StatusState        the handle
mounted.status().value()          -> MountStatus        the value
mounted.status().subscribe(cb)    -> Subscription       the subscription
mounted.status().into_stream()    -> impl Stream<..>    the stream form = the await form of the subscription
```

Lines 3 and 4 are **two faces of the same subscription**, not two capabilities.
Below `into_stream()` is the existing `Latest`/`Updates` subscription of this
cell, with not one edge more and not one edge less; to drop that stream is to
drop that `Subscription`. Both faces stay because the consumer has two shapes. A
consumer that puts the observation into a struct uses `subscribe`. A consumer
that awaits a condition in an async block uses `into_stream` (§6.5.1, the place
that waits for `Live`). And **this choice has nothing to do with whether the
value is on the tree**.

```rust
/// The mount's place in its connection lifecycle (BDR-0033), as a handle.
///
/// The one handle in the family (§2.4) that is **not** rooted at a tree node:
/// `MountStatus` is a client-local liveness projection that no wire message
/// carries, so its value lives in the `Latest<MountStatus>` cell this module
/// keeps. Cheap to clone; every clone addresses the same cell. `Send + Sync`
/// like every other handle in §2.4 — one `Arc` and nothing else.
#[derive(Debug, Clone)]
pub struct StatusState { ... }

impl StatusState {
    /// The current status, as a value. `Connecting` until the first accepted
    /// initial patch, and — unchanged — `Connecting` **forever** for a handle
    /// held across a `disconnect()`.
    pub fn value(&self) -> MountStatus;

    /// Subscribe. RAII, and the same `Subscription` every tree view hands
    /// back, so it lives in the same `Vec` as they do.
    ///
    /// The callback is handed the status it is being called *for*, not just
    /// "something changed": the cell coalesces, so a callback that re-read
    /// `value()` could observe a **later** edge than its own (§2.4).
    ///
    /// It does **not** fire on registration. Subscribe first, `value()` second:
    /// that order can repeat one idempotent assignment, never miss an edge.
    #[must_use = "dropping the subscription unsubscribes"]
    pub fn subscribe(&self, on_change: impl Fn(MountStatus) + Send + Sync + 'static)
        -> Subscription;

    /// **Consumes this handle** and hands back the same subscription in `await`
    /// shape, for a consumer whose shape is a loop — `while let Some(status) =
    /// ..` waiting on a condition (§6.5.1).
    ///
    /// Not an accessor and not a getter: `into_` is the shape conversion, and
    /// the handle is the thing being converted (§2.4). Handles are `Clone`, so
    /// a caller that still needs the handle converts a clone
    /// (`status.clone().into_stream()`); the common
    /// `mounted.status().into_stream()` consumes the one the accessor just
    /// made, and costs nothing.
    ///
    /// This is the existing `Latest` subscription, unchanged: latest-value not
    /// a queue, edges only, and the **first poll replays** `value()`.
    #[must_use = "the stream is the subscription; dropping it unsubscribes"]
    pub fn into_stream(self) -> impl Stream<Item = MountStatus> + Send + 'static;
}
```

**The implementation, in three items.**

1. **The callback list of `Latest<T>` sits next to `Updates<T>`.** The cell holds
   a set of senders and wakers, and next to it a set of
   `Arc<dyn Fn(MountStatus) + Send + Sync>`. After `set_with` decides that there
   is an edge, it **clones the owed callbacks under the cell lock, and calls them
   one by one only after it releases the lock** — word for word the same
   discipline as the `Notify` of the tree (§2.6). Therefore "the API runs caller
   code under a lock in exactly one place" is still true, and a callback can call
   `value()`, call `subscribe()`, or even drop its own `Subscription`, without a
   deadlock.
2. **The callbacks fire on the actor task.** The only writers are
   `RootSink::set_status` (`src/mounted.rs:170`) and `RootSink::clear` (`:201`),
   and both are on the path where the actor processes envelopes and teardown.
   That is the same task and the same head-of-line blocking cost as the callbacks
   of the state nodes (§2.6). The contract is therefore the same: **a callback
   only schedules; it does not compute.**
3. **`Subscription` uses the cell variant.** `Latest<T>` implements the
   `Unsubscribe` of §2.5, and `StatusState::subscribe` hands out a
   `Weak<dyn Unsubscribe>` plus a `SubscriberId`. After the cell is `close()`d,
   the drop of a remaining `Subscription` is a no-op, the same as one that points
   at a released node.

**Why a callback does not fire once at registration, but the first poll of the
stream replays.** This asymmetry follows from where the code runs; it is not
arbitrary. The replay of the stream happens on the **consumer's own** first poll,
in the consumer's task. A callback that fires at registration would run user code
on the **registrant's** thread, inside the call stack of `subscribe`. That is
exactly what §2.6 works to exclude, and it would also make `subscribe` and
`State::subscribe` (which never calls the callback) into two different things in
the same API. The cost is that the consumer must write "subscribe first,
`value()` second", and this order cannot miss anything: an edge that lands
between the two arrives through the callback, and the worst case is that the same
idempotent assignment happens twice.

**The window for one stale call is also the same.** The rule in §2.5, that "a
callback can be called one more time after its `Subscription` is dropped", holds
here word for word, and for the same reason (the call is outside the lock).

**The equivalent across crates.** The TypeScript side has `connection.status()`
plus `connection.onStatusChange(cb)` (`docs/client-contract.md`, "Connection
status") — the same two capabilities, two names. The Rust side has the same two
capabilities with one name only: `status()` hands out the property, and
`.value()` and `.subscribe()` are the two actions on it.

`RootCell`:

```rust
pub(crate) struct RootCell<St: Store> {
    tree: StateTree,
    events: Mutex<EventRegistry>,
    status: Latest<MountStatus>,
    uploads: Arc<Uploads>,
    _marker: PhantomData<fn() -> St>,
}
```

`RootSink::clear` is: `tree.close()` (and drop the returned `Notify`, which tells
each subscriber that the root is gone), then the close of the event registry,
`status.close()` and `uploads.clear()`.

### 5.5 `PatchEngine` is not a supported public entry point

**Decision (owner): do not make it public.** `PatchEngine`, `PatchEnvelope`,
`PatchOp`, `StreamOp`, `UploadOp`, `PushEvent` and `Uploads` are all off the
public surface. `docs/rust-client.md` §7 accordingly makes no promise for them.

*Why.* To make `PatchEngine` public pulls the whole **write half** of the tree
onto the public surface — `StateTree::apply`/`begin`/`close`, `Transaction`,
`Notify`, `ChangeSet`, `CollectionEdit`, `NodeKind`, `NodeId`, `Node`,
`SemanticValue`, `TreeError` — that is, the half of this document that the
implementation is most likely to overturn (the carry-over table, the journal and
the rollback, the settle order). For a capability with **no known consumer**,
this is a much larger semver promise, and the rule in AGENTS.md is: with no
second caller, make no promise.

*Where the public surface cuts, exactly.* The tree API has a read half and a
write half, and only the read half continues to be the consumer surface:

| | Stays or goes |
|---|---|
| `State<T>`, `StreamState`, `StoreState`, `AsyncState`, `UploadSlotState`, `UploadSlot`, `Subscription`, `Change`, `CollectionEdit`, `ReadError`, `NodeId` | **Public** — this is the new consumer surface, handed out by `Mounted::state()` |
| The read-only methods of `StateTree` (`root`, `node`, `to_hydrated`, `store_ids`, `len`) | **Public** — `State::tree()` returns it, and the type chain must be nameable |
| `StateTree::apply`/`begin`/`close`/`is_closed`, `Transaction`, `Notify`, `ChangeSet`, `NodeKind`, `Node`, `SemanticValue`, `TreeError`, `Unsubscribe`, `SubscriberId` | **Not public** — there is no caller outside `musubi-client`. The consumer gets the small amount of change information it needs as a `&[CollectionEdit]`, through the second parameter of `StreamState::subscribe` (§6.3), and does not have to see `ChangeSet` itself; `Unsubscribe`/`SubscriberId` are likewise needed by the cell variant that implements `Subscription` across crates (§2.5), and the consumer sees only `Subscription` |
| `PatchEngine`, `PatchEnvelope`, `PatchOp`, `StreamOp`, `UploadOp`, `PushEvent`, `Uploads` | **Not public** — not in the `pub use` of `crates/musubi-client/src/lib.rs`, and all `pub(crate)` |

`StoreId` and `UploadSlot` are not affected: they are still re-exported from
`musubi_client::generated`, because the prelude list of the generated bundle
names them (`docs/rust-codegen.md` §4.5). `PatchOp` and `StreamOp` are internal
paths, like `PatchEnvelope` — no public signature mentions them. This narrows the
re-export promise of §1.3 by one step; it does not overturn it. It narrows
**which** paths must continue to resolve, not where they resolve to.

*The enforcement is non-publication, not visibility.* `musubi-state` is
`publish = false`, and the consumer can reach only `musubi_client` and the names
that the prelude of the generated bundle names. The write half is still `pub` in
`musubi-state` (the cross-crate call needs it), but it is not re-exported, the
documentation does not name it, and it carries `#[doc(hidden)]`.

*The tests of the engine are in-crate, not integration tests.* Envelope decoding
and the op allowlist are in the `#[cfg(test)] mod tests` of `src/envelope.rs`.
Version discipline and atomicity are in the module of the same name in
`src/engine.rs`. Hydration and the change set are in the projection tests and the
transaction tests of `musubi-state`. "A handle with no connection behind it must
not transfer" is in `src/uploads/registry.rs`. **The `tests/` directory does not
decide the public surface**: a case written from `tests/` forces the thing under
test to be `pub`, and that puts the test location before the public surface,
which is the wrong order. What stays in `tests/` is what truly crosses the public
surface — the connection suite with the scripted socket, the fixture replay, and
the upload transfer.

*Why the TypeScript precedent is not a constraint.* `packages/client/src/index.ts`
exports `applyPatch`, `applyStreamOps` and `applyUploadOps`. They are three
**pure functions**: a document goes in, a document comes out, with no identity,
no subscribers, no transaction and no lifecycle, and the promise is the signature
itself. The equivalent on the Rust side is **not** `PatchEngine`. It is the tree
of `musubi-state` — a retained structure with `NodeId` identity, with RAII
subscriptions, and with a lifetime that spans envelopes. To apply the precedent
of a pure function to a retained stateful object treats "a similar shape" as "the
same promise". If an equivalent is really needed, that equivalent is already
here: the read half under `Mounted::state()` is the Rust form of "read the state
without wiring it up yourself", and its shape is in fact better to use than a
loop that folds envelopes by hand.

---

## 6. The API for the advanced surfaces: command, event, stream, upload

The first five sections define the state plane. Of the four advanced surfaces, only
**one** is on the state tree: stream. The other three — command, event and upload — are
not on the tree, and their **method of combination** with the tree is the same one: do
an action, then let the node that the action really changes notify you, instead of
polling a full root. The upload handle obeys the uniform convention of §2.4 (read with
`value()`, observe with `subscribe()`, take a loop with `into_stream()`), and it has a
bridge that goes from the tree to the handle in one step (`Mounted::upload_at`, §3.4).
§6.1 and §6.2 explain why command and event each have their present shape.

This section uses the shape of `CartState` from §4.2 (`title`, `lines`, `messages`,
`feed`, `checkout_panel`, `avatar`), because one shape covers all four surfaces. The
examples also use four obvious scalar fields — `total: i64`, `discount: i64`,
`last_coupon_status`, `avatar_url: String`. These fields change no argument. They only
make the examples read like real code. The comparison code of §6.3 changes to
`examples/chat_room/desktop`, because that is the real stream consumer in the repository.

**§6.1–§6.4 take the surfaces apart one at a time. §6.5 puts them back into one
program.** §6.5 writes the same business scenario twice — once with the plain client
(tokio, headless) and once with gpui. All the shapes come from the real store in
`examples/chat_room`. A reader who wants to see the combined form first can go directly
to §6.5, where each of the four surfaces carries a link back to this section.

| Surface | On the tree? | API surface | How the result is observed |
|---|---|---|---|
| command | No — the control plane | `command()` / `command_on()` | Subscribe to the node that this command will change (§6.1) |
| event | No — a separate registry | `events::<E, T>(&store_id)` | The event stream itself. It is orthogonal to node subscription and does not join the uniform convention of §2.4 (§6.2) |
| stream | **Yes** | `StreamState<T>` + `CollectionEdit` | Collection-level subscription and row-level subscription, two layers (§6.3) |
| upload | The slot is on the tree, but it is an inert leaf | The slot accessor returns `UploadSlotState`, and `Mounted::upload_at` bridges to the handle in one step. The handle carries `value()`/`subscribe()`, and the stream form is `into_stream()` | `Upload::subscribe(..)`, or `.into_stream()`. The upload plane and the tree do not notify each other (§6.4) |

The four surfaces together:

```rust
use generated::nav::*;                       // CartStateExt and so on, the nav module of §4.2

let cart: Mounted<CartPageStore> = connection.mount(params).await?;
let state: State<CartState> = cart.state();  // Not an Option. The root node always exists

// command — started on the control plane, the landing is observed on the tree
cart.command(ApplyCoupon { code: "SAVE10".into() }).await?;
let _total = state.total().subscribe(|change| redraw(change.revision));

// event — a queue orthogonal to the tree. There is no current value to read
let mut toasts = cart.events::<ToastPayload, _>(&StoreId::root());

// stream — two subscription layers: the collection watches the edits, the row watches itself
let rows: StreamState<MessageState> = state.messages();
let _list = rows.subscribe(|_change, edits| splice(edits));
let _row = rows.by_key("msg-1").unwrap().subscribe(|_| redraw_row());

// upload — the slot is an inert leaf on the tree (`UploadSlotState`), with a bridge to
// the handle in one step (§3.4). The live state is on the handle, and the handle shape
// agrees with the tree (§2.4)
let avatar = cart.upload_at(&state.avatar()).expect("root is mounted");
avatar.select(files).await?;
avatar.start().await?;
let _bar = avatar.subscribe(|handle| set_bar(handle.progress()));

// Connection status — the second handle outside the tree, with the same
// `.value()` / `.subscribe()` (§5.4)
let _pill = cart.status().subscribe(|status| set_pill(status));
```

### 6.1 command: started on the control plane, landing on the tree

**A command is the control plane, not state** (§5.3). It has no node and no revision,
and to send a command does not notify any subscriber. The content is in its other
half: **how you know that it landed**.

BDR-0009 holds all the tension here: **the reply is not gated by the patches**.
`reply.ok == true` only shows that the server accepted the command. It does not show
that the state change from that command reached the client. In v1 the only way to
observe the landing is to poll the full root:

```rust
// v1: send the command, wait for the next full root snapshot, then find the change yourself.
let previous = cart.snapshot().unwrap().total;
let reply = cart.command(ApplyCoupon { code: "SAVE10".into() }).await?;

let mut updates = cart.updates();
while let Some(snapshot) = updates.next().await {
    if snapshot.total != previous {   // A manual diff, and it works only for scalars
        break;
    }
    // Every accepted envelope arrives here — including pure upload cycles, pure event
    // cycles, and any change with no relation to this coupon. Each one has just paid
    // for one full root deserialization.
}
```

In v2 the landing has an object that you can subscribe to directly:

```rust
use generated::nav::*;

let state = cart.state();
let total = state.total();          // State<i64> — one node, not a snapshot

// Install the subscription first. `Subscription` is RAII and lives until `_sub` is
// dropped, so no patch that lands during the command `await` is missed. This is the
// same discipline as v1, where you must open `updates()` first. Only the token
// changes: it is now a value that you can put into a struct.
let (tx, landed) = oneshot::channel();
let tx = Mutex::new(Some(tx));
let _sub = total.subscribe(move |change| {
    if let Some(tx) = lock(&tx).take() {
        let _ = tx.send(change.revision);
    }
});

let reply = cart.command(ApplyCoupon { code: "SAVE10".into() }).await?;
if !reply.ok {
    return Ok(show(reply.message.as_deref().unwrap_or("rejected")));
}

// You wait for "the total node changed", not for "another envelope arrived".
let revision = landed.await?;
show(format!("total {} (rev {revision})", total.value()));
```

Those ten lines with `oneshot` do not enter the crate. `musubi-state` has no async API
surface (§1.3), and to wait for the next change is the consumer's own composition. For
a `Future`, attach a oneshot. For a stream, attach an mpsc. For a gpui notification,
use `observe` from `musubi-gpui`.

**In a real UI you do not need to wait at all.** Install the subscription one time in
the view constructor, and let the command handler only send:

```rust
impl CartView {
    fn new(cart: Mounted<CartPageStore>, cx: &mut Context<Self>) -> Self {
        let state = cart.state();
        let subs = vec![
            musubi_gpui::observe(&state.total(), cx),      // the total row
            musubi_gpui::observe(&state.discount(), cx),   // the discount row
        ];

        Self { cart, state, _subs: subs }
    }

    fn on_apply(&mut self, code: SharedString, cx: &mut Context<Self>) {
        let cart = self.cart.clone();

        cx.background_spawn(async move {
            cart.command(ApplyCoupon { code: code.into() }).await
        })
        .detach();

        // Do no UI update here. When the patch lands, the two subscriptions above each
        // notify their own row.
        // The result is the same if the server rejects the coupon — `last_coupon_status`
        // is another node. The view that subscribes to it redraws, and the total row
        // does not.
    }
}
```

*(§5.1 writes the adapter function as `observe(state, entity, cx)`. `Context<V>` carries
the entity itself, so the call site has two arguments.)*

**The target of `command_on` changes from a snapshot field to a node view:**

```rust
let panel = state.checkout_panel();                   // StoreState<PanelState>
let target = panel.store_id().expect("the panel is mounted");

cart.command_on(&target, Pay { method: "card".into() }).await?;
```

`panel` is a view bound to a `NodeId`, and store nodes are reconciled by `store_id`
(§3.2). Therefore `panel` stays valid across a reconnect and across a move to another
parent node, and the view that holds it does not need to read the store id from the
snapshot at each render. v1 must read the id again each time, because
`snapshot.checkout_panel.store_id` exists only on the current snapshot.

| | v1: full root polling with `updates()` | v2: node subscription |
|---|---|---|
| Wake condition | Any accepted envelope | Only when the subscribed node (or a descendant) really changed |
| "Did it land?" | The caller keeps the previous value and compares | `Change` is the answer itself. The revision is monotonic |
| Cost of each wake | One full root deserialization | Zero. You materialize only what you read |
| Unrelated cycles (pure upload, pure event, another field) | Wakes anyway | No wake (§3.4, §6.2) |
| Cancel the observation | Drop the Stream | Drop the `Subscription` (you can put it into a struct) |
| Command failure | All the changes are mixed in the same stream | The failure state is another node, and it notifies only its own subscribers |

*This surface was checked against the uniform convention of §2.4: nothing needs to
change.* A command is an action and the reply is its one-time result, and neither one is
a property. The **only observable thing** on this surface — whether the command landed —
is already answered with a handle (`state.total()`). `reply.ok` is a field access after
materialization and `command_on(&panel.store_id(), ..)` takes an identity, so neither
one must become a handle.

### 6.2 event: the second plane, orthogonal to the tree

**`Mounted::events::<E, T>(&store_id)` is the only entry point of the event plane**
(§5.3, §7). An event is not state: it is not on the tree, it has no node and no
revision, it does not appear in the `ChangeSet`, and it never wakes any node subscriber,
even when it arrives in the same envelope as a group of patches.

**It is also the only surface that does not join the uniform convention of §2.4.** The
first four rows of the table below give the reason: an event has no current value, so
`value()` cannot be defined; events do not merge, so the concept of a latest value does
not hold; and an event that occurs before the subscription is missed, while a late
property subscription can still read the value. To force an event into the property
form, you must invent a current value that holds the most recent event, and that form
makes a slow consumer lose events with no report — the delivery promise of BDR-0032
becomes void at once. A queue is the correct semantics for an event, so `events()` keeps
its name.

```rust
// One view installs two lines at the same time. They never wake each other.

// State: node subscription. Latest-value semantics — you can read the current value on
// the tree at any time.
let _title = musubi_gpui::observe(&state.title(), cx);

// Event: queue stream. Discrete semantics — there is no such thing as a current event.
let mut toasts = cart.events::<ToastPayload, _>(&StoreId::root());
cx.spawn(async move |this, cx| {
    while let Some(toast) = toasts.next().await {
        this.update(cx, |view, cx| {
            view.push_toast(toast.message);
            cx.notify();
        })?;
    }
    anyhow::Ok(())
})
.detach();

// Events of a child store: the same registry with a different store_id — that id also
// comes from a node view.
let panel = state.checkout_panel().store_id().expect("the panel is mounted");
let mut receipts = cart.events::<ReceiptReadyPayload, _>(&panel);
```

The differences between the two planes, item by item:

| | State node subscription | Event stream |
|---|---|---|
| Carrier | A node on the tree | The `(store_id, name)` registry |
| Semantics | Latest value: `value()` gives the current value at any time | Queue: no current value, only arrivals |
| Merge | One transaction notifies one time at most. `1 -> 2 -> 1` does not notify | Each event is delivered on its own and never merges |
| Can you miss it? | No — the value stays on the tree, and a late subscription can still read it | Yes — an event that occurs before the subscription is not sent again (BDR-0032) |
| Backpressure | No queue. A slow callback holds up the actor (§2.6) | An unbounded queue. A slow consumer collects its own backlog |
| Cancel | Drop the `Subscription` | Drop the Stream |
| Relative order | First | Second — step 9 of §3.6 comes before step 11 |

That order is a contract, not an accident: a consumer that reads state after an event
reads the state of **this envelope**, and not the state of the previous one.

One combination is common: the event says what happened, and the state says what is now
true. After one `send_message` the server pushes a `MessagePosted` event (to play a
sound and to scroll the list to the top) and also inserts the row through `stream_ops`
(to render it). In v2 these two actions take separate paths: the event stream triggers
the sound, the collection subscription triggers the row, and neither one redraws because
of the other. In v1 the same envelope wakes every `updates()` consumer, so the sound and
the full root recomputation are tied together.

### 6.3 stream: the two subscription layers of `StreamState<T>`

This is the only one of the four surfaces that is on the tree, and the only one that
gets a new API surface. A stream is an **ordered and keyed** collection (§3.1), and
`StreamState<T>` is its view.

#### Get a `StreamState`

```rust
// A stream declared directly (`stream(T)` in the manifest, §4.3)
let rows: StreamState<MessageState> = state.messages();

// stream_async (`AsyncResult.of(stream(T))`): first the async node, then the collection
let feed: AsyncState<Vec<MessageState>> = state.feed();
let rows: Option<StreamState<MessageState>> = feed.ok_stream();
```

`ok_stream()` gives `None` only when the wire `result` is `null`, so it covers exactly
the duty of the `stale_or_fresh` helper function in `examples/chat_room/desktop` today:
**if the result is present, give the collection, whether the result is fresh or stale**.
`feed.status()` answers the separate question of whether the result is stale, and a
status change notifies only the async node and no row (§3.3). Today the two questions
sit in one function, because there is only one full root snapshot and no place to ask
them separately.

#### The two subscription layers: collection level and row level

```rust
// Collection level: the shape of the list changed — insert, remove, move, reset.
let _list = rows.subscribe(|_change, edits| apply_splices(edits));

// Row level: a field of this row itself changed. The identity is the item_key, not the
// index, so the row survives a reorder.
let row: State<MessageState> = rows.by_key("msg-1").unwrap();
let _body = row.subscribe(|change| redraw_row(change.revision));

// If you want only the fact that the collection changed, and not the edit list: go down
// to the generic view, and the callback returns to one argument. The notification time
// is identical, and only the difference list is absent.
let _count = rows.as_state().subscribe(|_change| redraw_counter());
```

The two `subscribe` calls are two methods on two types, not an overload:
`StreamState::subscribe` takes two arguments, `State::subscribe` takes one, and
`StreamState` does not `Deref` to `State`, so a call site always has exactly one
candidate (§2.4 gives the full argument for the naming).

Who is notified, one op at a time:

| `stream_op` | Collection node | Affected row node | Other rows | `collection_edits` |
|---|---|---|---|---|
| `insert` of a new `item_key` | Notify | A new node, with no subscriber at this moment | No notify | `Inserted { index }` |
| `insert` of an existing key, same position, changed value | Notify | Notify | No notify | Empty — a change inside a row is not a list edit |
| `insert` of an existing key, changed position, same value | Notify | **No notify** | No notify | `Moved { from, to }` |
| `insert` of an existing key, same position and same value | No notify | No notify | No notify | Empty — for this op, the transaction changed nothing (§9.2) |
| `delete` | Notify | Notify one time, and after that `is_live() == false` | No notify | `Removed { index }` |
| An overflow row that `limit` trims | Notify | The same as above | No notify | `Removed { index }` |
| `reset` ++ a group of `insert` (a full refresh) | Notify, unless the refreshed list is identical byte for byte | Notify only the rows whose value really changed — the carry-over table keeps the `NodeId` (§3.1) | No notify | `Reset` followed by several `Inserted` |

Two properties are easy to misread. They are stated here clearly:

- **The semantic value of the collection contains the semantic value of every row**
  (§9.1), so **any** change inside a row notifies the collection and its ancestors. A
  row-level subscription does not buy "the collection is not notified". It buys "**only
  the view of that row redraws**": the collection subscriber gets an empty edit slice,
  so the list driver splices nothing; the row subscriber gets a notification, so only
  that row redraws.
- **The edits are given in application order, and the index of each edit is the index at
  the moment when that edit occurs.** The adapter can apply them in the same order and
  does not need to correct the indexes. This small benefit is what makes
  `CollectionEdit` worth its existence.

#### Read: `len` / `at` / `iter` / `by_key`

```rust
rows.len();                              // Materializes nothing. Reads the item count of the collection node
rows.is_empty();
rows.keys();                             // The item_key values in list order
rows.at(3);                              // Option<State<MessageState>> — address by index
rows.by_key("msg-1");                    // Option<State<MessageState>> — address by key
for (key, row) in rows.iter() { ... }    // Iteration over (Arc<str>, State<MessageState>)
rows.value();                            // Vec<MessageState> — materializes the whole list, see §10.1
```

`iter()` produces **row views**, not row snapshots: you can `subscribe` to each item on
its own, you can store each item in a row component, and each item stays alive across
transactions. This is the most substantial difference from the `&[MessageState]` of v1 —
that slice is data borrowed from the current snapshot, it has no identity, and it does
not survive the next envelope.

The row renderer of a virtualized list therefore becomes one single-row materialization:

```rust
fn message_row(&self, index: usize, dimmed: bool) -> AnyElement {
    let Some(row) = self.rows.at(index) else {
        return empty_row();               // The row count of `ListState` and the
                                          // collection are not aligned in this frame
                                          // (between the splice and the redraw). They
                                          // align in the next frame
    };

    render_bubble(&row.value(), dimmed)   // Materializes only the four fields of this row
}
```

*An honest statement.* Row rendering in v1 is also cheap — it does an index lookup into
a full root snapshot that is already deserialized. The difference is not at the render
point. The difference is **when you pay the cost**: v1 pays for one full root per
envelope, whatever the number of rows drawn; v2 pays for the number of rows really
drawn, and the 97 rows that are not drawn (a list of 100 rows, a viewport of 3 rows)
cost nothing.

#### The connection to the gpui `ListState`

This is the whole content of the second reason for `musubi-gpui` (capability 2 in §5.1):

```rust
/// Translates a keyed `ChangeSet` into list splices. This is the only place where the
/// vocabulary of `musubi-state` meets the vocabulary of gpui.
pub fn drive_list<T, V: 'static>(
    rows: &StreamState<T>,
    list: &ListState,
    cx: &mut Context<V>,
) -> Subscription {
    let list = list.clone();
    let view = cx.entity().downgrade();
    let app = cx.to_async();

    rows.subscribe(move |_change, edits| {
        // The callback is `Send + Sync`, and a gpui entity is `!Send` and thread
        // affine: this one hop is the boilerplate that capability (1) describes (§5.1).
        let edits = edits.to_vec();
        let (list, view) = (list.clone(), view.clone());

        app.clone().spawn(async move |cx| {
            view.update(cx, |_view, cx| {
                for edit in &edits {
                    match edit {
                        CollectionEdit::Inserted { index, .. } => list.splice(*index..*index, 1),
                        CollectionEdit::Removed { index, .. } => {
                            list.splice(*index..*index + 1, 0)
                        }
                        CollectionEdit::Moved { from, to, .. } => {
                            list.splice(*from..*from + 1, 0);
                            list.splice(*to..*to, 1);
                        }
                        CollectionEdit::Reset => list.reset(0),
                    }
                }

                cx.notify();
            })
        })
        .detach();
    })
}
```

**`splice` is the unverified contact point from §10.2.** If gpui 0.2.2 exposes no
incremental update other than `reset(count)`, this function degrades to one line,
`list.reset(rows.len())`. The row height cache is dropped as before, but the
**row-level subscription still works**, so the benefit of the per-row redraw stays
complete and only the item "no recomputation of the row heights" is lost. The fallback
path is clean, and this is what §5.1 means when it says that capability (1) alone is
enough to support that crate.

#### Comparison: `examples/chat_room/desktop`

With the full root snapshot, this view has the following shape:

```rust
struct ChatWindow {
    snapshot: Option<Arc<State>>,       // One full root, replaced by each envelope
    messages: ListState,                // Must be kept in sync with the field above by hand
}

// Wakes one time per envelope, whether or not the envelope touched the message list.
while let Some(snapshot) = updates.next().await {
    view.adopt(Some(snapshot), window, cx);
    cx.notify();                        // Redraws the whole window
}

fn adopt(&mut self, snapshot: Option<Arc<State>>, window: &mut Window, cx: &mut Context<Self>) {
    let Some(state) = snapshot else { return };

    // 1. Name draft: read again for each envelope, and compared with the input by hand
    let name = state.current_user.name.clone();
    if self.name_input.read(cx).value().as_ref() != name.as_str() {
        self.name_input.update(cx, |input, cx| input.set_value(&name, window, cx));
    }

    // 2. List: reset as soon as the length changes, which drops the cached height of every row
    let count = stale_or_fresh(&state.messages).len();
    if self.messages.item_count() != count {
        self.messages.reset(count);
    }

    self.snapshot = Some(state);
}

fn messages(&self) -> &[MessageState] {
    self.snapshot.as_deref().map(|s| stale_or_fresh(&s.messages)).unwrap_or_default()
}
```

With the retained tree, the same view:

```rust
use generated::nav::*;
// The alias only makes `State<State>` readable in the text: the first name is the view,
// the second name is the store shape.
use generated::chat_room::stores::chat_room_store::State as ChatState;

struct ChatWindow {
    state: State<ChatState>,                     // The root view, alive across a reconnect
    feed: AsyncState<Vec<MessageState>>,         // The stream_async node
    rows: Option<StreamState<MessageState>>,     // Present only when the result is not null
    list: ListState,
    _subs: Vec<Subscription>,                    // The fixed subscriptions
    _list_driver: Option<Subscription>,          // Created and released with the collection node
}

impl ChatWindow {
    fn new(chat: Mounted<ChatRoomStore>, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let state = chat.state();
        let feed = state.messages();

        let subs = vec![
            // 1. Name draft: subscribe to that one leaf. A change in another field no
            //    longer touches the input, so the window where an unrelated envelope
            //    overwrites the text while the user types is gone.
            //    (`observe_with` is the variant of `observe` that takes a callback.
            //     `observe` itself only calls `cx.notify()` one time.)
            musubi_gpui::observe_with(&state.current_user().name(), window, cx, |view, name, window, cx| {
                view.set_draft(&name.value(), window, cx);
            }),
            // 2. Load state: subscribe to the async node itself. The `loading <-> ok`
            //    change touches only that node (§3.3), so the list dims on a reconnect
            //    and **no** row redraws. The same callback also re-installs the list
            //    driver when the result appears or disappears.
            musubi_gpui::observe_with(&feed, window, cx, |view, _feed, _window, cx| {
                view.rebind_rows(cx);
                cx.notify();
            }),
        ];

        let mut this = Self {
            state,
            feed,
            rows: None,
            list: ListState::new(0, ListAlignment::Top, px(200.0)),
            _subs: subs,
            _list_driver: None,
        };

        this.rebind_rows(cx);
        this
    }

    /// Idempotent. The collection node is born in the transaction where the result is
    /// first not `null`, and it lives until the root is unmounted or the server sets
    /// the result back to `null`. The driver is therefore installed and removed only
    /// when the presence of the collection changes, and an ordinary row arrival never
    /// passes through here. The criterion is node identity: the same `NodeId` means
    /// that the driver is still installed in the same place.
    fn rebind_rows(&mut self, cx: &mut Context<Self>) {
        let next = self.feed.ok_stream();

        if next.as_ref().map(StreamState::node) == self.rows.as_ref().map(StreamState::node) {
            return;
        }

        self._list_driver = next
            .as_ref()
            .map(|rows| musubi_gpui::drive_list(rows, &self.list, cx));
        self.rows = next;
    }

    /// The read path shared by the rendering and the headless tests. The output changes
    /// from `&[MessageState]` to a collection view, so `chat.messages().len()` is
    /// written as `chat.message_count()`. Every asserted number stays the same.
    fn messages(&self) -> Option<&StreamState<MessageState>> {
        self.rows.as_ref()                        // Materializes nothing, borrows no snapshot
    }

    fn message_count(&self) -> usize {
        self.rows.as_ref().map_or(0, StreamState::len)
    }
}
```

The account, item by item:

| | With the full root snapshot | With the retained tree |
|---|---|---|
| An envelope that touches only `online_users` | Full root deserialization + `adopt` + a redraw of the whole window | Notifies only the subscribers of `online_users`. The list, the input and the bubbles do not move |
| A new message arrives | Full root deserialization + `ListState::reset(count)`, which drops the row height cache of 100 rows | One `splice(0..0, 1)`, and the row height cache of 99 rows is kept |
| An existing message is edited | The length does not change ⇒ no reset, but the whole window redraws | Only the subscribers of that row are notified, and only that row redraws |
| A reconnect, `ok -> loading` (with the old payload) | Full root deserialization + a redraw of the whole window | Notifies only the async node. The list dims, and zero rows redraw |
| An envelope with upload progress only | Full root deserialization + a redraw of the whole window | Zero wakes on the state plane (§6.4) |
| The name draft | One string comparison per envelope | The callback runs only when `current_user.name` really changed |

The six headless `#[gpui::test]` tests are the acceptance gate for this read path
(§5.3): the poster, the row count, `debug_bounds(..)` and the scripted wire frame
assertions all read through the view accessors and never read state through `Mounted` —
that is exactly why they can act as a gate. The row count is read as
`chat.message_count()`, because the accessor hands out a collection view and not a slice
borrowed from a snapshot.

### 6.4 upload: two planes coexist and do not notify each other

**The semantics of the upload plane do not change at all** (§3.4, the liveness table of
§7). What changes is the two method names on the handle (§2.4) and the path that reaches
the handle (§3.4). The half on the tree is an inert leaf —
`NodeKind::UploadSlot { name, owner }`. Its semantic value is the name plus the owner,
the server renders the same marker in every cycle, and the owner is fixed at creation,
so this leaf **never changes and never notifies**. The live upload state is in the
`Uploads` registry, and the handle that `Mounted::upload_at(&slot)` hands out in one
step reads and writes it.

```rust
// The half on the tree: a constant leaf that hands out a handle (§2.4).
let slot: UploadSlotState = state.avatar();

// The bridge to the upload handle in one step: both key halves come from the node, with
// no bare string and no hand-written StoreId.
let avatar: Upload = cart.upload_at(&slot).expect("root is mounted");

// You can still read the **value** of the slot. It is only that the path from the tree
// to the upload plane no longer passes through it.
let name: UploadSlot = slot.value();

// The control plane: unchanged (the liveness table of §7).
let entries = avatar
    .select(vec![UploadFile::new("me.png", "image/png", bytes)])
    .await?;
avatar.start().await?;

// The data plane: a second plane with no relation to the tree, but its shape agrees with
// the tree word for word (§2.4).
let current: UploadHandle = avatar.value();                        // the value
let _bar = avatar.subscribe(|handle| set_bar(handle.progress()));  // an RAII subscription

// The loop form is still available: the same subscription in a different form, and the
// semantics do not change by one word. `into_stream` consumes a handle, and a handle is
// `Clone`. `avatar` is used again below, so give it a clone.
let mut progress = avatar.clone().into_stream();
while let Some(handle) = progress.next().await {
    render_bar(handle.progress());
}

// The server writing the URL into the state after the upload completes is a separate
// matter, and it goes through the tree:
let _url = state.avatar_url().subscribe(|_| redraw_avatar());
```

**The API surface of the upload plane:**

| | |
|---|---|
| Get the handle | `Mounted::upload_at(&slot) -> Option<Upload>` — one step, and both key halves come from the node (§3.4). `Mounted::upload(&store_id, name)` is the primitive below it |
| Read the current value | `Upload::value() -> UploadHandle` |
| Install one observation | `Upload::subscribe(cb) -> Subscription` |
| Get the loop form | `Upload::into_stream(self) -> impl Stream` — the `await` form of the same subscription |
| `select`/`start`/cancel/preflight/an external `Uploader` | The control plane, orthogonal to the tree (`docs/rust-client.md` §10) |
| The `UploadHandle` value type | The fields, `progress()`, `PartialEq`, and the difference from the TypeScript client: each read gives a clone, not a mutable object that changes in the hands of the reader |

**A direct answer once more: `into_stream()` does not get a handle.** The handle is the
`Upload` that `Mounted::upload_at(&slot)` returns. `into_stream()` takes that handle (or
takes a clone of it) and returns the `await` form of the same subscription.
`upload.subscribe(cb)` and `upload.clone().into_stream()` are two forms of the same
subscription, not two capabilities — below both of them is the notification that this
cell owes at the publish point, and one form gives it to a callback while the other
gives it to `poll_next`.

**The implementation mapping.** The cell today already has the shape "fold under the
lock, publish outside the lock" (`UploadCell::publish` does one `retain` on the sender
list), so put the callback list beside it with the same discipline: clone under the
lock, call outside the lock. **An honest statement: the callbacks run on two tasks.**
The fold of `upload_ops` comes from the actor task, and the control plane state changes
(`select`, `start`, a transfer failure) come from **the task that calls them**
(`UploadCell::update` is the `notify()` of the control plane). This is word for word the
same as the behavior of that stream today, which also has these two `unbounded_send`
calls. The uniform convention does not change it, but it is worth writing down now,
because the contract that a callback only schedules and does not compute applies here
too, and the object scheduled here is often a UI thread.

*One thing that the uniform convention buys along the way.* The queue semantics stay on
`.into_stream()`. **The callback form has no queue** — it runs to completion
synchronously at the publish point and collects no backlog. A consumer that only wants
to draw a progress bar can now avoid that unbounded queue completely. In the table of
§6.2, upload and event share one cost, and this is the part of that cost that the upload
half does not have to pay.

*An honest statement about one awkward name, and about how much this round of renaming
removes.* The thing that `Upload::value()` returns is called `UploadHandle` — a **value**
with "Handle" in its name. The name `UploadHandle` is older than this uniform
convention, and it means the value "the state of that upload at this moment", not the
handle that the glossary defines. To rename it needs a change to every signature in
`src/uploads/*` plus three documents, and it buys only one tidy word, so **do not
rename it**. This note exists so that a reader does not read the name as an error.

Note that the change of the value reader from `get()` to `value()` already removes most
of this awkwardness. `upload.get()` reads as "get a Handle", and the reader must supply
the border between the handle and the value. `upload.value()` reads as "get the value of
this handle, and that value happens to carry the historical name `UploadHandle`" — the
method name states the role, and what remains is only one imprecise type name, and no
longer two roles covered by one word.

The boundaries, one cycle type at a time:

| What happened | Tree subscribers | Handle subscribers |
|---|---|---|
| An envelope with `upload_ops` only (progress 0 → 37) | **Not one wake** — the semantic value of the slot did not change | Wake |
| The upload completes, and the server writes the URL into the `avatar_url` field at the same time | Wakes `avatar_url` and its ancestors, and no other field | Wake (the complete op) |
| An envelope with state only | Wakes the changed nodes | No wake |
| A store unmount | The subtree is released, `is_live() == false` | Pruned (`tree.store_ids()`, §3.5) |
| A root unmount | `tree.close()` notifies one time, and after that all views are dead | The stream ends |

This is a **net gain over v1, and it is already implemented**: the change notification
rules of `docs/rust-client.md` §5 contain the clause "or its `store_id` appears in
`upload_ops`", and that clause is deleted (§3.4). Today an upload split into 100 parts
makes every accepted progress op trigger one full root deserialization plus one full
root publication — 100 times for one upload. In v2 the count is 0. The stream of the
upload progress bar advances as before with no lost item, because that stream was never
on the tree.

*A symmetric statement.* The reverse also holds: a view that subscribes to `avatar_url`
**does not** redraw because the progress bar moved, and it does not redraw because one
`select` failed — that is the `status` of the handle, not a field on the tree. To watch
both, install two subscriptions, one through the tree and one through the handle. They
do not disturb each other, and this is the purpose of keeping the upload outside the
tree.
### 6.5 Two end-to-end examples: one scenario, two kinds of consumer

§6.1–§6.4 cut the four planes open one at a time. This section puts them back into
**one program**. It writes the same business scenario two times, so that the way to
combine the advanced functions becomes a shape that you can copy, and not four
unrelated fragments.

**The scenario** (six steps, with the real shape of `examples/chat_room`, not the
illustrative `CartState`): mount → render the message stream → send one message →
observe the receipt → upload one attachment and show the progress → disconnect and
reconnect.

**The two kinds of consumer:**

- **§6.5.1 The plain client** — `musubi-client-tokio`, headless, with no UI
  framework. It uses only `State<T>` subscriptions, one wake channel, and one loop.
- **§6.5.2 gpui** — the same scenario through `musubi-gpui`. The crate absorbs the
  thread hop and the list splice. This part shows what stays in the view code.

The two examples do not repeat a fragment that this document already shows. For the
single row renderer, see §6.3. For the implementation of `drive_list`, see §6.3. For
the boundary table of the two upload planes, see §6.4. For the `oneshot` form of
"wait for one settle", see §6.1. This section writes only the part that **combines**
them.

*The shape, stated once.* For the generated bundle, see
`examples/chat_room/desktop/src/generated.rs`. `messages` is `stream_async`
(⇒ `AsyncState<Vec<MessageState>>`, plus `ok_stream()`). `current_user` is an object.
`online_users` is `AsyncResult<Vec<OnlineUser>>`. `last_send_status` is an internally
tagged union (⇒ a leaf; match after `value()`, §4.3). `attachment` is one upload slot
(⇒ `UploadSlotState`, §4.3).
**This store does not declare push events today.** Therefore the event plane appears
in the two examples as a marked side note, with the shape from §6.2. It is the only
part of the four planes that does not come from a real store, and this text states
that fact.

#### 6.5.1 The plain client: `musubi-client-tokio`, headless

A program that you can start with `cargo run`. Its complete structure is this: **the
subscriptions put "what changed" into one channel, and one loop turns the channel
content into output.** There is no UI framework, and none is necessary. The
reactivity is in `musubi-state`, not in the renderer.

```rust
use anyhow::Context as _;
use futures::StreamExt;
use musubi_client_tokio::{
    // `StoreId` is not here. Since the upload uses `upload_at` (§3.4), the body of
    // this program never needs a hand-written store identity. Import it only when
    // the side note for the event plane needs it.
    generated::{AsyncState, State, StreamState, Subscription},
    CollectionEdit, Connection, MountStatus, Mounted, UploadFile,
};
use tokio::sync::mpsc::{self, UnboundedSender};

mod generated;
use generated::chat_room::stores::chat_room_store::{
    Attach, ChatRoomStore, ChatRoomStoreLastSendStatus as SendStatus, Params, SendMessage,
    State as ChatState,
};
use generated::chat_room::MessageState;
use generated::nav::*;                       // §4.2: once per file

const ROOM: &str = "lobby";

/// A subscription callback runs on the actor task (§2.6). The contract is "schedule
/// only, do not compute". Therefore each callback does one thing only: it puts one
/// tag into this channel. The rendering happens in the loop below.
#[derive(Debug)]
enum Wake {
    Feed,                        // the async node of the history: loading <-> ok <-> failed
    Rows(Vec<CollectionEdit>),   // the collection shape changed (edits arrive only with a notification, §2.4)
    Row(String),                 // a field of one row changed; the item_key identifies the row
    Receipt,                     // last_send_status settled
    Status(MountStatus),         // outside the tree: the BDR-0033 connection status (§5.4)
    Progress(u32),               // outside the tree: the upload progress (§6.4)
}

struct Headless {
    chat: Mounted<ChatRoomStore>,           // to hold it is to keep the mount (`Drop` is the unmount)
    state: State<ChatState>,
    feed: AsyncState<Vec<MessageState>>,
    rows: Option<StreamState<MessageState>>,
    tx: UnboundedSender<Wake>,
    _subs: Vec<Subscription>,               // the fixed subscriptions
    _rows_sub: Option<Subscription>,        // lives and dies with the collection node
    _row_subs: Vec<Subscription>,           // one per row
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let connection: Connection =
        musubi_client_tokio::builder("ws://127.0.0.1:4000/musubi").build()?;
    let (tx, mut wakes) = mpsc::unbounded_channel::<Wake>();

    // ── 1. Mount ─────────────────────────────────────────────
    // When mount returns, the root node already exists, even if the first patch has
    // not settled: `state()` is not an `Option` (§5.3). Write "nothing has settled
    // yet" as `revision() == 0`.
    let chat: Mounted<ChatRoomStore> = connection
        .mount::<ChatRoomStore>(ROOM, Params { room_id: ROOM.into() })
        .await?;

    let state: State<ChatState> = chat.state();
    let feed: AsyncState<Vec<MessageState>> = state.messages();

    // ── 2. Install the subscriptions first, then read ────────────────────
    // `Subscription` is RAII: the subscription lives as long as the token. If you
    // write `let _ = ..`, the subscription ends at once. `#[must_use]` catches this
    // mistake (§2.5).
    let subs = vec![
        // 2a. The receipt: one leaf node. Whether the command settled is whether
        //     this node changed (§6.1).
        state.last_send_status().subscribe({
            let tx = tx.clone();
            move |_change| { let _ = tx.send(Wake::Receipt); }
        }),
        // 2b. The async node of the history. `ok -> loading` (the reconnection)
        //     changes only this node, and no row (§3.3). Therefore this callback
        //     also reinstalls the collection subscription.
        feed.subscribe({
            let tx = tx.clone();
            move |_change| { let _ = tx.send(Wake::Feed); }
        }),
        // 2c. The online user count: another async node. When it changes, the two
        //     subscriptions above do not move. The v1 design, which notified every
        //     subscriber for each envelope, cannot give this. (The reuse of the
        //     `Wake::Feed` arm only saves one variant; the notifications of the two
        //     nodes are independent.)
        state.online_users().subscribe({
            let tx = tx.clone();
            move |_change| { let _ = tx.send(Wake::Feed); }
        }),
        // 2d. The connection status: the handle **outside** the tree (§5.4). The
        //     code is word for word the same as the three subscriptions above. This
        //     is what the uniform convention of §2.4 buys: one `Vec` named `_subs`
        //     holds every observation, and one status stream no longer needs a
        //     separate task and a separate field. The callback receives the edge
        //     that woke it, and not an instruction to read the current value again
        //     (the cell merges values).
        chat.status().subscribe({
            let tx = tx.clone();
            move |status| { let _ = tx.send(Wake::Status(status)); }
        }),
    ];

    let mut app = Headless {
        chat: chat.clone(),
        state,
        feed,
        rows: None,
        tx: tx.clone(),
        _subs: subs,
        _rows_sub: None,
        _row_subs: Vec::new(),
    };
    app.rebind_rows();                       // the collection can already exist (the cache seed)

    // ── 3./5. The script: send one message, then upload one attachment ────
    tokio::spawn({
        let (chat, state, tx) = (chat.clone(), app.state.clone(), tx.clone());
        async move {
            // Send the command and stop there. The subscription 2a reports the
            // settle. This code does not wait, and it does not poll.
            let reply = chat.command(SendMessage { body: "hello".into() }).await?;
            println!("queued={}", reply.queued);   // BDR-0009: accepted ≠ settled

            // The command above needs no wait, because it does not read the tree.
            // The code below reads the tree, so it first waits for `Live`: when
            // `mount` returns, the first patch has not settled
            // (`MountStatus::Connecting`). This is a wait for the first patch, and
            // the connection status is the criterion. An equivalent form waits for
            // `state.revision() != 0` (§5.3). **A subscription itself
            // never needs a wait**: you can install a node view now, and it fires
            // when the patch settles.
            //
            // This code must await a condition. Therefore it takes the **stream
            // form** of the same property instead of the callback form. Two forms,
            // one property (§2.4). `status()` makes a handle here, and
            // `into_stream()` consumes it in place, so no clone is necessary. The
            // first poll replays the current value. Therefore this loop does not
            // hang, even if `Live` already occurred (§5.4).
            let mut statuses = chat.status().into_stream();
            while let Some(status) = statuses.next().await {
                if status == MountStatus::Live {
                    break;
                }
            }

            // The upload: one step bridges from the slot handle to the upload
            // handle, and both key halves come from the node (§3.4). You do not
            // materialize the slot to get the name, and you do not write
            // `StoreId::root()`.
            let upload = chat
                .upload_at(&state.attachment())
                .context("attachment slot is gone")?;

            // Subscribe before `select`: this plane of the handle has queue
            // semantics and does not replay (§6.4). This code also takes the stream
            // form. An async task that already runs consumes it, and the stream
            // itself is the subscription, while a `Subscription` would need another
            // place to live. `into_stream` consumes one handle, and the code below
            // still uses `upload`, so it gets a clone.
            let mut progress = upload.clone().into_stream();
            tokio::spawn(async move {
                while let Some(handle) = progress.next().await {
                    let _ = tx.send(Wake::Progress(handle.progress()));
                }
            });

            let bytes = std::fs::read("note.md")?;
            upload
                .select(vec![UploadFile::new("note.md", "text/markdown", bytes)])
                .await?;
            upload.start().await?;

            // The server consumes the item in `attach`, and inserts that row into
            // the stream through PubSub. Therefore the settle of the attachment
            // uses the same collection subscription as a normal message.
            let reply = chat.command(Attach {}).await?;
            println!("attached={} name={:?}", reply.attached, reply.name);
            anyhow::Ok(())
        }
    });

    // ── 4./6. One loop turns the wakes into output ───────────────────────
    while let Some(wake) = wakes.recv().await {
        match wake {
            // The collection can have just appeared or just disappeared, so
            // reinstall; also report the loading state once.
            //
            // `feed.status()` gives a **value** directly, not a handle. The status
            // of an async node is part of the semantics of that node itself, and it
            // has no separate identity to subscribe to (the criterion in §2.4, and
            // §3.3). This does not contradict `chat.status()` above, which gives a
            // handle: that one has its own cell.
            Wake::Feed => {
                app.rebind_rows();
                println!("history: {:?}  online: {:?}",
                         app.feed.status(), app.state.online_users().status());
            }
            // The edits come in application order, and each index holds at the
            // moment of that edit. Apply them directly (§6.3).
            Wake::Rows(edits) => {
                for edit in &edits {
                    match edit {
                        CollectionEdit::Inserted { item_key, index, .. } =>
                            println!("+ [{index}] {item_key}"),
                        CollectionEdit::Removed { item_key, index } =>
                            println!("- [{index}] {item_key}"),
                        CollectionEdit::Moved { item_key, from, to } =>
                            println!("~ {item_key} {from} -> {to}"),
                        CollectionEdit::Reset => println!("== reset"),
                    }
                }
                app.rebind_row_subs();       // a new row needs a row subscription, and an old row dies with its node
            }
            // A change inside a row: materialize only this row (§6.3, "materialize
            // as much as you read").
            Wake::Row(item_key) => {
                if let Some(row) = app.rows.as_ref().and_then(|r| r.by_key(&item_key)) {
                    let msg = row.value();
                    println!("* {} {}: {}", msg.id, msg.sender, msg.body);
                }
            }
            // The receipt: one leaf, so one match is enough (a union is a leaf, §4.3).
            Wake::Receipt => match app.state.last_send_status().value() {
                SendStatus::Idle => {}
                SendStatus::Ok { id } => println!("delivered {id}"),
                SendStatus::Failed { reason } => println!("send failed: {reason}"),
            },
            // The disconnection and the reconnection: this arm **does nothing**. The
            // tree stays, and the last good state stays readable (BDR-0015). The
            // `replace ""` of the rejoin is one reconciliation: an unchanged subtree
            // keeps its NodeId, keeps its subscriptions, and notifies nobody (§7).
            Wake::Status(status) => println!("connection: {status:?}"),
            Wake::Progress(percent) => println!("upload {percent}%"),
        }
    }

    Ok(())
}

impl Headless {
    /// Idempotent. The collection node appears in the transaction where `result` is
    /// not `null` for the first time, and it lives until the root unmounts or the
    /// server sets the result back to `null`. The criterion is the node identity
    /// (the same rule and the same reason as `rebind_rows` in §6.3).
    fn rebind_rows(&mut self) {
        let next = self.feed.ok_stream();
        if next.as_ref().map(StreamState::node) == self.rows.as_ref().map(StreamState::node) {
            return;
        }

        self._rows_sub = next.as_ref().map(|rows| {
            let tx = self.tx.clone();
            rows.subscribe(move |_change, edits| {
                let _ = tx.send(Wake::Rows(edits.to_vec()));
            })
        });
        self.rows = next;
        self.rebind_row_subs();
    }

    /// One row subscription per row. The row identity is the `item_key`. Therefore a
    /// pure reorder notifies no row, and it needs no reinstall (§3.1).
    ///
    /// *To keep the example short, this code reinstalls every row subscription after
    /// each batch of edits.* A real consumer adds and removes per edit: `Inserted`
    /// installs one, and `Removed` drops one (the discipline of §2.5: a node that is
    /// declared removed reads as dead, so the row view must go with that edit).
    fn rebind_row_subs(&mut self) {
        let subs = match self.rows.as_ref() {
            None => Vec::new(),
            Some(rows) => rows
                .iter()
                .map(|(item_key, row)| {
                    let (tx, key) = (self.tx.clone(), item_key.to_string());
                    row.subscribe(move |_change| { let _ = tx.send(Wake::Row(key.clone())); })
                })
                .collect(),
        };

        self._row_subs = subs;
    }
}

// ── The event plane (side note) ─────────────────────────────────────────
// `ChatRoomStore` does not declare push events today, so this part has no generated
// type in this repository. The shape comes from §6.2. This plane is orthogonal to
// every subscription above: an event never notifies a node subscriber, and a node
// change never enters the event queue. (To enable it, add `StoreId` back to the
// imports above. The event plane dispatches by `(store_id, name)`, and it is the only
// place in this program that still needs an explicit store identity.)
//
// let mut toasts = chat.events::<ToastPayload, _>(&StoreId::root());
// tokio::spawn(async move {
//     while let Some(toast) = toasts.next().await { println!("toast: {}", toast.message); }
// });
```

**The planes that this code shows:**

| Plane | Location in the code | Key point | See |
|---|---|---|---|
| **command** | `command(SendMessage)`, `command(Attach)` | Send it and stop there. The `last_send_status` subscription reports the settle. No poll, and no stored previous value for comparison | §6.1 |
| **event** | The side note at the end (this store does not declare it) | A second plane, orthogonal to the node subscriptions; queue semantics, no replay | §6.2 |
| **stream** | `feed.ok_stream()` + `rows.subscribe` + one `row.subscribe` per row | Two layers of subscription: the collection watches the edits, and a row watches itself. Apply the edits directly, and do not diff | §6.3 |
| **upload** | `chat.upload_at(&state.attachment())` + `select`/`start` + the `into_stream()` loop | The slot is a lazy leaf handle, and one step bridges to the upload handle (no bare string, no hand-written `StoreId`). The live state is on the handle, and the progress notifies no node | §3.4, §6.4 |
| The state plane | The four fixed subscriptions + the `Wake` loop | A callback schedules only and does not compute (the actor task). `Subscription` is RAII | §2.5, §2.6 |
| The two handles outside the tree | The `chat.status().subscribe(..)` of 2d, and the `status().into_stream()` that waits for `Live` | Two forms of one property: use the callback to put the observation into a struct, and use the stream form to `await` a condition | §2.4, §5.4 |
| Reconnection | The `Wake::Status` arm, which is empty | The tree survives the rejoin, and the reconciliation keeps the identity. The state plane has nothing to do | §5.4, §7 |

*One combination needs a separate statement.* The settle of the attachment and the
settle of a normal message use the **same collection subscription**. After the server
consumes the item in the `attach` command, it sends that row to every client with a
`stream_insert` through PubSub. Therefore the three stages of the upload fall on three
different planes. The precheck and the transfer are on the handle (`Progress`). The
command reply is on the control plane (`attached=true`). The **result** is on the tree
(one `CollectionEdit::Inserted`). The three lines notify nothing of each other, and
this is the boundary table of §6.4 in a real program.

#### 6.5.2 gpui: the same scenario, through `musubi-gpui`

The same scenario, and the same store. There is only one difference: the `Wake`
channel above and its loop disappear completely. `musubi-gpui` absorbs the hop between
a `Send + Sync` callback and a `!Send` gpui entity (§5.1, capability 1), and it also
absorbs the translation of the collection edits into a `ListState` splice
(capability 2). What stays is the view code.

```rust
// The imports of gpui and gpui-component (`Entity`, `Context`, `Window`, `ListState`,
// `px` …) do not change, so this text omits them. `Task` no longer appears in the
// fields: the two loops outside the tree are now subscriptions.
use musubi_client::{
    generated::{AsyncState, State, StreamState, Subscription},
    MountStatus, Mounted, Upload, UploadFile, UploadHandle,
};

use generated::nav::*;
use generated::chat_room::MessageState;
use generated::chat_room::stores::chat_room_store::{
    Attach, ChatRoomStore, ChatRoomStoreLastSendStatus as SendStatus, SendMessage,
    State as ChatState,
};

struct ChatWindow {
    chat: Mounted<ChatRoomStore>,
    state: State<ChatState>,                 // the root view; it survives a reconnection
    feed: AsyncState<Vec<MessageState>>,     // the stream_async node
    rows: Option<StreamState<MessageState>>, // present only when the result is not null
    list: ListState,
    composer: Entity<InputState>,

    // The two planes outside the tree; the semantics are word for word those of today.
    status: MountStatus,                     // BDR-0033 (§5.4)
    upload: Option<Upload>,                  // the control plane
    attachment: Option<UploadHandle>,        // the most recent progress snapshot

    // There is only one type of subscription token. Therefore the observations on the
    // tree and off the tree live in the same `Vec` (§2.4). Today this part has three
    // fields: one `Vec<Subscription>` and two `Task<()>`. After the unification it has
    // one field, plus two that live and die with a node or a handle.
    _subs: Vec<Subscription>,                // the fixed ones: four on the tree + one status
    _list_driver: Option<Subscription>,      // on the tree: lives and dies with the collection node
    _upload_sub: Option<Subscription>,       // outside the tree: lives and dies with the upload handle
}

impl ChatWindow {
    fn new(chat: Mounted<ChatRoomStore>, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let state = chat.state();
        let feed = state.messages();
        let composer = cx.new(|cx| InputState::new(window, cx));

        // ── 2. Subscriptions on the tree: subscribe to what the view needs ───
        let subs = vec![
            // The name draft: subscribe to that one leaf. A change of another field
            // no longer touches the input box.
            musubi_gpui::observe_with(&state.current_user().name(), window, cx, |view, name, window, cx| {
                view.set_draft(&name.value(), window, cx);
            }),
            // 4. The receipt: another leaf. The command handler makes no UI update;
            //    this subscription makes it.
            musubi_gpui::observe(&state.last_send_status(), cx),
            // The online user count: a third one. It and the two above notify
            // nothing of each other.
            musubi_gpui::observe(&state.online_users(), cx),
            // 2. The loading state: subscribe to the async node itself.
            //    `ok <-> loading` changes only this node (§3.3). Therefore, when the
            //    list dims at a reconnection, it **redraws no row**. The same
            //    callback also reinstalls the list driver when the collection
            //    appears or disappears.
            musubi_gpui::observe_with(&feed, window, cx, |view, _feed, _window, cx| {
                view.rebind_rows(cx);
                cx.notify();
            }),
            // The connection status outside the tree: **the same `subscribe`, and
            // the same `Subscription`** (§2.4). Therefore it lives beside the four
            // subscriptions above. `musubi-gpui` depends only on `musubi-state`, and
            // it cannot reach the `StatusState` of `musubi-client` (§5.1). Therefore
            // this code uses the bare form of that hop: `to_view` only knows that a
            // `Send` value must go to the view. It knows no handle type, so a handle
            // outside the tree can also use it. This callback also installs the
            // upload subscription after `Live`: the slot name must come from the
            // tree, and when `mount` returns, the first patch has not settled
            // (§6.5.1 has the same wait).
            chat.status().subscribe(musubi_gpui::to_view(window, cx, |view, status, _window, cx| {
                view.status = status;
                view.watch_upload(cx);       // idempotent: it returns if `upload.is_some()`
                cx.notify();
            })),
        ];

        // Subscribe first, then read. At worst this order repeats one idempotent
        // assignment, and it cannot miss an edge (§5.4).
        let status = chat.status().value();

        let mut this = Self {
            chat,
            state,
            feed,
            rows: None,
            list: ListState::new(0, ListAlignment::Top, px(200.0)),
            composer,
            status,
            upload: None,
            attachment: None,
            _subs: subs,
            _list_driver: None,
            _upload_sub: None,
        };

        this.rebind_rows(cx);        // §6.3: idempotent; the criterion is the node identity
        this
    }

    // For the implementations of `rebind_rows` and `drive_list`, see §6.3; they do
    // not change by one word. The first installs and removes the driver by the node
    // identity. The second translates `&[CollectionEdit]` into `ListState::splice`.
    // For the single row renderer (`message_row`), see §6.3 as well: after
    // `rows.at(index)`, one `value()` for one row. You pay for the rows that you draw.
    //
    // `watch_upload` has one hop less than `app.rs:464` today. One
    // `self.chat.upload_at(&self.state.attachment())` gives the handle (§3.4). It no
    // longer materializes the slot to get the name and then writes `StoreId::root()`
    // to search the registry. Then it calls
    // **`subscribe(to_view(..))` before `value()`**, and writes each `UploadHandle`
    // into `self.attachment` (§6.4). The token goes into `_upload_sub`, and the
    // `Task<()>` disappears together with its loop. The order and the reason do not
    // change: this plane does not replay, so the subscription must come before the
    // read. It does not touch the state plane, and the state plane does not touch it.

    // ── 3. Send a message: the handler only sends ────────────────────────
    fn send(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let body = self.composer.read(cx).value().to_string();
        let chat = self.chat.clone();

        cx.background_spawn(async move { chat.command(SendMessage { body }).await })
            .detach();

        self.composer.update(cx, |input, cx| input.clear(window, cx));
        // This handler makes no update that relates to the state. When the receipt
        // settles, the `last_send_status` subscription above redraws the receipt row,
        // and the message row itself reaches the list through the collection
        // subscription. A rejection by the server works the same way: a failure is
        // another variant of the same node, and no other view moves.
    }

    // ── 5. The attachment: control plane on the handle, result on the tree ───
    fn attach(&mut self, path: PathBuf, cx: &mut Context<Self>) {
        // `watch_upload` installs the handle through `upload_at` after `Live` (§3.4).
        // The slot is a lazy leaf and never changes, so that step runs only once.
        let Some(upload) = self.upload.clone() else { return };
        let chat = self.chat.clone();

        cx.background_spawn(async move {
            let bytes = std::fs::read(&path)?;
            let name = path.file_name().unwrap_or_default().to_string_lossy();
            upload
                .select(vec![UploadFile::new(name, content_type(&path), bytes)])
                .await?;
            upload.start().await?;
            chat.command(Attach {}).await?;      // the server consumes the item and broadcasts that row
            anyhow::Ok(())
        })
        .detach();

        // The stream of `watch_upload` drives the progress bar (zero notifications on
        // the tree), and the collection subscription drives that row. One user
        // action, and two independent lines.
    }

    // ── 6. Reconnection: no code ──────────────────────────────
    // `Reconnecting` flips the connection indicator (the status stream outside the
    // tree). When `feed` goes back to `loading`, the view redraws only the header and
    // dims the list. The `replace ""` of the rejoin is one reconciliation: an
    // unchanged row keeps its `NodeId` and its row height cache, and zero rows are
    // redrawn (§3.3, §7). This section has no code, and that absence is the
    // conclusion itself.
}
```

**The planes that this code shows:**

| Plane | Location in the code | Difference from §6.5.1 | See |
|---|---|---|---|
| **command** | The `cx.background_spawn` in `send()` / `attach()` | No difference. A command is on the control plane, and it does not depend on the UI framework | §6.1 |
| **event** | Absent (this store does not declare it) | On the gpui side it is one `cx.spawn` loop; for the shape, see §6.2 | §6.2 |
| **stream** | `rebind_rows` + `drive_list` | The hand-written edit loop disappears, and one `drive_list` replaces it. The output is `ListState::splice`, not `println!` | §6.3 |
| **upload** | `attach()` + `watch_upload()` | The step that gets the handle is the same in the two examples (`upload_at`, §3.4). The progress plane is already outside the tree, and this example takes the callback form (`subscribe` + `to_view`) instead of the stream form of §6.5.1, because the view must put the token into a field, and the headless program already runs an async task | §3.4, §6.4 |
| The state plane | Four `observe*` calls plus one `status().subscribe(to_view(..))` | The `Wake` channel and its loop disappear completely, because the hop is in the adapter | §5.1 |
| The two handles outside the tree | They live in the same `_subs` beside the four subscriptions on the tree | This is the most direct gain of the §2.4 unification on the gpui side: three fields become one, and no `Task<()>` is left | §2.4 |
| Reconnection | No code | The same in the two examples: the tree survives the rejoin, and the state plane has nothing to do | §5.4, §7 |

**The difference between the two examples, in one sentence: `musubi-gpui` absorbs
exactly two things — the thread hop for each subscription, and the translation of
`&[CollectionEdit]` into a list splice.** The `Wake` channel and the `match` loop of
§6.5.1 are the hop boilerplate that you must write yourself without the adapter. That
boilerplate is reasonable in a headless program, because a channel is the shape that
the program needs; in gpui you must write it again for each field of each view, and
that is where §5.1 makes its case. In every other respect the two examples correspond
line by line: the same node subscriptions, the same identity criterion in
`rebind_rows`, the same "send the command and stop there", the same empty reconnection
arm, and the same single wait for `Live` before a read of the tree.

**The third point that the two examples share comes from §2.4:** the two handles
outside the tree are no longer a separate mechanism. In the headless example they live
in the same `Vec<Subscription>` as the three node subscriptions (2d), and the gpui
example does the same. The only part that differs by the shape of the consumer is the
choice between the **callback form and the stream form**. Use `subscribe` to put the
observation into a struct. Use `into_stream` to `await` a condition in an async block.
This choice does not depend on the position of the handle relative to the tree, and
that is the correct result of the unification.

---

## 7. The parts that survive unchanged

| Area | Files | Why it is not affected |
|---|---|---|
| The connection actor, the single FIFO inbox, the total order, the head-of-queue rule | `src/actor.rs`, `docs/rust-client.md` §2.4 | The tree replaces *what the actor does with an envelope*, not how the envelope arrives at it. The only handler change is `patch()`/`publish()` (§3.6). |
| The four seams — `Connector`, `Socket`, `Spawner`, `Timer` | `crates/phoenix-channel`, `crates/musubi-client-tokio` | There is no new runtime requirement. `musubi-state` has no async surface at all. |
| The Phoenix channel protocol, framing, heartbeats, join and push timeouts, socket-level reconnect | `crates/phoenix-channel/*` | It is completely below the data plane. |
| Mount, the reference-counted alias, the hold return on unmount, `Drop` as unmount | `src/actor.rs`, `src/mounted.rs`, `docs/rust-client.md` §7 | The lifetime of `Mounted` does not change. The design only removes two readers. |
| Reconnect and recovery (BDR-0015), `soft_reset`, version discipline | `src/actor.rs`, `src/engine.rs`, `docs/rust-client.md` §9 | `soft_reset` still only forgets the version and keeps the tree. This is exactly what keeps the last good render alive through a rejoin. The `replace ""` of a rejoin now *reconciles* the tree instead of replacing a `Value`. This is a strict improvement, because an unchanged subtree keeps its identity and notifies nobody. |
| Commands, `command_on`, reply typing, the "replies are not patch-gated" contract | `src/mounted.rs`, `docs/rust-client.md` §6.2 | No change. `StoreState::store_id()` replaces `snapshot.panel.store_id` as the way to name the target. For the combined usage see §6.1. |
| Push events (BDR-0032), the unbounded queue of `events()`, the dispatch by `(store_id, name)` | `src/mounted.rs` | No change, including the "after the state publish" order (§3.6, step 11). For the relation to node subscriptions see §6.2. |
| Uploads and their two planes: the `upload_ops` fold, the `UploadHandle` value type, preflight, chunked binary transfer, the external `Uploader`, `select`/`start`, the `Mounted::upload(&store_id, name)` primitive | `src/uploads/*`, `docs/rust-client.md` §10 | The upload slot is a lazy leaf (§3.4). There is only one semantic change: the pruning reads `tree.store_ids()` instead of the index. The three actions on the handle use the uniform convention of §2.4 (`value()`/`subscribe()`, and `into_stream()` for the stream form). `Mounted::upload_at(&slot)` is a **shorter entry point** to the same registry lookup, not a second mechanism (§3.4). For the boundary between the two planes see §6.4. |
| Every semantic of `MountStatus`, the `Latest`/`Updates` cell, BDR-0033 | `src/latest.rs`, `src/mounted.rs` | The values, the cell, the edge semantics, the replay on the first poll, and the permanent `Connecting` after `disconnect()` are all set by rules outside this design. The path that reaches it is a property: `status() -> StatusState` (§2.4, §5.4). |
| The mount cache (stale-while-revalidate), `CacheStore`, `CacheEntry`, `cache_key`, GC, write throttling | `src/cache.rs`, `src/cache_coordinator.rs`, `docs/rust-client.md` §6.4 | **`CacheEntry::data` is the wire tree, and it stays the wire tree.** The write now passes `tree.to_wire(root)` where it passed `engine.document()` before. This replaces the `Value` clone that `on_publish` already made. The seed passes the cached `Value` into `PatchEngine::seed`, which now **builds a retained tree from it** instead of treating it as a shadow document. A seeded stream slot still renders as `[]` until a live envelope fills it again, because the cached tree holds no `stream_ops`. A seed that fails validation is still discarded, it still leaves a cold mount, and a seeded root still does not enter `Live`. |
| The wire fixtures and the capture task | `test/support/wire_capture/*`, `crates/musubi-client/tests/fixtures/*.json` | **This design does not affect the fixture files.** The replay is the gate for the equivalence of the tree and the wire representation. See §8. |
| The error taxonomy | `src/error.rs`, §11 | `TreeError` maps onto the existing variants (§2.3). `MusubiError::Decode` keeps its meaning and its trigger conditions. It only moves from step 6 of the cycle to step 4. |
| The builder, the configuration keys, the `mix compile.musubi_rust` compiler contract, the module tree, hoisting, naming | `src/connection.rs`, `lib/musubi/codegen/*`, `docs/rust-codegen.md` §1–§3.6, §4.1–§4.4, §4.6–§4.7 | §4.1. |
| The Elixir server, the TypeScript client, `@musubi/react` | Everything under `lib/` and `packages/` | The wire contract does not change. |

---

## 8. The wire fixture: the gate for the equivalence of the tree and the wire representation

The replay of the 21 wire fixtures (`crates/musubi-client/tests/fixtures.rs`) is the
external acceptance gate of this design. It deserves a precise description, because it
is the only place where the client computation is compared against what the server
wrote down.

- **The server side produces the JSON files of the fixtures.** `expected_state` is the
  server's own wire root (`Musubi.Page.Server.State.previous_wire_root`), written by
  the Elixir test suite. `mix musubi.capture_wire` + `git add --intent-to-add` +
  `git diff --exit-code` is the drift gate on the capture side. The client never
  writes them.
- **The comparison uses the hydrated form.** The replay feeds the envelopes into the
  tree one by one, then compares `mounted.state().value::<Value>()` against
  `hydrated(fixture)`. The stores of the fixture declare
  `St::State = serde_json::Value` (the `fixture_stores!` macro), so `value()` is a
  total function there. There is no generated struct, no drift layering, and no panic
  path.
- **It is not circular.** That document belongs to the server, and the client must
  compute it only from what the fixture delivers. To compute it therefore also proves
  that `to_hydrated` reproduces the hydration semantics of the server exactly. The
  `to_wire` projection, which the mount cache write uses (§7), is pinned by the test
  cases of the cache round.
- **The frame-by-frame assertions do not touch the state plane.** The outbound frames,
  the command replies, the event stream, and the check for no trailing frame do not
  pass through the tree.

## 9. Semantics appendix

This is the contract. Everything above either follows from it, or is only a wiring
detail.

### 9.1 Equivalence

A node is **changed** when its semantic value changes. The semantic value is defined
recursively:

| Node kind | Condition for semantic equivalence |
|---|---|
| `Null` | Always equivalent |
| `Bool` / `Number` / `String` | Scalar `==` |
| `Object` | The key **sets** are equal **and** each child node with the same key is semantically equivalent. The *order* of the keys is not part of the value. |
| `Store` | The `store_id` values are equal **and** the fields are equal by the `Object` rule |
| `Array` | The lengths are equal **and** each child node **at the same index** is semantically equivalent (handoff §19: index identity; the general runtime infers no business identity for a plain JSON array) |
| `Collection` | The ordered sequences of `(item_key, item_semantic)` are equal. Therefore a reorder without any item change **is also** a change (§3.1) |
| `Async` | The `status` values are equal **and** the `result` values are equal **and** the `reason` values are equal |
| `UploadSlot` | The names are equal **and** the `owner` values are equal (the node creation fixes both, so in practice this row is always equivalent, §3.4) |
| Across kinds | Never equivalent |

Propagation: a changed child node makes its parent node changed, and this continues up
to the root. An untouched sibling node gets no notification. The definition is
recursive, but the implementation is incremental. It recomputes only the dirty paths
and their ancestors, and it never does a full-tree DFS. Each unchanged child node
contributes the same `Arc` that it already holds, so the comparison in the parent node
stops at pointer equality.

**Decision (owner) — a plain array keeps the index identity: process the data exactly
as the backend sends it.** The identity of `NodeKind::Array` **is** the index. The
client does no positional diff. It does not infer that an item is an earlier item that
moved. It also invents no business identity for a collection without keys. Therefore
one `add /list/0` does change the semantic value at every later index, and it does
notify all of them. This is not an over-notification that you must remove. It is the
definition of the index identity. There are three reasons:

1. **The server has already made the complete statement.** Every op is the product of a
   server render diff. If the client rewrites "two ops" into "one move", it overrides
   the statement of the server with a heuristic. When the heuristic guesses wrong — two
   elements with equal values, a true full reorder, or a rewrite that removes and then
   inserts — nothing can correct it. The error shows as an identity mismatch, where a
   subscription follows the wrong row. That is much worse than an over-notification.
2. **A collection that needs a key identity already has a key.** A `stream` has an
   `item_key`. A child store has a `store_id` (§3.1, §3.2). The remaining collections
   without a key are exactly the collections for which the server declared **no**
   identity. For them the index is the only identity that exists.
3. **A keyless positional diff has no second caller and no unique answer.** It must
   select at least one of these: the longest common subsequence, a full rebuild from
   the first point of difference, or a pairing by value hash. Each of them behaves
   worse on some class of real payload. AGENTS.md forbids an abstraction without a
   second caller. Here there is not even a first caller.

This decision cancels the earlier open question, "first survey the real envelopes, then
decide whether to do a positional diff". The result of the survey will not change the
answer. If a page later splices a large list by position, and a profile names it as a
hot spot, the correct fix is still not to guess the identity in the general runtime.
The correct fix is to let the server declare `stream` for that field. That tool already
exists in Musubi for this purpose, and it gives a true identity, not a guess.

**Amendment — `add` / `remove` are structural ops. They move the node, not the value.**
The decision above applies unchanged to **equivalence** (an `Array` compares at the
same index) and to a **whole-list `replace`**. Position *k* is what the server puts at
position *k*, and `reconcile_array` implements this literally. But `add /list/i` is not
the same statement. RFC 6902 defines it as an insertion, and the server diff has
already stated that one more element is present here. Therefore a shift of the whole
tail by one position **repeats that statement**. It does not guess a move from two ops,
and reason 1 says exactly that you must not guess. Therefore, after an `add` or a
`remove`: the array node itself changes and notifies, because its semantic value is the
ordered sequence of the child semantic values. An element that only moved position does
not change and does not notify, and it keeps its own `NodeId`, its subtree, and its
subscribers.

The earlier implementation rewrote the **values** of the tail one by one, and each
position reconciled the value of its predecessor. There are two hard reasons to replace
it, and neither is a question of style:

1. **It loses data, and it does so silently.** The wire projection of a `Collection` is
   only the bare marker. The stream content travels only in `stream_ops` and never
   enters the value (§3.1). Therefore, after `semantic_deep().to_wire()` shifts a
   stream slot by one position, the result is an **empty** collection, and the
   collection index still points at this empty one.
2. **Its cost is two deep copies of the tail per op**, and it holds the arena lock for
   the whole time. In release mode, 50 ops in a 2.1 KB envelope applied to an array of
   20 000 elements block the client for 1.99 seconds.

For the index identity to hold, the node at an index must take the value of its
predecessor. That necessarily gives O(tail) deep copies of a subtree, and there is no
cheap method. A child store already moves the node instead of rewriting it (the
adoption in §3.2). Therefore the move of the node also makes every kind of element in
an array behave the same.

### 9.2 Transactions

- **One server message is one transaction.** Record the semantic value of a node when
  the transaction first touches it. Apply every op. Settle the dirty set from the
  bottom up. Compare the recorded value with the final value. Build the `ChangeSet`.
  **Then** notify.
- **A `1 -> 2 -> 1` inside one transaction is not a change.** The comparison uses the
  value recorded at the first touch, not the previous intermediate value. Therefore a
  field that an envelope changes and changes back notifies nobody, and it does not
  advance the revision. This is also true for several `Transaction::apply` calls that
  share the same transaction.
- **The ops apply from left to right**, and `ops` comes before `stream_ops` (§3.6).
- **The journal gives the atomicity.** Any failure rolls back every change, including
  the nodes that the transaction allocated, and it keeps the revision and the semantic
  value exactly as they were. A panic in the middle of a transaction unwinds through
  the same rollback path.
- **There is no notification per op.** Notification happens per transaction, never per
  op (handoff §32).

### 9.3 Revision and notification

- Each node has one `revision: u64`. It starts at `0`, and **only** a transaction that
  truly changes the semantic value of that node increments it. A node that is touched
  and then restored keeps its revision.
- `revision() == 0` means that no transaction has ever touched this node. For a root
  this is exactly the state where nothing has landed yet (§5.3).
- `Change { revision }` is everything that the subscriber is told. There is no clone of
  the old value or the new value. The callback reads again through its own `State<T>`
  (handoff §24).
- A subscriber registered on node `N` is called when `N` appears in the `ChangeSet`.
  That is, when the value of `N` itself changed, or when the value of one of its
  descendants changed.
- The subscribers are collected under the tree lock and called after the lock is
  released, once per transaction, in an unspecified order. A callback can still be
  called one more time after its `Subscription` is dropped (§2.5).
- A **removed** node appears in the `ChangeSet`, is notified once, and is then
  released. After that a `State<T>` that points at it reads `is_live() == false`.

### 9.4 Worked example — §31 of the handoff

The tree: `{ count: 1, items: [ { name: "foo" } ] }`. The subscribers:

| | Subscribed to |
|---|---|
| A | `count` |
| B | `items` |
| C | `items[0]` |
| D | `items[0].name` |
| E | root |

The envelope: `[{"op":"replace","path":"/items/0/name","value":"bar"}]`.

1. Resolve `/items/0/name` to the node of D. Record its old semantic value (`"foo"`),
   set it to `NodeKind::String("bar")`, and mark it dirty.
2. Settle: D = `"bar"`. C recomputes to `{name: "bar"}`, one item, with a new `Arc` for
   the changed child node. B recomputes to a sequence of one element. The root
   recomputes to `{count: <old Arc>, items: <new Arc>}`. The child node `count`
   contributes the same `Arc`, so nothing below it is examined.
3. Compare: D changed, C changed, B changed, and the root changed. The node `count`
   never became dirty, and it is not in the ancestor set, so it was never compared.
4. `ChangeSet::changed()` = `[D, C, B, root]`, with the child node before the parent
   node.

**Notified: D, C, B, E. Not notified: A.**

### 9.5 Worked example — one `stream_op` insert (specific to Musubi)

The tree, for the root of the chat store (`store_id: []`):

```
root
├── title            : String("Inbox")
├── current_user     : Object { name: String("me") }
└── feed             : Object
    └── messages     : Collection { name: "messages", owner: [], items: [
                          ("msg-2", N2), ("msg-1", N1)
                       ] }
```

The subscribers:

| | Subscribed to |
|---|---|
| A | `title` (a sibling field) |
| B | `feed` |
| C | `feed.messages` (the collection itself) |
| D | the item node `N1` of `msg-1` |
| E | root |

The envelope: `ops: []`, and

```json
"stream_ops": [
  {"op":"insert","stream":"messages","ref":"1","store_id":[],
   "item_key":"msg-3","at":0,"item":{"id":"3","body":"hi"},"limit":-100}
]
```

1. `ops` is empty, so nothing is addressed by a pointer. `(store_id: [], "messages")`
   resolves to that `Collection` node through the store map. It does **not** go through
   a JSON pointer, because no pointer can address a stream item (§3.1).
2. No item with `item_key: "msg-3"` exists, so nothing is removed. The length after the
   removal is 2, and `at: 0` means a prepend. `limit: -100` ⇒ `size = 100`,
   `len = 3 <= 100`, so there is no trim.
3. Create the new item node `N3` from `{"id":"3","body":"hi"}`. The item list becomes
   `[("msg-3", N3), ("msg-2", N2), ("msg-1", N1)]`. `N1` and `N2` do not move at all.
   They keep the same `NodeId`, the same semantic `Arc`, and the same subscribers.
4. Settle: the semantic value of the collection is that ordered
   `(item_key, item_semantic)` sequence, and it now has a new first element, so it
   changed. When `feed` recomputes, it takes a new `Arc` for `messages`, and no other
   field can contribute an unchanged `Arc`, because `feed` has only one field. When the
   root recomputes, `title` and `current_user` both contribute an unchanged `Arc`.
5. Compare: the collection changed, `feed` changed, and the root changed. The semantic
   values of `N1` and `N2` did not change, so their revisions do not move. Only their
   *index* moved, and the index belongs to the collection, not to them.

**Notified: C, B, E. Not notified: A (a sibling field) and D (an item whose own value
did not change).**

The `ChangeSet` also carries this edit, so the list adapter never does a diff:

```rust
change_set.collection_edits(messages_node)
// [ CollectionEdit::Inserted { item_key: "msg-3", index: 0, node: N3 } ]
```

Two variants on the same tree, for completeness:

- **A pure reorder** — an `insert` at `at: 0` for the key `"msg-1"` that *already
  exists*. The upsert removes the item and inserts it again, but it **reuses `N1`**
  (§3.1) and reconciles the item value into it. If the value is exactly the same, `N1`
  does not change. The ordered sequence of the collection does change, so C, B, and E
  are notified, and D is **not** notified. This edit is
  `CollectionEdit::Moved { item_key: "msg-1", from: 1, to: 0 }`.
- **A `limit` trim** — an append beyond `limit: -100` drops the item at the head. The
  node of the dropped item is released. Its subscribers are notified once, and after
  that their `State<MessageState>` reads `is_live() == false`. This edit is
  `CollectionEdit::Removed { item_key, index: 0 }`, and the list adapter turns it into
  a splice of one row and a discarded row view.

---

## 10. Open questions

Only two remain. The other two that were listed here now have answers. The identity of
a plain array is a semantic that the owner decided (§9.1). The public surface of
`PatchEngine` is a narrowing that the owner decided (§5.5).

### 10.1 The implementation path of `value()` (the preference is set; a profile review remains)

*This section discusses only the implementation path. The question "can we remove that
read function and access the property directly" is not an open question. §2.4 answers
it directly, in three subsections. "The uniform convention: a property is a handle"
gives the terms (handle / value / subscription / stream form) and shows that **a
property access is already `x.prop()`**. The whole API surface follows this rule (the
five views on the tree, `StatusState`, `Upload`), with no second method name and no
verb of its own for each surface. "Why the method is called `value()`, not `get()`, and
certainly not `handler()`" answers the note of the owner about the naming. "Why a read
is written as `value()` instead of a direct property access" explains why the remaining
step, materialization, can only be a method call in Rust. There are no computed
properties, three approximation paths were rejected, and the subsection explains why
even the `Display`/`PartialEq` sugar is not added.*

One call, two implementation paths:

```rust
let item: Item = state.items().at(3).unwrap().value();
```

**(a) `to_hydrated` + `serde_json::from_value` — about ten lines.**

```rust
impl<T: DeserializeOwned> State<T> {
    pub fn try_value(&self) -> Result<T, ReadError> {
        // Traversal ①: node subtree -> serde_json::Value (the lock is held here, then released)
        let hydrated = self.tree.to_hydrated(self.node).ok_or(ReadError::Gone)?;
        // Traversal ②: Value -> T (no lock)
        serde_json::from_value(hydrated).map_err(ReadError::Shape)
    }
}
```

```
node subtree ──traversal ①──▶ serde_json::Value ──traversal ②──▶ Item
                              (one complete intermediate    (the whole intermediate
                               tree, one heap allocation     tree is released at once)
                               per container, one copy
                               per string)
```

**(b) A `Deserializer` backed by the node — about 300 lines.**

```rust
struct NodeDeserializer<'a> {
    tree: &'a StateTreeInner,   // the lock is already held
    node: NodeId,
}

impl<'de, 'a> serde::Deserializer<'de> for NodeDeserializer<'a> {
    type Error = ReadError;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ReadError> {
        match self.kind() {
            NodeKind::Null => visitor.visit_unit(),
            NodeKind::Bool(b) => visitor.visit_bool(b),
            NodeKind::Number(n) => /* three branches: i64 / u64 / f64 */,
            NodeKind::String(s) => visitor.visit_str(&s),          // borrow the Arc<str> directly
            NodeKind::Array(children) => visitor.visit_seq(NodeSeq::new(self.tree, children)),
            NodeKind::Object(fields) => visitor.visit_map(NodeMap::new(self.tree, fields)),
            NodeKind::Store { fields, store_id } => /* synthesize the __musubi_store_id__ key */,
            NodeKind::Collection { items, .. } => visitor.visit_seq(/* only the item nodes */),
            NodeKind::Async { .. } => /* synthesize a map with three keys */,
            // `owner` is not in the projection: it is one half of the upload key that the
            // client resolves locally (§2.1, §3.4). The wire has no `owner`, and neither
            // `to_wire` nor `to_hydrated` writes it.
            NodeKind::UploadSlot { name, .. } => /* synthesize a marker map */,
        }
    }

    // Plus hand-written branches for option / enum / newtype_struct / ignored_any,
    // and forward_to_deserialize_any! for the rest.
}

impl<T: DeserializeOwned> State<T> {
    pub fn try_value(&self) -> Result<T, ReadError> {
        T::deserialize(NodeDeserializer { .. })    // traversal ①, the only one
    }
}
```

```
node subtree ──traversal ①──▶ Item
              (no intermediate Value, no intermediate allocation)
```

The quantified difference:

| | (a) `to_hydrated` + `from_value` | (b) node-backed `Deserializer` |
|---|---|---|
| Subtree traversals | 2 | 1 |
| Intermediate heap allocations | One per container (`Map`/`Vec`), one per string | 0 |
| String copies | 2 (`Arc<str>` → `String` → `T`) | 1 (`visit_str` → `T`; 0 when `T` borrows) |
| Amount of new code | ~10 lines | ~300 lines: one branch per `NodeKind`, `SeqAccess`/`MapAccess`, the `Option`/enum/newtype special cases, four synthesized forms, the context on the error paths, and the tests that compare bit for bit against (a) |
| Error diagnostics | `serde_json::Error` carries the path, the line, and the column | You must maintain the path context yourself to get the same quality |
| Lock | Held only across traversal ① | **Held across the whole `T::deserialize`**. This becomes the second place in §2.6 that runs caller code under the lock, because a `Deserialize` impl can be hand-written. It needs a separate argument |

At what scale it becomes perceptible (estimated from the shape of the chat example,
with four fields in `MessageState`):

| What you read | Allocations of (a) | Is it perceptible |
|---|---|---|
| One leaf (`State<String>::value()`) | 2 | Not measurable. It is the same order as one `String` clone |
| One row (`State<MessageState>::value()`) | ~6 | 60 frames per second × 10 rows in the viewport ≈ 3600 small allocations per second. It is measurable, but against the layout and the paint of one gpui frame it is still noise |
| A whole list (`StreamState::value()`, 100 rows) | ~600 | It hurts in a render loop. But the **first answer is not (b). The first answer is to not read this way.** Use `at(index)` / `by_key()` to read only the rows that you draw (§6.3) |
| A whole root (`state().value()`) | ~900 | This is exactly what v1 did for every envelope, and what this design removes. Keep it for the fixture replay (once per fixture) and for the whole-value assertion of `try_value()` |

**The preference: land (a) first.** There are three reasons. (1) (b) is a **purely
internal replacement**. The signature of `try_value`, `ReadError`, and every call site
stay the same. Therefore it can land at any time, no caller must change, there is no
semver consequence, and a delay creates no debt. (2) The cost of (a) correlates
strongly with a read that is too coarse, and a read that is too coarse has a cheaper
fix: read per node. If (b) lands first, it hides this more important correction. (3)
`docs/rust-client.md` §4.6 already delayed this pipeline once, and the reason of that
time still holds today.

**The trigger condition for the review, written so that you can decide it:** a profile
shows `serde_json::from_value` among the top entries of the render loop of a **real**
consumer, **and** that consumer already reads per node. That is, the cost of (a) is not
a disguise for the easier problem of a read that is too coarse. If both conditions
hold, land (b). If only the first holds, fix the read method first.

### 10.2 Two contact points between `musubi-gpui` and gpui that are not yet verified

The notification hop needs the cross-thread entity update path of gpui 0.2.2
(`AsyncApp` / `WeakEntity::update`). The list driver (§6.3) needs some incremental
`ListState` update that 0.2.2 exposes in addition to `reset(count)`. This document made
both of these capability assertions without the source code of that crate.

**Both are now verified. One conclusion is correct, and one deviates.**

- **`ListState::splice` exists.** The list driver is therefore incremental, and
  capability (2) of §5.1 is a present argument, not a forward-looking one. The `reset`
  fallback path survives only as the `#[non_exhaustive]` branch, which
  `CollectionEdit::Reset` needs anyway.
- **The cross-thread entity update path exists, but its shape is not the one that this
  document drew.** `AsyncApp` is `!Send`, so the hop lands as one channel plus a
  foreground drain task. Therefore `to_view` and `observe_with` each take one more
  `&Window`. The behavior, the order, and the RAII lifetime do not change. For the full
  account see "the two deviations" in §5.1.
