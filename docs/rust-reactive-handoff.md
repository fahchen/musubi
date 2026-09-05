# Musubi Rust Reactive State Client — Handoff Design (user-authored, verbatim)

## 1. Goal
Rust-native, typed, reactive client state runtime for Musubi.
Constraints: server protocol unchanged (existing JSON Patch); client applies patches; client state is a retained reactive state tree, not a plain serde_json::Value; any subtree is independently usable as reactive state; per-node subscription; RAII subscription tokens (drop = unsubscribe); parent change derived recursively from children semantic equality; GPUI is only an adapter (core has no GPUI dependency); NO React hooks / Signal graph / thread-local dependency tracking / VDOM.

## 2. Core mental model
JSON Patch -> retained state tree -> recursive semantic equality -> changed nodes -> node subscribers -> UI callback. Patch is only input; notification is decided by semantic equality of node values before/after the patch TRANSACTION.

## 3-4. State tree + NodeKind
Long-lived retained tree; stable client-local NodeId per node.
    pub struct StateTree { nodes: SlotMap<NodeId, Node>, root: NodeId }
    pub struct Node { parent: Option<NodeId>, kind: NodeKind, revision: u64, semantic: SemanticValue, subscribers: Subscribers }
    pub enum NodeKind { Null, Bool(bool), Number(serde_json::Number), String(Arc<str>), Array(Vec<NodeId>), Object(HashMap<Arc<str>, NodeId>) }
Never re-deserialize the whole tree after a patch.

## 5-7. State<T> is the public abstraction
    pub struct State<T> { tree: Arc<StateTreeInner>, node: NodeId, _marker: PhantomData<fn() -> T> }
"A typed reactive view rooted at one node of the shared retained state tree." State<AppState>, State<Vec<Item>>, State<Item>, State<String> are the same thing, differing only in typed navigation. Any subtree is a full reactive state: .get(), .subscribe(...), .revision(); passable to components that know nothing about the root. Navigation stays reactive: state.items() -> State<Vec<Item>> (not Vec<Item>); items.first() -> Option<State<Item>>; item.name() -> State<String>. Only .get() materializes a detached (non-reactive) snapshot T.

## 8. Typed facade generated over generic JSON tree
Runtime implements only the generic tree; codegen generates typed navigation:
    impl State<AppState> { pub fn count(&self) -> State<i64>; pub fn items(&self) -> State<Vec<Item>>; }
    impl State<Item> { pub fn name(&self) -> State<String>; }
    impl<T> State<Vec<T>> { pub fn len(&self) -> usize; pub fn get(&self, index: usize) -> Option<State<T>>; pub fn first(&self) -> Option<State<T>>; pub fn iter(&self) -> impl Iterator<Item = State<T>>; }
No string JSON paths in user code.

## 9-11. Subscription
    impl<T> State<T> { pub fn subscribe(&self, callback: impl Fn(Change) + Send + Sync + 'static) -> Subscription; }
Subscription is RAII: { tree: Weak<StateTreeInner>, node: NodeId, subscriber: SubscriberId }; Drop unsubscribes. get() must NOT implicitly subscribe (no thread-local current-component tracking); reactive dependency is explicit via subscribe().

## 12-14. Equality semantics
Node changed = semantic value changed. Scalar: old == new. Object: key set equal AND every child semantically equal. Array: length equal AND every child at same index semantically equal. child changed -> parent changed -> ancestors changed; untouched siblings not notified. Semantically recursive, operationally incremental: only recompute dirty path + ancestors, never full-tree DFS. Per-node SemanticValue cache with structural sharing (unchanged child reuses old Arc) so ancestor equality is fast.

## 15. NodeId != JSON path
JSON Pointer = patch input location; NodeId = retained client identity. State<T> binds NodeId, not path; never re-interprets "items/0" on each use.

## 16-17. Patch applies to the retained tree; replace must reconcile
    impl StateTree { pub fn apply(&mut self, patch: &json_patch::Patch) -> Result<ChangeSet, PatchError>; }
Find retained node, reconcile/mutate, mark dirty, propagate dirty ancestors. A replace (incl. root replace) must recursively reconcile, preserving NodeIds of semantically-unchanged children, NOT destroy+recreate+notify-everyone.

## 18-19. Object / Array reconcile
Object: same key -> recurse; new key -> create; removed key -> remove; key ordering must not affect equality (BTreeMap / sorted keys). Array (generic JSON): index identity — do not infer business identity in the generic runtime; a future specialized KEYED collection reconciliation (e.g. Musubi child stores with stable store_id) is a separate capability layer.

## 20-23. Transactional apply, dirty propagation, ChangeSet, revision
Whole server message = one transaction: remember old semantics, apply all ops, recompute dirty bottom-up, compare old vs final, build ChangeSet, THEN notify. count 1 -> 2 -> 1 within one transaction = NO notification. ChangeSet = { changed: Vec<NodeId> } (only final semantic changes). Per-node revision: u64, incremented only when a transaction truly changes the semantic value; state.revision() public.

## 24-25. Notification
    pub struct Change { pub revision: u64 }
No old/new value clones by default; callback re-reads via state.get(). NEVER invoke subscribers while holding the tree lock (reentrant deadlock via state.get() in callback): lock -> apply -> collect callbacks -> unlock -> invoke.

## 26-28. GPUI integration
Core independent of GPUI. A view holds any subtree + Subscription; callback schedules entity update then cx.notify(). Fine-grained composition: AppView(State<AppState>) -> ItemsView(State<Vec<Item>>) -> ItemView(State<Item>) -> NameView(State<String>); each notified only when its subtree semantically changes. KEY PROPERTY: the tree structure IS the dependency graph — no SignalId/ConsumerId/edges/thread-local current effect.

## 29. Suggested crate layout
musubi-protocol (wire models) / musubi-state (StateTree, Node, NodeId, State<T>, Subscription, ChangeSet, semantic equality, patch reconciliation; no network, no UI) / musubi-client (websocket, phoenix channel, mount/reconnect, receives patches, calls StateTree::apply, commands/events, cache) / musubi-gpui (State<T> -> GPUI subscription/cx.notify; very thin) / musubi-codegen (typed state structs, State<T> navigation impls, typed commands/events).

## 30-31. Minimal intended API + example patch behavior
let state: State<AppState> = store.state(); state.count().get(); state.items(); state.items().first().unwrap().name(); subscriptions per node; drop(subscription) auto-unsubscribes.
Patch replace /items/0/name foo->bar with subscribers A(count) B(items) C(items[0]) D(name) E(root): notify D, C, B, E; NOT A.

## 32. Explicit non-goals
React hooks; VDOM; Signal graph; automatic render dependency tracking; thread-local current subscriber; string JSON path as public API; server protocol changes; GPUI dependency in core; whole-state deserialize after every patch; notification after each individual patch op.

## 33. Core design principle
"A retained typed JSON state tree where every node is independently observable, every subtree is itself a first-class reactive state, and parent change is recursively derived from child semantic equality."
State<T> = Arc<StateTree> + NodeId + Type.
Flow: Musubi JSON Patch -> transactional retained-tree reconciliation -> recursive semantic equality -> ChangeSet<NodeId> -> node subscribers -> RAII-managed callbacks -> GPUI / other consumers.

Next step per author: define the five interfaces first — StateTree / Node / State<T> / Subscription / apply() — since they determine whether codegen and the GPUI adapter stay simple.
