//! `State<T>` and the four navigation views (`docs/rust-reactive-state.md`
//! §2.4).
//!
//! # The four words
//!
//! | Term | What it is | Handed out by |
//! |---|---|---|
//! | **handle** | a property's incarnation on the client: it has identity, it can be stored in a struct, passed to a component that knows nothing about the root, and subscribed to | `x.prop()` |
//! | **value** | one detached, non-reactive snapshot: plain Rust data with no tie to the tree | `handle.value()` |
//! | **subscription** | one live observation, RAII; dropping it unsubscribes | `handle.subscribe(cb)` |
//! | **stream form** | the same subscription in `await` shape | `handle.into_stream()`, and **only** on the two handles outside the tree |
//!
//! Nothing here has an `into_stream`: this crate has no async surface, and
//! conjuring a stream per node would mean either an unbounded queue per node or
//! one materialization per node per envelope. A consumer that wants a `Future`
//! or a `Stream` wires one itself, in the ten lines a `oneshot` takes.

use std::marker::PhantomData;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use slotmap::Key as _;

use crate::change::{Change, CollectionEdit};
use crate::error::ReadError;
use crate::node::{AsyncStatus, NodeId, NodeKind};
use crate::subscription::Subscription;
use crate::tree::StateTree;
use crate::wire::{AsyncError, AsyncResult, StoreField, StoreId, UploadSlot};

/// A typed reactive view rooted at one node of a shared retained tree.
///
/// `State<AppState>`, `State<Vec<Item>>`, `State<Item>` and `State<String>` are
/// the same thing; they differ only in typed navigation. Any subtree is a full
/// reactive state — `value()`, `subscribe()`, `revision()` — and is passable to
/// a component that knows nothing about the root.
///
/// `PhantomData<fn() -> T>` makes `State<T>` `Send + Sync` for **every** `T`,
/// `!Send` ones included, and covariant in `T`. That is load-bearing: it is
/// what lets a `State<Item>` cross to the UI thread without `Item: Send`.
pub struct State<T> {
    tree: StateTree,
    node: NodeId,
    _marker: PhantomData<fn() -> T>,
}

impl<T> Clone for State<T> {
    /// Hand-written: `T: Clone` is not required, and must not be.
    fn clone(&self) -> Self {
        Self {
            tree: self.tree.clone(),
            node: self.node,
            _marker: PhantomData,
        }
    }
}

impl<T> std::fmt::Debug for State<T> {
    /// Prints the view's **identity**, never its value: no materialization, no
    /// lock held across a formatter, no chance of a panic in a log line.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("State")
            .field("node", &self.node)
            .field("revision", &self.revision())
            .field("live", &self.is_live())
            .finish()
    }
}

impl<T> State<T> {
    /// Binds a view to one node of one tree.
    pub(crate) fn new(tree: StateTree, node: NodeId) -> Self {
        Self {
            tree,
            node,
            _marker: PhantomData,
        }
    }

    /// The node this view is rooted at.
    pub fn node(&self) -> NodeId {
        self.node
    }

    /// The tree it belongs to.
    pub fn tree(&self) -> &StateTree {
        &self.tree
    }

    /// The node's revision. `0` means no transaction has ever touched it —
    /// which for a root is exactly "the initial patch has not landed".
    pub fn revision(&self) -> u64 {
        self.tree.node(self.node).map_or(0, |node| node.revision)
    }

    /// Whether the node is still in an open tree. `false` once the node was
    /// removed, or once the tree was closed by teardown.
    pub fn is_live(&self) -> bool {
        !self.tree.is_closed() && self.tree.node(self.node).is_some()
    }

    /// Re-type this view in place. The escape hatch codegen and hand-written
    /// navigation both use; no data moves.
    pub fn cast<U>(&self) -> State<U> {
        State::new(self.tree.clone(), self.node)
    }

    /// The child at `key` — the primitive every generated field accessor is
    /// built from, and **infallible**, as §2.4's handle law requires: `x.prop()`
    /// costs nothing, reads no value and cannot fail.
    ///
    /// A key the node does not hold — because the root is still `Null` before
    /// the first patch, because teardown emptied it, or because this node is not
    /// a container at all — yields a handle rooted at a null [`NodeId`], which
    /// is a slot no node can ever occupy. That handle reads `is_live() == false`
    /// and `try_value() == Err(ReadError::Gone)`, so the checked reads stay the
    /// checked reads and navigation never panics on the way to them.
    pub fn child<U>(&self, key: &str) -> State<U> {
        let child = self
            .tree
            .inner()
            .lock()
            .child_by_key(self.node, key)
            .unwrap_or_else(NodeId::null);

        State::new(self.tree.clone(), child)
    }

    /// [`child`](Self::child), with an absent key reported instead of handed
    /// back as a dead handle.
    ///
    /// The form the crate's own views use when absence is a branch rather than
    /// a state — [`AsyncState::result`], for one.
    pub fn field<U>(&self, key: &str) -> Option<State<U>> {
        let child = self.tree.inner().lock().child_by_key(self.node, key)?;

        Some(State::new(self.tree.clone(), child))
    }

    /// Subscribe. RAII: dropping the returned guard unsubscribes.
    ///
    /// `value()` never subscribes implicitly — there is no thread-local current
    /// subscriber and no automatic dependency tracking (handoff §11, §32).
    ///
    /// The callback runs **after** the tree lock is released, so it may read,
    /// subscribe, or even open its own transaction. It may also be invoked once
    /// after this subscription is dropped; see [`Subscription`].
    #[must_use = "dropping the subscription unsubscribes"]
    pub fn subscribe(&self, on_change: impl Fn(Change) + Send + Sync + 'static) -> Subscription {
        self.tree
            .subscribe(self.node, Arc::new(move |change, _| on_change(change)))
    }
}

impl<T: DeserializeOwned> State<T> {
    /// This subtree's value: one detached, non-reactive snapshot of it.
    ///
    /// The single materialization point. What comes back is a plain `T` with no
    /// tie to the tree — not a handle, not a view, not a guard.
    ///
    /// # Panics
    ///
    /// If the node was removed, or if its shape does not match `T`. Both are
    /// contract violations the caller can rule out; see [`Self::try_value`] for
    /// the checked form.
    #[track_caller]
    pub fn value(&self) -> T {
        match self.try_value() {
            Ok(value) => value,
            Err(error) => panic!("reading node {:?}: {error}", self.node),
        }
    }

    /// The same read, with the failure reported instead of raised.
    ///
    /// # Implementation
    ///
    /// Path (a) of §10.1: project the subtree to a `serde_json::Value`, then
    /// `from_value`. Two walks and one intermediate tree. The alternative —
    /// path (b), a `Deserializer` backed by the nodes themselves — is a **pure
    /// internal replacement**: this signature, [`ReadError`] and every call site
    /// stay exactly as they are, so it can land whenever a profile asks for it
    /// and deferring it owes nothing. The trigger is written down: a profile
    /// showing `serde_json::from_value` near the top of a **real** consumer's
    /// render loop **and** that consumer already reading per node, since (a)'s
    /// cost is mostly the disguise of a coarser problem — reading a whole list
    /// where `at()` / `by_key()` would read a row.
    pub fn try_value(&self) -> Result<T, ReadError> {
        // Teardown empties the root rather than freeing it (§2.2), so the
        // closed check has to come first or a torn-down root would read as
        // codegen drift — `Shape` — instead of as [`ReadError::Gone`] (§2.5).
        if self.tree.is_closed() {
            return Err(ReadError::Gone);
        }

        let hydrated = self.tree.to_hydrated(self.node).ok_or(ReadError::Gone)?;

        serde_json::from_value(hydrated).map_err(ReadError::Shape)
    }
}

impl<T> State<Vec<T>> {
    /// How many elements the array holds. `0` when the node is not an array.
    pub fn len(&self) -> usize {
        self.tree.inner().lock().ordered_children(self.node).len()
    }

    /// Whether the array is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The element at `index`.
    ///
    /// Named `at` rather than `get` so navigation and materialization never
    /// share a verb (§2.4): `at` hands back a handle, `value` hands back a
    /// value.
    pub fn at(&self, index: usize) -> Option<State<T>> {
        let child = self
            .tree
            .inner()
            .lock()
            .ordered_children(self.node)
            .get(index)
            .copied()?;

        Some(State::new(self.tree.clone(), child))
    }

    /// The first element.
    pub fn first(&self) -> Option<State<T>> {
        self.at(0)
    }

    /// The last element.
    pub fn last(&self) -> Option<State<T>> {
        let len = self.len();

        self.at(len.checked_sub(1)?)
    }

    /// Snapshots the child ids under the lock, then yields views. The iterator
    /// holds no lock, so a consumer may `subscribe()` while iterating.
    pub fn iter(&self) -> impl Iterator<Item = State<T>> + use<T> {
        let tree = self.tree.clone();

        self.tree
            .inner()
            .lock()
            .ordered_children(self.node)
            .into_iter()
            .map(move |child| State::new(tree.clone(), child))
    }
}

impl<T> State<Option<T>> {
    /// The value view when the node is not `Null`.
    pub fn as_some(&self) -> Option<State<T>> {
        if self.is_none() {
            return None;
        }

        Some(self.cast())
    }

    /// Whether the node is JSON `null`.
    pub fn is_none(&self) -> bool {
        matches!(
            self.tree.node(self.node).map(|node| node.kind),
            None | Some(NodeKind::Null)
        )
    }
}

/// A stream slot: ordered **and** keyed. `value()` still yields `Vec<T>`, so
/// the snapshot type on a generated `State` struct is unchanged (§4.3).
///
/// A newtype over `State<Vec<T>>` with inherent forwarding methods, not a
/// `Deref` — using `Deref` as inheritance is a Rust anti-pattern, and the
/// surface here is four methods.
pub struct StreamState<T>(State<Vec<T>>);

impl<T> Clone for StreamState<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> std::fmt::Debug for StreamState<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StreamState")
            .field("node", &self.0.node)
            .field("revision", &self.0.revision())
            .field("live", &self.0.is_live())
            .finish()
    }
}

impl<T> StreamState<T> {
    /// How many items the list holds.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the list is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Item keys in list order.
    pub fn keys(&self) -> Vec<Arc<str>> {
        match self.0.tree.node(self.0.node).map(|node| node.kind) {
            Some(NodeKind::Collection { items, .. }) => {
                items.into_iter().map(|(key, _)| key).collect()
            }
            _ => Vec::new(),
        }
    }

    /// The item with this key. Identity survives repositioning (§3.1).
    pub fn by_key(&self, item_key: &str) -> Option<State<T>> {
        let items = match self.0.tree.node(self.0.node).map(|node| node.kind) {
            Some(NodeKind::Collection { items, .. }) => items,
            _ => return None,
        };
        let node = items
            .into_iter()
            .find(|(key, _)| &**key == item_key)
            .map(|(_, node)| node)?;

        Some(State::new(self.0.tree.clone(), node))
    }

    /// The item at `index`, in list order.
    pub fn at(&self, index: usize) -> Option<State<T>> {
        self.0.at(index)
    }

    /// `(item_key, item)` in list order — what a keyed list adapter renders.
    pub fn iter(&self) -> impl Iterator<Item = (Arc<str>, State<T>)> + use<T> {
        let tree = self.0.tree.clone();
        let items = match tree.node(self.0.node).map(|node| node.kind) {
            Some(NodeKind::Collection { items, .. }) => items,
            _ => Vec::new(),
        };

        items
            .into_iter()
            .map(move |(key, node)| (key, State::new(tree.clone(), node)))
    }

    /// Subscribe, and be handed **this transaction's edits to this collection**
    /// along with the change.
    ///
    /// Called on exactly the occasions a plain node subscription is called; the
    /// slice is empty when the change was confined to items' own fields. The
    /// edits are not state — they are the difference between two states, which
    /// a callback cannot recompute by re-reading the tree — so they have to
    /// arrive with the notification.
    #[must_use = "dropping the subscription unsubscribes"]
    pub fn subscribe(
        &self,
        on_change: impl Fn(Change, &[CollectionEdit]) + Send + Sync + 'static,
    ) -> Subscription {
        self.0.tree.subscribe(self.0.node, Arc::new(on_change))
    }

    /// The same node seen as an ordinary `State<Vec<T>>`: a one-argument
    /// `subscribe`, index addressing, and whatever else generic code expects.
    /// It changes what a callback is *shown*, never when it is called.
    pub fn as_state(&self) -> State<Vec<T>> {
        self.0.clone()
    }

    /// The node's revision.
    pub fn revision(&self) -> u64 {
        self.0.revision()
    }

    /// The node this view is rooted at.
    pub fn node(&self) -> NodeId {
        self.0.node
    }
}

impl<T: DeserializeOwned> StreamState<T> {
    /// The whole list, materialized.
    ///
    /// # Panics
    ///
    /// As [`State::value`] does.
    #[track_caller]
    pub fn value(&self) -> Vec<T> {
        self.0.value()
    }

    /// The same read, with the failure reported instead of raised.
    pub fn try_value(&self) -> Result<Vec<T>, ReadError> {
        self.0.try_value()
    }
}

impl<T> From<State<Vec<T>>> for StreamState<T> {
    /// The conversion a generated stream accessor ends its chain with
    /// (`docs/rust-reactive-state.md` §4.3): the field lookup produces a
    /// [`State`], and `.into()` is what re-types it as the keyed view.
    fn from(state: State<Vec<T>>) -> Self {
        Self(state)
    }
}

/// A mounted child store. `store_id()` is what `Mounted::command_on` takes.
pub struct StoreState<S> {
    state: State<S>,
    /// Read once, when the handle is made, and never re-read: a store id is the
    /// node's identity, so re-deriving it from a node that has since been freed
    /// or re-rendered is how a child's command ends up dispatched at some other
    /// store.
    store_id: Option<StoreId>,
}

impl<S> Clone for StoreState<S> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            store_id: self.store_id.clone(),
        }
    }
}

impl<S> std::fmt::Debug for StoreState<S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StoreState")
            .field("node", &self.state.node)
            .field("store_id", &self.store_id)
            .field("revision", &self.state.revision())
            .field("live", &self.state.is_live())
            .finish()
    }
}

impl<S> StoreState<S> {
    /// The child's server-authored path, or `None` when this handle is not on a
    /// mounted store node — the same answer, for the same reason, that
    /// [`UploadSlotState::key`] gives.
    ///
    /// Identity, not a property: it is fixed for the node's life, and a
    /// different one would be a different store — so it hands back a value, not
    /// a handle (§2.4). The value is the one read when the handle was made, so
    /// a handle held across its store's unmount still names **its own** store:
    /// `command_on` then fails against a store the server no longer has, which
    /// is the loud outcome. Pair it with [`is_live`](State::is_live) to tell the
    /// two apart.
    ///
    /// # Deviation
    ///
    /// §2.4 signs `-> StoreId`. There is no honest `StoreId` for a handle that
    /// never had a store node under it — navigation is infallible (see
    /// [`State::child`]), so such a handle exists — and answering
    /// `StoreId::root()` is exactly the silently-wrong-target bug §3.4 exists to
    /// delete.
    pub fn store_id(&self) -> Option<StoreId> {
        self.store_id.clone()
    }

    /// The child's own shape, for the generated `Ext` trait to navigate.
    pub fn fields(&self) -> State<S> {
        self.state.clone()
    }

    /// Subscribe. RAII, as everywhere.
    #[must_use = "dropping the subscription unsubscribes"]
    pub fn subscribe(&self, on_change: impl Fn(Change) + Send + Sync + 'static) -> Subscription {
        self.state.subscribe(on_change)
    }

    /// The node's revision.
    pub fn revision(&self) -> u64 {
        self.state.revision()
    }

    /// The node this view is rooted at.
    pub fn node(&self) -> NodeId {
        self.state.node
    }
}

impl<S: DeserializeOwned> StoreState<S> {
    /// The child store's fields, wrapped with the id they were rendered under.
    ///
    /// # Panics
    ///
    /// As [`State::value`] does.
    #[track_caller]
    pub fn value(&self) -> StoreField<S> {
        self.state.cast::<StoreField<S>>().value()
    }

    /// The same read, with the failure reported instead of raised.
    pub fn try_value(&self) -> Result<StoreField<S>, ReadError> {
        self.state.cast::<StoreField<S>>().try_value()
    }
}

impl<S> From<State<S>> for StoreState<S> {
    /// The conversion a generated child-store accessor ends its chain with
    /// (§4.3), and the one place the store id is read.
    fn from(state: State<S>) -> Self {
        let store_id = match state.tree.node(state.node).map(|node| node.kind) {
            Some(NodeKind::Store { store_id, .. }) => Some(store_id),
            _ => None,
        };

        Self { state, store_id }
    }
}

/// An async node. The status is part of *this* node's semantics; the result is
/// a subtree that reconciles on its own (§3.3).
pub struct AsyncState<T>(State<T>);

impl<T> Clone for AsyncState<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> std::fmt::Debug for AsyncState<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AsyncState")
            .field("node", &self.0.node)
            .field("revision", &self.0.revision())
            .field("live", &self.0.is_live())
            .finish()
    }
}

impl<T> AsyncState<T> {
    /// Which of the three wire statuses the node is in.
    ///
    /// A value, not a handle: the status has no node, no revision and no
    /// subscriber list of its own — subscribing to *this* handle already
    /// observes it, because a `loading -> ok` flip changes this node's
    /// semantics (§2.4, §3.3).
    ///
    /// A handle whose node is gone also reads [`AsyncStatus::Loading`] — the
    /// one status that promises nothing — so this alone cannot tell a dead
    /// handle from a running task. [`is_live`](State::is_live) is what does.
    pub fn status(&self) -> AsyncStatus {
        match self.0.tree.node(self.0.node).map(|node| node.kind) {
            Some(NodeKind::Async { status, .. }) => status,
            // A node that is gone, or was re-rendered as something else, reads
            // as loading — the one status that promises nothing.
            _ => AsyncStatus::Loading,
        }
    }

    /// The `result` subtree. `None` when the wire `result` is `null`.
    pub fn result(&self) -> Option<State<T>> {
        let result: State<T> = self.0.field("result")?;

        if matches!(
            result.tree.node(result.node).map(|node| node.kind),
            Some(NodeKind::Null)
        ) {
            return None;
        }

        Some(result)
    }

    /// The `reason` subtree.
    ///
    /// An async node always has one — `classify` synthesises a `Null` child
    /// when the wire omits the key — so the dead handle [`State::child`] falls
    /// back to is reachable only from a handle whose own node is gone, and it
    /// reads as gone in turn rather than aliasing this node under a type it does
    /// not have.
    pub fn reason(&self) -> State<Option<AsyncError>> {
        self.0.child("reason")
    }

    /// Subscribe. RAII, as everywhere.
    #[must_use = "dropping the subscription unsubscribes"]
    pub fn subscribe(&self, on_change: impl Fn(Change) + Send + Sync + 'static) -> Subscription {
        self.0.subscribe(on_change)
    }

    /// The node's revision.
    pub fn revision(&self) -> u64 {
        self.0.revision()
    }

    /// The node this view is rooted at.
    pub fn node(&self) -> NodeId {
        self.0.node
    }
}

impl<T: DeserializeOwned> AsyncState<T> {
    /// The whole async value, in the three-variant shape an app matches on.
    ///
    /// # Panics
    ///
    /// As [`State::value`] does.
    #[track_caller]
    pub fn value(&self) -> AsyncResult<T> {
        self.0.cast::<AsyncResult<T>>().value()
    }

    /// The same read, with the failure reported instead of raised.
    pub fn try_value(&self) -> Result<AsyncResult<T>, ReadError> {
        self.0.cast::<AsyncResult<T>>().try_value()
    }
}

impl<T> AsyncState<Vec<T>> {
    /// The `result` subtree of a `stream_async` field, as a keyed collection.
    /// `None` while the result is `null`.
    pub fn ok_stream(&self) -> Option<StreamState<T>> {
        Some(self.result()?.into())
    }
}

impl<T> From<State<T>> for AsyncState<T> {
    /// The conversion a generated async accessor ends its chain with (§4.3).
    fn from(state: State<T>) -> Self {
        Self(state)
    }
}

/// An upload slot — the tree's inert half of the upload plane (§3.4).
///
/// A leaf: there is nothing under it to navigate to, and it never notifies,
/// because the server re-renders the same marker every cycle. It is a distinct
/// type rather than a plain `State<UploadSlot>` for exactly one reason: it
/// knows **both** halves of the `(store_id, name)` upload key, which is what
/// turns "walk from the state tree to the live upload handle" into one step
/// with no bare strings.
#[derive(Clone)]
pub struct UploadSlotState(State<UploadSlot>);

impl std::fmt::Debug for UploadSlotState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("UploadSlotState")
            .field("node", &self.0.node)
            .field("live", &self.0.is_live())
            .finish()
    }
}

impl UploadSlotState {
    /// Both halves of the upload key, or `None` if the slot node is gone (its
    /// store was unmounted, or the tree was closed).
    ///
    /// The owner is the nearest enclosing store, resolved once at node creation
    /// (§2.1), so a slot declared inside a child store reports that child's id
    /// rather than the root's.
    pub fn key(&self) -> Option<(StoreId, Arc<str>)> {
        match self.0.tree.node(self.0.node).map(|node| node.kind) {
            Some(NodeKind::UploadSlot { name, owner }) => Some((owner, name)),
            _ => None,
        }
    }

    /// The slot's marker, materialized.
    ///
    /// # Panics
    ///
    /// As [`State::value`] does.
    #[track_caller]
    pub fn value(&self) -> UploadSlot {
        self.0.value()
    }

    /// The same read, with the failure reported instead of raised.
    pub fn try_value(&self) -> Result<UploadSlot, ReadError> {
        self.0.try_value()
    }

    /// Registers, and never fires (§3.4). Present so the handle family has no
    /// exceptions, not because anything is expected to call it.
    #[must_use = "dropping the subscription unsubscribes"]
    pub fn subscribe(&self, on_change: impl Fn(Change) + Send + Sync + 'static) -> Subscription {
        self.0.subscribe(on_change)
    }

    /// The node's revision. Set when the node is created and never bumped
    /// again.
    pub fn revision(&self) -> u64 {
        self.0.revision()
    }

    /// The node this view is rooted at.
    pub fn node(&self) -> NodeId {
        self.0.node
    }
}

impl From<State<UploadSlot>> for UploadSlotState {
    /// The conversion a generated upload-slot accessor ends its chain with
    /// (§4.3). The slot's snapshot type is still `UploadSlot`; what changes is
    /// that the *handle* knows both halves of the upload key (§3.4).
    fn from(state: State<UploadSlot>) -> Self {
        Self(state)
    }
}
