//! The retained tree of one mounted root (`docs/rust-reactive-state.md` §2.2).

use std::sync::{Arc, Mutex, MutexGuard};

use serde_json::Value;

use crate::arena::{Arena, Callback};
use crate::change::Notify;
use crate::error::TreeError;
use crate::node::{Node, NodeId};
use crate::state::State;
use crate::subscription::{SubscriberId, Subscription};
use crate::transaction::Transaction;
use crate::wire::{PatchOp, StoreId, StreamOp};

/// The shared half of a tree: one arena behind one lock (§2.6).
pub(crate) struct StateTreeInner {
    arena: Mutex<Arena>,
}

impl StateTreeInner {
    /// Locks the arena, ignoring poisoning.
    ///
    /// Ignoring it is *safe* here rather than merely tolerable: the journal is
    /// a drop guard, so a panic inside a transaction unwinds through the
    /// rollback and the arena a poisoned lock guards is consistent (§2.6).
    pub(crate) fn lock(&self) -> MutexGuard<'_, Arena> {
        self.arena
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Drops one node subscriber. Called from `Subscription`'s `Drop`.
    pub(crate) fn unsubscribe(&self, node: NodeId, id: SubscriberId) {
        self.lock().unsubscribe(node, id);
    }
}

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
    /// The root's [`NodeId`] is allocated here and **never changes** — not on a
    /// `replace ""`, not on a rejoin, not on a cache seed. That is what makes
    /// `Mounted::state()` a value an embedder can hold across a reconnect.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(StateTreeInner {
                arena: Mutex::new(Arena::new()),
            }),
        }
    }

    /// The root as a typed reactive view. `T` is unchecked here; drift is the
    /// mount's problem, not the tree's (§4.4).
    pub fn root<T>(&self) -> State<T> {
        let root = self.root_id();

        State::new(self.clone(), root)
    }

    /// The root's [`NodeId`].
    pub fn root_id(&self) -> NodeId {
        self.inner.lock().root
    }

    /// One transaction, applied and committed. `ops` land before `stream_ops`,
    /// which is the only order in which every op's target exists (§3.6).
    ///
    /// Atomic: on any error every mutation is rolled back and the tree is
    /// exactly as it was. Subscribers are **not** invoked here — the returned
    /// guard owes them (§2.3).
    #[doc(hidden)]
    pub fn apply(&self, ops: &[PatchOp], stream_ops: &[StreamOp]) -> Result<Notify, TreeError> {
        let mut transaction = self.begin();

        transaction.apply(ops, stream_ops)?;

        Ok(transaction.commit())
    }

    /// A transaction the caller drives, for the one case that needs to inspect
    /// the result before deciding: drift validation (§4.4).
    #[doc(hidden)]
    pub fn begin(&self) -> Transaction<'_> {
        Transaction::open(&self.inner)
    }

    /// Ends the tree: empties the root to `Null`, notifies, and refuses every
    /// later transaction. Terminal — the analogue of `Latest::close`, and what
    /// `RootSink::clear` calls at teardown.
    #[doc(hidden)]
    pub fn close(&self) -> Notify {
        let mut transaction = self.begin();

        transaction.close();

        transaction.commit()
    }

    /// A copy of one node's metadata, or `None` if it has been freed.
    pub fn node(&self, id: NodeId) -> Option<Node> {
        self.inner.lock().node(id)
    }

    /// The hydrated projection of a subtree: stream slots as arrays, store
    /// nodes carrying `__musubi_store_id__`, upload slots as their marker,
    /// async nodes as their wire shape (§3.5). What
    /// [`State::value`](crate::State::value) reads.
    pub fn to_hydrated(&self, id: NodeId) -> Option<Value> {
        let arena = self.inner.lock();

        Some(arena.nodes.get(id)?.semantic.to_hydrated())
    }

    /// The wire projection of a subtree: stream slots back to
    /// `{"__musubi_stream__": name}`, everything else as above. This is the
    /// shape the mount cache stores (§7).
    pub fn to_wire(&self, id: NodeId) -> Option<Value> {
        let arena = self.inner.lock();

        Some(arena.nodes.get(id)?.semantic.to_wire())
    }

    /// Every live store id. Replaces the pruning half of `index.rs` (§3.5).
    pub fn store_ids(&self) -> Vec<StoreId> {
        self.inner.lock().stores.keys().cloned().collect()
    }

    /// The node a store id resolves to, or `None` if that store is not mounted.
    pub fn store_node(&self, store_id: &StoreId) -> Option<NodeId> {
        self.inner.lock().stores.get(store_id).copied()
    }

    /// Node count. Tests and diagnostics.
    pub fn len(&self) -> usize {
        self.inner.lock().nodes.len()
    }

    /// Always `false`: the root node outlives every transaction, so a tree
    /// never holds zero nodes. Present because `len` is.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether `close` has ended this tree. The write half's, not the read
    /// half's (§5.5): a consumer asks [`State::is_live`], which folds this
    /// together with "the node is still there".
    pub(crate) fn is_closed(&self) -> bool {
        self.inner.lock().closed
    }

    /// Registers one node subscriber and hands back its RAII token.
    ///
    /// A node that is gone, or a tree that is closed, still hands back a token:
    /// it is simply inert, so a consumer never has to branch on liveness just
    /// to subscribe.
    pub(crate) fn subscribe(&self, node: NodeId, callback: Callback) -> Subscription {
        let id = self.inner.lock().subscribe(node, callback);

        Subscription::node(Arc::downgrade(&self.inner), node, id)
    }

    /// The shared half, for the views.
    pub(crate) fn inner(&self) -> &Arc<StateTreeInner> {
        &self.inner
    }
}

impl Default for StateTree {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for StateTree {
    /// Prints the tree's shape, never its contents: `Debug` on a view is
    /// identity, not value (§2.4).
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let arena = self.inner.lock();

        formatter
            .debug_struct("StateTree")
            .field("nodes", &arena.nodes.len())
            .field("stores", &arena.stores.len())
            .field("closed", &arena.closed)
            .finish()
    }
}
