//! The one RAII subscription token (`docs/rust-reactive-state.md` §2.5).

use std::sync::Weak;

use crate::node::NodeId;
use crate::tree::StateTreeInner;

/// One subscriber's identity within one target. Opaque.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubscriberId(u64);

impl SubscriberId {
    /// Mints an id from a counter the target owns.
    ///
    /// The seam for an [`Unsubscribe`] implementor: a cell outside any tree
    /// mints ids for its own subscribers and hands them back to
    /// [`Subscription::cell`]. Ids are unique **within one target**, never
    /// globally.
    pub fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// The counter value behind the id, for a target that files subscribers in
    /// a map of its own.
    pub fn as_raw(self) -> u64 {
        self.0
    }
}

/// What a non-tree subscription target must be able to do.
///
/// Implemented in `musubi-client` by the status and upload cells (§5.4, §6.4);
/// `musubi-state` never names them. The lower crate states the contract, the
/// upper one satisfies it.
pub trait Unsubscribe: Send + Sync {
    /// Drops one subscriber. Called from [`Subscription`]'s `Drop`, at most
    /// once per id, and never while the target's own lock is held by the
    /// caller.
    fn unsubscribe(&self, id: SubscriberId);
}

/// One RAII subscription. Dropping it unsubscribes.
///
/// **One token for the whole API** (§2.4): a node subscription, a `StatusState`
/// subscription and an `Upload` subscription are all this type, so one
/// `Vec<Subscription>` holds every observation a view has.
///
/// Holds a `Weak` to whatever it was registered on, so a subscription never
/// keeps that thing alive, and dropping one against an already-dropped target
/// is a no-op.
///
/// **A callback may be invoked once after its subscription is dropped.**
/// [`Notify`](crate::Notify) clones the owed callbacks under the tree lock and
/// invokes them after releasing it, so a drop that lands in that window is too
/// late to cancel the call. The contract is therefore: *a callback is invoked
/// at most once per transaction, and may be invoked one more time after its
/// subscription is dropped; callbacks must tolerate one stale call.* The two
/// cells outside the tree follow the same rule, so a consumer writes that
/// tolerance once.
#[must_use = "dropping the subscription unsubscribes"]
pub struct Subscription(Target);

/// Where a subscription is registered.
enum Target {
    /// A node of a retained tree. A `Weak` and two ids; no allocation.
    Node {
        tree: Weak<StateTreeInner>,
        node: NodeId,
        id: SubscriberId,
    },
    /// A cell outside any tree — `musubi-client`'s status and upload planes.
    Cell {
        cell: Weak<dyn Unsubscribe>,
        id: SubscriberId,
    },
}

impl Subscription {
    /// A subscription on one node of one tree.
    pub(crate) fn node(tree: Weak<StateTreeInner>, node: NodeId, id: SubscriberId) -> Self {
        Self(Target::Node { tree, node, id })
    }

    /// A subscription on a cell outside any tree.
    ///
    /// The seam §2.5 signs for `musubi-client`: the cell registers the
    /// subscriber itself, mints the [`SubscriberId`], and hands both halves
    /// here so its consumers get back the same token a node subscription
    /// produces.
    pub fn cell(cell: Weak<dyn Unsubscribe>, id: SubscriberId) -> Self {
        Self(Target::Cell { cell, id })
    }
}

impl std::fmt::Debug for Subscription {
    /// Prints the target's shape, never the callback.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            Target::Node { node, id, .. } => formatter
                .debug_struct("Subscription")
                .field("node", node)
                .field("id", id)
                .finish(),
            Target::Cell { id, .. } => formatter
                .debug_struct("Subscription")
                .field("cell", &true)
                .field("id", id)
                .finish(),
        }
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        match &self.0 {
            Target::Node { tree, node, id } => {
                if let Some(tree) = tree.upgrade() {
                    tree.unsubscribe(*node, *id);
                }
            }
            Target::Cell { cell, id } => {
                if let Some(cell) = cell.upgrade() {
                    cell.unsubscribe(*id);
                }
            }
        }
    }
}
