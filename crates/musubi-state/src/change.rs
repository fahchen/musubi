//! What a transaction changed, and the callbacks it owes
//! (`docs/rust-reactive-state.md` §2.3).

use std::collections::HashMap;
use std::sync::Arc;

use crate::arena::Callback;
use crate::node::NodeId;

/// What a subscriber is told.
///
/// No old/new value: the callback re-reads through its own
/// [`State`](crate::State) (handoff §24–25).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct Change {
    /// The node's revision after the transaction.
    pub revision: u64,
}

/// One keyed edit a [`Collection`](crate::NodeKind::Collection) node took.
///
/// Indices are the ones valid **at the moment that edit is applied**, in edit
/// order, so an incremental list adapter can replay the slice straight onto its
/// own row list.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum CollectionEdit {
    /// A key the list did not hold arrived at `index`.
    Inserted {
        /// The item's identity within the stream.
        item_key: Arc<str>,
        /// Where it landed.
        index: usize,
        /// The item's node — a handle, not a value clone.
        node: NodeId,
    },
    /// A key left the list: a `delete`, or a `limit` trim.
    Removed {
        /// The item's identity within the stream.
        item_key: Arc<str>,
        /// Where it was.
        index: usize,
    },
    /// An `insert` for a key the list already held, at a new position.
    Moved {
        /// The item's identity within the stream.
        item_key: Arc<str>,
        /// Where it was.
        from: usize,
        /// Where it landed.
        to: usize,
    },
    /// Everything before this edit is gone; what follows rebuilds the list.
    Reset,
}

/// What one transaction changed.
#[derive(Debug, Clone, Default)]
pub struct ChangeSet {
    changed: Vec<NodeId>,
    edits: HashMap<NodeId, Vec<CollectionEdit>>,
}

impl ChangeSet {
    /// Builds the set. Only [`Transaction::commit`](crate::Transaction::commit)
    /// calls this, and it has already dropped the edits of every node that did
    /// not change.
    pub(crate) fn new(changed: Vec<NodeId>, edits: HashMap<NodeId, Vec<CollectionEdit>>) -> Self {
        Self { changed, edits }
    }

    /// Every node whose semantic value changed: first the nodes still in the
    /// tree, children before parents, then the nodes this transaction removed,
    /// again children before parents.
    ///
    /// The two runs are not interleaved — a removed child is detached before it
    /// is notified, so it has no depth in the tree the settled nodes were sorted
    /// by (§9.3).
    pub fn changed(&self) -> &[NodeId] {
        &self.changed
    }

    /// Whether this node changed.
    pub fn contains(&self, id: NodeId) -> bool {
        self.changed.contains(&id)
    }

    /// Whether nothing changed.
    pub fn is_empty(&self) -> bool {
        self.changed.is_empty()
    }

    /// The keyed edits one collection node took, in application order. Empty
    /// for every node that is not a `Collection`, and for a collection whose
    /// change was confined to an item's own fields.
    ///
    /// Also empty for a node that is not in this change set at all — a
    /// transaction that rewrote a list into exactly what it already was
    /// changed nothing and edited nothing.
    ///
    /// This is the surface an incremental list adapter consumes; it reaches
    /// that adapter as the second argument of
    /// [`StreamState::subscribe`](crate::StreamState::subscribe).
    pub fn collection_edits(&self, id: NodeId) -> &[CollectionEdit] {
        self.edits.get(&id).map_or(&[], Vec::as_slice)
    }
}

/// One owed callback: whose subscription it is, and what it will be told.
pub(crate) struct Owed {
    pub(crate) node: NodeId,
    pub(crate) change: Change,
    pub(crate) callback: Callback,
}

/// The callbacks a committed transaction owes, and the change set that
/// produced them.
///
/// **The tree lock is already released when this exists.** There is no API that
/// hands a caller a callback while the lock is held; that is the handoff's
/// never-notify-under-the-lock rule made structural rather than conventional.
///
/// Dropping it invokes every owed callback exactly once, on the dropping
/// thread. Holding it is how a caller sequences notification against the rest
/// of its own commit (§3.6, steps 5–9).
///
/// **One subscriber's panic costs only that subscriber's notification.** The
/// callbacks are independent observations, and this drop runs on the client's
/// actor task: letting one unwind would skip every callback after it *and* take
/// the connection down with it (§4.4). Each is therefore caught; the panic hook
/// has already reported it by then, so nothing is swallowed silently.
#[must_use = "dropping this is what notifies subscribers"]
pub struct Notify {
    changes: ChangeSet,
    owed: Vec<Owed>,
}

impl Notify {
    pub(crate) fn new(changes: ChangeSet, owed: Vec<Owed>) -> Self {
        Self { changes, owed }
    }

    /// What the transaction changed. Readable before the callbacks run.
    pub fn changes(&self) -> &ChangeSet {
        &self.changes
    }
}

impl std::fmt::Debug for Notify {
    /// Prints the shape, never the callbacks — they have no `Debug`.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Notify")
            .field("changed", &self.changes.changed().len())
            .field("owed", &self.owed.len())
            .finish()
    }
}

impl Drop for Notify {
    fn drop(&mut self) {
        // The lock is long gone, so a callback may freely read, subscribe, or
        // even open its own transaction (§2.6).
        for owed in std::mem::take(&mut self.owed) {
            let call = std::panic::AssertUnwindSafe(|| {
                (owed.callback)(owed.change, self.changes.collection_edits(owed.node));
            });

            // Nothing here is left half-written by an unwind: the callback owns
            // whatever it touched, and this loop's own state is one moved `Vec`.
            let _ = std::panic::catch_unwind(call);
        }
    }
}
