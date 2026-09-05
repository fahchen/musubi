//! The retained reactive state tree of the Musubi client.
//!
//! One mounted root is one [`StateTree`]: a long-lived arena of nodes whose
//! [`NodeId`]s outlive every envelope. A patch is only *input*; whether anyone
//! is notified is decided by comparing each node's semantic value from before
//! the whole transaction with the one it settles on.
//!
//! ```text
//! PatchEnvelope
//!   ->  one transaction against the retained tree
//!         ops        -> pointer-addressed reconciliation
//!         stream_ops -> key-addressed collection reconciliation
//!   ->  recursive semantic equality, bottom-up over the dirty set
//!   ->  ChangeSet<NodeId> (+ per-collection keyed edits)
//!   ->  the subscribers of exactly the changed nodes
//!   ->  RAII-managed callbacks
//! ```
//!
//! The tree's structure **is** the dependency graph: no signal graph, no
//! thread-local current subscriber, no VDOM, and [`State::value`] never
//! subscribes implicitly.
//!
//! # The five interfaces
//!
//! * [`StateTree`] — the retained tree of one root, cheap to clone, one lock.
//! * [`Node`] / [`NodeId`] / [`NodeKind`] / [`SemanticValue`] — what a node is,
//!   and what equality sees.
//! * [`State`] and the four views ([`StreamState`], [`StoreState`],
//!   [`AsyncState`], [`UploadSlotState`]) — a typed reactive view rooted at one
//!   node; any subtree is a full reactive state.
//! * [`Subscription`] — one RAII token for the whole API, tree or not.
//! * [`StateTree::apply`] / [`Transaction`] / [`ChangeSet`] / [`Notify`] — one
//!   server message is one transaction, and the callbacks it owes are handed
//!   over only after the lock is released.
//!
//! # The four words
//!
//! `x.prop()` gives a **handle**, `handle.value()` gives a **value**,
//! `handle.subscribe(cb)` gives a **subscription**, and `drop(subscription)` is
//! the only way to unsubscribe. Nothing else is spelled a second way.
//!
//! ```
//! use musubi_state::{PatchOp, StateTree};
//! use serde_json::json;
//!
//! let tree = StateTree::new();
//! let root = tree.root::<serde_json::Value>();
//! let count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
//! let seen = count.clone();
//!
//! let notify = tree
//!     .apply(
//!         &[PatchOp::Replace { path: String::new(), value: json!({"count": 1}) }],
//!         &[],
//!     )
//!     .unwrap();
//! drop(notify);
//!
//! let field = root.field::<i64>("count").unwrap();
//! let subscription = field.subscribe(move |_| {
//!     seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
//! });
//!
//! assert_eq!(field.value(), 1);
//!
//! // A transaction that puts the value back is not a change.
//! drop(tree.apply(
//!     &[
//!         PatchOp::Replace { path: "/count".into(), value: json!(2) },
//!         PatchOp::Replace { path: "/count".into(), value: json!(1) },
//!     ],
//!     &[],
//! ));
//!
//! assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 0);
//! drop(subscription);
//! ```
//!
//! # Scope
//!
//! No network, no UI, no runtime: `serde` / `serde_json` / `slotmap` /
//! `thiserror` and nothing else. The socket, the envelope, the upload plane and
//! the event plane are `musubi-client`'s; the gpui adapter is
//! `musubi-gpui`'s.
//!
//! # Concurrency
//!
//! The whole arena sits behind one `std::sync::Mutex`. Writes are one per
//! envelope, on the actor task; reads are one per `value()` / `revision()` /
//! `subscribe()`. Poisoning is ignored, and here that is *safe* rather than
//! merely tolerable: the journal is a drop guard, so a panic inside a
//! transaction unwinds through the rollback.
//!
//! **Caller code never runs under the lock**, with exactly one exception the
//! client opts into: drift validation deserializes inside an open
//! [`Transaction`]. `subscribe` registers and returns; `apply` and `commit`
//! collect callbacks but invoke none; [`Notify`]'s `Drop` invokes them with the
//! lock already released, so a callback may read, subscribe, or open its own
//! transaction without deadlocking.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod arena;
mod change;
mod error;
mod marker;
mod node;
mod pointer;
mod state;
mod subscription;
#[cfg(test)]
mod tests;
mod transaction;
mod tree;
mod wire;

pub use crate::change::{Change, CollectionEdit};
pub use crate::error::ReadError;
pub use crate::node::{AsyncStatus, NodeId};
pub use crate::state::{AsyncState, State, StoreState, StreamState, UploadSlotState};
pub use crate::subscription::Subscription;
pub use crate::tree::StateTree;

// The write half (§5.5). `pub` because `musubi-client` calls it across the
// crate boundary, `#[doc(hidden)]` because it is not a consumer surface: it is
// the half of this design most likely to be overturned by implementation
// (carry-over table, journal and rollback, settle order), and it has no caller
// outside this workspace. `musubi-client` re-exports none of it.
#[doc(hidden)]
pub use crate::change::{ChangeSet, Notify};
#[doc(hidden)]
pub use crate::error::TreeError;
#[doc(hidden)]
pub use crate::node::{Node, NodeKind, SemanticValue};
#[doc(hidden)]
pub use crate::subscription::{SubscriberId, Unsubscribe};
#[doc(hidden)]
pub use crate::transaction::Transaction;
pub use crate::wire::{
    AsyncError, AsyncErrorKind, AsyncResult, PatchOp, StoreField, StoreId, StreamOp, UploadSlot,
};
