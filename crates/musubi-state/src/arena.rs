//! The node arena: one `SlotMap` plus the two incremental indices
//! (`docs/rust-reactive-state.md` §2.1, §3.5).
//!
//! Everything here runs under the tree's single lock. Nothing here invokes a
//! caller's callback — that is [`Notify`](crate::Notify)'s job, after the lock
//! is released (§2.6).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use serde_json::Value;
use slotmap::SlotMap;

use crate::change::{Change, CollectionEdit};
use crate::node::{AsyncStatus, Node, NodeId, NodeKind, Semantic, SemanticValue};
use crate::subscription::SubscriberId;
use crate::wire::StoreId;

/// One subscriber's callback.
///
/// Two arguments, always: a plain node subscription wraps a one-argument
/// closure, so the arena stores exactly one shape (§2.4).
pub(crate) type Callback = Arc<dyn Fn(Change, &[CollectionEdit]) + Send + Sync>;

/// The key a collection is indexed by: its owning store plus its declared name.
pub(crate) type CollectionKey = (StoreId, String);

/// The deepest a node may sit below the root, and therefore the longest parent
/// chain any walk up the tree can meet.
///
/// The tree is built from wire input, and depth composes across ops and across
/// envelopes: `serde_json`'s own 128-level nesting limit bounds one document,
/// not a hundred `add`s at successively deeper paths. Every recursive walk over
/// a subtree — semantic equality, the two projections, `Drop` — would then abort
/// the **process** on stack exhaustion rather than unwinding, so the cap is
/// enforced where nodes are created and every walker is written to survive a
/// broken one.
pub(crate) const MAX_DEPTH: usize = 256;

/// One retained node, as the arena holds it.
pub(crate) struct NodeData {
    pub(crate) parent: Option<NodeId>,
    pub(crate) kind: NodeKind,
    pub(crate) revision: u64,
    pub(crate) semantic: SemanticValue,
    pub(crate) subscribers: Vec<(SubscriberId, Callback)>,
}

/// The whole retained tree of one mounted root.
pub(crate) struct Arena {
    pub(crate) nodes: SlotMap<NodeId, NodeData>,
    pub(crate) root: NodeId,
    /// `store_id -> node`, maintained incrementally: one insert per store node
    /// created, one removal per store node freed. This is what replaced the
    /// per-envelope `build_store_index` (§3.5).
    pub(crate) stores: HashMap<StoreId, NodeId>,
    /// `(owner, stream name) -> node`. The marker's owner is resolved **once**,
    /// when the node is created, and never re-resolved (§3.5).
    pub(crate) collections: HashMap<CollectionKey, NodeId>,
    /// Set by [`StateTree::close`](crate::StateTree::close). Terminal.
    pub(crate) closed: bool,
    next_subscriber: u64,
}

impl Arena {
    /// An arena holding one root node, `Null`, revision `0`.
    pub(crate) fn new() -> Self {
        let mut nodes = SlotMap::with_key();
        let root = nodes.insert(NodeData {
            parent: None,
            kind: NodeKind::Null,
            revision: 0,
            semantic: SemanticValue::new(Semantic::Null),
            subscribers: Vec::new(),
        });

        Self {
            nodes,
            root,
            stores: HashMap::new(),
            collections: HashMap::new(),
            closed: false,
            next_subscriber: 0,
        }
    }

    /// A copy of one node's metadata, or `None` if it has been freed.
    pub(crate) fn node(&self, id: NodeId) -> Option<Node> {
        self.nodes.get(id).map(|node| Node {
            parent: node.parent,
            kind: node.kind.clone(),
            revision: node.revision,
            semantic: node.semantic.clone(),
            subscribers: node.subscribers.len(),
        })
    }

    /// The child one object key, store field, or async slot resolves to.
    ///
    /// Arrays and collections are addressed by index and by item key, not by
    /// key, so they answer `None` here.
    pub(crate) fn child_by_key(&self, id: NodeId, key: &str) -> Option<NodeId> {
        match &self.nodes.get(id)?.kind {
            NodeKind::Object(fields) | NodeKind::Store { fields, .. } => fields.get(key).copied(),
            NodeKind::Async { result, reason, .. } => match key {
                "result" => Some(*result),
                "reason" => Some(*reason),
                _ => None,
            },
            _ => None,
        }
    }

    /// The child ids of an array or a collection, in list order.
    ///
    /// One snapshot, for the callers that genuinely need the whole list — an
    /// iterator that must not hold the lock while a consumer subscribes. Every
    /// caller that addresses **one** child goes through [`child_at`],
    /// [`child_by_item_key`] or [`child_count`] instead, none of which
    /// allocates: `at(i)` is the crate's own answer to reading a whole list, and
    /// it would be no answer at all if it cost one copy of the list per element.
    ///
    /// [`child_at`]: Self::child_at
    /// [`child_by_item_key`]: Self::child_by_item_key
    /// [`child_count`]: Self::child_count
    pub(crate) fn ordered_children(&self, id: NodeId) -> Vec<NodeId> {
        match self.nodes.get(id).map(|node| &node.kind) {
            Some(NodeKind::Array(items)) => items.clone(),
            Some(NodeKind::Collection { items, .. }) => {
                items.iter().map(|(_, child)| *child).collect()
            }
            _ => Vec::new(),
        }
    }

    /// The child one array index or list position addresses, without
    /// materializing the list.
    pub(crate) fn child_at(&self, id: NodeId, index: usize) -> Option<NodeId> {
        match &self.nodes.get(id)?.kind {
            NodeKind::Array(items) => items.get(index).copied(),
            NodeKind::Collection { items, .. } => items.get(index).map(|(_, child)| *child),
            _ => None,
        }
    }

    /// The child a collection files under one item key (§3.1).
    pub(crate) fn child_by_item_key(&self, id: NodeId, item_key: &str) -> Option<NodeId> {
        match &self.nodes.get(id)?.kind {
            NodeKind::Collection { items, .. } => items
                .iter()
                .find(|(key, _)| &**key == item_key)
                .map(|(_, child)| *child),
            _ => None,
        }
    }

    /// How many children an array or a collection holds. `0` for every other
    /// kind, which is what a `len()` on a node that is not a list reads.
    pub(crate) fn child_count(&self, id: NodeId) -> usize {
        match self.nodes.get(id).map(|node| &node.kind) {
            Some(NodeKind::Array(items)) => items.len(),
            Some(NodeKind::Collection { items, .. }) => items.len(),
            _ => 0,
        }
    }

    /// A collection's item keys, in list order.
    pub(crate) fn item_keys(&self, id: NodeId) -> Vec<Arc<str>> {
        match self.nodes.get(id).map(|node| &node.kind) {
            Some(NodeKind::Collection { items, .. }) => {
                items.iter().map(|(key, _)| key.clone()).collect()
            }
            _ => Vec::new(),
        }
    }

    /// A collection's `(item_key, child)` pairs, in list order. The keyed
    /// counterpart of [`ordered_children`](Self::ordered_children), and taken
    /// once per iterator for the same reason.
    pub(crate) fn ordered_items(&self, id: NodeId) -> Vec<(Arc<str>, NodeId)> {
        match self.nodes.get(id).map(|node| &node.kind) {
            Some(NodeKind::Collection { items, .. }) => items.clone(),
            _ => Vec::new(),
        }
    }

    /// Every child of a node, in whatever order its kind holds them.
    pub(crate) fn children(&self, id: NodeId) -> Vec<NodeId> {
        match self.nodes.get(id).map(|node| &node.kind) {
            Some(NodeKind::Array(items)) => items.clone(),
            Some(NodeKind::Object(fields) | NodeKind::Store { fields, .. }) => {
                fields.values().copied().collect()
            }
            Some(NodeKind::Collection { items, .. }) => {
                items.iter().map(|(_, child)| *child).collect()
            }
            Some(NodeKind::Async { result, reason, .. }) => vec![*result, *reason],
            _ => Vec::new(),
        }
    }

    /// Whether a node is JSON `null` — or gone, which reads the same way to the
    /// two views that ask (`State<Option<T>>` and an async `result`).
    pub(crate) fn is_null(&self, id: NodeId) -> bool {
        matches!(
            self.nodes.get(id).map(|node| &node.kind),
            None | Some(NodeKind::Null)
        )
    }

    /// One async node's status (§3.3), which is the node's own semantics rather
    /// than a child.
    pub(crate) fn async_status(&self, id: NodeId) -> Option<AsyncStatus> {
        match &self.nodes.get(id)?.kind {
            NodeKind::Async { status, .. } => Some(*status),
            _ => None,
        }
    }

    /// The id a store node is filed under, without its fields.
    pub(crate) fn store_id_of(&self, id: NodeId) -> Option<StoreId> {
        match &self.nodes.get(id)?.kind {
            NodeKind::Store { store_id, .. } => Some(store_id.clone()),
            _ => None,
        }
    }

    /// Both halves of an upload slot's key (§3.4).
    pub(crate) fn upload_key(&self, id: NodeId) -> Option<(StoreId, Arc<str>)> {
        match &self.nodes.get(id)?.kind {
            NodeKind::UploadSlot { name, owner } => Some((owner.clone(), name.clone())),
            _ => None,
        }
    }

    /// The nearest enclosing store of a node's **children** — the node's own id
    /// when it is a store node, otherwise the nearest one above it.
    ///
    /// Only reconciliation calls this, and only for a node it is about to
    /// rebuild; a `Collection` or `UploadSlot` created underneath keeps the
    /// answer forever (§3.5).
    pub(crate) fn owner_of(&self, id: NodeId) -> StoreId {
        let mut cursor = Some(id);

        for _ in 0..=MAX_DEPTH + 1 {
            let Some(current) = cursor else {
                return StoreId::root();
            };
            let Some(node) = self.nodes.get(current) else {
                return StoreId::root();
            };

            if let NodeKind::Store { store_id, .. } = &node.kind {
                return store_id.clone();
            }

            cursor = node.parent;
        }

        debug_assert!(false, "a parent chain longer than the depth cap");

        StoreId::root()
    }

    /// Whether every node of a subtree sits within `budget` levels of its own
    /// root.
    ///
    /// The composition half of the depth cap (§3.2): `build` measures one node
    /// at a time, which bounds a subtree that arrives from the wire, but a
    /// subtree that is **re-parented** arrives whole and its matching
    /// descendants never reach `build` at all. Destination depth plus this
    /// answer is what the write boundary refuses on.
    ///
    /// Iterative, and stops at the first node past the budget: a move that is
    /// about to be refused pays `budget` levels, not the subtree. `walked` is
    /// what keeps a broken tree from costing exponential work in the branches a
    /// cycle would repeat.
    pub(crate) fn height_within(&self, id: NodeId, budget: usize) -> bool {
        let mut stack = vec![(id, 0usize)];
        let mut walked = HashSet::new();

        while let Some((current, level)) = stack.pop() {
            if level > budget {
                return false;
            }

            if !walked.insert(current) {
                continue;
            }

            for child in self.children(current) {
                stack.push((child, level + 1));
            }
        }

        true
    }

    /// How far below the root a node sits. `0` for the root and for any node
    /// this transaction has detached.
    ///
    /// The write paths get this for free from the pointer they walked — one
    /// token is one level — so this is for the callers that reach a node
    /// through an index instead: the stream ops, which resolve
    /// `(store_id, stream)` and never a pointer (§3.1).
    pub(crate) fn depth(&self, id: NodeId) -> usize {
        let mut cursor = self.nodes.get(id).and_then(|node| node.parent);
        let mut depth = 0;

        while let Some(current) = cursor {
            depth += 1;

            if depth > MAX_DEPTH + 1 {
                debug_assert!(false, "a parent chain longer than the depth cap");
                break;
            }

            cursor = self.nodes.get(current).and_then(|node| node.parent);
        }

        depth
    }

    /// One node's semantic value, computed from its children's **cached**
    /// values.
    ///
    /// This is the settle step's recompute (§2.3): an unchanged child
    /// contributes the exact `Arc` it already had, so a parent's recompute is a
    /// run of pointer copies and its comparison stops at pointer equality.
    pub(crate) fn semantic_shallow(&self, id: NodeId) -> SemanticValue {
        self.compute(id, &|arena, child| {
            arena.nodes.get(child).map_or_else(
                || SemanticValue::new(Semantic::Null),
                |node| node.semantic.clone(),
            )
        })
    }

    /// One subtree's semantic value, computed from the **kinds** alone.
    ///
    /// Used mid-transaction, where cached values are deliberately stale until
    /// commit settles them: `Transaction::to_hydrated` is the one caller
    /// (§4.4 drift validation).
    pub(crate) fn semantic_deep(&self, id: NodeId) -> SemanticValue {
        self.compute(id, &|arena, child| arena.semantic_deep(child))
    }

    fn compute(&self, id: NodeId, child: &dyn Fn(&Self, NodeId) -> SemanticValue) -> SemanticValue {
        let Some(node) = self.nodes.get(id) else {
            return SemanticValue::new(Semantic::Null);
        };

        let semantic = match &node.kind {
            NodeKind::Null => Semantic::Null,
            NodeKind::Bool(flag) => Semantic::Bool(*flag),
            NodeKind::Number(number) => Semantic::Number(number.clone()),
            NodeKind::String(text) => Semantic::String(text.clone()),
            NodeKind::Array(items) => {
                Semantic::Array(items.iter().map(|item| child(self, *item)).collect())
            }
            NodeKind::Object(fields) => Semantic::Object(
                fields
                    .iter()
                    .map(|(key, node)| (key.clone(), child(self, *node)))
                    .collect(),
            ),
            NodeKind::Store { store_id, fields } => Semantic::Store {
                store_id: store_id.clone(),
                fields: fields
                    .iter()
                    .map(|(key, node)| (key.clone(), child(self, *node)))
                    .collect(),
            },
            NodeKind::Collection { name, owner, items } => Semantic::Collection {
                name: name.clone(),
                owner: owner.clone(),
                items: items
                    .iter()
                    .map(|(key, node)| (key.clone(), child(self, *node)))
                    .collect(),
            },
            NodeKind::Async {
                status,
                result,
                reason,
            } => Semantic::Async {
                status: *status,
                result: child(self, *result),
                reason: child(self, *reason),
            },
            NodeKind::UploadSlot { name, owner } => Semantic::UploadSlot {
                name: name.clone(),
                owner: owner.clone(),
            },
        };

        SemanticValue::new(semantic)
    }

    /// The hydrated projection of a subtree, straight off the kinds.
    ///
    /// Committed reads go through the cached [`SemanticValue`] instead; this
    /// exists for the one mid-transaction reader.
    pub(crate) fn to_hydrated_deep(&self, id: NodeId) -> Option<Value> {
        self.nodes.get(id)?;

        Some(self.semantic_deep(id).to_hydrated())
    }

    /// Registers a callback and hands back its id.
    pub(crate) fn subscribe(&mut self, id: NodeId, callback: Callback) -> SubscriberId {
        let subscriber = SubscriberId::from_raw(self.next_subscriber);

        self.next_subscriber += 1;

        // A node that is already gone, or a closed tree, still mints an id: the
        // token is inert, and dropping it finds nothing to remove.
        if let Some(node) = self.nodes.get_mut(id) {
            node.subscribers.push((subscriber, callback));
        }

        subscriber
    }

    /// Drops one subscriber. A no-op for an id that is not registered — which
    /// is what a subscription against a closed tree drops.
    pub(crate) fn unsubscribe(&mut self, id: NodeId, subscriber: SubscriberId) {
        if let Some(node) = self.nodes.get_mut(id) {
            node.subscribers
                .retain(|(existing, _)| *existing != subscriber);
        }
    }

    /// Post-order walk of a subtree: children before their parent.
    ///
    /// The order removal notification is delivered in, and the order slots are
    /// freed in.
    ///
    /// `walked` is what keeps this from recursing forever if the tree it is
    /// handed ever stops being one — a node reached twice is visited once, so a
    /// broken invariant costs a missing notification rather than the stack.
    /// It does **not** assert: this runs inside `commit`, past the point where
    /// the guard and the journal were taken, so an unwind from here would leave
    /// a half-committed tree behind a poisoned lock with no rollback left to
    /// run.
    pub(crate) fn subtree_post_order(
        &self,
        id: NodeId,
        into: &mut Vec<NodeId>,
        walked: &mut HashSet<NodeId>,
    ) {
        if !walked.insert(id) {
            return;
        }

        for child in self.children(id) {
            self.subtree_post_order(child, into, walked);
        }

        if self.nodes.contains_key(id) {
            into.push(id);
        }
    }

    /// Frees one node's slot and drops its entries from the two indices.
    ///
    /// The index entries are only dropped when they still point *at this node*:
    /// a store that was adopted elsewhere in the same transaction has already
    /// re-pointed its entry, and this must not undo that.
    pub(crate) fn free(&mut self, id: NodeId) {
        let Some(node) = self.nodes.remove(id) else {
            return;
        };

        match node.kind {
            NodeKind::Store { store_id, .. } => {
                if self.stores.get(&store_id) == Some(&id) {
                    self.stores.remove(&store_id);
                }
            }
            NodeKind::Collection { name, owner, .. } => {
                let key = (owner, name.to_string());

                if self.collections.get(&key) == Some(&id) {
                    self.collections.remove(&key);
                }
            }
            _ => {}
        }
    }
}
