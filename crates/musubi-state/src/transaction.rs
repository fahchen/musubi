//! One transaction against the retained tree (`docs/rust-reactive-state.md`
//! §2.3, §3.1, §3.6, §9.2).
//!
//! One server message is one transaction. The journal is a drop guard: every
//! mutation records what it displaced, `commit` is the only way to keep the
//! work, and a panic mid-transaction unwinds through the rollback rather than
//! leaving the tree half-applied. Rollback is O(diff), not O(tree) — which
//! makes atomicity **cheaper** than v1's, where it cost one whole-tree clone
//! per envelope.

use std::cmp::Reverse;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, MutexGuard};

use serde_json::{Map, Value};

use crate::arena::{Arena, CollectionKey, NodeData};
use crate::change::{Change, ChangeSet, CollectionEdit, Notify, Owed};
use crate::error::TreeError;
use crate::marker::{ASYNC_STATUS_KEY, STORE_ID_KEY, Shape, classify};
use crate::node::{AsyncStatus, NodeId, NodeKind, Semantic, SemanticValue};
use crate::pointer::{self, ArrayIndex};
use crate::tree::StateTreeInner;
use crate::wire::{PatchOp, StoreId, StreamOp};

/// The message every accessor uses: an open transaction always holds its guard,
/// and only `commit` takes it away.
const OPEN: &str = "an open transaction holds the arena lock";

/// What one node looked like before this transaction first touched it.
struct Touched {
    id: NodeId,
    parent: Option<NodeId>,
    kind: NodeKind,
    revision: u64,
    semantic: SemanticValue,
}

/// One index mutation, and the value it displaced.
enum IndexUndo {
    Store(StoreId, Option<NodeId>),
    Collection(CollectionKey, Option<NodeId>),
}

/// Everything this transaction has to be able to undo, plus everything commit
/// needs in order to settle.
#[derive(Default)]
struct Journal {
    touched: Vec<Touched>,
    touched_set: HashSet<NodeId>,
    allocated: Vec<NodeId>,
    index_undo: Vec<IndexUndo>,
    /// Detached and awaiting free at commit.
    pending: Vec<NodeId>,
    pending_set: HashSet<NodeId>,
    /// §3.1's carry-over table: nodes that left a collection during this
    /// transaction, still reusable by an `insert` for the same key, and freed
    /// at commit if nothing claims them.
    carry: HashMap<(NodeId, String), NodeId>,
    carry_order: Vec<(NodeId, String)>,
    edits: HashMap<NodeId, Vec<CollectionEdit>>,
    /// The dirty set: every node whose value has to be recomputed at commit,
    /// against the value it held when this transaction first reached it. Depth
    /// is deliberately **not** recorded — a node can be re-parented after it is
    /// dirtied, and commit needs the order of the tree it is committing.
    settle: HashMap<NodeId, SemanticValue>,
    settle_order: Vec<NodeId>,
    closed: bool,
}

/// An open transaction. Holds the tree's lock; `!Send`, and lives on whichever
/// task drives the envelope (the actor task).
///
/// Dropping it **rolls back**. [`commit`](Transaction::commit) is the only way
/// to keep the work.
pub struct Transaction<'a> {
    arena: Option<MutexGuard<'a, Arena>>,
    journal: Journal,
    /// The store ids this op has already installed somewhere. A render that
    /// carries one `__musubi_store_id__` under two keys is a server bug, and
    /// this is what keeps the second sighting from *adopting* the first — which
    /// would alias one node under two parents (§3.2).
    ///
    /// Scoped to the op, not to the transaction: two ops in one envelope may
    /// legitimately move the same store twice.
    claimed: HashSet<StoreId>,
}

impl<'a> Transaction<'a> {
    /// Takes the lock and opens the journal.
    pub(crate) fn open(tree: &'a Arc<StateTreeInner>) -> Self {
        Self {
            arena: Some(tree.lock()),
            journal: Journal::default(),
            claimed: HashSet::new(),
        }
    }

    /// Applies one batch. May be called more than once; every call joins the
    /// same transaction, so `1 -> 2 -> 1` across two calls still notifies
    /// nobody.
    pub fn apply(&mut self, ops: &[PatchOp], stream_ops: &[StreamOp]) -> Result<(), TreeError> {
        if self.arena().closed {
            return Err(TreeError::Closed);
        }

        // Ops first: the initial `replace ""` is what creates the slot a stream
        // op in the same envelope fills (§3.1).
        for op in ops {
            self.apply_patch_op(op)?;
        }

        for op in stream_ops {
            self.apply_stream_op(op);
        }

        Ok(())
    }

    /// The hydrated projection of a node **as this transaction has it**, before
    /// it is committed. The one thing a caller inspects mid-transaction, and
    /// only for drift validation (§4.4).
    pub fn to_hydrated(&self, id: NodeId) -> Option<Value> {
        self.arena_ref().to_hydrated_deep(id)
    }

    /// The root's id, so a drift check can name the node it validates.
    pub fn root_id(&self) -> NodeId {
        self.arena_ref().root
    }

    /// Settle the dirty set bottom-up, diff, bump revisions, collect
    /// subscribers, release the lock. Nothing here can fail.
    #[must_use = "dropping the Notify is what runs the subscribers"]
    pub fn commit(mut self) -> Notify {
        // Taking the guard is what tells `Drop` the work was kept.
        let mut guard = self.arena.take().expect(OPEN);
        let arena = &mut *guard;
        let mut journal = std::mem::take(&mut self.journal);

        let mut changed = Vec::new();
        let mut changed_set = HashSet::new();

        // 1–2. Settle bottom-up, then diff against the value recorded when this
        //      transaction first reached the node.
        //
        //      Depth is measured **here**, against the tree as committed, not
        //      when the node was dirtied: reconciliation re-parents nodes (§3.2
        //      adoption, §3.1 carry-over), and a stale depth would settle an
        //      ancestor before its child and cache a value built from the
        //      child's superseded one.
        let mut order = std::mem::take(&mut journal.settle_order);
        let mut depths = HashMap::new();

        order.sort_by_key(|id| Reverse(depth_of(arena, *id, &mut depths)));

        for id in order {
            if !arena.nodes.contains_key(id) {
                continue;
            }

            let settled = arena.semantic_shallow(id);
            let old = &journal.settle[&id];

            if settled == *old {
                // Restore the *old* `Arc` so an ancestor's comparison keeps
                // stopping at pointer equality, and leave the revision alone.
                arena.nodes[id].semantic = old.clone();
            } else {
                let node = &mut arena.nodes[id];

                node.semantic = settled;
                node.revision += 1;

                if changed_set.insert(id) {
                    changed.push(id);
                }
            }
        }

        // A node that left the tree is notified once and then freed (§9.3).
        let removed = Self::removed_nodes(arena, &journal);

        for id in &removed {
            if changed_set.insert(*id) {
                arena.nodes[*id].revision += 1;
                changed.push(*id);
            }
        }

        // 3. Collect. A collection's edits are dropped unless the collection
        //    itself changed — a list rewritten into what it already was edited
        //    nothing (§2.3).
        let edits = journal
            .edits
            .into_iter()
            .filter(|(id, _)| changed_set.contains(id))
            .collect();
        let mut owed = Vec::new();

        for id in &changed {
            let Some(node) = arena.nodes.get(*id) else {
                continue;
            };
            let change = Change {
                revision: node.revision,
            };

            for (_, callback) in &node.subscribers {
                owed.push(Owed {
                    node: *id,
                    change,
                    callback: callback.clone(),
                });
            }
        }

        for id in removed {
            arena.free(id);
        }

        // 4. Release the lock, and only then hand back what is owed.
        drop(guard);

        Notify::new(ChangeSet::new(changed, edits), owed)
    }

    /// Empties the root and refuses every later transaction. Only
    /// [`StateTree::close`](crate::StateTree::close) calls this.
    pub(crate) fn close(&mut self) {
        let root = self.arena().root;

        self.reconcile(root, &Value::Null, &StoreId::root());

        if !self.arena().closed {
            self.journal.closed = true;
            self.arena().closed = true;
        }
    }

    /// Every node that leaves the tree at this commit, children before parents
    /// and each one exactly once.
    fn removed_nodes(arena: &Arena, journal: &Journal) -> Vec<NodeId> {
        let mut roots = Vec::new();
        let mut seen = HashSet::new();

        for id in &journal.pending {
            // A node that was adopted back out of the pending set is not
            // removed at all.
            if journal.pending_set.contains(id) && seen.insert(*id) {
                roots.push(*id);
            }
        }

        for key in &journal.carry_order {
            if let Some(id) = journal.carry.get(key) {
                if seen.insert(*id) {
                    roots.push(*id);
                }
            }
        }

        let mut removed = Vec::new();

        for root in roots {
            arena.subtree_post_order(root, &mut removed);
        }

        removed
    }

    // ---------------------------------------------------------------- ops

    fn apply_patch_op(&mut self, op: &PatchOp) -> Result<(), TreeError> {
        self.claimed.clear();

        match op {
            PatchOp::Add { path, value } => self.add(path, value),
            PatchOp::Remove { path } => self.remove(path),
            PatchOp::Replace { path, value } => self.replace(path, value),
        }
    }

    fn replace(&mut self, path: &str, value: &Value) -> Result<(), TreeError> {
        let tokens = pointer::tokens(path)?;

        if let Some(parent) = self.async_status_target(&tokens, path)? {
            return self.set_async_status(parent, value, path);
        }

        let node = self.walk(&tokens, path)?;
        let owner = self.owner_for(node);

        self.reconcile(node, value, &owner);

        Ok(())
    }

    /// The async node a pointer ending in `status` addresses, if any.
    ///
    /// `status` is part of an async node's **own** semantic value (§3.3), not a
    /// child node, so `/profile/status` has no node to resolve to — and the
    /// server emits exactly that op every time a task flips state. This is the
    /// one place a pointer's last token is read before it is walked.
    fn async_status_target(
        &self,
        tokens: &[String],
        path: &str,
    ) -> Result<Option<NodeId>, TreeError> {
        let Some((key, parents)) = tokens.split_last() else {
            return Ok(None);
        };

        if key != ASYNC_STATUS_KEY {
            return Ok(None);
        }

        let parent = self.walk(parents, path)?;

        Ok(matches!(self.arena_ref().nodes[parent].kind, NodeKind::Async { .. }).then_some(parent))
    }

    /// Writes one async node's status in place, keeping `result` and `reason`.
    ///
    /// Only the async node is dirtied: a `loading -> failed` flip is a change to
    /// *it*, and the result subtree it may still be carrying is untouched, so a
    /// row view subscribed under it is not woken (§3.3).
    fn set_async_status(&mut self, id: NodeId, value: &Value, path: &str) -> Result<(), TreeError> {
        let incoming = value
            .as_str()
            .and_then(AsyncStatus::from_wire)
            .ok_or_else(|| TreeError::Pointer {
                path: path.to_owned(),
                reason: "an async node\'s status must be one of loading, ok or failed",
            })?;

        let NodeKind::Async {
            status,
            result,
            reason,
        } = self.arena_ref().nodes[id].kind.clone()
        else {
            return Ok(());
        };

        if status == incoming {
            return Ok(());
        }

        self.touch_and_dirty(id);
        self.arena().nodes[id].kind = NodeKind::Async {
            status: incoming,
            result,
            reason,
        };

        Ok(())
    }

    fn add(&mut self, path: &str, value: &Value) -> Result<(), TreeError> {
        let tokens = pointer::tokens(path)?;

        // RFC 6902: `add` with an empty path replaces the whole document.
        let Some((key, parents)) = tokens.split_last() else {
            return self.replace(path, value);
        };

        let parent = self.walk(parents, path)?;
        let owner = self.arena_ref().owner_of(parent);
        let kind = self.arena_ref().nodes[parent].kind.clone();

        match kind {
            NodeKind::Object(fields) | NodeKind::Store { fields, .. } => {
                let existing = fields.get(key.as_str()).copied();

                self.put_field(parent, key, existing, value, &owner);
            }
            NodeKind::Array(children) => {
                let index = match pointer::array_index(key) {
                    Some(ArrayIndex::End) => children.len(),
                    Some(ArrayIndex::At(index)) if index <= children.len() => index,
                    _ => return Err(TreeError::Index { path: path.into() }),
                };

                self.array_add(parent, &children, index, value, &owner);
            }
            NodeKind::Async { result, reason, .. } => {
                let slot = match key.as_str() {
                    "result" => result,
                    "reason" => reason,
                    // RFC 6902 `add` onto an existing key is a replace, and
                    // `status` is the node\'s own semantics rather than a child.
                    ASYNC_STATUS_KEY => return self.set_async_status(parent, value, path),
                    _ => {
                        return Err(TreeError::Pointer {
                            path: path.into(),
                            reason: "an async node has only `result` and `reason` as children",
                        });
                    }
                };

                let settled = self.reconcile_child(parent, Some(slot), value, &owner);

                self.rewire_async_slot(parent, slot, settled);
            }
            _ => {
                return Err(TreeError::Pointer {
                    path: path.into(),
                    reason: "the parent is not a container an `add` can address",
                });
            }
        }

        Ok(())
    }

    fn remove(&mut self, path: &str) -> Result<(), TreeError> {
        let tokens = pointer::tokens(path)?;

        let Some((key, parents)) = tokens.split_last() else {
            return Err(TreeError::Pointer {
                path: path.into(),
                reason: "the document root cannot be removed",
            });
        };

        let parent = self.walk(parents, path)?;
        let kind = self.arena_ref().nodes[parent].kind.clone();

        match kind {
            NodeKind::Object(mut fields) | NodeKind::Store { mut fields, .. } => {
                let Some(child) = fields.remove(key.as_str()) else {
                    return Err(TreeError::Pointer {
                        path: path.into(),
                        reason: "no such key",
                    });
                };

                self.touch_and_dirty(parent);
                self.release(child);
                self.set_fields(parent, fields);
            }
            NodeKind::Array(children) => {
                let Some(ArrayIndex::At(index)) = pointer::array_index(key) else {
                    return Err(TreeError::Index { path: path.into() });
                };

                if index >= children.len() {
                    return Err(TreeError::Index { path: path.into() });
                }

                let owner = self.arena_ref().owner_of(parent);

                self.array_remove(parent, &children, index, &owner);
            }
            _ => {
                return Err(TreeError::Pointer {
                    path: path.into(),
                    reason: "the parent is not a container a `remove` can address",
                });
            }
        }

        Ok(())
    }

    // ----------------------------------------------------------- addressing

    /// Walks already-unescaped tokens from the root.
    fn walk(&self, tokens: &[String], path: &str) -> Result<NodeId, TreeError> {
        let arena = self.arena_ref();
        let mut node = arena.root;

        for token in tokens {
            node = match &arena.nodes[node].kind {
                NodeKind::Object(fields) | NodeKind::Store { fields, .. } => *fields
                    .get(token.as_str())
                    .ok_or_else(|| TreeError::Pointer {
                        path: path.to_owned(),
                        reason: "no such key",
                    })?,
                NodeKind::Array(children) => {
                    let Some(ArrayIndex::At(index)) = pointer::array_index(token) else {
                        return Err(TreeError::Index {
                            path: path.to_owned(),
                        });
                    };

                    *children.get(index).ok_or_else(|| TreeError::Index {
                        path: path.to_owned(),
                    })?
                }
                NodeKind::Async { result, reason, .. } => match token.as_str() {
                    "result" => *result,
                    "reason" => *reason,
                    // `status` is not a child; a pointer that *ends* there is
                    // handled before the walk, and one that descends through it
                    // has nowhere to go.
                    _ => {
                        return Err(TreeError::Pointer {
                            path: path.to_owned(),
                            reason: "an async node has only `result` and `reason` as children",
                        });
                    }
                },
                NodeKind::Collection { .. } => {
                    return Err(TreeError::Pointer {
                        path: path.to_owned(),
                        reason: "stream items are not pointer-addressable (§3.1)",
                    });
                }
                _ => {
                    return Err(TreeError::Pointer {
                        path: path.to_owned(),
                        reason: "cannot descend into a scalar",
                    });
                }
            };
        }

        Ok(node)
    }

    /// The store a node's **replacement children** belong to: the nearest
    /// enclosing store above it, since the node itself is about to be rewritten.
    fn owner_for(&self, node: NodeId) -> StoreId {
        let arena = self.arena_ref();

        arena.nodes[node]
            .parent
            .map_or_else(StoreId::root, |parent| arena.owner_of(parent))
    }

    // ---------------------------------------------------------- reconcile

    /// Rewrites one node to hold `value`, keeping every identity the incoming
    /// shape allows it to keep.
    ///
    /// A `replace` — the root's included — reconciles recursively; it never
    /// destroys and recreates, so a node whose value survives keeps its
    /// [`NodeId`], its revision and its subscribers (handoff §17).
    fn reconcile(&mut self, id: NodeId, value: &Value, owner: &StoreId) {
        let shape = classify(value);
        let Some(current) = self.arena_ref().nodes.get(id).map(|node| node.kind.clone()) else {
            return;
        };

        match (current, shape) {
            (NodeKind::Null, Shape::Null) => {}
            (NodeKind::Bool(old), Shape::Bool(new)) if old == new => {}
            (NodeKind::Number(old), Shape::Number(new)) if old == *new => {}
            (NodeKind::String(old), Shape::String(new)) if &*old == new => {}
            (NodeKind::Object(fields), Shape::Object(incoming)) => {
                self.reconcile_fields(id, &fields, incoming, None, owner);
            }
            (
                NodeKind::Store { store_id, fields },
                Shape::Store {
                    store_id: incoming_id,
                    fields: incoming,
                },
            ) if store_id == incoming_id => {
                let owner = store_id.clone();

                self.reconcile_fields(id, &fields, incoming, Some(store_id), &owner);
            }
            (NodeKind::Array(children), Shape::Array(values)) => {
                self.reconcile_array(id, &children, values, owner);
            }
            (NodeKind::Collection { name, .. }, Shape::Collection { name: incoming })
                if &*name == incoming =>
            {
                // A re-rendered marker says nothing about the items; contents
                // arrive in `stream_ops` and nowhere else (§3.1).
            }
            (NodeKind::UploadSlot { name, .. }, Shape::UploadSlot { name: incoming })
                if &*name == incoming => {}
            (
                NodeKind::Async {
                    status,
                    result,
                    reason,
                },
                Shape::Async {
                    status: incoming_status,
                    result: incoming_result,
                    reason: incoming_reason,
                },
            ) => {
                if status != incoming_status {
                    // The status is part of *this* node's semantics, so a
                    // `loading -> ok` flip notifies it even when the result is
                    // unchanged (§3.3).
                    self.touch_and_dirty(id);
                    self.arena().nodes[id].kind = NodeKind::Async {
                        status: incoming_status,
                        result,
                        reason,
                    };
                }

                let settled = self.reconcile_child(id, Some(result), incoming_result, owner);

                self.rewire_async_slot(id, result, settled);

                let settled = self.reconcile_child(id, Some(reason), incoming_reason, owner);

                self.rewire_async_slot(id, reason, settled);
            }
            _ => self.rebuild(id, value, owner),
        }
    }

    /// Installs a wholly different value into an existing node.
    ///
    /// The node itself survives — this is still a reconcile from the point of
    /// view of whoever holds a `State` on it — but none of its children do.
    fn rebuild(&mut self, id: NodeId, value: &Value, owner: &StoreId) {
        self.touch_and_dirty(id);

        for child in self.arena_ref().children(id) {
            self.release(child);
        }

        self.unregister(id);
        self.install(id, value, owner);
    }

    /// Reconciles one child slot, honouring store identity over position.
    ///
    /// Returns the node that now occupies the slot: the same one whenever the
    /// shape allowed it to be kept, and the **adopted** node when the incoming
    /// value carries the id of a store that already lives somewhere else in
    /// this tree (§3.2). The caller installs the returned id and releases the
    /// one it displaced.
    ///
    /// A store id this op has already placed is **not** adopted a second time:
    /// duplicate ids in one render are a server bug, and the second sighting
    /// becomes a new node rather than a second parent for the first one (§3.2).
    fn reconcile_child(
        &mut self,
        parent: NodeId,
        existing: Option<NodeId>,
        value: &Value,
        owner: &StoreId,
    ) -> NodeId {
        if let Shape::Store { store_id, .. } = classify(value) {
            let keeps_identity = existing.is_some_and(|node| self.is_store(node, &store_id));
            let elsewhere = self.arena_ref().stores.get(&store_id).copied();
            let duplicate = !self.claimed.insert(store_id.clone());

            if !keeps_identity && !duplicate {
                if let Some(found) = elsewhere {
                    if Some(found) != existing && self.arena_ref().nodes.contains_key(found) {
                        self.adopt(parent, found);
                        self.reconcile(found, value, owner);

                        return found;
                    }
                }
            }
        }

        match existing {
            Some(id) => {
                self.reconcile(id, value, owner);

                id
            }
            None => self.build(Some(parent), value, owner),
        }
    }

    fn reconcile_fields(
        &mut self,
        id: NodeId,
        old: &BTreeMap<Arc<str>, NodeId>,
        incoming: &Map<String, Value>,
        store: Option<StoreId>,
        owner: &StoreId,
    ) {
        let mut new = BTreeMap::new();

        for (key, value) in incoming {
            if store.is_some() && key == STORE_ID_KEY {
                continue;
            }

            let existing = old.get(key.as_str()).copied();
            let child = self.reconcile_child(id, existing, value, owner);
            // Reuse the interned key when the node already had it: one fewer
            // allocation per field per envelope.
            let key = old
                .get_key_value(key.as_str())
                .map_or_else(|| Arc::from(key.as_str()), |(key, _)| key.clone());

            new.insert(key, child);
        }

        let kept: HashSet<NodeId> = new.values().copied().collect();

        for child in old.values() {
            if !kept.contains(child) {
                self.release_if_still_mine(id, *child);
            }
        }

        if &new != old {
            self.touch_and_dirty(id);

            let kind = match store {
                Some(store_id) => NodeKind::Store {
                    store_id,
                    fields: new,
                },
                None => NodeKind::Object(new),
            };

            self.arena().nodes[id].kind = kind;
        }
    }

    /// Index identity, verbatim (§9.1): position *k* holds whatever the server
    /// put at position *k*, and a subscriber on that position is told when the
    /// value there changes.
    fn reconcile_array(&mut self, id: NodeId, old: &[NodeId], values: &[Value], owner: &StoreId) {
        let mut new = Vec::with_capacity(values.len());
        let mut claimed = HashSet::new();

        for (index, value) in values.iter().enumerate() {
            let existing = unclaimed(old.get(index).copied(), &claimed);
            let child = self.reconcile_child(id, existing, value, owner);

            claimed.insert(child);
            new.push(child);
        }

        self.install_array(id, old, new);
    }

    /// `add /list/i`: every later index takes the value of its predecessor, and
    /// the array grows by one at the end.
    ///
    /// The shift is a positional rewrite rather than a `Vec::insert` of the new
    /// node, because index **is** the identity (§9.1): a view bound to index 1
    /// must be told that index 1 now holds something else, not silently follow
    /// the element that moved.
    ///
    /// The rewrite runs **right to left**. Each position takes the value of its
    /// predecessor, so left to right would overwrite a predecessor that is a
    /// store node before the slot it moves into ever gets to adopt it — and
    /// §3.2 promises a store that moved keeps its `NodeId`, its subtree and its
    /// subscribers.
    fn array_add(
        &mut self,
        id: NodeId,
        old: &[NodeId],
        index: usize,
        value: &Value,
        owner: &StoreId,
    ) {
        // What every position from `index + 1` on will hold: each old element
        // from `index`, pushed one slot right.
        let shifted: Vec<Value> = old[index..]
            .iter()
            .map(|child| self.arena_ref().semantic_deep(*child).to_wire())
            .collect();

        let mut tail = Vec::with_capacity(shifted.len());
        let mut claimed = HashSet::new();

        for (offset, moved) in shifted.iter().enumerate().rev() {
            let existing = unclaimed(old.get(index + 1 + offset).copied(), &claimed);
            let settled = self.reconcile_child(id, existing, moved, owner);

            claimed.insert(settled);
            tail.push(settled);
        }

        tail.reverse();

        // Whatever was at `index` has by now moved to `index + 1` if it is a
        // store; the inserted value gets a node of its own in that case.
        let displaced = unclaimed(old.get(index).copied(), &claimed);
        let inserted = self.reconcile_child(id, displaced, value, owner);
        let mut new = old[..index].to_vec();

        new.push(inserted);
        new.extend(tail);

        self.install_array(id, old, new);
    }

    /// `remove /list/i`: every later index takes the value of its successor,
    /// and the array shrinks by one at the end.
    ///
    /// Left to right here, for the mirror of [`array_add`](Self::array_add)'s
    /// reason: each position takes the value of its **successor**, which this
    /// pass has not reached yet.
    fn array_remove(&mut self, id: NodeId, old: &[NodeId], index: usize, owner: &StoreId) {
        let shifted: Vec<Value> = old[index + 1..]
            .iter()
            .map(|child| self.arena_ref().semantic_deep(*child).to_wire())
            .collect();

        let mut new = old[..index].to_vec();
        let mut claimed = HashSet::new();

        for (offset, child) in old[index..old.len() - 1].iter().enumerate() {
            let existing = unclaimed(Some(*child), &claimed);
            let settled = self.reconcile_child(id, existing, &shifted[offset], owner);

            claimed.insert(settled);
            new.push(settled);
        }

        self.install_array(id, old, new);
    }

    /// Installs a rebuilt child list, releasing whatever fell out of it.
    fn install_array(&mut self, id: NodeId, old: &[NodeId], new: Vec<NodeId>) {
        let kept: HashSet<NodeId> = new.iter().copied().collect();

        for child in old {
            if !kept.contains(child) {
                self.release_if_still_mine(id, *child);
            }
        }

        if new != old {
            self.touch_and_dirty(id);
            self.arena().nodes[id].kind = NodeKind::Array(new);
        }
    }

    /// Writes back an object-shaped kind, preserving whether it is a store.
    fn set_fields(&mut self, id: NodeId, fields: BTreeMap<Arc<str>, NodeId>) {
        let kind = match self.arena_ref().nodes[id].kind.clone() {
            NodeKind::Store { store_id, .. } => NodeKind::Store { store_id, fields },
            _ => NodeKind::Object(fields),
        };

        self.arena().nodes[id].kind = kind;
    }

    /// Installs one field, reconciling into the node already under that key.
    fn put_field(
        &mut self,
        parent: NodeId,
        key: &str,
        existing: Option<NodeId>,
        value: &Value,
        owner: &StoreId,
    ) {
        let child = self.reconcile_child(parent, existing, value, owner);

        if existing == Some(child) {
            return;
        }

        if let Some(displaced) = existing {
            self.release_if_still_mine(parent, displaced);
        }

        let mut fields = match self.arena_ref().nodes[parent].kind.clone() {
            NodeKind::Object(fields) | NodeKind::Store { fields, .. } => fields,
            _ => return,
        };

        fields.insert(Arc::from(key), child);

        self.touch_and_dirty(parent);
        self.set_fields(parent, fields);
    }

    /// Points an async node's `result` or `reason` at the node reconciliation
    /// settled on, when adoption moved it.
    fn rewire_async_slot(&mut self, id: NodeId, was: NodeId, now: NodeId) {
        if was == now {
            return;
        }

        let NodeKind::Async {
            status,
            result,
            reason,
        } = self.arena_ref().nodes[id].kind.clone()
        else {
            return;
        };

        self.touch_and_dirty(id);
        self.release(was);
        self.arena().nodes[id].kind = NodeKind::Async {
            status,
            result: if result == was { now } else { result },
            reason: if reason == was { now } else { reason },
        };
    }

    // ------------------------------------------------------------ stream ops

    /// Folds one stream op onto its collection node.
    ///
    /// `(store_id, stream)` resolves through the tree's own collection index —
    /// never through a JSON pointer, because no pointer can address a stream
    /// item (§3.1). An op naming a slot this render does not have is dropped:
    /// Musubi refuses a render that omits a declared stream, and a store that
    /// unmounted in the same cycle took its collection with it, so the two
    /// clients still materialize identically.
    fn apply_stream_op(&mut self, op: &StreamOp) {
        let key = match op {
            StreamOp::Reset { stream, store_id }
            | StreamOp::Insert {
                stream, store_id, ..
            }
            | StreamOp::Delete {
                stream, store_id, ..
            } => (store_id.clone(), stream.clone()),
        };

        let Some(collection) = self.arena_ref().collections.get(&key).copied() else {
            return;
        };

        if !self.arena_ref().nodes.contains_key(collection) {
            return;
        }

        match op {
            StreamOp::Reset { .. } => self.stream_reset(collection),
            StreamOp::Delete { item_key, .. } => self.stream_delete(collection, item_key),
            StreamOp::Insert {
                item_key,
                at,
                item,
                limit,
                ..
            } => self.stream_insert(collection, item_key, item, *at, *limit),
        }
    }

    fn stream_reset(&mut self, collection: NodeId) {
        let items = self.items(collection);

        self.touch_and_dirty(collection);

        for (key, node) in items {
            self.carry_out(collection, &key, node);
        }

        self.set_items(collection, Vec::new());
        self.record_edit(collection, CollectionEdit::Reset);
    }

    fn stream_delete(&mut self, collection: NodeId, item_key: &str) {
        let mut items = self.items(collection);

        let Some(index) = items.iter().position(|(key, _)| &**key == item_key) else {
            return;
        };

        let (key, node) = items.remove(index);

        self.touch_and_dirty(collection);
        self.carry_out(collection, &key, node);
        self.set_items(collection, items);
        self.record_edit(
            collection,
            CollectionEdit::Removed {
                item_key: key,
                index,
            },
        );
    }

    /// Upsert, then position, then trim — in that exact order, byte-for-byte
    /// with `packages/client/src/streams.ts` (§3.1).
    ///
    /// The upsert half keeps the **node**: an insert for a key the list already
    /// holds reconciles the item's value into the node that key already had, so
    /// a `State` bound to that row survives, keeps its subscribers, and is told
    /// only about the fields that actually moved.
    fn stream_insert(
        &mut self,
        collection: NodeId,
        item_key: &str,
        item: &Value,
        at: i64,
        limit: Option<i64>,
    ) {
        let owner = match &self.arena_ref().nodes[collection].kind {
            NodeKind::Collection { owner, .. } => owner.clone(),
            _ => return,
        };
        let mut items = self.items(collection);

        self.touch_and_dirty(collection);

        // Remove first: the index is resolved against the post-removal length.
        let from = items.iter().position(|(key, _)| &**key == item_key);
        let mut existing = from.map(|index| items.remove(index));

        if existing.is_none() {
            // §3.1's carry-over: a `reset` followed by re-inserts is the most
            // common refresh on the wire, and it must behave like the keyed
            // diff it is rather than destroying every row.
            let carried = self
                .journal
                .carry
                .remove(&(collection, item_key.to_owned()));

            existing = carried.map(|node| (Arc::from(item_key), node));
        }

        let index = insertion_index(at, items.len());
        let entry = match existing {
            Some((key, node)) => {
                self.touch(node);
                self.arena().nodes[node].parent = Some(collection);
                self.reconcile(node, item, &owner);

                (key, node)
            }
            None => {
                let node = self.build(Some(collection), item, &owner);

                (Arc::from(item_key), node)
            }
        };

        items.insert(index, entry.clone());

        match from {
            Some(previous) if previous != index => self.record_edit(
                collection,
                CollectionEdit::Moved {
                    item_key: entry.0,
                    from: previous,
                    to: index,
                },
            ),
            // Same key, same position: whatever changed is inside the item, and
            // an item's own fields are not a collection edit (§2.3).
            Some(_) => {}
            None => self.record_edit(
                collection,
                CollectionEdit::Inserted {
                    item_key: entry.0,
                    index,
                    node: entry.1,
                },
            ),
        }

        self.trim(collection, &mut items, limit, at);
        self.set_items(collection, items);
    }

    /// Trims to `limit`, dropping from the end for `at == 0` and from the front
    /// otherwise.
    ///
    /// The direction is chosen by `at`, **not** by the sign of `limit`: the
    /// server writes negative limits (`-100`) by convention and the client does
    /// not read that sign.
    fn trim(
        &mut self,
        collection: NodeId,
        items: &mut Vec<(Arc<str>, NodeId)>,
        limit: Option<i64>,
        at: i64,
    ) {
        let Some(limit) = limit else {
            return;
        };

        let size = usize::try_from(limit.unsigned_abs()).unwrap_or(usize::MAX);

        while items.len() > size {
            let index = if size > 0 && at == 0 {
                items.len() - 1
            } else {
                0
            };
            let (key, node) = items.remove(index);

            self.carry_out(collection, &key, node);
            self.record_edit(
                collection,
                CollectionEdit::Removed {
                    item_key: key,
                    index,
                },
            );
        }
    }

    /// Detaches one item and files it under the carry-over table.
    fn carry_out(&mut self, collection: NodeId, item_key: &str, node: NodeId) {
        self.touch(node);
        self.arena().nodes[node].parent = None;

        let key = (collection, item_key.to_owned());

        if let Some(displaced) = self.journal.carry.insert(key.clone(), node) {
            // Two carry-outs of one key inside one transaction: the older node
            // can no longer be claimed, so it is an ordinary removal.
            self.mark_pending(displaced);
        }

        self.journal.carry_order.push(key);
    }

    fn items(&self, collection: NodeId) -> Vec<(Arc<str>, NodeId)> {
        match &self.arena_ref().nodes[collection].kind {
            NodeKind::Collection { items, .. } => items.clone(),
            _ => Vec::new(),
        }
    }

    fn set_items(&mut self, collection: NodeId, items: Vec<(Arc<str>, NodeId)>) {
        let NodeKind::Collection { name, owner, .. } =
            self.arena_ref().nodes[collection].kind.clone()
        else {
            return;
        };

        self.arena().nodes[collection].kind = NodeKind::Collection { name, owner, items };
    }

    fn record_edit(&mut self, collection: NodeId, edit: CollectionEdit) {
        self.journal.edits.entry(collection).or_default().push(edit);
    }

    // --------------------------------------------------------- node lifecycle

    /// Creates a node and its whole subtree from one wire value.
    ///
    /// A node a transaction created starts at revision `1`: it *was* touched by
    /// a transaction, which is exactly what `revision() == 0` denies (§9.3).
    fn build(&mut self, parent: Option<NodeId>, value: &Value, owner: &StoreId) -> NodeId {
        let id = self.arena().nodes.insert(NodeData {
            parent,
            kind: NodeKind::Null,
            revision: 1,
            semantic: SemanticValue::new(Semantic::Null),
            subscribers: Vec::new(),
        });

        self.journal.allocated.push(id);
        self.install(id, value, owner);

        id
    }

    /// Gives an existing node the kind, children and indices `value` calls for.
    fn install(&mut self, id: NodeId, value: &Value, owner: &StoreId) {
        let kind = match classify(value) {
            Shape::Null => NodeKind::Null,
            Shape::Bool(flag) => NodeKind::Bool(flag),
            Shape::Number(number) => NodeKind::Number(number.clone()),
            Shape::String(text) => NodeKind::String(Arc::from(text)),
            Shape::Array(values) => {
                let mut children = Vec::with_capacity(values.len());

                for value in values {
                    children.push(self.reconcile_child(id, None, value, owner));
                }

                NodeKind::Array(children)
            }
            Shape::Object(fields) => {
                let mut children = BTreeMap::new();

                for (key, value) in fields {
                    let child = self.reconcile_child(id, None, value, owner);

                    children.insert(Arc::from(key.as_str()), child);
                }

                NodeKind::Object(children)
            }
            Shape::Store { store_id, fields } => {
                let mut children = BTreeMap::new();

                for (key, value) in fields {
                    if key == STORE_ID_KEY {
                        continue;
                    }

                    let child = self.reconcile_child(id, None, value, &store_id);

                    children.insert(Arc::from(key.as_str()), child);
                }

                self.set_store(store_id.clone(), Some(id));

                NodeKind::Store {
                    store_id,
                    fields: children,
                }
            }
            Shape::Collection { name } => {
                self.set_collection((owner.clone(), name.to_owned()), Some(id));

                NodeKind::Collection {
                    name: Arc::from(name),
                    owner: owner.clone(),
                    items: Vec::new(),
                }
            }
            Shape::Async {
                status,
                result,
                reason,
            } => NodeKind::Async {
                status,
                result: self.reconcile_child(id, None, result, owner),
                reason: self.reconcile_child(id, None, reason, owner),
            },
            Shape::UploadSlot { name } => NodeKind::UploadSlot {
                name: Arc::from(name),
                owner: owner.clone(),
            },
        };

        self.arena().nodes[id].kind = kind;

        let semantic = self.arena_ref().semantic_shallow(id);

        self.arena().nodes[id].semantic = semantic;
    }

    /// Drops a node's index entries, for a node about to stop being what it was.
    fn unregister(&mut self, id: NodeId) {
        match self.arena_ref().nodes[id].kind.clone() {
            NodeKind::Store { store_id, .. } => {
                if self.arena_ref().stores.get(&store_id) == Some(&id) {
                    self.set_store(store_id, None);
                }
            }
            NodeKind::Collection { name, owner, .. } => {
                let key = (owner, name.to_string());

                if self.arena_ref().collections.get(&key) == Some(&id) {
                    self.set_collection(key, None);
                }
            }
            _ => {}
        }
    }

    /// Re-parents an existing store node, resurrecting it if this transaction
    /// had already detached it.
    fn adopt(&mut self, parent: NodeId, node: NodeId) {
        self.touch(node);
        self.unpend(node);

        if let Some(old_parent) = self.arena_ref().nodes[node].parent {
            self.detach(old_parent, node);
        }

        self.arena().nodes[node].parent = Some(parent);
    }

    /// Removes one child from its parent's kind, so no node is ever reachable
    /// from two parents.
    fn detach(&mut self, parent: NodeId, child: NodeId) {
        let Some(kind) = self
            .arena_ref()
            .nodes
            .get(parent)
            .map(|node| node.kind.clone())
        else {
            return;
        };

        self.touch_and_dirty(parent);

        let kind = match kind {
            NodeKind::Object(mut fields) => {
                fields.retain(|_, node| *node != child);

                NodeKind::Object(fields)
            }
            NodeKind::Store {
                store_id,
                mut fields,
            } => {
                fields.retain(|_, node| *node != child);

                NodeKind::Store { store_id, fields }
            }
            NodeKind::Array(mut children) => {
                children.retain(|node| *node != child);

                NodeKind::Array(children)
            }
            NodeKind::Collection {
                name,
                owner,
                mut items,
            } => {
                items.retain(|(_, node)| *node != child);

                NodeKind::Collection { name, owner, items }
            }
            NodeKind::Async {
                status,
                result,
                reason,
            } => {
                let owner = self.arena_ref().owner_of(parent);
                let filler = self.build(Some(parent), &Value::Null, &owner);

                NodeKind::Async {
                    status,
                    result: if result == child { filler } else { result },
                    reason: if reason == child { filler } else { reason },
                }
            }
            other => other,
        };

        self.arena().nodes[parent].kind = kind;
    }

    /// Detaches one node from the tree and files it for freeing at commit.
    fn release(&mut self, id: NodeId) {
        if !self.mark_pending(id) {
            return;
        }

        self.touch(id);
        self.arena().nodes[id].parent = None;
    }

    /// Releases a child **unless** something else in this transaction adopted
    /// it: a store node that moved is not a store node that was removed (§3.2).
    fn release_if_still_mine(&mut self, parent: NodeId, child: NodeId) {
        let owner = self
            .arena_ref()
            .nodes
            .get(child)
            .and_then(|node| node.parent);

        if owner == Some(parent) {
            self.release(child);
        }
    }

    fn mark_pending(&mut self, id: NodeId) -> bool {
        if !self.journal.pending_set.insert(id) {
            return false;
        }

        self.journal.pending.push(id);

        true
    }

    /// Un-files a node an adoption claimed back.
    fn unpend(&mut self, id: NodeId) {
        self.journal.pending_set.remove(&id);
        self.journal.carry.retain(|_, node| *node != id);
    }

    fn is_store(&self, id: NodeId, store_id: &StoreId) -> bool {
        match self.arena_ref().nodes.get(id) {
            Some(node) => matches!(
                &node.kind,
                NodeKind::Store { store_id: existing, .. } if existing == store_id
            ),
            None => false,
        }
    }

    fn set_store(&mut self, store_id: StoreId, node: Option<NodeId>) {
        let previous = match node {
            Some(node) => self.arena().stores.insert(store_id.clone(), node),
            None => self.arena().stores.remove(&store_id),
        };

        self.journal
            .index_undo
            .push(IndexUndo::Store(store_id, previous));
    }

    fn set_collection(&mut self, key: CollectionKey, node: Option<NodeId>) {
        let previous = match node {
            Some(node) => self.arena().collections.insert(key.clone(), node),
            None => self.arena().collections.remove(&key),
        };

        self.journal
            .index_undo
            .push(IndexUndo::Collection(key, previous));
    }

    // --------------------------------------------------------------- journal

    /// Records what one node looked like before this transaction changed it.
    /// Only the **first** touch is kept: that is the value §9.2 diffs against,
    /// which is what makes `1 -> 2 -> 1` notify nobody.
    fn touch(&mut self, id: NodeId) {
        if self.journal.touched_set.contains(&id) {
            return;
        }

        let arena = self.arena.as_ref().expect(OPEN);
        let Some(node) = arena.nodes.get(id) else {
            return;
        };
        let entry = Touched {
            id,
            parent: node.parent,
            kind: node.kind.clone(),
            revision: node.revision,
            semantic: node.semantic.clone(),
        };

        self.journal.touched_set.insert(id);
        self.journal.touched.push(entry);
    }

    /// Puts a node and every ancestor of it into the settle set, each with the
    /// value it had when this transaction first reached it.
    ///
    /// The walk runs all the way to the root every time rather than stopping at
    /// the first node already in the set: a node that is re-parented after it
    /// was dirtied has ancestors that nothing else put there, and stopping early
    /// would leave them holding a value built from this node's superseded one.
    /// Only the **first** entry for a node is kept — that is the value §9.2
    /// diffs against.
    fn mark_dirty(&mut self, id: NodeId) {
        let mut cursor = Some(id);

        while let Some(current) = cursor {
            let Some(node) = self.arena.as_ref().expect(OPEN).nodes.get(current) else {
                break;
            };

            cursor = node.parent;

            if let Entry::Vacant(slot) = self.journal.settle.entry(current) {
                slot.insert(node.semantic.clone());
                self.journal.settle_order.push(current);
            }
        }
    }

    fn touch_and_dirty(&mut self, id: NodeId) {
        self.touch(id);
        self.mark_dirty(id);
    }

    fn arena(&mut self) -> &mut Arena {
        self.arena.as_mut().expect(OPEN)
    }

    fn arena_ref(&self) -> &Arena {
        self.arena.as_ref().expect(OPEN)
    }
}

impl Drop for Transaction<'_> {
    /// Replays the journal backwards: restores every mutated node's kind,
    /// semantic value and revision, and frees every node the transaction
    /// allocated. O(diff), not O(tree).
    fn drop(&mut self) {
        let journal = std::mem::take(&mut self.journal);
        let Some(arena) = self.arena.as_mut() else {
            // `commit` took the guard; there is nothing to undo.
            return;
        };

        for id in journal.allocated.iter().rev() {
            arena.nodes.remove(*id);
        }

        for touched in journal.touched.iter().rev() {
            if let Some(node) = arena.nodes.get_mut(touched.id) {
                node.parent = touched.parent;
                node.kind = touched.kind.clone();
                node.revision = touched.revision;
                node.semantic = touched.semantic.clone();
            }
        }

        for undo in journal.index_undo.iter().rev() {
            match undo {
                IndexUndo::Store(store_id, previous) => match previous {
                    Some(node) => {
                        arena.stores.insert(store_id.clone(), *node);
                    }
                    None => {
                        arena.stores.remove(store_id);
                    }
                },
                IndexUndo::Collection(key, previous) => match previous {
                    Some(node) => {
                        arena.collections.insert(key.clone(), *node);
                    }
                    None => {
                        arena.collections.remove(key);
                    }
                },
            }
        }

        if journal.closed {
            arena.closed = false;
        }
    }
}

/// The node a position may reconcile into: the one that was there, unless
/// another position in this same rebuild has already taken it.
///
/// Adoption moves a node **within** an array as readily as into one (§3.2), and
/// reconciling a node that has already taken a new position would overwrite the
/// value it was just given.
fn unclaimed(existing: Option<NodeId>, claimed: &HashSet<NodeId>) -> Option<NodeId> {
    existing.filter(|node| !claimed.contains(node))
}

/// How deep a node sits in the tree **as it now stands**, memoized per commit.
///
/// A detached node answers `0`, which sorts it last: nothing above it is
/// waiting on its value, and its own children still answer `1` and settle first.
fn depth_of(arena: &Arena, id: NodeId, memo: &mut HashMap<NodeId, u32>) -> u32 {
    if let Some(depth) = memo.get(&id) {
        return *depth;
    }

    let depth = match arena.nodes.get(id).and_then(|node| node.parent) {
        Some(parent) => depth_of(arena, parent, memo).saturating_add(1),
        None => 0,
    };

    memo.insert(id, depth);

    depth
}

/// Resolves `at` against the **post-removal** length (§3.1).
fn insertion_index(at: i64, len: usize) -> usize {
    if at <= 0 {
        // -1 appends; 0 and every other negative prepend.
        if at == -1 { len } else { 0 }
    } else {
        usize::try_from(at).unwrap_or(usize::MAX).min(len)
    }
}
