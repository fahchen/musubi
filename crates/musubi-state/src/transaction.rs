//! One transaction against the retained tree (`docs/rust-reactive-state.md`
//! §2.3, §3.1, §3.6, §9.2).
//!
//! One server message is one transaction. The journal is a drop guard: every
//! mutation records what it displaced, `commit` is the only way to keep the
//! work, and a panic mid-transaction unwinds through the rollback rather than
//! leaving the tree half-applied. Rollback is O(diff), not O(tree) — which
//! makes atomicity **cheaper** than v1's, where it cost one whole-tree clone
//! per envelope.
//!
//! # One write path
//!
//! Every op that puts a value somewhere — `add`, `replace`, a rewritten store
//! marker, an array shift — reaches the tree through
//! [`reconcile_child`](Transaction::reconcile_child), which is the one place
//! that knows a value's store id may name a node that already exists somewhere
//! else. An op path that reconciles a node directly would not consult the store
//! index at all, and §3.2's promise (a child store that moved keeps its
//! `NodeId`, its subtree, its stream collections and its subscribers) would hold
//! only for the op shapes that happened to go through the child-level path.

use std::cmp::Reverse;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, MutexGuard};

use serde_json::{Map, Value};

use crate::arena::{Arena, CollectionKey, MAX_DEPTH, NodeData};
use crate::change::{Change, ChangeSet, CollectionEdit, Notify, Owed};
use crate::error::TreeError;
use crate::marker::{ASYNC_STATUS_KEY, STORE_ID_KEY, Shape, classify, parse_store_id};
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

/// What a write path found under a pointer: the half of a [`NodeKind`] that
/// decides where a value goes, without the half that holds the children.
enum Container {
    /// An object or a child store: keyed.
    Fields,
    /// A plain JSON array: positional.
    Array,
    /// An async node, whose two slots are not in a map at all.
    Async { result: NodeId, reason: NodeId },
    /// A stream slot. Addressable by no pointer (§3.1).
    Collection,
    /// A scalar, or an upload slot: nothing to descend into.
    Leaf,
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
    /// would alias one node under two parents (§3.2). A positional rewrite adds
    /// the ids it is going to leave standing where they are, for the same
    /// reason.
    ///
    /// Scoped to the op — patch op or stream op — not to the transaction: two
    /// ops in one envelope may legitimately move the same store twice.
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
            self.apply_stream_op(op)?;
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

        // `Null` allocates nothing, so the depth cap cannot refuse this.
        let _ = self.reconcile(root, &Value::Null, &StoreId::root(), 0);

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
        let mut walked = HashSet::new();

        for root in roots {
            arena.subtree_post_order(root, &mut removed, &mut walked);
        }

        removed
    }

    // ---------------------------------------------------------------- ops

    fn apply_patch_op(&mut self, op: &PatchOp) -> Result<(), TreeError> {
        self.claimed.clear();

        let path = match op {
            PatchOp::Add { path, .. }
            | PatchOp::Remove { path }
            | PatchOp::Replace { path, .. } => path,
        };
        let tokens = pointer::tokens(path)?;

        // A pointer into the store marker addresses a node's identity rather
        // than one of its children (§3.2).
        if let Some(at) = tokens.iter().position(|token| token == STORE_ID_KEY) {
            return self.marker_op(op, &tokens, at, path);
        }

        match op {
            PatchOp::Add { value, .. } => self.add(&tokens, value, path),
            PatchOp::Remove { .. } => self.remove(&tokens, path),
            PatchOp::Replace { value, .. } => self.replace(&tokens, value, path),
        }
    }

    fn replace(&mut self, tokens: &[String], value: &Value, path: &str) -> Result<(), TreeError> {
        if let Some(parent) = self.async_status_target(tokens, path)? {
            return self.set_async_status(parent, value, path);
        }

        let Some((key, parents)) = tokens.split_last() else {
            // The document root. It has no slot to be written into: its
            // `NodeId` is fixed for the tree's life (§2.2), so it is the one
            // node reconciled in place whatever the incoming value is.
            let root = self.arena_ref().root;

            return self.reconcile(root, value, &StoreId::root(), 0);
        };

        let parent = self.walk(parents, path)?;

        self.write_slot(parent, key, value, parents.len(), path)
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

    fn add(&mut self, tokens: &[String], value: &Value, path: &str) -> Result<(), TreeError> {
        // RFC 6902: `add` with an empty path replaces the whole document.
        let Some((key, parents)) = tokens.split_last() else {
            return self.replace(tokens, value, path);
        };

        let parent = self.walk(parents, path)?;
        let owner = self.arena_ref().owner_of(parent);
        let depth = parents.len() + 1;

        // Which container this is, read under a borrow: writing one key must not
        // copy the sibling map just to find out what it is writing into.
        match self.container_of(parent) {
            Container::Fields => self.put_field(parent, key, value, &owner, depth),
            Container::Array => {
                // For an array the child list *is* the kind, so this snapshot is
                // the one the rewrite needs rather than a copy of anything else.
                let children = self.arena_ref().ordered_children(parent);
                let index = match pointer::array_index(key) {
                    Some(ArrayIndex::End) => children.len(),
                    Some(ArrayIndex::At(index)) if index <= children.len() => index,
                    _ => return Err(TreeError::Index { path: path.into() }),
                };

                self.array_add(parent, &children, index, value, &owner, depth)
            }
            Container::Async { result, reason } => {
                let slot = match key.as_str() {
                    "result" => result,
                    "reason" => reason,
                    // RFC 6902 `add` onto an existing key is a replace, and
                    // `status` is the node's own semantics rather than a child.
                    ASYNC_STATUS_KEY => return self.set_async_status(parent, value, path),
                    _ => {
                        return Err(TreeError::Pointer {
                            path: path.into(),
                            reason: "an async node has only `result` and `reason` as children",
                        });
                    }
                };

                let settled = self.reconcile_child(parent, Some(slot), value, &owner, depth)?;

                self.rewire_async_slot(parent, slot, settled);

                Ok(())
            }
            Container::Collection | Container::Leaf => Err(TreeError::Pointer {
                path: path.into(),
                reason: "the parent is not a container an `add` can address",
            }),
        }
    }

    fn remove(&mut self, tokens: &[String], path: &str) -> Result<(), TreeError> {
        let Some((key, parents)) = tokens.split_last() else {
            return Err(TreeError::Pointer {
                path: path.into(),
                reason: "the document root cannot be removed",
            });
        };

        let parent = self.walk(parents, path)?;

        match self.container_of(parent) {
            Container::Fields => {
                let Some(child) = self.arena_ref().child_by_key(parent, key) else {
                    return Err(TreeError::Pointer {
                        path: path.into(),
                        reason: "no such key",
                    });
                };

                // `touch` takes the snapshot rollback replays — the one copy of
                // this map the op makes — and the key is then dropped from the
                // node's own storage rather than from a second copy of it.
                self.touch_and_dirty(parent);
                self.release(child);
                self.remove_field(parent, key);

                Ok(())
            }
            Container::Array => {
                let Some(ArrayIndex::At(index)) = pointer::array_index(key) else {
                    return Err(TreeError::Index { path: path.into() });
                };
                let children = self.arena_ref().ordered_children(parent);

                if index >= children.len() {
                    return Err(TreeError::Index { path: path.into() });
                }

                self.array_remove(parent, &children, index);

                Ok(())
            }
            Container::Async { .. } | Container::Collection | Container::Leaf => {
                Err(TreeError::Pointer {
                    path: path.into(),
                    reason: "the parent is not a container a `remove` can address",
                })
            }
        }
    }

    // ------------------------------------------------------------ the marker

    /// One op whose pointer addresses `__musubi_store_id__`, or an element of
    /// it.
    ///
    /// `Musubi.Diff` descends into the marker like any other object key: a
    /// reordered list of child stores arrives as
    /// `replace /rows/0/__musubi_store_id__/0 "b"`, and a plain row prepended
    /// before a store arrives as `remove /rows/0/__musubi_store_id__`. The
    /// TypeScript client applies those against a plain JSON document, so they
    /// are contract-legal and this tree has to take them too. It keeps the id on
    /// the node rather than in the field map, so the op is re-expressed as what
    /// it is — a change of that node's **identity** — and routed back through
    /// the one write path, where adoption, the duplicate rule and the store
    /// index all still apply (§3.2).
    fn marker_op(
        &mut self,
        op: &PatchOp,
        tokens: &[String],
        at: usize,
        path: &str,
    ) -> Result<(), TreeError> {
        let (prefix, rest) = (&tokens[..at], &tokens[at + 1..]);
        let node = self.walk(prefix, path)?;
        let segments = match &self.arena_ref().nodes[node].kind {
            NodeKind::Store { store_id, .. } => store_id.as_slice().to_vec(),
            NodeKind::Object(_) => Vec::new(),
            _ => {
                return Err(TreeError::Pointer {
                    path: path.to_owned(),
                    reason: "only an object or a child store carries a store id",
                });
            }
        };
        let marker = rewrite_store_id(op, rest, segments, path)?;
        let Value::Object(mut object) = self.arena_ref().semantic_deep(node).to_wire() else {
            return Err(TreeError::Pointer {
                path: path.to_owned(),
                reason: "only an object or a child store carries a store id",
            });
        };

        match marker {
            Some(id) => object.insert(STORE_ID_KEY.to_owned(), id),
            None => object.remove(STORE_ID_KEY),
        };

        let value = Value::Object(object);
        let Some((key, parents)) = prefix.split_last() else {
            // The root's own id: the one node whose identity the tree keeps
            // across a change of store id, because its `NodeId` is what
            // `Mounted::state()` holds (§2.2).
            let root = self.arena_ref().root;

            return self.reconcile(root, &value, &StoreId::root(), 0);
        };
        let parent = self.walk(parents, path)?;

        self.write_slot(parent, key, &value, parents.len(), path)
    }

    // ----------------------------------------------------------- addressing

    /// Walks already-unescaped tokens from the root.
    ///
    /// A node reached through `n` tokens sits exactly `n` levels below the root:
    /// every token descends one node, whatever the parent's kind. That is where
    /// the write paths get the depth they check against the cap.
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

    /// Writes one value into a slot of `parent` that already holds something —
    /// what a `replace` addresses. `depth` is the parent's own depth.
    fn write_slot(
        &mut self,
        parent: NodeId,
        key: &str,
        value: &Value,
        depth: usize,
        path: &str,
    ) -> Result<(), TreeError> {
        let owner = self.arena_ref().owner_of(parent);

        match self.container_of(parent) {
            Container::Fields => {
                if self.arena_ref().child_by_key(parent, key).is_none() {
                    return Err(TreeError::Pointer {
                        path: path.to_owned(),
                        reason: "no such key",
                    });
                }

                self.put_field(parent, key, value, &owner, depth + 1)
            }
            Container::Array => {
                let Some(ArrayIndex::At(index)) = pointer::array_index(key) else {
                    return Err(TreeError::Index {
                        path: path.to_owned(),
                    });
                };
                let children = self.arena_ref().ordered_children(parent);

                if index >= children.len() {
                    return Err(TreeError::Index {
                        path: path.to_owned(),
                    });
                }

                self.put_index(parent, &children, index, value, &owner, depth + 1)
            }
            Container::Async { result, reason } => {
                let slot = match key {
                    "result" => result,
                    "reason" => reason,
                    _ => {
                        return Err(TreeError::Pointer {
                            path: path.to_owned(),
                            reason: "an async node has only `result` and `reason` as children",
                        });
                    }
                };
                let settled = self.reconcile_child(parent, Some(slot), value, &owner, depth + 1)?;

                self.rewire_async_slot(parent, slot, settled);

                Ok(())
            }
            Container::Collection => Err(TreeError::Pointer {
                path: path.to_owned(),
                reason: "stream items are not pointer-addressable (§3.1)",
            }),
            Container::Leaf => Err(TreeError::Pointer {
                path: path.to_owned(),
                reason: "cannot descend into a scalar",
            }),
        }
    }

    /// What kind of container a node is, and where a write into it has to go.
    ///
    /// The cheap half of a [`NodeKind`], read under a borrow. Every write path
    /// dispatches on this rather than on a clone of the kind: a one-key `add`,
    /// `remove` or `replace` against a store with a hundred fields used to copy
    /// all hundred entries of the sibling map before it knew what it was
    /// looking at.
    fn container_of(&self, id: NodeId) -> Container {
        match self.arena_ref().nodes.get(id).map(|node| &node.kind) {
            Some(NodeKind::Object(_) | NodeKind::Store { .. }) => Container::Fields,
            Some(NodeKind::Array(_)) => Container::Array,
            Some(NodeKind::Async { result, reason, .. }) => Container::Async {
                result: *result,
                reason: *reason,
            },
            Some(NodeKind::Collection { .. }) => Container::Collection,
            _ => Container::Leaf,
        }
    }

    // ---------------------------------------------------------- reconcile

    /// Rewrites one node to hold `value`, keeping every identity the incoming
    /// shape allows it to keep.
    ///
    /// A `replace` — the root's included — reconciles recursively; it never
    /// destroys and recreates, so a node whose value survives keeps its
    /// [`NodeId`], its revision and its subscribers (handoff §17).
    ///
    /// `depth` is this node's own depth, carried down so the write boundary can
    /// refuse a value that would nest past [`MAX_DEPTH`].
    fn reconcile(
        &mut self,
        id: NodeId,
        value: &Value,
        owner: &StoreId,
        depth: usize,
    ) -> Result<(), TreeError> {
        let shape = classify(value);
        let Some(node) = self.arena_ref().nodes.get(id) else {
            return Ok(());
        };

        // The cases that leave the node exactly as it stands, decided under a
        // borrow. A re-rendered stream marker is one of them — and it arrives
        // every cycle, for every stream, so cloning the kind to answer it would
        // copy every item the collection holds, per envelope.
        let unchanged = match (&node.kind, &shape) {
            (NodeKind::Null, Shape::Null) => true,
            (NodeKind::Bool(old), Shape::Bool(new)) => old == new,
            (NodeKind::Number(old), Shape::Number(new)) => old == *new,
            (NodeKind::String(old), Shape::String(new)) => &**old == *new,
            // A re-rendered marker says nothing about the items; contents
            // arrive in `stream_ops` and nowhere else (§3.1).
            (NodeKind::Collection { name, .. }, Shape::Collection { name: incoming }) => {
                &**name == *incoming
            }
            (NodeKind::UploadSlot { name, .. }, Shape::UploadSlot { name: incoming }) => {
                &**name == *incoming
            }
            _ => false,
        };

        if unchanged {
            return Ok(());
        }

        let current = self.arena_ref().nodes[id].kind.clone();

        match (current, shape) {
            (NodeKind::Object(fields), Shape::Object(incoming)) => {
                self.reconcile_fields(id, &fields, incoming, None, owner, depth)?;
            }
            (
                NodeKind::Store { store_id, fields },
                Shape::Store {
                    store_id: incoming_id,
                    fields: incoming,
                },
            ) if store_id == incoming_id => {
                let owner = store_id.clone();

                self.reconcile_fields(id, &fields, incoming, Some(store_id), &owner, depth)?;
            }
            (NodeKind::Array(children), Shape::Array(values)) => {
                self.reconcile_array(id, &children, values, owner, depth)?;
            }
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

                let settled =
                    self.reconcile_child(id, Some(result), incoming_result, owner, depth + 1)?;

                self.rewire_async_slot(id, result, settled);

                let settled =
                    self.reconcile_child(id, Some(reason), incoming_reason, owner, depth + 1)?;

                self.rewire_async_slot(id, reason, settled);
            }
            _ => self.rebuild(id, value, owner, depth)?,
        }

        Ok(())
    }

    /// Installs a wholly different value into an existing node.
    ///
    /// The node itself survives — this is still a reconcile from the point of
    /// view of whoever holds a `State` on it — but none of its children do.
    fn rebuild(
        &mut self,
        id: NodeId,
        value: &Value,
        owner: &StoreId,
        depth: usize,
    ) -> Result<(), TreeError> {
        self.touch_and_dirty(id);

        for child in self.arena_ref().children(id) {
            self.release(child);
        }

        self.unregister(id);
        self.install(id, value, owner, depth)
    }

    /// Reconciles one child slot, honouring store identity over position.
    ///
    /// Returns the node that now occupies the slot: the same one whenever the
    /// shape allowed it to be kept, the **adopted** node when the incoming value
    /// carries the id of a store that already lives somewhere else in this tree
    /// (§3.2), and a fresh one otherwise. The caller installs the returned id
    /// and releases the one it displaced.
    ///
    /// Three rules decide, in this order:
    ///
    /// 1. **A store node's identity is its id.** It is reusable for its own id
    ///    and for nothing else — not for a different id, not for a plain value.
    ///    A store that stops being rendered here unmounts (§3.2's fresh-mount
    ///    semantics) rather than being rewritten in place, which is what keeps a
    ///    live `StoreState` from silently addressing some other store's node.
    /// 2. **A store id this op has already placed is not adopted a second
    ///    time.** Duplicate ids in one render are a server bug, and the second
    ///    sighting becomes a new node rather than a second parent for the first.
    /// 3. **A node is never adopted onto its own descendant.** That would close
    ///    a parent cycle, and every walk up the tree — `mark_dirty`'s included,
    ///    which runs holding the arena lock — would never reach a root again.
    ///
    /// A collection is adopted by the same three rules, filed under
    /// `(owner, name)` rather than under a store id — see
    /// [`adopt_collection`](Self::adopt_collection).
    fn reconcile_child(
        &mut self,
        parent: NodeId,
        existing: Option<NodeId>,
        value: &Value,
        owner: &StoreId,
        depth: usize,
    ) -> Result<NodeId, TreeError> {
        match classify(value) {
            Shape::Store { store_id, .. } => {
                let duplicate = !self.claimed.insert(store_id.clone());

                if let Some(node) = existing.filter(|node| self.is_store(*node, &store_id)) {
                    self.reconcile(node, value, owner, depth)?;

                    return Ok(node);
                }

                if !duplicate {
                    let elsewhere = self.arena_ref().stores.get(&store_id).copied();

                    if let Some(found) = elsewhere {
                        if Some(found) != existing
                            && self.arena_ref().nodes.contains_key(found)
                            && !self.is_ancestor(found, parent)
                        {
                            self.adopt(parent, found);
                            self.reconcile(found, value, owner, depth)?;

                            return Ok(found);
                        }
                    }
                }

                return self.build(Some(parent), value, owner, depth);
            }
            // A stream marker whose slot does **not** already hold that very
            // collection. The guard is what keeps the hot path free: a render
            // repeats every marker it has every cycle, and that repeat must not
            // cost an index lookup to answer.
            Shape::Collection { name }
                if !existing.is_some_and(|node| self.is_collection(node, name)) =>
            {
                if let Some(found) = self.adopt_collection(parent, existing, owner, name) {
                    self.reconcile(found, value, owner, depth)?;

                    return Ok(found);
                }
            }
            _ => {}
        }

        match existing {
            // A child store the render no longer puts here: it unmounts, and
            // the plain value that replaced it gets a node of its own.
            Some(id) if self.is_store_node(id) => self.build(Some(parent), value, owner, depth),
            Some(id) => {
                self.reconcile(id, value, owner, depth)?;

                Ok(id)
            }
            None => self.build(Some(parent), value, owner, depth),
        }
    }

    /// Re-parents the collection filed under `(owner, name)` onto `parent`, if
    /// there is one to take.
    ///
    /// A collection's identity is that key, exactly as a store's identity is its
    /// id. Its items are only ever reachable through the index a stream op
    /// resolves (§3.5), and they are never re-sent: a marker is a *name*, and
    /// the wire projection of a whole stream is that same bare marker (§3.1). So
    /// a store node that is **rebuilt** rather than adopted — the newcomer
    /// §3.2's duplicate rule hands the second sighting of an id, which is the
    /// shape `Musubi.Diff` emits for a row prepended before a store row — has to
    /// take the live collection with it. Standing up an empty one instead takes
    /// the index key from the node that still holds the items, silently, and the
    /// store's stream reads as empty from that op onward even though the store
    /// never unmounted.
    ///
    /// Refused for the two cases that would stop the tree being one: a node
    /// already parented here — two markers for one key in one render is a server
    /// bug, and the second sighting builds its own rather than becoming a second
    /// parent — and a node the adoption would close a cycle through.
    fn adopt_collection(
        &mut self,
        parent: NodeId,
        existing: Option<NodeId>,
        owner: &StoreId,
        name: &str,
    ) -> Option<NodeId> {
        let key = (owner.clone(), name.to_owned());
        let found = self.arena_ref().collections.get(&key).copied()?;

        if Some(found) == existing
            || self.arena_ref().nodes.get(found)?.parent == Some(parent)
            || self.is_ancestor(found, parent)
        {
            return None;
        }

        self.adopt(parent, found);

        Some(found)
    }

    fn reconcile_fields(
        &mut self,
        id: NodeId,
        old: &BTreeMap<Arc<str>, NodeId>,
        incoming: &Map<String, Value>,
        store: Option<StoreId>,
        owner: &StoreId,
        depth: usize,
    ) -> Result<(), TreeError> {
        let mut new = BTreeMap::new();

        for (key, value) in incoming {
            if store.is_some() && key == STORE_ID_KEY {
                continue;
            }

            let existing = old.get(key.as_str()).copied();
            let child = self.reconcile_child(id, existing, value, owner, depth + 1)?;
            // Reuse the interned key when the node already had it: one fewer
            // allocation per field per envelope.
            let key = old
                .get_key_value(key.as_str())
                .map_or_else(|| Arc::from(key.as_str()), |(key, _)| key.clone());

            new.insert(key, child);
        }

        let kept: HashSet<NodeId> = new.values().copied().collect();
        let before: Vec<NodeId> = old.values().copied().collect();

        self.release_displaced(id, &before, &kept);

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

        Ok(())
    }

    /// Index identity, verbatim (§9.1): position *k* holds whatever the server
    /// put at position *k*, and a subscriber on that position is told when the
    /// value there changes.
    ///
    /// Store identity still wins over position *within* the rewrite: a position
    /// whose value names a store that lives at another index adopts it, and the
    /// index it came from is filled by whatever the incoming list puts there.
    fn reconcile_array(
        &mut self,
        id: NodeId,
        old: &[NodeId],
        values: &[Value],
        owner: &StoreId,
        depth: usize,
    ) -> Result<(), TreeError> {
        let mut new = Vec::with_capacity(values.len());
        let mut claimed = HashSet::new();

        for (index, value) in values.iter().enumerate() {
            let existing = unclaimed(old.get(index).copied(), &claimed);
            let child = self.reconcile_child(id, existing, value, owner, depth + 1)?;

            claimed.insert(child);
            new.push(child);
        }

        self.install_array(id, old, new);

        Ok(())
    }

    /// `add /list/i`: the value is **inserted** at `i`, and every element from
    /// there on keeps its node and moves one slot right.
    ///
    /// # The shift moves nodes, not values
    ///
    /// §9.1 reads the identity of a `NodeKind::Array` off the index, and a whole
    /// list `replace` still honours that to the letter (see
    /// [`reconcile_array`](Self::reconcile_array)): position *k* holds whatever
    /// the server put at position *k*. `add /list/i` is a different statement.
    /// RFC 6902 defines it as an insertion — the server's diff has already said
    /// "an element appeared here", so moving the tail is reading that statement
    /// literally rather than inferring a move from two ops.
    ///
    /// Rewriting the tail's *values* instead — reconciling each element's
    /// predecessor into it, which is what this did — cost two deep copies of the
    /// tail per op (`semantic_deep().to_wire()`, then a rebuild from the JSON it
    /// produced) with the arena lock held: 50 ops against a 20 000-element array
    /// wedged a release-mode client for two seconds. It was also **lossy**, and
    /// silently: the wire projection of a `Collection` is its bare marker,
    /// because stream contents travel in `stream_ops` and never in a value
    /// (§3.1), so a stream slot shifted through JSON came out the other side
    /// with none of its items. A store standing in the tail already moved by
    /// adoption rather than by rewrite (§3.2), so shifting the node is also what
    /// makes every element behave the same way.
    ///
    /// What a consumer sees: the array node changes and notifies, because its
    /// semantic is the ordered sequence of its children's; an element that only
    /// moved does not, because its value did not change. A view bound to the
    /// list re-reads it; a view bound to a row follows its row.
    fn array_add(
        &mut self,
        id: NodeId,
        old: &[NodeId],
        index: usize,
        value: &Value,
        owner: &StoreId,
        depth: usize,
    ) -> Result<(), TreeError> {
        // Every element of this array keeps the node it already holds, so a
        // store standing anywhere in it is not up for adoption by the value
        // being inserted: this render carries that id twice, and §3.2's
        // duplicate rule gives the second sighting a node of its own rather than
        // blanking the slot the first one stands in.
        self.claim_stores(old);

        let inserted = self.reconcile_child(id, None, value, owner, depth)?;
        // Read the children back rather than shifting the snapshot: the inserted
        // value may have adopted a store from somewhere else in the tree, and
        // `detach` leaves an addressable `Null` standing in the slot it vacated
        // (§3.2) — which this parent has to keep if the vacated slot was its own.
        let mut new = self.arena_ref().ordered_children(id);
        let index = index.min(new.len());

        new.insert(index, inserted);

        self.install_array(id, old, new);

        Ok(())
    }

    /// `remove /list/i`: the node at `i` leaves the tree, and every element
    /// after it keeps its node and moves one slot left.
    ///
    /// The mirror of [`array_add`](Self::array_add), and positional for the same
    /// reasons. Nothing is reconciled here at all: a removal carries no value.
    fn array_remove(&mut self, id: NodeId, old: &[NodeId], index: usize) {
        let mut new = Vec::with_capacity(old.len() - 1);

        new.extend_from_slice(&old[..index]);
        new.extend_from_slice(&old[index + 1..]);

        // The node that fell out is released by `install_array`, which frees
        // everything the new child list no longer holds.
        self.install_array(id, old, new);
    }

    /// Installs a rebuilt child list, releasing whatever fell out of it.
    fn install_array(&mut self, id: NodeId, old: &[NodeId], new: Vec<NodeId>) {
        let kept: HashSet<NodeId> = new.iter().copied().collect();

        self.release_displaced(id, old, &kept);

        if new != old {
            self.touch_and_dirty(id);
            self.arena().nodes[id].kind = NodeKind::Array(new);
        }
    }

    /// Drops one key from an object-shaped node, in place.
    fn remove_field(&mut self, id: NodeId, key: &str) {
        if let NodeKind::Object(fields) | NodeKind::Store { fields, .. } =
            &mut self.arena().nodes[id].kind
        {
            fields.remove(key);
        }
    }

    /// This node's object fields, for a kind that has them.
    fn fields_of(&self, id: NodeId) -> Option<BTreeMap<Arc<str>, NodeId>> {
        match &self.arena_ref().nodes.get(id)?.kind {
            NodeKind::Object(fields) | NodeKind::Store { fields, .. } => Some(fields.clone()),
            _ => None,
        }
    }

    /// Installs one field, reconciling into the node already under that key.
    ///
    /// `depth` is the child's depth.
    fn put_field(
        &mut self,
        parent: NodeId,
        key: &str,
        value: &Value,
        owner: &StoreId,
        depth: usize,
    ) -> Result<(), TreeError> {
        if !matches!(self.container_of(parent), Container::Fields) {
            return Ok(());
        }

        let existing = self.arena_ref().child_by_key(parent, key);
        // The map as it was, kept only when this write could disturb another key
        // of it: adoption happens for a store-shaped value and for nothing else
        // (§3.2), so every other field write reads one key and copies nothing.
        let before = matches!(classify(value), Shape::Store { .. })
            .then(|| self.fields_of(parent))
            .flatten();
        let child = self.reconcile_child(parent, existing, value, owner, depth)?;

        if existing == Some(child) {
            return Ok(());
        }

        // Adoption may have taken the node out of another key of this same
        // parent — the shape a reorder of two child stores arrives in. The node
        // this write displaced takes that key, so both keep their identity; the
        // null `detach` left there is dropped (§3.2).
        let vacated = before.and_then(|fields| {
            fields
                .iter()
                .find(|(name, node)| **node == child && &***name != key)
                .map(|(name, _)| name.clone())
        });

        self.touch_and_dirty(parent);

        let mut displaced = existing;

        // In place: `touch` above already holds the snapshot rollback replays,
        // so writing one key does not need a second copy of the map.
        if let NodeKind::Object(fields) | NodeKind::Store { fields, .. } =
            &mut self.arena().nodes[parent].kind
        {
            match fields.get_mut(key) {
                Some(slot) => *slot = child,
                None => {
                    fields.insert(Arc::from(key), child);
                }
            }

            if let (Some(node), Some(name)) = (existing, vacated) {
                displaced = fields.insert(name, node);
            }
        }

        if let Some(node) = displaced {
            self.release_if_still_mine(parent, node);
        }

        Ok(())
    }

    /// Installs one array position, reconciling into the node already there.
    ///
    /// `depth` is the child's depth.
    fn put_index(
        &mut self,
        parent: NodeId,
        old: &[NodeId],
        index: usize,
        value: &Value,
        owner: &StoreId,
        depth: usize,
    ) -> Result<(), TreeError> {
        let existing = old[index];
        let child = self.reconcile_child(parent, Some(existing), value, owner, depth)?;

        if child == existing {
            return Ok(());
        }

        let mut new = old.to_vec();

        new[index] = child;

        // As in `put_field`: a store adopted out of another position of this
        // same array is a reorder, and the node this write displaced takes the
        // position it left rather than a null placeholder.
        if let Some(vacated) = old.iter().position(|node| *node == child) {
            new[vacated] = existing;
        }

        self.install_array(parent, old, new);

        Ok(())
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
    fn apply_stream_op(&mut self, op: &StreamOp) -> Result<(), TreeError> {
        // A stream op is an op: it may legitimately move a store that an
        // earlier op in the same envelope already placed (§3.2).
        self.claimed.clear();

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
            return Ok(());
        };

        if !self.arena_ref().nodes.contains_key(collection) {
            return Ok(());
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
            } => return self.stream_insert(collection, item_key, item, *at, *limit),
        }

        Ok(())
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
    ) -> Result<(), TreeError> {
        let owner = match &self.arena_ref().nodes[collection].kind {
            NodeKind::Collection { owner, .. } => owner.clone(),
            _ => return Ok(()),
        };
        let depth = self.arena_ref().depth(collection) + 1;
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

                // Through the same child-level write every other op uses: an
                // item key is the row's identity, but a store id is the
                // *node's*, so a row that stopped being the store it was does
                // not keep that store's node (§3.2).
                let settled = self.reconcile_child(collection, Some(node), item, &owner, depth)?;

                if settled != node {
                    self.release(node);
                }

                (key, settled)
            }
            None => {
                let node = self.build(Some(collection), item, &owner, depth)?;

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

        Ok(())
    }

    /// Trims to `limit`, dropping from the end for `at == 0` and from the front
    /// otherwise.
    ///
    /// The direction is chosen by `at`, **not** by the sign of `limit`: the
    /// server writes negative limits (`-100`) by convention and the client does
    /// not read that sign.
    ///
    /// The overflow leaves in one `split_off` or one `drain`, not one
    /// `Vec::remove` per row: a limit that trims *k* rows off an *n*-row list
    /// used to cost O(n·k) — 116 ms for a single op against a 20 000-row list,
    /// 450 ms at 40 000, with the arena lock held throughout. The edits are
    /// recorded in exactly the order and with exactly the indices the row-by-row
    /// removal produced, because a list adapter replays them in order (§6.3).
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

        if items.len() <= size {
            return;
        }

        if size > 0 && at == 0 {
            // Off the end, last row first: each edit names the index its row
            // held at the moment it was taken out.
            let overflow = items.split_off(size);

            for (offset, (key, node)) in overflow.into_iter().enumerate().rev() {
                self.carry_out(collection, &key, node);
                self.record_edit(
                    collection,
                    CollectionEdit::Removed {
                        item_key: key,
                        index: size + offset,
                    },
                );
            }
        } else {
            // Off the front, where every removal is at index 0.
            let overflow: Vec<_> = items.drain(..items.len() - size).collect();

            for (key, node) in overflow {
                self.carry_out(collection, &key, node);
                self.record_edit(
                    collection,
                    CollectionEdit::Removed {
                        item_key: key,
                        index: 0,
                    },
                );
            }
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
        // In place: cloning the kind to read `name` and `owner` back out would
        // copy the whole item list the caller is about to replace.
        if let NodeKind::Collection { items: slot, .. } = &mut self.arena().nodes[collection].kind {
            *slot = items;
        }
    }

    fn record_edit(&mut self, collection: NodeId, edit: CollectionEdit) {
        self.journal.edits.entry(collection).or_default().push(edit);
    }

    // --------------------------------------------------------- node lifecycle

    /// An empty node, parented but not yet filled.
    ///
    /// A node a transaction created starts at revision `1`: it *was* touched by
    /// a transaction, which is exactly what `revision() == 0` denies (§9.3).
    fn null_node(&mut self, parent: Option<NodeId>) -> NodeId {
        let id = self.arena().nodes.insert(NodeData {
            parent,
            kind: NodeKind::Null,
            revision: 1,
            semantic: SemanticValue::new(Semantic::Null),
            subscribers: Vec::new(),
        });

        self.journal.allocated.push(id);

        id
    }

    /// Creates a node and its whole subtree from one wire value.
    ///
    /// The one place a node is allocated from wire input, and therefore the one
    /// place the depth cap has to hold: `serde_json`'s own nesting limit bounds
    /// a single document, but `add` at successively deeper paths composes depth
    /// across ops and across envelopes, and the recursive walks over a tree
    /// (semantic equality, projection, `Drop`) abort the **process** rather than
    /// unwinding when they run out of stack.
    fn build(
        &mut self,
        parent: Option<NodeId>,
        value: &Value,
        owner: &StoreId,
        depth: usize,
    ) -> Result<NodeId, TreeError> {
        if depth > MAX_DEPTH {
            return Err(TreeError::Depth { limit: MAX_DEPTH });
        }

        let id = self.null_node(parent);

        self.install(id, value, owner, depth)?;

        Ok(id)
    }

    /// Gives an existing node the kind, children and indices `value` calls for.
    fn install(
        &mut self,
        id: NodeId,
        value: &Value,
        owner: &StoreId,
        depth: usize,
    ) -> Result<(), TreeError> {
        let kind = match classify(value) {
            Shape::Null => NodeKind::Null,
            Shape::Bool(flag) => NodeKind::Bool(flag),
            Shape::Number(number) => NodeKind::Number(number.clone()),
            Shape::String(text) => NodeKind::String(Arc::from(text)),
            Shape::Array(values) => {
                let mut children = Vec::with_capacity(values.len());

                for value in values {
                    children.push(self.reconcile_child(id, None, value, owner, depth + 1)?);
                }

                NodeKind::Array(children)
            }
            Shape::Object(fields) => {
                let mut children = BTreeMap::new();

                for (key, value) in fields {
                    let child = self.reconcile_child(id, None, value, owner, depth + 1)?;

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

                    let child = self.reconcile_child(id, None, value, &store_id, depth + 1)?;

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
                result: self.reconcile_child(id, None, result, owner, depth + 1)?,
                reason: self.reconcile_child(id, None, reason, owner, depth + 1)?,
            },
            Shape::UploadSlot { name } => NodeKind::UploadSlot {
                name: Arc::from(name),
                owner: owner.clone(),
            },
        };

        self.arena().nodes[id].kind = kind;

        let semantic = self.arena_ref().semantic_shallow(id);

        self.arena().nodes[id].semantic = semantic;

        Ok(())
    }

    /// Drops a node's index entries, for a node about to stop being what it was.
    fn unregister(&mut self, id: NodeId) {
        // Only the two keys are read out, never the children: a collection about
        // to be rebuilt may be holding a list of thousands.
        let (store, collection) = match &self.arena_ref().nodes[id].kind {
            NodeKind::Store { store_id, .. } => (Some(store_id.clone()), None),
            NodeKind::Collection { name, owner, .. } => {
                (None, Some((owner.clone(), name.to_string())))
            }
            _ => (None, None),
        };

        if let Some(store_id) = store {
            if self.arena_ref().stores.get(&store_id) == Some(&id) {
                self.set_store(store_id, None);
            }
        }

        if let Some(key) = collection {
            if self.arena_ref().collections.get(&key) == Some(&id) {
                self.set_collection(key, None);
            }
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

        // The adopted node arrives with the value it held before this
        // transaction touched it — its cache is stale until commit settles it —
        // so its new ancestors have to settle *after* it. They are put in the
        // dirty set here rather than left to whoever writes the parent's kind,
        // because a parent this same op is still building has nothing to
        // compare against and would otherwise never be recomputed (§2.3).
        self.mark_dirty(parent);
    }

    /// Removes one child from its parent's kind, so no node is ever reachable
    /// from two parents.
    ///
    /// The slot it vacates keeps a fresh `Null` node instead of disappearing.
    /// `Musubi.Diff` emits the op that **lands** a moving store before the one
    /// that vacates its old slot — `[add /b/w {store}, replace /a null]` — so
    /// the source key has to stay addressable for the rest of the envelope, or
    /// a legitimate server frame is rejected and the client remounts. The server
    /// always vacates the slot later in the same envelope, so the committed
    /// value matches the render; if it ever does not, drift validation catches
    /// it (§4.4).
    fn detach(&mut self, parent: NodeId, child: NodeId) {
        if !self.arena_ref().nodes.contains_key(parent) {
            return;
        }

        self.touch_and_dirty(parent);

        // Taken, not copied: `touch` above already holds the snapshot rollback
        // replays, and nothing between here and the write-back reads this node's
        // children — `null_node` only allocates.
        let taken = std::mem::replace(&mut self.arena().nodes[parent].kind, NodeKind::Null);
        let kind = match taken {
            NodeKind::Object(mut fields) => {
                self.fill_field(parent, &mut fields, child);

                NodeKind::Object(fields)
            }
            NodeKind::Store {
                store_id,
                mut fields,
            } => {
                self.fill_field(parent, &mut fields, child);

                NodeKind::Store { store_id, fields }
            }
            NodeKind::Array(mut children) => {
                if let Some(index) = children.iter().position(|node| *node == child) {
                    children[index] = self.null_node(Some(parent));
                }

                NodeKind::Array(children)
            }
            NodeKind::Collection {
                name,
                owner,
                mut items,
            } => {
                // A stream's item list is keyed, not positional: a key whose
                // node left is a key that is gone, and `stream_ops` are what put
                // one back (§3.1).
                items.retain(|(_, node)| *node != child);

                NodeKind::Collection { name, owner, items }
            }
            NodeKind::Async {
                status,
                result,
                reason,
            } => {
                let filler = self.null_node(Some(parent));

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

    /// Leaves a null node under the key `child` occupied.
    fn fill_field(
        &mut self,
        parent: NodeId,
        fields: &mut BTreeMap<Arc<str>, NodeId>,
        child: NodeId,
    ) {
        let Some(key) = fields
            .iter()
            .find(|(_, node)| **node == child)
            .map(|(key, _)| key.clone())
        else {
            return;
        };

        let filler = self.null_node(Some(parent));

        fields.insert(key, filler);
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

    /// Releases every child a rebuild displaced: the ones the parent held when
    /// the rebuild started, and the null placeholders an adoption left behind on
    /// the way.
    fn release_displaced(&mut self, parent: NodeId, old: &[NodeId], kept: &HashSet<NodeId>) {
        let live = self.arena_ref().children(parent);

        for child in old.iter().copied().chain(live) {
            if !kept.contains(&child) {
                self.release_if_still_mine(parent, child);
            }
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

    fn is_collection(&self, id: NodeId, name: &str) -> bool {
        matches!(
            self.arena_ref().nodes.get(id),
            Some(node) if matches!(&node.kind, NodeKind::Collection { name: existing, .. } if &**existing == name)
        )
    }

    fn is_store_node(&self, id: NodeId) -> bool {
        matches!(
            self.arena_ref().nodes.get(id).map(|node| &node.kind),
            Some(NodeKind::Store { .. })
        )
    }

    /// Whether `node` is `parent` itself, or one of its ancestors.
    ///
    /// Adopting a node under one of its own descendants would give the tree a
    /// parent cycle, and every walk up from inside it — `mark_dirty`'s
    /// included, which runs holding the arena lock — would spin forever.
    fn is_ancestor(&self, node: NodeId, parent: NodeId) -> bool {
        let arena = self.arena_ref();
        let mut cursor = Some(parent);
        let mut steps = 0;

        while let Some(current) = cursor {
            if current == node {
                return true;
            }

            steps += 1;

            if steps > MAX_DEPTH + 1 {
                debug_assert!(false, "a parent chain longer than the depth cap");

                // Refusing the adoption is the safe answer: it costs one store
                // its `NodeId`, where the alternative is a wedged process.
                return true;
            }

            cursor = arena.nodes.get(current).and_then(|data| data.parent);
        }

        false
    }

    /// Claims the ids of the store nodes among `nodes`, so nothing later in the
    /// same op adopts one out from under a position that is keeping it.
    fn claim_stores(&mut self, nodes: &[NodeId]) {
        let ids: Vec<StoreId> = nodes
            .iter()
            .filter_map(|node| match &self.arena_ref().nodes.get(*node)?.kind {
                NodeKind::Store { store_id, .. } => Some(store_id.clone()),
                _ => None,
            })
            .collect();

        for id in ids {
            self.claimed.insert(id);
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
        let mut steps = 0;

        while let Some(current) = cursor {
            let Some(node) = self.arena.as_ref().expect(OPEN).nodes.get(current) else {
                break;
            };

            cursor = node.parent;

            if let Entry::Vacant(slot) = self.journal.settle.entry(current) {
                slot.insert(node.semantic.clone());
                self.journal.settle_order.push(current);
            }

            steps += 1;

            // No chain from a node to its root is longer than the depth cap.
            // This walk is the one that must not trust that: it runs **holding
            // the arena lock**, so a broken invariant here would not be a wrong
            // answer but a process at 100% CPU with every reader blocked behind
            // it. Bailing costs a stale ancestor value instead.
            if steps > MAX_DEPTH + 1 {
                debug_assert!(
                    false,
                    "the parent chain of a dirty node does not reach a root"
                );
                break;
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

/// Applies one op to a node's store id, yielding the marker the node should
/// carry afterwards — `None` when the op removed it and the node becomes a
/// plain object.
fn rewrite_store_id(
    op: &PatchOp,
    rest: &[String],
    mut segments: Vec<String>,
    path: &str,
) -> Result<Option<Value>, TreeError> {
    let malformed = || TreeError::Pointer {
        path: path.to_owned(),
        reason: "a store id is an array of strings",
    };
    let out_of_range = || TreeError::Index {
        path: path.to_owned(),
    };

    match rest {
        // The whole marker.
        [] => match op {
            PatchOp::Remove { .. } => Ok(None),
            PatchOp::Add { value, .. } | PatchOp::Replace { value, .. } => {
                if parse_store_id(value).is_none() {
                    return Err(malformed());
                }

                Ok(Some(value.clone()))
            }
        },
        // One segment of it.
        [token] => {
            let Some(index) = pointer::array_index(token) else {
                return Err(out_of_range());
            };

            match op {
                PatchOp::Remove { .. } => {
                    let ArrayIndex::At(index) = index else {
                        return Err(out_of_range());
                    };

                    if index >= segments.len() {
                        return Err(out_of_range());
                    }

                    segments.remove(index);
                }
                PatchOp::Replace { value, .. } => {
                    let (ArrayIndex::At(index), Some(segment)) = (index, value.as_str()) else {
                        return Err(malformed());
                    };
                    let Some(slot) = segments.get_mut(index) else {
                        return Err(out_of_range());
                    };

                    *slot = segment.to_owned();
                }
                PatchOp::Add { value, .. } => {
                    let Some(segment) = value.as_str() else {
                        return Err(malformed());
                    };
                    let index = match index {
                        ArrayIndex::End => segments.len(),
                        ArrayIndex::At(index) if index <= segments.len() => index,
                        ArrayIndex::At(_) => return Err(out_of_range()),
                    };

                    segments.insert(index, segment.to_owned());
                }
            }

            Ok(Some(Value::Array(
                segments.into_iter().map(Value::String).collect(),
            )))
        }
        _ => Err(malformed()),
    }
}

/// How deep a node sits in the tree **as it now stands**, memoized per commit.
///
/// A detached node answers `0`, which sorts it last: nothing above it is
/// waiting on its value, and its own children still answer `1` and settle first.
///
/// Iterative, and capped: a parent chain that does not reach a root would
/// otherwise recurse until the stack ran out, and this runs inside `commit`,
/// which cannot fail.
fn depth_of(arena: &Arena, id: NodeId, memo: &mut HashMap<NodeId, u32>) -> u32 {
    let mut chain = Vec::new();
    let mut cursor = Some(id);
    let mut base = None;

    while let Some(current) = cursor {
        if let Some(known) = memo.get(&current) {
            base = Some(*known);
            break;
        }

        if chain.len() > MAX_DEPTH + 1 {
            debug_assert!(false, "a parent chain longer than the depth cap");
            break;
        }

        chain.push(current);
        cursor = arena.nodes.get(current).and_then(|node| node.parent);
    }

    let mut depth = base.map_or(0, |known| known.saturating_add(1));

    for node in chain.iter().rev() {
        memo.insert(*node, depth);
        depth = depth.saturating_add(1);
    }

    memo.get(&id).copied().unwrap_or(0)
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
