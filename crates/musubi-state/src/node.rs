//! Nodes and semantic values (`docs/rust-reactive-state.md` §2.1).

use std::collections::BTreeMap;
use std::sync::Arc;

use serde_json::{Map, Number, Value};

use crate::marker::{ASYNC_MARKER_KEY, STORE_ID_KEY, STREAM_MARKER_KEY, UPLOAD_MARKER_KEY};
use crate::wire::StoreId;

slotmap::new_key_type! {
    /// Client-local identity of one retained node.
    ///
    /// Stable for the node's lifetime and **never** reused after the node is
    /// freed: the generation half of the index is what makes a
    /// [`State`](crate::State) that outlived its node read as dead rather than
    /// as some later node that took its slot.
    pub struct NodeId;
}

/// A copy of one node's metadata, as of the moment it was read.
///
/// Nodes are not handed out by reference. A `&Node` would either escape the
/// tree lock or hold it across caller code, and caller code is allowed to call
/// `subscribe()` — so this is an owned copy, produced by
/// [`StateTree::node`](crate::StateTree::node). It is a diagnostics and adapter
/// surface, not the read path: [`State::value`](crate::State::value) does not
/// go through it.
#[derive(Debug, Clone)]
pub struct Node {
    /// `None` for the root, which is the only parentless node.
    pub parent: Option<NodeId>,
    /// What the node is, and where its children live.
    pub kind: NodeKind,
    /// Bumped only by a transaction that changed this node's semantic value.
    /// `0` means no transaction has ever touched it.
    pub revision: u64,
    /// The node's value as equality sees it.
    pub semantic: SemanticValue,
    /// Live subscriptions on this node. Diagnostics only.
    pub subscribers: usize,
}

/// What a node is, and where its children live.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum NodeKind {
    /// JSON `null`.
    Null,
    /// A JSON boolean.
    Bool(bool),
    /// A JSON number, kept as `serde_json` parsed it.
    Number(Number),
    /// A JSON string.
    String(Arc<str>),

    /// A plain JSON array. Children are **index**-identified (§9.1).
    Array(Vec<NodeId>),

    /// A plain JSON object. Children are key-identified; key order is not
    /// semantic, which is why this is a `BTreeMap`.
    Object(BTreeMap<Arc<str>, NodeId>),

    /// An object that also carries `__musubi_store_id__`. Reconciled by
    /// **store id**, not by position (§3.2).
    Store {
        /// The server-authored path this node is filed under.
        store_id: StoreId,
        /// The child store's own rendered fields.
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
        /// Which of the three wire statuses this node is in.
        status: AsyncStatus,
        /// The `result` subtree; a `Null` node when the wire result is `null`.
        result: NodeId,
        /// The `reason` subtree; a `Null` node when the wire reason is `null`.
        reason: NodeId,
    },

    /// `{"__musubi_upload__": "<name>"}`. Inert: live upload state lives on the
    /// `Upload` plane, never in the tree (§3.4).
    UploadSlot {
        /// The declared slot name, from the wire marker.
        name: Arc<str>,
        /// The nearest enclosing store, resolved once at node creation —
        /// exactly as `Collection` does it. This is the half of the
        /// `(store_id, name)` upload key that no call site has to spell, and it
        /// is what lets the client bridge from the tree to the upload plane in
        /// one step (§3.4).
        owner: StoreId,
    },
}

/// The three wire statuses of an async node.
///
/// The typed [`AsyncResult`](crate::AsyncResult) an app matches on is a
/// separate type; this is only what the tree needs to decide equality.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AsyncStatus {
    /// The task is running.
    Loading,
    /// The task succeeded.
    Ok,
    /// The task failed.
    Failed,
}

impl AsyncStatus {
    /// Reads the wire `status` string.
    pub(crate) fn from_wire(status: &str) -> Option<Self> {
        match status {
            "loading" => Some(Self::Loading),
            "ok" => Some(Self::Ok),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }

    /// The wire spelling, for the hydrated and wire projections.
    pub(crate) fn as_wire(self) -> &'static str {
        match self {
            Self::Loading => "loading",
            Self::Ok => "ok",
            Self::Failed => "failed",
        }
    }
}

/// A node's value as equality sees it.
///
/// Cheap to clone (one `Arc` bump), cheap to compare (pointer equality is the
/// fast path), and **structurally shared**: a child that a transaction did not
/// change keeps the exact `Arc` it had, so its parent's comparison stops at the
/// pointer. That sharing is what makes recursive equality operationally
/// incremental rather than a full-tree DFS.
#[derive(Debug, Clone)]
pub struct SemanticValue(Arc<Semantic>);

impl SemanticValue {
    /// Wraps one computed semantic. Every allocation of one goes through here.
    pub(crate) fn new(semantic: Semantic) -> Self {
        Self(Arc::new(semantic))
    }

    /// The semantic, for the recursive projections and for equality.
    pub(crate) fn get(&self) -> &Semantic {
        &self.0
    }

    /// Whether two values are the *same* allocation.
    ///
    /// The settle step restores the old `Arc` for an unchanged node precisely
    /// so this keeps hitting one level up; the tests assert on it.
    pub(crate) fn is_shared_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// The hydrated projection of this value (§3.5): stream slots as arrays,
    /// store nodes carrying `__musubi_store_id__`, upload slots as their
    /// marker, async nodes as their wire shape.
    pub fn to_hydrated(&self) -> Value {
        self.project(Projection::Hydrated)
    }

    /// The wire projection of this value (§3.5): markers back in place, so a
    /// stream slot is `{"__musubi_stream__": name}` again.
    pub fn to_wire(&self) -> Value {
        self.project(Projection::Wire)
    }

    fn project(&self, mode: Projection) -> Value {
        match self.get() {
            Semantic::Null => Value::Null,
            Semantic::Bool(flag) => Value::Bool(*flag),
            Semantic::Number(number) => Value::Number(number.clone()),
            Semantic::String(text) => Value::String(text.to_string()),
            Semantic::Array(items) => {
                Value::Array(items.iter().map(|item| item.project(mode)).collect())
            }
            Semantic::Object(fields) => Value::Object(project_fields(fields, mode)),
            Semantic::Store { store_id, fields } => {
                let mut object = project_fields(fields, mode);

                object.insert(
                    STORE_ID_KEY.to_owned(),
                    serde_json::to_value(store_id).unwrap_or(Value::Null),
                );

                Value::Object(object)
            }
            Semantic::Collection { name, items, .. } => match mode {
                Projection::Hydrated => {
                    Value::Array(items.iter().map(|(_, item)| item.project(mode)).collect())
                }
                Projection::Wire => {
                    let mut object = Map::new();

                    object.insert(
                        STREAM_MARKER_KEY.to_owned(),
                        Value::String(name.to_string()),
                    );

                    Value::Object(object)
                }
            },
            Semantic::Async {
                status,
                result,
                reason,
            } => {
                let mut object = Map::new();

                object.insert(ASYNC_MARKER_KEY.to_owned(), Value::Bool(true));
                object.insert(
                    "status".to_owned(),
                    Value::String(status.as_wire().to_owned()),
                );
                object.insert("result".to_owned(), result.project(mode));
                object.insert("reason".to_owned(), reason.project(mode));

                Value::Object(object)
            }
            // `owner` is not projected: it is the client-local half of the
            // upload key (§2.1, §3.4), and the wire never carried it.
            Semantic::UploadSlot { name, .. } => {
                let mut object = Map::new();

                object.insert(
                    UPLOAD_MARKER_KEY.to_owned(),
                    Value::String(name.to_string()),
                );

                Value::Object(object)
            }
        }
    }
}

impl PartialEq for SemanticValue {
    /// `Arc::ptr_eq` first, structural comparison second. Pointer equality is
    /// an **optimization, not the definition**: two distinct allocations
    /// holding equal contents are equal.
    fn eq(&self, other: &Self) -> bool {
        self.is_shared_with(other) || self.0 == other.0
    }
}

impl Eq for SemanticValue {}

/// Which of the two projections (§3.5) a walk is producing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Projection {
    /// What a generated type deserializes from.
    Hydrated,
    /// What the mount cache stores.
    Wire,
}

/// Projects one field list, which is already in sorted key order because the
/// node holds a `BTreeMap` (§9.1: key *order* is not part of the value).
fn project_fields(fields: &[(Arc<str>, SemanticValue)], mode: Projection) -> Map<String, Value> {
    fields
        .iter()
        .map(|(key, value)| (key.to_string(), value.project(mode)))
        .collect()
}

/// The recursive definition of §9.1, one variant per [`NodeKind`].
///
/// Field lists are `Vec` rather than `BTreeMap` because they are built from an
/// already-sorted source and only ever compared and projected in order — the
/// cheaper representation for the two things this type does.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Semantic {
    Null,
    Bool(bool),
    Number(Number),
    String(Arc<str>),
    Array(Vec<SemanticValue>),
    Object(Vec<(Arc<str>, SemanticValue)>),
    Store {
        store_id: StoreId,
        fields: Vec<(Arc<str>, SemanticValue)>,
    },
    /// `name` and `owner` are fixed when the node is created, so comparing them
    /// is free and never fires for a node compared against its own earlier
    /// self; what §9.1 defines this row by is `items`, the ordered
    /// `(item_key, item_semantic)` sequence.
    Collection {
        name: Arc<str>,
        owner: StoreId,
        items: Vec<(Arc<str>, SemanticValue)>,
    },
    Async {
        status: AsyncStatus,
        result: SemanticValue,
        reason: SemanticValue,
    },
    UploadSlot {
        name: Arc<str>,
        owner: StoreId,
    },
}
