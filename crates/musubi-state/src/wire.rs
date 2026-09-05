//! The wire vocabulary the tree itself names (`docs/rust-reactive-state.md`
//! §1.3).
//!
//! [`StoreId`], [`PatchOp`] and [`StreamOp`] sank here because
//! [`StateTree::apply`](crate::StateTree::apply) has to name them and
//! `musubi-client` depends on *this* crate — leaving them upstream would be a
//! cycle. [`UploadSlot`], [`StoreField`], [`AsyncResult`], [`AsyncError`] and
//! [`AsyncErrorKind`] followed for the same reason one layer up: §2.4 signs
//! `UploadSlotState::value() -> UploadSlot`,
//! `StoreState::<S>::value() -> StoreField<S>` and
//! `AsyncState::<T>::value() -> AsyncResult<T>`, so a handle here cannot name
//! its own return type unless the type is here. Every one of them is re-exported
//! verbatim from `musubi_client::generated`, so no consumer path changes and
//! the canonical prelude list (`docs/rust-codegen.md` §4.5) still resolves.
//!
//! **Discipline (§1.3.1, point 5).** This crate adds no inherent method and no
//! local trait impl to these types beyond the ones that came with them, so
//! splitting a `musubi-protocol` crate out later stays one move plus a set of
//! re-exports. Helpers the tree needs are free functions or methods on the
//! tree's own types.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A server-authored store path; the root store's path is empty.
///
/// A newtype rather than a `Vec<String>` alias so a path cannot be confused
/// with an arbitrary string vector. Store ids are **server-authored**: the
/// client echoes them verbatim and never constructs or parses one.
///
/// ```
/// use musubi_state::StoreId;
///
/// let child: StoreId = serde_json::from_value(serde_json::json!(["cart", "0"])).unwrap();
///
/// assert_eq!(child.as_slice(), ["cart".to_owned(), "0".to_owned()]);
/// assert!(StoreId::root().as_slice().is_empty());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StoreId(Vec<String>);

impl StoreId {
    /// The root store's path — the empty path.
    ///
    /// ```
    /// use musubi_state::StoreId;
    ///
    /// assert_eq!(serde_json::to_value(StoreId::root()).unwrap(), serde_json::json!([]));
    /// ```
    pub fn root() -> Self {
        Self(Vec::new())
    }

    /// The path segments, parent first.
    ///
    /// ```
    /// use musubi_state::StoreId;
    ///
    /// let id: StoreId = serde_json::from_value(serde_json::json!(["panel"])).unwrap();
    ///
    /// assert_eq!(id.as_slice(), ["panel".to_owned()]);
    /// ```
    pub fn as_slice(&self) -> &[String] {
        &self.0
    }
}

/// A mounted child store: the wire node carries `__musubi_store_id__`
/// alongside the child's own rendered fields.
///
/// `store_id` lives on the wrapper, never as a hand-declared field on a
/// generated `State` struct, so `mounted.command_on(&panel.store_id(), ..)` is
/// how a child command reaches its target.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoreField<S> {
    /// The child's server-authored path.
    #[serde(rename = "__musubi_store_id__")]
    pub store_id: StoreId,
    /// The child's rendered fields, flattened into the same wire object.
    #[serde(flatten)]
    pub state: S,
}

/// An upload slot. The wire node is `{"__musubi_upload__": "<name>"}`.
///
/// Inert by design: the live upload state is not part of the state tree
/// (§3.4). The slot carries the declared name — one half of the
/// `(store_id, name)` upload key; the other half is
/// [`UploadSlotState::key`](crate::UploadSlotState::key), which reads the
/// node's resolved owner instead of leaving it to the call site.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UploadSlot {
    /// The declared upload name.
    #[serde(rename = "__musubi_upload__")]
    pub name: String,
}

/// The wire shape of `Musubi.AsyncResult`.
///
/// Field names are the **wire** names (`result` / `reason`), not the
/// TypeScript client's app-facing `data` / `error` normalization: the derive
/// then works with no hand-written `Deserialize` and the three variants line
/// up 1:1 with `%Musubi.AsyncResult{status, result, reason}`.
///
/// The wire node also carries `"__musubi_async__": true`; an internally-tagged
/// enum ignores the extra key, so serializing an `AsyncResult` back out omits
/// it. That is acceptable because state never travels client → server.
///
/// ```
/// use musubi_state::AsyncResult;
/// use serde_json::json;
///
/// let wire = json!({"__musubi_async__": true, "status": "ok", "result": 7, "reason": null});
///
/// assert_eq!(
///     serde_json::from_value::<AsyncResult<u8>>(wire).unwrap(),
///     AsyncResult::Ok { result: 7, reason: None }
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum AsyncResult<T> {
    /// The task is running. `result` is the prior value when the server kept
    /// it for stale-while-loading UX.
    Loading {
        /// The prior value, when one was preserved.
        result: Option<T>,
        /// The prior failure, when one was preserved.
        reason: Option<AsyncError>,
    },
    /// The task succeeded.
    Ok {
        /// The resolved value.
        result: T,
        /// Always `None` in practice; the wire renders the key regardless.
        reason: Option<AsyncError>,
    },
    /// The task failed. `result` is the prior value when the server kept it.
    Failed {
        /// The prior value, when one was preserved.
        result: Option<T>,
        /// Why the task failed.
        reason: Option<AsyncError>,
    },
}

/// Why an [`AsyncResult`] failed.
///
/// The server renders `%{"kind" => "error" | "exit", "value" => ...}` when it
/// can classify the failure, and falls back to an `inspect/1`-shaped term
/// otherwise — hence the untagged [`Opaque`](AsyncError::Opaque) arm.
///
/// ```
/// use musubi_state::{AsyncError, AsyncErrorKind};
/// use serde_json::json;
///
/// let structured: AsyncError =
///     serde_json::from_value(json!({"kind": "exit", "value": "timeout"})).unwrap();
///
/// assert!(matches!(structured, AsyncError::Structured { kind: AsyncErrorKind::Exit, .. }));
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AsyncError {
    /// The classified shape `{"kind": ..., "value": ...}`.
    Structured {
        /// Whether the task raised or exited.
        kind: AsyncErrorKind,
        /// The wire-serialized reason term.
        value: Value,
    },
    /// Anything else the server rendered.
    Opaque(Value),
}

/// How a failed [`AsyncResult`] failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AsyncErrorKind {
    /// The task raised.
    Error,
    /// The task exited.
    Exit,
}

/// One RFC 6902 op, restricted to the three the server can emit (BDR-0014).
///
/// The allowlist is enforced where the envelope is decoded, one crate up, so
/// `move` / `copy` / `test` never reach the tree.
#[derive(Debug, Clone, PartialEq)]
pub enum PatchOp {
    /// Insert a value at `path`.
    Add {
        /// RFC 6901 pointer into the wire tree.
        path: String,
        /// The value to insert.
        value: Value,
    },
    /// Remove the value at `path`.
    Remove {
        /// RFC 6901 pointer into the wire tree.
        path: String,
    },
    /// Overwrite the value at `path`.
    Replace {
        /// RFC 6901 pointer into the wire tree; `""` addresses the whole tree.
        path: String,
        /// The replacement value.
        value: Value,
    },
}

/// One stream delta (`docs/streams.md`), stamped with its owning store.
///
/// `ref` is the per-store slot ref; the client ignores it and keys everything
/// by `(store_id, stream)`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum StreamOp {
    /// Empty the stream.
    Reset {
        /// The declared stream name.
        stream: String,
        /// The owning store's path.
        store_id: StoreId,
    },
    /// Upsert an item, then position it (`docs/rust-client.md` §5).
    Insert {
        /// The declared stream name.
        stream: String,
        /// The owning store's path.
        store_id: StoreId,
        /// The item's identity within the stream.
        item_key: String,
        /// `-1` appends, `0` or any other negative prepends, `> 0` inserts at
        /// `min(at, len)`.
        at: i64,
        /// The rendered item.
        item: Value,
        /// Cap on the stream's length after this insert; `null` means no cap.
        limit: Option<i64>,
    },
    /// Drop every entry with this item key.
    Delete {
        /// The declared stream name.
        stream: String,
        /// The owning store's path.
        store_id: StoreId,
        /// The item's identity within the stream.
        item_key: String,
    },
}
