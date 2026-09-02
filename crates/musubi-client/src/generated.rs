//! The shared runtime types the generated bundle re-exports.
//!
//! `mix compile.musubi_rust` emits a type-only file whose prelude module is
//! exactly one line (`docs/rust-codegen.md` §4.5):
//!
//! ```text
//! pub use ::musubi_client::generated::{
//!     AsyncError, AsyncResult, Command, Event, NoReply, Store, StoreField, StoreId, UploadSlot,
//! };
//! ```
//!
//! Every item in that list lives here, because a bundle-local `trait Store`
//! would be a *different* trait from the one [`Connection::mount`] is generic
//! over. [`AsyncErrorKind`] is reachable through [`AsyncError`] and is exported
//! too, though no generated item names it directly.
//!
//! [`Connection::mount`]: https://github.com/fahchen/musubi/blob/main/docs/rust-client.md

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A server-authored store path; the root store's path is empty.
///
/// A newtype rather than a `Vec<String>` alias so a path cannot be confused
/// with an arbitrary string vector. Store ids are **server-authored**: the
/// client echoes them verbatim and never constructs or parses one.
///
/// ```
/// use musubi_client::generated::StoreId;
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
    /// use musubi_client::generated::StoreId;
    ///
    /// assert_eq!(serde_json::to_value(StoreId::root()).unwrap(), serde_json::json!([]));
    /// ```
    pub fn root() -> Self {
        Self(Vec::new())
    }

    /// The path segments, parent first.
    ///
    /// ```
    /// use musubi_client::generated::StoreId;
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
/// generated `State` struct, so `mounted.command_on(&snap.panel.store_id, ..)`
/// is how a child command reaches its target.
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
/// Inert in v1: the upload engine is deferred (`docs/rust-client.md` §10), so
/// only the declared name reaches the generated types.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UploadSlot {
    /// The declared upload name.
    #[serde(rename = "__musubi_upload__")]
    pub name: String,
}

/// The reply type generated for a command that declares no `reply do` block.
///
/// `{:noreply, socket}` replies `{}` on the wire, so this deserializes from
/// any object and carries nothing.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct NoReply {}

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
/// use musubi_client::generated::AsyncResult;
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
/// use musubi_client::generated::{AsyncError, AsyncErrorKind};
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

/// A store the client can mount, implemented by the generated marker type.
///
/// The marker (`CartStore`) and the rendered shape (`State`) are two distinct
/// types: `Mounted<CartStore>` holds `Arc<<CartStore as Store>::State>`.
///
/// Not sealed — a sealed trait could not be implemented by a file generated
/// into a consumer crate.
pub trait Store: Send + Sync + 'static {
    /// The fully-qualified Elixir module name, e.g. `"MyApp.Stores.CartStore"`.
    const MODULE: &'static str;
    /// The store's rendered shape.
    type State: DeserializeOwned + Send + Sync + 'static;
}

/// A command payload, generic over the owning store so that
/// `Mounted::<St>::command::<C: Command<St>>` type-checks the pairing.
pub trait Command<S: Store>: Serialize + Send + 'static {
    /// The declared command name, as sent in the `command` push payload.
    const NAME: &'static str;
    /// What the server's `phx_reply` carries on `status: "ok"`.
    type Reply: DeserializeOwned + Send + 'static;
}

/// A push event payload (BDR-0032), implemented on the payload struct.
///
/// The wire name has to come from the type because the dispatch key is
/// `(store_id, name)`.
pub trait Event<S: Store>: DeserializeOwned + Send + 'static {
    /// The declared event name.
    const NAME: &'static str;
}
