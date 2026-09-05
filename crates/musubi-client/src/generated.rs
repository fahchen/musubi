//! The shared runtime types the generated bundle re-exports.
//!
//! `mix compile.musubi_rust` emits a type-only file whose prelude module is
//! exactly one item (`docs/rust-codegen.md` §4.5):
//!
//! ```text
//! pub use ::musubi_client::generated::{
//!     AsyncError, AsyncResult, AsyncState, Command, Event, NoReply, State, StateTree,
//!     Store, StoreField, StoreId, StoreState, StreamState, Subscription, UploadSlot,
//!     UploadSlotState,
//! };
//! ```
//!
//! Every item in that list is nameable here, because a bundle-local
//! `trait Store` would be a *different* trait from the one
//! [`Connection::mount`](crate::Connection::mount) is generic over. The
//! generated `Params` struct is *not* in that list: it is per-store, so the
//! bundle declares it rather than re-exporting it. [`AsyncErrorKind`] is
//! reachable through [`AsyncError`] and is exported too, though no generated
//! item names it directly.
//!
//! # Where these live
//!
//! Three groups, and the split is the crate boundary of
//! `docs/rust-reactive-state.md` §1.3:
//!
//! * The **tree vocabulary** — [`State`] and the four navigation views,
//!   [`Subscription`], [`StateTree`] — is `musubi-state`'s, because the tree is.
//! * The **value types a view's `value()` returns** — [`StoreId`],
//!   [`StoreField`], [`UploadSlot`], [`AsyncResult`], [`AsyncError`] — sank into
//!   `musubi-state` with them: a handle cannot name its own return type
//!   otherwise. They are re-exported here verbatim, so no consumer path changed.
//! * The **traits the generated marker types implement** — [`Store`],
//!   [`Command`], [`Event`] — and [`NoReply`] stay here: they name
//!   [`Mounted`](crate::Mounted), commands and events, none of which the tree
//!   knows about.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

pub use musubi_state::{
    AsyncError, AsyncErrorKind, AsyncResult, AsyncState, State, StateTree, StoreField, StoreId,
    StoreState, StreamState, Subscription, UploadSlot, UploadSlotState,
};

/// The reply type generated for a command that declares no `reply do` block.
///
/// `{:noreply, socket}` replies `{}` on the wire, so this deserializes from
/// any object and carries nothing.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct NoReply {}

/// A store the client can mount, implemented by the generated marker type.
///
/// The marker (`CartStore`) and the rendered shape (`State`) are two distinct
/// types: `Mounted<CartStore>` hands out
/// [`State<<CartStore as Store>::State>`](State).
///
/// Not sealed — a sealed trait could not be implemented by a file generated
/// into a consumer crate.
pub trait Store: Send + Sync + 'static {
    /// The fully-qualified Elixir module name, e.g. `"MyApp.Stores.CartStore"`.
    const MODULE: &'static str;
    /// The store's rendered shape.
    type State: DeserializeOwned + Send + Sync + 'static;
    /// The mount params object: one field per `attr/3` declaration, required
    /// attrs plain and optional ones `Option`. A store declaring no `attr`
    /// gets an empty struct, which serializes to `{}`.
    type Params: Serialize + Send + 'static;
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
