//! The runtime-agnostic Musubi client core.
//!
//! Musubi is server-authoritative: one BEAM process per connected page owns a
//! tree of stores and pushes RFC 6902 patches to the client. This crate is the
//! Rust peer of `packages/client` — a second consumer of the same wire
//! contract, not a port of the TypeScript runtime.
//!
//! # Entry point
//!
//! [`Connection`] is one socket; [`Connection::mount`] joins one channel per
//! root store and hands back a [`Mounted`] handle. Everything past that point —
//! `state()`, `status()`, `command()`, `events()`, `upload_at()` — is a method
//! on the handle, and unmounting is [`Drop`].
//!
//! ```text
//! let cart: Mounted<CartStore> = connection.mount("cart:page", Params {}).await?;
//! ```
//!
//! # Shape
//!
//! ```text
//! phx_reply / "patch" push
//!        │
//!        ▼
//! envelope decode         ── op allowlist (add/remove/replace only)
//!        │
//!        ▼
//! one transaction on the retained tree   ── version discipline
//!        │                                ── pointer-addressed reconciliation
//!        │                                ── key-addressed stream reconciliation
//!        │                                ── whole-root drift check (§4.4)
//!        ▼
//! ChangeSet ──▶ the subscribers of exactly the nodes whose semantic value moved
//! ```
//!
//! There is no shadow document and no whole-root snapshot. The tree is
//! **retained**: a node's identity outlives every envelope, so a `State<T>` an
//! embedder is holding survives a reconnect, and a patch that puts a value back
//! where it was notifies nobody. `docs/rust-reactive-state.md` is the design
//! record; [`musubi_state`] is where the tree lives.
//!
//! # Four words
//!
//! `x.prop()` gives a **handle**, `handle.value()` gives a **value**,
//! `handle.subscribe(cb)` gives a **subscription**, and `drop(subscription)` is
//! the only way to unsubscribe. [`Mounted::state`], [`Mounted::status`] and
//! [`Mounted::upload_at`] are the three property accessors; nothing is spelled a
//! second way.
//!
//! # Concurrency
//!
//! One actor owns the socket and every mounted root; the public handles are
//! cheap `Clone` values over its inbox. State reaches the embedder through the
//! retained tree — one lock, written once per envelope on the actor task, read
//! per `value()` — and mount status through a per-root **latest-value cell**.
//! Subscriber callbacks run on the actor task with no lock held, so the contract
//! is *schedule, do not compute*. The discrete-item subscriptions (`events()`,
//! [`Upload::into_stream`]) stay unbounded queues. None of it goes back through
//! the inbox.
//!
//! # Generated code
//!
//! [`generated`] holds every runtime type `mix compile.musubi_rust` re-exports
//! into its prelude module. The bundle is type-only; nothing in it duplicates a
//! definition from this crate, because a bundle-local `trait Store` would be a
//! different trait from [`generated::Store`].
//!
//! # Caching
//!
//! Opt-in and connection-wide: [`ConnectionBuilder::cache`] takes a
//! [`CacheStore`], and every mount then seeds from the last-known wire tree for
//! its `(module, id, params)` before the live initial patch lands
//! (`docs/rust-client.md` §6.4). The crate ships the in-process
//! [`MemoryCacheStore`]; a durable store is the embedder's, because the file
//! system and the platform's storage are runtime decisions this crate does not
//! make.
//!
//! # Uploads
//!
//! Both halves are here (`docs/rust-client.md` §10). The **data plane** folds
//! `upload_ops` into per-`(store_id, name)` [`UploadHandle`]s that
//! [`Mounted::upload_at`] hands out, reachable in one step from the slot node on
//! the tree — both halves of the key come from the node, so no call site spells
//! either. The slot itself is an **inert leaf**: the server re-renders the same
//! marker every cycle, so a pure-upload envelope wakes no state subscriber at
//! all. The **control plane** — `select`/`start`/`cancel`/`reset` — is on the
//! same handle: preflight, channel-mode chunk transfer over binary frames, and
//! external [`Uploader`]s. The crate stays runtime-free throughout, so the
//! embedder reads the file and hands over an [`UploadFile`].

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod actor;
mod cache;
mod cache_coordinator;
mod connection;
mod engine;
mod envelope;
mod error;
pub mod generated;
mod latest;
mod mounted;
mod uploads;

pub use crate::cache::{
    CACHE_WRITE_THROTTLE, CacheEntry, CacheStore, DEFAULT_CACHE_GC_TIME, MemoryCacheStore,
    cache_key, now_ms,
};
pub use crate::connection::{BuildError, Connection, ConnectionBuilder};
pub use crate::error::{CommandError, MusubiError, PatchError, Result, TransferError};
pub use crate::latest::StatusState;
pub use crate::mounted::{MountStatus, Mounted};
pub use crate::uploads::{
    CancelSignal, EntryStatus, Upload, UploadAccept, UploadConfig, UploadEntry, UploadError,
    UploadErrorCode, UploadFile, UploadHandle, UploadProgress, UploadRequest, UploadStatus,
    Uploader, UploaderError,
};
// The consumer half of the retained tree (`docs/rust-reactive-state.md` §5.5).
// The write half — `StateTree::apply`/`begin`/`close`, `Transaction`, `Notify`,
// `ChangeSet`, `NodeKind`, `Node`, `SemanticValue`, `TreeError` — is reachable
// only through `musubi_state` itself, is `#[doc(hidden)]` there, and has no
// caller outside this crate.
pub use musubi_state::{
    AsyncState, AsyncStatus, Change, CollectionEdit, NodeId, ReadError, State, StateTree,
    StoreState, StreamState, Subscription, UploadSlotState,
};
// The runtime seams are defined one layer down and re-exported here, so an
// embedder implements them against `musubi_client` alone (§3).
pub use phoenix_channel::{BinaryPush, Connector, Frame, Socket, Spawner, Timer, TransportError};

/// Locks a mutex, ignoring poisoning.
///
/// A panic in a subscriber's `Drop` must not take the whole connection down
/// with it: the data behind these locks is plain state with no invariant a
/// half-finished write could break.
pub(crate) fn lock<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
