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
//! `snapshot()`, `updates()`, `command()`, `events()` — is a method on the
//! handle, and unmounting is [`Drop`].
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
//! PatchEnvelope::decode   ── op allowlist (add/remove/replace only)
//!        │
//!        ▼
//! PatchEngine::apply      ── version discipline
//!        │                  ── json-patch over the pristine shadow document
//!        │                  ── stream materialization (client-owned)
//!        │                  ── store index rebuild + stream pruning
//!        ▼
//! hydrated state Value
//!        │
//!        ▼
//! serde into the generated `S::State`, published to `Mounted::snapshot`
//! ```
//!
//! The shadow document is a `serde_json::Value` kept exactly as it arrived:
//! patch pointers address the wire tree, so hydration (stream-marker
//! substitution) produces an owned copy per cycle and never mutates the tree.
//!
//! # Concurrency
//!
//! One actor owns the socket and every mounted root; the public handles are
//! cheap `Clone` values over its inbox. State reaches the embedder through a
//! per-root snapshot cell and per-subscription channels, never through the
//! inbox, and there is no callback registry — a subscription **is** a `Stream`,
//! and dropping it unsubscribes.
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
//! [`Mounted::upload`] hands out, with `snapshot()`/`updates()` mirroring the
//! state surface; an upload slot on the state stays the inert
//! [`generated::UploadSlot`], which carries the name those handles are keyed
//! by. The **control plane** — `select`/`start`/`cancel`/`reset` — is on the
//! same handle: preflight, channel-mode chunk transfer over binary frames, and
//! external [`Uploader`]s. The crate stays runtime-free throughout, so the
//! embedder reads the file and hands over an [`UploadFile`].

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod actor;
mod cache;
mod connection;
mod engine;
mod envelope;
mod error;
pub mod generated;
mod hydrate;
mod index;
mod mounted;
mod patch;
mod streams;
mod transfer;
mod uploads;

pub use crate::cache::{
    CACHE_WRITE_THROTTLE, CacheEntry, CacheStore, DEFAULT_CACHE_GC_TIME, MemoryCacheStore,
    cache_key, now_ms,
};
pub use crate::connection::{BuildError, Connection, ConnectionBuilder};
pub use crate::engine::PatchEngine;
pub use crate::envelope::{PatchEnvelope, PatchOp, PushEvent, StreamOp};
pub use crate::error::{CommandError, MusubiError, PatchError, Result, TransferError};
pub use crate::mounted::Mounted;
pub use crate::transfer::{
    CancelSignal, UploadFile, UploadProgress, UploadRequest, Uploader, UploaderError,
};
pub use crate::uploads::{
    EntryStatus, Upload, UploadAccept, UploadConfig, UploadEntry, UploadError, UploadErrorCode,
    UploadHandle, UploadOp, UploadStatus, Uploads,
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
