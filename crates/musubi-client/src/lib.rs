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
//! let cart: Mounted<CartStore> = connection.mount("cart:page", json!({})).await?;
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
//! # Scope
//!
//! Uploads are deferred (`docs/rust-client.md` §10): `upload_ops` are parsed
//! and discarded, and an upload slot deserializes into the inert
//! [`generated::UploadSlot`].

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod actor;
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

pub use crate::connection::{BuildError, Connection, ConnectionBuilder};
pub use crate::engine::PatchEngine;
pub use crate::envelope::{PatchEnvelope, PatchOp, PushEvent, StreamOp, UploadOp};
pub use crate::error::{CommandError, MusubiError, PatchError, Result};
pub use crate::mounted::Mounted;
// The runtime seams are defined one layer down and re-exported here, so an
// embedder implements them against `musubi_client` alone (§3).
pub use phoenix_channel::{Connector, Frame, Socket, Spawner, Timer, TransportError};
