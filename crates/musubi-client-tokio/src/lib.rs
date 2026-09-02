//! The tokio transport for the Musubi client.
//!
//! [`musubi_client`] is runtime-free by construction: the socket, the executor
//! and the clock are traits an embedder supplies (`docs/rust-client.md` §2.2).
//! This crate is the tokio answer to all three — [`TungsteniteConnector`],
//! [`TokioSpawner`] and [`TokioTimer`] — plus the [`builder`] one-liner that
//! pre-fills them.
//!
//! Everything in `musubi_client` is re-exported here, so a tokio embedder
//! depends on this crate alone.
//!
//! ```text
//! let connection = musubi_client_tokio::builder("wss://example.test/socket").build()?;
//! let cart: Mounted<CartStore> = connection.mount("cart:page", json!({})).await?;
//! ```
//!
//! Choosing a runtime is a crate choice, not a feature flag: depending on
//! `musubi-client` alone keeps tokio out of the tree entirely, which is what
//! the gpui embedder needs.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod connector;
mod runtime;

pub use crate::connector::TungsteniteConnector;
pub use crate::runtime::{TokioSpawner, TokioTimer};
pub use musubi_client::*;

/// A [`ConnectionBuilder`] with the three tokio seams already set.
///
/// Nothing else is required, though every other setter (`topic`, `heartbeat`,
/// `join_timeout`, `push_timeout`) still applies, and so does overriding one of
/// the three seams. Must be called from inside a tokio runtime: `build()`
/// spawns the connection actor.
///
/// ```no_run
/// let connection = musubi_client_tokio::builder("wss://example.test/socket").build()?;
/// # Ok::<_, musubi_client_tokio::BuildError>(())
/// ```
pub fn builder(url: impl Into<String>) -> ConnectionBuilder {
    Connection::builder()
        .url(url)
        .connector(TungsteniteConnector)
        .spawner(TokioSpawner)
        .timer(TokioTimer)
}
