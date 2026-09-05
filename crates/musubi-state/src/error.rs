//! The two error enums (`docs/rust-reactive-state.md` §2.3, §2.4).

/// Why a transaction could not be applied.
///
/// `musubi-client` maps these onto its taxonomy: `Pointer` and `Index` become
/// `MusubiError::Patch(PatchError::Apply)`, the version-mismatch class, and
/// `Closed` is unreachable from the actor, which always drops a root before
/// closing its tree.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum TreeError {
    /// The pointer did not resolve, or resolved into a non-container.
    #[error("pointer {path:?} does not resolve: {reason}")]
    Pointer {
        /// The RFC 6901 pointer, as it arrived.
        path: String,
        /// Which rule it broke.
        reason: &'static str,
    },
    /// An array index was out of bounds, or not a valid RFC 6901 index token.
    #[error("array index in {path:?} is out of bounds or malformed")]
    Index {
        /// The RFC 6901 pointer, as it arrived.
        path: String,
    },
    /// The transaction was applied to a tree that `close` had already ended.
    #[error("the tree is closed")]
    Closed,
}

/// Why a read did not produce a value.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ReadError {
    /// The node has been removed, or the tree was closed.
    #[error("the node is gone")]
    Gone,
    /// The node's shape does not match the requested type — codegen drift.
    #[error("the node's shape does not match the requested type: {0}")]
    Shape(#[from] serde_json::Error),
}
