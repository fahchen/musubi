//! The client error taxonomy (`docs/rust-client.md` §11).
//!
//! Error identity is by variant, never by string matching. Reason strings the
//! server sends are propagated verbatim and are not parsed into variants — the
//! server's reason list is not a stability contract.

use phoenix_channel::TransportError;
use serde_json::Value;
use thiserror::Error;

use crate::generated::StoreId;

/// The crate-wide result alias, in the `std::io::Result` style.
pub type Result<T, E = MusubiError> = std::result::Result<T, E>;

/// Anything that can go wrong between the socket and a mounted root.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum MusubiError {
    /// Socket/IO level: connect failed, frame decode failed, socket closed.
    #[error(transparent)]
    Transport(#[from] TransportError),
    /// The channel join was rejected by the server.
    #[error("join of {topic} was rejected: {reason}")]
    Join {
        /// The channel topic that was rejected.
        topic: String,
        /// The server's reason string, verbatim.
        reason: String,
    },
    /// A join or a push exceeded its timeout.
    #[error("timed out")]
    Timeout,
    /// No channel, or `version == 0` (mid-reconnect) at dispatch time.
    #[error("not connected")]
    NotConnected,
    /// An envelope failed version continuity; recovery has been initiated.
    #[error("patch envelope broke version continuity")]
    VersionMismatch,
    /// The root was unmounted (or dropped) with work in flight.
    #[error("root was unmounted")]
    Unmounted,
    /// `disconnect()` was called with work in flight.
    #[error("connection was disconnected")]
    Disconnected,
    /// The envelope violated the contract: bad discriminator, `root_id`
    /// mismatch, unsupported op, bad pointer, initial version != 1.
    #[error("protocol violation: {0}")]
    Protocol(&'static str),
    /// RFC 6902 application failed.
    #[error(transparent)]
    Patch(#[from] PatchError),
    /// The wire tree did not match the generated types — i.e. codegen drift.
    #[error("state of store {store_id:?} did not match the generated types: {source}")]
    Decode {
        /// The store whose subtree failed to deserialize.
        store_id: StoreId,
        /// The underlying serde failure.
        #[source]
        source: serde_json::Error,
    },
    /// A dispatched command did not succeed.
    #[error(transparent)]
    Command(#[from] CommandError),
    /// An upload's control plane failed: preflight, chunk transfer, or an
    /// external uploader (`docs/rust-client.md` §10.2).
    #[error(transparent)]
    Transfer(#[from] TransferError),
}

/// Why applying an envelope's `ops` failed.
///
/// The document is left untouched in every case: the allowlist runs at decode,
/// before anything is applied, and `json_patch::patch` is atomic on failure.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PatchError {
    /// The envelope carried an op outside the `add`/`remove`/`replace`
    /// allowlist (BDR-0014 — the server never emits `move`/`copy`/`test`).
    #[error("unsupported patch op: {op}")]
    UnsupportedOp {
        /// The rejected op name, verbatim.
        op: String,
    },
    /// An op's `path` was not a valid RFC 6901 JSON Pointer.
    #[error("invalid json pointer: {path}")]
    InvalidPointer {
        /// The rejected pointer, verbatim.
        path: String,
    },
    /// The RFC 6902 application itself failed: bad pointer, index out of
    /// bounds, traversal into a non-container.
    #[error("patch op {index} at {path} failed: {reason}")]
    Apply {
        /// Index of the failing op within the envelope's `ops`.
        index: usize,
        /// The failing op's pointer.
        path: String,
        /// The `json-patch` description of the failure.
        reason: String,
    },
}

impl From<json_patch::PatchError> for PatchError {
    fn from(error: json_patch::PatchError) -> Self {
        Self::Apply {
            index: error.operation,
            path: error.path.to_string(),
            reason: error.kind.to_string(),
        }
    }
}

/// The outcome of a command that did not reply `status: "ok"`.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CommandError {
    /// The server replied `status: "error"`.
    #[error("command {command} on store {store_id:?} failed: {reply}")]
    Failed {
        /// The declared command name.
        command: &'static str,
        /// The store the command was dispatched on.
        store_id: StoreId,
        /// The error response, verbatim.
        reply: Value,
        /// The first string-valued field among `code`, `error`, `reason`.
        code: Option<String>,
    },
    /// No reply arrived before the push timeout.
    #[error("command {command} on store {store_id:?} timed out")]
    Timeout {
        /// The declared command name.
        command: &'static str,
        /// The store the command was dispatched on.
        store_id: StoreId,
    },
}

/// Why an upload's control plane could not finish (§10.2).
///
/// Everything an upload shares with the rest of the client — a rejected join, a
/// timeout, a dropped socket — stays on [`MusubiError`]; only the
/// upload-specific failures live here. Server-driven entry failures are **not**
/// errors: they arrive as `{op: error}` on
/// [`UploadEntry::errors`](crate::UploadEntry::errors).
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum TransferError {
    /// A main-channel upload push was answered with `status: "error"`.
    #[error("upload push {event} was rejected: {reason}")]
    Rejected {
        /// The pushed event: `allow_upload`, `cancel_upload`, ….
        event: &'static str,
        /// The server's reason string, verbatim.
        reason: String,
    },
    /// The chunk sub-channel rejected a chunk and stopped; the matching
    /// `{op: error}` carries the machine-readable code.
    #[error("chunk of entry {entry_ref} was rejected: {reason}")]
    Chunk {
        /// The entry whose transfer failed.
        entry_ref: String,
        /// The server's reason string, verbatim.
        reason: String,
    },
    /// The transfer was cancelled while it was running.
    #[error("transfer of entry {entry_ref} was cancelled")]
    Cancelled {
        /// The entry whose transfer was cancelled.
        entry_ref: String,
    },
    /// The server picked an external uploader this client did not register
    /// (BDR-0027).
    #[error("no uploader registered as {uploader:?}, which entry {entry_ref} needs")]
    NoUploader {
        /// The uploader name the server chose.
        uploader: String,
        /// The entry that needed it.
        entry_ref: String,
    },
    /// A registered external uploader reported a failure; the server has been
    /// told, as `code: "external_failed"`.
    #[error("uploader failed for entry {entry_ref}: {message}")]
    Uploader {
        /// The entry whose transfer failed.
        entry_ref: String,
        /// The uploader's own message.
        message: String,
    },
}
