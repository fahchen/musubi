//! The upload wire vocabulary: the deltas the server emits and everything they
//! carry (BDR-0024–BDR-0028, `docs/uploads.md`, `docs/rust-client.md` §10).
//!
//! Shape only — nothing here folds, transfers or notifies. It is what
//! [`PatchEnvelope`](crate::PatchEnvelope) decodes `upload_ops` into, what
//! [`registry`](super::registry) folds, and the vocabulary
//! [`transfer`](super::transfer) reports back in. Decoding is deliberately
//! lenient where the wire is open: a code a newer server adds survives verbatim
//! as [`UploadErrorCode::Other`] rather than failing the envelope it arrived in.

use serde::{Deserialize, Serialize};

use crate::generated::StoreId;

/// `max_entries` before any `config` op lands (`lib/musubi/upload/config.ex`).
const DEFAULT_MAX_ENTRIES: u32 = 1;
/// `max_file_size` before any `config` op lands — 8 MB.
const DEFAULT_MAX_FILE_SIZE: u64 = 8_000_000;
/// `chunk_size` before any `config` op lands — 64 kB.
const DEFAULT_CHUNK_SIZE: u64 = 64_000;
/// The progress value at and above which an entry is complete.
pub(in crate::uploads) const COMPLETE: u32 = 100;

/// What an upload accepts, as declared by `accept:` (BDR-0026).
///
/// Enforced at preflight only, and against the **file extension** — the MIME
/// type is never consulted — so this is display/filter information for the
/// client, not a second gate.
///
/// ```
/// use musubi_client::UploadAccept;
/// use serde_json::json;
///
/// let any: UploadAccept = serde_json::from_value(json!("any")).unwrap();
/// let list: UploadAccept = serde_json::from_value(json!([".png", ".jpg"])).unwrap();
///
/// assert_eq!(any, UploadAccept::Any);
/// assert!(matches!(list, UploadAccept::Extensions(exts) if exts.len() == 2));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UploadAccept {
    /// `accept: :any` — every extension passes.
    Any,
    /// The declared extension allowlist, e.g. `[".png"]`.
    #[serde(untagged)]
    Extensions(Vec<String>),
}

/// One upload's declared limits, as carried by the `config` op.
///
/// `chunk_timeout` is deliberately absent: it never reaches the wire — it
/// lives only inside the signed preflight token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadConfig {
    /// The accepted extensions, or `:any`.
    pub accept: UploadAccept,
    /// How many entries may be live at once.
    pub max_entries: u32,
    /// The per-file byte ceiling.
    pub max_file_size: u64,
    /// The channel-mode slice size, in bytes.
    pub chunk_size: u64,
}

impl Default for UploadConfig {
    /// The framework defaults, used until the mount-time `config` op lands.
    ///
    /// ```
    /// use musubi_client::{UploadAccept, UploadConfig};
    ///
    /// let config = UploadConfig::default();
    ///
    /// assert_eq!(config.accept, UploadAccept::Any);
    /// assert_eq!(config.max_entries, 1);
    /// ```
    fn default() -> Self {
        Self {
            accept: UploadAccept::Any,
            max_entries: DEFAULT_MAX_ENTRIES,
            max_file_size: DEFAULT_MAX_FILE_SIZE,
            chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }
}

/// One upload failure's code.
///
/// The union is **open** on both clients: the server allowlists what it emits
/// per path, but a newer server may add a code, and a client that fails to
/// decode it would drop the whole envelope. Unknown codes land in
/// [`Other`](UploadErrorCode::Other).
///
/// ```
/// use musubi_client::UploadErrorCode;
/// use serde_json::json;
///
/// let known: UploadErrorCode = serde_json::from_value(json!("too_large")).unwrap();
/// let future: UploadErrorCode = serde_json::from_value(json!("quota_exceeded")).unwrap();
///
/// assert_eq!(known, UploadErrorCode::TooLarge);
/// assert!(matches!(future, UploadErrorCode::Other(code) if code == "quota_exceeded"));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UploadErrorCode {
    /// The file is larger than `max_file_size` (preflight or chunk stream).
    TooLarge,
    /// Accepting the entry would exceed `max_entries`.
    TooManyFiles,
    /// The extension is outside `accept`.
    NotAccepted,
    /// No chunk arrived within `chunk_timeout`.
    ChunkTimeout,
    /// A pushed chunk was larger than the negotiated `chunk_size`.
    ChunkTooLarge,
    /// A registered external uploader rejected the transfer (BDR-0027).
    ExternalFailed,
    /// The entry was malformed — a missing `client_ref`, `name` or `size`.
    PreflightRejected,
    /// The server failed to write the chunk.
    Internal,
    /// A code this client does not know, kept verbatim.
    #[serde(untagged)]
    Other(String),
}

/// One upload failure, attached to an entry or to the handle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadError {
    /// What went wrong.
    pub code: UploadErrorCode,
    /// The server-authored message; display text, never matched on.
    pub message: String,
}

/// One entry's lifecycle state.
///
/// `Cancelled` is **reserved**: a cancel is a `cancel` op, which deletes the
/// entry outright, so no entry is ever observed in this state. The variant
/// exists because the wire type permits it and the TypeScript client models it
/// the same way.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryStatus {
    /// Accepted at preflight, transfer not started.
    Pending,
    /// Bytes are moving.
    Uploading,
    /// The server has the whole file.
    Success,
    /// The entry failed; see [`UploadEntry::errors`].
    Error,
    /// Reserved; see the type docs.
    Cancelled,
}

/// One handle's lifecycle state.
///
/// Driven entirely by the client's own API — [`Upload::select`](crate::Upload::select),
/// [`Upload::start`](crate::Upload::start), [`Upload::cancel`](crate::Upload::cancel)
/// and [`Upload::reset`](crate::Upload::reset) — never by an op
/// (`docs/rust-client.md` §10.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UploadStatus {
    /// Nothing selected.
    Idle,
    /// Files were selected and preflighted.
    Selecting,
    /// A transfer is running.
    Uploading,
    /// Every entry finished.
    Success,
    /// Preflight or a transfer failed.
    Error,
    /// Reserved; the TypeScript client never assigns it either.
    Cancelled,
}

/// One selected file, as the server tracks it.
///
/// The field set is exactly the entry wire whitelist
/// (`lib/musubi/upload/entry.ex`); the server-side transport fields (`path`,
/// `token`, `store_pid`, …) are never serialized.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadEntry {
    /// The server-generated entry ref, e.g. `"u_a3f…"`.
    pub r#ref: String,
    /// The file name as the client reported it.
    pub client_name: String,
    /// The byte size as the client reported it.
    pub client_size: u64,
    /// The MIME type as the client reported it; `""` when unknown.
    pub client_type: String,
    /// Percent complete, `0..=100`.
    pub progress: u32,
    /// The entry's lifecycle state.
    pub status: EntryStatus,
    /// Every failure recorded against this entry, oldest first.
    #[serde(default)]
    pub errors: Vec<UploadError>,
}

impl UploadEntry {
    /// Accepted, not yet transferring.
    pub fn is_pending(&self) -> bool {
        self.status == EntryStatus::Pending
    }

    /// Bytes are moving.
    pub fn is_uploading(&self) -> bool {
        self.status == EntryStatus::Uploading
    }

    /// The server has the whole file.
    pub fn is_success(&self) -> bool {
        self.status == EntryStatus::Success
    }

    /// The entry failed.
    pub fn is_error(&self) -> bool {
        self.status == EntryStatus::Error
    }

    /// Reserved; see [`EntryStatus::Cancelled`].
    pub fn is_cancelled(&self) -> bool {
        self.status == EntryStatus::Cancelled
    }
}

/// One upload delta (BDR-0025), stamped with its owning store.
///
/// Applied in array order, independently of `ops` and `stream_ops`. Uploads
/// are singletons per store, so `(store_id, upload)` identifies the handle and
/// `ref` identifies the entry within it.
///
/// ```
/// use musubi_client::UploadOp;
/// use serde_json::json;
///
/// let op: UploadOp = serde_json::from_value(json!({
///     "op": "progress", "upload": "avatar", "store_id": [], "ref": "u_a3f", "progress": 33
/// }))
/// .unwrap();
///
/// assert_eq!(op.upload(), "avatar");
/// ```
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum UploadOp {
    /// Replace the declared limits. Emitted once per upload at mount.
    Config {
        /// The declared upload name.
        upload: String,
        /// The owning store's path.
        store_id: StoreId,
        /// The declared limits.
        config: UploadConfig,
    },
    /// Upsert an entry accepted at preflight.
    Add {
        /// The declared upload name.
        upload: String,
        /// The owning store's path.
        store_id: StoreId,
        /// The entry ref, mirroring `entry.ref`.
        r#ref: String,
        /// The entry as the server has it.
        entry: UploadEntry,
    },
    /// Move an entry's progress. Throttled to 10 Hz per entry, so the final
    /// `100` may never arrive — `Complete` is the completion signal.
    Progress {
        /// The declared upload name.
        upload: String,
        /// The owning store's path.
        store_id: StoreId,
        /// The entry ref.
        r#ref: String,
        /// Percent complete, `0..=100`.
        progress: u32,
    },
    /// The transfer finished; never throttled, never dropped.
    Complete {
        /// The declared upload name.
        upload: String,
        /// The owning store's path.
        store_id: StoreId,
        /// The entry ref.
        r#ref: String,
    },
    /// A failure. Without a `ref` it belongs to the handle rather than to an
    /// entry — permitted by the wire type, unused by the server today.
    Error {
        /// The declared upload name.
        upload: String,
        /// The owning store's path.
        store_id: StoreId,
        /// The entry ref, when the failure belongs to one entry.
        #[serde(default)]
        r#ref: Option<String>,
        /// What went wrong.
        error: UploadError,
    },
    /// Drop the entry. Cancellation is a deletion, not a status.
    Cancel {
        /// The declared upload name.
        upload: String,
        /// The owning store's path.
        store_id: StoreId,
        /// The entry ref.
        r#ref: String,
    },
    /// Drop every entry and every handle-level error.
    Reset {
        /// The declared upload name.
        upload: String,
        /// The owning store's path.
        store_id: StoreId,
    },
}

impl UploadOp {
    /// The declared upload name this op addresses.
    pub fn upload(&self) -> &str {
        match self {
            Self::Config { upload, .. }
            | Self::Add { upload, .. }
            | Self::Progress { upload, .. }
            | Self::Complete { upload, .. }
            | Self::Error { upload, .. }
            | Self::Cancel { upload, .. }
            | Self::Reset { upload, .. } => upload,
        }
    }

    /// The owning store's path.
    pub fn store_id(&self) -> &StoreId {
        match self {
            Self::Config { store_id, .. }
            | Self::Add { store_id, .. }
            | Self::Progress { store_id, .. }
            | Self::Complete { store_id, .. }
            | Self::Error { store_id, .. }
            | Self::Cancel { store_id, .. }
            | Self::Reset { store_id, .. } => store_id,
        }
    }
}

/// Wire-shaped ops, built the way the server writes them.
///
/// Shared with [`registry`](super::registry): the fold's tests are about what
/// an op *does*, and the shape it arrives in is this module's business.
#[cfg(test)]
pub(in crate::uploads) mod fixtures {
    use serde_json::{Value, json};

    use super::UploadOp;

    pub(in crate::uploads) fn decode(ops: Vec<Value>) -> Vec<UploadOp> {
        ops.into_iter()
            .map(|op| serde_json::from_value(op).expect("op is a valid upload op"))
            .collect()
    }

    pub(in crate::uploads) fn add(r#ref: &str, status: &str, progress: u32) -> Value {
        json!({
            "op": "add", "upload": "avatar", "store_id": [], "ref": r#ref,
            "entry": entry(r#ref, status, progress)
        })
    }

    pub(in crate::uploads) fn entry(r#ref: &str, status: &str, progress: u32) -> Value {
        json!({
            "ref": r#ref, "client_name": "me.png", "client_size": 1234,
            "client_type": "image/png", "progress": progress, "status": status,
            "errors": []
        })
    }

    pub(in crate::uploads) fn progress(r#ref: &str, progress: u32) -> Value {
        json!({
            "op": "progress", "upload": "avatar", "store_id": [],
            "ref": r#ref, "progress": progress
        })
    }

    pub(in crate::uploads) fn complete(r#ref: &str) -> Value {
        json!({"op": "complete", "upload": "avatar", "store_id": [], "ref": r#ref})
    }

    pub(in crate::uploads) fn error(r#ref: Option<&str>, code: &str) -> Value {
        json!({
            "op": "error", "upload": "avatar", "store_id": [], "ref": r#ref,
            "error": {"code": code, "message": "boom"}
        })
    }

    pub(in crate::uploads) fn cancel(r#ref: &str) -> Value {
        json!({"op": "cancel", "upload": "avatar", "store_id": [], "ref": r#ref})
    }

    pub(in crate::uploads) fn reset() -> Value {
        json!({"op": "reset", "upload": "avatar", "store_id": []})
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::fixtures::{add, cancel, complete, decode, error, progress, reset};
    use super::*;

    #[test]
    fn every_op_variant_decodes_from_its_wire_shape() {
        let ops = decode(vec![
            json!({
                "op": "config", "upload": "avatar", "store_id": [],
                "config": {
                    "accept": "any", "max_entries": 1,
                    "max_file_size": 8_000_000, "chunk_size": 64_000
                }
            }),
            add("u_1", "pending", 0),
            progress("u_1", 33),
            complete("u_1"),
            error(Some("u_1"), "too_large"),
            error(None, "external_failed"),
            cancel("u_1"),
            reset(),
        ]);

        assert!(matches!(
            ops.as_slice(),
            [
                UploadOp::Config { config, .. },
                UploadOp::Add { r#ref, entry, .. },
                UploadOp::Progress { progress: 33, .. },
                UploadOp::Complete { .. },
                UploadOp::Error { r#ref: Some(_), error, .. },
                UploadOp::Error { r#ref: None, .. },
                UploadOp::Cancel { .. },
                UploadOp::Reset { .. },
            ] if config.accept == UploadAccept::Any
                && r#ref == "u_1"
                && entry.client_name == "me.png"
                && error.code == UploadErrorCode::TooLarge
        ));
        assert!(ops.iter().all(|op| op.store_id() == &StoreId::root()));
    }

    #[test]
    fn an_entry_without_an_errors_key_decodes_as_an_entry_without_errors() {
        let ops = decode(vec![json!({
            "op": "add", "upload": "avatar", "store_id": [], "ref": "u_1",
            "entry": {
                "ref": "u_1", "client_name": "me.png", "client_size": 12,
                "client_type": "image/png", "progress": 0, "status": "pending"
            }
        })]);

        assert!(matches!(
            ops.as_slice(),
            [UploadOp::Add { entry, .. }] if entry.errors.is_empty()
        ));
    }
}
