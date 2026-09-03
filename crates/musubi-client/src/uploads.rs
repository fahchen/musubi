//! The upload data plane: wire ops, the per-handle state machine, and the
//! registry a mounted root exposes them through (BDR-0024–BDR-0028,
//! `docs/uploads.md`, `docs/rust-client.md` §10).
//!
//! This is a deliberate op-for-op port of `packages/client/src/uploads.ts` —
//! the two clients must fold `upload_ops` identically or the same page shows
//! different upload state in each. What lives here is the **data plane** only:
//! op application, the derived progress, and change notification. Nothing in
//! this module writes [`UploadHandle::status`] — the server drives entries, and
//! the client's own API drives the handle, which is
//! [`transfer`](crate::transfer)'s job.
//!
//! # Shape
//!
//! ```text
//! upload_ops ──▶ Uploads (registry, keyed by (store_id, name))
//!                  │
//!                  ├─ UploadCell ── UploadHandle value + subscribers
//!                  │                + per-entry transport state (§10.2)
//!                  └─ UploadCell ── …
//!                       ▲
//!                       │ Mounted::upload(&store_id, name)
//!                    Upload ── snapshot() / updates()
//!                           ── select() / start() / cancel() / reset()
//! ```
//!
//! Handles are created on demand and live for as long as their store is in the
//! index: an [`Upload`] taken before the first op sees the defaults, and
//! [`Uploads::prune`] drops a handle only once its store leaves the tree.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use futures_channel::mpsc::{self, UnboundedSender};
use futures_core::Stream;
use serde::{Deserialize, Serialize};

use crate::generated::StoreId;
use crate::lock;
use crate::transfer::{EntryTransport, UploadControl};

/// `max_entries` before any `config` op lands (`lib/musubi/upload/config.ex`).
const DEFAULT_MAX_ENTRIES: u32 = 1;
/// `max_file_size` before any `config` op lands — 8 MB.
const DEFAULT_MAX_FILE_SIZE: u64 = 8_000_000;
/// `chunk_size` before any `config` op lands — 64 kB.
const DEFAULT_CHUNK_SIZE: u64 = 64_000;
/// The progress value at and above which an entry is complete.
pub(crate) const COMPLETE: u32 = 100;

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

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
/// Driven entirely by the client's own API — [`Upload::select`],
/// [`Upload::start`], [`Upload::cancel`] and [`Upload::reset`] — never by an
/// op (`docs/rust-client.md` §10.2).
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

// ---------------------------------------------------------------------------
// The handle value
// ---------------------------------------------------------------------------

/// One upload's observable state, as a value.
///
/// Read with [`Upload::snapshot`] or received from [`Upload::updates`]. Unlike
/// the TypeScript client — whose handle is one mutable object kept alive for
/// the connection — this is a plain clone, so an old snapshot never mutates
/// under its reader.
#[derive(Debug, Clone, PartialEq)]
pub struct UploadHandle {
    /// The owning store's path.
    pub store_id: StoreId,
    /// The declared upload name.
    pub name: String,
    /// The declared limits; defaults until the `config` op lands.
    pub config: UploadConfig,
    /// The handle's own lifecycle state (client-driven; see [`UploadStatus`]).
    pub status: UploadStatus,
    /// The live entries, in the order the server added them.
    pub entries: Vec<UploadEntry>,
    /// Failures that belong to the handle rather than to one entry.
    pub errors: Vec<UploadError>,
}

/// What one envelope's fold did to a handle.
///
/// `removed` names the entries `cancel` and `reset` deleted, in fold order, so
/// the cell can evict exactly their transport state — see
/// [`UploadCell::apply_ops`].
#[derive(Debug, Default)]
struct Applied {
    /// Whether anything changed, i.e. whether subscribers run.
    touched: bool,
    /// The refs the fold deleted.
    removed: Vec<String>,
}

impl UploadHandle {
    /// The mean of every entry's progress, rounded half-up; `0` with no
    /// entries.
    ///
    /// A plain mean over **all** entries — pending and failed ones included —
    /// exactly as the TypeScript client computes it.
    pub fn progress(&self) -> u32 {
        let count = u64::try_from(self.entries.len()).unwrap_or(u64::MAX);

        if count == 0 {
            return 0;
        }

        // Summed as `u64`: `progress` is `0..=100` by contract, but nothing
        // rejects a larger value on the wire and a `u32` sum could then wrap.
        let total: u64 = self
            .entries
            .iter()
            .map(|entry| u64::from(entry.progress))
            .sum();

        // Integer round-half-up: every operand is non-negative.
        u32::try_from((total + count / 2) / count).unwrap_or(u32::MAX)
    }

    /// The entry with this ref, if it is still live.
    pub fn entry(&self, r#ref: &str) -> Option<&UploadEntry> {
        self.entries.iter().find(|entry| entry.r#ref == r#ref)
    }

    /// Nothing selected.
    pub fn is_idle(&self) -> bool {
        self.status == UploadStatus::Idle
    }

    /// Files were selected and preflighted.
    pub fn is_selecting(&self) -> bool {
        self.status == UploadStatus::Selecting
    }

    /// A transfer is running.
    pub fn is_uploading(&self) -> bool {
        self.status == UploadStatus::Uploading
    }

    /// Every entry finished.
    pub fn is_success(&self) -> bool {
        self.status == UploadStatus::Success
    }

    /// Preflight or a transfer failed.
    pub fn is_error(&self) -> bool {
        self.status == UploadStatus::Error
    }

    /// Adds an entry the preflight reply accepted, unless the matching
    /// `{op: add}` has already landed.
    ///
    /// The reply is delivered before the envelope carrying that op (BDR-0009),
    /// but only the *usual* way round is guaranteed — an `add` that arrived
    /// first is the server's own record of the entry and wins.
    pub(crate) fn seed_entry(&mut self, entry: UploadEntry) {
        if self.position(&entry.r#ref).is_none() {
            self.entries.push(entry);
        }
    }

    /// A fresh handle: framework defaults, idle, no entries.
    fn new(store_id: StoreId, name: String) -> Self {
        Self {
            store_id,
            name,
            config: UploadConfig::default(),
            status: UploadStatus::Idle,
            entries: Vec::new(),
            errors: Vec::new(),
        }
    }

    /// Folds one envelope's ops for this handle, in array order.
    ///
    /// Reports what the fold touched: the TypeScript client's `touched` flag,
    /// which decides whether subscribers run, plus the refs the fold deleted,
    /// which is what the caller prunes transport state by.
    fn apply_ops(&mut self, ops: &[&UploadOp]) -> Applied {
        let mut applied = Applied::default();

        for op in ops {
            // Collected before the op runs: `reset` clears the entry list, so
            // afterwards there is nothing left to name.
            match op {
                UploadOp::Cancel { r#ref, .. } => applied.removed.push(r#ref.clone()),
                UploadOp::Reset { .. } => applied
                    .removed
                    .extend(self.entries.iter().map(|entry| entry.r#ref.clone())),
                _ => {}
            }

            applied.touched |= self.apply_op(op);
        }

        applied
    }

    /// Folds one op. See `applyOps` in `packages/client/src/uploads.ts`.
    fn apply_op(&mut self, op: &UploadOp) -> bool {
        match op {
            UploadOp::Config { config, .. } => {
                self.config = config.clone();
                true
            }
            UploadOp::Add { r#ref, entry, .. } => {
                // An `add` may restate an entry the preflight reply already
                // seeded (BDR-0009 delivers the reply first), so the wire
                // fields win but the entry keeps its position.
                match self.position(r#ref) {
                    Some(index) => {
                        let existing = &mut self.entries[index];

                        existing.progress = entry.progress;
                        existing.status = entry.status;
                        existing.errors = entry.errors.clone();
                    }
                    None => self.entries.push(entry.clone()),
                }

                true
            }
            UploadOp::Progress {
                r#ref, progress, ..
            } => self.with_entry(r#ref, |entry| {
                entry.progress = *progress;
                entry.status = if *progress >= COMPLETE {
                    EntryStatus::Success
                } else {
                    EntryStatus::Uploading
                };
            }),
            UploadOp::Complete { r#ref, .. } => self.with_entry(r#ref, |entry| {
                // The 10 Hz throttle can swallow the final `progress: 100`,
                // so completion sets it rather than assuming it arrived.
                entry.progress = COMPLETE;
                entry.status = EntryStatus::Success;
            }),
            UploadOp::Error { r#ref, error, .. } => {
                match r#ref {
                    Some(r#ref) => {
                        self.with_entry(r#ref, |entry| {
                            entry.status = EntryStatus::Error;
                            entry.errors.push(error.clone());
                        });
                    }
                    None => self.errors.push(error.clone()),
                }

                // Touched even when the ref is unknown: the TypeScript client
                // notifies on every error op.
                true
            }
            UploadOp::Cancel { r#ref, .. } => {
                if let Some(index) = self.position(r#ref) {
                    self.entries.remove(index);
                }

                true
            }
            UploadOp::Reset { .. } => {
                self.entries.clear();
                self.errors.clear();
                true
            }
        }
    }

    /// Mutates the entry with this ref, reporting whether it existed.
    ///
    /// An op for an unknown ref is dropped silently: entries are removed by
    /// `cancel` and by `consume_uploaded_entries/3`, and a late op for one is
    /// not an error.
    fn with_entry(&mut self, r#ref: &str, mutate: impl FnOnce(&mut UploadEntry)) -> bool {
        let Some(index) = self.position(r#ref) else {
            return false;
        };

        mutate(&mut self.entries[index]);

        true
    }

    /// Where this ref sits in the insertion-ordered entry list.
    fn position(&self, r#ref: &str) -> Option<usize> {
        self.entries.iter().position(|entry| entry.r#ref == r#ref)
    }
}

// ---------------------------------------------------------------------------
// The registry
// ---------------------------------------------------------------------------

/// The key every handle is filed under.
///
/// The TypeScript client concatenates `json(store_id) + "\0" + name` because a
/// JS `Map` keys by reference; that string is an implementation detail, not a
/// wire format, so the Rust port hashes the pair directly.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct UploadKey {
    store_id: StoreId,
    name: String,
}

/// One handle's live cell: the current value, its subscribers, and the
/// client-side transport state of its entries.
///
/// The transport state is deliberately *not* on [`UploadHandle`]: bytes,
/// tokens and sub-channels are control-plane machinery, and the handle is a
/// value every subscriber gets a clone of.
#[derive(Debug)]
pub(crate) struct UploadCell {
    /// The owning store's path; immutable, so the control plane reads it
    /// without taking the handle lock.
    pub(crate) store_id: StoreId,
    /// The declared upload name; immutable, as above.
    pub(crate) name: String,
    handle: Mutex<UploadHandle>,
    updates: Mutex<Vec<UnboundedSender<UploadHandle>>>,
    transport: Mutex<HashMap<String, EntryTransport>>,
}

impl UploadCell {
    /// A fresh cell holding a default handle.
    fn new(store_id: StoreId, name: String) -> Self {
        Self {
            handle: Mutex::new(UploadHandle::new(store_id.clone(), name.clone())),
            updates: Mutex::new(Vec::new()),
            transport: Mutex::new(HashMap::new()),
            store_id,
            name,
        }
    }

    /// Applies one envelope's ops for this handle and, if anything changed,
    /// publishes exactly one snapshot — the TypeScript client's single
    /// `notify()` per handle per envelope.
    fn apply_ops(&self, ops: &[&UploadOp]) {
        let (applied, snapshot) = {
            let mut handle = lock(&self.handle);
            let applied = handle.apply_ops(ops);

            if !applied.touched {
                return;
            }

            (applied, handle.clone())
        };

        // `cancel` and `reset` delete entries; their bytes, token and
        // sub-channel go with them, exactly as the TypeScript client drops its
        // internal entry. Only the refs *those ops* removed are evicted:
        // diffing the whole map against the handle instead would drop the
        // transport state of an entry `Upload::select` has already inserted
        // but not yet seeded onto the handle, and `Upload::start` skips an
        // entry with no transport state.
        if !applied.removed.is_empty() {
            let mut transport = lock(&self.transport);

            for r#ref in &applied.removed {
                transport.remove(r#ref);
            }
        }

        self.publish(snapshot);
    }

    /// The current handle value.
    pub(crate) fn snapshot(&self) -> UploadHandle {
        lock(&self.handle).clone()
    }

    /// Mutates the handle and publishes the result unconditionally — the
    /// control plane's `notify()`, which fires on every status change even
    /// when nothing else moved.
    pub(crate) fn update<T>(&self, mutate: impl FnOnce(&mut UploadHandle) -> T) -> T {
        let (outcome, snapshot) = {
            let mut handle = lock(&self.handle);
            let outcome = mutate(&mut handle);

            (outcome, handle.clone())
        };

        self.publish(snapshot);

        outcome
    }

    /// Runs `mutate` over the transport index.
    pub(crate) fn transport<T>(
        &self,
        mutate: impl FnOnce(&mut HashMap<String, EntryTransport>) -> T,
    ) -> T {
        mutate(&mut lock(&self.transport))
    }

    /// Delivers one snapshot to every live subscriber.
    fn publish(&self, snapshot: UploadHandle) {
        lock(&self.updates).retain(|sender| sender.unbounded_send(snapshot.clone()).is_ok());
    }
}

/// Every upload handle of one mounted root, keyed by `(store_id, name)`.
///
/// Shared between the [`PatchEngine`](crate::PatchEngine) that folds the ops
/// and the [`Mounted`](crate::Mounted) that hands out [`Upload`]s, so both see
/// one set of handles.
#[derive(Debug, Default)]
pub struct Uploads {
    handles: Mutex<HashMap<UploadKey, Arc<UploadCell>>>,
    /// How the control plane reaches the server. `None` for a registry with no
    /// connection behind it — a bare [`PatchEngine`](crate::PatchEngine) — in
    /// which case `select`/`start`/`cancel`/`reset` report
    /// [`MusubiError::NotConnected`](crate::MusubiError).
    control: Option<Arc<UploadControl>>,
}

impl Uploads {
    /// The registry a mounted root gets: handles wired to the connection, so
    /// the ones it hands out can also drive transfers.
    pub(crate) fn new(control: Arc<UploadControl>) -> Self {
        Self {
            handles: Mutex::new(HashMap::new()),
            control: Some(control),
        }
    }

    /// The handle for `(store_id, name)`, created on first use.
    ///
    /// Taking a handle before the server has said anything about it is normal:
    /// a subscriber can be attached the moment the marker appears on the state,
    /// and the defaults stand in until the `config` op lands.
    pub fn handle(&self, store_id: &StoreId, name: &str) -> Upload {
        let key = UploadKey {
            store_id: store_id.clone(),
            name: name.to_owned(),
        };

        let cell = Arc::clone(
            lock(&self.handles)
                .entry(key)
                .or_insert_with(|| Arc::new(UploadCell::new(store_id.clone(), name.to_owned()))),
        );

        Upload {
            cell,
            control: self.control.clone(),
        }
    }

    /// Folds one envelope's `upload_ops`, grouped per handle.
    ///
    /// Ops keep their array order within a handle, and each touched handle
    /// publishes once — never once per op.
    pub(crate) fn apply_ops(&self, ops: &[UploadOp]) {
        if ops.is_empty() {
            return;
        }

        // Grouping first keeps the registry lock off the notification path,
        // and preserves "one publish per handle per envelope".
        let mut order: Vec<Arc<UploadCell>> = Vec::new();
        let mut grouped: Vec<Vec<&UploadOp>> = Vec::new();

        for op in ops {
            let cell = self.handle(op.store_id(), op.upload()).cell;

            match order.iter().position(|known| Arc::ptr_eq(known, &cell)) {
                Some(index) => grouped[index].push(op),
                None => {
                    order.push(cell);
                    grouped.push(vec![op]);
                }
            }
        }

        for (cell, ops) in order.iter().zip(&grouped) {
            cell.apply_ops(ops);
        }
    }

    /// Drops every handle whose owning store is gone from the freshly rebuilt
    /// index, ending its `updates()` streams.
    ///
    /// Uploads are not resumable (BDR-0003), and a store that reappears mounts
    /// fresh (BDR-0011), so a vanished store must not leave its handles behind.
    pub(crate) fn prune(&self, live_store_ids: &HashSet<StoreId>) {
        lock(&self.handles).retain(|key, _| live_store_ids.contains(&key.store_id));
    }

    /// Drops every handle. Called when the root leaves the registry.
    pub(crate) fn clear(&self) {
        lock(&self.handles).clear();
    }
}

/// A handle on one upload of one store.
///
/// Cheap to clone; every clone addresses the same upload. Reading is
/// [`snapshot`](Self::snapshot), watching is [`updates`](Self::updates) — the
/// same shape [`Mounted`](crate::Mounted) uses for state and events.
///
/// ```text
/// let avatar = cart.upload(&StoreId::root(), "avatar");
/// let mut updates = avatar.updates();
///
/// while let Some(handle) = updates.next().await {
///     render(handle.progress());
/// }
/// ```
#[derive(Debug, Clone)]
pub struct Upload {
    pub(crate) cell: Arc<UploadCell>,
    pub(crate) control: Option<Arc<UploadControl>>,
}

impl Upload {
    /// The current handle state.
    ///
    /// Always available — an upload with no ops yet reads as an idle handle
    /// carrying the framework defaults.
    pub fn snapshot(&self) -> UploadHandle {
        self.cell.snapshot()
    }

    /// One item per envelope that touched this upload, oldest first.
    ///
    /// The stream **is** the subscription: dropping it unsubscribes, and it
    /// ends when the owning store leaves the tree or the root is unmounted. It
    /// does not replay [`snapshot`](Self::snapshot) — read that first if the
    /// current state matters.
    #[must_use = "the stream is the subscription; dropping it unsubscribes"]
    pub fn updates(&self) -> impl Stream<Item = UploadHandle> + Send + 'static {
        let (sender, receiver) = mpsc::unbounded();

        lock(&self.cell.updates).push(sender);

        receiver
    }
}

#[cfg(test)]
mod tests {
    use futures_executor::block_on;
    use futures_util::{FutureExt, StreamExt};
    use serde_json::{Value, json};

    use super::*;

    // ---- op application, table-driven ------------------------------------

    /// One row: the ops to fold, then the entry refs / statuses / progress and
    /// the handle-level errors they must produce.
    struct Row {
        name: &'static str,
        ops: Vec<Value>,
        entries: Vec<(&'static str, EntryStatus, u32)>,
        handle_errors: usize,
        progress: u32,
    }

    #[test]
    fn op_application_matches_the_typescript_client() {
        let rows = vec![
            Row {
                name: "add seeds a pending entry",
                ops: vec![add("u_1", "pending", 0)],
                entries: vec![("u_1", EntryStatus::Pending, 0)],
                handle_errors: 0,
                progress: 0,
            },
            Row {
                name: "add is an upsert that keeps the position and takes the wire fields",
                ops: vec![
                    add("u_1", "pending", 0),
                    add("u_2", "pending", 0),
                    add("u_1", "uploading", 50),
                ],
                entries: vec![
                    ("u_1", EntryStatus::Uploading, 50),
                    ("u_2", EntryStatus::Pending, 0),
                ],
                handle_errors: 0,
                progress: 25,
            },
            Row {
                name: "progress moves an entry to uploading",
                ops: vec![add("u_1", "pending", 0), progress("u_1", 33)],
                entries: vec![("u_1", EntryStatus::Uploading, 33)],
                handle_errors: 0,
                progress: 33,
            },
            Row {
                name: "progress at 100 is a success without a complete op",
                ops: vec![add("u_1", "pending", 0), progress("u_1", 100)],
                entries: vec![("u_1", EntryStatus::Success, 100)],
                handle_errors: 0,
                progress: 100,
            },
            Row {
                name: "progress for an unknown ref is ignored",
                ops: vec![add("u_1", "pending", 0), progress("u_ghost", 70)],
                entries: vec![("u_1", EntryStatus::Pending, 0)],
                handle_errors: 0,
                progress: 0,
            },
            Row {
                name: "complete forces progress to 100 even if the last one was throttled away",
                ops: vec![
                    add("u_1", "pending", 0),
                    progress("u_1", 40),
                    complete("u_1"),
                ],
                entries: vec![("u_1", EntryStatus::Success, 100)],
                handle_errors: 0,
                progress: 100,
            },
            Row {
                name: "complete for an unknown ref is ignored",
                ops: vec![add("u_1", "pending", 0), complete("u_ghost")],
                entries: vec![("u_1", EntryStatus::Pending, 0)],
                handle_errors: 0,
                progress: 0,
            },
            Row {
                name: "a ref-ed error fails the entry and appends to its errors",
                ops: vec![
                    add("u_1", "uploading", 20),
                    error(Some("u_1"), "chunk_timeout"),
                    error(Some("u_1"), "internal"),
                ],
                entries: vec![("u_1", EntryStatus::Error, 20)],
                handle_errors: 0,
                progress: 20,
            },
            Row {
                name: "a ref-less error lands on the handle",
                ops: vec![add("u_1", "pending", 0), error(None, "too_many_files")],
                entries: vec![("u_1", EntryStatus::Pending, 0)],
                handle_errors: 1,
                progress: 0,
            },
            Row {
                name: "an error for an unknown ref changes nothing",
                ops: vec![add("u_1", "pending", 0), error(Some("u_ghost"), "internal")],
                entries: vec![("u_1", EntryStatus::Pending, 0)],
                handle_errors: 0,
                progress: 0,
            },
            Row {
                name: "cancel deletes the entry instead of marking it cancelled",
                ops: vec![
                    add("u_1", "uploading", 50),
                    add("u_2", "pending", 0),
                    cancel("u_1"),
                ],
                entries: vec![("u_2", EntryStatus::Pending, 0)],
                handle_errors: 0,
                progress: 0,
            },
            Row {
                name: "cancel for an unknown ref changes nothing",
                ops: vec![add("u_1", "pending", 0), cancel("u_ghost")],
                entries: vec![("u_1", EntryStatus::Pending, 0)],
                handle_errors: 0,
                progress: 0,
            },
            Row {
                name: "reset clears entries and handle errors",
                ops: vec![
                    add("u_1", "success", 100),
                    error(None, "too_large"),
                    reset(),
                ],
                entries: vec![],
                handle_errors: 0,
                progress: 0,
            },
            Row {
                name: "progress is the plain mean over every entry, rounded half-up",
                ops: vec![
                    add("u_1", "uploading", 50),
                    add("u_2", "pending", 0),
                    add("u_3", "uploading", 51),
                ],
                entries: vec![
                    ("u_1", EntryStatus::Uploading, 50),
                    ("u_2", EntryStatus::Pending, 0),
                    ("u_3", EntryStatus::Uploading, 51),
                ],
                handle_errors: 0,
                progress: 34,
            },
        ];

        for row in rows {
            let uploads = Uploads::default();

            uploads.apply_ops(&decode(row.ops));

            let handle = uploads.handle(&StoreId::root(), "avatar").snapshot();
            let entries: Vec<(&str, EntryStatus, u32)> = handle
                .entries
                .iter()
                .map(|entry| (entry.r#ref.as_str(), entry.status, entry.progress))
                .collect();

            assert_eq!(entries, row.entries, "entries: {}", row.name);
            assert_eq!(
                handle.errors.len(),
                row.handle_errors,
                "handle errors: {}",
                row.name
            );
            assert_eq!(handle.progress(), row.progress, "progress: {}", row.name);
        }
    }

    #[test]
    fn entry_status_transitions_run_op_by_op() {
        let uploads = Uploads::default();
        let avatar = uploads.handle(&StoreId::root(), "avatar");
        let steps: [(Value, EntryStatus); 5] = [
            (add("u_1", "pending", 0), EntryStatus::Pending),
            (progress("u_1", 10), EntryStatus::Uploading),
            (error(Some("u_1"), "chunk_timeout"), EntryStatus::Error),
            (progress("u_1", 60), EntryStatus::Uploading),
            (complete("u_1"), EntryStatus::Success),
        ];

        for (op, expected) in steps {
            uploads.apply_ops(&decode(vec![op]));

            assert!(matches!(
                avatar.snapshot().entry("u_1"),
                Some(entry) if entry.status == expected
            ));
        }
    }

    #[test]
    fn a_config_op_replaces_the_defaults() {
        let uploads = Uploads::default();

        uploads.apply_ops(&decode(vec![json!({
            "op": "config", "upload": "avatar", "store_id": [],
            "config": {
                "accept": [".png", ".JPG"], "max_entries": 5,
                "max_file_size": 5_000_000, "chunk_size": 32_000
            }
        })]));

        let handle = uploads.handle(&StoreId::root(), "avatar").snapshot();

        assert!(matches!(
            handle.config,
            UploadConfig { accept: UploadAccept::Extensions(exts), max_entries: 5, max_file_size: 5_000_000, chunk_size: 32_000 }
                if exts == [".png".to_owned(), ".JPG".to_owned()]
        ));
    }

    #[test]
    fn a_fresh_handle_carries_the_framework_defaults() {
        let handle = Uploads::default()
            .handle(&StoreId::root(), "avatar")
            .snapshot();

        assert!(matches!(
            handle,
            UploadHandle {
                status: UploadStatus::Idle,
                config: UploadConfig { accept: UploadAccept::Any, max_entries: 1, .. },
                ref name,
                ..
            } if name == "avatar" && handle.entries.is_empty() && handle.errors.is_empty()
        ));
        assert_eq!(handle.progress(), 0);
    }

    // ---- registry --------------------------------------------------------

    #[test]
    fn uploads_of_different_stores_and_names_do_not_alias() {
        let uploads = Uploads::default();

        uploads.apply_ops(&decode(vec![
            add("u_1", "pending", 0),
            json!({
                "op": "add", "upload": "avatar", "store_id": ["panel"], "ref": "u_2",
                "entry": entry("u_2", "pending", 0)
            }),
            json!({
                "op": "add", "upload": "attachment", "store_id": [], "ref": "u_3",
                "entry": entry("u_3", "pending", 0)
            }),
        ]));

        assert_eq!(refs(&uploads, &StoreId::root(), "avatar"), ["u_1"]);
        assert_eq!(refs(&uploads, &store_id(&["panel"]), "avatar"), ["u_2"]);
        assert_eq!(refs(&uploads, &StoreId::root(), "attachment"), ["u_3"]);
    }

    #[test]
    fn the_same_key_always_resolves_to_the_same_handle() {
        let uploads = Uploads::default();
        let first = uploads.handle(&StoreId::root(), "avatar");

        uploads.apply_ops(&decode(vec![add("u_1", "uploading", 20)]));

        let second = uploads.handle(&StoreId::root(), "avatar");

        assert_eq!(first.snapshot(), second.snapshot());
        assert_eq!(first.snapshot().progress(), 20);
    }

    #[test]
    fn prune_drops_only_the_uploads_of_vanished_stores() {
        let uploads = Uploads::default();

        uploads.apply_ops(&decode(vec![
            add("u_1", "pending", 0),
            json!({
                "op": "add", "upload": "avatar", "store_id": ["panel"], "ref": "u_2",
                "entry": entry("u_2", "pending", 0)
            }),
        ]));
        uploads.prune(&HashSet::from([StoreId::root()]));

        assert_eq!(refs(&uploads, &StoreId::root(), "avatar"), ["u_1"]);
        assert!(refs(&uploads, &store_id(&["panel"]), "avatar").is_empty());
    }

    // ---- notification ----------------------------------------------------

    #[test]
    fn a_touched_handle_publishes_once_per_envelope() {
        let uploads = Uploads::default();
        let mut updates = uploads.handle(&StoreId::root(), "avatar").updates();

        uploads.apply_ops(&decode(vec![
            add("u_1", "pending", 0),
            progress("u_1", 40),
            progress("u_1", 80),
        ]));

        let published = block_on(updates.next()).expect("the handle was touched");

        assert!(matches!(
            published.entry("u_1"),
            Some(entry) if entry.progress == 80
        ));

        // One envelope, one publish: nothing else is queued.
        assert!(updates.next().now_or_never().is_none());
    }

    #[test]
    fn an_untouched_handle_publishes_nothing() {
        let uploads = Uploads::default();
        let mut updates = uploads.handle(&StoreId::root(), "avatar").updates();

        // Every op addresses an unknown entry, so nothing changes.
        uploads.apply_ops(&decode(vec![progress("u_ghost", 10), complete("u_ghost")]));

        assert!(updates.next().now_or_never().is_none());
    }

    #[test]
    fn ops_for_two_handles_publish_to_their_own_subscribers() {
        let uploads = Uploads::default();
        let mut root = uploads.handle(&StoreId::root(), "avatar").updates();
        let mut panel = uploads.handle(&store_id(&["panel"]), "avatar").updates();

        uploads.apply_ops(&decode(vec![json!({
            "op": "add", "upload": "avatar", "store_id": ["panel"], "ref": "u_2",
            "entry": entry("u_2", "pending", 0)
        })]));

        assert!(root.next().now_or_never().is_none());
        assert!(matches!(
            block_on(panel.next()),
            Some(handle) if handle.store_id == store_id(&["panel"])
        ));
    }

    #[test]
    fn pruning_a_store_ends_its_update_streams() {
        let uploads = Uploads::default();
        let mut updates = uploads.handle(&store_id(&["panel"]), "avatar").updates();

        uploads.prune(&HashSet::from([StoreId::root()]));

        assert!(matches!(updates.next().now_or_never(), Some(None)));
    }

    #[test]
    fn clearing_the_registry_ends_every_update_stream() {
        let uploads = Uploads::default();
        let mut updates = uploads.handle(&StoreId::root(), "avatar").updates();

        uploads.clear();

        assert!(matches!(updates.next().now_or_never(), Some(None)));
    }

    // ---- wire ------------------------------------------------------------

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
    fn an_unknown_error_code_survives_verbatim() {
        let uploads = Uploads::default();

        uploads.apply_ops(&decode(vec![error(None, "quota_exceeded")]));

        assert!(matches!(
            uploads.handle(&StoreId::root(), "avatar").snapshot().errors.as_slice(),
            [UploadError { code: UploadErrorCode::Other(code), message }]
                if code == "quota_exceeded" && message == "boom"
        ));
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

    // ---- fixtures --------------------------------------------------------

    fn decode(ops: Vec<Value>) -> Vec<UploadOp> {
        ops.into_iter()
            .map(|op| serde_json::from_value(op).expect("op is a valid upload op"))
            .collect()
    }

    fn add(r#ref: &str, status: &str, progress: u32) -> Value {
        json!({
            "op": "add", "upload": "avatar", "store_id": [], "ref": r#ref,
            "entry": entry(r#ref, status, progress)
        })
    }

    fn entry(r#ref: &str, status: &str, progress: u32) -> Value {
        json!({
            "ref": r#ref, "client_name": "me.png", "client_size": 1234,
            "client_type": "image/png", "progress": progress, "status": status,
            "errors": []
        })
    }

    fn progress(r#ref: &str, progress: u32) -> Value {
        json!({
            "op": "progress", "upload": "avatar", "store_id": [],
            "ref": r#ref, "progress": progress
        })
    }

    fn complete(r#ref: &str) -> Value {
        json!({"op": "complete", "upload": "avatar", "store_id": [], "ref": r#ref})
    }

    fn error(r#ref: Option<&str>, code: &str) -> Value {
        json!({
            "op": "error", "upload": "avatar", "store_id": [], "ref": r#ref,
            "error": {"code": code, "message": "boom"}
        })
    }

    fn cancel(r#ref: &str) -> Value {
        json!({"op": "cancel", "upload": "avatar", "store_id": [], "ref": r#ref})
    }

    fn reset() -> Value {
        json!({"op": "reset", "upload": "avatar", "store_id": []})
    }

    fn refs(uploads: &Uploads, store_id: &StoreId, name: &str) -> Vec<String> {
        uploads
            .handle(store_id, name)
            .snapshot()
            .entries
            .iter()
            .map(|entry| entry.r#ref.clone())
            .collect()
    }

    fn store_id(segments: &[&str]) -> StoreId {
        serde_json::from_value(json!(segments)).expect("store id is a string array")
    }
}
