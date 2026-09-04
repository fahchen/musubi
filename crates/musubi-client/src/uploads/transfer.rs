//! The upload control plane: preflight, channel-mode chunk transfer, and
//! external uploaders (BDR-0026, BDR-0027, `docs/rust-client.md` §10.2).
//!
//! Where [`registry`](super::registry) folds what the **server** says about an
//! upload, this module is what the **client** does about it: it drives
//! [`UploadHandle::status`](crate::UploadHandle::status) — the one field no op
//! ever writes — and moves the bytes.
//!
//! ```text
//! Upload::select(files) ── "allow_upload" ──▶ page server
//!        │                     ◀── {config, entries: {client_ref => …}, errors}
//!        ▼
//! Upload::start()
//!        ├─ channel mode ── join "musubi_upload:<ref>" with the preflight
//!        │                  token, then chunk_size binary frames, sequentially
//!        └─ external mode ─ the registered Uploader does the PUT itself, and
//!                           relays progress over "upload_progress"
//! ```
//!
//! The crate stays runtime-free: nothing here spawns, sleeps or reads a file.
//! Concurrency across entries is a `join_all` on the caller's own task, and the
//! bytes arrive as [`UploadFile`] — the embedder reads the file.
//!
//! This is the top of the upload tree: it reads the registry's cells, and
//! nothing in the registry reads back into here.

use std::collections::HashMap;
use std::fmt;
use std::future;
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use futures_channel::oneshot;
use futures_core::future::BoxFuture;
use futures_util::StreamExt;
use futures_util::future::join_all;
use phoenix_channel::{Channel, ChannelEvent, PhoenixSocket, PushError, Reply};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::actor::{ActorMsg, ConnectionInner};
use crate::error::{MusubiError, Result, TransferError};
use crate::generated::StoreId;
use crate::lock;

use super::ops::{COMPLETE, EntryStatus, UploadConfig, UploadEntry, UploadError, UploadStatus};
use super::registry::{Upload, UploadCell};

/// The main-channel preflight push (BDR-0024).
const EVENT_ALLOW_UPLOAD: &str = "allow_upload";
/// The main-channel cancellation push.
const EVENT_CANCEL_UPLOAD: &str = "cancel_upload";
/// The main-channel external-mode progress relay (BDR-0027).
const EVENT_UPLOAD_PROGRESS: &str = "upload_progress";
/// The main-channel external-mode failure report (BDR-0027).
const EVENT_UPLOAD_ERROR: &str = "upload_error";
/// The sub-channel event one slice of a file travels under (BDR-0026).
const EVENT_CHUNK: &str = "chunk";
/// The chunk sub-channel's topic prefix; the entry ref completes it.
const TOPIC_PREFIX: &str = "musubi_upload:";
/// The only `code` the server accepts from a client-reported failure.
const CODE_EXTERNAL_FAILED: &str = "external_failed";

// ---------------------------------------------------------------------------
// The embedder's side: files, uploaders, cancellation
// ---------------------------------------------------------------------------

/// One file to upload: its bytes, plus what the server is told about it.
///
/// `musubi-client` never touches a filesystem, so the embedder reads the file
/// and hands the bytes over. `client_size` on the wire is `bytes.len()` — a
/// size that disagreed with the bytes would strand the transfer, since the
/// server completes a channel-mode upload on `bytes_written >= client_size`.
///
/// ```
/// use musubi_client::UploadFile;
///
/// let file = UploadFile::new("me.png", "image/png", *b"\x89PNG");
///
/// assert_eq!(file.len(), 4);
/// assert_eq!(file.name, "me.png");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UploadFile {
    /// The file name; the extension is what `accept` is matched against.
    pub name: String,
    /// The MIME type, or `""` when unknown. Never validated by the server.
    pub content_type: String,
    /// The bytes, shared so handing them to an uploader costs no copy.
    pub bytes: Arc<[u8]>,
}

impl UploadFile {
    /// Builds a file from anything that becomes a shared byte slice.
    ///
    /// ```
    /// use musubi_client::UploadFile;
    ///
    /// let file = UploadFile::new("notes.txt", "text/plain", b"hi".to_vec());
    ///
    /// assert_eq!(file.content_type, "text/plain");
    /// ```
    pub fn new(
        name: impl Into<String>,
        content_type: impl Into<String>,
        bytes: impl Into<Arc<[u8]>>,
    ) -> Self {
        Self {
            name: name.into(),
            content_type: content_type.into(),
            bytes: bytes.into(),
        }
    }

    /// The byte length, as the server is told it.
    pub fn len(&self) -> u64 {
        self.bytes.len() as u64
    }

    /// Whether the file is empty. An empty file is still a valid upload: it
    /// travels as one empty chunk.
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

/// An external uploader: the app's own transfer for one named destination
/// (BDR-0027).
///
/// The server decides per entry whether it is `channel` or `external`, and
/// names the uploader; this client dispatches on that name against the registry
/// built with [`ConnectionBuilder::uploader`](crate::ConnectionBuilder::uploader).
/// The PUT is entirely the app's — the crate is runtime-free and ships no HTTP
/// client.
///
/// ```text
/// struct S3;
///
/// impl Uploader for S3 {
///     fn upload(&self, request: UploadRequest) -> BoxFuture<'static, Result<(), UploaderError>> {
///         Box::pin(async move {
///             let url = request.meta["url"].as_str().unwrap_or_default().to_owned();
///
///             put(&url, &request.bytes).await.map_err(UploaderError::new)?;
///             request.progress.report(100);
///
///             Ok(())
///         })
///     }
/// }
/// ```
pub trait Uploader: Send + Sync + 'static {
    /// Transfers one entry, reporting progress as it goes.
    ///
    /// Returning `Err` makes the client report `code: "external_failed"` to the
    /// server, which fails the entry; the message is propagated verbatim.
    fn upload(&self, request: UploadRequest) -> BoxFuture<'static, Result<(), UploaderError>>;
}

/// Everything an [`Uploader`] is given for one entry.
#[derive(Debug)]
#[non_exhaustive]
pub struct UploadRequest {
    /// The entry as the client currently has it.
    pub entry: UploadEntry,
    /// The file's bytes.
    pub bytes: Arc<[u8]>,
    /// The app-authored `meta` from `upload_external/3`, verbatim and opaque —
    /// the presigned URL and headers live in here.
    pub meta: Value,
    /// Where to report progress; relayed to the server as `upload_progress`.
    pub progress: UploadProgress,
    /// Cancellation, raised by [`Upload::cancel`] and [`Upload::reset`].
    pub cancel: CancelSignal,
}

/// The progress sink one [`UploadRequest`] carries.
///
/// Reporting is fire-and-forget: the relay is advisory, and an uploader must
/// not have to await the server to keep sending bytes. `100` is reported
/// automatically once the uploader resolves, which is what makes the server
/// emit `{op: complete}`.
#[derive(Clone)]
pub struct UploadProgress {
    relay: Arc<ProgressRelay>,
}

impl UploadProgress {
    /// Reports percent complete, clamped to `0..=100`.
    ///
    /// Reports are **coalesced**, never queued: at most one relay push per
    /// entry is in flight, and the next one carries the newest value reported
    /// while it was. An uploader reporting every few kilobytes therefore cannot
    /// outrun the socket — it only overwrites a number.
    ///
    /// ```text
    /// request.progress.report(sent * 100 / total);
    /// ```
    pub fn report(&self, percent: u32) {
        self.relay.report(percent.min(COMPLETE));
    }
}

impl fmt::Debug for UploadProgress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UploadProgress")
            .field("entry_ref", &self.relay.entry_ref)
            .finish_non_exhaustive()
    }
}

/// One external entry's report channel to the server, and the only producer in
/// this client that an app controls the rate of.
///
/// Each report used to be an independent detached push: one unbounded
/// `ActorMsg::RootPush`, one spawned task, one unbounded socket push, one
/// inflight-map entry and one timeout task, with nothing throttling any of it.
/// That is the one thing here that can sustainably outrun frame encoding, and
/// doing so starves the socket read loop — a spurious reconnect mid-upload,
/// taking every concurrent command with it.
///
/// So reports coalesce to their latest value, and the transfer's outcome is a
/// **barrier**: it is pushed last and nothing follows it. Without that the two
/// were independent, and a queued progress could land after `upload_error` —
/// which moves an entry the server already failed back to uploading
/// (`lib/musubi/page/server.ex`).
struct ProgressRelay {
    control: Arc<UploadControl>,
    store_id: StoreId,
    name: String,
    entry_ref: String,
    /// The entry's own signal. Cancellation *deletes* the entry server-side, so
    /// a report that lands afterwards would recreate state for something the
    /// user abandoned: once this is raised the relay sends nothing more, not
    /// even the final `100`.
    cancel: CancelSignal,
    state: Mutex<RelayState>,
}

/// What the relay has been told but not yet sent.
#[derive(Debug, Default)]
struct RelayState {
    /// The newest unsent percentage. Overwritten, never queued.
    pending: Option<u32>,
    /// The transfer's outcome, once it has one.
    outcome: Option<TransferOutcome>,
    /// Woken by `report` and `close`; taken by whoever wakes.
    waker: Option<Waker>,
}

/// How an external transfer ended, as the relay reports it.
#[derive(Debug)]
enum TransferOutcome {
    /// The uploader resolved. `progress: 100` is the completion signal — there
    /// is no other one in external mode.
    Succeeded,
    /// The uploader failed; the server is told, as `external_failed`.
    Failed {
        /// The uploader's own message, propagated verbatim.
        message: String,
    },
}

/// What the relay does next.
#[derive(Debug)]
enum RelayStep {
    /// Push this percentage, then keep relaying.
    Progress(u32),
    /// Push this terminal event, then stop.
    Terminal(&'static str, Value),
    /// Stop without pushing anything.
    Stop,
}

impl ProgressRelay {
    /// Records the newest percentage, unless the transfer already ended.
    fn report(&self, percent: u32) {
        let waker = {
            let mut state = lock(&self.state);

            // A report after the barrier is late by definition: the uploader
            // resolved, and its outcome is the last word on the entry.
            if state.outcome.is_some() {
                return;
            }

            state.pending = Some(percent);

            state.waker.take()
        };

        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Closes the relay with the transfer's outcome.
    fn close(&self, outcome: TransferOutcome) {
        let waker = {
            let mut state = lock(&self.state);

            state.outcome = Some(outcome);

            state.waker.take()
        };

        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Relays until the transfer is closed and its terminal report has gone
    /// out.
    ///
    /// One progress push at a time, **awaited**: the reply is what "in flight"
    /// means, and awaiting it is what orders everything behind it. Failures are
    /// ignored — the relay is advisory, and a report the server never saw must
    /// not fail the transfer.
    async fn run(&self) {
        loop {
            match future::poll_fn(|cx| self.poll_next(cx)).await {
                RelayStep::Progress(percent) => {
                    let _ = self
                        .control
                        .push(EVENT_UPLOAD_PROGRESS, self.progress_payload(percent))
                        .await;
                }
                RelayStep::Terminal(event, payload) => {
                    self.control.push_detached(event, payload);

                    return;
                }
                RelayStep::Stop => return,
            }
        }
    }

    /// Picks the next thing to send, parking until there is one.
    fn poll_next(&self, cx: &mut Context<'_>) -> Poll<RelayStep> {
        let mut state = lock(&self.state);

        if self.cancel.is_cancelled() {
            return Poll::Ready(RelayStep::Stop);
        }

        // A failure supersedes whatever is still queued: the server moves an
        // entry it already failed back to uploading for a progress that lands
        // after the error (`lib/musubi/page/server.ex`), so an unsent report
        // dies with the transfer. Success does not — a percentage the server
        // has not seen yet is still true, and the final `100` follows it.
        if matches!(state.outcome, Some(TransferOutcome::Failed { .. })) {
            state.pending = None;
        }

        if let Some(percent) = state.pending.take() {
            return Poll::Ready(RelayStep::Progress(percent));
        }

        if let Some(outcome) = state.outcome.take() {
            return Poll::Ready(match outcome {
                TransferOutcome::Succeeded => {
                    RelayStep::Terminal(EVENT_UPLOAD_PROGRESS, self.progress_payload(COMPLETE))
                }
                TransferOutcome::Failed { message } => RelayStep::Terminal(
                    EVENT_UPLOAD_ERROR,
                    json!({
                        "store_id": self.store_id,
                        "name": self.name,
                        "ref": self.entry_ref,
                        "code": CODE_EXTERNAL_FAILED,
                        "message": message,
                    }),
                ),
            });
        }

        state.waker = Some(cx.waker().clone());

        Poll::Pending
    }

    /// One `upload_progress` payload.
    fn progress_payload(&self, percent: u32) -> Value {
        json!({
            "store_id": self.store_id,
            "name": self.name,
            "ref": self.entry_ref,
            "progress": percent,
        })
    }
}

impl fmt::Debug for ProgressRelay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProgressRelay")
            .field("entry_ref", &self.entry_ref)
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

/// Cancellation for one in-flight transfer.
///
/// Uploads are one-shot (BDR-0003): cancelling deletes the entry server-side,
/// so a cancelled transfer is abandoned, never resumed. Poll it between IO
/// steps with [`is_cancelled`](Self::is_cancelled), or `select!` on
/// [`cancelled`](Self::cancelled) to abort a request already in flight.
#[derive(Debug, Clone, Default)]
pub struct CancelSignal {
    state: Arc<Mutex<CancelState>>,
}

/// The flag plus everyone waiting on it.
#[derive(Debug, Default)]
struct CancelState {
    cancelled: bool,
    waiters: Vec<oneshot::Sender<()>>,
}

impl CancelSignal {
    /// Whether cancellation has been requested.
    ///
    /// ```
    /// use musubi_client::CancelSignal;
    ///
    /// assert!(!CancelSignal::default().is_cancelled());
    /// ```
    pub fn is_cancelled(&self) -> bool {
        lock(&self.state).cancelled
    }

    /// Resolves once cancellation is requested — immediately, if it already
    /// has been.
    ///
    /// ```text
    /// futures_util::select! {
    ///     result = put(&url).fuse() => result?,
    ///     () = request.cancel.cancelled().fuse() => return Ok(()),
    /// }
    /// ```
    pub fn cancelled(&self) -> BoxFuture<'static, ()> {
        let (tx, rx) = oneshot::channel();
        let mut state = lock(&self.state);

        if state.cancelled {
            let _ = tx.send(());
        } else {
            state.waiters.push(tx);
        }

        Box::pin(async move {
            let _ = rx.await;
        })
    }

    /// Raises the signal and wakes everyone waiting on it.
    fn cancel(&self) {
        let mut state = lock(&self.state);

        state.cancelled = true;

        for waiter in state.waiters.drain(..) {
            let _ = waiter.send(());
        }
    }
}

/// What an [`Uploader`] reports when its transfer fails.
///
/// A plain message: the server forces the `code` to `external_failed` whatever
/// the client sends, so only the text carries information.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct UploaderError {
    /// What went wrong; propagated to the server verbatim.
    pub message: String,
}

impl UploaderError {
    /// Builds one from anything printable.
    ///
    /// ```
    /// use musubi_client::UploaderError;
    ///
    /// let error = UploaderError::new("403 Forbidden");
    ///
    /// assert_eq!(error.to_string(), "403 Forbidden");
    /// ```
    pub fn new(message: impl fmt::Display) -> Self {
        Self {
            message: message.to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// Per-entry client state
// ---------------------------------------------------------------------------

/// One accepted entry's client-side transport state.
///
/// Kept beside the handle rather than on it: bytes, tokens and sub-channels are
/// not observable state, and [`UploadHandle`](crate::UploadHandle) is a value
/// every subscriber gets a clone of.
#[derive(Debug)]
pub(in crate::uploads) struct EntryTransport {
    bytes: Arc<[u8]>,
    mode: TransferMode,
    cancel: CancelSignal,
    /// The chunk sub-channel, once joined — cancellation leaves it, which is
    /// what makes the server drop the partial file.
    channel: Option<Channel>,
}

impl EntryTransport {
    /// Aborts this entry: raises its cancellation and leaves the sub-channel,
    /// which is what makes the server drop the partial file.
    ///
    /// **The one place either happens.** Deleting the map entry does not stop
    /// anything on its own: a running transfer cloned the bytes, the mode and
    /// the signal before its first await and holds an `Arc` of the cell, so it
    /// survives the deletion — and in external mode the app's own PUT would then
    /// run to completion, landing the file in the destination bucket after the
    /// user cancelled it or navigated away. Aborting is exactly what
    /// `CancelSignal` is for, so every retirement goes through here.
    pub(in crate::uploads) fn abort(&mut self) {
        self.cancel.cancel();

        if let Some(channel) = self.channel.take() {
            let _ = channel.leave();
        }
    }
}

/// How one entry's bytes travel, as preflight decided (BDR-0027).
#[derive(Debug, Clone)]
enum TransferMode {
    /// Chunked over `musubi_upload:<ref>`, authorized by this token.
    Channel { token: String },
    /// Handed to a registered [`Uploader`].
    External { uploader: String, meta: Value },
}

// ---------------------------------------------------------------------------
// The connection seam
// ---------------------------------------------------------------------------

/// How an upload handle reaches the server.
///
/// Main-channel pushes go through the connection actor, which owns the current
/// channel incarnation — a handle must not pin a channel a recovery has
/// replaced. Chunk sub-channels are opened straight on the socket: they are
/// per-entry and short-lived, and the actor has no business tracking them.
pub(crate) struct UploadControl {
    inner: Arc<ConnectionInner>,
    root_id: Arc<str>,
    socket: PhoenixSocket,
    uploaders: Arc<HashMap<String, Arc<dyn Uploader>>>,
}

impl UploadControl {
    /// Wires one root's handles to the connection behind it.
    pub(crate) fn new(
        inner: Arc<ConnectionInner>,
        root_id: Arc<str>,
        socket: PhoenixSocket,
        uploaders: Arc<HashMap<String, Arc<dyn Uploader>>>,
    ) -> Self {
        Self {
            inner,
            root_id,
            socket,
            uploaders,
        }
    }

    /// Pushes on the root's main channel and waits for the reply.
    ///
    /// The actor routes the push and hands the raw outcome back; what a
    /// rejection or a failed push *means* is upload vocabulary, so the mapping
    /// onto [`TransferError`] happens here rather than on the actor.
    async fn push(&self, event: &'static str, payload: Value) -> Result<Value> {
        let (reply_tx, reply_rx) = oneshot::channel();

        self.inner.send(ActorMsg::RootPush {
            root_id: Arc::clone(&self.root_id),
            event,
            payload,
            reply: Some(reply_tx),
        })?;

        // The outer error is the actor's — the root is gone, or its channel is
        // not joined; the inner one is the push's own.
        let routed = reply_rx.await.map_err(|_| MusubiError::Disconnected)?;

        match routed? {
            Ok(reply) => upload_reply(event, reply),
            Err(error) => Err(push_error(error)),
        }
    }

    /// Pushes without waiting for the reply.
    ///
    /// Only the relay's **terminal** report goes this way: it is the last thing
    /// an entry sends, so there is nothing left for its acknowledgement to
    /// order, and a transfer must not hang on a reply nothing reads. Everything
    /// before it is awaited, which is what bounds the relay to one push in
    /// flight.
    fn push_detached(&self, event: &'static str, payload: Value) {
        let _ = self.inner.send(ActorMsg::RootPush {
            root_id: Arc::clone(&self.root_id),
            event,
            payload,
            reply: None,
        });
    }
}

impl fmt::Debug for UploadControl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UploadControl")
            .field("root_id", &self.root_id)
            .field("uploaders", &self.uploaders.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// The public control-plane surface
// ---------------------------------------------------------------------------

impl Upload {
    /// Preflights `files` and seeds the entries the server accepted.
    ///
    /// The reply is authoritative: it carries the live config, one entry per
    /// **accepted** file keyed by the index this call offered it under, and one
    /// error per rejected file. Rejections are handle-level errors — a rejected
    /// file produces no entry and no op at all (BDR-0024) — so a call that
    /// rejects everything still leaves the handle in
    /// [`UploadStatus::Error`].
    ///
    /// The matching `{op: add}` ops arrive **after** this reply (BDR-0009), and
    /// merge into the entries seeded here.
    ///
    /// A preflight that fails leaves the handle in [`UploadStatus::Error`] —
    /// including when this future is dropped before the reply lands. The error
    /// is returned to the caller *and* published, because the caller is rarely
    /// the only one watching: a spinner bound to `status` would otherwise sit in
    /// [`UploadStatus::Selecting`] forever.
    ///
    /// ```text
    /// let entries = avatar.select(vec![UploadFile::new("me.png", "image/png", bytes)]).await?;
    /// ```
    pub async fn select(&self, files: Vec<UploadFile>) -> Result<Vec<UploadEntry>> {
        if files.is_empty() {
            return Ok(self.snapshot().entries);
        }

        let control = self.control()?.clone();
        // Armed across everything below, and the reason none of the `?`s need
        // to know about status: whatever ends this selection short of the
        // final write — a rejected push, a timeout, a malformed reply, or the
        // future being dropped — publishes `Error` when the guard goes.
        let selection = self.cell.begin_selection();

        self.cell.update(|handle| {
            handle.status = UploadStatus::Selecting;
            handle.errors.clear();
        });

        let offered: Vec<Value> = files
            .iter()
            .enumerate()
            .map(|(index, file)| {
                json!({
                    "client_ref": index.to_string(),
                    "name": file.name,
                    "size": file.len(),
                    "type": file.content_type,
                })
            })
            .collect();

        let reply = control
            .push(
                EVENT_ALLOW_UPLOAD,
                json!({
                    "store_id": self.cell.store_id,
                    "name": self.cell.name,
                    "entries": offered,
                }),
            )
            .await?;

        let reply: PreflightReply = serde_json::from_value(reply)
            .map_err(|_| MusubiError::Protocol("allow_upload reply did not match the contract"))?;

        let mut accepted: Vec<(usize, AcceptedEntry)> = Vec::with_capacity(reply.entries.len());

        for (client_ref, entry) in reply.entries {
            // The reply keys entries by the `client_ref` this call chose, which
            // is the file's index. Anything else is the server describing a
            // file this selection never offered: there is no file to transfer
            // and no entry for an error to attach to, so it fails the
            // selection rather than being skipped — a reply made only of those
            // would otherwise finish with no entries, no errors, and a handle
            // still reading `selecting`.
            let index = client_ref
                .parse::<usize>()
                .ok()
                .filter(|index| *index < files.len())
                .ok_or(MusubiError::Protocol(
                    "allow_upload reply named a client_ref this selection never offered",
                ))?;

            accepted.push((index, entry));
        }

        // Sorted by the offered index, so entry order is selection order
        // whatever order the server's map iterates in.
        accepted.sort_by_key(|(index, _)| *index);

        let mut transport = Vec::with_capacity(accepted.len());
        let mut seeded = Vec::with_capacity(accepted.len());

        for (index, accepted) in accepted {
            let file = &files[index];
            let (entry_ref, mode) = accepted.split();

            transport.push((
                entry_ref.clone(),
                EntryTransport {
                    bytes: Arc::clone(&file.bytes),
                    mode,
                    cancel: CancelSignal::default(),
                    channel: None,
                },
            ));

            seeded.push(UploadEntry {
                r#ref: entry_ref,
                client_name: file.name.clone(),
                client_size: file.len(),
                client_type: file.content_type.clone(),
                progress: 0,
                status: EntryStatus::Pending,
                errors: Vec::new(),
            });
        }

        // Refused outright if the handle was retired while the preflight was in
        // flight: a teardown aborts the transport state it can see, and state
        // inserted behind it would never be aborted by anything.
        if !self.cell.insert_transport(transport) {
            return Err(MusubiError::Unmounted);
        }

        let errors: Vec<UploadError> = reply.errors.into_iter().map(|error| error.error).collect();

        selection.settled();

        Ok(self.cell.update(|handle| {
            handle.config = reply.config;

            for entry in seeded {
                handle.seed_entry(entry);
            }

            handle.status = if errors.is_empty() {
                UploadStatus::Selecting
            } else {
                UploadStatus::Error
            };
            handle.errors = errors;

            handle.entries.clone()
        }))
    }

    /// Transfers every selected entry, concurrently, and resolves when they
    /// have all finished.
    ///
    /// Each entry travels the way preflight decided: chunked over its own
    /// sub-channel, or through the registered [`Uploader`]. The handle ends in
    /// [`UploadStatus::Success`] only when nothing failed — an entry the server
    /// failed with `{op: error}` counts, even though no transfer here returned
    /// an error.
    ///
    /// The first failure is returned; the rest still ran to completion.
    ///
    /// An entry is transferred **once**: finishing it consumes its transport
    /// state, so calling this again after a transfer — or after a cancellation
    /// — moves only what is still outstanding. A second call while one is
    /// running is refused with [`TransferError::AlreadyStarted`] rather than
    /// racing it.
    ///
    /// ```text
    /// avatar.select(files).await?;
    /// avatar.start().await?;
    /// ```
    pub async fn start(&self) -> Result<()> {
        let control = self.control()?.clone();

        // One transfer per handle. Two concurrent ones would attach the same
        // `musubi_upload:<ref>` topic twice: the socket replaces the registry
        // entry and bumps the generation, so the loser is disconnected, its
        // pushes go stale, and its cleanup clears the channel the winner is
        // still using — after which no `cancel` can leave it.
        let Some(_claim) = self.cell.claim_transfer() else {
            return Err(TransferError::AlreadyStarted {
                name: self.cell.name.clone(),
            }
            .into());
        };

        self.cell
            .update(|handle| handle.status = UploadStatus::Uploading);

        let handle = self.snapshot();
        // Driven off the handle's entry order rather than the transport map's,
        // so concurrent transfers start in selection order.
        let jobs: Vec<UploadEntry> = handle
            .entries
            .iter()
            .filter(|entry| {
                self.cell
                    .transport(|transport| transport.contains_key(&entry.r#ref))
            })
            .cloned()
            .collect();

        let chunk_size = handle.config.chunk_size;
        let outcomes = join_all(
            jobs.into_iter()
                .map(|entry| self.transfer(&control, entry, chunk_size)),
        )
        .await;

        let failure = outcomes.into_iter().find_map(std::result::Result::err);

        self.cell.update(|handle| {
            let failed = failure.is_some() || handle.entries.iter().any(UploadEntry::is_error);

            handle.status = if failed {
                UploadStatus::Error
            } else {
                UploadStatus::Success
            };
        });

        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// Cancels one entry, or every entry when `entry_ref` is `None`.
    ///
    /// Aborts the transfer, leaves the chunk sub-channel — which is what makes
    /// the server delete the partial file — and tells the page server, which
    /// answers with `{op: cancel}`. The handle's own status is left alone: the
    /// entry disappears, so there is no cancelled state to observe (BDR-0025).
    ///
    /// ```text
    /// avatar.cancel(None).await?;
    /// ```
    pub async fn cancel(&self, entry_ref: Option<&str>) -> Result<()> {
        let control = self.control()?.clone();

        let refs: Vec<String> = match entry_ref {
            Some(entry_ref) => vec![entry_ref.to_owned()],
            // Every live entry, not just the ones with transport state: an
            // `{op: add}` that outran its preflight reply is still cancellable,
            // and so is an entry another client of the same store selected.
            None => {
                let mut refs: Vec<String> = self
                    .snapshot()
                    .entries
                    .into_iter()
                    .map(|entry| entry.r#ref)
                    .collect();

                self.cell.transport(|transport| {
                    for entry_ref in transport.keys() {
                        if !refs.contains(entry_ref) {
                            refs.push(entry_ref.clone());
                        }
                    }
                });

                refs
            }
        };

        for entry_ref in refs {
            // Aborted, not removed: the entry is deleted by the `{op: cancel}`
            // the server answers with, which is what also retires its transport
            // state.
            self.cell.transport(|transport| {
                if let Some(entry) = transport.get_mut(&entry_ref) {
                    entry.abort();
                }
            });

            // Sequential, like the TypeScript client: each cancel is confirmed
            // before the next is sent, so a failure names the entry it belongs
            // to.
            control
                .push(
                    EVENT_CANCEL_UPLOAD,
                    json!({
                        "store_id": self.cell.store_id,
                        "name": self.cell.name,
                        "ref": entry_ref,
                    }),
                )
                .await?;
        }

        Ok(())
    }

    /// Cancels everything and returns the handle to [`UploadStatus::Idle`].
    ///
    /// ```text
    /// avatar.reset().await?;
    /// ```
    pub async fn reset(&self) -> Result<()> {
        self.cancel(None).await?;

        self.cell.drop_transport(None);
        self.cell.update(|handle| {
            handle.entries.clear();
            handle.errors.clear();
            handle.status = UploadStatus::Idle;
        });

        Ok(())
    }

    /// The connection behind this handle, or [`MusubiError::NotConnected`] for
    /// a registry with none — a bare [`PatchEngine`](crate::PatchEngine).
    fn control(&self) -> Result<&Arc<UploadControl>> {
        self.control.as_ref().ok_or(MusubiError::NotConnected)
    }

    /// Transfers one entry the way preflight decided.
    async fn transfer(
        &self,
        control: &Arc<UploadControl>,
        entry: UploadEntry,
        chunk_size: u64,
    ) -> Result<()> {
        // Read once: a cancel between `start` and here removed the entry, and
        // there is then nothing left to send.
        let Some((bytes, mode, cancel)) = self.cell.transport(|transport| {
            transport.get(&entry.r#ref).map(|state| {
                (
                    Arc::clone(&state.bytes),
                    state.mode.clone(),
                    state.cancel.clone(),
                )
            })
        }) else {
            return Ok(());
        };

        // The entry belongs to this transfer from here on, and leaves with it
        // whatever the outcome — including the `start()` future being dropped,
        // which no code on the post-await path could promise. Consuming on
        // *every* terminal outcome is also what stops a later `start()` sending
        // the same entry again: the preflight token verifies statelessly for
        // 600s and the server opens a fresh temp file each time, so a replay
        // orphans the one before it.
        let _consumed = Transferred {
            cell: Arc::clone(&self.cell),
            entry_ref: entry.r#ref.clone(),
        };

        match mode {
            TransferMode::Channel { token } => {
                self.transfer_channel(control, &entry.r#ref, &token, &bytes, &cancel, chunk_size)
                    .await
            }
            TransferMode::External { uploader, meta } => {
                self.transfer_external(control, entry, bytes, uploader, meta, cancel)
                    .await
            }
        }
    }

    /// Channel mode (BDR-0026): join the per-entry topic with the preflight
    /// token, then push `chunk_size` slices sequentially.
    ///
    /// Completion is server-detected — when the running total reaches
    /// `client_size` the server replies `progress: 100` and stops the channel,
    /// and there is no `"close"` event to send. The authoritative signal is the
    /// `{op: complete}` that lands on the main channel; the per-chunk reply is
    /// only an ack.
    async fn transfer_channel(
        &self,
        control: &Arc<UploadControl>,
        entry_ref: &str,
        token: &str,
        bytes: &[u8],
        cancel: &CancelSignal,
        chunk_size: u64,
    ) -> Result<()> {
        let topic = format!("{TOPIC_PREFIX}{entry_ref}");

        let (channel, mut events) = control
            .socket
            .channel(topic.clone(), json!({"token": token}))
            .await
            .map_err(|_| MusubiError::Disconnected)?;

        // Recorded so `cancel` can leave it mid-transfer — and, when the entry
        // went away while the socket was opening it, left right here. Nothing
        // else knows about this channel yet: the retirement that removed the
        // entry ran before there was anything to take, so a channel that fails
        // to claim its entry has this frame as its only owner, and one left
        // registered would be rejoined by the socket's own recovery.
        let claimed = self
            .cell
            .transport(|transport| match transport.get_mut(entry_ref) {
                Some(state) => {
                    state.channel = Some(channel.clone());

                    true
                }
                None => false,
            });

        if !claimed {
            let _ = channel.leave();

            return Err(TransferError::Cancelled {
                entry_ref: entry_ref.to_owned(),
            }
            .into());
        }

        let outcome = async {
            channel.join().map_err(|_| MusubiError::Disconnected)?;

            match events.next().await {
                Some(ChannelEvent::Joined { .. }) => {}
                Some(ChannelEvent::JoinError { response }) => {
                    return Err(MusubiError::Join {
                        topic: topic.clone(),
                        reason: reason_of(&response),
                    });
                }
                Some(ChannelEvent::JoinTimeout) => return Err(MusubiError::Timeout),
                // The socket went away, or the channel was superseded.
                _ => return Err(MusubiError::Disconnected),
            }

            self.push_chunks(&channel, entry_ref, bytes, cancel, chunk_size)
                .await
        }
        .await;

        // Always: the server stops the channel on the final chunk and on every
        // failure, and a registered channel left behind would be rejoined by
        // the socket's own recovery — with a token still valid, that would open
        // a second upload of the same entry.
        let _ = channel.leave();
        self.cell.transport(|transport| {
            if let Some(state) = transport.get_mut(entry_ref) {
                state.channel = None;
            }
        });

        outcome
    }

    /// Pushes one file as `chunk_size` binary frames, sequentially.
    async fn push_chunks(
        &self,
        channel: &Channel,
        entry_ref: &str,
        bytes: &[u8],
        cancel: &CancelSignal,
        chunk_size: u64,
    ) -> Result<()> {
        let chunk_size = usize::try_from(chunk_size).unwrap_or(usize::MAX).max(1);
        // An empty file still needs one (empty) chunk: the server completes on
        // the first chunk whose running total reaches `client_size`, so sending
        // nothing would leave the entry hanging until the chunk-timeout
        // watchdog fires. (The TypeScript client's `offset < size` loop does
        // hang; this is a deliberate divergence, noted in §10.2.)
        let slices: Vec<&[u8]> = if bytes.is_empty() {
            vec![&[]]
        } else {
            bytes.chunks(chunk_size).collect()
        };

        for slice in slices {
            if cancel.is_cancelled() {
                return Err(TransferError::Cancelled {
                    entry_ref: entry_ref.to_owned(),
                }
                .into());
            }

            let reply = channel
                .push_binary(EVENT_CHUNK, slice.to_vec())
                .await
                .map_err(push_error)?;

            if !reply.is_ok() {
                return Err(TransferError::Chunk {
                    entry_ref: entry_ref.to_owned(),
                    reason: reason_of(&reply.response),
                }
                .into());
            }
        }

        Ok(())
    }

    /// External mode (BDR-0027): the registered uploader moves the bytes and
    /// this client only relays the outcome.
    async fn transfer_external(
        &self,
        control: &Arc<UploadControl>,
        entry: UploadEntry,
        bytes: Arc<[u8]>,
        uploader: String,
        meta: Value,
        cancel: CancelSignal,
    ) -> Result<()> {
        let entry_ref = entry.r#ref.clone();

        let Some(implementation) = control.uploaders.get(&uploader).cloned() else {
            // Nothing is reported to the server: it is a client-side
            // misconfiguration, not a transfer failure, and the entry stays
            // pending until it is cancelled or the page goes away.
            return Err(TransferError::NoUploader {
                uploader,
                entry_ref,
            }
            .into());
        };

        let relay = Arc::new(ProgressRelay {
            control: Arc::clone(control),
            store_id: self.cell.store_id.clone(),
            name: self.cell.name.clone(),
            entry_ref: entry_ref.clone(),
            cancel: cancel.clone(),
            state: Mutex::new(RelayState::default()),
        });

        let request = UploadRequest {
            entry,
            bytes,
            meta,
            progress: UploadProgress {
                relay: Arc::clone(&relay),
            },
            cancel,
        };

        // The relay runs on *this* task, beside the uploader: the crate spawns
        // nothing, and a detached push is what made the old relay unbounded and
        // unordered in the first place. `run` only resolves early when the
        // entry was cancelled, which is why the flag is needed at all.
        let mut uploading = pin!(implementation.upload(request));
        let mut relaying = pin!(relay.run());
        let mut relayed = false;

        let outcome = future::poll_fn(|cx| {
            if !relayed && relaying.as_mut().poll(cx).is_ready() {
                relayed = true;
            }

            uploading.as_mut().poll(cx)
        })
        .await;

        relay.close(match &outcome {
            Ok(()) => TransferOutcome::Succeeded,
            Err(error) => TransferOutcome::Failed {
                message: error.message.clone(),
            },
        });

        // Drains the barrier: the final `progress: 100` — the only thing that
        // makes the server mark the entry `:success` and emit `{op: complete}`
        // — or the `upload_error` that fails it, and nothing after either.
        if !relayed {
            relaying.await;
        }

        match outcome {
            Ok(()) => Ok(()),
            Err(error) => Err(TransferError::Uploader {
                entry_ref,
                message: error.message,
            }
            .into()),
        }
    }
}

/// Consumes one entry's transport state when its transfer ends.
///
/// A guard rather than a step after the await: dropping the `start()` future —
/// a `select!` losing its race, a task abandoned on navigation — must still
/// retire the entry and leave whatever sub-channel it had joined.
struct Transferred {
    cell: Arc<UploadCell>,
    entry_ref: String,
}

impl Drop for Transferred {
    fn drop(&mut self) {
        self.cell.drop_transport(Some(&self.entry_ref));
    }
}

// ---------------------------------------------------------------------------
// The preflight reply
// ---------------------------------------------------------------------------

/// The `allow_upload` reply (`lib/musubi/page/server.ex` `build_preflight_reply/2`).
///
/// Its `ref` field is the **upload name**, not an entry ref, and nothing needs
/// it: the handle already knows which upload it is.
#[derive(Debug, Deserialize)]
struct PreflightReply {
    config: UploadConfig,
    /// Accepted entries, keyed by the `client_ref` the client offered.
    #[serde(default)]
    entries: HashMap<String, AcceptedEntry>,
    /// Rejected files. A rejection produces no entry and no op.
    #[serde(default)]
    errors: Vec<PreflightRejection>,
}

/// One accepted entry, and how its bytes should travel.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum AcceptedEntry {
    /// Chunked over a sub-channel, authorized by a signed token.
    Channel { entry_ref: String, token: String },
    /// Handed to a named uploader; `meta` is opaque app data.
    External {
        entry_ref: String,
        #[serde(default)]
        uploader: String,
        #[serde(default)]
        meta: Value,
    },
}

impl AcceptedEntry {
    /// Splits into the entry ref and the transfer mode it implies.
    fn split(self) -> (String, TransferMode) {
        match self {
            Self::Channel { entry_ref, token } => (entry_ref, TransferMode::Channel { token }),
            Self::External {
                entry_ref,
                uploader,
                meta,
            } => (entry_ref, TransferMode::External { uploader, meta }),
        }
    }
}

/// One rejected file. `client_ref` identifies which one; it is dropped here
/// because a rejection has no entry to attach to.
#[derive(Debug, Deserialize)]
struct PreflightRejection {
    error: UploadError,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Maps a push that produced no reply onto the shared taxonomy (§11).
fn push_error(error: PushError) -> MusubiError {
    match error {
        PushError::Timeout => MusubiError::Timeout,
        PushError::NotJoined | PushError::Stale => MusubiError::NotConnected,
        PushError::Disconnected | PushError::SocketClosed(_) => MusubiError::Disconnected,
        PushError::MalformedReply => {
            MusubiError::Protocol("upload reply was not a phx_reply payload")
        }
        PushError::Unframable(_) => {
            MusubiError::Protocol("upload chunk could not be framed as a binary push")
        }
        // `PushError` is `#[non_exhaustive]`; any future variant still means no
        // reply can arrive.
        error => {
            tracing::warn!(%error, "unrecognized upload push failure");

            MusubiError::Disconnected
        }
    }
}

/// Maps one main-channel reply onto `Ok(response)` / a [`TransferError`].
fn upload_reply(event: &'static str, reply: Reply) -> Result<Value> {
    if reply.is_ok() {
        return Ok(reply.response);
    }

    Err(TransferError::Rejected {
        event,
        reason: reason_of(&reply.response),
    }
    .into())
}

/// Reads an error response's `reason`, falling back to the whole response.
fn reason_of(response: &Value) -> String {
    response
        .get("reason")
        .and_then(Value::as_str)
        .map_or_else(|| response.to_string(), str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_executor::block_on;

    #[test]
    fn a_cancel_signal_wakes_everyone_waiting_on_it() {
        let signal = CancelSignal::default();
        let waiting = signal.cancelled();

        assert!(!signal.is_cancelled());

        signal.cancel();

        assert!(signal.is_cancelled());
        block_on(waiting);
        // Taken after the fact, it resolves immediately.
        block_on(signal.cancelled());
    }

    #[test]
    fn a_preflight_reply_decodes_both_transfer_modes() {
        let mut reply: PreflightReply = serde_json::from_value(json!({
            "ref": "avatar",
            "config": {
                "accept": [".png"], "max_entries": 5,
                "max_file_size": 5_000_000, "chunk_size": 64_000
            },
            "entries": {
                "0": {"type": "channel", "entry_ref": "u_a3f", "token": "SFMyNTY"},
                "1": {
                    "type": "external", "entry_ref": "u_b9e",
                    "uploader": "S3", "meta": {"url": "https://example.test"}
                }
            },
            "errors": [
                {"client_ref": "2", "error": {"code": "too_large", "message": "too big"}}
            ]
        }))
        .unwrap();

        assert_eq!(reply.config.max_entries, 5);
        assert!(matches!(
            reply.errors.as_slice(),
            [PreflightRejection { error }] if error.message == "too big"
        ));
        assert!(matches!(
            reply.entries.remove("0").unwrap().split(),
            (entry_ref, TransferMode::Channel { token })
                if entry_ref == "u_a3f" && token == "SFMyNTY"
        ));
        assert!(matches!(
            reply.entries.remove("1").unwrap().split(),
            (entry_ref, TransferMode::External { uploader, meta })
                if entry_ref == "u_b9e" && uploader == "S3"
                    && meta["url"] == json!("https://example.test")
        ));
    }

    /// Regression: `select` inserts an entry's transport state before it
    /// seeds the entry onto the handle, and those two steps run on the
    /// caller's task while envelopes fold on another. An envelope that lands
    /// in between must leave the freshly inserted transport alone — otherwise
    /// `start` silently skips the entry and the handle still reports success.
    #[test]
    fn folding_an_envelope_only_evicts_the_transport_of_the_entries_it_deleted() {
        let uploads = crate::uploads::Uploads::default();
        let avatar = uploads.handle(&StoreId::root(), "avatar");

        uploads.apply_ops(&ops(json!([{
            "op": "add", "upload": "avatar", "store_id": [], "ref": "u_1",
            "entry": {
                "ref": "u_1", "client_name": "a.png", "client_size": 1,
                "client_type": "image/png", "progress": 0, "status": "pending", "errors": []
            }
        }])));

        // `u_2` is the entry mid-`select`: transport inserted, not yet seeded.
        for r#ref in ["u_1", "u_2"] {
            avatar.cell.transport(|transport| {
                transport.insert(r#ref.to_owned(), transport_of(r#ref));
            });
        }

        uploads.apply_ops(&ops(json!([
            {"op": "progress", "upload": "avatar", "store_id": [], "ref": "u_1", "progress": 50}
        ])));

        assert_eq!(
            transport_refs(&avatar),
            ["u_1", "u_2"],
            "an unrelated op must not evict a transport the handle has not seen yet"
        );

        uploads.apply_ops(&ops(json!([
            {"op": "cancel", "upload": "avatar", "store_id": [], "ref": "u_1"}
        ])));

        assert_eq!(
            transport_refs(&avatar),
            ["u_2"],
            "a cancel evicts exactly the entry it deleted"
        );
    }

    #[test]
    fn a_reply_without_a_reason_string_is_reported_whole() {
        assert_eq!(
            reason_of(&json!({"reason": "unauthorized"})),
            "unauthorized"
        );
        assert_eq!(reason_of(&json!({"detail": 1})), r#"{"detail":1}"#);
    }

    fn ops(ops: Value) -> Vec<crate::uploads::UploadOp> {
        serde_json::from_value(ops).expect("valid upload ops")
    }

    fn transport_of(r#ref: &str) -> EntryTransport {
        EntryTransport {
            bytes: Arc::from(r#ref.as_bytes()),
            mode: TransferMode::Channel {
                token: "tok".to_owned(),
            },
            cancel: CancelSignal::default(),
            channel: None,
        }
    }

    fn transport_refs(avatar: &Upload) -> Vec<String> {
        avatar.cell.transport(|transport| {
            let mut refs: Vec<String> = transport.keys().cloned().collect();
            refs.sort();

            refs
        })
    }
}
