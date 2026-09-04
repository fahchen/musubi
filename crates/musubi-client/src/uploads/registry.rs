//! The upload data plane: the per-handle state machine and the registry a
//! mounted root exposes it through (BDR-0024–BDR-0028, `docs/uploads.md`,
//! `docs/rust-client.md` §10).
//!
//! This is a deliberate op-for-op port of `packages/client/src/uploads.ts` —
//! the two clients must fold `upload_ops` identically or the same page shows
//! different upload state in each. What lives here is the **data plane** only:
//! op application, the derived progress, and change notification. Nothing in
//! this module writes [`UploadHandle::status`] — the server drives entries, and
//! the client's own API drives the handle, which is
//! [`transfer`](super::transfer)'s job.
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
//!
//! The two control-plane types this module names — [`EntryTransport`] on a cell
//! and [`UploadControl`] behind a handed-out [`Upload`] — are held, never read:
//! what is inside them is [`transfer`](super::transfer)'s alone.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use futures_channel::mpsc::{self, UnboundedSender};
use futures_core::Stream;

use crate::generated::StoreId;
use crate::lock;

use super::ops::{
    COMPLETE, EntryStatus, UploadConfig, UploadEntry, UploadError, UploadOp, UploadStatus,
};
use super::transfer::{EntryTransport, UploadControl};

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
    pub(in crate::uploads) fn seed_entry(&mut self, entry: UploadEntry) {
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
pub(in crate::uploads) struct UploadCell {
    /// The owning store's path; immutable, so the control plane reads it
    /// without taking the handle lock.
    pub(in crate::uploads) store_id: StoreId,
    /// The declared upload name; immutable, as above.
    pub(in crate::uploads) name: String,
    handle: Mutex<UploadHandle>,
    updates: Mutex<Vec<UnboundedSender<UploadHandle>>>,
    transport: Mutex<HashMap<String, EntryTransport>>,
    /// Terminal for the whole cell, set by [`close`](Self::close).
    ///
    /// Taken **before** `updates` and `transport`, and held across both: a
    /// teardown and the preflight landing on top of it have to be ordered
    /// against each other, not merely atomic one at a time. That order is the
    /// whole reason this is a `Mutex<bool>` rather than an atomic.
    closed: Mutex<bool>,
    /// Which selection attempt is current; bumped by every
    /// [`Upload::select`](super::Upload::select).
    ///
    /// A [`Selection`] guard that is still the current attempt when it is
    /// dropped is an *unfinished* one, and publishes
    /// [`UploadStatus::Error`] — including when the future it belonged to was
    /// dropped mid-flight.
    selection: Mutex<u64>,
    /// Whether a [`Upload::start`](super::Upload::start) is transferring right
    /// now. One claim per cell: two concurrent starts would attach the same
    /// `musubi_upload:<ref>` topic twice and the loser's cleanup would clobber
    /// the winner's channel.
    transferring: Mutex<bool>,
}

impl UploadCell {
    /// A fresh cell holding a default handle.
    fn new(store_id: StoreId, name: String) -> Self {
        Self {
            handle: Mutex::new(UploadHandle::new(store_id.clone(), name.clone())),
            updates: Mutex::new(Vec::new()),
            transport: Mutex::new(HashMap::new()),
            closed: Mutex::new(false),
            selection: Mutex::new(0),
            transferring: Mutex::new(false),
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
        for r#ref in &applied.removed {
            self.drop_transport(Some(r#ref));
        }

        self.publish(snapshot);
    }

    /// The current handle value.
    fn snapshot(&self) -> UploadHandle {
        lock(&self.handle).clone()
    }

    /// Mutates the handle and publishes the result unconditionally — the
    /// control plane's `notify()`, which fires on every status change even
    /// when nothing else moved.
    pub(in crate::uploads) fn update<T>(&self, mutate: impl FnOnce(&mut UploadHandle) -> T) -> T {
        let (outcome, snapshot) = {
            let mut handle = lock(&self.handle);
            let outcome = mutate(&mut handle);

            (outcome, handle.clone())
        };

        self.publish(snapshot);

        outcome
    }

    /// Runs `mutate` over the transport index.
    ///
    /// Reads and in-place edits only — **removal goes through
    /// [`drop_transport`](Self::drop_transport)** and insertion through
    /// [`insert_transport`](Self::insert_transport), because both carry
    /// obligations this raw accessor cannot enforce.
    pub(in crate::uploads) fn transport<T>(
        &self,
        mutate: impl FnOnce(&mut HashMap<String, EntryTransport>) -> T,
    ) -> T {
        mutate(&mut lock(&self.transport))
    }

    /// Records the transport state one preflight accepted — all of it, or none
    /// of it — reporting whether the insert happened.
    ///
    /// `false` means the cell was closed underneath the preflight, which is a
    /// live race: the reply is awaited on the caller's task while a teardown
    /// runs on the actor's. Re-populating a closed cell would leave transport
    /// state — bytes, a token, a joined sub-channel to come — that no teardown
    /// will ever abort again, so the entries are dropped here instead and the
    /// caller fails the selection.
    pub(in crate::uploads) fn insert_transport(
        &self,
        entries: Vec<(String, EntryTransport)>,
    ) -> bool {
        let closed = lock(&self.closed);

        if *closed {
            return false;
        }

        let mut transport = lock(&self.transport);

        for (entry_ref, state) in entries {
            transport.insert(entry_ref, state);
        }

        true
    }

    /// Removes transport state — one entry, or every entry when `entry_ref` is
    /// `None`.
    ///
    /// **The only way transport state leaves a cell.** Every removal raises the
    /// entry's [`CancelSignal`](crate::CancelSignal) and leaves its sub-channel
    /// first: a transfer cloned its state before it started and holds an `Arc`
    /// of this cell, so dropping the map entry alone would not stop it — in
    /// external mode the app's own PUT would run to completion and the file
    /// would land in the destination bucket *after* the user cancelled it or
    /// navigated away.
    pub(in crate::uploads) fn drop_transport(&self, entry_ref: Option<&str>) {
        let mut transport = lock(&self.transport);

        match entry_ref {
            Some(entry_ref) => {
                if let Some(mut state) = transport.remove(entry_ref) {
                    state.abort();
                }
            }
            None => {
                for (_, mut state) in transport.drain() {
                    state.abort();
                }
            }
        }
    }

    /// Claims this cell's single in-flight transfer, or `None` if one is
    /// already running.
    ///
    /// Releasing is the guard's [`Drop`], so a `start()` future that is dropped
    /// mid-transfer releases the claim too.
    pub(in crate::uploads) fn claim_transfer(self: &Arc<Self>) -> Option<TransferClaim> {
        let mut transferring = lock(&self.transferring);

        if *transferring {
            return None;
        }

        *transferring = true;

        Some(TransferClaim {
            cell: Arc::clone(self),
        })
    }

    /// Opens a selection attempt, superseding any older one.
    pub(in crate::uploads) fn begin_selection(self: &Arc<Self>) -> Selection {
        let attempt = {
            let mut selection = lock(&self.selection);

            *selection += 1;

            *selection
        };

        Selection {
            cell: Arc::clone(self),
            attempt,
            armed: true,
        }
    }

    /// Delivers one snapshot to every live subscriber.
    fn publish(&self, snapshot: UploadHandle) {
        lock(&self.updates).retain(|sender| sender.unbounded_send(snapshot.clone()).is_ok());
    }

    /// Subscribes to this cell, or hands back an already-ended stream once it
    /// has been closed.
    ///
    /// The read and the registration are one step under `closed`, so a teardown
    /// cannot land between them and leave a sender nothing will ever write to.
    fn subscribe(&self) -> impl Stream<Item = UploadHandle> + Send + 'static {
        let (sender, receiver) = mpsc::unbounded();
        let closed = lock(&self.closed);

        if !*closed {
            lock(&self.updates).push(sender);
        }

        drop(closed);

        receiver
    }

    /// Retires the cell: no more subscriptions, no more transport state.
    ///
    /// Dropping the senders with the cell is not enough: the registry is not
    /// the cell's only owner, and every live [`Upload`] holds an `Arc` of it.
    /// A handle the embedder is still holding would otherwise keep the senders
    /// — and its subscriber's receiver — alive forever, so removal from the
    /// registry has to end them explicitly, exactly as `RootSink::clear` does
    /// for a root's own subscriptions. Closure is **recorded** rather than the
    /// senders merely dropped, so a subscription taken afterwards is an ended
    /// stream instead of one waiting on a publish that can never come.
    fn close(&self) {
        let mut closed = lock(&self.closed);

        *closed = true;

        lock(&self.updates).clear();
        drop(closed);

        // After the flag, so nothing this aborts can be re-inserted behind it.
        self.drop_transport(None);
    }
}

/// The claim one running [`Upload::start`](super::Upload::start) holds on its
/// cell.
#[derive(Debug)]
pub(in crate::uploads) struct TransferClaim {
    cell: Arc<UploadCell>,
}

impl Drop for TransferClaim {
    fn drop(&mut self) {
        *lock(&self.cell.transferring) = false;
    }
}

/// One [`Upload::select`](super::Upload::select) attempt, from the transition
/// into [`UploadStatus::Selecting`] to whatever ends it.
///
/// Dropped while still armed, it publishes [`UploadStatus::Error`]: a preflight
/// that failed — or whose future was dropped — must not leave observers of
/// `status` watching a selection that will never resolve. A guard superseded by
/// a newer attempt publishes nothing, so a stale one cannot stomp the status of
/// the selection that replaced it.
#[derive(Debug)]
pub(in crate::uploads) struct Selection {
    cell: Arc<UploadCell>,
    attempt: u64,
    armed: bool,
}

impl Selection {
    /// Disarms the guard: the caller has written the final status itself.
    pub(in crate::uploads) fn settled(mut self) {
        self.armed = false;
    }
}

impl Drop for Selection {
    fn drop(&mut self) {
        if !self.armed || *lock(&self.cell.selection) != self.attempt {
            return;
        }

        self.cell
            .update(|handle| handle.status = UploadStatus::Error);
    }
}

/// Every upload handle of one mounted root, keyed by `(store_id, name)`.
///
/// Shared between the [`PatchEngine`](crate::PatchEngine) that folds the ops
/// and the [`Mounted`](crate::Mounted) that hands out [`Upload`]s, so both see
/// one set of handles.
#[derive(Debug, Default)]
pub struct Uploads {
    handles: Mutex<Handles>,
    /// How the control plane reaches the server. `None` for a registry with no
    /// connection behind it — a bare [`PatchEngine`](crate::PatchEngine) — in
    /// which case `select`/`start`/`cancel`/`reset` report
    /// [`MusubiError::NotConnected`](crate::MusubiError).
    control: Option<Arc<UploadControl>>,
}

/// The cells, plus what the registry remembers about the ones it no longer has.
///
/// A per-cell tombstone cannot answer for a cell the registry has *removed*:
/// `prune` drops keys and `clear` empties the map, and the very next
/// [`Uploads::handle`] would then mint a fresh, open cell for a store that is
/// gone — a stream nothing can ever publish to and nothing ever ends. Closure
/// is therefore recorded here, one flag for teardown and one set for the stores
/// pruning dropped, and read under the same lock the insert takes.
#[derive(Debug, Default)]
struct Handles {
    cells: HashMap<UploadKey, Arc<UploadCell>>,
    /// Set by [`Uploads::clear`]; terminal for the whole registry.
    closed: bool,
    /// The stores [`Uploads::prune`] dropped cells for, still absent from the
    /// index. Only stores that *had* a cell are recorded, so this is bounded by
    /// the uploads a root actually used rather than by every store it ever had.
    pruned: HashSet<StoreId>,
}

impl Handles {
    /// Whether a handle minted for this store now would be a dead one.
    fn is_closed(&self, store_id: &StoreId) -> bool {
        self.closed || self.pruned.contains(store_id)
    }
}

impl Uploads {
    /// The registry a mounted root gets: handles wired to the connection, so
    /// the ones it hands out can also drive transfers.
    pub(crate) fn new(control: Arc<UploadControl>) -> Self {
        Self {
            handles: Mutex::new(Handles::default()),
            control: Some(control),
        }
    }

    /// The handle for `(store_id, name)`, created on first use.
    ///
    /// Taking a handle before the server has said anything about it is normal:
    /// a subscriber can be attached the moment the marker appears on the state,
    /// and the defaults stand in until the `config` op lands.
    ///
    /// A handle for a key the registry has closed is born closed and is **not**
    /// filed: it reads as the defaults and its `updates()` ends immediately, and
    /// leaving it out of the index is what lets a pruned store that comes back
    /// (BDR-0011) get a live cell rather than the dead one minted while it was
    /// away.
    pub fn handle(&self, store_id: &StoreId, name: &str) -> Upload {
        let mut handles = lock(&self.handles);
        let fresh = || Arc::new(UploadCell::new(store_id.clone(), name.to_owned()));

        let cell = if handles.is_closed(store_id) {
            let cell = fresh();

            cell.close();

            cell
        } else {
            let key = UploadKey {
                store_id: store_id.clone(),
                name: name.to_owned(),
            };

            Arc::clone(handles.cells.entry(key).or_insert_with(fresh))
        };

        drop(handles);

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
    /// index, ending its `updates()` streams and aborting its transfers.
    ///
    /// Uploads are not resumable (BDR-0003), and a store that reappears mounts
    /// fresh (BDR-0011), so a vanished store must not leave its handles behind.
    pub(crate) fn prune(&self, live_store_ids: &HashSet<StoreId>) {
        let mut handles = lock(&self.handles);

        // A store that came back is live again, so its tombstone lifts and the
        // next handle taken on it is a live one.
        handles
            .pruned
            .retain(|store_id| !live_store_ids.contains(store_id));

        let gone: Vec<UploadKey> = handles
            .cells
            .keys()
            .filter(|key| !live_store_ids.contains(&key.store_id))
            .cloned()
            .collect();

        for key in gone {
            // Explicit, because an `Upload` the embedder still holds keeps the
            // cell alive past its removal from the index — and a transfer that
            // is already running holds one too.
            if let Some(cell) = handles.cells.remove(&key) {
                cell.close();
            }

            handles.pruned.insert(key.store_id);
        }
    }

    /// Drops every handle, ending its `updates()` streams and aborting its
    /// transfers. Called when the root leaves the registry.
    pub(crate) fn clear(&self) {
        let mut handles = lock(&self.handles);

        handles.closed = true;

        for cell in handles.cells.values() {
            cell.close();
        }

        handles.cells.clear();
        // Subsumed by `closed`, which answers for every key at once.
        handles.pruned.clear();
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
    pub(in crate::uploads) cell: Arc<UploadCell>,
    pub(in crate::uploads) control: Option<Arc<UploadControl>>,
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
    /// ends when the owning store leaves the tree or the root is unmounted. A
    /// subscription taken *after* that is an already-ended stream, never one
    /// waiting on a publish that can no longer come — including one taken
    /// through a handle the embedder kept across the teardown. It does not
    /// replay [`snapshot`](Self::snapshot) — read that first if the current
    /// state matters.
    #[must_use = "the stream is the subscription; dropping it unsubscribes"]
    pub fn updates(&self) -> impl Stream<Item = UploadHandle> + Send + 'static {
        self.cell.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use futures_executor::block_on;
    use futures_util::{FutureExt, StreamExt};
    use serde_json::{Value, json};

    use super::*;
    use crate::uploads::ops::fixtures::{
        add, cancel, complete, decode, entry, error, progress, reset,
    };
    use crate::uploads::ops::{UploadAccept, UploadErrorCode};

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

    // The two tests above drop the `Upload` the moment `updates()` returns, so
    // the cell dies with its registry entry. Holding the handle — what every
    // real consumer does — keeps the cell's `Arc` alive, and the streams must
    // still end.

    #[test]
    fn pruning_a_store_ends_its_update_streams_while_the_handle_is_held() {
        let uploads = Uploads::default();
        let held = uploads.handle(&store_id(&["panel"]), "avatar");
        let mut updates = held.updates();

        uploads.prune(&HashSet::from([StoreId::root()]));

        assert!(matches!(updates.next().now_or_never(), Some(None)));
        assert_eq!(held.snapshot().store_id, store_id(&["panel"]));
    }

    #[test]
    fn clearing_the_registry_ends_every_update_stream_while_the_handle_is_held() {
        let uploads = Uploads::default();
        let held = uploads.handle(&StoreId::root(), "avatar");
        let mut updates = held.updates();

        uploads.clear();

        assert!(matches!(updates.next().now_or_never(), Some(None)));
        assert_eq!(held.snapshot().store_id, StoreId::root());
    }

    // A handle the registry no longer has is the case a per-cell tombstone
    // cannot answer: `prune` removes the key and `clear` empties the map, so
    // the next `handle()` mints a *fresh* cell — open, and subscribed to
    // something no fold can ever reach.

    #[test]
    fn a_handle_taken_after_the_registry_was_cleared_hands_out_an_ended_stream() {
        let uploads = Uploads::default();

        uploads.clear();

        let mut updates = uploads.handle(&StoreId::root(), "avatar").updates();

        assert!(matches!(updates.next().now_or_never(), Some(None)));
    }

    #[test]
    fn a_handle_taken_after_its_store_was_pruned_hands_out_an_ended_stream() {
        let uploads = Uploads::default();
        let panel = store_id(&["panel"]);

        uploads.handle(&panel, "avatar");
        uploads.prune(&HashSet::from([StoreId::root()]));

        let mut updates = uploads.handle(&panel, "avatar").updates();

        assert!(matches!(updates.next().now_or_never(), Some(None)));
        // Another store's handle is untouched by its neighbour's tombstone.
        assert!(
            uploads
                .handle(&StoreId::root(), "avatar")
                .updates()
                .next()
                .now_or_never()
                .is_none()
        );
    }

    /// Guards the tombstone from over-reaching rather than a bug: a store that
    /// leaves the tree and comes back mounts fresh (BDR-0011), and its new
    /// handle has to be a live one. Passes before the closure fix too — there
    /// was no tombstone to over-reach with.
    #[test]
    fn a_store_that_comes_back_gets_a_live_handle_again() {
        let uploads = Uploads::default();
        let panel = store_id(&["panel"]);

        uploads.handle(&panel, "avatar");
        uploads.prune(&HashSet::from([StoreId::root()]));
        uploads.prune(&HashSet::from([StoreId::root(), panel.clone()]));

        let mut updates = uploads.handle(&panel, "avatar").updates();

        uploads.apply_ops(&decode(vec![json!({
            "op": "add", "upload": "avatar", "store_id": ["panel"], "ref": "u_2",
            "entry": entry("u_2", "pending", 0)
        })]));

        assert!(matches!(
            block_on(updates.next()),
            Some(handle) if handle.entry("u_2").is_some()
        ));
    }

    // ---- wire ------------------------------------------------------------

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

    // ---- fixtures --------------------------------------------------------

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
