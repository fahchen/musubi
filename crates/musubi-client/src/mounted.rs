//! The mounted-root handle and the shared cell the actor publishes into
//! (`docs/rust-client.md` §2.4, §6.2, §6.3, §7).
//!
//! The actor is not generic over [`Store`], so everything typed lives in a
//! [`RootCell`] the mount call creates: the actor keeps it as a
//! `dyn RootSink` and publishes into it, while [`Mounted`] keeps the same
//! allocation typed and reads out of it. That is what makes `snapshot()` a
//! lock-and-clone instead of an actor round trip, and it keeps every send on
//! the actor task as §2.4 requires.
//!
//! State and status are [`Latest`] cells — one value, not a queue — because
//! each of their items subsumes the one before it (§2.4). Push events are the
//! opposite kind of thing, so they stay one unbounded queue per subscription.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures_channel::mpsc::{self, UnboundedSender};
use futures_channel::oneshot;
use futures_core::Stream;
use futures_util::StreamExt;
use futures_util::future::ready;
use serde::Serialize;
use serde::de::{Deserialize, DeserializeOwned};
use serde_json::Value;

use crate::actor::{ActorMsg, CommandRequest, ConnectionInner, RootHold};
use crate::error::{MusubiError, Result};
use crate::generated::{Command, Event, Store, StoreId};
use crate::latest::Latest;
use crate::lock;
use crate::uploads::{Upload, UploadControl, Uploads};

/// Push-event subscribers, keyed the way BDR-0032 dispatches: `(store_id, name)`.
///
/// The payload is shared rather than copied: one event reaches every subscriber
/// of its key, and a whole `serde_json::Value` per subscriber is a deep clone
/// of a tree each of them only reads.
#[derive(Default)]
struct EventRegistry {
    senders: HashMap<(StoreId, String), Vec<UnboundedSender<Arc<Value>>>>,
    /// Set by [`RootSink::clear`], and the reason this is a struct rather than
    /// the bare map: teardown empties the map, so a per-key tombstone would say
    /// nothing about the keys the registry no longer has — and a stale handle
    /// subscribing to one of those would get a stream that never yields and
    /// never ends. Closure belongs to the whole registry, exactly as
    /// [`Latest::close`] holds it for state and status.
    closed: bool,
}

/// Where a mounted root is in its connection lifecycle (BDR-0033).
///
/// A client-local projection of the socket layer's liveness signal — no wire
/// message carries it, and the server is not involved. Terminal outcomes (a
/// rejected join, unmount, [`Connection::disconnect`](crate::Connection::disconnect))
/// stay on the mount error path and end the subscription streams; the status
/// deliberately has no error arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MountStatus {
    /// Mounted, but the first **accepted** initial patch has not landed yet.
    /// A cache seed renders data without leaving this state — seeded state is
    /// last-known, not live.
    Connecting,
    /// The initial patch landed; [`Mounted::snapshot`] tracks the server.
    Live,
    /// Liveness was lost after the root had been live — socket drop, heartbeat
    /// timeout, or version-gap recovery — and the reconnect machinery is
    /// working its way back. Ends when the rejoin's fresh initial patch lands.
    ///
    /// The last-good tree **keeps rendering** through this state (BDR-0015):
    /// [`Mounted::snapshot`] stays `Some`, so the status is how an embedder
    /// annotates stale rendering, never a cue to blank it.
    Reconnecting,
}

/// What the actor needs from a mounted root without knowing its [`Store`] type.
pub(crate) trait RootSink: Send + Sync + 'static {
    /// Deserializes the hydrated tree and publishes it into the state cell,
    /// which is what `snapshot()` reads and every live `updates()` subscriber
    /// converges on.
    fn publish(&self, state: &Value) -> std::result::Result<(), serde_json::Error>;

    /// Delivers one push event to every live `events()` subscriber of
    /// `(store_id, name)`. An event with no subscriber is dropped silently.
    fn dispatch_event(&self, store_id: &StoreId, name: &str, payload: &Value);

    /// Publishes a [`MountStatus`] transition into the status cell (BDR-0033).
    /// Repeats are dropped, and a root that has never been
    /// [`MountStatus::Live`] refuses [`MountStatus::Reconnecting`] — socket
    /// churn before the first accepted initial patch is still `Connecting`.
    fn set_status(&self, status: MountStatus);

    /// The root's upload registry, which the actor hands to its
    /// [`PatchEngine`](crate::PatchEngine) so the folded `upload_ops` land in
    /// the same handles [`Mounted::upload`] reads.
    fn uploads(&self) -> Arc<Uploads>;

    /// Ends every subscription and puts the readable surface back to its
    /// pre-initial baseline. Called once, when the root leaves the registry.
    fn clear(&self);
}

/// One mounted root's typed cell: the state and status cells, plus the
/// subscription senders of everything that is not a latest value.
pub(crate) struct RootCell<St: Store> {
    state: Latest<Arc<St::State>>,
    events: Mutex<EventRegistry>,
    status: Latest<MountStatus>,
    // Not behind a `Mutex` here: the registry has its own interior locking,
    // because the actor folds ops into it while the embedder reads handles out
    // of it.
    uploads: Arc<Uploads>,
}

impl<St: Store> RootCell<St> {
    /// An empty cell: no state yet, no subscribers, no upload handles.
    ///
    /// `control` is how the upload handles this cell hands out reach the
    /// server; it is built by the mount call, which is the only place that
    /// knows the root id.
    pub(crate) fn new(control: Arc<UploadControl>) -> Self {
        Self {
            state: Latest::new(None),
            events: Mutex::new(EventRegistry::default()),
            // Seeded where the state cell is empty: the pre-initial baseline is
            // a real status, and a subscriber replays it (BDR-0033).
            status: Latest::new(Some(MountStatus::Connecting)),
            uploads: Arc::new(Uploads::new(control)),
        }
    }
}

impl<St: Store> RootSink for RootCell<St> {
    fn publish(&self, state: &Value) -> std::result::Result<(), serde_json::Error> {
        // Deserialize from the borrowed tree: `&Value` is a `Deserializer`, so
        // the owned copy `from_value` would need is a third full clone of the
        // hydrated tree on the one per-envelope hot path (§4.2).
        let next = Arc::new(St::State::deserialize(state)?);

        self.state.set(next);

        Ok(())
    }

    fn dispatch_event(&self, store_id: &StoreId, name: &str, payload: &Value) {
        let mut events = lock(&self.events);
        let key = (store_id.clone(), name.to_owned());

        let Some(senders) = events.senders.get_mut(&key) else {
            return;
        };

        // One clone of the payload tree for the whole fan-out; each subscriber
        // costs a refcount bump, and deserializing reads the shared value.
        let payload = Arc::new(payload.clone());

        senders.retain(|sender| sender.unbounded_send(Arc::clone(&payload)).is_ok());

        // Dropping a stream unregisters it, but only a dispatch on its key
        // notices; an emptied key is removed rather than kept as a tombstone,
        // so a later subscription to it is a live one.
        if senders.is_empty() {
            events.senders.remove(&key);
        }
    }

    fn set_status(&self, status: MountStatus) {
        // Edges only, decided under the cell's lock so the dedupe and the write
        // are one step rather than a read the next writer can slip between.
        self.status.set_with(|current| match current {
            // A root that has never been live cannot be reconnecting: socket
            // churn before the first accepted initial patch is part of
            // `Connecting` (BDR-0033).
            Some(MountStatus::Connecting) if status == MountStatus::Reconnecting => None,
            Some(current) if *current == status => None,
            _ => Some(status),
        });
    }

    fn uploads(&self) -> Arc<Uploads> {
        Arc::clone(&self.uploads)
    }

    fn clear(&self) {
        // Closing a cell is the terminal signal: a subscriber still takes the
        // last value it has not seen and *then* ends, while `snapshot()` and
        // `status()` fall back to their pre-initial baseline (BDR-0033).
        self.state.close();

        {
            // Terminal for the event registry too, and recorded rather than
            // merely emptied: nothing rejoins after a teardown, so a handle
            // still held across one must get an ended stream instead of a
            // subscription no dispatch can ever reach.
            let mut events = lock(&self.events);

            events.senders.clear();
            events.closed = true;
        }

        self.status.close();
        self.uploads.clear();
    }
}

/// A handle on one mounted root.
///
/// Cheap to clone; every clone addresses the same channel and holds one count
/// of the root's refcount. **Unmount is [`Drop`]**: the last clone to go away
/// leaves the channel, which stops the server-side root via `terminate/2`.
///
/// ```text
/// let cart: Mounted<CartStore> = connection.mount("cart:page", Params {}).await?;
///
/// let mut updates = cart.updates();
/// let reply = cart.command(Checkout { coupon: None }).await?;
///
/// while let Some(state) = updates.next().await {
///     render(&state.title);
/// }
/// ```
pub struct Mounted<St: Store> {
    inner: Arc<ConnectionInner>,
    cell: Arc<RootCell<St>>,
    root_id: Arc<str>,
}

impl<St: Store> Mounted<St> {
    /// The last published state, or `None` before the initial patch lands.
    ///
    /// A reconnect does **not** clear it: the last-good rendering is kept while
    /// the channel rejoins and is swapped atomically when the fresh initial
    /// patch arrives (`docs/rust-client.md` §9).
    ///
    /// [`Connection::disconnect`](crate::Connection::disconnect) does clear it,
    /// and nothing rejoins afterwards: a handle still held across a disconnect
    /// reads `None` **forever**, which is indistinguishable here from a root
    /// whose initial patch has not landed yet. There is no terminal variant to
    /// read; the ended [`updates`](Self::updates) stream is the terminal
    /// signal.
    ///
    /// ```text
    /// let Some(state) = cart.snapshot() else { return };
    ///
    /// render(&state.title);
    /// ```
    pub fn snapshot(&self) -> Option<Arc<St::State>> {
        self.cell.state.get()
    }

    /// The latest state, and every later one this consumer keeps up with.
    ///
    /// **Latest-value, not a queue.** Each item is a whole root that subsumes
    /// the one before it, so the stream carries no backlog: a consumer that
    /// falls behind gets the newest state on its next poll and *never* the
    /// intermediates it missed. Nothing is lost by that — no client-side fold
    /// reads them — and a consumer that stalls cannot grow the client's
    /// memory. An app that needs every envelope needs
    /// [`events`](Self::events), which is a queue of discrete items.
    ///
    /// The first poll **replays** [`snapshot`](Self::snapshot) when there is
    /// one, so subscribing is enough to be current; reading `snapshot()` too is
    /// only for a synchronous first paint, not a race to close.
    ///
    /// The stream **is** the subscription: dropping it unsubscribes, and it
    /// ends when the root is unmounted or the connection is disconnected —
    /// after delivering a last value it had not yet seen.
    ///
    /// ```text
    /// let mut updates = cart.updates();
    ///
    /// while let Some(state) = updates.next().await {
    ///     render(&state.title);
    /// }
    /// ```
    #[must_use = "the stream is the subscription; dropping it unsubscribes"]
    pub fn updates(&self) -> impl Stream<Item = Arc<St::State>> + Send + 'static {
        self.cell.state.subscribe()
    }

    /// Where this root is in its connection lifecycle (BDR-0033).
    ///
    /// [`MountStatus::Connecting`] until the first accepted initial patch,
    /// [`MountStatus::Live`] after, [`MountStatus::Reconnecting`] from a
    /// socket drop / heartbeat timeout / version-gap recovery until the
    /// rejoin's fresh initial patch lands. Terminal outcomes stay on the
    /// mount error path; there is no error arm here.
    ///
    /// That has a consequence after
    /// [`Connection::disconnect`](crate::Connection::disconnect): teardown puts
    /// the cell back to the pre-initial baseline, so a handle still held across
    /// a disconnect reports [`MountStatus::Connecting`] **forever** — a root
    /// that will never connect, reading exactly like one that has not connected
    /// yet. As with [`snapshot`](Self::snapshot), the ended
    /// [`status_updates`](Self::status_updates) stream is the terminal signal.
    ///
    /// ```text
    /// if cart.status() == MountStatus::Reconnecting {
    ///     render_stale_badge();
    /// }
    /// ```
    pub fn status(&self) -> MountStatus {
        // A closed cell reports no value, which is exactly the pre-initial
        // baseline a torn-down root reads as.
        self.cell.status.get().unwrap_or(MountStatus::Connecting)
    }

    /// The current [`MountStatus`], and every later one this consumer keeps up
    /// with (BDR-0033).
    ///
    /// **Latest-value, not a queue**, like [`updates`](Self::updates): the
    /// first poll replays [`status`](Self::status), so a subscriber is current
    /// without reading it first, and a consumer that was not polling across a
    /// transition sees where the root ended up rather than a replay of a
    /// window that has already closed. Writes are edges only, so a status that
    /// did not change is not an item.
    ///
    /// The stream **is** the subscription: dropping it unsubscribes, and it
    /// ends when the root is unmounted or the connection is disconnected.
    ///
    /// ```text
    /// let mut statuses = cart.status_updates();
    ///
    /// while let Some(status) = statuses.next().await {
    ///     pill.set(status);
    /// }
    /// ```
    #[must_use = "the stream is the subscription; dropping it unsubscribes"]
    pub fn status_updates(&self) -> impl Stream<Item = MountStatus> + Send + 'static {
        self.cell.status.subscribe()
    }

    /// Dispatches a command on the root store.
    ///
    /// # Ordering
    ///
    /// The reply is **not gated** on the patch it caused, and carries no
    /// ordering relationship to it. BDR-0009 puts the reply on the wire first
    /// — reply, then the `"patch"` push, then server-side effects — but that
    /// is the *server's* frame order, not a client guarantee: the reply and the
    /// patch reach this client's actor through two independently woken tasks,
    /// so on a multi-threaded executor either can be handled first
    /// (`docs/rust-client.md` §2.4). A resolved reply therefore says nothing
    /// about [`snapshot`](Self::snapshot); apps that need "state settled" watch
    /// [`updates`](Self::updates) for the condition they care about.
    ///
    /// ```text
    /// let reply = cart.command(Checkout { coupon: None }).await?;
    /// ```
    pub async fn command<C: Command<St>>(&self, cmd: C) -> Result<C::Reply> {
        self.dispatch(StoreId::root(), <C as Command<St>>::NAME, cmd)
            .await
    }

    /// Dispatches a command on a child store, addressed by its server-authored
    /// [`StoreId`].
    ///
    /// Store ids are never constructed by hand: they arrive on the snapshot as
    /// `StoreField::store_id`. `T` is inferred from `cmd`'s [`Command`] impl.
    ///
    /// ```text
    /// let panel = &cart.snapshot().unwrap().checkout_panel;
    ///
    /// cart.command_on(&panel.store_id, Pay { amount: 12 }).await?;
    /// ```
    pub async fn command_on<C, T>(&self, target: &StoreId, cmd: C) -> Result<C::Reply>
    where
        T: Store,
        C: Command<T>,
    {
        self.dispatch(target.clone(), <C as Command<T>>::NAME, cmd)
            .await
    }

    /// Push events (BDR-0032) of one store, as a typed stream.
    ///
    /// A **queue**, unlike [`updates`](Self::updates): events are discrete
    /// occurrences, none of which stands in for another, so a slow consumer
    /// gets all of them (and pays for the backlog) rather than the latest one.
    ///
    /// The stream is the subscription: dropping it unregisters. Events with no
    /// live stream are dropped, and a payload that fails to deserialize is
    /// logged and skipped — an event is not state, so it never fails a cycle.
    ///
    /// Like [`updates`](Self::updates), it ends when the root is unmounted or
    /// the connection is disconnected — and a subscription taken *after* that
    /// is an already-ended stream, never one waiting on events that can no
    /// longer arrive.
    ///
    /// ```text
    /// let mut toasts = cart.events::<ToastPayload, _>(&StoreId::root());
    ///
    /// while let Some(toast) = toasts.next().await {
    ///     show(&toast.message);
    /// }
    /// ```
    #[must_use = "the stream is the subscription; dropping it unregisters"]
    pub fn events<E, T>(&self, store_id: &StoreId) -> impl Stream<Item = E> + Send + 'static
    where
        T: Store,
        E: Event<T>,
    {
        let (sender, receiver) = mpsc::unbounded();
        let mut events = lock(&self.cell.events);

        // Read under the lock the insert takes, so a teardown cannot land
        // between the two.
        if events.closed {
            // Nothing can ever write to this receiver again, so the sender is
            // dropped instead of registered and the stream below is an ended
            // one — the same answer `Latest::subscribe` gives after teardown.
            drop(sender);
        } else {
            events
                .senders
                .entry((store_id.clone(), E::NAME.to_owned()))
                .or_default()
                .push(sender);
        }

        drop(events);

        // `ready` rather than an `async` block: the returned stream stays
        // `Unpin`, so a consumer can poll it without pinning it first.
        receiver.filter_map(|payload| {
            // Deserialized from the shared payload by reference — an owned copy
            // would undo the point of sharing it across subscribers.
            ready(match E::deserialize(payload.as_ref()) {
                Ok(event) => Some(event),
                Err(error) => {
                    tracing::warn!(
                        event = E::NAME,
                        %error,
                        "dropping a push event whose payload did not match the generated type"
                    );
                    None
                }
            })
        })
    }

    /// One upload of one store, as a handle over its live state.
    ///
    /// Uploads are singletons per store (BDR-0028), so `(store_id, name)`
    /// addresses exactly one handle; `name` is the declared upload name, which
    /// arrives on the snapshot as [`UploadSlot::name`](crate::generated::UploadSlot).
    /// A handle exists from the first call — before any op has landed it reads
    /// as idle with the framework defaults — and the same key always resolves
    /// to the same handle, so it can be taken as soon as the marker appears.
    ///
    /// The handle carries the server-driven state *and* the control plane:
    /// [`select`](Upload::select), [`start`](Upload::start),
    /// [`cancel`](Upload::cancel) and [`reset`](Upload::reset) are on it
    /// (`docs/rust-client.md` §10.2).
    ///
    /// ```text
    /// let avatar = cart.upload(&StoreId::root(), &cart.snapshot()?.avatar.name);
    /// let mut updates = avatar.updates();
    ///
    /// while let Some(handle) = updates.next().await {
    ///     render(handle.progress());
    /// }
    /// ```
    pub fn upload(&self, store_id: &StoreId, name: &str) -> Upload {
        self.cell.uploads.handle(store_id, name)
    }

    /// Builds the handle around the hold the mount reply carried, which the
    /// actor has already counted.
    ///
    /// The guard is **disarmed** rather than kept as a field: releasing stays
    /// this type's own [`Drop`], which runs before its fields do and therefore
    /// enqueues its `Release` ahead of the `Shutdown` the last handle posts. A
    /// hold is released by exactly one of the two, never both.
    pub(crate) fn new(
        inner: Arc<ConnectionInner>,
        cell: Arc<RootCell<St>>,
        mut hold: RootHold,
    ) -> Self {
        Self {
            inner,
            cell,
            root_id: hold.disarm(),
        }
    }

    /// Serializes the payload, hands it to the actor, and types the reply.
    ///
    /// The reply is deserialized here rather than in the actor because the
    /// actor is not generic over the command. The name is passed in rather
    /// than read off a `Command<S>` bound so that `S` stays out of this
    /// signature — it is exactly the parameter `command_on` leaves to
    /// inference.
    async fn dispatch<C, R>(&self, store_id: StoreId, name: &'static str, cmd: C) -> Result<R>
    where
        C: Serialize,
        R: DeserializeOwned,
    {
        let payload = serde_json::to_value(cmd)
            .map_err(|_| MusubiError::Protocol("command payload must be serializable"))?;
        let (reply_tx, reply_rx) = oneshot::channel();

        self.inner.send(ActorMsg::Command(Box::new(CommandRequest {
            root_id: Arc::clone(&self.root_id),
            store_id: store_id.clone(),
            name,
            payload,
            reply: reply_tx,
        })))?;

        let reply = reply_rx.await.map_err(|_| MusubiError::Disconnected)??;

        serde_json::from_value(reply).map_err(|source| MusubiError::Decode { store_id, source })
    }
}

impl<St: Store> Clone for Mounted<St> {
    fn clone(&self) -> Self {
        // A clone is a second hold on the root; the refcount only reaches zero
        // once every clone has been dropped.
        let _ = self.inner.send(ActorMsg::Retain {
            root_id: Arc::clone(&self.root_id),
        });

        // Built directly rather than through `new`: the hold this clone owns is
        // the one `Retain` just counted, not a guard handed over by the actor.
        Self {
            inner: Arc::clone(&self.inner),
            cell: Arc::clone(&self.cell),
            root_id: Arc::clone(&self.root_id),
        }
    }
}

impl<St: Store> Drop for Mounted<St> {
    fn drop(&mut self) {
        // Unbounded, so this is safe from a synchronous `Drop`. A failed send
        // means the actor is already gone, which tore the root down anyway.
        let _ = self.inner.send(ActorMsg::Release {
            root_id: Arc::clone(&self.root_id),
        });
    }
}
