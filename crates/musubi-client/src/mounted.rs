//! The mounted-root handle and the shared cell the actor publishes into
//! (`docs/rust-client.md` §2.4, §6.2, §6.3, §7).
//!
//! The actor is not generic over [`Store`], so everything typed lives in a
//! [`RootCell`] the mount call creates: the actor keeps it as a
//! `dyn RootSink` and publishes into it, while [`Mounted`] keeps the same
//! allocation typed and reads out of it. That is what makes `snapshot()` a
//! lock-and-clone instead of an actor round trip, and it keeps every send on
//! the actor task as §2.4 requires.

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

use crate::actor::{ActorMsg, CommandRequest, ConnectionInner};
use crate::error::{MusubiError, Result};
use crate::generated::{Command, Event, Store, StoreId};
use crate::lock;
use crate::transfer::UploadControl;
use crate::uploads::{Upload, Uploads};

/// Push-event subscribers, keyed the way BDR-0032 dispatches: `(store_id, name)`.
type EventRegistry = HashMap<(StoreId, String), Vec<UnboundedSender<Value>>>;

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
    /// Deserializes the hydrated tree and publishes it to the snapshot cell and
    /// every live `updates()` subscriber.
    fn publish(&self, state: &Value) -> std::result::Result<(), serde_json::Error>;

    /// Delivers one push event to every live `events()` subscriber of
    /// `(store_id, name)`. An event with no subscriber is dropped silently.
    fn dispatch_event(&self, store_id: &StoreId, name: &str, payload: &Value);

    /// Publishes a [`MountStatus`] transition to the status cell and every
    /// live `status_updates()` subscriber (BDR-0033). Repeats are dropped, and
    /// a root that has never been [`MountStatus::Live`] refuses
    /// [`MountStatus::Reconnecting`] — socket churn before the first accepted
    /// initial patch is still `Connecting`.
    fn set_status(&self, status: MountStatus);

    /// The root's upload registry, which the actor hands to its
    /// [`PatchEngine`](crate::PatchEngine) so the folded `upload_ops` land in
    /// the same handles [`Mounted::upload`] reads.
    fn uploads(&self) -> Arc<Uploads>;

    /// Drops the snapshot and every subscription, ending their streams. Called
    /// once, when the root leaves the registry.
    fn clear(&self);
}

/// One mounted root's typed cell: the snapshot plus its subscription senders.
pub(crate) struct RootCell<St: Store> {
    snapshot: Mutex<Option<Arc<St::State>>>,
    updates: Mutex<Vec<UnboundedSender<Arc<St::State>>>>,
    events: Mutex<EventRegistry>,
    status: Mutex<MountStatus>,
    status_watchers: Mutex<Vec<UnboundedSender<MountStatus>>>,
    // Not behind the outer `Mutex`es: the registry has its own interior
    // locking, because the actor folds ops into it while the embedder reads
    // handles out of it.
    uploads: Arc<Uploads>,
}

impl<St: Store> RootCell<St> {
    /// An empty cell: no snapshot yet, no subscribers, no upload handles.
    ///
    /// `control` is how the upload handles this cell hands out reach the
    /// server; it is built by the mount call, which is the only place that
    /// knows the root id.
    pub(crate) fn new(control: Arc<UploadControl>) -> Self {
        Self {
            snapshot: Mutex::new(None),
            updates: Mutex::new(Vec::new()),
            events: Mutex::new(EventRegistry::new()),
            status: Mutex::new(MountStatus::Connecting),
            status_watchers: Mutex::new(Vec::new()),
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

        *lock(&self.snapshot) = Some(Arc::clone(&next));
        lock(&self.updates).retain(|sender| sender.unbounded_send(Arc::clone(&next)).is_ok());

        Ok(())
    }

    fn dispatch_event(&self, store_id: &StoreId, name: &str, payload: &Value) {
        let mut events = lock(&self.events);
        let key = (store_id.clone(), name.to_owned());

        let Some(senders) = events.get_mut(&key) else {
            return;
        };

        senders.retain(|sender| sender.unbounded_send(payload.clone()).is_ok());

        if senders.is_empty() {
            events.remove(&key);
        }
    }

    fn set_status(&self, status: MountStatus) {
        {
            let mut current = lock(&self.status);

            // A root that has never been live cannot be reconnecting: socket
            // churn before the first accepted initial patch is part of
            // `Connecting` (BDR-0033).
            if status == MountStatus::Reconnecting && *current == MountStatus::Connecting {
                return;
            }

            if *current == status {
                return;
            }

            *current = status;
        }

        lock(&self.status_watchers).retain(|watcher| watcher.unbounded_send(status).is_ok());
    }

    fn uploads(&self) -> Arc<Uploads> {
        Arc::clone(&self.uploads)
    }

    fn clear(&self) {
        *lock(&self.snapshot) = None;
        lock(&self.updates).clear();
        lock(&self.events).clear();
        // Back to the pre-initial baseline, coherent with the cleared
        // snapshot; the ended streams are the terminal signal (BDR-0033).
        *lock(&self.status) = MountStatus::Connecting;
        lock(&self.status_watchers).clear();
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
    /// ```text
    /// let Some(state) = cart.snapshot() else { return };
    ///
    /// render(&state.title);
    /// ```
    pub fn snapshot(&self) -> Option<Arc<St::State>> {
        lock(&self.cell.snapshot).clone()
    }

    /// One item per accepted patch envelope, oldest first.
    ///
    /// The stream **is** the subscription: dropping it unsubscribes, and it
    /// ends when the root is unmounted or the connection is disconnected. It
    /// does not replay [`snapshot`](Self::snapshot) — read that first if the
    /// current state matters.
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
        let (sender, receiver) = mpsc::unbounded();

        lock(&self.cell.updates).push(sender);

        receiver
    }

    /// Where this root is in its connection lifecycle (BDR-0033).
    ///
    /// [`MountStatus::Connecting`] until the first accepted initial patch,
    /// [`MountStatus::Live`] after, [`MountStatus::Reconnecting`] from a
    /// socket drop / heartbeat timeout / version-gap recovery until the
    /// rejoin's fresh initial patch lands. Terminal outcomes stay on the
    /// mount error path; there is no error arm here.
    ///
    /// ```text
    /// if cart.status() == MountStatus::Reconnecting {
    ///     render_stale_badge();
    /// }
    /// ```
    pub fn status(&self) -> MountStatus {
        *lock(&self.cell.status)
    }

    /// One item per [`MountStatus`] transition, oldest first (BDR-0033).
    ///
    /// The stream **is** the subscription: dropping it unsubscribes, and it
    /// ends when the root is unmounted or the connection is disconnected. It
    /// does not replay [`status`](Self::status) — read that first if the
    /// current value matters.
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
        let (sender, receiver) = mpsc::unbounded();

        lock(&self.cell.status_watchers).push(sender);

        receiver
    }

    /// Dispatches a command on the root store.
    ///
    /// # Ordering
    ///
    /// The reply resolves **before** the patch it caused is applied (BDR-0009:
    /// reply, then the `"patch"` push, then server-side effects). A resolved
    /// reply therefore says nothing about [`snapshot`](Self::snapshot); apps
    /// that need "state settled" watch [`updates`](Self::updates) for the
    /// condition they care about.
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
    /// The stream is the subscription: dropping it unregisters. Events with no
    /// live stream are dropped, and a payload that fails to deserialize is
    /// logged and skipped — an event is not state, so it never fails a cycle.
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

        lock(&self.cell.events)
            .entry((store_id.clone(), E::NAME.to_owned()))
            .or_default()
            .push(sender);

        // `ready` rather than an `async` block: the returned stream stays
        // `Unpin`, so a consumer can poll it without pinning it first.
        receiver.filter_map(|payload| {
            ready(match serde_json::from_value::<E>(payload) {
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

    /// Builds the handle. Called by the mount path once the actor has the root
    /// registered, so the refcount this handle owns is already counted.
    pub(crate) fn new(
        inner: Arc<ConnectionInner>,
        cell: Arc<RootCell<St>>,
        root_id: Arc<str>,
    ) -> Self {
        Self {
            inner,
            cell,
            root_id,
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

        Self::new(
            Arc::clone(&self.inner),
            Arc::clone(&self.cell),
            Arc::clone(&self.root_id),
        )
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
