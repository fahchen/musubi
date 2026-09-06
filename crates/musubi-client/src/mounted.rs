//! The mounted-root handle and the shared cell the actor publishes into
//! (`docs/rust-client.md` §2.4, §6.2, §6.3, §7;
//! `docs/rust-reactive-state.md` §5.3, §5.4).
//!
//! The actor is not generic over [`Store`], so everything typed lives in a
//! [`RootCell`] the mount call creates: the actor keeps it as a
//! `dyn RootSink` and drives it, while [`Mounted`] keeps the same allocation
//! typed and reads out of it. Every send stays on the actor task, as §2.4
//! requires.
//!
//! # Three properties, one shape
//!
//! [`Mounted`] has exactly three property accessors, and each hands back a
//! **handle** carrying `.value()` and `.subscribe(..)`:
//!
//! ```text
//! mounted.state()          -> State<St::State>   the retained tree's root
//! mounted.status()         -> StatusState        the liveness cell (BDR-0033)
//! mounted.upload_at(&slot) -> Option<Upload>     the upload plane, keyed by the node
//! ```
//!
//! There is no second way to read any of them (§5.3). The state one is not a
//! cell at all — it is a view on the [`StateTree`] the actor applies envelopes
//! to, so a read
//! costs the subtree it reads and a subscription wakes only when *that* node's
//! semantic value changes.
//!
//! Push events are the one thing that does not take this shape, and deliberately
//! (§6.2): an event is a discrete occurrence, so it has no current value to
//! read and cannot coalesce. It stays one unbounded queue per subscription.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use futures_channel::mpsc::{self, UnboundedSender};
use futures_channel::oneshot;
use futures_core::Stream;
use futures_util::StreamExt;
use futures_util::future::ready;
use musubi_state::{StateTree, UploadSlotState};
use serde::Serialize;
use serde::de::{Deserialize, DeserializeOwned};
use serde_json::Value;

use crate::actor::{ActorMsg, CommandRequest, ConnectionInner, RootHold};
use crate::error::{MusubiError, Result};
use crate::generated::{Command, Event, State, Store, StoreId};
use crate::latest::{Latest, StatusState};
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
    /// The initial patch landed; [`Mounted::state`] tracks the server.
    Live,
    /// Liveness was lost after the root had been live — socket drop, heartbeat
    /// timeout, or version-gap recovery — and the reconnect machinery is
    /// working its way back. Ends when the rejoin's fresh initial patch lands.
    ///
    /// The last-good tree **keeps rendering** through this state (BDR-0015):
    /// nothing on [`Mounted::state`] is cleared, and the rejoin's initial patch
    /// is *reconciled* into the same tree, so an unchanged subtree keeps its
    /// identity and notifies nobody. The status is how an embedder annotates
    /// stale rendering, never a cue to blank it.
    Reconnecting,
}

/// What the actor needs from a mounted root without knowing its [`Store`] type.
pub(crate) trait RootSink: Send + Sync + 'static {
    /// Deserializes a whole hydrated wire root into `St::State` and throws the
    /// result away.
    ///
    /// **Validation only** — the tree is built from the wire value, not from
    /// this. It is the dyn-erasure that keeps the actor non-generic over
    /// [`Store`], and it is where codegen drift becomes a loud failure instead
    /// of a silently partial rendering (§4.4).
    fn validate(&self, hydrated: &Value) -> std::result::Result<(), serde_json::Error>;

    /// The retained tree this root publishes into.
    fn tree(&self) -> &StateTree;

    /// Delivers one push event to every live `events()` subscriber of
    /// `(store_id, name)`. An event with no subscriber is dropped silently.
    fn dispatch_event(&self, store_id: &StoreId, name: &str, payload: &Value);

    /// Publishes a [`MountStatus`] transition into the status cell (BDR-0033).
    /// Repeats are dropped, and a root that has never been
    /// [`MountStatus::Live`] refuses [`MountStatus::Reconnecting`] — socket
    /// churn before the first accepted initial patch is still `Connecting`.
    fn set_status(&self, status: MountStatus);

    /// The root's upload registry, which the actor hands to its patch engine so
    /// the folded `upload_ops` land in the same handles [`Mounted::upload`]
    /// reads.
    fn uploads(&self) -> Arc<Uploads>;

    /// Ends every subscription and puts the readable surface back to its
    /// pre-initial baseline. Called once, when the root leaves the registry.
    fn clear(&self);
}

/// One mounted root's typed cell: the retained tree, the status cell, and the
/// subscription senders of everything that is neither.
///
/// `St` no longer appears in a field — the tree is untyped, and the typing is
/// [`Mounted::state`]'s `State<St::State>` view plus the `St::State`
/// deserialization [`validate`](RootSink::validate) runs. The marker is what
/// keeps that deserialization bound to the right store, and it is
/// `fn() -> St` so the cell is `Send + Sync` regardless of `St`.
pub(crate) struct RootCell<St: Store> {
    tree: StateTree,
    events: Mutex<EventRegistry>,
    /// Behind an `Arc` because [`StatusState`] is a handle over it: the cell
    /// outlives this struct for as long as one handle does.
    status: Arc<Latest<MountStatus>>,
    // Not behind a `Mutex` here: the registry has its own interior locking,
    // because the actor folds ops into it while the embedder reads handles out
    // of it.
    uploads: Arc<Uploads>,
    _marker: PhantomData<fn() -> St>,
}

impl<St: Store> RootCell<St> {
    /// An empty cell: a tree holding a `Null` root at revision `0`, no
    /// subscribers, no upload handles.
    ///
    /// `control` is how the upload handles this cell hands out reach the
    /// server; it is built by the mount call, which is the only place that
    /// knows the root id.
    pub(crate) fn new(control: Arc<UploadControl>) -> Self {
        Self {
            tree: StateTree::new(),
            events: Mutex::new(EventRegistry::default()),
            // The pre-initial baseline is a real status, and a subscriber
            // replays it (BDR-0033).
            status: Arc::new(Latest::new(Some(MountStatus::Connecting))),
            uploads: Arc::new(Uploads::new(control)),
            _marker: PhantomData,
        }
    }
}

impl<St: Store> RootSink for RootCell<St> {
    fn validate(&self, hydrated: &Value) -> std::result::Result<(), serde_json::Error> {
        // Deserialize from the borrowed tree — `&Value` is a `Deserializer`, so
        // this needs no owned copy — and drop the result. It is a shape check,
        // not a read: what an embedder reads is the tree.
        St::State::deserialize(hydrated).map(drop)
    }

    fn tree(&self) -> &StateTree {
        &self.tree
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
        // Closing the tree empties the root and tells every node subscriber the
        // root is gone; dropping the `Notify` is what runs them, and it runs
        // them with the tree lock already released.
        drop(self.tree.close());

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
/// let state = cart.state();
///
/// let _title = state.title().subscribe(|_| redraw());
/// let reply = cart.command(Checkout { coupon: None }).await?;
/// ```
pub struct Mounted<St: Store> {
    inner: Arc<ConnectionInner>,
    cell: Arc<RootCell<St>>,
    root_id: Arc<str>,
}

impl<St: Store> Mounted<St> {
    /// This root's state, as the root view of its retained tree.
    ///
    /// A **handle**, not a value: it costs nothing, cannot fail, and is the
    /// thing every generated field accessor navigates from. Nothing is
    /// materialized until a `.value()` somewhere below it.
    ///
    /// Not an `Option`. The root node always exists, so the lifecycle
    /// questions are answered on the view:
    ///
    /// | question | read |
    /// |---|---|
    /// | nothing has landed yet | `state().revision() == 0` |
    /// | one field | `state().title().value()` |
    /// | the whole root | `state().value()` / `state().try_value()` |
    /// | torn down by `disconnect()` | `!state().is_live()` |
    ///
    /// A reconnect does **not** clear it: the last-good rendering keeps
    /// rendering while the channel rejoins, and the fresh initial patch is
    /// *reconciled* into the same tree rather than replacing it, so a subtree
    /// the server re-sent unchanged keeps its `NodeId`, its subscribers and its
    /// revision (`docs/rust-client.md` §9).
    ///
    /// [`Connection::disconnect`](crate::Connection::disconnect) closes the
    /// tree, and nothing rejoins afterwards: the view stays valid and reads
    /// `is_live() == false` **forever**.
    ///
    /// ```text
    /// let state = cart.state();
    ///
    /// render(&state.title().value());
    /// let _watch = state.title().subscribe(|_| redraw());
    /// ```
    pub fn state(&self) -> State<St::State> {
        self.cell.tree.root::<St::State>()
    }

    /// Where this root is in its connection lifecycle (BDR-0033), as a handle.
    ///
    /// One property, three actions: `status().value()` reads,
    /// `status().subscribe(cb)` observes, and `status().into_stream()` hands the
    /// same subscription back in `await` shape. Every rule BDR-0033 fixes is
    /// unchanged — [`MountStatus::Connecting`] until the first accepted initial
    /// patch, [`MountStatus::Reconnecting`] only after a root has been live,
    /// terminal outcomes on the mount error path with no error arm here, and
    /// [`MountStatus::Connecting`] **forever** for a handle held across a
    /// [`Connection::disconnect`](crate::Connection::disconnect).
    ///
    /// The status is deliberately *not* a node of the tree: no wire message
    /// carries it, so a node for it would have to be excluded from the wire
    /// projection, from the hydrated projection and from drift validation
    /// (`docs/rust-reactive-state.md` §5.4).
    ///
    /// ```text
    /// if cart.status().value() == MountStatus::Reconnecting {
    ///     render_stale_badge();
    /// }
    /// ```
    pub fn status(&self) -> StatusState {
        StatusState::new(Arc::clone(&self.cell.status))
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
    /// about [`state`](Self::state); apps that need "state settled" subscribe to
    /// the node whose condition they care about.
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
    /// Store ids are never constructed by hand: they come off the tree, as
    /// [`StoreState::store_id`](musubi_state::StoreState::store_id). `T` is
    /// inferred from `cmd`'s [`Command`] impl.
    ///
    /// ```text
    /// let panel = cart.state().checkout_panel();
    ///
    /// cart.command_on(&panel.store_id(), Pay { amount: 12 }).await?;
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
    /// A **queue**, unlike every property handle on this type: events are
    /// discrete occurrences, none of which stands in for another, so a slow
    /// consumer gets all of them (and pays for the backlog) rather than the
    /// latest one. That is also why events do not take the
    /// `value()`/`subscribe()` shape — there is no "current event"
    /// (`docs/rust-reactive-state.md` §6.2).
    ///
    /// The stream is the subscription: dropping it unregisters. Events with no
    /// live stream are dropped, and a payload that fails to deserialize is
    /// logged and skipped — an event is not state, so it never fails a cycle.
    ///
    /// It ends when the root is unmounted or the connection is disconnected —
    /// and a subscription taken *after* that is an already-ended stream, never
    /// one waiting on events that can no longer arrive.
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

    /// The live upload handle for a slot on this mount's tree.
    ///
    /// **The way a consumer walks from the state tree to the upload plane.**
    /// Both halves of the `(store_id, name)` key come from the node — the owner
    /// is the nearest enclosing store, resolved once when the node was created —
    /// so there is nothing for the caller to spell, no bare string taken out of
    /// a materialized value, and no hand-written `StoreId::root()` that is
    /// simply wrong for a slot declared inside a child store
    /// (`docs/rust-reactive-state.md` §3.4).
    ///
    /// `None` exactly when the slot node is gone: its store was unmounted, or
    /// the root was torn down.
    ///
    /// ```text
    /// let avatar = cart.upload_at(&cart.state().avatar()).expect("the root is mounted");
    ///
    /// let _bar = avatar.subscribe(|handle| set_bar(handle.progress()));
    /// avatar.start().await?;
    /// ```
    pub fn upload_at(&self, slot: &UploadSlotState) -> Option<Upload> {
        let (store_id, name) = slot.key()?;

        Some(self.upload(&store_id, &name))
    }

    /// One upload of one store, addressed by its raw key.
    ///
    /// The registry primitive, kept for the handful of hand-written embedders
    /// that address a slot they never navigated to — but no longer the way a
    /// consumer walks from the tree, which is
    /// [`upload_at`](Self::upload_at) (§3.4).
    ///
    /// Uploads are singletons per store (BDR-0028), so `(store_id, name)`
    /// addresses exactly one handle. A handle exists from the first call —
    /// before any op has landed it reads as idle with the framework defaults —
    /// and the same key always resolves to the same handle, so it can be taken
    /// as soon as the marker appears.
    ///
    /// The handle carries the server-driven state *and* the control plane:
    /// [`select`](Upload::select), [`start`](Upload::start),
    /// [`cancel`](Upload::cancel) and [`reset`](Upload::reset) are on it
    /// (`docs/rust-client.md` §10.2).
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
