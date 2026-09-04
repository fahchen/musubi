//! The socket handle and its builder: what an embedder holds, plus the status
//! cell it reads liveness from.
//!
//! The task behind it is the `actor` submodule, the other half of this module
//! and the only one with state. Two types cross between them: handles post
//! `ActorMsg`s the actor is the sole consumer of, and the actor publishes
//! transitions into the `StatusCell` the handles read. Nothing else does.

mod actor;

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_channel::mpsc::{self, UnboundedReceiver, UnboundedSender};
use futures_channel::oneshot;
use futures_core::Stream;
use futures_util::StreamExt;
use serde_json::Value;

use self::actor::{Actor, Settings};
use crate::channel::{Channel, ChannelEvents};
use crate::error::{BuildError, SocketClosed};
use crate::seams::{Connector, Spawner, Timer};
use crate::url::endpoint_url;

// The channel handle speaks to the actor directly; every push and every leave
// is one of these.
pub(crate) use self::actor::{ActorMsg, PushPayload};

/// Phoenix's own default heartbeat interval; a missed reply within one
/// interval means the socket is dead (`phoenix.js` `heartbeatTimer`).
///
/// The interval is counted from each heartbeat's own write, not from a clock
/// running beside the actor, so it measures how long *this* heartbeat has gone
/// unanswered rather than how far the actor has fallen behind.
const DEFAULT_HEARTBEAT: Duration = Duration::from_secs(30);
/// Phoenix's own default push timeout, applied to joins and leaves too.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// Where the socket is in its connection lifecycle (BDR-0033).
///
/// The connection-wide liveness signal. The per-channel projection of the same
/// transitions arrives as [`ChannelEvent`](crate::ChannelEvent)s; this watch is
/// for embedders that want the one socket-level answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SocketStatus {
    /// Never connected yet: before the first [`Channel::join`] opens the
    /// socket lazily, and through initial connect attempts — a socket that
    /// has never been up is not "reconnecting".
    Connecting,
    /// The transport is open.
    Connected,
    /// The transport was lost after having been up (peer close, IO failure,
    /// missed heartbeat); the backoff ladder brings it back.
    Reconnecting,
    /// [`PhoenixSocket::disconnect`] was called; the socket never reconnects.
    Closed,
}

/// The status transitions, as a [`Stream`] of [`SocketStatus`].
///
/// One item per edge — a repeated value is never emitted — and the stream is
/// the subscription: dropping it unsubscribes, and it ends when the socket's
/// actor shuts down.
#[derive(Debug)]
pub struct SocketStatusUpdates {
    rx: UnboundedReceiver<SocketStatus>,
}

impl Stream for SocketStatusUpdates {
    type Item = SocketStatus;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_next_unpin(cx)
    }
}

/// The shared status cell: the actor writes transitions, the handles read.
#[derive(Debug)]
struct StatusCell {
    current: Mutex<SocketStatus>,
    watchers: Mutex<Vec<UnboundedSender<SocketStatus>>>,
}

impl StatusCell {
    fn new() -> Self {
        Self {
            current: Mutex::new(SocketStatus::Connecting),
            watchers: Mutex::new(Vec::new()),
        }
    }

    fn get(&self) -> SocketStatus {
        *lock(&self.current)
    }

    fn subscribe(&self) -> UnboundedReceiver<SocketStatus> {
        let (tx, rx) = mpsc::unbounded();

        lock(&self.watchers).push(tx);

        rx
    }

    /// Publishes a transition. A repeat of the current value is dropped, so
    /// watchers only ever see edges.
    fn set(&self, next: SocketStatus) {
        {
            let mut current = lock(&self.current);

            if *current == next {
                return;
            }

            *current = next;
        }

        lock(&self.watchers).retain(|watcher| watcher.unbounded_send(next).is_ok());
    }
}

/// Locks a mutex, ignoring poisoning: the cell holds plain state with no
/// invariant a half-finished write could break.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// The shared sender. Dropping the last handle (socket or channel) shuts the
/// actor down, so a forgotten socket does not keep reconnecting forever.
#[derive(Debug)]
pub(crate) struct SocketInner {
    pub(crate) tx: UnboundedSender<ActorMsg>,
}

impl Drop for SocketInner {
    fn drop(&mut self) {
        let _ = self.tx.unbounded_send(ActorMsg::Shutdown);
    }
}

/// A Phoenix socket: one connection, many channels.
///
/// The socket opens lazily on the first [`Channel::join`] and reconnects on its
/// own with the `phoenix.js` backoff ladder. Cloning is cheap; every clone
/// addresses the same actor.
#[derive(Debug, Clone)]
pub struct PhoenixSocket {
    inner: Arc<SocketInner>,
    status: Arc<StatusCell>,
}

impl PhoenixSocket {
    /// Starts a builder. `url`, `connector`, `spawner` and `timer` are required.
    ///
    /// ```text
    /// let socket = PhoenixSocket::builder()
    ///     .url("wss://example.test/socket")
    ///     .param("token", session_token)
    ///     .connector(TungsteniteConnector::default())
    ///     .spawner(TokioSpawner)
    ///     .timer(TokioTimer)
    ///     .build()?;
    /// ```
    pub fn builder() -> SocketBuilder {
        SocketBuilder::default()
    }

    /// Attaches a channel to `topic`, bumping the topic's generation.
    ///
    /// One channel per topic: attaching again replaces the previous entry and
    /// ends its event stream, and anything still holding the old [`Channel`]
    /// gets [`PushError::Stale`](crate::PushError::Stale). Leave a joined channel before re-attaching
    /// its topic — Phoenix refuses a second join on a topic it already holds.
    /// The returned channel is not joined yet — call [`Channel::join`].
    ///
    /// ```text
    /// let (channel, events) = socket.channel("room:lobby", json!({})).await?;
    /// channel.join()?;
    /// ```
    pub async fn channel(
        &self,
        topic: impl Into<String>,
        params: Value,
    ) -> Result<(Channel, ChannelEvents), SocketClosed> {
        let topic: Arc<str> = Arc::from(topic.into());
        let (reply_tx, reply_rx) = oneshot::channel();

        self.inner
            .tx
            .unbounded_send(ActorMsg::Attach {
                topic: Arc::clone(&topic),
                params,
                reply: reply_tx,
            })
            .map_err(|_| SocketClosed)?;

        let (generation, events) = reply_rx.await.map_err(|_| SocketClosed)?;

        Ok((
            Channel::new(topic, generation, Arc::clone(&self.inner)),
            ChannelEvents::new(events),
        ))
    }

    /// Where the socket is in its connection lifecycle, right now.
    ///
    /// ```text
    /// socket.status() //=> SocketStatus::Connected
    /// ```
    pub fn status(&self) -> SocketStatus {
        self.status.get()
    }

    /// The status transitions, oldest first (BDR-0033).
    ///
    /// The stream **is** the subscription: dropping it unsubscribes. It does
    /// not replay [`status`](Self::status) — read that first if the current
    /// value matters.
    ///
    /// ```text
    /// let mut statuses = socket.status_updates();
    /// // statuses.next().await //=> Some(SocketStatus::Connected)
    /// ```
    #[must_use = "the stream is the subscription; dropping it unsubscribes"]
    pub fn status_updates(&self) -> SocketStatusUpdates {
        SocketStatusUpdates {
            rx: self.status.subscribe(),
        }
    }

    /// Closes the socket for good: no reconnect, every channel dropped, every
    /// in-flight push rejected with [`PushError::Disconnected`](crate::PushError::Disconnected).
    ///
    /// ```text
    /// socket.disconnect().await?;
    /// ```
    pub async fn disconnect(&self) -> Result<(), SocketClosed> {
        let (ack_tx, ack_rx) = oneshot::channel();

        self.inner
            .tx
            .unbounded_send(ActorMsg::Disconnect { ack: ack_tx })
            .map_err(|_| SocketClosed)?;

        ack_rx.await.map_err(|_| SocketClosed)
    }
}

/// Builder for [`PhoenixSocket`].
#[derive(Default)]
pub struct SocketBuilder {
    url: Option<String>,
    params: BTreeMap<String, String>,
    connector: Option<Arc<dyn Connector>>,
    spawner: Option<Arc<dyn Spawner>>,
    timer: Option<Arc<dyn Timer>>,
    heartbeat: Option<Duration>,
    join_timeout: Option<Duration>,
    push_timeout: Option<Duration>,
}

impl SocketBuilder {
    /// The endpoint base, e.g. `wss://example.test/socket`. Required.
    ///
    /// `/websocket` and `vsn=2.0.0` are appended by the crate.
    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
        self
    }

    /// Adds one socket connect param. **Auth belongs here, never in join
    /// params.**
    pub fn param(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.params.insert(key.into(), value.into());
        self
    }

    /// The transport seam. Required.
    pub fn connector(mut self, connector: impl Connector) -> Self {
        self.connector = Some(Arc::new(connector));
        self
    }

    /// The executor seam. Required.
    pub fn spawner(mut self, spawner: impl Spawner) -> Self {
        self.spawner = Some(Arc::new(spawner));
        self
    }

    /// The clock seam. Required.
    pub fn timer(mut self, timer: impl Timer) -> Self {
        self.timer = Some(Arc::new(timer));
        self
    }

    /// Heartbeat interval; also the dead-socket detection window. Default 30s.
    pub fn heartbeat(mut self, interval: Duration) -> Self {
        self.heartbeat = Some(interval);
        self
    }

    /// How long a `phx_join` may go unanswered. Default 10s.
    pub fn join_timeout(mut self, timeout: Duration) -> Self {
        self.join_timeout = Some(timeout);
        self
    }

    /// How long any other push may go unanswered. Default 10s.
    pub fn push_timeout(mut self, timeout: Duration) -> Self {
        self.push_timeout = Some(timeout);
        self
    }

    /// Spawns the actor and returns the handle. The socket itself opens lazily
    /// on the first join, so the only failure is a missing seam.
    ///
    /// ```text
    /// let socket = builder.build()?;
    /// ```
    pub fn build(self) -> Result<PhoenixSocket, BuildError> {
        let url = self.url.ok_or(BuildError::MissingUrl)?;
        let connector = self.connector.ok_or(BuildError::MissingConnector)?;
        let spawner = self.spawner.ok_or(BuildError::MissingSpawner)?;
        let timer = self.timer.ok_or(BuildError::MissingTimer)?;

        let (tx, rx) = mpsc::unbounded();
        let status = Arc::new(StatusCell::new());

        Actor::spawn(
            Settings {
                url: endpoint_url(&url, &self.params),
                heartbeat: self.heartbeat.unwrap_or(DEFAULT_HEARTBEAT),
                join_timeout: self.join_timeout.unwrap_or(DEFAULT_TIMEOUT),
                push_timeout: self.push_timeout.unwrap_or(DEFAULT_TIMEOUT),
                connector,
                spawner,
                timer,
                status: Arc::clone(&status),
            },
            tx.clone(),
            rx,
        );

        Ok(PhoenixSocket {
            inner: Arc::new(SocketInner { tx }),
            status,
        })
    }
}
