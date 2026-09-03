//! The socket handle, its builder, and the single actor task that owns the
//! connection, the channel registry, and every timer.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures_channel::mpsc::{self, UnboundedReceiver, UnboundedSender};
use futures_channel::oneshot;
use futures_util::{FutureExt, SinkExt, StreamExt, select_biased};
use serde_json::{Value, json};

use crate::backoff::Backoff;
use crate::channel::{Channel, ChannelErrorReason, ChannelEvent, ChannelEvents};
use crate::error::{BuildError, PushError, SocketClosed, TransportError};
use crate::frame::{
    BinaryPush, EVENT_CLOSE, EVENT_ERROR, EVENT_HEARTBEAT, EVENT_JOIN, EVENT_LEAVE, EVENT_REPLY,
    Frame, Message, Reply, ReplyStatus, TOPIC_PHOENIX,
};
use crate::seams::{Connector, Socket, Spawner, Timer};
use crate::url::endpoint_url;

/// Phoenix's own default heartbeat interval; a missed reply within one
/// interval means the socket is dead (`phoenix.js` `heartbeatTimer`).
const DEFAULT_HEARTBEAT: Duration = Duration::from_secs(30);
/// Phoenix's own default push timeout, applied to joins and leaves too.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// Messages the actor accepts. Every handle method and every timer task is a
/// sender; the actor is the only consumer.
pub(crate) enum ActorMsg {
    Attach {
        topic: Arc<str>,
        params: Value,
        reply: oneshot::Sender<(u64, UnboundedReceiver<ChannelEvent>)>,
    },
    Join {
        topic: Arc<str>,
        generation: u64,
    },
    Leave {
        topic: Arc<str>,
        generation: u64,
    },
    Push {
        topic: Arc<str>,
        generation: u64,
        event: String,
        payload: PushPayload,
        reply: oneshot::Sender<Result<Reply, PushError>>,
    },
    Rejoin {
        topic: Arc<str>,
        generation: u64,
    },
    Connected(Result<Box<dyn Socket>, TransportError>),
    Reconnect,
    HeartbeatTick,
    Timeout {
        msg_ref: u64,
    },
    Disconnect {
        ack: oneshot::Sender<()>,
    },
    Shutdown,
}

/// What a push carries: a JSON payload in the serializer v2 five-tuple, or raw
/// bytes in a binary frame (upload chunks, BDR-0026).
pub(crate) enum PushPayload {
    Json(Value),
    Binary(Vec<u8>),
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
    /// gets [`PushError::Stale`]. Leave a joined channel before re-attaching
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

    /// Closes the socket for good: no reconnect, every channel dropped, every
    /// in-flight push rejected with [`PushError::Disconnected`].
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
        let actor = Actor {
            url: endpoint_url(&url, &self.params),
            heartbeat: self.heartbeat.unwrap_or(DEFAULT_HEARTBEAT),
            join_timeout: self.join_timeout.unwrap_or(DEFAULT_TIMEOUT),
            push_timeout: self.push_timeout.unwrap_or(DEFAULT_TIMEOUT),
            connector,
            spawner: Arc::clone(&spawner),
            timer,
            tx: tx.clone(),
            rx,
            socket: None,
            channels: HashMap::new(),
            inflight: HashMap::new(),
            next_ref: 0,
            next_generation: 0,
            connecting: false,
            reconnect_scheduled: false,
            closed: false,
            backoff: Backoff::default(),
            heartbeat_cancel: None,
            pending_heartbeat: None,
        };

        spawner.spawn(Box::pin(actor.run()));

        Ok(PhoenixSocket {
            inner: Arc::new(SocketInner { tx }),
        })
    }
}

/// Where a channel is in its join lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChannelStatus {
    /// Not joined: never joined, join failed, or the socket went away.
    Closed,
    /// A `phx_join` is in flight.
    Joining,
    /// The server acknowledged the join.
    Joined,
    /// A `phx_leave` is in flight; the entry is dropped when it resolves.
    Leaving,
}

/// One registry entry. There is at most one per topic.
struct ChannelState {
    generation: u64,
    params: Value,
    events: UnboundedSender<ChannelEvent>,
    status: ChannelStatus,
    join_ref: Option<String>,
    /// Whether the channel should be (re)joined whenever a socket is available.
    wants_join: bool,
    /// Set by a deliberate leave so the resulting `phx_close` neither surfaces
    /// nor re-enters reconnect handling.
    suppress_close: bool,
    backoff: Backoff,
    rejoin_scheduled: bool,
}

/// A push awaiting its `phx_reply`.
enum Inflight {
    Push {
        reply: oneshot::Sender<Result<Reply, PushError>>,
    },
    Join {
        topic: Arc<str>,
        generation: u64,
    },
    Leave {
        topic: Arc<str>,
        generation: u64,
    },
}

/// The single owner of the socket, the registry and the ref counter.
struct Actor {
    url: String,
    heartbeat: Duration,
    join_timeout: Duration,
    push_timeout: Duration,
    connector: Arc<dyn Connector>,
    spawner: Arc<dyn Spawner>,
    timer: Arc<dyn Timer>,
    tx: UnboundedSender<ActorMsg>,
    rx: UnboundedReceiver<ActorMsg>,
    socket: Option<Box<dyn Socket>>,
    channels: HashMap<Arc<str>, ChannelState>,
    inflight: HashMap<u64, Inflight>,
    next_ref: u64,
    next_generation: u64,
    connecting: bool,
    reconnect_scheduled: bool,
    closed: bool,
    backoff: Backoff,
    /// Dropping this cancels the heartbeat task bound to the current socket.
    heartbeat_cancel: Option<oneshot::Sender<()>>,
    /// The ref of a heartbeat still awaiting its reply.
    pending_heartbeat: Option<u64>,
}

impl Actor {
    async fn run(mut self) {
        loop {
            let next = match self.socket.as_mut() {
                Some(socket) => {
                    let mut msg = self.rx.next().fuse();
                    let mut frame = socket.next().fuse();

                    select_biased! {
                        msg = msg => Next::Msg(msg),
                        frame = frame => Next::Frame(frame),
                    }
                }
                None => Next::Msg(self.rx.next().await),
            };

            match next {
                Next::Msg(None) | Next::Msg(Some(ActorMsg::Shutdown)) => break,
                Next::Msg(Some(msg)) => self.handle_msg(msg).await,
                Next::Frame(Some(Ok(frame))) => self.handle_frame(frame).await,
                Next::Frame(Some(Err(error))) => {
                    tracing::debug!(%error, "socket read failed");
                    self.drop_socket(ChannelErrorReason::SocketClosed);
                    self.schedule_reconnect();
                }
                Next::Frame(None) => {
                    tracing::debug!("socket stream ended");
                    self.drop_socket(ChannelErrorReason::SocketClosed);
                    self.schedule_reconnect();
                }
            }
        }
    }

    async fn handle_msg(&mut self, msg: ActorMsg) {
        match msg {
            ActorMsg::Attach {
                topic,
                params,
                reply,
            } => self.attach(topic, params, reply),
            ActorMsg::Join { topic, generation } => self.join(topic, generation).await,
            ActorMsg::Leave { topic, generation } => self.leave(topic, generation).await,
            ActorMsg::Push {
                topic,
                generation,
                event,
                payload,
                reply,
            } => self.push(topic, generation, event, payload, reply).await,
            ActorMsg::Rejoin { topic, generation } => {
                if let Some(state) = self.live_mut(&topic, generation) {
                    state.rejoin_scheduled = false;
                }
                self.join(topic, generation).await;
            }
            ActorMsg::Connected(result) => self.connected(result).await,
            ActorMsg::Reconnect => {
                self.reconnect_scheduled = false;
                self.connect();
            }
            ActorMsg::HeartbeatTick => self.heartbeat_tick().await,
            ActorMsg::Timeout { msg_ref } => self.timeout(msg_ref),
            ActorMsg::Disconnect { ack } => {
                self.closed = true;
                self.drop_socket(ChannelErrorReason::SocketClosed);
                self.channels.clear();
                let _ = ack.send(());
            }
            // Handled by the loop so it can break.
            ActorMsg::Shutdown => {}
        }
    }

    fn attach(
        &mut self,
        topic: Arc<str>,
        params: Value,
        reply: oneshot::Sender<(u64, UnboundedReceiver<ChannelEvent>)>,
    ) {
        self.next_generation += 1;
        let generation = self.next_generation;
        let (events_tx, events_rx) = mpsc::unbounded();

        // Replacing the entry drops the previous sender, ending the old
        // channel's event stream.
        self.channels.insert(
            Arc::clone(&topic),
            ChannelState {
                generation,
                params,
                events: events_tx,
                status: ChannelStatus::Closed,
                join_ref: None,
                wants_join: false,
                suppress_close: false,
                backoff: Backoff::default(),
                rejoin_scheduled: false,
            },
        );

        let _ = reply.send((generation, events_rx));
    }

    async fn join(&mut self, topic: Arc<str>, generation: u64) {
        let Some(state) = self.live_mut(&topic, generation) else {
            return;
        };

        state.wants_join = true;
        state.suppress_close = false;

        if self.socket.is_none() {
            self.connect();
            return;
        }

        self.send_join(topic, generation).await;
    }

    async fn send_join(&mut self, topic: Arc<str>, generation: u64) {
        let msg_ref = self.next_ref();
        let ref_str = msg_ref.to_string();

        let Some(state) = self.live_mut(&topic, generation) else {
            return;
        };
        state.status = ChannelStatus::Joining;
        state.join_ref = Some(ref_str.clone());
        let payload = state.params.clone();

        self.inflight.insert(
            msg_ref,
            Inflight::Join {
                topic: Arc::clone(&topic),
                generation,
            },
        );
        self.spawn_timeout(msg_ref, self.join_timeout);
        self.send_frame(Message {
            join_ref: Some(ref_str.clone()),
            msg_ref: Some(ref_str),
            topic: topic.to_string(),
            event: EVENT_JOIN.to_owned(),
            payload,
        })
        .await;
    }

    async fn leave(&mut self, topic: Arc<str>, generation: u64) {
        let connected = self.socket.is_some();

        let Some(state) = self.live_mut(&topic, generation) else {
            return;
        };
        state.wants_join = false;
        state.suppress_close = true;
        state.status = ChannelStatus::Leaving;
        let join_ref = state.join_ref.clone();

        // Nothing to leave: no socket, or never joined. Drop the entry so the
        // event stream ends and nothing rejoins it.
        let (Some(join_ref), true) = (join_ref, connected) else {
            self.channels.remove(&topic);
            return;
        };

        let msg_ref = self.next_ref();

        self.inflight.insert(
            msg_ref,
            Inflight::Leave {
                topic: Arc::clone(&topic),
                generation,
            },
        );
        self.spawn_timeout(msg_ref, self.push_timeout);
        self.send_frame(Message {
            join_ref: Some(join_ref),
            msg_ref: Some(msg_ref.to_string()),
            topic: topic.to_string(),
            event: EVENT_LEAVE.to_owned(),
            payload: json!({}),
        })
        .await;
    }

    async fn push(
        &mut self,
        topic: Arc<str>,
        generation: u64,
        event: String,
        payload: PushPayload,
        reply: oneshot::Sender<Result<Reply, PushError>>,
    ) {
        let connected = self.socket.is_some();

        let Some(state) = self.channels.get(&topic) else {
            let _ = reply.send(Err(PushError::Stale));
            return;
        };
        if state.generation != generation {
            let _ = reply.send(Err(PushError::Stale));
            return;
        }
        if !connected || state.status != ChannelStatus::Joined {
            let _ = reply.send(Err(PushError::NotJoined));
            return;
        }
        let join_ref = state.join_ref.clone();
        let msg_ref = self.next_ref();
        let ref_str = msg_ref.to_string();

        // Framed before anything is registered: a payload that cannot be
        // encoded must not leave a timeout armed on a ref nothing will answer.
        let frame = match payload {
            PushPayload::Json(payload) => Message {
                join_ref,
                msg_ref: Some(ref_str),
                topic: topic.to_string(),
                event,
                payload,
            }
            .encode(),
            PushPayload::Binary(payload) => {
                let push = BinaryPush {
                    join_ref: join_ref.unwrap_or_default(),
                    msg_ref: ref_str,
                    topic: topic.to_string(),
                    event,
                    payload,
                };

                match push.encode() {
                    Ok(frame) => frame,
                    Err(error) => {
                        let _ = reply.send(Err(error.into()));
                        return;
                    }
                }
            }
        };

        self.inflight.insert(msg_ref, Inflight::Push { reply });
        self.spawn_timeout(msg_ref, self.push_timeout);
        self.send_encoded(frame).await;
    }

    async fn connected(&mut self, result: Result<Box<dyn Socket>, TransportError>) {
        self.connecting = false;

        let socket = match result {
            Ok(socket) => socket,
            Err(error) => {
                tracing::debug!(%error, "socket connect failed");
                self.notify_joining(ChannelEvent::Error {
                    reason: ChannelErrorReason::SocketClosed,
                });
                self.schedule_reconnect();
                return;
            }
        };

        if self.closed {
            return;
        }

        self.socket = Some(socket);
        self.backoff.reset();
        self.start_heartbeat();

        // Rejoin every registered channel; its join-ok fires again, which is
        // the one recovery hook consumers get.
        let pending: Vec<(Arc<str>, u64)> = self
            .channels
            .iter()
            .filter(|(_, state)| state.wants_join)
            .map(|(topic, state)| (Arc::clone(topic), state.generation))
            .collect();

        for (topic, generation) in pending {
            self.send_join(topic, generation).await;
        }
    }

    async fn heartbeat_tick(&mut self) {
        if self.socket.is_none() {
            return;
        }

        // A heartbeat still unanswered when the next one is due means the
        // socket is dead even though the transport has not noticed.
        if self.pending_heartbeat.is_some() {
            tracing::debug!("heartbeat timeout, tearing the socket down");
            self.drop_socket(ChannelErrorReason::HeartbeatTimeout);
            self.schedule_reconnect();
            return;
        }

        let msg_ref = self.next_ref();
        self.pending_heartbeat = Some(msg_ref);
        self.send_frame(Message {
            join_ref: None,
            msg_ref: Some(msg_ref.to_string()),
            topic: TOPIC_PHOENIX.to_owned(),
            event: EVENT_HEARTBEAT.to_owned(),
            payload: json!({}),
        })
        .await;
    }

    fn timeout(&mut self, msg_ref: u64) {
        if let Some(inflight) = self.inflight.remove(&msg_ref) {
            self.abandon(inflight, PushError::Timeout);
        }
    }

    /// Fails one inflight entry that can never resolve, dispatching by kind:
    /// the push learns why, a join reports [`ChannelEvent::JoinTimeout`] and is
    /// rescheduled, and a leave completes.
    ///
    /// Both callers have already removed the entry, so a join left un-notified
    /// here would sit in [`ChannelStatus::Joining`] forever — its own timeout
    /// no longer finds anything to fire on.
    fn abandon(&mut self, inflight: Inflight, push_error: PushError) {
        match inflight {
            Inflight::Push { reply } => {
                let _ = reply.send(Err(push_error));
            }
            Inflight::Join { topic, generation } => {
                if let Some(state) = self.live_mut(&topic, generation) {
                    state.status = ChannelStatus::Closed;
                    emit(state, ChannelEvent::JoinTimeout);
                }
                self.schedule_rejoin(&topic, generation);
            }
            Inflight::Leave { topic, generation } => {
                self.remove_if_live(&topic, generation);
            }
        }
    }

    async fn handle_frame(&mut self, frame: Frame) {
        let text = match frame {
            Frame::Text(text) => text,
            Frame::Binary(bytes) => {
                tracing::debug!(len = bytes.len(), "dropping binary frame");
                return;
            }
        };

        let message = match Message::decode(&text) {
            Ok(message) => message,
            Err(error) => {
                tracing::warn!(%error, "dropping undecodable frame");
                return;
            }
        };

        if message.event == EVENT_REPLY {
            self.handle_reply(message);
            return;
        }

        let topic: Arc<str> = Arc::from(message.topic.as_str());
        let Some(state) = self.channels.get_mut(&topic) else {
            return;
        };

        // A message stamped with a superseded join_ref belongs to a channel
        // incarnation we have already replaced.
        if message.join_ref.is_some() && message.join_ref != state.join_ref {
            return;
        }

        let generation = state.generation;

        match message.event.as_str() {
            EVENT_CLOSE => {
                if state.suppress_close {
                    self.channels.remove(&topic);
                    return;
                }
                state.status = ChannelStatus::Closed;
                state.join_ref = None;
                emit(state, ChannelEvent::Close);
                self.schedule_rejoin(&topic, generation);
            }
            EVENT_ERROR => {
                state.status = ChannelStatus::Closed;
                state.join_ref = None;
                emit(
                    state,
                    ChannelEvent::Error {
                        reason: ChannelErrorReason::Server,
                    },
                );
                self.schedule_rejoin(&topic, generation);
            }
            _ => emit(
                state,
                ChannelEvent::Message {
                    event: message.event,
                    payload: message.payload,
                },
            ),
        }
    }

    fn handle_reply(&mut self, message: Message) {
        let Some(msg_ref) = message.msg_ref.as_ref().and_then(|r| r.parse::<u64>().ok()) else {
            tracing::warn!(msg_ref = ?message.msg_ref, "reply without a usable ref");
            return;
        };

        if self.pending_heartbeat == Some(msg_ref) {
            self.pending_heartbeat = None;
            return;
        }

        let Some(inflight) = self.inflight.remove(&msg_ref) else {
            tracing::debug!(msg_ref, "reply for an unknown ref");
            return;
        };

        let reply = match serde_json::from_value::<Reply>(message.payload) {
            Ok(reply) => reply,
            Err(error) => {
                tracing::warn!(%error, "dropping malformed reply payload");
                self.abandon(inflight, PushError::MalformedReply);
                return;
            }
        };

        match inflight {
            Inflight::Push { reply: sender } => {
                let _ = sender.send(Ok(reply));
            }
            Inflight::Join { topic, generation } => self.join_replied(topic, generation, reply),
            Inflight::Leave { topic, generation } => {
                self.remove_if_live(&topic, generation);
            }
        }
    }

    fn join_replied(&mut self, topic: Arc<str>, generation: u64, reply: Reply) {
        let Some(state) = self.live_mut(&topic, generation) else {
            return;
        };

        match reply.status {
            ReplyStatus::Ok => {
                state.status = ChannelStatus::Joined;
                state.backoff.reset();
                emit(
                    state,
                    ChannelEvent::Joined {
                        response: reply.response,
                    },
                );
            }
            ReplyStatus::Error => {
                state.status = ChannelStatus::Closed;
                emit(
                    state,
                    ChannelEvent::JoinError {
                        response: reply.response,
                    },
                );
                self.schedule_rejoin(&topic, generation);
            }
        }
    }

    async fn send_frame(&mut self, message: Message) {
        self.send_encoded(message.encode()).await;
    }

    async fn send_encoded(&mut self, frame: Frame) {
        let result = match self.socket.as_mut() {
            Some(socket) => socket.send(frame).await,
            None => return,
        };

        if let Err(error) = result {
            tracing::debug!(%error, "socket write failed");
            self.drop_socket(ChannelErrorReason::SocketClosed);
            self.schedule_reconnect();
        }
    }

    /// Tears the socket down: cancels the heartbeat, fails everything in
    /// flight, and reports `reason` to every channel that wanted to be joined.
    fn drop_socket(&mut self, reason: ChannelErrorReason) {
        self.socket = None;
        self.heartbeat_cancel = None;
        self.pending_heartbeat = None;

        for (_, inflight) in self.inflight.drain() {
            if let Inflight::Push { reply } = inflight {
                let _ = reply.send(Err(PushError::Disconnected));
            }
        }

        self.channels.retain(|_, state| {
            // A leave that will never be acknowledged still ends the channel.
            if state.status == ChannelStatus::Leaving {
                return false;
            }

            let was_live = state.status != ChannelStatus::Closed;
            state.status = ChannelStatus::Closed;
            state.join_ref = None;

            if was_live {
                emit(state, ChannelEvent::Error { reason });
            }

            true
        });
    }

    fn connect(&mut self) {
        if self.closed || self.connecting || self.socket.is_some() {
            return;
        }

        self.connecting = true;
        let connector = Arc::clone(&self.connector);
        let url = self.url.clone();
        let tx = self.tx.clone();

        self.spawner.spawn(Box::pin(async move {
            let result = connector.connect(&url).await;
            let _ = tx.unbounded_send(ActorMsg::Connected(result));
        }));
    }

    fn schedule_reconnect(&mut self) {
        if self.closed || self.reconnect_scheduled || self.connecting || self.socket.is_some() {
            return;
        }
        if !self.channels.values().any(|state| state.wants_join) {
            return;
        }

        self.reconnect_scheduled = true;
        let delay = self.backoff.next_delay();
        self.spawn_after(delay, ActorMsg::Reconnect);
    }

    fn schedule_rejoin(&mut self, topic: &Arc<str>, generation: u64) {
        // While the socket is down, reconnecting rejoins everything; a second
        // per-channel ladder would only duplicate the attempt.
        if self.closed || self.socket.is_none() {
            return;
        }

        let Some(state) = self.channels.get_mut(topic) else {
            return;
        };
        if state.generation != generation || !state.wants_join || state.rejoin_scheduled {
            return;
        }

        state.rejoin_scheduled = true;
        let delay = state.backoff.next_delay();
        self.spawn_after(
            delay,
            ActorMsg::Rejoin {
                topic: Arc::clone(topic),
                generation,
            },
        );
    }

    fn start_heartbeat(&mut self) {
        let (cancel_tx, cancel_rx) = oneshot::channel();
        let timer = Arc::clone(&self.timer);
        let tx = self.tx.clone();
        let interval = self.heartbeat;

        self.spawner.spawn(Box::pin(async move {
            let mut cancel = cancel_rx.fuse();

            loop {
                let mut tick = timer.sleep(interval).fuse();

                select_biased! {
                    _ = cancel => break,
                    () = tick => {}
                }

                if tx.unbounded_send(ActorMsg::HeartbeatTick).is_err() {
                    break;
                }
            }
        }));

        self.heartbeat_cancel = Some(cancel_tx);
    }

    fn spawn_timeout(&self, msg_ref: u64, after: Duration) {
        self.spawn_after(after, ActorMsg::Timeout { msg_ref });
    }

    fn spawn_after(&self, delay: Duration, msg: ActorMsg) {
        let timer = Arc::clone(&self.timer);
        let tx = self.tx.clone();

        self.spawner.spawn(Box::pin(async move {
            timer.sleep(delay).await;
            let _ = tx.unbounded_send(msg);
        }));
    }

    fn next_ref(&mut self) -> u64 {
        self.next_ref += 1;
        self.next_ref
    }

    /// The registry entry for `topic`, only if `generation` is still current.
    fn live_mut(&mut self, topic: &Arc<str>, generation: u64) -> Option<&mut ChannelState> {
        self.channels
            .get_mut(topic)
            .filter(|state| state.generation == generation)
    }

    fn remove_if_live(&mut self, topic: &Arc<str>, generation: u64) {
        if self.live_mut(topic, generation).is_some() {
            self.channels.remove(topic);
        }
    }

    /// Reports `event` to every channel waiting to be joined.
    fn notify_joining(&self, event: ChannelEvent) {
        for state in self.channels.values().filter(|state| state.wants_join) {
            emit(state, event.clone());
        }
    }
}

/// What the actor loop woke up for.
enum Next {
    Msg(Option<ActorMsg>),
    Frame(Option<Result<Frame, TransportError>>),
}

/// Delivers one event; a dropped receiver is not an error.
fn emit(state: &ChannelState, event: ChannelEvent) {
    let _ = state.events.unbounded_send(event);
}
