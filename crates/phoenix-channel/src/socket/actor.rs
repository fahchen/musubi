//! The socket actor: the one task that owns the transport, the channel
//! registry, the ref counter and every timer.
//!
//! None of it is public API. The handle half of the module posts work as
//! `ActorMsg` and reads liveness off the `StatusCell` this writes; those two
//! types are the whole seam between the halves.
//!
//! No timer here can be cancelled — the `Timer` seam hands back a future, not a
//! handle — so every timer is *fenced* instead: what it carries names the thing
//! it was armed for, and the actor checks that name when the message lands. A
//! push names its ref, a join reply and a join timeout name their attempt, a
//! rejoin names the attempt it was armed to replace. Anything the channel has
//! since moved off is dropped rather than acted on, which is what keeps a
//! stale timer from stacking a second `phx_join` on a live one.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures_channel::mpsc::{self, UnboundedReceiver, UnboundedSender};
use futures_channel::oneshot;
use futures_util::{FutureExt, SinkExt, StreamExt, select_biased};
use serde_json::{Value, json};

use super::{SocketStatus, StatusCell};
use crate::backoff::Backoff;
use crate::channel::{ChannelErrorReason, ChannelEvent};
use crate::error::{PushError, TransportError};
use crate::frame::{
    BinaryPush, EVENT_CLOSE, EVENT_ERROR, EVENT_HEARTBEAT, EVENT_JOIN, EVENT_LEAVE, EVENT_REPLY,
    Frame, Message, Reply, ReplyStatus, TOPIC_PHOENIX,
};
use crate::seams::{Connector, Socket, Spawner, Timer};

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
        /// The attempt this rejoin was armed to replace. A timer that fires
        /// after its attempt was superseded is dropped, not acted on.
        attempt: u64,
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

/// What the builder decided, as the actor needs it.
///
/// The rest of the actor's state is its own, and starts empty.
pub(super) struct Settings {
    pub(super) url: String,
    pub(super) heartbeat: Duration,
    pub(super) join_timeout: Duration,
    pub(super) push_timeout: Duration,
    pub(super) connector: Arc<dyn Connector>,
    pub(super) spawner: Arc<dyn Spawner>,
    pub(super) timer: Arc<dyn Timer>,
    /// The liveness watch shared with every [`PhoenixSocket`](super::PhoenixSocket)
    /// handle.
    pub(super) status: Arc<StatusCell>,
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
    /// How many `phx_join`s this channel has sent. The current value names the
    /// attempt that [`ChannelStatus::Joining`] refers to, so a reply or a
    /// timeout stamped with an older one is known to belong to an attempt that
    /// has already been abandoned — and a rejoin armed against an older one is
    /// known to be redundant.
    ///
    /// A generation (which only `attach` bumps) cannot do this job: both
    /// attempts of one channel share it.
    attempt: u64,
    /// Whether the channel should be (re)joined whenever a socket is available.
    wants_join: bool,
    /// Set by a deliberate leave so the resulting `phx_close` neither surfaces
    /// nor re-enters reconnect handling.
    suppress_close: bool,
    backoff: Backoff,
    /// The attempt a pending rejoin timer was armed against, if any. A rejoin
    /// is disarmed by setting this to `None`, which is enough to make the timer
    /// that still fires a no-op.
    rejoin_scheduled: Option<u64>,
}

/// A push awaiting its `phx_reply`.
enum Inflight {
    Push {
        reply: oneshot::Sender<Result<Reply, PushError>>,
    },
    Join {
        topic: Arc<str>,
        generation: u64,
        attempt: u64,
    },
    Leave {
        topic: Arc<str>,
        generation: u64,
    },
}

/// The single owner of the socket, the registry and the ref counter.
pub(super) struct Actor {
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
    /// The liveness watch shared with every
    /// [`PhoenixSocket`](super::PhoenixSocket) handle.
    status: Arc<StatusCell>,
}

impl Actor {
    /// Builds the actor from the builder's settings and hands its loop to the
    /// spawner. The socket itself is opened lazily, by the first join.
    pub(super) fn spawn(
        settings: Settings,
        tx: UnboundedSender<ActorMsg>,
        rx: UnboundedReceiver<ActorMsg>,
    ) {
        let spawner = Arc::clone(&settings.spawner);
        let actor = Self {
            url: settings.url,
            heartbeat: settings.heartbeat,
            join_timeout: settings.join_timeout,
            push_timeout: settings.push_timeout,
            connector: settings.connector,
            spawner: settings.spawner,
            timer: settings.timer,
            status: settings.status,
            tx,
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
    }

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
            ActorMsg::Rejoin {
                topic,
                generation,
                attempt,
            } => self.rejoin(topic, generation, attempt).await,
            ActorMsg::Connected(result) => self.connected(result).await,
            ActorMsg::Reconnect => {
                self.reconnect_scheduled = false;
                self.connect();
            }
            ActorMsg::HeartbeatTick => self.heartbeat_tick().await,
            ActorMsg::Timeout { msg_ref } => self.timeout(msg_ref).await,
            ActorMsg::Disconnect { ack } => {
                self.closed = true;
                self.drop_socket(ChannelErrorReason::SocketClosed);
                self.channels.clear();
                self.status.set(SocketStatus::Closed);
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
                attempt: 0,
                wants_join: false,
                suppress_close: false,
                backoff: Backoff::default(),
                rejoin_scheduled: None,
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

    /// A scheduled rejoin came due.
    ///
    /// `phoenix.js` gates its rejoin timer on `socket.isConnected()` and resets
    /// it whenever the socket errors: while the transport is down the reconnect
    /// ladder owns recovery and rejoins everything on open, so a rejoin that
    /// dialled on its own would only jump the queue the ladder exists to hold.
    async fn rejoin(&mut self, topic: Arc<str>, generation: u64, attempt: u64) {
        let armed = match self.live_mut(&topic, generation) {
            Some(state) if state.rejoin_scheduled == Some(attempt) => {
                state.rejoin_scheduled = None;
                true
            }
            _ => false,
        };

        if armed && self.socket.is_some() {
            self.join(topic, generation).await;
        }
    }

    /// Sends one `phx_join`, if the channel has no other claim on the topic.
    ///
    /// Phoenix answers a second `phx_join` for a topic the socket already holds
    /// by killing that channel and running the join again
    /// (`Phoenix.Socket.shutdown_duplicate_channel/1`), so stacking attempts
    /// asks the server to throw away the very work we are waiting on. Only a
    /// [`ChannelStatus::Closed`] channel may join — which is `phoenix.js`'s
    /// `joinedOnce` guard and its `rejoin()` leaving-guard in one condition.
    async fn send_join(&mut self, topic: Arc<str>, generation: u64) {
        let msg_ref = self.next_ref();
        let ref_str = msg_ref.to_string();

        let Some(state) = self.live_mut(&topic, generation) else {
            return;
        };
        if state.status != ChannelStatus::Closed {
            return;
        }

        state.status = ChannelStatus::Joining;
        state.join_ref = Some(ref_str.clone());
        state.attempt += 1;
        let attempt = state.attempt;
        let payload = state.params.clone();

        self.inflight.insert(
            msg_ref,
            Inflight::Join {
                topic: Arc::clone(&topic),
                generation,
                attempt,
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
        // Leaving supersedes any join still in flight: a reply that arrives
        // after this must not report a channel on its way out as joined.
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
                self.report_lost();
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
        self.status.set(SocketStatus::Connected);

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

    async fn timeout(&mut self, msg_ref: u64) {
        if let Some(inflight) = self.inflight.remove(&msg_ref) {
            self.abandon(inflight, PushError::Timeout).await;
        }
    }

    /// Fails one inflight entry that can never resolve, dispatching by kind:
    /// the push learns why, a join reports [`ChannelEvent::JoinTimeout`], is
    /// taken back off the server and rescheduled, and a leave completes.
    ///
    /// Both callers have already removed the entry, so a join that is still the
    /// channel's current attempt has to be resolved here: nothing else will
    /// fire on it, and it would otherwise sit in [`ChannelStatus::Joining`]
    /// forever. An attempt the channel has already moved off is another matter
    /// — the fence drops it, because whatever superseded it did the resolving.
    ///
    /// A timeout and an unreadable reply are the only two ways an attempt ends
    /// without the server knowing:
    /// it either never answered, or answered something we could not read. Every
    /// other way a join is superseded — a `phx_error`, a deliberate leave, a
    /// dead transport — is one the server already knows about, so this is the
    /// one path that has to say `phx_leave` on its own.
    async fn abandon(&mut self, inflight: Inflight, push_error: PushError) {
        match inflight {
            Inflight::Push { reply } => {
                let _ = reply.send(Err(push_error));
            }
            Inflight::Join {
                topic,
                generation,
                attempt,
            } => {
                let Some(state) = self.joining(&topic, generation, attempt) else {
                    return;
                };
                state.status = ChannelStatus::Closed;
                let join_ref = state.join_ref.take();
                emit(state, ChannelEvent::JoinTimeout);

                if let Some(join_ref) = join_ref {
                    self.leave_abandoned_join(&topic, join_ref).await;
                }

                self.schedule_rejoin(&topic, generation);
            }
            Inflight::Leave { topic, generation } => {
                self.remove_if_live(&topic, generation);
            }
        }
    }

    /// Tells the server to drop the channel an abandoned `phx_join` may still
    /// be building, the way `phoenix.js` does before it re-arms (`channel.js`,
    /// the join-timeout hook).
    ///
    /// Without it a join that merely runs longer than the join timeout never
    /// converges: the server finishes the mount and holds the channel, the
    /// retry arrives as a duplicate join, and Phoenix answers that by killing
    /// the channel it just finished and starting the same expensive join over.
    /// The leave is stamped with the abandoned attempt's join_ref, which is
    /// what `Phoenix.Socket` matches it against.
    ///
    /// Fire and forget: nothing is registered for the reply, because the
    /// channel stays in the registry to be rejoined rather than dropped.
    async fn leave_abandoned_join(&mut self, topic: &Arc<str>, join_ref: String) {
        let msg_ref = self.next_ref();

        self.send_frame(Message {
            join_ref: Some(join_ref),
            msg_ref: Some(msg_ref.to_string()),
            topic: topic.to_string(),
            event: EVENT_LEAVE.to_owned(),
            payload: json!({}),
        })
        .await;
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
            self.handle_reply(message).await;
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
            EVENT_CLOSE if state.suppress_close => {
                self.channels.remove(&topic);
            }
            EVENT_CLOSE => self.went_down(&topic, generation, ChannelEvent::Close),
            EVENT_ERROR => self.went_down(
                &topic,
                generation,
                ChannelEvent::Error {
                    reason: ChannelErrorReason::Server,
                },
            ),
            _ => emit(
                state,
                ChannelEvent::Message {
                    event: message.event,
                    payload: message.payload,
                },
            ),
        }
    }

    /// The server tore the channel down: it is closed, `event` is reported and
    /// a rejoin is scheduled.
    ///
    /// Leaving [`ChannelStatus::Joining`] is what abandons a join still in
    /// flight — the reply it may still get, and the timeout it will get, both
    /// name the attempt and find it superseded. No `phx_leave` is owed: the
    /// server is the one that closed the channel.
    fn went_down(&mut self, topic: &Arc<str>, generation: u64, event: ChannelEvent) {
        let Some(state) = self.live_mut(topic, generation) else {
            return;
        };
        state.status = ChannelStatus::Closed;
        state.join_ref = None;
        emit(state, event);
        self.schedule_rejoin(topic, generation);
    }

    async fn handle_reply(&mut self, message: Message) {
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
                self.abandon(inflight, PushError::MalformedReply).await;
                return;
            }
        };

        match inflight {
            Inflight::Push { reply: sender } => {
                let _ = sender.send(Ok(reply));
            }
            Inflight::Join {
                topic,
                generation,
                attempt,
            } => self.join_replied(topic, generation, attempt, reply),
            Inflight::Leave { topic, generation } => {
                self.remove_if_live(&topic, generation);
            }
        }
    }

    fn join_replied(&mut self, topic: Arc<str>, generation: u64, attempt: u64, reply: Reply) {
        let Some(state) = self.joining(&topic, generation, attempt) else {
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

    /// Reports a lost or failed transport on the status watch: a socket that
    /// has never been up stays [`SocketStatus::Connecting`]; anything else is
    /// [`SocketStatus::Reconnecting`]. A closed socket reports nothing — the
    /// disconnect path owns [`SocketStatus::Closed`].
    fn report_lost(&self) {
        if self.closed || self.status.get() == SocketStatus::Connecting {
            return;
        }

        self.status.set(SocketStatus::Reconnecting);
    }

    /// Tears the socket down: cancels the heartbeat, fails everything in
    /// flight, and reports `reason` to every channel that wanted to be joined.
    fn drop_socket(&mut self, reason: ChannelErrorReason) {
        self.report_lost();
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
            // Whatever the old socket had running is gone with it: a join in
            // flight is superseded by the reset above, and a rejoin armed
            // against it is disarmed here. Reconnecting rejoins everything, so
            // a rejoin timer that outlived its socket would only stack a second
            // `phx_join` on the one the reconnect already sent.
            state.rejoin_scheduled = None;

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
        if state.generation != generation || !state.wants_join || state.rejoin_scheduled.is_some() {
            return;
        }

        let attempt = state.attempt;
        state.rejoin_scheduled = Some(attempt);
        let delay = state.backoff.next_delay();
        self.spawn_after(
            delay,
            ActorMsg::Rejoin {
                topic: Arc::clone(topic),
                generation,
                attempt,
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

    /// The registry entry for `topic`, only if it is still waiting on `attempt`.
    ///
    /// This is the fence every join reply and every join timeout passes: a
    /// channel that has moved on — joined, closed, leaving, or already onto a
    /// later attempt — must not be reopened, failed or rescheduled by one it
    /// stopped waiting for.
    fn joining(
        &mut self,
        topic: &Arc<str>,
        generation: u64,
        attempt: u64,
    ) -> Option<&mut ChannelState> {
        self.live_mut(topic, generation)
            .filter(|state| state.status == ChannelStatus::Joining && state.attempt == attempt)
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
