//! The connection actor: one owned task, one registry of mounted roots
//! (`docs/rust-client.md` §2.4, §6, §9).
//!
//! Every handle method turns into one [`ActorMsg`]; the actor is the only
//! consumer, so there is no shared mutable state and no lock around the tree.
//! Inbound channel events reach the actor through one forwarding task per
//! channel incarnation, stamped with the generation that was current when the
//! channel was attached — anything from a superseded incarnation is dropped.

use std::any::Any;
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender};
use futures_channel::oneshot;
use futures_util::StreamExt;
use phoenix_channel::{
    Channel, ChannelEvent, ChannelEvents, PhoenixSocket, PushError, Reply, ReplyStatus, Spawner,
    Timer,
};
use serde_json::{Value, json};

use crate::cache::{CACHE_WRITE_THROTTLE, CacheEntry, CacheStore, cache_key, now_ms};
use crate::engine::PatchEngine;
use crate::envelope::PatchEnvelope;
use crate::error::{CommandError, MusubiError, Result};
use crate::generated::StoreId;
use crate::mounted::RootSink;
use crate::transfer;

/// The push event a patch envelope arrives under.
const EVENT_PATCH: &str = "patch";
/// The push event a command is dispatched under.
const EVENT_COMMAND: &str = "command";
/// The error-response fields a command's `code` is read from, in priority
/// order; the first **string-valued** one wins (§6.2).
const CODE_FIELDS: [&str; 3] = ["code", "error", "reason"];
/// How many dispatches one cache-seeded root may hold while its live initial
/// patch is still in flight (§6.2).
///
/// The queue exists so a seeded root can be interacted with before `version`
/// reaches `1`; it is not a retry buffer, so it is small and overflowing it
/// rejects rather than grows.
const MAX_QUEUED_DISPATCHES: usize = 32;

/// The typed cell of a mounted root, as the actor sees it: opaque, and handed
/// back to the mount caller to downcast.
pub(crate) type AnyCell = Arc<dyn Any + Send + Sync>;

/// Everything the actor accepts. Every handle method and every forwarding task
/// is a sender; the actor is the only consumer.
pub(crate) enum ActorMsg {
    /// Mount a root, or alias an existing one.
    Mount(Box<MountRequest>),
    /// A [`Mounted`](crate::Mounted) handle was cloned.
    Retain {
        /// The root the clone holds.
        root_id: Arc<str>,
    },
    /// A [`Mounted`](crate::Mounted) handle was dropped.
    Release {
        /// The root the handle held.
        root_id: Arc<str>,
    },
    /// Dispatch a command on a mounted root.
    Command(Box<CommandRequest>),
    /// A dispatched command's push resolved.
    CommandReply {
        /// The root the command was dispatched on.
        root_id: Arc<str>,
        /// The actor-assigned id of the command.
        id: u64,
        /// What the push produced.
        outcome: std::result::Result<Reply, PushError>,
    },
    /// One channel event, stamped with the generation it was forwarded for.
    Channel {
        /// The root the channel belongs to.
        root_id: Arc<str>,
        /// The channel incarnation that produced the event.
        generation: u64,
        /// The event itself.
        event: ChannelEvent,
    },
    /// Push one upload control-plane event on a root's main channel
    /// (`docs/rust-client.md` §10.2).
    ///
    /// Routed through the actor rather than pushed from the handle because the
    /// current channel incarnation is the actor's to know: a recovery replaces
    /// it, and a handle holding the old one would push into a stale channel.
    UploadPush {
        /// The root whose channel carries the push.
        root_id: Arc<str>,
        /// The event: `allow_upload`, `cancel_upload`, `upload_progress` or
        /// `upload_error`.
        event: &'static str,
        /// The already-built payload.
        payload: Value,
        /// Where the reply goes; `None` for the fire-and-forget relays.
        reply: Option<oneshot::Sender<Result<Value>>>,
    },
    /// A cache read produced a usable entry for a root still awaiting its
    /// initial patch (`docs/rust-client.md` §6.4).
    CacheSeed {
        /// The root to seed.
        root_id: Arc<str>,
        /// The slot the read was issued for. A root is addressed by
        /// `"<module>:<id>"` but its cache slot also keys on the mount params,
        /// so a read that outlives its own mount is identified — and dropped —
        /// by this.
        key: Arc<str>,
        /// The entry, already checked against the buster and the gc window.
        entry: CacheEntry,
    },
    /// One cache slot's write throttle elapsed.
    CacheFlush {
        /// The slot to write.
        key: Arc<str>,
    },
    /// One cache slot's gc window elapsed after its root was torn down.
    CacheEvict {
        /// The slot to drop.
        key: Arc<str>,
        /// Which arming this fire belongs to; a re-mount invalidates it.
        epoch: u64,
    },
    /// Tear everything down; the socket is closed for good.
    Disconnect {
        /// Resolved once every root is gone and the socket is closed.
        ack: oneshot::Sender<()>,
    },
    /// The last handle went away.
    Shutdown,
}

/// A mount request, carrying the cell the caller already built.
///
/// The cell travels with the request because only the caller knows the
/// [`Store`](crate::generated::Store) type; the actor either adopts it (fresh
/// root) or drops it and returns the existing one (alias).
pub(crate) struct MountRequest {
    /// The store's Elixir module name.
    pub(crate) module: &'static str,
    /// The caller-supplied root id.
    pub(crate) id: String,
    /// The mount params, already validated to be a JSON object.
    pub(crate) params: Value,
    /// The candidate cell, typed by the caller.
    pub(crate) cell: AnyCell,
    /// The same allocation, as the actor's publish target.
    pub(crate) sink: Arc<dyn RootSink>,
    /// Resolved with the root's cell once the initial patch has landed.
    pub(crate) reply: oneshot::Sender<Result<AnyCell>>,
}

/// A command dispatch.
pub(crate) struct CommandRequest {
    /// The root whose channel carries the push.
    pub(crate) root_id: Arc<str>,
    /// The target store, server-authored (`[]` for the root).
    pub(crate) store_id: StoreId,
    /// The declared command name.
    pub(crate) name: &'static str,
    /// The serialized command payload.
    pub(crate) payload: Value,
    /// Resolved with the raw `phx_reply` response.
    pub(crate) reply: oneshot::Sender<Result<Value>>,
}

/// The shared sender behind every handle.
///
/// Dropping the last handle — the [`Connection`](crate::Connection) and every
/// [`Mounted`](crate::Mounted) — shuts the actor down, so a forgotten
/// connection does not keep a socket reconnecting forever.
pub(crate) struct ConnectionInner {
    tx: UnboundedSender<ActorMsg>,
}

impl ConnectionInner {
    /// Wraps the actor's inbox.
    pub(crate) fn new(tx: UnboundedSender<ActorMsg>) -> Self {
        Self { tx }
    }

    /// Enqueues one message; a dead actor means the connection is gone.
    pub(crate) fn send(&self, msg: ActorMsg) -> Result<()> {
        self.tx
            .unbounded_send(msg)
            .map_err(|_| MusubiError::Disconnected)
    }
}

impl Drop for ConnectionInner {
    fn drop(&mut self) {
        let _ = self.tx.unbounded_send(ActorMsg::Shutdown);
    }
}

/// The single owner of the socket and the root registry.
pub(crate) struct Actor {
    socket: PhoenixSocket,
    base_topic: String,
    spawner: Arc<dyn Spawner>,
    timer: Arc<dyn Timer>,
    cache: Option<CacheConfig>,
    tx: UnboundedSender<ActorMsg>,
    rx: UnboundedReceiver<ActorMsg>,
    roots: HashMap<Arc<str>, Root>,
    /// Throttled writes in flight, keyed by cache slot.
    cache_writes: HashMap<Arc<str>, CacheWriter>,
    /// Armed evictions, keyed by cache slot; the value is the only epoch whose
    /// [`ActorMsg::CacheEvict`] still counts.
    cache_evictions: HashMap<Arc<str>, u64>,
    next_eviction_epoch: u64,
    next_command_id: u64,
    closed: bool,
}

/// The connection-wide cache settings (`docs/rust-client.md` §6.4).
#[derive(Clone)]
pub(crate) struct CacheConfig {
    pub(crate) store: Arc<dyn CacheStore>,
    pub(crate) buster: Arc<str>,
    pub(crate) gc_time: Duration,
}

impl CacheConfig {
    fn gc_ms(&self) -> u64 {
        u64::try_from(self.gc_time.as_millis()).unwrap_or(u64::MAX)
    }

    /// Whether an entry has aged out of the gc window, or was written under a
    /// different shape token.
    fn is_usable(&self, entry: &CacheEntry) -> bool {
        entry.buster == *self.buster && now_ms().saturating_sub(entry.updated_at) <= self.gc_ms()
    }
}

/// One cache slot's trailing throttle: the latest tree, and whether a flush is
/// already armed.
#[derive(Default)]
struct CacheWriter {
    pending: Option<CacheEntry>,
    armed: bool,
}

impl Actor {
    /// Builds the actor. [`run`](Self::run) is what the spawner is handed.
    pub(crate) fn new(
        socket: PhoenixSocket,
        base_topic: String,
        spawner: Arc<dyn Spawner>,
        timer: Arc<dyn Timer>,
        cache: Option<CacheConfig>,
        tx: UnboundedSender<ActorMsg>,
        rx: UnboundedReceiver<ActorMsg>,
    ) -> Self {
        Self {
            socket,
            base_topic,
            spawner,
            timer,
            cache,
            tx,
            rx,
            roots: HashMap::new(),
            cache_writes: HashMap::new(),
            cache_evictions: HashMap::new(),
            next_eviction_epoch: 0,
            next_command_id: 0,
            closed: false,
        }
    }

    /// Drains the inbox until the last handle goes away.
    pub(crate) async fn run(mut self) {
        while let Some(msg) = self.rx.next().await {
            if matches!(msg, ActorMsg::Shutdown) {
                break;
            }

            self.handle(msg).await;
        }
    }

    async fn handle(&mut self, msg: ActorMsg) {
        match msg {
            ActorMsg::Mount(request) => self.mount(*request).await,
            ActorMsg::Retain { root_id } => self.retain(&root_id),
            ActorMsg::Release { root_id } => self.release(&root_id),
            ActorMsg::Command(request) => self.command(*request),
            ActorMsg::CommandReply {
                root_id,
                id,
                outcome,
            } => self.command_replied(&root_id, id, outcome),
            ActorMsg::Channel {
                root_id,
                generation,
                event,
            } => self.channel_event(&root_id, generation, event).await,
            ActorMsg::UploadPush {
                root_id,
                event,
                payload,
                reply,
            } => self.upload_push(&root_id, event, payload, reply),
            ActorMsg::CacheSeed {
                root_id,
                key,
                entry,
            } => self.cache_seed(&root_id, &key, entry),
            ActorMsg::CacheFlush { key } => self.cache_flush(&key),
            ActorMsg::CacheEvict { key, epoch } => self.cache_evict(&key, epoch),
            ActorMsg::Disconnect { ack } => self.disconnect(ack).await,
            // Handled by the loop so it can break.
            ActorMsg::Shutdown => {}
        }
    }

    /// Mounts `(module, id)`, or aliases the root that already holds it.
    ///
    /// The registry insert happens **before the first await**, so two
    /// concurrent mounts of one `(module, id)` cannot open two channels on the
    /// same topic (§7).
    async fn mount(&mut self, request: MountRequest) {
        let root_id: Arc<str> = Arc::from(format!("{}:{}", request.module, request.id));

        if self.roots.contains_key(&root_id) {
            self.alias(&root_id, request);
            return;
        }

        if self.closed {
            let _ = request.reply.send(Err(MusubiError::Disconnected));
            return;
        }

        let topic: Arc<str> = Arc::from(format!("{}:{}", self.base_topic, root_id));
        // The slot this mount reads and writes. `None` when the connection has
        // no cache store, which is what makes every cache path below a no-op.
        let key: Option<Arc<str>> = self
            .cache
            .as_ref()
            .map(|_| Arc::from(cache_key(request.module, &request.id, &request.params)));

        self.roots.insert(
            Arc::clone(&root_id),
            Root {
                module: request.module,
                id: request.id,
                topic,
                params: request.params,
                cache_key: key.clone(),
                seeded: false,
                refcount: 1,
                generation: 0,
                channel: None,
                engine: PatchEngine::with_uploads(request.sink.uploads()),
                sink: request.sink,
                cell: request.cell,
                published: false,
                recovering: false,
                pending_mounts: vec![request.reply],
                pending_commands: HashMap::new(),
                pending_dispatches: Vec::new(),
            },
        );

        if let Some(key) = key {
            // A re-mount cancels the eviction its own unmount armed.
            self.cache_evictions.remove(&key);
            // Spawned rather than awaited: the read races the join below, so a
            // slow store delays the seed, never the revalidation (§6.4).
            self.read_cache(&root_id, key);
        }

        self.attach_and_join(&root_id).await;
    }

    /// Adds a second consumer to a live root: first-mount params win, and the
    /// caller only waits when the root has never published state.
    fn alias(&mut self, root_id: &Arc<str>, request: MountRequest) {
        let Some(root) = self.roots.get_mut(root_id) else {
            return;
        };

        root.refcount += 1;

        if root.params != request.params {
            tracing::warn!(
                root_id = %root_id,
                "mount aliased an existing root with different params; first-mount params are \
                 authoritative and the later ones are ignored — use a distinct id for a separate \
                 instance"
            );
        }

        if !root.published {
            root.pending_mounts.push(request.reply);
            return;
        }

        let cell = Arc::clone(&root.cell);

        // The hold was taken above, before the send; a caller whose mount
        // future was already dropped never receives the cell and never builds
        // the `Mounted` that would release it.
        if request.reply.send(Ok(cell)).is_err() {
            self.release(root_id);
        }
    }

    /// Opens a fresh channel incarnation for `root_id` and joins it.
    ///
    /// Used by the first mount and by version-mismatch recovery. The join-ok
    /// event fires on this join and on every transport-level rejoin, which is
    /// the single recovery hook (§9).
    async fn attach_and_join(&mut self, root_id: &Arc<str>) {
        let Some(root) = self.roots.get(root_id) else {
            return;
        };
        let topic = root.topic.to_string();
        let params = json!({"module": root.module, "id": root.id, "params": root.params});

        let attached = self.socket.channel(topic, params).await;

        let Some(root) = self.roots.get_mut(root_id) else {
            return;
        };

        let Ok((channel, events)) = attached else {
            self.fail_join(root_id, || MusubiError::Disconnected);
            return;
        };

        root.generation += 1;
        let generation = root.generation;

        if channel.join().is_err() {
            self.fail_join(root_id, || MusubiError::Disconnected);
            return;
        }

        root.channel = Some(channel);
        self.forward(Arc::clone(root_id), generation, events);
    }

    /// Pumps one channel's events into the inbox, stamped with `generation`.
    fn forward(&self, root_id: Arc<str>, generation: u64, mut events: ChannelEvents) {
        let tx = self.tx.clone();

        self.spawner.spawn(Box::pin(async move {
            while let Some(event) = events.next().await {
                let msg = ActorMsg::Channel {
                    root_id: Arc::clone(&root_id),
                    generation,
                    event,
                };

                if tx.unbounded_send(msg).is_err() {
                    break;
                }
            }
        }));
    }

    fn retain(&mut self, root_id: &Arc<str>) {
        if let Some(root) = self.roots.get_mut(root_id) {
            root.refcount += 1;
        }
    }

    /// Drops one hold on a root; at zero the root is torn down and its channel
    /// left, which stops the server-side root via `terminate/2`.
    fn release(&mut self, root_id: &Arc<str>) {
        let Some(root) = self.roots.get_mut(root_id) else {
            return;
        };

        root.refcount = root.refcount.saturating_sub(1);

        if root.refcount == 0 {
            self.teardown(root_id, || MusubiError::Unmounted);
        }
    }

    /// Dispatches a command, or rejects it outright: there is no queueing and
    /// no retry (§6.2).
    fn command(&mut self, request: CommandRequest) {
        if self.closed {
            let _ = request.reply.send(Err(MusubiError::Disconnected));
            return;
        }

        // The root left the registry: its last handle was dropped, or a failed
        // mount released it.
        let Some(root) = self.roots.get_mut(&request.root_id) else {
            let _ = request.reply.send(Err(MusubiError::Unmounted));
            return;
        };

        // No channel, or `version == 0` (mid-reconnect): a dispatch is either
        // sendable now or rejected — *unless* a cache seed already made this
        // root renderable, in which case the caller is looking at state and the
        // dispatch queues behind the live initial patch (§6.2, §6.4).
        if root.engine.version() == 0 {
            if !root.seeded {
                let _ = request.reply.send(Err(MusubiError::NotConnected));
                return;
            }

            // The queue is a bridge across one revalidation, not a retry
            // buffer: past its bound the honest answer is the same one an
            // unseeded root gives.
            if root.pending_dispatches.len() >= MAX_QUEUED_DISPATCHES {
                let _ = request.reply.send(Err(MusubiError::NotConnected));
                return;
            }

            root.pending_dispatches.push(request);
            return;
        }

        let Some(channel) = root.channel.clone() else {
            let _ = request.reply.send(Err(MusubiError::NotConnected));
            return;
        };

        self.next_command_id += 1;
        let id = self.next_command_id;
        let payload = json!({
            "store_id": request.store_id,
            "name": request.name,
            "payload": request.payload,
        });

        root.pending_commands.insert(
            id,
            PendingCommand {
                name: request.name,
                store_id: request.store_id,
                reply: request.reply,
            },
        );

        let root_id = request.root_id;
        let tx = self.tx.clone();

        self.spawner.spawn(Box::pin(async move {
            let outcome = channel.push(EVENT_COMMAND, payload).await;

            let _ = tx.unbounded_send(ActorMsg::CommandReply {
                root_id,
                id,
                outcome,
            });
        }));
    }

    /// Resolves one command. A command already rejected in bulk is gone from
    /// the map, and its late reply is dropped.
    fn command_replied(
        &mut self,
        root_id: &Arc<str>,
        id: u64,
        outcome: std::result::Result<Reply, PushError>,
    ) {
        let Some(pending) = self
            .roots
            .get_mut(root_id)
            .and_then(|root| root.pending_commands.remove(&id))
        else {
            return;
        };

        let result = match outcome {
            Ok(Reply {
                status: ReplyStatus::Ok,
                response,
            }) => Ok(response),
            Ok(Reply {
                status: ReplyStatus::Error,
                response,
            }) => Err(CommandError::Failed {
                command: pending.name,
                store_id: pending.store_id,
                code: error_code(&response),
                reply: response,
            }
            .into()),
            Err(PushError::Timeout) => Err(CommandError::Timeout {
                command: pending.name,
                store_id: pending.store_id,
            }
            .into()),
            Err(PushError::NotJoined | PushError::Stale) => Err(MusubiError::NotConnected),
            Err(PushError::Disconnected | PushError::SocketClosed(_)) => {
                Err(MusubiError::Disconnected)
            }
            Err(PushError::MalformedReply) => Err(MusubiError::Protocol(
                "command reply was not a phx_reply payload",
            )),
            // `PushError` is `#[non_exhaustive]`; any future variant still
            // means no reply can arrive, which is what `Disconnected` says.
            Err(error) => {
                tracing::warn!(%error, "unrecognized command push failure");
                Err(MusubiError::Disconnected)
            }
        };

        let _ = pending.reply.send(result);
    }

    /// Pushes one upload control-plane event on a root's channel.
    ///
    /// Unlike a command there is no version gate: preflight and cancellation
    /// are about the upload's own state, which the initial patch says nothing
    /// about. A channel that is not joined still rejects the push.
    fn upload_push(
        &mut self,
        root_id: &Arc<str>,
        event: &'static str,
        payload: Value,
        reply: Option<oneshot::Sender<Result<Value>>>,
    ) {
        let channel = self
            .roots
            .get(root_id)
            .ok_or(MusubiError::Unmounted)
            .and_then(|root| root.channel.clone().ok_or(MusubiError::NotConnected));

        let channel = match channel {
            Ok(channel) => channel,
            Err(error) => {
                if let Some(reply) = reply {
                    let _ = reply.send(Err(error));
                }

                return;
            }
        };

        self.spawner.spawn(Box::pin(async move {
            let outcome = channel.push(event, payload).await;

            // Dropped for a detached push, which is what makes it detached.
            let Some(reply) = reply else {
                return;
            };

            let _ = reply.send(match outcome {
                Ok(received) => transfer::upload_reply(event, received),
                Err(error) => Err(transfer::push_error(error)),
            });
        }));
    }

    /// Routes one channel event, dropping anything from a superseded channel
    /// incarnation (§3.2 generation guarding).
    async fn channel_event(&mut self, root_id: &Arc<str>, generation: u64, event: ChannelEvent) {
        let Some(root) = self.roots.get(root_id) else {
            return;
        };

        if root.generation != generation {
            return;
        }

        let topic = Arc::clone(&root.topic);

        match event {
            ChannelEvent::Joined { response } => self.joined(root_id, &response),
            ChannelEvent::JoinError { response } => {
                let reason = join_reason(&response);

                self.fail_join(root_id, || MusubiError::Join {
                    topic: topic.to_string(),
                    reason: reason.clone(),
                });
            }
            ChannelEvent::JoinTimeout => self.fail_join(root_id, || MusubiError::Timeout),
            ChannelEvent::Message { event, payload } if event == EVENT_PATCH => {
                self.patch(root_id, payload).await;
            }
            ChannelEvent::Message { event, .. } => {
                tracing::debug!(%event, "ignoring an unknown channel event");
            }
            ChannelEvent::Close | ChannelEvent::Error { .. } => self.disconnected(root_id),
            // `ChannelEvent` is `#[non_exhaustive]`; a variant this crate does
            // not know cannot carry Musubi state.
            _ => tracing::debug!("ignoring an unrecognized channel event"),
        }
    }

    /// Handles a join ok — the first join **and** every rejoin.
    ///
    /// The server (re)started the page server and will push a fresh initial
    /// patch, so the version goes back to `0` while the last-good tree stays in
    /// place; the `replace ""` then swaps it out atomically (§9).
    fn joined(&mut self, root_id: &Arc<str>, response: &Value) {
        let mismatched = response
            .get("root_id")
            .and_then(Value::as_str)
            .is_some_and(|reply_root_id| reply_root_id != &**root_id);

        if mismatched {
            tracing::error!(expected = %root_id, "join reply carried a different root id");
            self.fail_join(root_id, || {
                MusubiError::Protocol("join reply root_id did not match the mounted root")
            });
            return;
        }

        if let Some(root) = self.roots.get_mut(root_id) {
            root.engine.soft_reset();
        }
    }

    /// A join was rejected, timed out, or could not be sent.
    ///
    /// Pending mounts fail (each releasing the hold it took) and pending
    /// commands are rejected. A **live** root is deliberately kept: a failed
    /// re-join must not blank the consumer, and the transport keeps rejoining
    /// (§9).
    fn fail_join(&mut self, root_id: &Arc<str>, reason: impl Fn() -> MusubiError) {
        if let Some(root) = self.roots.get_mut(root_id) {
            root.recovering = false;
            reject_commands(root, &reason);
        }

        self.fail_pending_mounts(root_id, reason);
    }

    /// Applies one `"patch"` push (§4.3).
    async fn patch(&mut self, root_id: &Arc<str>, payload: Value) {
        let Some(root) = self.roots.get_mut(root_id) else {
            return;
        };
        let awaiting_initial = root.engine.version() == 0;
        // Whether anything is actually waiting on this envelope. Nothing is,
        // after a rejoin of an already-published root — and there the initial
        // check would otherwise reject every later envelope forever.
        let stalled = root.pending_mounts.is_empty();

        let envelope = match PatchEnvelope::decode(payload) {
            Ok(envelope) => envelope,
            Err(error) => {
                self.reject_envelope(root_id, &error, awaiting_initial, stalled)
                    .await;

                return;
            }
        };

        if envelope.root_id != **root_id {
            tracing::warn!(
                expected = %root_id,
                actual = %envelope.root_id,
                "dropping a patch envelope addressed to another root"
            );
            return;
        }

        match root.engine.apply(&envelope) {
            Ok(state) => {
                if let Err(error) = self.publish(root_id, &envelope, &state) {
                    // A silent partial state is worse than a loud stall, so the
                    // envelope fails and the last-good rendering is kept. The
                    // waiting mounts learn it is codegen drift (§11) before the
                    // root goes into recovery.
                    tracing::error!(root_id = %root_id, %error, "root state did not match the generated types");
                    self.fail_mounts_with(root_id, error);
                    self.recover(root_id).await;
                }
            }
            // §4.5: the initial envelope must be `0 -> 1`. Nothing is recovered
            // by rejoining a root that never started, so the mount just fails —
            // unless nothing was waiting, in which case only a rejoin can move
            // the engine off version 0 again.
            Err(MusubiError::Protocol(message)) => {
                tracing::warn!(reason = message, "rejecting the initial patch envelope");
                self.fail_pending_mounts(root_id, || MusubiError::Protocol(message));

                if stalled {
                    self.recover(root_id).await;
                }
            }
            // A version gap or a failed op both mean client and server
            // diverged; the tree is untouched, so recovery keeps rendering it.
            Err(error) => {
                tracing::warn!(%error, "patch envelope rejected; recovering the root");
                self.recover(root_id).await;
            }
        }
    }

    /// Handles an undecodable `"patch"` payload: fail whatever was waiting on
    /// it, and recover when the failure is version-mismatch-class.
    async fn reject_envelope(
        &mut self,
        root_id: &Arc<str>,
        error: &MusubiError,
        awaiting_initial: bool,
        stalled: bool,
    ) {
        tracing::warn!(%error, "rejecting a patch envelope");

        if awaiting_initial {
            // Nothing else will resolve a mount that was waiting for this
            // envelope, so fail it rather than hang.
            self.fail_pending_mounts(root_id, || {
                MusubiError::Protocol("initial patch envelope was rejected")
            });

            if stalled {
                self.recover(root_id).await;
            }
        } else if matches!(error, MusubiError::Patch(_)) {
            // §4.1: an op outside the allowlist is a version-mismatch-class
            // failure. A payload that is not an envelope at all is only
            // dropped, as in the TypeScript client — the next envelope's
            // version gap recovers it.
            self.recover(root_id).await;
        }
    }

    /// Publishes an accepted envelope: state first, then its push events, then
    /// the mounts that were waiting for it (§4.3 steps 6–7).
    ///
    /// Returns [`MusubiError::Decode`] when the tree did not match the
    /// generated types, which is codegen drift (§11).
    fn publish(
        &mut self,
        root_id: &Arc<str>,
        envelope: &PatchEnvelope,
        state: &Value,
    ) -> Result<()> {
        let Some(root) = self.roots.get_mut(root_id) else {
            return Ok(());
        };

        // The root's own subtree is what failed, so the reported store is the
        // root path even when a nested store node is the culprit.
        root.sink
            .publish(state)
            .map_err(|source| MusubiError::Decode {
                store_id: StoreId::root(),
                source,
            })?;

        root.published = true;
        root.recovering = false;

        for event in &envelope.events {
            root.sink
                .dispatch_event(&event.store_id, &event.name, &event.payload);
        }

        self.resolve_mounts(root_id);
        self.flush_dispatches(root_id);
        self.schedule_cache_write(root_id);

        Ok(())
    }

    /// Hands the root's cell to every mount waiting on it.
    ///
    /// A mount whose future was dropped never receives the cell, so the hold it
    /// took at mount time has to be given back here — nothing else will.
    fn resolve_mounts(&mut self, root_id: &Arc<str>) {
        let Some(root) = self.roots.get_mut(root_id) else {
            return;
        };

        let cell = Arc::clone(&root.cell);
        let mut abandoned = 0;

        for reply in root.pending_mounts.drain(..) {
            if reply.send(Ok(Arc::clone(&cell))).is_err() {
                abandoned += 1;
            }
        }

        for _ in 0..abandoned {
            self.release(root_id);
        }
    }

    /// Dispatches everything a cache-seeded root queued, in the order it was
    /// queued (§6.2).
    ///
    /// Called once the live initial patch has been published, so the version
    /// gate each of these re-enters is now open.
    fn flush_dispatches(&mut self, root_id: &Arc<str>) {
        let Some(root) = self.roots.get_mut(root_id) else {
            return;
        };

        root.seeded = false;

        let queued = std::mem::take(&mut root.pending_dispatches);

        for request in queued {
            self.command(request);
        }
    }

    /// Fails every pending mount of one root, handing `error` itself to the
    /// first of them.
    ///
    /// [`MusubiError`] is not `Clone` — `Decode` carries a `serde_json::Error`
    /// — so the remaining mounts get the [`MusubiError::VersionMismatch`] that
    /// describes the recovery which follows.
    fn fail_mounts_with(&mut self, root_id: &Arc<str>, error: MusubiError) {
        let error = Cell::new(Some(error));

        self.fail_pending_mounts(root_id, || {
            error.take().unwrap_or(MusubiError::VersionMismatch)
        });
    }

    /// Version-mismatch recovery on a still-live channel (§9).
    ///
    /// Soft reset — the last-good tree, index and streams keep rendering — then
    /// leave the diverged channel (which stops the server-side root) and join a
    /// fresh one. A failed re-join is **not** fatal: the transport keeps
    /// rejoining and the join-ok hook finishes the recovery.
    async fn recover(&mut self, root_id: &Arc<str>) {
        let Some(root) = self.roots.get_mut(root_id) else {
            return;
        };

        if root.recovering {
            return;
        }

        root.recovering = true;
        root.engine.soft_reset();
        reject_commands(root, &|| MusubiError::VersionMismatch);

        if let Some(channel) = root.channel.take() {
            let _ = channel.leave();
        }

        self.fail_pending_mounts(root_id, || MusubiError::VersionMismatch);

        if self.roots.contains_key(root_id) {
            self.attach_and_join(root_id).await;
        }
    }

    /// Transport drop or server-initiated close for one root's channel.
    ///
    /// The channel stays registered so the socket layer rejoins it; the
    /// last-good state keeps rendering and `version = 0` makes the rejoin's
    /// initial patch swap fresh state in atomically (§9).
    fn disconnected(&mut self, root_id: &Arc<str>) {
        if let Some(root) = self.roots.get_mut(root_id) {
            // Whatever recovery was in flight is over; the rejoin's join-ok
            // hook restarts it.
            root.recovering = false;
            root.engine.soft_reset();
            reject_commands(root, &|| MusubiError::Disconnected);
        }

        self.fail_pending_mounts(root_id, || MusubiError::Disconnected);
    }

    /// Fails every mount waiting on this root, releasing the hold each took.
    ///
    /// A root left with no holder is torn down: nothing must rejoin an orphan.
    fn fail_pending_mounts(&mut self, root_id: &Arc<str>, reason: impl Fn() -> MusubiError) {
        let Some(root) = self.roots.get_mut(root_id) else {
            return;
        };

        let pending = std::mem::take(&mut root.pending_mounts);

        if pending.is_empty() {
            return;
        }

        root.refcount = root.refcount.saturating_sub(pending.len());
        let orphaned = root.refcount == 0;

        for reply in pending {
            let _ = reply.send(Err(reason()));
        }

        if orphaned {
            self.teardown(root_id, reason);
        }
    }

    /// Drops a root from the registry and leaves its channel.
    fn teardown(&mut self, root_id: &Arc<str>, reason: impl Fn() -> MusubiError) {
        let Some(mut root) = self.roots.remove(root_id) else {
            return;
        };

        reject_commands(&mut root, &reason);

        if let Some(key) = root.cache_key.take() {
            self.cache_teardown(key);
        }

        for reply in root.pending_mounts.drain(..) {
            let _ = reply.send(Err(reason()));
        }

        root.sink.clear();

        if let Some(channel) = root.channel.take() {
            let _ = channel.leave();
        }
    }

    /// Closes the connection for good: every root torn down, every pending
    /// caller rejected with [`MusubiError::Disconnected`], socket closed.
    async fn disconnect(&mut self, ack: oneshot::Sender<()>) {
        self.closed = true;

        for root_id in self.roots.keys().cloned().collect::<Vec<_>>() {
            self.teardown(&root_id, || MusubiError::Disconnected);
        }

        let _ = self.socket.disconnect().await;
        let _ = ack.send(());
    }

    // -- Cache (`docs/rust-client.md` §6.4) ---------------------------------

    /// Reads one root's cache slot off the actor task.
    ///
    /// Staleness is decided here rather than in [`cache_seed`](Self::cache_seed)
    /// so that dropping an unusable entry costs the actor nothing, and so the
    /// entry that reaches the actor is already the one it may seed.
    fn read_cache(&self, root_id: &Arc<str>, key: Arc<str>) {
        let Some(cache) = self.cache.clone() else {
            return;
        };
        let root_id = Arc::clone(root_id);
        let tx = self.tx.clone();

        self.spawner.spawn(Box::pin(async move {
            let Some(entry) = cache.store.get(&key).await else {
                return;
            };

            if !cache.is_usable(&entry) {
                cache.store.evict(&key).await;
                return;
            }

            let _ = tx.unbounded_send(ActorMsg::CacheSeed {
                root_id,
                key,
                entry,
            });
        }));
    }

    /// Seeds one root from its cache entry: the shadow document is adopted and
    /// published, and every mount waiting on the root resolves against it —
    /// before the live initial patch, which then swaps the whole tree out
    /// atomically.
    ///
    /// A cache read can suspend past the live initial patch, so a root that has
    /// already published keeps what the server sent; the stale seed is dropped.
    ///
    /// It can also suspend past the *mount* it was issued for — a failed join
    /// tears the root down and the caller re-mounts `(module, id)` with
    /// different params — so a seed whose slot is no longer the root's is
    /// dropped too: it holds another slot's tree.
    fn cache_seed(&mut self, root_id: &Arc<str>, key: &Arc<str>, entry: CacheEntry) {
        let Some(root) = self.roots.get_mut(root_id) else {
            return;
        };

        if root.published || root.engine.version() != 0 {
            return;
        }

        if root.cache_key.as_deref() != Some(key.as_ref()) {
            return;
        }

        let state = root.engine.seed(entry.data);

        if let Err(error) = root.sink.publish(&state) {
            // A tree written by an older build can be a shape this binary no
            // longer deserializes. That is not a protocol failure — the live
            // patch is still coming — so the seed is dropped, the slot is
            // evicted, and the mount goes on waiting for the cold path.
            tracing::warn!(
                root_id = %root_id,
                %error,
                "dropping a cache entry whose tree did not match the generated types"
            );
            root.engine.discard_seed();

            if let Some(key) = root.cache_key.clone() {
                self.evict_now(key);
            }

            return;
        }

        root.published = true;
        root.seeded = true;

        self.resolve_mounts(root_id);
    }

    /// Queues one root's tree for persistence, at most one write per
    /// [`CACHE_WRITE_THROTTLE`] per slot, always the latest tree.
    ///
    /// The *wire* tree is what is stored: seeding is then the same marker
    /// substitution the engine already does, with no second decoding path.
    fn schedule_cache_write(&mut self, root_id: &Arc<str>) {
        let Some(cache) = self.cache.clone() else {
            return;
        };
        let Some(root) = self.roots.get(root_id) else {
            return;
        };
        let Some(key) = root.cache_key.clone() else {
            return;
        };
        let entry = CacheEntry {
            data: root.engine.document().clone(),
            updated_at: now_ms(),
            buster: cache.buster.to_string(),
        };

        let writer = self.cache_writes.entry(Arc::clone(&key)).or_default();

        writer.pending = Some(entry);

        if writer.armed {
            return;
        }

        writer.armed = true;

        let timer = Arc::clone(&self.timer);
        let tx = self.tx.clone();

        self.spawner.spawn(Box::pin(async move {
            timer.sleep(CACHE_WRITE_THROTTLE).await;

            let _ = tx.unbounded_send(ActorMsg::CacheFlush { key });
        }));
    }

    /// Writes one slot's latest tree, ending its throttle window.
    fn cache_flush(&mut self, key: &Arc<str>) {
        let Some(writer) = self.cache_writes.get_mut(key) else {
            return;
        };

        writer.armed = false;

        let Some(entry) = writer.pending.take() else {
            // Nothing accumulated during the window: the slot is idle, so it
            // stops costing a map entry.
            self.cache_writes.remove(key);
            return;
        };

        self.write_now(Arc::clone(key), entry);
    }

    /// Flushes and arms the gc timer for a slot whose root has been torn down.
    ///
    /// The remaining window is measured from the entry's own `updated_at`, so
    /// an entry that was already half-expired when the root unmounted is not
    /// given a fresh full lifetime.
    fn cache_teardown(&mut self, key: Arc<str>) {
        let pending = self
            .cache_writes
            .remove(&key)
            .and_then(|writer| writer.pending);

        let Some(cache) = self.cache.clone() else {
            return;
        };

        // A disconnect keeps whatever was flushed: the entry ages out on its
        // own, and a reconnecting app can seed from it again.
        if self.closed {
            if let Some(entry) = pending {
                self.write_now(key, entry);
            }

            return;
        }

        self.next_eviction_epoch += 1;
        let epoch = self.next_eviction_epoch;

        self.cache_evictions.insert(Arc::clone(&key), epoch);

        let timer = Arc::clone(&self.timer);
        let tx = self.tx.clone();
        let gc_ms = cache.gc_ms();

        self.spawner.spawn(Box::pin(async move {
            if let Some(entry) = pending {
                cache.store.put(&key, entry).await;
            }

            let age = cache
                .store
                .get(&key)
                .await
                .map_or(0, |entry| now_ms().saturating_sub(entry.updated_at));

            timer
                .sleep(Duration::from_millis(gc_ms.saturating_sub(age)))
                .await;

            let _ = tx.unbounded_send(ActorMsg::CacheEvict { key, epoch });
        }));
    }

    /// Drops a slot whose gc window elapsed with no root holding it.
    fn cache_evict(&mut self, key: &Arc<str>, epoch: u64) {
        // A re-mount of the same slot dropped the epoch, which is how it
        // cancels the eviction its own unmount armed.
        if self.cache_evictions.get(key) != Some(&epoch) {
            return;
        }

        self.cache_evictions.remove(key);
        self.evict_now(Arc::clone(key));
    }

    /// Fire-and-forget write. Failures are the store's to swallow (§6.4), so
    /// nothing here can throw into the patch path.
    fn write_now(&self, key: Arc<str>, entry: CacheEntry) {
        let Some(cache) = self.cache.clone() else {
            return;
        };

        self.spawner
            .spawn(Box::pin(async move { cache.store.put(&key, entry).await }));
    }

    /// Fire-and-forget removal.
    fn evict_now(&self, key: Arc<str>) {
        let Some(cache) = self.cache.clone() else {
            return;
        };

        self.spawner
            .spawn(Box::pin(async move { cache.store.evict(&key).await }));
    }
}

/// One mounted root: its channel incarnation, its patch engine, and everything
/// waiting on it.
struct Root {
    module: &'static str,
    id: String,
    /// `"<base_topic>:<root_id>"`, shared so routing one event costs no
    /// allocation.
    topic: Arc<str>,
    params: Value,
    /// The cache slot `(module, id, params)` addresses; `None` when the
    /// connection has no cache store (§6.4).
    cache_key: Option<Arc<str>>,
    /// Whether a cache entry made this root renderable before its live initial
    /// patch. Cleared when that patch lands, and by every bulk rejection.
    seeded: bool,
    /// Live [`Mounted`](crate::Mounted) handles **plus** mounts still awaiting
    /// their initial patch.
    refcount: usize,
    /// Bumped per `attach_and_join`; stamps every forwarded channel event.
    generation: u64,
    channel: Option<Channel>,
    engine: PatchEngine,
    sink: Arc<dyn RootSink>,
    cell: AnyCell,
    /// Whether any state has ever been published. Aliasing mounts only wait
    /// while it is `false`; afterwards the last-good snapshot is good enough.
    published: bool,
    /// Guards re-entry into version-mismatch recovery (§9).
    recovering: bool,
    pending_mounts: Vec<oneshot::Sender<Result<AnyCell>>>,
    pending_commands: HashMap<u64, PendingCommand>,
    /// Dispatches held behind a seeded root's in-flight initial patch (§6.2).
    pending_dispatches: Vec<CommandRequest>,
}

/// A command whose push has not resolved yet.
struct PendingCommand {
    name: &'static str,
    store_id: StoreId,
    reply: oneshot::Sender<Result<Value>>,
}

/// Rejects every in-flight command of one root, and everything a cache seed
/// let it queue.
///
/// The bulk-rejection sets of §6.2: `Disconnected` on channel close/error,
/// `Unmounted` on teardown, `VersionMismatch` on recovery, and the join failure
/// reason on a failed (re)join. Clearing `seeded` with them is what stops the
/// next dispatch from queueing behind a revalidation that is not coming: after
/// any of these the root is back to the plain `NotConnected` contract until a
/// fresh initial patch lands.
fn reject_commands(root: &mut Root, reason: &impl Fn() -> MusubiError) {
    root.seeded = false;

    for (_, pending) in root.pending_commands.drain() {
        let _ = pending.reply.send(Err(reason()));
    }

    for request in root.pending_dispatches.drain(..) {
        let _ = request.reply.send(Err(reason()));
    }
}

/// Reads a command error response's `code`: the first string-valued field among
/// `code`, `error` and `reason` (§6.2).
fn error_code(response: &Value) -> Option<String> {
    CODE_FIELDS
        .iter()
        .find_map(|field| response.get(field).and_then(Value::as_str))
        .map(str::to_owned)
}

/// Reads a join error's reason, falling back to the whole response when the
/// server did not send a `reason` string.
fn join_reason(response: &Value) -> String {
    response
        .get("reason")
        .and_then(Value::as_str)
        .map_or_else(|| response.to_string(), str::to_owned)
}
