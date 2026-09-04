//! The connection handle and its builder (`docs/rust-client.md` §7).
//!
//! [`Connection`] is a cheap `Clone` over the actor's inbox; all the state
//! lives on the actor task (§2.4). Three of [`phoenix_channel`]'s four runtime
//! seams are supplied here — [`Connector`], [`Spawner`] and [`Timer`] — and
//! shared with the socket underneath, which is what builds the fourth: a
//! [`Socket`](phoenix_channel::Socket) is what the connector returns, never
//! something the embedder hands to this builder.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures_channel::mpsc;
use futures_channel::oneshot;
use phoenix_channel::{Connector, PhoenixSocket, Spawner, Timer};
use serde::Serialize;
use serde_json::Value;

use crate::actor::{Actor, ActorMsg, CacheConfig, ConnectionInner, MountRequest};
use crate::cache::{CacheStore, DEFAULT_CACHE_GC_TIME};
use crate::error::{MusubiError, Result};
use crate::generated::Store;
use crate::mounted::{Mounted, RootCell};
use crate::uploads::{UploadControl, Uploader};

/// A seam missing from [`Connection::builder`].
///
/// The builder has no other failure mode: the socket opens lazily, so nothing
/// is attempted at build time.
pub use phoenix_channel::BuildError;

/// The channel topic every root's topic is prefixed with.
const DEFAULT_TOPIC: &str = "musubi:connection";

/// A Musubi connection: one socket, many mounted roots.
///
/// The socket opens lazily on the first [`mount`](Self::mount) and reconnects
/// on its own; cloning is cheap and every clone addresses the same actor.
///
/// ```text
/// let connection = Connection::builder()
///     .url("wss://example.test/socket")
///     .connector(TungsteniteConnector::default())
///     .spawner(TokioSpawner)
///     .timer(TokioTimer)
///     .build()?;
///
/// let cart: Mounted<CartStore> = connection.mount("cart:page", Params {}).await?;
/// ```
#[derive(Clone)]
pub struct Connection {
    inner: Arc<ConnectionInner>,
    /// Shared with the actor: upload chunk sub-channels are opened straight on
    /// it, since they are per-entry and outlive nothing (§10.2).
    socket: PhoenixSocket,
    /// The external-uploader registry, keyed by the name the server chooses
    /// (BDR-0027).
    uploaders: Arc<HashMap<String, Arc<dyn Uploader>>>,
}

impl Connection {
    /// Starts a builder. `url`, `connector`, `spawner` and `timer` are
    /// required.
    ///
    /// ```text
    /// let connection = Connection::builder()
    ///     .url("wss://example.test/socket")
    ///     .connector(connector)
    ///     .spawner(spawner)
    ///     .timer(timer)
    ///     .build()?;
    /// ```
    pub fn builder() -> ConnectionBuilder {
        ConnectionBuilder::default()
    }

    /// Mounts a root store and resolves once its initial patch has been
    /// applied.
    ///
    /// `params` is the **mount** params object — the channel join payload's
    /// `params` key, not the socket connect params. It is the store's generated
    /// [`Store::Params`] struct: one field per `attr/3` declaration, so a
    /// required attr cannot be forgotten at the call site.
    ///
    /// Mounting the same `(module, id)` twice aliases one root: the second call
    /// returns a second handle over the same channel, and the **first** mount's
    /// params win (the later ones are ignored, with a warning).
    ///
    /// Unmounting is [`Drop`]: when the last [`Mounted`] clone goes away the
    /// channel is left and the server stops the root.
    ///
    /// ```text
    /// let room: Mounted<ChatRoomStore> =
    ///     connection.mount(ROOM_ID, Params { room_id: ROOM_ID.into() }).await?;
    /// ```
    pub async fn mount<St: Store>(&self, id: &str, params: St::Params) -> Result<Mounted<St>> {
        self.mount_with_params::<St>(id, params).await
    }

    /// Mounts a root store with an arbitrary params object.
    ///
    /// [`mount`](Self::mount) is the ergonomic path; this is the escape hatch.
    /// The generated [`Store::Params`] is built from the store's `attr/3`
    /// declarations, but `attr/3` is the *child-store assign* contract: the
    /// page server hands the join payload's `params` map to `mount/2`
    /// unvalidated, so a root whose `mount/2` reads a key it never declared as
    /// an attr is legal — and unreachable through the typed struct.
    ///
    /// The params still have to serialize to a JSON object, exactly as the
    /// join payload requires.
    ///
    /// ```text
    /// let room: Mounted<ChatRoomStore> = connection
    ///     .mount_with_params(ROOM_ID, json!({"room_id": ROOM_ID, "invite": token}))
    ///     .await?;
    /// ```
    pub async fn mount_with_params<St: Store>(
        &self,
        id: &str,
        params: impl Serialize,
    ) -> Result<Mounted<St>> {
        if id.is_empty() {
            return Err(MusubiError::Protocol("mount id must not be empty"));
        }

        // The server's join payload requires `params` to be a map, and nothing
        // in `Serialize` says it will be one — neither a hand-written
        // `Store::Params` (the trait is not sealed) nor a caller's own value.
        let params = serde_json::to_value(params)
            .ok()
            .filter(Value::is_object)
            .ok_or(MusubiError::Protocol(
                "mount params must serialize to a JSON object",
            ))?;

        // Deterministic from `(module, id)`, so the handles' control plane can
        // be wired before the actor has even seen the mount.
        let root_id: Arc<str> = Arc::from(format!("{}:{id}", St::MODULE));
        let control = Arc::new(UploadControl::new(
            Arc::clone(&self.inner),
            Arc::clone(&root_id),
            self.socket.clone(),
            Arc::clone(&self.uploaders),
        ));

        let cell = Arc::new(RootCell::<St>::new(control));
        let (reply_tx, reply_rx) = oneshot::channel();

        self.inner.send(ActorMsg::Mount(Box::new(MountRequest {
            module: St::MODULE,
            id: id.to_owned(),
            params,
            cell: Arc::clone(&cell) as Arc<_>,
            sink: cell,
            reply: reply_tx,
        })))?;

        let cell = reply_rx.await.map_err(|_| MusubiError::Disconnected)??;

        // The registry is keyed by `"<module>:<id>"` and `MODULE` comes from
        // `St`, so the downcast only fails if two generated store markers claim
        // the same Elixir module. The actor already counted this caller's hold,
        // and no `Mounted` will exist to drop it, so it goes back here.
        let cell = cell.downcast::<RootCell<St>>().map_err(|_| {
            let _ = self.inner.send(ActorMsg::Release {
                root_id: Arc::clone(&root_id),
            });

            MusubiError::Protocol("another store type is already mounted under this module and id")
        })?;

        Ok(Mounted::new(Arc::clone(&self.inner), cell, root_id))
    }

    /// Closes the connection for good: every root is torn down, every pending
    /// caller is rejected with [`MusubiError::Disconnected`], and the socket is
    /// closed without reconnecting.
    ///
    /// ```text
    /// connection.disconnect().await?;
    /// ```
    pub async fn disconnect(self) -> Result<()> {
        let (ack_tx, ack_rx) = oneshot::channel();

        self.inner.send(ActorMsg::Disconnect { ack: ack_tx })?;

        ack_rx.await.map_err(|_| MusubiError::Disconnected)
    }
}

/// Builder for [`Connection`].
#[derive(Default)]
pub struct ConnectionBuilder {
    uploaders: HashMap<String, Arc<dyn Uploader>>,
    cache: Option<Arc<dyn CacheStore>>,
    cache_buster: Option<String>,
    cache_gc_time: Option<Duration>,
    url: Option<String>,
    connector: Option<Arc<dyn Connector>>,
    spawner: Option<Arc<dyn Spawner>>,
    timer: Option<Arc<dyn Timer>>,
    topic: Option<String>,
    heartbeat: Option<Duration>,
    join_timeout: Option<Duration>,
    push_timeout: Option<Duration>,
}

impl ConnectionBuilder {
    /// The endpoint base, e.g. `wss://example.test/socket`. Required.
    ///
    /// `/websocket` and `vsn=2.0.0` are appended by the transport layer.
    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
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

    /// The base channel topic every root is mounted under. Default
    /// `"musubi:connection"`.
    pub fn topic(mut self, topic: impl Into<String>) -> Self {
        self.topic = Some(topic.into());
        self
    }

    /// Heartbeat interval; also the dead-socket detection window. Default 30s.
    pub fn heartbeat(mut self, interval: Duration) -> Self {
        self.heartbeat = Some(interval);
        self
    }

    /// How long a channel join may go unanswered. Default 10s.
    pub fn join_timeout(mut self, timeout: Duration) -> Self {
        self.join_timeout = Some(timeout);
        self
    }

    /// How long a command push may go unanswered. Default 10s.
    pub fn push_timeout(mut self, timeout: Duration) -> Self {
        self.push_timeout = Some(timeout);
        self
    }

    /// Registers an external uploader under the name the server's
    /// `upload_external/3` returns (BDR-0027).
    ///
    /// Dispatch is by that name, so a store choosing an uploader this
    /// connection never registered fails the entry with
    /// [`TransferError::NoUploader`](crate::TransferError::NoUploader) rather
    /// than silently falling back to channel mode. Registering the same name
    /// twice keeps the last one.
    ///
    /// ```text
    /// let connection = Connection::builder()
    ///     .url(url)
    ///     .connector(connector)
    ///     .spawner(spawner)
    ///     .timer(timer)
    ///     .uploader("S3", S3Uploader::new(client))
    ///     .build()?;
    /// ```
    pub fn uploader(mut self, name: impl Into<String>, uploader: impl Uploader) -> Self {
        self.uploaders.insert(name.into(), Arc::new(uploader));
        self
    }

    /// Enables stale-while-revalidate mounts against `store`
    /// (`docs/rust-client.md` §6.4).
    ///
    /// With a store set, [`mount`](Connection::mount) publishes the last-known
    /// tree for `(module, id, params)` as soon as it is read and resolves
    /// against it, while the live join revalidates in the background; the real
    /// initial patch then replaces the seed in one whole-root op. A mount with
    /// no entry — or with a stale one — behaves exactly as it does without a
    /// cache.
    ///
    /// The setting is connection-wide, unlike the TypeScript client's per-mount
    /// `cache` option: every root of this connection uses it.
    ///
    /// ```text
    /// let connection = Connection::builder()
    ///     .url(url)
    ///     .connector(connector)
    ///     .spawner(spawner)
    ///     .timer(timer)
    ///     .cache(MemoryCacheStore::new())
    ///     .cache_buster(env!("CARGO_PKG_VERSION"))
    ///     .build()?;
    /// ```
    pub fn cache(mut self, store: impl CacheStore) -> Self {
        self.cache = Some(Arc::new(store));
        self
    }

    /// The shape token cached trees are written under. Default `""`.
    ///
    /// An entry whose buster does not match the current one is discarded rather
    /// than seeded, which is how a build whose state shape changed avoids
    /// rendering a tree it can no longer read. Set it to the build or schema
    /// version whenever the store is durable; with the in-process
    /// [`MemoryCacheStore`](crate::MemoryCacheStore) nothing outlives the
    /// binary, so it can be left unset.
    pub fn cache_buster(mut self, buster: impl Into<String>) -> Self {
        self.cache_buster = Some(buster.into());
        self
    }

    /// How long a cached tree stays seedable after its last write. Default 5
    /// minutes, matching `packages/client/src/cache.ts`.
    ///
    /// It is also the eviction window: a slot whose root unmounts is dropped
    /// once the remainder of this elapses, unless the same slot is mounted
    /// again first.
    pub fn cache_gc_time(mut self, gc_time: Duration) -> Self {
        self.cache_gc_time = Some(gc_time);
        self
    }

    /// Spawns the actor; the socket opens lazily on first use, so the only
    /// build-time error is a missing required seam.
    ///
    /// ```text
    /// let connection = builder.build()?;
    /// ```
    pub fn build(self) -> Result<Connection, BuildError> {
        let url = self.url.ok_or(BuildError::MissingUrl)?;
        let connector = self.connector.ok_or(BuildError::MissingConnector)?;
        let spawner = self.spawner.ok_or(BuildError::MissingSpawner)?;
        let timer = self.timer.ok_or(BuildError::MissingTimer)?;

        let mut socket = PhoenixSocket::builder()
            .url(url)
            .connector(connector)
            .spawner(Arc::clone(&spawner))
            .timer(Arc::clone(&timer));

        if let Some(heartbeat) = self.heartbeat {
            socket = socket.heartbeat(heartbeat);
        }
        if let Some(join_timeout) = self.join_timeout {
            socket = socket.join_timeout(join_timeout);
        }
        if let Some(push_timeout) = self.push_timeout {
            socket = socket.push_timeout(push_timeout);
        }

        let socket = socket.build()?;
        let (tx, rx) = mpsc::unbounded();
        let cache = self.cache.map(|store| CacheConfig {
            store,
            buster: Arc::from(self.cache_buster.unwrap_or_default()),
            gc_time: self.cache_gc_time.unwrap_or(DEFAULT_CACHE_GC_TIME),
        });
        let actor = Actor::new(
            socket.clone(),
            self.topic.unwrap_or_else(|| DEFAULT_TOPIC.to_owned()),
            Arc::clone(&spawner),
            timer,
            cache,
            tx.clone(),
            rx,
        );

        spawner.spawn(Box::pin(actor.run()));

        Ok(Connection {
            inner: Arc::new(ConnectionInner::new(tx)),
            socket,
            uploaders: Arc::new(self.uploaders),
        })
    }
}
