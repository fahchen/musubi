//! The connection handle and its builder (`docs/rust-client.md` §7).
//!
//! [`Connection`] is a cheap `Clone` over the actor's inbox; all the state
//! lives on the actor task (§2.4). The four runtime seams are supplied here and
//! shared with the [`phoenix_channel`] socket underneath.

use std::sync::Arc;
use std::time::Duration;

use futures_channel::mpsc;
use futures_channel::oneshot;
use phoenix_channel::{Connector, PhoenixSocket, Spawner, Timer};
use serde::Serialize;
use serde_json::Value;

use crate::actor::{Actor, ActorMsg, ConnectionInner, MountRequest};
use crate::error::{MusubiError, Result};
use crate::generated::Store;
use crate::mounted::{Mounted, RootCell};

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
/// let cart: Mounted<CartStore> = connection.mount("cart:page", json!({})).await?;
/// ```
#[derive(Clone)]
pub struct Connection {
    inner: Arc<ConnectionInner>,
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
    /// `params` key, not the socket connect params — and must serialize to a
    /// JSON object. It is untyped because `attr/3` declarations are not carried
    /// by the shared codegen manifest; a store that declares a required attr
    /// still needs it here.
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
    ///     connection.mount("lobby", json!({"room_id": "lobby"})).await?;
    /// ```
    pub async fn mount<St: Store>(&self, id: &str, params: impl Serialize) -> Result<Mounted<St>> {
        if id.is_empty() {
            return Err(MusubiError::Protocol("mount id must not be empty"));
        }

        let params = serde_json::to_value(params)
            .ok()
            .filter(Value::is_object)
            .ok_or(MusubiError::Protocol(
                "mount params must serialize to a JSON object",
            ))?;

        let cell = Arc::new(RootCell::<St>::new());
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
        let root_id: Arc<str> = Arc::from(format!("{}:{id}", St::MODULE));

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
            .timer(timer);

        if let Some(heartbeat) = self.heartbeat {
            socket = socket.heartbeat(heartbeat);
        }
        if let Some(join_timeout) = self.join_timeout {
            socket = socket.join_timeout(join_timeout);
        }
        if let Some(push_timeout) = self.push_timeout {
            socket = socket.push_timeout(push_timeout);
        }

        let (tx, rx) = mpsc::unbounded();
        let actor = Actor::new(
            socket.build()?,
            self.topic.unwrap_or_else(|| DEFAULT_TOPIC.to_owned()),
            Arc::clone(&spawner),
            tx.clone(),
            rx,
        );

        spawner.spawn(Box::pin(actor.run()));

        Ok(Connection {
            inner: Arc::new(ConnectionInner::new(tx)),
        })
    }
}
