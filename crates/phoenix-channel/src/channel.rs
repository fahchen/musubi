//! The per-topic channel handle and its event stream.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures_channel::mpsc::UnboundedReceiver;
use futures_channel::oneshot;
use futures_core::Stream;
use futures_util::StreamExt;
use serde_json::Value;

use crate::error::{PushError, SocketClosed};
use crate::frame::Reply;
use crate::socket::{ActorMsg, SocketInner};

/// Why a channel reported an error rather than a clean close.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelErrorReason {
    /// The underlying socket went away (peer close, IO failure, or a failed
    /// reconnect attempt).
    SocketClosed,
    /// A heartbeat went unanswered for a full interval, so the socket was
    /// declared dead and torn down.
    HeartbeatTimeout,
    /// The server pushed `phx_error` for this topic.
    Server,
}

/// Everything a channel reports to its owner, in arrival order.
///
/// The stream ends when the channel is dropped from the socket's registry —
/// after a deliberate [`Channel::leave`], or when a newer channel is attached
/// to the same topic.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum ChannelEvent {
    /// The join succeeded. **Fires on the first join and on every rejoin**, so
    /// consumers can treat it as the single recovery hook.
    Joined {
        /// The server's join reply body.
        response: Value,
    },
    /// The join was rejected; the server's reason is propagated verbatim.
    JoinError {
        /// The server's error reply body.
        response: Value,
    },
    /// The join did not reply within the configured join timeout.
    JoinTimeout,
    /// A server-initiated message on this topic.
    Message {
        /// The event name.
        event: String,
        /// The event payload.
        payload: Value,
    },
    /// The server closed this channel (`phx_close`) without us asking.
    Close,
    /// The channel lost its footing; a rejoin has been scheduled.
    Error {
        /// What went wrong.
        reason: ChannelErrorReason,
    },
}

/// The receiving half of a channel: a [`Stream`] of [`ChannelEvent`].
///
/// Split from [`Channel`] so that a consumer can hold the stream mutably
/// (in a `select!`) while still pushing through a cloned handle.
#[derive(Debug)]
pub struct ChannelEvents {
    rx: UnboundedReceiver<ChannelEvent>,
}

impl ChannelEvents {
    pub(crate) fn new(rx: UnboundedReceiver<ChannelEvent>) -> Self {
        Self { rx }
    }
}

impl Stream for ChannelEvents {
    type Item = ChannelEvent;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_next_unpin(cx)
    }
}

/// A handle to one topic on a [`PhoenixSocket`](crate::PhoenixSocket).
///
/// Cheap to clone; every clone addresses the same registry entry and is
/// invalidated together when a newer channel takes over the topic.
#[derive(Debug, Clone)]
pub struct Channel {
    topic: Arc<str>,
    generation: u64,
    inner: Arc<SocketInner>,
}

impl Channel {
    pub(crate) fn new(topic: Arc<str>, generation: u64, inner: Arc<SocketInner>) -> Self {
        Self {
            topic,
            generation,
            inner,
        }
    }

    /// The channel's topic.
    ///
    /// ```text
    /// channel.topic() //=> "musubi:connection:MyApp.CartStore:cart"
    /// ```
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// The generation stamped on this handle.
    ///
    /// Bumped every time a channel is attached to the topic; the socket drops
    /// anything arriving from, or destined for, a stale generation.
    ///
    /// ```text
    /// channel.generation() //=> 1
    /// ```
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Joins the channel, opening the socket if it is not open yet.
    ///
    /// The outcome is *not* returned here: it arrives on [`ChannelEvents`] as
    /// [`ChannelEvent::Joined`], [`ChannelEvent::JoinError`] or
    /// [`ChannelEvent::JoinTimeout`], because a rejoin after a reconnect
    /// produces the very same events with no caller to return them to.
    ///
    /// ```text
    /// channel.join()?;
    /// // events.next().await //=> Some(ChannelEvent::Joined { .. })
    /// ```
    pub fn join(&self) -> Result<(), SocketClosed> {
        self.send(ActorMsg::Join {
            topic: Arc::clone(&self.topic),
            generation: self.generation,
        })
    }

    /// Leaves the channel and drops it from the socket's registry.
    ///
    /// The resulting `phx_close` is suppressed, so it neither surfaces as
    /// [`ChannelEvent::Close`] nor triggers a rejoin. The event stream ends
    /// once the server acknowledges the leave.
    ///
    /// ```text
    /// channel.leave()?;
    /// // events.next().await //=> None
    /// ```
    pub fn leave(&self) -> Result<(), SocketClosed> {
        self.send(ActorMsg::Leave {
            topic: Arc::clone(&self.topic),
            generation: self.generation,
        })
    }

    /// Pushes an event and resolves with the server's reply.
    ///
    /// A `status: "error"` reply is an `Ok(Reply)` — the error payload is data,
    /// not a failure of the push. [`PushError`] covers only the cases where no
    /// reply can arrive.
    ///
    /// ```text
    /// let reply = channel.push("command", json!({"name": "add"})).await?;
    /// // reply.is_ok() //=> true
    /// ```
    pub async fn push(&self, event: impl Into<String>, payload: Value) -> Result<Reply, PushError> {
        let (reply_tx, reply_rx) = oneshot::channel();

        self.send(ActorMsg::Push {
            topic: Arc::clone(&self.topic),
            generation: self.generation,
            event: event.into(),
            payload,
            reply: reply_tx,
        })?;

        reply_rx.await.map_err(|_| PushError::from(SocketClosed))?
    }

    fn send(&self, msg: ActorMsg) -> Result<(), SocketClosed> {
        self.inner.tx.unbounded_send(msg).map_err(|_| SocketClosed)
    }
}
