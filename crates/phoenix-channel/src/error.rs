//! Error types for the transport seam, the socket handle, and pushes.

use thiserror::Error;

use crate::frame::BinaryFrameError;

/// A failure at the socket/IO level.
///
/// This is the error type of the [`Socket`](crate::Socket) seam: transport
/// implementations map their own failures onto it, and the crate never
/// inspects the payload beyond logging it.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum TransportError {
    /// The [`Connector`](crate::Connector) could not open a socket.
    #[error("connect failed: {reason}")]
    Connect {
        /// Transport-supplied description of the failure.
        reason: String,
    },
    /// The socket was closed by the peer or by the transport.
    #[error("socket closed")]
    Closed,
    /// A read or write failed on an otherwise open socket.
    #[error("socket io failed: {reason}")]
    Io {
        /// Transport-supplied description of the failure.
        reason: String,
    },
}

impl TransportError {
    /// Builds a [`TransportError::Connect`] from anything printable.
    ///
    /// ```
    /// use phoenix_channel::TransportError;
    ///
    /// let err = TransportError::connect("dns failure");
    /// assert_eq!(err.to_string(), "connect failed: dns failure");
    /// ```
    pub fn connect(reason: impl std::fmt::Display) -> Self {
        Self::Connect {
            reason: reason.to_string(),
        }
    }

    /// Builds a [`TransportError::Io`] from anything printable.
    ///
    /// ```
    /// use phoenix_channel::TransportError;
    ///
    /// let err = TransportError::io("broken pipe");
    /// assert_eq!(err.to_string(), "socket io failed: broken pipe");
    /// ```
    pub fn io(reason: impl std::fmt::Display) -> Self {
        Self::Io {
            reason: reason.to_string(),
        }
    }
}

/// A seam missing from [`PhoenixSocket::builder`](crate::PhoenixSocket::builder).
///
/// The builder has no other failure mode: the socket opens lazily, so nothing
/// is attempted at build time.
#[derive(Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum BuildError {
    /// `url` was never set.
    #[error("socket url is required")]
    MissingUrl,
    /// `connector` was never set.
    #[error("connector is required")]
    MissingConnector,
    /// `spawner` was never set.
    #[error("spawner is required")]
    MissingSpawner,
    /// `timer` was never set.
    #[error("timer is required")]
    MissingTimer,
}

/// The socket actor is no longer running, so the request went nowhere.
///
/// Returned by the handle methods that only enqueue work.
#[derive(Debug, Error, PartialEq, Eq)]
#[error("socket actor stopped")]
pub struct SocketClosed;

/// Why a push (or a join/leave, which are pushes) did not produce a reply.
///
/// A server reply with `status: "error"` is *not* one of these: it is a
/// [`Reply`](crate::Reply) with [`ReplyStatus::Error`](crate::ReplyStatus), so
/// callers can read the error payload.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum PushError {
    /// The channel had not completed a join when the push was issued.
    #[error("channel is not joined")]
    NotJoined,
    /// A newer channel was attached to the same topic; this handle is stale.
    #[error("channel handle was superseded by a newer channel on the same topic")]
    Stale,
    /// The socket went away (or [`disconnect`](crate::PhoenixSocket::disconnect)
    /// was called) while the push was in flight.
    #[error("socket disconnected while the push was in flight")]
    Disconnected,
    /// No reply arrived before the configured push timeout.
    #[error("push timed out")]
    Timeout,
    /// The server replied with something that is not a `phx_reply` payload.
    #[error("malformed reply payload")]
    MalformedReply,
    /// The socket actor is no longer running.
    #[error(transparent)]
    SocketClosed(#[from] SocketClosed),
    /// A binary push could not be framed; see
    /// [`BinaryPush`](crate::BinaryPush).
    #[error(transparent)]
    Unframable(#[from] BinaryFrameError),
}
