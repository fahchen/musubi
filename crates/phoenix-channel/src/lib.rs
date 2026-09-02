//! A Phoenix Channel client: serializer v2 framing, joins, pushes, heartbeats
//! and reconnect, over a socket the caller supplies.
//!
//! The crate is **not** Musubi-aware and **not** runtime-aware. Everything
//! executor-specific sits behind four seams — [`Socket`], [`Connector`],
//! [`Spawner`] and [`Timer`] — so the same protocol code runs on tokio, on a
//! GUI executor, or on a test's manual pump.
//!
//! # Shape
//!
//! One [`PhoenixSocket`] owns one connection and a registry of at most one
//! [`Channel`] per topic. A single actor task holds the socket, the ref
//! counter and the registry; handles talk to it over an unbounded channel, so
//! there is no shared mutable state and inbound ordering (reply before push)
//! is observable exactly as it arrived.
//!
//! ```text
//! let socket = PhoenixSocket::builder()
//!     .url("wss://example.test/socket")
//!     .connector(connector)
//!     .spawner(spawner)
//!     .timer(timer)
//!     .build()?;
//!
//! let (channel, mut events) = socket.channel("room:lobby", json!({})).await?;
//! channel.join()?;
//!
//! while let Some(event) = events.next().await {
//!     // ChannelEvent::Joined arrives here — on the first join and on every
//!     // rejoin after a reconnect.
//! }
//! ```
//!
//! # Recovery
//!
//! The socket reconnects on its own with the `phoenix.js` backoff ladder plus
//! jitter, and rejoins every registered channel afterwards. A missed heartbeat
//! reply within one interval declares the socket dead and starts that cycle.
//! A deliberate [`Channel::leave`] suppresses the resulting `phx_close`, so it
//! neither surfaces as an event nor triggers a rejoin.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod backoff;
mod channel;
mod error;
mod frame;
mod seams;
mod socket;
mod url;

pub use crate::channel::{Channel, ChannelErrorReason, ChannelEvent, ChannelEvents};
pub use crate::error::{BuildError, PushError, SocketClosed, TransportError};
pub use crate::frame::{Frame, Message, Reply, ReplyStatus};
pub use crate::seams::{Connector, Socket, Spawner, Timer};
pub use crate::socket::{PhoenixSocket, SocketBuilder};
