//! The four seams the crate is generic over: socket, connector, spawner, timer.
//!
//! Everything runtime-specific lives behind these traits, so the crate itself
//! depends on no executor. `futures`' [`Sink`]/[`Stream`] are used instead of an
//! `async_trait` transport: no per-frame boxed future, no `async-trait`
//! dependency in the public API.

use std::sync::Arc;
use std::time::Duration;

use futures_core::Stream;
use futures_core::future::BoxFuture;
use futures_sink::Sink;

use crate::error::TransportError;
use crate::frame::Frame;

/// A connected socket: a [`Sink`] of outbound frames and a [`Stream`] of
/// inbound frames.
///
/// The blanket impl below means transports never name this trait — anything
/// that is both halves is a `Socket`.
pub trait Socket:
    Sink<Frame, Error = TransportError>
    + Stream<Item = Result<Frame, TransportError>>
    + Send
    + Unpin
    + 'static
{
}

impl<T> Socket for T where
    T: Sink<Frame, Error = TransportError>
        + Stream<Item = Result<Frame, TransportError>>
        + Send
        + Unpin
        + 'static
{
}

/// How to (re)open a socket.
///
/// Called once per connect and once per reconnect attempt; the crate owns
/// backoff, so an implementation should attempt exactly one connection and
/// report the outcome.
pub trait Connector: Send + Sync + 'static {
    /// Opens one socket against the fully-built endpoint URL (`/websocket`,
    /// `vsn=2.0.0` and connect params already appended).
    fn connect(&self, url: &str) -> BoxFuture<'static, Result<Box<dyn Socket>, TransportError>>;
}

/// Detached task spawning.
///
/// `gpui::BackgroundExecutor`, `tokio::spawn`, `async_std::task::spawn` or a
/// test's manual pump all satisfy this.
pub trait Spawner: Send + Sync + 'static {
    /// Runs `fut` to completion somewhere; the returned handle, if any, is
    /// dropped by the implementation.
    fn spawn(&self, fut: BoxFuture<'static, ()>);
}

/// Time.
///
/// Needed for heartbeats, join/push timeouts and reconnect backoff. Injectable
/// so tests are deterministic.
pub trait Timer: Send + Sync + 'static {
    /// Resolves after (at least) `dur`.
    fn sleep(&self, dur: Duration) -> BoxFuture<'static, ()>;
}

// The builders below take each seam by value, so an embedder sharing one seam
// between layers hands them an `Arc` instead.

impl<T: Connector + ?Sized> Connector for Arc<T> {
    fn connect(&self, url: &str) -> BoxFuture<'static, Result<Box<dyn Socket>, TransportError>> {
        (**self).connect(url)
    }
}

impl<T: Spawner + ?Sized> Spawner for Arc<T> {
    fn spawn(&self, fut: BoxFuture<'static, ()>) {
        (**self).spawn(fut);
    }
}

impl<T: Timer + ?Sized> Timer for Arc<T> {
    fn sleep(&self, dur: Duration) -> BoxFuture<'static, ()> {
        (**self).sleep(dur)
    }
}
