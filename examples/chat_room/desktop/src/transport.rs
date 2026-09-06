//! The reference gpui transport adapter (`docs/rust-gpui-example.md` §5).
//!
//! `musubi-client` ships no gpui crate on purpose (`docs/rust-client.md` §2.3):
//! gpui is pre-1.0 with an unpublished ABI, and the adapter is small enough
//! that vendoring it into the workspace would buy nothing. This file is the
//! only copy, and it is written to be **copied verbatim** into other gpui
//! embedders — hence the comments on the parts that are not obvious.
//!
//! Three of the four seams are satisfied here without a second thread pool:
//!
//! | Seam | Implementation |
//! | :-- | :-- |
//! | [`Spawner`] | [`GpuiSpawner`] — `gpui::BackgroundExecutor::spawn` |
//! | [`Timer`] | [`GpuiTimer`] — `gpui::BackgroundExecutor::timer` |
//! | [`Connector`] | [`SmolConnector`] — `async-net` + `async-tungstenite` |
//! | [`Socket`] | [`WsSocket`] — blanket impl over the `Sink`/`Stream` pair |
//!
//! gpui runs `smol` + `async-task` on top of its platform dispatcher and hosts
//! no tokio reactor, so the transport is deliberately smol-family:
//! `async-net` is `async-io`-backed, which means the futures below are driven
//! by whatever executor polls them — gpui's background executor — and
//! `async-tungstenite` needs no runtime feature flag.
//!
//! The example dials plain `ws://`, so **this connector links no TLS stack**:
//! no rustls, no native-tls and no certificate verifier is reachable from the
//! Musubi path, and [`authority`] rejects `wss://` rather than downgrading it
//! silently. That is a statement about the transport, not about the binary —
//! gpui's own HTTP client pulls rustls in through `gpui_http_client`, and this
//! file never touches it. A production client adds
//! `async-tls`/`async-native-tls` in [`SmolConnector::connect`] and nowhere
//! else.

use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use async_tungstenite::WebSocketStream;
use async_tungstenite::tungstenite::{Error as WsError, Message};
use futures::future::BoxFuture;
use futures::{FutureExt, Sink, Stream};
use gpui::BackgroundExecutor;
// The seams are re-exported by `musubi_client`, so an embedder never adds a
// direct dependency on `phoenix-channel`.
use musubi_client::{Connector, Frame, Socket, Spawner, Timer, TransportError};

/// The port a scheme-only `ws://` URL implies.
const DEFAULT_WS_PORT: u16 = 80;

/// [`Spawner`] over `gpui::BackgroundExecutor`.
///
/// Build it from `cx.background_executor().clone()`. The connection actor's
/// future is `Send + 'static` by design, so plain background-thread semantics
/// apply; `.detach()` is correct because the actor is torn down by
/// `Connection::disconnect`, not by dropping a `Task`.
#[derive(Clone)]
pub struct GpuiSpawner(pub BackgroundExecutor);

impl Spawner for GpuiSpawner {
    fn spawn(&self, fut: BoxFuture<'static, ()>) {
        self.0.spawn(fut).detach();
    }
}

/// [`Timer`] over `gpui::BackgroundExecutor::timer`.
///
/// Covers the heartbeat interval, join/push timeouts and the reconnect backoff
/// ladder. No `tokio::time`, and no `smol::Timer` of our own — gpui already
/// owns a timer wheel on its dispatcher.
#[derive(Clone)]
pub struct GpuiTimer(pub BackgroundExecutor);

impl Timer for GpuiTimer {
    fn sleep(&self, dur: Duration) -> BoxFuture<'static, ()> {
        // `timer` returns a `Task<()>`, which is itself a future; boxing it is
        // the whole implementation.
        self.0.timer(dur).boxed()
    }
}

/// [`Connector`] over `async-net` + `async-tungstenite`.
///
/// The URL handed to [`Connector::connect`] is already complete — the socket
/// layer appended `/websocket`, `vsn=2.0.0` and any connect params — so it is
/// passed to the handshake verbatim. One call attempts exactly one connection;
/// backoff and retries belong to `musubi-client`.
#[derive(Debug, Default, Clone, Copy)]
pub struct SmolConnector;

impl Connector for SmolConnector {
    fn connect(&self, url: &str) -> BoxFuture<'static, Result<Box<dyn Socket>, TransportError>> {
        let url = url.to_owned();

        async move {
            let addr = authority(&url)?;

            // `async-net` resolves the authority on the blocking pool and
            // registers the socket with `async-io`'s reactor; nothing here is
            // bound to a particular executor.
            let tcp = async_net::TcpStream::connect(addr.as_str())
                .await
                .map_err(TransportError::connect)?;

            // `async-tungstenite` has no URL parser of its own: it reads the
            // host and path straight out of the string for the HTTP upgrade
            // request, which is why `authority` above only has to produce the
            // TCP target.
            let (stream, _response) = async_tungstenite::client_async(&url, tcp)
                .await
                .map_err(TransportError::connect)?;

            Ok(Box::new(WsSocket::new(stream)) as Box<dyn Socket>)
        }
        .boxed()
    }
}

/// Maps tungstenite messages onto [`Frame`]s in both directions.
///
/// `Socket` itself is never named by a transport: the blanket impl in
/// `phoenix-channel` covers anything that is both a `Sink<Frame>` and a
/// `Stream<Item = Result<Frame, _>>`, so implementing those two is enough.
pub struct WsSocket {
    inner: WebSocketStream<async_net::TcpStream>,
    /// Sticky once the peer's close is observed: a `Close` frame ends the
    /// stream, and anything the peer sends after it is a protocol violation.
    closed: bool,
}

impl WsSocket {
    /// Wraps one connected websocket.
    fn new(inner: WebSocketStream<async_net::TcpStream>) -> Self {
        Self {
            inner,
            closed: false,
        }
    }
}

impl Stream for WsSocket {
    type Item = Result<Frame, TransportError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            if this.closed {
                return Poll::Ready(None);
            }

            match ready!(Pin::new(&mut this.inner).poll_next(cx)) {
                // `Utf8Bytes`/`Bytes` are tungstenite's zero-copy payload
                // types; `Frame` is owned, so both convert on the way in.
                Some(Ok(Message::Text(text))) => {
                    return Poll::Ready(Some(Ok(Frame::Text(text.to_string()))));
                }
                Some(Ok(Message::Binary(bytes))) => {
                    return Poll::Ready(Some(Ok(Frame::Binary(bytes.to_vec()))));
                }
                // A close frame is end-of-stream. Reconnecting is the socket
                // layer's job, not the transport's.
                Some(Ok(Message::Close(_))) | None => {
                    this.closed = true;

                    return Poll::Ready(None);
                }
                // Pings are answered by tungstenite itself, and `Message::Frame`
                // is never produced while reading. None of the three carries a
                // Phoenix message, so keep polling for one that does.
                Some(Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_))) => continue,
                Some(Err(error)) => {
                    let error = map_error(error);
                    this.closed = matches!(error, TransportError::Closed);

                    return Poll::Ready(Some(Err(error)));
                }
            }
        }
    }
}

impl Sink<Frame> for WsSocket {
    type Error = TransportError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.get_mut().inner)
            .poll_ready(cx)
            .map_err(map_error)
    }

    fn start_send(self: Pin<&mut Self>, frame: Frame) -> Result<(), Self::Error> {
        let message = match frame {
            Frame::Text(text) => Message::Text(text.into()),
            Frame::Binary(bytes) => Message::Binary(bytes.into()),
        };

        Pin::new(&mut self.get_mut().inner)
            .start_send(message)
            .map_err(map_error)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.get_mut().inner)
            .poll_flush(cx)
            .map_err(map_error)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.get_mut().inner)
            .poll_close(cx)
            .map_err(map_error)
    }
}

/// Maps a tungstenite failure onto the transport seam's error type.
///
/// Both "closed" variants collapse onto [`TransportError::Closed`]; everything
/// else (IO, protocol, capacity) is a [`TransportError::Io`] carrying the
/// rendered cause, which is all the socket layer ever does with it.
fn map_error(error: WsError) -> TransportError {
    match error {
        WsError::ConnectionClosed | WsError::AlreadyClosed => TransportError::Closed,
        other => TransportError::io(other),
    }
}

/// Extracts the TCP target (`host:port`) from a `ws://host[:port]/path` URL.
///
/// Fifteen lines instead of a `url` dependency, because the only URL this ever
/// sees is the one `phoenix-channel` built from the base the app configured.
/// `wss://` is rejected rather than silently downgraded: this connector links
/// no TLS stack, so there is nothing to hand the upgraded stream to.
fn authority(url: &str) -> Result<String, TransportError> {
    let rest = url.strip_prefix("ws://").ok_or_else(|| {
        TransportError::connect(format!(
            "only ws:// is supported (this connector links no TLS stack): {url}"
        ))
    })?;

    host_port(rest, DEFAULT_WS_PORT)
        .ok_or_else(|| TransportError::connect(format!("no authority in {url}")))
}

/// Normalizes the authority of `rest` — everything after a `<scheme>://` — to
/// `host:port`, supplying `default_port` when the URL omits one. `None` when
/// there is no authority at all.
///
/// Split out of [`authority`] so that the attachment previews in
/// [`crate::attachments`] can reuse the rule rather than write a second parser:
/// the bracketed-IPv6 and default-port answers have to agree between the socket
/// the app dials and the `http://` origin it derives from it.
pub fn host_port(rest: &str, default_port: u16) -> Option<String> {
    // The authority runs to the first `/`, `?` or `#`.
    let host = rest
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default()
        .trim_start_matches("//");

    if host.is_empty() {
        return None;
    }

    // An IPv6 literal is bracketed (`[::1]:4002`), so look for the port
    // separator after the closing bracket only.
    let has_port = match host.rfind(']') {
        Some(bracket) => host[bracket..].contains(':'),
        None => host.contains(':'),
    };

    Some(if has_port {
        host.to_owned()
    } else {
        format!("{host}:{default_port}")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_an_explicit_port() {
        assert_eq!(
            authority("ws://127.0.0.1:4002/socket/websocket?vsn=2.0.0").unwrap(),
            "127.0.0.1:4002"
        );
    }

    #[test]
    fn defaults_the_port_when_the_url_omits_it() {
        assert_eq!(
            authority("ws://example.test/socket").unwrap(),
            "example.test:80"
        );
    }

    #[test]
    fn keeps_a_bracketed_ipv6_literal_intact() {
        assert_eq!(authority("ws://[::1]/socket").unwrap(), "[::1]:80");
        assert_eq!(authority("ws://[::1]:4002/socket").unwrap(), "[::1]:4002");
    }

    #[test]
    fn rejects_a_scheme_it_cannot_serve() {
        assert!(authority("wss://example.test/socket").is_err());
    }
}
