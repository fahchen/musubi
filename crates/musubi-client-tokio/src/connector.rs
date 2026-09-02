//! The tokio-tungstenite transport seam (`docs/rust-client.md` §2.3).

use std::pin::Pin;
use std::task::{Context, Poll, ready};

use futures_core::Stream;
use futures_core::future::BoxFuture;
use futures_sink::Sink;
use futures_util::{SinkExt, StreamExt};
use musubi_client::{Connector, Frame, Socket, TransportError};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};

/// Opens sockets with [`tokio_tungstenite::connect_async`] over rustls with the
/// webpki root store, so `wss://` works with no caller-side TLS setup.
///
/// The URL handed to [`Connector::connect`] is already complete — the socket
/// layer appended `/websocket`, `vsn=2.0.0` and the connect params — so this
/// impl passes it through verbatim.
///
/// ```no_run
/// use musubi_client_tokio::{Connection, TokioSpawner, TokioTimer, TungsteniteConnector};
///
/// let connection = Connection::builder()
///     .url("wss://example.test/socket")
///     .connector(TungsteniteConnector)
///     .spawner(TokioSpawner)
///     .timer(TokioTimer)
///     .build()?;
/// # Ok::<_, musubi_client_tokio::BuildError>(())
/// ```
#[derive(Debug, Default, Clone, Copy)]
pub struct TungsteniteConnector;

impl Connector for TungsteniteConnector {
    fn connect(&self, url: &str) -> BoxFuture<'static, Result<Box<dyn Socket>, TransportError>> {
        let url = url.to_owned();

        Box::pin(async move {
            let (stream, _response) = connect_async(&url).await.map_err(TransportError::connect)?;

            Ok(Box::new(TungsteniteSocket::new(stream)) as Box<dyn Socket>)
        })
    }
}

/// Maps tungstenite messages onto [`Frame`]s in both directions.
///
/// Generic over the underlying websocket rather than named against
/// `WebSocketStream<MaybeTlsStream<TcpStream>>`, so the mapping is unit
/// testable without a network.
struct TungsteniteSocket<S> {
    inner: S,
    /// Sticky once the peer's close is observed: a `Close` frame ends the
    /// stream, and anything the peer sends after it is a protocol violation.
    closed: bool,
}

impl<S> TungsteniteSocket<S> {
    /// Wraps one connected websocket.
    fn new(inner: S) -> Self {
        Self {
            inner,
            closed: false,
        }
    }
}

impl<S> Stream for TungsteniteSocket<S>
where
    S: Stream<Item = Result<Message, WsError>> + Unpin,
{
    type Item = Result<Frame, TransportError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            if this.closed {
                return Poll::Ready(None);
            }

            match ready!(this.inner.poll_next_unpin(cx)) {
                Some(Ok(Message::Text(text))) => return Poll::Ready(Some(Ok(Frame::Text(text)))),
                Some(Ok(Message::Binary(bytes))) => {
                    return Poll::Ready(Some(Ok(Frame::Binary(bytes))));
                }
                // A close frame is end-of-stream; reconnecting is the socket
                // layer's job, not the transport's.
                Some(Ok(Message::Close(_))) | None => {
                    this.closed = true;
                    return Poll::Ready(None);
                }
                // Pings are answered by tungstenite itself and `Frame` is never
                // produced while reading; none of the three carries a Phoenix
                // message, so keep polling for one that does.
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

impl<S> Sink<Frame> for TungsteniteSocket<S>
where
    S: Sink<Message, Error = WsError> + Unpin,
{
    type Error = TransportError;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().inner.poll_ready_unpin(cx).map_err(map_error)
    }

    fn start_send(self: Pin<&mut Self>, frame: Frame) -> Result<(), Self::Error> {
        let message = match frame {
            Frame::Text(text) => Message::Text(text),
            Frame::Binary(bytes) => Message::Binary(bytes),
        };

        self.get_mut()
            .inner
            .start_send_unpin(message)
            .map_err(map_error)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().inner.poll_flush_unpin(cx).map_err(map_error)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().inner.poll_close_unpin(cx).map_err(map_error)
    }
}

/// Maps a tungstenite failure onto the transport seam's error type.
///
/// Both "closed" variants collapse onto [`TransportError::Closed`]; everything
/// else (IO, TLS, protocol, capacity) is a [`TransportError::Io`] carrying the
/// rendered cause, which is all the socket layer ever does with it.
fn map_error(error: WsError) -> TransportError {
    match error {
        WsError::ConnectionClosed | WsError::AlreadyClosed => TransportError::Closed,
        other => TransportError::io(other),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use futures_executor::block_on;

    use super::*;

    #[test]
    fn maps_text_and_binary_frames_in_both_directions() {
        let sent = Arc::new(Mutex::new(Vec::new()));
        let mut socket = socket(
            [Ok(Message::Text("[null,null,\"t\",\"e\",{}]".to_owned()))],
            &sent,
        );

        assert!(matches!(
            block_on(socket.next()),
            Some(Ok(Frame::Text(text))) if text == "[null,null,\"t\",\"e\",{}]"
        ));

        block_on(socket.send(Frame::Text("out".to_owned()))).unwrap();
        block_on(socket.send(Frame::Binary(vec![1, 2, 3]))).unwrap();

        assert_eq!(
            *sent.lock().unwrap(),
            vec![
                Message::Text("out".to_owned()),
                Message::Binary(vec![1, 2, 3])
            ]
        );
    }

    #[test]
    fn reads_a_binary_frame() {
        let mut socket = socket([Ok(Message::Binary(vec![7, 8]))], &Arc::default());

        assert!(matches!(
            block_on(socket.next()),
            Some(Ok(Frame::Binary(bytes))) if bytes == vec![7, 8]
        ));
    }

    #[test]
    fn a_close_message_ends_the_stream_and_hides_everything_after_it() {
        let mut socket = socket(
            [
                Ok(Message::Close(None)),
                Ok(Message::Text("after close".to_owned())),
            ],
            &Arc::default(),
        );

        assert!(block_on(socket.next()).is_none());
        assert!(block_on(socket.next()).is_none());
    }

    #[test]
    fn skips_control_frames_that_carry_no_phoenix_message() {
        let mut socket = socket(
            [
                Ok(Message::Ping(vec![])),
                Ok(Message::Pong(vec![])),
                Ok(Message::Text("payload".to_owned())),
            ],
            &Arc::default(),
        );

        assert!(matches!(
            block_on(socket.next()),
            Some(Ok(Frame::Text(text))) if text == "payload"
        ));
    }

    #[test]
    fn maps_a_closed_connection_error_to_closed_and_then_ends_the_stream() {
        let mut socket = socket(
            [
                Err(WsError::ConnectionClosed),
                Ok(Message::Text("after close".to_owned())),
            ],
            &Arc::default(),
        );

        assert!(matches!(
            block_on(socket.next()),
            Some(Err(TransportError::Closed))
        ));
        assert!(block_on(socket.next()).is_none());
    }

    #[test]
    fn maps_any_other_error_to_io_without_ending_the_stream() {
        let mut socket = socket(
            [
                Err(WsError::Utf8),
                Ok(Message::Text("still alive".to_owned())),
            ],
            &Arc::default(),
        );

        assert!(matches!(
            block_on(socket.next()),
            Some(Err(TransportError::Io { .. }))
        ));
        assert!(matches!(
            block_on(socket.next()),
            Some(Ok(Frame::Text(text))) if text == "still alive"
        ));
    }

    #[test]
    fn ends_the_stream_when_the_underlying_websocket_ends() {
        assert!(block_on(socket([], &Arc::default()).next()).is_none());
    }

    /// A scripted websocket: `inbound` is drained by the [`Stream`] half,
    /// `sent` records the [`Sink`] half. No IO, so the mapping is the only
    /// thing under test.
    struct MockWs {
        inbound: VecDeque<Result<Message, WsError>>,
        sent: Arc<Mutex<Vec<Message>>>,
    }

    impl Stream for MockWs {
        type Item = Result<Message, WsError>;

        fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Ready(self.get_mut().inbound.pop_front())
        }
    }

    impl Sink<Message> for MockWs {
        type Error = WsError;

        fn poll_ready(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, message: Message) -> Result<(), Self::Error> {
            self.sent.lock().unwrap().push(message);

            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    /// Builds the socket under test over a scripted websocket.
    fn socket(
        inbound: impl IntoIterator<Item = Result<Message, WsError>>,
        sent: &Arc<Mutex<Vec<Message>>>,
    ) -> TungsteniteSocket<MockWs> {
        TungsteniteSocket::new(MockWs {
            inbound: inbound.into_iter().collect(),
            sent: Arc::clone(sent),
        })
    }
}
