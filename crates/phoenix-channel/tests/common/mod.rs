//! The scripted-transport rig both protocol test suites drive: a `LocalPool`
//! pumped by hand, the three seams wired to it, and the server end of the mock
//! socket.
//!
//! `crates/musubi-client/tests/connection.rs` includes this file directly
//! (`#[path = "../../phoenix-channel/tests/common/mod.rs"]`) so the two suites
//! cannot drift; each keeps only what is specific to its own layer.
//!
//! Three knobs let a test observe the client mid-flight rather than only at
//! rest, which is otherwise impossible because every pump runs it to
//! quiescence. [`ServerEnd::stall_writes`] holds the socket's sink not-ready,
//! so a test can watch what the client still manages while a write of its own
//! is going nowhere. [`Harness::armed`] reads which sleeps exist at that
//! moment. [`Harness::resolve`] fires a timer *without* pumping afterwards, so
//! two things can be made ready against a parked actor and the test can see
//! which of them it takes first.

// Each suite uses a subset of the rig.
#![allow(dead_code)]

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use futures_channel::mpsc::{self, UnboundedReceiver, UnboundedSender};
use futures_core::future::BoxFuture;
use futures_executor::LocalPool;
use futures_util::task::{LocalSpawnExt, noop_waker};
use futures_util::{Sink, Stream, StreamExt};
use phoenix_channel::{
    BinaryPush, Connector, Frame, Message, ReplyStatus, Socket, Spawner, Timer, TransportError,
};
use serde_json::{Value, json};

/// Pending `ManualTimer` sleeps: how long each was for, and how to wake it.
pub type Sleeps = Arc<Mutex<Vec<(Duration, futures_channel::oneshot::Sender<()>)>>>;

/// Where a spawned future parks its output until a test collects it.
pub type Slot<T> = Arc<Mutex<Option<T>>>;

/// Anything a [`ServerEnd`] can drive forward — i.e. a [`Harness`].
pub trait Pump {
    fn pump(&mut self);
}

/// The three seams, already wired to one harness.
pub struct Seams {
    pub connector: MockConnector,
    pub spawner: PumpSpawner,
    pub timer: ManualTimer,
}

/// A `LocalPool` plus the three seams, wired to whatever `inner` the suite
/// builds out of them (a `PhoenixSocket`, a `Connection`).
pub struct Harness<T> {
    pool: LocalPool,
    spawned: Arc<Mutex<Vec<BoxFuture<'static, ()>>>>,
    sleeps: Sleeps,
    sockets: Arc<Mutex<VecDeque<MockSocket>>>,
    urls: Arc<Mutex<Vec<String>>>,
    pub inner: T,
}

impl<T> Harness<T> {
    /// Builds the seams, hands them to `build`, and keeps both ends.
    pub fn new_with(build: impl FnOnce(Seams) -> T) -> Self {
        let spawned = Arc::new(Mutex::new(Vec::new()));
        let sleeps: Sleeps = Arc::new(Mutex::new(Vec::new()));
        let sockets = Arc::new(Mutex::new(VecDeque::new()));
        let urls = Arc::new(Mutex::new(Vec::new()));

        let inner = build(Seams {
            connector: MockConnector {
                sockets: Arc::clone(&sockets),
                urls: Arc::clone(&urls),
            },
            spawner: PumpSpawner {
                spawned: Arc::clone(&spawned),
            },
            timer: ManualTimer {
                sleeps: Arc::clone(&sleeps),
            },
        });

        Self {
            pool: LocalPool::new(),
            spawned,
            sleeps,
            sockets,
            urls,
            inner,
        }
    }

    /// Hands the connector one more socket and keeps its server end.
    pub fn queue_socket(&mut self) -> ServerEnd {
        let (to_client, inbound) = mpsc::unbounded();
        let (outbound, from_client) = mpsc::unbounded();
        let stall = Stall::default();

        self.sockets.lock().unwrap().push_back(MockSocket {
            inbound,
            outbound,
            stall: stall.clone(),
        });

        ServerEnd {
            to_client: Some(to_client),
            from_client,
            pending: Vec::new(),
            stall,
        }
    }

    /// Every URL the connector was asked for, in order.
    pub fn connected_urls(&self) -> Vec<String> {
        self.urls.lock().unwrap().clone()
    }

    /// Resolves every pending sleep of exactly `dur`, then settles.
    pub fn fire(&mut self, dur: Duration) {
        self.fire_where(|pending| pending == dur);
    }

    /// Resolves every pending sleep of exactly `dur` **without settling**.
    ///
    /// [`fire`](Self::fire) runs the client to quiescence, so whatever it
    /// resolves is fully handled before the test's next line. Some questions
    /// are about what the actor does when two things are ready *at once* — a
    /// timer tick in its inbox and a frame the transport has delivered but it
    /// has not read yet — and the only way to ask one is to arm both against a
    /// parked actor. Resolving first puts the timer's task ahead of the actor
    /// in the pool's run queue, so the message it posts is already waiting by
    /// the time the actor is polled for the frame that woke it.
    pub fn resolve(&mut self, dur: Duration) {
        self.resolve_where(|pending| pending == dur);
    }

    /// Resolves the reconnect/rejoin ladder, whose rungs are all sub-second.
    pub fn fire_backoff(&mut self) {
        self.fire_where(|pending| pending < Duration::from_secs(1));
    }

    /// Resolves the `nth` matching sleep only — 0 being the oldest armed —
    /// and leaves the rest pending.
    ///
    /// [`fire_backoff`](Self::fire_backoff) fires the whole sub-second band at
    /// once, which is all a test needs while one ladder is running. A channel's
    /// rejoin and the socket's reconnect both start at the same 10ms rung, so
    /// when both are armed only the order they were armed in tells them apart —
    /// and *which of the two fires first* is the whole question a rejoin timer
    /// that outlived its socket asks.
    pub fn fire_nth(&mut self, nth: usize, matches: impl Fn(Duration) -> bool) {
        let mut seen = 0;

        self.fire_where(|pending| {
            if !matches(pending) {
                return false;
            }

            let hit = seen == nth;
            seen += 1;
            hit
        });
    }

    /// How many sleeps of exactly `dur` are armed right now.
    ///
    /// A clock that arms its next tick only once the work of the current one is
    /// done has an observable signature: between the two there is nothing armed
    /// at all. This is how a test reads that.
    pub fn armed(&self, dur: Duration) -> usize {
        self.sleeps
            .lock()
            .unwrap()
            .iter()
            .filter(|(pending, _)| *pending == dur)
            .count()
    }

    pub fn fire_where(&mut self, matches: impl FnMut(Duration) -> bool) {
        self.resolve_where(matches);
        self.pump();
    }

    pub fn resolve_where(&mut self, mut matches: impl FnMut(Duration) -> bool) {
        let mut kept = Vec::new();

        for (dur, waker) in self.sleeps.lock().unwrap().drain(..) {
            if matches(dur) {
                let _ = waker.send(());
            } else {
                kept.push((dur, waker));
            }
        }

        self.sleeps.lock().unwrap().extend(kept);
    }

    pub fn spawn_capture<T2: Send + 'static>(
        &mut self,
        fut: impl Future<Output = T2> + Send + 'static,
    ) -> Slot<T2> {
        let slot = Arc::new(Mutex::new(None));
        let sink = Arc::clone(&slot);

        self.spawned.lock().unwrap().push(Box::pin(async move {
            let value = fut.await;
            *sink.lock().unwrap() = Some(value);
        }));

        slot
    }

    pub fn settle<T2>(&mut self, slot: Slot<T2>) -> T2 {
        self.peek(&slot)
            .expect("future should have resolved by now")
    }

    pub fn peek<T2>(&mut self, slot: &Slot<T2>) -> Option<T2> {
        self.pump();

        slot.lock().unwrap().take()
    }
}

impl<T> Pump for Harness<T> {
    /// Moves every spawned future into the pool and runs until nothing is
    /// runnable and nothing new was spawned.
    fn pump(&mut self) {
        for _ in 0..64 {
            let batch: Vec<BoxFuture<'static, ()>> =
                self.spawned.lock().unwrap().drain(..).collect();
            let idle = batch.is_empty();

            for fut in batch {
                self.pool
                    .spawner()
                    .spawn_local(fut)
                    .expect("the local pool accepts tasks");
            }

            self.pool.run_until_stalled();

            if idle && self.spawned.lock().unwrap().is_empty() {
                return;
            }
        }

        panic!("the harness never settled");
    }
}

/// The server side of a [`MockSocket`].
pub struct ServerEnd {
    to_client: Option<UnboundedSender<Result<Frame, TransportError>>>,
    from_client: UnboundedReceiver<Frame>,
    /// Frames read off the socket but not yet claimed. Text and binary frames
    /// are claimed by different assertions (`sent` vs `sent_binary`), so a
    /// frame of the other kind must not be dropped — or consumed out of order.
    pending: Vec<Frame>,
    stall: Stall,
}

impl ServerEnd {
    /// Holds the socket's sink not-ready, so whatever the client writes stays
    /// unwritten until [`resume_writes`](Self::resume_writes).
    ///
    /// Real transports stall exactly like this whenever their send buffer
    /// fills, and it is the one condition a peer can impose indefinitely
    /// without saying anything about it: no error, no close, just a socket that
    /// stops taking bytes. What the client can still do while it is held here —
    /// read the frames the peer is *sending*, fire its own timeouts, hang up —
    /// is the whole question the knob exists to ask.
    pub fn stall_writes(&self) {
        self.stall.inner.lock().unwrap().held = true;
    }

    /// Lets writing proceed again and wakes whoever was parked on it.
    pub fn resume_writes(&self) {
        let waker = {
            let mut state = self.stall.inner.lock().unwrap();
            state.held = false;
            state.waker.take()
        };

        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Every text frame the client wrote since the last call, decoded.
    pub fn sent(&mut self, harness: &mut impl Pump) -> Vec<Message> {
        self.claim(harness, |frame| match frame {
            Frame::Text(text) => {
                Ok(Message::decode(&text).expect("client text frames are five-tuples"))
            }
            other => Err(other),
        })
    }

    /// Every binary frame the client wrote since the last call, decoded.
    pub fn sent_binary(&mut self, harness: &mut impl Pump) -> Vec<BinaryPush> {
        self.claim(harness, |frame| match frame {
            Frame::Binary(bytes) => {
                Ok(BinaryPush::decode(&bytes).expect("client binary frames are pushes"))
            }
            other => Err(other),
        })
    }

    /// Reads everything outstanding, keeping whatever `take` does not claim.
    fn claim<T>(
        &mut self,
        harness: &mut impl Pump,
        take: impl Fn(Frame) -> Result<T, Frame>,
    ) -> Vec<T> {
        harness.pump();

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        while let Poll::Ready(Some(frame)) = self.from_client.poll_next_unpin(&mut cx) {
            self.pending.push(frame);
        }

        let mut claimed = Vec::new();
        let mut kept = Vec::new();

        for frame in std::mem::take(&mut self.pending) {
            match take(frame) {
                Ok(value) => claimed.push(value),
                Err(frame) => kept.push(frame),
            }
        }

        self.pending = kept;
        claimed
    }

    pub fn push(&self, message: Message) {
        if let Some(to_client) = &self.to_client {
            let _ = to_client.unbounded_send(Ok(message.encode()));
        }
    }

    /// Pushes a server-initiated event on the channel `join` established.
    pub fn push_event(&self, join: &Message, event: &str, payload: Value) {
        self.push(Message {
            join_ref: join.msg_ref.clone(),
            msg_ref: None,
            topic: join.topic.clone(),
            event: event.to_owned(),
            payload,
        });
    }

    /// Replies to a binary push. Phoenix answers one with an ordinary text
    /// `phx_reply`, never with a binary frame.
    pub fn reply_binary(&self, to: &BinaryPush, status: ReplyStatus, response: Value) {
        self.reply(
            &Message {
                join_ref: Some(to.join_ref.clone()),
                msg_ref: Some(to.msg_ref.clone()),
                topic: to.topic.clone(),
                event: to.event.clone(),
                payload: Value::Null,
            },
            status,
            response,
        );
    }

    pub fn reply(&self, to: &Message, status: ReplyStatus, response: Value) {
        self.push(Message {
            join_ref: to.join_ref.clone(),
            msg_ref: to.msg_ref.clone(),
            topic: to.topic.clone(),
            event: "phx_reply".to_owned(),
            payload: json!({
                "status": match status {
                    ReplyStatus::Ok => "ok",
                    ReplyStatus::Error => "error",
                },
                "response": response,
            }),
        });
    }

    /// Ends the inbound stream, which is how a transport reports a drop.
    pub fn disconnect(&mut self) {
        self.to_client = None;
    }
}

/// Whether a [`MockSocket`]'s sink is currently accepting writes, shared with
/// the [`ServerEnd`] that flips it.
#[derive(Clone, Default)]
pub struct Stall {
    inner: Arc<Mutex<StallState>>,
}

#[derive(Default)]
struct StallState {
    held: bool,
    waker: Option<Waker>,
}

/// A socket whose two halves are plain unbounded channels.
pub struct MockSocket {
    inbound: UnboundedReceiver<Result<Frame, TransportError>>,
    outbound: UnboundedSender<Frame>,
    stall: Stall,
}

impl Stream for MockSocket {
    type Item = Result<Frame, TransportError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inbound.poll_next_unpin(cx)
    }
}

impl Sink<Frame> for MockSocket {
    type Error = TransportError;

    /// The stall lives here rather than in `poll_flush` because this is where a
    /// real websocket refuses work: `poll_ready` flushes what it is holding and
    /// reports the socket not-ready when the kernel will take no more. A frame
    /// that gets past it has left the client for good.
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut state = self.stall.inner.lock().unwrap();

        if state.held {
            state.waker = Some(cx.waker().clone());
            return Poll::Pending;
        }

        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: Frame) -> Result<(), Self::Error> {
        self.outbound
            .unbounded_send(item)
            .map_err(|_| TransportError::Closed)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

/// Hands out pre-queued sockets and records the URLs it was asked for.
pub struct MockConnector {
    sockets: Arc<Mutex<VecDeque<MockSocket>>>,
    urls: Arc<Mutex<Vec<String>>>,
}

impl Connector for MockConnector {
    fn connect(&self, url: &str) -> BoxFuture<'static, Result<Box<dyn Socket>, TransportError>> {
        self.urls.lock().unwrap().push(url.to_owned());
        let next = self.sockets.lock().unwrap().pop_front();

        Box::pin(async move {
            match next {
                Some(socket) => Ok(Box::new(socket) as Box<dyn Socket>),
                None => Err(TransportError::connect("no socket queued")),
            }
        })
    }
}

/// Parks every spawned future until the harness pumps it.
pub struct PumpSpawner {
    spawned: Arc<Mutex<Vec<BoxFuture<'static, ()>>>>,
}

impl Spawner for PumpSpawner {
    fn spawn(&self, fut: BoxFuture<'static, ()>) {
        self.spawned.lock().unwrap().push(fut);
    }
}

/// A clock that only moves when a test says so.
pub struct ManualTimer {
    sleeps: Sleeps,
}

impl Timer for ManualTimer {
    fn sleep(&self, dur: Duration) -> BoxFuture<'static, ()> {
        let (tx, rx) = futures_channel::oneshot::channel();
        self.sleeps.lock().unwrap().push((dur, tx));

        Box::pin(async move {
            let _ = rx.await;
        })
    }
}

/// Everything a subscription has emitted so far.
pub fn drain<S: Stream + Unpin>(stream: &mut S) -> Vec<S::Item> {
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut drained = Vec::new();

    while let Poll::Ready(Some(item)) = stream.poll_next_unpin(&mut cx) {
        drained.push(item);
    }

    drained
}

/// Whether a subscription has ended, i.e. its sender was dropped.
pub fn ended<S: Stream + Unpin>(stream: &mut S) -> bool {
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    loop {
        match stream.poll_next_unpin(&mut cx) {
            Poll::Ready(Some(_)) => continue,
            Poll::Ready(None) => return true,
            Poll::Pending => return false,
        }
    }
}
