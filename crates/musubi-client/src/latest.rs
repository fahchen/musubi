//! The latest-value cell behind [`Mounted::status`](crate::Mounted::status)
//! (`docs/rust-client.md` §2.4, `docs/rust-reactive-state.md` §5.4).
//!
//! [`MountStatus`] is not state. It is a client-local liveness projection that
//! no wire message carries, so it does not live on the retained tree: putting it
//! there would mean inventing a node the server never renders and then excluding
//! it from `to_wire`, from `to_hydrated` and from drift validation. State lives
//! on the tree instead, reached through `Mounted::state()`; this module is the
//! one cell a root keeps, reached through a handle.
//!
//! This surface carries whole values: every item **subsumes** the one
//! before it, and nothing downstream folds intermediates. A queue is therefore
//! the wrong shape — a consumer that stalls buys a backlog it can only throw
//! away, one entry per accepted envelope, growing without bound while it
//! stalls, and then runs its body once per entry to arrive where the newest
//! one already was. This cell keeps one value and a version instead: a
//! receiver that is behind takes the current value, never the ones it missed,
//! so a stalled consumer costs one waker rather than a queue.
//!
//! Two consequences worth naming. Coalescing is **structural**, not a policy a
//! reader opts into: an intermediate value is gone the moment the next write
//! lands, so no consumer can observe one. And a receiver starts *behind* the
//! cell, so its first poll delivers whatever the cell already holds — the
//! replay that closes the subscribe-then-read-the-current-value window every
//! consumer of the old queues had to open by hand.
//!
//! Runtime-free like the rest of the crate: a `std::sync::Mutex`, a waker list
//! and a callback list, no `tokio::sync::watch` (§2.4).
//!
//! # Two shapes, one subscription
//!
//! [`StatusState::subscribe`] and [`StatusState::into_stream`] are the same
//! edge, handed to a consumer that keeps its observations in a struct and to one
//! that writes a loop. The callback list and the waker list are woken by the
//! same write, and both are invoked **after** the cell's lock is released —
//! the tree's never-notify-under-the-lock discipline, applied to the one cell
//! that is not a tree node (`docs/rust-reactive-state.md` §2.6, §5.4).

use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Waker};

use futures_core::Stream;
use musubi_state::{SubscriberId, Subscription, Unsubscribe};

use crate::lock;
use crate::mounted::MountStatus;

/// One registered callback on a cell.
type Callback<T> = Arc<dyn Fn(&T) + Send + Sync>;

/// The one slot every receiver of a cell reads.
struct Slot<T> {
    /// The current value; `None` until the first write.
    ///
    /// [`Latest::close`] deliberately leaves it in place — a receiver that has
    /// not caught up still owes it before ending. [`Latest::get`] is what stops
    /// reporting it.
    value: Option<T>,
    /// Bumped by every write. A receiver whose `seen` is lower is behind and
    /// takes `value` on its next poll; receivers start at `0`, which is what
    /// makes their first poll a replay.
    version: u64,
    /// Set by [`Latest::close`]: a caught-up receiver ends instead of parking.
    closed: bool,
    /// Handed out by [`Latest::subscribe`], so a receiver can find its own
    /// waker in `wakers`.
    next_id: u64,
    /// One waker per parked receiver — the cell's entire per-stream cost.
    /// Keyed, so re-polling replaces an entry rather than appending one.
    wakers: Vec<(u64, Waker)>,
    /// The callback half of the same subscription (§5.4). Keyed by the same
    /// counter as the wakers, so no id is ever both.
    callbacks: Vec<(u64, Callback<T>)>,
}

/// The shared half of a cell: what a [`Latest`], its [`Updates`] receivers and
/// its [`Subscription`]s all address.
///
/// A named type rather than a bare `Arc<Mutex<Slot<T>>>` because a
/// [`Subscription`] needs something to hold a `Weak<dyn Unsubscribe>` to.
struct Shared<T> {
    slot: Mutex<Slot<T>>,
}

impl<T: Send + Sync + 'static> Unsubscribe for Shared<T> {
    /// Drops one callback. Streams deregister through [`Updates`]'s own `Drop`.
    fn unsubscribe(&self, id: SubscriberId) {
        lock(&self.slot)
            .callbacks
            .retain(|(registered, _)| *registered != id.as_raw());
    }
}

/// The write half of a latest-value cell: one value, any number of receivers.
///
/// Not `Clone`: exactly one writer owns it, and dropping it ends every
/// receiver exactly as [`close`](Self::close) does.
///
/// Generic on purpose, with exactly one production instantiation
/// (`Latest<MountStatus>`): the parameter is what lets this module's own tests
/// exercise the cell's *mechanics* — replay, coalescing, one waker per stalled
/// receiver, close-then-drain — against values chosen to make an assertion
/// legible, instead of against a three-variant `Copy` enum whose edges are the
/// mount lifecycle. Nothing else here is shaped by it.
pub(crate) struct Latest<T> {
    shared: Arc<Shared<T>>,
}

impl<T> Latest<T> {
    /// A cell holding `initial`, which a receiver replays on its first poll.
    ///
    /// A seeded cell starts at version 1 rather than 0 precisely so that
    /// replay covers it: version 0 means "nothing has ever been written", and
    /// that is the only state a fresh receiver parks in.
    pub(crate) fn new(initial: Option<T>) -> Self {
        Self {
            shared: Arc::new(Shared {
                slot: Mutex::new(Slot {
                    version: u64::from(initial.is_some()),
                    value: initial,
                    closed: false,
                    next_id: 0,
                    wakers: Vec::new(),
                    callbacks: Vec::new(),
                }),
            }),
        }
    }

    /// Ends every receiver.
    ///
    /// Terminal, and the last value survives it: a receiver that has not caught
    /// up delivers that value and *then* ends, so nothing published is dropped
    /// by teardown itself. [`get`](Self::get) reports `None` from here on.
    pub(crate) fn close(&self) {
        let woken = {
            let mut slot = lock(&self.shared.slot);

            slot.closed = true;
            // Terminal for the callback half too. Closing is not an edge — it
            // carries no status — so nothing is invoked; a `Subscription` still
            // held is simply inert from here, exactly as one pointing at a freed
            // node is.
            slot.callbacks.clear();

            std::mem::take(&mut slot.wakers)
        };

        for (_, waker) in woken {
            waker.wake();
        }
    }

    /// Registers one callback and hands back its RAII token (§5.4).
    ///
    /// It does **not** fire on registration: that would run caller code inside
    /// the `subscribe` call stack, on the *registrant's* thread, and would make
    /// this `subscribe` a different thing from
    /// [`State::subscribe`](musubi_state::State::subscribe), which never calls
    /// its callback. Subscribe first and read second — an order that can repeat
    /// one idempotent assignment, never miss an edge.
    ///
    /// A closed cell still hands back a token; it is simply inert, so no
    /// consumer has to branch on liveness just to subscribe.
    fn on_change(&self, callback: Callback<T>) -> Subscription
    where
        T: Send + Sync + 'static,
    {
        let id = {
            let mut slot = lock(&self.shared.slot);
            let id = slot.next_id;

            slot.next_id += 1;

            if !slot.closed {
                slot.callbacks.push((id, callback));
            }

            id
        };

        Subscription::cell(
            Arc::downgrade(&self.shared) as Weak<dyn Unsubscribe>,
            SubscriberId::from_raw(id),
        )
    }

    /// A new receiver, behind the cell by construction.
    pub(crate) fn subscribe(&self) -> Updates<T> {
        let mut slot = lock(&self.shared.slot);
        let id = slot.next_id;

        slot.next_id += 1;

        Updates {
            shared: Arc::clone(&self.shared),
            id,
            // A subscription taken after teardown starts caught up instead: a
            // closed cell has no current value to replay — [`get`](Self::get)
            // says the same — so all it hands back is an ended stream.
            seen: if slot.closed { slot.version } else { 0 },
        }
    }
}

impl<T: Clone> Latest<T> {
    /// Overwrites the value and wakes every parked receiver.
    ///
    /// The cell's only production writer is [`set_with`](Self::set_with) — the
    /// status is edges-only, and deciding an edge is a read-modify-write. This
    /// is the unconditional form the tests use.
    #[cfg(test)]
    pub(crate) fn set(&self, value: T) {
        self.set_with(|_| Some(value));
    }

    /// Overwrites the value with what `next` makes of the current one, under
    /// the cell's lock; `None` writes nothing and wakes nobody.
    ///
    /// The read-modify-write an edges-only writer needs — dedupe and write are
    /// one step here, not a read a concurrent writer can slip between.
    pub(crate) fn set_with(&self, next: impl FnOnce(Option<&T>) -> Option<T>) {
        let (woken, owed, edge) = {
            let mut slot = lock(&self.shared.slot);

            // Closing is terminal: a late write must not resurrect a stream
            // whose consumer has already been told the root is gone.
            if slot.closed {
                return;
            }

            let Some(value) = next(slot.value.as_ref()) else {
                return;
            };

            slot.value = Some(value.clone());
            slot.version += 1;

            // Cloned under the lock, invoked without it — the same discipline
            // the tree's `Notify` follows (§2.6).
            let owed: Vec<Callback<T>> = slot
                .callbacks
                .iter()
                .map(|(_, callback)| Arc::clone(callback))
                .collect();

            (std::mem::take(&mut slot.wakers), owed, value)
        };

        // Woken outside the lock: a waker is free to poll the stream straight
        // through, which would deadlock on a guard still held here.
        for (_, waker) in woken {
            waker.wake();
        }

        // The value travels with the call rather than being re-read: this cell
        // coalesces, so a callback that read `get()` could observe a *later*
        // edge than the one it was called for (§2.4).
        for callback in owed {
            callback(&edge);
        }
    }

    /// The current value: `None` before the first write and after
    /// [`close`](Self::close).
    pub(crate) fn get(&self) -> Option<T> {
        let slot = lock(&self.shared.slot);

        if slot.closed {
            return None;
        }

        slot.value.clone()
    }
}

impl<T> Drop for Latest<T> {
    fn drop(&mut self) {
        // The receivers outlive the writer — they hold the slot, not the cell —
        // so a dropped writer has to end them or they park forever.
        self.close();
    }
}

/// One subscription to a [`Latest`] cell.
///
/// The stream **is** the subscription (§7): dropping it deregisters this
/// receiver, and it ends once the cell is closed and this receiver has caught
/// up. Missing an intermediate value is not an error here — it is the contract.
pub(crate) struct Updates<T> {
    shared: Arc<Shared<T>>,
    /// This receiver's key in [`Slot::wakers`].
    id: u64,
    /// The version already delivered; `0` at construction, which is what makes
    /// the first poll a replay of whatever the cell holds.
    seen: u64,
}

impl<T: Clone> Stream for Updates<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let this = self.get_mut();
        let mut slot = lock(&this.shared.slot);

        if slot.version > this.seen {
            this.seen = slot.version;

            // A bumped version always has a value: only a write bumps it, and
            // `close` keeps the last one for exactly this delivery.
            if let Some(value) = slot.value.clone() {
                return Poll::Ready(Some(value));
            }
        }

        if slot.closed {
            return Poll::Ready(None);
        }

        // Replace this receiver's waker instead of pushing a second one: a
        // stream polled repeatedly between writes must not grow the list.
        match slot.wakers.iter_mut().find(|(id, _)| *id == this.id) {
            Some((_, parked)) => parked.clone_from(cx.waker()),
            None => slot.wakers.push((this.id, cx.waker().clone())),
        }

        Poll::Pending
    }
}

impl<T> Drop for Updates<T> {
    fn drop(&mut self) {
        // Otherwise the cell keeps a waker per subscription ever taken, which
        // is the unbounded growth this shape exists to avoid.
        lock(&self.shared.slot)
            .wakers
            .retain(|(id, _)| *id != self.id);
    }
}

/// The mount's place in its connection lifecycle (BDR-0033), as a handle.
///
/// The one handle in the family (`docs/rust-reactive-state.md` §2.4) that is
/// **not** rooted at a tree node: [`MountStatus`] is a client-local liveness
/// projection no wire message carries, so its value lives in the latest-value
/// cell this module keeps. Cheap to clone; every clone addresses the same cell.
///
/// ```text
/// mounted.status()                  -> StatusState      handle
/// mounted.status().value()          -> MountStatus      value
/// mounted.status().subscribe(cb)    -> Subscription     subscription
/// mounted.status().into_stream()    -> impl Stream<..>  the subscription, in `await` shape
/// ```
///
/// The last two are two faces of one subscription, not two capabilities: under
/// [`into_stream`](Self::into_stream) is this cell's existing receiver, not one
/// edge more and not one edge less.
#[derive(Clone)]
pub struct StatusState {
    cell: Arc<Latest<MountStatus>>,
}

impl StatusState {
    /// The handle over one root's status cell.
    pub(crate) fn new(cell: Arc<Latest<MountStatus>>) -> Self {
        Self { cell }
    }

    /// The current status, as a value.
    ///
    /// [`MountStatus::Connecting`] until the first accepted initial patch, and
    /// — unchanged — [`MountStatus::Connecting`] **forever** for a handle held
    /// across a [`Connection::disconnect`](crate::Connection::disconnect):
    /// teardown puts the cell back to its pre-initial baseline, so a root that
    /// will never connect reads exactly like one that has not connected yet.
    pub fn value(&self) -> MountStatus {
        self.cell.get().unwrap_or(MountStatus::Connecting)
    }

    /// Subscribe. RAII, and the same [`Subscription`] every tree view hands
    /// back, so it lives in the same `Vec` as they do.
    ///
    /// The callback is handed the status it is being called *for*, not just
    /// "something changed": the cell coalesces, so a callback that re-read
    /// [`value`](Self::value) could observe a **later** edge than its own.
    ///
    /// It does **not** fire on registration. Subscribe first, `value()` second:
    /// that order can repeat one idempotent assignment, never miss an edge.
    ///
    /// The callback runs on the actor task — the same task, and the same
    /// head-of-line cost, as a state subscriber — so the contract is the same:
    /// **schedule, do not compute.**
    #[must_use = "dropping the subscription unsubscribes"]
    pub fn subscribe(
        &self,
        on_change: impl Fn(MountStatus) + Send + Sync + 'static,
    ) -> Subscription {
        self.cell
            .on_change(Arc::new(move |status: &MountStatus| on_change(*status)))
    }

    /// **Consumes this handle** and hands back the same subscription in `await`
    /// shape, for a consumer whose shape is a loop.
    ///
    /// Not an accessor and not a getter: `into_` is the shape conversion, and
    /// the handle is the thing being converted (§2.4). Handles are [`Clone`], so
    /// a caller that still needs the handle converts a clone
    /// (`status.clone().into_stream()`); the common
    /// `mounted.status().into_stream()` consumes the one the accessor just made,
    /// and costs nothing.
    ///
    /// This is the cell's existing subscription, unchanged: latest-value not a
    /// queue, edges only, and the **first poll replays**
    /// [`value`](Self::value).
    #[must_use = "the stream is the subscription; dropping it unsubscribes"]
    pub fn into_stream(self) -> impl Stream<Item = MountStatus> + Send + 'static {
        self.cell.subscribe()
    }
}

impl std::fmt::Debug for StatusState {
    /// Prints the value, which is a one-byte `Copy` enum: cheap, infallible,
    /// and the only thing about this handle worth reading in a log.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StatusState")
            .field("value", &self.value())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Wake;

    use futures_util::StreamExt;
    use futures_util::task::noop_waker;

    use super::*;

    #[test]
    fn the_first_poll_replays_the_current_value_and_an_empty_cell_parks() {
        let empty = Latest::new(None);
        let mut receiver = empty.subscribe();

        assert!(poll(&mut receiver).is_pending(), "nothing written yet");

        empty.set("first");

        assert_eq!(poll(&mut receiver), Poll::Ready(Some("first")));

        // A subscription taken after the write opens with it — this is what
        // replaces the subscribe-then-read-the-snapshot idiom.
        assert_eq!(poll(&mut empty.subscribe()), Poll::Ready(Some("first")));

        let seeded = Latest::new(Some("seed"));

        assert_eq!(poll(&mut seeded.subscribe()), Poll::Ready(Some("seed")));
    }

    #[test]
    fn a_receiver_that_never_polls_sees_only_the_latest_value() {
        let cell = Latest::new(None);
        let mut receiver = cell.subscribe();

        for n in 0..1_000 {
            cell.set(n);
        }

        assert_eq!(poll(&mut receiver), Poll::Ready(Some(999)));
        assert!(poll(&mut receiver).is_pending(), "caught up");
    }

    #[test]
    fn a_stalled_receiver_costs_one_waker_and_no_buffer() {
        let cell = Latest::new(None);
        let mut receiver = cell.subscribe();

        // Polling a hundred times replaces this receiver's waker rather than
        // appending one: the park list cannot grow between writes.
        for _ in 0..100 {
            assert!(poll(&mut receiver).is_pending());
        }

        assert_eq!(
            lock(&cell.shared.slot).wakers.len(),
            1,
            "one receiver, one waker"
        );

        for n in 0..100 {
            cell.set(n);
        }

        // The other half of "a stalled consumer cannot grow the cell": a
        // hundred writes it never picked up left one value behind, so it runs
        // its body once, not a hundred times.
        assert!(
            lock(&cell.shared.slot).wakers.is_empty(),
            "the first write woke it"
        );
        assert_eq!(poll(&mut receiver), Poll::Ready(Some(99)));
        assert!(poll(&mut receiver).is_pending());
    }

    #[test]
    fn every_receiver_tracks_the_cell_independently() {
        let cell = Latest::new(None);
        let mut eager = cell.subscribe();
        let mut lazy = cell.subscribe();

        cell.set("first");

        assert_eq!(poll(&mut eager), Poll::Ready(Some("first")));

        cell.set("second");

        // The eager receiver sees both, the lazy one only the latest.
        assert_eq!(poll(&mut eager), Poll::Ready(Some("second")));
        assert_eq!(poll(&mut lazy), Poll::Ready(Some("second")));
        assert!(poll(&mut lazy).is_pending());
    }

    #[test]
    fn a_write_wakes_every_parked_receiver_exactly_once() {
        let cell = Latest::new(None);
        let mut receiver = cell.subscribe();
        let flag = Arc::new(Wakes::default());
        let waker = Waker::from(Arc::clone(&flag));

        assert!(
            receiver
                .poll_next_unpin(&mut Context::from_waker(&waker))
                .is_pending()
        );
        cell.set("first");

        assert_eq!(flag.0.load(Ordering::Relaxed), 1);

        // Woken once, not once per write: the receiver is not parked again
        // until it polls, so the second write finds no waker to call.
        cell.set("second");

        assert_eq!(flag.0.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn closing_delivers_the_last_unseen_value_and_then_ends() {
        let cell = Latest::new(None);
        let mut behind = cell.subscribe();
        let mut caught_up = cell.subscribe();

        cell.set("last");

        assert_eq!(poll(&mut caught_up), Poll::Ready(Some("last")));

        cell.close();

        assert_eq!(poll(&mut behind), Poll::Ready(Some("last")));
        assert_eq!(poll(&mut behind), Poll::Ready(None));
        assert_eq!(poll(&mut caught_up), Poll::Ready(None));
        // Terminal in both directions: the cell reports no current value and
        // takes no further write.
        assert_eq!(cell.get(), None);

        cell.set("late");

        // A subscription taken afterwards is an ended stream, never a replay
        // of a value the cell no longer reports.
        assert_eq!(poll(&mut cell.subscribe()), Poll::Ready(None));
    }

    #[test]
    fn closing_wakes_a_parked_receiver() {
        let cell = Latest::<&str>::new(None);
        let mut receiver = cell.subscribe();
        let flag = Arc::new(Wakes::default());
        let waker = Waker::from(Arc::clone(&flag));

        assert!(
            receiver
                .poll_next_unpin(&mut Context::from_waker(&waker))
                .is_pending()
        );
        cell.close();

        assert_eq!(flag.0.load(Ordering::Relaxed), 1);
        assert_eq!(poll(&mut receiver), Poll::Ready(None));
    }

    #[test]
    fn dropping_the_writer_ends_every_receiver() {
        let cell = Latest::new(Some("only"));
        let mut receiver = cell.subscribe();

        drop(cell);

        assert_eq!(poll(&mut receiver), Poll::Ready(Some("only")));
        assert_eq!(poll(&mut receiver), Poll::Ready(None));
    }

    #[test]
    fn dropping_a_receiver_deregisters_its_waker() {
        let cell = Latest::<&str>::new(None);
        let mut kept = cell.subscribe();
        let mut dropped = cell.subscribe();

        assert!(poll(&mut kept).is_pending());
        assert!(poll(&mut dropped).is_pending());
        assert_eq!(lock(&cell.shared.slot).wakers.len(), 2);

        drop(dropped);

        assert_eq!(lock(&cell.shared.slot).wakers.len(), 1);
        assert_eq!(lock(&cell.shared.slot).wakers[0].0, kept.id);
    }

    #[test]
    fn a_write_the_closure_declines_changes_nothing() {
        let cell = Latest::new(Some("held"));
        let mut receiver = cell.subscribe();

        assert_eq!(poll(&mut receiver), Poll::Ready(Some("held")));

        cell.set_with(|current| {
            assert_eq!(current, Some(&"held"));

            None
        });

        assert!(poll(&mut receiver).is_pending(), "no version was bumped");
        assert_eq!(cell.get(), Some("held"));
    }

    // ---- the callback half (§5.4) ----------------------------------------

    #[test]
    fn a_callback_is_not_invoked_at_registration() {
        let cell = Arc::new(Latest::new(Some(MountStatus::Connecting)));
        let status = StatusState::new(Arc::clone(&cell));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let record = Arc::clone(&seen);

        let subscription = status.subscribe(move |status| lock(&record).push(status));

        assert!(
            lock(&seen).is_empty(),
            "registration runs no caller code; `value()` is how you read the current one"
        );
        assert_eq!(status.value(), MountStatus::Connecting);

        cell.set(MountStatus::Live);

        assert_eq!(*lock(&seen), [MountStatus::Live]);

        drop(subscription);
    }

    #[test]
    fn a_dropped_subscription_takes_at_most_one_more_call() {
        let cell = Arc::new(Latest::new(Some(MountStatus::Connecting)));
        let status = StatusState::new(Arc::clone(&cell));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let record = Arc::clone(&seen);

        let subscription = status.subscribe(move |status| lock(&record).push(status));

        cell.set(MountStatus::Live);
        drop(subscription);
        cell.set(MountStatus::Reconnecting);

        // The one tolerated stale call is the one already cloned out from under
        // the lock when the drop lands; a drop that completes first is final.
        assert_eq!(*lock(&seen), [MountStatus::Live]);
        assert!(lock(&cell.shared.slot).callbacks.is_empty());
    }

    #[test]
    fn the_stream_and_the_callback_are_the_same_edges() {
        let cell = Arc::new(Latest::new(Some(MountStatus::Connecting)));
        let status = StatusState::new(Arc::clone(&cell));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let record = Arc::clone(&seen);

        let _subscription = status.subscribe(move |status| lock(&record).push(status));
        let mut stream = status.clone().into_stream();

        // The stream replays the current value; the callback does not.
        assert_eq!(
            poll(&mut stream),
            Poll::Ready(Some(MountStatus::Connecting))
        );
        assert!(lock(&seen).is_empty());

        cell.set(MountStatus::Live);

        assert_eq!(poll(&mut stream), Poll::Ready(Some(MountStatus::Live)));
        assert_eq!(*lock(&seen), [MountStatus::Live]);
    }

    #[test]
    fn closing_the_cell_retires_the_callbacks_without_calling_them() {
        let cell = Arc::new(Latest::new(Some(MountStatus::Live)));
        let status = StatusState::new(Arc::clone(&cell));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let record = Arc::clone(&seen);

        let _subscription = status.subscribe(move |status| lock(&record).push(status));

        cell.close();

        assert!(lock(&seen).is_empty(), "closing carries no status");
        assert_eq!(
            status.value(),
            MountStatus::Connecting,
            "a torn-down root reads as its pre-initial baseline (BDR-0033)"
        );

        // A subscription taken afterwards is inert rather than filed.
        let _late = status.subscribe(|_| unreachable!("a closed cell never publishes"));

        cell.set(MountStatus::Live);

        assert!(lock(&seen).is_empty());
    }

    /// Polls without a real task, the way every consumer-facing assertion here
    /// wants to read: one step, no executor.
    fn poll<S: Stream + Unpin>(receiver: &mut S) -> Poll<Option<S::Item>> {
        let waker = noop_waker();

        receiver.poll_next_unpin(&mut Context::from_waker(&waker))
    }

    /// A waker that counts how often it was called.
    #[derive(Default)]
    struct Wakes(AtomicUsize);

    impl Wake for Wakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
}
