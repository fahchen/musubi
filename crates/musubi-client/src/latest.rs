//! The latest-value cell behind [`Mounted::updates`](crate::Mounted::updates)
//! and [`Mounted::status_updates`](crate::Mounted::status_updates)
//! (`docs/rust-client.md` §2.4).
//!
//! Both surfaces carry whole-root values: every item **subsumes** the one
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
//! Runtime-free like the rest of the crate: a `std::sync::Mutex` and a waker
//! list, no `tokio::sync::watch` (§2.4).

use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use futures_core::Stream;

use crate::lock;

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
    /// One waker per parked receiver — the cell's entire per-subscriber cost.
    /// Keyed, so re-polling replaces an entry rather than appending one.
    wakers: Vec<(u64, Waker)>,
}

/// The write half of a latest-value cell: one value, any number of receivers.
///
/// Not `Clone`: exactly one writer owns it, and dropping it ends every
/// receiver exactly as [`close`](Self::close) does.
pub(crate) struct Latest<T> {
    slot: Arc<Mutex<Slot<T>>>,
}

impl<T> Latest<T> {
    /// A cell holding `initial`, which a receiver replays on its first poll.
    ///
    /// A seeded cell starts at version 1 rather than 0 precisely so that
    /// replay covers it: version 0 means "nothing has ever been written", and
    /// that is the only state a fresh receiver parks in.
    pub(crate) fn new(initial: Option<T>) -> Self {
        Self {
            slot: Arc::new(Mutex::new(Slot {
                version: u64::from(initial.is_some()),
                value: initial,
                closed: false,
                next_id: 0,
                wakers: Vec::new(),
            })),
        }
    }

    /// Overwrites the value and wakes every parked receiver.
    pub(crate) fn set(&self, value: T) {
        self.set_with(|_| Some(value));
    }

    /// Overwrites the value with what `next` makes of the current one, under
    /// the cell's lock; `None` writes nothing and wakes nobody.
    ///
    /// The read-modify-write an edges-only writer needs — dedupe and write are
    /// one step here, not a read a concurrent writer can slip between.
    pub(crate) fn set_with(&self, next: impl FnOnce(Option<&T>) -> Option<T>) {
        let woken = {
            let mut slot = lock(&self.slot);

            // Closing is terminal: a late write must not resurrect a stream
            // whose consumer has already been told the root is gone.
            if slot.closed {
                return;
            }

            let Some(value) = next(slot.value.as_ref()) else {
                return;
            };

            slot.value = Some(value);
            slot.version += 1;

            std::mem::take(&mut slot.wakers)
        };

        // Woken outside the lock: a waker is free to poll the stream straight
        // through, which would deadlock on a guard still held here.
        for (_, waker) in woken {
            waker.wake();
        }
    }

    /// Ends every receiver.
    ///
    /// Terminal, and the last value survives it: a receiver that has not caught
    /// up delivers that value and *then* ends, so nothing published is dropped
    /// by teardown itself. [`get`](Self::get) reports `None` from here on.
    pub(crate) fn close(&self) {
        let woken = {
            let mut slot = lock(&self.slot);

            slot.closed = true;

            std::mem::take(&mut slot.wakers)
        };

        for (_, waker) in woken {
            waker.wake();
        }
    }

    /// A new receiver, behind the cell by construction.
    pub(crate) fn subscribe(&self) -> Updates<T> {
        let mut slot = lock(&self.slot);
        let id = slot.next_id;

        slot.next_id += 1;

        Updates {
            slot: Arc::clone(&self.slot),
            id,
            // A subscription taken after teardown starts caught up instead: a
            // closed cell has no current value to replay — [`get`](Self::get)
            // says the same — so all it hands back is an ended stream.
            seen: if slot.closed { slot.version } else { 0 },
        }
    }
}

impl<T: Clone> Latest<T> {
    /// The current value: `None` before the first write and after
    /// [`close`](Self::close).
    pub(crate) fn get(&self) -> Option<T> {
        let slot = lock(&self.slot);

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
    slot: Arc<Mutex<Slot<T>>>,
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
        let mut slot = lock(&this.slot);

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
        lock(&self.slot).wakers.retain(|(id, _)| *id != self.id);
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

        assert_eq!(lock(&cell.slot).wakers.len(), 1, "one receiver, one waker");

        for n in 0..100 {
            cell.set(n);
        }

        // The other half of "a stalled consumer cannot grow the cell": a
        // hundred writes it never picked up left one value behind, so it runs
        // its body once, not a hundred times.
        assert!(
            lock(&cell.slot).wakers.is_empty(),
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
        assert_eq!(lock(&cell.slot).wakers.len(), 2);

        drop(dropped);

        assert_eq!(lock(&cell.slot).wakers.len(), 1);
        assert_eq!(lock(&cell.slot).wakers[0].0, kept.id);
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

    /// Polls without a real task, the way every consumer-facing assertion here
    /// wants to read: one step, no executor.
    fn poll<T: Clone>(receiver: &mut Updates<T>) -> Poll<Option<T>> {
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
