//! Reconnect/rejoin backoff: the `phoenix.js` ladder plus jitter.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::time::Duration;

/// The `phoenix.js` default reconnect ladder, in milliseconds. The last rung is
/// the steady-state interval once the ladder is exhausted.
const LADDER_MS: [u64; 10] = [10, 50, 100, 150, 200, 250, 500, 1000, 2000, 5000];

/// Jitter is added on top of the rung, up to this fraction of it, so that a
/// fleet of clients reconnecting after a server restart spreads out.
/// Denominator of the jitter fraction (`1/4` ⇒ up to +25%).
const JITTER_DENOMINATOR: u64 = 4;

/// The exponential backoff ladder, with a cursor.
///
/// Cheap to clone; every channel keeps its own for rejoin attempts and the
/// socket keeps one for reconnects.
#[derive(Debug, Clone, Default)]
pub(crate) struct Backoff {
    attempt: usize,
}

impl Backoff {
    /// Returns the delay for the current attempt and advances the cursor.
    ///
    /// The base delay walks the ladder and then holds at its last rung; the
    /// returned value is the base plus up to 25% jitter.
    pub(crate) fn next_delay(&mut self) -> Duration {
        let base = Self::base_ms(self.attempt);
        self.attempt = self.attempt.saturating_add(1);

        // Spread, not randomness — and it costs no dependency. A *fresh*
        // `RandomState` per call is what varies the value: std keys each one
        // from a thread-local seed pair whose first half it increments on every
        // construction, so hashing the same (empty) input yields a different
        // digest each call. That gives both axes a reconnect fleet needs —
        // successive delays on one client differ (the per-call counter), and
        // delays across clients differ (the per-thread random seed). Hoisting
        // the `RandomState` onto `Backoff` would fix the keys and collapse the
        // jitter to a per-instance constant.
        let seed = RandomState::new().build_hasher().finish();
        let jitter = seed % (base / JITTER_DENOMINATOR + 1);

        Duration::from_millis(base + jitter)
    }

    /// Rewinds to the first rung, after a successful open or join.
    pub(crate) fn reset(&mut self) {
        self.attempt = 0;
    }

    fn base_ms(attempt: usize) -> u64 {
        LADDER_MS[attempt.min(LADDER_MS.len() - 1)]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn walks_the_ladder_then_holds_at_five_seconds() {
        let mut backoff = Backoff::default();

        for base in LADDER_MS {
            let delay = backoff.next_delay().as_millis() as u64;

            assert!(
                (base..=base + base / JITTER_DENOMINATOR).contains(&delay),
                "{delay}ms outside the jittered band for a {base}ms rung"
            );
        }

        for _ in 0..3 {
            let delay = backoff.next_delay().as_millis() as u64;

            assert!(
                (5000..=6250).contains(&delay),
                "{delay}ms is not steady state"
            );
        }
    }

    #[test]
    fn jitter_varies_between_calls_at_one_rung() {
        // Guards the per-call `RandomState::new()`: storing one on `Backoff`
        // would key every `finish()` identically over the same empty input, so
        // this loop would return one constant and the jitter would be gone.
        //
        // The steady-state rung is fixed at 5000ms, so every sample here draws
        // from the same 1251-wide jitter band. Asserting only "not all equal"
        // keeps this honest — individual collisions are expected — and with 16
        // samples a false failure needs all 15 to collide, ~1251^-15.
        let mut backoff = Backoff::default();
        for _ in 0..LADDER_MS.len() {
            backoff.next_delay();
        }

        let first = backoff.next_delay();
        let varied = (0..15).any(|_| backoff.next_delay() != first);

        assert!(
            varied,
            "16 consecutive delays at the 5000ms rung were all {first:?}"
        );
    }

    #[test]
    fn reset_rewinds_to_the_first_rung() {
        let mut backoff = Backoff::default();
        for _ in 0..LADDER_MS.len() {
            backoff.next_delay();
        }

        backoff.reset();

        assert!(backoff.next_delay() <= Duration::from_millis(12));
    }
}
