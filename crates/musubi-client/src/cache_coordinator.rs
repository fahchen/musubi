//! The mount cache's policy half (`docs/rust-client.md` §6.4).
//!
//! [`CacheStore`] is what an embedder implements; this is what the connection
//! actor drives it with — which slot a mount reads, when an accepted envelope
//! is written back, and how long an unmounted root's slot outlives it. The two
//! are separate modules because they face opposite ways: the store is public
//! vocabulary, while none of the throttling and eviction bookkeeping here is an
//! embedder's business.
//!
//! The actor keeps the per-root half that is not cache policy at all —
//! `Root::cache_key` is mount identity and `Root::seeded` is dispatch
//! gating — and calls in at four points: [`CacheCoordinator::key`] when a root
//! is mounted, then [`on_mount`](CacheCoordinator::on_mount),
//! [`on_publish`](CacheCoordinator::on_publish) and
//! [`on_teardown`](CacheCoordinator::on_teardown). The two timers arm
//! themselves and come back as [`ActorMsg`]s the actor routes here, which is
//! what keeps every decision on the actor task and every `CacheStore` call on a
//! spawned one.
//!
//! That inbox is also why this module and the actor name each other: the actor
//! owns the loop and the root registry, this owns the slots, and the only way
//! back onto the loop is a message.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures_channel::mpsc::UnboundedSender;
use phoenix_channel::{Spawner, Timer};
use serde_json::Value;

use crate::actor::ActorMsg;
use crate::cache::{CACHE_WRITE_THROTTLE, CacheEntry, CacheStore, cache_key, now_ms};

/// The connection-wide cache settings (`docs/rust-client.md` §6.4).
#[derive(Clone)]
pub(crate) struct CacheConfig {
    pub(crate) store: Arc<dyn CacheStore>,
    pub(crate) buster: Arc<str>,
    pub(crate) gc_time: Duration,
}

impl CacheConfig {
    fn gc_ms(&self) -> u64 {
        u64::try_from(self.gc_time.as_millis()).unwrap_or(u64::MAX)
    }

    /// Whether an entry has aged out of the gc window, or was written under a
    /// different shape token.
    fn is_usable(&self, entry: &CacheEntry) -> bool {
        entry.buster == *self.buster && now_ms().saturating_sub(entry.updated_at) <= self.gc_ms()
    }
}

/// One cache slot's trailing throttle: the latest tree, and whether a flush is
/// already armed.
#[derive(Default)]
struct CacheWriter {
    pending: Option<CacheEntry>,
    armed: bool,
}

/// Every cache slot the connection is currently responsible for.
///
/// A connection with no [`CacheStore`] still has one of these; `config` is
/// `None`, which is what makes every entry point below a no-op instead of a
/// branch at each of the actor's call sites.
pub(crate) struct CacheCoordinator {
    config: Option<CacheConfig>,
    spawner: Arc<dyn Spawner>,
    timer: Arc<dyn Timer>,
    tx: UnboundedSender<ActorMsg>,
    /// Throttled writes in flight, keyed by cache slot.
    writes: HashMap<Arc<str>, CacheWriter>,
    /// Armed evictions, keyed by cache slot; the value is the only epoch whose
    /// [`ActorMsg::CacheEvict`] still counts.
    evictions: HashMap<Arc<str>, u64>,
    next_eviction_epoch: u64,
}

impl CacheCoordinator {
    /// Builds the coordinator around the seams the actor was given.
    pub(crate) fn new(
        config: Option<CacheConfig>,
        spawner: Arc<dyn Spawner>,
        timer: Arc<dyn Timer>,
        tx: UnboundedSender<ActorMsg>,
    ) -> Self {
        Self {
            config,
            spawner,
            timer,
            tx,
            writes: HashMap::new(),
            evictions: HashMap::new(),
            next_eviction_epoch: 0,
        }
    }

    /// The slot a mount of `(module, id, params)` addresses, or `None` when the
    /// connection has no cache store.
    ///
    /// The root keeps it: a slot keys on the mount params, which the root id
    /// does not, so it is the root's own identity that decides whether a read
    /// arriving later still belongs to it.
    pub(crate) fn key(&self, module: &str, id: &str, params: &Value) -> Option<Arc<str>> {
        self.config
            .as_ref()
            .map(|_| Arc::from(cache_key(module, id, params)))
    }

    /// A root was registered: cancel the eviction its own unmount armed, and
    /// read its slot off the actor task.
    ///
    /// Spawned rather than awaited: the read races the join, so a slow store
    /// delays the seed, never the revalidation (§6.4). Staleness is decided out
    /// there too, so that dropping an unusable entry costs the actor nothing
    /// and the entry that reaches it is already one it may seed.
    pub(crate) fn on_mount(&mut self, root_id: &Arc<str>, key: Arc<str>) {
        let Some(config) = self.config.clone() else {
            return;
        };

        self.evictions.remove(&key);

        let root_id = Arc::clone(root_id);
        let tx = self.tx.clone();

        self.spawner.spawn(Box::pin(async move {
            let Some(entry) = config.store.get(&key).await else {
                return;
            };

            if !config.is_usable(&entry) {
                config.store.evict(&key).await;
                return;
            }

            let _ = tx.unbounded_send(ActorMsg::CacheSeed {
                root_id,
                key,
                entry,
            });
        }));
    }

    /// An envelope was published: queue that root's tree for persistence, at
    /// most one write per [`CACHE_WRITE_THROTTLE`] per slot, always the latest
    /// tree.
    ///
    /// The *wire* tree is what is stored: seeding is then the same marker
    /// substitution the engine already does, with no second decoding path.
    pub(crate) fn on_publish(&mut self, key: Option<&Arc<str>>, document: &Value) {
        let Some(config) = self.config.clone() else {
            return;
        };
        let Some(key) = key else {
            return;
        };
        let entry = CacheEntry {
            data: document.clone(),
            updated_at: now_ms(),
            buster: config.buster.to_string(),
        };

        let writer = self.writes.entry(Arc::clone(key)).or_default();

        writer.pending = Some(entry);

        if writer.armed {
            return;
        }

        writer.armed = true;

        let timer = Arc::clone(&self.timer);
        let tx = self.tx.clone();
        let key = Arc::clone(key);

        self.spawner.spawn(Box::pin(async move {
            timer.sleep(CACHE_WRITE_THROTTLE).await;

            let _ = tx.unbounded_send(ActorMsg::CacheFlush { key });
        }));
    }

    /// A root left the registry: flush what it had, then arm the gc timer for
    /// its slot.
    ///
    /// The remaining window is measured from the entry's own `updated_at`, so
    /// an entry that was already half-expired when the root unmounted is not
    /// given a fresh full lifetime.
    pub(crate) fn on_teardown(&mut self, key: Arc<str>, connection_closed: bool) {
        let pending = self.writes.remove(&key).and_then(|writer| writer.pending);

        let Some(config) = self.config.clone() else {
            return;
        };

        // A disconnect keeps whatever was flushed: the entry ages out on its
        // own, and a reconnecting app can seed from it again.
        if connection_closed {
            if let Some(entry) = pending {
                self.write_now(key, entry);
            }

            return;
        }

        self.next_eviction_epoch += 1;
        let epoch = self.next_eviction_epoch;

        self.evictions.insert(Arc::clone(&key), epoch);

        let timer = Arc::clone(&self.timer);
        let tx = self.tx.clone();
        let gc_ms = config.gc_ms();

        self.spawner.spawn(Box::pin(async move {
            if let Some(entry) = pending {
                config.store.put(&key, entry).await;
            }

            let age = config
                .store
                .get(&key)
                .await
                .map_or(0, |entry| now_ms().saturating_sub(entry.updated_at));

            timer
                .sleep(Duration::from_millis(gc_ms.saturating_sub(age)))
                .await;

            let _ = tx.unbounded_send(ActorMsg::CacheEvict { key, epoch });
        }));
    }

    /// Writes one slot's latest tree, ending its throttle window.
    pub(crate) fn flush(&mut self, key: &Arc<str>) {
        let Some(writer) = self.writes.get_mut(key) else {
            return;
        };

        writer.armed = false;

        let Some(entry) = writer.pending.take() else {
            // Nothing accumulated during the window: the slot is idle, so it
            // stops costing a map entry.
            self.writes.remove(key);
            return;
        };

        self.write_now(Arc::clone(key), entry);
    }

    /// Drops a slot whose gc window elapsed with no root holding it.
    pub(crate) fn evict(&mut self, key: &Arc<str>, epoch: u64) {
        // A re-mount of the same slot dropped the epoch, which is how it
        // cancels the eviction its own unmount armed.
        if self.evictions.get(key) != Some(&epoch) {
            return;
        }

        self.evictions.remove(key);
        self.evict_now(Arc::clone(key));
    }

    /// Drops one slot now, ahead of its gc window: the cached tree the
    /// generated types rejected is the only caller.
    pub(crate) fn discard(&self, key: Arc<str>) {
        self.evict_now(key);
    }

    /// Fire-and-forget write. Failures are the store's to swallow (§6.4), so
    /// nothing here can throw into the patch path.
    fn write_now(&self, key: Arc<str>, entry: CacheEntry) {
        let Some(config) = self.config.clone() else {
            return;
        };

        self.spawner
            .spawn(Box::pin(async move { config.store.put(&key, entry).await }));
    }

    /// Fire-and-forget removal.
    fn evict_now(&self, key: Arc<str>) {
        let Some(config) = self.config.clone() else {
            return;
        };

        self.spawner
            .spawn(Box::pin(async move { config.store.evict(&key).await }));
    }
}
