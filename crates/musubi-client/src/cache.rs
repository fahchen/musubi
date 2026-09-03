//! The stale-while-revalidate mount cache (`docs/rust-client.md` §6.4).
//!
//! A cached entry holds the **wire tree** — the shadow document exactly as it
//! arrived, stream markers and all — so seeding is the same substitution the
//! patch engine already does, not a second decoding path. Entries are keyed by
//! the mount identity `(module, id, params)`, which is what makes two mounts of
//! the same store with different params two cache slots.
//!
//! The peer of `packages/client/src/cache.ts`. Two deliberate differences:
//!
//! - The store is **connection-wide** ([`ConnectionBuilder::cache`]) rather
//!   than per-mount, so there is no per-mount options object and no
//!   `initialData` seed.
//! - A store reports failure by returning `None` / doing nothing, rather than
//!   by raising: a cache is an optimization, and a broken one must degrade to a
//!   cold mount instead of failing the mount.
//!
//! [`ConnectionBuilder::cache`]: crate::ConnectionBuilder::cache

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_core::future::BoxFuture;
use serde_json::Value;

use crate::lock;

/// How long an entry stays usable after its last write. Mirrors
/// `packages/client/src/cache.ts`'s `DEFAULT_GC_MS`.
pub const DEFAULT_CACHE_GC_TIME: Duration = Duration::from_secs(300);

/// The trailing-throttle window for cache writes: a burst of envelopes costs at
/// most one write per interval, always the latest tree.
pub const CACHE_WRITE_THROTTLE: Duration = Duration::from_secs(1);

/// One cached mount: the wire tree, when it was written, and the shape token it
/// was written under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheEntry {
    /// The shadow document — the wire tree with its `__musubi_store_id__` and
    /// `__musubi_stream__` markers intact.
    pub data: Value,
    /// Wall-clock milliseconds since the Unix epoch, as [`now_ms`] reports it.
    pub updated_at: u64,
    /// The [`buster`](crate::ConnectionBuilder::cache_buster) the entry was
    /// written under; an entry whose buster no longer matches is discarded
    /// rather than seeded.
    pub buster: String,
}

/// Where cached mounts are kept.
///
/// The crate ships [`MemoryCacheStore`] only: a durable store is the embedder's
/// job, because the file system, the keychain and the browser's storage are all
/// runtime decisions this crate does not make.
///
/// Every method is fallible in practice and infallible in the signature. An
/// implementation that cannot read reports `None`, and one that cannot write
/// does nothing — the caller then degrades to a cold mount, which is always
/// correct. Nothing here is on the socket's critical path: reads race the live
/// join, and writes are throttled and fire-and-forget.
pub trait CacheStore: Send + Sync + 'static {
    /// Reads one entry. Staleness is the caller's check, not the store's.
    fn get(&self, key: &str) -> BoxFuture<'static, Option<CacheEntry>>;

    /// Writes one entry, replacing whatever was under `key`.
    fn put(&self, key: &str, entry: CacheEntry) -> BoxFuture<'static, ()>;

    /// Removes one entry. Removing a key that is not there is not an error.
    fn evict(&self, key: &str) -> BoxFuture<'static, ()>;
}

// So an embedder can share one store with its own code without a newtype.
impl<T: CacheStore + ?Sized> CacheStore for Arc<T> {
    fn get(&self, key: &str) -> BoxFuture<'static, Option<CacheEntry>> {
        (**self).get(key)
    }

    fn put(&self, key: &str, entry: CacheEntry) -> BoxFuture<'static, ()> {
        (**self).put(key, entry)
    }

    fn evict(&self, key: &str) -> BoxFuture<'static, ()> {
        (**self).evict(key)
    }
}

/// A process-local [`CacheStore`].
///
/// Survives unmount/remount within one process, which is what makes a
/// re-entered screen render instantly; it does not survive a restart. Cloning
/// the `Arc` shares the map.
///
/// ```
/// use futures_executor::block_on;
/// use musubi_client::{CacheEntry, CacheStore, MemoryCacheStore, now_ms};
/// use serde_json::json;
///
/// let store = MemoryCacheStore::new();
/// let entry = CacheEntry {
///     data: json!({"title": "Cart"}),
///     updated_at: now_ms(),
///     buster: "v1".to_owned(),
/// };
///
/// block_on(store.put("cart|MyApp.CartStore|{}", entry.clone()));
///
/// assert_eq!(block_on(store.get("cart|MyApp.CartStore|{}")), Some(entry));
///
/// block_on(store.evict("cart|MyApp.CartStore|{}"));
///
/// assert_eq!(block_on(store.get("cart|MyApp.CartStore|{}")), None);
/// ```
#[derive(Debug, Default)]
pub struct MemoryCacheStore {
    entries: Mutex<HashMap<String, CacheEntry>>,
}

impl MemoryCacheStore {
    /// An empty store.
    ///
    /// ```
    /// use futures_executor::block_on;
    /// use musubi_client::{CacheStore, MemoryCacheStore};
    ///
    /// assert_eq!(block_on(MemoryCacheStore::new().get("cart")), None);
    /// ```
    pub fn new() -> Self {
        Self::default()
    }
}

impl CacheStore for MemoryCacheStore {
    fn get(&self, key: &str) -> BoxFuture<'static, Option<CacheEntry>> {
        let entry = lock(&self.entries).get(key).cloned();

        Box::pin(async move { entry })
    }

    fn put(&self, key: &str, entry: CacheEntry) -> BoxFuture<'static, ()> {
        lock(&self.entries).insert(key.to_owned(), entry);

        Box::pin(async {})
    }

    fn evict(&self, key: &str) -> BoxFuture<'static, ()> {
        lock(&self.entries).remove(key);

        Box::pin(async {})
    }
}

/// The cache slot one mount addresses: `"<id>|<module>|<canonical params>"`.
///
/// Params are canonicalized (keys sorted, recursively), so the declaration
/// order of a `Params` struct's fields cannot fork one store into two slots.
///
/// It agrees with `storeCacheKey` in `packages/client/src/cache.ts` for
/// object-valued params over non-float scalars — which is every generated
/// `Params` struct, whose unset optional attrs serialize to absent keys just as
/// TypeScript omits them. It is *not* byte-identical beyond that: a TypeScript
/// mount with no params at all canonicalizes to `null` where Rust always has an
/// object (`Params {}` ⇒ `{}`), and floats render the way `serde_json` renders
/// them (`1.0`, not `JSON.stringify`'s `1`). Two clients may share one durable
/// backing store on those terms.
///
/// ```
/// use musubi_client::cache_key;
/// use serde_json::json;
///
/// assert_eq!(
///     cache_key("MyApp.CartStore", "cart", &json!({"b": 2, "a": 1})),
///     cache_key("MyApp.CartStore", "cart", &json!({"a": 1, "b": 2}))
/// );
///
/// assert_eq!(
///     cache_key("MyApp.CartStore", "cart", &json!({"currency": "EUR"})),
///     r#"cart|MyApp.CartStore|{"currency":"EUR"}"#
/// );
/// ```
pub fn cache_key(module: &str, id: &str, params: &Value) -> String {
    let mut key = format!("{id}|{module}|");

    write_canonical(params, &mut key);

    key
}

/// Wall-clock milliseconds since the Unix epoch — the [`CacheEntry`] time base.
///
/// A clock before the epoch reads as `0`, which only ever makes an entry look
/// stale.
///
/// ```
/// assert!(musubi_client::now_ms() > 0);
/// ```
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
        })
}

/// Serializes `value` with object keys in sorted order.
///
/// `serde_json`'s default `Map` is already sorted, but the `preserve_order`
/// feature is additive: any crate in the graph enabling it would otherwise
/// silently fork one store's cache slot in two.
fn write_canonical(value: &Value, out: &mut String) {
    match value {
        Value::Object(fields) => {
            let mut keys: Vec<&String> = fields.keys().collect();
            keys.sort_unstable();

            out.push('{');

            for (position, key) in keys.into_iter().enumerate() {
                if position > 0 {
                    out.push(',');
                }

                write_json_string(key, out);
                out.push(':');
                write_canonical(&fields[key], out);
            }

            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');

            for (position, item) in items.iter().enumerate() {
                if position > 0 {
                    out.push(',');
                }

                write_canonical(item, out);
            }

            out.push(']');
        }
        Value::String(text) => write_json_string(text, out),
        // The remaining scalars have exactly one JSON spelling, and `Value`'s
        // `Display` is that spelling.
        scalar => out.push_str(&scalar.to_string()),
    }
}

/// Appends `text` as a JSON string literal, escaped the way `serde_json` writes
/// one.
fn write_json_string(text: &str, out: &mut String) {
    out.push_str(&Value::String(text.to_owned()).to_string());
}
