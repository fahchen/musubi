//! The reference file-backed [`CacheStore`] (`docs/rust-client.md` §6.4).
//!
//! `musubi-client` ships [`MemoryCacheStore`](musubi_client::MemoryCacheStore)
//! only, on purpose: where cached trees survive a restart is a runtime decision
//! the crate does not make. This file is the durable answer for a desktop app —
//! one `serde_json` file, whole-map writes — and like `transport.rs` it is
//! written to be **copied verbatim** into other embedders.
//!
//! Three properties carry the design:
//!
//! - **One JSON file, rewritten whole on every write.** The crate already
//!   trailing-throttles puts to one per second per burst
//!   (`CACHE_WRITE_THROTTLE`), and a mount cache holds one entry per mounted
//!   root — a handful of small trees — so an append log or a file-per-key
//!   layout would buy nothing here.
//! - **A missing, corrupt or unreadable file is an empty cache, never an
//!   error.** The [`CacheStore`] contract makes every method infallible in the
//!   signature: a store that cannot read returns `None` and the mount degrades
//!   to a cold one. Failures are logged via `tracing` (an embedder's subscriber
//!   picks them up; this example installs none) and the next put overwrites the
//!   bad file.
//! - **Writes go to a sibling temp file, then rename.** A crash mid-write
//!   leaves the previous file intact instead of half a JSON document — and even
//!   if that ever failed, the bullet above turns the damage into a cold mount.
//!
//! The IO is synchronous inside the trait's methods, exactly like
//! `MemoryCacheStore`'s map access: the file is a few kilobytes, nothing here
//! is on the socket's critical path, and pulling in an async-fs crate for one
//! demo file would be the wrong trade. An embedder with a bigger cache moves
//! the `fs` calls into the returned future on its blocking pool.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::{fs, io};

use futures::future::BoxFuture;
use musubi_client::{CacheEntry, CacheStore};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Where the cache lives: `~/.chat-room-desktop-cache.json`.
///
/// A dot-file under the home directory is the simplest spot that is actually
/// durable: `std::env::temp_dir()` is periodically cleaned (macOS purges it
/// after ~3 days idle), and "next to the binary" is `target/` here — wiped by
/// `cargo clean` — or a read-only install dir elsewhere. A production app would
/// use the platform data directory (the `dirs` crate); one file for a demo does
/// not need the dependency. A homeless environment falls back to the temp dir,
/// which merely degrades relaunches there to cold mounts.
pub fn default_path() -> PathBuf {
    std::env::home_dir()
        .unwrap_or_else(std::env::temp_dir)
        .join(".chat-room-desktop-cache.json")
}

/// The on-disk shape of one entry — [`CacheEntry`] field for field.
///
/// Mirrored rather than serialized directly because `CacheEntry` deliberately
/// derives no serde traits: the wire format of a durable store is the
/// embedder's contract, not the crate's.
#[derive(Serialize, Deserialize)]
struct PersistedEntry {
    data: Value,
    updated_at: u64,
    buster: String,
}

/// A [`CacheStore`] over one JSON file.
///
/// The file is read once, at [`open`](Self::open); after that the map lives in
/// memory and every mutation rewrites the file. The `Mutex` covers both, so two
/// throttled puts cannot interleave their write-then-rename.
pub struct FileCacheStore {
    entries: Mutex<HashMap<String, CacheEntry>>,
    path: PathBuf,
}

impl FileCacheStore {
    /// Opens the store at `path`, adopting whatever a previous run left there.
    ///
    /// Never fails: a missing file is a first run, and anything else wrong with
    /// it (unreadable, not JSON, not this shape) is logged and treated as
    /// empty.
    pub fn open(path: PathBuf) -> Self {
        let entries = match fs::read(&path) {
            Ok(bytes) => match serde_json::from_slice::<HashMap<String, PersistedEntry>>(&bytes) {
                Ok(persisted) => persisted
                    .into_iter()
                    .map(|(key, entry)| {
                        (
                            key,
                            CacheEntry {
                                data: entry.data,
                                updated_at: entry.updated_at,
                                buster: entry.buster,
                            },
                        )
                    })
                    .collect(),
                Err(error) => {
                    tracing::warn!(?path, %error, "cache file is corrupt; starting empty");
                    HashMap::new()
                }
            },
            Err(error) if error.kind() == io::ErrorKind::NotFound => HashMap::new(),
            Err(error) => {
                tracing::warn!(?path, %error, "cache file is unreadable; starting empty");
                HashMap::new()
            }
        };

        Self {
            entries: Mutex::new(entries),
            path,
        }
    }

    /// The map, poisoned or not: every write under the lock is a whole-value
    /// insert or remove, so the map is coherent even after a panic — and a
    /// cache degrades, it never panics twice.
    fn lock(&self) -> MutexGuard<'_, HashMap<String, CacheEntry>> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Serializes the whole map to `path`, via sibling-temp-file + rename.
    ///
    /// Called with the lock held. Failure is logged and swallowed: an
    /// unwritable cache means the *next* launch mounts cold, nothing more.
    fn save(&self, entries: &HashMap<String, CacheEntry>) {
        let persisted: HashMap<&String, PersistedEntry> = entries
            .iter()
            .map(|(key, entry)| {
                (
                    key,
                    PersistedEntry {
                        data: entry.data.clone(),
                        updated_at: entry.updated_at,
                        buster: entry.buster.clone(),
                    },
                )
            })
            .collect();

        // Pretty on purpose: the file is part of the demo, and small enough
        // that inspecting it beats saving bytes.
        let json = serde_json::to_vec_pretty(&persisted).expect("Value maps serialize");
        let staging = self.path.with_extension("tmp");
        let result = fs::write(&staging, json).and_then(|()| fs::rename(&staging, &self.path));

        if let Err(error) = result {
            tracing::warn!(path = ?self.path, %error, "cache write failed; entry kept in memory");
        }
    }
}

impl CacheStore for FileCacheStore {
    fn get(&self, key: &str) -> BoxFuture<'static, Option<CacheEntry>> {
        let entry = self.lock().get(key).cloned();

        Box::pin(async move { entry })
    }

    fn put(&self, key: &str, entry: CacheEntry) -> BoxFuture<'static, ()> {
        let mut entries = self.lock();

        entries.insert(key.to_owned(), entry);
        self.save(&entries);

        Box::pin(async {})
    }

    fn evict(&self, key: &str) -> BoxFuture<'static, ()> {
        let mut entries = self.lock();

        // Evicting an absent key must not touch the file — teardown gc fires
        // for slots that may never have been written.
        if entries.remove(key).is_some() {
            self.save(&entries);
        }

        Box::pin(async {})
    }
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;
    use musubi_client::now_ms;
    use serde_json::json;

    use super::*;

    /// A unique throwaway path per test, so `cargo test`'s parallel runners
    /// cannot race each other on one file.
    fn scratch(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "chat-room-cache-{}-{name}.json",
            std::process::id()
        ))
    }

    #[test]
    fn round_trips_across_a_reopen_and_evicts() {
        let path = scratch("round-trip");
        let entry = CacheEntry {
            data: json!({"__musubi_store_id__": [], "title": "Cached"}),
            updated_at: now_ms(),
            buster: "v1".to_owned(),
        };

        let store = FileCacheStore::open(path.clone());
        block_on(store.put("general|Store|{}", entry.clone()));
        drop(store);

        // A fresh open — the restart — reads what the last one wrote.
        let store = FileCacheStore::open(path.clone());
        assert_eq!(block_on(store.get("general|Store|{}")), Some(entry));

        block_on(store.evict("general|Store|{}"));
        drop(store);

        let store = FileCacheStore::open(path.clone());
        assert_eq!(block_on(store.get("general|Store|{}")), None);

        fs::remove_file(path).ok();
    }

    #[test]
    fn a_corrupt_file_is_an_empty_cache_and_is_overwritten_by_the_next_put() {
        let path = scratch("corrupt");
        fs::write(&path, b"not json{").unwrap();

        let store = FileCacheStore::open(path.clone());
        assert_eq!(block_on(store.get("general|Store|{}")), None);

        let entry = CacheEntry {
            data: json!({"ok": true}),
            updated_at: now_ms(),
            buster: String::new(),
        };
        block_on(store.put("general|Store|{}", entry.clone()));
        drop(store);

        let store = FileCacheStore::open(path.clone());
        assert_eq!(block_on(store.get("general|Store|{}")), Some(entry));

        fs::remove_file(path).ok();
    }
}
