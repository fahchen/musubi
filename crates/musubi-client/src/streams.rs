//! Client-owned stream materialization (`docs/rust-client.md` §5).
//!
//! The server keeps no ordered key list, makes no upsert decision and does no
//! limit trimming (BDR-0018): it queues raw deltas and the client folds them.
//! This module is a deliberate op-for-op port of
//! `packages/client/src/streams.ts` — the two clients must materialize
//! identically or the same page renders differently in each.

use std::collections::{HashMap, HashSet};

use serde_json::Value;

use crate::envelope::StreamOp;
use crate::generated::StoreId;

/// One materialized entry: the server-computed item key plus the rendered item.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StreamEntry {
    /// The item's identity within its stream.
    pub(crate) item_key: String,
    /// The rendered wire item.
    pub(crate) item: Value,
}

/// The key every stream is filed under.
///
/// The TypeScript client concatenates `json(store_id) + "\0" + name` because a
/// JS `Map` keys by reference; that string is an implementation detail, not a
/// wire format, so the Rust port hashes the pair directly.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct StreamKey {
    store_id: StoreId,
    name: String,
}

/// Every materialized stream of one root, keyed by `(store_id, stream)`.
#[derive(Debug, Clone, Default)]
pub(crate) struct StreamStore {
    entries: HashMap<StreamKey, Vec<StreamEntry>>,
}

impl StreamStore {
    /// Folds one envelope's stream ops in array order.
    pub(crate) fn apply_ops(&mut self, ops: &[StreamOp]) {
        for op in ops {
            match op {
                StreamOp::Reset { stream, store_id } => {
                    self.entries.insert(Self::key(store_id, stream), Vec::new());
                }
                StreamOp::Delete {
                    stream,
                    store_id,
                    item_key,
                } => {
                    self.entries
                        .entry(Self::key(store_id, stream))
                        .or_default()
                        .retain(|entry| &entry.item_key != item_key);
                }
                StreamOp::Insert {
                    stream,
                    store_id,
                    item_key,
                    at,
                    item,
                    limit,
                } => {
                    let entries = self.entries.entry(Self::key(store_id, stream)).or_default();

                    apply_insert(entries, item_key, item, *at, *limit);
                }
            }
        }
    }

    /// Drops every stream whose owning store is gone from the freshly rebuilt
    /// index.
    ///
    /// No `reset` is emitted when a store unmounts, so pruning is what makes a
    /// reappearing store start empty (BDR-0011 fresh-mount semantics).
    pub(crate) fn prune(&mut self, live_store_ids: &HashSet<StoreId>) {
        self.entries
            .retain(|key, _| live_store_ids.contains(&key.store_id));
    }

    /// The materialized entries of one stream, in list order.
    pub(crate) fn entries(&self, store_id: &StoreId, name: &str) -> &[StreamEntry] {
        self.entries
            .get(&Self::key(store_id, name))
            .map_or(&[], Vec::as_slice)
    }

    /// Builds the lookup key. One `StoreId` clone per access is the price of
    /// hashing the pair instead of a flattened string.
    fn key(store_id: &StoreId, name: &str) -> StreamKey {
        StreamKey {
            store_id: store_id.clone(),
            name: name.to_owned(),
        }
    }
}

/// Upsert, then position, then trim — in that exact order.
///
/// An insert for an existing item key **removes** the old entry first: the
/// item is repositioned, not updated in place.
fn apply_insert(
    entries: &mut Vec<StreamEntry>,
    item_key: &str,
    item: &Value,
    at: i64,
    limit: Option<i64>,
) {
    if let Some(index) = entries.iter().position(|entry| entry.item_key == item_key) {
        entries.remove(index);
    }

    let index = insertion_index(at, entries.len());

    entries.insert(
        index,
        StreamEntry {
            item_key: item_key.to_owned(),
            item: item.clone(),
        },
    );

    trim(entries, limit, at);
}

/// Resolves `at` against the **post-removal** length.
fn insertion_index(at: i64, len: usize) -> usize {
    if at <= 0 {
        // -1 appends; 0 and every other negative prepend.
        if at == -1 { len } else { 0 }
    } else {
        usize::try_from(at).unwrap_or(usize::MAX).min(len)
    }
}

/// Trims to `limit`, dropping from the end for `at == 0` and from the front
/// otherwise.
///
/// The direction is chosen by `at`, **not** by the sign of `limit`: the server
/// writes negative limits (`-100`) by convention and the client does not read
/// that sign.
fn trim(entries: &mut Vec<StreamEntry>, limit: Option<i64>, at: i64) {
    let Some(limit) = limit else {
        return;
    };

    let size = usize::try_from(limit.unsigned_abs()).unwrap_or(usize::MAX);

    if size == 0 {
        entries.clear();
        return;
    }

    if entries.len() <= size {
        return;
    }

    if at == 0 {
        entries.truncate(size);
    } else {
        entries.drain(..entries.len() - size);
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn insert_appends_for_at_minus_one_and_prepends_for_zero() {
        let mut store = StreamStore::default();

        store.apply_ops(&[
            insert("a", -1, None),
            insert("b", -1, None),
            insert("c", 0, None),
        ]);

        assert_eq!(item_keys(&store), ["c", "a", "b"]);
    }

    #[test]
    fn insert_prepends_for_every_negative_at_other_than_minus_one() {
        let mut store = StreamStore::default();

        store.apply_ops(&[
            insert("a", -1, None),
            insert("b", -2, None),
            insert("c", -7, None),
        ]);

        assert_eq!(item_keys(&store), ["c", "b", "a"]);
    }

    #[test]
    fn insert_clamps_a_positive_at_to_the_current_length() {
        let mut store = StreamStore::default();

        store.apply_ops(&[
            insert("a", -1, None),
            insert("b", -1, None),
            insert("c", 99, None),
        ]);

        assert_eq!(item_keys(&store), ["a", "b", "c"]);

        store.apply_ops(&[insert("d", 1, None)]);

        assert_eq!(item_keys(&store), ["a", "d", "b", "c"]);
    }

    #[test]
    fn insert_removes_the_existing_key_before_resolving_the_index() {
        let mut store = StreamStore::default();

        store.apply_ops(&[
            insert("a", -1, None),
            insert("b", -1, None),
            insert("c", -1, None),
        ]);
        // Post-removal length is 2, so `at: 2` lands last, not third-of-three.
        store.apply_ops(&[insert("a", 2, None)]);

        assert_eq!(item_keys(&store), ["b", "c", "a"]);
    }

    #[test]
    fn insert_carries_the_item_verbatim_and_replaces_it_on_reinsert() {
        let mut store = StreamStore::default();

        store.apply_ops(&[insert("a", -1, None)]);

        assert_eq!(
            store.entries(&StoreId::root(), "messages"),
            [StreamEntry {
                item_key: "a".to_owned(),
                item: json!({"id": "a"})
            }]
        );

        store.apply_ops(&[StreamOp::Insert {
            stream: "messages".to_owned(),
            store_id: StoreId::root(),
            item_key: "a".to_owned(),
            at: -1,
            item: json!({"id": "a", "body": "edited"}),
            limit: None,
        }]);

        assert!(matches!(
            store.entries(&StoreId::root(), "messages"),
            [entry] if entry.item == json!({"id": "a", "body": "edited"})
        ));
    }

    #[test]
    fn a_null_limit_never_trims() {
        let mut store = StreamStore::default();

        store.apply_ops(
            &(0..5)
                .map(|n| insert(&n.to_string(), -1, None))
                .collect::<Vec<_>>(),
        );

        assert_eq!(item_keys(&store), ["0", "1", "2", "3", "4"]);
    }

    #[test]
    fn a_zero_limit_empties_the_stream() {
        let mut store = StreamStore::default();

        store.apply_ops(&[insert("a", -1, None), insert("b", -1, Some(0))]);

        assert!(item_keys(&store).is_empty());
    }

    #[test]
    fn a_limit_at_or_above_the_length_does_not_trim() {
        let mut store = StreamStore::default();

        store.apply_ops(&[insert("a", -1, Some(-2)), insert("b", -1, Some(-2))]);

        assert_eq!(item_keys(&store), ["a", "b"]);
    }

    #[test]
    fn appending_past_the_limit_drops_from_the_front() {
        let mut store = StreamStore::default();

        store.apply_ops(&[
            insert("a", -1, Some(-2)),
            insert("b", -1, Some(-2)),
            insert("c", -1, Some(-2)),
        ]);

        assert_eq!(item_keys(&store), ["b", "c"]);
    }

    #[test]
    fn prepending_past_the_limit_drops_from_the_end() {
        let mut store = StreamStore::default();

        store.apply_ops(&[
            insert("a", 0, Some(-2)),
            insert("b", 0, Some(-2)),
            insert("c", 0, Some(-2)),
        ]);

        assert_eq!(item_keys(&store), ["c", "b"]);
    }

    #[test]
    fn a_positive_at_past_the_limit_drops_from_the_front() {
        let mut store = StreamStore::default();

        store.apply_ops(&[
            insert("a", -1, Some(2)),
            insert("b", -1, Some(2)),
            insert("c", 1, Some(2)),
        ]);

        assert_eq!(item_keys(&store), ["c", "b"]);
    }

    #[test]
    fn a_positive_limit_trims_exactly_like_its_negative_twin() {
        let mut appended = StreamStore::default();
        let mut prepended = StreamStore::default();

        appended.apply_ops(&[
            insert("a", -1, Some(2)),
            insert("b", -1, Some(2)),
            insert("c", -1, Some(2)),
        ]);
        prepended.apply_ops(&[
            insert("a", 0, Some(2)),
            insert("b", 0, Some(2)),
            insert("c", 0, Some(2)),
        ]);

        assert_eq!(item_keys(&appended), ["b", "c"]);
        assert_eq!(item_keys(&prepended), ["c", "b"]);
    }

    #[test]
    fn trimming_drops_the_whole_overflow_at_once() {
        let mut store = StreamStore::default();

        store.apply_ops(
            &(0..5)
                .map(|n| insert(&n.to_string(), -1, None))
                .collect::<Vec<_>>(),
        );
        store.apply_ops(&[insert("5", -1, Some(-2))]);

        assert_eq!(item_keys(&store), ["4", "5"]);
    }

    #[test]
    fn delete_drops_every_entry_with_the_key_and_reset_empties_the_stream() {
        let mut store = StreamStore::default();

        store.apply_ops(&[insert("a", -1, None), insert("b", -1, None)]);
        store.apply_ops(&[StreamOp::Delete {
            stream: "messages".to_owned(),
            store_id: StoreId::root(),
            item_key: "a".to_owned(),
        }]);

        assert_eq!(item_keys(&store), ["b"]);

        store.apply_ops(&[StreamOp::Reset {
            stream: "messages".to_owned(),
            store_id: StoreId::root(),
        }]);

        assert!(item_keys(&store).is_empty());
    }

    #[test]
    fn streams_of_different_stores_and_names_do_not_alias() {
        let mut store = StreamStore::default();

        store.apply_ops(&[
            insert("a", -1, None),
            StreamOp::Insert {
                stream: "messages".to_owned(),
                store_id: store_id(&["panel"]),
                item_key: "b".to_owned(),
                at: -1,
                item: json!({"id": "b"}),
                limit: None,
            },
            StreamOp::Insert {
                stream: "alerts".to_owned(),
                store_id: StoreId::root(),
                item_key: "c".to_owned(),
                at: -1,
                item: json!({"id": "c"}),
                limit: None,
            },
        ]);

        assert_eq!(item_keys(&store), ["a"]);
        assert_eq!(keys_of(&store, &store_id(&["panel"]), "messages"), ["b"]);
        assert_eq!(keys_of(&store, &StoreId::root(), "alerts"), ["c"]);
    }

    #[test]
    fn prune_drops_only_the_streams_of_vanished_stores() {
        let mut store = StreamStore::default();

        store.apply_ops(&[
            insert("a", -1, None),
            StreamOp::Insert {
                stream: "messages".to_owned(),
                store_id: store_id(&["panel"]),
                item_key: "b".to_owned(),
                at: -1,
                item: json!({"id": "b"}),
                limit: None,
            },
        ]);
        store.prune(&HashSet::from([StoreId::root()]));

        assert_eq!(item_keys(&store), ["a"]);
        assert!(store.entries(&store_id(&["panel"]), "messages").is_empty());
    }

    fn insert(item_key: &str, at: i64, limit: Option<i64>) -> StreamOp {
        StreamOp::Insert {
            stream: "messages".to_owned(),
            store_id: StoreId::root(),
            item_key: item_key.to_owned(),
            at,
            item: json!({"id": item_key}),
            limit,
        }
    }

    fn item_keys(store: &StreamStore) -> Vec<String> {
        keys_of(store, &StoreId::root(), "messages")
    }

    fn keys_of(store: &StreamStore, store_id: &StoreId, name: &str) -> Vec<String> {
        store
            .entries(store_id, name)
            .iter()
            .map(|entry| entry.item_key.clone())
            .collect()
    }

    fn store_id(segments: &[&str]) -> StoreId {
        serde_json::from_value(json!(segments)).expect("store id is a string array")
    }
}
