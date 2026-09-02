//! The hydration walk (`docs/rust-client.md` §4.6).
//!
//! Rust is nominal, so a generated `State` struct cannot resolve a stream
//! marker on its own: the marker carries a name but no `store_id`, and serde
//! has no ambient context. The engine therefore walks the patched shadow
//! document once per accepted envelope, tracking the nearest enclosing
//! `__musubi_store_id__`, and substitutes the materialized array for each
//! stream marker before serde ever runs.
//!
//! What the walk deliberately does **not** touch:
//!
//! * Upload slots (`{"__musubi_upload__": name}`) — the generated field type
//!   is the inert [`UploadSlot`](crate::generated::UploadSlot), which
//!   deserializes from the marker as-is (§10).
//! * Async nodes — [`AsyncResult`](crate::generated::AsyncResult) is an
//!   internally-tagged enum that deserializes the wire shape directly, so the
//!   node needs no rewriting. Markers *inside* its `result` are still
//!   rewritten by the ordinary recursion, which is what makes `stream_async`
//!   render as `AsyncResult<Vec<Item>>`.
//!
//! The walk produces an owned copy. The shadow document is never hydrated in
//! place: patch pointers address the wire tree, so the wire tree must stay
//! pristine across cycles.

use serde_json::{Map, Value};

use crate::generated::StoreId;
use crate::index::{STORE_ID_KEY, parse_store_id};
use crate::streams::{StreamEntry, StreamStore};

/// The wire key marking a stream slot.
const STREAM_MARKER_KEY: &str = "__musubi_stream__";

/// Substitutes every stream marker in `doc` with its materialized array.
pub(crate) fn hydrate(doc: &Value, streams: &StreamStore) -> Value {
    walk(doc, &StoreId::root(), streams)
}

/// Recurses, rewriting stream markers and re-basing the nearest-enclosing
/// store id on the way down.
fn walk(value: &Value, store_id: &StoreId, streams: &StreamStore) -> Value {
    match value {
        Value::Array(items) => Value::Array(
            items
                .iter()
                .map(|item| walk(item, store_id, streams))
                .collect(),
        ),
        Value::Object(fields) => {
            if let Some(name) = stream_marker(fields) {
                return materialize(streams.entries(store_id, name));
            }

            let context = fields
                .get(STORE_ID_KEY)
                .and_then(parse_store_id)
                .unwrap_or_else(|| store_id.clone());

            Value::Object(
                fields
                    .iter()
                    .map(|(key, field)| (key.clone(), walk(field, &context, streams)))
                    .collect(),
            )
        }
        other => other.clone(),
    }
}

/// Reads a stream marker: an object whose **only** key is
/// `__musubi_stream__`, with a string value.
///
/// The single-key rule is what keeps a lookalike (a rendered map that happens
/// to carry a `__musubi_stream__` field alongside others) from being eaten.
fn stream_marker(fields: &Map<String, Value>) -> Option<&str> {
    if fields.len() != 1 {
        return None;
    }

    fields.get(STREAM_MARKER_KEY)?.as_str()
}

/// Renders the materialized entries as the JSON array the generated `Vec<T>`
/// field deserializes from.
///
/// Items are substituted verbatim: they are rendered wire terms owned by the
/// stream, not part of the store tree the walk is re-basing.
fn materialize(entries: &[StreamEntry]) -> Value {
    Value::Array(entries.iter().map(|entry| entry.item.clone()).collect())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::envelope::StreamOp;

    #[test]
    fn substitutes_a_marker_at_the_root_of_the_store() {
        let streams = seeded(StoreId::root(), "messages", &["a", "b"]);

        assert_eq!(
            hydrate(
                &json!({"__musubi_store_id__": [], "messages": {"__musubi_stream__": "messages"}}),
                &streams
            ),
            json!({
                "__musubi_store_id__": [],
                "messages": [{"id": "a"}, {"id": "b"}]
            })
        );
    }

    #[test]
    fn substitutes_markers_at_every_nesting_depth_and_inside_arrays() {
        let streams = seeded(StoreId::root(), "messages", &["a"]);

        assert_eq!(
            hydrate(
                &json!({
                    "__musubi_store_id__": [],
                    "deep": {"deeper": {"deepest": {"__musubi_stream__": "messages"}}},
                    "list": [{"__musubi_stream__": "messages"}, 7]
                }),
                &streams
            ),
            json!({
                "__musubi_store_id__": [],
                "deep": {"deeper": {"deepest": [{"id": "a"}]}},
                "list": [[{"id": "a"}], 7]
            })
        );
    }

    #[test]
    fn resolves_a_marker_against_the_nearest_enclosing_store() {
        let mut streams = seeded(StoreId::root(), "messages", &["root"]);

        streams.apply_ops(&[StreamOp::Insert {
            stream: "messages".to_owned(),
            store_id: store_id(&["panel"]),
            item_key: "child".to_owned(),
            at: -1,
            item: json!({"id": "child"}),
            limit: None,
        }]);

        assert_eq!(
            hydrate(
                &json!({
                    "__musubi_store_id__": [],
                    "messages": {"__musubi_stream__": "messages"},
                    "panel": {
                        "__musubi_store_id__": ["panel"],
                        "messages": {"__musubi_stream__": "messages"}
                    }
                }),
                &streams
            ),
            json!({
                "__musubi_store_id__": [],
                "messages": [{"id": "root"}],
                "panel": {
                    "__musubi_store_id__": ["panel"],
                    "messages": [{"id": "child"}]
                }
            })
        );
    }

    #[test]
    fn substitutes_a_marker_inside_an_async_result_and_leaves_the_node_alone() {
        let streams = seeded(StoreId::root(), "messages", &["a"]);

        assert_eq!(
            hydrate(
                &json!({
                    "__musubi_store_id__": [],
                    "feed": {
                        "__musubi_async__": true,
                        "status": "ok",
                        "result": {"__musubi_stream__": "messages"},
                        "reason": null
                    }
                }),
                &streams
            ),
            json!({
                "__musubi_store_id__": [],
                "feed": {
                    "__musubi_async__": true,
                    "status": "ok",
                    "result": [{"id": "a"}],
                    "reason": null
                }
            })
        );
    }

    #[test]
    fn an_unknown_stream_materializes_as_an_empty_array() {
        assert_eq!(
            hydrate(
                &json!({"__musubi_store_id__": [], "messages": {"__musubi_stream__": "absent"}}),
                &StreamStore::default()
            ),
            json!({"__musubi_store_id__": [], "messages": []})
        );
    }

    #[test]
    fn leaves_marker_lookalikes_untouched() {
        let streams = seeded(StoreId::root(), "messages", &["a"]);

        assert_eq!(
            hydrate(
                &json!({
                    "__musubi_store_id__": [],
                    "two_keys": {"__musubi_stream__": "messages", "other": 1},
                    "not_a_string": {"__musubi_stream__": 7},
                    "upload": {"__musubi_upload__": "avatar"},
                    "nested_lookalike": {"__musubi_stream__": {"__musubi_stream__": "messages"}}
                }),
                &streams
            ),
            json!({
                "__musubi_store_id__": [],
                "two_keys": {"__musubi_stream__": "messages", "other": 1},
                "not_a_string": {"__musubi_stream__": 7},
                "upload": {"__musubi_upload__": "avatar"},
                "nested_lookalike": {"__musubi_stream__": [{"id": "a"}]}
            })
        );
    }

    fn seeded(store_id: StoreId, name: &str, item_keys: &[&str]) -> StreamStore {
        let mut streams = StreamStore::default();

        streams.apply_ops(
            &item_keys
                .iter()
                .map(|item_key| StreamOp::Insert {
                    stream: name.to_owned(),
                    store_id: store_id.clone(),
                    item_key: (*item_key).to_owned(),
                    at: -1,
                    item: json!({"id": item_key}),
                    limit: None,
                })
                .collect::<Vec<_>>(),
        );

        streams
    }

    fn store_id(segments: &[&str]) -> StoreId {
        serde_json::from_value(json!(segments)).expect("store id is a string array")
    }
}
