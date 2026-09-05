//! The marker lookalike rules (`docs/rust-reactive-state.md` §3.5).
//!
//! A state field cannot be named `__musubi_*` — `Musubi.DSL.Field.validate_reserved!/1`
//! raises when `state do` expands — so a value that merely *looks* like a
//! marker can only be data, and the rules below are what keep it from being
//! eaten.

use serde_json::{Map, Value};

use crate::node::AsyncStatus;
use crate::wire::StoreId;

/// The wire key marking a store node.
pub(crate) const STORE_ID_KEY: &str = "__musubi_store_id__";
/// The wire key marking a stream slot.
pub(crate) const STREAM_MARKER_KEY: &str = "__musubi_stream__";
/// The wire key marking an upload slot.
pub(crate) const UPLOAD_MARKER_KEY: &str = "__musubi_upload__";
/// The wire key discriminating an async node.
pub(crate) const ASYNC_MARKER_KEY: &str = "__musubi_async__";

/// The key an async node's status is rendered under — the one key of an async
/// node that is the node's **own** semantics rather than a child (§3.3).
pub(crate) const ASYNC_STATUS_KEY: &str = "status";

/// A borrowable `null`, for an async node the server rendered without one of
/// its two payload keys.
static NULL: Value = Value::Null;

/// What an incoming [`Value`] is, once the markers have been read.
///
/// Borrowed from the value it classifies: reconciliation decides whether the
/// target node can be kept before it copies anything.
#[derive(Debug)]
pub(crate) enum Shape<'a> {
    Null,
    Bool(bool),
    Number(&'a serde_json::Number),
    String(&'a str),
    Array(&'a Vec<Value>),
    Object(&'a Map<String, Value>),
    Store {
        store_id: StoreId,
        fields: &'a Map<String, Value>,
    },
    Collection {
        name: &'a str,
    },
    Async {
        status: AsyncStatus,
        result: &'a Value,
        reason: &'a Value,
    },
    UploadSlot {
        name: &'a str,
    },
}

/// Classifies one incoming wire value.
///
/// Order matters: the three single-key markers are checked before the async
/// discriminator and the store id, because each of them is a *whole* node
/// rather than a key inside one.
pub(crate) fn classify(value: &Value) -> Shape<'_> {
    match value {
        Value::Null => Shape::Null,
        Value::Bool(flag) => Shape::Bool(*flag),
        Value::Number(number) => Shape::Number(number),
        Value::String(text) => Shape::String(text),
        Value::Array(items) => Shape::Array(items),
        Value::Object(fields) => classify_object(fields),
    }
}

fn classify_object(fields: &Map<String, Value>) -> Shape<'_> {
    if let Some(name) = sole_string_marker(fields, STREAM_MARKER_KEY) {
        return Shape::Collection { name };
    }

    if let Some(name) = sole_string_marker(fields, UPLOAD_MARKER_KEY) {
        return Shape::UploadSlot { name };
    }

    if let Some(status) = async_status(fields) {
        return Shape::Async {
            status,
            result: fields.get("result").unwrap_or(&NULL),
            reason: fields.get("reason").unwrap_or(&NULL),
        };
    }

    if let Some(store_id) = fields.get(STORE_ID_KEY).and_then(parse_store_id) {
        return Shape::Store { store_id, fields };
    }

    Shape::Object(fields)
}

/// Reads a marker: an object whose **only** key is `key`, with a string value.
///
/// The single-key rule is what keeps a lookalike (a rendered map that happens
/// to carry the marker key alongside others) from being eaten.
fn sole_string_marker<'a>(fields: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
    if fields.len() != 1 {
        return None;
    }

    fields.get(key)?.as_str()
}

/// Reads the async discriminator: `__musubi_async__: true` **and** a `status`
/// this build knows.
///
/// Both halves are required, so an ordinary map that happens to carry
/// `status` / `result` / `reason` is never mistaken for an async node
/// (`docs/client-contract.md`).
fn async_status(fields: &Map<String, Value>) -> Option<AsyncStatus> {
    if fields.get(ASYNC_MARKER_KEY)? != &Value::Bool(true) {
        return None;
    }

    AsyncStatus::from_wire(fields.get(ASYNC_STATUS_KEY)?.as_str()?)
}

/// Reads a `__musubi_store_id__` value, ignoring anything that is not an array
/// of strings.
pub(crate) fn parse_store_id(value: &Value) -> Option<StoreId> {
    serde_json::from_value(value.clone()).ok()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn a_sole_string_marker_is_a_slot_and_a_lookalike_is_data() {
        assert!(matches!(
            classify(&json!({"__musubi_stream__": "messages"})),
            Shape::Collection { name: "messages" }
        ));
        assert!(matches!(
            classify(&json!({"__musubi_upload__": "avatar"})),
            Shape::UploadSlot { name: "avatar" }
        ));

        // Two keys, or a non-string value: data.
        assert!(matches!(
            classify(&json!({"__musubi_stream__": "messages", "other": 1})),
            Shape::Object(_)
        ));
        assert!(matches!(
            classify(&json!({"__musubi_stream__": 7})),
            Shape::Object(_)
        ));
        assert!(matches!(
            classify(&json!({"__musubi_upload__": {"__musubi_upload__": "a"}})),
            Shape::Object(_)
        ));
    }

    #[test]
    fn an_async_node_needs_both_the_discriminator_and_a_known_status() {
        assert!(matches!(
            classify(
                &json!({"__musubi_async__": true, "status": "ok", "result": 1, "reason": null})
            ),
            Shape::Async {
                status: AsyncStatus::Ok,
                ..
            }
        ));
        assert!(matches!(
            classify(&json!({"status": "ok", "result": 1, "reason": null})),
            Shape::Object(_)
        ));
        assert!(matches!(
            classify(&json!({"__musubi_async__": true, "status": "queued"})),
            Shape::Object(_)
        ));
        assert!(matches!(
            classify(&json!({"__musubi_async__": "yes", "status": "ok"})),
            Shape::Object(_)
        ));
    }

    #[test]
    fn a_store_id_is_read_only_as_an_array_of_strings() {
        assert!(matches!(
            classify(&json!({"__musubi_store_id__": ["panel"], "total": 1})),
            Shape::Store { .. }
        ));
        assert!(matches!(
            classify(&json!({"__musubi_store_id__": "root", "total": 1})),
            Shape::Object(_)
        ));
    }

    #[test]
    fn an_async_marker_wins_over_a_store_id_on_the_same_object() {
        // The server never renders both; the order is pinned so the classifier
        // has one answer rather than two.
        assert!(matches!(
            classify(&json!({
                "__musubi_async__": true,
                "__musubi_store_id__": [],
                "status": "loading",
                "result": null,
                "reason": null
            })),
            Shape::Async { .. }
        ));
    }
}
