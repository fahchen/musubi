//! The `store_id -> pointer` index over the shadow document
//! (`docs/rust-client.md` §4.3, step 5).
//!
//! The index is what makes a child store addressable: stream pruning
//! (BDR-0011), change notification (§5) and, later, per-store snapshots all key
//! off it.

use std::collections::HashMap;

use serde_json::Value;

use crate::generated::StoreId;

/// The wire key marking a store node.
pub(crate) const STORE_ID_KEY: &str = "__musubi_store_id__";

/// Where each mounted store's node sits in the shadow document, as an RFC 6901
/// pointer.
pub(crate) type StoreIndex = HashMap<StoreId, String>;

/// Walks the wire tree and indexes every store node by its server-authored id.
///
/// A node is a store node when it carries `__musubi_store_id__` as an array of
/// strings; nested stores are indexed too, so the map holds the whole tree.
pub(crate) fn build_store_index(root: &Value) -> StoreIndex {
    let mut index = StoreIndex::new();

    visit(root, &mut String::new(), &mut index);

    index
}

/// Depth-first walk, extending `pointer` in place so a deep tree costs one
/// allocation per segment rather than one string per node.
fn visit(value: &Value, pointer: &mut String, index: &mut StoreIndex) {
    match value {
        Value::Array(items) => {
            for (position, item) in items.iter().enumerate() {
                let restore = pointer.len();

                pointer.push('/');
                pointer.push_str(&position.to_string());
                visit(item, pointer, index);
                pointer.truncate(restore);
            }
        }
        Value::Object(fields) => {
            if let Some(store_id) = fields.get(STORE_ID_KEY).and_then(parse_store_id) {
                index.insert(store_id, pointer.clone());
            }

            for (key, field) in fields {
                let restore = pointer.len();

                pointer.push('/');
                pointer.push_str(&escape(key));
                visit(field, pointer, index);
                pointer.truncate(restore);
            }
        }
        _ => {}
    }
}

/// Reads a `__musubi_store_id__` value, ignoring anything that is not an array
/// of strings — a state field cannot be named `__musubi_*` (the DSL raises at
/// `state do` expansion), so a lookalike can only be data.
pub(crate) fn parse_store_id(value: &Value) -> Option<StoreId> {
    serde_json::from_value(value.clone()).ok()
}

/// Escapes one RFC 6901 reference token.
fn escape(token: &str) -> String {
    token.replace('~', "~0").replace('/', "~1")
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn indexes_every_store_node_by_pointer() {
        let index = build_store_index(&json!({
            "__musubi_store_id__": [],
            "title": "Cart",
            "panel": {
                "__musubi_store_id__": ["panel"],
                "rows": [
                    {"__musubi_store_id__": ["panel", "row-1"], "total": 1}
                ]
            }
        }));

        assert_eq!(index.get(&StoreId::root()).map(String::as_str), Some(""));
        assert_eq!(
            index.get(&store_id(&["panel"])).map(String::as_str),
            Some("/panel")
        );
        assert_eq!(
            index
                .get(&store_id(&["panel", "row-1"]))
                .map(String::as_str),
            Some("/panel/rows/0")
        );
    }

    #[test]
    fn escapes_reference_tokens() {
        let index = build_store_index(&json!({
            "a/b": {"c~d": {"__musubi_store_id__": ["nested"]}}
        }));

        assert_eq!(
            index.get(&store_id(&["nested"])).map(String::as_str),
            Some("/a~1b/c~0d")
        );
    }

    #[test]
    fn ignores_store_id_lookalikes() {
        let index = build_store_index(&json!({"__musubi_store_id__": "root", "a": 1}));

        assert!(index.is_empty());
    }

    fn store_id(segments: &[&str]) -> StoreId {
        serde_json::from_value(json!(segments)).expect("store id is a string array")
    }
}
