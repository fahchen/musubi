//! The patch envelope wire shape and its decode-time validation
//! (`docs/rust-client.md` §4.4).
//!
//! Unknown fields are ignored everywhere — the server is allowed to add keys,
//! and `deny_unknown_fields` would break on the very first one.

use serde::{Deserialize, Deserializer};
use serde_json::Value;

use crate::error::{MusubiError, PatchError, Result};
use crate::generated::StoreId;

/// The envelope discriminator every `"patch"` push carries.
const ENVELOPE_TYPE: &str = "patch";

/// What an `add`/`replace` op without a `value` fails with.
const MISSING_VALUE: MusubiError = MusubiError::Protocol("add and replace ops must carry a value");

/// One decoded, validated patch envelope.
///
/// Build one with [`PatchEnvelope::decode`]; the struct has no `Deserialize`
/// impl of its own because the op allowlist is part of decoding.
#[derive(Debug, Clone, PartialEq)]
pub struct PatchEnvelope {
    /// The root this envelope belongs to. Validated by the caller against the
    /// root it mounted, then ignored.
    pub root_id: String,
    /// The version this envelope was computed against.
    pub base_version: u64,
    /// The version the root reaches once this envelope is applied.
    pub version: u64,
    /// The RFC 6902 ops, already restricted to the allowlist.
    pub ops: Vec<PatchOp>,
    /// The stream deltas, in flush order (parent store first).
    pub stream_ops: Vec<StreamOp>,
    /// The upload deltas. Parsed and discarded in v1 (`docs/rust-client.md` §10).
    pub upload_ops: Vec<UploadOp>,
    /// The transient push events (BDR-0032) dispatched after state is applied.
    pub events: Vec<PushEvent>,
}

impl PatchEnvelope {
    /// Decodes a `"patch"` push payload, rejecting anything outside the
    /// `add`/`remove`/`replace` allowlist (BDR-0014).
    ///
    /// ```
    /// use musubi_client::PatchEnvelope;
    /// use serde_json::json;
    ///
    /// let envelope = PatchEnvelope::decode(json!({
    ///     "type": "patch",
    ///     "root_id": "MyApp.CartStore:cart",
    ///     "base_version": 0,
    ///     "version": 1,
    ///     "ops": [{"op": "replace", "path": "", "value": {"title": "Cart"}}]
    /// }))
    /// .unwrap();
    ///
    /// assert_eq!(envelope.version, 1);
    /// assert!(envelope.stream_ops.is_empty());
    /// ```
    ///
    /// A `move`, `copy` or `test` op is a protocol violation — the server
    /// emits a pure minimal structural diff and never falls back to one:
    ///
    /// ```
    /// use musubi_client::{MusubiError, PatchEnvelope, PatchError};
    /// use serde_json::json;
    ///
    /// let error = PatchEnvelope::decode(json!({
    ///     "type": "patch",
    ///     "root_id": "MyApp.CartStore:cart",
    ///     "base_version": 1,
    ///     "version": 2,
    ///     "ops": [{"op": "move", "from": "/a", "path": "/b"}]
    /// }))
    /// .unwrap_err();
    ///
    /// assert!(matches!(
    ///     error,
    ///     MusubiError::Patch(PatchError::UnsupportedOp { op }) if op == "move"
    /// ));
    /// ```
    pub fn decode(payload: Value) -> Result<Self> {
        let raw: RawEnvelope = serde_json::from_value(payload)
            .map_err(|_| MusubiError::Protocol("payload is not a patch envelope"))?;

        if raw.r#type != ENVELOPE_TYPE {
            return Err(MusubiError::Protocol(
                "patch envelope discriminator must be \"patch\"",
            ));
        }

        let ops = raw
            .ops
            .into_iter()
            .map(PatchOp::from_wire)
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            root_id: raw.root_id,
            base_version: raw.base_version,
            version: raw.version,
            ops,
            stream_ops: raw.stream_ops,
            upload_ops: raw.upload_ops,
            events: raw.events,
        })
    }
}

/// The envelope exactly as it arrives, before the op allowlist runs.
///
/// `ops` and `stream_ops` are always sent, but `#[serde(default)]` costs
/// nothing and matches the tolerance the TypeScript client already extends to
/// `upload_ops` and `events`.
#[derive(Deserialize)]
struct RawEnvelope {
    r#type: String,
    root_id: String,
    base_version: u64,
    version: u64,
    #[serde(default)]
    ops: Vec<RawOp>,
    #[serde(default)]
    stream_ops: Vec<StreamOp>,
    #[serde(default)]
    upload_ops: Vec<UploadOp>,
    #[serde(default)]
    events: Vec<PushEvent>,
}

/// One RFC 6902 op exactly as it arrives, before the allowlist runs.
///
/// `path` stays a string here: RFC 6901 syntax is the pointer layer's
/// business, and a malformed pointer belongs with the other application
/// failures rather than with the decode failures.
#[derive(Deserialize)]
struct RawOp {
    op: String,
    path: String,
    #[serde(default, deserialize_with = "present_value")]
    value: Option<Value>,
}

/// Reads a present `value` key, `null` included.
///
/// A plain `Option<Value>` would fold an explicit `null` into `None`, and
/// `null` is a legitimate value — an Elixir `nil` field renders as one.
fn present_value<'de, D>(deserializer: D) -> std::result::Result<Option<Value>, D::Error>
where
    D: Deserializer<'de>,
{
    Value::deserialize(deserializer).map(Some)
}

/// One RFC 6902 op, restricted to the three the server can emit (BDR-0014).
#[derive(Debug, Clone, PartialEq)]
pub enum PatchOp {
    /// Insert a value at `path`.
    Add {
        /// RFC 6901 pointer into the wire tree.
        path: String,
        /// The value to insert.
        value: Value,
    },
    /// Remove the value at `path`.
    Remove {
        /// RFC 6901 pointer into the wire tree.
        path: String,
    },
    /// Overwrite the value at `path`.
    Replace {
        /// RFC 6901 pointer into the wire tree; `""` addresses the whole tree.
        path: String,
        /// The replacement value.
        value: Value,
    },
}

impl PatchOp {
    /// Narrows a decoded op to the allowlist.
    ///
    /// `move`, `copy` and `test` — and anything else a future server might
    /// send — are rejected here, so `json_patch::patch` never sees an op it
    /// would happily apply.
    fn from_wire(raw: RawOp) -> Result<Self> {
        match raw.op.as_str() {
            "add" => Ok(Self::Add {
                path: raw.path,
                value: raw.value.ok_or(MISSING_VALUE)?,
            }),
            "remove" => Ok(Self::Remove { path: raw.path }),
            "replace" => Ok(Self::Replace {
                path: raw.path,
                value: raw.value.ok_or(MISSING_VALUE)?,
            }),
            _ => Err(PatchError::UnsupportedOp { op: raw.op }.into()),
        }
    }
}

/// One stream delta (`docs/streams.md`), stamped with its owning store.
///
/// `ref` is the per-store slot ref; the client ignores it and keys everything
/// by `(store_id, stream)`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum StreamOp {
    /// Empty the stream.
    Reset {
        /// The declared stream name.
        stream: String,
        /// The owning store's path.
        store_id: StoreId,
    },
    /// Upsert an item, then position it (`docs/rust-client.md` §5).
    Insert {
        /// The declared stream name.
        stream: String,
        /// The owning store's path.
        store_id: StoreId,
        /// The item's identity within the stream.
        item_key: String,
        /// `-1` appends, `0` or any other negative prepends, `> 0` inserts at
        /// `min(at, len)`.
        at: i64,
        /// The rendered item.
        item: Value,
        /// Cap on the stream's length after this insert; `null` means no cap.
        limit: Option<i64>,
    },
    /// Drop every entry with this item key.
    Delete {
        /// The declared stream name.
        stream: String,
        /// The owning store's path.
        store_id: StoreId,
        /// The item's identity within the stream.
        item_key: String,
    },
}

/// One upload delta (BDR-0025).
///
/// v1 parses uploads only far enough to keep change notification correct
/// (`docs/rust-client.md` §10): the op is otherwise discarded, so only the
/// fields every variant shares are modelled.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct UploadOp {
    /// The op name (`config`, `add`, `progress`, `complete`, `error`,
    /// `cancel`, `reset`).
    pub op: String,
    /// The declared upload name.
    pub upload: String,
    /// The owning store's path.
    pub store_id: StoreId,
}

/// One transient push event (BDR-0032), dispatched per `(store_id, name)`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct PushEvent {
    /// The emitting store's path.
    pub store_id: StoreId,
    /// The declared event name.
    pub name: String,
    /// The wire-serialized payload.
    pub payload: Value,
}
