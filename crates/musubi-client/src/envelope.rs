//! The patch envelope wire shape and its decode-time validation
//! (`docs/rust-client.md` §4.4).
//!
//! Unknown fields are ignored everywhere — the server is allowed to add keys,
//! and `deny_unknown_fields` would break on the very first one.
//!
//! The envelope is crate-internal (`docs/rust-reactive-state.md` §5.5): folding
//! one by hand is not a supported entry point, so nothing here is re-exported.

use serde::{Deserialize, Deserializer};
use serde_json::Value;

use crate::error::{MusubiError, PatchError, Result};
use crate::generated::StoreId;
use crate::uploads::UploadOp;

pub(crate) use musubi_state::{PatchOp, StreamOp};

/// The envelope discriminator every `"patch"` push carries.
const ENVELOPE_TYPE: &str = "patch";

/// What an `add`/`replace` op without a `value` fails with.
const MISSING_VALUE: MusubiError = MusubiError::Protocol("add and replace ops must carry a value");

/// One decoded, validated patch envelope.
///
/// Build one with [`PatchEnvelope::decode`]; the struct has no `Deserialize`
/// impl of its own because the op allowlist is part of decoding.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PatchEnvelope {
    /// The root this envelope belongs to. Validated by the caller against the
    /// root it mounted, then ignored.
    pub(crate) root_id: String,
    /// The version this envelope was computed against.
    pub(crate) base_version: u64,
    /// The version the root reaches once this envelope is applied.
    pub(crate) version: u64,
    /// The RFC 6902 ops, already restricted to the allowlist.
    pub(crate) ops: Vec<PatchOp>,
    /// The stream deltas, in flush order (parent store first).
    pub(crate) stream_ops: Vec<StreamOp>,
    /// The upload deltas, in flush order. Folded into the root's upload
    /// registry (`docs/rust-client.md` §10), never into the tree.
    pub(crate) upload_ops: Vec<UploadOp>,
    /// The transient push events (BDR-0032) dispatched after state is applied.
    pub(crate) events: Vec<PushEvent>,
}

impl PatchEnvelope {
    /// Decodes a `"patch"` push payload, rejecting anything outside the
    /// `add`/`remove`/`replace` allowlist (BDR-0014) — so `move`, `copy` and
    /// `test` never reach the tree.
    pub(crate) fn decode(payload: Value) -> Result<Self> {
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
            .map(allowed_op)
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
    #[serde(default, deserialize_with = "lossy_upload_ops")]
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

/// Decodes `upload_ops` element by element, skipping the ones this build does
/// not understand.
///
/// One unrecognised `op` tag — or an unrecognised `entry.status` inside an
/// `add` — must not take the whole envelope with it: the state `ops`, the
/// `stream_ops` and the `events` travelling alongside are unrelated, and
/// failing the envelope would gap the root's version over an upload delta.
/// `applyOps` in `packages/client/src/uploads.ts` is a `switch` with no
/// `default`, so it already ignores exactly these ops and applies the rest.
fn lossy_upload_ops<'de, D>(deserializer: D) -> std::result::Result<Vec<UploadOp>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = Vec::<Value>::deserialize(deserializer)?;

    Ok(raw
        .into_iter()
        .filter_map(|op| match serde_json::from_value(op.clone()) {
            Ok(op) => Some(op),
            Err(error) => {
                tracing::warn!(%error, %op, "skipping an upload op this build cannot decode");

                None
            }
        })
        .collect())
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

/// Narrows a decoded op to the allowlist.
///
/// `move`, `copy` and `test` — and anything else a future server might send —
/// are rejected here, one crate above the tree, which is why
/// [`PatchOp`](musubi_state::PatchOp) is a three-variant enum with nothing to
/// reject.
fn allowed_op(raw: RawOp) -> Result<PatchOp> {
    match raw.op.as_str() {
        "add" => Ok(PatchOp::Add {
            path: raw.path,
            value: raw.value.ok_or(MISSING_VALUE)?,
        }),
        "remove" => Ok(PatchOp::Remove { path: raw.path }),
        "replace" => Ok(PatchOp::Replace {
            path: raw.path,
            value: raw.value.ok_or(MISSING_VALUE)?,
        }),
        _ => Err(PatchError::UnsupportedOp { op: raw.op }.into()),
    }
}

/// One transient push event (BDR-0032), dispatched per `(store_id, name)`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub(crate) struct PushEvent {
    /// The emitting store's path.
    pub(crate) store_id: StoreId,
    /// The declared event name.
    pub(crate) name: String,
    /// The wire-serialized payload.
    pub(crate) payload: Value,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::uploads::UploadOp;

    const ROOT_ID: &str = "MyApp.Stores.CartStore:cart";

    #[test]
    fn decode_rejects_a_foreign_discriminator() {
        let error = PatchEnvelope::decode(json!({
            "type": "snapshot",
            "root_id": ROOT_ID,
            "base_version": 0,
            "version": 1
        }))
        .unwrap_err();

        assert!(
            matches!(error, MusubiError::Protocol(message) if message.contains("discriminator"))
        );
    }

    #[test]
    fn decode_rejects_a_payload_that_is_not_an_envelope() {
        let error = PatchEnvelope::decode(json!({"type": "patch", "version": 1})).unwrap_err();

        assert!(matches!(error, MusubiError::Protocol(_)));
    }

    #[test]
    fn decode_rejects_every_op_outside_the_allowlist() {
        let rejected = [
            (json!({"op": "move", "from": "/a", "path": "/b"}), "move"),
            (json!({"op": "copy", "from": "/a", "path": "/b"}), "copy"),
            (json!({"op": "test", "path": "/a", "value": 1}), "test"),
        ];

        for (op, name) in rejected {
            let error = decode_error(vec![op]);

            assert!(
                matches!(error, MusubiError::Patch(PatchError::UnsupportedOp { op }) if op == name),
                "expected {name} to be rejected"
            );
        }
    }

    #[test]
    fn decode_keeps_the_three_allowed_ops_and_defaults_the_optional_arrays() {
        let envelope = decode(vec![
            json!({"op": "add", "path": "/a", "value": 1}),
            json!({"op": "remove", "path": "/a"}),
            json!({"op": "replace", "path": "", "value": {}}),
        ]);

        assert_eq!(envelope.ops.len(), 3);
        assert_eq!(envelope.root_id, ROOT_ID);
        assert!(envelope.stream_ops.is_empty());
        assert!(envelope.upload_ops.is_empty());
        assert!(envelope.events.is_empty());
    }

    #[test]
    fn decode_reads_stream_upload_and_event_arrays() {
        let envelope = PatchEnvelope::decode(json!({
            "type": "patch",
            "root_id": ROOT_ID,
            "base_version": 1,
            "version": 2,
            "ops": [],
            "stream_ops": [{
                "op": "insert", "stream": "messages", "ref": "0", "store_id": [],
                "item_key": "m-1", "at": -1, "item": {"id": "m-1"}, "limit": -100
            }],
            "upload_ops": [
                {"op": "progress", "upload": "avatar", "store_id": [], "ref": "e", "progress": 10}
            ],
            "events": [{"store_id": [], "name": "toast", "payload": {"message": "hi"}}]
        }))
        .unwrap();

        assert_eq!(envelope.stream_ops.len(), 1);
        assert!(matches!(
            envelope.upload_ops.as_slice(),
            [UploadOp::Progress { upload, r#ref, progress: 10, .. }]
                if upload == "avatar" && r#ref == "e"
        ));
        assert!(matches!(
            envelope.events.as_slice(),
            [event] if event.name == "toast" && event.payload["message"] == json!("hi")
        ));
    }

    #[test]
    fn an_upload_op_this_build_cannot_decode_is_skipped_rather_than_failing_the_envelope() {
        let envelope = PatchEnvelope::decode(json!({
            "type": "patch",
            "root_id": ROOT_ID,
            "base_version": 1,
            "version": 2,
            "ops": [{"op": "replace", "path": "/title", "value": "Cart"}],
            "upload_ops": [
                {"op": "teleport", "upload": "avatar", "store_id": [], "ref": "e"},
                {"op": "progress", "upload": "avatar", "store_id": [], "ref": "e", "progress": 10}
            ]
        }))
        .unwrap();

        assert_eq!(
            envelope.ops.len(),
            1,
            "the state ops travel with the envelope and must survive it"
        );
        assert!(matches!(
            envelope.upload_ops.as_slice(),
            [UploadOp::Progress { r#ref, progress: 10, .. }] if r#ref == "e"
        ));
    }

    #[test]
    fn an_explicit_null_value_is_a_value_but_a_missing_one_is_not() {
        let envelope = decode(vec![
            json!({"op": "replace", "path": "/title", "value": null}),
        ]);

        assert!(matches!(
            envelope.ops.as_slice(),
            [PatchOp::Replace { path, value }] if path == "/title" && value.is_null()
        ));

        let error = decode_error(vec![json!({"op": "add", "path": "/title"})]);

        assert!(matches!(error, MusubiError::Protocol(message) if message.contains("value")));
    }

    fn decode(ops: Vec<Value>) -> PatchEnvelope {
        PatchEnvelope::decode(json!({
            "type": "patch",
            "root_id": ROOT_ID,
            "base_version": 0,
            "version": 1,
            "ops": ops
        }))
        .expect("allowed ops decode")
    }

    fn decode_error(ops: Vec<Value>) -> MusubiError {
        PatchEnvelope::decode(json!({
            "type": "patch",
            "root_id": ROOT_ID,
            "base_version": 0,
            "version": 1,
            "ops": ops
        }))
        .expect_err("the envelope is rejected")
    }
}
