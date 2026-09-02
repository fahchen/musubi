//! Layer-2 tests for the data plane (`docs/rust-client.md` §12): envelope
//! decoding and the op allowlist, version discipline, `json-patch` error
//! mapping and atomicity, hydration, and the §5 change set.

use musubi_client::generated::{AsyncResult, StoreField, UploadSlot};
use musubi_client::{MusubiError, PatchEngine, PatchEnvelope, PatchError};
use serde::Deserialize;
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Envelope decoding
// ---------------------------------------------------------------------------

#[test]
fn decode_rejects_a_foreign_discriminator() {
    let error = PatchEnvelope::decode(json!({
        "type": "snapshot",
        "root_id": ROOT_ID,
        "base_version": 0,
        "version": 1
    }))
    .unwrap_err();

    assert!(matches!(error, MusubiError::Protocol(message) if message.contains("discriminator")));
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
        "upload_ops": [{"op": "progress", "upload": "avatar", "store_id": [], "ref": "e", "progress": 10}],
        "events": [{"store_id": [], "name": "toast", "payload": {"message": "hi"}}]
    }))
    .unwrap();

    assert_eq!(envelope.stream_ops.len(), 1);
    assert_eq!(envelope.upload_ops.len(), 1);
    assert!(matches!(
        envelope.events.as_slice(),
        [event] if event.name == "toast" && event.payload["message"] == json!("hi")
    ));
}

#[test]
fn an_explicit_null_value_is_a_value_but_a_missing_one_is_not() {
    let mut engine = mounted();

    engine
        .apply(&envelope(
            1,
            2,
            vec![json!({"op": "replace", "path": "/title", "value": null})],
        ))
        .unwrap();

    assert_eq!(engine.document()["title"], json!(null));

    let error = decode_error(vec![json!({"op": "add", "path": "/title"})]);

    assert!(matches!(error, MusubiError::Protocol(message) if message.contains("value")));
}

// ---------------------------------------------------------------------------
// Version discipline
// ---------------------------------------------------------------------------

#[test]
fn the_initial_envelope_must_be_base_zero_version_one() {
    for (base_version, version) in [(0, 2), (1, 2), (0, 0)] {
        let mut engine = PatchEngine::new();
        let error = engine
            .apply(&envelope(base_version, version, vec![]))
            .unwrap_err();

        assert!(
            matches!(error, MusubiError::Protocol(message) if message.contains("version 1")),
            "expected {base_version}->{version} to be refused"
        );
        assert_eq!(engine.version(), 0);
    }
}

#[test]
fn a_gap_in_the_sequence_is_a_version_mismatch_and_changes_nothing() {
    let mut engine = mounted();

    let error = engine
        .apply(&envelope(
            2,
            3,
            vec![json!({"op": "replace", "path": "/title", "value": "Gapped"})],
        ))
        .unwrap_err();

    assert!(matches!(error, MusubiError::VersionMismatch));
    assert_eq!(engine.version(), 1);
    assert_eq!(engine.document()["title"], json!("Cart"));
}

#[test]
fn a_replayed_envelope_is_a_version_mismatch() {
    let mut engine = mounted();

    engine
        .apply(&envelope(
            1,
            2,
            vec![json!({"op": "replace", "path": "/title", "value": "Second"})],
        ))
        .unwrap();
    let error = engine
        .apply(&envelope(
            1,
            2,
            vec![json!({"op": "replace", "path": "/title", "value": "Replay"})],
        ))
        .unwrap_err();

    assert!(matches!(error, MusubiError::VersionMismatch));
    assert_eq!(engine.document()["title"], json!("Second"));
}

#[test]
fn a_stream_only_cycle_still_bumps_the_sequence() {
    let mut engine = mounted();

    engine
        .apply(&stream_envelope(1, 2, vec![insert_op(&[], "m-1", -1)]))
        .unwrap();

    assert_eq!(engine.version(), 2);
}

// ---------------------------------------------------------------------------
// Patch application: error mapping and atomicity
// ---------------------------------------------------------------------------

#[test]
fn a_failing_op_maps_to_apply_and_leaves_the_whole_envelope_unapplied() {
    let mut engine = mounted();

    let error = engine
        .apply(&stream_envelope_with_ops(
            1,
            2,
            vec![
                json!({"op": "replace", "path": "/title", "value": "Applied first"}),
                json!({"op": "remove", "path": "/absent"}),
            ],
            vec![insert_op(&[], "m-1", -1)],
        ))
        .unwrap_err();

    assert!(matches!(
        error,
        MusubiError::Patch(PatchError::Apply { index: 1, ref path, .. }) if path == "/absent"
    ));
    assert_eq!(engine.document()["title"], json!("Cart"));
    assert_eq!(engine.version(), 1);
    assert_eq!(state(&mut engine)["messages"], json!([]));
}

#[test]
fn a_malformed_pointer_is_rejected_before_any_op_runs() {
    let mut engine = mounted();

    let error = engine
        .apply(&envelope(
            1,
            2,
            vec![
                json!({"op": "replace", "path": "/title", "value": "Applied first"}),
                json!({"op": "replace", "path": "nope", "value": 1}),
            ],
        ))
        .unwrap_err();

    assert!(matches!(
        error,
        MusubiError::Patch(PatchError::InvalidPointer { ref path }) if path == "nope"
    ));
    assert_eq!(engine.document()["title"], json!("Cart"));
}

#[test]
fn ops_apply_left_to_right() {
    let mut engine = mounted();

    engine
        .apply(&envelope(
            1,
            2,
            vec![
                json!({"op": "add", "path": "/tags", "value": ["a"]}),
                json!({"op": "add", "path": "/tags/-", "value": "b"}),
                json!({"op": "replace", "path": "/tags/0", "value": "z"}),
            ],
        ))
        .unwrap();

    assert_eq!(engine.document()["tags"], json!(["z", "b"]));
}

// ---------------------------------------------------------------------------
// Hydration and the pristine shadow document
// ---------------------------------------------------------------------------

#[test]
fn the_shadow_document_keeps_its_markers_while_the_state_is_hydrated() {
    let mut engine = mounted();

    let state = engine
        .apply(&stream_envelope(1, 2, vec![insert_op(&[], "m-1", -1)]))
        .unwrap();

    assert_eq!(
        engine.document()["messages"],
        json!({"__musubi_stream__": "messages"})
    );
    assert_eq!(state["messages"], json!([{"id": "m-1"}]));
}

#[test]
fn the_hydrated_state_deserializes_into_the_generated_shapes() {
    let mut engine = mounted();

    let hydrated = engine
        .apply(&stream_envelope(
            1,
            2,
            vec![insert_op(&[], "m-1", -1), insert_op(&["panel"], "p-1", -1)],
        ))
        .unwrap();
    let state: CartState = serde_json::from_value(hydrated).unwrap();

    assert!(matches!(
        state,
        CartState {
            title,
            messages,
            avatar: UploadSlot { name },
            feed: AsyncResult::Ok { result, reason: None },
            panel: StoreField { store_id, state: PanelState { messages: ref panel_messages } },
        }
        if title == "Cart"
            && messages == [Message { id: "m-1".to_owned() }]
            && name == "avatar"
            && result == 7
            && store_id.as_slice() == ["panel".to_owned()]
            && panel_messages == &[Message { id: "p-1".to_owned() }]
    ));
}

// ---------------------------------------------------------------------------
// Store index, pruning and the change set
// ---------------------------------------------------------------------------

#[test]
fn a_vanished_store_loses_its_streams() {
    let mut engine = mounted();

    engine
        .apply(&stream_envelope(
            1,
            2,
            vec![insert_op(&["panel"], "p-1", -1)],
        ))
        .unwrap();
    engine
        .apply(&envelope(
            2,
            3,
            vec![json!({"op": "remove", "path": "/panel"})],
        ))
        .unwrap();

    // BDR-0011 fresh-mount semantics: the reappearing store starts empty.
    let reappeared = engine
        .apply(&envelope(
            3,
            4,
            vec![json!({
                "op": "add",
                "path": "/panel",
                "value": {
                    "__musubi_store_id__": ["panel"],
                    "messages": {"__musubi_stream__": "messages"}
                }
            })],
        ))
        .unwrap();

    assert_eq!(reappeared["panel"]["messages"], json!([]));
}

#[test]
fn an_upload_op_is_discarded_and_leaves_its_marker_in_place() {
    let mut engine = mounted();

    let envelope = PatchEnvelope::decode(json!({
        "type": "patch",
        "root_id": ROOT_ID,
        "base_version": 1,
        "version": 2,
        "ops": [],
        "upload_ops": [{
            "op": "progress", "upload": "avatar", "store_id": ["panel"],
            "ref": "entry-1", "progress": 42
        }]
    }))
    .unwrap();
    let state = engine.apply(&envelope).unwrap();

    assert_eq!(state["avatar"], json!({"__musubi_upload__": "avatar"}));
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const ROOT_ID: &str = "MyApp.Stores.CartStore:cart";

/// The generated shape of `MyApp.Stores.CartStore`, hand-written to the rules
/// `docs/rust-codegen.md` §4.5/§4.6 fix for the emitter.
#[derive(Debug, Deserialize)]
struct CartState {
    title: String,
    messages: Vec<Message>,
    avatar: UploadSlot,
    feed: AsyncResult<u8>,
    panel: StoreField<PanelState>,
}

#[derive(Debug, Deserialize)]
struct PanelState {
    messages: Vec<Message>,
}

#[derive(Debug, Deserialize, PartialEq)]
struct Message {
    id: String,
}

/// An engine holding the initial envelope's tree: a root store with a stream,
/// an upload slot, an async field and one child store.
fn mounted() -> PatchEngine {
    let mut engine = PatchEngine::new();

    engine
        .apply(&envelope(
            0,
            1,
            vec![json!({
                "op": "replace",
                "path": "",
                "value": {
                    "__musubi_store_id__": [],
                    "title": "Cart",
                    "messages": {"__musubi_stream__": "messages"},
                    "avatar": {"__musubi_upload__": "avatar"},
                    "feed": {
                        "__musubi_async__": true,
                        "status": "ok",
                        "result": 7,
                        "reason": null
                    },
                    "panel": {
                        "__musubi_store_id__": ["panel"],
                        "label": "",
                        "messages": {"__musubi_stream__": "messages"}
                    }
                }
            })],
        ))
        .unwrap();

    engine
}

fn envelope(base_version: u64, version: u64, ops: Vec<Value>) -> PatchEnvelope {
    stream_envelope_with_ops(base_version, version, ops, vec![])
}

fn stream_envelope(base_version: u64, version: u64, stream_ops: Vec<Value>) -> PatchEnvelope {
    stream_envelope_with_ops(base_version, version, vec![], stream_ops)
}

fn stream_envelope_with_ops(
    base_version: u64,
    version: u64,
    ops: Vec<Value>,
    stream_ops: Vec<Value>,
) -> PatchEnvelope {
    PatchEnvelope::decode(json!({
        "type": "patch",
        "root_id": ROOT_ID,
        "base_version": base_version,
        "version": version,
        "ops": ops,
        "stream_ops": stream_ops
    }))
    .expect("fixture envelope decodes")
}

fn insert_op(store_id: &[&str], item_key: &str, at: i64) -> Value {
    json!({
        "op": "insert",
        "stream": "messages",
        "ref": "0",
        "store_id": store_id,
        "item_key": item_key,
        "at": at,
        "item": {"id": item_key},
        "limit": null
    })
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
    .expect_err("op is outside the allowlist")
}

/// The hydrated state of the next accepted (no-op) envelope.
fn state(engine: &mut PatchEngine) -> Value {
    let version = engine.version();

    engine
        .apply(&envelope(version, version + 1, vec![]))
        .expect("empty envelope applies")
}
