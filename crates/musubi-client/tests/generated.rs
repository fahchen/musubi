//! Layer-2 tests for the types the generated bundle re-exports
//! (`docs/rust-client.md` §6.1, §12; `docs/rust-codegen.md` §4.5/§4.6): the
//! `AsyncResult` wire shape, the store/upload wrappers, and the three traits
//! the bundle implements.
//!
//! Every case here is a serde-shape assertion, so none of them needs state fed
//! through the client. The one that did — a `stream_async` field materializing
//! as `AsyncResult<Vec<T>>` — is now a projection test in `musubi-state`, where
//! the collection it projects lives (`docs/rust-reactive-state.md` §5.5).

use musubi_client::generated::{
    AsyncError, AsyncErrorKind, AsyncResult, Command, Event, NoReply, Store, StoreField, StoreId,
    UploadSlot,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// AsyncResult
// ---------------------------------------------------------------------------

#[test]
fn loading_carries_no_result_until_one_is_preserved() {
    assert_eq!(
        async_result(json!({"status": "loading", "result": null, "reason": null})),
        AsyncResult::Loading {
            result: None,
            reason: None
        }
    );
    assert_eq!(
        async_result(json!({"status": "loading", "result": 3, "reason": null})),
        AsyncResult::Loading {
            result: Some(3),
            reason: None
        }
    );
}

#[test]
fn ok_carries_the_result_and_a_null_reason() {
    assert_eq!(
        async_result(json!({"status": "ok", "result": 7, "reason": null})),
        AsyncResult::Ok {
            result: 7,
            reason: None
        }
    );
}

#[test]
fn failed_carries_the_structured_reason_and_the_prior_result() {
    assert_eq!(
        async_result(json!({
            "status": "failed",
            "result": 3,
            "reason": {"kind": "error", "value": {"message": "boom"}}
        })),
        AsyncResult::Failed {
            result: Some(3),
            reason: Some(AsyncError::Structured {
                kind: AsyncErrorKind::Error,
                value: json!({"message": "boom"})
            })
        }
    );
    assert_eq!(
        async_result(json!({
            "status": "failed",
            "result": null,
            "reason": {"kind": "exit", "value": "timeout"}
        })),
        AsyncResult::Failed {
            result: None,
            reason: Some(AsyncError::Structured {
                kind: AsyncErrorKind::Exit,
                value: json!("timeout")
            })
        }
    );
}

#[test]
fn an_unclassified_reason_falls_back_to_opaque() {
    let opaque = [
        json!("** (RuntimeError) boom"),
        json!({"kind": "shutdown", "value": 1}),
        json!({"value": "no kind"}),
        json!(["exit", 2]),
    ];

    for reason in opaque {
        assert!(
            matches!(
                async_result(json!({"status": "failed", "result": null, "reason": reason})),
                AsyncResult::Failed {
                    reason: Some(AsyncError::Opaque(value)),
                    ..
                } if value == reason
            ),
            "expected {reason} to fall back to Opaque"
        );
    }
}

#[test]
fn the_async_marker_is_ignored_on_the_way_in_and_omitted_on_the_way_out() {
    let wire = json!({"__musubi_async__": true, "status": "ok", "result": 7, "reason": null});

    assert_eq!(
        serde_json::to_value(async_result(wire)).unwrap(),
        json!({"status": "ok", "result": 7, "reason": null})
    );
}

#[test]
fn an_unknown_status_or_a_missing_key_is_a_deserialization_failure() {
    let rejected = [
        json!({"status": "pending", "result": null, "reason": null}),
        json!({"status": "ok", "reason": null}),
        json!({"result": 7, "reason": null}),
    ];

    for wire in rejected {
        assert!(
            serde_json::from_value::<AsyncResult<u8>>(wire.clone()).is_err(),
            "expected {wire} to be rejected"
        );
    }
}

// ---------------------------------------------------------------------------
// Store, upload and reply wrappers
// ---------------------------------------------------------------------------

#[test]
fn a_store_field_flattens_the_child_fields_beside_the_store_id() {
    let wire = json!({"__musubi_store_id__": ["panel"], "label": "Panel"});
    let field: StoreField<PanelState> = serde_json::from_value(wire.clone()).unwrap();

    assert_eq!(field.store_id.as_slice(), ["panel".to_owned()]);
    assert_eq!(field.state.label, "Panel");
    assert_eq!(serde_json::to_value(field).unwrap(), wire);
}

#[test]
fn an_upload_slot_deserializes_from_its_marker_untouched() {
    let wire = json!({"__musubi_upload__": "avatar"});
    let slot: UploadSlot = serde_json::from_value(wire.clone()).unwrap();

    assert_eq!(slot.name, "avatar");
    assert_eq!(serde_json::to_value(slot).unwrap(), wire);
}

#[test]
fn no_reply_accepts_the_empty_object_a_noreply_command_replies() {
    assert!(serde_json::from_value::<NoReply>(json!({})).is_ok());
    assert!(serde_json::from_value::<NoReply>(json!({"ignored": 1})).is_ok());
    assert_eq!(serde_json::to_value(NoReply {}).unwrap(), json!({}));
}

#[test]
fn a_store_id_is_transparent_over_its_segments() {
    let wire = json!(["panel", "row-1"]);
    let store_id: StoreId = serde_json::from_value(wire.clone()).unwrap();

    assert_eq!(
        store_id.as_slice(),
        ["panel".to_owned(), "row-1".to_owned()]
    );
    assert_eq!(serde_json::to_value(&store_id).unwrap(), wire);
    assert_ne!(store_id, StoreId::root());
}

// ---------------------------------------------------------------------------
// The traits the bundle implements
// ---------------------------------------------------------------------------

#[test]
fn the_generated_impls_carry_the_wire_names() {
    assert_eq!(CartStore::MODULE, "MyApp.Stores.CartStore");
    assert_eq!(<Checkout as Command<CartStore>>::NAME, "checkout");
    assert_eq!(<ToastPayload as Event<CartStore>>::NAME, "toast");
    assert_eq!(
        serde_json::to_value(CartParams {
            room_id: "lobby".to_owned(),
            currency: None,
        })
        .unwrap(),
        json!({"room_id": "lobby"}),
        "an unset optional attr is an absent key, not an explicit null"
    );
    assert_eq!(
        serde_json::to_value(Checkout { coupon: None }).unwrap(),
        json!({"coupon": null})
    );
}

// ---------------------------------------------------------------------------
// Fixtures: the shape `mix compile.musubi_rust` would emit for a cart store
// ---------------------------------------------------------------------------

/// The zero-sized marker type; `State` is the rendered shape.
struct CartStore;

impl Store for CartStore {
    const MODULE: &'static str = "MyApp.Stores.CartStore";
    type State = FeedState;
    type Params = CartParams;
}

/// The generated mount-params struct: `attr :room_id, String.t(), required:
/// true` is a plain field, an optional attr an `Option` that serializes to an
/// absent key — `normalize_assigns/2` gates a declared default on the key
/// being absent, and `cache_key/3` has to agree with `storeCacheKey`.
#[derive(Debug, Serialize)]
struct CartParams {
    room_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    currency: Option<String>,
}

/// The rendered shape of a store whose only field is `stream_async(:feed)`.
#[derive(Debug, Deserialize)]
#[allow(dead_code, reason = "the field is what `Deserialize` fills in")]
struct FeedState {
    feed: AsyncResult<Vec<Message>>,
}

#[derive(Debug, Deserialize, Serialize)]
struct PanelState {
    label: String,
}

#[derive(Debug, Deserialize, PartialEq)]
struct Message {
    id: String,
}

#[derive(Debug, Serialize)]
struct Checkout {
    coupon: Option<String>,
}

impl Command<CartStore> for Checkout {
    const NAME: &'static str = "checkout";
    type Reply = NoReply;
}

#[derive(Debug, Deserialize)]
struct ToastPayload {
    #[allow(
        dead_code,
        reason = "the generated payload field is what is under test"
    )]
    message: String,
}

impl Event<CartStore> for ToastPayload {
    const NAME: &'static str = "toast";
}

fn async_result(wire: Value) -> AsyncResult<u8> {
    serde_json::from_value(wire).expect("async result deserializes")
}
