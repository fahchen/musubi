//! Protocol tests for the connection layer over a scripted `MockSocket` plus a
//! `ManualTimer` (`docs/rust-client.md` §12, layer 3): mount/join, duplicate
//! mount aliasing, drop-at-refcount-0 teardown, version-gap recovery including
//! a failed re-join, bulk command rejection per teardown path,
//! reply-before-patch ordering, and push-event dispatch.
//!
//! Nothing here sleeps: the executor is a `LocalPool` the test pumps by hand
//! and every timer fires only when a test says so.

use std::future::Future;
use std::sync::Arc;
use std::task::Context;
use std::time::Duration;

use futures_util::task::noop_waker;
use musubi_client::generated::{Command, Event, NoReply, Store, StoreId};
use musubi_client::{CommandError, Connection, Mounted, MusubiError};
use phoenix_channel::{Message, ReplyStatus};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

// The scripted-transport rig is shared with `phoenix-channel`'s protocol
// suite; files under a `tests/` subdirectory are not test targets themselves.
#[path = "../../phoenix-channel/tests/common/mod.rs"]
mod common;

use common::{Pump, Seams, ServerEnd, Slot, drain, ended};

const MODULE: &str = "MyApp.Stores.CartStore";
const ROOT_ID: &str = "MyApp.Stores.CartStore:cart";
const TOPIC: &str = "musubi:connection:MyApp.Stores.CartStore:cart";
const PUSH_TIMEOUT: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// Mount
// ---------------------------------------------------------------------------

#[test]
fn mount_joins_one_channel_per_root_and_hydrates_the_initial_patch() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let pending = harness.mount_later("cart", json!({"currency": "EUR"}));

    let sent = server.sent(&mut harness);
    assert!(matches!(
        sent.as_slice(),
        [Message { topic, event, payload, .. }]
            if topic == TOPIC
                && event == "phx_join"
                && payload["module"] == json!(MODULE)
                && payload["id"] == json!("cart")
                && payload["params"] == json!({"currency": "EUR"})
    ));

    server.reply(&sent[0], ReplyStatus::Ok, json!({"root_id": ROOT_ID}));
    harness.pump();
    let cart = harness.finish_mount(&mut server, &sent[0], pending);

    let state = cart.snapshot().expect("the initial patch has been applied");
    assert!(matches!(
        state.as_ref(),
        CartState { title, messages } if title == "Cart" && messages.len() == 1
    ));
    assert!(matches!(
        state.messages.as_slice(),
        [ChatMessage { id, body }] if id == "m-1" && body == "hello"
    ));
}

#[test]
fn a_rejected_join_fails_the_mount_with_the_server_reason() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let pending = harness.mount_later("cart", json!({}));

    let sent = server.sent(&mut harness);
    server.reply(
        &sent[0],
        ReplyStatus::Error,
        json!({"reason": "unauthorized"}),
    );

    assert!(matches!(
        harness.settle(pending),
        Err(MusubiError::Join { topic, reason }) if topic == TOPIC && reason == "unauthorized"
    ));
}

#[test]
fn an_initial_envelope_that_does_not_start_at_version_1_fails_the_mount() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let pending = harness.mount_later("cart", json!({}));

    let sent = server.sent(&mut harness);
    server.reply(&sent[0], ReplyStatus::Ok, json!({"root_id": ROOT_ID}));
    harness.pump();
    server.push_event(&sent[0], "patch", envelope(4, 5, json!([])));

    assert!(matches!(
        harness.settle(pending),
        Err(MusubiError::Protocol(message))
            if message == "Initial patch envelope must start at version 1"
    ));
}

#[test]
fn mount_params_must_be_a_json_object() {
    let mut harness = Harness::new();
    let _server = harness.queue_socket();
    let pending = harness.mount_later("cart", json!("not-an-object"));

    assert!(matches!(
        harness.settle(pending),
        Err(MusubiError::Protocol(message))
            if message == "mount params must serialize to a JSON object"
    ));
}

// ---------------------------------------------------------------------------
// Aliasing and teardown
// ---------------------------------------------------------------------------

#[test]
fn a_duplicate_mount_aliases_the_live_root_instead_of_joining_twice() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (join, first) = harness.mount(&mut server, "cart");

    let pending = harness.mount_later("cart", json!({}));

    let Ok(second) = harness.settle(pending) else {
        panic!("the aliasing mount resolves from the live root")
    };
    assert!(server.sent(&mut harness).is_empty(), "no second phx_join");
    assert!(matches!(
        (first.snapshot(), second.snapshot()),
        (Some(one), Some(two)) if Arc::ptr_eq(&one, &two)
    ));

    // Only the last handle to go away tears the root down.
    drop(second);
    harness.pump();
    assert!(server.sent(&mut harness).is_empty(), "no premature leave");

    drop(first);
    harness.pump();
    assert!(matches!(
        server.sent(&mut harness).as_slice(),
        [Message { event, topic, .. }] if event == "phx_leave" && topic == TOPIC
    ));

    // The channel is gone, so a late envelope on it changes nothing.
    server.push_event(&join, "patch", envelope(1, 2, json!([])));
    harness.pump();
}

#[test]
fn an_aliasing_mount_awaits_the_in_flight_initial_patch() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let first = harness.mount_later("cart", json!({"currency": "EUR"}));

    let sent = server.sent(&mut harness);
    server.reply(&sent[0], ReplyStatus::Ok, json!({"root_id": ROOT_ID}));
    harness.pump();

    // First-mount params win; the mismatched ones are ignored with a warning.
    let second = harness.mount_later("cart", json!({"currency": "USD"}));
    harness.pump();
    assert!(harness.peek(&first).is_none(), "no initial patch yet");
    assert!(harness.peek(&second).is_none(), "no initial patch yet");

    server.push_event(&sent[0], "patch", initial_envelope());

    let (Ok(first), Ok(second)) = (harness.settle(first), harness.settle(second)) else {
        panic!("both mounts resolve on the one initial patch")
    };

    assert!(server.sent(&mut harness).is_empty(), "no second phx_join");
    assert!(matches!(
        (first.snapshot(), second.snapshot()),
        (Some(one), Some(two)) if Arc::ptr_eq(&one, &two)
    ));
}

#[test]
fn a_failed_join_releases_every_waiting_mount() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let first = harness.mount_later("cart", json!({}));
    let sent = server.sent(&mut harness);
    let second = harness.mount_later("cart", json!({}));
    harness.pump();

    server.reply(
        &sent[0],
        ReplyStatus::Error,
        json!({"reason": "unknown root"}),
    );
    harness.pump();

    assert!(matches!(
        harness.settle(first),
        Err(MusubiError::Join { .. })
    ));
    assert!(matches!(
        harness.settle(second),
        Err(MusubiError::Join { .. })
    ));

    // Refcount is back to zero, so the orphaned root left its channel and a
    // fresh mount joins from scratch.
    let third = harness.mount_later("cart", json!({}));
    assert!(matches!(
        server.sent(&mut harness).as_slice(),
        [Message { event: leave, .. }, Message { event: join, .. }]
            if leave == "phx_leave" && join == "phx_join"
    ));
    drop(third);
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

#[test]
fn a_command_reply_resolves_before_the_patch_it_caused_is_applied() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (join, cart) = harness.mount(&mut server, "cart");
    let pending = harness.command_later(&cart, Checkout { coupon: None });

    let sent = server.sent(&mut harness);
    assert!(matches!(
        sent.as_slice(),
        [Message { event, payload, .. }]
            if event == "command"
                && payload["store_id"] == json!([])
                && payload["name"] == json!("checkout")
                && payload["payload"] == json!({"coupon": null})
    ));

    // BDR-0009 puts the reply on the wire first, and the client does not gate
    // it on the patch: it resolves with nothing else delivered.
    server.reply(&sent[0], ReplyStatus::Ok, json!({"order_id": "o-1"}));

    assert!(matches!(
        harness.settle(pending),
        Ok(CheckoutReply { order_id }) if order_id == "o-1"
    ));
    assert!(
        matches!(
            cart.snapshot().as_deref(),
            Some(CartState { title, .. }) if title == "Cart"
        ),
        "a resolved reply must not imply the command's patch was applied"
    );

    server.push_event(
        &join,
        "patch",
        envelope(
            1,
            2,
            json!([{"op": "replace", "path": "/title", "value": "Checked out"}]),
        ),
    );
    harness.pump();

    assert!(matches!(
        cart.snapshot().as_deref(),
        Some(CartState { title, .. }) if title == "Checked out"
    ));
}

#[test]
fn an_error_reply_carries_the_first_string_valued_code_field() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (_join, cart) = harness.mount(&mut server, "cart");

    let pending = harness.command_later(&cart, Checkout { coupon: None });
    let sent = server.sent(&mut harness);
    server.reply(
        &sent[0],
        ReplyStatus::Error,
        json!({"code": null, "error": "unknown command", "reason": "ignored"}),
    );

    assert!(matches!(
        harness.settle(pending),
        Err(MusubiError::Command(CommandError::Failed { command, store_id, code, .. }))
            if command == "checkout"
                && store_id == StoreId::root()
                && code.as_deref() == Some("unknown command")
    ));
}

#[test]
fn a_noreply_command_deserializes_from_an_empty_reply() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (_join, cart) = harness.mount(&mut server, "cart");

    let pending = harness.command_later(&cart, Refresh {});
    let sent = server.sent(&mut harness);
    server.reply(&sent[0], ReplyStatus::Ok, json!({}));

    assert!(matches!(harness.settle(pending), Ok(NoReply { .. })));
}

#[test]
fn a_command_that_never_replies_times_out() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (_join, cart) = harness.mount(&mut server, "cart");
    let pending = harness.command_later(&cart, Checkout { coupon: None });

    server.sent(&mut harness);
    harness.fire(PUSH_TIMEOUT);

    assert!(matches!(
        harness.settle(pending),
        Err(MusubiError::Command(CommandError::Timeout { command, store_id }))
            if command == "checkout" && store_id == StoreId::root()
    ));
}

#[test]
fn a_command_dispatched_mid_reconnect_is_rejected_rather_than_queued() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (_join, cart) = harness.mount(&mut server, "cart");

    let _next = harness.queue_socket();
    server.disconnect();
    harness.pump();

    let pending = harness.command_later(&cart, Checkout { coupon: None });
    assert!(matches!(
        harness.settle(pending),
        Err(MusubiError::NotConnected)
    ));
    // Last-good rendering survives the reconnect window.
    assert!(cart.snapshot().is_some());
}

// ---------------------------------------------------------------------------
// Bulk rejection, per teardown path
// ---------------------------------------------------------------------------

#[test]
fn a_channel_drop_rejects_pending_commands_with_disconnected() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (_join, cart) = harness.mount(&mut server, "cart");
    let pending = harness.command_later(&cart, Checkout { coupon: None });

    server.sent(&mut harness);
    let _next = harness.queue_socket();
    server.disconnect();

    assert!(matches!(
        harness.settle(pending),
        Err(MusubiError::Disconnected)
    ));
}

#[test]
fn dropping_the_last_handle_leaves_the_channel_and_drops_the_root() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (_join, cart) = harness.mount(&mut server, "cart");
    let mut updates = cart.updates();

    drop(cart);
    harness.pump();

    assert!(
        matches!(
            server.sent(&mut harness).as_slice(),
            [Message { event, topic, .. }] if event == "phx_leave" && topic == TOPIC
        ),
        "leaving the channel is what stops the server-side root"
    );
    assert!(ended(&mut updates), "subscriptions end with the root");
}

#[test]
fn a_version_gap_rejects_pending_commands_with_version_mismatch() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (join, cart) = harness.mount(&mut server, "cart");
    let pending = harness.command_later(&cart, Checkout { coupon: None });

    server.sent(&mut harness);
    server.push_event(&join, "patch", envelope(7, 8, json!([])));

    assert!(matches!(
        harness.settle(pending),
        Err(MusubiError::VersionMismatch)
    ));
}

#[test]
fn disconnect_tears_every_root_down_and_rejects_pending_commands() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (_join, cart) = harness.mount(&mut server, "cart");
    let pending = harness.command_later(&cart, Checkout { coupon: None });
    let mut updates = cart.updates();

    server.sent(&mut harness);
    harness.disconnect();

    assert!(matches!(
        harness.settle(pending),
        Err(MusubiError::Disconnected)
    ));
    assert!(cart.snapshot().is_none(), "the root is gone");
    assert!(ended(&mut updates), "subscriptions end with the root");

    let later = harness.command_later(&cart, Checkout { coupon: None });
    assert!(matches!(
        harness.settle(later),
        Err(MusubiError::Disconnected)
    ));
}

// ---------------------------------------------------------------------------
// Recovery
// ---------------------------------------------------------------------------

#[test]
fn a_version_gap_leaves_and_rejoins_while_the_last_good_state_keeps_rendering() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (join, cart) = harness.mount(&mut server, "cart");

    server.push_event(&join, "patch", envelope(7, 8, json!([])));
    harness.pump();

    let sent = server.sent(&mut harness);
    assert!(
        matches!(
            sent.as_slice(),
            [Message { event: leave, .. }, Message { event: rejoin, .. }]
                if leave == "phx_leave" && rejoin == "phx_join"
        ),
        "recovery leaves the diverged channel and joins a fresh one"
    );
    assert!(
        matches!(
            cart.snapshot().as_deref(),
            Some(CartState { title, .. }) if title == "Cart"
        ),
        "the last-good tree keeps rendering through the recreate window"
    );

    // The fresh join's initial patch swaps the state in atomically.
    let rejoin = &sent[1];
    server.reply(rejoin, ReplyStatus::Ok, json!({"root_id": ROOT_ID}));
    harness.pump();
    server.push_event(
        rejoin,
        "patch",
        envelope(
            0,
            1,
            json!([{
                "op": "replace",
                "path": "",
                "value": {"__musubi_store_id__": [], "title": "Recovered", "messages": []}
            }]),
        ),
    );
    harness.pump();

    assert!(matches!(
        cart.snapshot().as_deref(),
        Some(CartState { title, .. }) if title == "Recovered"
    ));
}

#[test]
fn an_op_outside_the_allowlist_recovers_while_a_non_envelope_is_only_dropped() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (join, cart) = harness.mount(&mut server, "cart");

    // Not an envelope at all: dropped, exactly as the TypeScript client does.
    server.push_event(&join, "patch", json!({"hello": "world"}));
    harness.pump();
    assert!(server.sent(&mut harness).is_empty(), "no recovery");

    // A `move` op is a protocol violation (BDR-0014) and recovers the root.
    server.push_event(
        &join,
        "patch",
        envelope(
            1,
            2,
            json!([{"op": "move", "from": "/title", "path": "/other"}]),
        ),
    );
    harness.pump();

    assert!(matches!(
        server.sent(&mut harness).as_slice(),
        [Message { event: leave, .. }, Message { event: rejoin, .. }]
            if leave == "phx_leave" && rejoin == "phx_join"
    ));
    assert!(matches!(
        cart.snapshot().as_deref(),
        Some(CartState { title, .. }) if title == "Cart"
    ));
}

#[test]
fn a_failed_re_join_during_recovery_keeps_the_last_good_state() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (join, cart) = harness.mount(&mut server, "cart");

    server.push_event(&join, "patch", envelope(7, 8, json!([])));
    harness.pump();
    let sent = server.sent(&mut harness);
    server.reply(
        &sent[1],
        ReplyStatus::Error,
        json!({"reason": "unknown root"}),
    );
    harness.pump();

    assert!(
        matches!(
            cart.snapshot().as_deref(),
            Some(CartState { title, .. }) if title == "Cart"
        ),
        "a failed re-join must not blank the consumer"
    );

    // The transport keeps rejoining; the join-ok hook finishes the recovery.
    harness.fire_backoff();
    let rejoin = server.sent(&mut harness);
    assert!(matches!(
        rejoin.as_slice(),
        [Message { event, .. }] if event == "phx_join"
    ));
}

#[test]
fn a_rejoin_after_a_transport_drop_re_arms_the_initial_patch_waiter() {
    let mut harness = Harness::new();
    let mut first = harness.queue_socket();
    let (_join, cart) = harness.mount(&mut first, "cart");
    let mut updates = cart.updates();

    let mut second = harness.queue_socket();
    first.disconnect();
    harness.pump();
    assert!(cart.snapshot().is_some(), "last-good state is kept");

    harness.fire_backoff();
    let rejoin = second.sent(&mut harness);
    second.reply(&rejoin[0], ReplyStatus::Ok, json!({"root_id": ROOT_ID}));
    harness.pump();
    second.push_event(
        &rejoin[0],
        "patch",
        envelope(
            0,
            1,
            json!([{
                "op": "replace",
                "path": "",
                "value": {"__musubi_store_id__": [], "title": "Rejoined", "messages": []}
            }]),
        ),
    );
    harness.pump();

    assert!(matches!(
        cart.snapshot().as_deref(),
        Some(CartState { title, .. }) if title == "Rejoined"
    ));
    assert!(matches!(
        drain(&mut updates).as_slice(),
        [state] if state.title == "Rejoined"
    ));
}

// ---------------------------------------------------------------------------
// Subscriptions
// ---------------------------------------------------------------------------

#[test]
fn updates_yield_one_item_per_accepted_envelope() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (join, cart) = harness.mount(&mut server, "cart");
    let mut updates = cart.updates();

    server.push_event(
        &join,
        "patch",
        envelope(
            1,
            2,
            json!([{"op": "replace", "path": "/title", "value": "Second"}]),
        ),
    );
    harness.pump();
    server.push_event(
        &join,
        "patch",
        envelope(
            2,
            3,
            json!([{"op": "replace", "path": "/title", "value": "Third"}]),
        ),
    );
    harness.pump();

    assert!(matches!(
        drain(&mut updates).as_slice(),
        [second, third] if second.title == "Second" && third.title == "Third"
    ));
}

#[test]
fn push_events_are_dispatched_to_their_store_after_the_state_is_published() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (join, cart) = harness.mount(&mut server, "cart");
    let mut toasts = cart.events::<ToastPayload, _>(&StoreId::root());

    let mut payload = envelope(
        1,
        2,
        json!([{"op": "replace", "path": "/title", "value": "Second"}]),
    );
    payload["events"] = json!([
        {"store_id": [], "name": "toast", "payload": {"message": "saved"}},
        {"store_id": ["panel"], "name": "toast", "payload": {"message": "other store"}},
        {"store_id": [], "name": "toast", "payload": {"unexpected": true}},
    ]);
    server.push_event(&join, "patch", payload);
    harness.pump();

    assert!(matches!(
        cart.snapshot().as_deref(),
        Some(CartState { title, .. }) if title == "Second"
    ));
    assert!(
        matches!(
            drain(&mut toasts).as_slice(),
            [ToastPayload { message }] if message == "saved"
        ),
        "another store's events and undeserializable payloads are dropped"
    );
}

#[test]
fn a_mount_whose_future_was_dropped_gives_its_hold_back() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();

    harness.abandon_mount("cart");

    let sent = server.sent(&mut harness);
    server.reply(&sent[0], ReplyStatus::Ok, json!({"root_id": ROOT_ID}));
    harness.pump();
    server.push_event(&sent[0], "patch", initial_envelope());
    harness.pump();

    // The hold the mount took before awaiting is the root's only one, so
    // publishing to a receiver that is gone must leave the channel.
    assert!(matches!(
        server.sent(&mut harness).as_slice(),
        [Message { event, topic, .. }] if event == "phx_leave" && topic == TOPIC
    ));
}

#[test]
fn an_aliasing_mount_whose_future_was_dropped_gives_its_hold_back() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (_join, cart) = harness.mount(&mut server, "cart");

    harness.abandon_mount("cart");
    harness.pump();
    assert!(server.sent(&mut harness).is_empty(), "no second phx_join");

    // The alias took a second hold before replying; with it leaked, the last
    // live handle going away would never reach refcount 0.
    drop(cart);
    harness.pump();
    assert!(matches!(
        server.sent(&mut harness).as_slice(),
        [Message { event, topic, .. }] if event == "phx_leave" && topic == TOPIC
    ));
}

#[test]
fn state_that_does_not_match_the_generated_types_fails_the_mount_with_decode() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let pending = harness.mount_later("cart", json!({}));

    let sent = server.sent(&mut harness);
    server.reply(&sent[0], ReplyStatus::Ok, json!({"root_id": ROOT_ID}));
    harness.pump();

    // `title` is gone: the bundle was generated against another server (§11).
    server.push_event(
        &sent[0],
        "patch",
        envelope(
            0,
            1,
            json!([{
                "op": "replace",
                "path": "",
                "value": {"__musubi_store_id__": [], "messages": []}
            }]),
        ),
    );

    assert!(matches!(
        harness.settle(pending),
        Err(MusubiError::Decode { store_id, .. }) if store_id == StoreId::root()
    ));
}

#[test]
fn a_rejected_envelope_on_a_published_root_awaiting_its_initial_patch_recovers() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (join, _cart) = harness.mount(&mut server, "cart");

    // A rejoin puts the engine back at version 0 with nobody waiting on it.
    server.reply(&join, ReplyStatus::Ok, json!({"root_id": ROOT_ID}));
    harness.pump();

    // A malformed initial envelope would otherwise pin the engine at 0 forever,
    // rejecting every later envelope on the same check.
    server.push_event(&join, "patch", envelope(7, 8, json!([])));
    harness.pump();

    assert!(matches!(
        server.sent(&mut harness).as_slice(),
        [
            Message { event: leave, .. },
            Message { event: rejoin, .. },
        ] if leave == "phx_leave" && rejoin == "phx_join"
    ));
}

// ---------------------------------------------------------------------------
// Generated-code stand-ins
// ---------------------------------------------------------------------------

/// The zero-sized marker `mix compile.musubi_rust` emits per store.
struct CartStore;

impl Store for CartStore {
    const MODULE: &'static str = MODULE;
    type State = CartState;
}

#[derive(Debug, Deserialize)]
struct CartState {
    title: String,
    /// `stream(Message)` renders as a plain `Vec`: hydration substitutes the
    /// materialized array before serde runs.
    messages: Vec<ChatMessage>,
}

#[derive(Debug, Deserialize)]
struct ChatMessage {
    id: String,
    body: String,
}

#[derive(Serialize)]
struct Checkout {
    coupon: Option<String>,
}

impl Command<CartStore> for Checkout {
    const NAME: &'static str = "checkout";
    type Reply = CheckoutReply;
}

#[derive(Debug, Deserialize)]
struct CheckoutReply {
    order_id: String,
}

#[derive(Serialize)]
struct Refresh {}

impl Command<CartStore> for Refresh {
    const NAME: &'static str = "refresh";
    type Reply = NoReply;
}

#[derive(Debug, Deserialize)]
struct ToastPayload {
    message: String,
}

impl Event<CartStore> for ToastPayload {
    const NAME: &'static str = "toast";
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

/// The envelope every `mount` helper answers the first join with.
fn initial_envelope() -> Value {
    let mut envelope = envelope(
        0,
        1,
        json!([{
            "op": "replace",
            "path": "",
            "value": {
                "__musubi_store_id__": [],
                "title": "Cart",
                "messages": {"__musubi_stream__": "messages"}
            }
        }]),
    );

    envelope["stream_ops"] = json!([{
        "op": "insert",
        "stream": "messages",
        "ref": "0",
        "store_id": [],
        "item_key": "m-1",
        "at": -1,
        "item": {"id": "m-1", "body": "hello"},
        "limit": null
    }]);

    envelope
}

fn envelope(base_version: u64, version: u64, ops: Value) -> Value {
    json!({
        "type": "patch",
        "root_id": ROOT_ID,
        "base_version": base_version,
        "version": version,
        "ops": ops,
        "stream_ops": []
    })
}

/// The shared rig (`phoenix-channel/tests/common/mod.rs`) wired to one
/// [`Connection`].
type Harness = common::Harness<Connection>;

impl Harness {
    fn new() -> Self {
        Self::new_with(|seams: Seams| {
            Connection::builder()
                .url("wss://example.test/socket")
                .connector(seams.connector)
                .spawner(seams.spawner)
                .timer(seams.timer)
                .push_timeout(PUSH_TIMEOUT)
                .build()
                .expect("every seam is set")
        })
    }

    /// Mounts a root the whole way: join, join ok, initial patch. Returns the
    /// join message (every server push has to echo its `join_ref`) and the
    /// handle.
    fn mount(&mut self, server: &mut ServerEnd, id: &str) -> (Message, Mounted<CartStore>) {
        let pending = self.mount_later(id, json!({"currency": "EUR"}));
        let sent = server.sent(self);

        server.reply(&sent[0], ReplyStatus::Ok, json!({"root_id": ROOT_ID}));
        self.pump();

        let mounted = self.finish_mount(server, &sent[0], pending);

        (sent.into_iter().next().expect("one join"), mounted)
    }

    fn finish_mount(
        &mut self,
        server: &mut ServerEnd,
        join: &Message,
        pending: Slot<musubi_client::Result<Mounted<CartStore>>>,
    ) -> Mounted<CartStore> {
        server.push_event(join, "patch", initial_envelope());

        match self.settle(pending) {
            Ok(mounted) => mounted,
            Err(error) => panic!("mount failed: {error}"),
        }
    }

    fn mount_later(
        &mut self,
        id: &str,
        params: Value,
    ) -> Slot<musubi_client::Result<Mounted<CartStore>>> {
        let connection = self.inner.clone();
        let id = id.to_owned();

        self.spawn_capture(async move { connection.mount::<CartStore>(&id, params).await })
    }

    /// Starts a mount, lets it register with the actor, then drops the future —
    /// the `tokio::time::timeout(.., conn.mount(..))` case, where the oneshot
    /// receiver is gone before the actor can hand the cell over.
    fn abandon_mount(&mut self, id: &str) {
        let connection = self.inner.clone();
        let mut mounting = Box::pin(connection.mount::<CartStore>(id, json!({})));
        let waker = noop_waker();

        assert!(
            mounting
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending(),
            "the mount request is sent, then awaits the initial patch"
        );

        drop(mounting);
    }

    fn command_later<C>(
        &mut self,
        cart: &Mounted<CartStore>,
        cmd: C,
    ) -> Slot<Result<C::Reply, MusubiError>>
    where
        C: Command<CartStore>,
    {
        let cart = cart.clone();

        self.spawn_capture(async move { cart.command(cmd).await })
    }

    fn disconnect(&mut self) {
        let connection = self.inner.clone();
        let slot = self.spawn_capture(async move { connection.disconnect().await });

        assert!(self.settle(slot).is_ok());
    }
}
