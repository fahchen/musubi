//! Protocol tests for the connection layer over a scripted `MockSocket` plus a
//! `ManualTimer` (`docs/rust-client.md` §12, layer 3): mount/join, duplicate
//! mount aliasing, drop-at-refcount-0 teardown, version-gap recovery including
//! a failed re-join, bulk command rejection per teardown path,
//! a command reply that is not gated on its patch, and push-event dispatch.
//!
//! Nothing here sleeps: the executor is a `LocalPool` the test pumps by hand
//! and every timer fires only when a test says so.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::task::Context;
use std::time::Duration;

use futures_channel::oneshot;
use futures_core::future::BoxFuture;
use futures_executor::block_on;
use futures_util::task::noop_waker;
use musubi_client::generated::{Command, Event, NoReply, Store, StoreId};
use musubi_client::{
    CACHE_WRITE_THROTTLE, CacheEntry, CacheStore, CommandError, Connection, EntryStatus,
    MemoryCacheStore, MountStatus, Mounted, MusubiError, UploadEntry, cache_key, now_ms,
};
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
/// The socket layer's default heartbeat interval, which the harness keeps.
const HEARTBEAT: Duration = Duration::from_secs(30);
/// The shape token `Harness::new_cached` writes and reads entries under.
const BUSTER: &str = "v1";

// ---------------------------------------------------------------------------
// Mount
// ---------------------------------------------------------------------------

#[test]
fn mount_joins_one_channel_per_root_and_hydrates_the_initial_patch() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let pending = harness.mount_later("cart", currency("EUR"));

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
    let pending = harness.mount_later("cart", CartParams::default());

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
    let pending = harness.mount_later("cart", CartParams::default());

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
fn mount_with_params_sends_keys_the_generated_params_struct_has_no_attr_for() {
    // `attr/3` is the child-store assign contract, not the server's mount-param
    // contract: `mount/2` reads the join payload's `params` map as it arrives.
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let connection = harness.inner.clone();
    let pending = harness.spawn_capture(async move {
        connection
            .mount_with_params::<CartStore>("cart", json!({"currency": "EUR", "invite": "tok"}))
            .await
    });

    let sent = server.sent(&mut harness);
    assert!(matches!(
        sent.as_slice(),
        [Message { event, payload, .. }]
            if event == "phx_join"
                && payload["params"] == json!({"currency": "EUR", "invite": "tok"})
    ));

    server.reply(&sent[0], ReplyStatus::Ok, json!({"root_id": ROOT_ID}));
    harness.pump();

    let cart = harness.finish_mount(&mut server, &sent[0], pending);
    assert_eq!(
        cart.snapshot().expect("the initial patch landed").title,
        "Cart"
    );
}

#[test]
fn mount_params_must_be_a_json_object() {
    let mut harness = Harness::new();
    let _server = harness.queue_socket();
    let connection = harness.inner.clone();
    let pending = harness.spawn_capture(async move {
        connection
            .mount::<OddParamsStore>("cart", OddParams("not-an-object"))
            .await
    });

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

    let pending = harness.mount_later("cart", CartParams::default());

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
    let first = harness.mount_later("cart", currency("EUR"));

    let sent = server.sent(&mut harness);
    server.reply(&sent[0], ReplyStatus::Ok, json!({"root_id": ROOT_ID}));
    harness.pump();

    // First-mount params win; the mismatched ones are ignored with a warning.
    let second = harness.mount_later("cart", currency("USD"));
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
    let first = harness.mount_later("cart", CartParams::default());
    let sent = server.sent(&mut harness);
    let second = harness.mount_later("cart", CartParams::default());
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
    let third = harness.mount_later("cart", CartParams::default());
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
fn a_command_reply_is_not_gated_on_the_patch_it_caused() {
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
    let mut statuses = cart.status_updates();

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
    assert!(ended(&mut statuses), "the status stream ends with the root");
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
fn an_envelope_addressed_to_another_root_fails_the_mount_waiting_on_it() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let pending = harness.mount_later("cart", CartParams::default());

    let sent = server.sent(&mut harness);
    server.reply(&sent[0], ReplyStatus::Ok, json!({"root_id": ROOT_ID}));
    harness.pump();

    // Unreachable against a correct server — the id is stamped by the one page
    // process bound to this channel — but a dropped envelope is the one thing
    // that can leave `mount(..).await` waiting forever.
    let mut payload = initial_envelope();
    payload["root_id"] = json!("MyApp.Stores.CartStore:other");
    server.push_event(&sent[0], "patch", payload);

    assert!(matches!(
        harness.settle(pending),
        Err(MusubiError::Protocol(message))
            if message == "patch envelope was addressed to another root"
    ));
}

#[test]
fn an_envelope_addressed_to_another_root_recovers_a_published_root() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (join, cart) = harness.mount(&mut server, "cart");

    let mut payload = envelope(
        1,
        2,
        json!([{"op": "replace", "path": "/title", "value": "Second"}]),
    );
    payload["root_id"] = json!("MyApp.Stores.CartStore:other");
    server.push_event(&join, "patch", payload);
    harness.pump();

    // A published root has no pending mount to fail, so nothing else would
    // ever move it off the version it is stuck on: only a rejoin does.
    assert!(matches!(
        server.sent(&mut harness).as_slice(),
        [Message { event: leave, .. }, Message { event: rejoin, .. }]
            if leave == "phx_leave" && rejoin == "phx_join"
    ));
    assert!(
        matches!(
            cart.snapshot().as_deref(),
            Some(CartState { title, .. }) if title == "Cart"
        ),
        "the last-good tree keeps rendering through the recreate window"
    );
    assert_eq!(cart.status(), MountStatus::Reconnecting);
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
// Mount status (BDR-0033)
// ---------------------------------------------------------------------------

#[test]
fn a_cold_mount_is_live_once_its_initial_patch_has_been_accepted() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (_join, cart) = harness.mount(&mut server, "cart");

    assert_eq!(cart.status(), MountStatus::Live);
}

#[test]
fn a_seeded_mount_stays_connecting_until_the_live_initial_patch() {
    let store = Arc::new(MemoryCacheStore::new());
    seed(&store, cached_tree("Cached cart"), BUSTER);

    let mut harness = Harness::new_cached(&store);
    let mut server = harness.queue_socket();
    let pending = harness.mount_later("cart", currency("EUR"));
    let sent = server.sent(&mut harness);
    let cart = harness.settle(pending).expect("the seeded mount resolves");
    let mut statuses = cart.status_updates();

    // Rendering cached state is not being live: the seed is last-known data,
    // not an accepted initial patch.
    assert_eq!(cart.status(), MountStatus::Connecting);
    // A fresh subscription opens with the current status rather than the next
    // transition, so the pill is right without reading `status()` first.
    assert_eq!(drain(&mut statuses), vec![MountStatus::Connecting]);

    server.reply(&sent[0], ReplyStatus::Ok, json!({"root_id": ROOT_ID}));
    harness.pump();
    server.push_event(&sent[0], "patch", initial_envelope());
    harness.pump();

    assert_eq!(cart.status(), MountStatus::Live);
    assert_eq!(drain(&mut statuses), vec![MountStatus::Live]);
}

#[test]
fn a_transport_drop_reports_reconnecting_and_the_rejoins_fresh_patch_restores_live() {
    let mut harness = Harness::new();
    let mut first = harness.queue_socket();
    let (_join, cart) = harness.mount(&mut first, "cart");
    // A consumer that keeps up sees every edge; the drains below are what
    // "keeping up" means for a latest-value stream.
    let mut statuses = cart.status_updates();
    assert_eq!(drain(&mut statuses), vec![MountStatus::Live]);

    let mut second = harness.queue_socket();
    first.disconnect();
    harness.pump();

    assert_eq!(cart.status(), MountStatus::Reconnecting);
    assert_eq!(drain(&mut statuses), vec![MountStatus::Reconnecting]);
    assert!(
        cart.snapshot().is_some(),
        "the last-good tree keeps rendering through the window (BDR-0015)"
    );

    harness.fire_backoff();
    let rejoin = second.sent(&mut harness);
    second.reply(&rejoin[0], ReplyStatus::Ok, json!({"root_id": ROOT_ID}));
    harness.pump();

    // The rejoin alone is not recovery: live returns only with the fresh
    // initial patch that swaps the state in.
    assert_eq!(cart.status(), MountStatus::Reconnecting);
    assert!(drain(&mut statuses).is_empty(), "no edge was crossed");

    second.push_event(&rejoin[0], "patch", initial_envelope());
    harness.pump();

    assert_eq!(cart.status(), MountStatus::Live);
    assert_eq!(drain(&mut statuses), vec![MountStatus::Live]);
}

#[test]
fn a_heartbeat_timeout_reports_reconnecting_without_any_command() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (_join, cart) = harness.mount(&mut server, "cart");
    let mut statuses = cart.status_updates();

    harness.fire(HEARTBEAT);
    assert!(
        matches!(
            server.sent(&mut harness).as_slice(),
            [Message { event, .. }] if event == "heartbeat"
        ),
        "nothing but the heartbeat itself goes out"
    );
    // Unanswered for a full interval: the socket is declared dead even though
    // the transport has not noticed — and no command was ever dispatched.
    harness.fire(HEARTBEAT);

    assert_eq!(cart.status(), MountStatus::Reconnecting);
    assert_eq!(drain(&mut statuses), vec![MountStatus::Reconnecting]);
    assert!(
        cart.snapshot().is_some(),
        "the last-good tree keeps rendering"
    );
}

#[test]
fn a_version_gap_recovery_passes_through_reconnecting() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (join, cart) = harness.mount(&mut server, "cart");
    let mut statuses = cart.status_updates();
    assert_eq!(drain(&mut statuses), vec![MountStatus::Live]);

    server.push_event(&join, "patch", envelope(7, 8, json!([])));
    harness.pump();

    assert_eq!(cart.status(), MountStatus::Reconnecting);
    assert_eq!(drain(&mut statuses), vec![MountStatus::Reconnecting]);

    // Recovery left the diverged channel and joined a fresh one; its initial
    // patch completes the loop.
    let sent = server.sent(&mut harness);
    let rejoin = &sent[1];
    server.reply(rejoin, ReplyStatus::Ok, json!({"root_id": ROOT_ID}));
    harness.pump();
    server.push_event(rejoin, "patch", initial_envelope());
    harness.pump();

    assert_eq!(cart.status(), MountStatus::Live);
    assert_eq!(drain(&mut statuses), vec![MountStatus::Live]);
}

#[test]
fn a_root_that_was_never_live_stays_connecting_through_a_socket_drop() {
    let store = Arc::new(MemoryCacheStore::new());
    seed(&store, cached_tree("Cached cart"), BUSTER);

    let mut harness = Harness::new_cached(&store);
    let mut server = harness.queue_socket();
    let pending = harness.mount_later("cart", currency("EUR"));
    server.sent(&mut harness);
    let cart = harness.settle(pending).expect("the seeded mount resolves");
    let mut statuses = cart.status_updates();
    // The opening replay, not a transition — consumed here so the assertion
    // below is about edges only.
    assert_eq!(drain(&mut statuses), vec![MountStatus::Connecting]);

    let _next = harness.queue_socket();
    server.disconnect();
    harness.pump();

    // Socket churn before the root was ever live is still `Connecting`; only
    // a root that has been live can be `Reconnecting`.
    assert_eq!(cart.status(), MountStatus::Connecting);
    assert!(drain(&mut statuses).is_empty(), "no edge was crossed");
}

// ---------------------------------------------------------------------------
// Subscriptions
// ---------------------------------------------------------------------------

#[test]
fn updates_open_with_the_current_state_and_coalesce_to_the_latest_one() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (join, cart) = harness.mount(&mut server, "cart");
    let mut updates = cart.updates();

    // The subscription opens with what `snapshot()` holds: reading it first is
    // a first-paint convenience, not a window a consumer has to close.
    assert!(matches!(
        drain(&mut updates).as_slice(),
        [initial] if initial.title == "Cart"
    ));

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

    // Two accepted envelopes, one item: each state is a whole root that
    // subsumes the one before it, so a consumer that was not polling gets
    // where the root ended up instead of a backlog to replay.
    assert!(matches!(
        drain(&mut updates).as_slice(),
        [third] if third.title == "Third"
    ));
    // And a subscription taken now opens on the same value — no consumer can
    // observe the state that was skipped.
    assert!(matches!(
        drain(&mut cart.updates()).as_slice(),
        [third] if third.title == "Third"
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
fn a_subscription_taken_after_teardown_ends_instead_of_waiting_forever() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (_join, cart) = harness.mount(&mut server, "cart");
    let mut before = cart.events::<ToastPayload, _>(&StoreId::root());
    let panel: StoreId = serde_json::from_value(json!(["panel"])).expect("a child store path");

    // The handle outlives the root, which is the documented disconnect case:
    // nothing rejoins afterwards, so a subscription taken now has no sender
    // that could ever write to it.
    harness.disconnect();

    assert!(ended(&mut before), "a live subscription ends with the root");
    assert!(
        ended(&mut cart.events::<ToastPayload, _>(&StoreId::root())),
        "a key the cleared registry used to have"
    );
    assert!(
        ended(&mut cart.events::<ToastPayload, _>(&panel)),
        "and a key it never had — closure is the registry's, not a key's"
    );
    // The same rule the state and status cells already keep (`Latest::close`).
    assert!(ended(&mut cart.updates()));
    assert!(ended(&mut cart.status_updates()));
}

#[test]
fn a_dropped_events_stream_is_unregistered_without_closing_its_key() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (join, cart) = harness.mount(&mut server, "cart");
    let dropped = cart.events::<ToastPayload, _>(&StoreId::root());

    drop(dropped);
    server.push_event(&join, "patch", toast(1, 2, "first"));
    harness.pump();

    // Pruning the last subscriber of a key must not tombstone the key: a
    // subscription taken afterwards is live, not an ended stream.
    let mut fresh = cart.events::<ToastPayload, _>(&StoreId::root());

    server.push_event(&join, "patch", toast(2, 3, "second"));
    harness.pump();

    assert!(matches!(
        drain(&mut fresh).as_slice(),
        [ToastPayload { message }] if message == "second"
    ));
}

#[test]
fn upload_ops_reach_the_handles_the_root_hands_out() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (join, cart) = harness.mount(&mut server, "cart");
    let avatar = cart.upload(&StoreId::root(), "avatar");
    let mut updates = avatar.updates();

    let mut payload = envelope(1, 2, json!([]));
    payload["upload_ops"] = json!([
        {
            "op": "add", "upload": "avatar", "store_id": [], "ref": "u_1",
            "entry": {
                "ref": "u_1", "client_name": "me.png", "client_size": 1234,
                "client_type": "image/png", "progress": 0, "status": "pending",
                "errors": []
            }
        },
        {"op": "progress", "upload": "avatar", "store_id": [], "ref": "u_1", "progress": 60},
    ]);
    server.push_event(&join, "patch", payload);
    harness.pump();

    // The handle taken before the ops landed is the one the actor folded into.
    assert!(matches!(
        avatar.snapshot().entry("u_1"),
        Some(UploadEntry {
            progress: 60,
            status: EntryStatus::Uploading,
            ..
        })
    ));
    assert!(matches!(
        drain(&mut updates).as_slice(),
        [handle] if handle.progress() == 60
    ));
}

#[test]
fn unmounting_a_root_ends_its_upload_streams() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (_join, cart) = harness.mount(&mut server, "cart");
    let mut updates = cart.upload(&StoreId::root(), "avatar").updates();

    drop(cart);
    harness.pump();

    assert!(ended(&mut updates));
}

#[test]
fn an_upload_subscription_taken_after_teardown_ends_instead_of_waiting_forever() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (_join, cart) = harness.mount(&mut server, "cart");
    let held = cart.upload(&StoreId::root(), "avatar");

    // The handle outlives the root, exactly as `events()` can: nothing rejoins
    // afterwards, so neither a handle taken before the teardown nor one taken
    // through the retired root after it can ever be published to.
    harness.disconnect();

    assert!(
        ended(&mut held.updates()),
        "a handle kept across the teardown"
    );
    assert!(
        ended(&mut cart.upload(&StoreId::root(), "avatar").updates()),
        "and one taken from the root afterwards, which the registry no longer has a cell for"
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
fn a_mount_abandoned_after_the_actor_answered_gives_its_hold_back() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let connection = harness.inner.clone();
    let mut mounting = Box::pin(connection.mount::<CartStore>("cart", CartParams::default()));
    let waker = noop_waker();

    assert!(
        mounting
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending(),
        "the mount request is sent, then awaits the initial patch"
    );

    let sent = server.sent(&mut harness);
    server.reply(&sent[0], ReplyStatus::Ok, json!({"root_id": ROOT_ID}));
    harness.pump();
    server.push_event(&sent[0], "patch", initial_envelope());
    harness.pump();

    // The other half of the window the twin above covers: the cell was *sent*
    // — the receiver was alive at that instant — and the future is dropped
    // before it ever polls again, so nobody ever owns what the actor handed
    // over. The hold has to come back with the value, not with the send's
    // return code.
    drop(mounting);
    harness.pump();

    assert!(matches!(
        server.sent(&mut harness).as_slice(),
        [Message { event, topic, .. }] if event == "phx_leave" && topic == TOPIC
    ));
}

#[test]
fn an_aliasing_mount_abandoned_after_the_actor_answered_gives_its_hold_back() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (_join, cart) = harness.mount(&mut server, "cart");
    let connection = harness.inner.clone();
    let mut mounting = Box::pin(connection.mount::<CartStore>("cart", CartParams::default()));
    let waker = noop_waker();

    assert!(
        mounting
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending(),
        "the alias is one message to the actor, which has not run yet"
    );

    harness.pump();
    assert!(server.sent(&mut harness).is_empty(), "no second phx_join");

    // The alias answered a live receiver and *then* lost it, which is the same
    // window on the aliasing path.
    drop(mounting);
    harness.pump();

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
    let pending = harness.mount_later("cart", CartParams::default());

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
fn an_envelope_the_generated_types_reject_lands_none_of_what_travelled_with_it() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (join, cart) = harness.mount(&mut server, "cart");
    let avatar = cart.upload(&StoreId::root(), "avatar");
    let mut uploads = avatar.updates();
    let mut updates = cart.updates();

    // The replay of the state the mount already published.
    assert_eq!(drain(&mut updates).len(), 1);

    // `title` is gone (§11) — and a stream op and an upload op are travelling
    // on the same envelope, neither of which the deserialize gets to see.
    let mut payload = envelope(
        1,
        2,
        json!([{
            "op": "replace",
            "path": "",
            "value": {"__musubi_store_id__": [], "messages": {"__musubi_stream__": "messages"}}
        }]),
    );

    payload["stream_ops"] = json!([{
        "op": "insert", "stream": "messages", "store_id": [], "item_key": "m-2",
        "at": -1, "item": {"id": "m-2", "body": "leaked"}, "limit": null
    }]);
    payload["upload_ops"] = json!([{
        "op": "add", "upload": "avatar", "store_id": [], "ref": "u_1",
        "entry": {
            "ref": "u_1", "client_name": "me.png", "client_size": 1234,
            "client_type": "image/png", "progress": 0, "status": "pending",
            "errors": []
        }
    }]);

    server.push_event(&join, "patch", payload);
    harness.pump();

    // The last-good rendering is kept, and nothing else moved either: an
    // upload subscriber must not run ahead of an envelope the embedder never
    // saw.
    assert!(matches!(
        cart.snapshot(),
        Some(state) if state.title == "Cart" && state.messages.len() == 1
    ));
    assert!(drain(&mut updates).is_empty(), "no state was published");
    assert!(avatar.snapshot().entry("u_1").is_none());
    assert!(drain(&mut uploads).is_empty(), "no upload op was published");

    // The root recovers, and the rejoin's fresh initial patch renders without a
    // trace of the rejected envelope's stream op.
    let sent = server.sent(&mut harness);
    assert!(matches!(
        sent.as_slice(),
        [
            Message { event: leave, .. },
            Message { event: rejoin, .. },
        ] if leave == "phx_leave" && rejoin == "phx_join"
    ));

    server.reply(&sent[1], ReplyStatus::Ok, json!({"root_id": ROOT_ID}));
    harness.pump();
    server.push_event(&sent[1], "patch", initial_envelope());
    harness.pump();

    assert!(matches!(
        cart.snapshot(),
        Some(state) if matches!(state.messages.as_slice(), [message] if message.id == "m-1")
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
// Stale-while-revalidate cache (§6.4)
// ---------------------------------------------------------------------------

#[test]
fn a_cache_hit_renders_before_the_initial_patch_and_is_replaced_by_it() {
    let store = Arc::new(MemoryCacheStore::new());
    seed(&store, cached_tree("Cached cart"), BUSTER);

    let mut harness = Harness::new_cached(&store);
    let mut server = harness.queue_socket();
    let pending = harness.mount_later("cart", currency("EUR"));

    // The join is out, but the server has answered nothing at all yet.
    let sent = server.sent(&mut harness);
    let cart = match harness.settle(pending) {
        Ok(mounted) => mounted,
        Err(error) => panic!("the seeded mount resolves: {error}"),
    };

    let seeded = cart.snapshot().expect("the cache entry was published");
    assert!(matches!(
        seeded.as_ref(),
        CartState { title, messages }
            // Streams are not cached, so a seeded stream slot hydrates empty.
            if title == "Cached cart" && messages.is_empty()
    ));

    // The live initial patch swaps the whole tree out atomically.
    server.reply(&sent[0], ReplyStatus::Ok, json!({"root_id": ROOT_ID}));
    harness.pump();
    server.push_event(&sent[0], "patch", initial_envelope());
    harness.pump();

    let live = cart.snapshot().expect("the initial patch has been applied");
    assert!(matches!(
        live.as_ref(),
        CartState { title, messages } if title == "Cart" && messages.len() == 1
    ));
}

#[test]
fn a_stale_entry_is_evicted_and_the_mount_falls_back_to_the_cold_path() {
    let store = Arc::new(MemoryCacheStore::new());
    // Written by a build whose state shape is not this one's.
    seed(&store, cached_tree("Cached cart"), "older-build");

    let mut harness = Harness::new_cached(&store);
    let mut server = harness.queue_socket();
    let pending = harness.mount_later("cart", currency("EUR"));

    let sent = server.sent(&mut harness);
    assert!(
        harness.peek(&pending).is_none(),
        "no seed, so no early mount"
    );
    assert_eq!(
        block_on(store.get(&cart_key())),
        None,
        "the entry is dropped"
    );

    server.reply(&sent[0], ReplyStatus::Ok, json!({"root_id": ROOT_ID}));
    harness.pump();

    let cart = harness.finish_mount(&mut server, &sent[0], pending);
    assert_eq!(
        cart.snapshot().expect("the initial patch landed").title,
        "Cart"
    );
}

#[test]
fn a_cached_tree_the_generated_types_reject_is_dropped_rather_than_published() {
    let store = Arc::new(MemoryCacheStore::new());
    // `title` is a `String` on this build; the cached tree predates that.
    seed(
        &store,
        json!({"__musubi_store_id__": [], "title": 42}),
        BUSTER,
    );

    let mut harness = Harness::new_cached(&store);
    let mut server = harness.queue_socket();
    let pending = harness.mount_later("cart", currency("EUR"));

    let sent = server.sent(&mut harness);
    assert!(
        harness.peek(&pending).is_none(),
        "a seed that cannot be deserialized never resolves the mount"
    );
    assert_eq!(
        block_on(store.get(&cart_key())),
        None,
        "the entry is dropped"
    );

    server.reply(&sent[0], ReplyStatus::Ok, json!({"root_id": ROOT_ID}));
    harness.pump();

    let cart = harness.finish_mount(&mut server, &sent[0], pending);
    assert_eq!(
        cart.snapshot().expect("the initial patch landed").title,
        "Cart"
    );
}

#[test]
fn dispatches_on_a_seeded_root_flush_in_order_once_the_initial_patch_lands() {
    let store = Arc::new(MemoryCacheStore::new());
    seed(&store, cached_tree("Cached cart"), BUSTER);

    let mut harness = Harness::new_cached(&store);
    let mut server = harness.queue_socket();
    let pending = harness.mount_later("cart", currency("EUR"));
    let sent = server.sent(&mut harness);
    let cart = harness.settle(pending).expect("the seeded mount resolves");

    let checkout = harness.command_later(&cart, Checkout { coupon: None });
    let refresh = harness.command_later(&cart, Refresh {});

    assert!(
        server.sent(&mut harness).is_empty(),
        "both dispatches wait for the live initial patch"
    );

    server.reply(&sent[0], ReplyStatus::Ok, json!({"root_id": ROOT_ID}));
    harness.pump();
    server.push_event(&sent[0], "patch", initial_envelope());

    let pushes = server.sent(&mut harness);
    assert!(matches!(
        pushes.as_slice(),
        [
            Message { event: first, payload: checkout_payload, .. },
            Message { event: second, payload: refresh_payload, .. },
        ] if first == "command"
            && second == "command"
            && checkout_payload["name"] == json!("checkout")
            && refresh_payload["name"] == json!("refresh")
    ));

    server.reply(&pushes[0], ReplyStatus::Ok, json!({"order_id": "order-1"}));
    server.reply(&pushes[1], ReplyStatus::Ok, json!({}));

    assert!(matches!(
        harness.settle(checkout),
        Ok(CheckoutReply { order_id }) if order_id == "order-1"
    ));
    assert!(harness.settle(refresh).is_ok());
}

#[test]
fn a_version_gap_during_revalidation_rejects_the_queued_dispatches() {
    let store = Arc::new(MemoryCacheStore::new());
    seed(&store, cached_tree("Cached cart"), BUSTER);

    let mut harness = Harness::new_cached(&store);
    let mut server = harness.queue_socket();
    let pending = harness.mount_later("cart", currency("EUR"));
    let sent = server.sent(&mut harness);
    let cart = harness.settle(pending).expect("the seeded mount resolves");

    server.reply(&sent[0], ReplyStatus::Ok, json!({"root_id": ROOT_ID}));
    harness.pump();

    let queued = harness.command_later(&cart, Checkout { coupon: None });

    // Revalidation produced an envelope that does not start at version 1.
    server.push_event(&sent[0], "patch", envelope(4, 5, json!([])));

    assert!(matches!(
        harness.settle(queued),
        Err(MusubiError::VersionMismatch)
    ));

    // And the root is back to the plain contract: no queueing behind a
    // revalidation that is not coming.
    let after = harness.command_later(&cart, Checkout { coupon: None });
    assert!(matches!(
        harness.settle(after),
        Err(MusubiError::NotConnected)
    ));
}

#[test]
fn a_dispatch_mid_reconnect_is_rejected_even_with_a_cache_configured() {
    // Queueing is a property of the *seed*, not of the cache being on: a root
    // that reached version 1 on its own is back to the plain §6.2 contract as
    // soon as a reconnect puts it at 0.
    let store = Arc::new(MemoryCacheStore::new());
    let mut harness = Harness::new_cached(&store);
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
}

#[test]
fn a_seeded_roots_dispatch_queue_is_bounded() {
    let store = Arc::new(MemoryCacheStore::new());
    seed(&store, cached_tree("Cached cart"), BUSTER);

    let mut harness = Harness::new_cached(&store);
    let mut server = harness.queue_socket();
    let pending = harness.mount_later("cart", currency("EUR"));
    let sent = server.sent(&mut harness);
    let cart = harness.settle(pending).expect("the seeded mount resolves");

    // 32 fit; the 33rd gets the same answer an unseeded root would give.
    let queued: Vec<_> = (0..32)
        .map(|_| harness.command_later(&cart, Refresh {}))
        .collect();
    let overflow = harness.command_later(&cart, Refresh {});

    assert!(matches!(
        harness.settle(overflow),
        Err(MusubiError::NotConnected)
    ));

    server.reply(&sent[0], ReplyStatus::Ok, json!({"root_id": ROOT_ID}));
    harness.pump();
    server.push_event(&sent[0], "patch", initial_envelope());

    let pushes = server.sent(&mut harness);
    assert_eq!(pushes.len(), 32, "everything queued is dispatched");

    for (push, slot) in pushes.iter().zip(queued) {
        server.reply(push, ReplyStatus::Ok, json!({}));
        assert!(harness.settle(slot).is_ok());
    }
}

#[test]
fn a_cache_read_that_outlives_its_own_mount_does_not_seed_the_next_one() {
    // A root is addressed by `"<module>:<id>"`, but its slot also keys on the
    // mount params: a read issued for one params object must never seed a
    // re-mount of the same id under another.
    let store = Arc::new(GatedCacheStore::default());
    store.put_now(&cart_key(), cached_tree("Cached EUR cart"));

    let mut harness = Harness::new_cached_with(Arc::clone(&store));
    let mut server = harness.queue_socket();
    let first = harness.mount_later("cart", currency("EUR"));
    let sent = server.sent(&mut harness);

    // The join fails while the read is still suspended, which tears the root
    // down under the reader.
    server.reply(
        &sent[0],
        ReplyStatus::Error,
        json!({"reason": "unauthorized"}),
    );
    assert!(matches!(
        harness.settle(first),
        Err(MusubiError::Join { .. })
    ));

    let second = harness.mount_later("cart", currency("USD"));
    // The teardown's `phx_leave` goes out first; the re-mount's join follows.
    let rejoin: Vec<Message> = server
        .sent(&mut harness)
        .into_iter()
        .filter(|message| message.event == "phx_join")
        .collect();

    store.release();
    harness.pump();

    assert!(
        second.lock().unwrap().is_none(),
        "the EUR slot's read must not resolve the USD mount"
    );

    server.reply(&rejoin[0], ReplyStatus::Ok, json!({"root_id": ROOT_ID}));
    harness.pump();

    let cart = harness.finish_mount(&mut server, &rejoin[0], second);

    assert_eq!(
        cart.snapshot()
            .expect("the live initial patch published")
            .title,
        "Cart",
        "the EUR slot's tree must not reach the USD mount"
    );
}

#[test]
fn accepted_envelopes_are_written_through_the_throttle_and_flushed_on_unmount() {
    let store = Arc::new(MemoryCacheStore::new());
    let mut harness = Harness::new_cached(&store);
    let mut server = harness.queue_socket();
    let (_join, cart) = harness.mount(&mut server, "cart");

    assert_eq!(
        block_on(store.get(&cart_key())),
        None,
        "the write is trailing-throttled, not immediate"
    );

    harness.fire(CACHE_WRITE_THROTTLE);

    let entry = block_on(store.get(&cart_key())).expect("the tree was persisted");
    assert_eq!(entry.buster, BUSTER);
    // The *wire* tree is what is cached: markers intact, streams not folded in.
    assert_eq!(entry.data["title"], json!("Cart"));
    assert_eq!(
        entry.data["messages"],
        json!({"__musubi_stream__": "messages"})
    );

    drop(cart);
    harness.pump();

    // Unmount flushes, then arms the gc window; nothing is dropped yet.
    assert!(block_on(store.get(&cart_key())).is_some());

    harness.fire_where(|pending| pending > Duration::from_secs(60));

    assert_eq!(block_on(store.get(&cart_key())), None, "the slot aged out");
}

#[test]
fn a_remount_inside_the_gc_window_cancels_the_eviction() {
    let store = Arc::new(MemoryCacheStore::new());
    let mut harness = Harness::new_cached(&store);
    let mut server = harness.queue_socket();
    let (_join, cart) = harness.mount(&mut server, "cart");

    harness.fire(CACHE_WRITE_THROTTLE);
    drop(cart);
    harness.pump();

    // Same socket: only the channel was left, so the re-mount rejoins on it.
    let (_rejoin, remounted) = harness.mount(&mut server, "cart");

    harness.fire_where(|pending| pending > Duration::from_secs(60));

    assert!(
        block_on(store.get(&cart_key())).is_some(),
        "the re-mount owns the slot again"
    );
    assert!(remounted.snapshot().is_some());
}

// ---------------------------------------------------------------------------
// Generated-code stand-ins
// ---------------------------------------------------------------------------

/// The zero-sized marker `mix compile.musubi_rust` emits per store.
struct CartStore;

impl Store for CartStore {
    const MODULE: &'static str = MODULE;
    type State = CartState;
    type Params = CartParams;
}

/// The `Params` struct the generator emits from `attr :currency, String.t()` —
/// optional, so `Option<String>`.
#[derive(Debug, Default, Serialize)]
struct CartParams {
    currency: Option<String>,
}

fn currency(code: &str) -> CartParams {
    CartParams {
        currency: Some(code.to_owned()),
    }
}

/// A store whose `Params` serializes to a JSON string rather than an object.
/// `Store::Params` is only bound by `Serialize` and the trait is not sealed, so
/// `mount`'s object guard is still reachable from a hand-written impl.
struct OddParamsStore;

#[derive(Serialize)]
#[serde(transparent)]
struct OddParams(&'static str);

impl Store for OddParamsStore {
    const MODULE: &'static str = "MyApp.Stores.OddParamsStore";
    type State = CartState;
    type Params = OddParams;
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

/// An envelope whose only cargo is one root-store `"toast"` event.
fn toast(base_version: u64, version: u64, message: &str) -> Value {
    let mut payload = envelope(base_version, version, json!([]));

    payload["events"] = json!([{"store_id": [], "name": "toast", "payload": {"message": message}}]);

    payload
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

    /// The same rig with a stale-while-revalidate cache wired to `store`.
    fn new_cached(store: &Arc<MemoryCacheStore>) -> Self {
        Self::new_cached_with(Arc::clone(store))
    }

    /// The same rig over any `CacheStore` — the seed tests that need a read
    /// they can suspend build their own.
    fn new_cached_with(store: impl CacheStore + Clone) -> Self {
        Self::new_with(move |seams: Seams| {
            Connection::builder()
                .url("wss://example.test/socket")
                .connector(seams.connector)
                .spawner(seams.spawner)
                .timer(seams.timer)
                .push_timeout(PUSH_TIMEOUT)
                .cache(store.clone())
                .cache_buster(BUSTER)
                .build()
                .expect("every seam is set")
        })
    }

    /// Mounts a root the whole way: join, join ok, initial patch. Returns the
    /// join message (every server push has to echo its `join_ref`) and the
    /// handle.
    fn mount(&mut self, server: &mut ServerEnd, id: &str) -> (Message, Mounted<CartStore>) {
        let pending = self.mount_later(id, currency("EUR"));
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
        params: CartParams,
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
        let mut mounting = Box::pin(connection.mount::<CartStore>(id, CartParams::default()));
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

/// A cache store whose reads only resolve once the test releases them.
///
/// The seed race is about a read that outlives the mount that issued it, which
/// needs a `get` a test can hold open across a teardown and a re-mount.
#[derive(Default)]
struct GatedCacheStore {
    entries: Mutex<HashMap<String, CacheEntry>>,
    gates: Mutex<Vec<oneshot::Sender<()>>>,
}

impl GatedCacheStore {
    /// Writes one entry without going through the gate.
    fn put_now(&self, key: &str, data: Value) {
        self.entries.lock().unwrap().insert(
            key.to_owned(),
            CacheEntry {
                data,
                updated_at: now_ms(),
                buster: BUSTER.to_owned(),
            },
        );
    }

    /// Lets every suspended read finish.
    fn release(&self) {
        for gate in self.gates.lock().unwrap().drain(..) {
            let _ = gate.send(());
        }
    }
}

impl CacheStore for GatedCacheStore {
    fn get(&self, key: &str) -> BoxFuture<'static, Option<CacheEntry>> {
        let entry = self.entries.lock().unwrap().get(key).cloned();
        let (tx, rx) = oneshot::channel();

        self.gates.lock().unwrap().push(tx);

        Box::pin(async move {
            rx.await.ok()?;

            entry
        })
    }

    fn put(&self, key: &str, entry: CacheEntry) -> BoxFuture<'static, ()> {
        self.entries.lock().unwrap().insert(key.to_owned(), entry);

        Box::pin(async {})
    }

    fn evict(&self, key: &str) -> BoxFuture<'static, ()> {
        self.entries.lock().unwrap().remove(key);

        Box::pin(async {})
    }
}

/// The slot every cache test's `"cart"` mount addresses.
fn cart_key() -> String {
    cache_key(MODULE, "cart", &json!({"currency": "EUR"}))
}

/// Writes one entry into a store before the connection is built.
fn seed(store: &Arc<MemoryCacheStore>, data: Value, buster: &str) {
    block_on(store.put(
        &cart_key(),
        CacheEntry {
            data,
            updated_at: now_ms(),
            buster: buster.to_owned(),
        },
    ));
}

/// A cached wire tree: markers intact, exactly as the shadow document holds it.
fn cached_tree(title: &str) -> Value {
    json!({
        "__musubi_store_id__": [],
        "title": title,
        "messages": {"__musubi_stream__": "messages"}
    })
}
