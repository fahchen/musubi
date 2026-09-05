//! Protocol tests for the upload control plane over a scripted `MockSocket`
//! (`docs/rust-client.md` §10.2, §12 layer 3): preflight, channel-mode chunk
//! transfer over binary frames, cancellation, external uploaders, and what a
//! dropped socket does to an upload in flight.
//!
//! Nothing here sleeps: the executor is a `LocalPool` the test pumps by hand
//! and every timer fires only when a test says so.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_channel::oneshot;
use futures_core::future::BoxFuture;
use futures_util::future::{Either, select};
use musubi_client::generated::{Store, StoreId};
use musubi_client::{
    Connection, EntryStatus, Mounted, MusubiError, TransferError, Upload, UploadEntry,
    UploadErrorCode, UploadFile, UploadRequest, UploadStatus, Uploader, UploaderError,
};
use phoenix_channel::{BinaryPush, Message, ReplyStatus};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

// The scripted-transport rig is shared with `phoenix-channel`'s protocol
// suite; files under a `tests/` subdirectory are not test targets themselves.
#[path = "../../phoenix-channel/tests/common/mod.rs"]
mod common;

use common::{Pump, Seams, ServerEnd, Slot};

const MODULE: &str = "MyApp.Stores.CartStore";
const ROOT_ID: &str = "MyApp.Stores.CartStore:cart";
const TOPIC: &str = "musubi:connection:MyApp.Stores.CartStore:cart";
const UPLOAD_TOPIC: &str = "musubi_upload:u_1";
const PUSH_TIMEOUT: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// Preflight
// ---------------------------------------------------------------------------

#[test]
fn select_preflights_every_file_and_seeds_only_the_accepted_ones() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (_join, cart) = harness.mount(&mut server, "cart");
    let avatar = cart.upload(&StoreId::root(), "avatar");

    let selecting = harness.select(
        &avatar,
        vec![png("me.png", b"abcde"), png("big.png", b"xx")],
    );

    let sent = server.sent(&mut harness);
    assert!(matches!(
        sent.as_slice(),
        [Message { topic, event, payload, .. }]
            if topic == TOPIC
                && event == "allow_upload"
                && payload["store_id"] == json!([])
                && payload["name"] == json!("avatar")
                && payload["entries"] == json!([
                    {"client_ref": "0", "name": "me.png", "size": 5, "type": "image/png"},
                    {"client_ref": "1", "name": "big.png", "size": 2, "type": "image/png"},
                ])
    ));

    server.reply(
        &sent[0],
        ReplyStatus::Ok,
        preflight(
            json!({"0": {"type": "channel", "entry_ref": "u_1", "token": "tok"}}),
            json!([{
                "client_ref": "1",
                "error": {"code": "too_large", "message": "file exceeds the maximum size"}
            }]),
        ),
    );

    let entries = harness.settle(selecting).expect("preflight replied");
    assert!(matches!(
        entries.as_slice(),
        [UploadEntry { r#ref, client_name, client_size: 5, status: EntryStatus::Pending, .. }]
            if r#ref == "u_1" && client_name == "me.png"
    ));

    let handle = avatar.value();
    assert_eq!(
        handle.status,
        UploadStatus::Error,
        "a rejected file leaves the handle in error, with no entry of its own"
    );
    assert!(matches!(
        handle.errors.as_slice(),
        [error] if error.code == UploadErrorCode::TooLarge
    ));
    assert_eq!(handle.config.chunk_size, 2, "the reply's config is adopted");
}

#[test]
fn select_keeps_the_entry_an_add_op_already_created() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (join, cart) = harness.mount(&mut server, "cart");
    let avatar = cart.upload(&StoreId::root(), "avatar");

    let selecting = harness.select(&avatar, vec![png("me.png", b"abcde")]);
    let sent = server.sent(&mut harness);

    // BDR-0009 puts the reply first, but the ops land in the same tick here;
    // whichever arrives first, there must be exactly one entry.
    server.push_event(
        &join,
        "patch",
        upload_envelope(1, 2, json!([add_op("u_1", 40)])),
    );
    harness.pump();
    server.reply(
        &sent[0],
        ReplyStatus::Ok,
        preflight(
            json!({"0": {"type": "channel", "entry_ref": "u_1", "token": "tok"}}),
            json!([]),
        ),
    );

    harness.settle(selecting).expect("preflight replied");

    assert!(matches!(
        avatar.value().entries.as_slice(),
        [UploadEntry { r#ref, progress: 40, status: EntryStatus::Uploading, .. }]
            if r#ref == "u_1"
    ));
}

#[test]
fn a_rejected_preflight_leaves_the_handle_in_error_rather_than_selecting() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (_join, cart) = harness.mount(&mut server, "cart");
    let avatar = cart.upload(&StoreId::root(), "avatar");

    let selecting = harness.select(&avatar, vec![png("me.png", b"abcde")]);
    let sent = server.sent(&mut harness);

    server.reply(
        &sent[0],
        ReplyStatus::Error,
        json!({"reason": "uploads are closed"}),
    );

    assert!(matches!(
        harness.settle(selecting),
        Err(MusubiError::Transfer(TransferError::Rejected { event, reason }))
            if event == "allow_upload" && reason == "uploads are closed"
    ));
    // The caller gets the error, but it is rarely the only one watching: a
    // spinner bound to `status` would otherwise never resolve.
    assert_eq!(avatar.value().status, UploadStatus::Error);
}

#[test]
fn a_preflight_that_times_out_leaves_the_handle_in_error_rather_than_selecting() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (_join, cart) = harness.mount(&mut server, "cart");
    let avatar = cart.upload(&StoreId::root(), "avatar");

    let selecting = harness.select(&avatar, vec![png("me.png", b"abcde")]);
    assert_eq!(server.sent(&mut harness).len(), 1);

    harness.fire(PUSH_TIMEOUT);

    assert!(matches!(
        harness.settle(selecting),
        Err(MusubiError::Timeout)
    ));
    assert_eq!(avatar.value().status, UploadStatus::Error);
}

#[test]
fn abandoning_a_preflight_leaves_the_handle_in_error_rather_than_selecting() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (_join, cart) = harness.mount(&mut server, "cart");
    let avatar = cart.upload(&StoreId::root(), "avatar");

    let (abandon, selecting) = harness.select_abandonable(&avatar, vec![png("me.png", b"abcde")]);
    assert_eq!(server.sent(&mut harness).len(), 1);
    assert_eq!(avatar.value().status, UploadStatus::Selecting);

    // Nothing on the error path runs when the future itself goes away, so the
    // transition out of `selecting` cannot live there.
    abandon.now(&mut harness);

    assert!(
        harness.settle(selecting).is_none(),
        "the preflight was dropped"
    );
    assert_eq!(avatar.value().status, UploadStatus::Error);
}

#[test]
fn a_reply_naming_a_client_ref_this_selection_never_offered_is_a_protocol_error() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (_join, cart) = harness.mount(&mut server, "cart");
    let avatar = cart.upload(&StoreId::root(), "avatar");

    let selecting = harness.select(&avatar, vec![png("me.png", b"abcde")]);
    let sent = server.sent(&mut harness);

    // One file was offered, under `client_ref: "0"`. An entry for anything else
    // describes a file this client does not have: skipping it silently would
    // finish with no entries, no errors, and a handle still reading `selecting`.
    server.reply(
        &sent[0],
        ReplyStatus::Ok,
        preflight(
            json!({"7": {"type": "channel", "entry_ref": "u_1", "token": "tok"}}),
            json!([]),
        ),
    );

    assert!(matches!(
        harness.settle(selecting),
        Err(MusubiError::Protocol(_))
    ));
    assert_eq!(avatar.value().status, UploadStatus::Error);
}

// ---------------------------------------------------------------------------
// Channel mode (BDR-0026)
// ---------------------------------------------------------------------------

#[test]
fn channel_mode_pushes_the_file_as_binary_chunks_and_leaves_when_it_is_done() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (join, cart) = harness.mount(&mut server, "cart");
    let avatar = cart.upload(&StoreId::root(), "avatar");

    harness.preflight_one(&mut server, &avatar, b"abcde");

    let starting = harness.start(&avatar);
    let sent = server.sent(&mut harness);

    assert_eq!(avatar.value().status, UploadStatus::Uploading);
    assert!(
        matches!(
            sent.as_slice(),
            [Message { topic, event, payload, .. }]
                if topic == UPLOAD_TOPIC
                    && event == "phx_join"
                    && payload == &json!({"token": "tok"})
        ),
        "the sub-channel is joined with the stateless preflight token"
    );
    server.reply(&sent[0], ReplyStatus::Ok, json!({}));

    // `chunk_size` is 2, so a five-byte file is three slices, the last short.
    // Each is pushed only after the previous one is acknowledged.
    for (slice, progress) in [(&b"ab"[..], 40), (&b"cd"[..], 80), (&b"e"[..], 100)] {
        let pushes = server.sent_binary(&mut harness);

        assert!(
            matches!(
                pushes.as_slice(),
                [BinaryPush { topic, event, payload, .. }]
                    if topic == UPLOAD_TOPIC && event == "chunk" && payload == slice
            ),
            "expected one {slice:?} chunk, got {pushes:?}"
        );

        server.reply_binary(&pushes[0], ReplyStatus::Ok, json!({"progress": progress}));
    }

    assert!(harness.settle(starting).is_ok());
    assert!(
        matches!(
            server.sent(&mut harness).as_slice(),
            [Message { topic, event, .. }] if topic == UPLOAD_TOPIC && event == "phx_leave"
        ),
        "the finished sub-channel is left, so the socket never rejoins it"
    );

    // The authoritative completion signal is the op, not the chunk reply.
    server.push_event(
        &join,
        "patch",
        upload_envelope(
            1,
            2,
            json!([
                add_op("u_1", 0),
                {"op": "complete", "upload": "avatar", "store_id": [], "ref": "u_1"}
            ]),
        ),
    );
    harness.pump();

    let handle = avatar.value();
    assert_eq!(handle.status, UploadStatus::Success);
    assert_eq!(handle.progress(), 100);
    assert!(matches!(
        handle.entry("u_1"),
        Some(UploadEntry {
            status: EntryStatus::Success,
            progress: 100,
            ..
        })
    ));
}

#[test]
fn an_empty_file_still_travels_as_one_empty_chunk() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (_join, cart) = harness.mount(&mut server, "cart");
    let avatar = cart.upload(&StoreId::root(), "avatar");

    harness.preflight_one(&mut server, &avatar, b"");
    let starting = harness.start(&avatar);
    harness.join_upload_channel(&mut server);

    // The server completes on the first chunk whose running total reaches
    // `client_size`; sending nothing at all would hang until the watchdog
    // fires (a deliberate divergence from the TypeScript client, §10.2).
    let pushes = server.sent_binary(&mut harness);
    assert!(matches!(
        pushes.as_slice(),
        [BinaryPush { event, payload, .. }] if event == "chunk" && payload.is_empty()
    ));

    server.reply_binary(&pushes[0], ReplyStatus::Ok, json!({"progress": 100}));

    assert!(harness.settle(starting).is_ok());
    assert_eq!(avatar.value().status, UploadStatus::Success);
}

#[test]
fn a_rejected_chunk_fails_the_entry_with_the_servers_reason() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (join, cart) = harness.mount(&mut server, "cart");
    let avatar = cart.upload(&StoreId::root(), "avatar");

    harness.preflight_one(&mut server, &avatar, b"abcde");
    let starting = harness.start(&avatar);
    harness.join_upload_channel(&mut server);

    let pushes = server.sent_binary(&mut harness);
    server.reply_binary(
        &pushes[0],
        ReplyStatus::Error,
        json!({"reason": "upload too large"}),
    );

    assert!(matches!(
        harness.settle(starting),
        Err(MusubiError::Transfer(TransferError::Chunk { entry_ref, reason }))
            if entry_ref == "u_1" && reason == "upload too large"
    ));
    assert_eq!(avatar.value().status, UploadStatus::Error);
    assert!(
        matches!(
            server.sent(&mut harness).as_slice(),
            [Message { event, .. }] if event == "phx_leave"
        ),
        "the stopped sub-channel is left rather than rejoined"
    );

    // The machine-readable code arrives on the main channel, as an op.
    server.push_event(
        &join,
        "patch",
        upload_envelope(
            1,
            2,
            json!([
                add_op("u_1", 0),
                {
                    "op": "error", "upload": "avatar", "store_id": [], "ref": "u_1",
                    "error": {"code": "too_large", "message": "upload too large"}
                }
            ]),
        ),
    );
    harness.pump();

    assert!(matches!(
        avatar.value().entry("u_1"),
        Some(UploadEntry { status: EntryStatus::Error, errors, .. })
            if errors[0].code == UploadErrorCode::TooLarge
    ));
}

#[test]
fn a_chunk_that_is_never_answered_times_the_transfer_out() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (join, cart) = harness.mount(&mut server, "cart");
    let avatar = cart.upload(&StoreId::root(), "avatar");

    harness.preflight_one(&mut server, &avatar, b"abcde");
    let starting = harness.start(&avatar);
    harness.join_upload_channel(&mut server);

    // The chunk-timeout watchdog stops the channel without replying, so the
    // client only learns of it from its own push timeout.
    assert_eq!(server.sent_binary(&mut harness).len(), 1);
    harness.fire(PUSH_TIMEOUT);

    assert!(matches!(
        harness.settle(starting),
        Err(MusubiError::Timeout)
    ));

    server.push_event(
        &join,
        "patch",
        upload_envelope(
            1,
            2,
            json!([
                add_op("u_1", 0),
                {
                    "op": "error", "upload": "avatar", "store_id": [], "ref": "u_1",
                    "error": {"code": "chunk_timeout", "message": "upload timed out between chunks"}
                }
            ]),
        ),
    );
    harness.pump();

    let handle = avatar.value();
    assert_eq!(handle.status, UploadStatus::Error);
    assert!(matches!(
        handle.entry("u_1"),
        Some(UploadEntry { errors, .. }) if errors[0].code == UploadErrorCode::ChunkTimeout
    ));
}

#[test]
fn cancelling_mid_transfer_leaves_the_sub_channel_and_tells_the_page_server() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (join, cart) = harness.mount(&mut server, "cart");
    let avatar = cart.upload(&StoreId::root(), "avatar");

    harness.preflight_one(&mut server, &avatar, b"abcde");
    let starting = harness.start(&avatar);
    harness.join_upload_channel(&mut server);
    assert_eq!(
        server.sent_binary(&mut harness).len(),
        1,
        "one chunk in flight"
    );

    let cancelling = harness.cancel(&avatar, Some("u_1"));
    let sent = server.sent(&mut harness);
    assert!(
        matches!(
            sent.as_slice(),
            [
                Message { topic: left, event: leave, .. },
                Message { topic: main, event: cancel, payload, .. },
            ] if left == UPLOAD_TOPIC
                && leave == "phx_leave"
                && main == TOPIC
                && cancel == "cancel_upload"
                && payload == &json!({"store_id": [], "name": "avatar", "ref": "u_1"})
        ),
        "the sub-channel is left first, so the server drops the partial file"
    );

    server.reply(&sent[1], ReplyStatus::Ok, json!({}));
    assert!(harness.settle(cancelling).is_ok());

    // The in-flight chunk can no longer be answered; the transfer ends on its
    // own push timeout and the handle reports the failure.
    harness.fire(PUSH_TIMEOUT);
    assert!(harness.settle(starting).is_err());
    assert_eq!(avatar.value().status, UploadStatus::Error);

    // Cancellation is a deletion, never a status (BDR-0025).
    server.push_event(
        &join,
        "patch",
        upload_envelope(
            1,
            2,
            json!([
                add_op("u_1", 0),
                {"op": "cancel", "upload": "avatar", "store_id": [], "ref": "u_1"}
            ]),
        ),
    );
    harness.pump();

    assert!(avatar.value().entries.is_empty());
}

#[test]
fn a_server_cancel_op_leaves_the_sub_channel_of_the_transfer_it_kills() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (join, cart) = harness.mount(&mut server, "cart");
    let avatar = cart.upload(&StoreId::root(), "avatar");

    harness.preflight_one(&mut server, &avatar, b"abcde");
    let starting = harness.start(&avatar);
    harness.join_upload_channel(&mut server);
    assert_eq!(
        server.sent_binary(&mut harness).len(),
        1,
        "one chunk in flight"
    );

    // Somebody else cancelled the entry — another tab, or a server-side rule.
    // The op *deletes* it (BDR-0025), and deleting the transport state is not
    // enough on its own: the transfer cloned everything it needs before its
    // first await, so only the signal and the leave stop it.
    server.push_event(
        &join,
        "patch",
        upload_envelope(
            1,
            2,
            json!([
                add_op("u_1", 0),
                {"op": "cancel", "upload": "avatar", "store_id": [], "ref": "u_1"}
            ]),
        ),
    );
    harness.pump();

    assert!(
        matches!(
            server.sent(&mut harness).as_slice(),
            [Message { topic, event, .. }] if topic == UPLOAD_TOPIC && event == "phx_leave"
        ),
        "the sub-channel is left, which is what makes the server drop the partial file"
    );

    harness.fire(PUSH_TIMEOUT);
    assert!(harness.settle(starting).is_err());
}

#[test]
fn a_server_cancel_op_raises_the_signal_an_external_uploader_is_waiting_on() {
    let mut harness = Harness::with_uploader(CancellableUploader);
    let mut server = harness.queue_socket();
    let (join, cart) = harness.mount(&mut server, "cart");
    let avatar = cart.upload(&StoreId::root(), "avatar");

    harness.preflight(
        &mut server,
        &avatar,
        b"abcde",
        json!({"0": {"type": "external", "entry_ref": "u_1", "uploader": "S3", "meta": {}}}),
    );

    let starting = harness.start(&avatar);
    assert!(
        server.sent(&mut harness).is_empty(),
        "the uploader is parked on its cancellation signal"
    );

    server.push_event(
        &join,
        "patch",
        upload_envelope(
            1,
            2,
            json!([
                add_op("u_1", 0),
                {"op": "cancel", "upload": "avatar", "store_id": [], "ref": "u_1"}
            ]),
        ),
    );
    harness.pump();

    // Without the signal the app's own PUT runs to completion and the file
    // lands in the destination bucket after the user abandoned it.
    assert!(harness.settle(starting).is_ok());
    assert!(
        server.sent(&mut harness).is_empty(),
        "nothing is reported for an entry the server already deleted — a `100` \
         would move it back to success"
    );
}

#[test]
fn unmounting_the_root_mid_transfer_leaves_the_sub_channel() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (_join, cart) = harness.mount(&mut server, "cart");
    let avatar = cart.upload(&StoreId::root(), "avatar");

    harness.preflight_one(&mut server, &avatar, b"abcde");
    let starting = harness.start(&avatar);
    harness.join_upload_channel(&mut server);
    assert_eq!(
        server.sent_binary(&mut harness).len(),
        1,
        "one chunk in flight"
    );

    // Navigating away. The registry is cleared, and a transfer it left running
    // would keep pushing into a sub-channel nobody is going to leave.
    drop(cart);
    harness.pump();

    let sent = server.sent(&mut harness);
    assert!(
        sent.iter()
            .any(|frame| frame.topic == UPLOAD_TOPIC && frame.event == "phx_leave"),
        "expected the sub-channel to be left too, got {sent:?}"
    );

    harness.fire(PUSH_TIMEOUT);
    assert!(harness.settle(starting).is_err());
}

#[test]
fn reset_cancels_every_entry_and_returns_the_handle_to_idle() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (_join, cart) = harness.mount(&mut server, "cart");
    let avatar = cart.upload(&StoreId::root(), "avatar");

    harness.preflight_one(&mut server, &avatar, b"abcde");

    let resetting = harness.reset(&avatar);
    let sent = server.sent(&mut harness);
    assert!(matches!(
        sent.as_slice(),
        [Message { event, payload, .. }]
            if event == "cancel_upload" && payload["ref"] == json!("u_1")
    ));

    server.reply(&sent[0], ReplyStatus::Ok, json!({}));
    assert!(harness.settle(resetting).is_ok());

    let handle = avatar.value();
    assert_eq!(handle.status, UploadStatus::Idle);
    assert!(handle.entries.is_empty() && handle.errors.is_empty());
}

#[test]
fn starting_again_after_a_finished_transfer_does_not_send_the_entry_twice() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (_join, cart) = harness.mount(&mut server, "cart");
    let avatar = cart.upload(&StoreId::root(), "avatar");

    harness.preflight_one(&mut server, &avatar, b"abcde");

    let starting = harness.start(&avatar);
    harness.join_upload_channel(&mut server);

    for progress in [40, 80, 100] {
        let pushes = server.sent_binary(&mut harness);

        server.reply_binary(&pushes[0], ReplyStatus::Ok, json!({"progress": progress}));
    }

    assert!(harness.settle(starting).is_ok());
    assert_eq!(
        server.sent(&mut harness).len(),
        1,
        "the finished sub-channel is left"
    );

    // The preflight token verifies statelessly for 600s and the server opens a
    // fresh temp file per join, so a replay would orphan the one it already
    // wrote — and overwrite the entry's path with an empty file.
    let again = harness.start(&avatar);

    assert!(harness.settle(again).is_ok());
    assert!(
        server.sent(&mut harness).is_empty(),
        "a finished entry is consumed: there is nothing left to transfer"
    );
    assert!(server.sent_binary(&mut harness).is_empty());
}

#[test]
fn a_second_start_while_one_is_running_is_refused_rather_than_racing_it() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (_join, cart) = harness.mount(&mut server, "cart");
    let avatar = cart.upload(&StoreId::root(), "avatar");

    harness.preflight_one(&mut server, &avatar, b"abcde");

    let starting = harness.start(&avatar);
    harness.join_upload_channel(&mut server);
    assert_eq!(
        server.sent_binary(&mut harness).len(),
        1,
        "one chunk in flight"
    );

    // A second transfer would join `musubi_upload:u_1` again; attaching
    // replaces the socket's registry entry and bumps the generation, so the
    // first transfer's pushes go stale and its cleanup clears the channel the
    // second one is using.
    let second = harness.start(&avatar);

    assert!(
        server.sent(&mut harness).is_empty(),
        "the second start opens no channel of its own"
    );
    assert!(matches!(
        harness.settle(second),
        Err(MusubiError::Transfer(TransferError::AlreadyStarted { name })) if name == "avatar"
    ));

    // And the first one is still the transfer that owns the entry.
    let pushes = server.sent_binary(&mut harness);
    assert!(pushes.is_empty(), "no second stream of chunks: {pushes:?}");

    harness.fire(PUSH_TIMEOUT);
    assert!(harness.settle(starting).is_err());
}

#[test]
fn abandoning_a_transfer_leaves_its_sub_channel_and_retires_its_entry() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (_join, cart) = harness.mount(&mut server, "cart");
    let avatar = cart.upload(&StoreId::root(), "avatar");

    harness.preflight_one(&mut server, &avatar, b"abcde");

    let (abandon, starting) = harness.start_abandonable(&avatar);
    harness.join_upload_channel(&mut server);
    assert_eq!(
        server.sent_binary(&mut harness).len(),
        1,
        "one chunk in flight"
    );

    // The `select!` that lost its race, the task dropped on navigation: no code
    // on the post-await path runs, so the cleanup has to be `Drop`'s.
    abandon.now(&mut harness);

    assert!(
        harness.settle(starting).is_none(),
        "the transfer was dropped"
    );
    assert!(
        matches!(
            server.sent(&mut harness).as_slice(),
            [Message { topic, event, .. }] if topic == UPLOAD_TOPIC && event == "phx_leave"
        ),
        "the sub-channel it joined is left, or the socket's recovery rejoins it"
    );

    // The claim went with it, and so did the entry.
    let again = harness.start(&avatar);

    assert!(harness.settle(again).is_ok());
    assert!(
        server.sent(&mut harness).is_empty(),
        "the abandoned entry is retired, not left for the next start to re-send"
    );
}

#[test]
fn a_dropped_socket_fails_the_transfer_rather_than_resuming_it() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (_join, cart) = harness.mount(&mut server, "cart");
    let avatar = cart.upload(&StoreId::root(), "avatar");

    harness.preflight_one(&mut server, &avatar, b"abcde");
    let starting = harness.start(&avatar);
    harness.join_upload_channel(&mut server);
    assert_eq!(server.sent_binary(&mut harness).len(), 1);

    // Uploads are not resumable (BDR-0003): the chunk in flight is failed and
    // nothing re-sends it, even though the socket itself reconnects.
    server.disconnect();
    harness.pump();

    assert!(matches!(
        harness.settle(starting),
        Err(MusubiError::Disconnected)
    ));
    assert_eq!(avatar.value().status, UploadStatus::Error);
}

// ---------------------------------------------------------------------------
// External mode (BDR-0027)
// ---------------------------------------------------------------------------

#[test]
fn external_mode_hands_the_bytes_to_the_registered_uploader_and_relays_progress() {
    let calls = Calls::default();
    let mut harness = Harness::with_uploader(ScriptedUploader {
        calls: Arc::clone(&calls),
        outcome: Ok(()),
    });
    let mut server = harness.queue_socket();
    let (_join, cart) = harness.mount(&mut server, "cart");
    let avatar = cart.upload(&StoreId::root(), "avatar");

    harness.preflight(
        &mut server,
        &avatar,
        b"abcde",
        json!({"0": {
            "type": "external", "entry_ref": "u_1",
            "uploader": "S3", "meta": {"url": "https://example.test/put"}
        }}),
    );

    let starting = harness.start(&avatar);

    // One relay push at a time: the uploader's own report goes out first and
    // the transfer does not resolve until the server has acknowledged it.
    let relayed = server.sent(&mut harness);
    assert!(
        matches!(
            relayed.as_slice(),
            [Message { event, payload, .. }]
                if event == "upload_progress"
                    && payload["progress"] == json!(50)
                    && payload["ref"] == json!("u_1")
        ),
        "expected one in-flight progress relay, got {relayed:?}"
    );

    server.reply(&relayed[0], ReplyStatus::Ok, json!({}));
    assert!(harness.settle(starting).is_ok());

    let calls = calls.lock().unwrap();
    assert!(
        matches!(
            calls.as_slice(),
            [(entry_ref, bytes, meta)]
                if entry_ref == "u_1"
                    && bytes == b"abcde"
                    && meta["url"] == json!("https://example.test/put")
        ),
        "the uploader gets the entry's bytes and the opaque meta verbatim"
    );

    assert!(
        matches!(
            server.sent(&mut harness).as_slice(),
            [Message { event, payload, .. }]
                if event == "upload_progress"
                    && payload["progress"] == json!(100)
                    && payload["ref"] == json!("u_1")
        ),
        "then 100 once the uploader resolves — the only completion signal there is"
    );
    assert_eq!(avatar.value().status, UploadStatus::Success);
}

#[test]
fn progress_reported_faster_than_the_socket_drains_coalesces_to_the_latest_value() {
    let mut harness = Harness::with_uploader(FloodingUploader);
    let mut server = harness.queue_socket();
    let (_join, cart) = harness.mount(&mut server, "cart");
    let avatar = cart.upload(&StoreId::root(), "avatar");

    harness.preflight(
        &mut server,
        &avatar,
        b"abcde",
        json!({"0": {"type": "external", "entry_ref": "u_1", "uploader": "S3", "meta": {}}}),
    );

    let starting = harness.start(&avatar);

    // A hundred reports, none of them acknowledged: exactly one push is on the
    // wire. Unbounded relaying would have queued a hundred `RootPush`es, a
    // hundred spawned tasks and a hundred inflight-map entries, and starved the
    // socket read loop doing it.
    let first = server.sent(&mut harness);
    assert!(
        matches!(
            first.as_slice(),
            [Message { event, payload, .. }]
                if event == "upload_progress" && payload["progress"] == json!(1)
        ),
        "expected a single in-flight relay, got {first:?}"
    );

    server.reply(&first[0], ReplyStatus::Ok, json!({}));

    // The one after it carries the newest value, not the next one in line.
    let second = server.sent(&mut harness);
    assert!(
        matches!(
            second.as_slice(),
            [Message { event, payload, .. }]
                if event == "upload_progress" && payload["progress"] == json!(99)
        ),
        "expected the latest value, got {second:?}"
    );

    server.reply(&second[0], ReplyStatus::Ok, json!({}));
    assert!(harness.settle(starting).is_ok());

    assert!(
        matches!(
            server.sent(&mut harness).as_slice(),
            [Message { event, payload, .. }]
                if event == "upload_progress" && payload["progress"] == json!(100)
        ),
        "and the completion report is last"
    );
}

#[test]
fn an_uploader_that_rejects_is_reported_as_external_failed() {
    let mut harness = Harness::with_uploader(ScriptedUploader {
        calls: Calls::default(),
        outcome: Err("403 Forbidden"),
    });
    let mut server = harness.queue_socket();
    let (_join, cart) = harness.mount(&mut server, "cart");
    let avatar = cart.upload(&StoreId::root(), "avatar");

    harness.preflight(
        &mut server,
        &avatar,
        b"abcde",
        json!({"0": {"type": "external", "entry_ref": "u_1", "uploader": "S3", "meta": {}}}),
    );

    let starting = harness.start(&avatar);

    assert!(matches!(
        harness.settle(starting),
        Err(MusubiError::Transfer(TransferError::Uploader { entry_ref, message }))
            if entry_ref == "u_1" && message == "403 Forbidden"
    ));
    assert!(
        matches!(
            server.sent(&mut harness).as_slice(),
            [Message { event, payload, .. }]
                if event == "upload_error"
                    && payload["code"] == json!("external_failed")
                    && payload["message"] == json!("403 Forbidden")
                    && payload["ref"] == json!("u_1")
        ),
        "the failure is the last word: a progress the relay had not sent yet dies with it, \
         because the server moves an entry it already failed back to uploading for one"
    );
    assert_eq!(avatar.value().status, UploadStatus::Error);
}

#[test]
fn an_uploader_the_connection_never_registered_fails_the_entry_locally() {
    let mut harness = Harness::new();
    let mut server = harness.queue_socket();
    let (_join, cart) = harness.mount(&mut server, "cart");
    let avatar = cart.upload(&StoreId::root(), "avatar");

    harness.preflight(
        &mut server,
        &avatar,
        b"abcde",
        json!({"0": {"type": "external", "entry_ref": "u_1", "uploader": "S3", "meta": {}}}),
    );

    let starting = harness.start(&avatar);

    assert!(matches!(
        harness.settle(starting),
        Err(MusubiError::Transfer(TransferError::NoUploader { uploader, entry_ref }))
            if uploader == "S3" && entry_ref == "u_1"
    ));
    assert!(
        server.sent(&mut harness).is_empty(),
        "a missing registration is a client-side misconfiguration, not a transfer failure"
    );
    assert_eq!(avatar.value().status, UploadStatus::Error);
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// Everything a [`ScriptedUploader`] was asked to transfer.
type Calls = Arc<Mutex<Vec<(String, Vec<u8>, Value)>>>;

/// An uploader that records its request, reports half-way, and then does what
/// the test told it to.
struct ScriptedUploader {
    calls: Calls,
    outcome: Result<(), &'static str>,
}

impl Uploader for ScriptedUploader {
    fn upload(&self, request: UploadRequest) -> BoxFuture<'static, Result<(), UploaderError>> {
        self.calls.lock().unwrap().push((
            request.entry.r#ref.clone(),
            request.bytes.to_vec(),
            request.meta.clone(),
        ));

        let outcome = self.outcome;

        Box::pin(async move {
            request.progress.report(50);

            outcome.map_err(UploaderError::new)
        })
    }
}

/// Waits for its cancellation signal and then gives up, which is what an
/// uploader that `select!`s on `cancelled()` around its own request does.
struct CancellableUploader;

impl Uploader for CancellableUploader {
    fn upload(&self, request: UploadRequest) -> BoxFuture<'static, Result<(), UploaderError>> {
        Box::pin(async move {
            request.cancel.cancelled().await;

            Ok(())
        })
    }
}

/// Reports a hundred times in one burst, with a single await point after the
/// first — which is what a real uploader chunking a large body looks like.
struct FloodingUploader;

impl Uploader for FloodingUploader {
    fn upload(&self, request: UploadRequest) -> BoxFuture<'static, Result<(), UploaderError>> {
        Box::pin(async move {
            request.progress.report(1);

            // The one turn the relay beside this uploader gets.
            YieldOnce(false).await;

            for percent in 2..=99 {
                request.progress.report(percent);
            }

            Ok(())
        })
    }
}

/// Yields to the executor exactly once.
struct YieldOnce(bool);

impl Future for YieldOnce {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 {
            return Poll::Ready(());
        }

        self.0 = true;
        cx.waker().wake_by_ref();

        Poll::Pending
    }
}

fn png(name: &str, bytes: &[u8]) -> UploadFile {
    UploadFile::new(name, "image/png", bytes.to_vec())
}

/// The `allow_upload` reply, with `chunk_size: 2` so a handful of bytes is
/// several chunks.
fn preflight(entries: Value, errors: Value) -> Value {
    json!({
        "ref": "avatar",
        "config": {
            "accept": [".png"],
            "max_entries": 3,
            "max_file_size": 5_000,
            "chunk_size": 2
        },
        "entries": entries,
        "errors": errors
    })
}

fn add_op(entry_ref: &str, progress: u32) -> Value {
    json!({
        "op": "add", "upload": "avatar", "store_id": [], "ref": entry_ref,
        "entry": {
            "ref": entry_ref, "client_name": "me.png", "client_size": 5,
            "client_type": "image/png", "progress": progress,
            "status": if progress == 0 { "pending" } else { "uploading" },
            "errors": []
        }
    })
}

/// An envelope whose only content is `upload_ops` — an upload-only cycle still
/// bumps the version (BDR-0025).
fn upload_envelope(base_version: u64, version: u64, upload_ops: Value) -> Value {
    json!({
        "type": "patch",
        "root_id": ROOT_ID,
        "base_version": base_version,
        "version": version,
        "ops": [],
        "stream_ops": [],
        "upload_ops": upload_ops
    })
}

fn initial_envelope() -> Value {
    json!({
        "type": "patch",
        "root_id": ROOT_ID,
        "base_version": 0,
        "version": 1,
        "ops": [{
            "op": "replace",
            "path": "",
            "value": {
                "__musubi_store_id__": [],
                "title": "Cart",
                "avatar": {"__musubi_upload__": "avatar"}
            }
        }],
        "stream_ops": []
    })
}

/// The zero-sized marker `mix compile.musubi_rust` emits per store.
struct CartStore;

impl Store for CartStore {
    const MODULE: &'static str = MODULE;
    type State = CartState;
    type Params = CartParams;
}

#[derive(Debug, Default, Serialize)]
struct CartParams {}

#[derive(Debug, Deserialize)]
struct CartState {
    #[allow(dead_code)]
    title: String,
    /// An upload slot stays the inert marker on the state; the live handle is
    /// reached through `Mounted::upload` (§10.1).
    #[allow(dead_code)]
    avatar: musubi_client::generated::UploadSlot,
}

/// The shared rig (`phoenix-channel/tests/common/mod.rs`) wired to one
/// [`Connection`].
type Harness = common::Harness<Connection>;

impl Harness {
    fn new() -> Self {
        Self::new_with(|seams: Seams| build(Connection::builder(), seams))
    }

    fn with_uploader(uploader: impl Uploader) -> Self {
        Self::new_with(|seams: Seams| build(Connection::builder().uploader("S3", uploader), seams))
    }

    /// Mounts a root the whole way: join, join ok, initial patch.
    fn mount(&mut self, server: &mut ServerEnd, id: &str) -> (Message, Mounted<CartStore>) {
        let connection = self.inner.clone();
        let id = id.to_owned();
        let pending = self
            .spawn_capture(async move { connection.mount::<CartStore>(&id, CartParams {}).await });

        let sent = server.sent(self);
        server.reply(&sent[0], ReplyStatus::Ok, json!({"root_id": ROOT_ID}));
        self.pump();
        server.push_event(&sent[0], "patch", initial_envelope());

        let mounted = match self.settle(pending) {
            Ok(mounted) => mounted,
            Err(error) => panic!("mount failed: {error}"),
        };

        (sent.into_iter().next().expect("one join"), mounted)
    }

    /// Selects one channel-mode file and settles the preflight.
    fn preflight_one(&mut self, server: &mut ServerEnd, avatar: &Upload, bytes: &[u8]) {
        self.preflight(
            server,
            avatar,
            bytes,
            json!({"0": {"type": "channel", "entry_ref": "u_1", "token": "tok"}}),
        );
    }

    /// Selects one file and answers the preflight with `entries`.
    fn preflight(&mut self, server: &mut ServerEnd, avatar: &Upload, bytes: &[u8], entries: Value) {
        let selecting = self.select(avatar, vec![png("me.png", bytes)]);
        let sent = server.sent(self);

        server.reply(&sent[0], ReplyStatus::Ok, preflight(entries, json!([])));
        self.settle(selecting).expect("preflight replied");
    }

    /// Answers the chunk sub-channel's `phx_join`.
    fn join_upload_channel(&mut self, server: &mut ServerEnd) {
        let sent = server.sent(self);

        assert!(
            matches!(sent.as_slice(), [Message { topic, .. }] if topic == UPLOAD_TOPIC),
            "expected the sub-channel join, got {sent:?}"
        );
        server.reply(&sent[0], ReplyStatus::Ok, json!({}));
    }

    fn select(&mut self, avatar: &Upload, files: Vec<UploadFile>) -> Slot<Selected> {
        let avatar = avatar.clone();

        self.spawn_capture(async move { avatar.select(files).await })
    }

    fn start(&mut self, avatar: &Upload) -> Slot<musubi_client::Result<()>> {
        let avatar = avatar.clone();

        self.spawn_capture(async move { avatar.start().await })
    }

    /// `select`, beside a switch that drops it mid-flight.
    fn select_abandonable(
        &mut self,
        avatar: &Upload,
        files: Vec<UploadFile>,
    ) -> (Abandon, Slot<Option<Selected>>) {
        let avatar = avatar.clone();

        self.abandonable(async move { avatar.select(files).await })
    }

    /// `start`, beside a switch that drops it mid-transfer.
    fn start_abandonable(
        &mut self,
        avatar: &Upload,
    ) -> (Abandon, Slot<Option<musubi_client::Result<()>>>) {
        let avatar = avatar.clone();

        self.abandonable(async move { avatar.start().await })
    }

    /// Spawns `call` racing a switch the test holds, so a test can observe what
    /// a *dropped* future leaves behind — the one thing no assertion on a
    /// resolved one can reach.
    fn abandonable<T: Send + 'static>(
        &mut self,
        call: impl Future<Output = T> + Send + 'static,
    ) -> (Abandon, Slot<Option<T>>) {
        let (switch, flipped) = oneshot::channel::<()>();
        let slot = self.spawn_capture(async move {
            match select(Box::pin(call), flipped).await {
                Either::Left((outcome, _)) => Some(outcome),
                // Dropping the losing half is the cancellation.
                Either::Right((_, abandoned)) => {
                    drop(abandoned);

                    None
                }
            }
        });

        (Abandon(switch), slot)
    }

    fn cancel(
        &mut self,
        avatar: &Upload,
        entry_ref: Option<&str>,
    ) -> Slot<musubi_client::Result<()>> {
        let avatar = avatar.clone();
        let entry_ref = entry_ref.map(str::to_owned);

        self.spawn_capture(async move { avatar.cancel(entry_ref.as_deref()).await })
    }

    fn reset(&mut self, avatar: &Upload) -> Slot<musubi_client::Result<()>> {
        let avatar = avatar.clone();

        self.spawn_capture(async move { avatar.reset().await })
    }
}

type Selected = musubi_client::Result<Vec<UploadEntry>>;

/// The switch [`Harness::abandonable`] hands back: flipping it drops the call.
struct Abandon(oneshot::Sender<()>);

impl Abandon {
    fn now(self, harness: &mut Harness) {
        let _ = self.0.send(());

        harness.pump();
    }
}

fn build(builder: musubi_client::ConnectionBuilder, seams: Seams) -> Connection {
    builder
        .url("wss://example.test/socket")
        .connector(seams.connector)
        .spawner(seams.spawner)
        .timer(seams.timer)
        .push_timeout(PUSH_TIMEOUT)
        .build()
        .expect("every seam is set")
}
