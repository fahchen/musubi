//! Protocol tests for the upload control plane over a scripted `MockSocket`
//! (`docs/rust-client.md` §10.2, §12 layer 3): preflight, channel-mode chunk
//! transfer over binary frames, cancellation, external uploaders, and what a
//! dropped socket does to an upload in flight.
//!
//! Nothing here sleeps: the executor is a `LocalPool` the test pumps by hand
//! and every timer fires only when a test says so.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_core::future::BoxFuture;
use musubi_client::generated::{Store, StoreId};
use musubi_client::{
    Connection, EntryStatus, Mounted, MusubiError, PatchEngine, TransferError, Upload, UploadEntry,
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

    let handle = avatar.snapshot();
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
        avatar.snapshot().entries.as_slice(),
        [UploadEntry { r#ref, progress: 40, status: EntryStatus::Uploading, .. }]
            if r#ref == "u_1"
    ));
}

#[test]
fn a_handle_with_no_connection_behind_it_cannot_transfer() {
    let engine = PatchEngine::new();
    let avatar = engine.uploads().handle(&StoreId::root(), "avatar");

    assert!(matches!(
        futures_executor::block_on(avatar.select(vec![png("me.png", b"a")])),
        Err(MusubiError::NotConnected)
    ));
    assert!(matches!(
        futures_executor::block_on(avatar.start()),
        Err(MusubiError::NotConnected)
    ));
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

    assert_eq!(avatar.snapshot().status, UploadStatus::Uploading);
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

    let handle = avatar.snapshot();
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
    assert_eq!(avatar.snapshot().status, UploadStatus::Success);
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
    assert_eq!(avatar.snapshot().status, UploadStatus::Error);
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
        avatar.snapshot().entry("u_1"),
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

    let handle = avatar.snapshot();
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
    assert_eq!(avatar.snapshot().status, UploadStatus::Error);

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

    assert!(avatar.snapshot().entries.is_empty());
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

    let handle = avatar.snapshot();
    assert_eq!(handle.status, UploadStatus::Idle);
    assert!(handle.entries.is_empty() && handle.errors.is_empty());
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
    assert_eq!(avatar.snapshot().status, UploadStatus::Error);
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
            [
                Message { event: first, payload: half, .. },
                Message { event: second, payload: done, .. },
            ] if first == "upload_progress"
                && second == "upload_progress"
                && half["progress"] == json!(50)
                && done["progress"] == json!(100)
                && done["ref"] == json!("u_1")
        ),
        "the uploader's own report is relayed, then 100 once it resolves"
    );
    assert_eq!(avatar.snapshot().status, UploadStatus::Success);
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
            [
                Message { event: relayed, .. },
                Message { event: failed, payload, .. },
            ] if relayed == "upload_progress"
                && failed == "upload_error"
                && payload["code"] == json!("external_failed")
                && payload["message"] == json!("403 Forbidden")
                && payload["ref"] == json!("u_1")
        ),
        "progress reported before the failure is still relayed, then the failure itself"
    );
    assert_eq!(avatar.snapshot().status, UploadStatus::Error);
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
    assert_eq!(avatar.snapshot().status, UploadStatus::Error);
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
