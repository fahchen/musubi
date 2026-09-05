//! Layer-1 wire-fixture replay (`docs/rust-client.md` §12): every JSON file
//! `mix musubi.capture_wire` wrote under `tests/fixtures/` is driven back
//! through a real [`Connection`] over the scripted `MockSocket`.
//!
//! One pass per fixture, frame by frame, in the recorded order:
//!
//!   * a `dir: "out"` frame is **not** injected — it names an action the
//!     client is expected to take (mount, command, `select`, `start`,
//!     `cancel`, unmount), and the frame the client actually writes to the
//!     socket must equal it, topic included;
//!   * a `dir: "in"` frame is fed to the client verbatim: a `phx_reply`
//!     answers the oldest unanswered push, a `"patch"` push rides the join the
//!     fixture established.
//!
//! Afterwards the root's `state().value()` must equal `expected_state`. That
//! document
//! is the server's **pre-hydration** wire tree, so the only rewriting this file
//! does is substituting each `{"__musubi_stream__": name}` marker with the
//! array the scenario's `stream_ops` materialize to — hand-derived from
//! `packages/client/src/streams.ts`, never from this crate's own stream engine
//! (see [`materialized_stream`]).
//!
//! Nothing here sleeps: the executor is a `LocalPool` the test pumps by hand
//! and every timer fires only when a test says so.

use std::collections::VecDeque;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_core::future::BoxFuture;
use futures_util::StreamExt;
use futures_util::stream::BoxStream;
use musubi_client::generated::{Command, Event, Store, StoreId};
use musubi_client::{
    CommandError, Connection, ConnectionBuilder, Mounted, MusubiError, Upload, UploadEntry,
    UploadFile, UploadRequest, Uploader, UploaderError,
};
use phoenix_channel::{Message, ReplyStatus};
use serde::{Deserialize, Serialize};
use serde_json::Value;

// The scripted-transport rig is shared with `phoenix-channel`'s protocol
// suite; files under a `tests/` subdirectory are not test targets themselves.
#[path = "../../phoenix-channel/tests/common/mod.rs"]
mod common;

use common::{Pump, Seams, Slot, drain};

/// Every root's topic is this plus the server-authored `"<module>:<id>"`.
const TOPIC_PREFIX: &str = "musubi:connection:";
/// Long enough that no fixture's push can time out: the `ManualTimer` only
/// fires when a test says so, and no test here says so.
const PUSH_TIMEOUT: Duration = Duration::from_secs(5);
/// What the client reports once an external uploader resolves; the scripted
/// uploader must not report it itself, or it would be pushed twice.
const COMPLETE: u64 = 100;

// ---------------------------------------------------------------------------
// The suite
// ---------------------------------------------------------------------------

#[test]
fn every_captured_fixture_replays_through_the_client() {
    let fixtures = load_fixtures();

    assert!(
        fixtures.len() >= 21,
        "expected the captured scenario set (docs/rust-client.md §12), got {} fixtures",
        fixtures.len()
    );

    for fixture in &fixtures {
        replay(fixture);
    }
}

/// Reads and parses every fixture, sorted by file name.
///
/// The directory is enumerated rather than listed in code, so a scenario added
/// to `test/support/wire_capture/scenarios.ex` is replayed the moment it is
/// captured — a fixture nobody replays would otherwise be a silent gap.
fn load_fixtures() -> Vec<Fixture> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures");

    let mut paths: Vec<PathBuf> = fs::read_dir(&dir)
        .unwrap_or_else(|error| panic!("{}: {error}", dir.display()))
        .map(|entry| entry.expect("a readable directory entry").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
        .collect();

    paths.sort();

    paths
        .iter()
        .map(|path| {
            let json = fs::read_to_string(path).expect("a readable fixture");
            let fixture: Fixture = serde_json::from_str(&json)
                .unwrap_or_else(|error| panic!("{}: {error}", path.display()));

            assert_eq!(
                path.file_stem().and_then(|stem| stem.to_str()),
                Some(fixture.scenario.as_str()),
                "a fixture's file name is its scenario name"
            );

            fixture
        })
        .collect()
}

/// One captured scenario (`docs/rust-client.md` §12).
#[derive(Debug, Deserialize)]
struct Fixture {
    scenario: String,
    frames: Vec<Frame>,
    /// The server's wire root after the last delivered envelope, before
    /// hydration. `null` when the scenario never mounted anything.
    expected_state: Value,
}

/// One recorded frame. `dir` is relative to the client.
#[derive(Debug, Deserialize)]
struct Frame {
    dir: String,
    event: String,
    payload: Value,
}

impl Frame {
    fn is_out(&self) -> bool {
        self.dir == "out"
    }

    /// The `command` frame the capture pushed by hand to record the server's
    /// malformed-frame reply. A typed client cannot express it: the command
    /// name is a `Command::NAME` const, so there is no way to omit it.
    fn is_nameless_command(&self) -> bool {
        self.event == "command" && self.payload.get("name").is_none()
    }
}

// ---------------------------------------------------------------------------
// Store and command markers
// ---------------------------------------------------------------------------

/// Declares the zero-sized marker `mix compile.musubi_rust` would emit per
/// fixture store, and the module-name dispatch that picks one.
///
/// `State` is a `serde_json::Value` on purpose: the fixtures' stores are
/// arbitrary shapes, and the layer-1 contract is the *wire tree*, not one
/// generated struct's field set (which layer 2 covers).
macro_rules! fixture_stores {
    ($($marker:ident => $module:literal),+ $(,)?) => {
        $(
            struct $marker;

            impl Store for $marker {
                const MODULE: &'static str = $module;
                type State = Value;
                type Params = Value;
            }
        )+

        /// Replays one fixture with the marker whose `MODULE` it mounts.
        fn replay(fixture: &Fixture) {
            match root_module(fixture) {
                $($module => run::<$marker>(fixture),)+
                other => panic!("{}: no store marker for {other}", fixture.scenario),
            }
        }
    };
}

fixture_stores! {
    AlphaRootStore => "Musubi.WireCapture.Stores.AlphaRootStore",
    AsyncRootStore => "Musubi.WireCapture.Stores.AsyncRootStore",
    BetaRootStore => "Musubi.WireCapture.Stores.BetaRootStore",
    ChildStore => "Musubi.WireCapture.Stores.ChildStore",
    EventRootStore => "Musubi.WireCapture.Stores.EventRootStore",
    MetaRootStore => "Musubi.WireCapture.Stores.MetaRootStore",
    StreamRootStore => "Musubi.WireCapture.Stores.StreamRootStore",
    ToggleRootStore => "Musubi.WireCapture.Stores.ToggleRootStore",
    UploadRootStore => "Musubi.WireCapture.Stores.UploadRootStore",
}

/// Declares one payload marker per captured command name, plus the dispatch
/// that turns a recorded name back into a typed `command_on` call.
///
/// The name has to reach the client through [`Command::NAME`], so a generated
/// type per name is the only way to send one — which is exactly what the
/// generated bundle does.
macro_rules! fixture_commands {
    ($($marker:ident => $name:literal),+ $(,)?) => {
        $(
            #[derive(Serialize)]
            #[serde(transparent)]
            struct $marker(Value);

            impl<St: Store> Command<St> for $marker {
                const NAME: &'static str = $name;
                type Reply = Value;
            }
        )+

        /// Dispatches the recorded command and hands back its pending reply.
        fn dispatch_command<St: Store>(
            harness: &mut Harness,
            mounted: &Mounted<St>,
            target: &StoreId,
            name: &str,
            payload: Value,
        ) -> Slot<musubi_client::Result<Value>> {
            match name {
                $($name => {
                    let mounted = mounted.clone();
                    let target = target.clone();

                    harness.spawn_capture(async move {
                        mounted.command_on::<$marker, St>(&target, $marker(payload)).await
                    })
                })+
                other => panic!("no command marker for {other}"),
            }
        }
    };
}

fixture_commands! {
    Delete => "delete",
    Drop => "drop",
    Echo => "echo",
    Insert => "insert",
    Load => "load",
    Missing => "missing",
    Notify => "notify",
    Put => "put",
    Rename => "rename",
    Seed => "seed",
    Toggle => "toggle",
}

/// Declares one payload marker per captured push-event name (BDR-0032).
macro_rules! fixture_events {
    ($($marker:ident => $name:literal),+ $(,)?) => {
        $(
            #[derive(Deserialize)]
            #[serde(transparent)]
            struct $marker(Value);
        )+

        $(
            impl<St: Store> Event<St> for $marker {
                const NAME: &'static str = $name;
            }
        )+

        /// Subscribes to one recorded `(store_id, name)` event stream.
        fn subscribe_event<St: Store>(
            mounted: &Mounted<St>,
            store_id: &StoreId,
            name: &str,
        ) -> BoxStream<'static, Value> {
            match name {
                $($name => mounted
                    .events::<$marker, St>(store_id)
                    .map(|payload| payload.0)
                    .boxed(),)+
                other => panic!("no event marker for {other}"),
            }
        }
    };
}

fixture_events! {
    Toast => "toast",
}

// ---------------------------------------------------------------------------
// The replay
// ---------------------------------------------------------------------------

/// The shared rig (`phoenix-channel/tests/common/mod.rs`) wired to one
/// [`Connection`].
type Harness = common::Harness<Connection>;

/// What a frame the client wrote is still waiting on.
enum Pending {
    /// A push whose reply nothing in this file inspects — `phx_leave`, and the
    /// second and later frames of one action.
    Silent,
    /// The join; the mount resolves on the *initial patch*, not on this reply,
    /// so it is polled separately.
    Mount,
    Command(Slot<musubi_client::Result<Value>>),
    Select(Slot<musubi_client::Result<Vec<UploadEntry>>>),
    Control(Slot<musubi_client::Result<()>>),
}

fn run<St: Store<State = Value, Params = Value>>(fixture: &Fixture) {
    let scenario = fixture.scenario.as_str();
    let mut harness = new_harness(fixture);
    let mut server = harness.queue_socket();

    let mut mounted: Option<Mounted<St>> = None;
    let mut pending_mount: Option<Slot<musubi_client::Result<Mounted<St>>>> = None;
    let mut mount_error: Option<MusubiError> = None;
    let mut join: Option<Message> = None;
    let mut topic: Option<String> = None;
    let mut upload: Option<Upload> = None;
    // Whether `start` has already been driven; see the `upload_progress` arm.
    let mut transferring = false;
    let mut events: Vec<(StoreId, String, BoxStream<'static, Value>)> = Vec::new();

    // Frames the client wrote but no fixture frame has claimed yet: one action
    // can write several (an external upload reports progress twice), and each
    // is claimed by its own recorded frame.
    let mut emitted: VecDeque<Message> = VecDeque::new();
    // Claimed frames in send order, which is the order their replies arrive.
    let mut awaiting: VecDeque<(Message, Pending)> = VecDeque::new();
    // Replies to frames no compliant client can send (see `is_nameless_command`).
    let mut unclaimed_replies = 0_usize;

    for frame in &fixture.frames {
        if frame.is_out() {
            if frame.is_nameless_command() {
                unclaimed_replies += 1;
                continue;
            }

            // Only the *first* recorded frame of an action performs it; the
            // rest were written by the same call and are already queued.
            let pending = if emitted.is_empty() {
                let pending = match frame.event.as_str() {
                    "phx_join" => {
                        let connection = harness.inner.clone();
                        let id = string_at(&frame.payload, "id");
                        let params = frame.payload["params"].clone();

                        pending_mount = Some(harness.spawn_capture(async move {
                            connection.mount::<St>(&id, params).await
                        }));

                        Pending::Mount
                    }
                    "phx_leave" => {
                        // Unmounting is `Drop` (§7): the last handle going away
                        // is what leaves the channel.
                        drop(
                            mounted
                                .take()
                                .unwrap_or_else(|| panic!("{scenario}: nothing is mounted")),
                        );

                        Pending::Silent
                    }
                    "command" => {
                        let mounted = mounted
                            .as_ref()
                            .unwrap_or_else(|| panic!("{scenario}: nothing is mounted"));

                        Pending::Command(dispatch_command(
                            &mut harness,
                            mounted,
                            &store_id_at(&frame.payload),
                            &string_at(&frame.payload, "name"),
                            frame.payload["payload"].clone(),
                        ))
                    }
                    "allow_upload" => {
                        let handle = mounted
                            .as_ref()
                            .unwrap_or_else(|| panic!("{scenario}: nothing is mounted"))
                            .upload(
                                &store_id_at(&frame.payload),
                                &string_at(&frame.payload, "name"),
                            );
                        let files = offered_files(&frame.payload);

                        upload = Some(handle.clone());

                        Pending::Select(
                            harness.spawn_capture(async move { handle.select(files).await }),
                        )
                    }
                    // Progress is written by the transfer itself, so the action
                    // behind the first recorded one is `start`. Every later one
                    // belongs to that same call: the relay coalesces reports
                    // and awaits each push's reply, so they are written one
                    // pump apart rather than all at once.
                    "upload_progress" if transferring => Pending::Silent,
                    "upload_progress" => {
                        transferring = true;

                        let handle = upload
                            .clone()
                            .unwrap_or_else(|| panic!("{scenario}: no upload was selected"));

                        Pending::Control(harness.spawn_capture(async move { handle.start().await }))
                    }
                    "cancel_upload" => {
                        let handle = upload
                            .clone()
                            .unwrap_or_else(|| panic!("{scenario}: no upload was selected"));
                        let entry_ref = handle
                            .value()
                            .entries
                            .first()
                            .map(|entry| entry.r#ref.clone())
                            .unwrap_or_else(|| panic!("{scenario}: the upload has no entry"));

                        assert_eq!(
                            Value::String(entry_ref.clone()),
                            frame.payload["ref"],
                            "{scenario}: the client cancels the server-issued entry ref"
                        );

                        Pending::Control(
                            harness.spawn_capture(
                                async move { handle.cancel(Some(&entry_ref)).await },
                            ),
                        )
                    }
                    other => panic!("{scenario}: cannot drive a {other} frame"),
                };

                emitted.extend(server.sent(&mut harness));
                pending
            } else {
                Pending::Silent
            };

            let actual = emitted.pop_front().unwrap_or_else(|| {
                panic!("{scenario}: the client wrote no frame for {}", frame.event)
            });

            if actual.event == "phx_join" {
                topic = Some(actual.topic.clone());
                join = Some(actual.clone());
            }

            assert_frame(scenario, frame, &actual, topic.as_deref());
            awaiting.push_back((actual, pending));

            continue;
        }

        match frame.event.as_str() {
            "phx_reply" => {
                if unclaimed_replies > 0 {
                    unclaimed_replies -= 1;
                    continue;
                }

                let (to, pending) = awaiting
                    .pop_front()
                    .unwrap_or_else(|| panic!("{scenario}: a reply with nothing to answer"));
                let status = reply_status(scenario, &frame.payload);
                let response = frame.payload["response"].clone();

                server.reply(&to, status, response.clone());
                harness.pump();
                settle(&mut harness, scenario, pending, status, &response);
            }
            "patch" => {
                let join = join
                    .as_ref()
                    .unwrap_or_else(|| panic!("{scenario}: a patch before any join"));

                server.push_event(join, "patch", frame.payload.clone());
                harness.pump();
            }
            other => panic!("{scenario}: cannot deliver a {other} frame"),
        }

        // The mount resolves on its initial patch, which is a push and not a
        // reply, so it is polled after every delivered frame instead.
        if let Some(slot) = pending_mount.as_ref() {
            if let Some(result) = harness.peek(slot) {
                pending_mount = None;

                match result {
                    Ok(handle) => {
                        events = recorded_events(fixture)
                            .into_iter()
                            .map(|(store_id, name)| {
                                let stream = subscribe_event(&handle, &store_id, &name);

                                (store_id, name, stream)
                            })
                            .collect();
                        mounted = Some(handle);
                    }
                    Err(error) => mount_error = Some(error),
                }
            }
        }
    }

    emitted.extend(server.sent(&mut harness));

    assert_eq!(
        emitted
            .iter()
            .map(|frame| frame.event.as_str())
            .collect::<Vec<_>>(),
        expected_recovery(scenario),
        "{scenario}: unexpected frames after the last recorded one"
    );

    assert_events(scenario, fixture, &mut events);
    assert_state(scenario, fixture, mounted.as_ref(), mount_error.as_ref());
}

/// Builds the connection under test, with every uploader the fixture's
/// preflight replies name registered.
fn new_harness(fixture: &Fixture) -> Harness {
    let script = external_progress(fixture);
    let uploaders = named_uploaders(fixture);

    Harness::new_with(move |seams: Seams| {
        let builder =
            uploaders
                .iter()
                .fold(Connection::builder(), |builder: ConnectionBuilder, name| {
                    builder.uploader(name, ScriptedUploader::new(script.clone()))
                });

        builder
            .url("wss://fixtures.test/socket")
            .connector(seams.connector)
            .spawner(seams.spawner)
            .timer(seams.timer)
            .push_timeout(PUSH_TIMEOUT)
            .build()
            .expect("every seam is set")
    })
}

/// Asserts one frame the client wrote against the recorded one.
fn assert_frame(scenario: &str, expected: &Frame, actual: &Message, topic: Option<&str>) {
    assert_eq!(
        actual.event, expected.event,
        "{scenario}: expected a {} frame, got {actual:?}",
        expected.event
    );
    assert_eq!(
        actual.payload, expected.payload,
        "{scenario}: the {} payload does not match the capture",
        expected.event
    );
    assert_eq!(
        Some(actual.topic.as_str()),
        topic,
        "{scenario}: every frame of a root rides that root's topic"
    );

    if expected.event == "phx_join" {
        assert_eq!(
            actual.topic,
            format!(
                "{TOPIC_PREFIX}{}:{}",
                string_at(&expected.payload, "module"),
                string_at(&expected.payload, "id")
            ),
            "{scenario}: the topic is the prefix plus the server-authored root id"
        );
    }
}

/// Checks whatever the answered push was waiting on.
fn settle(
    harness: &mut Harness,
    scenario: &str,
    pending: Pending,
    status: ReplyStatus,
    response: &Value,
) {
    match (pending, status) {
        (Pending::Command(slot), ReplyStatus::Ok) => {
            let reply = harness
                .settle(slot)
                .unwrap_or_else(|error| panic!("{scenario}: the command failed: {error}"));

            assert_eq!(
                &reply, response,
                "{scenario}: the command reply reaches the caller verbatim"
            );
        }
        (Pending::Command(slot), ReplyStatus::Error) => {
            assert!(
                matches!(
                    harness.settle(slot),
                    Err(MusubiError::Command(CommandError::Failed { ref reply, .. }))
                        if reply == response
                ),
                "{scenario}: an error reply fails the command with the response verbatim"
            );
        }
        (Pending::Select(slot), _) => {
            let accepted = harness
                .settle(slot)
                .unwrap_or_else(|error| panic!("{scenario}: the preflight failed: {error}"));

            assert_eq!(
                accepted
                    .iter()
                    .map(|entry| entry.r#ref.as_str())
                    .collect::<Vec<_>>(),
                accepted_refs(response),
                "{scenario}: the accepted entries are seeded in client_ref order"
            );
        }
        (Pending::Control(slot), _) => {
            harness.settle(slot).unwrap_or_else(|error| {
                panic!("{scenario}: the upload control call failed: {error}")
            });
        }
        (Pending::Mount | Pending::Silent, _) => {}
    }
}

/// Asserts every recorded push event reached its typed subscription, in order.
fn assert_events(
    scenario: &str,
    fixture: &Fixture,
    events: &mut [(StoreId, String, BoxStream<'static, Value>)],
) {
    for (store_id, name, stream) in events {
        let expected: Vec<Value> = fixture
            .frames
            .iter()
            .filter(|frame| !frame.is_out())
            .filter_map(|frame| frame.payload.get("events")?.as_array())
            .flatten()
            .filter(|event| &store_id_at(event) == store_id && string_at(event, "name") == *name)
            .map(|event| event["payload"].clone())
            .collect();

        assert_eq!(
            drain(stream),
            expected,
            "{scenario}: the {name} events of {store_id:?} reach their subscription"
        );
    }
}

/// Asserts the mounted root's `state().value()` against `expected_state`.
fn assert_state<St: Store<State = Value>>(
    scenario: &str,
    fixture: &Fixture,
    mounted: Option<&Mounted<St>>,
    mount_error: Option<&MusubiError>,
) {
    if fixture.expected_state.is_null() {
        assert!(
            mounted.is_none() && mount_error.is_some(),
            "{scenario}: a null expected_state is a scenario that never mounted"
        );

        return;
    }

    let mounted =
        mounted.unwrap_or_else(|| panic!("{scenario}: the scenario's root is not mounted"));
    let state = mounted.state();

    assert!(
        state.revision() > 0,
        "{scenario}: the root published no state"
    );
    // A fixture store declares `State = serde_json::Value`, so `value()` is a
    // total function here: no generated struct, no drift layering, no panic
    // path — the tree's hydrated projection compared against the server's own
    // wire root.
    assert_eq!(
        state.value(),
        hydrated(fixture),
        "{scenario}: the client's tree does not match the server's wire root"
    );
}

// ---------------------------------------------------------------------------
// Reading the fixture
// ---------------------------------------------------------------------------

fn root_module(fixture: &Fixture) -> &str {
    fixture
        .frames
        .iter()
        .find(|frame| frame.is_out() && frame.event == "phx_join")
        .map(|frame| string_at_ref(&frame.payload, "module"))
        .unwrap_or_else(|| panic!("{}: no join frame", fixture.scenario))
}

fn string_at(payload: &Value, key: &str) -> String {
    string_at_ref(payload, key).to_owned()
}

fn string_at_ref<'a>(payload: &'a Value, key: &str) -> &'a str {
    payload[key]
        .as_str()
        .unwrap_or_else(|| panic!("{key} is a string on {payload}"))
}

fn store_id_at(payload: &Value) -> StoreId {
    serde_json::from_value(payload["store_id"].clone()).expect("store_id is a path")
}

fn reply_status(scenario: &str, payload: &Value) -> ReplyStatus {
    match string_at_ref(payload, "status") {
        "ok" => ReplyStatus::Ok,
        "error" => ReplyStatus::Error,
        other => panic!("{scenario}: unknown reply status {other}"),
    }
}

/// The files an `allow_upload` frame offered, rebuilt at their recorded size.
///
/// The bytes themselves are never captured — only `size` is on the wire — so
/// they are zeroes: what the client sends is the length.
fn offered_files(payload: &Value) -> Vec<UploadFile> {
    payload["entries"]
        .as_array()
        .expect("entries is a list")
        .iter()
        .map(|entry| {
            let size = usize::try_from(entry["size"].as_u64().expect("size is a number"))
                .expect("a fixture file fits in memory");

            UploadFile::new(
                string_at(entry, "name"),
                string_at(entry, "type"),
                vec![0_u8; size],
            )
        })
        .collect()
}

/// The entry refs a preflight reply accepted, in `client_ref` order.
fn accepted_refs(response: &Value) -> Vec<&str> {
    let mut accepted: Vec<(usize, &str)> = response["entries"]
        .as_object()
        .expect("entries is an object")
        .iter()
        .map(|(client_ref, entry)| {
            (
                client_ref.parse().expect("client_ref is an index"),
                string_at_ref(entry, "entry_ref"),
            )
        })
        .collect();

    accepted.sort_unstable();

    accepted.into_iter().map(|(_index, entry)| entry).collect()
}

/// Every uploader name the fixture's preflight replies dispatch to.
fn named_uploaders(fixture: &Fixture) -> Vec<String> {
    let mut names: Vec<String> = fixture
        .frames
        .iter()
        .filter(|frame| !frame.is_out() && frame.event == "phx_reply")
        .filter_map(|frame| frame.payload["response"].get("entries")?.as_object())
        .flat_map(|entries| entries.values())
        .filter_map(|entry| Some(entry.get("uploader")?.as_str()?.to_owned()))
        .collect();

    names.sort_unstable();
    names.dedup();

    names
}

/// What the scripted uploader must report, read off the recorded
/// `upload_progress` frames.
///
/// The terminal `100` is excluded: the client writes that one itself once the
/// uploader resolves (`docs/rust-client.md` §10.2), so an uploader reporting it
/// would push it twice.
fn external_progress(fixture: &Fixture) -> Vec<u64> {
    fixture
        .frames
        .iter()
        .filter(|frame| frame.is_out() && frame.event == "upload_progress")
        .filter_map(|frame| frame.payload["progress"].as_u64())
        .filter(|progress| *progress < COMPLETE)
        .collect()
}

/// Every `(store_id, name)` a recorded envelope pushes an event for.
fn recorded_events(fixture: &Fixture) -> Vec<(StoreId, String)> {
    let mut pairs: Vec<(StoreId, String)> = fixture
        .frames
        .iter()
        .filter(|frame| !frame.is_out())
        .filter_map(|frame| frame.payload.get("events")?.as_array())
        .flatten()
        .map(|event| (store_id_at(event), string_at(event, "name")))
        .collect();

    pairs.dedup();

    pairs
}

// ---------------------------------------------------------------------------
// The expected document
// ---------------------------------------------------------------------------

/// `expected_state` with every stream marker substituted, i.e. the document
/// the client's own hydration pass must produce.
fn hydrated(fixture: &Fixture) -> Value {
    substitute_streams(&fixture.expected_state, &fixture.scenario)
}

fn substitute_streams(value: &Value, scenario: &str) -> Value {
    match value {
        Value::Array(items) => Value::Array(
            items
                .iter()
                .map(|item| substitute_streams(item, scenario))
                .collect(),
        ),
        Value::Object(fields) => {
            // The single-key rule (§4.6): an object carrying
            // `__musubi_stream__` *plus* other keys is a rendered map, not a
            // slot.
            if fields.len() == 1 {
                if let Some(name) = fields.get("__musubi_stream__").and_then(Value::as_str) {
                    return materialized_stream(scenario, name);
                }
            }

            Value::Object(
                fields
                    .iter()
                    .map(|(key, field)| (key.clone(), substitute_streams(field, scenario)))
                    .collect(),
            )
        }
        other => other.clone(),
    }
}

/// What each stream scenario's `stream_ops` materialize to.
///
/// Hand-derived from `packages/client/src/streams.ts` — the behavioural
/// reference — rather than from this crate's `streams.rs`, so the two are
/// still being compared and not merely restated. Items are substituted
/// verbatim, so an entry is its `item` alone.
fn materialized_stream(scenario: &str, name: &str) -> Value {
    let ids: &[&str] = match (scenario, name) {
        // reset, then three appends.
        ("stream_reset", "items") => &["1", "2", "3"],
        // Appending `9` (`at: -1`), then prepending `0` (`at: 0`).
        ("stream_insert", "items") => &["0", "1", "2", "9"],
        // `item-2` deleted out of the middle.
        ("stream_delete", "items") => &["1", "3"],
        // `7` at index 1, then re-inserting the existing `1` at index 2:
        // the upsert removes it before the index is resolved.
        ("stream_at_variants", "items") => &["7", "2", "1", "3"],
        // Four seeded, `5` appended under `limit: 3` (trims the front) leaves
        // `3,4,5`; `6` prepended under `limit: -2` (`at == 0`, so it trims the
        // tail) leaves `6,3`; `limit: 0` then empties the stream.
        ("stream_limit_variants", "items") => &[],
        _ => panic!("{scenario}: no materialized {name} stream is declared"),
    };

    Value::Array(ids.iter().map(|id| serde_json::json!({"id": id})).collect())
}

/// The frames a scenario writes *after* its last recorded one.
///
/// Both entries are client-side teardown the capture cannot contain, because
/// the server never authored them:
///
///   * a rejected join leaves nobody holding the root, and a root with no
///     holder is torn down — leave included, so the socket layer cannot rejoin
///     an orphan on the next reconnect (§9);
///   * a version gap keeps the last-good document and re-creates the channel,
///     and the capture stops at the gap.
///
/// Everything else must write exactly what was recorded and nothing more.
fn expected_recovery(scenario: &str) -> Vec<&'static str> {
    match scenario {
        "mount_rejected_unknown_root" => vec!["phx_leave"],
        "version_gap" => vec!["phx_leave", "phx_join"],
        _ => Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// The scripted uploader
// ---------------------------------------------------------------------------

/// An external uploader (BDR-0027) that reports the fixture's recorded
/// progress values and then resolves.
struct ScriptedUploader {
    script: Arc<Mutex<VecDeque<u64>>>,
}

impl ScriptedUploader {
    fn new(script: Vec<u64>) -> Self {
        Self {
            script: Arc::new(Mutex::new(script.into())),
        }
    }
}

impl Uploader for ScriptedUploader {
    fn upload(&self, request: UploadRequest) -> BoxFuture<'static, Result<(), UploaderError>> {
        let reports: Vec<u64> = self.script.lock().unwrap().drain(..).collect();

        Box::pin(async move {
            for percent in reports {
                request
                    .progress
                    .report(u32::try_from(percent).expect("progress is a percentage"));
            }

            Ok(())
        })
    }
}
