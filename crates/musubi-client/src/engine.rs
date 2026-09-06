//! The per-root patch engine: version discipline, one transaction per envelope,
//! and the two planes that are not the tree
//! (`docs/rust-reactive-state.md` §3.6, `docs/rust-client.md` §4.5).
//!
//! There is no shadow document any more. The engine owns a
//! [`StateTree`] — the retained tree of one mounted root — and one envelope is
//! one transaction against it: `ops` land first, because the initial
//! `replace ""` is what creates the slot a `stream_op` in the same envelope
//! fills, and `upload_ops` fold into the registry afterwards, because an upload
//! slot on the tree is an inert leaf (§3.4).
//!
//! Applying is **one** call: the transaction journal makes
//! [`apply`](PatchEngine::apply) atomic on its own, and the drift check runs
//! *inside* the transaction, against
//! [`Transaction::to_hydrated`](musubi_state::Transaction::to_hydrated) (§4.4),
//! so nothing has to be deserialized between two halves of a commit.

use std::collections::HashSet;
use std::sync::Arc;

use musubi_state::{Notify, PatchOp, StateTree};
use serde_json::Value;

use crate::envelope::PatchEnvelope;
use crate::error::{MusubiError, PatchError, Result};
use crate::generated::StoreId;
use crate::uploads::Uploads;

/// The message an out-of-sequence initial envelope produces.
const INITIAL_VERSION_MESSAGE: &str = "Initial patch envelope must start at version 1";

/// The drift check §4.4 runs inside the transaction, before it commits.
///
/// A function rather than the [`RootSink`](crate::mounted::RootSink) itself so
/// the engine stays free of the `Store` type erasure: all it needs is "hand this
/// hydrated root to the generated types and tell me whether they took it".
pub(crate) type Validate<'a> = &'a dyn Fn(&Value) -> std::result::Result<(), serde_json::Error>;

/// One root's retained tree, upload registry and version.
#[derive(Debug)]
pub(crate) struct PatchEngine {
    version: u64,
    tree: StateTree,
    uploads: Arc<Uploads>,
}

impl PatchEngine {
    /// The engine a mounted root gets: the tree its [`Mounted`](crate::Mounted)
    /// hands out [`State`](musubi_state::State) views of, and the upload
    /// registry its handles come from — so folded ops and observed values are
    /// the same objects.
    pub(crate) fn new(tree: StateTree, uploads: Arc<Uploads>) -> Self {
        Self {
            version: 0,
            tree,
            uploads,
        }
    }

    /// The last accepted envelope's `version`; `0` means "awaiting the initial
    /// envelope" (fresh, or mid-reconnect).
    pub(crate) fn version(&self) -> u64 {
        self.version
    }

    /// The wire projection of the whole tree — stream slots back to their
    /// markers — which is the shape the mount cache stores (§7).
    pub(crate) fn document(&self) -> Value {
        self.tree
            .to_wire(self.tree.root_id())
            .unwrap_or(Value::Null)
    }

    /// Forgets the version while keeping the tree and uploads (§9).
    ///
    /// Every recovery path — a rejoin, a version gap, a transport drop — leaves
    /// the last-good rendering in place and waits for a fresh initial envelope
    /// to reconcile it. Only the version is reset, so the next envelope must
    /// again be `base_version: 0, version: 1`.
    pub(crate) fn soft_reset(&mut self) {
        self.version = 0;
    }

    /// Builds the tree from a cached wire tree, without touching the version
    /// (`docs/rust-client.md` §6.4).
    ///
    /// The engine stays at `0`, so the live initial envelope is still required
    /// to be `base_version: 0, version: 1` and still reconciles the whole tree
    /// in one `replace ""`. Streams are **not** seeded — `stream_ops` are not
    /// part of a cached tree — so a seeded stream slot reads as `[]` until the
    /// live envelope refills it, exactly as in the TypeScript client.
    ///
    /// A cached tree written by an older build can be a shape this binary can
    /// no longer deserialize. That is what `validate` catches, and the seed is
    /// then **rolled back** by the transaction rather than undone by a second
    /// call: the root is left exactly where a cold mount would have left it.
    pub(crate) fn seed(&mut self, document: Value, validate: Validate<'_>) -> Result<Notify> {
        let mut transaction = self.tree.begin();

        transaction
            .apply(
                &[PatchOp::Replace {
                    path: String::new(),
                    value: document,
                }],
                &[],
            )
            .map_err(|error| PatchError::from_tree(0, error))?;

        Self::validate(&transaction, validate)?;

        let notify = transaction.commit();

        self.prune();

        Ok(notify)
    }

    /// Applies one envelope as one transaction, in the order §3.6 fixes.
    ///
    /// Steps 1–8: check the version, open the transaction, land `ops` then
    /// `stream_ops`, validate the root when this envelope carries one (§4.4),
    /// commit, fold `upload_ops`, prune, advance the version. **Step 9 is the
    /// caller's**: dropping the returned [`Notify`] is what runs the state
    /// subscribers, and holding it is how the actor sequences them against
    /// `Live`, the events and the cache write.
    ///
    /// A failure at any of steps 1, 3 or 4 drops the transaction, which rolls
    /// the tree back to exactly what it was: the version does not advance, no
    /// upload subscriber has heard of this envelope, no state subscriber is
    /// notified, and the last-good tree keeps rendering while
    /// `docs/rust-client.md` §9 recovery restarts the root.
    pub(crate) fn apply(
        &mut self,
        envelope: &PatchEnvelope,
        validate: Validate<'_>,
    ) -> Result<Notify> {
        self.check_version(envelope)?;

        let mut transaction = self.tree.begin();

        // One op at a time, so a failure can name *which* op failed — the index
        // `PatchError::Apply` has always carried. Every call joins the same
        // transaction, so this is one transaction, not one per op (§2.3).
        for (index, op) in envelope.ops.iter().enumerate() {
            transaction
                .apply(std::slice::from_ref(op), &[])
                .map_err(|error| PatchError::from_tree(index, error))?;
        }

        transaction
            .apply(&[], &envelope.stream_ops)
            .map_err(|error| PatchError::from_tree(envelope.ops.len(), error))?;

        if Self::carries_drift_check(envelope) {
            Self::validate(&transaction, validate)?;
        }

        let notify = transaction.commit();

        // The first thing outside the tree to hear that this envelope was
        // accepted (§3.6 steps 6–8).
        self.uploads.apply_ops(&envelope.upload_ops);
        self.prune();
        self.version = envelope.version;

        Ok(notify)
    }

    /// Whether this envelope runs the whole-root drift check (§4.4).
    ///
    /// Layer 1 — every envelope carrying a root `replace ""`, i.e. once per
    /// mount and once per rejoin — is always on, release builds included, and is
    /// the only whole-root deserialization left in the client. Layer 2 widens it
    /// to *every* accepted envelope under `debug_assertions`, which is v1's
    /// per-envelope cost kept exactly where it is free.
    fn carries_drift_check(envelope: &PatchEnvelope) -> bool {
        cfg!(debug_assertions)
            || envelope
                .ops
                .iter()
                .any(|op| matches!(op, PatchOp::Replace { path, .. } if path.is_empty()))
    }

    /// Runs the drift check against the transaction's own view of the root.
    ///
    /// The root's own subtree is what failed, so the reported store is the root
    /// path even when a nested store node is the culprit.
    fn validate(transaction: &musubi_state::Transaction<'_>, validate: Validate<'_>) -> Result<()> {
        let root = transaction.root_id();
        let hydrated = transaction.to_hydrated(root).unwrap_or(Value::Null);

        validate(&hydrated).map_err(|source| MusubiError::Decode {
            store_id: StoreId::root(),
            source,
        })
    }

    /// Drops the upload handles of every store the tree no longer holds
    /// (§3.6 step 7, BDR-0011).
    ///
    /// Streams need no equivalent: a `Collection` node is freed with the store
    /// subtree that owns it, so pruning them is structural (§3.5).
    fn prune(&self) {
        let live_store_ids: HashSet<StoreId> = self.tree.store_ids().into_iter().collect();

        self.uploads.prune(&live_store_ids);
    }

    /// Enforces version continuity (§4.5).
    ///
    /// `version` is a message sequence, not a state version: event-only and
    /// stream-only cycles bump it (BDR-0018), and idle cycles emit nothing, so
    /// the sequence is gapless for the life of one page runtime.
    fn check_version(&self, envelope: &PatchEnvelope) -> Result<()> {
        if self.version == 0 {
            if envelope.base_version != 0 || envelope.version != 1 {
                return Err(MusubiError::Protocol(INITIAL_VERSION_MESSAGE));
            }

            return Ok(());
        }

        if envelope.base_version != self.version || envelope.version != self.version + 1 {
            return Err(MusubiError::VersionMismatch);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use musubi_state::State;
    use serde_json::json;

    use super::*;

    const ROOT_ID: &str = "MyApp.Stores.CartStore:cart";

    /// The validator a bare engine runs: the generated types are the mount's
    /// business, and these tests have none.
    const ACCEPT: Validate<'static> = &|_| Ok(());

    // ---- version discipline ---------------------------------------------

    #[test]
    fn the_initial_envelope_must_be_base_zero_version_one() {
        for (base_version, version) in [(0, 2), (1, 2), (0, 0)] {
            let mut engine = bare();
            let error = engine
                .apply(&envelope(base_version, version, vec![]), ACCEPT)
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
            .apply(
                &envelope(
                    2,
                    3,
                    vec![json!({"op": "replace", "path": "/title", "value": "Gapped"})],
                ),
                ACCEPT,
            )
            .unwrap_err();

        assert!(matches!(error, MusubiError::VersionMismatch));
        assert_eq!(engine.version(), 1);
        assert_eq!(title(&engine), "Cart");
    }

    #[test]
    fn a_replayed_envelope_is_a_version_mismatch() {
        let mut engine = mounted();

        drop(
            engine
                .apply(
                    &envelope(
                        1,
                        2,
                        vec![json!({"op": "replace", "path": "/title", "value": "Second"})],
                    ),
                    ACCEPT,
                )
                .unwrap(),
        );
        let error = engine
            .apply(
                &envelope(
                    1,
                    2,
                    vec![json!({"op": "replace", "path": "/title", "value": "Replay"})],
                ),
                ACCEPT,
            )
            .unwrap_err();

        assert!(matches!(error, MusubiError::VersionMismatch));
        assert_eq!(title(&engine), "Second");
    }

    #[test]
    fn a_stream_only_cycle_still_bumps_the_sequence() {
        let mut engine = mounted();

        drop(
            engine
                .apply(
                    &stream_envelope(1, 2, vec![insert_op(&[], "m-1", -1)]),
                    ACCEPT,
                )
                .unwrap(),
        );

        assert_eq!(engine.version(), 2);
    }

    // ---- atomicity -------------------------------------------------------

    #[test]
    fn a_failing_op_maps_to_apply_and_leaves_the_whole_envelope_unapplied() {
        let mut engine = mounted();

        let error = engine
            .apply(
                &stream_envelope_with_ops(
                    1,
                    2,
                    vec![
                        json!({"op": "replace", "path": "/title", "value": "Applied first"}),
                        json!({"op": "remove", "path": "/absent"}),
                    ],
                    vec![insert_op(&[], "m-1", -1)],
                ),
                ACCEPT,
            )
            .unwrap_err();

        assert!(matches!(
            error,
            MusubiError::Patch(PatchError::Apply { index: 1, ref path, .. }) if path == "/absent"
        ));
        assert_eq!(title(&engine), "Cart");
        assert_eq!(engine.version(), 1);
        assert_eq!(
            root(&engine).field::<Value>("messages").unwrap().value(),
            json!([]),
            "the stream op travelling with it was rolled back too"
        );
    }

    #[test]
    fn a_malformed_pointer_is_an_application_failure_and_nothing_before_it_survives() {
        let mut engine = mounted();

        let error = engine
            .apply(
                &envelope(
                    1,
                    2,
                    vec![
                        json!({"op": "replace", "path": "/title", "value": "Applied first"}),
                        json!({"op": "replace", "path": "nope", "value": 1}),
                    ],
                ),
                ACCEPT,
            )
            .unwrap_err();

        assert!(matches!(
            error,
            MusubiError::Patch(PatchError::Apply { index: 1, ref path, .. }) if path == "nope"
        ));
        assert_eq!(title(&engine), "Cart");
    }

    #[test]
    fn ops_apply_left_to_right() {
        let mut engine = mounted();

        drop(
            engine
                .apply(
                    &envelope(
                        1,
                        2,
                        vec![
                            json!({"op": "add", "path": "/tags", "value": ["a"]}),
                            json!({"op": "add", "path": "/tags/-", "value": "b"}),
                            json!({"op": "replace", "path": "/tags/0", "value": "z"}),
                        ],
                    ),
                    ACCEPT,
                )
                .unwrap(),
        );

        assert_eq!(
            root(&engine).field::<Value>("tags").unwrap().value(),
            json!(["z", "b"])
        );
    }

    #[test]
    fn the_wire_projection_keeps_the_markers_the_ops_address() {
        let mut engine = mounted();

        drop(
            engine
                .apply(
                    &stream_envelope(1, 2, vec![insert_op(&[], "m-1", -1)]),
                    ACCEPT,
                )
                .unwrap(),
        );

        assert_eq!(
            engine.document()["messages"],
            json!({"__musubi_stream__": "messages"}),
            "the cache stores the wire tree, markers included"
        );
        assert_eq!(
            root(&engine).field::<Value>("messages").unwrap().value(),
            json!([{"id": "m-1"}]),
            "while a reader sees the materialized list"
        );
    }

    // ---- drift detection -------------------------------------------------

    #[test]
    fn a_rejected_root_replace_rolls_the_whole_transaction_back() {
        let mut engine = mounted();
        let reject: Validate<'_> =
            &|_| Err(serde_json::from_value::<u8>(json!("not a number")).unwrap_err());

        let error = engine
            .apply(
                &envelope(
                    1,
                    2,
                    vec![json!({"op": "replace", "path": "", "value": {"title": "Drifted"}})],
                ),
                reject,
            )
            .unwrap_err();

        assert!(matches!(error, MusubiError::Decode { .. }));
        assert_eq!(engine.version(), 1);
        assert_eq!(title(&engine), "Cart");
    }

    #[test]
    fn the_drift_check_sees_the_transaction_before_it_commits() {
        let mut engine = mounted();
        let seen = std::sync::Mutex::new(Vec::new());
        let record: Validate<'_> = &|hydrated| {
            seen.lock().unwrap().push(hydrated.clone());

            Ok(())
        };

        drop(
            engine
                .apply(
                    &envelope(
                        1,
                        2,
                        vec![json!({"op": "replace", "path": "/title", "value": "Checkout"})],
                    ),
                    record,
                )
                .unwrap(),
        );

        let seen = seen.into_inner().unwrap();

        assert_eq!(
            seen.len(),
            1,
            "debug builds run layer 2 on every accepted envelope (§4.4)"
        );
        assert_eq!(seen[0]["title"], json!("Checkout"));
    }

    // ---- uploads ---------------------------------------------------------

    #[test]
    fn an_upload_op_reaches_the_registry_and_leaves_its_marker_in_place() {
        let mut engine = mounted();

        let envelope = PatchEnvelope::decode(json!({
            "type": "patch",
            "root_id": ROOT_ID,
            "base_version": 1,
            "version": 2,
            "ops": [],
            "upload_ops": [
                {
                    "op": "add", "upload": "avatar", "store_id": ["panel"], "ref": "entry-1",
                    "entry": {
                        "ref": "entry-1", "client_name": "me.png", "client_size": 1234,
                        "client_type": "image/png", "progress": 0, "status": "pending",
                        "errors": []
                    }
                },
                {
                    "op": "progress", "upload": "avatar", "store_id": ["panel"],
                    "ref": "entry-1", "progress": 42
                }
            ]
        }))
        .expect("the envelope decodes");

        drop(engine.apply(&envelope, ACCEPT).unwrap());

        // The slot on the tree stays inert; the live state is the handle.
        assert_eq!(
            root(&engine).field::<Value>("avatar").unwrap().value(),
            json!({"__musubi_upload__": "avatar"})
        );

        let handle = engine
            .uploads
            .handle(&store_id(&["panel"]), "avatar")
            .value();

        assert_eq!(handle.progress(), 42);
    }

    #[test]
    fn a_vanished_store_loses_its_uploads() {
        let mut engine = mounted();

        drop(
            engine
                .apply(
                    &PatchEnvelope::decode(json!({
                        "type": "patch",
                        "root_id": ROOT_ID,
                        "base_version": 1,
                        "version": 2,
                        "ops": [],
                        "upload_ops": [{
                            "op": "error", "upload": "avatar", "store_id": ["panel"],
                            "error": {"code": "too_large", "message": "too big"}
                        }]
                    }))
                    .expect("the envelope decodes"),
                    ACCEPT,
                )
                .unwrap(),
        );

        assert_eq!(
            engine
                .uploads
                .handle(&store_id(&["panel"]), "avatar")
                .value()
                .errors
                .len(),
            1
        );

        drop(
            engine
                .apply(
                    &envelope(2, 3, vec![json!({"op": "remove", "path": "/panel"})]),
                    ACCEPT,
                )
                .unwrap(),
        );

        assert!(
            engine
                .uploads
                .handle(&store_id(&["panel"]), "avatar")
                .value()
                .errors
                .is_empty()
        );
    }

    #[test]
    fn a_vanished_store_loses_its_streams_without_a_pruning_walk() {
        let mut engine = mounted();

        drop(
            engine
                .apply(
                    &stream_envelope(1, 2, vec![insert_op(&["panel"], "p-1", -1)]),
                    ACCEPT,
                )
                .unwrap(),
        );
        drop(
            engine
                .apply(
                    &envelope(2, 3, vec![json!({"op": "remove", "path": "/panel"})]),
                    ACCEPT,
                )
                .unwrap(),
        );

        // BDR-0011 fresh-mount semantics: the reappearing store starts empty,
        // because its `Collection` node was freed with its subtree (§3.5).
        drop(
            engine
                .apply(
                    &envelope(
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
                    ),
                    ACCEPT,
                )
                .unwrap(),
        );

        assert_eq!(
            root(&engine).field::<Value>("panel").unwrap().value()["messages"],
            json!([])
        );
    }

    // ---- fixtures --------------------------------------------------------

    /// An engine with no connection behind it: a tree and a bare registry.
    fn bare() -> PatchEngine {
        PatchEngine::new(StateTree::new(), Arc::new(Uploads::default()))
    }

    /// An engine holding the initial envelope's tree: a root store with a
    /// stream, an upload slot, an async field and one child store.
    fn mounted() -> PatchEngine {
        let mut engine = bare();

        drop(
            engine
                .apply(
                    &envelope(
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
                    ),
                    ACCEPT,
                )
                .expect("the initial envelope lands"),
        );

        engine
    }

    fn root(engine: &PatchEngine) -> State<Value> {
        engine.tree.root::<Value>()
    }

    fn title(engine: &PatchEngine) -> String {
        root(engine).field::<String>("title").unwrap().value()
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

    fn store_id(segments: &[&str]) -> StoreId {
        serde_json::from_value(json!(segments)).expect("store id is a string array")
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
}
