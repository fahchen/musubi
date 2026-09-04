//! The per-root patch engine: version discipline, patch application, stream
//! materialization and hydration (`docs/rust-client.md` §4.2–§4.6).
//!
//! The engine owns the **shadow document** — the authoritative wire tree as a
//! `serde_json::Value`. Ops address that tree, so it is kept pristine: every
//! cycle works on an owned copy, hydrates that copy in place, and the
//! connection actor deserializes it into `Arc<S::State>`.
//!
//! Applying an envelope is therefore two calls, not one: [`PatchEngine::prepare`]
//! produces the hydrated state without touching a thing, and
//! [`PatchEngine::commit`] lands it. The caller deserializes in between, so a
//! tree that does not match the generated types is dropped with the engine
//! still on the previous version (§11).

use std::collections::HashSet;
use std::sync::Arc;

use serde_json::Value;

use crate::envelope::PatchEnvelope;
use crate::error::{MusubiError, Result};
use crate::generated::StoreId;
use crate::index::{StoreIndex, build_store_index};
use crate::streams::{StagedStreams, StreamStore, StreamsView};
use crate::uploads::Uploads;
use crate::{hydrate, patch};

/// The message an out-of-sequence initial envelope produces.
const INITIAL_VERSION_MESSAGE: &str = "Initial patch envelope must start at version 1";

/// One root's shadow document, store index, streams, uploads and version.
#[derive(Debug)]
pub struct PatchEngine {
    version: u64,
    document: Value,
    index: StoreIndex,
    streams: StreamStore,
    uploads: Arc<Uploads>,
}

impl PatchEngine {
    /// A fresh engine, awaiting its initial envelope.
    ///
    /// ```
    /// use musubi_client::PatchEngine;
    ///
    /// assert_eq!(PatchEngine::new().version(), 0);
    /// ```
    pub fn new() -> Self {
        Self::with_uploads(Arc::new(Uploads::default()))
    }

    /// The engine a mounted root gets: same as [`new`](Self::new), except the
    /// upload registry is the one its [`Mounted`](crate::Mounted) hands out
    /// handles from, so folded `upload_ops` and observed handles are the same
    /// objects.
    pub(crate) fn with_uploads(uploads: Arc<Uploads>) -> Self {
        Self {
            version: 0,
            document: Value::Null,
            index: StoreIndex::new(),
            streams: StreamStore::default(),
            uploads,
        }
    }

    /// The root's upload handles, keyed by `(store_id, name)`.
    ///
    /// ```
    /// use musubi_client::PatchEngine;
    /// use musubi_client::generated::StoreId;
    ///
    /// let engine = PatchEngine::new();
    /// let avatar = engine.uploads().handle(&StoreId::root(), "avatar");
    ///
    /// assert!(avatar.snapshot().is_idle());
    /// ```
    pub fn uploads(&self) -> &Uploads {
        &self.uploads
    }

    /// The last accepted envelope's `version`; `0` means "awaiting the initial
    /// envelope" (fresh, or mid-reconnect).
    ///
    /// ```
    /// use musubi_client::{PatchEngine, PatchEnvelope};
    /// use serde_json::json;
    ///
    /// let mut engine = PatchEngine::new();
    /// let envelope = PatchEnvelope::decode(json!({
    ///     "type": "patch",
    ///     "root_id": "MyApp.CartStore:cart",
    ///     "base_version": 0,
    ///     "version": 1,
    ///     "ops": [{"op": "replace", "path": "", "value": {"__musubi_store_id__": []}}]
    /// }))
    /// .unwrap();
    ///
    /// engine.apply(&envelope).unwrap();
    ///
    /// assert_eq!(engine.version(), 1);
    /// ```
    pub fn version(&self) -> u64 {
        self.version
    }

    /// The pristine wire tree the ops address.
    ///
    /// Stream markers are still in place here; what [`PatchEngine::apply`]
    /// returns is the hydrated view.
    ///
    /// ```
    /// use musubi_client::PatchEngine;
    ///
    /// assert!(PatchEngine::new().document().is_null());
    /// ```
    pub fn document(&self) -> &Value {
        &self.document
    }

    /// Forgets the version while keeping the tree, index, streams and uploads
    /// (§9).
    ///
    /// Every recovery path — a rejoin, a version gap, a transport drop — leaves
    /// the last-good rendering in place and waits for a fresh initial envelope
    /// to swap it out atomically. Only the version is reset, so the next
    /// envelope must again be `base_version: 0, version: 1`.
    ///
    /// ```
    /// use musubi_client::{PatchEngine, PatchEnvelope};
    /// use serde_json::json;
    ///
    /// let mut engine = PatchEngine::new();
    /// let initial = PatchEnvelope::decode(json!({
    ///     "type": "patch",
    ///     "root_id": "MyApp.CartStore:cart",
    ///     "base_version": 0,
    ///     "version": 1,
    ///     "ops": [{"op": "replace", "path": "", "value": {"title": "Cart"}}]
    /// }))
    /// .unwrap();
    ///
    /// engine.apply(&initial).unwrap();
    /// engine.soft_reset();
    ///
    /// assert_eq!(engine.version(), 0);
    /// assert_eq!(engine.document()["title"], json!("Cart"));
    /// ```
    pub fn soft_reset(&mut self) {
        self.version = 0;
    }

    /// Adopts a cached wire tree as the current document, without touching the
    /// version (`docs/rust-client.md` §6.4).
    ///
    /// The engine stays at `0`, so the live initial envelope is still required
    /// to be `base_version: 0, version: 1` and still swaps the whole tree out
    /// in one `replace ""`. Streams are **not** seeded — `stream_ops` are not
    /// part of the cached tree — so a seeded stream slot hydrates to `[]` until
    /// the live envelope refills it, exactly as in the TypeScript client.
    ///
    /// ```
    /// use musubi_client::PatchEngine;
    /// use serde_json::json;
    ///
    /// let mut engine = PatchEngine::new();
    /// let state = engine.seed(json!({"__musubi_store_id__": [], "title": "Cart"}));
    ///
    /// assert_eq!(state["title"], json!("Cart"));
    /// assert_eq!(engine.version(), 0);
    /// ```
    pub fn seed(&mut self, document: Value) -> Value {
        self.document = document;
        self.index = build_store_index(&self.document);

        let live_store_ids: HashSet<StoreId> = self.index.keys().cloned().collect();

        self.streams.prune(&live_store_ids);
        self.uploads.prune(&live_store_ids);

        let mut state = self.document.clone();

        hydrate::hydrate(&mut state, StreamsView::committed(&self.streams));

        state
    }

    /// Undoes a [`seed`](Self::seed) whose tree did not match the generated
    /// types, putting the engine back where a cold mount would have left it.
    ///
    /// A cached tree written by an older build can be a shape this binary can
    /// no longer deserialize; dropping it is always safe, because the live
    /// initial patch replaces the whole tree anyway.
    ///
    /// ```
    /// use musubi_client::PatchEngine;
    /// use serde_json::json;
    ///
    /// let mut engine = PatchEngine::new();
    ///
    /// engine.seed(json!({"title": "Cart"}));
    /// engine.discard_seed();
    ///
    /// assert!(engine.document().is_null());
    /// ```
    pub fn discard_seed(&mut self) {
        self.document = Value::Null;
        self.index = StoreIndex::new();
    }

    /// Applies one envelope in the order §4.3 fixes: validate, patch, stream
    /// ops, upload ops, rebuild the index, prune, hydrate.
    ///
    /// Upload ops are folded into the registry rather than into the tree: an
    /// upload slot stays the inert `{"__musubi_upload__": name}` marker on the
    /// hydrated state, and its live state is read through
    /// [`uploads`](Self::uploads) (§10).
    ///
    /// Nothing is mutated unless every step succeeds — `json_patch::patch` is
    /// atomic, the op allowlist already ran at decode, and the version check
    /// runs first — so a rejected envelope leaves the previous tree
    /// authoritative and the caller can enter recovery (§9).
    ///
    /// This is the crate-internal `prepare` step followed immediately by
    /// `commit`. A caller that has to *validate* the state before it lands —
    /// the connection actor, which deserializes it into the generated types —
    /// takes the two steps separately instead.
    ///
    /// ```
    /// use musubi_client::{PatchEngine, PatchEnvelope};
    /// use serde_json::json;
    ///
    /// let mut engine = PatchEngine::new();
    /// let initial = PatchEnvelope::decode(json!({
    ///     "type": "patch",
    ///     "root_id": "MyApp.CartStore:cart",
    ///     "base_version": 0,
    ///     "version": 1,
    ///     "ops": [{
    ///         "op": "replace",
    ///         "path": "",
    ///         "value": {"__musubi_store_id__": [], "title": "Cart"}
    ///     }]
    /// }))
    /// .unwrap();
    ///
    /// let state = engine.apply(&initial).unwrap();
    ///
    /// assert_eq!(state["title"], json!("Cart"));
    /// ```
    //
    // ponytail: no change set is returned — v1 publishes one whole-root
    // snapshot per envelope, so nothing consumes it. The §5 change-notification
    // rule gets computed here when the per-store snapshot cache lands.
    pub fn apply(&mut self, envelope: &PatchEnvelope) -> Result<Value> {
        let staged = self.prepare(envelope)?;

        Ok(self.commit(staged))
    }

    /// Runs everything an envelope can fail at, against a working copy: the
    /// version check, the ops, the index rebuild, the stream fold and the
    /// hydration walk (§4.3 steps 1–5 and §4.6).
    ///
    /// Takes `&self`: preparing cannot move the engine, so a
    /// [`StagedPatch`] that is dropped rather than committed leaves the
    /// version, the tree, the streams and every upload subscriber exactly as
    /// they were.
    ///
    /// The working copy is the copy the hydration walk used to make on its own
    /// — the ops land on it, the index is read off it, and the walk then
    /// rewrites it in place into the hydrated state — so the cycle still costs
    /// one copy of the tree.
    ///
    /// Pruning is a commit-phase step, so the walk runs against streams that
    /// still include ones the commit is about to drop. It cannot see the
    /// difference: a stream is pruned when its owning store is absent from the
    /// freshly rebuilt index, and every store id the walk resolves a marker
    /// against is one it read out of that same tree — a prunable stream has no
    /// marker left to look it up with. The one store id the walk can name
    /// without reading it is the root, and the wire root is always a store node
    /// (§4.6).
    pub(crate) fn prepare<'a>(&self, envelope: &'a PatchEnvelope) -> Result<StagedPatch<'a>> {
        self.check_version(envelope)?;

        let mut state = self.document.clone();

        patch::apply_ops(&mut state, &envelope.ops)?;

        let index = build_store_index(&state);
        let streams = self.streams.stage(&envelope.stream_ops);

        hydrate::hydrate(&mut state, StreamsView::staged(&self.streams, &streams));

        Ok(StagedPatch {
            envelope,
            state,
            index,
            streams,
        })
    }

    /// Lands a prepared envelope and hands back its hydrated state: the tree,
    /// the index, the streams and the version first, then the upload ops —
    /// whose subscribers are the first thing outside the engine to hear that
    /// this envelope was accepted (§10).
    ///
    /// The shadow document replays the ops rather than adopting the working
    /// copy, because that copy has already been rewritten in place into the
    /// hydrated state; replaying a delta is what keeps the cycle at one copy of
    /// the tree instead of two.
    pub(crate) fn commit(&mut self, staged: StagedPatch<'_>) -> Value {
        let StagedPatch {
            envelope,
            state,
            index,
            streams,
        } = staged;

        if let Err(error) = patch::apply_ops(&mut self.document, &envelope.ops) {
            // Unreachable: `prepare` just applied these same ops to a copy of
            // this same tree, and `json_patch::patch` is a pure function of the
            // two. Should it ever happen, the tree is left as it was and the
            // version is not bumped — the state of a *rejected* envelope, which
            // the next one recovers from (§9) — rather than a tree and a
            // version that disagree.
            tracing::error!(%error, "a prepared envelope did not replay onto the shadow document");

            return state;
        }

        self.index = index;
        self.streams.commit(streams);
        self.uploads.apply_ops(&envelope.upload_ops);

        let live_store_ids: HashSet<StoreId> = self.index.keys().cloned().collect();

        self.streams.prune(&live_store_ids);
        self.uploads.prune(&live_store_ids);

        self.version = envelope.version;

        state
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

impl Default for PatchEngine {
    fn default() -> Self {
        Self::new()
    }
}

/// One envelope, applied to a working copy and not yet accepted.
///
/// Everything the envelope changes lives here until [`PatchEngine::commit`]
/// takes it: the hydrated state, the rebuilt index, the stream fold, and the
/// envelope whose ops the shadow document still has to see. It borrows the
/// envelope and shares nothing with the engine, so dropping it is how a caller
/// rejects an envelope it has already inspected.
pub(crate) struct StagedPatch<'a> {
    envelope: &'a PatchEnvelope,
    state: Value,
    index: StoreIndex,
    streams: StagedStreams,
}

impl StagedPatch<'_> {
    /// The hydrated tree this envelope produces, for the caller to deserialize
    /// before deciding.
    pub(crate) fn state(&self) -> &Value {
        &self.state
    }
}

#[cfg(test)]
mod tests {
    use futures_executor::block_on;
    use futures_util::{FutureExt, StreamExt};
    use serde_json::json;

    use super::*;

    const ROOT_ID: &str = "MyApp.CartStore:cart";

    #[test]
    fn a_staged_patch_carries_the_whole_envelope_before_any_of_it_lands() {
        let mut engine = PatchEngine::new();

        engine
            .apply(&initial())
            .expect("the initial envelope lands");

        let second = second();
        let staged = engine.prepare(&second).expect("the envelope prepares");

        assert_eq!(staged.state()["title"], json!("Checkout"));
        assert_eq!(
            staged.state()["messages"],
            json!([{"id": "a"}, {"id": "b"}]),
            "the staged fold is what the walk materialized"
        );
    }

    #[test]
    fn dropping_a_staged_patch_leaves_the_engine_exactly_as_it_was() {
        let mut engine = PatchEngine::new();

        engine
            .apply(&initial())
            .expect("the initial envelope lands");

        let avatar = engine.uploads().handle(&StoreId::root(), "avatar");
        let mut updates = avatar.updates();
        let document = engine.document().clone();

        let second = second();

        drop(engine.prepare(&second).expect("the envelope prepares"));

        assert_eq!(engine.version(), 1, "the version did not move");
        assert_eq!(
            engine.document(),
            &document,
            "the shadow document is intact"
        );
        assert_eq!(
            engine
                .streams
                .entries(&StoreId::root(), "messages")
                .iter()
                .map(|entry| entry.item_key.clone())
                .collect::<Vec<_>>(),
            ["a"],
            "the stream fold was dropped with it"
        );
        assert!(
            avatar.snapshot().entry("u_1").is_none(),
            "the upload ops were never folded"
        );
        assert!(
            updates.next().now_or_never().is_none(),
            "and no upload subscriber was told otherwise"
        );
    }

    #[test]
    fn committing_a_staged_patch_lands_every_part_of_it() {
        let mut engine = PatchEngine::new();

        engine
            .apply(&initial())
            .expect("the initial envelope lands");

        let avatar = engine.uploads().handle(&StoreId::root(), "avatar");
        let mut updates = avatar.updates();

        let state = engine.apply(&second()).expect("the envelope lands");

        assert_eq!(engine.version(), 2);
        assert_eq!(engine.document()["title"], json!("Checkout"));
        assert_eq!(state["messages"], json!([{"id": "a"}, {"id": "b"}]));
        assert!(
            engine.document()["messages"]["__musubi_stream__"] == json!("messages"),
            "the shadow document keeps the marker the ops address"
        );
        assert!(matches!(
            block_on(updates.next()),
            Some(handle) if handle.entry("u_1").is_some()
        ));
    }

    /// `version: 1`, one stream entry, no upload state yet.
    fn initial() -> PatchEnvelope {
        envelope(
            0,
            1,
            json!([{
                "op": "replace",
                "path": "",
                "value": {
                    "__musubi_store_id__": [],
                    "title": "Cart",
                    "messages": {"__musubi_stream__": "messages"},
                    "avatar": {"__musubi_upload__": "avatar"}
                }
            }]),
            json!([{
                "op": "insert", "stream": "messages", "store_id": [],
                "item_key": "a", "at": -1, "item": {"id": "a"}, "limit": null
            }]),
            json!([]),
        )
    }

    /// The envelope under test: it moves the tree, the stream and an upload
    /// handle at once, so a dropped stage has three things to leave alone.
    fn second() -> PatchEnvelope {
        envelope(
            1,
            2,
            json!([{"op": "replace", "path": "/title", "value": "Checkout"}]),
            json!([{
                "op": "insert", "stream": "messages", "store_id": [],
                "item_key": "b", "at": -1, "item": {"id": "b"}, "limit": null
            }]),
            json!([{
                "op": "add", "upload": "avatar", "store_id": [], "ref": "u_1",
                "entry": {
                    "ref": "u_1", "client_name": "me.png", "client_size": 12,
                    "client_type": "image/png", "progress": 0, "status": "pending",
                    "errors": []
                }
            }]),
        )
    }

    fn envelope(
        base_version: u64,
        version: u64,
        ops: Value,
        stream_ops: Value,
        upload_ops: Value,
    ) -> PatchEnvelope {
        PatchEnvelope::decode(json!({
            "type": "patch",
            "root_id": ROOT_ID,
            "base_version": base_version,
            "version": version,
            "ops": ops,
            "stream_ops": stream_ops,
            "upload_ops": upload_ops,
        }))
        .expect("the envelope decodes")
    }
}
