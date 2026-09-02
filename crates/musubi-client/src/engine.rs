//! The per-root patch engine: version discipline, patch application, stream
//! materialization and hydration (`docs/rust-client.md` §4.2–§4.6).
//!
//! The engine owns the **shadow document** — the authoritative wire tree as a
//! `serde_json::Value`. Ops address that tree, so it is kept pristine: every
//! cycle hydrates into an owned copy and the connection actor deserializes the
//! copy into `Arc<S::State>`.

use std::collections::HashSet;

use serde_json::Value;

use crate::envelope::PatchEnvelope;
use crate::error::{MusubiError, Result};
use crate::generated::StoreId;
use crate::index::{StoreIndex, build_store_index};
use crate::streams::StreamStore;
use crate::{hydrate, patch};

/// The message an out-of-sequence initial envelope produces.
const INITIAL_VERSION_MESSAGE: &str = "Initial patch envelope must start at version 1";

/// One root's shadow document, store index, streams and version.
#[derive(Debug)]
pub struct PatchEngine {
    version: u64,
    document: Value,
    index: StoreIndex,
    streams: StreamStore,
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
        Self {
            version: 0,
            document: Value::Null,
            index: StoreIndex::new(),
            streams: StreamStore::default(),
        }
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

    /// Forgets the version while keeping the tree, index and streams (§9).
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

    /// Applies one envelope in the order §4.3 fixes: validate, patch, stream
    /// ops, upload ops, rebuild the index, hydrate.
    ///
    /// Nothing is mutated unless every step succeeds — `json_patch::patch` is
    /// atomic, the op allowlist already ran at decode, and the version check
    /// runs first — so a rejected envelope leaves the previous tree
    /// authoritative and the caller can enter recovery (§9).
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
        self.check_version(envelope)?;

        patch::apply_ops(&mut self.document, &envelope.ops)?;

        self.streams.apply_ops(&envelope.stream_ops);

        if !envelope.upload_ops.is_empty() {
            // Uploads are deferred wholesale (§10).
            tracing::debug!(
                count = envelope.upload_ops.len(),
                "discarding upload ops: the upload engine is not implemented"
            );
        }

        self.index = build_store_index(&self.document);

        let live_store_ids: HashSet<StoreId> = self.index.keys().cloned().collect();

        self.streams.prune(&live_store_ids);

        self.version = envelope.version;

        Ok(hydrate::hydrate(&self.document, &self.streams))
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
