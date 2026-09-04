# Generated Rust code — full-surface worked example

Status: **illustrative companion** to `docs/rust-codegen.md` (normative
generator spec) and `docs/rust-client.md` (crate API). This document shows, for
one synthetic app exercising **every Musubi feature that reaches the wire
types**, what `mix compile.musubi_rust` emits and how an application consumes
it. If this file and the two design docs disagree, the design docs win.

Feature coverage map:

| Musubi feature | Declared in §1 | Generated in §2 | Used in §3 |
| :--- | :--- | :--- | :--- |
| `Musubi.State` modules (`state do`) | `Demo.ProfileState`, `Demo.LineItem` | bare structs in `demo::` | field reads |
| Root store + `attr/3` mount params | `CartPageStore` | `Params` struct | §3.2 |
| Child store + `Module.state()` | `checkout_panel` field | `StoreField<...State>` | §3.5 child commands |
| Scalars, `atom()`, literals, `map()` | `title` … `metadata` | `String`/`i64`/`bool`/`Map` | §3.3 |
| `T \| nil` | `coupon` | `Option<String>` | §3.3 |
| Atom-literal union | `phase`, `type` | C-like enums, `r#type` raw ident | §3.3 |
| Tagged union of maps | `sync` | `#[serde(tag = "type")]` enum | §3.3 |
| Inline map block (`field ... do`) | `shipping` | hoisted struct | §3.3 |
| `stream/3` | `line_items` | `Vec<LineItem>` | §3.4 |
| `stream_async/3` | `suggestions` | `AsyncResult<Vec<LineItem>>` | §3.4 |
| `assign_async` (`AsyncResult.of`) | `summary` | `AsyncResult<CartPageStoreSummary>` | §3.4 |
| Commands: payload + reply | `:apply_coupon`, `:pay` | `Command` impls | §3.6 |
| Commands: empty payload / `{:noreply}` | `:refresh` | `Refresh {}` / `NoReply` | §3.6 |
| Push events (BDR-0032), with/without payload | `:toast`, `:ping`, `:receipt_ready` | `Event` impls | §3.7 |
| Uploads (BDR-0024…0027) | `upload :attachments` | inert `UploadSlot` + runtime handle | §3.8 |
| Reconnect (BDR-0015), reply-before-patch (BDR-0009) | — (runtime) | — | §3.5, §3.9 |

`start_async/3`, lifecycle hooks, `send_update`, and PubSub `handle_info/2` are
server-side mechanics: they mutate state through the same envelopes and leave
**no trace in the generated types**, so they appear here only as this note.

---

## 1. Source (Elixir)

```elixir
defmodule Demo.ProfileState do
  use Musubi.State

  state do
    field(:name, String.t())
    field(:bio, String.t() | nil)
  end
end

defmodule Demo.LineItem do
  use Musubi.State

  state do
    field(:id, String.t())
    field(:sku, String.t())
    field(:qty, integer())
    field(:price_cents, integer())
  end
end

defmodule Demo.Stores.CheckoutPanelStore do
  # Child store: created by the parent's render output, never mounted directly.
  use Musubi.Store

  state do
    field(:status, :open | :paid)
    field(:total_cents, integer())
  end

  command :pay do
    payload do
      field :method, String.t()
    end

    reply do
      field :ok, boolean()
    end
  end

  event :receipt_ready do
    field :url, String.t()
  end

  # mount/1, render/1, handle_command/3 elided.
end

defmodule Demo.Stores.CartPageStore do
  use Musubi.Store, root: true

  attr(:cart_id, String.t(), required: true)

  state do
    # scalars
    field(:title, String.t())
    field(:revision, integer())
    field(:subtotal_cents, integer())
    field(:locked, boolean())
    field(:locale, atom())
    # nil-union -> Option
    field(:coupon, String.t() | nil)
    # atom-literal unions -> C-like enums; `:type` is a Rust keyword
    field(:phase, :browsing | :checking_out | :done)
    field(:type, :guest | :member)
    # open JSON object
    field(:metadata, map())
    # inline literal-keyed map -> hoisted struct
    field :shipping do
      field(:street, String.t())
      field(:city, String.t())
    end

    field(:tags, list(String.t()))
    # cross-module state reference
    field(:profile, Demo.ProfileState.t())
    # tagged union of maps sharing the discriminant key :type
    field(:sync, %{type: :idle} | %{type: :error, message: String.t()})
    # synchronous stream
    stream(:line_items, Demo.LineItem.t(), item_key: & &1.id, limit: -50)
    # async-seeded stream
    stream_async(:suggestions, Demo.LineItem.t(), item_key: & &1.id)
    # assign_async with an anonymous shape
    field(:summary, Musubi.AsyncResult.of(%{count: integer(), total_cents: integer()}))
    # mounted child store
    field(:checkout_panel, Demo.Stores.CheckoutPanelStore.state())
  end

  upload :attachments,
    accept: ~w(.pdf .png),
    max_entries: 3,
    max_file_size: 5_000_000

  command :apply_coupon do
    payload do
      field :code, String.t()
    end

    reply do
      field :ok, boolean()
      field :message, String.t() | nil
    end
  end

  # No payload block, handler returns {:noreply, socket}.
  command :refresh

  event :toast do
    field :message, String.t()
    field :level, atom(), doc: "severity"
  end

  event :ping

  # mount/2, render/1, handle_command/3, handle_async/3, handle_info/2 elided —
  # only declarations reach the manifest and therefore the generator.
end
```

---

## 2. Generated bundle

`mix compile.musubi_rust` writes one file (default
`priv/codegen/rust/musubi.rs`). Below is the **verbatim** output of
`Musubi.Codegen.Rust.render/1` for the source above — no annotations added, so
it round-trips `rustfmt --edition 2024 --check` exactly as it ships:

```rust
// Generated by `mix compile.musubi_rust`. Do not edit by hand.
// Include as a module file (`mod generated;`) or via `include!`.

#![allow(clippy::all, dead_code, unused_imports)]

// Prelude: re-exports only. The shared runtime types are owned by the
// client crate (`:rust_codegen_runtime_path`, default `musubi_client`).
pub mod musubi {
    pub use ::musubi_client::generated::{
        AsyncError, AsyncResult, Command, Event, NoReply, Store, StoreField, StoreId, UploadSlot,
    };
}

pub mod demo {
    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    pub struct LineItem {
        pub id: String,
        pub sku: String,
        pub qty: i64,
        pub price_cents: i64,
    }

    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    pub struct ProfileState {
        pub name: String,
        pub bio: Option<String>,
    }

    pub mod stores {
        pub mod cart_page_store {
            /// Zero-sized marker type implementing `Store`. Distinct from `State`.
            pub struct CartPageStore;

            impl super::super::super::musubi::Store for CartPageStore {
                const MODULE: &'static str = "Demo.Stores.CartPageStore";
                type State = State;
                type Params = Params;
            }

            /// The store's rendered shape: state fields plus one `UploadSlot` per
            /// declared upload. Reached as `<CartPageStore as Store>::State`.
            #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
            pub struct State {
                pub title: String,
                pub revision: i64,
                pub subtotal_cents: i64,
                pub locked: bool,
                pub locale: String,
                pub coupon: Option<String>,
                pub phase: CartPageStorePhase,
                pub r#type: CartPageStoreType,
                pub metadata: serde_json::Map<String, serde_json::Value>,
                pub shipping: CartPageStoreShipping,
                pub tags: Vec<String>,
                pub profile: super::super::super::demo::ProfileState,
                pub sync: CartPageStoreSync,
                pub line_items: Vec<super::super::super::demo::LineItem>,
                pub suggestions: super::super::super::musubi::AsyncResult<
                    Vec<super::super::super::demo::LineItem>,
                >,
                pub summary: super::super::super::musubi::AsyncResult<CartPageStoreSummary>,
                pub checkout_panel: super::super::super::musubi::StoreField<
                    super::super::super::demo::stores::checkout_panel_store::State,
                >,
                pub attachments: super::super::super::musubi::UploadSlot,
            }

            /// The mount params object, one field per `attr/3` declaration: required
            /// attrs are plain fields, optional ones `Option` that serialize to an
            /// absent key rather than an explicit `null`. A store declaring no `attr`
            /// gets an empty struct, which serializes to `{}`.
            #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
            pub struct Params {
                pub cart_id: String,
            }

            #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
            pub enum CartPageStorePhase {
                #[serde(rename = "browsing")]
                Browsing,
                #[serde(rename = "checking_out")]
                CheckingOut,
                #[serde(rename = "done")]
                Done,
            }

            #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
            pub struct CartPageStoreShipping {
                pub street: String,
                pub city: String,
            }

            #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
            pub struct CartPageStoreSummary {
                pub count: i64,
                pub total_cents: i64,
            }

            #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
            #[serde(tag = "type")]
            pub enum CartPageStoreSync {
                #[serde(rename = "idle")]
                Idle,
                #[serde(rename = "error")]
                Error { message: String },
            }

            #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
            pub enum CartPageStoreType {
                #[serde(rename = "guest")]
                Guest,
                #[serde(rename = "member")]
                Member,
            }

            #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
            pub struct ApplyCoupon {
                pub code: String,
            }

            #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
            pub struct ApplyCouponReply {
                pub ok: bool,
                pub message: Option<String>,
            }

            impl super::super::super::musubi::Command<CartPageStore> for ApplyCoupon {
                const NAME: &'static str = "apply_coupon";
                type Reply = ApplyCouponReply;
            }

            #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
            pub struct Refresh {}

            impl super::super::super::musubi::Command<CartPageStore> for Refresh {
                const NAME: &'static str = "refresh";
                type Reply = super::super::super::musubi::NoReply;
            }

            /// Push event payload (BDR-0032).
            #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
            pub struct ToastPayload {
                pub message: String,
                /// severity
                pub level: String,
            }

            impl super::super::super::musubi::Event<CartPageStore> for ToastPayload {
                const NAME: &'static str = "toast";
            }

            /// Push event payload (BDR-0032).
            #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
            pub struct PingPayload {}

            impl super::super::super::musubi::Event<CartPageStore> for PingPayload {
                const NAME: &'static str = "ping";
            }
        }

        pub mod checkout_panel_store {
            /// Zero-sized marker type implementing `Store`. Distinct from `State`.
            pub struct CheckoutPanelStore;

            impl super::super::super::musubi::Store for CheckoutPanelStore {
                const MODULE: &'static str = "Demo.Stores.CheckoutPanelStore";
                type State = State;
                type Params = Params;
            }

            /// The store's rendered shape: state fields plus one `UploadSlot` per
            /// declared upload. Reached as `<CheckoutPanelStore as Store>::State`.
            #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
            pub struct State {
                pub status: CheckoutPanelStoreStatus,
                pub total_cents: i64,
            }

            /// The mount params object, one field per `attr/3` declaration: required
            /// attrs are plain fields, optional ones `Option` that serialize to an
            /// absent key rather than an explicit `null`. A store declaring no `attr`
            /// gets an empty struct, which serializes to `{}`.
            #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
            pub struct Params {}

            #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
            pub enum CheckoutPanelStoreStatus {
                #[serde(rename = "open")]
                Open,
                #[serde(rename = "paid")]
                Paid,
            }

            #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
            pub struct Pay {
                pub method: String,
            }

            #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
            pub struct PayReply {
                pub ok: bool,
            }

            impl super::super::super::musubi::Command<CheckoutPanelStore> for Pay {
                const NAME: &'static str = "pay";
                type Reply = PayReply;
            }

            /// Push event payload (BDR-0032).
            #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
            pub struct ReceiptReadyPayload {
                pub url: String,
            }

            impl super::super::super::musubi::Event<CheckoutPanelStore> for ReceiptReadyPayload {
                const NAME: &'static str = "receipt_ready";
            }
        }
    }
}
```

Reading notes:

- **`super::` chains, not crate paths.** The bundle does not know its own crate
  name (it may be `include!`d anywhere), so cross-module references are emitted
  prost-style (`docs/rust-codegen.md` §4.3). The chain is **root-relative, not
  shortest-relative**: from three modules deep every reference climbs back to
  the file root and descends again — `super::super::super::musubi::AsyncResult`
  for the prelude, `super::super::super::demo::LineItem` for a state sibling,
  `super::super::super::demo::stores::checkout_panel_store::State` for a sibling
  store (never the shorter `super::checkout_panel_store::State`). One rule, no
  common-prefix arithmetic.
- **Line wrapping is rustfmt's.** `suggestions` and `checkout_panel` exceed 100
  columns, so their generic argument lists are wrapped exactly as
  `cargo fmt` would wrap them (§4.1) — the bundle is rustfmt-stable by
  construction and `cargo fmt --all --check` over it is a no-op.
- **Items in the store module**, in order: the zero-sized marker, its `Store`
  impl, `pub struct State` (state fields **then** one `UploadSlot` per upload),
  the types hoisted out of `State` sorted by name (`CartPageStorePhase`,
  `CartPageStoreShipping`, `CartPageStoreSummary`, `CartPageStoreSync`,
  `CartPageStoreType`), then the commands and the events in declaration order.
- **`type` is a Rust keyword** ⇒ `pub r#type`, with no `#[serde(rename)]`:
  serde's derive strips the `r#` when deriving the wire name.
- **Empty payloads** are unit-like structs *with braces* (`pub struct Refresh {}`)
  so they serialize as `{}`; `{:noreply, socket}` replies type as
  `musubi::NoReply` (a deliberate divergence from TS's `never`).
- **`float()` is not a declarable field type.** `Musubi.Type` accepts
  `integer()`, `boolean()`, `atom()`, literals, `String.t()`, containers and
  module references — not `float()` — so this example uses integer cents. The
  `f64` row of `docs/rust-codegen.md` §3.2 is reachable only through float
  literals (`1.0`, or a union of them).
- **Invisible declarations.** `item_key`/`limit` stream opts, upload config
  values (`accept`, `max_entries`, …), attr `default:` values, and every
  callback leave no trace: the generator consumes only
  `{fields, commands, events, attrs, uploads}` from the manifest, and of an
  attr only its name, type and required-ness. Upload config arrives at runtime
  via the `config` upload op; attr defaults are applied server-side.
- **Field docs**: command/event fields render `///` from `doc:`; state fields
  don't (mirrors the TS asymmetry).

---

## 3. Consuming the bundle

### 3.1 Cargo wiring (tokio application)

The crates ship inside the Hex tarball (`docs/rust-client.md` §1.3); the
generated file is committed to the consumer's own crate.

```toml
[dependencies]
musubi-client       = { path = "../deps/musubi/crates/musubi-client" }
musubi-client-tokio = { path = "../deps/musubi/crates/musubi-client-tokio" }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
```

```rust
mod generated; // the file above, written by `mix compile.musubi_rust`

use generated::demo::stores::cart_page_store::{
    ApplyCoupon, CartPageStore, CartPageStoreSync, Refresh, ToastPayload,
};
use generated::demo::stores::checkout_panel_store::{CheckoutPanelStore, Pay};
use generated::musubi::{AsyncResult, StoreId};
```

A gpui embedder swaps only this section: depend on `musubi-client` alone (no
tokio crate) and supply `Spawner`/`Timer`/`Connector` over gpui's executor —
see `docs/rust-gpui-example.md` §5.

### 3.2 Connect and mount

```rust
// The tokio crate's convenience builder: the core `Connection::builder()`
// pre-filled with TokioSpawner / TokioTimer / TungsteniteConnector.
let connection = musubi_client_tokio::builder("wss://example.app/musubi")
    .heartbeat(Duration::from_secs(30))     // optional; 30s is the default
    .build()?;

// Mount params are the store's generated `Params` struct: `cart_id` is a
// plain field because this store's `attr/3` declares `required: true`.
// Only root stores mount; the server rejects a child module with
// "declared store is not a root store".
let mounted = connection
    .mount::<CartPageStore>("cart-1", Params { cart_id: "cart-1".to_owned() })
    .await?;
```

### 3.3 Reading plain state

```rust
// One-shot read. `None` until the initial patch lands (and mid-reconnect —
// keep rendering the last-good data you hold, per BDR-0015).
if let Some(state) = mounted.snapshot() {
    let _ = (&state.title, state.revision, state.subtotal_cents, state.locked);
    let _ = &state.locale;                    // atom() ⇒ String
    if let Some(code) = &state.coupon { show_coupon(code); }
    let _ = &state.shipping.street;           // hoisted inline block
    let _ = state.metadata.get("theme");      // map() ⇒ serde_json::Map
    let _ = &state.profile.name;              // cross-module state struct

    match state.r#type {                      // keyword field, raw ident
        generated::demo::stores::cart_page_store::CartPageStoreType::Guest => {}
        generated::demo::stores::cart_page_store::CartPageStoreType::Member => {}
    }

    match &state.sync {                       // internally tagged union
        CartPageStoreSync::Idle => {}
        CartPageStoreSync::Error { message } => show_sync_error(message),
    }
}

// Push-driven: a stream of snapshots, one per accepted envelope.
let mut updates = mounted.updates();
while let Some(state) = updates.next().await {
    redraw(&state);
}
```

### 3.4 Streams and async values

```rust
let state = mounted.snapshot().unwrap();

// stream/3: already materialized in stream order by the client runtime
// (insert-at/limit semantics per docs/streams.md) — a plain Vec.
for item in &state.line_items {
    render_row(&item.sku, item.qty, item.price_cents);
}

// stream_async/3: the same Vec, wrapped in the loading|ok|failed AsyncResult.
match &state.suggestions {
    AsyncResult::Loading { result, .. } => render_stale_or_spinner(result.as_deref()),
    AsyncResult::Ok { result, .. } => render_suggestions(result),
    AsyncResult::Failed { reason, .. } => render_error(reason),
}

// assign_async with an anonymous shape: hoisted struct inside the result.
if let AsyncResult::Ok { result: summary, .. } = &state.summary {
    render_totals(summary.count, summary.total_cents);
}
```

### 3.5 Child stores

```rust
// `checkout_panel` is StoreField<State>: server-authored store_id + the
// child's own fields, flattened. Never construct or parse store ids.
let panel = &state.checkout_panel;
render_panel(&panel.state.status, panel.state.total_cents);

// Dispatch a child command through the root's channel by echoing the
// child's store_id. The target store type is inferred from `Pay`'s
// `Command<CheckoutPanelStore>` impl — no turbofish.
let reply = mounted
    .command_on(&panel.store_id, Pay { method: "card".into() })
    .await?;
assert!(reply.ok);
```

### 3.6 Commands

```rust
// Typed dispatch: `ApplyCoupon` implements `Command<CartPageStore>`, so the
// reply type is inferred as `ApplyCouponReply`.
let reply = mounted.command(ApplyCoupon { code: "SAVE10".into() }).await?;
if !reply.ok { show(reply.message.as_deref().unwrap_or("rejected")); }

// Empty payload + {:noreply} handler: NoReply deserializes from `{}`.
let _: generated::musubi::NoReply = mounted.command(Refresh {}).await?;

// Reply-before-patch (BDR-0009): a resolved reply does NOT mean the
// corresponding state change has been applied — watch `updates()` for that.
// Errors surface as MusubiError::Command / ::NotConnected / ::Timeout
// (docs/rust-client.md §11).
```

### 3.7 Push events

```rust
// Events are typed Streams keyed on (store_id, name); root store_id is the
// empty path. The stream is the subscription — dropping it unregisters.
let mut toasts = mounted.events::<ToastPayload, _>(&StoreId::root());
tokio::spawn(async move {
    while let Some(toast) = toasts.next().await {
        show_toast(&toast.message, &toast.level);
    }
});

// Child-store event, no extra machinery: same registry, child's store_id.
use generated::demo::stores::checkout_panel_store::ReceiptReadyPayload;
let mut receipts =
    mounted.events::<ReceiptReadyPayload, _>(&state.checkout_panel.store_id);
```

Events are transient (BDR-0032): no ack, no replay; a cold client can miss
mount-time events, and reconnect re-fires them (the server re-runs `mount`).

### 3.8 Uploads

```rust
// The state slot stays inert: `attachments` deserializes from the wire marker
// as UploadSlot { name }, and that name is the key to the live handle. The
// handle carries both planes — the server-driven data plane (snapshot(),
// updates(), one item per envelope that touched it) and the client-driven
// control plane (select / start / cancel / reset) — docs/rust-client.md §10.
let slot_name = &state.attachments.name; // "attachments"
let attachments = mounted.upload(&StoreId::root(), slot_name);

let entries = attachments
    .select(vec![UploadFile::new("spec.pdf", "application/pdf", bytes)])
    .await?;                       // preflight: the server signs one token per entry
attachments.start().await?;        // channel mode: binary chunks, sequential per entry

// External (direct-to-S3) mode needs no call-site change: the server's
// upload_external/3 names an uploader, registered on the builder via
// ConnectionBuilder::uploader(name, impl Uploader) — the app does the PUT.
```

### 3.9 Teardown and reconnect

```rust
// Unmount is RAII: dropping the last `Mounted` clone leaves the channel and
// the server stops the root. No explicit unmount call exists.
drop(mounted);

// Whole-connection teardown, observable:
connection.disconnect().await?;
```

On transport drop, the runtime keeps the last-good tree rendering, rejoins
with backoff, and the server re-runs `mount` with the original params — a fresh
version sequence and a fresh initial patch swap the state in atomically
(BDR-0015). In-flight commands fail with `Disconnected`; streams are rebuilt
from whatever `mount` re-seeds (BDR-0022).

---

## 4. Regeneration workflow

```elixir
# consumer mix.exs
compilers: Mix.compilers() ++ [:musubi_rust]

# config
config :musubi, :rust_codegen_output_path, "desktop/src/generated.rs"
```

`mix compile` keeps the file in sync; `mix compile.musubi_rust --check` fails
CI when the committed file drifts from the store declarations — same contract
as the TS target.
