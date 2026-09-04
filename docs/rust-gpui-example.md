# gpui example client — plan

Status: **shipped**, as `examples/chat_room/desktop/` (four hand-written Rust
files plus the committed generated bundle) and four Elixir-side edits to
`examples/chat_room`. This document decided what to build, in what order, and
what had to land first; sections marked "as landed" record where the D0 spike
(§7.1) forced a deviation from the original decision.

The deliverable is a native desktop client, written with
[gpui](https://www.gpui.rs), that consumes a Musubi server through the
`musubi-client` crate (`docs/rust-client.md`) and the generated Rust bundle
(`docs/rust-codegen.md`). Its purpose is to prove — visibly, in a running
window — that the Musubi wire contract is client-agnostic: the same store, the
same channel, the same patch stream, rendered by a program that shares no code
with `packages/client`.

---

## 0. Decision summary

| Question | Decision |
| :-- | :-- |
| New example or reuse a backend? | **Reuse `examples/chat_room`.** Add a sibling `desktop/` crate next to the existing `ui/` |
| Directory / crate name | `examples/chat_room/desktop/`, Cargo package `chat-room-desktop` |
| Backend port | **4002** (unchanged). No new port is allocated |
| gpui dependency form | crates.io `gpui = "0.2.2"` (published 2025-10-22, Apache-2.0), **not** a git pin on `zed-industries/zed` |
| Widget layer | crates.io `gpui-component = "0.5.1"` for `Input`/`Button`/theming. **As landed:** the spike (§7.1) passed, so the hand-rolled-input fallback was not needed; 0.5.1 spells the widget `Input` + `InputState`, not `TextInput` |
| Cargo workspace | The example crate is **detached** from the repo-root workspace with an empty `[workspace]` table; own `Cargo.lock`, own `target/` |
| Generated types | `mix compile.musubi_rust` writes `desktop/src/generated.rs`, **committed**, exactly as `ui/src/generated/musubi.d.ts` is committed |
| Async runtime | **No tokio on the Musubi path.** `musubi-client` core only (runtime-free by construction; the tokio impls live in the separate `musubi-client-tokio` crate, not depended on here); `Spawner`/`Timer` over `gpui::BackgroundExecutor`; `Connector` over `async-net` + `async-tungstenite`. **As landed:** gpui itself links tokio via `gpui_http_client`, so the invariant is checkable but not an empty `cargo tree` (§5.4) |
| Run | `mix server` (terminal 1) + `mix desktop` (terminal 2); optionally `mix ui` (terminal 3) to demo two heterogeneous clients on one room |
| Platform | macOS first-class. Linux best-effort. Windows out of scope for v1 |

---

## 1. Which example

### 1.1 Decision: `examples/chat_room` gains `desktop/`

```
examples/chat_room/
  ui/        # React client   (exists)
  desktop/   # gpui client    (new)
```

One backend, one port, one store tree, two clients written in two languages
against two generated bundles. `mix server` serves both simultaneously.

Rationale:

- **The demo is the point.** Running `mix ui` and `mix desktop` side by side
  against one `mix server` shows a message typed in the browser appearing in the
  native window, and presence changing in both, driven by the same
  PubSub-triggered patch envelopes. A duplicated backend cannot show that, and
  it is the single most convincing artifact this example can produce.
- **`chat_room` already covers the interesting client surface** and nothing
  else. One root store, one channel, no child stores, no client-side routing,
  no persistence — so the Rust client only has to implement join → initial
  patch → incremental patches → command dispatch. It still exercises
  `stream_async` (`AsyncResult` wrapping a stream), `assign_async` (a plain
  `AsyncResult<list>`), a three-arm tagged union (`last_send_status`), two
  commands with two different reply shapes, and PubSub-driven cross-client
  updates. The ~1.5 s artificial latency on the history seed
  (`@history_load_delay_ms`) makes the `loading → ok` transition visible on
  every mount and reconnect, which is exactly the state a native client must
  render correctly.
- **Zero backend duplication.** A copied backend is a second `mix.lock`, a
  second port pair, and a second copy of `chat_room_store.ex` that silently
  drifts from the original the first time either is edited.
- **No new port allocation.** The convention (`cart_page` 4001/4101,
  `chat_room` 4002/4102, `poll_app` 4003/4103) does not need extending.
- **The example convention is preserved.** `examples/<name>/` is "a standalone
  mini-app that depends on musubi via `path: \"../..\"`" (AGENTS.md:95). It
  says nothing about how many front-ends that app has. `ui/` and `desktop/` are
  two front-ends of one app.

### 1.2 Rejected alternatives

| Alternative | Why not |
| :-- | :-- |
| New `examples/gpui_chat/` copying the chat_room backend to ports 4004/4104 | Duplicates 481 LOC of backend that will drift; loses the two-clients-one-server demo; costs a port pair for no new server behavior |
| Point a Rust crate outside `examples/` (e.g. `crates/musubi-client/examples/gpui_chat.rs`) at the chat_room server | Breaks the "example is self-contained and starts with `mix server`" convention; a cargo example cannot carry a committed generated bundle produced by the *Elixir* project's compiler; also drags gpui into the library crate's dev-dependency graph and CI |
| Reuse `examples/cart_page` | Child-store proxy materialization (`HeaderStore`/`CartStore`/`CartLineStore`, 3 levels) plus lifecycle hooks and persistence. Every one of those is a `musubi-client` feature that has to be finished *and* a UI concern; wrong first target |
| Reuse `examples/poll_app` | Two roots and client-side routing. Two simultaneous channels and a router are orthogonal to proving the wire contract |

### 1.3 Cost accepted

`examples/chat_room` stops being a single-language example. Its `mix.exs`,
`config/config.exs`, `.gitignore`, and `README.md` all change (§2.3), and its
`README.md` grows a third "Start the example" block. The root `README.md`
bullet becomes `examples/chat_room - PubSub-backed chat room, with React and
gpui clients`.

---

## 2. File tree

### 2.1 Planned tree

New files marked `+`, edited files marked `~`.

```
examples/chat_room/
~ .gitignore                    # + /desktop/target/
~ README.md                     # + "## Desktop client" section + third start block
~ mix.exs                       # + :musubi_rust compiler, + `desktop` alias
~ config/config.exs             # + :rust_codegen_output_path
  lib/                          # UNCHANGED — no server change is required
  ui/                           # UNCHANGED
+ desktop/
+   Cargo.toml
+   Cargo.lock                  # committed (binary crate; reproducible `cargo run`)
+   src/
+     main.rs                   # Application::new().run(...), window, wiring
+     app.rs                    # ChatWindow: the single Render entity
+     transport.rs              # GpuiSpawner, GpuiTimer, SmolConnector, WsSocket
+     generated.rs              # COMMITTED. `mix compile.musubi_rust` output
```

Four hand-written Rust files. `app.rs` is the only one that grows with UI
scope; `transport.rs` is the file other gpui embedders will copy — as landed it
is 294 lines, about 260 of them before the unit-test module for the URL
parser.

Deliberately absent:

- No `desktop/README.md` — the example has one README (`examples/chat_room/README.md`).
- No `desktop/.gitignore` — **as landed**, `examples/chat_room/.gitignore`
  carries `/desktop/target/` and a second file ignoring the same directory
  would be redundant.
- No `rust-toolchain.toml` in v1 (see open questions).
- No `build.rs`, no `cargo fmt`/`clippy` wiring in CI — examples are not in CI
  today and this plan does not change that.
- No `desktop/src/generated/` directory. The bundle is a single file and the
  generated header carries a `#![allow(...)]` **inner** attribute
  (`docs/rust-codegen.md` §4.1), which requires it to be a module file. As
  `src/generated.rs` it is reachable with a plain `mod generated;` and no
  `#[path]` attribute.

### 2.2 `desktop/Cargo.toml` sketch

```toml
[package]
name = "chat-room-desktop"
version = "0.1.0"
edition = "2024"
rust-version = "1.85"       # matches musubi-client MSRV (docs/rust-client.md 1.4)
publish = false
license = "MIT"

# Detached from any parent Cargo workspace on purpose. Examples are
# documentation, not members of the library's build graph (AGENTS.md:95), and
# gpui must not appear in the root Cargo.lock or in musubi-client's CI.
[workspace]

[dependencies]
# The Musubi client runtime core — runtime-free by construction. The tokio
# impls live in the separate `musubi-client-tokio` crate, deliberately absent.
musubi-client = { path = "../../../crates/musubi-client" }

# UI. crates.io, Apache-2.0, both self-consistent: gpui-component 0.5.1
# declares `gpui ^0.2.2` / `gpui-macros ^0.2.2` as published dependencies.
gpui = "0.2.2"
gpui-component = "0.5.1"

# Transport: smol-family, no tokio reactor.
async-net = "2"                                        # TcpStream over async-io
# `handshake` gives `client_async`, `futures-03-sink` gives the `Sink` impl on
# `WebSocketStream`. Spelling out the crate's own defaults pins away the
# `tokio-runtime` / `async-std-runtime` / TLS features.
async-tungstenite = { version = "0.35", default-features = false, features = [
  "handshake",
  "futures-03-sink",
] }
futures = "0.3"

serde = { version = "1", features = ["derive"] }       # generated.rs derives
serde_json = "1"
```

Notes:

- **`gpui = "0.2.2"`, not git.** crates.io `gpui` latest is 0.2.2, published
  2025-10-22, Apache-2.0. The `zed-industries/zed` `main` README documents
  `gpui` + `gpui_platform`, but `gpui_platform` **does not exist on crates.io**
  and cannot: `crates/gpui_platform/Cargo.toml` sets `publish.workspace = true`
  against a workspace `publish = false`. So `main` is git-only. A git pin in a
  documentation example means CI/readers fetching an unpinned fast-moving
  branch, a `Cargo.lock` that references a commit rather than a version, and an
  API (`gpui_platform::application()`) that no published crate matches. Use
  crates.io; revisit when `gpui_platform` publishes.
- **API consequence:** on 0.2.2 the entry point is
  `gpui::Application::new().run(|cx: &mut App| ...)`. On `main` it is
  `gpui_platform::application().run(...)`. Every snippet in this document is
  the 0.2.2 form, which is also what gpui.rs still documents.
- **`async-tungstenite = "0.35"`, not 0.33.** The original sketch pinned 0.33
  because that is what zed's own workspace uses. **As landed** the crate is on
  0.35 with `handshake` + `futures-03-sink` spelled out: the zed-pin rationale
  bought compatibility with an executor family, and `async-net` (not zed) is
  what supplies the stream here, so the newer release with the features named
  explicitly is the better pin. The features are the crate's defaults; naming
  them is what keeps `tokio-runtime` and the TLS features off across future
  minor bumps.
- **gpui default features.** Spike output (§7.1 question 4): **keep the
  defaults**. `gpui-component 0.5.1` declares a plain `gpui = "0.2.2"`, so
  cargo feature unification re-enables anything a `default-features = false`
  here would drop. The x11/wayland features are inert on macOS.
- **No tokio on the Musubi path.** Spike output: an empty `cargo tree -i tokio`
  is *not* achievable, because gpui reaches tokio through
  `gpui_http_client → zed-reqwest → hyper`. The checkable invariant, and the
  one the README carries, is: `musubi-client-tokio` is absent from
  `[dependencies]`, `async-tungstenite`'s runtime features are off, and every
  path in `cargo tree -i tokio -e normal` runs through `gpui_http_client`.

### 2.3 Elixir-side edits

`mix.exs`:

```elixir
compilers: Mix.compilers() ++ [:musubi_ts, :musubi_rust],
...
defp aliases do
  [
    server: ["deps.get", "run --no-halt"],
    ui: [&ui_setup/1, &ui_dev/1],
    desktop: [&desktop_run/1]
  ]
end

defp desktop_run(_args), do: cmd!("cargo run", "desktop")
```

`ui_cmd!/1` generalizes to `cmd!(command, dir)` so `ui` and `desktop` share it;
the failure mode (`Mix.raise("`#{command}` exited with status #{status}")`)
is unchanged. `mix desktop` deliberately does **not** run `cargo fetch` first —
`cargo run` already resolves and builds.

`config/config.exs`:

```elixir
config :musubi, :ts_codegen_output_path, "ui/src/generated/musubi.d.ts"
config :musubi, :rust_codegen_output_path, "desktop/src/generated.rs"
```

Both compilers read the same manifest
(`_build/dev/musubi-codegen/<module>/state.term` after the shared-manifest
hoist), so enabling both costs one extra render and one extra file write per
`mix compile`.

`.gitignore` gains `/desktop/target/`; the root `.gitignore` already covers the
workspace's own `/target/`.

---

## 3. Generated bundle

### 3.1 What `mix compile.musubi_rust` emits for `chat_room`

Derived from `docs/rust-codegen.md` §3–§4 applied to the three state modules
this example declares (`ChatRoom.MessageState`, `ChatRoom.OnlineUser`,
`ChatRoom.Stores.ChatRoomStore`). Illustrative, not normative — the renderer
owns the exact text.

```rust
// Generated by `mix compile.musubi_rust`. Do not edit by hand.
#![allow(clippy::all, dead_code, unused_imports)]

// Prelude: re-exports only. The shared runtime types live in the client crate
// (docs/rust-codegen.md §4.5).
pub mod musubi {
    pub use ::musubi_client::generated::{
        AsyncError, AsyncResult, Command, Event, NoReply, Store, StoreField, StoreId, UploadSlot,
    };
}

pub mod chat_room {
    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    pub struct MessageState { pub id: String, pub body: String, pub sender: String }

    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    pub struct OnlineUser { pub id: String, pub name: String }

    pub mod stores {
        // A `:store` module gets its own `pub mod` holding a zero-sized marker
        // plus the shape struct (docs/rust-codegen.md §4.2/§4.6).
        pub mod chat_room_store {
            /// Marker type. This is the `St: Store` parameter.
            pub struct ChatRoomStore;

            impl super::super::super::musubi::Store for ChatRoomStore {
                const MODULE: &'static str = "ChatRoom.Stores.ChatRoomStore";
                type State = State;
            }

            /// The rendered shape. `<ChatRoomStore as Store>::State`.
            #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
            pub struct State {
                // `stream_async :messages` => AsyncResult<Vec<_>>; the stream
                // marker is resolved by the client's hydration pass before
                // serde runs (docs/rust-client.md §4.6), so there is no
                // StreamField type.
                pub messages: super::super::super::musubi::AsyncResult<Vec<super::super::MessageState>>,
                pub current_user: super::super::OnlineUser,
                pub online_users: super::super::super::musubi::AsyncResult<Vec<super::super::OnlineUser>>,
                pub last_send_status: ChatRoomStoreLastSendStatus,
            }

            // Hoisted: Rust is nominal, so the three-arm tagged union in
            // `state do` cannot be written inline. The prefix is the store
            // marker name, not `State` (docs/rust-codegen.md §3.5).
            // Explicit per-variant renames, never `rename_all`: atoms can
            // carry arbitrary characters (docs/rust-codegen.md §3.4 case 5).
            #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
            #[serde(tag = "type")]
            pub enum ChatRoomStoreLastSendStatus {
                #[serde(rename = "idle")]
                Idle,
                #[serde(rename = "ok")]
                Ok { id: String },
                #[serde(rename = "failed")]
                Failed { reason: String },
            }

            // Command payloads + their `Command<ChatRoomStore>` impls, and the
            // reply structs, also live here.
        }
    }
}
```

The hoisted enum is the single most valuable thing this example demonstrates
about the Rust target: the TypeScript bundle writes
`{ type: "idle" } | { type: "ok"; id: string } | ...` inline and gets
exhaustiveness from the compiler for free; Rust has to name the type, and the
consuming UI then gets a `match` that the compiler forces to cover all three
arms. §4.5 renders it.

### 3.2 The bundle ⇄ crate seam (settled)

Both source documents now agree, and this is what the example is written
against: **the client crate owns the shared runtime types; the bundle
re-exports them.**

- The bundle emits `impl ::musubi_client::generated::Store for ChatRoomStore`
  and `impl ::musubi_client::generated::Command<ChatRoomStore> for SendMessage`,
  and its `pub mod musubi` contains nothing but
  `pub use ::musubi_client::generated::{AsyncError, AsyncResult, Command, Event,
  NoReply, Store, StoreField, StoreId, UploadSlot};`
  (`docs/rust-codegen.md` §4.5, `docs/rust-client.md` §8.2).
- The traits are **not** sealed — a sealed trait cannot be implemented from a
  file generated into a consumer crate.
- `:rust_codegen_runtime_path` (default `"musubi_client"`) selects the path, so
  a consumer that re-exports the crate under another name can retarget it.

This still deserves a BDR, because it fixes the public shape of every generated
Rust bundle and the coupling between two independently versioned artifacts —
but it is no longer a blocker for D5.

### 3.3 Drift checking

The bundle is committed, so it can rot. `mix compile.musubi_rust --check`
exists for exactly this and returns a `Mix.Task.Compiler.Diagnostic` on drift.
Examples are not in CI today and this plan does not add them. **As landed** the
guard is weaker than the original sketch claimed: `mix compile` only re-renders
the bundle when a store module actually recompiles, so a `mix server` on a warm
`_build` leaves a stale bundle alone (and, with an emptied codegen manifest,
can rewrite it down to the prelude). The documented refresh is
`mix compile --force`, with `mix compile.musubi_rust --check` reporting drift
without writing. State that in the README rather than building CI for it.

---

## 4. Component inventory

One window, one root entity, one root store. Every component below maps to a
Musubi feature that already exists in `chat_room` — nothing new is added to the
server.

| # | Component | Musubi feature | gpui construct |
| :-- | :-- | :-- | :-- |
| 1 | `ChatWindow` root view | root store mount (join = mount) | `Entity<ChatWindow>` + `impl Render` |
| 2 | Message list | `stream_async :messages` → materialized `Vec<MessageState>` | `gpui::list` + `ListState` |
| 3 | History loading / failed states | `AsyncResult` `loading \| ok \| failed` | `match` on the generated enum |
| 4 | Composer + send | `send_message` command, reply `{queued}` | `gpui-component` `Input` + `cx.spawn` |
| 5 | Delivery receipt | `last_send_status` tagged union (`start_async`/`handle_async`) | `match` on `ChatRoomStoreLastSendStatus` |
| 6 | Identity + rename | `set_name` command, reply `{ok, name}` | `Input` + `Button` |
| 7 | Online panel | `assign_async :online_users` + PubSub | `AsyncResult` `match` + plain column |
| 8 | Connection pill | `Mounted::status_updates` (BDR-0033) over reconnect (BDR-0015) | `MountStatus` field fed by the stream |
| 9 | Attach button + progress | `upload :attachment` in channel mode, `attach` command | `Button` + `App::prompt_for_paths` + `Upload::updates` |
| 10 | Instant relaunch | SWR mount cache (`docs/rust-client.md` §6.4): `ConnectionBuilder::cache` over a durable `CacheStore` | `cache_store.rs` — one JSON file under `$HOME`, whole-map writes, corrupt-file tolerant |
| 10 | Attachment chip on a row | the consumed entry, as plain state on `MessageState` | column inside the bubble |

### 4.1 `ChatWindow`

**As landed** (the plan's sketch had a non-optional `mounted`, which the
window-first startup in §5.3 made impossible):

```rust
use generated::chat_room::stores::chat_room_store::{self as store, ChatRoomStore};

struct ChatWindow {
    url: SharedString,                      // for the "connecting to ..." line
    mount_error: Option<SharedString>,      // a rejected join is a rendered panel
    snapshot: Option<Arc<store::State>>,    // last good; never cleared on reconnect
    status: MountStatus,                    // the crate's liveness stream (BDR-0033)
    mounted: Option<Mounted<ChatRoomStore>>, // None until the join succeeds
    feedback: SharedString,
    busy: bool,                             // one command at a time
    composer: Entity<InputState>,           // gpui-component
    name_input: Entity<InputState>,
    _updates: Task<()>,                     // held: dropping cancels the subscription
    _in_flight: Option<Task<()>>,           // held: one command at a time
    _subscriptions: Vec<Subscription>,      // held: Enter-to-submit on both inputs
}
```

`mounted` is an `Option` because the window opens before the join resolves:
commands are refused until it is `Some`, and `mount_error` is what the message
pane renders instead of a list. The view keeps no derived connection enum of
its own — the pill (§4.7) reads `mount_error`/`mounted` for the pre-mount
arms and the crate's `MountStatus` for everything after.

`_updates` and `_in_flight` are held rather than `.detach()`ed so that closing
the window cancels both — dropping a gpui `Task` cancels it, and a detached
update loop would keep a `Mounted` alive past the view.

### 4.2 Message list — `gpui::list`

The stream materializes to an ordinary `Vec<MessageState>` on the snapshot
(`docs/rust-client.md` §4.6), so the list is a plain slice render:

```rust
list(self.messages.clone(), move |ix, _window, _cx| message_row(&state, ix, dimmed)).flex_1()
```

Two ordering facts carry over from the server: the store inserts with
`at: 0` and `limit: -100`, so index 0 is the **newest** message and the client
never holds more than 100. The view renders newest-first (no reversal, no
scroll-to-bottom bookkeeping), matching the browser client.

Rows are message bubbles whose bodies wrap, so there is no single height to
measure once and `uniform_list` does not apply; `gpui::list` virtualizes over
variable heights instead. The price is one piece of view state: `ListState`
caches a height per row, so the view has to call `reset(count)` whenever the
stream's length changes.

### 4.3 Async states

`messages` and `online_users` are both `AsyncResult<T>` and both render through
the same three-arm match. This is where the 1.5 s seed latency pays off: the
window opens showing "Loading history", then flips to a populated list, on
every start and every reconnect.

```rust
match &snapshot.messages {
    AsyncResult::Loading { result, .. } => /* skeleton, or stale rows if result is Some */,
    AsyncResult::Ok { result, .. }      => /* the virtualized list */,
    AsyncResult::Failed { reason, .. }  => /* "Could not load history" + reason */,
}
```

Field names are the wire names `result` / `reason`, and **every** variant
carries both (`docs/rust-client.md` §6.1) — hence the `..` on the `Ok` arm.
`reason` is `Option<AsyncError>`, where `AsyncError` is
`Structured { kind, value } | Opaque(Value)`.

The `Loading { result: Some(_) }` arm matters: on reconnect the async value goes
back to `loading` while still carrying the previous payload, and rendering the
stale rows dimmed instead of blanking the list is the behavior a native client
should model.

### 4.4 Composer, and the BDR-0009 demonstration

`send_message` returns `{queued: true}` *before* the message appears. That is
not a bug to hide, it is the contract (reply → patch → server-side effects),
and the desktop client should make it visible: the composer clears and the
feedback line shows "queued" the moment the reply resolves, and the message row
appears one envelope later.

```rust
let mounted = self.mounted.clone();
self._in_flight = Some(cx.spawn(async move |this, cx| {
    let result = mounted.command(SendMessage { body }).await;
    this.update(cx, |view, cx| {
        view.feedback = match result {
            Ok(reply) if reply.queued => "queued for async delivery".into(),
            Ok(_) => "send request returned".into(),
            Err(err) => format!("send failed: {err}").into(),
        };
        cx.notify();
    }).ok();
}));
```

No `command_and_wait_for_patch` helper is used or wanted (`docs/rust-client.md`
§6.2 rejects it as a race dressed as an API).

Text input is the one thing gpui does not provide: `crates/gpui/examples/input.rs`
is a ~400-line from-scratch single-line field. `gpui-component` supplies
`Input`/`InputState` and is the reason it is a dependency at all. The planned
fallback — a send button with a canned body and no free-text entry — was
specified in case the crates.io combination failed to compile (§7.1); **as
landed it was not needed**. The widget is `gpui_component::input::Input` over an
`Entity<InputState>`; 0.5.1 has no type called `TextInput`.

### 4.5 Delivery receipt

```rust
use store::ChatRoomStoreLastSendStatus as SendStatus;

match &snapshot.last_send_status {
    SendStatus::Idle => "idle".into(),
    SendStatus::Ok { id } => format!("ok ({id})").into(),
    SendStatus::Failed { reason } => format!("failed ({reason})").into(),
}
```

The last command reply, when there is one, takes precedence over this line —
the same `feedback || renderSendStatus(...)` the browser client uses.

The arms bind inline struct-variant fields directly, which is why
`docs/rust-codegen.md` §3.4 case 5 inlines struct variants rather than wrapping
each in a newtype.

Server-side this field is written only by `handle_async/3`, so the pill flips
after the `start_async` task settles — a second, independent patch with no
command reply attached. Together with §4.4 it renders the whole
command → reply → patch → async-completion → patch sequence in one screen.

### 4.6 Attachments — the upload data plane

Uploads are the one Musubi feature that is **not** state: `upload :attachment`
is declared outside `state do`, the framework injects an inert `UploadSlot`
marker into the render output, and the live handle is driven by a separate
`upload_ops` stream (BDR-0028). The window models that split directly — two
fields, two update loops:

```rust
upload: Option<Upload>,          // the control plane: select / start / cancel
attach: Option<UploadHandle>,    // the last value its own updates stream gave
```

`watch_upload` takes the handle the first time a snapshot names the slot
(`mounted.upload(&StoreId::root(), &state.attachment.name)`) and spawns a second
foreground loop over `Upload::updates()`. Progress therefore repaints the
composer dock *without* the message list re-rendering: an upload op marks no
`socket.assigns` key changed, so it produces an envelope with an empty `ops`
array.

The transfer itself is three awaits, in order — `select` (preflight; the server
signs one token per accepted entry), `start` (join `musubi_upload:<ref>`, push
the bytes as binary frames), then the `attach` command, which is what consumes
the finished entry server-side. The command is not optional: a completed entry
sits in the server's index until something consumes it, and
`consume_uploaded_entries/3` may only run inside a command handler. The row that
announces the file arrives afterwards on the ordinary message stream, carrying
the attachment as plain state — never out of the reply (BDR-0009).

`musubi-client` never touches a filesystem, so the embedder reads the file. The
picker is gpui 0.2.2's `App::prompt_for_paths(PathPromptOptions { files: true,
directories: false, multiple: false, prompt })`, which returns a
`oneshot::Receiver<Result<Option<Vec<PathBuf>>>>`; the bytes are then read on
`cx.background_executor()` rather than the UI thread.

**The test seam.** A native modal cannot be driven from a script, so
`ChatWindow::attach` takes an `Option<PathBuf>`: the button passes `None` and
gets the dialog, and the test passes `Some(path)` for a file it wrote itself —
everything after the path is the same code the button runs.

**What this surfaced.** Channel-mode uploads had never run over a real Phoenix
transport before this example — the wire fixtures use external mode, because a
channel-mode token is signed per run and could not survive `git diff
--exit-code`, and `Phoenix.ChannelTest.push/3` hands the channel a raw binary.
The real serializer does not: `Phoenix.Socket.V2.JSONSerializer.decode_binary/1`
tags the payload `{:binary, data}`, which matched no `handle_in/3` clause, so
every real chunk crashed its sub-channel and the entry came back as
`{op: cancel}`. `Musubi.Transport.UploadChannel` now accepts both shapes and
`test/musubi/transport/upload_channel_test.exs` covers the serializer's.

### 4.7 Connection pill

`Mounted::snapshot()` returns `None` before the initial patch and **is never
cleared afterwards** — not by a reconnect either (`crates/musubi-client/src/mounted.rs`
clears the cell only on teardown, deliberately, because BDR-0015 requires the
client to keep rendering the last good tree). The `Option` says "have I ever
loaded"; "am I current" is the crate's own status surface (BDR-0033):
`Mounted::status()` / `Mounted::status_updates()` with
`MountStatus { Connecting, Live, Reconnecting }`.

The pill renders that stream directly. A socket that drops while the app is
idle flips it to "reconnecting" with no command involved (within one heartbeat
interval when the death is silent), and the rejoin's fresh initial patch flips
it back to "live". The v1 `stale`-flag workaround — set by the first command
that failed with `NotConnected` / `Disconnected` / `Transport` — is gone; a
command that fails on a dead socket now merely coincides with a pill that has
already flipped.

**As landed** the pill is `mount_error` → offline (a rejected join is terminal
and never enters the status stream), `mounted.is_none()` → connecting (no
handle to read a status from yet), then the `MountStatus` verbatim:
`Connecting` → joining, `Live` → live, `Reconnecting` → reconnecting.

---

## 5. Executor bridging

gpui runs `smol` + `async-task` on top of its platform dispatcher and hosts no
tokio reactor. `musubi-client` is runtime-agnostic by construction with exactly
three seams (`docs/rust-client.md` §2.2). All three are satisfied here without
a second thread pool.

### 5.1 `Spawner` and `Timer`

```rust
#[derive(Clone)]
pub struct GpuiSpawner(pub gpui::BackgroundExecutor);

impl musubi_client::Spawner for GpuiSpawner {
    fn spawn(&self, fut: BoxFuture<'static, ()>) {
        self.0.spawn(fut).detach();     // the actor task owns its own lifetime
    }
}

#[derive(Clone)]
pub struct GpuiTimer(pub gpui::BackgroundExecutor);

impl musubi_client::Timer for GpuiTimer {
    fn sleep(&self, dur: Duration) -> BoxFuture<'static, ()> {
        let executor = self.0.clone();
        Box::pin(async move { executor.timer(dur).await })
    }
}
```

`BackgroundExecutor::timer(Duration) -> Task<()>` covers heartbeats (30 s),
push timeouts, and reconnect backoff. No `tokio::time`,
no `smol::Timer` of our own.

`spawn` here is the connection actor's spawn, and the actor future is
`Send + 'static` by design, so `background_spawn` semantics apply cleanly. The
`.detach()` is correct: the actor is torn down by `Connection::disconnect`, not
by dropping a `Task`.

### 5.2 `Connector` — smol-native websocket

```rust
pub struct SmolConnector;

impl musubi_client::Connector for SmolConnector {
    fn connect(&self, url: &str) -> BoxFuture<'static, Result<Box<dyn Socket>, TransportError>> {
        let url = url.to_owned();
        Box::pin(async move {
            let addr = host_port(&url)?;                       // "127.0.0.1:4002"
            let stream = async_net::TcpStream::connect(addr).await?;
            let (ws, _resp) = async_tungstenite::client_async(&url, stream).await?;
            Ok(Box::new(WsSocket(ws)) as Box<dyn Socket>)
        })
    }
}
```

`async-net` is `async-io`-backed (the smol family), so the returned future is
driven by whatever executor polls it — gpui's background executor — and needs
no runtime feature flag on `async-tungstenite` (hence `default-features = false`).
**As landed** the pin is `async-tungstenite = "0.35"` with `handshake` +
`futures-03-sink` named explicitly (§2.2), not the 0.33 zed uses: `async-net`
supplies the stream, so zed's pin was never the compatibility constraint.

The example dials plain `ws://127.0.0.1:4002/socket/websocket?vsn=2.0.0`, so
**this connector links no TLS stack** — no rustls, no native-tls and no
certificate verifier is reachable from the Musubi path, and `authority` rejects
`wss://` rather than downgrading silently. That is a claim about the transport,
not about the binary: gpui's own HTTP client pulls rustls in through
`gpui_http_client`, exactly as it pulls in tokio (§5.4). A production client
adds `async-tls`/`rustls` here and nowhere else.

`WsSocket` is a newtype implementing `Sink<Frame>` and
`Stream<Item = Result<Frame, TransportError>>` by mapping
`tungstenite::Message::Text ⇄ Frame::Text`, `Binary ⇄ Frame::Binary`, dropping
`Ping`/`Pong`/`Frame`, treating `Close` as end-of-stream, and converting
`tungstenite::Error` into `TransportError`. Roughly 60 lines of mechanical
`Pin<&mut Self>` forwarding; it is the reference adapter other gpui embedders
copy, so it lives in its own file with comments.

### 5.3 Socket thread → UI thread

Snapshot sends happen on the actor task, which is not the gpui main thread.
`Entity<T>` and `Context<T>` are `!Send`, so nothing may cross directly. The
crossing is a `Stream` consumed by a foreground `cx.spawn`, which is the exact
pattern `Context::spawn` is shaped for (it hands out a `WeakEntity<Self>` plus a
holdable `AsyncApp`):

```rust
// inside cx.new(|cx| { ... }) for ChatWindow
let mut updates = mounted.updates();
let task = cx.spawn(async move |this, cx| {
    while let Some(snapshot) = updates.next().await {
        let alive = this.update(cx, |view, cx| {
            view.snapshot = Some(snapshot);
            cx.notify();                    // the only re-render trigger in gpui
        });
        if alive.is_err() { break; }        // window closed
    }
});
// A second, identical loop consumes mounted.status_updates() into
// view.status — that stream is what drives the connection pill (§4.7).
```

Rules this encodes, all of which are load-bearing:

- Never hold `Entity<T>` in a background future; hold the `WeakEntity` +
  `AsyncApp` that `Context::spawn` provides.
- `this.update(...)` is fallible — a closed window is a normal exit, not an error.
- `cx.notify()` is what schedules a repaint. Mutating `view` without it renders
  nothing.
- The returned `Task<()>` is stored in the view (`_updates`); dropping it
  cancels the loop, which is the desired teardown when the window closes.

**As landed the order is inverted:** `main.rs` builds the `Connection` and
opens the window unconditionally, and the mount runs inside `ChatWindow::new`
as the head of the same `cx.spawn` shown above. The plan had the mount happen
first, in the `Application::run` closure, via
`cx.background_executor().block(...)` or an `AsyncApp` spawn that opens the
window on success. Window-first is strictly better and is why `mounted` is an
`Option` (§4.1): a rejected join is *always* a rendered panel, with no path on
which the app can block the main thread or exit before anything is drawn. It
mirrors `main.tsx`'s "Connect failed" panel without mirroring its top-level
`await`.

**The mount carries params.** `ChatRoom.Stores.ChatRoomStore` declares
`attr(:room_id, String.t(), required: true)` and its `mount/2` does
`Map.fetch!(params, "room_id")`. That attr is generated as a plain field on the
store's `Params` struct (`docs/rust-client.md` §7), so the required param
cannot be forgotten at the call site — omitting it no longer waits for a
server-side rejection, it fails to compile:

```rust
let mounted = connection
    .mount::<ChatRoomStore>(room_id, Params { room_id: room_id.to_owned() })  // "general"
    .await?;
```

### 5.4 The no-tokio invariant

The plan stated this as an empty `cargo tree -i tokio`. **That is not
achievable and the shipped example does not claim it**: gpui 0.2.2 depends on
`gpui_http_client → zed-reqwest → hyper → tokio`, so tokio is in the binary
whatever the Musubi side does. The invariant is about the *Musubi path*, and it
is checked as three statements:

```sh
cd examples/chat_room/desktop
cargo tree -i tokio -e normal   # every path runs through gpui_http_client
grep musubi-client-tokio Cargo.toml   # absent
```

- `musubi-client-tokio` is not a dependency — the core `musubi-client` crate is
  runtime-free by construction.
- `async-tungstenite` is `default-features = false` with only `handshake` and
  `futures-03-sink`, so its `tokio-runtime` feature is off.
- Every remaining path to tokio in `cargo tree -i tokio -e normal` goes through
  `gpui_http_client`, which nothing in this example calls.

Zed's own answer for unavoidable tokio dependencies is the unpublished
`gpui_tokio` crate, which stands up a second runtime as a gpui `Global`. This
example does not need it and should not vendor it.

---

## 6. Running it

### 6.1 Terminals

```sh
cd examples/chat_room
mix server      # deps.get + run --no-halt; Phoenix on 127.0.0.1:4002
```

```sh
cd examples/chat_room
mix desktop     # cargo run (in desktop/)
```

Optional third terminal, for the cross-client demo:

```sh
cd examples/chat_room
mix ui          # pnpm install + pnpm dev; open http://localhost:4102
```

Type in one, watch it appear in the other. Both clients see the same presence
list because both send the same channel join payload —

```json
{"module": "ChatRoom.Stores.ChatRoomStore", "id": "general", "params": {"room_id": "general"}}
```

— exactly what `ui/src/musubi.ts` sends. The `params.room_id` is **required**
(`attr(:room_id, String.t(), required: true)`; `mount/2` does
`Map.fetch!(params, "room_id")`), so a join with `params: {}` fails. Two
channels, two page servers, one PubSub topic.

The desktop client takes the server URL from `MUSUBI_URL`, defaulting to
`ws://127.0.0.1:4002/socket`. No config file.

### 6.2 Platform expectations

| Platform | Status | Requirement |
| :-- | :-- | :-- |
| macOS | **Primary.** Metal backend, first-class in gpui | Xcode plus the **Metal Toolchain**, which Xcode 26 unbundles: `xcodebuild -downloadComponent MetalToolchain` (~690 MB, no sudo), verified with `xcrun -sdk macosx metal --version`. **As landed** this, not `xcode-select`, is the blocker the first build hits — gpui's build script fails with `cannot execute tool 'metal' due to missing Metal Toolchain`, which reads like a problem with the pin |
| Linux | Best-effort, untested by the author | X11 and/or Wayland; both features are on by default in gpui 0.2.2 |
| Windows | **Out of scope for v1** | Supported on zed `main` (Win32 + DirectWrite) but the published 0.2.2 README says "macOS or Linux". Requires the git path |

gpui is explicitly pre-1.0 ("There will often be breaking changes between
versions"), so the README should say the example is pinned to
`gpui 0.2.2` and that bumping it is expected to require source changes.

### 6.3 Reconnect demo

Stop `mix server` and watch the message list stay rendered (BDR-0015: keep
last-good, no resync) while the pill flips to "reconnecting" on its own —
`Mounted::status_updates()` reports the drop the moment the client notices it,
within one heartbeat interval when the socket dies silently (§4.7, BDR-0033).
A **Send** during the window still fails with `Disconnected` on the feedback
line, coinciding with the pill rather than causing it. Restart the server,
watch the client rejoin, receive a `replace ""` at `version: 1`, flip the pill
back to "live", and run `messages` back through `loading → ok` with the 1.5 s
seed delay. That sequence is the whole recovery contract in one observable
loop and belongs in the README.

---

## 7. Ordering

Milestones in this document are labelled **D0–D8** to keep them distinct from
the `R0–R8` ladders in `docs/rust-client.md` §15 and the implementation order in
`docs/rust-codegen.md` §9. Cross-document prerequisites below are cited by their
own labels.

### 7.1 D0 — gpui spike (do this first; blocked on nothing)

Everything else in this plan assumes a gpui window can be opened and driven
from this repo's toolchain. That assumption is cheap to test and is the highest
single risk, because the crates.io combination `gpui 0.2.2` + `gpui-component
0.5.1` is self-consistent per crates.io metadata but has not been compiled by
anyone here.

Throwaway crate outside the repo, one afternoon, answering exactly four
questions:

1. Does `gpui 0.2.2` + `gpui-component 0.5.1` compile and open a window on
   macOS with the pinned toolchain? What feature flags are actually needed?
2. Does `gpui-component`'s `TextInput` work on the 0.2.2 API surface (its
   README documents the git path), or must the example hand-roll `input.rs`?
3. Do `GpuiSpawner`/`GpuiTimer`/`SmolConnector` (§5.1–5.2) compile and does an
   `async-net` + `async-tungstenite` socket driven by
   `BackgroundExecutor::spawn` actually pump frames?
4. End-to-end against the **existing** `examples/chat_room` server on 4002,
   hand-rolling the Phoenix v2 5-tuple frames and applying patches with
   `serde_json::Value` + the `json-patch` crate — i.e. no `musubi-client`, no
   generated types, just proof that the wire works from Rust and that patches
   land in a gpui view.

Step 4 is the answer to "what is buildable today": the whole client is
reachable right now with about 200 lines of untyped glue, because the server,
the channel, the envelopes, and the room already exist and are running. The
spike is not committed; it de-risks D1–D5 and produces the `transport.rs` that
ships.

**Outcomes.** All four questions came back green, with four corrections that
the sections above now carry:

| # | Question | Answer |
| :-- | :-- | :-- |
| 1 | Does `gpui 0.2.2` + `gpui-component 0.5.1` compile and open a window on macOS? What feature flags? | Yes. **Keep gpui's default features** — `gpui-component` declares a plain `gpui = "0.2.2"`, so feature unification re-enables anything `default-features = false` would drop, and the x11/wayland features are inert on macOS (§2.2). The one environment requirement the plan got wrong is the unbundled **Metal Toolchain**, not Xcode selection (§6.2) |
| 2 | Does the widget layer work on the 0.2.2 API, or must the example hand-roll `input.rs`? | It works; the canned-body fallback was not needed. The widget is `Input` over `Entity<InputState>`, not `TextInput` (§4.4). `gpui_component::init(cx)` must be the first call inside `Application::run`, and the window's first-level view must be a `Root` |
| 3 | Do the three seams compile and pump frames over `BackgroundExecutor`? | Yes, on `async-tungstenite 0.35` rather than the sketched 0.33 (§5.2). `Socket` needs no manual impl — `phoenix-channel`'s blanket impl covers any `Sink<Frame>` + `Stream<Item = Result<Frame, _>>` |
| 4 | End-to-end against the running 4002 server | Yes. It also produced the correction in §5.4: an empty `cargo tree -i tokio` is unreachable because gpui itself links tokio |

### 7.2 Dependency chain

| Step | Deliverable | Source of truth | Blocks |
| :-- | :-- | :-- | :-- |
| D0 | gpui spike (§7.1) | this doc | everything, but nothing blocks it |
| D1 | Shared manifest hoist: `Musubi.Plugin.Codegen`, `Musubi.Codegen.Manifest`, `_build/<env>/musubi-codegen/` — this is client milestone **R0** | `docs/rust-codegen.md` §1.2 | D2 |
| D2 | `Musubi.Codegen.Rust` + `Mix.Tasks.Compile.MusubiRust` + `:rust_codegen_output_path` — client milestone **R6**, codegen §9 steps 2–6 | `docs/rust-codegen.md` §2–4 | D5 |
| D3 | `musubi-client` core: seams, phoenix layer, patch engine (add/remove/replace only), stream engine — client milestones **R1–R3** | `docs/rust-client.md` §2–5 | D4 |
| D4 | `musubi-client` app surface: `AsyncResult`, `Store`/`Command`/`Event`, `mount`/`snapshot`/`updates`/`command`, reconnect — client milestones **R4–R5** | `docs/rust-client.md` §6–7, §9 | D6 |
| D5 | Write the bundle ⇄ crate seam up as a BDR (§3.2, already decided in both docs); regenerate `desktop/src/generated.rs` from the real compiler | §3.2 | D6 |
| D6 | `desktop/` crate: `transport.rs` + read-only UI (list + async states + presence) | this doc §4–5 | D7 |
| D7 | Commands: composer, rename, delivery receipt, connection pill | this doc §4.4–4.6 | D8 |
| D8 | Wiring + docs: `mix desktop` alias, `.gitignore`, chat_room README section, root README bullet | this doc §2.3, §8 | — |

D6 is reachable with only D3's patch engine plus D4's snapshot surface;
commands (D7) can lag. A read-only desktop client that renders a live chat room
is already a complete demonstration of the server-authoritative half of the
contract, and it is the right place to cut if the example needs to ship early.

Neither `musubi-client` nor `:musubi_rust` exists today, so D6 cannot start
before D2 and D4 land — with the sole exception of the D0 spike, which
deliberately routes around both.

---

## 8. Repo registration touchpoints

| File | Change |
| :-- | :-- |
| `README.md` (Examples list) | `examples/chat_room` bullet gains "with React and gpui clients" |
| `examples/chat_room/README.md` | New `## Desktop client` section: what it demonstrates, the Rust toolchain requirement, the `mix desktop` block, the `cargo tree -i tokio` invariant, the reconnect demo |
| `examples/chat_room/.gitignore` | `+ /desktop/target/` |
| `AGENTS.md` | No change required. The examples bullet (line 95) already covers this; it says nothing about front-end count or language |
| root `mix.exs` | No change. Examples are not deps, not in `package/0` `:files`, not in `precommit` |
| `.github/workflows/ci.yml` | No change. Examples are not built in CI today, and adding a gpui build (Xcode, GPU) would be a large CI change for a documentation artifact |
| root `Cargo.toml` (workspace, from `docs/rust-client.md` §1.2) | No change. The example crate detaches with its own empty `[workspace]` table |
| `pnpm-workspace.yaml` | No change. The glob is `examples/*/ui`; `examples/*/desktop` is not matched, so the Rust crate stays out of the pnpm graph and out of `pnpm -r run {test,typecheck,lint}` |

---

## 9. Non-goals

- **Uploads.** `chat_room` declares none, and `musubi-client` v1 defers the
  upload engine wholesale (`docs/rust-client.md` §10). Choosing `chat_room` is
  partly a way to avoid needing it.
- **Child stores / multi-root / routing.** Deliberately excluded by the choice
  of backend.
- **Push events (BDR-0032).** `chat_room` emits none. The client crate supports
  them; the example does not exercise them.
- **Auth.** The **socket connect** params are empty — no token, exactly as `ui/`
  does (`new Socket("/socket", {})`), and `config/dev.exs` sets
  `check_origin: false`. This says nothing about **mount** params, which are a
  different thing and are not empty: the join payload carries the required
  `room_id` attr (§5.3, §6.1).
- **Packaging.** No `.app` bundle, no code signing, no installer. `cargo run`.
- **Windows and web (`gpui_web`).** Both require the git path.
- **A shared Rust UI toolkit.** `app.rs` is one file of straight-line render
  code and `theme.rs` is a flat list of the colors `ui/src/App.css` already
  hard-codes. No component library, no design-token pipeline, no
  state-management layer on top of `Mounted`.

---

## Open questions

1. ~~**Mid-reconnect mount status.**~~ Resolved by BDR-0033: `musubi-client`
   exposes `MountStatus { Connecting, Live, Reconnecting }` via
   `Mounted::status()` / `Mounted::status_updates()` (fed by the socket
   layer's own liveness signal; `phoenix-channel` gained the connection-wide
   `PhoenixSocket::status_updates()` watch), the TS client the per-connection
   `connection.status()` / `onStatusChange()` analogue, and the pill renders
   the stream directly (§4.7). The `Unmounted` arm from the original proposal
   was dropped — teardown already surfaces as ended streams and
   `Unmounted`/`Disconnected` command errors. The client-side rendering
   obligation (keep the last-good tree while reconnecting) is stated in
   BDR-0033 and `docs/client-contract.md`.
2. ~~**Directory name.**~~ Resolved: `desktop/`. It names the artifact rather
   than the toolkit, so swapping toolkits later does not rename the directory,
   and it is not matched by the `examples/*/ui` pnpm glob. Alternatives
   considered and rejected: `ui-gpui/` (symmetric with `ui/`, but ties the name
   to a pre-1.0 dependency), `gpui/` (reads as a vendored copy of the
   framework).
3. ~~**`gpui-component` viability on the crates.io pin.**~~ Resolved by D0
   question 2: viable. `gpui-component 0.5.1` compiles and runs against
   crates.io `gpui 0.2.2`, so the canned-body fallback in §4.4 was not built.
   Its widget is `Input`/`InputState`; there is no `TextInput` on 0.5.1.
4. ~~**gpui feature flags on macOS.**~~ Resolved by D0 question 1: **keep the
   defaults**. `gpui-component` declares a plain `gpui = "0.2.2"`, so cargo
   feature unification re-enables anything `default-features = false` would
   drop; the x11/wayland features are inert on macOS. See §2.2.
5. **Toolchain pinning.** No `rust-toolchain.toml` is planned, on the theory
   that examples should build with whatever recent stable the reader has. gpui
   0.2.2's README asks for "the latest version of stable Rust", which is not a
   pinnable statement. If D0 finds a minimum that actually matters, add the file
   and record it in the README instead of the crate's `rust-version`.
6. **Native clients and `check_origin` / auth.** `config/dev.exs` sets
   `check_origin: false`, so the desktop client connects in dev without ceremony.
   `docs/client-contract.md` otherwise assumes a browser socket
   (`params: { token: window.userToken }`, cookie-bearing, origin-checked). What
   a native client is supposed to do in production — where the token comes from,
   what `check_origin` should be — is undocumented. Out of scope for this
   example; the gap should be recorded in `docs/client-contract.md`.
7. **Committing `Cargo.lock`.** Planned yes (binary crate, reproducible
   `cargo run` for readers). The cost is a large lockfile in the repo (gpui
   pulls a wide tree) and dependabot-style churn on a file nothing in CI reads.
8. ~~**`mix desktop` ergonomics.**~~ Resolved: the alias prints a one-line
   `Mix.shell().info` warning about the cold-cache gpui build and then runs
   `cargo run`. A `cargo build` + `cargo run` pair was rejected as two
   resolutions for one outcome.
9. ~~Where the reference gpui adapter lives.~~ Resolved: the only copy is this
   example's `examples/chat_room/desktop/src/transport.rs`; `docs/rust-client.md`
   §2.3 links here rather than shipping a crate-side `gpui_adapter.rs`.

Settled since the first draft, listed so the resolutions are findable: the
bundle ⇄ crate seam (§3.2 — the crate owns the shared types, the bundle
re-exports and implements), compiler/config naming (`:musubi_rust` /
`mix compile.musubi_rust` / `:rust_codegen_output_path` /
`:rust_codegen_root_module` = `"musubi"`, sibling prelude layout), and the
stream field representation (`Vec<T>`; there is no `StreamField`).
