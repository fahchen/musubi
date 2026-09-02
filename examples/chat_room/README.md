# chat_room

A real-time chat room example built as a single Musubi store. It demonstrates
async-seeded message streams (`stream_async`), Agent-backed online users via
`assign_async`, PubSub delivery, and `start_async` send handling over the
Phoenix Channel transport. Messages and presence are kept in
application-owned Elixir agents; each room stores and streams only the latest
100 messages. The mount path injects ~1.5s of artificial latency on the
history seed so the `loading → ok` `AsyncResult` transition is visible
client-side.

## Store tree

```text
ChatRoom.Stores.ChatRoomStore (root)
  attrs: room_id
  state:
    messages          AsyncResult<stream of ChatRoom.MessageState>   # stream_async
    current_user      ChatRoom.OnlineUser
    online_users      AsyncResult<list(ChatRoom.OnlineUser)>         # assign_async
    last_send_status  idle | ok | failed                             # start_async
```

## Commands

| Command | Payload | Reply | Behavior |
| :-- | :-- | :-- | :-- |
| `set_name` | `{ name: string }` | `{ ok: boolean, name: string }` | Updates the current user's display name and broadcasts the room's online-user list. |
| `send_message` | `{ body: string }` | `{ queued: boolean }` | Queues message delivery and updates `last_send_status` when the async task completes. |

## Start the example

From the repository root, in up to three terminals:

```sh
cd examples/chat_room
mix server   # deps.get + run --no-halt
```

```sh
cd examples/chat_room
mix ui       # pnpm install + pnpm dev (in ui/)
```

```sh
cd examples/chat_room
mix desktop  # cargo run (in desktop/)
```

Open http://localhost:4102 for the React client; `mix desktop` opens a native
window. Both are optional and both can run at once — see below.

## Desktop client

`desktop/` is a native [gpui](https://www.gpui.rs) client for the same server,
same port, same store, same channel. It shares no code with `ui/`: the React
client consumes `@musubi/client` plus the generated TypeScript bundle
(`ui/src/generated/musubi.d.ts`), and the desktop client consumes the
`musubi-client` Rust crate plus the generated Rust bundle
(`desktop/src/generated.rs`). Both bundles are emitted from the same store
declarations by `mix compile`.

Run `mix ui` and `mix desktop` side by side against one `mix server` and the
point of the example shows up on screen: a message typed in the browser appears
in the native window, a rename in either moves a row in both presence lists, and
neither client knows the other exists. Only the server does.

What it demonstrates, beyond what the React client already shows:

- **`AsyncResult` in a nominal type system.** `messages` and `online_users` are
  both `AsyncResult<Vec<_>>` and both render through the same three-arm match.
  The mount path's artificial ~1.5s history delay makes the `loading → ok` flip
  visible on every start and every reconnect.
- **A hoisted tagged union.** `last_send_status` is declared inline in
  `state do`, and Rust cannot express an anonymous union — so the generated
  bundle names it `ChatRoomStoreLastSendStatus` and the delivery-receipt pill
  is a `match` the compiler forces to cover all three arms.
- **Reply before patch (BDR-0009).** `send_message` replies `{queued: true}`
  *before* the row exists. The client shows the reply on the feedback line and
  lets the row arrive one envelope later; the delivery pill flips on a second,
  independent patch when the `start_async` task settles. No state is ever read
  out of a command reply.
- **A runtime-free client.** `musubi-client` has no executor of its own, so
  `desktop/src/transport.rs` supplies the four seams over gpui's executor:
  `Spawner`/`Timer` on `BackgroundExecutor`, and a `Connector`/`Socket` pair
  over `async-net` + `async-tungstenite`, and is meant to be copied verbatim
  into other gpui apps.

### Requirements

- A recent stable Rust toolchain — `mise install` at the repo root provides
  the pinned one (`rust` in `mise.toml`). The crate declares
  `rust-version = "1.85"` (edition 2024), matching `musubi-client`'s MSRV.
- **macOS with the Metal Toolchain installed.** Xcode 26 unbundles the Metal
  compiler, and gpui's build script needs it:

  ```sh
  xcodebuild -downloadComponent MetalToolchain   # ~690 MB, no sudo
  xcrun -sdk macosx metal --version              # should print a version
  ```

  Without it the first build fails inside `gpui`'s build script with
  `cannot execute tool 'metal' due to missing Metal Toolchain`, which is easy to
  misread as a problem with the pin.
- Linux is best-effort and untested here; Windows is out of scope, because it
  needs gpui's git branch rather than the crates.io release.

### Notes

- **gpui is pre-1.0.** The crate pins `gpui = "0.2.2"` and
  `gpui-component = "0.5.1"` from crates.io and is detached from the repo's
  Cargo workspace; `desktop/Cargo.toml` says why for both. Bumping either is
  expected to require source changes.
- **`desktop/src/generated.rs` is committed**, exactly like the TypeScript
  bundle, and can therefore go stale. `mix compile.musubi_rust --check` reports
  drift without writing, and `mix compile --force` regenerates it. A plain
  `mix compile` only re-renders the bundle when a store module recompiles, so
  do not rely on `mix server` to refresh it.
- **The Musubi transport uses no tokio.** Not an empty `cargo tree -i tokio`,
  though — gpui itself depends on `gpui_http_client → zed-reqwest → hyper →
  tokio`, which this example never calls. The checkable statement is that every
  path to tokio runs through gpui:

  ```sh
  cd desktop && cargo tree -i tokio -e normal   # every path goes via gpui_http_client
  ```

  `musubi-client-tokio` is not a dependency, and `async-tungstenite` is pinned
  to `handshake` + `futures-03-sink` with its runtime and TLS features off.
- **The server URL** comes from `MUSUBI_URL`, defaulting to
  `ws://127.0.0.1:4002/socket`. There is no config file. The Musubi connector
  links no TLS stack of its own and rejects `wss://` rather than silently
  downgrading; gpui's HTTP client brings rustls in independently, and nothing
  on the Musubi path touches it.

### Reconnect demo

With the desktop client running, stop `mix server`. The message list stays
rendered — BDR-0015 says a client keeps its last good tree rather than blanking
— but the pill still reads "live": nothing tells the view the socket went away
until something tries to use it. Press **Send**. The command fails with
`Disconnected`, the feedback line says so, and *then* the pill flips to
"reconnecting". Restart the server: the client rejoins, receives a fresh initial
patch at `version: 1`, and `messages` runs through `loading → ok` again with the
1.5s seed delay, so the whole recovery contract is one observable loop.

The idle-disconnect blind spot is a gap in the crate rather than in this client:
`Mounted::snapshot()` is never cleared, so there is no mount-status signal to
render. `docs/rust-gpui-example.md` open question 1 proposes a `MountStatus`.
