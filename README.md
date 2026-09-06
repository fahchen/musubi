# Musubi

[![Hex.pm](https://img.shields.io/hexpm/v/musubi.svg)](https://hex.pm/packages/musubi)
[![HexDocs](https://img.shields.io/badge/hex-docs-lightgreen.svg)](https://hexdocs.pm/musubi)
[![License](https://img.shields.io/hexpm/l/musubi.svg)](https://github.com/fahchen/musubi/blob/main/LICENSE)
[![CI](https://github.com/fahchen/musubi/actions/workflows/ci.yml/badge.svg)](https://github.com/fahchen/musubi/actions/workflows/ci.yml)

Musubi is a server-authoritative runtime for Elixir/Phoenix applications. A
Phoenix socket owns one Musubi connection, and that connection can mount many
declared root stores. Each root store runs in its own page-scoped process,
renders typed state on the server, and streams RFC 6902 JSON Patch envelopes to
the client.

Musubi is useful when you want LiveView-style server authority, but your client
is a TypeScript, React, or Rust application that owns rendering.

## Current Status

Musubi is pre-1.0. The public model is intentionally narrow:

- backend modules declare stores with `use Musubi.Store`
- socket modules declare mountable roots with `use Musubi.Socket, roots: [...]`
- clients call `connect(socket)` once, then mount root stores by `{module, id}`
- child stores are created by server render output and are not mounted directly
- commands, streams, async values, uploads, push events, and patch application
  are handled by the runtime packages

Breaking changes are still possible before 1.0.

## Installation

Add Musubi to your Phoenix application:

```elixir
def deps do
  [
    {:musubi, "~> 0.13.1"}
  ]
end
```

For generated TypeScript types, add Musubi's compiler to the consumer app:

```elixir
def project do
  [
    app: :my_app,
    compilers: Mix.compilers() ++ [:musubi_ts]
  ]
end
```

Configure the generated `.d.ts` output path:

```elixir
config :musubi, :ts_codegen_output_path, "assets/src/generated/musubi.d.ts"
```

The JavaScript client packages ship inside the Musubi Hex package under
`deps/musubi/packages/`. Reference them by local path from the frontend
project's `package.json` (adjust the relative path so it points at
`deps/musubi/packages/<name>` from the JS app root):

```json
{
  "dependencies": {
    "@musubi/client": "file:../deps/musubi/packages/client",
    "@musubi/react": "file:../deps/musubi/packages/react",
    "phoenix": "file:../deps/phoenix"
  }
}
```

Then run the package manager once after `mix deps.get`:

```sh
pnpm install   # or npm install / yarn install
```

`@musubi/client` and `@musubi/react` ship TypeScript source directly; the
consumer bundler (Vite, Phoenix esbuild) transpiles on demand — no build
step required.

For a Rust client, add the Rust compiler instead of — or alongside —
`:musubi_ts`:

```elixir
def project do
  [
    app: :my_app,
    compilers: Mix.compilers() ++ [:musubi_rust]
  ]
end
```

Configure the generated `.rs` bundle path:

```elixir
config :musubi, :rust_codegen_output_path, "desktop/src/generated.rs"
```

The Rust client crates — `musubi-client`, `musubi-client-tokio`, and the
`phoenix-channel` protocol layer beneath them — ship inside the Musubi Hex
package under `deps/musubi/crates/`. Path-depend on them from the Rust
project's `Cargo.toml` (adjust the relative path so it points at
`deps/musubi/crates/<name>` from the crate root):

```toml
[dependencies]
musubi-client = { path = "../deps/musubi/crates/musubi-client" }
# tokio applications also take the tokio spawner, timer and connector:
musubi-client-tokio = { path = "../deps/musubi/crates/musubi-client-tokio" }
```

The Rust client is a peer of the TypeScript one, not a subset:
`Connection::mount::<St>` takes the store's generated `Params` struct (one
field per `attr/3`, so a required mount param cannot be forgotten at the call
site), `Mounted::state()` hands back a typed view on a **retained reactive
state tree** — one transaction per envelope, per-node subscriptions, RAII
tokens — alongside typed `command(...)` dispatch and typed `events(...)`
streams, uploads run through `Mounted::upload_at` in both channel and external
(direct-to-cloud) mode, an opt-in stale-while-revalidate cache hangs off the
builder (`ConnectionBuilder::cache`), and per-root liveness is observable via
`Mounted::status()`, whose handle carries `.value()` / `.subscribe()`.

`musubi-client` pulls in no async runtime, so a non-tokio embedder such as a
gpui desktop app skips `musubi-client-tokio` and supplies its own `Spawner`,
`Timer`, and `Connector`. See
[Rust Reactive State](docs/rust-reactive-state.md),
[Rust Client](docs/rust-client.md), [Rust Codegen](docs/rust-codegen.md), and
[Rust Codegen Example](docs/rust-codegen-example.md).

## Minimal Example

Declare a root store:

```elixir
defmodule MyAppWeb.Stores.CounterStore do
  use Musubi.Store, root: true

  state do
    field :count, integer()
  end

  command :increment do
    payload do
      field :amount, integer()
    end
  end

  @impl Musubi.Store
  def mount(params, socket) do
    {:ok, assign(socket, :count, Map.get(params, "count", 0))}
  end

  @impl Musubi.Store
  def render(socket), do: %{count: socket.assigns.count}

  @impl Musubi.Store
  def handle_command(:increment, %{"amount" => amount}, socket) do
    {:noreply, update(socket, :count, &(&1 + amount))}
  end
end
```

Expose it through a Musubi socket:

```elixir
defmodule MyAppWeb.UserSocket do
  use Musubi.Socket,
    roots: [
      MyAppWeb.Stores.CounterStore
    ]

  @impl Musubi.Socket
  def handle_connect(_params, socket), do: {:ok, socket}

  @impl Musubi.Socket
  def handle_join(_params, socket), do: {:ok, socket}
end
```

Wire the socket into your Phoenix endpoint. Your `UserSocket` (built with
`use Musubi.Socket`) is a Phoenix socket — mount it on the endpoint like
any other transport:

```elixir
defmodule MyAppWeb.Endpoint do
  use Phoenix.Endpoint, otp_app: :my_app

  socket "/socket", MyAppWeb.UserSocket,
    websocket: true,
    longpoll: false

  # ... remaining plugs
end
```

The mount path (`"/socket"`) is the same URL the TypeScript client passes
to `new Socket(...)` below.

Mount the root from TypeScript:

```ts
import { Socket } from "phoenix"
import { connect } from "@musubi/client"

const socket = new Socket("/socket", { params: { token: window.userToken } })
const connection = await connect<Musubi.Stores>(socket)

const { store: counter, unmount } = await connection.mountStore({
  module: "MyAppWeb.Stores.CounterStore",
  id: "counter",
  params: { count: 1 },
})

await counter.dispatchCommand("increment", { amount: 1 })
await unmount()
```

`Musubi.Stores` is the store registry the `:musubi_ts` compiler writes into
your `.d.ts` bundle — one entry per declared store, with its state shape,
commands and events. You name it once, on `connect`; from there the `module`
string literal drives type inference for every later `mountStore` call, so
`counter` is fully typed and `dispatchCommand` only accepts commands that
store declares. Command failures and timeouts throw a `MusubiCommandError`
(from `@musubi/client`) with `kind`, `command`, `storeId`, `reply`, and an
extracted `code`.

React consumers typically go through `createMusubi<Musubi.Stores>()`
from `@musubi/react`, which takes the registry once and returns the full hook
set — `MusubiProvider` (accepts `connection` or `socket`),
`useMusubiConnectionStatus`, `useMusubiRoot`, `useMusubiRootSuspense`,
`useMusubiSnapshot`, and `useMusubiCommand` (mutation-shaped:
`{ dispatch, isPending, error, data, reset }`). Use `keyOf(proxy)` for
stable React list keys over child proxies.

## Server-driven child refresh

To push new assigns to one specific mounted child store from the server, call
`Musubi.send_update/2,3` — aligned with `Phoenix.LiveView.send_update`:

```elixir
# Inside the root store's handle_info/2 (self() is the page process):
def handle_info({:comments_changed, file_id}, socket) do
  Musubi.send_update(["files", file_id, "comments"], %{reload_token: make_ref()})
  {:noreply, socket}
end
```

The assigns map is delivered to the addressed store's `update/2`; only that
subtree re-renders and one scoped JSON Patch envelope ships. This is the
intra-page last hop for cross-connection fan-out: broadcast over
`Phoenix.PubSub`, and each page's root `handle_info/2` calls `send_update`.
Musubi has no built-in PubSub abstraction — the application owns the broadcast.

## Documentation

- [Getting Started](guides/getting-started.md)
- [Phoenix Setup](guides/phoenix-setup.md)
- [Client and React](guides/client-and-react.md)
- [Streams](guides/streams.md)
- [Uploads](guides/uploads.md)
- [Push Events](guides/push-events.md)
- [Testing](guides/testing.md)
- [Client Contract](docs/client-contract.md)
- [Streams Reference](docs/streams.md)
- [Uploads Reference](docs/uploads.md)
- [Push Events Reference](docs/push-events.md)
- [Persistence Pattern](docs/persistence-pattern.md)
- [Rust Codegen](docs/rust-codegen.md)
- [Rust Client](docs/rust-client.md)
- [Rust Codegen Example](docs/rust-codegen-example.md)

Build local ExDoc output with:

```sh
mix deps.get
mix docs
```

## Examples

The repository includes standalone Phoenix examples under `examples/`:

- `examples/cart_page` - cart UI with nested stores and persistence hooks
  ([demo](examples/cart_page/demo.gif))
- `examples/chat_room` - PubSub-backed chat room, with React and gpui clients
  ([demo](examples/chat_room/demo.gif))
- `examples/poll_app` - multi-root polling application with streams and async
  ([demo](examples/poll_app/demo.gif))

Each example depends on Musubi with `path: "../.."`.
