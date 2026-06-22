# Getting Started

This tutorial builds a small counter root store, exposes it through a Phoenix
socket, and mounts it from a TypeScript client.

## 1. Add Musubi

In the Phoenix app:

```elixir
def deps do
  [
    {:musubi, "~> 0.9.2"}
  ]
end
```

Run:

```sh
mix deps.get
```

If the application has a TypeScript frontend, add the Musubi compiler so the
generated ambient types stay in sync with the server stores:

```elixir
def project do
  [
    app: :my_app,
    compilers: Mix.compilers() ++ [:musubi_ts]
  ]
end
```

Configure the generated type bundle:

```elixir
config :musubi, :ts_codegen_output_path, "assets/src/generated/musubi.d.ts"
```

## 2. Define A Root Store

Root stores opt in with `use Musubi.Store, root: true`. They may implement
`mount/2`, which receives client mount params before `init/1` runs.

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
  def render(socket) do
    %{count: socket.assigns.count}
  end

  @impl Musubi.Store
  def handle_command(:increment, %{"amount" => amount}, socket) do
    {:noreply, update(socket, :count, &(&1 + amount))}
  end
end
```

The `state do` block is both a runtime validation contract and the source for
TypeScript generation. `render/1` returns the Elixir-shaped state; Musubi
serializes it for the wire.

## 3. Declare Mountable Roots

A Musubi socket declares the root stores a client may mount. Application code
implements Musubi callbacks; Phoenix socket and channel behaviours are handled
by the adapter.

```elixir
defmodule MyAppWeb.UserSocket do
  use Musubi.Socket,
    roots: [
      MyAppWeb.Stores.CounterStore
    ]

  @impl Musubi.Socket
  def handle_connect(%{"token" => token}, socket) do
    with {:ok, user} <- MyApp.Auth.verify_user_token(token) do
      {:ok, Musubi.Socket.assign(socket, :current_user, user)}
    else
      _error -> :error
    end
  end

  @impl Musubi.Socket
  def handle_join(_params, socket), do: {:ok, socket}
end
```

`handle_connect/2` runs when Phoenix establishes the socket. Use it for
connection-level authentication and assigns shared by every mounted root.
`handle_join/2` runs once when the Musubi connection joins.

## 4. Wire The Phoenix Endpoint

Register the Musubi socket in the Phoenix endpoint. `MyAppWeb.UserSocket`
(built with `use Musubi.Socket`) is a Phoenix socket — mount it like any
other transport:

```elixir
defmodule MyAppWeb.Endpoint do
  use Phoenix.Endpoint, otp_app: :my_app

  socket "/socket", MyAppWeb.UserSocket,
    websocket: true,
    longpoll: false

  # ... remaining plugs
end
```

If the application needs Phoenix session data in Musubi stores, configure
`connect_info`:

```elixir
socket "/socket", MyAppWeb.UserSocket,
  websocket: [connect_info: [session: @session_options]],
  longpoll: false
```

Stores can then read it with `Musubi.Socket.session(socket)`.

## 5. Mount From TypeScript

`@musubi/client` ships inside the Musubi Hex package under
`deps/musubi/packages/client`. Reference it by local path from the
frontend project's `package.json` (adjust the relative path so it points
at `deps/musubi/packages/client` from the JS app root):

```json
{
  "dependencies": {
    "@musubi/client": "file:../deps/musubi/packages/client",
    "phoenix": "file:../deps/phoenix"
  }
}
```

Then install once after `mix deps.get`:

```sh
pnpm install   # or npm install / yarn install
```

Open one connection, then mount one or more declared root stores by module
name and id:

```ts
import { Socket } from "phoenix"
import { connect } from "@musubi/client"

const socket = new Socket("/socket", {
  params: { token: window.userToken },
})

const connection = await connect<Musubi.Stores>(socket)

const { store: counter, unmount } = await connection.mountStore({
  module: "MyAppWeb.Stores.CounterStore",
  id: "counter",
  params: { count: 1 },
})

console.log(counter.count)

await counter.dispatchCommand("increment", { amount: 1 })
await unmount()
```

`connect<R>(socket)` binds the registry once; later `mountStore` calls
infer the store type from the `module` literal alone. React consumers
typically use `createMusubi<Musubi.Stores>()` from `@musubi/react` to
get the same inference across hooks. The `id` must be unique within the
Musubi connection — a single connection can mount many root stores as
long as each root id is distinct.

## 6. Regenerate Types

When a store's `state do` or `command` declarations change, regenerate the
TypeScript bundle:

```sh
mix compile
```

In CI, check for drift:

```sh
mix compile.musubi_ts --check
```

## Wire Encoding: Atoms Become Strings

Atom-typed fields and atom literals serialize to JSON strings. The TypeScript
codegen emits matching string-literal unions; compare with strings on the
client.

| Elixir field type                            | TypeScript                            | Wire    |
| :------------------------------------------- | :------------------------------------ | :------ |
| `field :winner, :p1 \| :p2 \| :draw \| nil`  | `"p1" \| "p2" \| "draw" \| null`      | `"p1"`  |
| `field :status, atom()`                      | `string`                              | `"on"`  |

The mental model: every atom alternative becomes its string form (the
atom's name verbatim, via `Atom.to_string/1`). Elixir atoms are
lowercase by convention, so `:p1` arrives as `"p1"`; an atom like
`:HTTPError` would arrive as `"HTTPError"`. `nil` serialises to JSON
`null`. The Elixir side keeps the atom shape inside `socket.assigns`;
the conversion happens on the way out through `Musubi.Wire.to_wire/1`.

## Next Steps

- Read `Phoenix Setup` for connection/session details.
- Read `Client and React` for React hooks and root cleanup.
- Read `Testing` for the `Musubi.Testing` store-test harness.
- Read `Client Contract` for the wire and proxy model.
