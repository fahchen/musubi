# Phoenix Setup

Musubi is Phoenix-first. The runtime uses Phoenix sockets and channels as the
transport, while application modules implement Musubi callbacks.

## Endpoint

Register one Musubi socket in the endpoint:

```elixir
defmodule MyAppWeb.Endpoint do
  use Phoenix.Endpoint, otp_app: :my_app

  socket "/socket", MyAppWeb.UserSocket,
    websocket: true,
    longpoll: false
end
```

If the application needs Phoenix session data in stores, pass the session
through `connect_info`:

```elixir
socket "/socket", MyAppWeb.UserSocket,
  websocket: [connect_info: [session: @session_options]],
  longpoll: false
```

The Musubi socket captures `connect_info[:session]` during connect. Every root
mounted on the same Musubi connection can read it with:

```elixir
Musubi.Socket.session(socket)
```

## Socket Module

Use `Musubi.Socket` instead of `Phoenix.Socket` in application code:

```elixir
defmodule MyAppWeb.UserSocket do
  use Musubi.Socket,
    roots: [
      MyApp.Stores.DashboardStore,
      MyApp.Stores.PollRoomStore
    ]

  @impl Musubi.Socket
  def handle_connect(params, socket) do
    case MyApp.Auth.verify(params["token"]) do
      {:ok, user} -> {:ok, Musubi.Socket.assign(socket, :current_user, user)}
      :error -> :error
    end
  end

  @impl Musubi.Socket
  def handle_join(_params, socket), do: {:ok, socket}
end
```

`use Musubi.Socket` generates the Phoenix socket adapter internally and
registers the `"musubi:*"` channel route. Do not add a separate Musubi channel
entry by hand.

## Callback Responsibilities

`handle_connect/2` runs once when Phoenix establishes the socket. It receives
transport params from the client. Use it for:

- user authentication
- long-lived assigns shared by every Musubi connection and mounted root
- rejecting the socket with `:error` or `{:error, :unauthorized}`

`handle_join/2` runs once per root channel (each root store joins its own
channel). It receives that root's mount params (`module` / `id` / `params`) and
runs just before the root's `mount/2`. Use it for:

- per-root authorization (reject the join with `:error` / `{:error, :unauthorized}`)
- workspace or tenant scope checks keyed off the mount params
- setting assigns for the root mounted on that channel

For assigns shared by *every* root, prefer `handle_connect/2` (socket-level,
runs once per socket). Root store `mount/2` then runs for the root, receiving the
same mount params:

```elixir
@impl Musubi.Store
def mount(%{"poll_id" => poll_id}, socket) do
  socket =
    socket
    |> assign(:poll_id, poll_id)
    |> assign(:current_user, socket.assigns.current_user)

  {:ok, socket}
end
```

## Params, Session, And Connect Info

Use these scopes consistently:

| Scope | Source | Lifetime | Use for |
| :-- | :-- | :-- | :-- |
| connect params | `new Socket("/socket", params: ...)` | Phoenix socket | auth tokens and transport credentials |
| connect info | Phoenix endpoint `connect_info` | Phoenix socket | session, peer data, headers configured by the endpoint |
| session | `connect_info[:session]` | Musubi socket | browser session data shared by mounted roots |
| join params | per-root channel join payload (`{ module, id, params }`, sent by `mountStore`) | one root store | seen by `handle_join/2`; identical to that root's mount params |
| mount params | `connection.mountStore({ module, id, params })` | one root store | business identity such as `poll_id` or `cart_id` |

Child stores do not receive `mount/2`. They run `init/1` and can still read
root params, session, connect info, and inherited assigns from the socket.

## Root Declaration Rules

Only stores declared in `roots: [...]` may be mounted by the client:

```elixir
use Musubi.Socket,
  roots: [
    MyApp.Stores.CartPageStore,
    MyApp.Stores.InboxStore
  ]
```

The client mounts by module string and root id. `mountStore` returns a
`{ store, unmount }` pair:

```ts
const { store: cart, unmount } = await connection.mountStore({
  module: "MyApp.Stores.CartPageStore",
  id: "cart:current-user",
  params: { cart_id: "current-user" },
})
```

Calling `mountStore` again with the same root id on one Musubi connection
attaches to the existing mounted root instead of creating a second
server-side root. Each caller receives its own `unmount` handle, but the
underlying root stays mounted until the last caller unmounts:

```ts
await unmount()
```

Child stores are server-owned and cannot be mounted or unmounted directly by
the client.

## Live Reloading on Store Changes

For dev-time auto-refresh of the generated TypeScript bundle, include your
Musubi store directory in Phoenix's live-reload patterns and point the
codegen output into your JS dev server's watched tree:

```elixir
# config/dev.exs
config :my_app, MyAppWeb.Endpoint,
  live_reload: [
    patterns: [
      ~r"lib/.*/stores/.*\\.ex$",
      # ... existing patterns
    ]
  ]

config :musubi, :ts_codegen_output_path, "ui/src/generated/musubi.d.ts"
```

The chain is: edit a store, save, `Phoenix.CodeReloader` recompiles on the
next HTTP request, `:musubi_ts` rewrites the bundle, the JS dev server (Vite
HMR / equivalent) picks up the file change.

If you change a store in IEx without a request firing, `recompile` in the
IEx prompt runs the same compiler chain and updates the bundle.
