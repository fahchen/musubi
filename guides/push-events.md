# Push Events

This guide walks a toast notification from a Phoenix `Musubi.Store` to a React
UI. Push events are transient, fire-and-forget signals from the server to one
connected client — the analog of `Phoenix.LiveView.push_event/3` + client
`handleEvent`. Unlike state and streams, an event carries no server memory: the
client consumes it once and the server keeps no record of it. For the full
reference (wire shape, validation, the `:after_serialize` transform hook) see
[Push events reference](docs/push-events.md).

## 1. Declare the event on the store

Events are declared per store with the payload-only `event` DSL (like `command`,
but no reply). Each event is dispatched on the client to that store's proxy.

```elixir
defmodule MyAppWeb.Stores.InboxStore do
  use Musubi.Store, root: true

  state do
    field :title, String.t()
  end

  command :save

  event :toast do
    field :message, String.t()
    field :level, atom()
  end

  @impl Musubi.Store
  def mount(_params, socket), do: {:ok, assign(socket, :title, "Inbox")}

  @impl Musubi.Store
  def render(socket), do: %{title: socket.assigns.title}

  @impl Musubi.Store
  def handle_command(:save, _payload, socket) do
    socket = push_event(socket, :toast, %{message: "Saved", level: :info})
    {:reply, %{ok: true}, socket}
  end
end
```

`push_event(socket, name, payload)` queues the event and returns the socket for
pipe-chaining. It can be called from any store callback (`handle_command`,
`handle_info`, `handle_async`, `mount`), on the root or any child socket — each
store's events are tagged with its own `store_id` and delivered to that store's
proxy.

## 2. Expose the store through a Musubi socket

No event-specific wiring:

```elixir
defmodule MyAppWeb.MusubiSocket do
  use Musubi.Socket, roots: [MyAppWeb.Stores.InboxStore]
end
```

## 3. Handle the event on the client

`useMusubiEvent` registers a handler for the lifetime of the component; the
payload type is inferred from the declaration.

```tsx
import { useMusubiRoot, useMusubiEvent, useMusubiCommand } from "./musubi"
import { showToast } from "./toast"

export function Inbox() {
  const store = useMusubiRoot({ module: "MyAppWeb.Stores.InboxStore", id: "inbox" })
  const { dispatch: save } = useMusubiCommand(store, "save")

  useMusubiEvent(store, "toast", (payload) => {
    showToast(payload.message, payload.level)
  })

  if (!store) return <p>Loading…</p>
  return <button onClick={() => save({})}>Save</button>
}
```

Plain TypeScript uses `store.handleEvent`, which returns an unsubscribe thunk:

```ts
const off = store.handleEvent("toast", (payload) => {
  showToast(payload.message)
})

off() // later
```

## 4. Know the semantics

- **Fire-and-forget.** No ack, no retry. The event ships once with the patch
  envelope and is dispatched after that envelope's state ops apply.
- **No replay on reconnect.** Reconnect replays *state*, not past events. A
  handler only sees events fired after it registers — so for data that must be
  present at mount, use **state**, not an event.
- **Per-store.** A handler on a store proxy only receives that store's events; the
  same name on two stores never collides.

See the [Push events reference](docs/push-events.md) for the wire shape, the
dev-time payload validation hook, and the `:after_serialize` transform stage for
auditing/redacting outbound events.
