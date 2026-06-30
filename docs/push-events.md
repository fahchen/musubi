# Push Events

Push events are transient, fire-and-forget signals from the server to one
connected client — a toast, a "scroll to bottom", a "playSound". They are the
analog of `Phoenix.LiveView.push_event/3` + client `handleEvent`. Unlike state
patches and streams, an event carries **no server state**: the client consumes
it once and the server keeps no record of it.

See BDR-0032 for the design rationale and BDR-0005 for why server-side pub/sub
stays application-owned.

## Declaration

Declare events in the **root store** with the `event` DSL — like `command`, but
payload-only. Events are root-scoped (no `store_id` on the wire), so declaring
one in a child store is a compile-time error.

```elixir
defmodule MyApp.Inbox do
  use Musubi.Store, root: true

  state do
    field :title, String.t()
  end

  event :ping

  event :toast do
    field :msg, String.t()
    field :level, atom()
  end
end
```

The declaration drives the generated TypeScript types: `EventName` and
`EventPayload` per store, so `handleEvent` / `useMusubiEvent` know each event's
payload shape at compile time.

It is also validated at runtime for **dev-correctness** (not security): a
`push_event` for a declared event whose payload is missing a field or has a type
mismatch raises `ArgumentError`, the same treatment a command reply gets from its
declared schema. An undeclared event name is not validated. A bad `push_event` in
a command handler surfaces synchronously from the dispatch.

## Server

Queue an event from any store callback (a command handler, `handle_info`,
`handle_async`) with `push_event`. Although events are *declared* on the root,
`push_event` may be *called* from any store socket — the events flatten into the
one root envelope.

```elixir
def handle_command(:save, _payload, socket) do
  socket
  |> assign(:saved_at, DateTime.utc_now())
  |> push_event(:toast, %{msg: "Saved", level: :info})
  |> then(&{:reply, %{ok: true}, &1})
end
```

`push_event(socket, name, payload)` returns the socket for pipe-chaining.
`name` is an atom or string; `payload` is any wire-encodable term (serialized
via `Musubi.Wire`). Events queued during one render cycle are drained and folded
into that cycle's patch envelope, so **one push** carries the diff and its
events together.

## Client

The client owns the consumption logic. Register a handler on a mounted store
proxy:

```ts
const off = store.handleEvent("toast", (payload) => {
  showToast(payload.msg)
})

// later
off() // unsubscribe
```

`handleEvent(name, handler)` returns an unsubscribe thunk. Multiple handlers per
name are allowed; an event with no registered handler is dropped. Handlers run
once per matching event, **after** that envelope's state ops are applied (so the
store reflects the new state when the handler reads it). Registrations live on
the root connection and survive reconnect.

### React

`useMusubiEvent` wraps `handleEvent` in an effect and refs the handler, so an
inline closure does not re-subscribe on every render:

```tsx
useMusubiEvent(store, "toast", (payload: { msg: string }) => {
  showToast(payload.msg)
})
```

## Semantics

- **No ack, no retry.** Delivered once with the patch envelope; the application
  never acks.
- **No replay on reconnect.** Reconnect re-mounts the root and replays *state*
  via the initial patch; past events are gone. If no client is connected when
  `push_event` runs, the event is dropped.
- **Dropped on version mismatch.** If the envelope is rejected for a version gap
  and recovery kicks in, that envelope's events are discarded with it.
- **Root-scoped.** Events carry no `store_id`; they are dispatched by `name`
  alone, regardless of which store queued them.
- **Event-only cycles still emit.** A cycle that only pushes events (no state
  change) still ships an envelope and bumps `version` — events are not subject
  to the idle-cycle skip that empty diffs are.
- **A handler only sees events fired after it registers.** Events are dispatched
  once on receipt; there is no buffer. A cold client that pushes events during
  `mount` may not have a handler registered yet when the initial envelope
  arrives, so those events can be missed — use **state** (replayed on reconnect)
  for data that must be present at mount, and `push_event` for transient signals
  the client is already listening for.
