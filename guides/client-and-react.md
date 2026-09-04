# Client And React

The TypeScript packages mirror the backend connection model:

- `@musubi/client` owns Phoenix channel interaction, patch application, streams,
  async result normalization, command dispatch, and store proxies.
- `@musubi/react` provides React context and hooks over an `MusubiConnection`.

## Plain TypeScript

Create one Phoenix socket, then call `connect<Musubi.Stores>(socket)` once.
The generic argument binds the generated registry for the connection;
every later `mountStore` call infers the store type from the `module`
string literal alone.

```ts
import { Socket } from "phoenix"
import { connect } from "@musubi/client"

const socket = new Socket("/socket", {
  params: { token: window.userToken },
})

const connection = await connect<Musubi.Stores>(socket)
```

Mount a declared root store. `mountStore` returns a
`{ store, unmount }` pair:

```ts
const { store: dashboard, unmount } = await connection.mountStore({
  module: "MyApp.Stores.DashboardStore",
  id: "dashboard",
})
```

Read state through the proxy:

```ts
dashboard.header.title
dashboard.polls.map((poll) => poll.title)
```

Dispatch a declared command:

```ts
await dashboard.dispatchCommand("refresh", {})
```

Unmount when the root is no longer needed by calling the `unmount`
closure returned from `mountStore`:

```ts
await unmount()
```

## React: createMusubi

`@musubi/react` exports a `createMusubi<R>()` factory. Call it exactly
once per app, alongside the Phoenix socket. The factory closes over the
generated registry `R` and returns a `connect`, `MusubiProvider`, and
hook set whose closures all know `R` without any further generic
threading:

```ts
// src/musubi.ts
import { Socket } from "phoenix"
import { createMusubi } from "@musubi/react"

export const socket = new Socket("/socket", {
  params: { token: window.userToken },
})

export const {
  connect,
  MusubiProvider,
  useMusubiConnection,
  useMusubiConnectionStatus,
  useMusubiRoot,
  useMusubiRootSuspense,
  useMusubiSnapshot,
  useMusubiCommand,
  useMusubiEvent,
} = createMusubi<Musubi.Stores>()
```

`MusubiProvider` accepts either a pre-opened `connection` or a raw
`socket`. The two props are mutually exclusive (enforced at the type
level and at runtime).

Variant A — open the connection at app boot, pass `connection`:

```tsx
// src/main.tsx
import { createRoot } from "react-dom/client"
import App from "./App"
import { connect, MusubiProvider, socket } from "./musubi"

const root = createRoot(document.getElementById("root")!)
const connection = await connect(socket)

root.render(
  <MusubiProvider connection={connection}>
    <App />
  </MusubiProvider>,
)
```

Variant B — pass `socket`; the provider opens the connection itself and
exposes its lifecycle through `useMusubiConnectionStatus()`:

```tsx
import { MusubiProvider, socket } from "./musubi"

root.render(
  <MusubiProvider socket={socket}>
    <App />
  </MusubiProvider>,
)
```

### Observe Connection Status

`useMusubiConnectionStatus()` returns
`{ state: "connecting" | "ready" | "error", connection, error? }`. Use
it to render a fallback while the socket-prop provider is connecting, or
to surface a connect failure:

```tsx
import { useMusubiConnectionStatus } from "./musubi"

function AppShell({ children }: { children: React.ReactNode }) {
  const status = useMusubiConnectionStatus()

  if (status.state === "connecting") return <Spinner />
  if (status.state === "error") return <p>Connect failed: {status.error.message}</p>
  return <>{children}</>
}
```

`useMusubiConnection()` returns the live `MusubiConnection<R>` once
ready and throws if called before ready — use `useMusubiConnectionStatus()`
when the calling component must tolerate the pre-ready states.

## Mount Roots In React

Use the factory's `useMusubiRoot` to mount a declared root under the
nearest provider. The `module` literal alone drives inference:

```tsx
import { useMusubiRoot } from "./musubi"

export function Dashboard() {
  const root = useMusubiRoot({
    module: "MyApp.Stores.DashboardStore",
    id: "dashboard",
  })

  if (root.status === "loading") {
    return null
  }

  if (root.status === "error") {
    return <p>{root.error.message}</p>
  }

  return <DashboardContent store={root.store} />
}
```

`useMusubiRoot` unmounts the root when the component unmounts by default. Pass
`unmountOnCleanup: false` when a mounted root should outlive the component.

Mount cache keying is canonical: `params` are stringified with sorted
keys, so `{ a: 1, b: 2 }` and `{ b: 2, a: 1 }` resolve to the same
mounted root. Two components requesting the same `{module, id, params}`
share one server-side mount and ref-count its lifetime.

### Suspense Variant

`useMusubiRootSuspense(options)` returns the `StoreProxy` directly. It
throws the in-flight Promise for `<Suspense>` and a cached Error for
the nearest error boundary, and shares the same root-mount cache as
`useMusubiRoot` (Suspense-safe orphan cleanup on abort):

```tsx
import { Suspense } from "react"
import { ErrorBoundary } from "react-error-boundary"
import { useMusubiRootSuspense } from "./musubi"

function Dashboard() {
  const store = useMusubiRootSuspense({
    module: "MyApp.Stores.DashboardStore",
    id: "dashboard",
  })
  return <DashboardContent store={store} />
}

export function DashboardPage() {
  return (
    <ErrorBoundary fallback={<p>Failed to load dashboard</p>}>
      <Suspense fallback={<Spinner />}>
        <Dashboard />
      </Suspense>
    </ErrorBoundary>
  )
}
```

## Stale-While-Revalidate Cache

Mounting always needs a server round-trip to establish the live channel
subscription, so a re-mount of a store you held moments ago (a route swap)
flashes `loading` again. The opt-in cache removes that flash: when a valid
entry for a store's identity exists, the mount **resolves immediately with the
last-known state** (`fromCache: true`) while the live mount revalidates in the
background and swaps in the server's initial patch when it lands.

Because the channel is always live, there is no TanStack-style `staleTime` — a
cached mount *always* revalidates; the cache only controls what is shown in the
meantime.

### Enable it per mount

Pass `cache` to `mountStore` (or `useMusubiRoot`). An empty object uses the
defaults: a connection-scoped in-memory backend and a 5-minute `gcTime`.

```ts
const { store, fromCache, isFetching, revalidated } = await connection.mountStore({
  module: "MyApp.Stores.DashboardStore",
  id: "dashboard",
  cache: {},
})

// `revalidated` resolves when the live initial patch replaces the seed
// (resolves immediately for a cold mount with no cached entry).
await revalidated
```

Options:

```ts
cache: {
  gcTime: 300_000,        // eviction TTL measured from the last accepted patch
  buster: "v3",           // data-shape version; a mismatch discards the entry
  persister: myPersister, // storage backend (default: connection in-memory Map)
  initialData: seedState, // seed the first mount when no entry exists
}
```

### Persist across reloads

The default backend is in-memory and cleared on `disconnect`. For state that
survives a page reload, use the Web Storage adapter. Always set `buster` for a
durable backend so a deploy that changes the store's data shape discards stale
entries (omitting it logs a dev warning).

```ts
import { createStorageCachePersister } from "@musubi/client"

const persister = createStorageCachePersister(localStorage) // or sessionStorage

await connection.mountStore({
  module: "MyApp.Stores.DashboardStore",
  id: "dashboard",
  cache: { persister, buster: "v3" },
})
```

Cache I/O is best-effort: a throwing persister degrades to a cold mount rather
than failing the mount, and malformed stored entries are discarded on read.

### In React

`useMusubiRoot` forwards `cache` and adds two signals plus `keepPreviousData`:

```tsx
const root = useMusubiRoot({
  module: "MyApp.Stores.DashboardStore",
  id: "dashboard",
  cache: { persister, buster: "v3" },
  keepPreviousData: true, // keep the prior store visible across an id/params
})                        // change instead of flashing loading

if (root.status === "loading") return <Spinner />
if (root.status === "error") return <p>{root.error.message}</p>

return (
  <DashboardContent
    store={root.store}
    revalidating={root.isFetching}      // showing cached data, refresh in flight
    staleError={root.revalidationError} // refresh failed; stale store still shown
  />
)
```

`isFetching` is `true` while a cached (or kept-previous) store is shown pending
revalidation, then settles to `false`. On a revalidation failure the stale
store stays visible and the error surfaces on `revalidationError` rather than
blanking the UI. The Suspense variant is unchanged — SWR does not map onto
throwing a promise.

### Commands during the stale window

A command dispatched while a cache-seeded mount is still revalidating (before
the live initial patch lands) is **queued** behind the initial patch instead of
rejecting "Store is not connected"; it dispatches once the store is live, or
rejects if revalidation fails.

### Clear the cache

```ts
await connection.clearStoreCache({ module, id, params }) // one entry
await connection.clearStoreCache()                       // every entry
```

## Subscribe To Snapshots

`useMusubiSnapshot` subscribes to proxy updates and returns an immutable
snapshot:

```tsx
import type { StoreProxy } from "@musubi/react"
import { keyOf } from "@musubi/react"
import { useMusubiCommand, useMusubiSnapshot } from "./musubi"

type Store<M extends keyof Musubi.Stores & string> = StoreProxy<M, Musubi.Stores>

function DashboardContent({ store }: { store: Store<"MyApp.Stores.DashboardStore"> }) {
  const title = useMusubiSnapshot(store, (snapshot) => snapshot.header.title)
  const { dispatch: refresh, isPending, error } = useMusubiCommand(store, "refresh")

  return (
    <>
      <button disabled={isPending} onClick={() => void refresh({})}>
        {title}
      </button>
      {error && <p role="alert">{error.code ?? error.message}</p>}
    </>
  )
}
```

Use selectors to keep React renders focused. A component that selects
`snapshot.header.title` does not need to re-render for unrelated store fields.
When a selector is supplied, `useMusubiSnapshot` defaults `equalityFn`
to `shallowEqual`, so selectors that return an object/tuple of fields
do not cause spurious re-renders when their elements are referentially
equal. Pass an explicit `equalityFn` to override.

### Commands And Structured Errors

`useMusubiCommand(proxy, name)` returns a mutation-shaped result:

```ts
interface MusubiCommandResult<M, K, R> {
  dispatch: (payload) => Promise<Reply>
  isPending: boolean
  error: MusubiCommandError | null
  data: Reply | null
  reset: () => void
}
```

Concurrent `dispatch` calls are sequenced by a monotonic request token —
only the latest call's outcome lands in `data` / `error`. Call `reset()`
to clear `data` / `error` and return to the idle state.

Both `dispatch` rejection and the hook's `error` field carry a
`MusubiCommandError` (re-exported from `@musubi/client`):

```ts
import { MusubiCommandError } from "@musubi/client"

class MusubiCommandError extends Error {
  kind: "failed" | "timeout"     // server reply error vs. dispatch timeout
  command: string                 // command name
  storeId: readonly string[]      // target store path
  reply: unknown                  // raw server reply (failed kind only)
  code: string | undefined        // extracted from reply.code/error/reason
}
```

`MusubiCommandError.is(value)` is a cross-module-safe type guard (uses
`name` rather than `instanceof`, so it works across bundle boundaries).
Use `error.code` for routing to user-visible copy; fall back to
`error.message` for unstructured cases. Calling `dispatchCommand` on a
store proxy directly throws the same class.

```tsx
const { dispatch, error } = useMusubiCommand(cart, "checkout")

async function onSubmit() {
  try {
    await dispatch({})
  } catch (e) {
    if (MusubiCommandError.is(e) && e.kind === "timeout") {
      toast("Took too long — try again")
    }
  }
}
```

## Push Events

The server can push transient, fire-and-forget events to the client (a toast, a
"scroll to bottom") with `push_event(socket, name, payload)` from a store
callback. Unlike state, an event carries no server memory and is dispatched once
on receipt — see `docs/push-events.md`.

Plain TypeScript — register a handler on a mounted store proxy:

```ts
const off = dashboard.handleEvent("toast", (payload) => {
  showToast(payload.msg)
})

off() // unsubscribe
```

React — `useMusubiEvent` wraps `handleEvent` in an effect and refs the handler,
so an inline closure does not re-subscribe each render:

```tsx
useMusubiEvent(store, "toast", (payload: { msg: string }) => {
  showToast(payload.msg)
})
```

A handler only receives events fired after it registers; for data that must be
present at mount, use state, not an event.

## Target Child Stores

Every store in the tree — not just the root — can declare commands and accept
them through its own proxy. Children show up as nested proxies on the root,
reachable by field access for single children and by array index for list
children. `useMusubiCommand` works on any proxy.

### Declaring a command on a child store

The Elixir DSL is identical for root and child stores:

```elixir
defmodule MyApp.Stores.CartLineStore do
  use Musubi.Store

  attr(:line, map(), required: true)
  attr(:on_qty_change, (String.t(), integer() -> :ok), required: true)

  state do
    field(:qty, integer())
  end

  command :inc_qty do
    reply do
      field :qty, integer()
    end
  end

  @impl Musubi.Store
  def handle_command(:inc_qty, _payload, socket) do
    next_qty = socket.assigns.qty + 1
    socket = assign(socket, :qty, next_qty)
    :ok = socket.assigns.on_qty_change.(socket.assigns.id, next_qty)
    {:reply, %{"qty" => next_qty}, socket}
  end
end
```

### Reaching the child proxy from React

Field access returns a single child proxy; iterating a list field returns one
proxy per element. Each carries its own `dispatchCommand`:

```tsx
function HeaderRename({ root }: { root: Store<"MyApp.Stores.CartPageStore"> }) {
  const { dispatch: setName } = useMusubiCommand(root.header, "rename")
  return <button onClick={() => void setName({ name: "Ada" })}>Rename</button>
}

function CartLine({ lineProxy }: { lineProxy: Store<"MyApp.Stores.CartLineStore"> }) {
  const line = useMusubiSnapshot(lineProxy)
  const { dispatch: inc } = useMusubiCommand(lineProxy, "inc_qty")
  return <button onClick={() => void inc({})}>{line.qty}</button>
}

function CartLines({ root }: { root: Store<"MyApp.Stores.CartPageStore"> }) {
  return (
    <ul>
      {root.cart.lines.map((lineProxy) => (
        <CartLine key={keyOf(lineProxy)} lineProxy={lineProxy} />
      ))}
    </ul>
  )
}
```

`keyOf(proxy)` returns a stable string derived from the proxy's
`store_id` path. Use it as a React list `key`; do not read
`__musubi_store_id__` directly. `keyOf` is exported from `@musubi/client`
and re-exported by `@musubi/react`.

The wire payload carries the full `store_id` path, so the page server routes
`inc_qty` directly to the addressed `CartLineStore` instance. Authorization
and audit hooks attached to ancestor stores fire as part of that chain.

### Notify the parent from a child command

Children don't write to a parent's `socket.assigns` directly. The conventional
pattern is a function-valued attr (BDR-0010) — the parent supplies a closure
in its `render/1`, and the child invokes it after mutating its own state:

```elixir
# Parent
def render(socket) do
  %{
    lines:
      for line <- socket.assigns.lines do
        child(CartLineStore,
          id: line.id,
          line: line,
          on_qty_change: socket.assigns.on_qty_change   # stable closure built in mount/1
        )
      end
  }
end
```

What the closure does is up to the parent — write through a shared
`Persistence` snapshot whose PubSub broadcast re-flows the new state through
the root, `send(self(), {:msg, ...})` so the root's `handle_info/2` picks it
up, or hit any other application boundary. Building the closure once in
`mount/1` (rather than per-render) keeps the function reference stable across
re-renders so child memoization via `__changed__` (BDR-0013) still applies.

See `examples/cart_page` for a runnable demo of the full child-command +
callback-attr round-trip.

## Async Results And Streams

The client normalizes `Musubi.AsyncResult` values to:

```ts
type AsyncResult<T> =
  | { status: "loading"; data: T | null; error: null }
  | { status: "ok"; data: T; error: null }
  | { status: "failed"; data: T | null; error: unknown }
```

Streams are materialized as arrays. The server sends stream ops; the client
owns list materialization and limit trimming.
