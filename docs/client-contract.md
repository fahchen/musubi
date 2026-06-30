# Musubi client contract

This document summarizes the current client-facing Musubi contract. The
authoritative runtime behavior still lives in `spec/` and the BDR records;
this file describes how the generated TypeScript surface and client packages
fit that contract.

## Status

The settled direction is:

- clients open one Musubi connection, then mount declared roots by `{module, id}`
- one physical `Phoenix.Socket` carries many logical Musubi roots
- the server owns the store tree and sends patch envelopes
- the TypeScript client materializes the tree, streams, async values, and
  store proxies
- generated TypeScript is type-only; it does not emit runtime descriptors,
  registries, store objects, or proxy implementations
- generated marker internals are symbol-branded type information, never wire
  data

Runtime keys are deliberately stable. In particular, keep
`__musubi_store_id__` as the reserved field on rendered store nodes.

## Public Client Shape

Applications create one Phoenix socket, open one Musubi connection, and mount
declared roots through that connection. The generated `Musubi.Stores`
registry type is bound to the API exactly once — via the `R` generic on
`connect<R>(socket)`, or via `createMusubi<R>()` in `@musubi/react` —
and the `module` string literal then drives type inference for every
later `mountStore` call. `mountStore` returns a `{ store, unmount }`
pair.

```ts
const phx = new Phoenix.Socket("/socket", {
  params: { token: window.userToken },
})

const connection = await connect<Musubi.Stores>(phx)

const { store: cart, unmount } = await connection.mountStore({
  module: "MyApp.Stores.CartPageStore",
  id: "cart:page",
  params: { cart_id: "cart:page" },
})

cart.title
cart.header.title
cart.lines.map((line) => line.name)

const reply = await cart.dispatchCommand("checkout", {})

await unmount()
```

The backend socket module declares the root-store allowlist and implements only
Musubi callbacks. Phoenix socket and channel behaviours are adapter details.

```elixir
defmodule MyAppWeb.UserSocket do
  use Musubi.Socket,
    roots: [
      MyApp.Stores.CartPageStore,
      MyApp.Stores.DashboardStore
    ]

  @impl Musubi.Socket
  def handle_connect(%{"token" => token}, socket) do
    {:ok, Musubi.Socket.assign(socket, :token, token)}
  end

  @impl Musubi.Socket
  def handle_join(_params, socket), do: {:ok, socket}
end
```

Public rules:

- callers open one connection and mount roots by module name plus root id
- callers do not pass generated runtime values
- callers do not decode patches, streams, or async wire values manually
- callers may explicitly unmount a mounted root by awaiting the `unmount`
  closure returned from `mountStore`
- mounts are ref-counted: each `mountStore` caller receives its own
  `unmount` handle; the underlying root tears down only when the last
  caller unmounts
- `connection.disconnect()` returns `Promise<void>`
- child stores are exposed as nested proxies
- streams are exposed as materialized arrays
- async values are exposed as normalized `AsyncResult<T>`
- command failures and timeouts surface as `MusubiCommandError` (see
  below); the public `MusubiConnection<R>` interface exposes only
  `mountStore` and `disconnect` — the connection topic is not part of
  the public surface

```ts
interface MountedStore<M, R> {
  readonly store: StoreProxy<M, R>
  readonly unmount: () => Promise<void>
}
```

## Identity

Musubi connection identity is the Phoenix channel topic:

```ts
type Connect = {
  topic?: string
}
```

The default base topic is `"musubi:connection"`. Each mounted root gets its own
channel on `"<base>:<root_id>"` and joins it with the mount payload (join is the
mount — see Wire Contract). Auth and transport-level data should come from
Phoenix socket params/connect_info; root business params belong to `mountStore`.

Root mount identity is:

```ts
type MountStore = {
  module: string
  id: string
  params?: Record<string, unknown>
}
```

The `module` string must match a root store module declared by the backend
connection. The `id` must be explicit and unique within that connection.

Mounted store identity inside a connected tree is:

```ts
type StoreId = string[]
```

Rules:

- the root store id is `[]`
- child store ids are authored by the server
- the client echoes server-provided ids verbatim when dispatching commands
- the client never constructs or parses store ids

Every rendered store node carries:

```ts
type StoreNodeRef = {
  __musubi_store_id__: StoreId
}
```

## Wire Contract

One root store = one channel. Mounting a declared root opens a channel on topic
`"<base>:<root_id>"` and joins it; the join payload is the mount:

```ts
type JoinPayload = {
  module: string
  id: string
  params: Record<string, unknown>
}
```

`id` is the caller-supplied `MountStoreOptions.id`. The wire `root_id` is
`"<module>:<id>"`, composed identically on both sides: the client builds it to
form the topic, and the server composes the same value and returns it on the
join `:ok` reply (`{ root_id }`) for confirmation. Composing on both module and
caller id lets two roots of different modules share one caller-facing id on a
connection without colliding.

Joining **is** mounting: the server starts the root page server on join and
emits the initial patch. Leaving the channel **is** unmounting: a client
`leave()` (or a transport drop) stops that root via the channel's `terminate/2`.
There are no separate `mount` / `unmount` messages.

### Reconnect

Phoenix owns reconnect: after a drop it automatically re-joins each channel,
which re-runs the server join and rebuilds that one root. The client drives
recovery from the channel's own join reply — no socket-level reopen handling.
The last-good snapshot keeps rendering until the rebuilt root's initial patch
(`replace ""`) atomically swaps in.

### Duplicate `(module, id)`

A second `mountStore` of the same `(module, id)` is a multi-observer scenario,
handled entirely client-side: it aliases the existing `RootConnection` (bumps a
local `refCount`, returns the same `StoreProxy`), without opening a second
channel or any server round-trip. The last release (refCount → 0) schedules a
brief grace timer before leaving the channel; a remount within the window
cancels it and reuses the mount — covers React 19 route-swap commit batching and
StrictMode effect replay.

Commands target a store within the channel's root by `store_id` (one root per
channel, so no `root_id` on the wire):

```ts
type CommandMessage = {
  store_id: StoreId
  name: string
  payload: Record<string, unknown>
}
```

Patch pushes use JSON Patch for ordinary state and `stream_ops` for stream
materialization:

```ts
type JsonPatchOp =
  | { op: "add"; path: string; value: unknown }
  | { op: "remove"; path: string }
  | { op: "replace"; path: string; value: unknown }

type StreamOp =
  | { op: "reset"; stream: string; ref: string; store_id: StoreId }
  | {
      op: "insert"
      stream: string
      ref: string
      store_id: StoreId
      item_key: string
      at: number
      item: unknown
      limit: number | null
    }
  | {
      op: "delete"
      stream: string
      ref: string
      store_id: StoreId
      item_key: string
    }

type PushEvent = {
  name: string
  payload: unknown
}

type PatchEnvelope = {
  type: "patch"
  base_version: number
  version: number
  ops: JsonPatchOp[]
  stream_ops: StreamOp[]
  events: PushEvent[]
}

type WireStreamMarker = {
  __musubi_stream__: string
}

type ConnectionPatchEnvelope = PatchEnvelope & {
  root_id: string
}
```

Envelope rules:

- the initial envelope carries `base_version: 0` and `version: 1`
- each later envelope must apply to the client's current version
- idle render cycles emit no envelope
- reconnect creates a fresh page runtime and fresh version sequence
- each channel carries one root; the patch envelope still includes `root_id`
  (matching that channel's root) for envelope symmetry
- stream placement paths contain `WireStreamMarker` objects in `ops`
- stream contents move through `stream_ops`
- `events` carries transient push events (BDR-0032), dispatched once on receipt
  via `store.handleEvent(name, cb)`; they own no version/recovery state and are
  not replayed on reconnect. A cycle with only events still emits an envelope and
  bumps `version`

See `Musubi.Stream` for declaration, render placement, and validation
rules, and `docs/push-events.md` for push events.

## Async Values

The wire shape mirrors `Musubi.AsyncResult` serialization:

```ts
type WireAsyncError =
  | { kind: "error"; value: unknown }
  | { kind: "exit"; value: unknown }

type WireAsyncResult<T = unknown> =
  | { status: "loading"; result: T | null; reason: null }
  | { status: "ok"; result: T; reason: null }
  | { status: "failed"; result: T | null; reason: WireAsyncError | unknown }
```

The public client normalizes this to:

```ts
type AsyncError =
  | { kind: "error"; value: unknown }
  | { kind: "exit"; value: unknown }

type AsyncResult<T> =
  | { status: "loading"; data: T | null; error: null }
  | { status: "ok"; data: T; error: null }
  | { status: "failed"; data: T | null; error: AsyncError | unknown }
```

Normalization rules:

- `result` becomes `data`
- `reason` becomes `error`
- `AsyncResult.of(T)` projects to `AsyncResult<T>`
- `AsyncResult.of(stream(T))` projects to `AsyncResult<T[]>`; on the wire the
  async `result` is the stream marker, and item content still arrives through
  `stream_ops`

## Command Errors

`dispatchCommand` and the React `useMusubiCommand` dispatcher both
surface failures and timeouts as a single class exported from
`@musubi/client`:

```ts
class MusubiCommandError extends Error {
  readonly name: "MusubiCommandError"
  readonly kind: "failed" | "timeout"
  readonly command: string
  readonly storeId: readonly string[]
  readonly reply: unknown
  readonly code: string | undefined

  static is(value: unknown): value is MusubiCommandError
}
```

Rules:

- `kind: "failed"` carries the raw server `reply`; `kind: "timeout"`
  has `reply: undefined`
- `code` is extracted from the reply by checking string `code`, `error`,
  then `reason` fields (in that order)
- `cause` is preserved via `Error.cause` when supplied
- `MusubiCommandError.is(value)` is name-based and cross-module safe

## Stable List Keys

`keyOf(proxy)` (exported from `@musubi/client`, re-exported from
`@musubi/react`) returns a stable string identity for a store proxy
derived from its `store_id` path. It is the supported way to key React
lists of child proxies; callers must not synthesize keys from
`__musubi_store_id__` directly.

## React Surface

`@musubi/react` exposes `createMusubi<R>()`, which closes over the
registry once and returns the full hook set bound to `R`:

```ts
interface MusubiFactory<R> {
  connect: (socket: SocketLike, options?: ConnectOptions) => Promise<MusubiConnection<R>>
  MusubiProvider: FC<MusubiProviderProps<R>>
  useMusubiConnection: () => MusubiConnection<R>
  useMusubiConnectionStatus: () => MusubiConnectionStatus<R>
  useMusubiRoot: <M>(options: UseMusubiRootOptions<M, R>) => MusubiRootMount<M, R>
  useMusubiRootSuspense: <M>(options: UseMusubiRootOptions<M, R>) => StoreProxy<M, R>
  useMusubiSnapshot: { /* selector + optional equalityFn (defaults to shallowEqual) */ }
  useMusubiCommand: <M, K>(proxy: StoreProxy<M, R>, name: K) => MusubiCommandResult<M, K, R>
}

type MusubiConnectionStatus<R> =
  | { state: "connecting"; connection: null }
  | { state: "ready"; connection: MusubiConnection<R> }
  | { state: "error"; connection: null; error: Error }

type MusubiProviderProps<R> =
  | { connection: MusubiConnection<R>; socket?: never; children: ReactNode }
  | { socket: SocketLike; topic?: string; connection?: never; children: ReactNode }

interface MusubiCommandResult<M, K, R> {
  dispatch: (payload: CommandPayload<M, K, R>) => Promise<CommandReply<M, K, R>>
  isPending: boolean
  error: MusubiCommandError | null
  data: CommandReply<M, K, R> | null
  reset: () => void
}
```

Rules:

- `MusubiProvider` accepts either `connection` or `socket`, never both;
  with `socket`, the provider owns the connect/disconnect lifecycle
- `useMusubiConnectionStatus()` is the only safe hook inside the
  "connecting" / "error" states; `useMusubiConnection()` throws unless
  the status is "ready"
- `useMusubiRoot` and `useMusubiRootSuspense` share one ref-counted
  root-mount cache keyed by `{module, id, canonical(params)}`; params
  are stringified with sorted keys so literal-equal params share mounts
- `useMusubiRootSuspense` throws an in-flight Promise for `<Suspense>`
  and a cached Error for the nearest error boundary
- `useMusubiSnapshot` defaults `equalityFn` to `shallowEqual` when a
  selector is supplied; pass an explicit `equalityFn` to override
- `useMusubiCommand` sequences concurrent `dispatch` calls with a
  monotonic request token: only the latest call's outcome lands in
  `data` / `error`; `reset()` clears both
- a `{:noreply, socket}` handler resolves with an empty object `{}`, not
  `null` (see `dispatchCommand` below). After such a command `data` is
  `{}` (truthy) — never use `data` as a "did it finish?" flag; rely on
  `isPending` / `error`, or inspect `reply` fields explicitly

## Generated TypeScript

`mix compile.musubi_ts` emits an ambient `.d.ts` bundle. It owns the generated
`Musubi.Stores` interface and the marker types used by `@musubi/client`.

```ts
declare namespace Musubi {
  type StoreId = string[]

  const Type: unique symbol

  interface StoreDef<Module extends string, Shape, Commands> {
    readonly [Type]: {
      module: Module
      shape: Shape
      commands: Commands
    }
  }

  type StoreField<Module extends string> = {
    readonly [Type]: { kind: "store"; module: Module }
  }

  type StreamField<Item> = {
    readonly [Type]: { kind: "stream"; item: Item }
  }

  type AsyncField<Value> = {
    readonly [Type]: { kind: "async"; value: Value }
  }

  interface Stores {
    "MyApp.Stores.CartPageStore": StoreDef<
      "MyApp.Stores.CartPageStore",
      {
        title: string
        header: StoreField<"MyApp.Stores.HeaderStore">
        lines: StreamField<MyApp.CartLine>
        profile: AsyncField<MyApp.Profile>
      },
      {
        checkout: {
          payload: {}
          reply: { order_id: string } | { error: string }
        }
      }
    >
  }
}
```

Marker rules:

- markers are type-only
- marker properties never appear on the wire
- the runtime never reads marker properties
- symbol branding prevents ordinary user objects from matching Musubi marker
  types by accident

## Client Projection

The client package derives public proxy and snapshot types from a
caller-supplied registry type `R`. User-facing helpers take the module
key first and require `R` as the second generic. The registry itself is
bound once for the connection by `connect<R>(socket)` or
`createMusubi<R>()`, not threaded through every call. Consumers pass
their generated `Musubi.Stores` type (or any store-map type) directly —
there is no intermediate `Registry` symbol.

```ts
type StoreModule<R> = Extract<keyof R, string>
type DefOf<M extends StoreModule<R>, R> = R[M & keyof R]

type StoreSnapshot<M extends StoreModule<R>, R> = {
  readonly __musubi_store_id__: StoreId
} & {
  [K in keyof ShapeOf<M, R>]: SnapshotValue<ShapeOf<M, R>[K], R>
}

interface StoreRuntime<M extends StoreModule<R>, R> {
  readonly __musubi_store_id__: StoreId
  dispatchCommand<K extends CommandName<M, R>>(
    name: K,
    payload: CommandPayload<M, K, R>
  ): Promise<CommandReply<M, K, R>>
  subscribe(listener: () => void): () => void
  handleEvent<K extends EventName<M, R>>(
    name: K,
    handler: (payload: EventPayload<M, K, R>) => void
  ): () => void
  snapshot(): StoreSnapshot<M, R>
}

type StoreProxy<M extends StoreModule<R>, R> =
  StoreRuntime<M, R> & {
    [K in keyof ShapeOf<M, R>]: ProxyValue<ShapeOf<M, R>[K], R>
  }
```

`SnapshotValue<T, R>` and `ProxyValue<T, R>` keep `T` first because `T`
is a projected wire type, not a module key.

`dispatchCommand` resolves with the server's command reply. A
`{:reply, payload, socket}` handler resolves with `payload`; a
`{:noreply, socket}` handler resolves with an empty object `{}` (the
server emits an empty `:ok` reply — see `docs/PRD.md`). State updates
arrive out-of-band on the `patch` channel event regardless of reply
shape, so a `{:noreply, socket}` command still patches the UI.

The reply `payload` must be a map on the server (guarded by `is_map/1`);
returning a bare list, string, or other non-map raises `ArgumentError`
in the page runtime. Wrap scalars/lists in a map (e.g.
`{:reply, %{items: list}, socket}`), which the client receives as an
object.

`handleEvent` subscribes to a transient push event (BDR-0032). `EventName<M, R>`
and `EventPayload<M, K, R>` mirror `CommandName` / `CommandPayload`, derived from
the store's declared `events` (the `StoreDef` `Events` slot). When `name` is a
literal the payload narrows to that event's exact declared shape — not a union
and not `unknown`. Events are root-scoped, so `EventName` is `never` on a child
proxy (subscribe off the root proxy). The handler is invoked once per matching
event, after that envelope's state ops are applied; the registry lives on the
root connection and survives reconnect.

Reserved runtime member names on every store proxy:

- `__musubi_store_id__`
- `dispatchCommand`
- `subscribe`
- `handleEvent`
- `snapshot`

## Runtime Materialization

For each connected root, the TypeScript runtime maintains:

- the latest accepted version
- the patched wire tree
- a `store_id -> node` index
- a `(store_id, stream_name) -> materialized_list` table
- a `store_id -> proxy` cache

Property resolution on a proxy follows the live wire shape:

1. reserved runtime members return runtime implementations
2. wire values carrying `__musubi_store_id__` return cached nested proxies
3. wire values carrying `__musubi_stream__` return materialized arrays
4. async values return normalized `AsyncResult<T>`
5. async streams return normalized `AsyncResult<T[]>`
6. plain objects recurse through the same resolution rules
7. plain fields return their wire value

Generated marker types only drive TypeScript inference. Runtime behavior is
driven by the wire tree, stream tables, and proxy cache.

## Separation Of Concerns

Server/codegen owns:

- the declared store shape
- command payload and reply types
- type-only markers for store, stream, and async fields
- the generated `Musubi.Stores` registry

Client runtime owns:

- opening Phoenix Channel connections
- applying patch envelopes
- materializing streams
- normalizing async wire values
- constructing and caching proxies
- dispatching commands with server-provided `store_id` values
