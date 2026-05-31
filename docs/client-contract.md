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

The default topic is `"musubi:connection"`. The client sends an empty channel join
payload for the connection. Auth and transport-level data should come from
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

Mounting a declared root sends:

```ts
type MountMessage = {
  module: string
  id: string
  params: Record<string, unknown>
}
```

The `id` field is the caller-supplied `MountStoreOptions.id`. The server
composes the canonical wire root id as `"<module>:<caller-id>"` and
returns it on the `:ok` mount reply under the `root_id` key. Every
subsequent `unmount` / `command` / upload / `patch` message uses that
server-assigned `root_id`. The client never composes the wire id itself
— it round-trips whatever the server returned.

Composing on both module and caller id is what lets two roots of
different modules share the same caller-facing id on one connection
without colliding in the server's `mounted_roots` map. Duplicate
detection is the server's responsibility.

### Aliasing on duplicate `(module, id)`

A second mount of the same `(module, id)` on one connection is a
multi-observer scenario, not an error. The server replies with:

```json
{ "reason": "already_mounted", "root_id": "<existing root id>" }
```

…on the `:error` channel reply. The client looks the returned `root_id`
up in its local `connectionState.roots` map and:

- **Hit**: bumps the existing `RootConnection`'s local `refCount`,
  cancels any pending grace-timer teardown, discards the tentative
  connection it built before the mount push, and returns the canonical
  one. The aliased caller shares the same `StoreProxy`. The server
  never starts a second page runtime.
- **Miss**: throws `MusubiInconsistencyError`. The server claims a
  mount that the client has no record of (reconnect race, dropped
  unmount, server-side leak); fail loud rather than fabricate state.

The last `unmount` (refCount → 0) schedules a brief grace timer instead
of pushing immediately. A remount of the same `(module, id)` within the
grace window cancels the timer and reuses the existing mount — covers
React 19 route-swap commit batching and StrictMode effect replay.

Commands target mounted stores by `root_id` plus `store_id`:

```ts
type CommandMessage = {
  root_id: string
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

type PatchEnvelope = {
  type: "patch"
  base_version: number
  version: number
  ops: JsonPatchOp[]
  stream_ops: StreamOp[]
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
- in connection transport, every patch envelope includes `root_id`; the client
  applies it only to the matching mounted root runtime
- stream placement paths contain `WireStreamMarker` objects in `ops`
- stream contents move through `stream_ops`

See `Musubi.Stream` for declaration, render placement, and validation
rules.

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
  snapshot(): StoreSnapshot<M, R>
}

type StoreProxy<M extends StoreModule<R>, R> =
  StoreRuntime<M, R> & {
    [K in keyof ShapeOf<M, R>]: ProxyValue<ShapeOf<M, R>[K], R>
  }
```

`SnapshotValue<T, R>` and `ProxyValue<T, R>` keep `T` first because `T`
is a projected wire type, not a module key.

Reserved runtime member names on every store proxy:

- `__musubi_store_id__`
- `dispatchCommand`
- `subscribe`
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
