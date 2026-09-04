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
  `mountStore`, `clearStoreCache`, `status` / `onStatusChange`
  (BDR-0033, see Reconnect) and `disconnect` — the connection topic is
  not part of the public surface

```ts
interface MountedStore<M, R> {
  readonly store: StoreProxy<M, R>
  readonly unmount: () => Promise<void>
  // Stale-while-revalidate state; inert for an uncached mount. See Store Cache.
  readonly fromCache: boolean
  readonly isFetching: boolean
  readonly revalidated: Promise<void>
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
recovery from the channel's own join reply — socket-level reopen handling
plays no part in recovery. The last-good snapshot keeps rendering until the
rebuilt root's initial patch (`replace ""`) atomically swaps in.

#### Connection status (BDR-0033)

The socket lifecycle is observable per connection, so an idle disconnect is
visible without a failed command:

```ts
type MusubiSocketStatus = "connecting" | "ready" | "reconnecting"

connection.status()                       // current value
const off = connection.onStatusChange(cb) // transitions; returns unsubscribe
```

Rules:

- client-local only: no wire message carries the status and the server is
  not involved; it is driven by the phoenix.js socket `onOpen` / `onError` /
  `onClose` hooks (optional on `SocketLike` — a socket without them reads as
  a constant `"ready"` after `connect()`)
- `"connecting"` until the transport first opens — failed initial attempts
  stay here, they are not a regression; `"ready"` while open;
  `"reconnecting"` after a drop until phoenix.js reopens the socket
  (per-root recovery — rejoin + fresh initial patch — rides behind it)
- terminal outcomes (join rejection, unmount, `disconnect()`) stay on their
  existing error paths; the status deliberately has no error arm
- while `"reconnecting"` the mounted stores keep serving their last-good
  snapshots (the BDR-0015 obligation restated as a status-surface contract):
  the status exists to *annotate* stale rendering, never to blank it
- the Rust client surfaces the same signal per mounted root instead:
  `Mounted::status()` / `status_updates()` with
  `MountStatus { Connecting, Live, Reconnecting }` (`docs/rust-client.md`
  §7, §9)

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

type UploadError = {
  code:
    | "too_large"
    | "too_many_files"
    | "not_accepted"
    | "chunk_timeout"
    | "chunk_too_large"
    | "external_failed"
    | "preflight_rejected"
    | "internal"
    | (string & {})
  message: string
}

type WireUploadConfig = {
  accept: string[] | "any"
  max_entries: number
  max_file_size: number
  chunk_size: number
}

type WireUploadEntry = {
  ref: string
  client_name: string
  client_size: number
  client_type: string
  progress: number
  status: "pending" | "uploading" | "success" | "error" | "cancelled"
  errors: UploadError[]
}

type UploadOp =
  | { op: "config"; upload: string; store_id: StoreId; config: WireUploadConfig }
  | {
      op: "add"
      upload: string
      store_id: StoreId
      ref: string
      entry: WireUploadEntry
    }
  | {
      op: "progress"
      upload: string
      store_id: StoreId
      ref: string
      progress: number
    }
  | { op: "complete"; upload: string; store_id: StoreId; ref: string }
  | {
      op: "error"
      upload: string
      store_id: StoreId
      ref?: string
      error: UploadError
    }
  | { op: "cancel"; upload: string; store_id: StoreId; ref: string }
  | { op: "reset"; upload: string; store_id: StoreId }

type PushEvent = {
  store_id: string[]
  name: string
  payload: unknown
}

type PatchEnvelope = {
  type: "patch"
  base_version: number
  version: number
  ops: JsonPatchOp[]
  stream_ops: StreamOp[]
  upload_ops: UploadOp[]
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
- an envelope is emitted when **any** of `ops`, `stream_ops`, `upload_ops`, or
  `events` is non-empty; idle render cycles (all four empty) emit no envelope
- reconnect creates a fresh page runtime and fresh version sequence
- each channel carries one root; the patch envelope still includes `root_id`
  (matching that channel's root) for envelope symmetry
- stream placement paths contain `WireStreamMarker` objects in `ops`
- stream contents move through `stream_ops`
- `upload_ops` carries upload transfer state (config/add/progress/complete/
  error/cancel/reset), each tagged with the owning store's `store_id`. It is
  independent of `stream_ops` and applied in array order; the transfer protocol
  around it (tokens, chunking, progress coalescing) is in `docs/uploads.md`
- `events` carries transient push events (BDR-0032), each tagged with the
  emitting store's `store_id` and dispatched once on receipt via
  `store.handleEvent(name, cb)` per `(store_id, name)`; they own no
  version/recovery state and are not replayed on reconnect. A cycle with only
  events still emits an envelope and bumps `version`

See `Musubi.Stream` for declaration, render placement, and validation
rules, and `docs/push-events.md` for push events.

## Store Cache (Stale-While-Revalidate)

Caching is opt-in **per mount**: `mountStore` with a `cache` option resolves
immediately from a valid cached entry and revalidates in the background, while
an uncached mount resolves only once the live initial patch has landed. The
cache is a rendering optimization layered on top of the wire contract above —
it never changes what the server sends or how versions advance.

```ts
type CacheOptions = {
  // Maximum entry age in ms. Default `DEFAULT_GC_MS` (300_000, 5 minutes).
  gcTime?: number
  // Backend. Defaults to a connection-scoped in-memory persister.
  persister?: MusubiCachePersister
  // Shape/deploy version. Default "". An entry written under a different
  // buster is discarded on read.
  buster?: string
  // Seed used only when no valid entry exists; written through to the
  // persister so later mounts read it back as an ordinary entry.
  initialData?: unknown
}

type MusubiCacheEntry = {
  // The wire tree with markers intact (`__musubi_store_id__`,
  // `__musubi_stream__`, `__musubi_upload__`), so seeding runs the same
  // materialization as a live patch rather than a second decoding path.
  data: unknown
  // `Date.now()` at write time, in ms.
  updatedAt: number
  buster: string
}

type MaybePromise<T> = T | Promise<T>

interface MusubiCachePersister {
  getEntry(key: string): MaybePromise<MusubiCacheEntry | undefined>
  setEntry(key: string, entry: MusubiCacheEntry): MaybePromise<void>
  removeEntry(key: string): MaybePromise<void>
  clear?(): MaybePromise<void>
  // Storage-backed adapters set this so the runtime can warn (outside
  // production) when a durable cache is used with an empty `buster`.
  readonly durable?: boolean
}
```

A cache slot is keyed by mount identity, not by the wire `root_id`:

```ts
storeCacheKey({ module, id, params }) // "<id>|<module>|<canonical(params)>"
```

`canonicalStringify` sorts object keys at every depth and drops `undefined`
members, so params field order cannot fork one store into two slots; omitted
params canonicalize to `null`. Both helpers are exported from `@musubi/client`
and `@musubi/react` derives its mount key from `storeCacheKey`, so a store
mounted through either layer maps to the same slot.

Two persisters ship with the client: `createMemoryPersister()` — the default,
connection-scoped, `durable: false`, cleared on `disconnect()` — and
`createStorageCachePersister(storage, { prefix = "musubi:cache:" })` for
`localStorage` / `sessionStorage`, which is `durable: true` and JSON-encodes
entries under the prefix. Storage faults never reach the patch path: a quota or
serialization failure is warned and dropped, a throwing or malformed read is
treated as a miss and the offending key removed.

Seeding rules:

- the root registration and the channel join happen before the cache read, so a
  slow persister delays the seed and never the revalidation
- a seed installs the cached tree and store index with `version` still `0`; the
  live initial patch is still required to carry `base_version: 0, version: 1`,
  and its whole-root `replace ""` swaps the seed out in one op
- an entry whose `buster` differs, or whose age exceeds `gcTime`, is removed on
  read and the mount is cold
- a read that suspends past the live initial patch loses: `version !== 0` means
  the server's state stays, the seed is discarded, and `fromCache` is false
- a persister that throws is warned about and degrades to a cold mount
- `initialData` is consulted only after a miss (or an evicted entry) and is
  written through to the persister, so later mounts read it back as an ordinary
  entry and it ages out under the same `gcTime`
- streams and uploads are not cached — that state rides `stream_ops` /
  `upload_ops`, not the tree — so a seeded stream materializes as `[]` and a
  seeded upload slot as an idle handle until the live envelope refills them
- `fromCache` is true only for a mount that actually rendered a seed;
  `isFetching` stays true until `revalidated` settles, and `revalidated`
  rejects with the revalidation error (e.g. unmount or disconnect mid-flight)

Commands dispatched in the stale window queue rather than fail. While a
cache-seeded root is still at version `0`, `dispatchCommand` chains onto the
in-flight initial patch and re-dispatches once the live envelope lands; if
revalidation fails, the command rejects with that same error. An uncached mount
has no such window — dispatching before the initial patch rejects with "Store is
not connected".

Writes are throttled per slot. Every accepted envelope schedules the root's tree
on a trailing throttle (`CACHE_PERSIST_THROTTLE_MS`, 1s), so a burst of
envelopes costs at most one write per interval and always persists the latest
tree. The write is fire-and-forget; a rejection is warned, never thrown into the
patch path.

Teardown flushes, then arms eviction. When a root's last consumer unmounts — or
an orphaned root's channel drops — the pending write is flushed and a gc timer
is armed for the remainder of `gcTime` measured from the entry's own
`updatedAt`, so a slot that was already half-expired is not handed a fresh
lifetime. The timer is a no-op if the slot has been re-mounted or re-registered
by the time it fires. `disconnect()` flushes every pending write, drops the
timers and the registry, and clears the runtime-owned memory persister; a
durable persister keeps its flushed entries, subject to `gcTime` on the next
read. `connection.clearStoreCache(target?)` drops one slot outright (writer,
timer, registration and stored entry) or, with no target, clears every persister
on the connection.

The Rust client mirrors this design in `docs/rust-client.md` §6.4, where
`CacheEntry { data, updated_at, buster }` and `cache_key(module, id, params)`
are this entry and this key under Rust naming. Three divergences there are
deliberate: the Rust cache is connection-wide rather than per-mount (so it has
no `initialData`), its `disconnect()` flushes without evicting (the store is the
embedder's, not the runtime's), and it emits no durable-without-`buster` warning
(a `CacheStore` does not declare durability). Key compatibility holds for
object-valued params over non-float scalars only — TypeScript canonicalizes
omitted params to `null` where Rust always renders an object, and float
rendering differs — so point both clients at one durable store only under those
terms. This document stays normative for the TypeScript behavior.

## Async Values

The wire shape mirrors `Musubi.AsyncResult` serialization:

```ts
type WireAsyncError =
  | { kind: "error"; value: unknown }
  | { kind: "exit"; value: unknown }

type WireAsyncResult<T = unknown> =
  | {
      __musubi_async__: true
      status: "loading"
      result: T | null
      reason: null
    }
  | { __musubi_async__: true; status: "ok"; result: T; reason: null }
  | {
      __musubi_async__: true
      status: "failed"
      result: T | null
      reason: WireAsyncError | unknown
    }
```

`__musubi_async__: true` is the runtime discriminator the serializer adds
(`Musubi.AsyncResult`'s `Musubi.Wire` impl): it is what the client detects an
async value by, so an ordinary map that happens to carry `status` / `result` /
`reason` keys is never mistaken for one.

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
- `__musubi_async__` is consumed by the detection predicate and dropped — it
  never appears on the public `AsyncResult<T>`
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
  the status is "ready". It covers the connect handshake only; the
  live socket-liveness signal is `connection.status()` /
  `connection.onStatusChange()` (BDR-0033, see Reconnect)
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

  interface StoreDef<Module extends string, Shape, Commands, Events = {}> {
    readonly [Type]: {
      module: Module
      shape: Shape
      commands: Commands
      events: Events
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
      },
      {
        checkout_failed: {
          payload: { message: string }
        }
      }
    >
  }
}
```

The fourth slot is the store's declared push events (BDR-0032), keyed by event
name with a `payload` shape each — the source `EventName<M, R>` /
`EventPayload<M, K, R>` (and therefore `handleEvent`) narrow against. It
defaults to `{}`, so a store that declares no `event` blocks is emitted with an
empty literal in that position and `handleEvent` accepts no name. Events carry
no `reply` counterpart: they are fire-and-forget.

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
  // `undefined` before the initial patch and when the node is absent from the
  // index mid-reconnect — guard before dereferencing.
  snapshot(): StoreSnapshot<M, R> | undefined
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
and not `unknown`. Events are per-store: `handleEvent` on a store proxy subscribes
to that store's events (dispatch keyed by `(store_id, name)`), so a child proxy
exposes its own declared events. The handler is invoked once per matching event,
after that envelope's state ops are applied; the registry lives on the root
connection and survives reconnect.

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
