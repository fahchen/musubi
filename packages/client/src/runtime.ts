import {
  createMemoryPersister,
  createThrottledWriter,
  DEFAULT_GC_MS,
  storeCacheKey,
  type CacheOptions,
  type MusubiCacheEntry,
  type MusubiCachePersister,
  type ThrottledWriter
} from "./cache"
import { MusubiCommandError } from "./error"
import { applyPatch, parsePointer } from "./patch"
import {
  applyStreamOps,
  hasStreamKeyForStore,
  pruneStreams,
  touchedStoreKeys
} from "./streams"
import {
  applyUploadOps,
  pruneUploads,
  touchedStoresFromUploadOps,
  type UploadHandleImpl
} from "./uploads"
import type {
  ConnectionPatchEnvelope,
  ExternalUploader,
  JsonPatchOp,
  PatchEnvelope,
  StoreId,
  StreamEntry,
  StreamOp,
  UploadOp
} from "./types"
import { STORE_ID_KEY, storeIdKey } from "./types"

type PushStatus = "ok" | "error" | "timeout"

export interface PushLike {
  receive(status: PushStatus, callback: (payload: unknown) => void): PushLike
}

export interface ChannelLike {
  on(event: string, callback: (payload: unknown) => void): unknown
  onClose(callback: (reason: unknown) => void): unknown
  onError(callback: (reason: unknown) => void): unknown
  join(): PushLike
  push(event: string, payload: unknown): PushLike
  leave(): unknown
}

export interface SocketLike {
  connect(): unknown
  channel(topic: string, payload?: object): ChannelLike
}

type PendingConnect = {
  generation: number
  resolve: () => void
  reject: (error: Error) => void
}

export interface ConnectionListener {
  storeKey: string
  fn: () => void
}

export interface RootConnection {
  readonly module: string
  // Caller-supplied `MountStoreOptions.id` — sent in the mount payload so
  // the server can compose the canonical root id.
  readonly callerId: string
  readonly connection: ConnectionState
  readonly mountParams: Record<string, unknown>

  // Server-assigned wire root_id. Empty until the mount reply arrives;
  // once set, doubles as the key in `ConnectionState.roots`.
  id: string

  // Local consumer count. Each `mountConnectionRoot` call that resolves to
  // this `RootConnection` (fresh mount OR alias on `:already_mounted`)
  // increments; each `unmountConnectionRoot` call decrements. When it hits
  // zero, a brief grace timer fires the server `unmount` push; a re-mount
  // within the grace window cancels the timer.
  refCount: number
  graceTimer: ReturnType<typeof setTimeout> | null

  // Promise that resolves on the initial patch envelope (or rejects on
  // mount failure / channel close). External `mountConnectionRoot` callers
  // and the React adapter both `await` this. Cached on the connection so
  // aliasing callers (`:already_mounted` reply) can await the same
  // promise instead of racing.
  initialPatchPromise: Promise<void> | null

  // Mutable runtime state — read by the proxy on every property access.
  channel: ChannelLike | undefined
  channelGeneration: number
  root: unknown
  version: number
  storeIndex: Map<string, unknown>
  streams: Map<string, readonly StreamEntry<unknown>[]>
  uploads: Map<string, UploadHandleImpl>
  proxyCache: Map<string, unknown>
  snapshotCache: Map<string, unknown>
  storeListeners: Map<string, Set<() => void>>
  pendingCommandRejectors: Set<(reason: Error) => void>
  pendingConnect: PendingConnect | null
  connectPromise: Promise<void> | null
  recovering: boolean

  // Set while `mountConnectionRoot`'s inner `pushMount` await is in
  // flight. Disconnect calls this to settle the awaited mount reply
  // promise immediately with `{ kind: "error" }` so the mount caller
  // doesn't have to wait for Phoenix's push timeout. Cleared after
  // `pushMount` settles via any path.
  cancelMountPush: ((reason: string) => void) | null

  // Set while `unmountConnectionRoot` is awaiting the grace-timer
  // settlement. A timer cancellation (alias-on-remount, disconnect)
  // calls this to resolve the awaiting caller — the consumer's
  // "release my handle" intent is honored even if the internal
  // server-side teardown was skipped.
  pendingUnmountResolver: (() => void) | null

  // Cache slot key for this mount, or null when caching is disabled. When set,
  // accepted patches are persisted (throttled) and the entry is GC'd after the
  // grace teardown. Drives the stale-window command queue in
  // `dispatchConnectionCommand`.
  cacheKey: string | null
}

/**
 * Thrown when the server reports `:already_mounted` for a `root_id` the
 * client has no local record of — i.e. server and client `mounted_roots` /
 * `roots` are out of sync. Indicates a real bug (reconnect race, dropped
 * unmount, server-side state leak); should not be silently swallowed.
 */
export class MusubiInconsistencyError extends Error {
  readonly rootId: string

  constructor(rootId: string) {
    super(
      `Server reports root_id=${rootId} already mounted but the client has no matching RootConnection`
    )
    this.name = "MusubiInconsistencyError"
    this.rootId = rootId
  }
}

// Grace window (ms) between the last `unmountConnectionRoot` and the server
// unmount push. A re-mount of the same `(module, id)` within this window
// cancels the cleanup and reuses the existing mount — covers React 19
// route-swap commit batching and StrictMode effect replay where one
// component's unmount cleanup fires in the same task as the next
// component's mount.
const UNMOUNT_GRACE_MS = 0

export interface ConnectionState {
  readonly socket: SocketLike
  readonly topic: string
  readonly roots: Map<string, RootConnection>
  readonly uploaders: Record<string, ExternalUploader>

  // In-flight `mountConnectionRoot` tentatives that haven't yet been
  // inserted into `roots`. Tracked here so disconnect / channel close
  // can cancel them — otherwise their awaiting callers would hang
  // forever because there's no `roots` entry to find them through.
  readonly pendingMounts: Set<RootConnection>

  // Default cache backend for mounts that enable caching without supplying a
  // persister. Connection-scoped and ephemeral — cleared on disconnect.
  readonly memoryPersister: MusubiCachePersister
  // Per-cache-key trailing-throttle writers, shared across re-mounts of the
  // same key so bursts collapse into one write.
  readonly cacheWriters: Map<string, ThrottledWriter>
  // Per-cache-key record of which persister + gc/buster a key was mounted
  // with, so teardown GC and `clearStoreCache` act on the right backend.
  readonly cacheRegistry: Map<
    string,
    { persister: MusubiCachePersister; gcMs: number; buster: string }
  >
  // Pending post-teardown eviction timers, keyed by cache key. The torn-down
  // root is already gone from `roots`, so disconnect can't reach these through
  // it — tracked here so disconnect can cancel them instead of leaking a
  // gcTime-long closure over connection state.
  readonly cacheEvictionTimers: Map<string, ReturnType<typeof setTimeout>>

  channel: ChannelLike | undefined
  channelGeneration: number
  connectPromise: Promise<void> | null
  suppressDisconnectEvent: boolean
}

export interface SharedRuntime {
  readonly socket: SocketLike
  readonly connections: Map<string, ConnectionState>
}

const RUNTIMES: WeakMap<SocketLike, SharedRuntime> = new WeakMap()
const DEFAULT_CONNECTION_TOPIC = "musubi:connection"

export function getSharedRuntime(socket: SocketLike): SharedRuntime {
  const existing = RUNTIMES.get(socket)

  if (existing) {
    return existing
  }

  const runtime: SharedRuntime = { socket, connections: new Map() }
  RUNTIMES.set(socket, runtime)
  return runtime
}

export interface OpenConnectionOptions {
  topic?: string
  uploaders?: Record<string, ExternalUploader>
}

export interface MountConnectionRootOptions {
  module: string
  id: string
  params?: Record<string, unknown>
  cache?: CacheOptions
}

export function openConnectionState(
  socket: SocketLike,
  options: OpenConnectionOptions = {}
): { connection: ConnectionState; ready: Promise<void> } {
  const runtime = getSharedRuntime(socket)
  const topic = options.topic ?? DEFAULT_CONNECTION_TOPIC
  const existing = runtime.connections.get(topic)

  if (existing) {
    return { connection: existing, ready: ensureConnectionReady(existing) }
  }

  const connection: ConnectionState = {
    socket,
    topic,
    roots: new Map(),
    pendingMounts: new Set(),
    memoryPersister: createMemoryPersister(),
    cacheWriters: new Map(),
    cacheRegistry: new Map(),
    cacheEvictionTimers: new Map(),
    uploaders: options.uploaders ?? {},
    channel: undefined,
    channelGeneration: 0,
    connectPromise: null,
    suppressDisconnectEvent: false
  }

  runtime.connections.set(topic, connection)

  const ready = connectConnectionChannel(connection)

  return { connection, ready }
}

/**
 * Mount (or alias to an existing mount of) a root on `connectionState`.
 *
 * The server is the sole authority on `(module, id)` identity:
 *   * Fresh mount → server replies `:ok` with a canonical `root_id`. The
 *     newly built `RootConnection` is inserted into `connectionState.roots`
 *     under that id and returned.
 *   * Duplicate `(module, id)` on the same connection → server replies
 *     `:error` with `reason: "already_mounted"` and the existing `root_id`.
 *     The client looks the id up in its own `roots` Map:
 *       - Hit (expected): bump the existing entry's `refCount`, cancel any
 *         pending grace-timer cleanup, discard the tentative connection,
 *         and return the canonical one. This is the multi-observer case.
 *       - Miss: throw `MusubiInconsistencyError`. Server and client are
 *         out of sync (reconnect race, dropped unmount, server-side leak).
 *
 * Caller treats the returned `RootConnection` as the canonical reference;
 * the tentative connection built before the mount push is discarded on
 * the alias path.
 */
export interface MountRootResult {
  connection: RootConnection
  // True when the returned connection was seeded from cache and is still
  // revalidating against the server (its `version` is 0 until the live initial
  // patch lands).
  fromCache: boolean
  // Resolves when the live initial patch has been applied (the seed was
  // replaced by fresh server state), or immediately for a cold mount. Rejects
  // if revalidation fails (disconnect / unmount / malformed initial patch).
  revalidated: Promise<void>
}

export async function mountConnectionRoot(
  connectionState: ConnectionState,
  options: MountConnectionRootOptions
): Promise<MountRootResult> {
  const tentative = newRootConnection(connectionState, options)
  connectionState.pendingMounts.add(tentative)
  const cacheCfg = resolveCacheConfig(connectionState, options)

  try {
    await ensureConnectionReady(connectionState)
    if (!connectionState.channel) {
      throw new Error("Connection is not connected")
    }

    const generation = connectionState.channelGeneration
    tentative.channel = connectionState.channel
    tentative.channelGeneration = generation

    // Prepare the initial-patch waiter on the tentative BEFORE pushing —
    // if the server replies `:ok` quickly and the initial patch arrives
    // in the same task, we need `pendingConnect` already wired up.
    const tentativeInitialPatch = registerInitialPatchWaiter(tentative, generation)

    // Phoenix delivers reply frames in push order and processes them
    // serially in the JS event loop, so reply / patch interleaving is
    // not a concern: by the time the patch frame is handled, the mount
    // reply's microtask continuation has already settled and inserted
    // the connection into `connectionState.roots`. On channel close
    // mid-mount, Phoenix's push errors out via the default push timeout
    // — the awaiter unblocks then.
    const reply = await pushMount(connectionState.channel, tentative, {
      module: tentative.module,
      id: tentative.callerId,
      params: tentative.mountParams
    })

    if (reply.kind === "ok") {
      tentative.id = reply.rootId
      connectionState.roots.set(reply.rootId, tentative)
      tentative.initialPatchPromise = tentativeInitialPatch

      if (cacheCfg) {
        tentative.cacheKey = cacheCfg.key
        connectionState.cacheRegistry.set(cacheCfg.key, {
          persister: cacheCfg.persister,
          gcMs: cacheCfg.gcMs,
          buster: cacheCfg.buster
        })

        // Stale-while-revalidate: if a valid cache entry exists, render it now
        // and let `tentativeInitialPatch` swap in fresh state in the
        // background. The seed keeps `version === 0`, so the live initial
        // patch (a whole-root `replace ""`) fully replaces it.
        if (await trySeedFromCache(tentative, cacheCfg)) {
          return { connection: tentative, fromCache: true, revalidated: tentativeInitialPatch }
        }
      }

      try {
        await tentativeInitialPatch
      } catch (error) {
        connectionState.roots.delete(reply.rootId)
        tentative.channel = undefined
        throw error
      }

      return { connection: tentative, fromCache: false, revalidated: Promise.resolve() }
    }

    // Both `already_mounted` and `error` discard the tentative — its waiter
    // is no longer wired to anything the channel will resolve.
    tentative.pendingConnect = null
    tentative.channel = undefined

    if (reply.kind === "error") {
      throw new Error(`Root mount failed: ${reply.reason}`)
    }

    const existing = connectionState.roots.get(reply.rootId)
    if (!existing) {
      throw new MusubiInconsistencyError(reply.rootId)
    }

    warnOnParamsMismatch(existing, options)

    existing.refCount += 1
    cancelGraceTimer(existing)

    if (existing.initialPatchPromise && existing.version === 0) {
      // Aliasing caller arrived before the initial patch settled.
      try {
        await existing.initialPatchPromise
      } catch (error) {
        existing.refCount -= 1
        throw error
      }
    }

    // Alias onto a live root — no cache seeding; it is already connected.
    return { connection: existing, fromCache: false, revalidated: Promise.resolve() }
  } finally {
    connectionState.pendingMounts.delete(tentative)
    // If a prior `unmountConnectionRoot`'s grace timer fired while this
    // mount was pending and skipped teardown to wait for our alias, but
    // we ended up not aliasing (errored out, or `:ok` produced a fresh
    // root_id), the orphaned entry never gets torn down. Re-arm.
    rearmOrphanedRootTeardowns(connectionState, tentative.module, tentative.callerId)
  }
}

function newRootConnection(
  connectionState: ConnectionState,
  options: MountConnectionRootOptions
): RootConnection {
  return {
    module: options.module,
    callerId: options.id,
    id: "",
    connection: connectionState,
    mountParams: options.params ?? {},
    refCount: 1,
    graceTimer: null,
    initialPatchPromise: null,
    cancelMountPush: null,
    pendingUnmountResolver: null,
    channel: undefined,
    channelGeneration: 0,
    root: undefined,
    version: 0,
    storeIndex: new Map(),
    streams: new Map(),
    uploads: new Map(),
    proxyCache: new Map(),
    snapshotCache: new Map(),
    storeListeners: new Map(),
    pendingCommandRejectors: new Set(),
    pendingConnect: null,
    connectPromise: null,
    recovering: false,
    cacheKey: null
  }
}

interface ResolvedCacheConfig {
  key: string
  persister: MusubiCachePersister
  gcMs: number
  buster: string
  initialData: unknown
}

function resolveCacheConfig(
  connectionState: ConnectionState,
  options: MountConnectionRootOptions
): ResolvedCacheConfig | null {
  const cache = options.cache
  if (!cache) {
    return null
  }

  const persister = cache.persister ?? connectionState.memoryPersister
  const buster = cache.buster ?? ""
  const gcMs = cache.gcTime ?? DEFAULT_GC_MS

  if (persister.durable && buster === "" && !isProductionEnv()) {
    // eslint-disable-next-line no-console
    console.warn(
      `[musubi] mountStore({module: "${options.module}", id: "${options.id}"}) enabled a ` +
        `durable cache persister without a \`buster\`. Cached state will survive deploys ` +
        `even if the store's data shape changes — set \`cache.buster\` to your build/schema ` +
        `version so stale shapes are discarded.`
    )
  }

  return {
    key: storeCacheKey({
      module: options.module,
      id: options.id,
      ...(options.params !== undefined ? { params: options.params } : {})
    }),
    persister,
    gcMs,
    buster,
    initialData: cache.initialData
  }
}

// Seed the tentative connection's root from cache for stale-while-revalidate.
// Returns true when a valid entry (or `initialData`) was applied. Entries are
// invalidated on `buster` mismatch or age beyond `gcTime`. Cache I/O is
// best-effort: a throwing/rejecting custom persister degrades to a cold mount
// rather than aborting `mountConnectionRoot` and leaking a half-mounted root.
async function trySeedFromCache(
  tentative: RootConnection,
  cfg: ResolvedCacheConfig
): Promise<boolean> {
  const now = Date.now()
  let entry: MusubiCacheEntry | undefined
  try {
    entry = await cfg.persister.getEntry(cfg.key)

    if (entry && (entry.buster !== cfg.buster || now - entry.updatedAt > cfg.gcMs)) {
      await cfg.persister.removeEntry(cfg.key)
      entry = undefined
    }

    if (!entry && cfg.initialData !== undefined) {
      entry = { data: cfg.initialData, updatedAt: now, buster: cfg.buster }
      await cfg.persister.setEntry(cfg.key, entry)
    }
  } catch (error) {
    // eslint-disable-next-line no-console
    console.warn("[musubi] cache read failed; falling back to a cold mount:", error)
    return false
  }

  if (!entry) {
    return false
  }

  // An async persister can suspend long enough for the live initial patch to
  // land first (version → 1). Don't clobber fresh server state with the stale
  // seed; fall back to the cold-mount path so `fromCache` reports false.
  if (tentative.version !== 0) {
    return false
  }

  tentative.root = entry.data
  tentative.storeIndex = buildStoreIndex(entry.data)
  return true
}

function registerInitialPatchWaiter(
  connection: RootConnection,
  generation: number
): Promise<void> {
  const promise = new Promise<void>((resolve, reject) => {
    connection.pendingConnect = { generation, resolve, reject }
  })
  // Pre-attach a no-op `.catch` on the original promise so a rejection
  // arriving before anyone explicitly awaits it (e.g. disconnect firing
  // mid-mount, before `pushMount` has returned and `mountConnectionRoot`
  // reaches the `await tentativeInitialPatch` line) doesn't surface as
  // an unhandled rejection. The original promise stays in its rejected
  // state; subsequent `await` callers still observe the rejection.
  promise.catch(() => undefined)
  return promise
}

function cancelGraceTimer(connection: RootConnection): void {
  if (connection.graceTimer !== null) {
    clearTimeout(connection.graceTimer)
    connection.graceTimer = null
  }
  // Settle the awaiting `unmount()` caller — the consumer released
  // their handle; whether the server teardown actually ran is internal.
  const resolver = connection.pendingUnmountResolver
  if (resolver) {
    connection.pendingUnmountResolver = null
    resolver()
  }
}

function warnOnParamsMismatch(
  existing: RootConnection,
  options: MountConnectionRootOptions
): void {
  if (isProductionEnv()) {
    return
  }

  const next = options.params ?? {}
  if (sameParams(existing.mountParams, next)) {
    return
  }

  // eslint-disable-next-line no-console
  console.warn(
    `[musubi] mountConnectionRoot({module: "${options.module}", id: "${options.id}"}) ` +
      `aliased to an existing root with different params. First-mount params ` +
      `are authoritative; later params are ignored. Use a distinct id if you ` +
      `need separate instances.`
  )
}

function isProductionEnv(): boolean {
  // Reach `process.env.NODE_ENV` via `globalThis` so the check works in
  // Node and in tree-shaken browser bundles without requiring an ambient
  // `process` declaration (browser-only consumer apps don't include
  // `@types/node` and would otherwise see a "Cannot find name 'process'"
  // typecheck error).
  const env = (globalThis as { process?: { env?: { NODE_ENV?: string } } }).process?.env
    ?.NODE_ENV
  return env === "production"
}

function sameParams(a: Record<string, unknown>, b: Record<string, unknown>): boolean {
  const aKeys = Object.keys(a)
  if (aKeys.length !== Object.keys(b).length) return false
  for (const k of aKeys) {
    if (!Object.prototype.hasOwnProperty.call(b, k)) return false
    if (!Object.is(a[k], b[k])) return false
  }
  return true
}

/**
 * Drop a caller's hold on a `RootConnection`. When `refCount` hits zero a
 * grace timer schedules the server `unmount` push; a re-mount of the same
 * `(module, id)` within the grace window cancels the cleanup and reuses
 * the existing mount.
 */
export async function unmountConnectionRoot(connection: RootConnection): Promise<void> {
  if (connection.refCount <= 0) {
    return
  }

  connection.refCount -= 1
  if (connection.refCount > 0) {
    return
  }

  // Last caller went away. Defer the actual server unmount so a near-future
  // re-mount (route swap, StrictMode replay) can grab the existing entry
  // before we tear it down.
  cancelGraceTimer(connection)
  await scheduleRootTeardown(connection)
}

// Schedule a grace-window teardown for a `refCount === 0` root. Used both by
// `unmountConnectionRoot` (the normal release path) and by
// `mountConnectionRoot`'s post-error cleanup, which re-arms the timer for
// roots that were left orphaned when a previous timer firing skipped
// teardown to wait for a pending alias that subsequently failed.
function scheduleRootTeardown(connection: RootConnection): Promise<void> {
  // Defensive guard: every current caller checks `refCount === 0` first,
  // but anyone adding a future call site shouldn't be able to accidentally
  // schedule teardown for a held root.
  if (connection.refCount > 0) {
    return Promise.resolve()
  }

  const connectionState = connection.connection
  const rootId = connection.id

  return new Promise<void>((resolve) => {
    // Track the resolver on the connection so external paths
    // (`cancelGraceTimer` on alias-remount, disconnect handlers) can
    // settle this awaiter — otherwise the caller's `await unmount()`
    // hangs forever if the timer is cancelled before it fires.
    connection.pendingUnmountResolver = () => {
      connection.pendingUnmountResolver = null
      resolve()
    }

    connection.graceTimer = setTimeout(() => {
      connection.graceTimer = null

      if (connection.refCount > 0) {
        // A new caller showed up; cleanup cancelled. The consumer's
        // "release my handle" intent is honored regardless of whether
        // the server-side teardown ran.
        connection.pendingUnmountResolver = null
        resolve()
        return
      }

      if (!connectionState.roots.has(rootId)) {
        // Disconnect / external cleanup already removed us.
        connection.pendingUnmountResolver = null
        resolve()
        return
      }

      // Race guard: a concurrent `mountConnectionRoot` for the same
      // `(module, callerId)` may already be awaiting its mount push.
      // When the server replies `:already_mounted`, the alias path
      // looks `rootId` up in `connectionState.roots` — if we tear down
      // now, the alias finds nothing and throws
      // `MusubiInconsistencyError`. Skip teardown if such a pending
      // mount exists; the alias path will bump `refCount` (and cancel
      // any grace timer) shortly. If the pending mount instead settles
      // with `:error`, its `finally` re-arms us via
      // `scheduleRootTeardown`.
      const pendingAliasing = [...connectionState.pendingMounts].some(
        (p) =>
          p !== connection &&
          p.module === connection.module &&
          p.callerId === connection.callerId
      )
      if (pendingAliasing) {
        connection.pendingUnmountResolver = null
        resolve()
        return
      }

      const unmounted = new Error("Unmounted")
      connection.pendingConnect?.reject(unmounted)
      connection.pendingConnect = null
      rejectPendingCommands(connection, unmounted)
      resetConnectionState(connection)
      connection.channel = undefined
      connection.initialPatchPromise = null
      connectionState.roots.delete(rootId)
      scheduleCacheEviction(connection)

      if (!connectionState.channel) {
        connection.pendingUnmountResolver = null
        resolve()
        return
      }

      // Swallow server-side unmount errors (`unknown root`, push
      // timeout from a closing channel, etc.) — local state is
      // already torn down, and the consumer's intent was "release my
      // handle". Bubbling a server-cleanup failure to the caller is
      // surprising and not actionable. Log so the failure isn't
      // silent in dev / observability.
      receivePush(
        connectionState.channel.push("unmount", { root_id: rootId }) as PushLike,
        "Root unmount"
      )
        .catch((error: unknown) => {
          // eslint-disable-next-line no-console
          console.warn("[musubi] root unmount push failed:", error)
        })
        .finally(() => {
          connection.pendingUnmountResolver = null
          resolve()
        })
    }, UNMOUNT_GRACE_MS)
  })
}

// Re-arm teardown for any root left orphaned (refCount=0, no grace timer)
// after `mountConnectionRoot` settles non-aliasing. Specifically: if a
// previous unmount's timer fired and skipped teardown because this mount
// was pending, and this mount then settled `:error` (or `:ok` with a
// different rootId), the prior root never gets torn down without our
// re-arming.
function rearmOrphanedRootTeardowns(
  connectionState: ConnectionState,
  module: string,
  callerId: string
): void {
  for (const root of connectionState.roots.values()) {
    if (
      root.module === module &&
      root.callerId === callerId &&
      root.refCount === 0 &&
      root.graceTimer === null &&
      root.pendingUnmountResolver === null
    ) {
      scheduleRootTeardown(root).catch(() => undefined)
    }
  }
}

export async function disconnectConnectionState(
  connectionState: ConnectionState
): Promise<void> {
  const disconnectError = new Error("Disconnected")

  // Settle in-flight mounts immediately. Cancelling the mount push
  // (via `cancelMountPush`) makes the outer `await pushMount` resolve
  // with `{ kind: "error" }`, which takes the `:error` branch and
  // rejects the mount caller — no Phoenix push-timeout wait. Rejecting
  // `pendingConnect` covers the `:ok` race where the push happens to
  // settle just before disconnect, leaving the `:ok` branch's
  // `await tentativeInitialPatch` to surface the error. The
  // `registerInitialPatchWaiter` shield prevents the rejection from
  // surfacing as `unhandledRejection` if nobody is awaiting yet.
  for (const pending of connectionState.pendingMounts) {
    pending.cancelMountPush?.("Disconnected")
    pending.pendingConnect?.reject(disconnectError)
    pending.pendingConnect = null
  }
  connectionState.pendingMounts.clear()

  for (const root of connectionState.roots.values()) {
    // Cancel any in-flight recovery-remount push so the awaiting
    // `remountExistingConnection` settles immediately instead of
    // waiting for Phoenix's push timeout.
    root.cancelMountPush?.("Disconnected")
    cancelGraceTimer(root)
    root.pendingConnect?.reject(disconnectError)
    root.pendingConnect = null
    root.initialPatchPromise = null
    rejectPendingCommands(root, disconnectError)
    resetConnectionState(root)
    root.channel = undefined
  }

  // Clear `channel` BEFORE calling `leave()` so a synchronous throw
  // from the leave implementation doesn't leave a stale channel
  // reference on the connection state (which would short-circuit
  // `ensureConnectionReady` on the next mount attempt and try to
  // reuse a broken channel). The `try/finally` then guarantees
  // `roots` and the runtime entry are cleared regardless.
  const closingChannel = connectionState.channel
  connectionState.channel = undefined
  try {
    if (closingChannel) {
      connectionState.suppressDisconnectEvent = true
      closingChannel.leave()
    }
  } finally {
    connectionState.roots.clear()
    // Persist the latest pending value, then drop all cache state so a
    // reconnect starts from a clean in-memory slot. Durable persisters keep
    // their flushed entries (subject to gcTime on the next read).
    for (const writer of connectionState.cacheWriters.values()) {
      writer.flush()
      writer.cancel()
    }
    connectionState.cacheWriters.clear()
    for (const timer of connectionState.cacheEvictionTimers.values()) {
      clearTimeout(timer)
    }
    connectionState.cacheEvictionTimers.clear()
    connectionState.cacheRegistry.clear()
    // Defer into `.then` so a synchronous throw from `clear()` can't abort the
    // rest of disconnect cleanup.
    void Promise.resolve()
      .then(() => connectionState.memoryPersister.clear?.())
      .catch(() => undefined)
    const runtime = getSharedRuntime(connectionState.socket)
    runtime.connections.delete(connectionState.topic)
  }
}

export function subscribeStore(
  connection: RootConnection,
  storeId: StoreId,
  listener: () => void
): () => void {
  const key = storeIdKey(storeId)
  const listeners = connection.storeListeners.get(key) ?? new Set<() => void>()

  listeners.add(listener)
  connection.storeListeners.set(key, listeners)

  return () => {
    listeners.delete(listener)

    if (listeners.size === 0) {
      connection.storeListeners.delete(key)
    }
  }
}

export function dispatchConnectionCommand<Reply>(
  connection: RootConnection,
  storeId: StoreId,
  name: string,
  payload: unknown
): Promise<Reply> {
  if (!connection.channel) {
    return Promise.reject(new Error("Store is not connected"))
  }

  if (connection.version === 0) {
    // Cache-seeded store still revalidating: queue the command behind the live
    // initial patch instead of rejecting. Re-dispatch once connected
    // (version → 1). If revalidation fails, `initialPatchPromise` rejects and
    // the command rejects with the same error.
    if (connection.cacheKey && connection.initialPatchPromise) {
      return connection.initialPatchPromise.then(() =>
        dispatchConnectionCommand<Reply>(connection, storeId, name, payload)
      )
    }
    return Promise.reject(new Error("Store is not connected"))
  }

  const push = connection.channel.push("command", {
    root_id: connection.id,
    store_id: [...storeId],
    name,
    payload
  }) as PushLike

  return new Promise<Reply>((resolve, reject) => {
    const rejector = (reason: Error) => {
      cleanup()
      reject(reason)
    }

    const cleanup = () => {
      connection.pendingCommandRejectors.delete(rejector)
    }

    connection.pendingCommandRejectors.add(rejector)

    push
      .receive("ok", (reply) => {
        cleanup()
        resolve(reply as Reply)
      })
      .receive("error", (reply) => {
        cleanup()
        reject(
          new MusubiCommandError({
            kind: "failed",
            command: name,
            storeId: [...storeId],
            reply
          })
        )
      })
      .receive("timeout", () => {
        cleanup()
        reject(
          new MusubiCommandError({
            kind: "timeout",
            command: name,
            storeId: [...storeId]
          })
        )
      })
  })
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

function ensureConnectionReady(connectionState: ConnectionState): Promise<void> {
  if (connectionState.channel) {
    return Promise.resolve()
  }

  if (connectionState.connectPromise) {
    return connectionState.connectPromise
  }

  return connectConnectionChannel(connectionState)
}

function connectConnectionChannel(connectionState: ConnectionState): Promise<void> {
  if (connectionState.connectPromise) {
    return connectionState.connectPromise
  }

  connectionState.connectPromise = doConnectConnection(connectionState).finally(() => {
    connectionState.connectPromise = null
  })

  return connectionState.connectPromise
}

async function doConnectConnection(connectionState: ConnectionState): Promise<void> {
  // Phoenix.Socket.connect is idempotent.
  connectionState.socket.connect()

  const generation = connectionState.channelGeneration + 1
  connectionState.channelGeneration = generation

  const channel = connectionState.socket.channel(connectionState.topic, {})
  connectionState.channel = channel
  connectionState.suppressDisconnectEvent = false

  channel.on("patch", (payload: unknown) => {
    handleConnectionPatch(connectionState, payload, generation)
  })

  channel.onClose((reason: unknown) => {
    if (generation !== connectionState.channelGeneration) {
      return
    }

    if (connectionState.suppressDisconnectEvent) {
      connectionState.suppressDisconnectEvent = false
      return
    }

    handleConnectionDisconnect(connectionState, reason)
  })

  channel.onError((reason: unknown) => {
    if (generation !== connectionState.channelGeneration) {
      return
    }

    handleConnectionDisconnect(connectionState, reason)
  })

  try {
    await receivePush(channel.join() as PushLike)
  } catch (error) {
    connectionState.channel = undefined
    throw error
  }
}

type MountReply =
  | { kind: "ok"; rootId: string }
  | { kind: "already_mounted"; rootId: string }
  | { kind: "error"; reason: string }

function pushMount(
  channel: ChannelLike,
  connection: RootConnection,
  payload: { module: string; id: string; params: Record<string, unknown> }
): Promise<MountReply> {
  return new Promise<MountReply>((resolve) => {
    // Idempotent settle — receive callbacks AND external cancellation
    // (disconnect calling `connection.cancelMountPush`) both go through
    // here so whichever fires first wins and the others are no-ops.
    let settled = false
    const settle = (reply: MountReply): void => {
      if (settled) return
      settled = true
      connection.cancelMountPush = null
      resolve(reply)
    }

    connection.cancelMountPush = (reason) => settle({ kind: "error", reason })

    ;(channel.push("mount", payload) as PushLike)
      .receive("ok", (replyPayload) => {
        if (isRecord(replyPayload) && typeof replyPayload.root_id === "string" && replyPayload.root_id !== "") {
          settle({ kind: "ok", rootId: replyPayload.root_id })
        } else {
          settle({
            kind: "error",
            reason: `mount reply missing root_id: ${JSON.stringify(replyPayload)}`
          })
        }
      })
      .receive("error", (replyPayload) => {
        if (
          isRecord(replyPayload) &&
          replyPayload.reason === "already_mounted" &&
          typeof replyPayload.root_id === "string" &&
          replyPayload.root_id !== ""
        ) {
          settle({ kind: "already_mounted", rootId: replyPayload.root_id })
        } else {
          const reason =
            isRecord(replyPayload) && typeof replyPayload.reason === "string"
              ? replyPayload.reason
              : JSON.stringify(replyPayload)
          settle({ kind: "error", reason })
        }
      })
      .receive("timeout", () => {
        settle({ kind: "error", reason: "timeout" })
      })
  })
}

/**
 * Re-mount the existing `connection` after a version mismatch / recovery.
 * Reuses the same `RootConnection` instance (its `id`, `module`, `callerId`
 * are stable) — the server-assigned `root_id` must match what we already
 * have, otherwise something is badly out of sync.
 */
async function remountExistingConnection(connection: RootConnection): Promise<void> {
  const connectionState = connection.connection
  await ensureConnectionReady(connectionState)

  if (!connectionState.channel) {
    throw new Error("Connection is not connected")
  }

  const generation = connectionState.channelGeneration
  connection.channel = connectionState.channel
  connection.channelGeneration = generation
  const initialPatch = registerInitialPatchWaiter(connection, generation)
  connection.initialPatchPromise = initialPatch

  const reply = await pushMount(connectionState.channel, connection, {
    module: connection.module,
    id: connection.callerId,
    params: connection.mountParams
  })

  if (reply.kind === "error") {
    connection.pendingConnect = null
    connection.channel = undefined
    throw new Error(`Root remount failed: ${reply.reason}`)
  }

  // `:already_mounted` during recovery means the server still has our
  // entry — the prologue's `unmount` push didn't take effect (either
  // dropped on a flaky channel or processed after we'd already started
  // re-mounting). We just `resetConnectionState`-d this connection
  // locally, so the server will NOT re-emit an initial patch (the page
  // server didn't restart). Awaiting `initialPatch` here would hang
  // forever — fail loud instead so the consumer can drop the connection
  // and reconnect cleanly.
  if (reply.kind === "already_mounted") {
    connection.pendingConnect = null
    connection.channel = undefined
    connection.initialPatchPromise = null
    throw new Error(
      `Root remount aborted: server still has root_id=${reply.rootId} but the recovery unmount did not complete. Disconnect and reconnect to resynchronise.`
    )
  }

  if (reply.rootId !== connection.id) {
    connection.pendingConnect = null
    connection.channel = undefined
    throw new Error(
      `Root remount returned unexpected root_id: ${reply.rootId} (expected ${connection.id})`
    )
  }

  // Re-insert in the roots Map (recovery may have removed us during reset).
  connectionState.roots.set(connection.id, connection)

  await initialPatch
}

function handlePatch(
  connection: RootConnection,
  envelope: PatchEnvelope,
  generation: number
): void {
  if (generation !== connection.channelGeneration) {
    return
  }

  if (connection.version === 0) {
    if (envelope.base_version !== 0 || envelope.version !== 1) {
      const error = new Error("Initial patch envelope must start at version 1")
      connection.pendingConnect?.reject(error)
      connection.pendingConnect = null
      return
    }

    acceptEnvelope(connection, envelope, true)
    return
  }

  if (
    envelope.base_version !== connection.version ||
    envelope.version !== connection.version + 1
  ) {
    void recoverConnectionRootFromVersionMismatch(connection)
    return
  }

  acceptEnvelope(connection, envelope, false)
}

function acceptEnvelope(
  connection: RootConnection,
  envelope: PatchEnvelope,
  isInitial: boolean
): void {
  const previousRoot = connection.root
  const previousStoreIndex = connection.storeIndex
  const previousStreams = connection.streams
  const streamTouched = touchedStoreKeys(envelope.stream_ops)
  const uploadOps: UploadOp[] = envelope.upload_ops ?? []
  const uploadTouched = touchedStoresFromUploadOps(uploadOps)

  const nextRoot = applyPatch(connection.root, envelope.ops)
  const nextStoreIndex = buildStoreIndex(nextRoot)
  const validStoreIds = new Set(nextStoreIndex.keys())
  const nextStreams = pruneStreams(
    applyStreamOps(connection.streams, envelope.stream_ops),
    validStoreIds
  )

  connection.root = nextRoot
  connection.storeIndex = nextStoreIndex
  connection.streams = nextStreams
  applyUploadOps(connection, uploadOps)
  pruneUploads(connection.uploads, validStoreIds)
  invalidateSnapshotsForOps(
    connection.snapshotCache,
    envelope.ops,
    envelope.stream_ops,
    previousRoot,
    nextRoot
  )
  connection.version = envelope.version

  persistCacheEntry(connection, nextRoot)

  // Drop proxy entries whose store_id no longer exists in the tree. New
  // entries are created lazily by `proxy.ts` on demand.
  for (const key of Array.from(connection.proxyCache.keys())) {
    if (!validStoreIds.has(key)) {
      connection.proxyCache.delete(key)
    }
  }

  notifySubscribers(connection, previousStoreIndex, previousStreams, streamTouched, uploadTouched)

  if (isInitial) {
    connection.pendingConnect?.resolve()
    connection.pendingConnect = null
  }
}

// Throttled write-through of the latest root state to this mount's cache slot.
// No-op unless the mount enabled caching. Uses the registry-recorded persister
// so the write lands in the same backend the mount read from.
function persistCacheEntry(connection: RootConnection, data: unknown): void {
  const key = connection.cacheKey
  if (!key) {
    return
  }
  const connectionState = connection.connection
  const registered = connectionState.cacheRegistry.get(key)
  if (!registered) {
    return
  }

  let writer = connectionState.cacheWriters.get(key)
  if (!writer) {
    writer = createThrottledWriter(key, registered.persister)
    connectionState.cacheWriters.set(key, writer)
  }
  writer.schedule({ data, updatedAt: Date.now(), buster: registered.buster })
}

// Flush the pending throttled write (if any) and schedule cache eviction for a
// torn-down root. GC fires `gcTime` after the entry's last update (not after
// unmount), matching the read-time age check so the two never disagree.
function scheduleCacheEviction(connection: RootConnection): void {
  const key = connection.cacheKey
  if (!key) {
    return
  }
  const connectionState = connection.connection
  const writer = connectionState.cacheWriters.get(key)
  if (writer) {
    writer.flush()
    connectionState.cacheWriters.delete(key)
  }

  const registered = connectionState.cacheRegistry.get(key)
  if (!registered) {
    return
  }

  const pendingTimer = connectionState.cacheEvictionTimers.get(key)
  if (pendingTimer !== undefined) {
    clearTimeout(pendingTimer)
    connectionState.cacheEvictionTimers.delete(key)
  }

  // Defer the read into a `.then` so a synchronous throw from a custom
  // persister surfaces as a rejection (caught below) instead of bubbling out
  // of `scheduleCacheEviction`.
  void Promise.resolve()
    .then(() => registered.persister.getEntry(key))
    .then((entry) => {
      // No visible entry yet — e.g. an async persister's just-flushed write
      // hasn't landed. Preserve the full gcTime window rather than evicting
      // now and racing the in-flight write to deletion.
      const age = entry ? Date.now() - entry.updatedAt : 0
      const remaining = Math.max(0, registered.gcMs - age)
      const timer = setTimeout(() => {
        connectionState.cacheEvictionTimers.delete(key)
        // Only evict if no live mount re-claimed the key in the meantime.
        const stillRegistered = connectionState.cacheRegistry.get(key) === registered
        if (!stillRegistered || hasLiveRootForKey(connectionState, key)) {
          return
        }
        connectionState.cacheRegistry.delete(key)
        void Promise.resolve()
          .then(() => registered.persister.removeEntry(key))
          .catch(() => undefined)
      }, remaining)
      connectionState.cacheEvictionTimers.set(key, timer)
    })
    .catch(() => undefined)
}

function hasLiveRootForKey(connectionState: ConnectionState, key: string): boolean {
  for (const root of connectionState.roots.values()) {
    if (root.cacheKey === key) {
      return true
    }
  }
  return false
}

// Clear cache entries on demand. With a `target`, removes that one store's
// entry via the persister it was registered with (falling back to the
// connection's in-memory default if it was never mounted). Without a `target`,
// clears every registered persister plus the in-memory default.
export async function clearConnectionStoreCache(
  connectionState: ConnectionState,
  target?: { module: string; id: string; params?: Record<string, unknown> }
): Promise<void> {
  if (target) {
    const key = storeCacheKey(target)
    const writer = connectionState.cacheWriters.get(key)
    if (writer) {
      // Discard, not flush: clearing means "forget the latest value too".
      writer.cancel()
      connectionState.cacheWriters.delete(key)
    }
    const pendingTimer = connectionState.cacheEvictionTimers.get(key)
    if (pendingTimer !== undefined) {
      clearTimeout(pendingTimer)
      connectionState.cacheEvictionTimers.delete(key)
    }
    const registered = connectionState.cacheRegistry.get(key)
    const persister = registered?.persister ?? connectionState.memoryPersister
    connectionState.cacheRegistry.delete(key)
    await Promise.resolve(persister.removeEntry(key))
    return
  }

  // Discard pending writes, not flush: clearing means "forget the latest too".
  for (const writer of connectionState.cacheWriters.values()) {
    writer.cancel()
  }
  connectionState.cacheWriters.clear()
  for (const timer of connectionState.cacheEvictionTimers.values()) {
    clearTimeout(timer)
  }
  connectionState.cacheEvictionTimers.clear()

  const persisters = new Set<MusubiCachePersister>([connectionState.memoryPersister])
  for (const registered of connectionState.cacheRegistry.values()) {
    persisters.add(registered.persister)
  }
  connectionState.cacheRegistry.clear()

  await Promise.all(
    [...persisters].map((persister) =>
      persister.clear
        ? Promise.resolve(persister.clear())
        : Promise.resolve()
    )
  )
}

function handleConnectionPatch(
  connectionState: ConnectionState,
  payload: unknown,
  generation: number
): void {
  if (
    generation !== connectionState.channelGeneration ||
    !isConnectionPatchEnvelope(payload)
  ) {
    return
  }

  const connection = connectionState.roots.get(payload.root_id)

  if (!connection) {
    return
  }

  const { root_id: _rootId, ...envelope } = payload

  handlePatch(connection, envelope, connection.channelGeneration)
}

function notifySubscribers(
  connection: RootConnection,
  previousStoreIndex: ReadonlyMap<string, unknown>,
  previousStreams: ReadonlyMap<string, readonly StreamEntry<unknown>[]>,
  streamTouched: ReadonlySet<string>,
  uploadTouched: ReadonlySet<string>
): void {
  for (const [key, listeners] of connection.storeListeners) {
    const storeChanged = !Object.is(
      previousStoreIndex.get(key),
      connection.storeIndex.get(key)
    )

    const streamChanged =
      streamTouched.has(key) ||
      hasPrunedStreamForStore(previousStreams, connection.streams, key)

    const uploadChanged = uploadTouched.has(key)

    if (!storeChanged && !streamChanged && !uploadChanged) {
      continue
    }

    for (const listener of listeners) {
      listener()
    }
  }
}

async function recoverConnectionRootFromVersionMismatch(
  connection: RootConnection
): Promise<void> {
  const connectionState = connection.connection
  const rootId = connection.id

  if (connection.recovering) {
    return
  }

  connection.recovering = true
  connection.pendingConnect?.reject(new Error("Version mismatch"))
  connection.pendingConnect = null
  rejectPendingCommands(connection, new Error("Version mismatch"))
  resetConnectionState(connection)

  try {
    if (connectionState.channel) {
      await receivePush(
        connectionState.channel.push("unmount", { root_id: rootId }) as PushLike,
        "Root unmount"
      ).catch(() => undefined)
    }

    await remountExistingConnection(connection)
  } catch (error) {
    // Recovery is fire-and-forget (`void recover...` in `handlePatch`),
    // so a throw here would surface as an unhandled rejection. Force a
    // full disconnect instead — consumers see a clean tear-down
    // (pending commands rejected, snapshots stop updating, channel
    // left, runtime entry removed) and can reconnect explicitly. The
    // error itself is logged so the failure isn't silent.
    // Use `disconnectConnectionState` rather than just
    // `handleConnectionDisconnect`: the channel is still open in this
    // path, so we need to actually leave it and drop the connection
    // from the shared runtime, not just clear local state.
    // eslint-disable-next-line no-console
    console.error("[musubi] root recovery failed:", error)
    // Belt-and-braces `.catch` on the discarded promise: while
    // `disconnectConnectionState` doesn't currently throw, a synchronous
    // failure from `channel.leave()` would otherwise re-surface here as
    // an unhandled rejection — exactly the failure mode we're trying to
    // eliminate in this PR.
    disconnectConnectionState(connectionState).catch((cleanupError) => {
      // eslint-disable-next-line no-console
      console.error("[musubi] post-recovery disconnect failed:", cleanupError)
    })
  } finally {
    connection.recovering = false
  }
}

function handleConnectionDisconnect(
  connectionState: ConnectionState,
  _reason: unknown
): void {
  const disconnectError = new Error("Disconnected")

  // See `disconnectConnectionState` for the rationale on cancelling
  // both `pushMount` and `pendingConnect`.
  for (const pending of connectionState.pendingMounts) {
    pending.cancelMountPush?.("Disconnected")
    pending.pendingConnect?.reject(disconnectError)
    pending.pendingConnect = null
  }
  connectionState.pendingMounts.clear()

  for (const root of connectionState.roots.values()) {
    // Cancel any in-flight recovery-remount push (see
    // `disconnectConnectionState` for rationale).
    root.cancelMountPush?.("Disconnected")
    cancelGraceTimer(root)
    root.pendingConnect?.reject(disconnectError)
    root.pendingConnect = null
    root.initialPatchPromise = null
    rejectPendingCommands(root, disconnectError)
    resetConnectionState(root)
    root.channel = undefined
  }

  // Drop stale `roots` entries — otherwise a subsequent mount on the
  // (about-to-be-reconnected) state could find a disconnected entry
  // and alias to it via `:already_mounted`, handing the caller a
  // dead RootConnection.
  connectionState.roots.clear()

  connectionState.channel = undefined
}

function rejectPendingCommands(connection: RootConnection, reason: Error): void {
  for (const rejector of connection.pendingCommandRejectors) {
    rejector(reason)
  }

  connection.pendingCommandRejectors.clear()
}

function resetConnectionState(connection: RootConnection): void {
  connection.root = undefined
  connection.version = 0
  connection.storeIndex = new Map()
  connection.streams = new Map()
  connection.proxyCache = new Map()
  connection.snapshotCache = new Map()
}

function invalidateSnapshotsForOps(
  snapshotCache: Map<string, unknown>,
  ops: readonly JsonPatchOp[],
  streamOps: readonly StreamOp[],
  previousRoot: unknown,
  root: unknown
): void {
  if (ops.some((op) => op.path === "")) {
    snapshotCache.clear()
    return
  }

  for (const op of ops) {
    invalidateStoreIdsAlongPath(snapshotCache, previousRoot, op.path)
    invalidateStoreIdsAlongPath(snapshotCache, root, op.path)
    invalidateSnapshotSubtreesForOp(snapshotCache, previousRoot, op)
  }

  for (const op of streamOps) {
    invalidateStoreIdAncestors(snapshotCache, op.store_id)
  }
}

function invalidateSnapshotSubtreesForOp(
  snapshotCache: Map<string, unknown>,
  previousRoot: unknown,
  op: JsonPatchOp
): void {
  if (op.op !== "add") {
    invalidateStoreIdsInSubtree(snapshotCache, getPointerValue(previousRoot, op.path))
  }

  if (op.op !== "remove") {
    invalidateStoreIdsInSubtree(snapshotCache, op.value)
  }
}

function invalidateStoreIdsAlongPath(
  snapshotCache: Map<string, unknown>,
  root: unknown,
  pointerPath: string
): void {
  let current: unknown = root

  invalidateStoreKeyIfPresent(snapshotCache, current)

  for (const segment of parsePointer(pointerPath)) {
    current = getPointerChild(current, segment)

    if (current === undefined) {
      break
    }

    invalidateStoreKeyIfPresent(snapshotCache, current)
  }
}

function invalidateStoreIdAncestors(
  snapshotCache: Map<string, unknown>,
  storeId: StoreId
): void {
  for (let depth = 0; depth <= storeId.length; depth += 1) {
    snapshotCache.delete(storeIdKey(storeId.slice(0, depth)))
  }
}

function invalidateStoreIdsInSubtree(
  snapshotCache: Map<string, unknown>,
  value: unknown
): void {
  if (Array.isArray(value)) {
    for (const entry of value) {
      invalidateStoreIdsInSubtree(snapshotCache, entry)
    }

    return
  }

  if (!isRecord(value)) {
    return
  }

  invalidateStoreKeyIfPresent(snapshotCache, value)

  for (const child of Object.values(value)) {
    invalidateStoreIdsInSubtree(snapshotCache, child)
  }
}

function getPointerValue(root: unknown, pointerPath: string): unknown {
  let current: unknown = root

  for (const segment of parsePointer(pointerPath)) {
    current = getPointerChild(current, segment)

    if (current === undefined) {
      return undefined
    }
  }

  return current
}

function invalidateStoreKeyIfPresent(
  snapshotCache: Map<string, unknown>,
  value: unknown
): void {
  if (!isRecord(value)) {
    return
  }

  const maybeStoreId = value[STORE_ID_KEY]

  if (isStoreIdValue(maybeStoreId)) {
    snapshotCache.delete(storeIdKey(maybeStoreId))
  }
}

function getPointerChild(value: unknown, segment: string): unknown {
  if (Array.isArray(value)) {
    if (!/^(0|[1-9]\d*)$/.test(segment)) {
      return undefined
    }

    return value[Number.parseInt(segment, 10)]
  }

  if (isRecord(value)) {
    return value[segment]
  }

  return undefined
}

function buildStoreIndex(root: unknown): Map<string, unknown> {
  const index = new Map<string, unknown>()
  visitStoreNodes(root, index)
  return index
}

function visitStoreNodes(value: unknown, index: Map<string, unknown>): void {
  if (Array.isArray(value)) {
    for (const entry of value) {
      visitStoreNodes(entry, index)
    }

    return
  }

  if (!isRecord(value)) {
    return
  }

  const maybeStoreId = value[STORE_ID_KEY]

  if (isStoreIdValue(maybeStoreId)) {
    index.set(storeIdKey(maybeStoreId), value)
  }

  for (const child of Object.values(value)) {
    visitStoreNodes(child, index)
  }
}

function hasPrunedStreamForStore(
  previous: ReadonlyMap<string, readonly StreamEntry<unknown>[]>,
  next: ReadonlyMap<string, readonly StreamEntry<unknown>[]>,
  storeKey: string
): boolean {
  const storeId = JSON.parse(storeKey) as StoreId

  if (!hasStreamKeyForStore(previous, storeId)) {
    return false
  }

  return !hasStreamKeyForStore(next, storeId)
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null
}

function isConnectionPatchEnvelope(value: unknown): value is ConnectionPatchEnvelope {
  return (
    isRecord(value) &&
    value.type === "patch" &&
    typeof value.root_id === "string" &&
    typeof value.base_version === "number" &&
    typeof value.version === "number" &&
    Array.isArray(value.ops) &&
    Array.isArray(value.stream_ops)
  )
}

function isStoreIdValue(value: unknown): value is StoreId {
  return Array.isArray(value) && value.every((segment) => typeof segment === "string")
}

function receivePush(push: PushLike, action = "Channel join"): Promise<unknown> {
  return new Promise((resolve, reject) => {
    push
      .receive("ok", resolve)
      .receive("error", (payload) => {
        reject(new Error(`${action} failed: ${JSON.stringify(payload)}`))
      })
      .receive("timeout", () => {
        reject(new Error(`${action} timed out`))
      })
  })
}
