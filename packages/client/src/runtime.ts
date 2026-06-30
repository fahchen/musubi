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
  PushEvent,
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
  // One channel per root store, on topic `musubi:connection:<root_id>`. The
  // join params carry `{module, id, params}` — join IS the mount. Phoenix
  // auto-rejoins each channel after a transport drop, which re-runs the server
  // join and rebuilds the root; the per-channel `join().receive("ok")` callback
  // drives client-side recovery. No socket-level reopen handling needed.
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
  // Caller-supplied `MountStoreOptions.id` — sent in the join payload so the
  // server can compose the canonical root id.
  readonly callerId: string
  readonly connection: ConnectionState
  readonly mountParams: Record<string, unknown>

  // This root's own channel topic: `<baseTopic>:<root_id>`.
  readonly topic: string

  // Composed wire root_id (`module:callerId`). Known client-side at creation
  // (the server composes the same value) and confirmed in the join reply.
  // Doubles as the key in `ConnectionState.roots`.
  id: string

  // Local consumer count. Each `mountConnectionRoot` call that resolves to this
  // `RootConnection` (fresh mount OR alias) increments; each
  // `unmountConnectionRoot` call decrements. When it hits zero, a brief grace
  // timer leaves the channel (server stops the root); a re-mount within the
  // grace window cancels the timer.
  refCount: number
  graceTimer: ReturnType<typeof setTimeout> | null

  // Promise that resolves on the initial patch envelope (or rejects on join
  // failure / channel close). `mountConnectionRoot` callers and the React
  // adapter both `await` this. Re-armed on each reconnect rejoin.
  initialPatchPromise: Promise<void> | null

  // This root's channel. Stable across reconnects — Phoenix rejoins the same
  // object. Replaced only by the version-mismatch recreate path. Cleared while
  // a deliberate `leave()` is in flight.
  channel: ChannelLike | undefined
  channelGeneration: number

  // Set while we deliberately leave this channel (unmount teardown,
  // version-mismatch recreate, orphan drop). Suppresses the resulting
  // `onClose` so it does not re-enter disconnect handling.
  suppressClose: boolean

  // Mutable runtime state — read by the proxy on every property access.
  root: unknown
  version: number
  storeIndex: Map<string, unknown>
  streams: Map<string, readonly StreamEntry<unknown>[]>
  uploads: Map<string, UploadHandleImpl>
  proxyCache: Map<string, unknown>
  snapshotCache: Map<string, unknown>
  storeListeners: Map<string, Set<() => void>>
  // Transient push-event handlers keyed by event name (BDR-0032). Root-scoped
  // and lives on the RootConnection (not the channel) so registrations survive
  // reconnect.
  eventListeners: Map<string, Set<(payload: unknown) => void>>
  pendingCommandRejectors: Set<(reason: Error) => void>
  pendingConnect: PendingConnect | null
  recovering: boolean

  // Set while `unmountConnectionRoot` is awaiting the grace-timer settlement. A
  // timer cancellation (alias-on-remount, disconnect) calls this to resolve the
  // awaiting caller — the consumer's "release my handle" intent is honored even
  // if the internal server-side teardown was skipped.
  pendingUnmountResolver: (() => void) | null

  // Cache slot key for this mount, or null when caching is disabled.
  cacheKey: string | null
}

// Grace window (ms) between the last `unmountConnectionRoot` and leaving the
// channel. A re-mount of the same `(module, id)` within this window cancels the
// cleanup and reuses the existing mount — covers React 19 route-swap commit
// batching and StrictMode effect replay.
const UNMOUNT_GRACE_MS = 0

export interface ConnectionState {
  readonly socket: SocketLike
  // Base topic; each root channel joins `<baseTopic>:<root_id>`.
  readonly baseTopic: string
  readonly roots: Map<string, RootConnection>
  readonly uploaders: Record<string, ExternalUploader>

  // In-flight `mountConnectionRoot` tentatives that haven't yet been inserted
  // into `roots`. Tracked here so disconnect can cancel them.
  readonly pendingMounts: Set<RootConnection>

  // Default cache backend for mounts that enable caching without supplying a
  // persister. Connection-scoped and ephemeral — cleared on disconnect.
  readonly memoryPersister: MusubiCachePersister
  readonly cacheWriters: Map<string, ThrottledWriter>
  readonly cacheRegistry: Map<
    string,
    { persister: MusubiCachePersister; gcMs: number; buster: string }
  >
  readonly cacheEvictionTimers: Map<string, ReturnType<typeof setTimeout>>
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
  const baseTopic = options.topic ?? DEFAULT_CONNECTION_TOPIC
  const existing = runtime.connections.get(baseTopic)

  if (existing) {
    return { connection: existing, ready: Promise.resolve() }
  }

  const connection: ConnectionState = {
    socket,
    baseTopic,
    roots: new Map(),
    pendingMounts: new Set(),
    memoryPersister: createMemoryPersister(),
    cacheWriters: new Map(),
    cacheRegistry: new Map(),
    cacheEvictionTimers: new Map(),
    uploaders: options.uploaders ?? {}
  }

  runtime.connections.set(baseTopic, connection)

  // Open the transport now; per-root channels join lazily on mount. There is no
  // connection-level channel, so `ready` resolves immediately — auth and root
  // existence are validated on the first `mountConnectionRoot`.
  connection.socket.connect()

  return { connection, ready: Promise.resolve() }
}

export interface MountRootResult {
  connection: RootConnection
  fromCache: boolean
  revalidated: Promise<void>
}

export async function mountConnectionRoot(
  connectionState: ConnectionState,
  options: MountConnectionRootOptions
): Promise<MountRootResult> {
  const rootId = composeRootId(options.module, options.id)
  const existing = connectionState.roots.get(rootId)

  if (existing) {
    return aliasExistingRoot(existing, options)
  }

  const root = newRootConnection(connectionState, options)
  connectionState.pendingMounts.add(root)
  const cacheCfg = resolveCacheConfig(connectionState, options)

  try {
    // Create the channel + join (join IS the mount). Synchronous up to the
    // returned promise, so the `roots` insertion below lands before any await —
    // a concurrent mount of the same `(module, id)` aliases instead of opening a
    // second channel on the same topic.
    const initialPatch = attachAndJoinRoot(root)
    root.initialPatchPromise = initialPatch
    connectionState.roots.set(rootId, root)

    if (cacheCfg) {
      root.cacheKey = cacheCfg.key
      connectionState.cacheRegistry.set(cacheCfg.key, {
        persister: cacheCfg.persister,
        gcMs: cacheCfg.gcMs,
        buster: cacheCfg.buster
      })

      // Stale-while-revalidate: render a valid cache entry now and let the live
      // initial patch (a whole-root `replace ""`) swap in fresh state.
      if (await trySeedFromCache(root, cacheCfg)) {
        return { connection: root, fromCache: true, revalidated: initialPatch }
      }
    }

    try {
      await initialPatch
    } catch (error) {
      connectionState.roots.delete(rootId)
      leaveRootChannel(root)
      throw error
    }

    return { connection: root, fromCache: false, revalidated: Promise.resolve() }
  } finally {
    connectionState.pendingMounts.delete(root)
  }
}

// Alias a second consumer onto an existing root: bump refCount, cancel any
// pending grace teardown, and await the live initial patch if it is still in
// flight (so the caller never observes a not-yet-connected store).
async function aliasExistingRoot(
  existing: RootConnection,
  options: MountConnectionRootOptions
): Promise<MountRootResult> {
  warnOnParamsMismatch(existing, options)

  existing.refCount += 1
  cancelGraceTimer(existing)

  if (existing.initialPatchPromise && existing.version === 0) {
    try {
      await existing.initialPatchPromise
    } catch (error) {
      existing.refCount -= 1
      throw error
    }
  }

  return { connection: existing, fromCache: false, revalidated: Promise.resolve() }
}

function composeRootId(module: string, id: string): string {
  return `${module}:${id}`
}

function rootTopic(baseTopic: string, rootId: string): string {
  return `${baseTopic}:${rootId}`
}

function rootJoinParams(root: RootConnection): {
  module: string
  id: string
  params: Record<string, unknown>
} {
  return { module: root.module, id: root.callerId, params: root.mountParams }
}

function newRootConnection(
  connectionState: ConnectionState,
  options: MountConnectionRootOptions
): RootConnection {
  const id = composeRootId(options.module, options.id)

  return {
    module: options.module,
    callerId: options.id,
    id,
    connection: connectionState,
    mountParams: options.params ?? {},
    topic: rootTopic(connectionState.baseTopic, id),
    refCount: 1,
    graceTimer: null,
    initialPatchPromise: null,
    pendingUnmountResolver: null,
    channel: undefined,
    channelGeneration: 0,
    suppressClose: false,
    root: undefined,
    version: 0,
    storeIndex: new Map(),
    streams: new Map(),
    uploads: new Map(),
    proxyCache: new Map(),
    snapshotCache: new Map(),
    storeListeners: new Map(),
    eventListeners: new Map(),
    pendingCommandRejectors: new Set(),
    pendingConnect: null,
    recovering: false,
    cacheKey: null
  }
}

// Create this root's channel, wire its handlers, and join. Join carries the
// mount params; the server starts the root page server and emits the initial
// patch. Returns the initial-patch promise. Used both for the first mount and
// for the version-mismatch recreate path.
function attachAndJoinRoot(root: RootConnection): Promise<void> {
  const connectionState = root.connection
  // Phoenix.Socket.connect is idempotent.
  connectionState.socket.connect()

  const channel = connectionState.socket.channel(root.topic, rootJoinParams(root))
  const generation = root.channelGeneration + 1
  root.channelGeneration = generation
  root.channel = channel
  root.suppressClose = false

  const initialPatch = registerInitialPatchWaiter(root, generation)

  channel.on("patch", (payload: unknown) => {
    handleRootPatch(root, payload, generation)
  })

  channel.onClose(() => {
    if (generation !== root.channelGeneration) {
      return
    }
    if (root.suppressClose) {
      root.suppressClose = false
      return
    }
    handleRootDisconnect(root)
  })

  channel.onError(() => {
    if (generation !== root.channelGeneration) {
      return
    }
    handleRootDisconnect(root)
  })

  // `join().receive("ok", ...)` fires on the initial join AND on every Phoenix
  // rejoin (the join push's receive hooks survive `resend`), so this is the one
  // recovery hook: each rejoin re-runs the server mount and re-establishes the
  // root here.
  ;(channel.join() as PushLike)
    .receive("ok", (reply) => {
      if (generation !== root.channelGeneration) {
        return
      }
      handleRootJoined(root, reply, generation)
    })
    .receive("error", (reply) => {
      if (generation !== root.channelGeneration) {
        return
      }
      failRootJoin(root, `Root join failed: ${stringifyReply(reply)}`)
    })
    .receive("timeout", () => {
      if (generation !== root.channelGeneration) {
        return
      }
      failRootJoin(root, "Root join timed out")
    })

  return initialPatch
}

function handleRootJoined(root: RootConnection, reply: unknown, generation: number): void {
  const replyRootId = isRecord(reply) ? reply.root_id : undefined
  if (typeof replyRootId === "string" && replyRootId !== root.id) {
    failRootJoin(
      root,
      `Root join returned unexpected root_id: ${replyRootId} (expected ${root.id})`
    )
    return
  }

  // The server (re)started the page server on join and will emit the initial
  // patch next. Reset to version 0 so that patch is treated as the initial
  // envelope (whole-root `replace ""`), atomically swapping in fresh state over
  // any last-good snapshot kept across the reconnect window.
  root.version = 0

  if (!root.pendingConnect) {
    // Reconnect rejoin: re-arm the initial-patch waiter so the incoming patch
    // has a resolver. (The first join already armed it in `attachAndJoinRoot`.)
    root.initialPatchPromise = registerInitialPatchWaiter(root, generation)
  }
}

function failRootJoin(root: RootConnection, message: string): void {
  const error = new Error(message)
  root.pendingConnect?.reject(error)
  root.pendingConnect = null
  rejectPendingCommands(root, error)
}

function stringifyReply(reply: unknown): string {
  if (isRecord(reply) && typeof reply.reason === "string") {
    return reply.reason
  }
  return JSON.stringify(reply)
}

function registerInitialPatchWaiter(
  connection: RootConnection,
  generation: number
): Promise<void> {
  const promise = new Promise<void>((resolve, reject) => {
    connection.pendingConnect = { generation, resolve, reject }
  })
  // Pre-attach a no-op `.catch` so a rejection arriving before anyone explicitly
  // awaits it (e.g. disconnect firing mid-mount) doesn't surface as an unhandled
  // rejection. The original promise stays rejected; later `await` callers still
  // observe it.
  promise.catch(() => undefined)
  return promise
}

function cancelGraceTimer(connection: RootConnection): void {
  if (connection.graceTimer !== null) {
    clearTimeout(connection.graceTimer)
    connection.graceTimer = null
  }
  // Settle the awaiting `unmount()` caller — the consumer released their
  // handle; whether the server teardown actually ran is internal.
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
 * Drop a caller's hold on a `RootConnection`. When `refCount` hits zero a grace
 * timer leaves the channel (the server stops the root via `terminate/2`); a
 * re-mount of the same `(module, id)` within the grace window cancels the
 * cleanup and reuses the existing mount.
 */
export async function unmountConnectionRoot(connection: RootConnection): Promise<void> {
  if (connection.refCount <= 0) {
    return
  }

  connection.refCount -= 1
  if (connection.refCount > 0) {
    return
  }

  cancelGraceTimer(connection)
  await scheduleRootTeardown(connection)
}

function scheduleRootTeardown(connection: RootConnection): Promise<void> {
  if (connection.refCount > 0) {
    return Promise.resolve()
  }

  const connectionState = connection.connection
  const rootId = connection.id

  return new Promise<void>((resolve) => {
    connection.pendingUnmountResolver = () => {
      connection.pendingUnmountResolver = null
      resolve()
    }

    connection.graceTimer = setTimeout(() => {
      connection.graceTimer = null

      if (connection.refCount > 0) {
        // A new caller showed up; cleanup cancelled.
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

      const unmounted = new Error("Unmounted")
      connection.pendingConnect?.reject(unmounted)
      connection.pendingConnect = null
      rejectPendingCommands(connection, unmounted)
      resetConnectionState(connection)
      connection.initialPatchPromise = null
      connectionState.roots.delete(rootId)
      scheduleCacheEviction(connection)
      // Leaving the channel stops the server root via `terminate/2`.
      leaveRootChannel(connection)

      connection.pendingUnmountResolver = null
      resolve()
    }, UNMOUNT_GRACE_MS)
  })
}

// Deliberately leave this root's channel. Marks `suppressClose` so the
// resulting `onClose` does not re-enter disconnect handling, and clears the
// channel reference so Phoenix won't rejoin it.
function leaveRootChannel(root: RootConnection): void {
  const channel = root.channel
  root.channel = undefined
  if (channel) {
    root.suppressClose = true
    try {
      channel.leave()
    } catch {
      /* noop — local state is already torn down */
    }
  }
}

export async function disconnectConnectionState(
  connectionState: ConnectionState
): Promise<void> {
  const disconnectError = new Error("Disconnected")

  for (const pending of connectionState.pendingMounts) {
    pending.pendingConnect?.reject(disconnectError)
    pending.pendingConnect = null
  }
  connectionState.pendingMounts.clear()

  for (const root of connectionState.roots.values()) {
    cancelGraceTimer(root)
    root.pendingConnect?.reject(disconnectError)
    root.pendingConnect = null
    root.initialPatchPromise = null
    rejectPendingCommands(root, disconnectError)
    resetConnectionState(root)
    leaveRootChannel(root)
  }
  connectionState.roots.clear()

  // Persist the latest pending value, then drop all cache state so a reconnect
  // starts from a clean in-memory slot. Durable persisters keep their flushed
  // entries (subject to gcTime on the next read).
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
  runtime.connections.delete(connectionState.baseTopic)
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
    // Still revalidating (cache-seeded) or mid-reconnect: queue behind the live
    // initial patch instead of rejecting. Re-dispatch once connected
    // (version → 1). If revalidation fails, the command rejects with the same
    // error.
    if (connection.cacheKey && connection.initialPatchPromise) {
      return connection.initialPatchPromise.then(() =>
        dispatchConnectionCommand<Reply>(connection, storeId, name, payload)
      )
    }
    return Promise.reject(new Error("Store is not connected"))
  }

  const push = connection.channel.push("command", {
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

function handleRootPatch(
  root: RootConnection,
  payload: unknown,
  generation: number
): void {
  if (generation !== root.channelGeneration || !isConnectionPatchEnvelope(payload)) {
    return
  }

  const { root_id: _rootId, ...envelope } = payload

  handlePatch(root, envelope, generation)
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

  // Drop proxy entries whose store_id no longer exists in the tree. New entries
  // are created lazily by `proxy.ts` on demand.
  for (const key of Array.from(connection.proxyCache.keys())) {
    if (!validStoreIds.has(key)) {
      connection.proxyCache.delete(key)
    }
  }

  notifySubscribers(connection, previousStoreIndex, previousStreams, streamTouched, uploadTouched)

  // Transient push events (BDR-0032): dispatched once, after state is applied
  // and subscribers notified. Owns no version/recovery semantics.
  dispatchEvents(connection, envelope.events ?? [])

  if (isInitial) {
    connection.pendingConnect?.resolve()
    connection.pendingConnect = null
  }
}

function dispatchEvents(connection: RootConnection, events: readonly PushEvent[]): void {
  for (const event of events) {
    const handlers = connection.eventListeners.get(event.name)
    if (!handlers) {
      continue
    }
    // Snapshot before iterating: a handler may unsubscribe mid-dispatch.
    for (const handler of Array.from(handlers)) {
      handler(event.payload)
    }
  }
}

export function subscribeConnectionEvent(
  connection: RootConnection,
  name: string,
  handler: (payload: unknown) => void
): () => void {
  const handlers = connection.eventListeners.get(name) ?? new Set<(payload: unknown) => void>()

  handlers.add(handler)
  connection.eventListeners.set(name, handlers)

  return () => {
    handlers.delete(handler)

    if (handlers.size === 0) {
      connection.eventListeners.delete(name)
    }
  }
}

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

  void Promise.resolve()
    .then(() => registered.persister.getEntry(key))
    .then((entry) => {
      const age = entry ? Date.now() - entry.updatedAt : 0
      const remaining = Math.max(0, registered.gcMs - age)
      const timer = setTimeout(() => {
        connectionState.cacheEvictionTimers.delete(key)
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

export async function clearConnectionStoreCache(
  connectionState: ConnectionState,
  target?: { module: string; id: string; params?: Record<string, unknown> }
): Promise<void> {
  if (target) {
    const key = storeCacheKey(target)
    const writer = connectionState.cacheWriters.get(key)
    if (writer) {
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
      persister.clear ? Promise.resolve(persister.clear()) : Promise.resolve()
    )
  )
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

// Version mismatch on a still-live channel: client and server diverged. Leave
// the diverged channel (the server stops the root) and recreate it with a fresh
// join, which restarts the root and re-emits the initial patch. The last-good
// snapshot keeps rendering until the fresh initial patch lands.
async function recoverConnectionRootFromVersionMismatch(
  connection: RootConnection
): Promise<void> {
  if (connection.recovering) {
    return
  }

  connection.recovering = true
  const mismatch = new Error("Version mismatch")
  connection.pendingConnect?.reject(mismatch)
  connection.pendingConnect = null
  rejectPendingCommands(connection, mismatch)
  // Soft reset: keep last-good root/index/streams/snapshots so mounted proxies
  // keep serving complete (stale) data through the recreate window.
  connection.version = 0

  try {
    leaveRootChannel(connection)
    const initialPatch = attachAndJoinRoot(connection)
    connection.initialPatchPromise = initialPatch
    await initialPatch
  } catch (error) {
    // The recreate-join failed/timed out (server still down, transient error).
    // Recovery is fire-and-forget, so a throw here would surface as an unhandled
    // rejection — swallow it. Do NOT disconnect: that resets the root and blanks
    // the consumer with an `undefined` snapshot. Keep the last-good snapshot
    // rendering (it's still intact — version stayed 0) and rely on Phoenix to
    // keep rejoining the channel `attachAndJoinRoot` created; its
    // `join("ok")` handler completes recovery once the server is back.
    // eslint-disable-next-line no-console
    console.error("[musubi] root recovery failed; keeping last-good and awaiting rejoin:", error)
  } finally {
    connection.recovering = false
  }
}

// Transport drop / server-initiated close for one root's channel. Phoenix
// auto-rejoins the same channel on the next socket open; the join("ok") handler
// re-establishes the root. Keep last-good state rendering through the window.
function handleRootDisconnect(connection: RootConnection): void {
  const connectionState = connection.connection
  const disconnectError = new Error("Disconnected")

  cancelGraceTimer(connection)
  connection.pendingConnect?.reject(disconnectError)
  connection.pendingConnect = null
  rejectPendingCommands(connection, disconnectError)

  if (connection.refCount === 0) {
    // No live consumer — a release was mid grace-timer (just settled by
    // `cancelGraceTimer` above). Leave + drop so Phoenix won't rejoin an
    // orphan; the server-side root dies with the closed channel.
    connection.initialPatchPromise = null
    leaveRootChannel(connection)
    connectionState.roots.delete(connection.id)
    scheduleCacheEviction(connection)
    return
  }

  // Live root: keep the stale-but-complete snapshot so mounted proxies keep
  // rendering. `version = 0` makes the rejoin's initial patch (whole-root
  // `replace ""`) swap fresh state in atomically. Keep `connection.channel` —
  // it is the object Phoenix rejoins. Null the (already-resolved) initial-patch
  // promise so a cache-seeded `dispatchConnectionCommand` doesn't spin on it
  // through the window; the rejoin's `join("ok")` re-arms a fresh waiter.
  connection.initialPatchPromise = null
  connection.version = 0
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
