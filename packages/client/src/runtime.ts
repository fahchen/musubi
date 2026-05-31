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
export async function mountConnectionRoot(
  connectionState: ConnectionState,
  options: MountConnectionRootOptions
): Promise<RootConnection> {
  const tentative = newRootConnection(connectionState, options)
  connectionState.pendingMounts.add(tentative)

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
    const reply = await pushMount(connectionState.channel, {
      module: tentative.module,
      id: tentative.callerId,
      params: tentative.mountParams
    })

    if (reply.kind === "ok") {
      tentative.id = reply.rootId
      connectionState.roots.set(reply.rootId, tentative)
      tentative.initialPatchPromise = tentativeInitialPatch

      try {
        await tentativeInitialPatch
      } catch (error) {
        connectionState.roots.delete(reply.rootId)
        tentative.channel = undefined
        throw error
      }

      return tentative
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

    return existing
  } finally {
    connectionState.pendingMounts.delete(tentative)
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
    recovering: false
  }
}

function registerInitialPatchWaiter(
  connection: RootConnection,
  generation: number
): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    connection.pendingConnect = { generation, resolve, reject }
  })
}

function cancelGraceTimer(connection: RootConnection): void {
  if (connection.graceTimer !== null) {
    clearTimeout(connection.graceTimer)
    connection.graceTimer = null
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
  const connectionState = connection.connection
  const rootId = connection.id

  await new Promise<void>((resolve, reject) => {
    connection.graceTimer = setTimeout(() => {
      connection.graceTimer = null

      if (connection.refCount > 0) {
        // A new caller showed up; cleanup cancelled.
        resolve()
        return
      }

      if (!connectionState.roots.has(rootId)) {
        // Disconnect / external cleanup already removed us.
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

      if (!connectionState.channel) {
        resolve()
        return
      }

      receivePush(
        connectionState.channel.push("unmount", { root_id: rootId }) as PushLike,
        "Root unmount"
      )
        .then(() => resolve())
        .catch(reject)
    }, UNMOUNT_GRACE_MS)
  })
}

export async function disconnectConnectionState(
  connectionState: ConnectionState
): Promise<void> {
  const disconnectError = new Error("Disconnected")

  // In-flight `mountConnectionRoot` tentatives haven't yet been awaited
  // on their initial-patch promise (that happens only in the `:ok`
  // branch after `pushMount` returns). Clearing `pendingConnect` —
  // without rejecting it — avoids surfacing an unhandled rejection on
  // a promise no one is observing. The outer `pushMount` await unblocks
  // via Phoenix's default push timeout when the channel goes away;
  // `mountConnectionRoot` then takes the `:error` branch and surfaces
  // the failure to its actual awaiter.
  for (const pending of connectionState.pendingMounts) {
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
    root.channel = undefined
  }

  if (connectionState.channel) {
    connectionState.suppressDisconnectEvent = true
    connectionState.channel.leave()
    connectionState.channel = undefined
  }

  connectionState.roots.clear()

  const runtime = getSharedRuntime(connectionState.socket)
  runtime.connections.delete(connectionState.topic)
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
  if (!connection.channel || connection.version === 0) {
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
  payload: { module: string; id: string; params: Record<string, unknown> }
): Promise<MountReply> {
  return new Promise<MountReply>((resolve) => {
    ;(channel.push("mount", payload) as PushLike)
      .receive("ok", (replyPayload) => {
        if (isRecord(replyPayload) && typeof replyPayload.root_id === "string" && replyPayload.root_id !== "") {
          resolve({ kind: "ok", rootId: replyPayload.root_id })
        } else {
          resolve({
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
          resolve({ kind: "already_mounted", rootId: replyPayload.root_id })
        } else {
          const reason =
            isRecord(replyPayload) && typeof replyPayload.reason === "string"
              ? replyPayload.reason
              : JSON.stringify(replyPayload)
          resolve({ kind: "error", reason })
        }
      })
      .receive("timeout", () => {
        resolve({ kind: "error", reason: "timeout" })
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

  const reply = await pushMount(connectionState.channel, {
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
    void disconnectConnectionState(connectionState)
  } finally {
    connection.recovering = false
  }
}

function handleConnectionDisconnect(
  connectionState: ConnectionState,
  _reason: unknown
): void {
  const disconnectError = new Error("Disconnected")

  // See `disconnectConnectionState` for the rationale on not rejecting
  // pendingConnect for in-flight tentatives.
  for (const pending of connectionState.pendingMounts) {
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
    root.channel = undefined
  }

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
