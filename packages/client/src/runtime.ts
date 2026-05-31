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

export interface ConnectionState {
  readonly socket: SocketLike
  readonly topic: string
  readonly roots: Map<string, RootConnection>
  readonly uploaders: Record<string, ExternalUploader>

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

export function mountConnectionRoot(
  connectionState: ConnectionState,
  options: MountConnectionRootOptions
): { connection: RootConnection; ready: Promise<void> } {
  // No client-side dedup: every caller gets its own RootConnection and its
  // own server mount. The server is the sole authority on duplicate root
  // ids and will reply with `:already_mounted` if the same `(module, id)`
  // is mounted twice on one connection. Consumers that want sharing across
  // components layer their own ref-counting on top (e.g. `@musubi/react`'s
  // `pendingRootMounts`).
  //
  // Wire `root_id` is composed by the server as `"<module>:<callerId>"` and
  // echoed back in the mount reply. We compute the same composite locally
  // so the `connectionState.roots` Map entry exists before the initial
  // patch arrives — under mocked channels the patch can land in the same
  // call stack as the mount reply, so we cannot wait for the reply
  // microtask to insert. The mount reply still validates the server agrees.
  const rootId = `${options.module}:${options.id}`

  const connection: RootConnection = {
    module: options.module,
    callerId: options.id,
    id: rootId,
    connection: connectionState,
    mountParams: options.params ?? {},
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

  connectionState.roots.set(rootId, connection)

  const ready = ensureConnectionRootMounted(connection).catch((error) => {
    if (connection.version === 0) {
      connectionState.roots.delete(rootId)
    }

    throw error
  })

  return { connection, ready }
}

export async function unmountConnectionRoot(connection: RootConnection): Promise<void> {
  const connectionState = connection.connection
  const rootId = connection.id

  if (!connectionState.roots.has(rootId)) {
    return
  }

  connection.pendingConnect?.reject(new Error("Unmounted"))
  connection.pendingConnect = null
  rejectPendingCommands(connection, new Error("Unmounted"))
  resetConnectionState(connection)
  connection.channel = undefined
  connectionState.roots.delete(rootId)

  if (!connectionState.channel) {
    return
  }

  await receivePush(
    connectionState.channel.push("unmount", { root_id: rootId }) as PushLike,
    "Root unmount"
  )
}

export async function disconnectConnectionState(
  connectionState: ConnectionState
): Promise<void> {
  for (const root of connectionState.roots.values()) {
    root.pendingConnect?.reject(new Error("Disconnected"))
    root.pendingConnect = null
    rejectPendingCommands(root, new Error("Disconnected"))
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

function ensureConnectionRootMounted(connection: RootConnection): Promise<void> {
  if (connection.version >= 1 && connection.channel) {
    return Promise.resolve()
  }

  if (connection.connectPromise) {
    return connection.connectPromise
  }

  const connectionState = connection.connection

  connection.connectPromise = doMountConnectionRoot(connectionState, connection).finally(() => {
    connection.connectPromise = null
  })

  return connection.connectPromise
}

async function doMountConnectionRoot(
  connectionState: ConnectionState,
  connection: RootConnection
): Promise<void> {
  await ensureConnectionReady(connectionState)

  if (!connectionState.channel) {
    throw new Error("Connection is not connected")
  }

  const generation = connectionState.channelGeneration
  connection.channel = connectionState.channel
  connection.channelGeneration = generation

  const initialPatch = new Promise<void>((resolve, reject) => {
    connection.pendingConnect = { generation, resolve, reject }
  })

  try {
    const reply = await receivePush(
      connectionState.channel.push("mount", {
        module: connection.module,
        id: connection.callerId,
        params: connection.mountParams ?? {}
      }) as PushLike,
      "Root mount"
    )

    const assignedRootId = extractRootIdFromMountReply(reply)

    if (assignedRootId !== connection.id) {
      throw new Error(`Root mount returned unexpected root_id: ${assignedRootId}`)
    }
  } catch (error) {
    connection.pendingConnect = null
    connection.channel = undefined
    throw error
  }

  await initialPatch
}

function extractRootIdFromMountReply(reply: unknown): string {
  if (isRecord(reply) && typeof reply.root_id === "string" && reply.root_id !== "") {
    return reply.root_id
  }

  throw new Error(`Root mount reply missing root_id: ${JSON.stringify(reply)}`)
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

    await ensureConnectionRootMounted(connection)
  } finally {
    connection.recovering = false
  }
}

function handleConnectionDisconnect(
  connectionState: ConnectionState,
  _reason: unknown
): void {
  for (const root of connectionState.roots.values()) {
    root.pendingConnect?.reject(new Error("Disconnected"))
    root.pendingConnect = null
    rejectPendingCommands(root, new Error("Disconnected"))
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
