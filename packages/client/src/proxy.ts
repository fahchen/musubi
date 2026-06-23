import {
  dispatchConnectionCommand,
  subscribeStore,
  type RootConnection
} from "./runtime"
import { getStream } from "./streams"
import { getUploadHandle } from "./uploads"
import type {
  AsyncResult,
  StoreId,
  StoreModule,
  StoreProxy,
  StoreSnapshot,
  WireAsyncResult,
  WireStreamMarker,
  WireUploadMarker
} from "./types"
import { STORE_ID_KEY, STREAM_MARKER_KEY, UPLOAD_MARKER_KEY, storeIdKey } from "./types"

const ROOT_STORE_ID: StoreId = []
const RESERVED = new Set(["__musubi_store_id__", "dispatchCommand", "subscribe", "snapshot"])

export function getRootProxy<M extends StoreModule<R>, R = unknown>(
  connection: RootConnection
): StoreProxy<M, R> {
  return getProxyForStore<M, R>(connection, ROOT_STORE_ID)
}

function getProxyForStore<M extends StoreModule<R>, R = unknown>(
  connection: RootConnection,
  storeId: StoreId
): StoreProxy<M, R> {
  const key = storeIdKey(storeId)
  const cached = connection.proxyCache.get(key)

  if (cached) {
    return cached as StoreProxy<M, R>
  }

  const proxy = buildProxy(connection, storeId)
  connection.proxyCache.set(key, proxy)
  return proxy as StoreProxy<M, R>
}

function buildProxy(connection: RootConnection, storeId: StoreId): object {
  const key = storeIdKey(storeId)

  const target: Record<string, unknown> = {}

  const handler: ProxyHandler<Record<string, unknown>> = {
    get(_target, prop) {
      if (typeof prop !== "string") {
        return Reflect.get(_target, prop)
      }

      if (prop === "__musubi_store_id__") {
        return storeId
      }

      if (prop === "dispatchCommand") {
        return makeDispatchCommand(connection, storeId)
      }

      if (prop === "subscribe") {
        return (listener: () => void) => subscribeStore(connection, storeId, listener)
      }

      if (prop === "snapshot") {
        return () => snapshotStore(connection, storeId)
      }

      const node = connection.storeIndex.get(key) as Record<string, unknown> | undefined

      if (!node) {
        return undefined
      }

      return resolveField(connection, storeId, node, prop)
    },

    has(_target, prop) {
      if (typeof prop !== "string") {
        return false
      }

      if (RESERVED.has(prop)) {
        return true
      }

      const node = connection.storeIndex.get(key) as Record<string, unknown> | undefined
      return node !== undefined && prop in node
    },

    ownKeys() {
      const node = connection.storeIndex.get(key) as Record<string, unknown> | undefined
      const fieldKeys = node ? Object.keys(node).filter((k) => k !== STORE_ID_KEY) : []
      return [...RESERVED, ...fieldKeys]
    },

    getOwnPropertyDescriptor(_target, prop) {
      if (typeof prop !== "string") {
        return undefined
      }

      if (RESERVED.has(prop)) {
        return { enumerable: true, configurable: true, writable: false, value: undefined }
      }

      const node = connection.storeIndex.get(key) as Record<string, unknown> | undefined

      if (!node || !(prop in node)) {
        return undefined
      }

      return { enumerable: true, configurable: true, writable: false, value: undefined }
    },

    set() {
      throw new Error("Store proxies are read-only")
    },

    deleteProperty() {
      throw new Error("Store proxies are read-only")
    }
  }

  return new Proxy(target, handler)
}

function makeDispatchCommand(connection: RootConnection, storeId: StoreId) {
  return <Reply>(name: string, payload: unknown): Promise<Reply> =>
    dispatchConnectionCommand<Reply>(connection, storeId, name, payload)
}

// ---------------------------------------------------------------------------
// Field resolution
// ---------------------------------------------------------------------------

function resolveField(
  connection: RootConnection,
  storeId: StoreId,
  node: Record<string, unknown>,
  fieldName: string
): unknown {
  const wireValue = node[fieldName]
  return resolveValue(connection, storeId, wireValue)
}

function resolveValue(
  connection: RootConnection,
  storeId: StoreId,
  wireValue: unknown
): unknown {
  // Rule 2: nested mounted store node.
  if (isStoreNode(wireValue)) {
    const childId = (wireValue as { __musubi_store_id__: StoreId }).__musubi_store_id__
    return getProxyForStore(connection, childId)
  }

  if (isWireStreamMarker(wireValue)) {
    return getMaterializedStreamItems(connection, storeId, wireValue[STREAM_MARKER_KEY])
  }

  if (isWireUploadMarker(wireValue)) {
    return getUploadHandle(connection, storeId, wireValue[UPLOAD_MARKER_KEY])
  }

  if (isWireAsyncResult(wireValue)) {
    // Rule 3: async wire shape. The result may itself be a stream marker,
    // nested store node, array, or plain object, so resolve it recursively.
    return normalizeAsync(wireValue, resolveAsyncResult(connection, storeId, wireValue.result))
  }

  if (Array.isArray(wireValue)) {
    return wireValue.map((item) => resolveValue(connection, storeId, item))
  }

  if (isPlainRecord(wireValue)) {
    return Object.fromEntries(
      Object.entries(wireValue).map(([key, value]) => [
        key,
        resolveValue(connection, storeId, value)
      ])
    )
  }

  // Rule 4: plain field.
  return wireValue
}

function getMaterializedStreamItems(
  connection: RootConnection,
  storeId: StoreId,
  streamName: string
): unknown[] {
  const entries = getStream<unknown>(connection.streams, storeId, streamName)
  return entries.map((entry) => entry.item)
}

function normalizeAsync<T>(wire: WireAsyncResult, data: T | null): AsyncResult<T> {
  switch (wire.status) {
    case "loading":
      return { status: "loading", data, error: null }
    case "ok":
      return { status: "ok", data: data as T, error: null }
    case "failed":
      return { status: "failed", data, error: wire.reason }
  }
}

function resolveAsyncResult(
  connection: RootConnection,
  storeId: StoreId,
  result: unknown
): unknown {
  return result === null ? null : resolveValue(connection, storeId, result)
}

function isStoreNode(value: unknown): value is { __musubi_store_id__: StoreId } {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return false
  }

  const id = (value as Record<string, unknown>)[STORE_ID_KEY]
  return Array.isArray(id) && id.every((segment) => typeof segment === "string")
}

function isWireAsyncResult(value: unknown): value is WireAsyncResult {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return false
  }

  const record = value as Record<string, unknown>
  const status = record.status
  return (
    record.__musubi_async__ === true &&
    (status === "loading" || status === "ok" || status === "failed") &&
    "result" in record &&
    "reason" in record
  )
}

function isWireStreamMarker(value: unknown): value is WireStreamMarker {
  return (
    isPlainRecord(value) &&
    typeof value[STREAM_MARKER_KEY] === "string" &&
    Object.keys(value).length === 1
  )
}

function isWireUploadMarker(value: unknown): value is WireUploadMarker {
  return (
    isPlainRecord(value) &&
    typeof value[UPLOAD_MARKER_KEY] === "string" &&
    Object.keys(value).length === 1
  )
}

function isPlainRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}

// ---------------------------------------------------------------------------
// snapshot()
// ---------------------------------------------------------------------------

export function snapshotStore<M extends StoreModule<R>, R = unknown>(
  connection: RootConnection,
  storeId: StoreId
): StoreSnapshot<M, R> | undefined {
  const key = storeIdKey(storeId)
  const cached = connection.snapshotCache.get(key)

  if (cached) {
    return cached as StoreSnapshot<M, R>
  }

  const node = connection.storeIndex.get(key) as Record<string, unknown> | undefined

  // Node absent from the index (store not mounted, or a reconnect window where
  // the index was reset). Return `undefined` rather than a stub cast to the
  // full snapshot type: a stub typed as fully-hydrated lets consumers deref a
  // "guaranteed" field and crash at runtime. `undefined` forces a compile-time
  // guard instead. Stable identity, so `useSyncExternalStore` doesn't churn.
  if (!node) {
    return undefined
  }

  const out: Record<string, unknown> = { __musubi_store_id__: storeId }

  for (const [fieldName, wireValue] of Object.entries(node)) {
    if (fieldName === STORE_ID_KEY) continue
    out[fieldName] = snapshotField(connection, storeId, wireValue)
  }

  const snapshot = out as StoreSnapshot<M, R>
  connection.snapshotCache.set(key, snapshot)
  return snapshot
}

function snapshotField(
  connection: RootConnection,
  storeId: StoreId,
  wireValue: unknown
): unknown {
  return snapshotValue(connection, storeId, wireValue)
}

function snapshotValue(
  connection: RootConnection,
  storeId: StoreId,
  wireValue: unknown
): unknown {
  if (isStoreNode(wireValue)) {
    const childId = (wireValue as { __musubi_store_id__: StoreId }).__musubi_store_id__
    return snapshotStore(connection, childId)
  }

  if (isWireStreamMarker(wireValue)) {
    return getMaterializedStreamItems(connection, storeId, wireValue[STREAM_MARKER_KEY])
  }

  if (isWireUploadMarker(wireValue)) {
    return getUploadHandle(connection, storeId, wireValue[UPLOAD_MARKER_KEY])
  }

  if (isWireAsyncResult(wireValue)) {
    return normalizeAsync(wireValue, snapshotAsyncResult(connection, storeId, wireValue.result))
  }

  if (Array.isArray(wireValue)) {
    return wireValue.map((item) => snapshotValue(connection, storeId, item))
  }

  if (isPlainRecord(wireValue)) {
    return Object.fromEntries(
      Object.entries(wireValue).map(([key, value]) => [
        key,
        snapshotValue(connection, storeId, value)
      ])
    )
  }

  return wireValue
}

function snapshotAsyncResult(
  connection: RootConnection,
  storeId: StoreId,
  result: unknown
): unknown {
  return result === null ? null : snapshotValue(connection, storeId, result)
}
