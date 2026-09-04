import type { CacheOptions } from "./cache"
import { getRootProxy } from "./proxy"
import {
  clearConnectionStoreCache,
  disconnectConnectionState,
  getConnectionStatus,
  mountConnectionRoot,
  openConnectionState,
  subscribeConnectionStatus,
  unmountConnectionRoot
} from "./runtime"
import type { ConnectionState, MusubiSocketStatus, SocketLike } from "./runtime"
import type { ExternalUploader, StoreModule, StoreProxy } from "./types"

export interface ConnectOptions {
  topic?: string
  uploaders?: Record<string, ExternalUploader>
}

export interface MountStoreOptions<
  M extends StoreModule<R>,
  R
> {
  module: M
  id: string
  params?: Record<string, unknown>
  // Opt-in stale-while-revalidate cache. When a valid entry for this store's
  // identity exists, `mountStore` resolves immediately with stale data
  // (`fromCache: true`) while the live mount revalidates in the background.
  cache?: CacheOptions
}

export interface MountedStore<M extends StoreModule<R>, R> {
  readonly store: StoreProxy<M, R>
  readonly unmount: () => Promise<void>
  // True when the resolved `store` was seeded from cache and the live initial
  // patch hasn't landed yet. Always false for non-cached mounts.
  readonly fromCache: boolean
  // True while showing cached data pending revalidation; settles to false when
  // `revalidated` resolves.
  readonly isFetching: boolean
  // Resolves when the live initial patch lands (or immediately for a cold
  // mount); rejects if revalidation fails (e.g. unmount/disconnect mid-flight).
  readonly revalidated: Promise<void>
}

export interface MusubiConnection<R> {
  mountStore<M extends StoreModule<R>>(
    options: MountStoreOptions<M, R>
  ): Promise<MountedStore<M, R>>
  // Remove cached store state. With `target`, clears that one store's entry;
  // without it, clears every cache entry on this connection.
  clearStoreCache(target?: {
    module: string
    id: string
    params?: Record<string, unknown>
  }): Promise<void>
  // Socket-liveness status (BDR-0033): "connecting" until the transport first
  // opens, "ready" while it is open, "reconnecting" after a drop while
  // phoenix.js reconnects. Mounted stores keep serving their last-good
  // snapshot through "reconnecting" (BDR-0015) — annotate, don't blank.
  status(): MusubiSocketStatus
  // Observe status transitions; returns an unsubscribe thunk. No replay —
  // read `status()` first if the current value matters.
  onStatusChange(listener: (status: MusubiSocketStatus) => void): () => void
  disconnect(): Promise<void>
}

/**
 * Opens one Musubi connection over `socket`.
 *
 * Usage:
 *
 *     const connection = await connect<Musubi.Stores>(socket)
 *
 *     const { store, unmount } = await connection.mountStore({
 *       module: "MyApp.Stores.DashboardStore",
 *       id: "dashboard"
 *     })
 *
 * The `R` generic is bound once on `connect`; the `module` literal then
 * drives type inference for every later `mountStore` call. React consumers
 * usually go through `createMusubi<R>()` in `@musubi/react` instead, which
 * binds `R` once for the connection and all hooks.
 */
export async function connect<R>(
  socket: SocketLike,
  options: ConnectOptions = {}
): Promise<MusubiConnection<R>> {
  const openOptions: { topic?: string; uploaders?: Record<string, ExternalUploader> } = {}
  if (options.topic !== undefined) openOptions.topic = options.topic
  if (options.uploaders !== undefined) openOptions.uploaders = options.uploaders

  const { connection, ready } = openConnectionState(socket, openOptions)
  await ready

  return buildConnectionApi<R>(connection)
}

function buildConnectionApi<R>(connectionState: ConnectionState): MusubiConnection<R> {
  return {
    async mountStore<M extends StoreModule<R>>(
      options: MountStoreOptions<M, R>
    ): Promise<MountedStore<M, R>> {
      const { connection, fromCache, revalidated } = await mountConnectionRoot(
        connectionState,
        {
          module: options.module,
          id: options.id,
          ...(options.params !== undefined ? { params: options.params } : {}),
          ...(options.cache !== undefined ? { cache: options.cache } : {})
        }
      )

      const store = getRootProxy<M, R>(connection)
      let unmounted = false
      const unmount = (): Promise<void> => {
        if (unmounted) {
          return Promise.resolve()
        }

        unmounted = true
        return unmountConnectionRoot(connection)
      }

      let isFetching = fromCache
      revalidated.then(
        () => {
          isFetching = false
        },
        () => {
          isFetching = false
        }
      )

      return {
        store,
        unmount,
        fromCache,
        get isFetching() {
          return isFetching
        },
        revalidated
      }
    },

    async clearStoreCache(target?: {
      module: string
      id: string
      params?: Record<string, unknown>
    }): Promise<void> {
      await clearConnectionStoreCache(connectionState, target)
    },

    status(): MusubiSocketStatus {
      return getConnectionStatus(connectionState)
    },

    onStatusChange(listener: (status: MusubiSocketStatus) => void): () => void {
      return subscribeConnectionStatus(connectionState, listener)
    },

    async disconnect(): Promise<void> {
      await disconnectConnectionState(connectionState)
    }
  }
}
