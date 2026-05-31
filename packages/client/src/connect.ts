import { getRootProxy } from "./proxy"
import {
  disconnectConnectionState,
  mountConnectionRoot,
  openConnectionState,
  unmountConnectionRoot
} from "./runtime"
import type { ConnectionState, SocketLike } from "./runtime"
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
}

export interface MountedStore<M extends StoreModule<R>, R> {
  readonly store: StoreProxy<M, R>
  readonly unmount: () => Promise<void>
}

export interface MusubiConnection<R> {
  mountStore<M extends StoreModule<R>>(
    options: MountStoreOptions<M, R>
  ): Promise<MountedStore<M, R>>
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
      const connection = await mountConnectionRoot(connectionState, {
        module: options.module,
        id: options.id,
        ...(options.params !== undefined ? { params: options.params } : {})
      })

      const store = getRootProxy<M, R>(connection)
      let unmounted = false
      const unmount = (): Promise<void> => {
        if (unmounted) {
          return Promise.resolve()
        }

        unmounted = true
        return unmountConnectionRoot(connection)
      }

      return { store, unmount }
    },

    async disconnect(): Promise<void> {
      await disconnectConnectionState(connectionState)
    }
  }
}
