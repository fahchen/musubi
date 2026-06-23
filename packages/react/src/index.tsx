import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useId,
  useRef,
  useState
} from "react"
import type { FC, ReactNode } from "react"
import { useSyncExternalStoreWithSelector } from "use-sync-external-store/shim/with-selector"

import {
  canonicalStringify,
  connect as baseConnect,
  MusubiCommandError,
  storeCacheKey,
  type ConnectOptions,
  type MountStoreOptions,
  type MountedStore,
  type MusubiConnection,
  type SocketLike,
  type StoreModule,
  type StoreProxy,
  type StoreSnapshot,
  type CommandName,
  type CommandPayload,
  type CommandReply
} from "@musubi/client"

import { shallowEqual } from "./shallow"

export { shallowEqual } from "./shallow"
export { MusubiCommandError, keyOf } from "@musubi/client"

export type {
  AsyncResult,
  CommandName,
  CommandPayload,
  CommandReply,
  ConnectionPatchEnvelope,
  ExternalUploader,
  ExternalUploaderArgs,
  MountStoreOptions,
  MountedStore,
  MusubiCommandErrorKind,
  MusubiCommandErrorOptions,
  MusubiConnection,
  PatchEnvelope,
  StoreId,
  StoreModule,
  StoreProxy,
  StoreSnapshot,
  StreamEntry,
  StreamOp,
  UploadConfig,
  UploadEntry,
  UploadError,
  UploadHandle,
  UploadStatus
} from "@musubi/client"

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

export type MusubiRootMount<M extends StoreModule<R>, R> =
  | {
      status: "loading"
      store: null
      error: null
      isFetching: true
      revalidationError: null
    }
  | {
      status: "ready"
      store: StoreProxy<M, R>
      error: null
      isFetching: boolean
      revalidationError: Error | null
    }
  | {
      status: "error"
      store: null
      error: Error
      isFetching: false
      revalidationError: null
    }

export type UseMusubiRootOptions<
  M extends StoreModule<R>,
  R
> = MountStoreOptions<M, R> & {
  unmountOnCleanup?: boolean
  // Keep the previously-resolved store visible (with `isFetching: true`) while
  // a new key (id/params/module change) mounts, instead of flashing
  // `status: "loading"`. The new key's own cache seed, if any, then replaces it.
  keepPreviousData?: boolean
}

export type MusubiConnectionStatus<R> =
  | { state: "connecting"; connection: null }
  | { state: "ready"; connection: MusubiConnection<R> }
  | { state: "error"; connection: null; error: Error }

type ConnectionProviderProps<R> = {
  connection: MusubiConnection<R>
  socket?: never
  topic?: never
  children: ReactNode
}

type SocketProviderProps = {
  socket: SocketLike
  topic?: string
  uploaders?: Record<string, import("@musubi/client").ExternalUploader>
  connection?: never
  children: ReactNode
}

export type MusubiProviderProps<R> =
  | ConnectionProviderProps<R>
  | SocketProviderProps

export interface MusubiFactory<R> {
  connect: (socket: SocketLike, options?: ConnectOptions) => Promise<MusubiConnection<R>>
  MusubiProvider: FC<MusubiProviderProps<R>>
  useMusubiConnection: () => MusubiConnection<R>
  useMusubiConnectionStatus: () => MusubiConnectionStatus<R>
  useMusubiRoot: <M extends StoreModule<R>>(
    options: UseMusubiRootOptions<M, R>
  ) => MusubiRootMount<M, R>
  useMusubiRootSuspense: <M extends StoreModule<R>>(
    options: UseMusubiRootOptions<M, R>
  ) => StoreProxy<M, R>
  useMusubiSnapshot: {
    <M extends StoreModule<R>>(proxy: StoreProxy<M, R>): StoreSnapshot<M, R> | undefined
    <M extends StoreModule<R>, Selected>(
      proxy: StoreProxy<M, R>,
      selector: (snapshot: StoreSnapshot<M, R> | undefined) => Selected,
      equalityFn?: (a: Selected, b: Selected) => boolean
    ): Selected
  }
  useMusubiCommand: <M extends StoreModule<R>, K extends CommandName<M, R>>(
    proxy: StoreProxy<M, R>,
    name: K
  ) => MusubiCommandResult<M, K, R>
}

export interface MusubiCommandResult<
  M extends StoreModule<R>,
  K extends CommandName<M, R>,
  R
> {
  dispatch: (payload: CommandPayload<M, K, R>) => Promise<CommandReply<M, K, R>>
  isPending: boolean
  error: MusubiCommandError | null
  data: CommandReply<M, K, R> | null
  reset: () => void
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

/**
 * Returns a Musubi API closed over the registry `R`.
 *
 * Usage:
 *
 *     export const {
 *       connect,
 *       MusubiProvider,
 *       useMusubiRoot,
 *       useMusubiRootSuspense,
 *       useMusubiSnapshot,
 *       useMusubiCommand
 *     } = createMusubi<Musubi.Stores>()
 */
export function createMusubi<R>(): MusubiFactory<R> {
  const StatusContext = createContext<MusubiConnectionStatus<R> | null>(null)

  const MusubiProvider: FC<MusubiProviderProps<R>> = (props) => {
    if (props.connection !== undefined && props.socket !== undefined) {
      throw new Error(
        "<MusubiProvider> accepts either `connection` or `socket`, not both"
      )
    }

    if (props.connection === undefined && props.socket === undefined) {
      throw new Error(
        "<MusubiProvider> requires either `connection` or `socket`"
      )
    }

    if (props.connection !== undefined) {
      return (
        <StatusContext.Provider value={{ state: "ready", connection: props.connection }}>
          {props.children}
        </StatusContext.Provider>
      )
    }

    return <SocketProvider {...props}>{props.children}</SocketProvider>
  }

  const SocketProvider: FC<SocketProviderProps> = ({ socket, topic, uploaders, children }) => {
    const [status, setStatus] = useState<MusubiConnectionStatus<R>>({
      state: "connecting",
      connection: null
    })

    useEffect(() => {
      let cancelled = false
      let liveConnection: MusubiConnection<R> | null = null
      setStatus({ state: "connecting", connection: null })

      const options: ConnectOptions = {}
      if (topic !== undefined) options.topic = topic
      if (uploaders !== undefined) options.uploaders = uploaders

      baseConnect<R>(socket, options)
        .then((connection) => {
          if (cancelled) {
            // Race: parent unmounted (or socket/topic changed) before connect
            // resolved. Tear the freshly opened connection down immediately
            // so we don't leak a live channel.
            void connection.disconnect()
            return
          }

          liveConnection = connection
          setStatus({ state: "ready", connection })
        })
        .catch((cause: unknown) => {
          if (cancelled) return
          const error = cause instanceof Error ? cause : new Error(String(cause))
          setStatus({ state: "error", connection: null, error })
        })

      return () => {
        cancelled = true
        if (liveConnection) {
          const c = liveConnection
          liveConnection = null
          void c.disconnect()
        }
      }
    }, [socket, topic, uploaders])

    return <StatusContext.Provider value={status}>{children}</StatusContext.Provider>
  }

  function useMusubiConnectionStatus(): MusubiConnectionStatus<R> {
    const status = useContext(StatusContext)
    if (!status) {
      throw new Error(
        "useMusubiConnectionStatus must be used inside <MusubiProvider>"
      )
    }
    return status
  }

  function useMusubiConnection(): MusubiConnection<R> {
    const status = useContext(StatusContext)

    if (!status) {
      throw new Error(
        "useMusubiConnection must be used inside <MusubiProvider> (or call useMusubiConnectionStatus() to observe connecting/error states)"
      )
    }

    if (status.state !== "ready") {
      throw new Error(
        `useMusubiConnection requires a ready connection (current state: ${status.state}). Use useMusubiConnectionStatus() to observe connecting/error states.`
      )
    }

    return status.connection
  }

  function connect(
    socket: SocketLike,
    options?: ConnectOptions
  ): Promise<MusubiConnection<R>> {
    return baseConnect<R>(socket, options)
  }

  function useMusubiRoot<M extends StoreModule<R>>(
    options: UseMusubiRootOptions<M, R>
  ): MusubiRootMount<M, R> {
    const connection = useMusubiConnection()
    const [state, setState] = useState<MusubiRootMount<M, R>>({
      status: "loading",
      store: null,
      error: null,
      isFetching: true,
      revalidationError: null
    })

    const paramsKey = canonicalStringify(options.params ?? null)
    // Read at transition time, not via deps — toggling it shouldn't force a
    // mount teardown/remount; it only governs the loading-gap presentation.
    const keepPreviousDataRef = useRef(options.keepPreviousData ?? false)
    keepPreviousDataRef.current = options.keepPreviousData ?? false

    useEffect(() => {
      let cancelled = false
      const unmountOnCleanup = options.unmountOnCleanup ?? true
      const mountOptions = buildMountOptions<M, R>(options)

      // keepPreviousData governs only the gap before the new mount resolves:
      // keep the prior `store` visible (flagged `isFetching`) instead of
      // flashing `loading`. The new key's own cache seed, if any, then resolves
      // fast and replaces it.
      setState((prev) =>
        keepPreviousDataRef.current && prev.status === "ready"
          ? { ...prev, isFetching: true, revalidationError: null }
          : { status: "loading", store: null, error: null, isFetching: true, revalidationError: null }
      )

      const sharedMount = ensureRootMount<M, R>(connection, mountOptions)
      bumpMountRef(connection, sharedMount.key)

      sharedMount.promise
        .then((mounted) => {
          if (cancelled) return
          setState({
            status: "ready",
            store: mounted.store as StoreProxy<M, R>,
            error: null,
            isFetching: mounted.isFetching,
            revalidationError: null
          })
          // Cold mounts already resolved with `isFetching: false`; only a
          // cache-seeded mount is still revalidating. Skip the no-op handler
          // for cold mounts to avoid an extra render.
          if (!mounted.isFetching) return
          // Settle `isFetching` when revalidation lands. On failure keep the
          // (stale) store visible and surface the error rather than blanking
          // the UI.
          mounted.revalidated
            .then(() => {
              if (cancelled) return
              setState((prev) =>
                prev.status === "ready"
                  ? { ...prev, isFetching: false, revalidationError: null }
                  : prev
              )
            })
            .catch((error: unknown) => {
              if (cancelled) return
              setState((prev) =>
                prev.status === "ready"
                  ? {
                      ...prev,
                      isFetching: false,
                      revalidationError:
                        error instanceof Error ? error : new Error(String(error))
                    }
                  : prev
              )
            })
        })
        .catch((error: unknown) => {
          if (!cancelled) {
            setState({
              status: "error",
              store: null,
              error: error instanceof Error ? error : new Error(String(error)),
              isFetching: false,
              revalidationError: null
            })
          }
        })

      return () => {
        cancelled = true
        releaseRootMount(connection, sharedMount.key, unmountOnCleanup)
      }
      // paramsKey collapses logically-equal params objects so that two
      // callers passing {a:1,b:2} and {b:2,a:1} share a mount.
    }, [connection, options.module, options.id, paramsKey, options.unmountOnCleanup])

    return state
  }

  function useMusubiRootSuspense<M extends StoreModule<R>>(
    options: UseMusubiRootOptions<M, R>
  ): StoreProxy<M, R> {
    const connection = useMusubiConnection()
    const unmountOnCleanup = options.unmountOnCleanup ?? true

    const mountOptions = buildMountOptions<M, R>(options)

    // Render-phase: lookup-or-create. DO NOT bump refs here — that happens
    // in the commit-phase effect below. Suspense may discard this render.
    const sharedMount = ensureRootMount<M, R>(connection, mountOptions)

    // Per-identity render-phase token registered with `suspenseSweepRegistry`.
    // While React holds onto this fiber's hook state the token is reachable
    // and the finalizer does not run. When React discards the render
    // (parent unmounts, Suspense throws the subtree away) without ever
    // committing, the fiber's hook state is released, the token becomes
    // GC-eligible, and the finalizer eventually tears the orphaned mount
    // down. This replaces the old timer-based `scheduleSuspenseOrphanSweep`
    // (which raced React 19's MessageChannel commit and wedged Suspense in
    // an infinite mount/unmount loop).
    //
    // Each identity change in render allocates a FRESH token (not a single
    // fiber-stable token) so its registration can be unregistered
    // independently. With a shared token, a render N+1 that supersedes a
    // still-armed safety net from render N would also unregister N's
    // registration when its own effect cleanup ran later — silently
    // disarming the safety net on a render that hadn't committed yet.
    const activeTokenRef = useRef<object | null>(null)
    const activeHoldingsRef = useRef<SuspenseSweepHoldings | null>(null)

    // Stable per-fiber claim id. `useId` survives StrictMode dev
    // double-invoke (and Suspense retries) so all spurious
    // re-registrations of the same logical mount add the SAME id to
    // `shared.claimers`; `Set` deduplication keeps the bookkeeping
    // honest. Different fibers (siblings using the same root) get
    // distinct ids, so each owns its own claim and one's lifecycle
    // doesn't poison the other's safety net.
    const claimerId = useId()

    if (suspenseSweepRegistry !== null) {
      const previous = activeHoldingsRef.current
      const currentConnection = connection as MusubiConnection<unknown>
      const needsArm =
        previous === null ||
        previous.connection !== currentConnection ||
        previous.key !== sharedMount.key ||
        previous.unmountOnCleanup !== unmountOnCleanup ||
        !sharedMount.claimers.has(claimerId)

      if (needsArm) {
        // Deliberately do NOT unregister the previous token here. A
        // render-phase unregister is unsafe: a still-suspended earlier
        // render (e.g. an in-flight transition for identity B) has its
        // safety net armed *only* via the previous registration, and a
        // later render swapping to identity C would disarm B's net
        // before B ever gets a chance to commit — leaking the
        // server-side root if B's mount eventually settles. Instead,
        // just drop the strong reference (overwrite `activeTokenRef`
        // below); GC reclaims the abandoned token and the finalizer
        // sweeps the entry via its `claimerId` normally.
        sharedMount.claimers.add(claimerId)
        const nextToken: object = {}
        const nextHoldings: SuspenseSweepHoldings = {
          connection: currentConnection,
          key: sharedMount.key,
          unmountOnCleanup,
          claimerId,
          shared: sharedMount
        }
        suspenseSweepRegistry.register(nextToken, nextHoldings, nextToken)
        activeTokenRef.current = nextToken
        activeHoldingsRef.current = nextHoldings
      }
    }

    useEffect(() => {
      // Commit-phase: this render won; take a ref.
      bumpMountRef(connection, sharedMount.key)
      // Snapshot the token that was active when this effect set up so
      // cleanup can unregister exactly this registration even if a later
      // render swaps `activeTokenRef` out from under us.
      const tokenAtCommit = activeTokenRef.current
      const sharedAtCommit = sharedMount
      return () => {
        // Drop this fiber's render-phase claim — refs now owns the
        // entry's lifecycle for the committed lifespan.
        sharedAtCommit.claimers.delete(claimerId)
        releaseRootMount(connection, sharedMount.key, unmountOnCleanup)
        if (suspenseSweepRegistry !== null && tokenAtCommit !== null) {
          suspenseSweepRegistry.unregister(tokenAtCommit)
          if (activeTokenRef.current === tokenAtCommit) {
            activeTokenRef.current = null
            activeHoldingsRef.current = null
          }
        }
      }
      // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [connection, sharedMount.key, unmountOnCleanup])

    if (sharedMount.failed) {
      throw sharedMount.error
    }

    if (!sharedMount.settled) {
      throw sharedMount.promise
    }

    return (sharedMount.value as unknown as MountedStore<M, R>).store
  }

  function useMusubiSnapshotImpl<M extends StoreModule<R>, Selected>(
    proxy: StoreProxy<M, R>,
    selector?: (snapshot: StoreSnapshot<M, R> | undefined) => Selected,
    equalityFn?: (a: Selected, b: Selected) => boolean
  ): Selected | StoreSnapshot<M, R> | undefined {
    const subscribe = useCallback((onChange: () => void) => proxy.subscribe(onChange), [proxy])
    const getSnapshot = useCallback(() => proxy.snapshot(), [proxy])
    const resolvedSelector =
      selector ?? ((value: StoreSnapshot<M, R> | undefined) => value as unknown as Selected)
    // Default to shallowEqual when a selector is supplied so callers that
    // return fresh object/tuple literals don't re-render on every patch.
    const resolvedEquality =
      equalityFn ?? (selector ? (shallowEqual as (a: Selected, b: Selected) => boolean) : undefined)

    return useSyncExternalStoreWithSelector(
      subscribe,
      getSnapshot,
      getSnapshot,
      resolvedSelector,
      resolvedEquality
    )
  }

  const useMusubiSnapshot = useMusubiSnapshotImpl as MusubiFactory<R>["useMusubiSnapshot"]

  function useMusubiCommand<M extends StoreModule<R>, K extends CommandName<M, R>>(
    proxy: StoreProxy<M, R>,
    name: K
  ): MusubiCommandResult<M, K, R> {
    type Reply = CommandReply<M, K, R>
    const [state, setState] = useState<{
      isPending: boolean
      error: MusubiCommandError | null
      data: Reply | null
    }>({ isPending: false, error: null, data: null })

    const requestIdRef = useRef(0)
    const mountedRef = useRef(true)

    useEffect(() => {
      mountedRef.current = true
      return () => {
        mountedRef.current = false
      }
    }, [])

    const dispatch = useCallback(
      async (payload: CommandPayload<M, K, R>): Promise<Reply> => {
        const requestId = ++requestIdRef.current
        if (mountedRef.current) {
          setState({ isPending: true, error: null, data: null })
        }

        try {
          const reply = (await proxy.dispatchCommand(name, payload)) as Reply
          if (mountedRef.current && requestId === requestIdRef.current) {
            setState({ isPending: false, error: null, data: reply })
          }
          return reply
        } catch (cause) {
          const error = MusubiCommandError.is(cause)
            ? cause
            : new MusubiCommandError({
                kind: "failed",
                command: String(name),
                storeId: [...proxy.__musubi_store_id__],
                reply: { error: cause instanceof Error ? cause.message : String(cause) },
                cause
              })

          if (mountedRef.current && requestId === requestIdRef.current) {
            setState({ isPending: false, error, data: null })
          }
          throw error
        }
      },
      [proxy, name]
    )

    const reset = useCallback(() => {
      requestIdRef.current += 1
      if (mountedRef.current) {
        setState({ isPending: false, error: null, data: null })
      }
    }, [])

    return { dispatch, isPending: state.isPending, error: state.error, data: state.data, reset }
  }

  return {
    connect,
    MusubiProvider,
    useMusubiConnection,
    useMusubiConnectionStatus,
    useMusubiRoot,
    useMusubiRootSuspense,
    useMusubiSnapshot,
    useMusubiCommand
  }
}

// ---------------------------------------------------------------------------
// Shared mount ref-counting across hook callers
// ---------------------------------------------------------------------------

type SharedRootMount = {
  key: string
  refs: number
  promise: Promise<MountedStore<never, unknown>>
  settled: boolean
  failed: boolean
  value: MountedStore<never, unknown> | null
  error: Error | null
  cleanupTimer: ReturnType<typeof setTimeout> | null
  // Set of `useId()` claim ids — one per fiber currently holding a
  // render-phase safety net on this entry. The sweep is a no-op while
  // the set is non-empty; the last live consumer (committed via
  // `refs`, or a render-phase claim) is the one that ultimately
  // tears the entry down. A `Set` rather than a monotonic generation
  // makes this robust to React 19 StrictMode dev double-invoke and
  // Suspense retries, which can register a single fiber's claim
  // multiple times — `Set.add` deduplicates so a sibling consumer's
  // claim isn't crowded out by spurious re-registrations.
  claimers: Set<string>
}

const pendingRootMounts: WeakMap<
  MusubiConnection<unknown>,
  Map<string, SharedRootMount>
> = new WeakMap()

/** Internal: exposed for white-box tests, not part of the public API. */
export const __pendingRootMountsForTests = pendingRootMounts

type SuspenseSweepHoldings = {
  connection: MusubiConnection<unknown>
  key: string
  unmountOnCleanup: boolean
  // The `useId()` id of the fiber that armed this safety net. The
  // sweep removes this id from `shared.claimers` first, then bails if
  // anyone else (committed via `refs` or render-phase via remaining
  // claimer ids) still holds the entry.
  claimerId: string
  // Strong reference to the exact `SharedRootMount` instance that was
  // live at register time. The sweep bails when `mounts.get(key)` no
  // longer matches this object — i.e. the original entry was torn
  // down and replaced by a freshly-allocated one for the same key —
  // so a stale finalizer can never mutate or unmount the new entry.
  // Adds one extra pointer per pending registration; released when
  // the finalizer fires (or sooner via `unregister` from the commit
  // path).
  shared: SharedRootMount
}

/**
 * Core Suspense-orphan sweep. Exported for white-box unit tests because
 * the host's GC schedule is not deterministic; tests drive this directly
 * to cover the claimer-set and committed-claim cases without waiting on
 * `globalThis.gc()`.
 */
export function __runSuspenseOrphanSweep(holdings: SuspenseSweepHoldings): void {
  const { connection, key, unmountOnCleanup, claimerId, shared } = holdings
  const mounts = pendingRootMounts.get(connection)
  if (!mounts) return
  // Stale finalizer: the entry alive at registration time is no
  // longer the live one (the original was torn down and a fresh
  // `SharedRootMount` was allocated for the same key). Bail before
  // touching anything so the new entry's claimers/refs aren't
  // disturbed.
  if (mounts.get(key) !== shared) return
  // Drop our render-phase claim. Idempotent — `Set.delete` on an
  // already-removed id is a no-op, which covers re-arming and
  // already-committed paths.
  shared.claimers.delete(claimerId)
  // A committed consumer claimed the entry: leave it alone, ref
  // accounting owns the lifecycle now.
  if (shared.refs > 0) return
  // Other fibers' render-phase safety nets still hold the entry; one
  // of those will eventually do the teardown.
  if (shared.claimers.size > 0) return

  // Wait for the in-flight mount to settle before deciding. If the GC
  // fires before `shared.promise` resolves, `shared.value` is still
  // null; deleting the entry now would orphan the eventual
  // `MountedStore` with no one left to call `.unmount()` on it. Chain
  // off the promise so the unmount tracks the settled value (mirrors
  // the deferred unmount in `releaseRootMount`).
  //
  // Settling the mount is exactly what wakes React's Suspense ping for
  // the resumed render. Tearing down inline in the `.then` microtask
  // would delete the entry in the same flush as the settle, before
  // React's scheduler re-renders the discarded subtree — the resumed
  // render's `ensureRootMount` would then find no entry and allocate a
  // fresh one, producing the duplicate `unmount` + `mount` wire push
  // seen on SPA navigation under react-router v7's `startTransition`.
  // `sweepSettledOrphan` defers the teardown two macrotask hops past
  // the settle so the resumed render can re-claim first: hop one lets
  // React's scheduler run the resumed render-phase (re-adds the fiber's
  // claimer), hop two lets the commit-phase effect fire `bumpMountRef`
  // (bumps refs). Either signal flips a guard and bails. One hop is not
  // enough on test runtimes where React's scheduler postMessage trails
  // `setTimeout(0)`.
  const sweepSettledOrphan = (teardown?: () => void): void => {
    // hop 1: React's render-phase (resumed render re-adds its claimer)
    setTimeout(() => {
      // hop 2: React's commit-phase (effect fires `bumpMountRef` → refs)
      setTimeout(() => {
        if (mounts.get(key) !== shared) return
        if (shared.refs > 0) return
        if (shared.claimers.size > 0) return
        mounts.delete(key)
        teardown?.()
      }, 0)
    }, 0)
  }

  void shared.promise.then(
    (mounted) =>
      sweepSettledOrphan(unmountOnCleanup ? () => void mounted.unmount() : undefined),
    // Failed mount: nothing to unmount, just drop the dead entry so a
    // future render can retry.
    () => sweepSettledOrphan()
  )
}

/**
 * FinalizationRegistry safety net for `useMusubiRootSuspense`. When a
 * Suspense render starts a mount via `ensureRootMount` but never commits
 * (parent unmounts before the promise settles, React discards the
 * subtree, etc.), the commit-phase `useEffect` never runs so neither
 * `bumpMountRef` nor `releaseRootMount` ever touch refs. The per-render
 * token registered here keeps the entry's teardown reachable as long as
 * the fiber's hook state is reachable; once the fiber is released the
 * token becomes GC-eligible and the finalizer drops the orphaned entry
 * and (when requested) unmounts the server-side root. GC timing is
 * non-deterministic — the entry can linger until the next collection —
 * but cleanup is guaranteed eventually, instead of waiting for the whole
 * channel to terminate.
 *
 * `FinalizationRegistry` lands in Chrome 84 / Safari 14.1 / Node 14.6 —
 * universal across the React 19 support matrix — but feature-detect at
 * module load so an older host (kiosk WebViews, embedded shells) still
 * imports without throwing `ReferenceError`. With no registry the
 * orphaned mount lingers until the channel terminates (the pre-fix
 * baseline behaviour), which is acceptable degradation; the committed
 * path is unaffected because it owns its own ref-counted cleanup.
 */
const suspenseSweepRegistry: FinalizationRegistry<SuspenseSweepHoldings> | null =
  typeof FinalizationRegistry !== "undefined"
    ? new FinalizationRegistry<SuspenseSweepHoldings>(__runSuspenseOrphanSweep)
    : null

/**
 * Render-phase safe: returns the existing shared mount entry or creates a new
 * one. Does NOT bump refs (the caller does that on commit). Cancels any
 * pending cleanup timer so a re-mount during the cleanup grace period reuses
 * the existing mount.
 */
function ensureRootMount<M extends StoreModule<R>, R>(
  connection: MusubiConnection<R>,
  options: MountStoreOptions<M, R>
): SharedRootMount {
  const key = storeCacheKey(options)
  const mounts = rootMountsFor(connection)
  const existing = mounts.get(key)

  if (existing) {
    // Keyed by identity only (module|id|params), not by cache options — like
    // params, the first caller to mount an identity wins; later callers sharing
    // that identity reuse its mount (and its cache config).
    cancelCleanupTimer(existing)
    return existing
  }

  const shared: SharedRootMount = {
    key,
    refs: 0,
    promise: Promise.resolve(null as never),
    settled: false,
    failed: false,
    value: null,
    error: null,
    cleanupTimer: null,
    claimers: new Set<string>()
  }

  shared.promise = connection
    .mountStore(options)
    .then((mounted) => {
      shared.settled = true
      shared.value = mounted as unknown as MountedStore<never, unknown>
      return mounted as unknown as MountedStore<never, unknown>
    })
    .catch((cause: unknown) => {
      shared.settled = true
      shared.failed = true
      shared.error = cause instanceof Error ? cause : new Error(String(cause))
      // Don't delete here: the failed entry has to stay long enough for
      // Suspense/effect consumers to observe it. releaseRootMount removes
      // the entry once the last ref drops, so future mounts retry cleanly
      // (no poison).
      throw shared.error
    })
  // Swallow the unhandled rejection from the bare promise; callers that
  // .then() / .catch() this still observe the error normally.
  shared.promise.catch(() => undefined)

  mounts.set(key, shared)
  return shared
}

function cancelCleanupTimer(shared: SharedRootMount): void {
  if (shared.cleanupTimer) {
    clearTimeout(shared.cleanupTimer)
    shared.cleanupTimer = null
  }
}

function bumpMountRef<R>(connection: MusubiConnection<R>, key: string): void {
  const mounts = pendingRootMounts.get(connection as MusubiConnection<unknown>)
  const shared = mounts?.get(key)
  if (!shared) return
  cancelCleanupTimer(shared)
  shared.refs += 1
}

function releaseRootMount<R>(
  connection: MusubiConnection<R>,
  key: string,
  unmountOnCleanup: boolean
): void {
  const mounts = pendingRootMounts.get(connection as MusubiConnection<unknown>)
  const shared = mounts?.get(key)

  if (!mounts || !shared) {
    return
  }

  shared.refs -= 1

  if (shared.refs > 0) {
    return
  }

  if (!unmountOnCleanup) {
    mounts.delete(key)
    return
  }

  shared.cleanupTimer = setTimeout(() => {
    if (shared.refs > 0) {
      return
    }

    mounts.delete(key)

    if (!shared.failed && shared.value) {
      void shared.value.unmount()
    } else if (!shared.failed) {
      void shared.promise.then((mounted) => mounted.unmount()).catch(() => undefined)
    }
  }, 0)
}

function rootMountsFor<R>(
  connection: MusubiConnection<R>
): Map<string, SharedRootMount> {
  const key = connection as MusubiConnection<unknown>
  const existing = pendingRootMounts.get(key)

  if (existing) {
    return existing
  }

  const mounts = new Map<string, SharedRootMount>()
  pendingRootMounts.set(key, mounts)
  return mounts
}

function buildMountOptions<M extends StoreModule<R>, R>(
  options: MountStoreOptions<M, R>
): MountStoreOptions<M, R> {
  return {
    module: options.module,
    id: options.id,
    ...(options.params !== undefined ? { params: options.params } : {}),
    ...(options.cache !== undefined ? { cache: options.cache } : {})
  }
}

