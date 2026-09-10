// Promise-shaped reads over the sync snapshot surface.
//
// `snapshot()` is sync and may be `undefined` (store not mounted yet, or a
// reconnect window that reset the index), and async fields carry their own
// `status: "loading"` phase. Both settle later, driven by patch pushes. These
// helpers turn "settles later" into a Promise so non-React callers can `await`
// it and React callers can suspend on it.
//
// Both are scoped to one proxy: they wake on changes to that store, not on
// every patch the connection applies.

import type { StoreModule, StoreProxy, StoreSnapshot } from "./types"

// One shared "next notification" promise per proxy. React Suspense needs a
// STABLE promise identity across retries: a fresh promise per render makes the
// component re-suspend forever. Proxies are cached per store id, so proxy
// identity is a valid key. The entry is dropped as soon as it settles, so the
// next suspend arms a new one.
const pendingTicks = new WeakMap<object, Promise<void>>()

/**
 * Resolves on the next change that affects `proxy` — its own node, streams, or
 * uploads. A patch touching only a sibling store does not resolve it
 * (`notifySubscribers` skips listeners whose store is unchanged). Repeat calls
 * before it settles return the same Promise.
 *
 *     while (store.snapshot() === undefined) await nextSnapshot(store)
 */
export function nextSnapshot<M extends StoreModule<R>, R>(
  proxy: StoreProxy<M, R>
): Promise<void> {
  const key = proxy as unknown as object
  const existing = pendingTicks.get(key)
  if (existing) return existing

  let unsubscribe: (() => void) | undefined
  let fired = false

  const promise = new Promise<void>((resolve) => {
    const settle = (): void => {
      fired = true
      pendingTicks.delete(key)
      unsubscribe?.()
      resolve()
    }

    unsubscribe = proxy.subscribe(settle)
  })

  if (fired) {
    // A transport that notifies from inside `subscribe` settles the promise
    // before the constructor returns. Drop the subscription (`settle` ran
    // before `unsubscribe` was assigned) and do NOT cache: an already-settled
    // entry would make every later waiter resolve instantly, spinning
    // `waitFor` in a microtask loop.
    unsubscribe?.()
    return promise
  }

  pendingTicks.set(key, promise)
  return promise
}

/**
 * Resolves with the first non-`undefined` value `select` returns, re-running it
 * on every change that affects `proxy`. `select` receives `undefined` while the store node is absent.
 * A throw from `select` rejects the Promise — that is how an async field's
 * `failed` status surfaces.
 *
 *     const title = await waitFor(store, (s) => s?.title)
 *     const rows = await waitFor(store, (s) => {
 *       if (s?.items.status === "failed") throw new Error("items failed")
 *       return s?.items.status === "ok" ? s.items.data : undefined
 *     })
 *
 * ponytail: no AbortSignal. A caller waiting on a condition that never holds
 * keeps one subscription alive until the next change to this store; add
 * cancellation when a caller actually needs to give up early.
 */
export async function waitFor<M extends StoreModule<R>, R, T>(
  proxy: StoreProxy<M, R>,
  select: (snapshot: StoreSnapshot<M, R> | undefined) => T | undefined
): Promise<T> {
  for (;;) {
    const value = select(proxy.snapshot())
    if (value !== undefined) return value
    await nextSnapshot(proxy)
  }
}
