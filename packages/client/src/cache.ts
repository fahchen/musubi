// Client-side store cache: pluggable persistence for stale-while-revalidate
// mounts. A mount with `cache` options seeds last-known state immediately while
// the live mount revalidates in the background (see runtime.ts).

export type MaybePromise<T> = T | Promise<T>

export interface MusubiCacheEntry {
  data: unknown
  updatedAt: number
  buster: string
}

export interface MusubiCachePersister {
  getEntry(key: string): MaybePromise<MusubiCacheEntry | undefined>
  setEntry(key: string, entry: MusubiCacheEntry): MaybePromise<void>
  removeEntry(key: string): MaybePromise<void>
  clear?(): MaybePromise<void>
  // Storage-backed adapters set this so the runtime can warn when a durable
  // cache is used without a `buster` (stale shape survives across deploys).
  readonly durable?: boolean
}

export interface CacheOptions {
  gcTime?: number
  persister?: MusubiCachePersister
  buster?: string
  initialData?: unknown
}

export const DEFAULT_GC_MS = 300_000

// Per-store identity key. `@musubi/react` imports this for its mount-key so a
// store mounted by either layer maps to the same cache slot.
export function storeCacheKey(target: {
  module: string
  id: string
  params?: Record<string, unknown>
}): string {
  return `${target.id}|${target.module}|${canonicalStringify(target.params ?? null)}`
}

export function canonicalStringify(value: unknown): string {
  if (value === undefined) return "null"
  if (value === null || typeof value !== "object") return JSON.stringify(value)
  if (Array.isArray(value)) return `[${value.map(canonicalStringify).join(",")}]`
  const obj = value as Record<string, unknown>
  const keys = Object.keys(obj)
    .filter((k) => obj[k] !== undefined)
    .sort()
  return `{${keys.map((k) => `${JSON.stringify(k)}:${canonicalStringify(obj[k])}`).join(",")}}`
}

export function createMemoryPersister(): MusubiCachePersister {
  const store = new Map<string, MusubiCacheEntry>()
  return {
    durable: false,
    getEntry: (key) => store.get(key),
    setEntry: (key, entry) => {
      store.set(key, entry)
    },
    removeEntry: (key) => {
      store.delete(key)
    },
    clear: () => {
      store.clear()
    }
  }
}

export interface StorageLike {
  getItem(key: string): string | null
  setItem(key: string, value: string): void
  removeItem(key: string): void
  key?(index: number): string | null
  readonly length?: number
}

/**
 * Persist cache entries into a Web Storage-like backend (localStorage /
 * sessionStorage). Entries are namespaced under `prefix` and JSON-encoded.
 * Quota / serialization failures are swallowed (logged) — a failed write
 * degrades to "no cache", never throws into the patch path.
 */
export function createStorageCachePersister(
  storage: StorageLike,
  opts: { prefix?: string } = {}
): MusubiCachePersister {
  const prefix = opts.prefix ?? "musubi:cache:"
  const storageKey = (key: string): string => prefix + key
  const safeRemove = (storeKey: string): void => {
    try {
      storage.removeItem(storeKey)
    } catch {
      // best-effort: a throwing storage shouldn't break cache maintenance.
    }
  }

  return {
    durable: true,
    getEntry: (key) => {
      let raw: string | null
      try {
        raw = storage.getItem(storageKey(key))
      } catch {
        return undefined
      }
      if (raw === null) return undefined
      let parsed: unknown
      try {
        parsed = JSON.parse(raw)
      } catch {
        safeRemove(storageKey(key))
        return undefined
      }
      // Drop malformed/older shapes (missing updatedAt/buster, wrong types)
      // so they can't slip into seeding or eviction.
      if (!isCacheEntry(parsed)) {
        safeRemove(storageKey(key))
        return undefined
      }
      return parsed
    },
    setEntry: (key, entry) => {
      try {
        storage.setItem(storageKey(key), JSON.stringify(entry))
      } catch (error) {
        // eslint-disable-next-line no-console
        console.warn("[musubi] cache persist failed (quota / serialization):", error)
      }
    },
    removeEntry: (key) => {
      safeRemove(storageKey(key))
    },
    clear: () => {
      if (typeof storage.key !== "function") return
      const doomed: string[] = []
      // `length` is optional; when absent, walk `key(i)` until it returns null.
      for (let i = 0; storage.length === undefined || i < storage.length; i++) {
        let k: string | null
        try {
          k = storage.key(i)
        } catch {
          break
        }
        if (k === null) break
        if (k.startsWith(prefix)) doomed.push(k)
      }
      for (const k of doomed) safeRemove(k)
    }
  }
}

function isCacheEntry(value: unknown): value is MusubiCacheEntry {
  if (typeof value !== "object" || value === null) return false
  const entry = value as Record<string, unknown>
  return (
    "data" in entry &&
    typeof entry.updatedAt === "number" &&
    typeof entry.buster === "string"
  )
}

export interface ThrottledWriter {
  schedule(value: MusubiCacheEntry): void
  flush(): void
  cancel(): void
}

export const CACHE_PERSIST_THROTTLE_MS = 1000

/**
 * Trailing throttle for a single cache key: collapses a burst of patches into
 * at most one write per `intervalMs`, always persisting the latest value. The
 * write itself is fire-and-forget; rejections are logged, never thrown.
 */
export function createThrottledWriter(
  key: string,
  persister: MusubiCachePersister,
  intervalMs: number = CACHE_PERSIST_THROTTLE_MS
): ThrottledWriter {
  let timer: ReturnType<typeof setTimeout> | null = null
  let pending: MusubiCacheEntry | null = null

  const write = (entry: MusubiCacheEntry): void => {
    try {
      Promise.resolve(persister.setEntry(key, entry)).catch((error: unknown) => {
        // eslint-disable-next-line no-console
        console.warn("[musubi] cache write failed:", error)
      })
    } catch (error) {
      // eslint-disable-next-line no-console
      console.warn("[musubi] cache write failed:", error)
    }
  }

  const flush = (): void => {
    if (timer !== null) {
      clearTimeout(timer)
      timer = null
    }
    if (pending !== null) {
      const entry = pending
      pending = null
      write(entry)
    }
  }

  return {
    schedule: (value) => {
      pending = value
      if (timer === null) {
        timer = setTimeout(flush, intervalMs)
      }
    },
    flush,
    cancel: () => {
      if (timer !== null) {
        clearTimeout(timer)
        timer = null
      }
      pending = null
    }
  }
}
