import { afterEach, describe, expect, test, vi } from "vitest"

import type { MusubiCacheEntry, MusubiCachePersister, StorageLike } from "../src/cache"
import type { ConnectionPatchEnvelope, PatchEnvelope } from "../src/types"

type PushStatus = "ok" | "error" | "timeout"
type PushCallback = (payload: unknown) => void

class MockPush {
  private readonly callbacks = new Map<PushStatus, PushCallback[]>()

  receive(status: PushStatus, callback: PushCallback): this {
    const listeners = this.callbacks.get(status) ?? []
    listeners.push(callback)
    this.callbacks.set(status, listeners)
    return this
  }

  resolve(status: PushStatus, payload: unknown): void {
    for (const callback of this.callbacks.get(status) ?? []) {
      callback(payload)
    }
  }
}

class MockChannel {
  readonly pushes: Array<{ event: string; payload: unknown; push: MockPush }> = []
  readonly topic: string

  private readonly eventHandlers = new Map<string, Array<(payload: unknown) => void>>()
  private readonly closeHandlers: Array<(reason: unknown) => void> = []
  private readonly errorHandlers: Array<(reason: unknown) => void> = []
  private readonly joinPush = new MockPush()

  left = false

  constructor(topic: string) {
    this.topic = topic
  }

  on(event: string, callback: (payload: unknown) => void): void {
    const handlers = this.eventHandlers.get(event) ?? []
    handlers.push(callback)
    this.eventHandlers.set(event, handlers)
  }

  onClose(callback: (reason: unknown) => void): void {
    this.closeHandlers.push(callback)
  }

  onError(callback: (reason: unknown) => void): void {
    this.errorHandlers.push(callback)
  }

  join(): MockPush {
    return this.joinPush
  }

  push(event: string, payload: unknown): MockPush {
    const push = new MockPush()
    this.pushes.push({ event, payload, push })
    return push
  }

  leave(): void {
    this.left = true
    for (const callback of this.closeHandlers) {
      callback({ reason: "leave" })
    }
  }

  resolveJoin(payload: unknown = {}): void {
    this.joinPush.resolve("ok", payload)
  }

  emit(event: string, payload: unknown): void {
    for (const callback of this.eventHandlers.get(event) ?? []) {
      callback(payload)
    }
  }
}

class MockSocket {
  readonly channels: MockChannel[] = []
  connected = false

  connect(): void {
    this.connected = true
  }

  onOpen(_callback: () => void): void {}

  disconnect(): void {
    this.connected = false
  }

  channel(topic: string): MockChannel {
    const channel = new MockChannel(topic)
    this.channels.push(channel)
    return channel
  }
}

vi.mock("phoenix", () => ({ Socket: MockSocket }))

type TestStores = {
  "Test.Store": Musubi.StoreDef<
    "Test.Store",
    { title: string; counter: number },
    { rename: { payload: { title: string }; reply: { ok: true } } }
  >
}

function lastChannel(socket: MockSocket): MockChannel {
  const channel = socket.channels.at(-1)
  if (!channel) throw new Error("Missing mock channel")
  return channel
}

function lastPush(channel: MockChannel): { event: string; payload: unknown; push: MockPush } {
  const push = channel.pushes.at(-1)
  if (!push) throw new Error("Missing mock push")
  return push
}

function initialEnvelope(rootId: string, title = "Inbox", counter = 1): ConnectionPatchEnvelope {
  return envelope(rootId, 0, 1, [
    { op: "replace", path: "", value: { title, counter, __musubi_store_id__: [] } }
  ])
}

function envelope(
  rootId: string,
  baseVersion: number,
  version: number,
  ops: PatchEnvelope["ops"]
): ConnectionPatchEnvelope {
  return {
    type: "patch",
    root_id: rootId,
    base_version: baseVersion,
    version,
    ops,
    stream_ops: [],
    upload_ops: [],
    events: []
  }
}

// Persister whose stored entries can be inspected/preset by the test.
function controllablePersister(durable = false): MusubiCachePersister & {
  store: Map<string, MusubiCacheEntry>
  removed: string[]
} {
  const store = new Map<string, MusubiCacheEntry>()
  const removed: string[] = []
  return {
    durable,
    store,
    removed,
    getEntry: (key) => store.get(key),
    setEntry: (key, entry) => {
      store.set(key, entry)
    },
    removeEntry: (key) => {
      removed.push(key)
      store.delete(key)
    },
    clear: () => {
      store.clear()
    }
  }
}

// Drive one mount through its per-root channel join (join IS the mount) + the
// initial patch.
async function mountInitial(
  socket: MockSocket,
  connection: Awaited<ReturnType<typeof openConn>>,
  opts: {
    id: string
    cache?: import("../src/cache").CacheOptions
    title?: string
    counter?: number
  }
) {
  const mountedPromise = connection.mountStore({
    module: "Test.Store",
    id: opts.id,
    ...(opts.cache !== undefined ? { cache: opts.cache } : {})
  })
  await Promise.resolve()
  const channel = lastChannel(socket)
  const rootId = `Test.Store:${opts.id}`
  channel.resolveJoin({ root_id: rootId })
  await Promise.resolve()
  channel.emit("patch", initialEnvelope(rootId, opts.title ?? "Inbox", opts.counter ?? 1))
  return mountedPromise
}

async function openConn(socket: MockSocket) {
  const { connect } = await import("../src/connect")
  // No connection channel — `connect` resolves immediately; per-root channels
  // join lazily on `mountStore`.
  return connect<TestStores>(socket)
}

const nextTask = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 0))

// Release the last handle and let the grace timer (0ms) fire on the next task,
// which flushes the cache writer and leaves the root's channel.
async function unmountAndSettle(
  mounted: { unmount: () => Promise<void> }
): Promise<void> {
  const done = mounted.unmount()
  await nextTask()
  await done
}

describe("client store cache", () => {
  afterEach(() => {
    vi.resetModules()
    vi.restoreAllMocks()
  })

  test("cold mount with cache enabled stays loading until the initial patch (fromCache false)", async () => {
    const socket = new MockSocket()
    const connection = await openConn(socket)

    const mountedPromise = connection.mountStore({
      module: "Test.Store",
      id: "alpha",
      cache: {}
    })
    await Promise.resolve()
    const channel = lastChannel(socket)
    let resolved = false
    void mountedPromise.then(() => {
      resolved = true
    })

    channel.resolveJoin({ root_id: "Test.Store:alpha" })
    await Promise.resolve()
    expect(resolved).toBe(false)

    channel.emit("patch", initialEnvelope("Test.Store:alpha"))
    const mounted = await mountedPromise
    expect(mounted.fromCache).toBe(false)
    expect(mounted.isFetching).toBe(false)
    expect(mounted.store.title).toBe("Inbox")
  })

  test("re-mount of a cached identity seeds stale data before any patch, then swaps", async () => {
    const socket = new MockSocket()
    const connection = await openConn(socket)

    const firstMounted = await mountInitial(socket, connection, { id: "alpha", cache: {} })

    // Teardown flushes the throttled write into the (default memory) cache.
    await unmountAndSettle(firstMounted)

    const secondPromise = connection.mountStore({
      module: "Test.Store",
      id: "alpha",
      cache: {}
    })
    await Promise.resolve()
    // The re-mount opens a fresh per-root channel; its join IS the mount.
    const channel = lastChannel(socket)
    channel.resolveJoin({ root_id: "Test.Store:alpha" })

    // Resolves from cache BEFORE the live initial patch arrives.
    const secondMounted = await secondPromise
    expect(secondMounted.fromCache).toBe(true)
    expect(secondMounted.isFetching).toBe(true)
    expect(secondMounted.store.title).toBe("Inbox")

    const listener = vi.fn()
    secondMounted.store.subscribe(listener)

    channel.emit("patch", initialEnvelope("Test.Store:alpha", "Fresh", 5))
    expect(listener).toHaveBeenCalledTimes(1)
    expect(secondMounted.store.title).toBe("Fresh")
    expect(secondMounted.store.counter).toBe(5)

    await secondMounted.revalidated
    expect(secondMounted.isFetching).toBe(false)
  })

  test("entry older than gcTime is evicted on read and the mount loads cold", async () => {
    const persister = controllablePersister()
    persister.store.set("alpha|Test.Store|null", {
      data: { title: "Stale", counter: 9 },
      updatedAt: Date.now() - 10_000,
      buster: ""
    })

    const socket = new MockSocket()
    const connection = await openConn(socket)

    const mountedPromise = connection.mountStore({
      module: "Test.Store",
      id: "alpha",
      cache: { persister, gcTime: 1000 }
    })
    await Promise.resolve()
    const channel = lastChannel(socket)
    let resolved = false
    void mountedPromise.then(() => {
      resolved = true
    })
    channel.resolveJoin({ root_id: "Test.Store:alpha" })
    await nextTask()

    // Stale entry discarded → no seed → still loading.
    expect(resolved).toBe(false)
    expect(persister.removed).toContain("alpha|Test.Store|null")

    channel.emit("patch", initialEnvelope("Test.Store:alpha"))
    const mounted = await mountedPromise
    expect(mounted.fromCache).toBe(false)
  })

  test("entry with a mismatched buster is discarded and removed", async () => {
    const persister = controllablePersister()
    persister.store.set("alpha|Test.Store|null", {
      data: { title: "Old shape", counter: 1 },
      updatedAt: Date.now(),
      buster: "v1"
    })

    const socket = new MockSocket()
    const connection = await openConn(socket)

    const mountedPromise = connection.mountStore({
      module: "Test.Store",
      id: "alpha",
      cache: { persister, buster: "v2" }
    })
    await Promise.resolve()
    const channel = lastChannel(socket)
    let resolved = false
    void mountedPromise.then(() => {
      resolved = true
    })
    channel.resolveJoin({ root_id: "Test.Store:alpha" })
    await nextTask()

    expect(resolved).toBe(false)
    expect(persister.removed).toContain("alpha|Test.Store|null")

    channel.emit("patch", initialEnvelope("Test.Store:alpha"))
    expect((await mountedPromise).fromCache).toBe(false)
  })

  test("commands dispatched in the stale window queue until revalidation lands", async () => {
    const socket = new MockSocket()
    const connection = await openConn(socket)

    const firstMounted = await mountInitial(socket, connection, { id: "alpha", cache: {} })
    await unmountAndSettle(firstMounted)

    const secondPromise = connection.mountStore({ module: "Test.Store", id: "alpha", cache: {} })
    await Promise.resolve()
    const channel = lastChannel(socket)
    channel.resolveJoin({ root_id: "Test.Store:alpha" })
    const secondMounted = await secondPromise
    expect(secondMounted.fromCache).toBe(true)

    // Dispatch while still version 0 (stale window): must not reject, no
    // command push yet.
    const pushCountBefore = channel.pushes.length
    const replyPromise = secondMounted.store.dispatchCommand("rename", { title: "Queued" })
    await Promise.resolve()
    expect(channel.pushes.length).toBe(pushCountBefore)

    // Live initial patch lands → version 1 → queued command re-dispatches.
    channel.emit("patch", initialEnvelope("Test.Store:alpha"))
    await Promise.resolve()
    await Promise.resolve()
    const commandPush = lastPush(channel)
    expect(commandPush.event).toBe("command")
    commandPush.push.resolve("ok", { ok: true })
    await expect(replyPromise).resolves.toEqual({ ok: true })
  })

  test("a queued command rejects if the connection disconnects before revalidation", async () => {
    const socket = new MockSocket()
    const connection = await openConn(socket)

    const firstMounted = await mountInitial(socket, connection, { id: "alpha", cache: {} })
    await unmountAndSettle(firstMounted)

    const secondPromise = connection.mountStore({ module: "Test.Store", id: "alpha", cache: {} })
    await Promise.resolve()
    lastChannel(socket).resolveJoin({ root_id: "Test.Store:alpha" })
    const secondMounted = await secondPromise

    const replyPromise = secondMounted.store.dispatchCommand("rename", { title: "Doomed" })
    void replyPromise.catch(() => undefined)

    await connection.disconnect()
    await expect(replyPromise).rejects.toThrow(/Disconnected/)
  })

  test("storage persister: throttled write, flush on teardown, restore on re-mount", async () => {
    const backing = new Map<string, string>()
    const storage: StorageLike = {
      getItem: (k) => backing.get(k) ?? null,
      setItem: (k, v) => {
        backing.set(k, v)
      },
      removeItem: (k) => {
        backing.delete(k)
      }
    }
    const { createStorageCachePersister } = await import("../src/cache")
    const persister = createStorageCachePersister(storage)

    const socket = new MockSocket()
    const connection = await openConn(socket)

    const firstMounted = await mountInitial(socket, connection, {
      id: "alpha",
      cache: { persister, buster: "v1" }
    })

    // Throttled: nothing written synchronously after the patch.
    expect(backing.size).toBe(0)

    // Teardown flushes the pending write into storage.
    await unmountAndSettle(firstMounted)
    expect(backing.size).toBe(1)

    // Re-mount restores from storage before the live patch.
    const secondPromise = connection.mountStore({
      module: "Test.Store",
      id: "alpha",
      cache: { persister, buster: "v1" }
    })
    await Promise.resolve()
    lastChannel(socket).resolveJoin({ root_id: "Test.Store:alpha" })
    const secondMounted = await secondPromise
    expect(secondMounted.fromCache).toBe(true)
    expect(secondMounted.store.title).toBe("Inbox")
  })

  test("storage write quota failure is caught and warned, never thrown", async () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {})
    const storage: StorageLike = {
      getItem: () => null,
      setItem: () => {
        throw new Error("QuotaExceededError")
      },
      removeItem: () => {}
    }
    const { createStorageCachePersister } = await import("../src/cache")
    const persister = createStorageCachePersister(storage)

    const socket = new MockSocket()
    const connection = await openConn(socket)

    const mounted = await mountInitial(socket, connection, {
      id: "alpha",
      cache: { persister, buster: "v1" }
    })
    // Flush the throttled write (which throws inside setItem → caught).
    await unmountAndSettle(mounted)

    expect(warnSpy).toHaveBeenCalledWith(
      "[musubi] cache persist failed (quota / serialization):",
      expect.any(Error)
    )
  })

  test("durable persister without a buster emits a dev warning", async () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {})
    const persister = controllablePersister(true)

    const socket = new MockSocket()
    const connection = await openConn(socket)

    const mountedPromise = connection.mountStore({
      module: "Test.Store",
      id: "alpha",
      cache: { persister }
    })
    await Promise.resolve()
    const channel = lastChannel(socket)
    channel.resolveJoin({ root_id: "Test.Store:alpha" })
    await Promise.resolve()
    channel.emit("patch", initialEnvelope("Test.Store:alpha"))
    await mountedPromise

    expect(warnSpy.mock.calls.some((c) => String(c[0]).includes("durable cache persister"))).toBe(
      true
    )
  })

  test("initialData seeds the first mount and writes through to the persister", async () => {
    const persister = controllablePersister()

    const socket = new MockSocket()
    const connection = await openConn(socket)

    const mountedPromise = connection.mountStore({
      module: "Test.Store",
      id: "alpha",
      cache: { persister, initialData: { title: "Seeded", counter: 0, __musubi_store_id__: [] } }
    })
    await Promise.resolve()
    const channel = lastChannel(socket)
    channel.resolveJoin({ root_id: "Test.Store:alpha" })

    const mounted = await mountedPromise
    expect(mounted.fromCache).toBe(true)
    expect(mounted.store.title).toBe("Seeded")
    expect(persister.store.get("alpha|Test.Store|null")?.data).toEqual({
      title: "Seeded",
      counter: 0,
      __musubi_store_id__: []
    })

    channel.emit("patch", initialEnvelope("Test.Store:alpha"))
    await mounted.revalidated
  })

  test("clearStoreCache removes one entry or all entries", async () => {
    const persister = controllablePersister()

    const socket = new MockSocket()
    const connection = await openConn(socket)

    const mounted = await mountInitial(socket, connection, {
      id: "alpha",
      cache: { persister }
    })
    await unmountAndSettle(mounted)
    expect(persister.store.has("alpha|Test.Store|null")).toBe(true)

    await connection.clearStoreCache({ module: "Test.Store", id: "alpha" })
    expect(persister.store.has("alpha|Test.Store|null")).toBe(false)

    // Whole-connection clear wipes the default memory persister too.
    await connection.clearStoreCache()
  })

  test("no cache option behaves exactly like an uncached mount", async () => {
    const socket = new MockSocket()
    const connection = await openConn(socket)

    const mounted = await mountInitial(socket, connection, { id: "alpha" })
    expect(mounted.fromCache).toBe(false)
    expect(mounted.isFetching).toBe(false)
    expect(mounted.store.title).toBe("Inbox")
  })

  test("storage getEntry discards malformed entries and survives a throwing getItem", async () => {
    const backing = new Map<string, string>()
    backing.set("musubi:cache:k", JSON.stringify({ data: { v: 1 } })) // missing updatedAt/buster
    const removed: string[] = []
    const storage: StorageLike = {
      getItem: (k) => backing.get(k) ?? null,
      setItem: (k, v) => {
        backing.set(k, v)
      },
      removeItem: (k) => {
        removed.push(k)
        backing.delete(k)
      }
    }
    const { createStorageCachePersister } = await import("../src/cache")
    const persister = createStorageCachePersister(storage)

    expect(await persister.getEntry("k")).toBeUndefined()
    expect(removed).toContain("musubi:cache:k")

    const throwingStorage: StorageLike = {
      getItem: () => {
        throw new Error("SecurityError")
      },
      setItem: () => {},
      removeItem: () => {}
    }
    const guarded = createStorageCachePersister(throwingStorage)
    expect(await guarded.getEntry("k")).toBeUndefined()
  })

  test("storage clear works when StorageLike exposes key() but not length", async () => {
    const backing = new Map<string, string>([
      ["musubi:cache:a", "1"],
      ["other:b", "2"],
      ["musubi:cache:c", "3"]
    ])
    const keys = (): string[] => [...backing.keys()]
    const storage: StorageLike = {
      getItem: (k) => backing.get(k) ?? null,
      setItem: (k, v) => {
        backing.set(k, v)
      },
      removeItem: (k) => {
        backing.delete(k)
      },
      key: (i) => keys()[i] ?? null
      // no `length`
    }
    const { createStorageCachePersister } = await import("../src/cache")
    const persister = createStorageCachePersister(storage)

    await persister.clear?.()
    expect(backing.has("musubi:cache:a")).toBe(false)
    expect(backing.has("musubi:cache:c")).toBe(false)
    expect(backing.has("other:b")).toBe(true)
  })

  test("a throwing custom persister degrades to a cold mount", async () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {})
    const persister: MusubiCachePersister = {
      durable: false,
      getEntry: () => {
        throw new Error("persister boom")
      },
      setEntry: () => {},
      removeEntry: () => {}
    }

    const socket = new MockSocket()
    const connection = await openConn(socket)

    const mounted = await mountInitial(socket, connection, {
      id: "alpha",
      cache: { persister }
    })
    expect(mounted.fromCache).toBe(false)
    expect(mounted.store.title).toBe("Inbox")
    expect(
      warnSpy.mock.calls.some((c) => String(c[0]).includes("falling back to a cold mount"))
    ).toBe(true)
  })
})
