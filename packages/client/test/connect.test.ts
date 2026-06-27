import { afterEach, beforeEach, describe, expect, test, vi } from "vitest"

import type { PatchEnvelope, ConnectionPatchEnvelope, SnapshotValue } from "../src/types"
import type { MountedStore, MusubiConnection } from "../src/connect"

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
  readonly joinPayload: unknown
  readonly topic: string

  private readonly eventHandlers = new Map<string, Array<(payload: unknown) => void>>()
  private readonly closeHandlers: Array<(reason: unknown) => void> = []
  private readonly errorHandlers: Array<(reason: unknown) => void> = []
  private readonly joinPush = new MockPush()

  left = false

  constructor(topic: string, joinPayload?: unknown) {
    this.topic = topic
    this.joinPayload = joinPayload
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

  // Resolve the join push with `:ok`. Phoenix re-fires the joinPush receive
  // hooks on every (re)join, so calling this more than once models a reconnect
  // rejoin on the same channel object.
  resolveJoin(payload: unknown = {}): void {
    this.joinPush.resolve("ok", payload)
  }

  failJoin(payload: unknown = {}): void {
    this.joinPush.resolve("error", payload)
  }

  emit(event: string, payload: unknown): void {
    for (const callback of this.eventHandlers.get(event) ?? []) {
      callback(payload)
    }
  }

  // Transport drop: Phoenix fires the channel `onError` (state → errored) and
  // schedules a rejoin. We model the drop as `onClose` here — the runtime treats
  // both the same (keep last-good, await rejoin).
  disconnect(reason: unknown): void {
    for (const callback of this.closeHandlers) {
      callback(reason)
    }
  }

  fail(reason: unknown): void {
    for (const callback of this.errorHandlers) {
      callback(reason)
    }
  }
}

class MockSocket {
  static instances: MockSocket[] = []

  readonly channels: MockChannel[] = []
  connected = false

  constructor(_url?: string, _options?: unknown) {
    MockSocket.instances.push(this)
  }

  connect(): void {
    this.connected = true
  }

  channel(topic: string, payload?: unknown): MockChannel {
    const channel = new MockChannel(topic, payload)
    this.channels.push(channel)
    return channel
  }
}

vi.mock("phoenix", () => ({
  Socket: MockSocket
}))

type TestStores = {
  "Test.Store": Musubi.StoreDef<
    "Test.Store",
    {
      title: string
      child: Musubi.StoreField<"Test.Child">
      counter: number
      feed: {
        messages: Musubi.StreamField<{ body: string }>
      }
      async_messages: Musubi.AsyncField<Musubi.StreamField<{ id: string; body: string }>>
      metadata: {
        messages: string
      }
      users: Musubi.StreamField<{ id: string; name: string }>
    },
    {
      rename: {
        payload: { title: string }
        reply: { ok: true }
      }
    }
  >

  "Test.Child": Musubi.StoreDef<
    "Test.Child",
    {
      count: number
    },
    {}
  >

  "Test.Other": Musubi.StoreDef<
    "Test.Other",
    {
      label: string
    },
    {}
  >
}

type Equal<Left, Right> =
  (<T>() => T extends Left ? 1 : 2) extends (<T>() => T extends Right ? 1 : 2)
  ? true
  : false

type Assert<T extends true> = T

type PlainObjectSnapshot = Assert<
  Equal<SnapshotValue<{ title: string }>, { title: string }>
>

type EmptyObjectSnapshot = Assert<Equal<SnapshotValue<{}>, {}>>

describe("connect", () => {
  beforeEach(() => {
    MockSocket.instances = []
  })

  afterEach(() => {
    vi.resetModules()
  })

  test("connect opens the transport and resolves without joining a channel", async () => {
    const { connect } = await import("../src/connect")
    const socket = new MockSocket()
    const connection = await connect<TestStores>(socket)

    // No connection-level channel — each root channel joins lazily on mount.
    expect(socket.connected).toBe(true)
    expect(socket.channels.length).toBe(0)
    expect(connection).toBeTruthy()
  })

  test("mountStore requires an explicit id at compile time", async () => {
    const { connect } = await import("../src/connect")
    const socket = new MockSocket()
    const connection = await connect<TestStores>(socket)

    if (false) {
      // @ts-expect-error -- id is required
      void connection.mountStore({ module: "Test.Store" })
    }

    expect(connection).toBeTruthy()
  })

  test("mountStore joins a per-root channel and resolves only after the initial envelope", async () => {
    const socket = new MockSocket()
    const connection = await openConnection(socket)

    let resolved = false
    const mountedPromise = connection.mountStore({
      module: "Test.Store",
      id: "alpha-1",
      params: { room_id: "general" }
    })
    await Promise.resolve()

    const channel = lastChannel(socket)
    expect(channel.topic).toBe("musubi:connection:Test.Store:alpha-1")
    expect(channel.joinPayload).toEqual({
      module: "Test.Store",
      id: "alpha-1",
      params: { room_id: "general" }
    })

    void mountedPromise.then(() => {
      resolved = true
    })

    channel.resolveJoin({ root_id: "Test.Store:alpha-1" })
    await Promise.resolve()
    expect(resolved).toBe(false)

    channel.emit("patch", initialConnectionEnvelope("Test.Store:alpha-1", rootState()))

    const { store: proxy } = await mountedPromise
    expect(proxy.title).toBe("Inbox")
    expect(proxy.counter).toBe(1)
    expect(proxy.__musubi_store_id__).toEqual([])
  })

  test("nested store field returns a stable child proxy", async () => {
    const socket = new MockSocket()
    const connection = await openConnection(socket)
    const { mounted } = await mountRoot(socket, connection, { id: "alpha-1" })

    expect(mounted.store.child).toBe(mounted.store.child)
    expect(mounted.store.child.count).toBe(1)
  })

  test("dispatchCommand pushes the command on the root channel without a root_id", async () => {
    const socket = new MockSocket()
    const connection = await openConnection(socket)
    const { mounted, channel } = await mountRoot(socket, connection, { id: "alpha-1" })

    const replyPromise = mounted.store.dispatchCommand("rename", { title: "Outbox" })

    const commandPush = lastPush(channel)
    expect(commandPush.event).toBe("command")
    // One root per channel — the command no longer carries a `root_id`.
    expect(commandPush.payload).toEqual({
      store_id: [],
      name: "rename",
      payload: { title: "Outbox" }
    })

    commandPush.push.resolve("ok", { ok: true })
    await expect(replyPromise).resolves.toEqual({ ok: true })
  })

  test("distinct roots get distinct channels and patches route per channel", async () => {
    const socket = new MockSocket()
    const connection = await openConnection(socket)

    const { mounted: alpha, channel: alphaChannel } = await mountRoot(socket, connection, {
      id: "alpha-1"
    })
    const { mounted: beta, channel: betaChannel } = await mountRoot(socket, connection, {
      id: "beta-1",
      title: "Secondary"
    })

    expect(alphaChannel).not.toBe(betaChannel)
    expect(socket.channels.length).toBe(2)

    const alphaListener = vi.fn()
    const betaListener = vi.fn()
    alpha.store.subscribe(alphaListener)
    beta.store.subscribe(betaListener)

    betaChannel.emit(
      "patch",
      connectionEnvelope("Test.Store:beta-1", 1, 2, [{ op: "replace", path: "/counter", value: 9 }], [])
    )

    expect(alpha.store.counter).toBe(1)
    expect(beta.store.counter).toBe(9)
    expect(alphaListener).not.toHaveBeenCalled()
    expect(betaListener).toHaveBeenCalledTimes(1)
  })

  test("distinct modules sharing one caller id get distinct channels and roots", async () => {
    const socket = new MockSocket()
    const connection = await openConnection(socket)

    const { mounted: store, channel: storeChannel } = await mountRoot(socket, connection, {
      module: "Test.Store",
      id: "shared"
    })

    const otherPromise = connection.mountStore({ module: "Test.Other", id: "shared" })
    await Promise.resolve()
    const otherChannel = lastChannel(socket)
    expect(otherChannel).not.toBe(storeChannel)
    expect(otherChannel.topic).toBe("musubi:connection:Test.Other:shared")
    otherChannel.resolveJoin({ root_id: "Test.Other:shared" })
    await Promise.resolve()
    otherChannel.emit(
      "patch",
      connectionEnvelope("Test.Other:shared", 0, 1, [{ op: "replace", path: "", value: { label: "other" } }], [])
    )
    await otherPromise

    storeChannel.emit(
      "patch",
      connectionEnvelope("Test.Store:shared", 1, 2, [{ op: "replace", path: "/counter", value: 7 }], [])
    )

    expect(store.store.counter).toBe(7)
  })

  test("duplicate ids reuse the existing root without opening a second channel", async () => {
    const socket = new MockSocket()
    const connection = await openConnection(socket)

    const { mounted: first } = await mountRoot(socket, connection, { id: "shared-root" })
    expect(socket.channels.length).toBe(1)

    // Client-side dedup: the second mount for the same (module, id) aliases the
    // existing RootConnection synchronously — no second channel, no second join.
    const second = await connection.mountStore({ module: "Test.Store", id: "shared-root" })
    expect(second.store).toBe(first.store)
    expect(socket.channels.length).toBe(1)
  })

  test("an in-window remount cancels the grace teardown and reuses the root", async () => {
    const socket = new MockSocket()
    const connection = await openConnection(socket)
    const { mounted: first, channel } = await mountRoot(socket, connection, { id: "shared" })

    // Last caller releases — grace timer scheduled (0ms) but not yet fired.
    void first.unmount()
    expect(channel.left).toBe(false)

    // In-window remount: aliases the existing root, cancels the grace teardown.
    const second = await connection.mountStore({ module: "Test.Store", id: "shared" })
    expect(second.store).toBe(first.store)

    // Let the grace timer's would-have-fired tick pass; the channel was never
    // left because refCount went back to 1 before it ran.
    await nextTask()
    expect(channel.left).toBe(false)
    expect(socket.channels.length).toBe(1)
  })

  test("the last unmount leaves the root channel and resets the runtime", async () => {
    const socket = new MockSocket()
    const connection = await openConnection(socket)
    const { mounted, channel } = await mountRoot(socket, connection, { id: "alpha-1" })

    const unmountPromise = mounted.unmount()
    // Leaving the channel happens after the grace timer fires (next task).
    await nextTask()
    await unmountPromise

    expect(channel.left).toBe(true)
    expect(mounted.store.title).toBeUndefined()
    await expect(mounted.store.dispatchCommand("rename", { title: "Gone" })).rejects.toThrow(
      /Store is not connected/
    )
  })

  test("snapshot returns a plain object tree", async () => {
    const socket = new MockSocket()
    const connection = await openConnection(socket)
    const { mounted } = await mountRoot(socket, connection, { id: "alpha-1" })

    expect(mounted.store.snapshot()).toEqual({
      __musubi_store_id__: [],
      title: "Inbox",
      counter: 1,
      feed: { messages: [] },
      async_messages: { status: "loading", data: [], error: null },
      metadata: { messages: "literal" },
      users: [],
      child: { __musubi_store_id__: ["child"], count: 1 }
    })
  })

  test("stream markers resolve at nested paths", async () => {
    const socket = new MockSocket()
    const connection = await openConnection(socket)
    const mountedPromise = connection.mountStore({ module: "Test.Store", id: "alpha-1" })
    await Promise.resolve()
    const channel = lastChannel(socket)
    channel.resolveJoin({ root_id: "Test.Store:alpha-1" })
    await Promise.resolve()
    channel.emit(
      "patch",
      connectionEnvelope(
        "Test.Store:alpha-1",
        0,
        1,
        [{ op: "replace", path: "", value: rootState() }],
        [
          {
            op: "insert",
            stream: "messages",
            ref: "1",
            store_id: [],
            item_key: "messages-1",
            at: -1,
            item: { body: "hello" },
            limit: null
          },
          {
            op: "insert",
            stream: "async_messages",
            ref: "3",
            store_id: [],
            item_key: "async_messages-1",
            at: -1,
            item: { id: "a1", body: "loaded" },
            limit: null
          },
          {
            op: "insert",
            stream: "users",
            ref: "2",
            store_id: [],
            item_key: "users-u1",
            at: -1,
            item: { id: "u1", name: "Ada" },
            limit: null
          }
        ]
      )
    )

    const { store: proxy } = await mountedPromise

    expect(proxy.feed.messages).toEqual([{ body: "hello" }])
    expect(proxy.async_messages).toEqual({
      status: "loading",
      data: [{ id: "a1", body: "loaded" }],
      error: null
    })
    expect(proxy.metadata.messages).toBe("literal")
    expect(proxy.users).toEqual([{ id: "u1", name: "Ada" }])
    expect(proxy.snapshot()?.feed.messages).toEqual([{ body: "hello" }])
  })

  test("disconnect leaves every root channel", async () => {
    const socket = new MockSocket()
    const connection = await openConnection(socket)
    const { channel: alpha } = await mountRoot(socket, connection, { id: "alpha-1" })
    const { channel: beta } = await mountRoot(socket, connection, { id: "beta-1" })

    await connection.disconnect()

    expect(alpha.left).toBe(true)
    expect(beta.left).toBe(true)
  })

  test("disconnect mid-join rejects mountStore promptly without an unhandled rejection", async () => {
    const unhandled: unknown[] = []
    const onUnhandled = (reason: unknown): void => {
      unhandled.push(reason)
    }
    process.on("unhandledRejection", onUnhandled)

    try {
      const socket = new MockSocket()
      const connection = await openConnection(socket)

      const mountedPromise = connection.mountStore({ module: "Test.Store", id: "alpha-1" })
      await Promise.resolve()
      const channel = lastChannel(socket)
      // Join not resolved yet — disconnect before the join reply lands.
      await connection.disconnect()

      await expect(mountedPromise).rejects.toThrow(/Disconnected/)
      await nextTask()

      expect(unhandled).toEqual([])
      expect(channel.left).toBe(true)
    } finally {
      process.off("unhandledRejection", onUnhandled)
    }
  })

  test("disconnect between join ok and initial patch rejects mountStore promptly", async () => {
    const unhandled: unknown[] = []
    const onUnhandled = (reason: unknown): void => {
      unhandled.push(reason)
    }
    process.on("unhandledRejection", onUnhandled)

    try {
      const socket = new MockSocket()
      const connection = await openConnection(socket)

      const mountedPromise = connection.mountStore({ module: "Test.Store", id: "alpha-1" })
      await Promise.resolve()
      const channel = lastChannel(socket)
      channel.resolveJoin({ root_id: "Test.Store:alpha-1" })
      await Promise.resolve()

      // Initial patch never arrives — disconnect now.
      await connection.disconnect()

      await expect(mountedPromise).rejects.toThrow(/Disconnected/)
      await nextTask()

      expect(unhandled).toEqual([])
      expect(channel.left).toBe(true)
    } finally {
      process.off("unhandledRejection", onUnhandled)
    }
  })

  test("keeps last-good on hard disconnect and recovers via the same channel's rejoin", async () => {
    const socket = new MockSocket()
    const connection = await openConnection(socket)
    const { mounted, channel } = await mountRoot(socket, connection, { id: "alpha-1", title: "Inbox" })

    expect(mounted.store.snapshot()?.title).toBe("Inbox")
    expect(socket.channels.length).toBe(1)

    // Hard transport drop: channel onClose → keep last-good, version → 0.
    channel.disconnect({ reason: "socket closed" })
    expect(mounted.store.snapshot()?.title).toBe("Inbox")
    expect(mounted.store.title).toBe("Inbox")

    // Phoenix auto-rejoins the SAME channel object (no new channel) and re-fires
    // the join("ok") hook; the server restarts the root and emits a fresh patch.
    expect(socket.channels.length).toBe(1)
    channel.resolveJoin({ root_id: "Test.Store:alpha-1" })
    await Promise.resolve()
    channel.emit("patch", initialConnectionEnvelope("Test.Store:alpha-1", rootState("Fresh")))
    await Promise.resolve()

    expect(mounted.store.snapshot()?.title).toBe("Fresh")
    expect(mounted.store.title).toBe("Fresh")

    // Commands work again after recovery.
    const replyPromise = mounted.store.dispatchCommand("rename", { title: "X" })
    const commandPush = lastPush(channel)
    expect(commandPush.event).toBe("command")
    commandPush.push.resolve("ok", { ok: true })
    await expect(replyPromise).resolves.toEqual({ ok: true })
  })

  test("serves last-good snapshot through the version-mismatch recovery window", async () => {
    const socket = new MockSocket()
    const connection = await openConnection(socket)
    const { mounted, channel } = await mountRoot(socket, connection, { id: "alpha-1", title: "Inbox" })

    expect(mounted.store.snapshot()?.title).toBe("Inbox")

    // Version-mismatch patch → recovery leaves the diverged channel and joins a
    // fresh one. Last-good keeps rendering through the window.
    channel.emit(
      "patch",
      connectionEnvelope("Test.Store:alpha-1", 99, 100, [{ op: "replace", path: "/counter", value: 99 }], [])
    )
    await Promise.resolve()

    expect(channel.left).toBe(true)
    expect(mounted.store.snapshot()?.title).toBe("Inbox")
    expect(mounted.store.snapshot()?.counter).toBe(1)

    const recoveryChannel = lastChannel(socket)
    expect(recoveryChannel).not.toBe(channel)
    recoveryChannel.resolveJoin({ root_id: "Test.Store:alpha-1" })
    await Promise.resolve()
    recoveryChannel.emit("patch", initialConnectionEnvelope("Test.Store:alpha-1", rootState("Fresh")))
    await nextTask()

    expect(mounted.store.snapshot()?.title).toBe("Fresh")
  })

  test("version-mismatch recovery that fails to rejoin disconnects cleanly", async () => {
    const unhandled: unknown[] = []
    const onUnhandled = (reason: unknown): void => {
      unhandled.push(reason)
    }
    process.on("unhandledRejection", onUnhandled)
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {})

    try {
      const socket = new MockSocket()
      const connection = await openConnection(socket)
      const { channel } = await mountRoot(socket, connection, { id: "alpha-1" })

      channel.emit(
        "patch",
        connectionEnvelope("Test.Store:alpha-1", 99, 100, [{ op: "replace", path: "/counter", value: 99 }], [])
      )
      await Promise.resolve()

      // The recreate join fails — recovery catches, force-disconnects, logs.
      const recoveryChannel = lastChannel(socket)
      expect(recoveryChannel).not.toBe(channel)
      recoveryChannel.failJoin({ reason: "unauthorized" })
      await nextTask()

      expect(unhandled).toEqual([])
      expect(errorSpy).toHaveBeenCalledWith("[musubi] root recovery failed:", expect.any(Error))
      expect(recoveryChannel.left).toBe(true)
    } finally {
      errorSpy.mockRestore()
      process.off("unhandledRejection", onUnhandled)
    }
  })
})

async function openConnection(socket: MockSocket): Promise<MusubiConnection<TestStores>> {
  const { connect } = await import("../src/connect")
  return connect<TestStores>(socket)
}

// Drive a fresh root mount to completion through its per-root channel: join
// (which IS the mount) then the initial patch. Returns the live MountedStore and
// the channel it joined.
async function mountRoot(
  socket: MockSocket,
  connection: MusubiConnection<TestStores>,
  opts: { module?: keyof TestStores; id: string; params?: Record<string, unknown>; title?: string }
): Promise<{ mounted: MountedStore<keyof TestStores & string, TestStores>; channel: MockChannel; rootId: string }> {
  const module = (opts.module ?? "Test.Store") as keyof TestStores & string
  const mountedPromise = connection.mountStore({
    module,
    id: opts.id,
    ...(opts.params !== undefined ? { params: opts.params } : {})
  })
  await Promise.resolve()
  const channel = lastChannel(socket)
  const rootId = `${module}:${opts.id}`
  channel.resolveJoin({ root_id: rootId })
  await Promise.resolve()
  channel.emit("patch", initialConnectionEnvelope(rootId, rootState(opts.title ?? "Inbox")))
  const mounted = await mountedPromise
  return { mounted, channel, rootId }
}

const nextTask = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 0))

function lastChannel(socket: MockSocket): MockChannel {
  const channel = socket.channels.at(-1)

  if (!channel) {
    throw new Error("Missing mock channel")
  }

  return channel
}

function lastPush(channel: MockChannel): {
  event: string
  payload: unknown
  push: MockPush
} {
  const push = channel.pushes.at(-1)

  if (!push) {
    throw new Error("Missing mock push")
  }

  return push
}

function initialConnectionEnvelope(
  rootId: string,
  value: Record<string, unknown>
): ConnectionPatchEnvelope {
  return connectionEnvelope(rootId, 0, 1, [{ op: "replace", path: "", value }], [])
}

function connectionEnvelope(
  rootId: string,
  baseVersion: number,
  version: number,
  ops: PatchEnvelope["ops"],
  streamOps: PatchEnvelope["stream_ops"]
): ConnectionPatchEnvelope {
  return {
    type: "patch",
    root_id: rootId,
    base_version: baseVersion,
    version,
    ops,
    stream_ops: streamOps
  }
}

function rootState(title = "Inbox"): Record<string, unknown> {
  return {
    title,
    counter: 1,
    child: {
      count: 1,
      __musubi_store_id__: ["child"]
    },
    feed: {
      messages: { __musubi_stream__: "messages" }
    },
    async_messages: {
      __musubi_async__: true,
      status: "loading",
      result: { __musubi_stream__: "async_messages" },
      reason: null
    },
    metadata: {
      messages: "literal"
    },
    users: { __musubi_stream__: "users" },
    __musubi_store_id__: []
  }
}
