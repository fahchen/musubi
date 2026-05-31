import { afterEach, beforeEach, describe, expect, test, vi } from "vitest"

import type { PatchEnvelope, ConnectionPatchEnvelope, SnapshotValue } from "../src/types"

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

  resolveJoin(payload: unknown = {}): void {
    this.joinPush.resolve("ok", payload)
  }

  emit(event: string, payload: unknown): void {
    for (const callback of this.eventHandlers.get(event) ?? []) {
      callback(payload)
    }
  }

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

  disconnect(): void {
    this.connected = false

    for (const channel of this.channels) {
      channel.disconnect({ reason: "socket closed" })
    }
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

  test("joins one Musubi connection channel", async () => {
    const { connect } = await import("../src/connect")
    const socket = new MockSocket()
    const connectionPromise = connect<TestStores>(socket)

    const channel = lastChannel(socket)
    expect(channel.joinPayload).toEqual({})
    expect(socket.connected).toBe(true)

    channel.resolveJoin()

    const connection = await connectionPromise
    expect(channel.topic).toBe("musubi:connection")
    expect(connection).toBeTruthy()
  })

  test("mountStore requires an explicit id at compile time", async () => {
    const { connect } = await import("../src/connect")
    const socket = new MockSocket()
    const connectionPromise = connect<TestStores>(socket)
    const channel = lastChannel(socket)
    channel.resolveJoin()
    const connection = await connectionPromise

    if (false) {
      // @ts-expect-error -- id is required
      void connection.mountStore({ module: "Test.Store" })
    }

    expect(channel.topic).toBe("musubi:connection")
    expect(connection).toBeTruthy()
  })

  test("mountStore resolves only after the root initial envelope is applied", async () => {
    const { connect } = await import("../src/connect")
    const socket = new MockSocket()
    const connectionPromise = connect<TestStores>(socket)
    const channel = lastChannel(socket)
    channel.resolveJoin()
    const connection = await connectionPromise
    let resolved = false

    const mountedPromise = connection.mountStore({
      module: "Test.Store",
      id: "alpha-1",
      params: { room_id: "general" }
    })
    await Promise.resolve()

    void mountedPromise.then(() => {
      resolved = true
    })

    const mountPush = lastPush(channel)
    expect(mountPush.event).toBe("mount")
    expect(mountPush.payload).toEqual({
      module: "Test.Store",
      id: "Test.Store:alpha-1",
      params: { room_id: "general" }
    })

    mountPush.push.resolve("ok", { root_id: "Test.Store:alpha-1" })
    await Promise.resolve()
    expect(resolved).toBe(false)

    channel.emit("patch", initialConnectionEnvelope("Test.Store:alpha-1", rootState()))

    const { store: proxy } = await mountedPromise
    expect(proxy.title).toBe("Inbox")
    expect(proxy.counter).toBe(1)
    expect(proxy.__musubi_store_id__).toEqual([])
  })

  test("nested store field returns a stable child proxy", async () => {
    const { connect } = await import("../src/connect")
    const socket = new MockSocket()
    const connectionPromise = connect<TestStores>(socket)
    const channel = lastChannel(socket)
    channel.resolveJoin()
    const connection = await connectionPromise
    const mountedPromise = connection.mountStore({
      module: "Test.Store",
      id: "alpha-1"
    })
    await Promise.resolve()

    lastPush(channel).push.resolve("ok", { root_id: "Test.Store:alpha-1" })
    channel.emit("patch", initialConnectionEnvelope("Test.Store:alpha-1", rootState()))

    const { store: proxy } = await mountedPromise

    expect(proxy.child).toBe(proxy.child)
    expect(proxy.child.count).toBe(1)
  })

  test("dispatchCommand sends root_id with the command", async () => {
    const { connect } = await import("../src/connect")
    const socket = new MockSocket()
    const connectionPromise = connect<TestStores>(socket)
    const channel = lastChannel(socket)
    channel.resolveJoin()
    const connection = await connectionPromise
    const mountedPromise = connection.mountStore({
      module: "Test.Store",
      id: "alpha-1"
    })
    await Promise.resolve()

    lastPush(channel).push.resolve("ok", { root_id: "Test.Store:alpha-1" })
    channel.emit("patch", initialConnectionEnvelope("Test.Store:alpha-1", rootState()))

    const { store: proxy } = await mountedPromise
    const replyPromise = proxy.dispatchCommand("rename", { title: "Outbox" })

    const commandPush = lastPush(channel)
    expect(commandPush.event).toBe("command")
    expect(commandPush.payload).toEqual({
      root_id: "Test.Store:alpha-1",
      store_id: [],
      name: "rename",
      payload: { title: "Outbox" }
    })

    commandPush.push.resolve("ok", { ok: true })
    await expect(replyPromise).resolves.toEqual({ ok: true })
  })

  test("patches are routed by root_id", async () => {
    const { connect } = await import("../src/connect")
    const socket = new MockSocket()
    const connectionPromise = connect<TestStores>(socket)
    const channel = lastChannel(socket)
    channel.resolveJoin()
    const connection = await connectionPromise

    const alphaMountedPromise = connection.mountStore({
      module: "Test.Store",
      id: "alpha-1"
    })
    await Promise.resolve()
    lastPush(channel).push.resolve("ok", { root_id: "Test.Store:alpha-1" })
    channel.emit("patch", initialConnectionEnvelope("Test.Store:alpha-1", rootState()))
    const { store: alpha } = await alphaMountedPromise

    const betaMountedPromise = connection.mountStore({
      module: "Test.Store",
      id: "beta-1"
    })
    await Promise.resolve()
    lastPush(channel).push.resolve("ok", { root_id: "Test.Store:beta-1" })
    channel.emit("patch", initialConnectionEnvelope("Test.Store:beta-1", rootState("Secondary")))
    const { store: beta } = await betaMountedPromise

    const alphaListener = vi.fn()
    const betaListener = vi.fn()
    alpha.subscribe(alphaListener)
    beta.subscribe(betaListener)

    channel.emit(
      "patch",
      connectionEnvelope(
        "Test.Store:beta-1",
        1,
        2,
        [{ op: "replace", path: "/counter", value: 9 }],
        []
      )
    )

    expect(alpha.counter).toBe(1)
    expect(beta.counter).toBe(9)
    expect(alphaListener).not.toHaveBeenCalled()
    expect(betaListener).toHaveBeenCalledTimes(1)
  })

  test("mountStore reuses the existing root for duplicate ids in one connection", async () => {
    const { connect } = await import("../src/connect")
    const socket = new MockSocket()
    const connectionPromise = connect<TestStores>(socket)
    const channel = lastChannel(socket)
    channel.resolveJoin()
    const connection = await connectionPromise

    const firstPromise = connection.mountStore({
      module: "Test.Store",
      id: "shared-root"
    })
    await Promise.resolve()
    const firstPushCount = channel.pushes.length
    lastPush(channel).push.resolve("ok", { root_id: "Test.Store:shared-root" })
    channel.emit("patch", initialConnectionEnvelope("Test.Store:shared-root", rootState()))
    const firstMounted = await firstPromise

    const secondMounted = await connection.mountStore({
      module: "Test.Store",
      id: "shared-root"
    })

    expect(secondMounted.store).toBe(firstMounted.store)
    // No second mount push: dedup reuses the in-memory entry. The server is
    // the source of truth for duplicate-mount errors; locally we just attach.
    expect(channel.pushes.length).toBe(firstPushCount)
  })

  test("distinct modules sharing one id get distinct server mounts and patches", async () => {
    const { connect } = await import("../src/connect")
    const socket = new MockSocket()
    const connectionPromise = connect<TestStores>(socket)
    const channel = lastChannel(socket)
    channel.resolveJoin()
    const connection = await connectionPromise

    const firstPromise = connection.mountStore({
      module: "Test.Store",
      id: "shared"
    })
    await Promise.resolve()
    const firstMountPush = lastPush(channel)
    expect(firstMountPush.event).toBe("mount")
    // Wire `id` composes module + caller id so the server treats distinct
    // (module, id) pairs as distinct roots — see spec/cf-dashboard-route-mount-bug.md.
    expect(firstMountPush.payload).toMatchObject({
      module: "Test.Store",
      id: "Test.Store:shared"
    })
    firstMountPush.push.resolve("ok", { root_id: "Test.Store:shared" })
    channel.emit("patch", initialConnectionEnvelope("Test.Store:shared", rootState()))
    const first = await firstPromise

    const secondPromise = connection.mountStore({
      module: "Test.Other",
      id: "shared"
    })
    await Promise.resolve()
    const secondMountPush = lastPush(channel)
    expect(secondMountPush.event).toBe("mount")
    expect(secondMountPush.payload).toMatchObject({
      module: "Test.Other",
      id: "Test.Other:shared"
    })
    secondMountPush.push.resolve("ok", { root_id: "Test.Other:shared" })
    channel.emit(
      "patch",
      connectionEnvelope(
        "Test.Other:shared",
        0,
        1,
        [{ op: "replace", path: "", value: { label: "other" } }],
        []
      )
    )
    await secondPromise

    channel.emit(
      "patch",
      connectionEnvelope(
        "Test.Store:shared",
        1,
        2,
        [{ op: "replace", path: "/counter", value: 7 }],
        []
      )
    )

    expect(first.store.counter).toBe(7)
  })

  test("shared root unmount waits for the last caller handle", async () => {
    const { connect } = await import("../src/connect")
    const socket = new MockSocket()
    const connectionPromise = connect<TestStores>(socket)
    const channel = lastChannel(socket)
    channel.resolveJoin()
    const connection = await connectionPromise

    const firstPromise = connection.mountStore({
      module: "Test.Store",
      id: "shared-root"
    })
    await Promise.resolve()
    lastPush(channel).push.resolve("ok", { root_id: "Test.Store:shared-root" })
    channel.emit("patch", initialConnectionEnvelope("Test.Store:shared-root", rootState()))

    const firstMounted = await firstPromise
    const secondMounted = await connection.mountStore({
      module: "Test.Store",
      id: "shared-root"
    })
    const pushCountBeforeUnmount = channel.pushes.length

    await firstMounted.unmount()
    expect(channel.pushes.length).toBe(pushCountBeforeUnmount)
    expect(firstMounted.store.title).toBe("Inbox")

    const secondUnmountPromise = secondMounted.unmount()
    const unmountPush = lastPush(channel)

    expect(unmountPush.event).toBe("unmount")
    expect(unmountPush.payload).toEqual({ root_id: "Test.Store:shared-root" })

    unmountPush.push.resolve("ok", {})
    await secondUnmountPromise

    expect(firstMounted.store.title).toBeUndefined()
  })

  test("snapshot returns a plain object tree", async () => {
    const { connect } = await import("../src/connect")
    const socket = new MockSocket()
    const connectionPromise = connect<TestStores>(socket)
    const channel = lastChannel(socket)
    channel.resolveJoin()
    const connection = await connectionPromise
    const mountedPromise = connection.mountStore({
      module: "Test.Store",
      id: "alpha-1"
    })
    await Promise.resolve()

    lastPush(channel).push.resolve("ok", { root_id: "Test.Store:alpha-1" })
    channel.emit("patch", initialConnectionEnvelope("Test.Store:alpha-1", rootState()))

    const { store: proxy } = await mountedPromise
    const snapshot = proxy.snapshot()

    expect(snapshot).toEqual({
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
    const { connect } = await import("../src/connect")
    const socket = new MockSocket()
    const connectionPromise = connect<TestStores>(socket)
    const channel = lastChannel(socket)
    channel.resolveJoin()
    const connection = await connectionPromise
    const mountedPromise = connection.mountStore({
      module: "Test.Store",
      id: "alpha-1"
    })
    await Promise.resolve()

    lastPush(channel).push.resolve("ok", { root_id: "Test.Store:alpha-1" })
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
    expect(proxy.snapshot().feed.messages).toEqual([{ body: "hello" }])
    expect(proxy.snapshot().async_messages).toEqual({
      status: "loading",
      data: [{ id: "a1", body: "loaded" }],
      error: null
    })
    expect(proxy.snapshot().metadata.messages).toBe("literal")
  })

  test("unmount sends an unmount push and resets the root runtime", async () => {
    const { connect } = await import("../src/connect")
    const socket = new MockSocket()
    const connectionPromise = connect<TestStores>(socket)
    const channel = lastChannel(socket)
    channel.resolveJoin()
    const connection = await connectionPromise
    const mountedPromise = connection.mountStore({
      module: "Test.Store",
      id: "alpha-1"
    })
    await Promise.resolve()

    lastPush(channel).push.resolve("ok", { root_id: "Test.Store:alpha-1" })
    channel.emit("patch", initialConnectionEnvelope("Test.Store:alpha-1", rootState()))

    const { store: proxy, unmount } = await mountedPromise
    const unmountPromise = unmount()
    const unmountPush = lastPush(channel)

    expect(unmountPush.event).toBe("unmount")
    expect(unmountPush.payload).toEqual({ root_id: "Test.Store:alpha-1" })

    unmountPush.push.resolve("ok", {})
    await unmountPromise

    expect(proxy.title).toBeUndefined()
    await expect(proxy.dispatchCommand("rename", { title: "Gone" })).rejects.toThrow(
      /Store is not connected/
    )
  })

  test("disconnect leaves the connection channel", async () => {
    const { connect } = await import("../src/connect")
    const socket = new MockSocket()
    const connectionPromise = connect<TestStores>(socket)
    const channel = lastChannel(socket)
    channel.resolveJoin()
    const connection = await connectionPromise

    await connection.disconnect()

    expect(channel.left).toBe(true)
  })
})

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
