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

  private readonly openHandlers: Array<() => void> = []

  constructor(_url?: string, _options?: unknown) {
    MockSocket.instances.push(this)
  }

  connect(): void {
    this.connected = true
  }

  onOpen(callback: () => void): void {
    this.openHandlers.push(callback)
  }

  // Simulate Phoenix re-opening the transport after a drop.
  simulateReopen(): void {
    this.connected = true
    for (const callback of this.openHandlers) {
      callback()
    }
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
      id: "alpha-1",
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


    await Promise.resolve()
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


    await Promise.resolve()
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

    await Promise.resolve()
    channel.emit("patch", initialConnectionEnvelope("Test.Store:alpha-1", rootState()))
    const { store: alpha } = await alphaMountedPromise

    const betaMountedPromise = connection.mountStore({
      module: "Test.Store",
      id: "beta-1"
    })
    await Promise.resolve()
    lastPush(channel).push.resolve("ok", { root_id: "Test.Store:beta-1" })

    await Promise.resolve()
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

    await Promise.resolve()
    channel.emit("patch", initialConnectionEnvelope("Test.Store:shared-root", rootState()))
    await firstPromise

    // No client-side prediction: the second mount for the same (module, id)
    // hits the wire; the server replies with :already_mounted carrying the
    // canonical root_id; the client aliases to the existing RootConnection
    // and bumps the shared refCount. Multi-observer ergonomic — both
    // callers share one server mount and one StoreProxy.
    const firstMounted = await firstPromise
    const secondPromise = connection.mountStore({
      module: "Test.Store",
      id: "shared-root"
    })
    await Promise.resolve()
    expect(channel.pushes.length).toBe(firstPushCount + 1)
    const secondMountPush = lastPush(channel)
    expect(secondMountPush.event).toBe("mount")
    secondMountPush.push.resolve("error", {
      reason: "already_mounted",
      root_id: "Test.Store:shared-root"
    })

    const secondMounted = await secondPromise
    expect(secondMounted.store).toBe(firstMounted.store)
    // No second initial-patch envelope is required; aliased caller reuses
    // the existing connection's data tree.
  })

  test("mountStore throws MusubiInconsistencyError when server says already_mounted but client has no record", async () => {
    const { connect } = await import("../src/connect")
    const { MusubiInconsistencyError } = await import("../src/runtime")
    const socket = new MockSocket()
    const connectionPromise = connect<TestStores>(socket)
    const channel = lastChannel(socket)
    channel.resolveJoin()
    const connection = await connectionPromise

    const mountedPromise = connection.mountStore({
      module: "Test.Store",
      id: "orphan"
    })
    await Promise.resolve()
    const mountPush = lastPush(channel)
    // Server claims an entry exists for a root_id the client has never seen
    // — out-of-sync state, not a legitimate alias case. Client must throw,
    // not silently fabricate an entry.
    mountPush.push.resolve("error", {
      reason: "already_mounted",
      root_id: "Test.Store:phantom"
    })

    await expect(mountedPromise).rejects.toBeInstanceOf(MusubiInconsistencyError)
  })

  test("the last unmount fires the server push only after the grace timer; an in-window remount cancels it", async () => {
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
    await Promise.resolve()
    channel.emit(
      "patch",
      initialConnectionEnvelope("Test.Store:shared-root", rootState())
    )
    const firstMounted = await firstPromise

    const pushCountBefore = channel.pushes.length

    // Last caller unmounts. The grace timer is scheduled but hasn't fired
    // yet — no unmount push on the wire.
    void firstMounted.unmount()
    expect(channel.pushes.length).toBe(pushCountBefore)

    // In-window remount: server replies :already_mounted with the same
    // root_id (server never tore down because the unmount push never went
    // out), client aliases back to the same RootConnection, the pending
    // grace timer is cancelled.
    const secondPromise = connection.mountStore({
      module: "Test.Store",
      id: "shared-root"
    })
    await Promise.resolve()
    const secondMountPush = lastPush(channel)
    expect(secondMountPush.event).toBe("mount")
    secondMountPush.push.resolve("error", {
      reason: "already_mounted",
      root_id: "Test.Store:shared-root"
    })
    const secondMounted = await secondPromise
    expect(secondMounted.store).toBe(firstMounted.store)

    // Let the original grace timer would-have-fired tick pass; no unmount
    // push should be emitted because refCount went back to 1 before the
    // timer ran.
    await new Promise<void>((resolve) => setTimeout(resolve, 0))
    const unmountPushes = channel.pushes.filter((p) => p.event === "unmount")
    expect(unmountPushes.length).toBe(0)
  })

  test("an orphaned root after grace-timer skip + failed pending mount re-arms teardown", async () => {
    // Variant of the alias-deferred race: timer skipped teardown
    // because a pending mount existed, but that mount then settles
    // with `:error` (server returned an unrelated failure). Without
    // re-arming, the prior root would leak at refCount=0 forever.
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
    lastPush(channel).push.resolve("ok", { root_id: "Test.Store:shared" })
    await Promise.resolve()
    channel.emit("patch", initialConnectionEnvelope("Test.Store:shared", rootState()))
    const firstMounted = await firstPromise

    void firstMounted.unmount()
    const secondPromise = connection.mountStore({
      module: "Test.Store",
      id: "shared"
    })
    await Promise.resolve()
    const secondMountPush = lastPush(channel)
    expect(secondMountPush.event).toBe("mount")

    // Grace timer fires, sees pending mount, skips teardown.
    await new Promise<void>((resolve) => setTimeout(resolve, 0))
    expect(channel.pushes.filter((p) => p.event === "unmount").length).toBe(0)

    // Pending mount FAILS (not :already_mounted) — leaves the
    // original root orphaned at refCount=0.
    secondMountPush.push.resolve("error", { reason: "params must be a map" })
    await expect(secondPromise).rejects.toThrow(/Root mount failed/)

    // `mountConnectionRoot`'s finally re-armed teardown for the
    // orphaned root. Let the new grace timer fire.
    await new Promise<void>((resolve) => setTimeout(resolve, 0))
    const unmountPushes = channel.pushes.filter((p) => p.event === "unmount")
    expect(unmountPushes.length).toBe(1)
    expect(unmountPushes[0]!.payload).toEqual({ root_id: "Test.Store:shared" })
  })

  test("a grace timer firing while a remount push is in flight defers teardown and lets the alias succeed", async () => {
    // Race: `mountConnectionRoot` issued a fresh push (whose reply
    // will be `:already_mounted`), but the grace timer from a prior
    // `unmount()` fires before the reply arrives. If the timer
    // unconditionally tore the entry down, the alias path would hit
    // `MusubiInconsistencyError`. The timer must skip teardown when
    // a pending mount for the same `(module, callerId)` exists.
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
    lastPush(channel).push.resolve("ok", { root_id: "Test.Store:shared" })
    await Promise.resolve()
    channel.emit("patch", initialConnectionEnvelope("Test.Store:shared", rootState()))
    const firstMounted = await firstPromise

    // Last caller releases — grace timer scheduled.
    void firstMounted.unmount()

    // New caller starts a mount with the same (module, callerId);
    // push goes out but the reply is held.
    const secondPromise = connection.mountStore({
      module: "Test.Store",
      id: "shared"
    })
    await Promise.resolve()
    const remountPush = lastPush(channel)
    expect(remountPush.event).toBe("mount")

    // Let the grace timer fire — it must skip teardown because a
    // mount with the same (module, callerId) is in `pendingMounts`.
    await new Promise<void>((resolve) => setTimeout(resolve, 0))
    expect(channel.pushes.filter((p) => p.event === "unmount").length).toBe(0)

    // Server replies :already_mounted; alias path looks up the still-
    // present `roots` entry, succeeds, returns the same proxy.
    remountPush.push.resolve("error", {
      reason: "already_mounted",
      root_id: "Test.Store:shared"
    })
    const secondMounted = await secondPromise
    expect(secondMounted.store).toBe(firstMounted.store)
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
    // Server composes the canonical wire root id as `"<module>:<id>"` so
    // distinct modules sharing one caller id get distinct roots end-to-end.
    expect(firstMountPush.payload).toMatchObject({
      module: "Test.Store",
      id: "shared"
    })
    firstMountPush.push.resolve("ok", { root_id: "Test.Store:shared" })

    await Promise.resolve()
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
      id: "shared"
    })
    secondMountPush.push.resolve("ok", { root_id: "Test.Other:shared" })
    await Promise.resolve()
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

  test("unmount sends the unmount push using the server-assigned root_id", async () => {
    const { connect } = await import("../src/connect")
    const socket = new MockSocket()
    const connectionPromise = connect<TestStores>(socket)
    const channel = lastChannel(socket)
    channel.resolveJoin()
    const connection = await connectionPromise

    const mountedPromise = connection.mountStore({
      module: "Test.Store",
      id: "shared-root"
    })
    await Promise.resolve()
    lastPush(channel).push.resolve("ok", { root_id: "Test.Store:shared-root" })

    await Promise.resolve()
    channel.emit("patch", initialConnectionEnvelope("Test.Store:shared-root", rootState()))
    const mounted = await mountedPromise

    const unmountPromise = mounted.unmount()
    // Server unmount push fires after the grace timer (setTimeout 0), not
    // synchronously. Wait one task for the timer to fire.
    await new Promise<void>((resolve) => setTimeout(resolve, 0))
    const unmountPush = lastPush(channel)
    expect(unmountPush.event).toBe("unmount")
    expect(unmountPush.payload).toEqual({ root_id: "Test.Store:shared-root" })

    unmountPush.push.resolve("ok", {})
    await unmountPromise

    expect(mounted.store.title).toBeUndefined()
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


    await Promise.resolve()
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
    expect(proxy.snapshot()?.async_messages).toEqual({
      status: "loading",
      data: [{ id: "a1", body: "loaded" }],
      error: null
    })
    expect(proxy.snapshot()?.metadata.messages).toBe("literal")
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


    await Promise.resolve()
    channel.emit("patch", initialConnectionEnvelope("Test.Store:alpha-1", rootState()))

    const { store: proxy, unmount } = await mountedPromise
    const unmountPromise = unmount()
    // Grace timer defers the server unmount push to the next task.
    await new Promise<void>((resolve) => setTimeout(resolve, 0))
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

  test("unmount swallows server-side push failures so the consumer's release resolves", async () => {
    // Server might return :error on the unmount push (already gone,
    // unknown root, channel closing). Local state is already torn
    // down by that point; surfacing the failure to the consumer's
    // `await mounted.unmount()` is surprising and unactionable. The
    // failure should log via `console.warn` and the consumer's
    // promise should resolve.
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {})

    try {
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
      await Promise.resolve()
      channel.emit("patch", initialConnectionEnvelope("Test.Store:alpha-1", rootState()))
      const mounted = await mountedPromise

      const unmountPromise = mounted.unmount()
      await new Promise<void>((resolve) => setTimeout(resolve, 0))
      const unmountPush = lastPush(channel)
      expect(unmountPush.event).toBe("unmount")

      // Server replies :error.
      unmountPush.push.resolve("error", { reason: "unknown root" })

      // Consumer's promise still resolves cleanly.
      await expect(unmountPromise).resolves.toBeUndefined()
      expect(warnSpy).toHaveBeenCalledWith(
        "[musubi] root unmount push failed:",
        expect.any(Error)
      )
    } finally {
      warnSpy.mockRestore()
    }
  })

  test("disconnect mid-mount does not surface an unhandled rejection on the in-flight tentative", async () => {
    // Disconnect rejects the tentative's `pendingConnect` so that if
    // the mount push later returns `:ok`, the `:ok` branch's
    // `await tentativeInitialPatch` immediately throws and surfaces
    // the error to the mount caller. The
    // `registerInitialPatchWaiter` pre-attached `.catch` shield
    // prevents that rejection from surfacing as an unhandled
    // `PromiseRejectionEvent` if no one is awaiting the promise yet.
    // Node's `unhandledRejection` listener takes `(reason, promise)` —
    // not a browser-style `PromiseRejectionEvent` — so the listener
    // signature must match.
    const unhandled: unknown[] = []
    const onUnhandled = (reason: unknown): void => {
      unhandled.push(reason)
    }
    process.on("unhandledRejection", onUnhandled)

    try {
      const { connect } = await import("../src/connect")
      const socket = new MockSocket()
      const connectionPromise = connect<TestStores>(socket)
      const channel = lastChannel(socket)
      channel.resolveJoin()
      const connection = await connectionPromise

      // Kick off a mount, then disconnect before the mount reply arrives.
      const mountedPromise = connection.mountStore({
        module: "Test.Store",
        id: "alpha-1"
      })
      await Promise.resolve()
      const mountPush = lastPush(channel)
      expect(mountPush.event).toBe("mount")

      await connection.disconnect()

      // Even if a stale `:ok` reply lands after disconnect (mocked here),
      // the mount caller must settle — not hang — because the tentative's
      // initial-patch waiter was rejected by the disconnect handler.
      mountPush.push.resolve("ok", { root_id: "Test.Store:alpha-1" })

      await expect(mountedPromise).rejects.toThrow(/Disconnected/)

      // Let any micro/macrotasks settle so an unhandled rejection would
      // have surfaced by now.
      await new Promise<void>((resolve) => setTimeout(resolve, 0))

      expect(unhandled).toEqual([])
      expect(channel.left).toBe(true)
    } finally {
      process.off("unhandledRejection", onUnhandled)
    }
  })

  test("disconnect between mount :ok and initial patch rejects mountStore promptly", async () => {
    // Disconnect timing variant: server has already replied :ok and the
    // tentative is in `connectionState.roots`, but the initial patch
    // hasn't landed yet. Disconnect must surface to the mount caller
    // through the `:ok` branch's `await tentativeInitialPatch`, not
    // hang indefinitely.
    const unhandled: unknown[] = []
    const onUnhandled = (reason: unknown): void => {
      unhandled.push(reason)
    }
    process.on("unhandledRejection", onUnhandled)

    try {
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

      // Server replies :ok — `mountConnectionRoot` enters its `:ok` branch,
      // inserts into `roots`, and starts awaiting the initial patch.
      lastPush(channel).push.resolve("ok", { root_id: "Test.Store:alpha-1" })
      await Promise.resolve()

      // Disconnect now — patch never arrives.
      await connection.disconnect()

      await expect(mountedPromise).rejects.toThrow(/Disconnected/)
      await new Promise<void>((resolve) => setTimeout(resolve, 0))

      expect(unhandled).toEqual([])
      expect(channel.left).toBe(true)
    } finally {
      process.off("unhandledRejection", onUnhandled)
    }
  })

  test("recovery from version mismatch hitting :already_mounted disconnects cleanly", async () => {
    // Pre-fix behaviour: `remountExistingConnection` threw on
    // `:already_mounted`, which bubbled out of
    // `recoverConnectionRootFromVersionMismatch` (invoked via
    // `void recover...`) as an unhandled rejection AND left the
    // connection waiting forever on `initialPatchPromise`. The fix
    // catches the throw inside `recover`, force-disconnects, and logs.
    // Node `unhandledRejection` listener signature is `(reason, promise)`.
    const unhandled: unknown[] = []
    const onUnhandled = (reason: unknown): void => {
      unhandled.push(reason)
    }
    process.on("unhandledRejection", onUnhandled)
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {})

    try {
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
      await Promise.resolve()
      channel.emit("patch", initialConnectionEnvelope("Test.Store:alpha-1", rootState()))
      await mountedPromise

      // Simulate a version-mismatch patch — base_version=99 != current 1.
      // `handlePatch` schedules `recoverConnectionRootFromVersionMismatch`.
      channel.emit(
        "patch",
        connectionEnvelope(
          "Test.Store:alpha-1",
          99,
          100,
          [{ op: "replace", path: "/counter", value: 99 }],
          []
        )
      )

      // Recovery's prologue pushes `unmount`; reply with :ok (entry was
      // never really gone server-side — we simulate the stale case where
      // the unmount push succeeds but the server still has the entry on
      // the subsequent re-mount).
      await new Promise<void>((resolve) => setTimeout(resolve, 0))
      const unmountPush = lastPush(channel)
      expect(unmountPush.event).toBe("unmount")
      unmountPush.push.resolve("ok", {})
      // Drain microtasks so recover continues into remountExistingConnection
      // → ensureConnectionReady → pushMount (which fires the mount push).
      await new Promise<void>((resolve) => setTimeout(resolve, 0))

      // Re-mount push arrives; reply with :already_mounted carrying the
      // SAME root_id. This is the recovery-deadlock scenario.
      const remountPush = lastPush(channel)
      expect(remountPush.event).toBe("mount")
      remountPush.push.resolve("error", {
        reason: "already_mounted",
        root_id: "Test.Store:alpha-1"
      })

      // Let recovery's catch run + disconnectConnectionState cascade
      // (which leaves the channel and removes the runtime entry on top
      // of clearing local state).
      await new Promise<void>((resolve) => setTimeout(resolve, 0))

      expect(unhandled).toEqual([])
      // Connection state should be cleaned up: a follow-up command on
      // the proxy now fails with "Store is not connected" because
      // disconnect cleared the channel and reset state.
      expect(errorSpy).toHaveBeenCalledWith(
        "[musubi] root recovery failed:",
        expect.any(Error)
      )
      // Channel was actually torn down (not just local state cleared).
      expect(channel.left).toBe(true)
    } finally {
      errorSpy.mockRestore()
      process.off("unhandledRejection", onUnhandled)
    }
  })

  test("serves last-good snapshot through the version-mismatch recovery window", async () => {
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
    await Promise.resolve()
    channel.emit("patch", initialConnectionEnvelope("Test.Store:alpha-1", rootState("Inbox")))
    const mounted = await mountedPromise

    expect(mounted.store.snapshot()?.title).toBe("Inbox")

    // Version-mismatch patch → schedules recovery (soft reset + remount).
    channel.emit(
      "patch",
      connectionEnvelope(
        "Test.Store:alpha-1",
        99,
        100,
        [{ op: "replace", path: "/counter", value: 99 }],
        []
      )
    )

    // Recovery is parked awaiting the unmount reply. The index was NOT
    // emptied, so the mounted proxy still serves the complete last-good
    // snapshot rather than the bare stub that crashed consumers.
    await new Promise<void>((resolve) => setTimeout(resolve, 0))
    expect(mounted.store.snapshot()?.title).toBe("Inbox")
    expect(mounted.store.snapshot()?.counter).toBe(1)

    const unmountPush = lastPush(channel)
    expect(unmountPush.event).toBe("unmount")
    unmountPush.push.resolve("ok", {})
    await new Promise<void>((resolve) => setTimeout(resolve, 0))

    const remountPush = lastPush(channel)
    expect(remountPush.event).toBe("mount")
    remountPush.push.resolve("ok", { root_id: "Test.Store:alpha-1" })
    await new Promise<void>((resolve) => setTimeout(resolve, 0))

    // Remount's initial patch atomically swaps in fresh state.
    channel.emit("patch", initialConnectionEnvelope("Test.Store:alpha-1", rootState("Fresh")))
    await new Promise<void>((resolve) => setTimeout(resolve, 0))

    expect(mounted.store.snapshot()?.title).toBe("Fresh")
  })

  test("keeps last-good snapshot on hard disconnect and auto-remounts on reconnect", async () => {
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
    await Promise.resolve()
    channel.emit("patch", initialConnectionEnvelope("Test.Store:alpha-1", rootState("Inbox")))
    const mounted = await mountedPromise

    expect(mounted.store.snapshot()?.title).toBe("Inbox")

    // Hard socket drop (channel onClose → handleConnectionDisconnect).
    channel.disconnect({ reason: "socket closed" })

    // A: the last-good snapshot stays complete and readable through the
    // disconnected window instead of collapsing to a missing-snapshot stub.
    expect(mounted.store.snapshot()?.title).toBe("Inbox")
    expect(mounted.store.title).toBe("Inbox")

    // B: socket re-opens → re-join the channel and auto-remount the live root.
    socket.simulateReopen()
    await new Promise<void>((resolve) => setTimeout(resolve, 0))

    const reconnectChannel = lastChannel(socket)
    expect(reconnectChannel).not.toBe(channel)
    reconnectChannel.resolveJoin()
    await new Promise<void>((resolve) => setTimeout(resolve, 0))

    // Exactly one remount push on the fresh channel — no duplicate mount churn.
    const mountPushes = reconnectChannel.pushes.filter((p) => p.event === "mount")
    expect(mountPushes.length).toBe(1)
    mountPushes[0]!.push.resolve("ok", { root_id: "Test.Store:alpha-1" })
    await new Promise<void>((resolve) => setTimeout(resolve, 0))

    // The server's fresh initial patch atomically replaces the stale snapshot.
    reconnectChannel.emit(
      "patch",
      initialConnectionEnvelope("Test.Store:alpha-1", rootState("Fresh"))
    )
    await new Promise<void>((resolve) => setTimeout(resolve, 0))

    expect(mounted.store.snapshot()?.title).toBe("Fresh")
    expect(mounted.store.title).toBe("Fresh")
  })

  test("auto-remounts live roots on reopen after a silent drop leaves a stale channel (bfcache resume)", async () => {
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
    expect(channel.pushes.filter((p) => p.event === "mount").length).toBe(1)
    lastPush(channel).push.resolve("ok", { root_id: "Test.Store:alpha-1" })
    await Promise.resolve()
    channel.emit("patch", initialConnectionEnvelope("Test.Store:alpha-1", rootState("Inbox")))
    const mounted = await mountedPromise

    expect(mounted.store.snapshot()?.title).toBe("Inbox")

    // Silent drop: an iOS Safari bfcache freeze swallows a clean
    // `socket.disconnect()` — the WS closes without delivering the channel
    // onClose/onError that drives `handleConnectionDisconnect`, so
    // `connectionState.channel` and the live `root.channel` stay set. We
    // deliberately do NOT call `channel.disconnect()` here (that is the
    // already-covered clean-drop path); only the transport reopens.
    socket.simulateReopen()
    await new Promise<void>((resolve) => setTimeout(resolve, 0))

    // A fresh transport channel must be created and the live root re-mounted
    // on it. Pre-fix, `handleSocketReopen` bailed on the truthy stale
    // `connectionState.channel`, so no second channel appeared and the
    // consumer was left with no live data.
    const reconnectChannel = lastChannel(socket)
    expect(reconnectChannel).not.toBe(channel)
    reconnectChannel.resolveJoin()
    await new Promise<void>((resolve) => setTimeout(resolve, 0))

    const mountPushes = reconnectChannel.pushes.filter((p) => p.event === "mount")
    expect(mountPushes.length).toBe(1)
    expect(mountPushes[0]!.payload).toMatchObject({ module: "Test.Store", id: "alpha-1" })
    mountPushes[0]!.push.resolve("ok", { root_id: "Test.Store:alpha-1" })
    await new Promise<void>((resolve) => setTimeout(resolve, 0))

    // The server's fresh initial patch lands on the new channel → live data
    // restored, no manual reload or navigation.
    reconnectChannel.emit(
      "patch",
      initialConnectionEnvelope("Test.Store:alpha-1", rootState("Fresh"))
    )
    await new Promise<void>((resolve) => setTimeout(resolve, 0))

    expect(mounted.store.snapshot()?.title).toBe("Fresh")
    expect(mounted.store.title).toBe("Fresh")
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
