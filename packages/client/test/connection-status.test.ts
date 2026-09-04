import { describe, expect, test, vi } from "vitest"

import { connect } from "../src/connect"
import type { MusubiConnection } from "../src/connect"
import type { MusubiSocketStatus } from "../src/runtime"
import type { ConnectionPatchEnvelope } from "../src/types"

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
  private readonly joinPush = new MockPush()

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

  onError(): void {}

  join(): MockPush {
    return this.joinPush
  }

  push(event: string, payload: unknown): MockPush {
    const push = new MockPush()
    this.pushes.push({ event, payload, push })
    return push
  }

  leave(): void {}

  resolveJoin(payload: unknown = {}): void {
    this.joinPush.resolve("ok", payload)
  }

  emit(event: string, payload: unknown): void {
    for (const callback of this.eventHandlers.get(event) ?? []) {
      callback(payload)
    }
  }

  // Transport drop as the channel sees it (Phoenix errors every joined channel
  // when the socket goes away).
  disconnect(reason: unknown): void {
    for (const callback of this.closeHandlers) {
      callback(reason)
    }
  }
}

// A phoenix.js-shaped socket: channels plus the three lifecycle hooks and
// `off`. The tests drive `open` / `drop` / `fail` directly.
class MockLifecycleSocket {
  readonly channels: MockChannel[] = []
  readonly offCalls: unknown[][] = []
  connected = false

  private readonly openHandlers = new Map<string, () => void>()
  private readonly closeHandlers = new Map<string, (reason?: unknown) => void>()
  private readonly errorHandlers = new Map<string, (reason?: unknown) => void>()
  private nextRef = 0

  connect(): void {
    this.connected = true
  }

  channel(topic: string): MockChannel {
    const channel = new MockChannel(topic)
    this.channels.push(channel)
    return channel
  }

  onOpen(callback: () => void): string {
    const ref = `ref-${++this.nextRef}`
    this.openHandlers.set(ref, callback)
    return ref
  }

  onClose(callback: (reason?: unknown) => void): string {
    const ref = `ref-${++this.nextRef}`
    this.closeHandlers.set(ref, callback)
    return ref
  }

  onError(callback: (reason?: unknown) => void): string {
    const ref = `ref-${++this.nextRef}`
    this.errorHandlers.set(ref, callback)
    return ref
  }

  off(refs: unknown[]): void {
    this.offCalls.push(refs)
    for (const ref of refs) {
      this.openHandlers.delete(String(ref))
      this.closeHandlers.delete(String(ref))
      this.errorHandlers.delete(String(ref))
    }
  }

  open(): void {
    for (const callback of Array.from(this.openHandlers.values())) {
      callback()
    }
  }

  drop(reason?: unknown): void {
    for (const callback of Array.from(this.closeHandlers.values())) {
      callback(reason)
    }
  }

  fail(reason?: unknown): void {
    for (const callback of Array.from(this.errorHandlers.values())) {
      callback(reason)
    }
  }
}

// The pre-BDR-0033 shape: no lifecycle hooks at all.
class BareSocket {
  readonly channels: MockChannel[] = []
  connected = false

  connect(): void {
    this.connected = true
  }

  channel(topic: string): MockChannel {
    const channel = new MockChannel(topic)
    this.channels.push(channel)
    return channel
  }
}

type TestStores = {
  "Test.Store": Musubi.StoreDef<
    "Test.Store",
    { title: string },
    {
      rename: {
        payload: { title: string }
        reply: { ok: true }
      }
    }
  >
}

async function mountRoot(
  socket: MockLifecycleSocket,
  connection: MusubiConnection<TestStores>,
  id: string
): Promise<{ store: { title: string; snapshot(): { title: string } | undefined }; channel: MockChannel }> {
  const mountedPromise = connection.mountStore({ module: "Test.Store", id })
  await Promise.resolve()
  const channel = socket.channels.at(-1)
  if (!channel) {
    throw new Error("Missing mock channel")
  }
  const rootId = `Test.Store:${id}`
  channel.resolveJoin({ root_id: rootId })
  await Promise.resolve()
  channel.emit("patch", initialEnvelope(rootId))
  const mounted = await mountedPromise
  return { store: mounted.store, channel }
}

function initialEnvelope(rootId: string): ConnectionPatchEnvelope {
  return {
    type: "patch",
    root_id: rootId,
    base_version: 0,
    version: 1,
    ops: [
      {
        op: "replace",
        path: "",
        value: { title: "Inbox", __musubi_store_id__: [] }
      }
    ],
    stream_ops: [],
    upload_ops: [],
    events: []
  }
}

describe("connection status (BDR-0033)", () => {
  test("starts connecting and flips to ready on the socket's first open", async () => {
    const socket = new MockLifecycleSocket()
    const connection = await connect<TestStores>(socket)
    const seen: MusubiSocketStatus[] = []
    connection.onStatusChange((status) => seen.push(status))

    expect(connection.status()).toBe("connecting")

    socket.open()

    expect(connection.status()).toBe("ready")
    expect(seen).toEqual(["ready"])
  })

  test("a failed initial connect stays connecting rather than reconnecting", async () => {
    const socket = new MockLifecycleSocket()
    const connection = await connect<TestStores>(socket)
    const listener = vi.fn()
    connection.onStatusChange(listener)

    // phoenix.js fires onError and then onClose on every failed connect
    // attempt; a socket that has never been open is not "reconnecting".
    socket.fail(new Error("refused"))
    socket.drop({ code: 1006 })

    expect(connection.status()).toBe("connecting")
    expect(listener).not.toHaveBeenCalled()
  })

  test("a transport drop flips to reconnecting on its own — no command needed — and last-good keeps rendering", async () => {
    const socket = new MockLifecycleSocket()
    const connection = await connect<TestStores>(socket)
    socket.open()

    const { store, channel } = await mountRoot(socket, connection, "alpha-1")
    expect(connection.status()).toBe("ready")
    expect(store.snapshot()?.title).toBe("Inbox")

    const seen: MusubiSocketStatus[] = []
    connection.onStatusChange((status) => seen.push(status))

    // The transport dies while the app is idle: the socket-level hook flips
    // the status, and Phoenix errors the joined channel.
    socket.drop({ code: 1006 })
    channel.disconnect({ reason: "socket closed" })

    expect(connection.status()).toBe("reconnecting")
    expect(seen).toEqual(["reconnecting"])
    expect(channel.pushes).toEqual([])
    // BDR-0015 restated: the last-good snapshot keeps rendering through the
    // window; the status annotates it, never blanks it.
    expect(store.snapshot()?.title).toBe("Inbox")
    expect(store.title).toBe("Inbox")

    // The socket comes back; the channel rejoin + fresh initial patch ride
    // behind it as before.
    socket.open()

    expect(connection.status()).toBe("ready")
    expect(seen).toEqual(["reconnecting", "ready"])
  })

  test("repeated errors while reconnecting notify once per edge", async () => {
    const socket = new MockLifecycleSocket()
    const connection = await connect<TestStores>(socket)
    socket.open()

    const listener = vi.fn()
    connection.onStatusChange(listener)

    socket.fail(new Error("boom"))
    socket.drop({ code: 1006 })
    socket.fail(new Error("still down"))

    expect(listener).toHaveBeenCalledTimes(1)
    expect(listener).toHaveBeenCalledWith("reconnecting")
  })

  test("unsubscribing stops notifications", async () => {
    const socket = new MockLifecycleSocket()
    const connection = await connect<TestStores>(socket)
    const listener = vi.fn()
    const off = connection.onStatusChange(listener)

    off()
    socket.open()

    expect(connection.status()).toBe("ready")
    expect(listener).not.toHaveBeenCalled()
  })

  test("a socket without lifecycle hooks degrades to a constant ready", async () => {
    const socket = new BareSocket()
    const connection = await connect<TestStores>(socket)

    expect(connection.status()).toBe("ready")
  })

  test("disconnect detaches the lifecycle hooks via off", async () => {
    const socket = new MockLifecycleSocket()
    const connection = await connect<TestStores>(socket)
    const listener = vi.fn()
    connection.onStatusChange(listener)

    await connection.disconnect()

    expect(socket.offCalls).toHaveLength(1)
    expect(socket.offCalls[0]).toHaveLength(3)

    // The detached hooks no longer feed a dead connection.
    socket.open()
    expect(listener).not.toHaveBeenCalled()
  })
})
