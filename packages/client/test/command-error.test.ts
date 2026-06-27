import { afterEach, describe, expect, test, vi } from "vitest"

import { MusubiCommandError } from "../src/error"

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
    for (const callback of this.callbacks.get(status) ?? []) callback(payload)
  }
}

class MockChannel {
  readonly pushes: Array<{ event: string; payload: unknown; push: MockPush }> = []
  private readonly eventHandlers = new Map<string, Array<(payload: unknown) => void>>()
  private readonly closeHandlers: Array<(reason: unknown) => void> = []
  private readonly errorHandlers: Array<(reason: unknown) => void> = []
  private readonly joinPush = new MockPush()

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
    for (const callback of this.closeHandlers) callback({ reason: "leave" })
  }
  resolveJoin(payload: unknown = {}): void {
    this.joinPush.resolve("ok", payload)
  }
  emit(event: string, payload: unknown): void {
    for (const callback of this.eventHandlers.get(event) ?? []) callback(payload)
  }
}

class MockSocket {
  readonly channels: MockChannel[] = []
  connect(): void {}
  onOpen(_callback: () => void): void {}
  channel(_topic: string, _payload?: unknown): MockChannel {
    const ch = new MockChannel()
    this.channels.push(ch)
    return ch
  }
}

vi.mock("phoenix", () => ({ Socket: MockSocket }))

type TestStores = {
  "Test.Store": Musubi.StoreDef<
    "Test.Store",
    { title: string },
    { rename: { payload: { title: string }; reply: { ok: true } } }
  >
}

function initialEnvelope(rootId: string) {
  return {
    type: "patch",
    root_id: rootId,
    base_version: 0,
    version: 1,
    ops: [
      {
        op: "replace",
        path: "",
        value: { __musubi_store_id__: [], title: "Inbox" }
      }
    ],
    stream_ops: []
  }
}

async function setupProxy() {
  const { connect } = await import("../src/connect")
  const socket = new MockSocket()
  // No connection channel anymore — `connect` resolves immediately; the
  // per-root channel is created (and joined) by `mountStore`. Join IS the mount.
  const connection = await connect<TestStores>(socket)
  const mountedPromise = connection.mountStore({
    module: "Test.Store",
    id: "alpha-1"
  })
  await Promise.resolve()
  const channel = socket.channels[socket.channels.length - 1]!
  channel.resolveJoin({ root_id: "Test.Store:alpha-1" })
  await Promise.resolve()
  channel.emit("patch", initialEnvelope("Test.Store:alpha-1"))
  const { store: proxy } = await mountedPromise
  return { channel, proxy }
}

describe("MusubiCommandError class", () => {
  test("constructs failed kind with structured fields", () => {
    const err = new MusubiCommandError({
      kind: "failed",
      command: "rename",
      storeId: ["cart"],
      reply: { code: "invalid" }
    })
    expect(err.name).toBe("MusubiCommandError")
    expect(err.kind).toBe("failed")
    expect(err.command).toBe("rename")
    expect(err.storeId).toEqual(["cart"])
    expect(err.reply).toEqual({ code: "invalid" })
    expect(err.code).toBe("invalid")
    expect(err.message).toBe('Command "rename" failed: invalid')
  })

  test("constructs timeout kind", () => {
    const err = new MusubiCommandError({
      kind: "timeout",
      command: "rename",
      storeId: []
    })
    expect(err.kind).toBe("timeout")
    expect(err.code).toBeUndefined()
    expect(err.message).toBe('Command "rename" timed out')
  })

  test("extracts code from code/error/reason in order", () => {
    const codeErr = new MusubiCommandError({
      kind: "failed", command: "x", storeId: [], reply: { code: "C", error: "E" }
    })
    expect(codeErr.code).toBe("C")

    const errKey = new MusubiCommandError({
      kind: "failed", command: "x", storeId: [], reply: { error: "E", reason: "R" }
    })
    expect(errKey.code).toBe("E")

    const reasonKey = new MusubiCommandError({
      kind: "failed", command: "x", storeId: [], reply: { reason: "R" }
    })
    expect(reasonKey.code).toBe("R")
  })

  test("code is undefined for non-record/null/string replies", () => {
    expect(
      new MusubiCommandError({ kind: "failed", command: "x", storeId: [], reply: null }).code
    ).toBeUndefined()
    expect(
      new MusubiCommandError({ kind: "failed", command: "x", storeId: [], reply: "boom" }).code
    ).toBeUndefined()
    expect(
      new MusubiCommandError({ kind: "failed", command: "x", storeId: [], reply: 42 }).code
    ).toBeUndefined()
  })

  test("construction is safe for non-JSON-stringifiable replies (bigint, circular)", () => {
    const circular: Record<string, unknown> = {}
    circular.self = circular
    expect(() =>
      new MusubiCommandError({ kind: "failed", command: "x", storeId: [], reply: circular })
    ).not.toThrow()

    expect(() =>
      new MusubiCommandError({ kind: "failed", command: "x", storeId: [], reply: { n: 1n } })
    ).not.toThrow()
  })

  test("preserves cause via Error options", () => {
    const original = new Error("root")
    const err = new MusubiCommandError({
      kind: "failed",
      command: "x",
      storeId: [],
      reply: { code: "c" },
      cause: original
    })
    expect((err as Error & { cause?: unknown }).cause).toBe(original)
  })

  test("MusubiCommandError.is recognizes cross-module instances", async () => {
    const first = new MusubiCommandError({ kind: "timeout", command: "x", storeId: [] })
    expect(MusubiCommandError.is(first)).toBe(true)
    expect(MusubiCommandError.is(new Error("nope"))).toBe(false)
    expect(MusubiCommandError.is(null)).toBe(false)

    vi.resetModules()
    const fresh = await import("../src/error")
    const second = new fresh.MusubiCommandError({
      kind: "failed", command: "x", storeId: [], reply: { code: "c" }
    })
    expect(second).not.toBeInstanceOf(MusubiCommandError)
    expect(MusubiCommandError.is(second)).toBe(true)
    expect(fresh.MusubiCommandError.is(first)).toBe(true)
  })
})

describe("dispatchCommand error wiring", () => {
  afterEach(() => {
    vi.resetModules()
  })

  test("throws MusubiCommandError(kind=failed) on error reply", async () => {
    const { channel, proxy } = await setupProxy()
    const replyPromise = proxy.dispatchCommand("rename", { title: "Outbox" })
    const cmd = channel.pushes[channel.pushes.length - 1]!
    cmd.push.resolve("error", { code: "invalid_title" })

    await expect(replyPromise).rejects.toMatchObject({
      name: "MusubiCommandError",
      kind: "failed",
      command: "rename",
      storeId: [],
      code: "invalid_title"
    })
  })

  test("throws MusubiCommandError(kind=timeout) on timeout", async () => {
    const { channel, proxy } = await setupProxy()
    const replyPromise = proxy.dispatchCommand("rename", { title: "Outbox" })
    const cmd = channel.pushes[channel.pushes.length - 1]!
    cmd.push.resolve("timeout", undefined)

    await expect(replyPromise).rejects.toMatchObject({
      name: "MusubiCommandError",
      kind: "timeout",
      command: "rename"
    })
  })
})
