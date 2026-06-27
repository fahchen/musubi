import { act, render } from "@testing-library/react"
import * as React from "react"
import { describe, expect, test } from "vitest"

import { connect as baseConnect, type SocketLike } from "@musubi/client"
import { createMusubi } from "../src"

void React

// Real-runtime integration: drive `useMusubiSnapshot` through a per-root channel
// drop + Phoenix rejoin and assert the consumer never observes an `undefined`
// snapshot frame and never sees the store proxy identity change.

type Stores = {
  "React.Test.Root": Musubi.StoreDef<
    "React.Test.Root",
    { title: string; counter: number },
    { rename: { payload: { title: string }; reply: { ok: true } } }
  >
}

const { MusubiProvider, useMusubiRoot, useMusubiSnapshot } = createMusubi<Stores>()

type PushStatus = "ok" | "error" | "timeout"
type PushCallback = (payload: unknown) => void

class MockPush {
  private readonly callbacks = new Map<PushStatus, PushCallback[]>()
  receive(status: PushStatus, callback: PushCallback): this {
    const list = this.callbacks.get(status) ?? []
    list.push(callback)
    this.callbacks.set(status, list)
    return this
  }
  resolve(status: PushStatus, payload: unknown): void {
    for (const cb of this.callbacks.get(status) ?? []) cb(payload)
  }
}

class MockChannel {
  readonly topic: string
  private readonly eventHandlers = new Map<string, Array<(p: unknown) => void>>()
  private readonly closeHandlers: Array<(r: unknown) => void> = []
  private readonly errorHandlers: Array<(r: unknown) => void> = []
  private readonly joinPush = new MockPush()
  left = false

  constructor(topic: string) {
    this.topic = topic
  }
  on(event: string, cb: (p: unknown) => void): void {
    const list = this.eventHandlers.get(event) ?? []
    list.push(cb)
    this.eventHandlers.set(event, list)
  }
  onClose(cb: (r: unknown) => void): void {
    this.closeHandlers.push(cb)
  }
  onError(cb: (r: unknown) => void): void {
    this.errorHandlers.push(cb)
  }
  join(): MockPush {
    return this.joinPush
  }
  push(): MockPush {
    return new MockPush()
  }
  leave(): void {
    this.left = true
    for (const cb of this.closeHandlers) cb({ reason: "leave" })
  }
  // Re-fires on every call → models Phoenix rejoining this same channel.
  resolveJoin(payload: unknown): void {
    this.joinPush.resolve("ok", payload)
  }
  emit(event: string, payload: unknown): void {
    for (const cb of this.eventHandlers.get(event) ?? []) cb(payload)
  }
  drop(reason: unknown): void {
    for (const cb of this.errorHandlers) cb(reason)
    for (const cb of this.closeHandlers) cb(reason)
  }
}

class MockSocket implements SocketLike {
  readonly channels: MockChannel[] = []
  connect(): void {}
  channel(topic: string): MockChannel {
    const ch = new MockChannel(topic)
    this.channels.push(ch)
    return ch
  }
}

function rootState(title: string): Record<string, unknown> {
  return { __musubi_store_id__: [], title, counter: 1 }
}

function initialEnvelope(rootId: string, title: string) {
  return {
    type: "patch",
    root_id: rootId,
    base_version: 0,
    version: 1,
    ops: [{ op: "replace", path: "", value: rootState(title) }],
    stream_ops: []
  }
}

function lastChannel(socket: MockSocket): MockChannel {
  const ch = socket.channels.at(-1)
  if (!ch) throw new Error("no channel")
  return ch
}

describe("reconnect integration (real runtime + useMusubiSnapshot)", () => {
  test("no undefined frame and stable store identity across a channel rejoin", async () => {
    const ROOT_ID = "React.Test.Root:r1"
    const socket = new MockSocket()
    const connection = await baseConnect<Stores>(socket)

    const rendered: string[] = []
    const storeRefs: unknown[] = []

    function Shell() {
      const root = useMusubiRoot({ module: "React.Test.Root", id: "r1" })
      if (root.status !== "ready") {
        return <span data-testid="out">status:{root.status}</span>
      }
      return <Content store={root.store} />
    }

    function Content({ store }: { store: ReturnType<typeof useMusubiRoot> extends { store: infer S } ? S : never }) {
      const snap = useMusubiSnapshot(store as never) as { title: string } | undefined
      storeRefs.push(store)
      rendered.push(snap === undefined ? "UNDEFINED" : snap.title)
      // Mirror suikou's shell: a single `undefined` collapses the subtree.
      if (!snap) return <span data-testid="out">null</span>
      return <span data-testid="out">title:{snap.title}</span>
    }

    // Mount: render → effect mounts the root → resolve join + initial patch.
    await act(async () => {
      render(
        <MusubiProvider connection={connection}>
          <Shell />
        </MusubiProvider>
      )
    })
    await act(async () => {
      const ch = lastChannel(socket)
      ch.resolveJoin({ root_id: ROOT_ID })
      ch.emit("patch", initialEnvelope(ROOT_ID, "Inbox"))
    })

    expect(rendered.at(-1)).toBe("Inbox")
    const channel = lastChannel(socket)
    const refAfterHydrate = storeRefs.at(-1)
    const rendersBeforeDrop = rendered.length

    // Hard drop: keep last-good, version → 0.
    await act(async () => {
      channel.drop({ reason: "socket closed" })
    })

    // Phoenix rejoins the SAME channel, then the server emits a fresh initial
    // patch. No new channel object.
    await act(async () => {
      channel.resolveJoin({ root_id: ROOT_ID })
    })
    await act(async () => {
      channel.emit("patch", initialEnvelope(ROOT_ID, "Fresh"))
    })

    expect(socket.channels.length).toBe(1)
    expect(rendered.at(-1)).toBe("Fresh")

    // (a) No UNDEFINED frame at any point after the first hydrate.
    const afterHydrate = rendered.slice(rendersBeforeDrop - 1)
    expect(afterHydrate).not.toContain("UNDEFINED")
    expect(rendered).not.toContain("UNDEFINED")

    // (b) Store proxy identity stable before and after reconnect.
    expect(storeRefs.every((ref) => ref === refAfterHydrate)).toBe(true)
  })

  test("version-mismatch recovery (recreate channel) shows no undefined frame", async () => {
    const ROOT_ID = "React.Test.Root:r1"
    const socket = new MockSocket()
    const connection = await baseConnect<Stores>(socket)

    const rendered: string[] = []
    const storeRefs: unknown[] = []

    function Shell() {
      const root = useMusubiRoot({ module: "React.Test.Root", id: "r1" })
      if (root.status !== "ready") return <span data-testid="out">status:{root.status}</span>
      return <Content store={root.store} />
    }
    function Content({ store }: { store: unknown }) {
      const snap = useMusubiSnapshot(store as never) as { title: string } | undefined
      storeRefs.push(store)
      rendered.push(snap === undefined ? "UNDEFINED" : snap.title)
      if (!snap) return <span data-testid="out">null</span>
      return <span data-testid="out">title:{snap.title}</span>
    }

    await act(async () => {
      render(
        <MusubiProvider connection={connection}>
          <Shell />
        </MusubiProvider>
      )
    })
    await act(async () => {
      const ch = lastChannel(socket)
      ch.resolveJoin({ root_id: ROOT_ID })
      ch.emit("patch", initialEnvelope(ROOT_ID, "Inbox"))
    })
    expect(rendered.at(-1)).toBe("Inbox")
    const refAfterHydrate = storeRefs.at(-1)
    const staleChannel = lastChannel(socket)

    // Version-mismatch patch on the live channel → recovery leaves this channel
    // and joins a fresh one.
    await act(async () => {
      staleChannel.emit("patch", {
        type: "patch",
        root_id: ROOT_ID,
        base_version: 99,
        version: 100,
        ops: [{ op: "replace", path: "/counter", value: 99 }],
        stream_ops: []
      })
    })
    await act(async () => {
      const recovery = lastChannel(socket)
      recovery.resolveJoin({ root_id: ROOT_ID })
      recovery.emit("patch", initialEnvelope(ROOT_ID, "Fresh"))
    })

    expect(socket.channels.length).toBe(2)
    expect(rendered.at(-1)).toBe("Fresh")
    expect(rendered).not.toContain("UNDEFINED")
    expect(storeRefs.every((ref) => ref === refAfterHydrate)).toBe(true)
  })
})
