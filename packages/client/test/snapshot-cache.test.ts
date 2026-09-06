import { describe, expect, test } from "vitest"

import { snapshotStore } from "../src/proxy"
import { mountConnectionRoot, openConnectionState, type ChannelLike, type PushLike } from "../src/runtime"
import type { ConnectionPatchEnvelope, PatchEnvelope } from "../src/types"

type PushStatus = "ok" | "error" | "timeout"
type PushCallback = (payload: unknown) => void

class MockPush implements PushLike {
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

class MockChannel implements ChannelLike {
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

  connect(): void {}

  onOpen(_callback: () => void): void {}

  channel(_topic: string, _payload?: object): MockChannel {
    const channel = new MockChannel()
    this.channels.push(channel)
    return channel
  }
}

describe("snapshot cache invalidation", () => {
  test("preserves unrelated store snapshots across patch envelopes", async () => {
    const { channel, connection } = await mountTestRoot()

    const snapA1 = snapshotStore(connection, ["a"])

    channel.emit(
      "patch",
      connectionEnvelope(
        "Test.Root:root",
        1,
        2,
        [{ op: "replace", path: "/b/v", value: 2 }],
        []
      )
    )

    const snapA2 = snapshotStore(connection, ["a"])

    expect(Object.is(snapA1, snapA2)).toBe(true)
  })

  test("invalidates touched store snapshots and their ancestors", async () => {
    const { channel, connection } = await mountTestRoot()
    const root1 = snapshotStore(connection, [])
    const snapA1 = snapshotStore(connection, ["a"])

    channel.emit(
      "patch",
      connectionEnvelope(
        "Test.Root:root",
        1,
        2,
        [{ op: "replace", path: "/a/v", value: 2 }],
        []
      )
    )

    const root2 = snapshotStore(connection, [])
    const snapA2 = snapshotStore(connection, ["a"])

    expect(Object.is(root1, root2)).toBe(false)
    expect(Object.is(snapA1, snapA2)).toBe(false)
    expect(snapA2).toMatchObject({ v: 2 })
  })

  test("invalidates removed subtree snapshots", async () => {
    const { channel, connection } = await mountTestRoot()
    const child1 = snapshotStore(connection, ["a", "child"])

    channel.emit(
      "patch",
      connectionEnvelope(
        "Test.Root:root",
        1,
        2,
        [
          {
            op: "replace",
            path: "/a",
            value: {
              __musubi_store_id__: ["a"],
              v: 2,
              items: { __musubi_stream__: "items" }
            }
          }
        ],
        []
      )
    )

    const child2 = snapshotStore(connection, ["a", "child"])

    expect(Object.is(child1, child2)).toBe(false)
    expect(child2).toBeUndefined()
  })

  test("invalidates stream owner snapshots and their ancestors", async () => {
    const { channel, connection } = await mountTestRoot()
    const root1 = snapshotStore(connection, [])
    const snapA1 = snapshotStore(connection, ["a"])

    channel.emit(
      "patch",
      connectionEnvelope(
        "Test.Root:root",
        1,
        2,
        [],
        [
          {
            op: "insert",
            stream: "items",
            ref: "1",
            store_id: ["a"],
            item_key: "item-1",
            at: -1,
            item: { id: "1", label: "fresh" },
            limit: null
          }
        ]
      )
    )

    const root2 = snapshotStore(connection, [])
    const snapA2 = snapshotStore(connection, ["a"])

    expect(Object.is(root1, root2)).toBe(false)
    expect(Object.is(snapA1, snapA2)).toBe(false)
    expect(snapA2).toMatchObject({ items: [{ id: "1", label: "fresh" }] })
  })

  // Upload ops mutate the handle in place — no JSON patch op ever touches the
  // owning store's node — so the snapshot cache has to be invalidated off the
  // upload ops themselves. Otherwise `notifySubscribers` reports the store as
  // upload-touched while `snapshot()` hands back the identical object and
  // `useSyncExternalStore` skips the re-render.
  test("invalidates upload owner snapshots and their ancestors", async () => {
    const { channel, connection } = await mountTestRoot()

    channel.emit(
      "patch",
      connectionEnvelope("Test.Root:root", 1, 2, [], [], [
        {
          op: "add",
          upload: "avatar",
          store_id: ["a"],
          ref: "entry-1",
          entry: {
            ref: "entry-1",
            client_name: "avatar.png",
            client_size: 1024,
            client_type: "image/png",
            progress: 0,
            status: "pending",
            errors: []
          }
        }
      ])
    )

    const root1 = snapshotStore(connection, [])
    const snapA1 = snapshotStore(connection, ["a"])

    expect(snapA1).toMatchObject({ avatar: { progress: 0 } })

    channel.emit(
      "patch",
      connectionEnvelope("Test.Root:root", 2, 3, [], [], [
        { op: "progress", upload: "avatar", store_id: ["a"], ref: "entry-1", progress: 50 }
      ])
    )

    const root2 = snapshotStore(connection, [])
    const snapA2 = snapshotStore(connection, ["a"])

    expect(Object.is(root1, root2)).toBe(false)
    expect(Object.is(snapA1, snapA2)).toBe(false)
    expect(snapA2).toMatchObject({ avatar: { progress: 50 } })
  })
})

async function mountTestRoot(): Promise<{
  channel: MockChannel
  connection: Awaited<ReturnType<typeof mountConnectionRoot>>["connection"]
}> {
  const socket = new MockSocket()
  const { connection: connectionState, ready: connectionReady } = openConnectionState(socket)
  await connectionReady

  const mountPromise = mountConnectionRoot(connectionState, {
    module: "Test.Root",
    id: "root"
  })

  // The per-root channel is created synchronously by `mountConnectionRoot`
  // (join IS the mount). Resolve its join, then deliver the initial patch as a
  // separate frame — real Phoenix drains microtasks between them.
  await Promise.resolve()
  const channel = lastChannel(socket)
  channel.resolveJoin({ root_id: "Test.Root:root" })
  await Promise.resolve()
  channel.emit("patch", initialConnectionEnvelope("Test.Root:root", rootState()))
  const { connection } = await mountPromise

  return { channel, connection }
}

function lastChannel(socket: MockSocket): MockChannel {
  const channel = socket.channels.at(-1)

  if (!channel) {
    throw new Error("Missing mock channel")
  }

  return channel
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
  streamOps: PatchEnvelope["stream_ops"],
  uploadOps: PatchEnvelope["upload_ops"] = []
): ConnectionPatchEnvelope {
  return {
    type: "patch",
    root_id: rootId,
    base_version: baseVersion,
    version,
    ops,
    stream_ops: streamOps,
    upload_ops: uploadOps,
    events: []
  }
}

function rootState(): Record<string, unknown> {
  return {
    __musubi_store_id__: [],
    a: {
      __musubi_store_id__: ["a"],
      v: 1,
      child: {
        __musubi_store_id__: ["a", "child"],
        v: 1
      },
      items: {
        __musubi_stream__: "items"
      },
      avatar: {
        __musubi_upload__: "avatar"
      }
    },
    b: {
      __musubi_store_id__: ["b"],
      v: 1
    }
  }
}
