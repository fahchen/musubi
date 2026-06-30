import { describe, expect, test, vi } from "vitest"

import {
  applyUploadOps,
  getUploadHandle,
  pruneUploads,
  UploadHandleImpl
} from "../src/uploads"
import type { ChannelLike, PushLike, RootConnection } from "../src/runtime"
import type { ExternalUploader } from "../src/types"

interface CapturedPush {
  event: string
  payload: Record<string, unknown>
}

interface FakeChannelHandlers {
  [event: string]: (payload: Record<string, unknown>) => unknown
}

function makeFakeChannel(
  handlers: FakeChannelHandlers,
  captured: CapturedPush[]
): ChannelLike {
  return {
    on: () => undefined,
    onClose: () => undefined,
    onError: () => undefined,
    join: () => makeFakePush({}),
    leave: () => undefined,
    push: (event: string, payload: unknown) => {
      const p = (payload ?? {}) as Record<string, unknown>
      captured.push({ event, payload: p })
      const reply = handlers[event]?.(p)
      return makeFakePush(reply ?? {})
    }
  }
}

function makeFakePush(reply: unknown): PushLike {
  const self: PushLike = {
    receive(status, cb) {
      if (status === "ok") queueMicrotask(() => cb(reply))
      return self
    }
  }
  return self
}

function fakeConnection(options: {
  channel?: ChannelLike
  uploaders?: Record<string, ExternalUploader>
} = {}): RootConnection {
  return {
    module: "X",
    id: "r1",
    connection: {
      socket: {
        connect: () => undefined,
        channel: () => options.channel ?? ({} as never)
      },
      topic: "musubi:t",
      roots: new Map(),
      uploaders: options.uploaders ?? {},
      channel: options.channel,
      channelGeneration: 0,
      connectPromise: null,
      suppressDisconnectEvent: false
    },
    mountParams: {},
    refCount: 1,
    channel: options.channel,
    channelGeneration: 0,
    root: undefined,
    version: 0,
    storeIndex: new Map(),
    streams: new Map(),
    uploads: new Map(),
    proxyCache: new Map(),
    snapshotCache: new Map(),
    storeListeners: new Map(),
    pendingCommandRejectors: new Set(),
    pendingConnect: null,
    connectPromise: null,
    recovering: false
  } as unknown as RootConnection
}

function externalPreflightReply(entryRef: string) {
  return {
    ref: "avatar",
    config: {
      accept: "any" as const,
      max_entries: 1,
      max_file_size: 1_000_000,
      chunk_size: 64_000
    },
    entries: {
      "0": {
        type: "external" as const,
        entry_ref: entryRef,
        uploader: "S3",
        meta: { url: "https://example.com/put" }
      }
    },
    errors: []
  }
}

describe("UploadHandle op application", () => {
  test("config op updates handle.config", () => {
    const conn = fakeConnection()
    const handle = getUploadHandle(conn, [], "avatar")

    applyUploadOps(conn, [
      {
        op: "config",
        upload: "avatar",
        store_id: [],
        config: {
          accept: [".png"],
          max_entries: 3,
          max_file_size: 1_000_000,
          chunk_size: 128_000
        }
      }
    ])

    expect(handle.config).toEqual({
      accept: [".png"],
      maxEntries: 3,
      maxFileSize: 1_000_000,
      chunkSize: 128_000
    })
  })

  test("add op materializes an entry on the handle", () => {
    const conn = fakeConnection()
    const handle = getUploadHandle(conn, [], "avatar")

    applyUploadOps(conn, [
      {
        op: "add",
        upload: "avatar",
        store_id: [],
        ref: "e1",
        entry: {
          ref: "e1",
          client_name: "a.png",
          client_size: 100,
          client_type: "image/png",
          progress: 0,
          status: "pending",
          errors: []
        }
      }
    ])

    expect(handle.entries).toHaveLength(1)
    expect(handle.entries[0]?.clientName).toBe("a.png")
    expect(handle.entries[0]?.status).toBe("pending")
  })

  test("progress op updates entry.progress and status", () => {
    const conn = fakeConnection()
    const handle = getUploadHandle(conn, [], "avatar")

    applyUploadOps(conn, [
      {
        op: "add",
        upload: "avatar",
        store_id: [],
        ref: "e1",
        entry: {
          ref: "e1",
          client_name: "a.png",
          client_size: 100,
          client_type: "image/png",
          progress: 0,
          status: "pending",
          errors: []
        }
      },
      { op: "progress", upload: "avatar", store_id: [], ref: "e1", progress: 50 }
    ])

    expect(handle.entries[0]?.progress).toBe(50)
    expect(handle.entries[0]?.status).toBe("uploading")
  })

  test("complete op marks entry success and updates aggregate", () => {
    const conn = fakeConnection()
    const handle = getUploadHandle(conn, [], "avatar")

    applyUploadOps(conn, [
      {
        op: "add",
        upload: "avatar",
        store_id: [],
        ref: "e1",
        entry: {
          ref: "e1",
          client_name: "a.png",
          client_size: 100,
          client_type: "image/png",
          progress: 0,
          status: "pending",
          errors: []
        }
      },
      { op: "complete", upload: "avatar", store_id: [], ref: "e1" }
    ])

    expect(handle.entries[0]?.status).toBe("success")
    expect(handle.entries[0]?.progress).toBe(100)
    expect(handle.progress).toBe(100)
  })

  test("error op marks entry error", () => {
    const conn = fakeConnection()
    const handle = getUploadHandle(conn, [], "avatar")

    applyUploadOps(conn, [
      {
        op: "add",
        upload: "avatar",
        store_id: [],
        ref: "e1",
        entry: {
          ref: "e1",
          client_name: "big.png",
          client_size: 100,
          client_type: "image/png",
          progress: 0,
          status: "pending",
          errors: []
        }
      },
      {
        op: "error",
        upload: "avatar",
        store_id: [],
        ref: "e1",
        error: { code: "too_large", message: "file exceeds the maximum size" }
      }
    ])

    expect(handle.entries[0]?.status).toBe("error")
    expect(handle.entries[0]?.errors[0]?.code).toBe("too_large")
  })

  test("cancel op removes the entry", () => {
    const conn = fakeConnection()
    const handle = getUploadHandle(conn, [], "avatar")

    applyUploadOps(conn, [
      {
        op: "add",
        upload: "avatar",
        store_id: [],
        ref: "e1",
        entry: {
          ref: "e1",
          client_name: "a.png",
          client_size: 1,
          client_type: "",
          progress: 0,
          status: "pending",
          errors: []
        }
      },
      { op: "cancel", upload: "avatar", store_id: [], ref: "e1" }
    ])

    expect(handle.entries).toHaveLength(0)
  })

  test("reset op clears all entries and errors", () => {
    const conn = fakeConnection()
    const handle = getUploadHandle(conn, [], "avatar")

    applyUploadOps(conn, [
      {
        op: "add",
        upload: "avatar",
        store_id: [],
        ref: "e1",
        entry: {
          ref: "e1",
          client_name: "a.png",
          client_size: 1,
          client_type: "",
          progress: 100,
          status: "success",
          errors: []
        }
      },
      { op: "reset", upload: "avatar", store_id: [] }
    ])

    expect(handle.entries).toHaveLength(0)
    expect(handle.errors).toHaveLength(0)
  })
})

describe("getUploadHandle identity", () => {
  test("returns the same handle reference for repeat lookups", () => {
    const conn = fakeConnection()
    const h1 = getUploadHandle(conn, [], "avatar")
    const h2 = getUploadHandle(conn, [], "avatar")
    expect(h1).toBe(h2)
  })

  test("distinct {storeId, name} pairs get distinct handles", () => {
    const conn = fakeConnection()
    const h1 = getUploadHandle(conn, [], "avatar")
    const h2 = getUploadHandle(conn, ["line-1"], "avatar")
    const h3 = getUploadHandle(conn, [], "cover")
    expect(h1).not.toBe(h2)
    expect(h1).not.toBe(h3)
  })
})

describe("subscribe notifies on op application", () => {
  test("subscribe fires after each applyOps call", () => {
    const conn = fakeConnection()
    const handle = getUploadHandle(conn, [], "avatar") as UploadHandleImpl

    const listener = vi.fn()
    handle.subscribe(listener)

    applyUploadOps(conn, [
      {
        op: "config",
        upload: "avatar",
        store_id: [],
        config: { accept: "any", max_entries: 1, max_file_size: 1, chunk_size: 1 }
      }
    ])

    expect(listener).toHaveBeenCalledTimes(1)
  })
})

describe("pruneUploads", () => {
  test("drops uploads whose store_id is no longer valid", () => {
    const conn = fakeConnection()
    getUploadHandle(conn, [], "avatar")
    getUploadHandle(conn, ["dead-line"], "attachment")

    pruneUploads(conn.uploads, new Set(["[]"]))

    expect(conn.uploads.size).toBe(1)
  })
})

describe("custom external uploader contract", () => {
  test("registered uploader receives entry/file/meta + reports progress", async () => {
    const captured: CapturedPush[] = []
    const channel = makeFakeChannel(
      { allow_upload: () => externalPreflightReply("u_ok") },
      captured
    )

    const uploader = vi.fn<ExternalUploader>(async ({ onProgress }) => {
      onProgress(33)
      onProgress(100)
    })

    const conn = fakeConnection({ channel, uploaders: { S3: uploader } })
    const handle = getUploadHandle(conn, [], "avatar")

    const file = new File(["hello"], "a.png", { type: "image/png" })
    await handle.select([file])
    await handle.start()

    expect(uploader).toHaveBeenCalledOnce()

    const args = uploader.mock.calls[0]?.[0] as Parameters<ExternalUploader>[0]
    expect(args.file).toBe(file)
    expect(args.meta).toEqual({ url: "https://example.com/put" })
    expect(args.entry.ref).toBe("u_ok")
    expect(typeof args.onProgress).toBe("function")
    expect(args.signal).toBeInstanceOf(AbortSignal)

    const progressPushes = captured
      .filter((c) => c.event === "upload_progress")
      .map((c) => c.payload.progress)
    expect(progressPushes).toContain(33)
    expect(progressPushes).toContain(100)
  })

  test("uploader rejection pushes upload_error with external_failed", async () => {
    const captured: CapturedPush[] = []
    const channel = makeFakeChannel(
      { allow_upload: () => externalPreflightReply("u_fail") },
      captured
    )

    const uploader: ExternalUploader = () => Promise.reject(new Error("PUT 500"))

    const conn = fakeConnection({ channel, uploaders: { S3: uploader } })
    const handle = getUploadHandle(conn, [], "avatar")

    const file = new File(["x"], "a.png", { type: "image/png" })
    await handle.select([file])
    await handle.start()

    const errorPush = captured.find((c) => c.event === "upload_error")
    expect(errorPush).toBeDefined()
    expect(errorPush?.payload.code).toBe("external_failed")
    expect(errorPush?.payload.ref).toBe("u_fail")
    expect(errorPush?.payload.message).toContain("PUT 500")

    // `start()` swallows the rejection and flips handle status to error.
    expect(handle.status).toBe("error")
  })

  test("missing uploader registration flips handle status to error", async () => {
    const captured: CapturedPush[] = []
    const channel = makeFakeChannel(
      { allow_upload: () => externalPreflightReply("u_missing_reg") },
      captured
    )

    // No uploader registered under "S3" — startExternal throws.
    const conn = fakeConnection({ channel, uploaders: {} })
    const handle = getUploadHandle(conn, [], "avatar")

    const file = new File(["x"], "a.png", { type: "image/png" })
    await handle.select([file])
    await handle.start()

    expect(handle.status).toBe("error")
  })
})
