// Per-upload reactive handle exposed at `page.<name>` via the proxy.
//
// `UploadHandleImpl` is a stable mutable object whose reference is kept in
// `connection.uploads` for the connection lifetime. Upload ops mutate the
// handle in place; subscribers run after each batch so React (or any other
// adapter) can re-render without losing identity on the handle.

import type {
  ChannelLike,
  PushLike,
  RootConnection
} from "./runtime"
import type {
  EntryStatus,
  ExternalUploaderArgs,
  StoreId,
  UploadConfig,
  UploadEntry,
  UploadError,
  UploadHandle,
  UploadOp,
  UploadStatus
} from "./types"
import { uploadStoreKey } from "./types"

const DEFAULT_CONFIG: UploadConfig = {
  accept: "any",
  maxEntries: 1,
  maxFileSize: 8_000_000,
  chunkSize: 64_000
}

interface InternalEntry {
  ref: string
  clientName: string
  clientSize: number
  clientType: string
  progress: number
  status: EntryStatus
  errors: UploadError[]

  // Channel-mode runtime state (populated by `start()` after select).
  file: File | undefined
  channel: ChannelLike | undefined
  uploader: string | undefined
  meta: unknown
  abortController: AbortController | undefined
}

export class UploadHandleImpl implements UploadHandle {
  readonly storeId: StoreId
  readonly uploadName: string

  // Reactive surface — read by the proxy/snapshot path.
  config: UploadConfig = { ...DEFAULT_CONFIG }
  status: UploadStatus = "idle"
  entries: UploadEntry[] = []
  errors: UploadError[] = []

  // Internal — not exposed on the public surface.
  private internalEntries: Map<string, InternalEntry> = new Map()
  private listeners: Set<() => void> = new Set()
  private connection: RootConnection

  constructor(connection: RootConnection, storeId: StoreId, uploadName: string) {
    this.connection = connection
    this.storeId = storeId
    this.uploadName = uploadName
  }

  // ---- Reactive computed fields ------------------------------------------

  get progress(): number {
    if (this.entries.length === 0) return 0
    const total = this.entries.reduce((sum, e) => sum + e.progress, 0)
    return Math.round(total / this.entries.length)
  }

  get isIdle(): boolean { return this.status === "idle" }
  get isSelecting(): boolean { return this.status === "selecting" }
  get isUploading(): boolean { return this.status === "uploading" }
  get isSuccess(): boolean { return this.status === "success" }
  get isError(): boolean { return this.status === "error" }

  // ---- Subscriptions -----------------------------------------------------

  subscribe(listener: () => void): () => void {
    this.listeners.add(listener)
    return () => { this.listeners.delete(listener) }
  }

  notify(): void {
    for (const listener of this.listeners) listener()
  }

  // ---- Public API --------------------------------------------------------

  async select(files: FileList | File[]): Promise<readonly UploadEntry[]> {
    const arr: File[] = Array.from(files as FileList | File[])

    if (arr.length === 0) return this.entries

    this.status = "selecting"
    this.errors = []
    this.notify()

    const channel = this.connection.channel
    if (!channel) throw new Error("Connection is not open")

    const entriesPayload = arr.map((file, index) => ({
      client_ref: String(index),
      name: file.name,
      size: file.size,
      type: file.type
    }))

    const reply = await pushReceive<{
      ref: string
      config: {
        accept: string[] | "any"
        max_entries: number
        max_file_size: number
        chunk_size: number
      }
      entries: Record<string, {
        type: "channel" | "external"
        entry_ref: string
        token?: string
        uploader?: string
        meta?: unknown
      }>
      errors: { client_ref: string; error: UploadError }[]
    }>(
      channel.push("allow_upload", {
        store_id: [...this.storeId],
        name: this.uploadName,
        entries: entriesPayload
      }) as PushLike
    )

    this.config = {
      accept: reply.config.accept,
      maxEntries: reply.config.max_entries,
      maxFileSize: reply.config.max_file_size,
      chunkSize: reply.config.chunk_size
    }

    // The server already emits `{op: add}` ops over `upload_ops`; selecting
    // also stashes the matching File and per-entry transport meta on the
    // handle's internal index so `start()` can pick them up.
    for (const [clientRef, accepted] of Object.entries(reply.entries)) {
      const file = arr[Number(clientRef)]
      const existing = this.internalEntries.get(accepted.entry_ref) ?? null

      const merged: InternalEntry = existing ?? {
        ref: accepted.entry_ref,
        clientName: file?.name ?? "",
        clientSize: file?.size ?? 0,
        clientType: file?.type ?? "",
        progress: 0,
        status: "pending",
        errors: [],
        file: undefined,
        channel: undefined,
        uploader: undefined,
        meta: undefined,
        abortController: undefined
      }

      merged.file = file
      merged.uploader = accepted.uploader
      merged.meta = accepted.meta
      this.internalEntries.set(accepted.entry_ref, merged)

      if (accepted.type === "channel" && accepted.token) {
        this.pendingTokens.set(accepted.entry_ref, accepted.token)
      }
    }

    this.errors = reply.errors.map((e) => e.error)
    this.status = reply.errors.length > 0 ? "error" : "selecting"
    this.refreshEntries()
    this.notify()

    return this.entries
  }

  async start(): Promise<void> {
    this.status = "uploading"
    this.notify()

    const channel = this.connection.channel
    if (!channel) throw new Error("Connection is not open")

    const socket = this.connection.connection.socket

    const tasks: Promise<void>[] = []

    for (const entry of this.internalEntries.values()) {
      if (entry.uploader) {
        tasks.push(this.startExternal(entry))
      } else {
        tasks.push(this.startChannel(socket, entry))
      }
    }

    try {
      await Promise.all(tasks)
      const hasError = this.entries.some((e) => e.status === "error")
      this.status = hasError ? "error" : "success"
    } catch {
      this.status = "error"
    }

    this.notify()
  }

  async cancel(entryRef?: string): Promise<void> {
    const channel = this.connection.channel
    if (!channel) return

    const refs = entryRef
      ? [entryRef]
      : Array.from(this.internalEntries.keys())

    for (const ref of refs) {
      const entry = this.internalEntries.get(ref)

      if (entry?.abortController) {
        entry.abortController.abort()
      }

      if (entry?.channel) {
        try { entry.channel.leave() } catch { /* noop */ }
      }

      await pushReceive(
        channel.push("cancel_upload", {
          store_id: [...this.storeId],
          name: this.uploadName,
          ref
        }) as PushLike
      )
    }
  }

  async reset(): Promise<void> {
    await this.cancel()
    this.internalEntries.clear()
    this.entries = []
    this.errors = []
    this.status = "idle"
    this.notify()
  }

  // ---- Op application (called by runtime) --------------------------------

  applyOps(ops: UploadOp[]): void {
    let touched = false

    for (const op of ops) {
      switch (op.op) {
        case "config":
          this.config = {
            accept: op.config.accept,
            maxEntries: op.config.max_entries,
            maxFileSize: op.config.max_file_size,
            chunkSize: op.config.chunk_size
          }
          touched = true
          break

        case "add": {
          const wire = op.entry
          const next: InternalEntry = this.internalEntries.get(op.ref) ?? {
            ref: op.ref,
            clientName: wire.client_name,
            clientSize: wire.client_size,
            clientType: wire.client_type,
            progress: wire.progress,
            status: wire.status,
            errors: wire.errors ?? [],
            file: undefined,
            channel: undefined,
            uploader: undefined,
            meta: undefined,
            abortController: undefined
          }
          next.progress = wire.progress
          next.status = wire.status
          next.errors = wire.errors ?? []
          this.internalEntries.set(op.ref, next)
          touched = true
          break
        }

        case "progress": {
          const entry = this.internalEntries.get(op.ref)
          if (entry) {
            entry.progress = op.progress
            entry.status = op.progress >= 100 ? "success" : "uploading"
            touched = true
          }
          break
        }

        case "complete": {
          const entry = this.internalEntries.get(op.ref)
          if (entry) {
            entry.progress = 100
            entry.status = "success"
            touched = true
          }
          break
        }

        case "error": {
          if (op.ref) {
            const entry = this.internalEntries.get(op.ref)
            if (entry) {
              entry.status = "error"
              entry.errors = [...entry.errors, op.error]
            }
          } else {
            this.errors = [...this.errors, op.error]
          }
          touched = true
          break
        }

        case "cancel": {
          this.internalEntries.delete(op.ref)
          touched = true
          break
        }

        case "reset": {
          this.internalEntries.clear()
          this.errors = []
          touched = true
          break
        }
      }
    }

    if (touched) {
      this.refreshEntries()
      this.notify()
    }
  }

  // ---- Internals ---------------------------------------------------------

  private pendingTokens: Map<string, string> = new Map()

  private refreshEntries(): void {
    this.entries = Array.from(this.internalEntries.values()).map((e) => projectEntry(e))
  }

  private async startChannel(
    socket: { channel: (topic: string, payload?: object) => ChannelLike },
    entry: InternalEntry
  ): Promise<void> {
    const file = entry.file
    const token = this.pendingTokens.get(entry.ref)
    if (!file || !token) return

    const topic = `musubi_upload:${entry.ref}`
    const channel = socket.channel(topic, { token })
    entry.channel = channel

    try {
      await pushReceive(channel.join() as PushLike)

      const chunkSize = this.config.chunkSize

      // Server completes on the final chunk: when bytes_written reaches
      // client_size, it replies `progress: 100` and stops the channel.
      // No separate "close" event — the reply to the last chunk is the
      // completion signal.
      for (let offset = 0; offset < file.size; offset += chunkSize) {
        const slice = await file.slice(offset, offset + chunkSize).arrayBuffer()
        await pushReceive(channel.push("chunk", slice as ArrayBuffer) as PushLike)
      }
    } finally {
      this.pendingTokens.delete(entry.ref)
    }
  }

  private async startExternal(entry: InternalEntry): Promise<void> {
    if (!entry.file || !entry.uploader) return

    const uploaderName = entry.uploader
    const uploader = this.connection.connection.uploaders?.[uploaderName]

    if (!uploader) {
      throw new Error(`No registered uploader for "${uploaderName}"`)
    }

    const channel = this.connection.channel
    if (!channel) throw new Error("Connection is not open")

    const controller = new AbortController()
    entry.abortController = controller

    const args: ExternalUploaderArgs = {
      entry: projectEntry(entry),
      file: entry.file,
      meta: entry.meta,
      onProgress: (pct: number) => {
        channel.push("upload_progress", {
          store_id: [...this.storeId],
          name: this.uploadName,
          ref: entry.ref,
          progress: Math.max(0, Math.min(100, Math.round(pct)))
        })
      },
      signal: controller.signal
    }

    try {
      await uploader(args)
      args.onProgress(100)
    } catch (err) {
      // Wire the failure back to the server so the page server can
      // emit `{op: error, code: "external_failed"}` and the matching
      // entry transitions to `:error` (spec/domains/uploads/features/
      // external.feature § "Registered uploader rejects the PUT").
      channel.push("upload_error", {
        store_id: [...this.storeId],
        name: this.uploadName,
        ref: entry.ref,
        code: "external_failed",
        message: err instanceof Error ? err.message : String(err)
      })

      throw err
    } finally {
      entry.abortController = undefined
    }
  }
}

function projectEntry(entry: InternalEntry): UploadEntry {
  return {
    ref: entry.ref,
    clientName: entry.clientName,
    clientSize: entry.clientSize,
    clientType: entry.clientType,
    progress: entry.progress,
    status: entry.status,
    errors: [...entry.errors],
    get isPending() { return entry.status === "pending" },
    get isUploading() { return entry.status === "uploading" },
    get isSuccess() { return entry.status === "success" },
    get isError() { return entry.status === "error" },
    get isCancelled() { return entry.status === "cancelled" }
  }
}

// ---------------------------------------------------------------------------
// Connection helpers (registry of UploadHandle instances per connection)
// ---------------------------------------------------------------------------

export function getUploadHandle(
  connection: RootConnection,
  storeId: StoreId,
  uploadName: string
): UploadHandleImpl {
  const key = uploadStoreKey(storeId, uploadName)
  const existing = connection.uploads.get(key)

  if (existing) return existing

  const handle = new UploadHandleImpl(connection, storeId, uploadName)
  connection.uploads.set(key, handle)
  return handle
}

export function applyUploadOps(
  connection: RootConnection,
  ops: readonly UploadOp[]
): ReadonlySet<string> {
  const touched = new Set<string>()
  const byHandle = new Map<UploadHandleImpl, UploadOp[]>()

  for (const op of ops) {
    const handle = getUploadHandle(connection, op.store_id, op.upload)
    const list = byHandle.get(handle) ?? []
    list.push(op)
    byHandle.set(handle, list)
    touched.add(`${JSON.stringify(op.store_id)}\0${op.upload}`)
  }

  for (const [handle, handleOps] of byHandle) {
    handle.applyOps(handleOps)
  }

  return touched
}

export function pruneUploads(
  uploads: Map<string, UploadHandleImpl>,
  validStoreIds: ReadonlySet<string>
): void {
  for (const key of Array.from(uploads.keys())) {
    const storeKey = key.split("\0")[0] ?? ""
    if (!validStoreIds.has(storeKey)) {
      uploads.delete(key)
    }
  }
}

export function touchedStoresFromUploadOps(ops: readonly UploadOp[]): ReadonlySet<string> {
  return new Set(ops.map((op) => JSON.stringify(op.store_id)))
}

// ---------------------------------------------------------------------------
// Small helper to await a Phoenix push and produce a Promise
// ---------------------------------------------------------------------------

function pushReceive<T = unknown>(push: PushLike): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    push
      .receive("ok", (reply: unknown) => resolve(reply as T))
      .receive("error", (reply: unknown) => reject(new Error(JSON.stringify(reply))))
      .receive("timeout", () => reject(new Error("timeout")))
  })
}
