// Public types for `@musubi/client`.
//
// Consumers thread their generated `Musubi.Stores` type (or any
// store-map type) into the API exactly once via the `createMusubi<R>()`
// factory (from `@musubi/react`) or the `connect<R>()` generic (from
// `@musubi/client`). Every returned handle closes over `R`, so subsequent
// calls infer the store type from the `module` string literal without
// re-threading the registry generic.
//
// All helpers (`ShapeOf`, `CommandsOf`, `StoreSnapshot`, `StoreProxy`, …)
// take the module key first and require the registry type as the second
// generic parameter.

// ---------------------------------------------------------------------------
// Public runtime types
// ---------------------------------------------------------------------------

export type StoreId = string[]

export type AsyncError =
  | { kind: "error"; value: unknown }
  | { kind: "exit"; value: unknown }

export type AsyncResult<T> =
  | { status: "loading"; data: T | null; error: null }
  | { status: "ok"; data: T; error: null }
  | { status: "failed"; data: T | null; error: AsyncError | unknown }

// ---------------------------------------------------------------------------
// Registry-driven accessors
// ---------------------------------------------------------------------------

export type StoreModule<R> = Extract<keyof R, string>

export type DefOf<M extends StoreModule<R>, R> = R[M & keyof R]

type SymbolMarker<T> = T extends object
  ? NonNullable<T[Extract<keyof T, symbol>]>
  : never

type StoreDefMarker<T> = Extract<
  SymbolMarker<T>,
  {
    readonly module: string
    readonly shape: unknown
    readonly commands: unknown
    readonly events: unknown
  }
>

type FieldMarker<T> = Extract<
  SymbolMarker<T>,
  { readonly kind: "store" | "stream" | "async" | "upload" }
>

export type ShapeOf<M extends StoreModule<R>, R> =
  [StoreDefMarker<DefOf<M, R>>] extends [never]
    ? never
    : StoreDefMarker<DefOf<M, R>> extends { readonly shape: infer Shape }
      ? Shape
      : never

export type CommandsOf<M extends StoreModule<R>, R> =
  [StoreDefMarker<DefOf<M, R>>] extends [never]
    ? never
    : StoreDefMarker<DefOf<M, R>> extends { readonly commands: infer Commands }
      ? Commands
      : never

export type CommandName<M extends StoreModule<R>, R> = keyof CommandsOf<M, R>

export type CommandPayload<
  M extends StoreModule<R>,
  K extends CommandName<M, R>,
  R
> = CommandsOf<M, R>[K] extends { payload: infer Payload } ? Payload : never

export type CommandReply<
  M extends StoreModule<R>,
  K extends CommandName<M, R>,
  R
> = CommandsOf<M, R>[K] extends { reply: infer Reply } ? Reply : unknown

export type EventsOf<M extends StoreModule<R>, R> =
  [StoreDefMarker<DefOf<M, R>>] extends [never]
    ? never
    : StoreDefMarker<DefOf<M, R>> extends { readonly events: infer Events }
      ? Events
      : never

export type EventName<M extends StoreModule<R>, R> = keyof EventsOf<M, R> & string

export type EventPayload<
  M extends StoreModule<R>,
  K extends EventName<M, R>,
  R
> = EventsOf<M, R>[K] extends { payload: infer Payload } ? Payload : never

// ---------------------------------------------------------------------------
// Snapshot and proxy projection (symbol-branded generated marker matching)
// ---------------------------------------------------------------------------

type FieldMarkerOfKind<T, Kind extends "store" | "stream" | "async" | "upload"> = Extract<
  FieldMarker<T>,
  { readonly kind: Kind }
>

type IsStoreField<T> = [FieldMarkerOfKind<T, "store">] extends [never] ? false : true
type IsStreamField<T> = [FieldMarkerOfKind<T, "stream">] extends [never] ? false : true
type IsAsyncField<T> = [FieldMarkerOfKind<T, "async">] extends [never] ? false : true
type IsUploadField<T> = [FieldMarkerOfKind<T, "upload">] extends [never] ? false : true

type StoreFieldModule<T> =
  [FieldMarkerOfKind<T, "store">] extends [never]
    ? never
    : FieldMarkerOfKind<T, "store"> extends { readonly module: infer M }
      ? M
      : never
type StreamFieldItem<T> =
  [FieldMarkerOfKind<T, "stream">] extends [never]
    ? never
    : FieldMarkerOfKind<T, "stream"> extends { readonly item: infer Item }
      ? Item
      : never
type AsyncFieldValue<T> =
  [FieldMarkerOfKind<T, "async">] extends [never]
    ? never
    : FieldMarkerOfKind<T, "async"> extends { readonly value: infer Value }
      ? Value
      : never

type SnapshotAsyncValue<T, R> =
  IsStreamField<T> extends true
    ? SnapshotValue<StreamFieldItem<T>, R>[]
    : SnapshotValue<T, R>

export type SnapshotValue<T, R> =
  IsStoreField<T> extends true
    ? StoreFieldModule<T> extends infer M
      ? M extends StoreModule<R>
        ? StoreSnapshot<M, R>
        : never
      : never
    : IsUploadField<T> extends true
      ? UploadHandle
      : IsAsyncField<T> extends true
        ? AsyncResult<SnapshotAsyncValue<AsyncFieldValue<T>, R>>
        : IsStreamField<T> extends true
          ? SnapshotValue<StreamFieldItem<T>, R>[]
          : T extends readonly (infer U)[]
            ? SnapshotValue<U, R>[]
            : T extends object
              ? { [K in keyof T]: SnapshotValue<T[K], R> }
              : T

export type StoreSnapshot<M extends StoreModule<R>, R> = {
  readonly __musubi_store_id__: StoreId
} & {
  [K in keyof ShapeOf<M, R>]: SnapshotValue<ShapeOf<M, R>[K], R>
}

export type ProxyValue<T, R> =
  IsStoreField<T> extends true
    ? StoreFieldModule<T> extends infer M
      ? M extends StoreModule<R>
        ? StoreProxy<M, R>
        : never
      : never
    : IsUploadField<T> extends true
      ? UploadHandle
      : IsAsyncField<T> extends true
        ? AsyncResult<SnapshotAsyncValue<AsyncFieldValue<T>, R>>
        : IsStreamField<T> extends true
          ? SnapshotValue<StreamFieldItem<T>, R>[]
          : T extends readonly (infer U)[]
            ? ProxyValue<U, R>[]
            : T extends object
              ? { [K in keyof T]: ProxyValue<T[K], R> }
              : T

export interface StoreRuntime<M extends StoreModule<R>, R> {
  readonly __musubi_store_id__: StoreId
  dispatchCommand<K extends CommandName<M, R>>(
    name: K,
    payload: CommandPayload<M, K, R>
  ): Promise<CommandReply<M, K, R>>
  subscribe(listener: () => void): () => void
  // Registers a handler for a transient push event (BDR-0032). Events are
  // declared on the root store (`event :name do ... end`) and are root-scoped on
  // the wire, so only the root proxy carries event names — `EventName` is `never`
  // on child proxies. Returns an unsubscribe thunk. The handler is invoked once
  // per matching event, after that envelope's state ops are applied.
  handleEvent<K extends EventName<M, R>>(
    name: K,
    handler: (payload: EventPayload<M, K, R>) => void
  ): () => void
  // `undefined` while the store node is absent from the index (not yet
  // mounted, or mid-reconnect after a state reset). Callers must guard.
  snapshot(): StoreSnapshot<M, R> | undefined
}

export type StoreProxy<M extends StoreModule<R>, R> = StoreRuntime<M, R> & {
  [K in keyof ShapeOf<M, R>]: ProxyValue<ShapeOf<M, R>[K], R>
}

// ---------------------------------------------------------------------------
// Wire shapes
// ---------------------------------------------------------------------------

export type StreamEntry<T> = {
  itemKey: string
  item: T
}

export type WireStreamMarker = {
  __musubi_stream__: string
}

export type WireUploadMarker = {
  __musubi_upload__: string
}

export type UploadStatus =
  | "idle"
  | "selecting"
  | "uploading"
  | "success"
  | "error"
  | "cancelled"

export type EntryStatus =
  | "pending"
  | "uploading"
  | "success"
  | "error"
  | "cancelled"

export interface UploadError {
  code:
    | "too_large"
    | "too_many_files"
    | "not_accepted"
    | "chunk_timeout"
    | "external_failed"
    | "preflight_rejected"
    | (string & {})
  message: string
}

export interface UploadEntry {
  ref: string
  clientName: string
  clientSize: number
  clientType: string
  progress: number
  status: EntryStatus
  errors: UploadError[]
  readonly isPending: boolean
  readonly isUploading: boolean
  readonly isSuccess: boolean
  readonly isError: boolean
  readonly isCancelled: boolean
}

export interface UploadConfig {
  accept: string[] | "any"
  maxEntries: number
  maxFileSize: number
  chunkSize: number
}

export interface UploadHandle {
  readonly config: UploadConfig
  readonly status: UploadStatus
  readonly entries: readonly UploadEntry[]
  readonly errors: readonly UploadError[]
  readonly progress: number
  readonly isIdle: boolean
  readonly isSelecting: boolean
  readonly isUploading: boolean
  readonly isSuccess: boolean
  readonly isError: boolean
  select(files: FileList | File[]): Promise<readonly UploadEntry[]>
  start(): Promise<void>
  cancel(entryRef?: string): Promise<void>
  reset(): Promise<void>
}

export interface ExternalUploaderArgs {
  entry: UploadEntry
  file: File
  meta: unknown
  onProgress: (pct: number) => void
  signal: AbortSignal
}

export type ExternalUploader = (args: ExternalUploaderArgs) => Promise<void>

// Wire shapes for upload_ops
export type UploadOp =
  | {
      op: "config"
      upload: string
      store_id: StoreId
      config: {
        accept: string[] | "any"
        max_entries: number
        max_file_size: number
        chunk_size: number
      }
    }
  | {
      op: "add"
      upload: string
      store_id: StoreId
      ref: string
      entry: {
        ref: string
        client_name: string
        client_size: number
        client_type: string
        progress: number
        status: EntryStatus
        errors: UploadError[]
      }
    }
  | {
      op: "progress"
      upload: string
      store_id: StoreId
      ref: string
      progress: number
    }
  | { op: "complete"; upload: string; store_id: StoreId; ref: string }
  | {
      op: "error"
      upload: string
      store_id: StoreId
      ref?: string
      error: UploadError
    }
  | { op: "cancel"; upload: string; store_id: StoreId; ref: string }
  | { op: "reset"; upload: string; store_id: StoreId }

export type JsonPatchOp =
  | { op: "add"; path: string; value: unknown }
  | { op: "remove"; path: string }
  | { op: "replace"; path: string; value: unknown }

export type StreamOp =
  | { op: "reset"; stream: string; ref: string; store_id: StoreId }
  | {
      op: "insert"
      stream: string
      ref: string
      store_id: StoreId
      item_key: string
      at: number
      item: unknown
      limit: number | null
    }
  | {
      op: "delete"
      stream: string
      ref: string
      store_id: StoreId
      item_key: string
    }

// Transient push event (BDR-0032) folded into the patch envelope. Dispatched
// once on receipt, owns no version/recovery semantics.
export type PushEvent = {
  name: string
  payload: unknown
}

export type PatchEnvelope = {
  type: "patch"
  base_version: number
  version: number
  ops: JsonPatchOp[]
  stream_ops: StreamOp[]
  upload_ops: UploadOp[]
  events: PushEvent[]
}

export type ConnectionPatchEnvelope = PatchEnvelope & {
  root_id: string
}

export type WireAsyncError =
  | { kind: "error"; value: unknown }
  | { kind: "exit"; value: unknown }

export type WireAsyncResult<T = unknown> =
  | { __musubi_async__: true; status: "loading"; result: T | null; reason: null }
  | { __musubi_async__: true; status: "ok"; result: T; reason: null }
  | { __musubi_async__: true; status: "failed"; result: T | null; reason: WireAsyncError | unknown }

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

export const STORE_ID_KEY = "__musubi_store_id__" as const
export const STREAM_MARKER_KEY = "__musubi_stream__" as const
export const UPLOAD_MARKER_KEY = "__musubi_upload__" as const

const UPLOAD_KEY_SEP = "\0"

export function uploadStoreKey(storeId: StoreId, uploadName: string): string {
  return `${storeIdKey(storeId)}${UPLOAD_KEY_SEP}${uploadName}`
}

export function storeIdKey(storeId: StoreId): string {
  return JSON.stringify(storeId)
}

const STREAM_KEY_SEP = "\0"

export function streamStoreKey(storeId: StoreId, streamName: string): string {
  return `${storeIdKey(storeId)}${STREAM_KEY_SEP}${streamName}`
}

export function streamStoreKeyPrefix(storeId: StoreId): string {
  return `${storeIdKey(storeId)}${STREAM_KEY_SEP}`
}

export function streamFieldNameFromKey(key: string): string {
  const parts = key.split(STREAM_KEY_SEP)
  return parts[1] ?? ""
}

export function storeKeyFromStreamStoreKey(key: string): string {
  const parts = key.split(STREAM_KEY_SEP)
  return parts[0] ?? key
}
