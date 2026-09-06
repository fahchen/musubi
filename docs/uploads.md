# Uploads

Uploads are a transport-layer capability that lets a Musubi store accept
binary file uploads from connected clients. The server retains only a
short-lived temporary file per upload entry; the client owns the lifecycle
(select → start → cancel/complete). Unlike streams, uploads are not state:
they are declared separately and the framework auto-injects wire markers
so application code never composes upload state by hand.

## Declaration

Uploads are declared at the top level of a store module, outside `state do`:

```elixir
defmodule MyApp.Stores.AvatarStore do
  use Musubi.Store, root: true

  state do
    field :avatar_url, String.t() | nil
  end

  upload :avatar,
    accept: ~w(.jpg .jpeg .png),
    max_entries: 1,
    max_file_size: 5_000_000,
    chunk_size: 64_000,
    chunk_timeout: 10_000

  upload :cover,
    accept: ~w(.jpg .png),
    max_entries: 5
end
```

### Options

| Option           | Type                          | Default       | Notes                              |
| :--------------- | :---------------------------- | :------------ | :--------------------------------- |
| `:accept`        | `[String.t()]` \| `:any`      | required      | Extension list (`.png`) or `:any`  |
| `:max_entries`   | `pos_integer()`               | `1`           | Cap on simultaneous entries        |
| `:max_file_size` | `pos_integer()`               | `8_000_000`   | Bytes per entry                    |
| `:chunk_size`    | `pos_integer()`               | `64_000`      | Channel chunk size                 |
| `:chunk_timeout` | `pos_integer()`               | `10_000`      | ms between chunks before failure   |

### Compile-time validation

- Upload names must be unique within a store.
- An upload name must not collide with any state field name in the same
  store (the client surface is flat at `store.<name>`).
- Uploads may only be declared at the top level of a store. They are
  rejected inside `state do`, `field do ... end`, list type specs, or
  stream blocks.

## Render placement

`render/1` does **not** place upload markers. The framework auto-injects a
marker for each declared upload at the root of the store's render output
after `render/1` returns:

```elixir
def render(socket) do
  %{avatar_url: socket.assigns.avatar_url}
end
```

becomes

```json
{
  "avatar_url": null,
  "avatar": {"__musubi_upload__": "avatar"},
  "cover":  {"__musubi_upload__": "cover"},
  "__musubi_store_id__": []
}
```

Hand-written `__musubi_upload__` markers in render output are rejected,
matching the stream rule.

## Server callbacks

Two optional `Musubi.Store` callbacks dispatch upload-related events:

```elixir
@callback handle_progress(name :: atom(), entry :: Entry.t(), Socket.t()) ::
            {:noreply, Socket.t()}

@callback upload_external(name :: atom(), entry :: Entry.t(), Socket.t()) ::
            {:ok, meta :: map(), Socket.t()}
```

Both are `@optional_callbacks`. When `handle_progress/3` is not defined the
runtime skips the per-chunk callback. When `upload_external/3` is not
defined for an upload name the transport falls back to the channel-based
chunk path.

## Helpers (Store facade)

```elixir
consume_uploaded_entries(socket, name, fun) :: {Socket.t(), [term()]}
# fun = (meta, entry -> {:ok, val} | {:postpone, val})
# meta = %{path: String.t()}  in channel mode
#      | %{external: map()}   in external mode

cancel_upload(socket, name, ref) :: Socket.t()
uploaded_entries(socket, name)   :: {completed :: [Entry.t()], in_progress :: [Entry.t()]}
```

`consume_uploaded_entries/3` may only be called from a command handler;
calling it outside a handler raises `ArgumentError`. It returns the
updated socket plus the list of values produced by the function. Entries
that the function consumed with `{:ok, val}` are removed from the index;
postponed entries stay. When the upload's entry index becomes empty as
a result, a `{op: reset}` is emitted.

## Entry struct and wire whitelist

```elixir
defmodule Musubi.Upload.Entry do
  @derive {Musubi.Wire, only: [
    :ref, :client_name, :client_size, :client_type,
    :progress, :status, :errors
  ]}

  defstruct [
    # Wire-public
    :ref, :client_name, :client_size, :client_type,
    :progress, :status, :errors,

    # Server-private — must never appear in wire output
    :path, :token, :store_pid, :upload_channel_pid,
    :bytes_written, :external_meta, :preflighted_at
  ]

  @type status() :: :pending | :uploading | :success | :error | :cancelled
end
```

Errors carry a stable `code` atom and a user-friendly `message` string.
Messages must never embed paths, pids, tokens, IP addresses, or other
infrastructure detail.

## Wire protocol

### Topics

- Main page channel: `musubi:<opaque_ref>` (unchanged)
- Per-entry upload channel: `musubi_upload:<entry_ref>`

### New main-channel events

| Event              | Direction | Payload                                                                       |
| :----------------- | :-------- | :---------------------------------------------------------------------------- |
| `allow_upload`     | C → S     | `{name, entries: [{client_ref, name, size, type}]}`                           |
| `cancel_upload`    | C → S     | `{name, ref}`                                                                 |
| `upload_progress`  | C → S     | `{name, ref, progress}` (external mode only)                                  |

`allow_upload` reply (preflight result):

```json
{
  "ref": "avatar",
  "config": {
    "accept": [".jpg", ".jpeg", ".png"],
    "max_entries": 1,
    "max_file_size": 5000000,
    "chunk_size": 64000
  },
  "entries": {
    "0": {"type": "channel",  "entry_ref": "u_a3f", "token": "SFMyNTY..."},
    "1": {"type": "external", "entry_ref": "u_b9e", "uploader": "S3",
          "meta": {"url": "https://...", "headers": {}}}
  },
  "errors": []
}
```

Wire keys use snake_case; the TypeScript `UploadHandle.config` exposes
the camelCase API (`maxEntries`, `maxFileSize`, `chunkSize`) after the
client deserializes the reply.

### Per-entry sub-channel (channel mode)

```
join:   topic "musubi_upload:e_001"   payload {token}
event:  "chunk"                       payload <ArrayBuffer>   (Phoenix binary frame)
reply:  {progress: 0..100}
```

### Wire marker (auto-injected)

The framework injects one marker per declared upload at the root of the
store's render output:

```json
{"__musubi_upload__": "name"}
```

### Independent `upload_ops` stream

The envelope wire shape adds a third op array alongside `ops` and
`stream_ops`:

```json
{
  "type": "patch",
  "base_version": 5,
  "version": 6,
  "ops": [...],
  "stream_ops": [...],
  "upload_ops": [
    {"op": "config",   "upload": "avatar", "store_id": [], "config": {...}},
    {"op": "add",      "upload": "avatar", "store_id": [], "ref": "e_001", "entry": {...}},
    {"op": "progress", "upload": "avatar", "store_id": [], "ref": "e_001", "progress": 33},
    {"op": "complete", "upload": "avatar", "store_id": [], "ref": "e_001"},
    {"op": "error",    "upload": "avatar", "store_id": [], "ref": "e_002", "error": {"code":"too_large","message":"..."}},
    {"op": "cancel",   "upload": "avatar", "store_id": [], "ref": "e_001"},
    {"op": "reset",    "upload": "avatar", "store_id": []}
  ]
}
```

Op vocabulary: `config / add / progress / complete / error / cancel / reset`.
Every op carries a `store_id` matching the path of the store that declared
the upload.

### Coalescing and change tracking

Pending upload ops queue on `socket.assigns.__uploads__.__pending_ops__`.
During transport drain, consecutive `progress` ops sharing
`{store_id, upload, ref}` collapse to the latest value. A throttle limits
progress emission to 10 Hz by default.

Upload state mutation does **not** mark any other `socket.assigns` key as
changed, so progress updates do not trigger main-store re-renders.

## Token

```elixir
Phoenix.Token.sign(endpoint, "musubi_upload", %{
  store_pid:     pid(),
  store_id:      [String.t()],
  conf_ref:      String.t(),
  entry_ref:     String.t(),
  max_file_size: integer(),
  client_size:   integer(),
  accept:        [String.t()] | :any,
  chunk_size:    integer(),
  chunk_timeout: integer()
})
```

`store_id` is included so the sub-channel can route per-entry chunk
notifications back to the correct store node without consulting any
shared mutable state.

`client_size` lets the sub-channel mark the upload complete on the
final `"chunk"` frame (when bytes_written reaches client_size) — there
is no separate `"close"` event. `chunk_timeout` arms the per-entry
watchdog without an extra round-trip to the page server.

`accept` enforcement is **preflight-only**: the page server validates
`client_name`/`client_type` against `accept` before signing a token,
and a live token is itself proof that those checks passed. The chunk
sub-channel does not re-validate, since filename and MIME are not
carried on chunk frames and re-checking would not add a security
boundary (it would let a buggy client lie a second time about a value
that has already been authorized). To change an entry's accept set,
the application cancels the entry and re-issues `allow_upload`.

- `max_age: 600` seconds (10 minutes)
- HMAC signed with the endpoint secret
- The sub-channel verifies token on `join/3`; if the embedded pid is dead
  the join is rejected
- `Musubi.Transport.UploadChannel` is stateless: every authority signal
  (max size, store target, accept list) comes from the verified token
- Tokens never appear in render output or `upload_ops`; they are issued
  exactly once during the `allow_upload` preflight reply

## External mode

When `upload_external/3` is implemented for an upload name, the preflight
reply returns `{type: "external", uploader, meta}` instead of a token.
Clients dispatch the upload to a registered uploader (named by the
`uploader` string in `meta`) which performs the direct HTTP PUT to cloud
storage. The uploader reports progress through `upload_progress` events
on the main channel, and the server emits `{op: complete}` when progress
reaches 100. On failure the client pushes `upload_error` and the server
emits `{op: error, code: "external_failed"}`.

```elixir
@impl Musubi.Store
def upload_external(:avatar, entry, socket) do
  {url, headers} = MyApp.S3.presign(entry)
  {:ok, %{uploader: "S3", url: url, headers: headers}, socket}
end
```

The returned meta is opaque to Musubi — the application + its chosen
client uploader define its shape together. `@musubi/client` ships only
the `ExternalUploader` *contract* (see Client API below); apps bring
their own implementation. See `guides/uploads.md` for an example PUT-
via-XHR uploader.

## Client API

### TypeScript types

```ts
export type UploadStatus =
  | "idle" | "selecting" | "uploading" | "success" | "error" | "cancelled"

export type EntryStatus =
  | "pending" | "uploading" | "success" | "error" | "cancelled"

export interface UploadError {
  code:
    | "too_large" | "too_many_files" | "not_accepted"
    | "chunk_timeout" | "chunk_too_large" | "external_failed"
    | "preflight_rejected" | "internal"
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

export interface UploadHandle {
  readonly config: {
    accept: string[] | "any"
    maxEntries: number
    maxFileSize: number
    chunkSize: number
  }
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
```

### Store proxy exposure

Generated TS types merge state fields and declared uploads at the same
level on the `StoreProxy`. Compile-time validation forbids
upload/state name collisions, so the merged shape is unambiguous: a
field declared in `state do` keeps its original type; an upload name
resolves to `UploadHandle` at the marker injection site.

The proxy returned by `mountStore` is a `StoreProxy<M, R>` —
documented under `@musubi/client`. Its public surface is:

```ts
interface StoreProxy<M, R> {
  readonly __musubi_store_id__: readonly string[]
  dispatchCommand<K>(name: K, payload: ...): Promise<...>
  subscribe(listener: () => void): () => void
  snapshot(): StoreSnapshot<M, R> | undefined
  // plus every declared state field as a typed property,
  // plus every declared upload as `UploadHandle`.
}
```

`store.<name>` is a stable reactive `UploadHandle` for the connection
lifetime: the reference does not change on state updates; internal
state mutates in place as `upload_ops` arrive.

### Usage

```ts
import { Socket } from "phoenix"
import { connect } from "@musubi/client"
import { S3Uploader } from "./uploaders/s3"

const socket = new Socket("/socket")
const connection = await connect<Musubi.Stores>(socket, {
  uploaders: { S3: S3Uploader } // required for external mode
})

const { store, unmount } = await connection.mountStore({
  module: "MyApp.Stores.AvatarStore",
  id: "u:42"
})

await store.avatar.select(files)
await store.avatar.start()

const unsubscribe = store.subscribe(() => {
  const s = store.snapshot()
  if (s) console.log(s.avatar.status, s.avatar.progress)
})

await store.avatar.cancel(entryRef)
await store.avatar.reset()
await store.dispatchCommand("submit", {})

unsubscribe()
await unmount()
```

## React API

`@musubi/react` ships no dedicated `useMusubiUpload` hook. The reactive
proxy returned by `useMusubiRoot` already exposes upload handles as
typed properties:

```tsx
import { useMusubiRoot } from "@musubi/react"

function AvatarUploader() {
  const { store } = useMusubiRoot({
    module: "MyApp.Stores.AvatarStore",
    id: "u:42"
  })
  if (!store) return null

  const { avatar } = store

  const onChange = async (e: React.ChangeEvent<HTMLInputElement>) => {
    if (!e.target.files) return
    await avatar.select(e.target.files)
    await avatar.start()
  }

  return (
    <div>
      <input
        type="file"
        accept={Array.isArray(avatar.config.accept) ? avatar.config.accept.join(",") : undefined}
        onChange={onChange}
      />
      {avatar.isUploading && <progress value={avatar.progress} max={100} />}
      {avatar.errors.map((err) => <p key={err.code}>{err.message}</p>)}
      {avatar.entries.map((e) => (
        <div key={e.ref}>
          {e.clientName} — {e.progress}%
          {e.errors.map((err) => <small key={err.code}>{err.message}</small>)}
          <button onClick={() => avatar.cancel(e.ref)}>×</button>
        </div>
      ))}
      <button
        disabled={!avatar.isSuccess}
        onClick={() => store.dispatchCommand("submit", {})}
      >
        Save
      </button>
      {store.avatar_url && <img src={store.avatar_url} />}
    </div>
  )
}
```

For external mode, pass `uploaders` through the `MusubiProvider`.
`@musubi/client` ships only the `ExternalUploader` contract — the
application provides the implementation (see `guides/uploads.md` for
an XHR-based example):

```tsx
import { MusubiProvider } from "@musubi/react"
import { S3Uploader } from "./uploaders/s3"

<MusubiProvider socket={socket} uploaders={{ S3: S3Uploader }}>
  <AvatarUploader />
</MusubiProvider>
```

Drag-drop, file pickers, and any UI affordance are application
responsibility — Musubi ships no headless components.

## End-to-end flow (channel mode)

1. `connection.mountStore({module, id})` starts a page server.
2. Initial envelope carries the marker tree and `upload_ops` `config` ops.
3. `store.avatar.select(files)` sends `allow_upload`.
4. Server validates, signs tokens per entry, replies with the preflight
   result, and emits `{op:add}` for each entry.
5. `store.avatar.start()` joins `musubi_upload:<entry_ref>` per entry.
6. Client reads files into ArrayBuffer slices and pushes `chunk` events.
7. UploadChannel verifies token, writes via `Plug.Upload.random_file/1`,
   notifies the store pid, which enqueues `{op:progress}`.
8. Drain emits envelopes; client mutates handle state in place.
9. Final chunk → `{op:complete}`.
10. `store.dispatchCommand("submit", {})` triggers `consume_uploaded_entries/3`.
11. Application moves the temp file, replies with result, and the server
    emits `{op:reset}` for the upload plus a normal JSON Patch for the
    state field that received the URL.
12. UploadChannel exits, temp file is cleaned.

External mode replaces steps 5–7 with a direct PUT to the presigned URL
via the registered uploader, with `upload_progress` events on the main
channel driving the `{op:progress}` emission.

## Constraints (out of scope for v1)

- `:writer` custom write target — deferred to v2.
- `:auto_upload` sugar — clients call `select` then `start` explicitly.
- Headless drag-drop component — application owns UI.
- Directory uploads — `client_relative_path` is not exposed.
- Image preview helpers — application provides UI.
- Resumable uploads — reconnect discards in-flight progress (BDR-0003).
- Form integration — Musubi has no HTML rendering.
- Uploads inside list fields, stream blocks, nested `field` blocks — the
  declaration is compile-time and singleton. For per-item upload
  capability, use a child store per item with `upload :name` declared at
  the child store top level. `store_id` on each op naturally
  distinguishes the parent of the upload.
