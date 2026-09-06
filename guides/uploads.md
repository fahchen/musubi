# Uploads

This guide walks an avatar upload from a Phoenix `Musubi.Store` to a
React UI in two flavors: the default channel mode, and a direct-to-cloud
external mode. For the full reference (DSL options, wire protocol, BDRs)
see [Uploads reference](docs/uploads.md).

## 1. Declare the upload on the store

Uploads are top-level on a store — not inside `state do`, not inside
`field` or `stream` blocks. The framework auto-injects the wire marker
so render code never composes upload state by hand.

```elixir
defmodule MyAppWeb.Stores.AvatarStore do
  use Musubi.Store, root: true

  state do
    field :avatar_url, String.t() | nil
  end

  upload :avatar,
    accept: ~w(.jpg .jpeg .png),
    max_entries: 1,
    max_file_size: 5_000_000

  command :submit

  @impl Musubi.Store
  def mount(_params, socket), do: {:ok, assign(socket, :avatar_url, nil)}

  @impl Musubi.Store
  def render(socket), do: %{avatar_url: socket.assigns.avatar_url}

  @impl Musubi.Store
  def handle_command(:submit, _payload, socket) do
    {socket, [url]} =
      consume_uploaded_entries(socket, :avatar, fn %{path: path}, entry ->
        # `path` is a `Plug.Upload.random_file!/1` temp file owned by the
        # page server. Move it into permanent storage and return the URL
        # the client will see in `store.avatar_url` after the next render.
        dest = Path.join(["priv", "static", "uploads", entry.client_name])
        File.cp!(path, dest)
        {:ok, "/uploads/#{entry.client_name}"}
      end)

    {:reply, %{ok: true}, assign(socket, :avatar_url, url)}
  end
end
```

`consume_uploaded_entries/3` removes the entry from the page-server
index on `{:ok, val}` and deletes the temp file. Return
`{:postpone, val}` to leave it in place for a later attempt — the
file survives across consumes until the entry is consumed with `:ok`
or cancelled.

## 2. Expose the store through a Musubi socket

`use Musubi.Socket` registers both the connection channel
(`musubi:*`) and the per-entry upload sub-channel
(`musubi_upload:*`) — no extra wiring required.

```elixir
defmodule MyAppWeb.MusubiSocket do
  use Musubi.Socket,
    roots: [MyAppWeb.Stores.AvatarStore]
end
```

Mount it on the Phoenix endpoint like any other transport:

```elixir
socket "/socket", MyAppWeb.MusubiSocket,
  websocket: true,
  longpoll: false
```

## 3. Wire the client

`store.<upload_name>` resolves to a stable reactive `UploadHandle` for
the connection lifetime. The reference does not change on state
updates; internal state mutates in place as `upload_ops` arrive.

```tsx
import { useMusubiRoot } from "@musubi/react"

export function AvatarUploader() {
  const { store } = useMusubiRoot({
    module: "MyAppWeb.Stores.AvatarStore",
    id: "u:42"
  })

  if (!store) return <p>Loading…</p>

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
        accept={
          Array.isArray(avatar.config.accept)
            ? avatar.config.accept.join(",")
            : undefined
        }
        onChange={onChange}
      />

      {avatar.isUploading && <progress value={avatar.progress} max={100} />}

      {avatar.errors.map((err) => (
        <p key={err.code}>{err.message}</p>
      ))}

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

      {store.avatar_url && <img src={store.avatar_url} alt="avatar" />}
    </div>
  )
}
```

Drag-drop, file pickers, and any other UI affordance are application
responsibility — Musubi ships no headless components.

## 4. Switch to direct-to-cloud (external mode)

For large media, channel-mode chunks every byte through the BEAM.
External mode replaces the chunk pipeline with a single PUT from the
browser to a presigned cloud URL. The wire protocol is identical to
channel mode; only the preflight reply changes shape and the chunk
delivery path moves off the Phoenix channel.

Implement `upload_external/3` on the store. Musubi treats `meta` as
opaque — pick whatever shape your client uploader needs, plus any
fields the `:submit` handler reads at consume time.

```elixir
@impl Musubi.Store
def upload_external(:avatar, entry, socket) do
  public_url = MyApp.S3.public_url(entry)
  {presigned_url, headers} = MyApp.S3.presign_put(entry)

  meta = %{
    uploader: "S3",
    url: presigned_url,
    headers: headers,
    public_url: public_url
  }

  {:ok, meta, socket}
end
```

Consuming the upload in a command handler looks identical to channel
mode — except the meta carries an `:external` key instead of `:path`:

```elixir
@impl Musubi.Store
def handle_command(:submit, _payload, socket) do
  {socket, [url]} =
    consume_uploaded_entries(socket, :avatar, fn meta, entry ->
      %{external: %{public_url: public_url}} = meta

      # Optionally HEAD the object to confirm the client actually PUT it.
      :ok = MyApp.S3.assert_present!(entry.client_name)
      {:ok, public_url}
    end)

  {:reply, %{ok: true}, assign(socket, :avatar_url, url)}
end
```

### Bring your own uploader

`@musubi/client` ships the `ExternalUploader` *contract*, not an
implementation. Apps own the upload mechanism: the library has no
opinion on `fetch` vs. `XMLHttpRequest`, error semantics, or
cloud-specific quirks.

The whole contract:

```ts
type ExternalUploader = (args: {
  entry: UploadEntry
  file: File
  meta: unknown   // what your `upload_external/3` returned
  onProgress: (pct: number) => void
  signal: AbortSignal
}) => Promise<void>
```

Resolve → success. Reject → Musubi pushes `upload_error` so the
server emits `{op: error, code: "external_failed"}`.

**XHR (granular progress, large media):**

```ts
const S3Uploader: ExternalUploader = ({ file, meta, onProgress, signal }) => {
  const { url, headers } = meta as { url: string; headers?: Record<string, string> }
  return new Promise<void>((resolve, reject) => {
    const xhr = new XMLHttpRequest()
    xhr.open("PUT", url)
    for (const [k, v] of Object.entries(headers ?? {})) xhr.setRequestHeader(k, v)
    xhr.upload.onprogress = (e) =>
      e.lengthComputable && onProgress(Math.round((e.loaded / e.total) * 100))
    xhr.onload = () => (xhr.status < 300 ? resolve() : reject(new Error(`PUT ${xhr.status}`)))
    xhr.onerror = () => reject(new Error("network error"))
    signal.addEventListener("abort", () => xhr.abort())
    xhr.send(file)
  })
}
```

**fetch (no granular progress):**

```ts
const FetchUploader: ExternalUploader = async ({ file, meta, onProgress, signal }) => {
  const { url, headers } = meta as { url: string; headers?: Record<string, string> }
  const res = await fetch(url, { method: "PUT", body: file, headers, signal })
  if (!res.ok) throw new Error(`PUT ${res.status}`)
  onProgress(100)
}
```

`fetch` cannot observe request-body progress, so it jumps 0 → 100.
Fine for small files; for large media (the reason external mode
exists) prefer XHR.

Register your uploader on the client through `MusubiProvider`:

```tsx
import { Socket } from "phoenix"
import { MusubiProvider } from "@musubi/react"
import { S3Uploader } from "./uploaders/s3"

const socket = new Socket("/socket")

export function App() {
  return (
    <MusubiProvider socket={socket} uploaders={{ S3: S3Uploader }}>
      <AvatarUploader />
    </MusubiProvider>
  )
}
```

When the uploader's `Promise` rejects, Musubi pushes an
`upload_error` event back to the page server and the server emits
`{op: error, code: "external_failed"}` so the UI can surface the
failure via `handle.errors` / `entry.errors`.

## Per-item dynamic uploads (child stores)

`upload :name, opts` is a compile-time singleton bound to a fixed
path. For per-row attachments (one upload per cart line, per gallery
photo, per evidence record) use a child store per item; each child
declares its own `upload` at the top level and the `store_id` on each
upload op routes ops back to the correct child:

```elixir
defmodule CartLineStore do
  use Musubi.Store
  attr :line_id, String.t(), required: true

  state do
    field :line_id, String.t()
  end

  upload :attachment, accept: ~w(.pdf), max_entries: 1

  def init(socket), do: {:ok, assign(socket, :line_id, socket.assigns.line_id)}
  def render(socket), do: %{line_id: socket.assigns.line_id}
end

defmodule CartStore do
  use Musubi.Store, root: true

  state do
    field :lines, list(CartLineStore.state())
  end

  def render(socket) do
    %{
      lines:
        Enum.map(socket.assigns.lines, fn line ->
          child(CartLineStore, id: "line-#{line.id}", line_id: line.id)
        end)
    }
  end
end
```

On the client, `store.lines[i].attachment` is an `UploadHandle`
specific to that line; the server keeps `store_id: ["lines",
"line-N"]` on every emitted op.
