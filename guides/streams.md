# Streams

This guide walks a live message feed from a Phoenix `Musubi.Store` to a
React UI. Streams are for collections whose item values should not linger
in server memory after delivery: the server keeps only the stream config and
pending delta ops, the client owns the materialized list. For the full
reference (all runtime ops, options, wire shape) see [Streams reference](docs/streams.md).

## 1. Declare the stream on the store

Streams are declared inside `state do`. Give each item a stable `item_key` so
inserts/updates address the right row; `stream(:name)` in `render/1` places the
stream at the path where it was declared.

```elixir
defmodule MyAppWeb.Stores.FeedStore do
  use Musubi.Store, root: true

  state do
    field :title, String.t()

    stream :messages, item_key: &"msg-#{&1.id}", limit: -100 do
      field :id, String.t()
      field :body, String.t()
    end
  end

  command :post do
    payload do
      field :body, String.t()
    end
  end

  @impl Musubi.Store
  def mount(_params, socket) do
    {:ok,
     socket
     |> assign(:title, "Feed")
     |> stream(:messages, load_recent())}
  end

  @impl Musubi.Store
  def render(socket) do
    %{title: socket.assigns.title, messages: stream(:messages)}
  end

  @impl Musubi.Store
  def handle_command(:post, %{"body" => body}, socket) do
    message = %{id: Ecto.UUID.generate(), body: body}
    {:reply, %{ok: true}, stream_insert(socket, :messages, message)}
  end

  defp load_recent, do: [%{id: "1", body: "Welcome"}]
end
```

`stream(socket, :messages, items)` seeds the list; `stream_insert/3` appends one
item. Neither keeps item values on the server past the render cycle — after the
envelope ships, the runtime drops them and the client holds the list.

## 2. Expose the store through a Musubi socket

Same wiring as any Musubi store — no stream-specific setup:

```elixir
defmodule MyAppWeb.MusubiSocket do
  use Musubi.Socket, roots: [MyAppWeb.Stores.FeedStore]
end
```

## 3. Render the list on the client

The client resolves the stream marker into a plain array. Read it reactively
with `useMusubiSnapshot`; push new items with `useMusubiCommand`.

```tsx
import { useMusubiRoot, useMusubiSnapshot, useMusubiCommand } from "./musubi"

export function Feed() {
  const store = useMusubiRoot({ module: "MyAppWeb.Stores.FeedStore", id: "feed" })
  if (!store) return <p>Loading…</p>

  return <FeedList store={store} />
}

function FeedList({ store }: { store: /* StoreProxy */ any }) {
  const messages = useMusubiSnapshot(store, (s) => s.messages)
  const { dispatch: post, isPending } = useMusubiCommand(store, "post")

  return (
    <>
      <ul>
        {messages.map((m) => (
          <li key={m.id}>{m.body}</li>
        ))}
      </ul>
      <button disabled={isPending} onClick={() => post({ body: "Hi" })}>
        Post
      </button>
    </>
  )
}
```

`store.messages` is `Array<{ id: string; body: string }>` — the generated types
infer the item shape from the declaration. Never read or build the
`{ __musubi_stream__: ... }` marker directly.

## 4. Refresh the whole list

There is no separate reload API. When you already have fresh items, reset in one
envelope:

```elixir
def handle_command(:refresh, _payload, socket) do
  {:reply, %{ok: true}, stream(socket, :messages, load_recent(), reset: true)}
end
```

When the refresh needs a background fetch and the UI should show a loading
state, declare the stream `stream_async` and drive it with
`stream_async(socket, :messages, fn -> load_recent() end, reset: true)`; the
client then reads `store.messages` as an `AsyncResult<Item[]>`. See the
[Streams reference](docs/streams.md) for the async placement details.
