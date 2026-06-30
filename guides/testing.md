# Testing Stores

`Musubi.Testing` is the public test entry point for stores authored on top
of Musubi. It wraps `Musubi.Page.Server.start_link/1` with test-friendly
defaults and exposes the primary assertion surface, modelled on
`Phoenix.LiveViewTest`.

## Doctrine: assert through `render/2`, not `assigns/2`

Test the rendered wire-shape map — what the client would observe — not
internal `socket.assigns`. Renaming or splitting a field then trips the
test; assertions coupled to internal storage do not.

`assigns/2` is documented as an escape hatch for state that is not
surfaced through `render/1`. Reach for it sparingly.

## Minimum example

Suppose a store tracks a 1v1 fight:

```elixir
defmodule MyApp.Stores.RoomStore do
  use Musubi.Store, root: true

  state do
    field :hp, %{p1: integer(), p2: integer()}
    field :winner, :p1 | :p2 | :draw | nil
  end

  command :ko do
    payload do
      field :target, String.t()
    end

    reply do
      field :ok, boolean()
    end
  end

  @impl Musubi.Store
  def mount(_params, socket) do
    socket =
      socket
      |> Musubi.Socket.assign(:hp, %{p1: 100, p2: 100})
      |> Musubi.Socket.assign(:winner, nil)

    {:ok, socket}
  end

  @impl Musubi.Store
  def render(socket) do
    %{hp: socket.assigns.hp, winner: socket.assigns.winner}
  end

  @impl Musubi.Store
  def handle_command(:ko, %{"target" => "p2"}, socket) do
    socket =
      socket
      |> Musubi.Socket.assign(:hp, %{p1: 100, p2: 0})
      |> Musubi.Socket.assign(:winner, :p1)

    {:reply, %{"ok" => true}, socket}
  end
end
```

A focused test:

```elixir
defmodule MyApp.Stores.RoomStoreTest do
  use ExUnit.Case, async: true

  test "ko on p2 flips winner to p1" do
    page = Musubi.Testing.mount(MyApp.Stores.RoomStore, %{"room_code" => "AB12"})

    {:ok, %{"ok" => true}} =
      Musubi.Testing.dispatch_command(page, :ko, %{"target" => "p2"})

    assert Musubi.Testing.render(page) == %{
             hp: %{p1: 100, p2: 0},
             winner: :p1
           }
  end
end
```

## Wire shape: atoms stay atoms in `render/2`

`render/2` returns native Elixir terms — `:p1` stays an atom, the
`%{p1: ...}` map keeps atom keys. The JSON-string conversion happens
downstream on the way to the client (see "Wire Encoding: Atoms Become
Strings" in `Getting Started`). Tests are easier to read this way.

If you need to assert against the actual wire shape, pipe through
`Musubi.Wire.to_wire/1`:

```elixir
wire = page |> Musubi.Testing.render() |> Musubi.Wire.to_wire()
assert wire == %{"hp" => %{"p1" => 100, "p2" => 0}, "winner" => "p1"}
```

## Addressing child stores

`render/2`, `dispatch_command/4`, and `assigns/2` all accept an optional
`store_id` — the path from the root to the addressed node, matching the
shape of `Musubi.Socket.store_id/1`. The default `[]` addresses the root.

| `store_id`           | Addresses                              |
| :------------------- | :------------------------------------- |
| `[]`                 | root                                   |
| `["filters"]`        | root → child mounted with `id: "filters"` |
| `["cart", "i-42"]`   | root → cart child → its child `"i-42"` |

Each segment matches the `:id` passed to `Musubi.Child.child(Module, id: "...")`
inside the parent's `render/1` output. Segments are strings.

### Worked example

Parent + child store:

```elixir
defmodule MyApp.Stores.FiltersStore do
  use Musubi.Store

  state do
    field :query, String.t()
  end

  command :change_query do
    payload do
      field :query, String.t()
    end

    reply do
      field :ok, boolean()
    end
  end

  @impl Musubi.Store
  def mount(socket), do: {:ok, Musubi.Socket.assign(socket, :query, "")}

  @impl Musubi.Store
  def render(socket), do: %{query: socket.assigns.query}

  @impl Musubi.Store
  def handle_command(:change_query, %{"query" => q}, socket) do
    {:reply, %{"ok" => true}, Musubi.Socket.assign(socket, :query, q)}
  end
end

defmodule MyApp.Stores.RootStore do
  use Musubi.Store, root: true

  state do
    field :filters, MyApp.Stores.FiltersStore.t()
  end

  @impl Musubi.Store
  def mount(_params, socket), do: {:ok, socket}

  @impl Musubi.Store
  def render(_socket) do
    %{filters: Musubi.Child.child(MyApp.Stores.FiltersStore, id: "filters")}
  end

  @impl Musubi.Store
  def handle_command(_name, _payload, socket), do: {:noreply, socket}
end
```

Test that dispatches to the child and asserts on its rendered output:

```elixir
test "filter query updates through the child store" do
  page = Musubi.Testing.mount(MyApp.Stores.RootStore)

  {:ok, %{"ok" => true}} =
    Musubi.Testing.dispatch_command(
      page,
      :change_query,
      %{"query" => "shirt"},
      ["filters"]
    )

  assert Musubi.Testing.render(page, ["filters"]) == %{query: "shirt"}
end
```

### Lifecycle notes

- The child is mounted lazily by the resolver during the first render
  cycle, triggered automatically by `Musubi.Testing.mount/3`. By the
  time `dispatch_command/4` runs, the child is in the store table.
- Calling `dispatch_command/4` with a `store_id` for a child that was
  never rendered raises — the lookup fails fast.
- `Musubi.Testing.render(page)` at the root returns the raw `render/1`
  output, including the `%Musubi.Child{...}` placeholder. The
  placeholder is substituted with the child's rendered output later in
  the wire pipeline before the patch envelope ships to the client. Use
  `render(page, ["filters"])` to assert on the child's own output, or
  pipe through `Musubi.Wire.to_wire/1` to see the resolved wire shape.

## Push patches: handled automatically

By default `mount/3` sets the test process as the transport pid, so
push-patch envelopes arrive in the test mailbox. Most tests do not need
to consume them — `render/2` runs the store's `render/1` against the
current socket and is sufficient for state assertions.

If a test needs to observe patch sequencing (e.g. verifying a stream
operation was emitted), assert on the mailbox:

```elixir
assert_receive {:patch, %Musubi.Page.PatchEnvelope{ops: ops}}
```

## Push events

For transient push events (`push_event/3`, BDR-0032), use
`assert_push_event/3` / `refute_push_event/2` instead of unpacking the
envelope by hand:

```elixir
page = Musubi.Testing.mount(InboxStore)
Musubi.Testing.dispatch_command(page, :save, %{})

# Matches by name; payload is compared in wire shape (atoms → strings).
Musubi.Testing.assert_push_event(:toast, %{msg: "Saved", level: :info})

# And the negative case:
Musubi.Testing.dispatch_command(page, :noop, %{})
Musubi.Testing.refute_push_event(:toast)
```

Events ride the patch envelope, so these helpers **consume** the
`{:patch, _}` messages they scan past. Assert any state patches you care
about *before* asserting events, or assert the event first.

## Teardown

`mount/3` uses `ExUnit.Callbacks.start_supervised!/1`, so the page
server is torn down with the test process. No manual cleanup needed.
