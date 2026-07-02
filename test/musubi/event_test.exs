defmodule Musubi.EventTest do
  use ExUnit.Case, async: true

  alias Musubi.Event
  alias Musubi.Socket

  test "queue then flush returns events FIFO with stringified name and wire payload" do
    {events, _socket} =
      %Socket{}
      |> Event.push_event(:toast, %{msg: "first", level: :info})
      |> Event.push_event("ping", %{n: 2})
      |> Event.flush_pending()

    assert events == [
             %{name: "toast", payload: %{"msg" => "first", "level" => "info"}},
             %{name: "ping", payload: %{"n" => 2}}
           ]
  end

  # Locks the single-reverse invariant: push_event prepends (LIFO), flush
  # reverses exactly once back to enqueue order. A dropped or doubled reverse,
  # or any reordering, breaks this. Uses an odd count so an accidental sort or
  # partial reverse cannot coincidentally pass.
  test "flush preserves enqueue order across many events (reversed exactly once)" do
    names = ~w(a b c d e)

    queued =
      Enum.reduce(names, %Socket{}, fn name, socket ->
        Event.push_event(socket, name, %{seq: name})
      end)

    {events, _socket} = Event.flush_pending(queued)

    assert Enum.map(events, & &1.name) == names
  end

  test "flush clears the accumulator" do
    {_events, socket} =
      %Socket{}
      |> Event.push_event("a", %{})
      |> Event.flush_pending()

    assert {[], _socket} = Event.flush_pending(socket)
  end

  test "no events queued flushes empty" do
    assert {[], _socket} = Event.flush_pending(%Socket{})
  end
end
