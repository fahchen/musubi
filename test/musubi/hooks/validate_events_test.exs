defmodule Musubi.Hooks.ValidateEventsTest do
  use ExUnit.Case, async: true

  alias Musubi.Hooks.ValidateEvents
  alias Musubi.Socket

  defmodule RootWithEvents do
    @moduledoc false
    use Musubi.Store, root: true

    state do
      field :ok, boolean()
    end

    event :toast do
      field :msg, String.t()
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, socket}
    @impl Musubi.Store
    def render(_socket), do: %{ok: true}
    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defp root_socket, do: %Socket{module: RootWithEvents, private: %{}, assigns: %{}}

  test "returns {:cont, events, socket} unchanged for a valid payload" do
    socket = root_socket()
    events = [%{name: "toast", payload: %{"msg" => "hi"}}]

    assert {:cont, ^events, ^socket} = ValidateEvents.before_events(events, socket)
  end

  test "raises on a declared-event payload mismatch" do
    socket = root_socket()
    events = [%{name: "toast", payload: %{"msg" => 123}}]

    assert_raise ArgumentError, ~r/push event validation failed.*msg: expected/, fn ->
      ValidateEvents.before_events(events, socket)
    end
  end

  test "skips undeclared event names" do
    socket = root_socket()
    events = [%{name: "unknown", payload: %{"whatever" => true}}]

    assert {:cont, ^events, ^socket} = ValidateEvents.before_events(events, socket)
  end
end
