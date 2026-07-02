defmodule Musubi.Hooks.ValidateEventsTest do
  use ExUnit.Case, async: true

  alias Musubi.Hooks.ValidateEvents
  alias Musubi.Page.Frame
  alias Musubi.Socket

  defmodule StoreWithEvents do
    @moduledoc false
    use Musubi.Store

    state do
      field :ok, boolean()
    end

    event :toast do
      field :msg, String.t()
    end

    @impl Musubi.Store
    def render(_socket), do: %{ok: true}
    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defp socket, do: %Socket{module: StoreWithEvents, private: %{}, assigns: %{}}

  test "validates frame.events against the socket's own module, returning the frame" do
    frame = %Frame{render: %{}, events: [%{name: "toast", payload: %{"msg" => "hi"}}]}
    s = socket()

    assert {:cont, ^frame, ^s} = ValidateEvents.after_serialize(frame, s)
  end

  test "raises on a declared-event payload mismatch" do
    frame = %Frame{render: %{}, events: [%{name: "toast", payload: %{"msg" => 123}}]}

    assert_raise ArgumentError, ~r/push event validation failed.*msg: expected/, fn ->
      ValidateEvents.after_serialize(frame, socket())
    end
  end

  test "skips undeclared event names" do
    frame = %Frame{render: %{}, events: [%{name: "unknown", payload: %{"x" => 1}}]}
    s = socket()

    assert {:cont, ^frame, ^s} = ValidateEvents.after_serialize(frame, s)
  end
end
