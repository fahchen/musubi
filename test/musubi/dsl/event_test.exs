defmodule Musubi.DSL.EventTest do
  use ExUnit.Case, async: true

  defmodule RootWithEvents do
    @moduledoc false
    use Musubi.Store, root: true

    state do
      field :ok, boolean()
    end

    event(:ping)

    event :toast do
      field :msg, String.t()
      field :level, atom(), doc: "severity"
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, socket}
    @impl Musubi.Store
    def render(_socket), do: %{ok: true}
    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  test "events reflection lists declared events in order" do
    assert [%{name: :ping}, %{name: :toast}] = RootWithEvents.__musubi__(:events)
  end

  test "event without a block has empty payload_fields" do
    assert {:ok, %{payload_fields: []}} = RootWithEvents.__musubi__(:event, :ping)
  end

  test "event with a block captures payload_fields with doc opts" do
    assert {:ok, %{payload_fields: payload_fields}} = RootWithEvents.__musubi__(:event, :toast)

    assert [
             %{name: :msg, opts: []},
             %{name: :level, opts: [doc: "severity"]}
           ] = payload_fields
  end

  test "unknown event returns :error" do
    assert :error = RootWithEvents.__musubi__(:event, :nope)
  end

  describe "validate_events!/2 (dev-correctness)" do
    test "passes a declared event with a valid wire payload" do
      events = [%{name: "toast", payload: %{"msg" => "hi", "level" => "info"}}]
      assert ^events = Musubi.Event.validate_events!(events, RootWithEvents)
    end

    test "passes a no-field event regardless of payload" do
      events = [%{name: "ping", payload: %{"extra" => 1}}]
      assert ^events = Musubi.Event.validate_events!(events, RootWithEvents)
    end

    test "skips an undeclared event name" do
      events = [%{name: "not_declared", payload: %{"anything" => true}}]
      assert ^events = Musubi.Event.validate_events!(events, RootWithEvents)
    end

    test "raises on a type mismatch" do
      events = [%{name: "toast", payload: %{"msg" => 123, "level" => "info"}}]

      assert_raise ArgumentError, ~r/push event validation failed.*msg: expected/, fn ->
        Musubi.Event.validate_events!(events, RootWithEvents)
      end
    end

    test "raises on a missing required field" do
      events = [%{name: "toast", payload: %{"level" => "info"}}]

      assert_raise ArgumentError, ~r/msg: missing required field/, fn ->
        Musubi.Event.validate_events!(events, RootWithEvents)
      end
    end
  end

  test "declaring an event in a non-root store raises at compile time" do
    assert_raise CompileError,
                 ~r/event :toast not allowed: events may only be declared in a root store/,
                 fn ->
                   Code.compile_string("""
                   defmodule Musubi.DSL.EventTest.NonRootEvents do
                     use Musubi.Store

                     state do
                       field :ok, boolean()
                     end

                     event :toast

                     @impl Musubi.Store
                     def init(socket), do: {:ok, socket}
                     @impl Musubi.Store
                     def render(_socket), do: %{ok: true}
                   end
                   """)
                 end
  end
end
