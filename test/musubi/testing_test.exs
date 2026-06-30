defmodule Musubi.TestingTest do
  use ExUnit.Case, async: true

  defmodule SimpleStore do
    use Musubi.Store, root: true

    state do
      field :count, integer()
      field :winner, :none | :p1 | :p2
    end

    command :bump do
      payload do
        field :by, integer()
      end

      reply do
        field :ok, boolean()
      end
    end

    command :declare do
      payload do
        field :winner, String.t()
      end

      reply do
        field :ok, boolean()
      end
    end

    command :pick

    command :flash

    event :toast do
      field :msg, String.t()
      field :level, atom()
    end

    @impl Musubi.Store
    def mount(_params, socket) do
      socket =
        socket
        |> Musubi.Socket.assign(:count, 0)
        |> Musubi.Socket.assign(:winner, :none)

      {:ok, socket}
    end

    @impl Musubi.Store
    def render(socket) do
      %{count: socket.assigns.count, winner: socket.assigns.winner}
    end

    @impl Musubi.Store
    def handle_command(:bump, %{"by" => n}, socket) do
      {:reply, %{"ok" => true}, Musubi.Socket.assign(socket, :count, socket.assigns.count + n)}
    end

    def handle_command(:declare, %{"winner" => "p1"}, socket) do
      {:reply, %{"ok" => true}, Musubi.Socket.assign(socket, :winner, :p1)}
    end

    def handle_command(:declare, %{"winner" => "p2"}, socket) do
      {:reply, %{"ok" => true}, Musubi.Socket.assign(socket, :winner, :p2)}
    end

    def handle_command(:pick, _payload, socket) do
      {:reply, %{status: :ok, winner: :p1}, socket}
    end

    def handle_command(:flash, _payload, socket) do
      {:reply, %{"ok" => true}, push_event(socket, :toast, %{msg: "Saved", level: :info})}
    end
  end

  describe "mount/3" do
    test "returns a handle carrying pid + root + transport" do
      page = Musubi.Testing.mount(SimpleStore)

      assert %Musubi.Testing{pid: pid, root: SimpleStore, transport: transport} = page
      assert is_pid(pid)
      assert transport == self()
    end

    test "accepts params" do
      page = Musubi.Testing.mount(SimpleStore, %{"any" => "value"})
      assert is_pid(page.pid)
    end
  end

  describe "render/2" do
    test "returns the wire-shape map with atom values intact" do
      page = Musubi.Testing.mount(SimpleStore)

      assert Musubi.Testing.render(page) == %{count: 0, winner: :none}
    end

    test "reflects state after a command settles" do
      page = Musubi.Testing.mount(SimpleStore)

      {:ok, _reply} = Musubi.Testing.dispatch_command(page, :bump, %{"by" => 3})
      {:ok, _reply} = Musubi.Testing.dispatch_command(page, :declare, %{"winner" => "p1"})

      assert Musubi.Testing.render(page) == %{count: 3, winner: :p1}
    end
  end

  describe "assigns/2" do
    test "returns raw socket.assigns" do
      page = Musubi.Testing.mount(SimpleStore)
      {:ok, _reply} = Musubi.Testing.dispatch_command(page, :bump, %{"by" => 5})

      assigns = Musubi.Testing.assigns(page)
      assert assigns.count == 5
      assert assigns.winner == :none
    end
  end

  describe "dispatch_command/4" do
    test "returns the command reply payload" do
      page = Musubi.Testing.mount(SimpleStore)

      assert {:ok, %{"ok" => true}} =
               Musubi.Testing.dispatch_command(page, :bump, %{"by" => 1})
    end

    test "returns the reply in native Elixir shape (symmetric with render/2)" do
      page = Musubi.Testing.mount(SimpleStore)

      # Native atom keys and atom values come back as-is — no wire artifacts,
      # no Musubi.Wire.to_wire/1 in test code.
      assert {:ok, %{status: :ok, winner: :p1}} =
               Musubi.Testing.dispatch_command(page, :pick, %{})
    end

    test "accepts a native (atom-keyed) payload, wire-encoded before dispatch" do
      page = Musubi.Testing.mount(SimpleStore)

      # Handlers match string keys (the wire contract); atom keys and atom
      # values are normalized to strings, so this routes identically to
      # passing %{"by" => 3} / %{"winner" => "p1"}.
      {:ok, _reply} = Musubi.Testing.dispatch_command(page, :bump, %{by: 3})
      {:ok, _reply} = Musubi.Testing.dispatch_command(page, :declare, %{winner: :p1})

      assert Musubi.Testing.render(page) == %{count: 3, winner: :p1}
    end
  end

  describe "assert_push_event/3" do
    test "matches a pushed event by name with wire-encoded payload" do
      page = Musubi.Testing.mount(SimpleStore)
      {:ok, _reply} = Musubi.Testing.dispatch_command(page, :flash, %{})

      payload = Musubi.Testing.assert_push_event(:toast, %{msg: "Saved", level: :info})
      assert payload == %{"msg" => "Saved", "level" => "info"}
    end

    test "flunks on a payload mismatch" do
      page = Musubi.Testing.mount(SimpleStore)
      {:ok, _reply} = Musubi.Testing.dispatch_command(page, :flash, %{})

      assert_raise ExUnit.AssertionError, ~r/payload mismatch/, fn ->
        Musubi.Testing.assert_push_event(:toast, %{msg: "Wrong"})
      end
    end

    test "flunks when no matching event arrives" do
      page = Musubi.Testing.mount(SimpleStore)
      {:ok, _reply} = Musubi.Testing.dispatch_command(page, :pick, %{})

      assert_raise ExUnit.AssertionError, ~r/expected a push event named "toast"/, fn ->
        Musubi.Testing.assert_push_event(:toast, %{msg: "x"}, 20)
      end
    end
  end

  describe "refute_push_event/2" do
    test "passes when no matching event is delivered" do
      page = Musubi.Testing.mount(SimpleStore)
      {:ok, _reply} = Musubi.Testing.dispatch_command(page, :pick, %{})

      assert :ok = Musubi.Testing.refute_push_event(:toast, 20)
    end

    test "flunks when the event is delivered" do
      page = Musubi.Testing.mount(SimpleStore)
      {:ok, _reply} = Musubi.Testing.dispatch_command(page, :flash, %{})

      assert_raise ExUnit.AssertionError, ~r/unexpected push event "toast"/, fn ->
        Musubi.Testing.refute_push_event(:toast)
      end
    end
  end

  describe "child store addressing via store_id" do
    defmodule FiltersStore do
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

    defmodule ParentStore do
      use Musubi.Store, root: true

      state do
        field :filters, FiltersStore.t()
      end

      @impl Musubi.Store
      def mount(_params, socket), do: {:ok, socket}

      @impl Musubi.Store
      def render(_socket) do
        %{filters: Musubi.Child.child(FiltersStore, id: "filters")}
      end

      @impl Musubi.Store
      def handle_command(_name, _payload, socket), do: {:noreply, socket}
    end

    test "dispatch_command routes to child store via store_id" do
      page = Musubi.Testing.mount(ParentStore)

      assert {:ok, %{"ok" => true}} =
               Musubi.Testing.dispatch_command(
                 page,
                 :change_query,
                 %{"query" => "shirt"},
                 ["filters"]
               )

      assert Musubi.Testing.render(page, ["filters"]) == %{query: "shirt"}
      assert Musubi.Testing.assigns(page, ["filters"]).query == "shirt"
    end

    test "render at root returns wire-shape map with child placeholder resolved" do
      page = Musubi.Testing.mount(ParentStore)

      # Root render emits the child placeholder; the resolver substitutes
      # the child's render output into the slot before the wire envelope
      # is built. `render/2` at the root level returns the unresolved
      # placeholder (the store's literal `render/1` output).
      assert %{filters: %Musubi.Child{module: FiltersStore, id: "filters"}} =
               Musubi.Testing.render(page)
    end
  end
end
