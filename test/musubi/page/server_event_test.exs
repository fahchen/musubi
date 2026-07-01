defmodule Musubi.Page.ServerEventTest do
  @moduledoc """
  Verifies push events (BDR-0032) fold into the patch envelope: the page server
  drains queued events each render cycle into `PatchEnvelope.events`, ships one
  consolidated `"patch"`, and an event-only cycle still emits an envelope and
  bumps `version`. Events queued on a child socket flatten into the one root
  envelope (root-scoped).
  """

  use ExUnit.Case, async: true

  alias Musubi.Page.PatchEnvelope
  alias Musubi.Page.Server

  defmodule ChildStore do
    @moduledoc false
    use Musubi.Store

    state do
      field :n, integer()
    end

    @impl Musubi.Store
    def init(socket), do: {:ok, assign(socket, :n, 0)}

    @impl Musubi.Store
    def render(socket), do: %{n: socket.assigns.n}

    command :child_toast

    @impl Musubi.Store
    def handle_command(:child_toast, _payload, socket) do
      {:noreply, push_event(socket, "from_child", %{ok: true})}
    end
  end

  defmodule RootStore do
    @moduledoc false
    use Musubi.Store

    alias Musubi.Page.ServerEventTest.ChildStore

    state do
      field :title, String.t()
      field :child, ChildStore.state()
    end

    @impl Musubi.Store
    def mount(socket) do
      socket =
        if socket.assigns[:emit_on_mount] do
          push_event(socket, "boot", %{ready: true})
        else
          socket
        end

      {:ok, assign(socket, :title, "Page")}
    end

    @impl Musubi.Store
    def render(socket), do: %{title: socket.assigns.title, child: child(ChildStore, id: "child")}

    command :toast
    command :rename_and_toast
    command :double_toast

    @impl Musubi.Store
    def handle_command(:toast, _payload, socket) do
      {:noreply, push_event(socket, :toast, %{msg: "saved", level: :info})}
    end

    def handle_command(:rename_and_toast, _payload, socket) do
      socket =
        socket
        |> assign(:title, "Renamed")
        |> push_event("toast", %{msg: "renamed"})

      {:noreply, socket}
    end

    def handle_command(:double_toast, _payload, socket) do
      socket =
        socket
        |> push_event("first", %{n: 1})
        |> push_event("second", %{n: 2})

      {:noreply, socket}
    end
  end

  defmodule HaltEventStore do
    @moduledoc false
    use Musubi.Store

    alias Musubi.Lifecycle

    state do
      field :title, String.t()
    end

    command :gated

    @impl Musubi.Store
    def mount(socket) do
      socket =
        Lifecycle.attach_hook(socket, :emit_then_halt, :before_command, fn _name,
                                                                           _payload,
                                                                           sock ->
          {:halt, Musubi.Event.push_event(sock, "from_hook", %{n: 1})}
        end)

      {:ok, assign(socket, :title, "Page")}
    end

    @impl Musubi.Store
    def render(socket), do: %{title: socket.assigns.title}

    @impl Musubi.Store
    def handle_command(:gated, _payload, socket), do: {:noreply, socket}
  end

  defmodule TransformEventStore do
    @moduledoc false
    use Musubi.Store

    alias Musubi.Lifecycle

    state do
      field :title, String.t()
    end

    command :emit

    @impl Musubi.Store
    def mount(socket) do
      socket =
        Lifecycle.attach_hook(socket, :redact, :before_events, fn events, sock ->
          kept =
            events
            |> Enum.reject(&(&1.name == "secret"))
            |> Enum.map(&Map.update!(&1, :payload, fn p -> Map.put(p, "seen", true) end))

          {:cont, kept, sock}
        end)

      {:ok, assign(socket, :title, "Page")}
    end

    @impl Musubi.Store
    def render(socket), do: %{title: socket.assigns.title}

    @impl Musubi.Store
    def handle_command(:emit, _payload, socket) do
      socket =
        socket
        |> push_event("secret", %{n: 1})
        |> push_event("public", %{n: 2})

      {:noreply, socket}
    end
  end

  defp start!(assigns \\ %{}) do
    start_supervised!({Server, {RootStore, assigns, %{transport_pid: self()}}})
  end

  test "a halted command still renders, shipping events a before_command hook queued" do
    pid = start_supervised!({Server, {HaltEventStore, %{}, %{transport_pid: self()}}})
    assert_receive {:patch, %PatchEnvelope{version: 1}}

    # The hook queues an event then halts (handler never runs). The halt path
    # renders like :ok, so the hook's event ships in one envelope, in context.
    _reply = Server.command(pid, [], :gated, %{})
    assert_receive {:patch, env}
    assert %PatchEnvelope{ops: [], events: [%{name: "from_hook", payload: %{"n" => 1}}]} = env
  end

  test "event-only command emits envelope with ops: [] and bumps version" do
    pid = start!()
    assert_receive {:patch, %PatchEnvelope{version: 1}}

    {:ok, %{}} = Server.command(pid, [], :toast, %{})
    assert_receive {:patch, env}

    assert %PatchEnvelope{
             base_version: 1,
             version: 2,
             ops: [],
             stream_ops: [],
             events: [%{name: "toast", payload: %{"msg" => "saved", "level" => "info"}}]
           } = env
  end

  test "event and diff in the same cycle ship in one envelope" do
    pid = start!()
    assert_receive {:patch, %PatchEnvelope{version: 1}}

    {:ok, %{}} = Server.command(pid, [], :rename_and_toast, %{})
    assert_receive {:patch, env}

    assert %PatchEnvelope{
             version: 2,
             ops: [%{op: "replace", path: "/title", value: "Renamed"}],
             events: [%{name: "toast", payload: %{"msg" => "renamed"}}]
           } = env

    refute_receive {:patch, _}, 50
  end

  test "multiple events in one cycle preserve FIFO order in one envelope" do
    pid = start!()
    assert_receive {:patch, %PatchEnvelope{version: 1}}

    {:ok, %{}} = Server.command(pid, [], :double_toast, %{})
    assert_receive {:patch, env}

    assert %PatchEnvelope{
             events: [
               %{name: "first", payload: %{"n" => 1}},
               %{name: "second", payload: %{"n" => 2}}
             ]
           } = env
  end

  test "mount-time event rides the initial envelope" do
    _pid = start!(%{emit_on_mount: true})

    assert_receive {:patch, env}

    assert %PatchEnvelope{
             base_version: 0,
             version: 1,
             events: [%{name: "boot", payload: %{"ready" => true}}]
           } = env
  end

  test "event queued on a child socket flattens into the root envelope (root-scoped)" do
    pid = start!()
    assert_receive {:patch, %PatchEnvelope{version: 1}}

    {:ok, %{}} = Server.command(pid, ["child"], :child_toast, %{})
    assert_receive {:patch, env}

    assert %PatchEnvelope{events: [%{name: "from_child", payload: %{"ok" => true}}]} = env
  end

  test "a :before_events hook rewrites the outbound event list before egress" do
    pid = start_supervised!({Server, {TransformEventStore, %{}, %{transport_pid: self()}}})
    assert_receive {:patch, %PatchEnvelope{version: 1}}

    # mount attaches a :before_events hook dropping "secret" and tagging the rest.
    {:ok, %{}} = Server.command(pid, [], :emit, %{})
    assert_receive {:patch, env}

    assert %PatchEnvelope{
             events: [%{name: "public", payload: %{"n" => 2, "seen" => true}}]
           } = env
  end
end
