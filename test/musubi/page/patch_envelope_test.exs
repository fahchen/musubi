defmodule Musubi.Page.PatchEnvelopeTest do
  use ExUnit.Case, async: true

  alias Musubi.Page.PatchEnvelope
  alias Musubi.Page.Server
  alias Musubi.Stream

  defmodule TitleStore do
    @moduledoc false

    use Musubi.Store

    state do
      field :title, String.t()
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, Musubi.Socket.assign(socket, :title, "Inbox")}
    @impl Musubi.Store
    def render(socket), do: %{title: socket.assigns.title}

    command :rename do
      payload do
        field :title, String.t()
      end
    end

    @impl Musubi.Store
    def handle_command(:rename, %{"title" => title}, socket),
      do: {:noreply, Musubi.Socket.assign(socket, :title, title)}
  end

  defmodule SeedingStore do
    @moduledoc false

    use Musubi.Store

    state do
      field :title, String.t()
      stream :messages, String.t()
    end

    @impl Musubi.Store
    def mount(socket) do
      socket =
        socket
        |> Musubi.Socket.assign(:title, "Hello")
        |> Stream.stream_insert(:messages, %{id: "1", body: "first"})
        |> Stream.stream_insert(:messages, %{id: "2", body: "second"})

      {:ok, socket}
    end

    @impl Musubi.Store
    def render(socket), do: %{title: socket.assigns.title, messages: stream(:messages)}

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defmodule StreamOnlyHandlerStore do
    @moduledoc false

    use Musubi.Store

    state do
      field :title, String.t()
      stream :messages, String.t()
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, Musubi.Socket.assign(socket, :title, "static")}

    @impl Musubi.Store
    def render(socket), do: %{title: socket.assigns.title, messages: stream(:messages)}

    command :ping

    @impl Musubi.Store
    def handle_command(:ping, _payload, socket) do
      {:noreply, Stream.stream_insert(socket, :messages, %{id: "n", body: "noop"})}
    end
  end

  defmodule NoopStore do
    @moduledoc false

    use Musubi.Store

    state do
      field :ok, boolean()
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, Musubi.Socket.assign(socket, :ok, true)}
    @impl Musubi.Store
    def render(socket), do: %{ok: socket.assigns.ok}

    command :ping
    @impl Musubi.Store
    def handle_command(:ping, _payload, socket), do: {:noreply, socket}
  end

  describe "Rule: Initial state is delivered via the first patch envelope" do
    test "first envelope is base_version: 0, version: 1, single replace at root" do
      _pid = start_supervised!({Server, {TitleStore, %{}, %{transport_pid: self()}}})

      assert_receive {:patch, envelope}

      assert %PatchEnvelope{
               type: "patch",
               base_version: 0,
               version: 1,
               ops: [%{op: "replace", path: "", value: %{"title" => "Inbox"}}],
               stream_ops: []
             } = envelope
    end

    test "mount-time stream seeds split between marker at path and stream_ops" do
      _pid = start_supervised!({Server, {SeedingStore, %{}, %{transport_pid: self()}}})

      assert_receive {:patch, envelope}

      assert %PatchEnvelope{
               base_version: 0,
               version: 1,
               ops: [%{op: "replace", path: "", value: root_wire}],
               stream_ops: stream_ops
             } = envelope

      # Stream-typed `messages` field appears as a marker inside the wire root.
      assert root_wire["messages"] == %{"__musubi_stream__" => "messages"}
      assert root_wire["title"] == "Hello"

      # Stream content flows entirely through stream_ops.
      assert [
               %{op: "insert", stream: "messages", store_id: [], item_key: "messages-1"},
               %{op: "insert", stream: "messages", store_id: [], item_key: "messages-2"}
             ] = stream_ops
    end
  end

  describe "Rule: Version increments by 1 per emitted envelope" do
    test "subsequent envelope's base_version equals the prior version" do
      pid = start_supervised!({Server, {TitleStore, %{}, %{transport_pid: self()}}})
      assert_receive {:patch, %PatchEnvelope{version: 1}}

      {:ok, %{}} = Server.command(pid, [], :rename, %{"title" => "Outbox"})
      assert_receive {:patch, env2}

      assert %PatchEnvelope{
               base_version: 1,
               version: 2,
               ops: [%{op: "replace", path: "/title", value: "Outbox"}]
             } = env2
    end
  end

  describe "Rule: Stream-only render cycles still emit envelopes" do
    test "handler that only mutates a stream emits envelope with ops: []" do
      pid =
        start_supervised!({Server, {StreamOnlyHandlerStore, %{}, %{transport_pid: self()}}})

      assert_receive {:patch, %PatchEnvelope{version: 1}}

      {:ok, %{}} = Server.command(pid, [], :ping, %{})
      assert_receive {:patch, env}

      assert %PatchEnvelope{ops: [], stream_ops: [%{op: "insert", store_id: []}]} = env
    end
  end

  describe "Rule: idle render cycles emit nothing" do
    test "handler that returns socket unchanged with no stream ops emits no envelope" do
      pid = start_supervised!({Server, {NoopStore, %{}, %{transport_pid: self()}}})
      assert_receive {:patch, %PatchEnvelope{version: 1}}

      {:ok, %{}} = Server.command(pid, [], :ping, %{})
      refute_receive {:patch, _}, 100
    end
  end

  describe "Rule: push events ride the patch envelope" do
    test "build emits when only events are present and bumps version" do
      envelope = PatchEnvelope.build(4, [], [], [], [%{name: "toast", payload: %{"msg" => "hi"}}])

      assert %PatchEnvelope{
               base_version: 4,
               version: 5,
               ops: [],
               stream_ops: [],
               upload_ops: [],
               events: [%{name: "toast", payload: %{"msg" => "hi"}}]
             } = envelope
    end

    test "build returns nil only when ops, stream_ops, upload_ops, and events are all empty" do
      assert PatchEnvelope.build(3, [], [], [], []) == nil
      refute PatchEnvelope.build(3, [], [], [], [%{name: "e", payload: %{}}]) == nil
    end

    test "to_wire carries events" do
      wire =
        PatchEnvelope.to_wire(%PatchEnvelope{
          base_version: 0,
          version: 1,
          events: [%{name: "ping", payload: 1}]
        })

      assert wire["events"] == [%{name: "ping", payload: 1}]
    end

    test "initial carries mount-time events" do
      envelope =
        PatchEnvelope.initial(%{"title" => "Inbox"}, [], [], [%{name: "boot", payload: %{}}])

      assert envelope.events == [%{name: "boot", payload: %{}}]
    end
  end
end
