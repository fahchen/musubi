defmodule Musubi.Page.ServerChildAsyncTest do
  @moduledoc """
  Verifies async + lifecycle-hook support on child stores: assign_async,
  start_async, cancel_async, stream_async, plus the `:handle_async` and
  `:before_command` hook chains dispatched along the path chain so hooks
  attached on the child socket fire alongside any root-attached defaults.
  """

  use ExUnit.Case, async: true

  import Musubi.AsyncTestHelpers

  alias Musubi.AsyncResult
  alias Musubi.Page.PatchEnvelope
  alias Musubi.Page.Server
  alias Musubi.Page.StoreTable

  @async_terminal_events [
    [:musubi, :async, :stop],
    [:musubi, :async, :exception]
  ]

  defmodule WidgetStore do
    @moduledoc false
    use Musubi.Store

    import Musubi.AsyncTestHelpers

    state do
      field :data, String.t() | nil
      field :slow, Musubi.AsyncResult.of(String.t())
      stream_async :messages, %{id: String.t(), body: String.t()}
    end

    attr :test_pid, pid(), required: true

    command :load do
      payload do
        field :id, String.t()
      end
    end

    command :start_warm do
      payload do
        field :tag, String.t()
      end
    end

    command :start_slow

    command :cancel_slow

    command :load_messages

    @impl Musubi.Store
    def mount(socket) do
      hook_pid = socket.assigns.test_pid

      socket =
        socket
        |> Musubi.Lifecycle.attach_hook(:trace_async, :handle_async, fn name, result, sock ->
          send(hook_pid, {:child_handle_async_hook, sock.id, name, result})
          {:cont, sock}
        end)
        |> Musubi.Lifecycle.attach_hook(:trace_before, :before_command, fn name, payload, sock ->
          send(hook_pid, {:child_before_command_hook, sock.id, name, payload})
          {:cont, sock}
        end)
        |> Musubi.Socket.assign(:data, nil)
        |> Musubi.Socket.assign(:slow, AsyncResult.loading())
        |> Musubi.Socket.assign(:messages, AsyncResult.loading())

      {:ok, socket}
    end

    @impl Musubi.Store
    def render(socket) do
      %{
        data: Map.get(socket.assigns, :data),
        slow: socket.assigns.slow,
        messages: async_stream(:messages)
      }
    end

    @impl Musubi.Store
    def handle_command(:load, %{"id" => id}, socket) do
      fun = instrument(socket.assigns.test_pid, fn -> {:ok, "loaded:" <> id} end)
      {:reply, %{ok: true}, Musubi.Async.assign_async(socket, :data, fun)}
    end

    @impl Musubi.Store
    def handle_command(:start_warm, %{"tag" => tag}, socket) do
      fun = instrument(socket.assigns.test_pid, fn -> {:warmed, tag} end)
      {:noreply, Musubi.Async.start_async(socket, :warm, fun)}
    end

    @impl Musubi.Store
    def handle_command(:start_slow, _payload, socket) do
      fun =
        instrument(socket.assigns.test_pid, fn ->
          receive do
            {:never, _msg} -> {:ok, "never"}
          end
        end)

      {:noreply, Musubi.Async.assign_async(socket, :slow, fun)}
    end

    @impl Musubi.Store
    def handle_command(:cancel_slow, _payload, socket) do
      {:noreply, Musubi.Async.cancel_async(socket, :slow, :user_navigated)}
    end

    @impl Musubi.Store
    def handle_command(:load_messages, _payload, socket) do
      fun =
        instrument(socket.assigns.test_pid, fn ->
          {:ok, [%{id: "m1", body: "hi"}, %{id: "m2", body: "yo"}]}
        end)

      {:noreply, Musubi.Async.stream_async(socket, :messages, fun)}
    end

    @impl Musubi.Store
    def handle_async(:warm, {:ok, {:warmed, _tag}}, socket) do
      send(socket.assigns.test_pid, {:child_handle_async_callback, socket.id, :warm})
      {:noreply, socket}
    end

    @impl Musubi.Store
    def handle_async(_name, _result, socket), do: {:noreply, socket}
  end

  defmodule RootStore do
    @moduledoc false
    use Musubi.Store

    state do
      field :widget, map()
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, socket}

    @impl Musubi.Store
    def render(socket) do
      %{widget: Musubi.Child.child(WidgetStore, id: "w1", test_pid: socket.assigns.test_pid)}
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  describe "child store async + hook routing" do
    test "scenario 1: assign_async from a child writes AsyncResult onto the child's assigns" do
      ref = attach_async_terminal_handler!()
      pid = start!()

      assert {:ok, _reply} = Server.command(pid, ["w1"], :load, %{"id" => "abc"})
      await_task!()
      sync_server!(pid)

      assert_received {[:musubi, :async, :stop], ^ref, _, %{name: :data, status: :ok}}
      assert %AsyncResult{status: :ok, result: "loaded:abc"} = child_assign(pid, :data)
    end

    test "scenario 2: start_async from a child invokes the child's handle_async/3" do
      pid = start!()

      assert {:ok, _reply} = Server.command(pid, ["w1"], :start_warm, %{"tag" => "ada"})
      await_task!()
      sync_server!(pid)

      assert_received {:child_handle_async_callback, "w1", :warm}
    end

    test "scenario 3: :handle_async hook attached in the child's mount fires for child tasks" do
      pid = start!()

      assert {:ok, _reply} = Server.command(pid, ["w1"], :start_warm, %{"tag" => "ada"})
      await_task!()
      sync_server!(pid)

      assert_received {:child_handle_async_hook, "w1", :warm, {:ok, {:warmed, "ada"}}}
    end

    test "scenario 4: :before_command hook attached in the child's mount fires when a command targets it" do
      pid = start!()

      assert {:ok, _reply} = Server.command(pid, ["w1"], :load, %{"id" => "xyz"})
      await_task!()
      sync_server!(pid)

      assert_received {:child_before_command_hook, "w1", :load, %{"id" => "xyz"}}
    end

    test "scenario 5: cancel_async from a child resolves the slot to failed/{:exit, reason}" do
      telemetry_ref = attach_async_terminal_handler!()
      pid = start!()

      assert {:ok, _reply} = Server.command(pid, ["w1"], :start_slow, %{})

      task_pid = receive_task_pid!()
      sync_server!(pid)

      ref = Process.monitor(task_pid)

      assert {:ok, _reply} = Server.command(pid, ["w1"], :cancel_slow, %{})
      assert_receive {:DOWN, ^ref, _, _, _}, 200
      sync_server!(pid)

      assert_received {[:musubi, :async, :stop], ^telemetry_ref, _,
                       %{name: :slow, status: :failed}}

      assert %AsyncResult{status: :failed, reason: {:exit, :user_navigated}} =
               child_assign(pid, :slow)
    end

    test "scenario 6: stream_async from a child seeds stream ops + AsyncResult on the child" do
      ref = attach_async_terminal_handler!()
      pid = start!()

      assert {:ok, _reply} = Server.command(pid, ["w1"], :load_messages, %{})
      await_task!()
      sync_server!(pid)

      assert_received {:patch,
                       %PatchEnvelope{
                         stream_ops: [
                           %{op: "insert", stream: "messages", store_id: ["w1"]},
                           %{op: "insert", stream: "messages", store_id: ["w1"]}
                         ]
                       }}

      assert_received {[:musubi, :async, :stop], ^ref, _, %{name: :messages, status: :ok}}
      assert %AsyncResult{status: :ok, result: true} = child_assign(pid, :messages)
    end
  end

  defp start! do
    pid =
      start_supervised!(
        {Server, {RootStore, %{"page_id" => "p1", test_pid: self()}, %{transport_pid: self()}}}
      )

    sync_server!(pid)
    assert_received {:patch, %PatchEnvelope{base_version: 0, version: 1}}
    pid
  end

  defp attach_async_terminal_handler! do
    ref = :telemetry_test.attach_event_handlers(self(), @async_terminal_events)
    on_exit(fn -> :telemetry.detach(ref) end)
    ref
  end

  defp child_assign(pid, key) do
    %{store_table: registry} = :sys.get_state(pid)
    entry = StoreTable.get(registry, ["w1"])
    Map.get(entry.socket.assigns, key)
  end
end
