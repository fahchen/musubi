defmodule Musubi.Page.ServerAsyncTest do
  use ExUnit.Case, async: true

  import Musubi.AsyncTestHelpers
  import ExUnit.CaptureLog

  require Logger

  alias Musubi.Async
  alias Musubi.AsyncResult
  alias Musubi.Page.PatchEnvelope
  alias Musubi.Page.Server
  alias Musubi.Page.StoreTable
  alias Musubi.Stream

  defmodule AsyncStore do
    @moduledoc false
    use Musubi.Store

    import Musubi.AsyncTestHelpers

    state do
      field :profile, Musubi.AsyncResult.of(%{name: String.t()})
      field :user, Musubi.AsyncResult.of(String.t())
      field :org, Musubi.AsyncResult.of(String.t())
      field :cache_status, String.t()
      stream_async :messages, %{id: String.t(), body: String.t()}
    end

    command :load_profile do
      payload do
        field :name, String.t()
      end
    end

    command :load_profile_blocked do
      payload do
        field :name, String.t()
        field :tag, String.t()
      end
    end

    command :load_profile_bad_return

    command :load_profile_raise

    command :load_profile_exit

    command :load_identity

    command :load_identity_missing_key

    command :start_warm do
      payload do
        field :name, String.t()
      end
    end

    command :start_warm_blocked do
      payload do
        field :name, String.t()
        field :tag, String.t()
      end
    end

    command :start_warm_raise

    command :start_warm_exit

    command :cancel_warm do
      payload do
        field :reason, String.t()
      end
    end

    command :raising_handle_async

    command :cancel_profile_by_name do
      payload do
        field :reason, String.t()
      end
    end

    command :cancel_profile_by_value do
      payload do
        field :reason, String.t()
      end
    end

    command :stream_messages do
      payload do
        field :mode, String.t()
      end
    end

    command :stream_messages_blocked do
      payload do
        field :tag, String.t()
      end
    end

    command :cancel_messages do
      payload do
        field :reason, String.t()
      end
    end

    # Reference cancel atoms used by the test so `String.to_existing_atom/1`
    # in `handle_command/3` can resolve them at runtime.
    @cancel_reasons [:user_left, :user_navigated]
    def __cancel_reasons__, do: @cancel_reasons

    @impl Musubi.Store
    def mount(socket) do
      socket =
        socket
        |> Musubi.Socket.assign(:profile, AsyncResult.ok(nil, %{name: "cached"}))
        |> Musubi.Socket.assign(:user, AsyncResult.ok(nil, "cached-user"))
        |> Musubi.Socket.assign(:org, AsyncResult.ok(nil, "cached-org"))
        |> Musubi.Socket.assign(:cache_status, "cold")
        |> Musubi.Socket.assign(:messages, AsyncResult.loading())

      {:ok, socket}
    end

    @impl Musubi.Store
    def render(socket) do
      %{
        profile: socket.assigns.profile,
        user: socket.assigns.user,
        org: socket.assigns.org,
        cache_status: socket.assigns.cache_status,
        messages: async_stream(:messages)
      }
    end

    @impl Musubi.Store
    def handle_command(:load_profile, %{"name" => name}, socket) do
      fun = instrument(test_pid(socket), fn -> {:ok, %{name: name}} end)
      {:noreply, Musubi.Async.assign_async(socket, :profile, fun)}
    end

    @impl Musubi.Store
    def handle_command(:load_profile_blocked, %{"name" => name, "tag" => tag}, socket) do
      fun =
        instrument(test_pid(socket), fn ->
          receive do
            {:continue, ^tag} -> {:ok, %{name: name}}
          end
        end)

      {:noreply, Musubi.Async.assign_async(socket, :profile, fun)}
    end

    @impl Musubi.Store
    def handle_command(:load_profile_bad_return, _payload, socket) do
      fun = instrument(test_pid(socket), fn -> 123 end)
      {:noreply, Musubi.Async.assign_async(socket, :profile, fun)}
    end

    @impl Musubi.Store
    def handle_command(:load_profile_raise, _payload, socket) do
      fun = instrument(test_pid(socket), fn -> raise "boom" end)
      {:noreply, Musubi.Async.assign_async(socket, :profile, fun)}
    end

    @impl Musubi.Store
    def handle_command(:load_profile_exit, _payload, socket) do
      fun = instrument(test_pid(socket), fn -> exit(:boom) end)
      {:noreply, Musubi.Async.assign_async(socket, :profile, fun)}
    end

    @impl Musubi.Store
    def handle_command(:load_identity, _payload, socket) do
      fun = instrument(test_pid(socket), fn -> {:ok, %{user: "ada", org: "musubi"}} end)
      {:noreply, Musubi.Async.assign_async(socket, [:user, :org], fun)}
    end

    @impl Musubi.Store
    def handle_command(:load_identity_missing_key, _payload, socket) do
      fun = instrument(test_pid(socket), fn -> {:ok, %{user: "ada"}} end)
      {:noreply, Musubi.Async.assign_async(socket, [:user, :org], fun)}
    end

    @impl Musubi.Store
    def handle_command(:start_warm, %{"name" => name}, socket) do
      fun = instrument(test_pid(socket), fn -> {:warmed, name} end)
      {:noreply, Musubi.Async.start_async(socket, :warm_cache, fun)}
    end

    @impl Musubi.Store
    def handle_command(:start_warm_blocked, %{"name" => name, "tag" => tag}, socket) do
      fun =
        instrument(test_pid(socket), fn ->
          receive do
            {:continue, ^tag} -> {:warmed, name}
          end
        end)

      {:noreply, Musubi.Async.start_async(socket, :warm_cache, fun)}
    end

    @impl Musubi.Store
    def handle_command(:start_warm_raise, _payload, socket) do
      fun = instrument(test_pid(socket), fn -> raise "boom" end)
      {:noreply, Musubi.Async.start_async(socket, :warm_cache, fun)}
    end

    @impl Musubi.Store
    def handle_command(:start_warm_exit, _payload, socket) do
      fun = instrument(test_pid(socket), fn -> exit(:boom) end)
      {:noreply, Musubi.Async.start_async(socket, :warm_cache, fun)}
    end

    @impl Musubi.Store
    def handle_command(:cancel_warm, %{"reason" => reason}, socket) do
      {:noreply, Musubi.Async.cancel_async(socket, :warm_cache, String.to_existing_atom(reason))}
    end

    @impl Musubi.Store
    def handle_command(:raising_handle_async, _payload, socket) do
      fun = instrument(test_pid(socket), fn -> :ok end)
      {:noreply, Musubi.Async.start_async(socket, :raises, fun)}
    end

    @impl Musubi.Store
    def handle_command(:cancel_profile_by_name, %{"reason" => reason}, socket) do
      socket = Musubi.Async.cancel_async(socket, :profile, String.to_existing_atom(reason))
      {:noreply, socket}
    end

    @impl Musubi.Store
    def handle_command(:cancel_profile_by_value, %{"reason" => reason}, socket) do
      socket =
        Musubi.Async.cancel_async(socket, socket.assigns.profile, String.to_existing_atom(reason))

      {:noreply, socket}
    end

    @impl Musubi.Store
    def handle_command(:stream_messages, %{"mode" => "ok"}, socket) do
      fun =
        instrument(test_pid(socket), fn ->
          {:ok, [%{id: "m1", body: "First"}, %{id: "m2", body: "Second"}]}
        end)

      {:noreply, Musubi.Async.stream_async(socket, :messages, fun)}
    end

    @impl Musubi.Store
    def handle_command(:stream_messages, %{"mode" => "ok_with_opts"}, socket) do
      fun =
        instrument(test_pid(socket), fn ->
          {:ok, [%{id: "m1", body: "First"}, %{id: "m2", body: "Second"}], at: 0, limit: -100}
        end)

      {:noreply, Musubi.Async.stream_async(socket, :messages, fun)}
    end

    @impl Musubi.Store
    def handle_command(:stream_messages, %{"mode" => "ok_with_reset"}, socket) do
      fun =
        instrument(test_pid(socket), fn ->
          {:ok, [%{id: "m3", body: "Reset First"}, %{id: "m4", body: "Reset Second"}],
           reset: true}
        end)

      {:noreply, Musubi.Async.stream_async(socket, :messages, fun)}
    end

    @impl Musubi.Store
    def handle_command(:stream_messages, %{"mode" => "error"}, socket) do
      fun = instrument(test_pid(socket), fn -> {:error, :rate_limited} end)
      {:noreply, Musubi.Async.stream_async(socket, :messages, fun)}
    end

    @impl Musubi.Store
    def handle_command(:stream_messages, %{"mode" => "bad_return"}, socket) do
      fun = instrument(test_pid(socket), fn -> 123 end)
      {:noreply, Musubi.Async.stream_async(socket, :messages, fun)}
    end

    @impl Musubi.Store
    def handle_command(:stream_messages, %{"mode" => "not_enumerable"}, socket) do
      fun = instrument(test_pid(socket), fn -> {:ok, 123} end)
      {:noreply, Musubi.Async.stream_async(socket, :messages, fun)}
    end

    @impl Musubi.Store
    def handle_command(:stream_messages, %{"mode" => "raise"}, socket) do
      fun = instrument(test_pid(socket), fn -> raise "boom" end)
      {:noreply, Musubi.Async.stream_async(socket, :messages, fun)}
    end

    @impl Musubi.Store
    def handle_command(:stream_messages, %{"mode" => "exit"}, socket) do
      fun = instrument(test_pid(socket), fn -> exit(:boom) end)
      {:noreply, Musubi.Async.stream_async(socket, :messages, fun)}
    end

    @impl Musubi.Store
    def handle_command(:stream_messages_blocked, %{"tag" => tag}, socket) do
      fun =
        instrument(test_pid(socket), fn ->
          receive do
            {:continue, ^tag} -> {:ok, [%{id: "m5", body: "Blocked"}]}
          end
        end)

      {:noreply, Musubi.Async.stream_async(socket, :messages, fun)}
    end

    @impl Musubi.Store
    def handle_command(:cancel_messages, %{"reason" => reason}, socket) do
      socket = Musubi.Async.cancel_async(socket, :messages, String.to_existing_atom(reason))
      {:noreply, socket}
    end

    @impl Musubi.Store
    def handle_async(:warm_cache, {:ok, {:warmed, name}}, socket) do
      {:noreply, Musubi.Socket.assign(socket, :cache_status, "warm:" <> name)}
    end

    @impl Musubi.Store
    def handle_async(:warm_cache, {:exit, reason}, socket) do
      {:noreply, Musubi.Socket.assign(socket, :cache_status, "exit:" <> inspect(reason))}
    end

    @impl Musubi.Store
    def handle_async(:raises, {:ok, _value}, _socket) do
      raise "boom-in-handle-async"
    end

    defp test_pid(socket), do: socket.assigns["test_pid"]
  end

  defmodule MissingStreamStore do
    @moduledoc false
    use Musubi.Store

    command :load_messages

    @impl Musubi.Store
    def mount(socket), do: {:ok, socket}

    @impl Musubi.Store
    def render(_socket), do: %{}

    @impl Musubi.Store
    def handle_command(:load_messages, _payload, socket) do
      {:noreply, Musubi.Async.stream_async(socket, :messages, fn -> {:ok, []} end)}
    end
  end

  describe "assign_async/3,4" do
    test "writes the final ok value onto the socket" do
      pid = start!()

      assert {:ok, _reply} = Server.command(pid, [], :load_profile, %{"name" => "ada"})
      await_task!()
      sync_server!(pid)

      assert %AsyncResult{status: :ok, result: %{name: "ada"}, reason: nil} =
               root_socket(pid).assigns.profile
    end

    test "exposes the loading state while a task is still running" do
      pid = start!()

      assert {:ok, _reply} =
               Server.command(pid, [], :load_profile_blocked, %{
                 "name" => "ada",
                 "tag" => "loading"
               })

      task_pid = receive_task_pid!()
      sync_server!(pid)

      assert %AsyncResult{status: :loading, result: %{name: "cached"}, reason: nil} =
               root_socket(pid).assigns.profile

      ref = Process.monitor(task_pid)
      send(task_pid, {:continue, "loading"})
      assert_receive {:DOWN, ^ref, _, _, _}, 200
      sync_server!(pid)

      assert %AsyncResult{status: :ok, result: %{name: "ada"}, reason: nil} =
               root_socket(pid).assigns.profile
    end

    test "writes all keys for multi-key tasks" do
      pid = start!()

      assert {:ok, _reply} = Server.command(pid, [], :load_identity, %{})
      await_task!()
      sync_server!(pid)

      socket = root_socket(pid)
      assert %AsyncResult{status: :ok, result: "ada"} = socket.assigns.user
      assert %AsyncResult{status: :ok, result: "musubi"} = socket.assigns.org
    end

    test "marks invalid return values as failed" do
      pid = start!()

      assert {:ok, _reply} = Server.command(pid, [], :load_profile_bad_return, %{})
      await_task!()
      sync_server!(pid)

      assert %AsyncResult{status: :failed, reason: {:exit, {:error, %ArgumentError{}, _stack}}} =
               root_socket(pid).assigns.profile
    end

    test "marks missing multi-key results as failed" do
      pid = start!()

      assert {:ok, _reply} = Server.command(pid, [], :load_identity_missing_key, %{})
      await_task!()
      sync_server!(pid)

      socket = root_socket(pid)

      assert %AsyncResult{status: :failed, reason: {:exit, {:error, %ArgumentError{}, _stack}}} =
               socket.assigns.user

      assert %AsyncResult{status: :failed, reason: {:exit, {:error, %ArgumentError{}, _stack}}} =
               socket.assigns.org
    end

    test "marks raised exceptions as failed exits" do
      pid = start!()

      assert {:ok, _reply} = Server.command(pid, [], :load_profile_raise, %{})
      await_task!()
      sync_server!(pid)

      assert %AsyncResult{
               status: :failed,
               reason: {:exit, {:error, %RuntimeError{message: "boom"}, _stack}}
             } = root_socket(pid).assigns.profile
    end

    test "marks exited tasks as failed exits" do
      pid = start!()

      assert {:ok, _reply} = Server.command(pid, [], :load_profile_exit, %{})
      await_task!()
      sync_server!(pid)

      assert %AsyncResult{status: :failed, reason: {:exit, :boom}} =
               root_socket(pid).assigns.profile
    end

    test "cancel_async by name resolves the tracked assign to failed" do
      pid = start!()

      assert {:ok, _reply} =
               Server.command(pid, [], :load_profile_blocked, %{
                 "name" => "ada",
                 "tag" => "cancel-name"
               })

      task_pid = receive_task_pid!()
      task_ref = Process.monitor(task_pid)
      sync_server!(pid)

      assert %AsyncResult{status: :loading} = root_socket(pid).assigns.profile

      assert {:ok, _reply} =
               Server.command(pid, [], :cancel_profile_by_name, %{"reason" => "user_left"})

      assert_receive {:DOWN, ^task_ref, _, _, _}, 200
      sync_server!(pid)

      socket = root_socket(pid)
      assert %AsyncResult{status: :failed, reason: {:exit, :user_left}} = socket.assigns.profile
      assert %{} = Async.tracking(socket)
    end

    test "cancel_async by AsyncResult pre-writes the failure and drops tracking" do
      pid = start!()

      assert {:ok, _reply} =
               Server.command(pid, [], :load_profile_blocked, %{
                 "name" => "ada",
                 "tag" => "cancel-value"
               })

      task_pid = receive_task_pid!()
      task_ref = Process.monitor(task_pid)
      sync_server!(pid)

      assert %AsyncResult{status: :loading} = root_socket(pid).assigns.profile

      assert {:ok, _reply} =
               Server.command(pid, [], :cancel_profile_by_value, %{"reason" => "user_left"})

      assert_receive {:DOWN, ^task_ref, _, _, _}, 200
      sync_server!(pid)

      socket = root_socket(pid)
      assert %AsyncResult{status: :failed, reason: {:exit, :user_left}} = socket.assigns.profile
      assert %{} = Async.tracking(socket)
    end
  end

  describe "start_async/3,4" do
    test "does not mutate assigns before handle_async runs" do
      pid = start!()

      assert {:ok, _reply} =
               Server.command(pid, [], :start_warm_blocked, %{"name" => "ada", "tag" => "warm"})

      task_pid = receive_task_pid!()
      sync_server!(pid)

      # start_async does not pre-write assigns; cache_status stays "cold".
      refute_received {:patch, _envelope}
      assert %{assigns: %{cache_status: "cold"}} = root_socket(pid)

      ref = Process.monitor(task_pid)
      send(task_pid, {:continue, "warm"})
      assert_receive {:DOWN, ^ref, _, _, _}, 200
      sync_server!(pid)

      assert %{assigns: %{cache_status: "warm:ada"}} = root_socket(pid)
    end

    test "delivers raised task failures to handle_async/3" do
      pid = start!()

      assert {:ok, _reply} = Server.command(pid, [], :start_warm_raise, %{})
      await_task!()
      sync_server!(pid)

      assert "exit:" <> reason = root_socket(pid).assigns.cache_status
      assert reason =~ "RuntimeError"
      assert reason =~ "boom"
    end

    test "delivers task exits to handle_async/3" do
      pid = start!()

      assert {:ok, _reply} = Server.command(pid, [], :start_warm_exit, %{})
      await_task!()
      sync_server!(pid)

      assert %{assigns: %{cache_status: "exit::boom"}} = root_socket(pid)
    end

    test "delivers cancel exits to handle_async/3" do
      pid = start!()

      assert {:ok, _reply} =
               Server.command(pid, [], :start_warm_blocked, %{"name" => "ada", "tag" => "cancel"})

      task_pid = receive_task_pid!()
      sync_server!(pid)

      ref = Process.monitor(task_pid)

      assert {:ok, _reply} =
               Server.command(pid, [], :cancel_warm, %{"reason" => "user_navigated"})

      assert_receive {:DOWN, ^ref, _, _, _}, 200
      sync_server!(pid)

      assert %{assigns: %{cache_status: "exit::user_navigated"}} = root_socket(pid)
    end

    test "same-name overwrite keeps the latest result and lazy-discards the stale task" do
      ref = attach_telemetry_handler!([:musubi, :async, :lazy_discard])

      pid = start!()

      assert {:ok, _reply} =
               Server.command(pid, [], :start_warm_blocked, %{
                 "name" => "first",
                 "tag" => "first"
               })

      first_task = receive_task_pid!()
      sync_server!(pid)

      assert {:ok, _reply} = Server.command(pid, [], :start_warm, %{"name" => "second"})
      await_task!()
      sync_server!(pid)

      assert %{assigns: %{cache_status: "warm:second"}} = root_socket(pid)

      first_ref = Process.monitor(first_task)
      send(first_task, {:continue, "first"})
      assert_receive {:DOWN, ^first_ref, _, _, _}, 200
      sync_server!(pid)

      assert_received {[:musubi, :async, :lazy_discard], ^ref, _measurements, metadata}
      assert %{name: :warm_cache, kind: :start} = metadata
      assert %{assigns: %{cache_status: "warm:second"}} = root_socket(pid)
    end
  end

  describe "handle_async/3" do
    test "runtime survives, emits :exception telemetry, processes subsequent commands" do
      ref = attach_telemetry_handler!([:musubi, :async, :exception])

      pid = start!()

      capture_log(fn ->
        assert {:ok, _reply} = Server.command(pid, [], :raising_handle_async, %{})
        await_task!()
        sync_server!(pid)

        assert_received {[:musubi, :async, :exception], ^ref, _measurements, metadata}
        assert metadata.name == :raises
        assert metadata.kind == :start
        assert is_list(metadata.stacktrace)

        assert Process.alive?(pid)
        assert %{assigns: %{cache_status: "cold"}} = root_socket(pid)

        # Subsequent commands still work
        assert {:ok, _reply} = Server.command(pid, [], :start_warm, %{"name" => "after_crash"})
        await_task!()
        sync_server!(pid)

        assert %{assigns: %{cache_status: "warm:after_crash"}} = root_socket(pid)
        Logger.flush()
      end)
    end
  end

  describe "stream_async/3,4" do
    test "raises before spawning when no stream slot is declared" do
      Process.flag(:trap_exit, true)
      pid = start!(MissingStreamStore)
      Process.link(pid)

      capture_log(fn ->
        assert catch_exit(Server.command(pid, [], :load_messages, %{}))
        Logger.flush()
      end)

      refute Process.alive?(pid)
    end

    test "writes the final ok status onto the socket" do
      pid = start!()

      assert {:ok, _reply} = Server.command(pid, [], :stream_messages, %{"mode" => "ok"})
      await_task!()
      sync_server!(pid)

      assert %AsyncResult{status: :ok, result: true, reason: nil} =
               root_socket(pid).assigns.messages
    end

    test "shows loading while the stream task is running" do
      pid = start!()

      assert {:ok, _reply} =
               Server.command(pid, [], :stream_messages_blocked, %{"tag" => "loading"})

      task_pid = receive_task_pid!()
      sync_server!(pid)

      assert %AsyncResult{status: :loading, result: nil, reason: nil} =
               root_socket(pid).assigns.messages

      ref = Process.monitor(task_pid)
      send(task_pid, {:continue, "loading"})
      assert_receive {:DOWN, ^ref, _, _, _}, 200
      sync_server!(pid)

      assert %AsyncResult{status: :ok, result: true, reason: nil} =
               root_socket(pid).assigns.messages
    end

    test "emits insert ops with returned stream opts" do
      pid = start!()

      assert {:ok, _reply} =
               Server.command(pid, [], :stream_messages, %{"mode" => "ok_with_opts"})

      await_task!()
      sync_server!(pid)

      assert_received {:patch, %PatchEnvelope{stream_ops: stream_ops}}

      assert [
               %{op: "insert", at: 0, limit: -100, store_id: [], item_key: "messages-m1"},
               %{op: "insert", at: 0, limit: -100, store_id: [], item_key: "messages-m2"}
             ] = stream_ops

      assert %AsyncResult{status: :ok, result: true} = root_socket(pid).assigns.messages
    end

    test "emits a reset op when the task returns reset stream opts" do
      pid = start!()

      assert {:ok, _reply} =
               Server.command(pid, [], :stream_messages, %{"mode" => "ok_with_reset"})

      await_task!()
      sync_server!(pid)

      assert_received {:patch, %PatchEnvelope{stream_ops: stream_ops}}

      assert [
               %{op: "reset", stream: "messages", store_id: []},
               %{op: "insert", store_id: []},
               %{op: "insert", store_id: []}
             ] = stream_ops

      assert %AsyncResult{status: :ok, result: true} = root_socket(pid).assigns.messages
    end

    test "writes failed on {:error, reason} and leaves the stream slot untouched" do
      ref = attach_telemetry_handler!([:musubi, :async, :stop])

      pid = start!()

      assert {:ok, _reply} = Server.command(pid, [], :stream_messages, %{"mode" => "error"})
      await_task!()
      sync_server!(pid)

      assert_received {[:musubi, :async, :stop], ^ref, _measurements,
                       %{name: :messages, kind: :stream}}

      assert %AsyncResult{status: :failed, reason: {:error, :rate_limited}} =
               root_socket(pid).assigns.messages

      assert %Stream.Slot{inserts: [], deletes: [], reset?: false} =
               root_stream_slot(pid, :messages)
    end

    test "marks invalid return values as failed" do
      ref = attach_telemetry_handler!([:musubi, :async, :stop])

      pid = start!()

      assert {:ok, _reply} = Server.command(pid, [], :stream_messages, %{"mode" => "bad_return"})
      await_task!()
      sync_server!(pid)

      assert_received {[:musubi, :async, :stop], ^ref, _measurements,
                       %{name: :messages, kind: :stream}}

      assert %AsyncResult{status: :failed, reason: {:exit, {:error, %ArgumentError{}, _stack}}} =
               root_socket(pid).assigns.messages

      assert %Stream.Slot{inserts: [], deletes: [], reset?: false} =
               root_stream_slot(pid, :messages)
    end

    test "marks non-enumerable stream results as failed" do
      ref = attach_telemetry_handler!([:musubi, :async, :stop])

      pid = start!()

      assert {:ok, _reply} =
               Server.command(pid, [], :stream_messages, %{"mode" => "not_enumerable"})

      await_task!()
      sync_server!(pid)

      assert_received {[:musubi, :async, :stop], ^ref, _measurements,
                       %{name: :messages, kind: :stream}}

      assert %AsyncResult{status: :failed, reason: {:exit, {:error, %ArgumentError{}, _stack}}} =
               root_socket(pid).assigns.messages

      assert %Stream.Slot{inserts: [], deletes: [], reset?: false} =
               root_stream_slot(pid, :messages)
    end

    test "marks raised stream tasks as failed" do
      ref = attach_telemetry_handler!([:musubi, :async, :stop])

      pid = start!()

      assert {:ok, _reply} = Server.command(pid, [], :stream_messages, %{"mode" => "raise"})
      await_task!()
      sync_server!(pid)

      assert_received {[:musubi, :async, :stop], ^ref, _measurements,
                       %{name: :messages, kind: :stream}}

      assert %AsyncResult{
               status: :failed,
               reason: {:exit, {:error, %RuntimeError{message: "boom"}, _stack}}
             } = root_socket(pid).assigns.messages
    end

    test "marks exited stream tasks as failed" do
      ref = attach_telemetry_handler!([:musubi, :async, :stop])

      pid = start!()

      assert {:ok, _reply} = Server.command(pid, [], :stream_messages, %{"mode" => "exit"})
      await_task!()
      sync_server!(pid)

      assert_received {[:musubi, :async, :stop], ^ref, _measurements,
                       %{name: :messages, kind: :stream}}

      assert %AsyncResult{status: :failed, reason: {:exit, :boom}} =
               root_socket(pid).assigns.messages
    end

    test "cancel_async by name resolves the stream assign to failed" do
      telemetry_ref = attach_telemetry_handler!([:musubi, :async, :stop])

      pid = start!()

      assert {:ok, _reply} =
               Server.command(pid, [], :stream_messages_blocked, %{"tag" => "cancel"})

      task_pid = receive_task_pid!()
      sync_server!(pid)

      assert %AsyncResult{status: :loading} = root_socket(pid).assigns.messages

      ref = Process.monitor(task_pid)

      assert {:ok, _reply} =
               Server.command(pid, [], :cancel_messages, %{"reason" => "user_navigated"})

      assert_receive {:DOWN, ^ref, _, _, _}, 200
      sync_server!(pid)

      assert_received {[:musubi, :async, :stop], ^telemetry_ref, _measurements,
                       %{name: :messages, kind: :stream}}

      assert %AsyncResult{status: :failed, reason: {:exit, :user_navigated}} =
               root_socket(pid).assigns.messages

      assert %Stream.Slot{inserts: [], deletes: [], reset?: false} =
               root_stream_slot(pid, :messages)
    end
  end

  defp start!(store \\ AsyncStore) do
    pid =
      start_supervised!(
        {Server, {store, %{"page_id" => "p1", "test_pid" => self()}, %{transport_pid: self()}}}
      )

    flush_initial!(pid)
    pid
  end

  defp flush_initial!(pid) do
    sync_server!(pid)
    assert_received {:patch, %PatchEnvelope{base_version: 0, version: 1}}
  end

  defp root_socket(pid) do
    state = :sys.get_state(pid)
    %StoreTable.Entry{socket: socket} = StoreTable.get(state.store_table, [])
    socket
  end

  defp root_stream_slot(pid, name) do
    root_socket(pid).assigns[Stream.assigns_key()][name]
  end

  defp attach_telemetry_handler!(event) do
    ref = :telemetry_test.attach_event_handlers(self(), [event])
    on_exit(fn -> :telemetry.detach(ref) end)
    ref
  end
end
