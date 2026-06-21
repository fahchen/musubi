defmodule Musubi.AsyncTest do
  use ExUnit.Case, async: true

  import Musubi.AsyncTestHelpers
  import ExUnit.CaptureIO

  alias Musubi.Async
  alias Musubi.AsyncResult
  alias Musubi.Socket

  # `Musubi.AsyncSupervisor` is started by `Musubi.Application` at app boot.

  describe "assign_async/3 single-key" do
    test "writes loading immediately and tracks the task" do
      socket =
        Async.assign_async(base_socket(), :profile, instant(fn -> {:ok, %{name: "ada"}} end))

      assert %AsyncResult{status: :loading, result: nil, reason: nil} = socket.assigns.profile

      tracking = Async.tracking(socket)
      assert %{profile: %{kind: :assign, keys: [:profile], ref: ref}} = tracking
      assert is_reference(ref)
    end

    test "applying ok result writes AsyncResult.ok" do
      socket =
        Async.assign_async(base_socket(), :profile, instant(fn -> {:ok, %{name: "ada"}} end))

      {classified, entry} = drain_task_result!(socket, :profile)

      socket = Async.apply_task_result(socket, :profile, entry, classified)

      assert %AsyncResult{status: :ok, result: %{name: "ada"}, reason: nil} =
               socket.assigns.profile

      assert Async.tracking(socket) == %{}
    end

    test "applying error result writes AsyncResult.failed with prior preserved" do
      socket =
        base_socket()
        |> Socket.assign(:profile, AsyncResult.ok("snapshot"))
        |> Async.assign_async(:profile, instant(fn -> {:error, :unauthorized} end))

      {classified, entry} = drain_task_result!(socket, :profile)
      socket = Async.apply_task_result(socket, :profile, entry, classified)

      assert %AsyncResult{status: :failed, result: "snapshot", reason: {:error, :unauthorized}} =
               socket.assigns.profile
    end

    test "raised exception classifies as failed {:exit, ...}" do
      socket = Async.assign_async(base_socket(), :profile, instant(fn -> raise "boom" end))
      {classified, entry} = drain_task_result!(socket, :profile)

      socket = Async.apply_task_result(socket, :profile, entry, classified)

      assert %AsyncResult{status: :failed, reason: {:exit, {:error, %RuntimeError{}, _stack}}} =
               socket.assigns.profile
    end

    test "thrown value classifies as failed {:exit, {{:nocatch, ...}, ...}}" do
      socket = Async.assign_async(base_socket(), :profile, instant(fn -> throw(:bail) end))
      {classified, entry} = drain_task_result!(socket, :profile)

      socket = Async.apply_task_result(socket, :profile, entry, classified)

      assert %AsyncResult{status: :failed, reason: {:exit, {{:nocatch, :bail}, _stack}}} =
               socket.assigns.profile
    end

    test "preserves prior result during reload (no :reset)" do
      prior = AsyncResult.ok("snapshot")

      socket =
        base_socket()
        |> Socket.assign(:profile, prior)
        |> Async.assign_async(:profile, instant(fn -> {:ok, "fresh"} end))

      assert %AsyncResult{status: :loading, result: "snapshot", reason: nil} =
               socket.assigns.profile
    end

    test ":reset re-emits loading without prior" do
      prior = AsyncResult.ok("snapshot")

      socket =
        base_socket()
        |> Socket.assign(:profile, prior)
        |> Async.assign_async(:profile, instant(fn -> {:ok, "fresh"} end), reset: true)

      assert %AsyncResult{status: :loading, result: nil, reason: nil} = socket.assigns.profile
    end
  end

  describe "assign_async/3 multi-key" do
    test "writes loading for every key and resolves atomically" do
      socket =
        Async.assign_async(
          base_socket(),
          [:user, :org],
          instant(fn -> {:ok, %{user: "u", org: "o"}} end)
        )

      assert %AsyncResult{status: :loading} = socket.assigns.user
      assert %AsyncResult{status: :loading} = socket.assigns.org

      {classified, entry} = drain_task_result!(socket, [:user, :org])

      socket = Async.apply_task_result(socket, [:user, :org], entry, classified)
      assert %AsyncResult{status: :ok, result: "u"} = socket.assigns.user
      assert %AsyncResult{status: :ok, result: "o"} = socket.assigns.org
    end

    test ":reset subset only resets the listed keys" do
      socket =
        base_socket()
        |> Socket.assign(:user, AsyncResult.ok("u_prior"))
        |> Socket.assign(:org, AsyncResult.ok("o_prior"))
        |> Async.assign_async(
          [:user, :org],
          instant(fn -> {:ok, %{user: "u", org: "o"}} end),
          reset: [:user]
        )

      assert %AsyncResult{status: :loading, result: nil} = socket.assigns.user
      assert %AsyncResult{status: :loading, result: "o_prior"} = socket.assigns.org
    end
  end

  describe "start_async/3" do
    test "writes nothing to assigns by default" do
      socket = Async.start_async(base_socket(), :warm_cache, instant(fn -> :ok end))

      refute Map.has_key?(socket.assigns, :warm_cache)
      assert %{warm_cache: %{kind: :start, keys: nil}} = Async.tracking(socket)
    end

    test "second call with same name silently overwrites tracking; old result lazy-discards" do
      socket = Async.start_async(base_socket(), :foo, instant(fn -> :a end))
      first_ref = Async.tracking(socket).foo.ref

      socket = Async.start_async(socket, :foo, instant(fn -> :b end))
      second_ref = Async.tracking(socket).foo.ref

      refute first_ref == second_ref
      assert is_reference(second_ref)
    end
  end

  describe "cancel_async/2,3 by name" do
    test "kills task and stamps cancel_reason; :DOWN drives failed write" do
      socket = Async.assign_async(base_socket(), :slow, blocking())

      pid = Async.tracking(socket).slow.pid
      ref = Async.tracking(socket).slow.ref

      _started = receive_task_pid!()

      socket = Async.cancel_async(socket, :slow, :user_navigated_away)

      # cancel does not pre-write; tracking still present with cancel_reason
      assert %{slow: %{cancel_reason: :user_navigated_away}} = Async.tracking(socket)

      # :DOWN arrives because we killed pid with that reason
      assert_receive {:DOWN, ^ref, :process, ^pid, :user_navigated_away}, 200

      {:ok, entry} = Async.fetch_tracking(socket, :slow)
      socket = Async.apply_task_down(socket, :slow, entry, :user_navigated_away)

      assert %AsyncResult{status: :failed, reason: {:exit, :user_navigated_away}} =
               socket.assigns.slow
    end
  end

  describe "cancel_async/3 by AsyncResult variant" do
    test "pre-writes failed and drops tracking before killing the task" do
      socket = Async.assign_async(base_socket(), :slow, blocking())

      _started = receive_task_pid!()
      ar = socket.assigns.slow

      socket = Async.cancel_async(socket, ar, :user_navigated_away)

      assert %AsyncResult{status: :failed, reason: {:exit, :user_navigated_away}} =
               socket.assigns.slow

      # tracking already dropped
      assert Async.tracking(socket) == %{}
    end
  end

  describe "stream_async/3 enforcement" do
    test "raises when no stream slot is declared" do
      socket = base_socket()

      assert_raise ArgumentError, ~r/stream_async :messages/, fn ->
        Async.stream_async(socket, :messages, fn -> {:ok, []} end)
      end
    end
  end

  describe "supervisor override" do
    test ":supervisor option is honored" do
      sup_pid = start_supervised!(Task.Supervisor)

      socket =
        Async.assign_async(base_socket(), :profile, instant(fn -> {:ok, :v} end),
          supervisor: sup_pid
        )

      entry = Async.tracking(socket).profile
      assert entry.supervisor == sup_pid
    end
  end

  describe "re-assign never kills the prior task (BDR-0031, LiveView parity)" do
    test "plain re-assign leaves the prior in-flight task running" do
      socket = Async.assign_async(base_socket(), :profile, blocking())
      prior_pid = receive_task_pid!()
      prior_ref = Async.tracking(socket).profile.ref

      socket = Async.assign_async(socket, :profile, instant(fn -> {:ok, "second"} end))

      assert Process.alive?(prior_pid)
      refute Async.tracking(socket).profile.ref == prior_ref

      Process.exit(prior_pid, :kill)
    end

    test ":reset re-assign leaves the prior in-flight task running" do
      socket = Async.assign_async(base_socket(), :profile, blocking())
      prior_pid = receive_task_pid!()

      socket =
        Async.assign_async(socket, :profile, instant(fn -> {:ok, "second"} end), reset: true)

      assert Process.alive?(prior_pid)
      assert %AsyncResult{status: :loading, result: nil} = socket.assigns.profile

      Process.exit(prior_pid, :kill)
    end
  end

  defp base_socket do
    %Socket{module: nil, parent_path: [], id: ""}
  end

  # Wraps a 0-arity fn so the test sees the spawned task pid; for tasks that
  # finish on their own (no test-driven release).
  defp instant(fun), do: instrument(self(), fun)

  # Task body that blocks forever — the test cancels it explicitly.
  defp blocking do
    instrument(self(), fn ->
      receive do
        {:never, _msg} -> :ok
      end
    end)
  end

  # Wait for one instrumented task to spawn and finish, then drain its
  # `{ref, classified}` result message that's queued in the test mailbox.
  defp drain_task_result!(socket, name) do
    await_task!()

    {:ok, entry} = Async.fetch_tracking(socket, name)
    ref = entry.ref
    assert_received {^ref, classified}
    {classified, entry}
  end

  describe "compile-time socket-capture warning (facade macros)" do
    test "warns when assign_async closes over `socket` via fn" do
      stderr =
        capture_io(:stderr, fn ->
          Code.compile_string("""
          defmodule Musubi.AsyncTest.AssignFnCapture do
            use Musubi.Store

            state do
              field :user, String.t()
            end

            @impl Musubi.Store
            def mount(socket), do: {:ok, socket}
            @impl Musubi.Store
            def render(socket), do: %{user: socket.assigns.user}
            @impl Musubi.Store
            def handle_command(_name, _payload, socket), do: {:noreply, socket}

            def fetch(socket) do
              assign_async(socket, :user, fn -> {:ok, socket.assigns.user} end)
            end
          end
          """)
        end)

      assert stderr =~ "assign_async/3,4: the task fn captures `socket`"
    end

    test "warns when start_async closes over `socket` via & capture" do
      stderr =
        capture_io(:stderr, fn ->
          Code.compile_string("""
          defmodule Musubi.AsyncTest.StartCaptureCapture do
            use Musubi.Store

            state do
              field :ok, boolean()
            end

            @impl Musubi.Store
            def mount(socket), do: {:ok, socket}
            @impl Musubi.Store
            def render(_socket), do: %{ok: true}
            @impl Musubi.Store
            def handle_command(_name, _payload, socket), do: {:noreply, socket}

            def warm(socket) do
              start_async(socket, :warm, &touch(socket, &1))
            end

            defp touch(_socket, _x), do: :ok
          end
          """)
        end)

      assert stderr =~ "start_async/3,4: the task fn captures `socket`"
    end

    test "warns when stream_async closes over `socket` via fn" do
      stderr =
        capture_io(:stderr, fn ->
          Code.compile_string("""
          defmodule Musubi.AsyncTest.StreamFnCapture do
            use Musubi.Store

            state do
              stream :messages, String.t()
            end

            @impl Musubi.Store
            def mount(socket), do: {:ok, socket}
            @impl Musubi.Store
            def render(_socket), do: %{messages: stream(:messages)}
            @impl Musubi.Store
            def handle_command(_name, _payload, socket), do: {:noreply, socket}

            def fetch(socket) do
              stream_async(socket, :messages, fn -> {:ok, [socket.assigns.user]} end)
            end
          end
          """)
        end)

      assert stderr =~ "stream_async/3,4: the task fn captures `socket`"
    end

    test "does not warn when fn closes over locals only" do
      stderr =
        capture_io(:stderr, fn ->
          Code.compile_string("""
          defmodule Musubi.AsyncTest.AssignNoCapture do
            use Musubi.Store

            state do
              field :user, String.t()
            end

            @impl Musubi.Store
            def mount(socket), do: {:ok, socket}
            @impl Musubi.Store
            def render(socket), do: %{user: socket.assigns.user}
            @impl Musubi.Store
            def handle_command(_name, _payload, socket), do: {:noreply, socket}

            def fetch(socket) do
              user_id = socket.assigns[:user_id]
              assign_async(socket, :user, fn -> {:ok, user_id} end)
            end
          end
          """)
        end)

      refute stderr =~ "captures `socket`"
    end

    test "does not warn when the task fn is built by a helper that takes socket" do
      stderr =
        capture_io(:stderr, fn ->
          Code.compile_string("""
          defmodule Musubi.AsyncTest.HelperFnNoCapture do
            use Musubi.Store

            state do
              field :user, String.t()
            end

            @impl Musubi.Store
            def mount(socket), do: {:ok, socket}
            @impl Musubi.Store
            def render(socket), do: %{user: socket.assigns.user}
            @impl Musubi.Store
            def handle_command(_name, _payload, socket), do: {:noreply, socket}

            def fetch(socket) do
              start_async(socket, :user, build_fun(socket))
            end

            defp build_fun(_socket), do: fn -> :ok end
          end
          """)
        end)

      refute stderr =~ "captures `socket`"
    end
  end
end
