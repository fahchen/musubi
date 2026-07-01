defmodule Musubi.LifecycleTest do
  use ExUnit.Case, async: true

  alias Musubi.Lifecycle
  alias Musubi.Socket

  defmodule RootStore do
  end

  setup do
    socket = %Socket{id: "", parent_path: [], module: RootStore, assigns: %{}, private: %{}}
    %{socket: socket}
  end

  test "attach_hook registers hooks across all supported stages", %{socket: socket} do
    hooked_socket =
      Enum.reduce(Lifecycle.stages(), socket, fn stage, acc ->
        Lifecycle.attach_hook(acc, {:id, stage}, stage, hook_fun_for(stage))
      end)

    hooks = hooked_socket.private[:hooks]

    assert Enum.sort(Map.keys(hooks)) == Enum.sort(Lifecycle.stages())

    Enum.each(Lifecycle.stages(), fn stage ->
      assert [%{id: {:id, ^stage}}] = hooks[stage]
    end)
  end

  test "attach_hook preserves attachment order", %{socket: socket} do
    socket =
      socket
      |> Lifecycle.attach_hook(:first, :before_command, fn _command_name,
                                                           _payload,
                                                           current_socket ->
        {:cont, current_socket}
      end)
      |> Lifecycle.attach_hook(:second, :before_command, fn _command_name,
                                                            _payload,
                                                            current_socket ->
        {:cont, current_socket}
      end)

    assert [%{id: :first}, %{id: :second}] = socket.private[:hooks][:before_command]
  end

  test "re-attaching the same id on the same stage raises", %{socket: socket} do
    socket =
      Lifecycle.attach_hook(socket, :audit, :before_command, fn _command_name,
                                                                _payload,
                                                                current_socket ->
        {:cont, current_socket}
      end)

    assert_raise ArgumentError, ~r/hook :audit already attached/, fn ->
      Lifecycle.attach_hook(socket, :audit, :before_command, fn _command_name,
                                                                _payload,
                                                                current_socket ->
        {:cont, current_socket}
      end)
    end
  end

  test "attach_hook raises when the stage arity does not match", %{socket: socket} do
    assert_raise ArgumentError,
                 ~r/expected fun arity 3 for stage :before_command, got arity 2/,
                 fn ->
                   Lifecycle.attach_hook(socket, :bad, :before_command, fn _payload,
                                                                           current_socket ->
                     {:cont, current_socket}
                   end)
                 end
  end

  test "detach_hook silently no-ops when absent", %{socket: socket} do
    assert Lifecycle.detach_hook(socket, :missing, :before_command) == socket
  end

  test "run_hooks iterates in order and returns the final socket", %{socket: socket} do
    socket =
      socket
      |> Lifecycle.attach_hook(:first, :before_command, fn command_name,
                                                           _payload,
                                                           current_socket ->
        {:cont,
         put_in(current_socket.assigns[:order], [
           command_name | current_socket.assigns[:order] || []
         ])}
      end)
      |> Lifecycle.attach_hook(:second, :before_command, fn _command_name,
                                                            _payload,
                                                            current_socket ->
        order = Enum.reverse(current_socket.assigns.order)
        next_order = List.insert_at(order, -1, :second)
        {:cont, put_in(current_socket.assigns[:order], next_order)}
      end)

    assert {:cont, final_socket} =
             Lifecycle.run_hooks(socket, :before_command, [:first, %{id: 1}], false)

    assert final_socket.assigns.order == [:first, :second]
  end

  test "run_hooks short-circuits on {:halt, socket}", %{socket: socket} do
    socket =
      socket
      |> Lifecycle.attach_hook(:halting, :before_command, fn _command_name,
                                                             _payload,
                                                             current_socket ->
        {:halt, put_in(current_socket.assigns[:halted], true)}
      end)
      |> Lifecycle.attach_hook(:later, :before_command, fn _command_name,
                                                           _payload,
                                                           current_socket ->
        {:cont, put_in(current_socket.assigns[:later], true)}
      end)

    assert {:halt, final_socket} =
             Lifecycle.run_hooks(socket, :before_command, [:rename, %{title: "Inbox"}], false)

    assert final_socket.assigns.halted
    refute Map.has_key?(final_socket.assigns, :later)
  end

  test "run_hooks returns {:halt, reply, socket} when halt payloads are allowed", %{
    socket: socket
  } do
    socket =
      Lifecycle.attach_hook(socket, :replying, :before_command, fn _command_name,
                                                                   _payload,
                                                                   current_socket ->
        {:halt, %{ok: false}, current_socket}
      end)

    assert {:halt, %{ok: false}, ^socket} =
             Lifecycle.run_hooks(socket, :before_command, [:rename, %{title: "Inbox"}], true)
  end

  test "run_hooks raises when a halt payload is returned but not allowed", %{socket: socket} do
    socket =
      Lifecycle.attach_hook(socket, :replying, :after_command, fn _command_name,
                                                                  _payload,
                                                                  _reply,
                                                                  current_socket ->
        {:halt, %{ok: false}, current_socket}
      end)

    assert_raise ArgumentError, ~r/halt payloads are only allowed/, fn ->
      Lifecycle.run_hooks(socket, :after_command, [:rename, %{title: "Inbox"}, %{}], false)
    end
  end

  test "stage_arity returns the LiveView-aligned arity per stage" do
    assert Lifecycle.stage_arity(:before_command) == 3
    assert Lifecycle.stage_arity(:after_command) == 4
    assert Lifecycle.stage_arity(:handle_async) == 3
    assert Lifecycle.stage_arity(:handle_info) == 2
    assert Lifecycle.stage_arity(:after_render) == 2
    assert Lifecycle.stage_arity(:after_serialize) == 2
    assert Lifecycle.stage_arity(:before_events) == 2
  end

  describe "run_event_hooks/2" do
    setup do
      socket = %Socket{id: "", parent_path: [], module: RootStore, assigns: %{}, private: %{}}
      %{socket: socket, events: [%{name: "a", payload: %{}}, %{name: "b", payload: %{}}]}
    end

    test "returns events unchanged with no hooks", %{socket: socket, events: events} do
      assert {^events, ^socket} = Lifecycle.run_event_hooks(socket, events)
    end

    test "threads a rewritten event list through the chain", %{socket: socket, events: events} do
      socket =
        socket
        |> Lifecycle.attach_hook(:drop_a, :before_events, fn evs, s ->
          {:cont, Enum.reject(evs, &(&1.name == "a")), s}
        end)
        |> Lifecycle.attach_hook(:tag, :before_events, fn evs, s ->
          {:cont, Enum.map(evs, &Map.put(&1, :tagged, true)), s}
        end)

      assert {[%{name: "b", tagged: true}], ^socket} = Lifecycle.run_event_hooks(socket, events)
    end

    test "halt stops the chain, keeping that hook's events", %{socket: socket, events: events} do
      socket =
        socket
        |> Lifecycle.attach_hook(:halt_empty, :before_events, fn _evs, s -> {:halt, [], s} end)
        |> Lifecycle.attach_hook(:never, :before_events, fn _evs, _s ->
          raise "should not run"
        end)

      assert {[], ^socket} = Lifecycle.run_event_hooks(socket, events)
    end

    test "raises on an invalid hook result", %{socket: socket, events: events} do
      socket =
        Lifecycle.attach_hook(socket, :bad, :before_events, fn _evs, s -> {:cont, s} end)

      assert_raise ArgumentError, ~r/invalid :before_events hook result/, fn ->
        Lifecycle.run_event_hooks(socket, events)
      end
    end
  end

  defp hook_fun_for(:before_command) do
    fn _command_name, _payload, current_socket -> {:cont, current_socket} end
  end

  defp hook_fun_for(:after_command) do
    fn _command_name, _payload, _reply, current_socket -> {:cont, current_socket} end
  end

  defp hook_fun_for(:handle_async) do
    fn _name, _async_result, current_socket -> {:cont, current_socket} end
  end

  defp hook_fun_for(:handle_info) do
    fn _message, current_socket -> {:cont, current_socket} end
  end

  defp hook_fun_for(:after_render) do
    fn _resolved_output, current_socket -> {:cont, current_socket} end
  end

  defp hook_fun_for(:after_serialize) do
    fn _wire_output, current_socket -> {:cont, current_socket} end
  end

  defp hook_fun_for(:before_events) do
    fn events, current_socket -> {:cont, events, current_socket} end
  end
end
