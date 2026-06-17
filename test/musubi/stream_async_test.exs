defmodule Musubi.StreamAsyncTest do
  use ExUnit.Case, async: true

  import Musubi.AsyncTestHelpers

  alias Musubi.Async
  alias Musubi.AsyncResult
  alias Musubi.Socket
  alias Musubi.Stream

  defmodule MessagesStore do
    @moduledoc false
    use Musubi.Store

    state do
      stream_async :messages, %{id: String.t(), body: String.t()}
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, socket}
    @impl Musubi.Store
    def render(_socket) do
      %{messages: async_stream(:messages)}
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  describe "stream_async/3 happy path" do
    test "initializes the stream slot before task completion" do
      socket =
        Async.stream_async(
          base_socket(),
          :messages,
          instant(fn ->
            {:ok, [%{id: "1", body: "hi"}]}
          end)
        )

      assert %AsyncResult{status: :loading} = socket.assigns.messages
      assert %{messages: %Stream.Slot{}} = socket.assigns[Stream.assigns_key()]
      assert Stream.pending_ops(socket) == []
    end

    test "writes loading and seeds stream on success" do
      socket =
        Async.stream_async(
          base_socket(),
          :messages,
          instant(fn ->
            {:ok, [%{id: "1", body: "hi"}]}
          end)
        )

      assert %AsyncResult{status: :loading} = socket.assigns.messages

      {classified, entry} = drain_task_result!(socket, :messages)
      socket = Async.apply_task_result(socket, :messages, entry, classified)

      assert %AsyncResult{status: :ok, result: true, reason: nil} = socket.assigns.messages

      pending = Stream.pending_ops(socket)
      assert Enum.any?(pending, fn op -> op.op == "insert" and op.item_key == "messages-1" end)
    end

    test "stream opts pass through to stream/4" do
      socket =
        Async.stream_async(
          base_socket(),
          :messages,
          instant(fn ->
            {:ok, [%{id: "1", body: "hi"}], at: 0, limit: -100}
          end)
        )

      {classified, entry} = drain_task_result!(socket, :messages)
      socket = Async.apply_task_result(socket, :messages, entry, classified)

      assert [%{op: "insert", at: 0}] = Stream.pending_ops(socket)
    end
  end

  describe "stream_async/3 failure" do
    test "{:error, reason} writes failed and leaves stream untouched" do
      socket =
        Async.stream_async(base_socket(), :messages, instant(fn -> {:error, :rate_limited} end))

      {classified, entry} = drain_task_result!(socket, :messages)
      socket = Async.apply_task_result(socket, :messages, entry, classified)

      assert %AsyncResult{status: :failed, reason: {:error, :rate_limited}} =
               socket.assigns.messages

      assert Stream.pending_ops(socket) == []
    end

    test "invalid shape raises inside the task and surfaces as failed {:exit, ...}" do
      socket = Async.stream_async(base_socket(), :messages, instant(fn -> [%{id: "1"}] end))
      {classified, entry} = drain_task_result!(socket, :messages)

      socket = Async.apply_task_result(socket, :messages, entry, classified)

      assert %AsyncResult{status: :failed, reason: {:exit, _reason}} = socket.assigns.messages
    end
  end

  describe "stream_async/3 reset" do
    test ":reset re-emits loading without prior" do
      prior = AsyncResult.ok(true)

      socket =
        base_socket()
        |> Socket.assign(:messages, prior)
        |> Async.stream_async(:messages, instant(fn -> {:ok, []} end), reset: true)

      assert %AsyncResult{status: :loading, result: nil} = socket.assigns.messages
    end
  end

  defp base_socket do
    %Socket{module: MessagesStore, parent_path: [], id: ""}
  end

  defp instant(fun), do: instrument(self(), fun)

  defp drain_task_result!(socket, name) do
    await_task!()

    {:ok, entry} = Async.fetch_tracking(socket, name)
    ref = entry.ref
    assert_received {^ref, classified}
    {classified, entry}
  end
end
