defmodule Musubi.Page.ServerDiffShortcutTest do
  # Runs synchronously: this module `refute_receive`s `[:musubi, :diff, :stop]`,
  # and `:telemetry_test.attach_event_handlers/2` keys handlers on the event
  # name globally (it cannot scope by the emitting pid the way the previous
  # hand-rolled handler did). Under `async: true`, sibling page-server modules
  # emitting the same event would race into the refute. `async: false` isolates
  # this negative assertion to this module's own server.
  use ExUnit.Case, async: false

  alias Musubi.Page.PatchEnvelope
  alias Musubi.Page.Server
  alias Musubi.Page.Server.State

  defmodule NoopStore do
    @moduledoc false

    use Musubi.Store

    state do
      field :ok, boolean()
    end

    command :ping

    @impl Musubi.Store
    def mount(socket), do: {:ok, Musubi.Socket.assign(socket, :ok, true)}

    @impl Musubi.Store
    def render(socket), do: %{ok: socket.assigns.ok}

    @impl Musubi.Store
    def handle_command(:ping, _payload, socket), do: {:noreply, socket}
  end

  setup do
    ref = :telemetry_test.attach_event_handlers(self(), [[:musubi, :diff, :stop]])
    on_exit(fn -> :telemetry.detach(ref) end)
    {:ok, ref: ref}
  end

  test "no-op render cycle skips diff telemetry when the wire root is unchanged", %{ref: ref} do
    pid = start_supervised!({Server, {NoopStore, %{}, %{transport_pid: self()}}})
    assert_receive {:patch, %PatchEnvelope{base_version: 0, version: 1}}

    assert {:ok, %{}} = Server.command(pid, [], :ping, %{})
    assert %State{version: 1, previous_wire_root: %{"ok" => true}} = :sys.get_state(pid)
    refute_receive {[:musubi, :diff, :stop], ^ref, _measurements, _metadata}, 100
    refute_receive {:patch, _envelope}, 100
  end
end
