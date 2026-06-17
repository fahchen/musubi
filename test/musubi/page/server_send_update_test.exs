defmodule Musubi.Page.ServerSendUpdateTest do
  @moduledoc """
  Verifies the server-authoritative targeting primitive `Musubi.send_update`
  (BDR-0030): an assigns map delivered to one addressed child store's `update/2`
  dirties only that subtree, re-renders it via the existing `subtree_dirty?`
  gate, short-circuits the clean root's `render/1` (BDR-0023), and ships one
  coalesced patch scoped to the child path. A missing target is a no-op plus
  `[:musubi, :send_update, :no_target]` telemetry.
  """

  use ExUnit.Case, async: true

  alias Musubi.Page.PatchEnvelope
  alias Musubi.Page.Server

  setup do
    Process.flag(:trap_exit, true)
    :ok
  end

  defmodule CommentsStore do
    @moduledoc false
    use Musubi.Store

    state do
      field :reload_count, integer()
      field :body, String.t()
    end

    @impl Musubi.Store
    def init(socket) do
      socket =
        socket
        |> Musubi.Socket.assign(:reload_count, 0)
        |> Musubi.Socket.assign(:body, "initial")

      {:ok, socket}
    end

    # Reacts to a `reload_token` assign by reloading: bumps a counter and swaps
    # the body. The token value itself is not rendered — it only triggers work.
    @impl Musubi.Store
    def update(%{reload_token: _ref}, socket) do
      next =
        socket
        |> Musubi.Socket.assign(:reload_count, socket.assigns.reload_count + 1)
        |> Musubi.Socket.assign(:body, "reloaded")

      {:ok, next}
    end

    def update(assigns, socket), do: {:ok, Musubi.Socket.assign(socket, assigns)}

    @impl Musubi.Store
    def render(socket) do
      %{reload_count: socket.assigns.reload_count, body: socket.assigns.body}
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defmodule RootStore do
    @moduledoc false
    use Musubi.Store

    state do
      field :title, String.t()
      field :comments, Musubi.Page.ServerSendUpdateTest.CommentsStore.state()
    end

    @impl Musubi.Store
    def mount(socket), do: {:ok, Musubi.Socket.assign(socket, :title, "Page")}

    @impl Musubi.Store
    def render(socket) do
      send(socket.assigns.test_pid, :root_render_called)

      %{
        title: socket.assigns.title,
        comments: child(Musubi.Page.ServerSendUpdateTest.CommentsStore, id: "comments")
      }
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  describe "send_update targets a mounted child store" do
    test "refreshes only the child subtree; root render/1 short-circuits" do
      pid = start!()

      # Drain the mount render and bootstrap envelope.
      assert_received :root_render_called
      assert_received {:patch, %PatchEnvelope{base_version: 0, version: 1}}

      assert :ok = Musubi.send_update(pid, ["comments"], %{reload_token: make_ref()})
      sync_server!(pid)

      assert_receive {:patch, %PatchEnvelope{ops: ops}}

      # Diff is scoped to the child subtree — every op is at or under "/comments"
      # (a whole-object replace at "/comments" is equally valid), none at "/title".
      assert Enum.all?(ops, fn %{path: path} -> String.starts_with?(path, "/comments") end)
      assert ops != []

      # The reload actually happened on the child socket.
      assert %{reload_count: 1, body: "reloaded"} = child_render(pid, ["comments"])

      # BDR-0023: the clean root did not re-run its render/1 this cycle.
      refute_received :root_render_called
    end

    test "is a no-op + telemetry for an unmounted store_id, page stays alive" do
      attach_no_target_handler!()
      pid = start!()

      assert_received :root_render_called
      assert_received {:patch, %PatchEnvelope{base_version: 0, version: 1}}

      assert :ok = Musubi.send_update(pid, ["does_not_exist"], %{reload_token: make_ref()})
      sync_server!(pid)

      assert_received {:telemetry, [:musubi, :send_update, :no_target], _measurements,
                       %{store_id: ["does_not_exist"]}}

      refute_receive {:patch, %PatchEnvelope{}}
      assert Process.alive?(pid)
    end
  end

  defmodule OverlapChild do
    @moduledoc false
    use Musubi.Store

    state do
      field :label, String.t()
    end

    @impl Musubi.Store
    def init(socket), do: {:ok, socket}

    @impl Musubi.Store
    def update(assigns, socket), do: {:ok, Musubi.Socket.assign(socket, assigns)}

    @impl Musubi.Store
    def render(socket), do: %{label: Map.get(socket.assigns, :label)}

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  defmodule OverlapRoot do
    @moduledoc false
    use Musubi.Store

    state do
      field :child, Musubi.Page.ServerSendUpdateTest.OverlapChild.state()
    end

    # `passed_label` is internal state, not part of the render contract, so it is
    # a plain assign rather than a declared `state` field.
    @impl Musubi.Store
    def mount(socket), do: {:ok, Musubi.Socket.assign(socket, :passed_label, "from_parent")}

    @impl Musubi.Store
    def render(socket) do
      %{
        child:
          child(Musubi.Page.ServerSendUpdateTest.OverlapChild,
            id: "c",
            label: socket.assigns.passed_label
          )
      }
    end

    @impl Musubi.Store
    def handle_command(_name, _payload, socket), do: {:noreply, socket}
  end

  describe "send_update on a parent-controlled key (BDR-0030 caveat)" do
    test "is reconciled back to the parent's value — net no-op" do
      pid =
        start_supervised!({Server, {OverlapRoot, %{"page_id" => "p1"}, %{transport_pid: self()}}})

      assert_received {:patch, %PatchEnvelope{base_version: 0, version: 1}}
      assert %{label: "from_parent"} = child_render(pid, ["c"])

      # `label` is a key the parent passes, so the next resolve reverts it.
      assert :ok = Musubi.send_update(pid, ["c"], %{label: "pushed"})
      sync_server!(pid)

      assert %{label: "from_parent"} = child_render(pid, ["c"])
      # Net diff is empty (from_parent -> pushed -> from_parent), so no envelope.
      refute_receive {:patch, %PatchEnvelope{}}
    end
  end

  describe "send_update can address the root (store_id [])" do
    test "runs the root update path and re-renders the tree" do
      pid = start!()

      assert_received :root_render_called
      assert_received {:patch, %PatchEnvelope{base_version: 0, version: 1}}

      assert :ok = Musubi.send_update(pid, [], %{title: "Renamed"})
      sync_server!(pid)

      assert_receive {:patch, %PatchEnvelope{ops: ops}}
      assert Enum.any?(ops, fn %{path: path} -> path == "/title" end)
      assert %{title: "Renamed"} = child_render(pid, [])
    end
  end

  defp start! do
    start_supervised!(
      {Server, {RootStore, %{"page_id" => "p1", test_pid: self()}, %{transport_pid: self()}}}
    )
  end

  defp sync_server!(pid), do: :sys.get_state(pid)

  defp child_render(pid, store_id) do
    {:ok, %{socket: socket, module: module}} = Server.peek(pid, store_id)
    module.render(socket)
  end

  defp attach_no_target_handler! do
    test_pid = self()
    handler_id = "send-update-no-target-#{System.unique_integer([:positive, :monotonic])}"

    :telemetry.attach(
      handler_id,
      [:musubi, :send_update, :no_target],
      fn event, measurements, metadata, _config ->
        send(test_pid, {:telemetry, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end
end
