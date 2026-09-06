defmodule Musubi.WireCapture.Stores do
  @moduledoc false
  # Root stores driven by both `Musubi.Transport.ConnectionChannelTest` and
  # `mix musubi.capture_wire` (`docs/rust-client.md` §12 wants one mechanism,
  # not two). They live in `test/support` rather than inside the `.exs` suite
  # because a Mix task cannot reach modules defined in a test file, and in a
  # compiled module rather than in `lib/` because they must not ship in the Hex
  # tarball.
  #
  # Everything rendered here must be deterministic — no timestamps, pids or
  # random ids — or the fixture drift gate fails (`docs/rust-client.md` §12).
  #
  # Test-observation hooks (`send(test_pid, ...)`) read `"test_pid"` out of the
  # socket session, which the capture harness sets to its own pid and then
  # ignores.
end

defmodule Musubi.WireCapture.Stores.ChildStore do
  @moduledoc false

  use Musubi.Store

  state do
    field :label, String.t()
  end

  @impl Musubi.Store
  def init(socket) do
    socket
    |> Musubi.Socket.session()
    |> Map.fetch!("test_pid")
    |> send({:child_init, Musubi.Socket.session(socket), Musubi.Socket.connect_info(socket)})

    {:ok, socket}
  end

  @impl Musubi.Store
  def render(socket), do: %{label: socket.assigns.label}

  @impl Musubi.Store
  def handle_command(_name, _payload, socket), do: {:noreply, socket}
end

defmodule Musubi.WireCapture.Stores.AlphaRootStore do
  @moduledoc false

  use Musubi.Store, root: true

  alias Musubi.WireCapture.Stores.ChildStore

  attr :room_id, String.t(), required: true

  state do
    field :room_id, String.t()
    field :current_user, String.t()
    field :child, ChildStore.state()
  end

  command :rename do
    payload do
      field :room_id, String.t()
    end
  end

  @impl Musubi.Store
  def mount(params, socket) do
    session = Musubi.Socket.session(socket)
    test_pid = Map.fetch!(session, "test_pid")

    send(test_pid, {:alpha_mount, self(), params, socket.assigns.current_user})

    {:ok, Musubi.Socket.assign(socket, :room_id, Map.fetch!(params, "room_id"))}
  end

  @impl Musubi.Store
  def init(socket) do
    test_pid = Map.fetch!(Musubi.Socket.session(socket), "test_pid")
    send(test_pid, {:alpha_init, socket.assigns.room_id})
    {:ok, socket}
  end

  @impl Musubi.Store
  def render(socket) do
    %{
      room_id: socket.assigns.room_id,
      current_user: socket.assigns.current_user,
      child: child(ChildStore, id: "child", label: socket.assigns.room_id)
    }
  end

  @impl Musubi.Store
  def handle_command(:rename, %{"room_id" => room_id}, socket) do
    {:noreply, Musubi.Socket.assign(socket, :room_id, room_id)}
  end
end

defmodule Musubi.WireCapture.Stores.BetaRootStore do
  @moduledoc false

  use Musubi.Store, root: true

  state do
    field :label, String.t()
    field :current_user, String.t()
  end

  command :rename do
    payload do
      field :label, String.t()
    end
  end

  command :echo do
    payload do
      field :label, String.t()
    end

    reply do
      field :ok, boolean()
      field :label, String.t()
    end
  end

  @impl Musubi.Store
  def mount(params, socket) do
    test_pid = Map.fetch!(Musubi.Socket.session(socket), "test_pid")
    send(test_pid, {:beta_mount, self(), params, socket.assigns.current_user})
    {:ok, Musubi.Socket.assign(socket, :label, Map.fetch!(params, "label"))}
  end

  @impl Musubi.Store
  def render(socket) do
    %{label: socket.assigns.label, current_user: socket.assigns.current_user}
  end

  @impl Musubi.Store
  def handle_command(:rename, %{"label" => label}, socket) do
    {:noreply, Musubi.Socket.assign(socket, :label, label)}
  end

  # `{:reply, _, socket}` with no assign change: the reply lands, the idle
  # cycle emits nothing (BDR-0018).
  def handle_command(:echo, %{"label" => label}, socket) do
    {:reply, %{"ok" => true, "label" => label}, socket}
  end
end

defmodule Musubi.WireCapture.Stores.MetaRootStore do
  @moduledoc false
  # Drives the `add` and `remove` JSON Patch op kinds: a `map()` field whose
  # keys appear and disappear between renders.

  use Musubi.Store, root: true

  state do
    field :meta, map()
  end

  command :put do
    payload do
      field :key, String.t()
      field :value, String.t()
    end
  end

  command :drop do
    payload do
      field :key, String.t()
    end
  end

  @impl Musubi.Store
  def mount(_params, socket), do: {:ok, Musubi.Socket.assign(socket, :meta, %{"a" => "1"})}

  @impl Musubi.Store
  def render(socket), do: %{meta: socket.assigns.meta}

  @impl Musubi.Store
  def handle_command(:put, %{"key" => key, "value" => value}, socket) do
    {:noreply, Musubi.Socket.update(socket, :meta, &Map.put(&1, key, value))}
  end

  def handle_command(:drop, %{"key" => key}, socket) do
    {:noreply, Musubi.Socket.update(socket, :meta, &Map.delete(&1, key))}
  end
end

defmodule Musubi.WireCapture.Stores.StreamRootStore do
  @moduledoc false
  # Every stream op the client has to materialize: `reset`, `insert` at the
  # head / tail / an explicit index, `delete`, and both `limit` signs plus
  # `limit: 0`.

  use Musubi.Store, root: true

  state do
    field :title, String.t()
    stream(:items, map(), item_key: &"item-#{&1["id"]}")
  end

  command :seed do
    payload do
      field :count, integer()
    end
  end

  command :insert do
    payload do
      field :id, String.t()
      field :at, integer()
      field :limit, integer() | nil
    end
  end

  command :delete do
    payload do
      field :id, String.t()
    end
  end

  @impl Musubi.Store
  def mount(_params, socket) do
    {:ok, Musubi.Socket.assign(socket, :title, "stream")}
  end

  @impl Musubi.Store
  def render(socket), do: %{title: socket.assigns.title, items: stream(:items)}

  @impl Musubi.Store
  def handle_command(:seed, %{"count" => count}, socket) do
    items = for index <- 1..count//1, do: %{"id" => Integer.to_string(index)}

    {:noreply, stream(socket, :items, items, reset: true)}
  end

  def handle_command(:insert, %{"id" => id, "at" => at} = payload, socket) do
    {:noreply,
     stream_insert(socket, :items, %{"id" => id},
       at: at,
       limit: Map.get(payload, "limit")
     )}
  end

  def handle_command(:delete, %{"id" => id}, socket) do
    {:noreply, stream_delete(socket, :items, %{"id" => id})}
  end
end

defmodule Musubi.WireCapture.Stores.AsyncRootStore do
  @moduledoc false
  # `loading -> ok` and `loading -> failed` over `assign_async/3`. The task fun
  # is pure and returns immediately, so the capture only has to wait for the
  # two envelopes, not for a clock.

  use Musubi.Store, root: true

  state do
    field :profile, Musubi.AsyncResult.of(map())
  end

  command :load do
    payload do
      field :outcome, String.t()
    end
  end

  @impl Musubi.Store
  def mount(_params, socket) do
    {:ok, Musubi.Socket.assign(socket, :profile, Musubi.AsyncResult.loading())}
  end

  @impl Musubi.Store
  def render(socket), do: %{profile: socket.assigns.profile}

  @impl Musubi.Store
  def handle_command(:load, %{"outcome" => "ok"}, socket) do
    {:noreply, assign_async(socket, :profile, fn -> {:ok, %{"name" => "ada"}} end)}
  end

  def handle_command(:load, %{"outcome" => "failed"}, socket) do
    {:noreply, assign_async(socket, :profile, fn -> {:error, :nope} end)}
  end
end

defmodule Musubi.WireCapture.Stores.EventRootStore do
  @moduledoc false
  # The event-only cycle (BDR-0032 + BDR-0018): a command that pushes an event
  # and changes no assigns, so the envelope carries `events` and no `ops`.

  use Musubi.Store, root: true

  state do
    field :title, String.t()
  end

  event :toast do
    field :msg, String.t()
  end

  command :notify do
    payload do
      field :msg, String.t()
    end
  end

  @impl Musubi.Store
  def mount(_params, socket), do: {:ok, Musubi.Socket.assign(socket, :title, "events")}

  @impl Musubi.Store
  def render(socket), do: %{title: socket.assigns.title}

  @impl Musubi.Store
  def handle_command(:notify, %{"msg" => msg}, socket) do
    {:noreply, push_event(socket, :toast, %{msg: msg})}
  end
end

defmodule Musubi.WireCapture.Stores.ToggleRootStore do
  @moduledoc false
  # Child mount and unmount (BDR-0011 prune): the child is rendered only while
  # `:show?` is true, so unmounting emits a `replace` of the parent key and
  # prunes the child's registry entry.

  use Musubi.Store, root: true

  alias Musubi.WireCapture.Stores.ChildStore

  state do
    field :panel, ChildStore.state() | nil
  end

  command :toggle do
    payload do
      field :show, boolean()
    end
  end

  @impl Musubi.Store
  def mount(_params, socket), do: {:ok, Musubi.Socket.assign(socket, :show?, false)}

  @impl Musubi.Store
  def render(socket) do
    panel = if socket.assigns.show?, do: child(ChildStore, id: "panel", label: "panel")

    %{panel: panel}
  end

  @impl Musubi.Store
  def handle_command(:toggle, %{"show" => show}, socket) do
    {:noreply, Musubi.Socket.assign(socket, :show?, show)}
  end
end

defmodule Musubi.WireCapture.Stores.UploadRootStore do
  @moduledoc false
  # The upload control plane reachable over the *connection* channel:
  # `allow_upload` preflight, `upload_progress` in external-uploader mode, and
  # `cancel_upload`. Channel-mode chunk transfer rides a separate topic with
  # binary frames, which the §12 JSON frame schema cannot express, so it is not
  # captured here.

  use Musubi.Store, root: true

  state do
    field :title, String.t()
  end

  upload(:avatar, accept: ~w(.png), max_entries: 2, max_file_size: 1_000_000, chunk_size: 1_024)

  @impl Musubi.Store
  def mount(_params, socket), do: {:ok, Musubi.Socket.assign(socket, :title, "uploads")}

  @impl Musubi.Store
  def render(socket), do: %{title: socket.assigns.title}

  @impl Musubi.Store
  def handle_command(_name, _payload, socket), do: {:noreply, socket}

  # Defining this callback is what switches the preflight to external mode
  # (`Musubi.Upload.Preflight.uses_external?/2`). External mode is what the
  # capture uses, because channel mode mints a signed token whose bytes change
  # on every run — a fixture that carried one could never survive
  # `git diff --exit-code`.
  @impl Musubi.Store
  def upload_external(:avatar, entry, socket) do
    {:ok, %{uploader: "s3", url: "https://uploads.example.test/" <> entry.client_name}, socket}
  end
end
