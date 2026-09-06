defmodule Musubi.WireCapture.Socket do
  @moduledoc false
  # The `Musubi.Socket` both the connection-channel suite and
  # `mix musubi.capture_wire` connect through. Declares every fixture root in
  # `Musubi.WireCapture.Stores`; a root missing from this list is rejected at
  # join with `"declared store is not a root store"`.

  use Musubi.Socket,
    roots: [
      Musubi.WireCapture.Stores.AlphaRootStore,
      Musubi.WireCapture.Stores.AsyncRootStore,
      Musubi.WireCapture.Stores.BetaRootStore,
      Musubi.WireCapture.Stores.EventRootStore,
      Musubi.WireCapture.Stores.MetaRootStore,
      Musubi.WireCapture.Stores.StreamRootStore,
      Musubi.WireCapture.Stores.ToggleRootStore,
      Musubi.WireCapture.Stores.UploadRootStore
    ]

  @impl Musubi.Socket
  def handle_connect(%{"current_user" => current_user}, socket) do
    {:ok, Musubi.Socket.assign(socket, :current_user, current_user)}
  end

  @impl Musubi.Socket
  def handle_join(params, socket) do
    test_pid = socket |> Musubi.Socket.session() |> Map.fetch!("test_pid")
    send(test_pid, {:connection_join, params, socket.assigns.current_user})

    {:ok, socket}
  end
end

defmodule Musubi.WireCapture.Endpoint do
  @moduledoc false
  # Config lives in `config/test.exs` keyed by this module, per AGENTS.md: a
  # runtime `Application.put_env/3` would be a shared key another test could
  # read.

  use Phoenix.Endpoint, otp_app: :musubi

  socket("/musubi", Musubi.WireCapture.Socket, websocket: false, longpoll: false)
end
