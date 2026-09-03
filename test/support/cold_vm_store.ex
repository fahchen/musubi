defmodule Musubi.Test.Fixtures.ColdVMStore do
  @moduledoc """
  Fixture for the cold-VM regression test in `Musubi.Page.ServerTest`.

  Lives under `test/support/` so its `.beam` is written to disk —
  `:code.purge/1` + `:code.delete/1` only unloads from memory; the
  `Code.ensure_loaded?/1` guard inside `Musubi.Page.Server.module_exports?/3`
  must be able to re-read the `.beam` from disk to recover.
  """

  use Musubi.Store

  state do
    field :status, String.t()
  end

  @impl Musubi.Store
  def mount(socket), do: {:ok, Musubi.Socket.assign(socket, :status, "mounted")}

  @impl Musubi.Store
  def render(socket), do: %{status: socket.assigns.status}

  @impl Musubi.Store
  def handle_command(_name, _payload, socket), do: {:noreply, socket}
end

defmodule Musubi.Test.Fixtures.ColdVMParentStore do
  @moduledoc """
  Parent for the child-store half of the cold-VM regression test in
  `Musubi.Page.ServerTest`: it names `ColdVMStore` only through the module atom
  `child/2` carries, so nothing loads the child's `.beam` before
  `Musubi.Reconciler.init_store/1` probes it for an `init/1` callback.
  """

  use Musubi.Store, root: true

  state do
    field :cold, Musubi.Test.Fixtures.ColdVMStore.state()
  end

  @impl Musubi.Store
  def render(_socket), do: %{cold: child(Musubi.Test.Fixtures.ColdVMStore, id: "cold")}

  @impl Musubi.Store
  def handle_command(_name, _payload, socket), do: {:noreply, socket}
end
