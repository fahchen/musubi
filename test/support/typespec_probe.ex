defmodule Musubi.TestSupport.TypespecProbeChild do
  @moduledoc false

  use Musubi.Store

  state do
    field :amount, integer()
  end

  @impl Musubi.Store
  def mount(socket), do: {:ok, socket}

  @impl Musubi.Store
  def render(_socket), do: %{amount: 0}

  @impl Musubi.Store
  def handle_command(_name, _payload, socket), do: {:noreply, socket}

  # Snapshot the compile-time env (alias scope, module name, file) so codegen
  # tests can drive `Musubi.Codegen.TypeScript.Manifest.collect/1` against the
  # same env the `:musubi_ts` compiler would see at consumer compile time.
  @captured_env __ENV__
  @doc false
  def __env__, do: @captured_env
end

defmodule Musubi.TestSupport.TypespecProbe do
  @moduledoc false

  use Musubi.Store

  alias Musubi.TestSupport.TypespecProbeChild

  state do
    stream(:messages, String.t())
    stream(:items, TypespecProbeChild.t(), item_key: &"item-#{&1.amount}", limit: -50)
    field :load_stream, Musubi.AsyncResult.of(stream(TypespecProbeChild.t()))
    field :profile, Musubi.AsyncResult.of(TypespecProbeChild.t())
    field :status, %{type: :active} | %{type: :paused, value: integer()}
    field :child, TypespecProbeChild.state()
    field :tags, list(String.t())
  end

  @impl Musubi.Store
  def mount(socket), do: {:ok, socket}

  @impl Musubi.Store
  def render(_socket),
    do: %{
      messages: stream(:messages),
      items: stream(:items),
      load_stream: nil,
      profile: nil,
      status: %{type: :active},
      child: %{amount: 0},
      tags: []
    }

  @impl Musubi.Store
  def handle_command(_name, _payload, socket), do: {:noreply, socket}

  @captured_env __ENV__
  @doc false
  def __env__, do: @captured_env
end

defmodule Musubi.TestSupport.TypespecProbeWithUpload do
  @moduledoc false

  use Musubi.Store, root: true

  state do
    field :avatar_url, String.t() | nil
  end

  upload(:avatar, accept: ~w(.png))
  upload(:cover, accept: ~w(.jpg))

  @impl Musubi.Store
  def render(socket), do: %{avatar_url: socket.assigns[:avatar_url]}

  @impl Musubi.Store
  def handle_command(_name, _payload, socket), do: {:noreply, socket}

  @captured_env __ENV__
  @doc false
  def __env__, do: @captured_env
end

defmodule Musubi.TestSupport.TypespecProbeWithCommand do
  @moduledoc false

  use Musubi.Store

  state do
    field :selected_id, String.t() | nil
  end

  command :select do
    payload do
      field :id, String.t()
    end
  end

  command :refresh

  @impl Musubi.Store
  def mount(socket), do: {:ok, socket}
  @impl Musubi.Store
  def render(socket), do: %{selected_id: Map.get(socket.assigns, :selected_id)}
  @impl Musubi.Store
  def handle_command(_name, _payload, socket), do: {:noreply, socket}

  @captured_env __ENV__
  @doc false
  def __env__, do: @captured_env
end

defmodule Musubi.TestSupport.TypespecProbeWithEvents do
  @moduledoc false

  use Musubi.Store, root: true

  state do
    field :title, String.t()
  end

  event(:ping)

  event :toast do
    field :msg, String.t()
    field :level, atom()
  end

  @impl Musubi.Store
  def mount(socket), do: {:ok, socket}
  @impl Musubi.Store
  def render(socket), do: %{title: Map.get(socket.assigns, :title, "")}
  @impl Musubi.Store
  def handle_command(_name, _payload, socket), do: {:noreply, socket}

  @captured_env __ENV__
  @doc false
  def __env__, do: @captured_env
end
