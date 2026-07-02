defmodule Musubi.Transport.Channel do
  @moduledoc """
  Generic Phoenix Channel adapter that mounts any Musubi root store named in
  the channel module's allowlist.

  The public client API connects to a root by `{module, id}` (see
  `docs/client-contract.md`). `topic` is an internal transport detail:
  the client builds an opaque topic of the form `"musubi:<opaque_ref>"` and
  carries the requested `module`, `id`, and optional `params` in the channel
  join payload.

  ## Mounting in a Phoenix endpoint

      defmodule MyAppWeb.MusubiChannel do
        use Musubi.Transport.Channel,
          stores: [
            MyApp.Stores.CartPageStore,
            MyApp.Stores.InboxStore
          ]
      end

      defmodule MyAppWeb.UserSocket do
        use Phoenix.Socket

        channel "musubi:*", MyAppWeb.MusubiChannel

        def connect(_params, socket, _connect_info), do: {:ok, socket}
        def id(_socket), do: nil
      end

  ## Join payload

  Incoming join payload:

      %{
        "module" => "MyApp.Stores.CartPageStore",
        "id" => "cart:current-user",
        "params" => %{...optional...}
      }

  The channel rejects joins whose `module` is not in the configured allowlist.

  ## Lifecycle

  On `join/3` the adapter starts a fresh `Musubi.Page.Server` for the resolved
  root module + params and links it to the channel pid. On channel
  `terminate/2` the adapter unlinks the page server and stops it with the
  channel's terminate reason. Reconnect is recovery: each new
  join builds a fresh page server with `version: 1` and an initial
  `replace ""` envelope.

  ## Wire shape

  Incoming `"command"` payload:

      %{"store_id" => ["filters"], "name" => "change_query", "payload" => %{...}}

  `store_id` is the in-tree path-shaped locator for the addressed store node.
  Root is `[]`.

  Outgoing `"patch"` payload — `Musubi.Page.PatchEnvelope.to_wire/1`:

      %{
        "type" => "patch",
        "base_version" => 0,
        "version" => 1,
        "ops" => [...],
        "stream_ops" => [...]
      }

  ## Telemetry

    * `[:musubi, :channel, :join]` — `%{system_time: integer}`. Metadata:
      `module`, `id`, `topic`, `page_pid`.
    * `[:musubi, :channel, :terminate]` — `%{system_time: integer}`.
      Metadata: `module`, `id`, `topic`, `reason`, `page_pid`.
  """

  alias Musubi.Page.PatchEnvelope
  alias Musubi.Page.Server
  alias Musubi.Telemetry
  alias Musubi.Wire

  @doc false
  defmacro __using__(opts) do
    stores = Keyword.get(opts, :stores, [])

    quote bind_quoted: [stores: stores] do
      use Phoenix.Channel

      @__musubi_stores__ stores

      @doc false
      def __musubi_stores__, do: @__musubi_stores__

      @doc false
      @impl Phoenix.Channel
      def join(topic, params, socket) do
        Musubi.Transport.Channel.__join__(__MODULE__, topic, params, socket)
      end

      @doc false
      @impl Phoenix.Channel
      def handle_in("command", payload, socket) do
        Musubi.Transport.Channel.__handle_command__(payload, socket)
      end

      @doc false
      @impl Phoenix.Channel
      def handle_info({:patch, envelope}, socket) do
        Musubi.Transport.Channel.__handle_patch__(envelope, socket)
      end

      @doc false
      @impl Phoenix.Channel
      def terminate(reason, socket) do
        Musubi.Transport.Channel.__terminate__(reason, socket)
      end

      defoverridable join: 3, handle_in: 3, handle_info: 2, terminate: 2
    end
  end

  @doc false
  @spec __join__(module(), String.t(), map(), Phoenix.Socket.t()) ::
          {:ok, Phoenix.Socket.t()} | {:error, map()}
  def __join__(channel_module, topic, payload, %Phoenix.Socket{} = socket)
      when is_atom(channel_module) and is_binary(topic) and is_map(payload) do
    with {:ok, module_str} <- fetch_string(payload, "module"),
         {:ok, id} <- fetch_string(payload, "id"),
         {:ok, root_module} <- resolve_root(channel_module, module_str) do
      params = Map.get(payload, "params") || %{}
      join_params = Map.merge(params, %{"__musubi_root_id__" => id})

      {:ok, page_pid} =
        Server.start_link({root_module, join_params, %{transport_pid: self()}})

      Process.link(page_pid)

      Telemetry.emit(
        [:musubi, :channel, :join],
        %{system_time: System.system_time()},
        %{module: root_module, id: id, topic: topic, page_pid: page_pid}
      )

      {:ok,
       socket
       |> Phoenix.Socket.assign(:__musubi_page__, page_pid)
       |> Phoenix.Socket.assign(:__musubi_root__, root_module)
       |> Phoenix.Socket.assign(:__musubi_root_id__, id)
       |> Phoenix.Socket.assign(:__musubi_topic__, topic)}
    else
      {:error, reason} -> {:error, %{reason: reason}}
    end
  end

  @doc false
  @spec __handle_command__(map(), Phoenix.Socket.t()) ::
          {:reply, {:ok, map()} | {:error, map()}, Phoenix.Socket.t()}
  def __handle_command__(%{"name" => name} = payload, %Phoenix.Socket{} = socket)
      when is_binary(name) do
    page_pid = Map.fetch!(socket.assigns, :__musubi_page__)

    case Server.command_by_name(
           page_pid,
           Map.get(payload, "store_id", []),
           name,
           Map.get(payload, "payload", %{})
         ) do
      {:ok, reply} -> {:reply, {:ok, Wire.to_wire(reply)}, socket}
      {:error, reason} -> {:reply, {:error, %{reason: error_reason(reason)}}, socket}
    end
  end

  @doc false
  @spec __handle_patch__(PatchEnvelope.t(), Phoenix.Socket.t()) ::
          {:noreply, Phoenix.Socket.t()}
  def __handle_patch__(%PatchEnvelope{} = envelope, %Phoenix.Socket{} = socket) do
    Phoenix.Channel.push(socket, "patch", PatchEnvelope.to_wire(envelope))
    {:noreply, socket}
  end

  @doc false
  @spec __terminate__(term(), Phoenix.Socket.t()) :: :ok
  def __terminate__(reason, %Phoenix.Socket{} = socket) do
    page_pid = Map.get(socket.assigns, :__musubi_page__)
    topic = Map.get(socket.assigns, :__musubi_topic__)
    root_module = Map.get(socket.assigns, :__musubi_root__)
    id = Map.get(socket.assigns, :__musubi_root_id__)

    Telemetry.emit(
      [:musubi, :channel, :terminate],
      %{system_time: System.system_time()},
      %{module: root_module, id: id, topic: topic, reason: reason, page_pid: page_pid}
    )

    if is_pid(page_pid) and Process.alive?(page_pid) do
      Process.unlink(page_pid)
      GenServer.stop(page_pid, reason, 1_000)
    end

    :ok
  end

  defp fetch_string(payload, key) do
    case Map.get(payload, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _other -> {:error, "missing #{key}"}
    end
  end

  defp error_reason(:unknown_command), do: "unknown command"
  defp error_reason(:unknown_store), do: "unknown store"

  defp resolve_root(channel_module, module_str) do
    allowlist = channel_module.__musubi_stores__()

    matched =
      Enum.find(allowlist, fn store ->
        store |> Module.split() |> Enum.join(".") == module_str
      end)

    cond do
      matched == nil ->
        {:error, "module #{inspect(module_str)} is not in the channel allowlist"}

      not Code.ensure_loaded?(matched) ->
        {:error, "module #{inspect(module_str)} is not loadable"}

      not root_store?(matched) ->
        {:error, "module #{inspect(module_str)} is not a Musubi root store"}

      true ->
        {:ok, matched}
    end
  end

  defp root_store?(module) when is_atom(module) do
    function_exported?(module, :__musubi__, 1) and module.__musubi__(:root?)
  end
end
