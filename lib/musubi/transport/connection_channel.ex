defmodule Musubi.Transport.ConnectionChannel do
  @moduledoc """
  Phoenix Channel adapter for one Musubi root store.

  Each root store gets its own channel on topic `"musubi:connection:<root_id>"`.
  Join is the mount: `join/3` runs the socket module's
  `Musubi.Socket.handle_join/2`, composes the `root_id` from the join params,
  and starts exactly one root page server bound to this channel. Leaving the
  channel (client `leave()` or a transport drop) stops that root via
  `terminate/2`.

  Phoenix owns reconnect: on a dropped socket it automatically re-joins each
  channel, which re-runs `join/3` and rebuilds the root — the client drives the
  rest from the per-channel `join` reply. There is no separate `"mount"` /
  `"unmount"` message and no multiplexing of multiple roots over one channel.

  ## Telemetry

    * `[:musubi, :channel, :join]` — `%{system_time: integer}`. Metadata:
      `module`, `id`, `topic`, `page_pid`. `module` is the Musubi socket module,
      `id` is the composed `root_id`, and `page_pid` is the started root page
      server.
    * `[:musubi, :channel, :terminate]` — `%{system_time: integer}`.
      Metadata: `module`, `id`, `topic`, `reason`, `page_pid`, `root_count`.
      `root_count` is `1` when a root was mounted on this channel, else `0`.
  """

  use Phoenix.Channel

  alias Musubi.Page.PatchEnvelope
  alias Musubi.Page.Server
  alias Musubi.Socket
  alias Musubi.Telemetry
  alias Musubi.Transport.Socket, as: TransportSocket
  alias Musubi.Wire

  # Topic prefix for per-root connection channels; the suffix is the root id.
  @topic_prefix "musubi:connection:"

  # Phoenix socket assign containing the Musubi socket module.
  @socket_module_key :__musubi_socket_module__
  # Phoenix socket assign containing the joined Musubi socket context.
  @connection_socket_key :__musubi_connection_socket__
  # Phoenix socket assign containing this channel's single root runtime entry.
  @root_key :__musubi_root__
  # Phoenix socket assign containing the channel topic.
  @topic_key :__musubi_topic__

  @impl Phoenix.Channel
  @spec join(String.t(), map(), Phoenix.Socket.t()) ::
          {:ok, map(), Phoenix.Socket.t()} | {:error, map()}
  def join(@topic_prefix <> _suffix = topic, params, %Phoenix.Socket{} = socket)
      when is_map(params) do
    with {:ok, socket_module} <- fetch_socket_module(socket),
         {:ok, connect_socket} <- TransportSocket.fetch_connect_socket(socket),
         musubi_socket <- build_connection_socket(topic, connect_socket),
         {:ok, joined_socket} <- socket_module.handle_join(params, musubi_socket),
         {:ok, module_str} <- fetch_string(params, "module"),
         {:ok, caller_id} <- fetch_root_id(params),
         {:ok, root_params} <- fetch_params(params),
         root_id <- compose_root_id(module_str, caller_id),
         {:ok, root_module} <- fetch_declared_root(socket_module, module_str),
         :ok <- ensure_root_store(root_module),
         {:ok, page_pid} <- start_root_page(root_module, root_id, root_params, joined_socket, topic) do
      Telemetry.emit(
        [:musubi, :channel, :join],
        %{system_time: System.system_time()},
        %{module: socket_module, id: root_id, topic: topic, page_pid: page_pid}
      )

      {:ok, %{"root_id" => root_id},
       socket
       |> Phoenix.Socket.assign(@socket_module_key, socket_module)
       |> Phoenix.Socket.assign(@connection_socket_key, joined_socket)
       |> Phoenix.Socket.assign(@root_key, %{pid: page_pid, module: root_module, root_id: root_id})
       |> Phoenix.Socket.assign(@topic_key, topic)}
    else
      :error -> {:error, %{reason: "unauthorized"}}
      {:error, reason} -> {:error, %{reason: error_reason(reason)}}
    end
  end

  def join(_topic, _params, %Phoenix.Socket{}), do: {:error, %{reason: "unauthorized"}}

  @impl Phoenix.Channel
  @spec handle_in(String.t(), map(), Phoenix.Socket.t()) ::
          {:reply, {:ok, map()} | {:error, map()}, Phoenix.Socket.t()}
  def handle_in("command", payload, %Phoenix.Socket{} = socket) when is_map(payload) do
    with {:ok, name} <- fetch_string(payload, "name"),
         {:ok, page_pid} <- fetch_root_pid(socket),
         {:ok, reply} <-
           Server.command_by_name(
             page_pid,
             Map.get(payload, "store_id", []),
             name,
             Map.get(payload, "payload", %{})
           ) do
      {:reply, {:ok, Wire.to_wire(reply)}, socket}
    else
      {:error, reason} -> {:reply, {:error, %{reason: error_reason(reason)}}, socket}
    end
  end

  def handle_in("allow_upload", payload, %Phoenix.Socket{} = socket) when is_map(payload) do
    with {:ok, name_str} <- fetch_string(payload, "name"),
         {:ok, page_pid} <- fetch_root_pid(socket),
         store_id <- normalize_store_id(Map.get(payload, "store_id", [])),
         {:ok, name} <- resolve_upload_name_at(page_pid, store_id, name_str),
         entries <- Map.get(payload, "entries", []),
         endpoint <- socket.endpoint,
         {:ok, reply} <-
           Server.allow_upload(page_pid, store_id, name, List.wrap(entries), endpoint) do
      {:reply, {:ok, reply}, socket}
    else
      {:error, reason} -> {:reply, {:error, %{reason: error_reason(reason)}}, socket}
    end
  end

  def handle_in("cancel_upload", payload, %Phoenix.Socket{} = socket) when is_map(payload) do
    with {:ok, name_str} <- fetch_string(payload, "name"),
         {:ok, ref} <- fetch_string(payload, "ref"),
         {:ok, page_pid} <- fetch_root_pid(socket),
         store_id <- normalize_store_id(Map.get(payload, "store_id", [])),
         {:ok, name} <- resolve_upload_name_at(page_pid, store_id, name_str),
         :ok <- Server.cancel_upload(page_pid, store_id, name, ref) do
      {:reply, {:ok, %{}}, socket}
    else
      {:error, reason} -> {:reply, {:error, %{reason: error_reason(reason)}}, socket}
    end
  end

  def handle_in("upload_error", payload, %Phoenix.Socket{} = socket) when is_map(payload) do
    with {:ok, name_str} <- fetch_string(payload, "name"),
         {:ok, ref} <- fetch_string(payload, "ref"),
         {:ok, page_pid} <- fetch_root_pid(socket),
         store_id <- normalize_store_id(Map.get(payload, "store_id", [])),
         {:ok, name} <- resolve_upload_name_at(page_pid, store_id, name_str),
         error <- build_client_error(payload),
         :ok <- Server.upload_client_error(page_pid, store_id, name, ref, error) do
      {:reply, {:ok, %{}}, socket}
    else
      {:error, reason} -> {:reply, {:error, %{reason: error_reason(reason)}}, socket}
    end
  end

  def handle_in("upload_progress", payload, %Phoenix.Socket{} = socket) when is_map(payload) do
    with {:ok, name_str} <- fetch_string(payload, "name"),
         {:ok, ref} <- fetch_string(payload, "ref"),
         {:ok, page_pid} <- fetch_root_pid(socket),
         store_id <- normalize_store_id(Map.get(payload, "store_id", [])),
         {:ok, name} <- resolve_upload_name_at(page_pid, store_id, name_str),
         progress <- normalize_progress(payload),
         :ok <- Server.upload_progress(page_pid, store_id, name, ref, progress) do
      {:reply, {:ok, %{}}, socket}
    else
      {:error, reason} -> {:reply, {:error, %{reason: error_reason(reason)}}, socket}
    end
  end

  @impl Phoenix.Channel
  @spec handle_info({:musubi_root_patch, String.t(), PatchEnvelope.t()}, Phoenix.Socket.t()) ::
          {:noreply, Phoenix.Socket.t()}
  def handle_info({:musubi_root_patch, root_id, %PatchEnvelope{} = envelope}, socket)
      when is_binary(root_id) do
    payload =
      envelope
      |> PatchEnvelope.to_wire()
      |> Map.put("root_id", root_id)

    Phoenix.Channel.push(socket, "patch", payload)

    {:noreply, socket}
  end

  @impl Phoenix.Channel
  @spec terminate(term(), Phoenix.Socket.t()) :: :ok
  def terminate(reason, %Phoenix.Socket{} = socket) do
    root = Map.get(socket.assigns, @root_key)
    topic = Map.get(socket.assigns, @topic_key)
    socket_module = Map.get(socket.assigns, @socket_module_key)

    Telemetry.emit(
      [:musubi, :channel, :terminate],
      %{system_time: System.system_time()},
      %{
        module: socket_module,
        id: root && root.root_id,
        topic: topic,
        reason: reason,
        page_pid: root && root.pid,
        root_count: if(root, do: 1, else: 0)
      }
    )

    if root, do: stop_root(root.pid, reason)

    :ok
  end

  @spec fetch_socket_module(Phoenix.Socket.t()) :: {:ok, module()} | {:error, :missing_socket}
  defp fetch_socket_module(%Phoenix.Socket{handler: handler}) when is_atom(handler) do
    if function_exported?(handler, :__musubi_roots__, 0) do
      {:ok, handler}
    else
      {:error, :missing_socket}
    end
  end

  @spec build_connection_socket(String.t(), Socket.t()) :: Socket.t()
  defp build_connection_socket(topic, %Socket{} = connect_socket) when is_binary(topic) do
    %{connect_socket | topic: topic, transport_pid: self()}
  end

  @spec fetch_root_id(map()) :: {:ok, String.t()} | {:error, :missing_root_id}
  defp fetch_root_id(payload) when is_map(payload) do
    case Map.get(payload, "id") do
      value when is_binary(value) and value != "" -> {:ok, value}
      _other -> {:error, :missing_root_id}
    end
  end

  # The wire root_id composes the declared module string with the caller-supplied
  # id so two roots of different modules can share one caller id without
  # colliding. It is opaque to downstream consumers — they receive it back in the
  # join reply and round-trip it in the patch envelope.
  @spec compose_root_id(String.t(), String.t()) :: String.t()
  defp compose_root_id(module_str, caller_id)
       when is_binary(module_str) and is_binary(caller_id) do
    module_str <> ":" <> caller_id
  end

  @spec fetch_params(map()) :: {:ok, map()} | {:error, :invalid_params}
  defp fetch_params(payload) when is_map(payload) do
    case Map.get(payload, "params", %{}) do
      params when is_map(params) -> {:ok, params}
      _other -> {:error, :invalid_params}
    end
  end

  @spec fetch_declared_root(module(), String.t()) :: {:ok, module()} | {:error, :unknown_root}
  defp fetch_declared_root(socket_module, module_str)
       when is_atom(socket_module) and is_binary(module_str) do
    case Socket.fetch_root_by_module(socket_module, module_str) do
      {:ok, module} -> {:ok, module}
      :error -> {:error, :unknown_root}
    end
  end

  @spec ensure_root_store(module()) :: :ok | {:error, :not_root_store}
  defp ensure_root_store(module) when is_atom(module) do
    with true <- Code.ensure_loaded?(module),
         true <- function_exported?(module, :__musubi__, 1),
         true <- module.__musubi__(:root?) do
      :ok
    else
      _other -> {:error, :not_root_store}
    end
  end

  @spec start_root_page(module(), String.t(), map(), Socket.t(), String.t()) ::
          {:ok, pid()} | {:error, term()}
  defp start_root_page(root_module, root_id, params, %Socket{} = connection_socket, topic)
       when is_atom(root_module) and is_binary(root_id) and is_map(params) and is_binary(topic) do
    root_socket =
      Socket.inherit_context(connection_socket, %Socket{
        assigns: connection_socket.assigns,
        private: %{},
        topic: topic,
        transport_pid: self()
      })

    Server.start_link(
      {root_module, params, root_socket, %{transport_pid: self(), root_id: root_id}}
    )
  end

  @spec fetch_root_pid(Phoenix.Socket.t()) :: {:ok, pid()} | {:error, :unknown_root}
  defp fetch_root_pid(%Phoenix.Socket{} = socket) do
    case Map.get(socket.assigns, @root_key) do
      %{pid: pid} when is_pid(pid) -> {:ok, pid}
      _other -> {:error, :unknown_root}
    end
  end

  @spec stop_root(pid(), term()) :: :ok
  defp stop_root(pid, reason) when is_pid(pid) do
    # Page servers are started with `start_link/1`; unlink before controlled
    # stops so stopping the root does not take down the connection channel via
    # the link.
    Process.unlink(pid)

    if Process.alive?(pid) do
      GenServer.stop(pid, reason, 1_000)
    end

    :ok
  catch
    :exit, _reason -> :ok
  end

  @spec fetch_string(map(), String.t()) :: {:ok, String.t()} | {:error, :missing_field}
  defp fetch_string(payload, key) when is_map(payload) and is_binary(key) do
    case Map.get(payload, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _other -> {:error, :missing_field}
    end
  end

  @spec error_reason(term()) :: String.t()
  @spec resolve_upload_name_at(pid(), [String.t()], String.t()) ::
          {:ok, atom()} | {:error, :unknown_store | :unknown_upload}
  defp resolve_upload_name_at(page_pid, store_id, name_str)
       when is_pid(page_pid) and is_list(store_id) and is_binary(name_str) do
    case Server.peek(page_pid, store_id) do
      {:ok, %{module: module}} ->
        uploads = List.wrap(module.__musubi__(:uploads))

        case Enum.find(uploads, &(Atom.to_string(&1.name) == name_str)) do
          %{name: name} -> {:ok, name}
          nil -> {:error, :unknown_upload}
        end

      {:error, :not_mounted} ->
        {:error, :unknown_store}
    end
  end

  @spec normalize_store_id(term()) :: [String.t()]
  defp normalize_store_id(list) when is_list(list) do
    Enum.map(list, &to_string/1)
  end

  defp normalize_store_id(_other), do: []

  @spec normalize_progress(map()) :: non_neg_integer()
  defp normalize_progress(payload) when is_map(payload) do
    case Map.get(payload, "progress") do
      n when is_integer(n) and n >= 0 -> min(n, 100)
      _other -> 0
    end
  end

  # Wire payload shape: `%{"code" => "external_failed", "message" => "..."}`.
  # Unknown codes degrade to `:external_failed` so the server controls the
  # `Musubi.Upload.Error.code()` union and a malicious client cannot inject
  # arbitrary atoms.
  @spec build_client_error(map()) :: Musubi.Upload.Error.t()
  defp build_client_error(payload) when is_map(payload) do
    code = parse_client_error_code(Map.get(payload, "code"))

    case Map.get(payload, "message") do
      message when is_binary(message) and message != "" -> Musubi.Upload.Error.new(code, message)
      _other -> Musubi.Upload.Error.new(code)
    end
  end

  @allowed_client_error_codes ~w(external_failed)

  defp parse_client_error_code(code) when is_binary(code) do
    if code in @allowed_client_error_codes do
      String.to_existing_atom(code)
    else
      :external_failed
    end
  end

  defp parse_client_error_code(_other), do: :external_failed

  defp error_reason(:invalid_params), do: "params must be a map"
  defp error_reason(:missing_field), do: "missing required field"
  defp error_reason(:missing_root_id), do: "missing root id"
  defp error_reason(:missing_connection_socket), do: "missing Musubi connection socket"
  defp error_reason(:missing_socket), do: "missing Musubi socket"
  defp error_reason(:not_root_store), do: "declared store is not a root store"
  defp error_reason(:unauthorized), do: "unauthorized"
  defp error_reason(:unknown_command), do: "unknown command"
  defp error_reason(:unknown_root), do: "unknown root"
  defp error_reason(:unknown_store), do: "unknown store"
  defp error_reason(:unknown_upload), do: "unknown upload"
  defp error_reason(_other), do: "internal error"
end
