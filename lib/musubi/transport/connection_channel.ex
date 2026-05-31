defmodule Musubi.Transport.ConnectionChannel do
  @moduledoc """
  Phoenix Channel adapter for Musubi sockets with multiple root stores.

  The channel owns one joined Musubi socket and a dynamic set of root page
  servers. `join/3` runs the socket module's `Musubi.Socket.handle_join/2` once.
  Each client `"mount"` message starts one root store page server using the
  shared joined socket assigns and private connection context.

  ## Telemetry

    * `[:musubi, :channel, :join]` — `%{system_time: integer}`. Metadata:
      `module`, `id`, `topic`, `page_pid`. For this adapter `module` is the
      Musubi socket module, and `id`/`page_pid` are `nil` because roots mount
      later inside the joined connection.
    * `[:musubi, :channel, :terminate]` — `%{system_time: integer}`.
      Metadata: `module`, `id`, `topic`, `reason`, `page_pid`, `root_count`.
      `root_count` is the number of mounted root page servers the connection is
      stopping.
  """

  use Phoenix.Channel

  alias Musubi.Page.PatchEnvelope
  alias Musubi.Page.Server
  alias Musubi.Socket
  alias Musubi.Telemetry
  alias Musubi.Transport.Socket, as: TransportSocket
  alias Musubi.Wire

  # Phoenix socket assign containing the Musubi socket module.
  @socket_module_key :__musubi_socket_module__
  # Phoenix socket assign containing the joined Musubi socket context.
  @connection_socket_key :__musubi_connection_socket__
  # Phoenix socket assign containing mounted root runtime entries keyed by root id.
  @mounted_roots_key :__musubi_mounted_roots__
  # Phoenix socket assign containing the channel topic.
  @topic_key :__musubi_topic__

  @impl Phoenix.Channel
  @spec join(String.t(), map(), Phoenix.Socket.t()) ::
          {:ok, Phoenix.Socket.t()} | {:error, map()}
  def join(topic, params, %Phoenix.Socket{} = socket)
      when is_binary(topic) and is_map(params) do
    with {:ok, socket_module} <- fetch_socket_module(socket),
         {:ok, connect_socket} <- TransportSocket.fetch_connect_socket(socket),
         musubi_socket <- build_connection_socket(topic, connect_socket),
         {:ok, joined_socket} <- socket_module.handle_join(params, musubi_socket) do
      Telemetry.emit(
        [:musubi, :channel, :join],
        %{system_time: System.system_time()},
        %{module: socket_module, id: nil, topic: topic, page_pid: nil}
      )

      {:ok,
       socket
       |> Phoenix.Socket.assign(@socket_module_key, socket_module)
       |> Phoenix.Socket.assign(@connection_socket_key, joined_socket)
       |> Phoenix.Socket.assign(@mounted_roots_key, %{})
       |> Phoenix.Socket.assign(@topic_key, topic)}
    else
      :error -> {:error, %{reason: "unauthorized"}}
      {:error, reason} -> {:error, %{reason: error_reason(reason)}}
    end
  end

  @impl Phoenix.Channel
  @spec handle_in(String.t(), map(), Phoenix.Socket.t()) ::
          {:reply, {:ok, map()} | {:error, map()}, Phoenix.Socket.t()}
  def handle_in("mount", payload, %Phoenix.Socket{} = socket) when is_map(payload) do
    with {:ok, module_str} <- fetch_string(payload, "module"),
         {:ok, caller_id} <- fetch_root_id(payload),
         {:ok, params} <- fetch_params(payload),
         root_id <- compose_root_id(module_str, caller_id),
         {:ok, root_module} <- fetch_declared_root(socket, module_str),
         :ok <- ensure_root_store(root_module) do
      handle_mount(socket, root_id, root_module, params)
    else
      {:error, reason} -> {:reply, {:error, %{reason: error_reason(reason)}}, socket}
    end
  end

  def handle_in("command", payload, %Phoenix.Socket{} = socket) when is_map(payload) do
    with {:ok, name} <- fetch_string(payload, "name"),
         {:ok, root_id} <- fetch_string(payload, "root_id"),
         {:ok, page_pid} <- fetch_root_pid(socket, root_id),
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

  def handle_in("unmount", payload, %Phoenix.Socket{} = socket) when is_map(payload) do
    with {:ok, root_id} <- fetch_string(payload, "root_id"),
         {:ok, root_entry} <- fetch_root_entry(socket, root_id) do
      stop_root(root_entry.pid, {:shutdown, :unmounted})

      socket = update_mounted_roots(socket, &Map.delete(&1, root_id))

      {:reply, {:ok, %{}}, socket}
    else
      {:error, reason} -> {:reply, {:error, %{reason: error_reason(reason)}}, socket}
    end
  end

  def handle_in("allow_upload", payload, %Phoenix.Socket{} = socket) when is_map(payload) do
    with {:ok, root_id} <- fetch_string(payload, "root_id"),
         {:ok, name_str} <- fetch_string(payload, "name"),
         {:ok, page_pid} <- fetch_root_pid(socket, root_id),
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
    with {:ok, root_id} <- fetch_string(payload, "root_id"),
         {:ok, name_str} <- fetch_string(payload, "name"),
         {:ok, ref} <- fetch_string(payload, "ref"),
         {:ok, page_pid} <- fetch_root_pid(socket, root_id),
         store_id <- normalize_store_id(Map.get(payload, "store_id", [])),
         {:ok, name} <- resolve_upload_name_at(page_pid, store_id, name_str),
         :ok <- Server.cancel_upload(page_pid, store_id, name, ref) do
      {:reply, {:ok, %{}}, socket}
    else
      {:error, reason} -> {:reply, {:error, %{reason: error_reason(reason)}}, socket}
    end
  end

  def handle_in("upload_error", payload, %Phoenix.Socket{} = socket) when is_map(payload) do
    with {:ok, root_id} <- fetch_string(payload, "root_id"),
         {:ok, name_str} <- fetch_string(payload, "name"),
         {:ok, ref} <- fetch_string(payload, "ref"),
         {:ok, page_pid} <- fetch_root_pid(socket, root_id),
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
    with {:ok, root_id} <- fetch_string(payload, "root_id"),
         {:ok, name_str} <- fetch_string(payload, "name"),
         {:ok, ref} <- fetch_string(payload, "ref"),
         {:ok, page_pid} <- fetch_root_pid(socket, root_id),
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
    roots = mounted_roots(socket)
    topic = Map.get(socket.assigns, @topic_key)
    socket_module = Map.get(socket.assigns, @socket_module_key)

    Telemetry.emit(
      [:musubi, :channel, :terminate],
      %{system_time: System.system_time()},
      %{
        module: socket_module,
        id: nil,
        topic: topic,
        reason: reason,
        page_pid: nil,
        root_count: map_size(roots)
      }
    )

    Enum.each(roots, fn {_root_id, root_entry} ->
      stop_root(root_entry.pid, reason)
    end)

    :ok
  end

  # Dispatch a mount request to either the fresh-start or the alias path
  # depending on whether `mounted_roots[root_id]` already has an entry.
  @spec handle_mount(Phoenix.Socket.t(), String.t(), module(), map()) ::
          {:reply, {:ok, map()} | {:error, map()}, Phoenix.Socket.t()}
  defp handle_mount(%Phoenix.Socket{} = socket, root_id, root_module, params) do
    case Map.get(mounted_roots(socket), root_id) do
      nil -> start_fresh_root(socket, root_id, root_module, params)
      _existing -> reply_already_mounted(socket, root_id)
    end
  end

  @spec start_fresh_root(Phoenix.Socket.t(), String.t(), module(), map()) ::
          {:reply, {:ok, map()} | {:error, map()}, Phoenix.Socket.t()}
  defp start_fresh_root(%Phoenix.Socket{} = socket, root_id, root_module, params) do
    case start_root_page(root_module, root_id, params, socket) do
      {:ok, page_pid} ->
        entry = %{pid: page_pid, module: root_module}
        socket = update_mounted_roots(socket, &Map.put(&1, root_id, entry))
        {:reply, {:ok, %{"root_id" => root_id}}, socket}

      {:error, reason} ->
        {:reply, {:error, %{reason: error_reason(reason)}}, socket}
    end
  end

  # The (module, caller_id) pair is already mounted on this connection.
  # Reply with the canonical root_id so the client can alias to its local
  # RootConnection without spinning up a second server entry. The
  # `"already_mounted"` reason is a structured signal the client treats as
  # an alias hint, not as a hard error.
  @spec reply_already_mounted(Phoenix.Socket.t(), String.t()) ::
          {:reply, {:error, map()}, Phoenix.Socket.t()}
  defp reply_already_mounted(%Phoenix.Socket{} = socket, root_id) do
    {:reply, {:error, %{"reason" => "already_mounted", "root_id" => root_id}}, socket}
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

  # Connection-wide wire root_id composes the declared module string with the
  # caller-supplied id so two roots of different modules can share one caller
  # id on a single connection without colliding in `mounted_roots` or the
  # patch envelope. The composed string is opaque to downstream consumers —
  # they receive it back in the mount reply and round-trip it on subsequent
  # `unmount` / `command` / `patch` payloads.
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

  @spec fetch_declared_root(Phoenix.Socket.t(), String.t()) ::
          {:ok, module()} | {:error, :unknown_root}
  defp fetch_declared_root(%Phoenix.Socket{} = socket, module_str) when is_binary(module_str) do
    socket.assigns
    |> Map.fetch!(@socket_module_key)
    |> Socket.fetch_root_by_module(module_str)
    |> case do
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

  @spec start_root_page(module(), String.t(), map(), Phoenix.Socket.t()) ::
          {:ok, pid()} | {:error, :missing_connection_socket}
  defp start_root_page(root_module, root_id, params, %Phoenix.Socket{} = socket)
       when is_atom(root_module) and is_binary(root_id) and is_map(params) do
    case Map.fetch(socket.assigns, @connection_socket_key) do
      {:ok, %Socket{} = connection_socket} ->
        root_socket =
          Socket.inherit_context(connection_socket, %Socket{
            assigns: connection_socket.assigns,
            private: %{},
            topic: Map.get(socket.assigns, @topic_key),
            transport_pid: self()
          })

        Server.start_link(
          {root_module, params, root_socket, %{transport_pid: self(), root_id: root_id}}
        )

      :error ->
        {:error, :missing_connection_socket}
    end
  end

  @spec fetch_root_pid(Phoenix.Socket.t(), String.t()) :: {:ok, pid()} | {:error, :unknown_root}
  defp fetch_root_pid(%Phoenix.Socket{} = socket, root_id) when is_binary(root_id) do
    case fetch_root_entry(socket, root_id) do
      {:ok, %{pid: pid}} when is_pid(pid) -> {:ok, pid}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec fetch_root_entry(Phoenix.Socket.t(), String.t()) ::
          {:ok, %{pid: pid(), module: module()}} | {:error, :unknown_root}
  defp fetch_root_entry(%Phoenix.Socket{} = socket, root_id) when is_binary(root_id) do
    case Map.fetch(mounted_roots(socket), root_id) do
      {:ok, root_entry} -> {:ok, root_entry}
      :error -> {:error, :unknown_root}
    end
  end

  @spec update_mounted_roots(Phoenix.Socket.t(), (map() -> map())) :: Phoenix.Socket.t()
  defp update_mounted_roots(%Phoenix.Socket{} = socket, fun) when is_function(fun, 1) do
    Phoenix.Socket.assign(socket, @mounted_roots_key, fun.(mounted_roots(socket)))
  end

  @spec mounted_roots(Phoenix.Socket.t()) :: map()
  defp mounted_roots(%Phoenix.Socket{assigns: assigns}) do
    Map.get(assigns, @mounted_roots_key, %{})
  end

  @spec stop_root(pid(), term()) :: :ok
  defp stop_root(pid, reason) when is_pid(pid) do
    # Page servers are started with `start_link/1`; unlink before controlled
    # stops so unmounting one root does not terminate the connection channel.
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

  @spec error_reason(
          :invalid_params
          | :missing_field
          | :missing_root_id
          | :missing_connection_socket
          | :missing_socket
          | :not_root_store
          | :unauthorized
          | :unknown_command
          | :unknown_root
          | :unknown_store
        ) :: String.t()
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
end
