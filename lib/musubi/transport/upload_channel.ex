defmodule Musubi.Transport.UploadChannel do
  @moduledoc """
  Per-entry chunk sub-channel for Musubi uploads.

  Topic: `"musubi_upload:<entry_ref>"`. Joined with the Phoenix.Token
  issued during the `allow_upload` preflight.

  ## Statelessness

  The channel never consults a shared authorization table. Every
  authority signal it enforces (`max_file_size`, `chunk_size`,
  `client_size`, `chunk_timeout`, owning store pid, upload name) is
  recovered from the verified token payload on `join/3`.

  ## Lifecycle

    * `join/3` — verifies the token, confirms `store_pid` is alive,
      opens a `Plug.Upload.random_file!/1` temp file, and arms the
      `chunk_timeout` watchdog.
    * `handle_in("chunk", binary, socket)` — appends the binary frame
      to the temp file, enforces the per-token chunk and total size,
      resets the chunk-timeout watchdog, notifies the page server, and
      replies `%{progress: 0..100}`. The final chunk (when running
      total reaches `client_size`) is self-contained: the channel marks
      itself succeeded and stops cleanly without an extra event.
    * `terminate/2` — closes the temp file. The branch on the final
      flag (`:succeeded` / `:errored` / `:cancel`) decides whether to
      delete the temp file and emit `{op: cancel}`. A successful
      completion or an explicit error never produces a follow-up
      cancel op.
  """

  use Phoenix.Channel

  alias Musubi.Page.Server
  alias Musubi.Upload.Error
  alias Musubi.Upload.Token

  @topic_prefix "musubi_upload:"

  @assigns %{
    payload: :__musubi_upload_payload__,
    name: :__musubi_upload_name__,
    file_pid: :__musubi_upload_file_pid__,
    file_path: :__musubi_upload_file_path__,
    bytes_written: :__musubi_upload_bytes_written__,
    timeout_ref: :__musubi_upload_timeout_ref__,
    terminal_state: :__musubi_upload_terminal__
  }

  # Internal :timeout_ref values:
  # nil → no armed timer; reference → an outstanding Process.send_after ref.
  # Terminal states:
  #   nil        → no terminal state recorded (disconnect → cancel)
  #   :succeeded → upload reached client_size; do not cancel on terminate
  #   :errored   → an error op was already emitted; do not cancel on terminate

  @impl Phoenix.Channel
  def join(@topic_prefix <> entry_ref, %{"token" => token}, %Phoenix.Socket{} = socket)
      when is_binary(token) and entry_ref != "" do
    with {:ok, payload} <- Token.verify(socket.endpoint, token),
         :ok <- ensure_entry_ref(payload, entry_ref),
         :ok <- ensure_store_alive(payload),
         {:ok, file_path, file_pid} <- open_temp_file() do
      name_atom = String.to_existing_atom(payload.conf_ref)
      store_pid = payload.store_pid
      store_id = payload.store_id

      Server.register_upload_channel(store_pid, store_id, name_atom, entry_ref, self(), file_path)

      # Transfer Plug.Upload ownership to the page server pid so the temp
      # file outlives this channel process. Channel-side `terminate/2`
      # still removes the file explicitly on errored / cancel paths;
      # `give_away` only changes who Plug.Upload's monitor reaps when
      # the owning process dies.
      _give_away = Plug.Upload.give_away(file_path, store_pid, self())

      timeout_ref = arm_timeout(payload)

      socket =
        socket
        |> Phoenix.Socket.assign(@assigns.payload, payload)
        |> Phoenix.Socket.assign(@assigns.name, name_atom)
        |> Phoenix.Socket.assign(@assigns.file_pid, file_pid)
        |> Phoenix.Socket.assign(@assigns.file_path, file_path)
        |> Phoenix.Socket.assign(@assigns.bytes_written, 0)
        |> Phoenix.Socket.assign(@assigns.timeout_ref, timeout_ref)
        |> Phoenix.Socket.assign(@assigns.terminal_state, nil)

      {:ok, socket}
    else
      _error -> {:error, %{reason: "unauthorized"}}
    end
  end

  def join(_topic, _payload, _socket), do: {:error, %{reason: "unauthorized"}}

  # Both are the same chunk, so both are accepted — matching only the raw form
  # makes every real channel-mode upload crash the sub-channel on its first
  # frame.
  @impl Phoenix.Channel
  def handle_in("chunk", {:binary, binary}, %Phoenix.Socket{} = socket) when is_binary(binary),
    do: handle_in("chunk", binary, socket)

  def handle_in("chunk", binary, %Phoenix.Socket{} = socket) when is_binary(binary) do
    payload = socket.assigns[@assigns.payload]
    bytes = socket.assigns[@assigns.bytes_written]

    cond do
      byte_size(binary) > payload.chunk_size ->
        stop_with_error(socket, :chunk_too_large, "chunk too large")

      bytes + byte_size(binary) > payload.max_file_size ->
        stop_with_error(socket, :too_large, "upload too large")

      true ->
        write_chunk(socket, binary, bytes)
    end
  end

  defp write_chunk(%Phoenix.Socket{} = socket, binary, bytes) do
    payload = socket.assigns[@assigns.payload]
    file_pid = socket.assigns[@assigns.file_pid]

    case safe_binwrite(file_pid, binary) do
      :ok ->
        next_bytes = bytes + byte_size(binary)
        complete? = next_bytes >= payload.client_size

        Server.upload_channel_chunk(
          payload.store_pid,
          payload.store_id,
          socket.assigns[@assigns.name],
          payload.entry_ref,
          next_bytes,
          complete?
        )

        progress = compute_progress(next_bytes, payload.client_size)

        socket =
          socket
          |> cancel_timeout()
          |> Phoenix.Socket.assign(@assigns.bytes_written, next_bytes)

        if complete? do
          socket = Phoenix.Socket.assign(socket, @assigns.terminal_state, :succeeded)
          {:stop, :normal, {:ok, %{progress: 100}}, socket}
        else
          timeout_ref = arm_timeout(payload)
          socket = Phoenix.Socket.assign(socket, @assigns.timeout_ref, timeout_ref)
          {:reply, {:ok, %{progress: progress}}, socket}
        end

      {:error, _reason} ->
        stop_with_error(socket, :internal, "write failed")
    end
  end

  defp stop_with_error(%Phoenix.Socket{} = socket, code, reason_str) do
    payload = socket.assigns[@assigns.payload]
    name = socket.assigns[@assigns.name]
    notify_error(payload.store_pid, payload.store_id, name, payload.entry_ref, code)

    socket =
      socket
      |> cancel_timeout()
      |> Phoenix.Socket.assign(@assigns.terminal_state, :errored)

    {:stop, :normal, {:error, %{reason: reason_str}}, socket}
  end

  @impl Phoenix.Channel
  def handle_info({:musubi_upload_chunk_timeout, ref}, %Phoenix.Socket{} = socket) do
    case socket.assigns[@assigns.timeout_ref] do
      ^ref ->
        payload = socket.assigns[@assigns.payload]
        name = socket.assigns[@assigns.name]
        notify_error(payload.store_pid, payload.store_id, name, payload.entry_ref, :chunk_timeout)

        socket = Phoenix.Socket.assign(socket, @assigns.terminal_state, :errored)

        {:stop, :normal, socket}

      _stale ->
        {:noreply, socket}
    end
  end

  @impl Phoenix.Channel
  def terminate(_reason, %Phoenix.Socket{} = socket) do
    close_file(socket.assigns[@assigns.file_pid])

    case socket.assigns[@assigns.terminal_state] do
      :succeeded ->
        # File has been registered with the page server and now belongs
        # to the entry. Application code will delete it on consume.
        :ok

      :errored ->
        # An error op was already emitted; clean up the temp file so we
        # do not leak bytes but do not also emit a cancel.
        remove_file(socket.assigns[@assigns.file_path])
        :ok

      _other ->
        # Treat anything else (client disconnect / leave without
        # finishing) as a cancel. Page server is the source of truth on
        # whether the entry still exists.
        remove_file(socket.assigns[@assigns.file_path])
        emit_cancel(socket)
        :ok
    end
  end

  defp close_file(pid) when is_pid(pid) do
    if Process.alive?(pid), do: File.close(pid)
    :ok
  end

  defp close_file(_pid), do: :ok

  defp remove_file(path) when is_binary(path) do
    _result = File.rm(path)
    :ok
  end

  defp remove_file(_path), do: :ok

  defp emit_cancel(%Phoenix.Socket{} = socket) do
    payload = socket.assigns[@assigns.payload]
    name = socket.assigns[@assigns.name]

    cond do
      is_nil(payload) or is_nil(name) -> :ok
      not Process.alive?(payload.store_pid) -> :ok
      true -> Server.cancel_upload(payload.store_pid, payload.store_id, name, payload.entry_ref)
    end
  end

  defp ensure_entry_ref(%{entry_ref: ref}, ref), do: :ok
  defp ensure_entry_ref(_payload, _entry_ref), do: {:error, :mismatched_entry_ref}

  defp ensure_store_alive(%{store_pid: pid}) when is_pid(pid) do
    if Process.alive?(pid), do: :ok, else: {:error, :store_dead}
  end

  defp open_temp_file do
    path = Plug.Upload.random_file!("musubi_upload")
    {:ok, pid} = File.open(path, [:write, :binary])
    {:ok, path, pid}
  end

  defp notify_error(store_pid, store_id, name, entry_ref, code) do
    GenServer.cast(
      store_pid,
      {:upload_channel_error, store_id, name, entry_ref, Error.new(code)}
    )

    :ok
  end

  defp compute_progress(_bytes, 0), do: 0
  defp compute_progress(bytes, total) when bytes >= total, do: 100

  defp compute_progress(bytes, total)
       when is_integer(bytes) and is_integer(total) and total > 0 do
    div(bytes * 100, total)
  end

  defp arm_timeout(%{chunk_timeout: ms}) when is_integer(ms) and ms > 0 do
    ref = make_ref()
    Process.send_after(self(), {:musubi_upload_chunk_timeout, ref}, ms)
    ref
  end

  defp arm_timeout(_payload), do: nil

  defp cancel_timeout(%Phoenix.Socket{} = socket) do
    case socket.assigns[@assigns.timeout_ref] do
      ref when is_reference(ref) ->
        # Best-effort: the message may already be in the mailbox, but the
        # stale-ref guard in `handle_info` drops it harmlessly.
        Phoenix.Socket.assign(socket, @assigns.timeout_ref, nil)

      _other ->
        socket
    end
  end

  defp safe_binwrite(file_pid, binary) do
    IO.binwrite(file_pid, binary)
    :ok
  rescue
    _error -> {:error, :write_failed}
  end
end
