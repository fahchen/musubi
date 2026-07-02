defmodule Musubi.Hooks.ValidateReplySchema do
  @moduledoc """
  Validates a command's reply against the addressed store's declared
  `reply_fields` schema.

  Attached to the `:after_command` lifecycle stage. The runtime stamps
  the addressed store module on each chain socket via the private key
  `Musubi.Hooks.ValidateCommandSchema.target_private_key/0` before the
  `:before_command` stage; that stamp remains in place across the
  handler and `:after_command` stages, so this hook reuses it.

  The reply is converted to wire form once via `Musubi.Wire.to_wire/1`
  — the same whole-map shape the client receives — and validation walks
  each declared reply field against that wire map, dispatching to
  `Musubi.Type.valid?/3` (which expects wire form). Any mismatch raises
  `ArgumentError`. The raw `reply` argument is left
  untouched so user `:after_command` hooks still observe it.

  Successful validation emits `[:musubi, :validate, :reply, :stop]`.

  ## Halt path

  Halts that short-circuit before `:after_command` (denial paths from
  `:before_command` and authz halts) bypass this hook entirely. Reply
  shapes returned via `{:halt, reply, socket}` from `:before_command`
  are NOT validated.
  """

  alias Musubi.Hooks.ValidateCommandSchema
  alias Musubi.Schema
  alias Musubi.Socket
  alias Musubi.Wire

  @typedoc "Field-level validation error: `{field_name, message}`."
  @type validation_error() :: {atom(), String.t()}

  @doc """
  `:after_command` hook entrypoint. Validates `reply` against the
  declared `reply_fields` for `command_name` on the addressed store
  module.
  """
  @spec after_command(atom(), map(), map(), Socket.t()) :: Musubi.Lifecycle.hook_result()
  def after_command(command_name, _payload, reply, %Socket{} = socket)
      when is_atom(command_name) and is_map(reply) do
    target_module = target_module(socket)

    case command_spec(target_module, command_name) do
      :error ->
        {:cont, socket}

      {:ok, %{reply_fields: fields}} ->
        validate_fields!(target_module, command_name, fields, Wire.to_wire(reply))
        emit_stop(target_module, command_name)
        {:cont, socket}
    end
  end

  @spec target_module(Socket.t()) :: module() | nil
  defp target_module(%Socket{} = socket) do
    Socket.get_private(socket, ValidateCommandSchema.target_private_key()) || socket.module
  end

  @spec command_spec(module() | nil, atom()) ::
          {:ok, %{name: atom(), reply_fields: list(), opts: keyword()}} | :error
  defp command_spec(nil, _command_name), do: :error

  defp command_spec(module, command_name) when is_atom(module) and is_atom(command_name) do
    if function_exported?(module, :__musubi__, 2) do
      module.__musubi__(:command, command_name)
    else
      :error
    end
  end

  @spec validate_fields!(module(), atom(), [map()], map()) :: :ok
  defp validate_fields!(module, command_name, fields, reply) do
    case Schema.collect_field_errors(fields, reply, module) do
      [] -> :ok
      errors -> raise ArgumentError, format_errors(module, command_name, errors)
    end
  end

  @spec format_errors(module(), atom(), [validation_error()]) :: String.t()
  defp format_errors(module, command_name, errors) do
    details =
      Enum.map_join(errors, "; ", fn {name, message} -> "#{name}: #{message}" end)

    "command reply validation failed for #{inspect(module)}.#{command_name}: #{details}"
  end

  @spec emit_stop(module(), atom()) :: :ok
  defp emit_stop(module, command_name) do
    Musubi.Telemetry.emit(
      [:musubi, :validate, :reply, :stop],
      %{count: 1},
      %{store_module: module, command: command_name}
    )
  end
end
