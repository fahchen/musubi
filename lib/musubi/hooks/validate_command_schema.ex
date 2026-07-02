defmodule Musubi.Hooks.ValidateCommandSchema do
  @moduledoc """
  Validates a command's payload against the addressed store's declared
  `payload_fields` schema.

  Attached to the `:before_command` lifecycle stage. The runtime stamps
  the addressed store module on each chain socket via the private key
  `:__musubi_command_target__` before running `:before_command` hooks; the
  hook reads that target so a single root-attached hook can validate
  payloads for any descendant store.

  Validation walks each declared field, checks presence (string or atom
  key), and dispatches to `Musubi.Type.valid?/3`. Any mismatch raises
  `ArgumentError` (let-it-crash for malformed commands).

  Successful validation emits `[:musubi, :validate, :command, :stop]`.
  """

  alias Musubi.Socket
  alias Musubi.Type

  @target_private_key :__musubi_command_target__

  @typedoc "Field-level validation error: `{field_name, message}`."
  @type validation_error() :: {atom(), String.t()}

  @doc """
  Returns the `socket.private` key the runtime uses to communicate the
  addressed store module to this hook.

  ## Examples

      iex> Musubi.Hooks.ValidateCommandSchema.target_private_key()
      :__musubi_command_target__
  """
  @spec target_private_key() :: atom()
  def target_private_key, do: @target_private_key

  @doc """
  `:before_command` hook entrypoint. Validates `payload` against the
  declared schema for `command_name` on the addressed store module.

  ## Examples

      socket = %Musubi.Socket{module: MyApp.RootStore, private: %{}}
      Musubi.Hooks.ValidateCommandSchema.before_command(:noop, %{}, socket)
      #=> {:cont, socket}
  """
  @spec before_command(atom(), map(), Socket.t()) :: Musubi.Lifecycle.hook_result()
  def before_command(command_name, payload, %Socket{} = socket)
      when is_atom(command_name) and is_map(payload) do
    target_module = target_module(socket)

    case command_spec(target_module, command_name) do
      :error ->
        {:cont, socket}

      {:ok, %{payload_fields: fields}} ->
        validate_fields!(target_module, command_name, fields, payload)
        emit_stop(target_module, command_name)
        {:cont, socket}
    end
  end

  @spec target_module(Socket.t()) :: module() | nil
  defp target_module(%Socket{} = socket) do
    Socket.get_private(socket, @target_private_key) || socket.module
  end

  @spec command_spec(module() | nil, atom()) ::
          {:ok, %{name: atom(), payload_fields: list(), opts: keyword()}} | :error
  defp command_spec(nil, _command_name), do: :error

  defp command_spec(module, command_name) when is_atom(module) and is_atom(command_name) do
    if function_exported?(module, :__musubi__, 2) do
      module.__musubi__(:command, command_name)
    else
      :error
    end
  end

  @spec validate_fields!(module(), atom(), [map()], map()) :: :ok
  defp validate_fields!(module, command_name, fields, payload) do
    errors = Enum.reduce(fields, [], &collect_field_error(&1, payload, module, &2))

    case errors do
      [] ->
        :ok

      errors ->
        raise ArgumentError, format_errors(module, command_name, Enum.reverse(errors))
    end
  end

  @spec collect_field_error(map(), map(), module(), [validation_error()]) ::
          [validation_error()]
  defp collect_field_error(%{name: name, type: type_ast}, payload, module, acc) do
    case fetch_field(payload, name) do
      {:ok, value} ->
        if Type.valid?(value, type_ast, module) do
          acc
        else
          [{name, "expected #{Macro.to_string(type_ast)}, got: #{inspect(value)}"} | acc]
        end

      :error ->
        [{name, "missing required field"} | acc]
    end
  end

  @spec fetch_field(map(), atom()) :: {:ok, term()} | :error
  defp fetch_field(payload, name) when is_atom(name) do
    case Map.fetch(payload, name) do
      {:ok, value} -> {:ok, value}
      :error -> Map.fetch(payload, Atom.to_string(name))
    end
  end

  @spec format_errors(module(), atom(), [validation_error()]) :: String.t()
  defp format_errors(module, command_name, errors) do
    details =
      Enum.map_join(errors, "; ", fn {name, message} -> "#{name}: #{message}" end)

    "command payload validation failed for #{inspect(module)}.#{command_name}: #{details}"
  end

  @spec emit_stop(module(), atom()) :: :ok
  defp emit_stop(module, command_name) do
    Musubi.Telemetry.emit(
      [:musubi, :validate, :command, :stop],
      %{count: 1},
      %{store_module: module, command: command_name}
    )
  end
end
