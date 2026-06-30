defmodule Musubi.Schema do
  @moduledoc false
  # Shared declared-field validation against a wire-form map, used by both
  # command-reply validation (`Musubi.Hooks.ValidateReplySchema`) and push-event
  # validation (`Musubi.Event`). Callers format their own error message.

  alias Musubi.Type

  @doc """
  Returns `{field_name, message}` errors for each declared field whose value is
  missing from `wire_map` or fails its type. Errors are in declaration order;
  an empty list means the map satisfies the schema.
  """
  @spec collect_field_errors([map()], map(), module()) :: [{atom(), String.t()}]
  def collect_field_errors(fields, wire_map, module)
      when is_list(fields) and is_map(wire_map) and is_atom(module) do
    fields
    |> Enum.reduce([], &prepend_field_error(&1, wire_map, module, &2))
    |> Enum.reverse()
  end

  @spec prepend_field_error(map(), map(), module(), [{atom(), String.t()}]) ::
          [{atom(), String.t()}]
  defp prepend_field_error(%{name: name, type: type_ast}, wire_map, module, acc) do
    case Map.fetch(wire_map, to_string(name)) do
      {:ok, value} ->
        if Type.valid?(value, type_ast, module),
          do: acc,
          else: [{name, "expected #{Macro.to_string(type_ast)}, got: #{inspect(value)}"} | acc]

      :error ->
        [{name, "missing required field"} | acc]
    end
  end
end
