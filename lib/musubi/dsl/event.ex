defmodule Musubi.DSL.Event do
  @moduledoc false

  alias Musubi.Plugin.Normalize

  @reserved_event_prefix "musubi:"
  @allowed_field_opts [:doc]

  @doc """
  Declares a push event with no payload fields. Events may be declared in any
  store; each store's events are dispatched to that store's proxy on the client,
  keyed by `(store_id, name)` (BDR-0032).

  ## Examples

      defmodule ExampleStore do
        use Musubi.Store

        event :ping
      end
  """
  @spec event(atom()) :: Macro.t()
  defmacro event(name) when is_atom(name) do
    validate_event_name!(name)

    quote bind_quoted: [name: name] do
      @__musubi_events__ %{name: name, payload_fields: []}
    end
  end

  @doc """
  Declares a push event whose payload schema is described by `field` declarations
  inside the block.

  ## Examples

      event :toast do
        field :msg, String.t()
        field :level, atom(), doc: "severity"
      end
  """
  @spec event(atom(), do: Macro.t()) :: Macro.t()
  defmacro event(name, do: block) when is_atom(name) do
    validate_event_name!(name)

    quote do
      Module.delete_attribute(__MODULE__, :__musubi_event_payload_fields__)

      try do
        import Musubi.DSL.Event, only: [field: 2, field: 3]
        unquote(block)
      after
        :ok
      end

      payload_fields =
        __MODULE__
        |> Module.get_attribute(:__musubi_event_payload_fields__)
        |> List.wrap()
        |> Enum.reverse()
        |> Normalize.fields()

      Module.delete_attribute(__MODULE__, :__musubi_event_payload_fields__)

      @__musubi_events__ %{name: unquote(name), payload_fields: payload_fields}
    end
  end

  @doc """
  Declares a single payload field inside an `event :name do ... end` block.
  Supported opts: `:doc`.
  """
  @spec field(atom(), Macro.t()) :: Macro.t()
  @spec field(atom(), Macro.t(), keyword()) :: Macro.t()
  defmacro field(name, type, opts \\ []) when is_atom(name) and is_list(opts) do
    validate_field_opts!(opts)

    quote bind_quoted: [name: name, type: Macro.escape(type), opts: opts] do
      Module.put_attribute(
        __MODULE__,
        :__musubi_event_payload_fields__,
        Keyword.merge(opts, name: name, type: type)
      )
    end
  end

  @spec validate_event_name!(atom()) :: :ok
  defp validate_event_name!(name) do
    if String.starts_with?(Atom.to_string(name), @reserved_event_prefix) do
      raise ArgumentError,
            "event names using the reserved #{@reserved_event_prefix} prefix are not allowed"
    end

    :ok
  end

  @spec validate_field_opts!(keyword()) :: :ok
  defp validate_field_opts!(opts) do
    case Keyword.keys(opts) -- @allowed_field_opts do
      [] ->
        validate_doc_opt!(opts)

      extras ->
        raise CompileError,
          description:
            "unsupported event field opts: #{inspect(extras)}; only #{inspect(@allowed_field_opts)} are allowed"
    end
  end

  @spec validate_doc_opt!(keyword()) :: :ok
  defp validate_doc_opt!(opts) do
    case Keyword.fetch(opts, :doc) do
      :error ->
        :ok

      {:ok, value} when is_binary(value) ->
        :ok

      {:ok, value} ->
        raise CompileError,
          description: "event field `:doc` must be a binary, got: #{inspect(value)}"
    end
  end
end
