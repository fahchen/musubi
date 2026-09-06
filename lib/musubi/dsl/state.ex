defmodule Musubi.DSL.State do
  @moduledoc false

  @doc """
  Defines a typed Musubi state block on a store or reusable state module.

  ## Examples

      defmodule ExampleState do
        use Musubi.State

        state do
          field :title, String.t()
        end
      end
  """
  @spec state(do: Macro.t()) :: Macro.t()
  defmacro state(do: block) do
    quote do
      @derive Musubi.Wire

      typed_structor definer: Musubi.Plugin.Definer do
        plugin(Musubi.Plugin.StateField)
        plugin(Musubi.Plugin.Reflection)
        plugin(Musubi.Plugin.TypeSpec)
        plugin(Musubi.Plugin.Codegen)

        import TypedStructor, except: [field: 2, field: 3]
        import Musubi.DSL.Field, only: [field: 2, field: 3]

        # Drop the facade's runtime `stream/3,4` and `stream_async/3,4` for
        # the `state do` block so they cannot collide with the field-declaration
        # macros below. Also drop the top-level `upload/2` so the override
        # below can raise a contextual compile error.
        import Musubi.Store,
          except: [stream: 3, stream: 4, stream_async: 3, stream_async: 4]

        import Musubi.DSL.Upload, only: []

        import Musubi.DSL.State,
          only: [
            stream: 2,
            stream: 3,
            stream_async: 2,
            stream_async: 3,
            upload: 2
          ]

        unquote(block)
      end
    end
  end

  @doc """
  Declares a top-level stream field inside `state do`.

  ## Examples

      defmodule ExampleStore do
        use Musubi.Store

        state do
          stream :messages, MessageState.t(), limit: -100
        end
      end
  """
  @spec stream(atom(), Macro.t()) :: Macro.t()
  @spec stream(atom(), Macro.t(), keyword()) :: Macro.t()
  defmacro stream(name, do: block) when is_atom(name) do
    type = Musubi.DSL.Schema.stream_type(Musubi.DSL.Schema.type_from_block(block), [])

    quote do
      Musubi.DSL.Field.field(
        unquote(name),
        unquote(type),
        []
      )
    end
  end

  defmacro stream(name, item_type) when is_atom(name) do
    type = Musubi.DSL.Schema.stream_type(item_type, [])

    quote do
      Musubi.DSL.Field.field(
        unquote(name),
        unquote(type),
        []
      )
    end
  end

  defmacro stream(name, opts, do: block) when is_atom(name) and is_list(opts) do
    type = Musubi.DSL.Schema.stream_type(Musubi.DSL.Schema.type_from_block(block), opts)

    quote do
      Musubi.DSL.Field.field(
        unquote(name),
        unquote(type),
        unquote(opts)
      )
    end
  end

  defmacro stream(name, item_type, opts) when is_atom(name) and is_list(opts) do
    type = Musubi.DSL.Schema.stream_type(item_type, opts)

    quote do
      Musubi.DSL.Field.field(
        unquote(name),
        unquote(type),
        unquote(opts)
      )
    end
  end

  @doc """
  Declares an async-wrapped stream field inside `state do`.

  ## Examples

      defmodule ExampleStore do
        use Musubi.Store

        state do
          stream_async :messages, MessageState.t(), limit: -100
        end
      end
  """
  @spec stream_async(atom(), Macro.t()) :: Macro.t()
  @spec stream_async(atom(), Macro.t(), keyword()) :: Macro.t()
  defmacro stream_async(name, do: block) when is_atom(name) do
    type = Musubi.DSL.Schema.async_stream_type(Musubi.DSL.Schema.type_from_block(block), [])

    quote do
      Musubi.DSL.Field.field(
        unquote(name),
        unquote(type),
        []
      )
    end
  end

  defmacro stream_async(name, item_type) when is_atom(name) do
    type = Musubi.DSL.Schema.async_stream_type(item_type, [])

    quote do
      Musubi.DSL.Field.field(
        unquote(name),
        unquote(type),
        []
      )
    end
  end

  defmacro stream_async(name, opts, do: block) when is_atom(name) and is_list(opts) do
    type = Musubi.DSL.Schema.async_stream_type(Musubi.DSL.Schema.type_from_block(block), opts)

    quote do
      Musubi.DSL.Field.field(
        unquote(name),
        unquote(type),
        unquote(opts)
      )
    end
  end

  defmacro stream_async(name, item_type, opts) when is_atom(name) and is_list(opts) do
    type = Musubi.DSL.Schema.async_stream_type(item_type, opts)

    quote do
      Musubi.DSL.Field.field(
        unquote(name),
        unquote(type),
        unquote(opts)
      )
    end
  end

  @doc """
  Raises at compile time. Uploads are declared at the top level of a
  store module, not inside `state do` / `field` / `stream` blocks.
  """
  @spec upload(atom(), Macro.t()) :: no_return()
  defmacro upload(name, _opts_or_block) when is_atom(name) do
    Musubi.DSL.Upload.__forbid_inside_state__(name, __CALLER__)
  end
end
