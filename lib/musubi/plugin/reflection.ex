defmodule Musubi.Plugin.Reflection do
  @moduledoc false

  use TypedStructor.Plugin

  @spec __before_compile__(Macro.Env.t()) :: Macro.t()
  defmacro __before_compile__(env) do
    sections = collect_sections(env.module)
    plural_clauses = build_plural_clauses(sections)
    type_clauses = build_type_clauses(sections.fields)
    singular_clauses = build_all_singular_clauses(sections)
    validate_clauses = build_validate_clauses_for_kind(env.module, sections.fields)
    stream_runtime_clauses = build_stream_runtime_clauses(sections.streams)

    quote do
      unquote_splicing(plural_clauses)
      unquote_splicing(type_clauses)
      unquote_splicing(singular_clauses)
      unquote_splicing(validate_clauses)
      unquote_splicing(stream_runtime_clauses)
    end
  end

  defp collect_sections(module) do
    fields = Module.get_attribute(module, :__musubi_fields__) || []
    streams = Musubi.Plugin.StateField.stream_fields(fields)
    root? = Module.get_attribute(module, :__musubi_root__) || false

    commands =
      module |> Module.get_attribute(:__musubi_commands__) |> List.wrap() |> Enum.reverse()

    events =
      module |> Module.get_attribute(:__musubi_events__) |> List.wrap() |> Enum.reverse()

    attrs = module |> Module.get_attribute(:__musubi_attrs__) |> List.wrap() |> Enum.reverse()

    uploads =
      module
      |> Module.get_attribute(:__musubi_uploads__)
      |> List.wrap()
      |> Enum.map(fn {_name, %Musubi.Upload.Config{} = config, _file, _line} -> config end)

    %{
      fields: fields,
      streams: streams,
      commands: commands,
      events: events,
      attrs: attrs,
      uploads: uploads,
      root?: root?
    }
  end

  defp build_plural_clauses(sections) do
    %{
      fields: fields,
      streams: streams,
      commands: commands,
      events: events,
      attrs: attrs,
      uploads: uploads,
      root?: root?
    } = sections

    [
      quote(do: def(__musubi__(:fields), do: unquote(Macro.escape(fields)))),
      quote(do: def(__musubi__(:commands), do: unquote(Macro.escape(commands)))),
      quote(do: def(__musubi__(:events), do: unquote(Macro.escape(events)))),
      quote(do: def(__musubi__(:streams), do: unquote(Macro.escape(streams)))),
      quote(do: def(__musubi__(:attrs), do: unquote(Macro.escape(attrs)))),
      quote(do: def(__musubi__(:uploads), do: unquote(Macro.escape(uploads)))),
      quote(do: def(__musubi__(:root?), do: unquote(root?)))
    ]
  end

  defp build_type_clauses(fields) do
    for %{name: name, type: type} <- fields do
      quote do
        def __musubi__(:type, unquote(name)), do: unquote(Macro.escape(type))
      end
    end
  end

  defp build_all_singular_clauses(sections) do
    build_singular_clauses(:field, sections.fields, & &1.name) ++
      build_singular_clauses(:command, sections.commands, & &1.name) ++
      build_singular_clauses(:event, sections.events, & &1.name) ++
      build_singular_clauses(:stream, sections.streams, & &1.name) ++
      build_singular_clauses(:attr, sections.attrs, & &1.name) ++
      build_singular_clauses(:upload, sections.uploads, & &1.name) ++
      [
        quote(do: def(__musubi__(:field, _name), do: :error)),
        quote(do: def(__musubi__(:command, _name), do: :error)),
        quote(do: def(__musubi__(:event, _name), do: :error)),
        quote(do: def(__musubi__(:stream, _name), do: :error)),
        quote(do: def(__musubi__(:attr, _name), do: :error)),
        quote(do: def(__musubi__(:upload, _name), do: :error))
      ]
  end

  defp build_singular_clauses(kind, items, name_fun) do
    for item <- items do
      name = name_fun.(item)

      quote do
        def __musubi__(unquote(kind), unquote(name)),
          do: {:ok, unquote(Macro.escape(item))}
      end
    end
  end

  defp build_validate_clauses_for_kind(module, fields) do
    kind = Module.get_attribute(module, :__musubi_kind__) || :state
    build_validate_clauses(validate_callback_for(kind), fields)
  end

  defp validate_callback_for(:input), do: :__musubi_validate_input__
  defp validate_callback_for(_other), do: :__musubi_validate_state__

  # Compile-time bridge between the AST stored on `__musubi__(:streams)` and
  # the runtime callers in `Musubi.Stream`. The AST stays quoted on the
  # reflection key (per existing tests), but each stream slot also gets a
  # callable companion clause so runtime code never has to `Code.eval_quoted/3`.
  defp build_stream_runtime_clauses([]), do: []

  defp build_stream_runtime_clauses(streams) do
    item_key_clauses =
      for %{name: name, item_key: item_key_ast} <- streams do
        quote do
          @doc false
          def __musubi_stream_item_key__(unquote(name), item),
            do: unquote(item_key_ast).(item)
        end
      end

    config_clauses =
      for %{name: name, item_key: item_key_ast, limit: limit} <- streams do
        quote do
          @doc false
          def __musubi_stream_config__(unquote(name)),
            do: %{item_key: unquote(item_key_ast), limit: unquote(limit)}
        end
      end

    item_key_fallback =
      quote do
        @doc false
        def __musubi_stream_item_key__(name, _item) do
          raise ArgumentError,
                "no stream named #{inspect(name)} declared on #{inspect(__MODULE__)}"
        end
      end

    config_fallback =
      quote do
        @doc false
        def __musubi_stream_config__(name) do
          raise ArgumentError,
                "no stream named #{inspect(name)} declared on #{inspect(__MODULE__)}"
        end
      end

    Enum.concat([item_key_clauses, [item_key_fallback], config_clauses, [config_fallback]])
  end

  @impl TypedStructor.Plugin
  defmacro init(_opts), do: :ok

  defp build_validate_clauses(callback_name, fields) do
    expected_keys = Enum.map(fields, fn %{name: name} -> Atom.to_string(name) end)

    field_check_exprs = Enum.map(fields, &build_field_check_expr/1)

    map_clause =
      quote do
        @doc false
        @spec unquote(callback_name)(term()) ::
                :ok | {:error, [{String.t(), String.t()}]}
        def unquote(callback_name)(value) when is_map(value) do
          field_errors = List.flatten([unquote_splicing(field_check_exprs)])

          extra_errors =
            value
            |> Map.keys()
            |> Enum.reject(&(&1 in unquote(expected_keys)))
            |> Enum.map(fn extra_key ->
              {"$." <> to_string(extra_key), "unexpected field " <> inspect(extra_key)}
            end)

          case field_errors ++ extra_errors do
            [] -> :ok
            errors -> {:error, errors}
          end
        end
      end

    fallback_clause =
      quote do
        def unquote(callback_name)(value) do
          {:error, [{"$", "expected map, got: " <> inspect(value)}]}
        end
      end

    [map_clause, fallback_clause]
  end

  defp build_field_check_expr(%{name: name, type: type}) do
    key_string = Atom.to_string(name)
    path_string = "$." <> key_string
    type_label = Macro.to_string(type)
    escaped_type = Macro.escape(type)

    quote do
      Musubi.Plugin.Reflection.__check_field__(
        value,
        unquote(key_string),
        unquote(path_string),
        unquote(type_label),
        unquote(escaped_type),
        __MODULE__
      )
    end
  end

  @doc false
  @spec __check_field__(map(), String.t(), String.t(), String.t(), Macro.t(), module()) ::
          [{String.t(), String.t()}]
  def __check_field__(value, key, path, type_label, type_ast, module) do
    case Map.fetch(value, key) do
      :error ->
        [{path, "missing required field"}]

      {:ok, field_value} ->
        if Musubi.Type.valid?(field_value, type_ast, module) do
          []
        else
          [{path, "expected " <> type_label <> ", got: " <> inspect(field_value)}]
        end
    end
  end
end
