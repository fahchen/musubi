defmodule Musubi.Codegen.Manifest do
  @moduledoc false
  # Per-module compile-time manifest shared by every Musubi codegen target.
  #
  # The pattern mirrors `Phoenix.LiveView.ColocatedJS`: every Musubi `state do`
  # module gets its own subdirectory under `Mix.Project.build_path()`, and each
  # codegen Mix compiler (`Mix.Tasks.Compile.MusubiTs` and
  # `Mix.Tasks.Compile.MusubiRust`) discovers eligible modules by listing those
  # subdirectories — no beam scan, no `:application.get_key/2` walk.
  #
  # Layout:
  #
  #     <build>/musubi-codegen/<inspect(module)>/state.term
  #
  # Each `state.term` is `:erlang.term_to_binary(%{module, kind, fields,
  # commands, events, attrs, uploads, source})`. The payload is target-neutral
  # — raw Musubi reflection with quoted Elixir type ASTs, no renderer strings — so
  # one stamp feeds N renderers. The `module` atom inside the term is the
  # canonical reference; the directory name is purely organizational so
  # consumers can `mix clean`.
  #
  # `__after_compile__/2` is registered by `Musubi.Plugin.Codegen` on every
  # `state do` module and runs at the tail of that module's compilation,
  # serializing the data the codegen renderers need. Modules whose source
  # lives under `test/` are skipped so test fixtures don't pollute the bundle.

  @subdir "musubi-codegen"

  # Fields the DSL injects for runtime bookkeeping. They are not part of any
  # target's rendered shape, so the exclusion is target-agnostic policy and
  # lives here rather than in a renderer.
  @internal_fields [:__streams__]

  @type entry() ::
          {module(),
           %{
             kind: :state | :store,
             fields: list(),
             commands: list(),
             events: list(),
             attrs: list(),
             uploads: list()
           }}

  @doc false
  @spec __after_compile__(Macro.Env.t(), binary()) :: :ok
  def __after_compile__(env, _bytecode) do
    if eligible_source?(env.file) do
      data = Map.put(collect(env), :source, env.file)
      write_state!(env.module, data, target_dir())
    end

    :ok
  end

  @doc """
  Collects expanded `{kind, fields, commands}` reflection for `env.module`
  using `env`'s alias scope. The renderer consumes the resulting entry
  directly — every `{:__aliases__, _, _}` AST node is resolved to its
  fully-qualified form, so no further heuristic walk is needed at render
  time.
  """
  @spec collect(Macro.Env.t()) ::
          %{
            module: module(),
            kind: :state | :store,
            fields: list(),
            commands: list(),
            events: list(),
            attrs: list(),
            uploads: list()
          }
  def collect(%Macro.Env{module: module} = env) do
    %{
      module: module,
      kind: env_kind(env) || module_kind(module),
      fields: expand_field_aliases(List.wrap(module.__musubi__(:fields)), env),
      commands: expand_command_aliases(List.wrap(module.__musubi__(:commands)), env),
      events: expand_event_aliases(List.wrap(module.__musubi__(:events)), env),
      attrs: expand_field_aliases(List.wrap(module.__musubi__(:attrs)), env),
      uploads: read_uploads(module)
    }
  end

  defp read_uploads(module) do
    if function_exported?(module, :__musubi__, 1) do
      List.wrap(module.__musubi__(:uploads))
    else
      []
    end
  end

  defp env_kind(%Macro.Env{module: module}) when is_atom(module) do
    Module.get_attribute(module, :__musubi_kind__)
  rescue
    ArgumentError -> nil
  end

  # Test/manual-only counterpart to `__after_compile__/2`: it stamps the same
  # 8-key payload from module reflection alone, so — having no `Macro.Env` —
  # it performs **no** alias expansion. Entries it writes can therefore carry
  # single-segment `{:__aliases__, _, [:Child]}` nodes that the real compile
  # path never produces. Renderers must be written against the expanded form
  # `collect/1` emits; never against what `stamp/3` happens to persist.
  @doc false
  @spec stamp(module(), Path.t(), Path.t()) :: :ok
  def stamp(module, source_file, target) do
    Code.ensure_loaded?(module)

    data = %{
      module: module,
      source: source_file,
      kind: module_kind(module),
      fields: List.wrap(module.__musubi__(:fields)),
      commands: List.wrap(module.__musubi__(:commands)),
      events: List.wrap(module.__musubi__(:events)),
      attrs: List.wrap(module.__musubi__(:attrs)),
      uploads: read_uploads(module)
    }

    write_state!(module, data, target)
  end

  defp module_kind(module) do
    if function_exported?(module, :__musubi_kind__, 0) do
      module.__musubi_kind__()
    else
      :state
    end
  end

  defp write_state!(module, data, target) do
    File.mkdir_p!(module_dir(module, target))
    File.write!(state_path(module, target), :erlang.term_to_binary(data))
    :ok
  end

  # Replaces every `{:__aliases__, _, parts}` node inside a field/command
  # type AST with the fully-qualified alias atoms resolved against `env`'s
  # alias scope. Preserves anything `Macro.expand/2` can't resolve to an
  # atom (operators, locals, `unquote` artifacts, …) so the renderer's
  # fallback path still sees the original AST.
  defp expand_aliases(ast, env) do
    Macro.prewalk(ast, fn
      {:__aliases__, meta, _parts} = node ->
        case Macro.expand(node, env) do
          atom when is_atom(atom) ->
            {:__aliases__, meta, atom |> Module.split() |> Enum.map(&String.to_atom/1)}

          _other ->
            node
        end

      other ->
        other
    end)
  end

  defp expand_field_aliases(fields, env) do
    Enum.map(fields, fn field ->
      Map.update!(field, :type, &expand_aliases(&1, env))
    end)
  end

  defp expand_command_aliases(commands, env) do
    Enum.map(commands, fn command ->
      payload_fields = expand_field_list(command, :payload_fields, env)
      reply_fields = expand_field_list(command, :reply_fields, env)

      command
      |> Map.put(:payload_fields, payload_fields)
      |> Map.put(:reply_fields, reply_fields)
    end)
  end

  defp expand_event_aliases(events, env) do
    Enum.map(events, fn event ->
      Map.put(event, :payload_fields, expand_field_list(event, :payload_fields, env))
    end)
  end

  defp expand_field_list(command, key, env) do
    command
    |> Map.get(key, [])
    |> List.wrap()
    |> Enum.map(fn field -> Map.update!(field, :type, &expand_aliases(&1, env)) end)
  end

  @doc """
  Lists every stamped module's `{module, %{kind, fields, commands, events,
  attrs, uploads}}` entry under `target`. Skips entries whose module no longer
  loads (e.g. a `state do` module deleted from source between compiles — its dir
  lingers until `clean_outdated/1` or `mix clean`).
  """
  @spec list(Path.t()) :: [entry()]
  def list(target \\ target_dir()) do
    case File.ls(target) do
      {:ok, names} ->
        names
        |> Enum.flat_map(&read_entry(&1, target))
        |> Enum.sort_by(fn {module, _data} -> Module.split(module) end)

      _other ->
        []
    end
  end

  @doc """
  Drops DSL-internal bookkeeping fields from an entry's `:fields` list,
  leaving only the fields a codegen target should render.

  The exclusion list is target-agnostic policy, so every renderer calls this
  instead of re-deriving it.

      iex> [%{name: :__streams__, type: quote(do: map())}, %{name: :title, type: quote(do: String.t())}]
      ...> |> Musubi.Codegen.Manifest.renderable_fields()
      ...> |> Enum.map(& &1.name)
      [:title]
  """
  @spec renderable_fields([map()]) :: [map()]
  def renderable_fields(fields) when is_list(fields) do
    Enum.reject(fields, fn %{name: name} -> name in @internal_fields end)
  end

  @doc """
  Removes manifest subdirectories whose module is no longer loadable.
  """
  @spec clean_outdated(Path.t()) :: :ok
  def clean_outdated(target \\ target_dir()) do
    case File.ls(target) do
      {:ok, names} ->
        names
        |> Enum.filter(&File.dir?(Path.join(target, &1)))
        |> Enum.each(&maybe_remove_outdated(&1, target))

      _other ->
        :ok
    end

    :ok
  end

  defp maybe_remove_outdated(name, target) do
    dir = Path.join(target, name)

    if module_loadable?(name, target) do
      :ok
    else
      File.rm_rf!(dir)
    end
  end

  defp module_loadable?(name, target) do
    case read_module(name, target) do
      {:ok, module} -> Code.ensure_loaded?(module)
      :error -> false
    end
  end

  @doc false
  @spec target_dir() :: Path.t()
  def target_dir do
    # Tests scope an alternate target via
    # `Process.put(:__musubi_codegen_target_dir__, ...)` so they can drive
    # `__after_compile__/2` and a compiler's `list/0` / `clean_outdated/0`
    # against an isolated tmp dir without clobbering the real
    # `_build/<env>/musubi-codegen/` tree. Production callers leave the
    # process dict untouched and fall through to `Mix.Project.build_path()`.
    Process.get(:__musubi_codegen_target_dir__) || Path.join(Mix.Project.build_path(), @subdir)
  end

  defp module_dir(module, target), do: Path.join(target, inspect(module))
  defp state_path(module, target), do: Path.join(module_dir(module, target), "state.term")

  defp read_entry(name, target) do
    state_path = Path.join([target, name, "state.term"])

    with true <- File.dir?(Path.join(target, name)),
         {:ok, bin} <- File.read(state_path),
         %{module: module} = data <- safe_term(bin),
         true <- Code.ensure_loaded?(module) do
      kind = Map.get(data, :kind) || module_kind(module)
      uploads = List.wrap(Map.get(data, :uploads, []))
      events = List.wrap(Map.get(data, :events, []))
      attrs = List.wrap(Map.get(data, :attrs, []))

      [
        {module,
         %{
           kind: kind,
           fields: data.fields,
           commands: data.commands,
           events: events,
           attrs: attrs,
           uploads: uploads
         }}
      ]
    else
      _failure -> []
    end
  end

  defp read_module(name, target) do
    case File.read(Path.join([target, name, "state.term"])) do
      {:ok, bin} ->
        case safe_term(bin) do
          %{module: module} -> {:ok, module}
          _other -> :error
        end

      _error ->
        :error
    end
  end

  defp safe_term(bin) do
    :erlang.binary_to_term(bin)
  rescue
    ArgumentError -> nil
  end

  defp eligible_source?(file) when is_binary(file), do: not String.contains?(file, "/test/")

  defp eligible_source?(_other), do: true
end
