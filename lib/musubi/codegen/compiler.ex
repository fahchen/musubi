defmodule Musubi.Codegen.Compiler do
  @moduledoc false
  # The body both codegen Mix compilers share (`mix compile.musubi_ts`,
  # `mix compile.musubi_rust`). They differ only in the renderer they call, the
  # compiler name a drift diagnostic carries, and the output path — so the
  # drift/noop rules live here once.

  alias Musubi.Codegen.Manifest

  @type opts() :: [name: String.t(), renderer: module(), output_path: Path.t(), label: String.t()]

  @doc """
  Renders `opts[:renderer]`'s bundle for the current manifest and writes it to
  `opts[:output_path]`, or reports drift when `argv` carries `--check`.

  An empty manifest never overwrites an existing bundle: the run warns and
  returns `{:ok, []}` instead, so a wiped build directory cannot turn a
  committed bundle into an empty stub (and cannot fail a consumer's build
  either). `--check` is unaffected and still reports the difference as drift.
  """
  @spec run([String.t()], opts()) ::
          :noop | {:ok, []} | {:error, [Mix.Task.Compiler.Diagnostic.t()]}
  def run(argv, opts) do
    {parsed, _rest, _invalid} = OptionParser.parse(argv, strict: [check: :boolean])

    Manifest.clean_outdated()

    entries = Manifest.list()
    output_path = Keyword.fetch!(opts, :output_path)
    contents = Keyword.fetch!(opts, :renderer).render(entries)
    existing = File.read(output_path)

    cond do
      existing == {:ok, contents} ->
        :noop

      entries == [] and existing == {:error, :enoent} ->
        :noop

      parsed[:check] == true ->
        {:error, [drift_diagnostic(output_path, opts)]}

      # An empty manifest with a non-empty bundle on disk is a wiped
      # `_build/<env>/musubi-codegen/`, not a codebase that deleted every store:
      # stamping is owned by `@after_compile`, so nothing restamps until the
      # `state do` modules themselves recompile. Writing the empty render here
      # would replace a committed bundle with a prelude-only stub, so refuse and
      # keep the build green — a real recompile restamps and the next run writes
      # truthfully.
      entries == [] and match?({:ok, _bundle}, existing) ->
        warn_empty_manifest(output_path, opts)
        {:ok, []}

      true ->
        write_bundle!(contents, output_path, opts)
        {:ok, []}
    end
  end

  @doc "The manifest directory both compilers are invalidated by."
  @spec manifests() :: [Path.t()]
  def manifests, do: [Manifest.target_dir()]

  @doc "Drops the shared manifest."
  @spec clean() :: :ok
  def clean do
    _ignore = File.rm_rf(Manifest.target_dir())
    :ok
  end

  defp write_bundle!(contents, output_path, opts) do
    File.mkdir_p!(Path.dirname(output_path))
    File.write!(output_path, contents)
    Mix.shell().info("[#{Keyword.fetch!(opts, :name)}] wrote #{output_path}")
  end

  defp warn_empty_manifest(output_path, opts) do
    Mix.shell().error(
      "[#{Keyword.fetch!(opts, :name)}] kept #{output_path}: the codegen " <>
        "manifest under #{Manifest.target_dir()} is empty, so rendering it now " <>
        "would replace the existing bundle with an empty one. The manifest is " <>
        "stamped when a `state do` module compiles, so a wiped or cleaned build " <>
        "directory leaves it empty until those modules recompile. " <>
        "Run `mix compile --force` to restamp it."
    )
  end

  defp drift_diagnostic(output_path, opts) do
    name = Keyword.fetch!(opts, :name)

    %Mix.Task.Compiler.Diagnostic{
      compiler_name: name,
      file: output_path,
      message:
        "Musubi #{Keyword.fetch!(opts, :label)} bundle is out of date. " <>
          "Run `mix compile.#{name}` and commit the result.",
      position: nil,
      severity: :error
    }
  end
end
