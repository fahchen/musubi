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
