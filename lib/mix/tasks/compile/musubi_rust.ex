defmodule Mix.Tasks.Compile.MusubiRust do
  @shortdoc "Renders the Musubi Rust bundle for every `state do` module"

  @moduledoc """
  Mix compiler that walks every Musubi `state do` module exposed by the
  current Mix project and writes one Rust bundle file with `pub mod` blocks
  mirroring the Elixir module tree.

  ## Setup

  Add `:musubi_rust` to the project's compiler chain:

      def project do
        [
          ...,
          compilers: Mix.compilers() ++ [:musubi_rust]
        ]
      end

  Running `mix compile` then keeps the bundle in sync automatically. Invoke
  the compiler directly with `mix compile.musubi_rust` if you want to
  regenerate without a full project recompile. Either codegen target can be
  enabled alone; both read the same manifest.

  ## Options

    * `--check` — exit non-zero with a `Mix.Task.Compiler.Diagnostic` if the
      on-disk bundle differs from a freshly-rendered one. Wire this into a
      `precommit` / CI alias to gate drift:

          aliases: [
            precommit: ["compile --warnings-as-errors", "compile.musubi_rust --check", ...]
          ]

      `--check` must run *after* a compile, never after a `mix clean`: a clean
      empties the shared manifest, so a committed bundle would be reported as
      drift.

  ## Configuration

  Output path defaults to `priv/codegen/rust/musubi.rs`. Override per-app:

      config :musubi, :rust_codegen_output_path, "desktop/src/generated.rs"

  The generated prelude module name (`config :musubi,
  :rust_codegen_root_module`, default `"musubi"`) and the Rust path of the
  crate owning the shared runtime types (`config :musubi,
  :rust_codegen_runtime_path`, default `"musubi_client"`) are configurable too.

  ## Discovery

  Every Musubi `state do` module ends up with a manifest entry under
  `Mix.Project.build_path()/musubi-codegen/<inspect(module)>/state.term`,
  stamped at module-compile time by `Musubi.Plugin.Codegen`'s injected
  `@after_compile` callback. This compiler simply lists those entries —
  there is no beam scan or `:application.get_key/2` walk. Modules whose
  source lives under `test/` (e.g. `test/support/` fixtures) are skipped at
  stamp time so they never appear in the bundle.
  """

  use Mix.Task.Compiler

  alias Musubi.Codegen.Compiler
  alias Musubi.Codegen.Rust

  @default_output_path "priv/codegen/rust/musubi.rs"

  @impl Mix.Task.Compiler
  @spec run([String.t()]) ::
          :noop | {:ok, []} | {:error, [Mix.Task.Compiler.Diagnostic.t()]}
  def run(argv) do
    Compiler.run(argv,
      name: "musubi_rust",
      label: "Rust",
      renderer: Rust,
      output_path: configured_output_path()
    )
  end

  @impl Mix.Task.Compiler
  @spec manifests() :: [Path.t()]
  defdelegate manifests(), to: Compiler

  @impl Mix.Task.Compiler
  @spec clean() :: :ok
  defdelegate clean(), to: Compiler

  defp configured_output_path do
    Application.get_env(:musubi, :rust_codegen_output_path, @default_output_path)
  end
end
