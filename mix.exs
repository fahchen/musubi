defmodule Musubi.MixProject do
  use Mix.Project

  @version "0.9.0"
  @source_url "https://github.com/fahchen/musubi"

  def project do
    [
      app: :musubi,
      version: @version,
      description: description(),
      elixir: "~> 1.18",
      elixirc_paths: elixirc_paths(Mix.env()),
      consolidate_protocols: Mix.env() != :test,
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      docs: docs(),
      package: package(),
      dialyzer: [
        plt_local_path: "priv/plts/musubi.plt",
        plt_core_path: "priv/plts/core.plt",
        plt_add_apps: [:ex_unit, :mix],
        ignore_warnings: ".dialyzer_ignore.exs"
      ],
      aliases: aliases()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Musubi.Application, []}
    ]
  end

  def cli do
    [
      preferred_envs: [precommit: :test]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:telemetry, "~> 0.4 or ~> 1.0"},
      {:typed_structor, "~> 0.6.1"},
      {:jsonpatch, "~> 2.2"},
      {:phoenix, System.get_env("PHOENIX_VERSION", ">= 1.5.3 and < 2.0.0")},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.40", only: :dev, runtime: false},
      {:benchee, "~> 1.3", only: :dev, runtime: false}
    ]
  end

  defp description do
    "Server-authoritative, page-scoped runtime library for Elixir/Phoenix applications."
  end

  # Hex package contents. Includes the JS source under `packages/*/src`
  # so that consuming Phoenix apps can reference them via
  # `file:../deps/musubi/packages/<name>` from their JS package.json. The
  # consumer's bundler (Vite, esbuild) transpiles `.ts`/`.tsx` on demand.
  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url},
      files: ~w(
          lib
          mix.exs
          README.md
          CHANGELOG.md
          guides
          packages/client/src
          packages/client/package.json
          packages/react/src
          packages/react/package.json
        )
    ]
  end

  defp docs do
    public_modules = docs_modules()

    [
      main: "readme",
      source_url: @source_url,
      source_ref: "v#{@version}",
      filter_modules: fn module, _metadata -> module in public_modules end,
      skip_undefined_reference_warnings_on: &skip_doc_reference?/1,
      skip_code_autolink_to: &skip_doc_reference?/1,
      extras: [
        "README.md",
        "CHANGELOG.md",
        "guides/getting-started.md",
        "guides/phoenix-setup.md",
        "guides/client-and-react.md",
        "guides/uploads.md",
        "guides/testing.md",
        "docs/client-contract.md",
        "docs/persistence-pattern.md"
      ],
      groups_for_extras: [
        Tutorials: [
          "guides/getting-started.md",
          "guides/phoenix-setup.md",
          "guides/client-and-react.md",
          "guides/uploads.md",
          "guides/testing.md"
        ],
        Reference: [
          "docs/client-contract.md",
          "docs/persistence-pattern.md"
        ]
      ],
      groups_for_modules: [
        "Store Authoring": [
          Musubi.Store,
          Musubi.State,
          Musubi.Input,
          Musubi.Socket,
          Musubi.Child
        ],
        Runtime: [
          Musubi.Async,
          Musubi.AsyncResult,
          Musubi.Lifecycle,
          Musubi.Stream,
          Musubi.Telemetry
        ],
        Transport: [
          Musubi.Transport.Socket,
          Musubi.Transport.ConnectionChannel,
          Musubi.Transport.Channel
        ],
        Codegen: [
          Mix.Tasks.Compile.MusubiTs
        ],
        Testing: [
          Musubi.Testing
        ]
      ]
    ]
  end

  defp docs_modules do
    [
      Musubi.Store,
      Musubi.State,
      Musubi.Input,
      Musubi.Socket,
      Musubi.Child,
      Musubi.Async,
      Musubi.AsyncResult,
      Musubi.Lifecycle,
      Musubi.Stream,
      Musubi.Telemetry,
      Musubi.Transport.Socket,
      Musubi.Transport.ConnectionChannel,
      Musubi.Transport.Channel,
      Mix.Tasks.Compile.MusubiTs,
      Musubi.Testing
    ]
  end

  defp skip_doc_reference?(reference) when is_binary(reference) do
    Enum.any?(skipped_doc_references(), &String.starts_with?(reference, &1))
  end

  defp skip_doc_reference?(_other), do: false

  defp skipped_doc_references do
    [
      "Musubi.Application",
      "Musubi.Async.Telemetry",
      "Musubi.AsyncSupervisor",
      "Musubi.Codegen.TypeScript.Manifest",
      "Musubi.DSL.",
      "Musubi.Hooks.",
      "Musubi.Page.",
      "Musubi.Plugin.",
      "Musubi.Resolver",
      "Musubi.Socket.handle_join/2",
      "Musubi.State.__using__/1",
      "Musubi.Store.__using__/1",
      "Musubi.Stream.Slot",
      "Musubi.Type",
      "Musubi.Wire",
      "Resolver",
      "Module.__musubi_validate_state__/1"
    ]
  end

  defp aliases() do
    [
      precommit: [
        "compile --warnings-as-errors",
        "deps.unlock --unused",
        "format",
        "credo --strict",
        "compile.musubi_ts --check",
        "dialyzer",
        "test"
      ],
      bench: [
        "run bench/page_runtime_bench.exs",
        "run bench/diff_bench.exs",
        "run bench/stream_bench.exs"
      ]
    ]
  end
end
