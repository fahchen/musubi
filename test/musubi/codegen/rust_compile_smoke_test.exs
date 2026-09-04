defmodule Musubi.Codegen.RustCompileSmokeTest do
  # The golden tests in `rust_test.exs` compare the renderer's output to
  # itself, so a syntactically invalid bundle would stay green. This suite
  # closes that gap (docs/rust-codegen.md §6.5): render the full probe bundle,
  # drop it into a throwaway consumer crate, and let `cargo check` type-check
  # it against the real `musubi-client` crate.
  #
  # Tagged `:rust` and excluded by default in `test_helper.exs` — it needs a
  # Rust toolchain. Run it with `mix test --only rust`; CI does so in the Rust
  # job, where cargo is already set up.
  #
  # `async: false`: shells out to `cargo check` sharing the repo-level
  # `target/` dir — a heavyweight external process with its own file locking,
  # not something to interleave with the async suites.
  use ExUnit.Case, async: false

  @moduletag :rust
  # A cold `cargo check` builds serde and the client crate graph from scratch.
  @moduletag timeout: 600_000

  alias Musubi.Codegen.Manifest
  alias Musubi.Codegen.Rust

  alias Musubi.TestSupport.TypespecProbe
  alias Musubi.TestSupport.TypespecProbeChild
  alias Musubi.TestSupport.TypespecProbeNestedState
  alias Musubi.TestSupport.TypespecProbeWithAttrs
  alias Musubi.TestSupport.TypespecProbeWithCommand
  alias Musubi.TestSupport.TypespecProbeWithEvents
  alias Musubi.TestSupport.TypespecProbeWithReply
  alias Musubi.TestSupport.TypespecProbeWithUpload

  @tag :tmp_dir
  test "`cargo check` accepts the bundle rendered from the probe fixtures", %{tmp_dir: tmp_dir} do
    cargo = System.find_executable("cargo")

    assert cargo,
           "`cargo` not found on PATH — this test only runs under " <>
             "`mix test --only rust`, which requires a Rust toolchain"

    consumer = Path.join(tmp_dir, "consumer")
    File.mkdir_p!(Path.join(consumer, "src"))

    File.write!(Path.join(consumer, "Cargo.toml"), consumer_cargo_toml())
    File.write!(Path.join(consumer, "src/lib.rs"), "pub mod generated;\n")

    File.write!(
      Path.join(consumer, "src/generated.rs"),
      Rust.render(Enum.map(all_probes(), &entry/1))
    )

    # CARGO_TARGET_DIR points at the workspace's own `target/` so local
    # re-runs and CI's rust-cache reuse the already-built dependency graph;
    # cargo fingerprints keep the shared dir correct.
    {output, status} =
      System.cmd(cargo, ["check"],
        cd: consumer,
        stderr_to_stdout: true,
        env: [{"CARGO_TARGET_DIR", Path.join(File.cwd!(), "target")}]
      )

    assert status == 0, "`cargo check` rejected the rendered bundle:\n\n#{output}"
  end

  # The consumer mirrors what a real embedder writes (docs/rust-codegen-example.md):
  # edition 2024, a path dependency on the client crate, `serde` + `serde_json`.
  # The empty `[workspace]` table detaches it from any enclosing Cargo
  # workspace, so only the bundle under test is in the build graph.
  defp consumer_cargo_toml do
    """
    [package]
    name = "musubi-codegen-smoke"
    version = "0.0.0"
    edition = "2024"
    publish = false

    [workspace]

    [dependencies]
    musubi-client = { path = "#{Path.join(File.cwd!(), "crates/musubi-client")}" }
    serde = { version = "1", features = ["derive"] }
    serde_json = "1"
    """
  end

  # Same fixture set and manifest path as the golden test (`rust_test.exs`):
  # `Manifest.collect/1` over the probes' captured envs, rendered as one bundle.
  defp all_probes do
    [
      TypespecProbe,
      TypespecProbeChild,
      TypespecProbeNestedState,
      TypespecProbeWithAttrs,
      TypespecProbeWithCommand,
      TypespecProbeWithEvents,
      TypespecProbeWithReply,
      TypespecProbeWithUpload
    ]
  end

  defp entry(module) do
    data = Manifest.collect(module.__env__())
    {data.module, Map.take(data, [:kind, :fields, :commands, :events, :attrs, :uploads])}
  end
end
