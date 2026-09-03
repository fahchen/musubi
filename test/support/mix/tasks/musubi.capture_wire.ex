defmodule Mix.Tasks.Musubi.CaptureWire do
  @shortdoc "Captures Musubi wire fixtures for the Rust client test suite"

  @moduledoc """
  Drives the shared connection-channel harness in `test/support/wire_capture/`
  and writes one JSON file per scenario to
  `crates/musubi-client/tests/fixtures/<scenario>.json`
  (`docs/rust-client.md` §12, layer 1).

      MIX_ENV=test mix musubi.capture_wire

  The task lives under `test/support` — not `lib/` — because the fixture
  stores, socket and endpoint it drives are test-only and must not ship in the
  Hex tarball. `:test` is the task's preferred env (`mix.exs`), so the bare
  `mix musubi.capture_wire` works too.

  Output is canonical — deterministic across runs, per `docs/rust-client.md`
  §12 — which is what makes the CI drift gate work:

      mix musubi.capture_wire && git diff --exit-code crates/musubi-client/tests/fixtures
  """

  use Mix.Task

  alias Musubi.WireCapture

  @requirements ["app.start"]

  @impl Mix.Task
  @spec run([String.t()]) :: :ok
  def run(_argv) do
    # Phoenix logs a line per join and per command; a capture run drives
    # hundreds and none of them say anything about the fixtures.
    Logger.configure(level: :warning)

    WireCapture.start_harness()

    write!(WireCapture.capture_all(), WireCapture.output_dir())
  end

  defp write!(captured, dir) do
    File.mkdir_p!(dir)
    Enum.each(captured, fn {file, json} -> File.write!(Path.join(dir, file), json) end)
    remove_stale(captured, dir)

    Mix.shell().info("[musubi.capture_wire] wrote #{length(captured)} fixtures to #{dir}")
  end

  # A renamed or deleted scenario must not leave its file behind, or the drift
  # gate would keep passing on a fixture nothing captures any more.
  defp remove_stale(captured, dir) do
    Enum.each(orphans(captured, dir), &File.rm!(Path.join(dir, &1)))
  end

  defp orphans(captured, dir) do
    written = MapSet.new(captured, &elem(&1, 0))

    case File.ls(dir) do
      {:ok, names} -> Enum.filter(names, &(String.ends_with?(&1, ".json") and &1 not in written))
      {:error, _reason} -> []
    end
  end
end
