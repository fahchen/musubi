defmodule Mix.Tasks.Compile.MusubiTsTest do
  # async: false because the test scopes a manifest target_dir override via
  # Process dict and writes the rendered bundle to the configured
  # `:ts_codegen_output_path` (which `config/test.exs` points at
  # `test/tmp/musubi_ts_bundle.ts`). Concurrent runs would race on that path.
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias Mix.Tasks.Compile.MusubiTs
  alias Musubi.Codegen.Manifest
  alias Musubi.TestSupport.TypespecProbe
  alias Musubi.TestSupport.TypespecProbeChild

  # Stands in for a checked-in bundle: any content the empty render cannot
  # produce, so "did the compiler clobber it?" is decidable by equality.
  @committed_bundle "declare namespace Musubi { /* committed by hand */ }\n"

  setup do
    target =
      Path.join(
        System.tmp_dir!(),
        "musubi_ts_compile_#{:erlang.unique_integer([:positive])}"
      )

    File.mkdir_p!(target)
    Process.put(:__musubi_codegen_target_dir__, target)

    output_path = Application.fetch_env!(:musubi, :ts_codegen_output_path)
    File.mkdir_p!(Path.dirname(output_path))
    File.rm(output_path)

    on_exit(fn ->
      File.rm_rf!(target)
      File.rm(output_path)
    end)

    {:ok, target: target, output_path: output_path}
  end

  describe "run/1 — empty manifest" do
    test "returns :noop and does not create the bundle file", %{output_path: output_path} do
      assert MusubiTs.run([]) == :noop
      refute File.exists?(output_path)
    end

    test "returns :noop in --check mode", %{output_path: output_path} do
      assert MusubiTs.run(["--check"]) == :noop
      refute File.exists?(output_path)
    end

    test "leaves an existing bundle untouched and warns instead of emptying it",
         %{output_path: output_path} do
      File.write!(output_path, @committed_bundle)

      warning = capture_io(:stderr, fn -> assert {:ok, []} = MusubiTs.run([]) end)

      assert File.read!(output_path) == @committed_bundle
      assert warning =~ "musubi_ts"
      assert warning =~ output_path
      assert warning =~ "mix compile --force"
    end

    test "--check still reports drift against an existing bundle",
         %{output_path: output_path} do
      File.write!(output_path, @committed_bundle)

      assert {:error,
              [
                %Mix.Task.Compiler.Diagnostic{
                  severity: :error,
                  compiler_name: "musubi_ts",
                  file: ^output_path
                }
              ]} = MusubiTs.run(["--check"])

      assert File.read!(output_path) == @committed_bundle
    end
  end

  describe "run/1 — populated manifest" do
    setup %{target: target} do
      Manifest.stamp(TypespecProbe, "lib/x.ex", target)
      Manifest.stamp(TypespecProbeChild, "lib/y.ex", target)
      :ok
    end

    test "writes a fresh bundle covering every stamped module", %{output_path: output_path} do
      assert {:ok, []} = MusubiTs.run([])

      contents = File.read!(output_path)
      assert contents =~ "declare namespace Musubi {"
      refute contents =~ "declare global"
      refute contents =~ "export {}"
      refute contents =~ ~s|import "@musubi/client"|
      assert contents =~ "type AsyncResult<T>"
      assert contents =~ "interface StoreDef<Module extends string, Shape, Commands, Events = {}>"
      assert contents =~ ~s|"Musubi.TestSupport.TypespecProbe": StoreDef<|
      assert contents =~ ~s|"Musubi.TestSupport.TypespecProbeChild": StoreDef<|
      assert contents =~ "Musubi.StreamField<"
      assert contents =~ "Musubi.AsyncField<"
    end

    test "returns :noop when the bundle already matches", %{output_path: output_path} do
      assert {:ok, []} = MusubiTs.run([])
      assert MusubiTs.run([]) == :noop
      assert File.exists?(output_path)
    end

    test "rewrites a stale bundle", %{output_path: output_path} do
      File.write!(output_path, "// stale\n")
      assert {:ok, []} = MusubiTs.run([])

      assert File.read!(output_path) =~
               ~s|"Musubi.TestSupport.TypespecProbe": StoreDef<|
    end

    test "--check returns drift diagnostic on mismatch and does not write",
         %{output_path: output_path} do
      File.write!(output_path, "// stale\n")

      assert {:error,
              [
                %Mix.Task.Compiler.Diagnostic{
                  severity: :error,
                  compiler_name: "musubi_ts",
                  file: ^output_path
                }
              ]} = MusubiTs.run(["--check"])

      assert File.read!(output_path) == "// stale\n"
    end

    test "--check returns :noop when bundle matches", %{output_path: output_path} do
      assert {:ok, []} = MusubiTs.run([])
      assert MusubiTs.run(["--check"]) == :noop
      assert File.exists?(output_path)
    end
  end

  describe "manifests/0" do
    test "returns the manifest target dir so `mix clean` removes it", %{target: target} do
      assert MusubiTs.manifests() == [target]
    end
  end

  describe "clean/0" do
    test "deletes the manifest target dir", %{target: target} do
      Manifest.stamp(TypespecProbe, "lib/x.ex", target)
      assert File.dir?(Path.join(target, inspect(TypespecProbe)))

      MusubiTs.clean()

      refute File.exists?(target)
    end
  end
end
