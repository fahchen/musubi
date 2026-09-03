defmodule Musubi.WireCaptureTest do
  @moduledoc """
  Covers the capture harness `mix musubi.capture_wire` drives
  (`docs/rust-client.md` §12): the frame schema, the canonical encoding the
  drift gate depends on, and that the fixtures checked into
  `crates/musubi-client/tests/fixtures/` are the ones the current server
  emits.
  """

  use ExUnit.Case, async: false

  # The harness drives ~100 channel frames; Phoenix logs a line per join and
  # per command.
  @moduletag :capture_log

  alias Musubi.WireCapture
  alias Musubi.WireCapture.Recorder
  alias Musubi.WireCapture.Scenarios
  alias Musubi.WireCapture.Stores

  setup_all do
    start_supervised!({Phoenix.PubSub, name: Musubi.WireCapture.PubSub})
    start_supervised!(Musubi.WireCapture.Endpoint)

    # A full capture drives 21 channel scenarios, so it is run once for every
    # assertion that only needs to read the result.
    Process.flag(:trap_exit, true)

    # `setup_all` output is not covered by `:capture_log`, and one capture logs
    # a join/command line per frame.
    {captured, _log} = ExUnit.CaptureLog.with_log(&WireCapture.capture_all/0)

    {:ok, captured: captured}
  end

  setup do
    Process.flag(:trap_exit, true)
    :ok
  end

  describe "Recorder.capture/2" do
    test "records the join out-frame, its reply and the mount patch, in order" do
      scenario =
        Recorder.capture("probe", fn recorder ->
          Recorder.join(recorder, Stores.BetaRootStore, "probe-1", %{"label" => "hi"})
        end)

      assert %{"scenario" => "probe", "frames" => frames, "expected_state" => state} = scenario

      assert [
               %{"dir" => "out", "event" => "phx_join", "payload" => join},
               %{"dir" => "in", "event" => "phx_reply", "payload" => reply},
               %{"dir" => "in", "event" => "patch", "payload" => patch}
             ] = frames

      assert join["module"] == inspect(Stores.BetaRootStore)
      assert join["params"] == %{"label" => "hi"}
      assert reply["status"] == "ok"
      assert patch["base_version"] == 0 and patch["version"] == 1
      assert [%{"op" => "replace", "path" => "", "value" => ^state}] = patch["ops"]
    end

    test "expected_state is the server's own wire root, not a replay of the ops" do
      scenario =
        Recorder.capture("probe", fn recorder ->
          recorder
          |> Recorder.join(Stores.MetaRootStore, "probe-2", %{})
          |> Recorder.command([], "put", %{"key" => "b", "value" => "2"})
        end)

      assert scenario["expected_state"]["meta"] == %{"a" => "1", "b" => "2"}
    end

    test "a rejected join records the error reply and stops there" do
      scenario =
        Recorder.capture("probe", fn recorder ->
          Recorder.join(recorder, Stores.ChildStore, "probe-3", %{})
        end)

      assert [
               %{"dir" => "out", "event" => "phx_join"},
               %{
                 "dir" => "in",
                 "event" => "phx_reply",
                 "payload" => %{"status" => "error", "response" => %{"reason" => "unknown root"}}
               }
             ] = scenario["frames"]

      assert scenario["expected_state"] == nil
    end

    test "drop_in_frame/2 removes one server frame and leaves a version gap" do
      scenario =
        Recorder.capture("probe", fn recorder ->
          recorder
          |> Recorder.join(Stores.MetaRootStore, "probe-4", %{})
          |> Recorder.snapshot()
          |> Recorder.command([], "put", %{"key" => "b", "value" => "2"})
          |> Recorder.command([], "put", %{"key" => "c", "value" => "3"})
          |> Recorder.drop_in_frame(3)
        end)

      versions =
        for %{"dir" => "in", "event" => "patch", "payload" => patch} <- scenario["frames"],
            do: {patch["base_version"], patch["version"]}

      assert versions == [{0, 1}, {2, 3}]
      assert scenario["expected_state"]["meta"] == %{"a" => "1"}
    end
  end

  describe "WireCapture.encode/1" do
    test "sorts object keys at every depth and ends with a newline" do
      json = WireCapture.encode(%{"b" => 1, "a" => %{"d" => [%{"z" => 1, "y" => 2}], "c" => 3}})

      assert json == """
             {
               "a": {
                 "c": 3,
                 "d": [
                   {
                     "y": 2,
                     "z": 1
                   }
                 ]
               },
               "b": 1
             }
             """
    end
  end

  describe "the scenario set" do
    test "every scenario name is unique and file-safe" do
      names = Enum.map(Scenarios.all(), &elem(&1, 0))

      assert names == Enum.uniq(names)
      assert Enum.all?(names, &String.match?(&1, ~r/\A[a-z][a-z0-9_]*\z/))
    end

    test "covers every op kind, stream op and upload op the client has to apply",
         %{captured: captured} do
      corpus = Enum.map_join(captured, "\n", &elem(&1, 1))

      # JSON Patch ops, stream ops and upload ops share one `"op"` key.
      for op <- ~w(add remove replace reset insert delete config progress complete cancel) do
        assert String.contains?(corpus, ~s("op": "#{op}")),
               "no fixture carries op #{op}"
      end
    end
  end

  describe "checked-in fixtures" do
    test "match what the current server emits", %{captured: captured} do
      dir = WireCapture.output_dir()

      stale =
        captured
        |> Enum.reject(fn {file, json} -> File.read(Path.join(dir, file)) == {:ok, json} end)
        |> Enum.map(&elem(&1, 0))

      assert stale == [],
             "stale wire fixtures: #{Enum.join(stale, ", ")}. " <>
               "Run `mix musubi.capture_wire` and commit the result."
    end

    test "no orphan files linger under the fixture directory", %{captured: captured} do
      written = MapSet.new(captured, &elem(&1, 0))
      on_disk = WireCapture.output_dir() |> File.ls!() |> MapSet.new()

      assert MapSet.to_list(MapSet.difference(on_disk, written)) == []
    end
  end
end
