defmodule Musubi.WireCapture do
  @moduledoc false
  # Entry point shared by `mix musubi.capture_wire` and its test: boot the
  # harness's endpoint, capture every scenario, and encode each one as
  # canonical JSON.
  #
  # `docs/rust-client.md` §12 file shape:
  #
  #     {
  #       "scenario": "stream_insert",
  #       "frames": [{"dir": "in" | "out", "event": "...", "payload": {}}],
  #       "expected_state": {}
  #     }
  #
  # `expected_state` is the **server's** wire-form root after the scenario's
  # last delivered envelope — the document a client's patch engine must hold
  # before hydration (stream and upload markers still in place; stream contents
  # arrive out of band in `stream_ops`). It is read off the page server rather
  # than replayed from the captured ops, so a fixture cross-checks the patch
  # encoder instead of restating it.

  alias Musubi.WireCapture.Recorder
  alias Musubi.WireCapture.Scenarios

  @output_dir "crates/musubi-client/tests/fixtures"

  @doc "Directory the fixtures are written to, relative to the project root."
  @spec output_dir() :: Path.t()
  def output_dir, do: @output_dir

  @doc "Starts the PubSub + endpoint the harness needs."
  @spec start_harness() :: :ok
  def start_harness do
    # `Phoenix.ChannelTest.join/4` links the channel process to the caller, so
    # `leave/1` would otherwise take the capturing process down with it.
    Process.flag(:trap_exit, true)

    children = [
      {Phoenix.PubSub, name: Musubi.WireCapture.PubSub},
      Musubi.WireCapture.Endpoint
    ]

    {:ok, _pid} = Supervisor.start_link(children, strategy: :one_for_one)
    :ok
  end

  @doc "Captures every scenario and returns `{filename, json}` pairs, sorted."
  @spec capture_all() :: [{String.t(), String.t()}]
  def capture_all do
    Enum.map(Scenarios.all(), fn {name, fun} ->
      {name <> ".json", encode(Recorder.capture(name, fun))}
    end)
  end

  @doc """
  Encodes one scenario as canonical JSON: two-space indented, object keys in
  sorted order at every depth, one trailing newline. Sorting is what makes the
  regenerate-and-`git diff` drift gate meaningful — `Jason` would otherwise
  emit Erlang map order, which is stable per term but not across shapes.
  """
  @spec encode(map()) :: String.t()
  def encode(scenario) when is_map(scenario) do
    scenario |> sort_keys() |> Jason.encode!(pretty: [indent: "  "]) |> Kernel.<>("\n")
  end

  defp sort_keys(map) when is_map(map) and not is_struct(map) do
    map
    |> Enum.sort_by(fn {key, _value} -> to_string(key) end)
    |> Enum.map(fn {key, value} -> {key, sort_keys(value)} end)
    |> Jason.OrderedObject.new()
  end

  defp sort_keys(list) when is_list(list), do: Enum.map(list, &sort_keys/1)
  defp sort_keys(other), do: other
end
