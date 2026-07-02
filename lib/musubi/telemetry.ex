defmodule Musubi.Telemetry do
  @moduledoc """
  Telemetry emission helper for Musubi runtime events plus the canonical event
  catalog.

  `events/0` returns every event name emitted by the runtime, paired with a
  one-line description. Use it to attach a single handler to all Musubi events
  (e.g. for log-shipping, metrics aggregation, or test observation):

      Musubi.Telemetry.events()
      |> Enum.map(&elem(&1, 0))
      |> :telemetry.attach_many("musubi-collector", &MyTelemetry.handle_event/4, nil)
  """

  @typedoc "One Musubi telemetry event entry: `{event_name, description}`."
  @type event() :: {[atom()], String.t()}

  @events [
    {[:musubi, :command, :start],
     "Per-command span start. Metadata: page_id, store_id, command."},
    {[:musubi, :command, :stop],
     "Per-command span stop. Measurements: duration. Metadata: page_id, store_id, command, status."},
    {[:musubi, :command, :exception],
     "Per-command span when a handler raises. Metadata: page_id, store_id, command, kind, reason, stacktrace."},
    {[:musubi, :render, :stop],
     "Render cycle completion. Measurements: duration. Metadata: module."},
    {[:musubi, :resolve, :stop],
     "Render-output placeholder resolution completion. Measurements: duration."},
    {[:musubi, :validate, :stop],
     "Render-output validation pass. Measurements: duration. Metadata: module, status."},
    {[:musubi, :validate, :exception],
     "Render-output validation failure. Metadata: kind, reason, stacktrace."},
    {[:musubi, :validate, :command, :stop],
     "Command payload schema validation pass. Measurements: count. Metadata: store_module, command."},
    {[:musubi, :validate, :reply, :stop],
     "Command reply schema validation pass. Measurements: count. Metadata: store_module, command."},
    {[:musubi, :diff, :stop], "JSON Patch diff completion. Measurements: duration, op_count."},
    {[:musubi, :patch, :stop],
     "Patch envelope built. Measurements: count, stream_count. Metadata: module, version."},
    {[:musubi, :stream, :flush],
     "Stream pending-ops flushed for one entry. Measurements: count."},
    {[:musubi, :async, :start],
     "Async task started via `assign_async`/`start_async`/`stream_async`."},
    {[:musubi, :async, :stop], "Async task completed (`:ok` or `:failed`)."},
    {[:musubi, :async, :exception], "Async task crashed or `handle_async/3` raised."},
    {[:musubi, :async, :cancel], "`cancel_async/2,3` invoked for a tracked task."},
    {[:musubi, :async, :lazy_discard], "Late async result arrived after tracking was dropped."},
    {[:musubi, :pubsub, :receive],
     "`handle_info/2` dispatched on the root store; observability for app PubSub messages."},
    {[:musubi, :auth, :deny],
     "A `:before_command` hook returned `{:halt, reply, socket}` — graceful authorization denial."},
    {[:musubi, :send_update, :no_target],
     "`Musubi.send_update/2,3` addressed a store_id that is not mounted; no-op, no envelope pushed. Metadata: store_id, page_id."}
  ]

  @doc """
  Emits a telemetry event when the `:telemetry` module is available.

  ## Examples

      iex> Musubi.Telemetry.emit([:musubi, :render, :stop], %{duration: 10}, %{module: Example})
      :ok
  """
  @spec emit([atom()], map(), map()) :: :ok
  def emit(event_name, measurements, metadata)
      when is_list(event_name) and is_map(measurements) and is_map(metadata) do
    if Code.ensure_loaded?(:telemetry) and function_exported?(:telemetry, :execute, 3) do
      :telemetry.execute(event_name, measurements, metadata)
    end

    :ok
  end

  @doc """
  Returns every telemetry event the Musubi runtime emits along with a one-line
  description.

  ## Examples

      iex> [{[:musubi, :command, :start], _desc} | _rest] = Musubi.Telemetry.events()
  """
  @spec events() :: [event()]
  def events, do: @events
end
