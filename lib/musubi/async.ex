defmodule Musubi.Async do
  @moduledoc """
  Async lifecycle API for Musubi stores. Exposes `assign_async/3,4`,
  `start_async/3,4`, `cancel_async/2,3`, and `stream_async/3,4`.

  ## Public API surface (frozen for M5+)

    * `assign_async/3,4` — spawn a task whose result writes one or more
      `Musubi.AsyncResult` values into `socket.assigns`.
    * `start_async/3,4` — spawn a task whose result is delivered to the
      store's `handle_async/3` callback. `socket.assigns` is not mutated by
      the call itself.
    * `cancel_async/2,3` — kill an in-flight task by name/key or by
      `%AsyncResult{}` value. Always produces an `Musubi.AsyncResult.failed/2`
      write driven by the resulting `:DOWN` (or pre-written, when called via
      the `%AsyncResult{}` variant).
    * `stream_async/3,4` — composite `assign_async` + `Musubi.Stream.stream/4`:
      writes a status flag into `socket.assigns` and seeds the stream slot in
      one envelope.

  ## Task supervision

  Tasks run under `Musubi.AsyncSupervisor` (a `Task.Supervisor` started by
  `Musubi.Application`). Pass `:supervisor` to override per-call:

      Musubi.Async.assign_async(socket, :profile, fun, supervisor: MyApp.TaskSup)

  ## Reserved socket-private key

  Per-task tracking lives at `socket.private[:__musubi_async_refs__]` and is
  runtime-internal. Use `Musubi.Async.private_refs_key/0` to introspect
  without hard-coding the literal.

  ## Result classification

  | Event                                       | Terminal `Musubi.AsyncResult` value                  |
  | :------------------------------------------ | :-------------------------------------------------- |
  | `{:ok, val}`                                | `ok(prior, val)`                                    |
  | `{:error, reason}`                          | `failed(prior, {:error, reason})`                   |
  | task raises                                 | `failed(prior, {:exit, {kind, reason, stacktrace}})`|
  | task throws                                 | `failed(prior, {:exit, {{:nocatch, val}, st}})`     |
  | task exits with reason `r`                  | `failed(prior, {:exit, r})`                         |
  | `:timeout` fires                            | `failed(prior, {:exit, :timeout})`                  |
  | `cancel_async/2,3` with reason `r`          | `failed(prior, {:exit, r})`                         |
  | runtime no longer hosts originating node    | lazy-discard; `[:musubi, :async, :lazy_discard]`     |

  ## Telemetry

  Every async event emits `[:musubi, :async, :event]` with `event` in
  `:start | :stop | :exception | :cancel | :lazy_discard`. Metadata always
  includes `page_id`, `path`, `name`, and `kind` (`:assign | :start | :stream`).
  See `Musubi.Async.Telemetry` for the canonical metadata builder.
  """

  alias Musubi.AsyncResult
  alias Musubi.AsyncSupervisor
  alias Musubi.Socket
  alias Musubi.Stream

  @private_refs_key :__musubi_async_refs__

  @typedoc "Internal name a tracked task is filed under. Atom for `start_async`/`stream_async`; the key list for `assign_async`."
  @type tracking_name() :: atom() | [atom()]

  @typedoc "User-supplied name argument."
  @type name_arg() :: term()

  @typedoc "Async kind discriminator carried in tracking + telemetry metadata."
  @type kind() :: :assign | :start | :stream

  @typedoc "Acceptable `Task.Supervisor` reference — module name, registered atom, or pid."
  @type supervisor_ref() :: module() | atom() | pid()

  @typedoc "Per-task tracking entry stored under `socket.private[:__musubi_async_refs__]`."
  @type tracking_entry() :: %{
          required(:ref) => reference(),
          required(:pid) => pid(),
          required(:kind) => kind(),
          required(:keys) => [atom()] | nil,
          required(:prior) => %{atom() => AsyncResult.t()},
          required(:timer_ref) => reference() | nil,
          required(:cancel_reason) => term() | nil,
          required(:supervisor) => supervisor_ref()
        }

  @doc "Returns the reserved socket-private key holding async-task tracking entries."
  @spec private_refs_key() :: :__musubi_async_refs__
  def private_refs_key, do: @private_refs_key

  # ---------------------------------------------------------------------------
  # assign_async
  # ---------------------------------------------------------------------------

  @doc """
  Spawns a background task whose result writes `Musubi.AsyncResult` values into
  `socket.assigns` for the given `key` (or list of keys).

  Synchronously writes `Musubi.AsyncResult.loading(prior)` per key before
  returning the socket. On task completion the runtime atomically updates each
  key to either `Musubi.AsyncResult.ok(prior, value)` (single-key:
  `fun` returned `{:ok, value}`; multi-key: `fun` returned
  `{:ok, %{key1: v1, key2: v2}}`) or `Musubi.AsyncResult.failed(prior, reason)`.

  ## Options

    * `:reset` — `true` re-emits `loading()` (with no prior) for every managed
      key; a list of keys re-emits loading for that subset only. The prior
      task (if any) is NOT killed (LiveView parity); it keeps running and its
      result lazy-discards by ref. Use `cancel_async/2,3` to actively kill.
    * `:timeout` — milliseconds; on expiry the task is killed and the result
      becomes `failed(prior, {:exit, :timeout})`.
    * `:supervisor` — `Task.Supervisor` name; defaults to `Musubi.AsyncSupervisor`.

  ## Examples

      socket = Musubi.Async.assign_async(socket, :profile, fn -> {:ok, fetch()} end)
      socket = Musubi.Async.assign_async(socket, [:user, :org], fn -> {:ok, %{user: u, org: o}} end)
      socket = Musubi.Async.assign_async(socket, :profile, fun, reset: true, timeout: 5_000)
  """
  @spec assign_async(Socket.t(), name_arg(), (-> term()), keyword()) :: Socket.t()
  def assign_async(socket, key_or_keys, fun, opts \\ [])

  def assign_async(%Socket{} = socket, key, fun, opts) when is_atom(key) do
    do_assign_async(socket, key, [key], fun, opts)
  end

  def assign_async(%Socket{} = socket, keys, fun, opts) when is_list(keys) do
    validate_keys!(keys, :assign_async)
    do_assign_async(socket, keys, keys, fun, opts)
  end

  defp do_assign_async(socket, name, keys, fun, opts) when is_function(fun, 0) do
    {reset, opts} = Keyword.pop(opts, :reset, false)
    {timeout, opts} = Keyword.pop(opts, :timeout)
    {supervisor, _opts} = Keyword.pop(opts, :supervisor, AsyncSupervisor)

    # Re-assign never kills the prior task (LiveView parity): drop its tracking
    # so its result/`:DOWN` lazy-discard by ref. Only `cancel_async`/`:timeout`
    # kill. Killing a prior task mid-DB-call tears down a shared Ecto sandbox
    # connection — see docs/review-store-async-sqlite-problem.md.
    socket = drop_tracking(socket, name)
    prior = snapshot_prior(socket, keys)
    socket = write_loading_for_keys(socket, keys, prior, reset)

    task_keys = if is_list(name), do: keys, else: nil

    {ref, pid, timer_ref} =
      spawn_task(socket, name, :assign, assign_task_body(fun, task_keys), supervisor, timeout)

    socket =
      put_tracking(socket, name, %{
        ref: ref,
        pid: pid,
        kind: :assign,
        keys: keys,
        prior: prior,
        timer_ref: timer_ref,
        cancel_reason: nil,
        supervisor: supervisor
      })

    Musubi.Async.Telemetry.start(socket, name, :assign)
    socket
  end

  # ---------------------------------------------------------------------------
  # start_async
  # ---------------------------------------------------------------------------

  @doc """
  Spawns a background task whose result is routed to the store's
  `handle_async(name, result, socket)` callback.

  `socket.assigns` is not mutated by this call — applications that want a
  visible loading indicator should write one explicitly (typically by also
  calling `assign_async/3,4` for the same data).

  A second `start_async/3,4` with the same `name` silently overwrites the
  prior tracking ref. The older task continues running and its
  result is lazy-discarded on arrival, accompanied by a
  `[:musubi, :async, :lazy_discard]` telemetry event.

  ## Options

    * `:timeout` — milliseconds; on expiry the task is killed and
      `handle_async/3` receives `{:exit, :timeout}`.
    * `:supervisor` — `Task.Supervisor` name; defaults to `Musubi.AsyncSupervisor`.

  ## Examples

      socket = Musubi.Async.start_async(socket, :warm_cache, fn -> Cache.warm() end)
  """
  @spec start_async(Socket.t(), atom(), (-> term()), keyword()) :: Socket.t()
  def start_async(socket, name, fun, opts \\ [])

  def start_async(%Socket{} = socket, name, fun, opts)
      when is_atom(name) and is_function(fun, 0) and is_list(opts) do
    {timeout, opts} = Keyword.pop(opts, :timeout)
    {supervisor, _opts} = Keyword.pop(opts, :supervisor, AsyncSupervisor)

    socket = drop_tracking(socket, name)

    {ref, pid, timer_ref} =
      spawn_task(socket, name, :start, start_task_body(fun), supervisor, timeout)

    socket =
      put_tracking(socket, name, %{
        ref: ref,
        pid: pid,
        kind: :start,
        keys: nil,
        prior: %{},
        timer_ref: timer_ref,
        cancel_reason: nil,
        supervisor: supervisor
      })

    Musubi.Async.Telemetry.start(socket, name, :start)
    socket
  end

  # ---------------------------------------------------------------------------
  # cancel_async
  # ---------------------------------------------------------------------------

  @doc """
  Kills an in-flight task and resolves its tracked assigns to
  `Musubi.AsyncResult.failed(prior, {:exit, reason})`.

  Three calling shapes:

    * `cancel_async(socket, name)` — by name (atom for
      `start_async`/`stream_async`, atom or key list for `assign_async`).
      Kills the pid; the resulting `:DOWN` message drives the failed write.
    * `cancel_async(socket, name, reason)` — same, with explicit `reason`.
    * `cancel_async(socket, %AsyncResult{}, reason)` — pre-writes
      `failed/2` synchronously, then kills the task. Use when the caller
      already holds the `%AsyncResult{}` and wants the assign updated before
      returning.

  Default `reason` is `{:shutdown, :cancel}`.

  Emits `[:musubi, :async, :cancel]`.

  ## Examples

      socket = Musubi.Async.cancel_async(socket, :profile)
      socket = Musubi.Async.cancel_async(socket, :profile, :user_navigated_away)
      socket = Musubi.Async.cancel_async(socket, async_result, :user_navigated_away)
  """
  @spec cancel_async(Socket.t(), name_arg() | AsyncResult.t()) :: Socket.t()
  def cancel_async(socket, target),
    do: cancel_async(socket, target, {:shutdown, :cancel})

  @spec cancel_async(Socket.t(), name_arg() | AsyncResult.t(), term()) :: Socket.t()
  def cancel_async(%Socket{} = socket, %AsyncResult{} = ar, reason) do
    case find_name_for_async_result(socket, ar) do
      {:ok, name} -> cancel_by_name(socket, name, reason, pre_write_failed?: true)
      :error -> socket
    end
  end

  def cancel_async(%Socket{} = socket, name, reason) when is_atom(name) or is_list(name) do
    cancel_by_name(socket, name, reason, pre_write_failed?: false)
  end

  defp cancel_by_name(socket, name, reason, opts) do
    case fetch_tracking(socket, name) do
      {:ok, entry} ->
        Musubi.Async.Telemetry.cancel(socket, name, entry.kind, reason)

        socket =
          if Keyword.fetch!(opts, :pre_write_failed?) and entry.kind in [:assign, :stream] do
            do_write_failed(socket, entry, {:exit, reason})
          else
            socket
          end

        cancel_timer(entry.timer_ref)

        # Mark cancel reason so the eventual :DOWN handler reports it
        # rather than the raw exit reason from Process.exit/2.
        socket =
          if Keyword.fetch!(opts, :pre_write_failed?) do
            # Pre-written; drop tracking so :DOWN finds nothing and is a no-op.
            drop_tracking_only(socket, name)
          else
            update_tracking(socket, name, &Map.put(&1, :cancel_reason, reason))
          end

        kill_task(entry.pid, reason)
        socket

      :error ->
        socket
    end
  end

  # ---------------------------------------------------------------------------
  # stream_async
  # ---------------------------------------------------------------------------

  @doc """
  Composite `assign_async/3,4` + `Musubi.Stream.stream/4`. Spawns a background
  task whose successful return populates a previously-declared stream slot
  AND flips the matching `socket.assigns.<name>` `Musubi.AsyncResult` to
  `:ok` with `result: true` (the items live in the stream, not in assigns).

  The user fun must return one of:

    * `{:ok, enumerable}` — items are inserted into the stream with no opts.
    * `{:ok, enumerable, stream_opts}` — items inserted with the given
      `stream/4` options (e.g. `[at: 0, limit: -100, reset: true]`).
    * `{:error, reason}` — the assign becomes
      `Musubi.AsyncResult.failed(prior, {:error, reason})` and the stream
      contents are left untouched.

  Any other shape raises `ArgumentError` inside the task and surfaces as
  `Musubi.AsyncResult.failed(prior, {:exit, ...})`.

  Calling `stream_async` for a `name` with no matching `stream :name, ...`
  declaration raises `ArgumentError` immediately (before the task is spawned).

  ## Options

    * `:reset` — `true` re-emits `Musubi.AsyncResult.loading(prior)` for the
      assign and leaves stream contents alone. The prior task (if any) is NOT
      killed (LiveView parity); it keeps running and its result lazy-discards
      by ref. Use `cancel_async/2,3` to actively kill. The user fun decides
      whether to actually reset the stream by returning `{:ok, items, reset: true}`.
    * `:timeout` — milliseconds; on expiry the task is killed and the assign
      becomes `failed(prior, {:exit, :timeout})`. Stream untouched.
    * `:supervisor` — `Task.Supervisor` name; defaults to `Musubi.AsyncSupervisor`.

  ## Examples

      socket = Musubi.Async.stream_async(socket, :messages, fn -> {:ok, fetch_messages()} end)
      socket = Musubi.Async.stream_async(socket, :messages, fn -> {:ok, items, at: 0, limit: -100} end)
      socket = Musubi.Async.stream_async(socket, :messages, fun, reset: true, timeout: 5_000)
  """
  @spec stream_async(Socket.t(), atom(), (-> term()), keyword()) :: Socket.t()
  def stream_async(socket, name, fun, opts \\ [])

  def stream_async(%Socket{} = socket, name, fun, opts)
      when is_atom(name) and is_function(fun, 0) and is_list(opts) do
    ensure_stream_declared!(socket, name)

    {reset, opts} = Keyword.pop(opts, :reset, false)
    {timeout, opts} = Keyword.pop(opts, :timeout)
    {supervisor, _opts} = Keyword.pop(opts, :supervisor, AsyncSupervisor)

    # Re-assign never kills the prior task (LiveView parity): drop its tracking
    # so its result/`:DOWN` lazy-discard by ref. Only `cancel_async`/`:timeout`
    # kill. Killing a prior task mid-DB-call tears down a shared Ecto sandbox
    # connection — see docs/review-store-async-sqlite-problem.md.
    socket = drop_tracking(socket, name)
    prior = snapshot_prior(socket, [name])

    socket =
      socket
      |> write_loading_for_keys([name], prior, reset)
      |> Stream.stream(name, [])

    {ref, pid, timer_ref} =
      spawn_task(socket, name, :stream, stream_task_body(fun), supervisor, timeout)

    socket =
      put_tracking(socket, name, %{
        ref: ref,
        pid: pid,
        kind: :stream,
        keys: [name],
        prior: prior,
        timer_ref: timer_ref,
        cancel_reason: nil,
        supervisor: supervisor
      })

    Musubi.Async.Telemetry.start(socket, name, :stream)
    socket
  end

  # ---------------------------------------------------------------------------
  # Public introspection (used by the page server)
  # ---------------------------------------------------------------------------

  @doc false
  @spec tracking(Socket.t()) :: %{tracking_name() => tracking_entry()}
  def tracking(%Socket{} = socket), do: Socket.get_private(socket, @private_refs_key, %{})

  @doc false
  @spec fetch_tracking(Socket.t(), tracking_name()) :: {:ok, tracking_entry()} | :error
  def fetch_tracking(%Socket{} = socket, name) do
    Map.fetch(tracking(socket), name)
  end

  @doc false
  @spec drop_tracking_only(Socket.t(), tracking_name()) :: Socket.t()
  def drop_tracking_only(%Socket{} = socket, name) do
    Socket.put_private(socket, @private_refs_key, Map.delete(tracking(socket), name))
  end

  @doc """
  Applies a classified `assign_async`/`stream_async` task result to the
  socket. Called by `Musubi.Page.Server` from `handle_info`.

  `classified` is the wrapped task return: `{:ok, user_return}` or
  `{:exit, reason_class}`.
  """
  @spec apply_task_result(Socket.t(), tracking_name(), tracking_entry(), term()) :: Socket.t()
  def apply_task_result(%Socket{} = socket, name, entry, classified) do
    classified = unwrap_task_result(classified)
    socket = drop_tracking_only(socket, name)
    cancel_timer(entry.timer_ref)

    case entry.kind do
      :assign -> apply_assign_result(socket, entry, classified)
      :stream -> apply_stream_result(socket, entry, classified)
    end
  end

  defp apply_assign_result(socket, entry, {:ok, {:ok, value}}),
    do: write_assign_success(socket, entry, value)

  defp apply_assign_result(socket, entry, {:ok, {:error, reason}}),
    do: do_write_failed(socket, entry, {:error, reason})

  defp apply_assign_result(socket, entry, {:ok, other}),
    do: do_write_failed(socket, entry, invalid_shape_exit(:assign_async, other))

  defp apply_assign_result(socket, entry, {:exit, reason_class}),
    do: do_write_failed(socket, entry, {:exit, reason_class})

  defp apply_stream_result(socket, entry, {:ok, {:ok, items}}),
    do: write_stream_success(socket, entry, items, [])

  defp apply_stream_result(socket, entry, {:ok, {:ok, items, stream_opts}}),
    do: write_stream_success(socket, entry, items, stream_opts)

  defp apply_stream_result(socket, entry, {:ok, {:error, reason}}),
    do: do_write_failed(socket, entry, {:error, reason})

  defp apply_stream_result(socket, entry, {:ok, other}),
    do: do_write_failed(socket, entry, invalid_shape_exit(:stream_async, other))

  defp apply_stream_result(socket, entry, {:exit, reason_class}),
    do: do_write_failed(socket, entry, {:exit, reason_class})

  defp invalid_shape_exit(fun_name, other) do
    msg = "#{fun_name} user fun returned invalid shape: #{inspect(other)}"
    {:exit, {:error, %ArgumentError{message: msg}, []}}
  end

  defp unwrap_task_result({:musubi_async_result, _name, _kind, classified}), do: classified
  defp unwrap_task_result(classified), do: classified

  @doc """
  Resolves a `:DOWN` message for a tracked task. Called by
  `Musubi.Page.Server`. Writes `Musubi.AsyncResult.failed(prior, {:exit, reason})`
  to every key managed by the tracking entry.

  Honors a previously-stamped `:cancel_reason` (set by `cancel_async/2,3` or
  `:timeout`) so the surfaced reason matches the operator-visible cause
  rather than the raw `Process.exit/2` reason.
  """
  @spec apply_task_down(Socket.t(), tracking_name(), tracking_entry(), term()) :: Socket.t()
  def apply_task_down(%Socket{} = socket, name, entry, raw_reason) do
    socket = drop_tracking_only(socket, name)
    cancel_timer(entry.timer_ref)

    reason = entry.cancel_reason || raw_reason

    case entry.kind do
      :assign -> do_write_failed(socket, entry, {:exit, reason})
      :stream -> do_write_failed(socket, entry, {:exit, reason})
      :start -> socket
    end
  end

  @doc """
  Marks a tracking entry's `:cancel_reason` to `:timeout` and returns the entry
  so the caller can kill the task pid. Called by `Musubi.Page.Server` when a
  `{:musubi_async_timeout, ref}` message fires.
  """
  @spec mark_timeout(Socket.t(), tracking_name()) :: {Socket.t(), tracking_entry()} | :error
  def mark_timeout(%Socket{} = socket, name) do
    case fetch_tracking(socket, name) do
      {:ok, entry} ->
        next_entry = %{entry | cancel_reason: :timeout}
        socket = update_tracking(socket, name, fn _entry -> next_entry end)
        {socket, next_entry}

      :error ->
        :error
    end
  end

  # ---------------------------------------------------------------------------
  # Internal: assign + stream writers
  # ---------------------------------------------------------------------------

  defp write_assign_success(socket, %{keys: [single]} = entry, value) when is_atom(single) do
    Socket.assign(socket, single, AsyncResult.ok(prior_for(entry, single), value))
  end

  defp write_assign_success(socket, %{keys: keys} = entry, %{} = value_map) when is_list(keys) do
    case missing_assign_key(keys, value_map) do
      nil ->
        Enum.reduce(keys, socket, &assign_one_from_map(&2, &1, entry, value_map))

      key ->
        do_write_failed(socket, entry, missing_assign_key_exit(key, value_map))
    end
  end

  defp write_assign_success(socket, %{keys: keys} = entry, other) when is_list(keys) do
    do_write_failed(
      socket,
      entry,
      invalid_shape_exit(:assign_async, other)
    )
  end

  defp assign_one_from_map(socket, key, entry, value_map) do
    case Map.fetch(value_map, key) do
      {:ok, v} ->
        Socket.assign(socket, key, AsyncResult.ok(prior_for(entry, key), v))

      :error ->
        do_write_failed(socket, entry, missing_assign_key_exit(key, value_map))
    end
  end

  defp write_stream_success(socket, %{keys: [name]} = entry, items, stream_opts)
       when is_atom(name) and is_list(stream_opts) do
    case stream_enumerable(items) do
      {:ok, enumerable} ->
        socket
        |> Socket.assign(name, AsyncResult.ok(prior_for(entry, name), true))
        |> Stream.stream(name, enumerable, stream_opts)

      {:error, reason} ->
        do_write_failed(socket, entry, {:exit, {:error, reason, []}})
    end
  end

  defp stream_enumerable(items) do
    if is_list(items) do
      {:ok, items}
    else
      {:ok, Enum.to_list(items)}
    end
  rescue
    error in [Protocol.UndefinedError] ->
      {:error,
       %ArgumentError{
         message:
           "stream_async items must be enumerable, got: #{inspect(items)} " <>
             "(#{Exception.message(error)})"
       }}
  end

  defp missing_assign_key(keys, value_map) do
    Enum.find(keys, &(not is_map_key(value_map, &1)))
  end

  defp missing_assign_key_exit(key, value_map) do
    {:exit,
     {:error,
      %ArgumentError{
        message:
          "assign_async multi-key result missing key #{inspect(key)} in #{inspect(value_map)}"
      }, []}}
  end

  defp do_write_failed(socket, %{keys: nil}, _reason), do: socket

  defp do_write_failed(socket, %{keys: keys} = entry, reason) when is_list(keys) do
    Enum.reduce(keys, socket, fn key, acc ->
      Socket.assign(acc, key, AsyncResult.failed(prior_for(entry, key), reason))
    end)
  end

  # Normalize a captured prior into an `%AsyncResult{}` so `ok/2`/`failed/2`
  # always receive a struct (raw assign or absent key seeds an empty one;
  # `ok` discards the carried result, `failed` preserves it).
  defp prior_for(%{prior: prior}, key) do
    case Map.fetch(prior, key) do
      {:ok, %AsyncResult{} = ar} -> ar
      {:ok, other} -> AsyncResult.loading(other)
      :error -> AsyncResult.loading()
    end
  end

  # ---------------------------------------------------------------------------
  # Internal: loading + reset writers
  # ---------------------------------------------------------------------------

  defp write_loading_for_keys(socket, keys, prior, reset) do
    reset_keys = reset_keys(reset, keys)

    Enum.reduce(keys, socket, fn key, acc ->
      ar =
        if key in reset_keys do
          AsyncResult.loading()
        else
          AsyncResult.loading(Map.get(prior, key))
        end

      Socket.assign(acc, key, ar)
    end)
  end

  @spec reset_keys(boolean() | [atom()], [atom()]) :: [atom()]
  defp reset_keys(false, _keys), do: []
  defp reset_keys(true, keys), do: keys
  defp reset_keys(list, _keys) when is_list(list), do: list

  defp snapshot_prior(socket, keys) do
    Enum.reduce(keys, %{}, fn key, acc ->
      case Map.fetch(socket.assigns, key) do
        {:ok, value} -> Map.put(acc, key, value)
        :error -> acc
      end
    end)
  end

  defp drop_tracking(socket, name) do
    case fetch_tracking(socket, name) do
      {:ok, entry} ->
        # Per BDR-0019: silent overwrite. We do NOT kill the prior task.
        # The result will lazy-discard on arrival because its ref is gone
        # from the tracking map. The page server emits the
        # [:musubi, :async, :lazy_discard] event when it sees the orphan ref.
        cancel_timer(entry.timer_ref)
        drop_tracking_only(socket, name)

      :error ->
        socket
    end
  end

  defp put_tracking(socket, name, entry) do
    Socket.put_private(socket, @private_refs_key, Map.put(tracking(socket), name, entry))
  end

  defp update_tracking(socket, name, fun) do
    case fetch_tracking(socket, name) do
      {:ok, entry} -> put_tracking(socket, name, fun.(entry))
      :error -> socket
    end
  end

  # ---------------------------------------------------------------------------
  # Internal: task spawn
  # ---------------------------------------------------------------------------

  defp spawn_task(_socket, name, kind, body, supervisor, timeout) do
    %Task{ref: ref, pid: pid} =
      Task.Supervisor.async_nolink(supervisor, fn ->
        {:musubi_async_result, name, kind, body.()}
      end)

    timer_ref =
      case timeout do
        nil ->
          nil

        ms when is_integer(ms) and ms > 0 ->
          Process.send_after(self(), {:musubi_async_timeout, ref}, ms)
      end

    {ref, pid, timer_ref}
  end

  defp assign_task_body(fun, multi_keys) do
    fn ->
      try do
        result =
          case fun.() do
            {:ok, %{} = value_map} = ok when is_list(multi_keys) ->
              ensure_assign_result_keys!(multi_keys, value_map)
              ok

            {:ok, _value} = ok ->
              ok

            {:error, _reason} = err ->
              err

            other ->
              other
          end

        {:ok, result}
      rescue
        e -> {:exit, {:error, e, __STACKTRACE__}}
      catch
        :throw, val -> {:exit, {{:nocatch, val}, __STACKTRACE__}}
        :exit, reason -> {:exit, reason}
      end
    end
  end

  defp start_task_body(fun) do
    fn ->
      try do
        {:ok, fun.()}
      rescue
        e -> {:exit, {:error, e, __STACKTRACE__}}
      catch
        :throw, val -> {:exit, {{:nocatch, val}, __STACKTRACE__}}
        :exit, reason -> {:exit, reason}
      end
    end
  end

  defp stream_task_body(fun) do
    fn ->
      try do
        result =
          case fun.() do
            {:ok, items} = ok ->
              ensure_stream_items_enumerable!(items)
              ok

            {:ok, items, opts} = ok when is_list(opts) ->
              ensure_stream_items_enumerable!(items)
              ok

            {:error, _reason} = err ->
              err

            other ->
              other
          end

        {:ok, result}
      rescue
        e -> {:exit, {:error, e, __STACKTRACE__}}
      catch
        :throw, val -> {:exit, {{:nocatch, val}, __STACKTRACE__}}
        :exit, reason -> {:exit, reason}
      end
    end
  end

  defp kill_task(pid, reason) when is_pid(pid) do
    if Process.alive?(pid), do: Process.exit(pid, reason)
    :ok
  end

  defp ensure_assign_result_keys!(keys, value_map) do
    case missing_assign_key(keys, value_map) do
      nil ->
        :ok

      key ->
        raise ArgumentError,
              "assign_async multi-key result missing key #{inspect(key)} in #{inspect(value_map)}"
    end
  end

  defp ensure_stream_items_enumerable!(items) do
    case Enumerable.impl_for(items) do
      nil ->
        raise ArgumentError,
              "stream_async items must be enumerable, got: #{inspect(items)}"

      _impl ->
        :ok
    end
  end

  defp cancel_timer(nil), do: :ok

  defp cancel_timer(ref) when is_reference(ref) do
    _cancel = Process.cancel_timer(ref)
    :ok
  end

  # ---------------------------------------------------------------------------
  # Internal: validation
  # ---------------------------------------------------------------------------

  defp validate_keys!(keys, fun_name) do
    if keys == [] do
      raise ArgumentError, "#{fun_name} key list must be non-empty"
    end

    Enum.each(keys, fn
      key when is_atom(key) -> :ok
      other -> raise ArgumentError, "#{fun_name} keys must be atoms, got: #{inspect(other)}"
    end)
  end

  defp ensure_stream_declared!(%Socket{module: nil}, name) do
    raise ArgumentError,
          "stream_async :#{name} requires the socket to carry a module — call from inside a store handler."
  end

  defp ensure_stream_declared!(%Socket{module: module}, name)
       when is_atom(module) and is_atom(name) do
    has_config? =
      function_exported?(module, :__musubi_stream_config__, 1) and
        config_for(module, name) != nil

    unless has_config? do
      raise ArgumentError,
            "stream_async :#{name} called on #{inspect(module)} but no matching " <>
              "`stream :#{name}, ...` declaration was found inside `state do`."
    end

    :ok
  end

  defp config_for(module, name) do
    module.__musubi_stream_config__(name)
  rescue
    _error -> nil
  end

  defp find_name_for_async_result(socket, %AsyncResult{} = ar) do
    socket
    |> tracking()
    |> Enum.find_value(:error, fn {name, %{keys: keys}} ->
      cond do
        is_nil(keys) ->
          nil

        Enum.any?(keys, fn key -> Map.get(socket.assigns, key) === ar end) ->
          {:ok, name}

        true ->
          nil
      end
    end)
  end

  # ---------------------------------------------------------------------------
  # Compile-time entry points used by `Musubi.Store`'s facade macros.
  #
  # Each `__<name>_async__/5` is invoked from within a `defmacro` and runs at
  # compile time. It runs the socket-capture lint against the user fun AST and
  # returns the quoted runtime call. Keeping the lint here (rather than in a
  # standalone macros module) means the facade only has to ferry `__CALLER__`.
  # ---------------------------------------------------------------------------

  @doc false
  @spec __assign_async__(Macro.t(), Macro.t(), Macro.t(), Macro.t(), Macro.Env.t()) :: Macro.t()
  def __assign_async__(socket, key_or_keys, fun, opts, caller_env) do
    __warn_on_socket_capture__(fun, :assign_async, caller_env)

    quote do
      Musubi.Async.assign_async(
        unquote(socket),
        unquote(key_or_keys),
        unquote(fun),
        unquote(opts)
      )
    end
  end

  @doc false
  @spec __start_async__(Macro.t(), Macro.t(), Macro.t(), Macro.t(), Macro.Env.t()) :: Macro.t()
  def __start_async__(socket, name, fun, opts, caller_env) do
    __warn_on_socket_capture__(fun, :start_async, caller_env)

    quote do
      Musubi.Async.start_async(
        unquote(socket),
        unquote(name),
        unquote(fun),
        unquote(opts)
      )
    end
  end

  @doc false
  @spec __stream_async__(Macro.t(), Macro.t(), Macro.t(), Macro.t(), Macro.Env.t()) :: Macro.t()
  def __stream_async__(socket, name, fun, opts, caller_env) do
    __warn_on_socket_capture__(fun, :stream_async, caller_env)

    quote do
      Musubi.Async.stream_async(
        unquote(socket),
        unquote(name),
        unquote(fun),
        unquote(opts)
      )
    end
  end

  # Warn at compile time when the user fun closes over `socket`. Captured
  # sockets are frozen snapshots and risk data races against the next
  # handler's mutations — bind locals before the fn instead.
  @spec __warn_on_socket_capture__(Macro.t(), atom(), Macro.Env.t()) :: :ok
  defp __warn_on_socket_capture__(fun_ast, fun_name, caller) do
    if __captures_socket__(fun_ast) do
      IO.warn(
        "#{fun_name}/3,4: the task fn captures `socket`. " <>
          "Capturing the socket inside an async fun frozen at call time risks data races; " <>
          "bind the values you need to local variables before the fn instead.",
        Macro.Env.stacktrace(caller)
      )
    end

    :ok
  end

  # Only walk literal `fn …` or `&…` captures so calls like
  # `start_async(socket, :foo, build_fn(socket))` — where `socket` flows
  # through a helper rather than being captured by the task fun — don't
  # trigger a false warning.
  @spec __captures_socket__(Macro.t()) :: boolean()
  defp __captures_socket__({:fn, _meta, _clauses} = ast), do: __walk_for_socket__(ast)
  defp __captures_socket__({:&, _meta, _args} = ast), do: __walk_for_socket__(ast)
  defp __captures_socket__(_other), do: false

  @spec __walk_for_socket__(Macro.t()) :: boolean()
  defp __walk_for_socket__(ast) do
    {_ast, captured?} =
      Macro.prewalk(ast, false, fn
        {:socket, _meta, ctx} = node, _acc when is_atom(ctx) ->
          {node, true}

        node, acc ->
          {node, acc}
      end)

    captured?
  end
end
