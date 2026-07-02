defmodule Musubi.Lifecycle do
  @moduledoc """
  Lifecycle hook helpers for Musubi runtime stages.

  ## Stages

  | Stage              | Arity | Hook arguments                          |
  | :----------------- | :---- | :-------------------------------------- |
  | `:before_command`  | 3     | `(command_name, payload, socket)`       |
  | `:after_command`   | 4     | `(command_name, payload, reply, socket)`|
  | `:handle_async`    | 3     | `(name, async_result, socket)`          |
  | `:handle_info`     | 2     | `(message, socket)`                     |
  | `:after_render`   | 2     | `(frame, socket)`                       |
  | `:after_serialize` | 2     | `(frame, socket)`                       |

  `:after_render` and `:after_serialize` are the two *outbound* stages: they run
  per store socket each render cycle over that socket's `Musubi.Page.Frame` — its
  render output plus the push events (`Musubi.Event`, BDR-0032) it queued.
  `:after_render` sees the Elixir-form frame (atom keys, structs, atom values,
  native event payloads); `:after_serialize` sees the wire-form frame after
  `Musubi.Wire.to_wire/1` (string keys, plain maps, atoms-as-strings).

  Both are *transform* stages — run them with `run_transform_hooks/3`, and each
  hook returns `{:cont | :halt, frame, socket}` so it may rewrite, drop, or enrich
  the outbound frame (render redaction, event audit/validation, telemetry).
  Default render and push-event payload validation are attached at
  `:after_serialize`.
  """

  alias Musubi.Socket

  @type stage() ::
          :before_command
          | :after_command
          | :handle_async
          | :handle_info
          | :after_render
          | :after_serialize

  @type hook_id() :: term()
  @type hook_result() :: {:cont, Socket.t()} | {:halt, Socket.t()} | {:halt, term(), Socket.t()}
  @type hook_fun() :: function()
  @type hook_entry() :: %{id: hook_id(), fun: hook_fun()}
  @type hook_table() :: %{optional(stage()) => [hook_entry()]}

  @stages [
    :before_command,
    :after_command,
    :handle_async,
    :handle_info,
    :after_render,
    :after_serialize
  ]
  @stage_arity %{
    before_command: 3,
    after_command: 4,
    handle_async: 3,
    handle_info: 2,
    after_render: 2,
    after_serialize: 2
  }

  # Outbound stages that thread their data payload through the chain (via
  # `run_transform_hooks/3`) instead of only the socket. Each hook returns
  # `{:cont | :halt, datum, socket}` so it can rewrite the outbound frame.
  @transform_stages [:after_render, :after_serialize]

  @doc """
  Attaches a lifecycle hook for the given stage.

  ## Examples

      iex> socket = %Musubi.Socket{}
      iex> socket =
      ...>   Musubi.Lifecycle.attach_hook(socket, :audit, :after_render, fn _output, socket ->
      ...>     {:cont, socket}
      ...>   end)
      iex> Musubi.Socket.get_private(socket, :hooks)[:after_render] |> length()
      1
  """
  @spec attach_hook(Socket.t(), hook_id(), stage(), hook_fun()) :: Socket.t()
  def attach_hook(%Socket{} = socket, id, stage, fun)
      when stage in @stages and is_function(fun) do
    validate_hook_arity!(stage, fun)
    hooks = hooks(socket)
    stage_hooks = Map.get(hooks, stage, [])

    if Enum.any?(stage_hooks, &(&1.id == id)) do
      raise ArgumentError, "hook #{inspect(id)} already attached for stage #{inspect(stage)}"
    end

    next_stage_hooks = List.insert_at(stage_hooks, -1, %{id: id, fun: fun})
    put_hooks(socket, Map.put(hooks, stage, next_stage_hooks))
  end

  @doc """
  Detaches a lifecycle hook when one is present.

  ## Examples

      iex> socket =
      ...>   Musubi.Lifecycle.attach_hook(%Musubi.Socket{}, :audit, :after_render, fn _output, socket ->
      ...>     {:cont, socket}
      ...>   end)
      iex> socket = Musubi.Lifecycle.detach_hook(socket, :audit, :after_render)
      iex> Musubi.Socket.get_private(socket, :hooks)
      %{}
  """
  @spec detach_hook(Socket.t(), hook_id(), stage()) :: Socket.t()
  def detach_hook(%Socket{} = socket, id, stage) when stage in @stages do
    hooks = hooks(socket)
    stage_hooks = Map.get(hooks, stage, [])
    filtered_hooks = Enum.reject(stage_hooks, &(&1.id == id))

    if filtered_hooks == stage_hooks do
      socket
    else
      next_hooks =
        case filtered_hooks do
          [] -> Map.delete(hooks, stage)
          _hooks -> Map.put(hooks, stage, filtered_hooks)
        end

      put_hooks(socket, next_hooks)
    end
  end

  @doc """
  Runs every socket-only hook registered for a stage until one halts or all
  continue. For the transform stages (`:after_render`, `:after_serialize`) use
  `run_transform_hooks/3` instead — they thread a data frame, not just the socket.

  ## Examples

      iex> socket =
      ...>   Musubi.Lifecycle.attach_hook(%Musubi.Socket{}, :mark, :handle_info, fn _msg, socket ->
      ...>     {:cont, Musubi.Socket.assign(socket, :seen?, true)}
      ...>   end)
      iex> {:cont, socket} = Musubi.Lifecycle.run_hooks(socket, :handle_info, [:ping], false)
      iex> socket.assigns.seen?
      true
  """
  @spec run_hooks(Socket.t(), stage(), list(), boolean()) ::
          {:cont, Socket.t()} | {:halt, Socket.t()} | {:halt, term(), Socket.t()}
  def run_hooks(%Socket{} = socket, stage, hook_args, halt_payloads_allowed?)
      when stage in @stages and is_list(hook_args) and is_boolean(halt_payloads_allowed?) do
    socket
    |> hooks()
    |> Map.get(stage, [])
    |> Enum.reduce_while({:cont, socket}, fn %{fun: fun}, {:cont, current_socket} ->
      # credo:disable-for-next-line Credo.Check.Refactor.AppendSingleItem
      case apply(fun, hook_args ++ [current_socket]) do
        {:cont, %Socket{} = next_socket} ->
          {:cont, {:cont, next_socket}}

        {:halt, %Socket{} = next_socket} ->
          {:halt, {:halt, next_socket}}

        {:halt, reply, %Socket{} = next_socket} when halt_payloads_allowed? ->
          {:halt, {:halt, reply, next_socket}}

        {:halt, _reply, %Socket{}} ->
          raise ArgumentError,
                "halt payloads are only allowed when halt_payloads_allowed? is true"

        other ->
          raise ArgumentError, "invalid hook result: #{inspect(other)}"
      end
    end)
  end

  @doc """
  Runs a transform stage (`:after_render` / `:after_serialize`) over an outbound
  `datum` (a `Musubi.Page.Frame`).

  Unlike `run_hooks/4` (which only threads the socket), this folds the datum
  through each hook: a hook receives `(datum, socket)` and returns
  `{:cont, datum, socket}` to continue with a possibly-rewritten datum, or
  `{:halt, datum, socket}` to stop the chain early. Returns the final
  `{datum, socket}`. With no hooks attached it returns the datum unchanged.

  ## Examples

      iex> socket =
      ...>   Musubi.Lifecycle.attach_hook(%Musubi.Socket{}, :bump, :after_render, fn frame, socket ->
      ...>     {:cont, Map.put(frame, :seen, true), socket}
      ...>   end)
      iex> {frame, _socket} = Musubi.Lifecycle.run_transform_hooks(socket, :after_render, %{render: %{}})
      iex> frame.seen
      true
  """
  @spec run_transform_hooks(Socket.t(), stage(), term()) :: {term(), Socket.t()}
  def run_transform_hooks(%Socket{} = socket, stage, datum) when stage in @transform_stages do
    socket
    |> hooks()
    |> Map.get(stage, [])
    |> Enum.reduce_while({datum, socket}, fn %{fun: fun}, {current_datum, current_socket} ->
      case fun.(current_datum, current_socket) do
        {:cont, next_datum, %Socket{} = next_socket} ->
          {:cont, {next_datum, next_socket}}

        {:halt, next_datum, %Socket{} = next_socket} ->
          {:halt, {next_datum, next_socket}}

        other ->
          raise ArgumentError, "invalid #{inspect(stage)} hook result: #{inspect(other)}"
      end
    end)
  end

  @doc """
  Attaches an application-configured hook list to a socket.

  `config_key` is a `:musubi` application env key holding `{id, stage, fun}`
  entries:

    * `:default_hooks` — root-only concerns (command/reply schema validation,
      whole-tree render validation), attached to the root socket at mount.
    * `:store_hooks` — per-store concerns (push-event validation), attached to
      every store socket (root + each child at creation) so each store validates
      its own events.
  """
  @spec attach_hooks(Socket.t(), atom()) :: Socket.t()
  def attach_hooks(%Socket{} = socket, config_key) when is_atom(config_key) do
    :musubi
    |> Application.get_env(config_key, [])
    |> Enum.reduce(socket, fn {id, stage, fun}, acc -> attach_hook(acc, id, stage, fun) end)
  end

  @doc """
  Returns the supported lifecycle stages in execution order.

  ## Examples

      iex> Musubi.Lifecycle.stages()
      [:before_command, :after_command, :handle_async, :handle_info, :after_render, :after_serialize]
  """
  @spec stages() :: [stage()]
  def stages, do: @stages

  @doc """
  Returns the required hook function arity for a lifecycle stage.

  | Stage              | Arity | Hook arguments                          |
  | :----------------- | :---- | :-------------------------------------- |
  | `:before_command`  | 3     | `(command_name, payload, socket)`       |
  | `:after_command`   | 4     | `(command_name, payload, reply, socket)`|
  | `:handle_async`    | 3     | `(name, async_result, socket)`          |
  | `:handle_info`     | 2     | `(message, socket)`                     |
  | `:after_render`   | 2     | `(frame, socket)`                       |
  | `:after_serialize` | 2     | `(frame, socket)`                       |

  ## Examples

      iex> Musubi.Lifecycle.stage_arity(:before_command)
      3
      iex> Musubi.Lifecycle.stage_arity(:after_command)
      4
      iex> Musubi.Lifecycle.stage_arity(:after_serialize)
      2
  """
  @spec stage_arity(stage()) :: 2 | 3 | 4
  def stage_arity(stage) when stage in @stages do
    Map.fetch!(@stage_arity, stage)
  end

  def stage_arity(stage) do
    raise ArgumentError, "unknown lifecycle stage: #{inspect(stage)}"
  end

  @spec hooks(Socket.t()) :: hook_table()
  defp hooks(%Socket{private: private}), do: Map.get(private, :hooks, %{})

  @spec validate_hook_arity!(stage(), function()) :: :ok
  defp validate_hook_arity!(stage, fun) when is_function(fun) do
    expected_arity = stage_arity(stage)
    {:arity, actual_arity} = :erlang.fun_info(fun, :arity)

    if actual_arity == expected_arity do
      :ok
    else
      raise ArgumentError,
            "expected fun arity #{expected_arity} for stage #{inspect(stage)}, got arity #{actual_arity}"
    end
  end

  @spec put_hooks(Socket.t(), hook_table()) :: Socket.t()
  defp put_hooks(%Socket{private: private} = socket, hooks) do
    %{socket | private: Map.put(private, :hooks, hooks)}
  end
end
