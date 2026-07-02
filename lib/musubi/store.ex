defmodule Musubi.Store do
  @moduledoc """
  Compile-time DSL entrypoint, behaviour contract, and runtime facade for
  Musubi store modules.

  Stores `use Musubi.Store` and implement `init/1`, `render/1`,
  `update/2`, `handle_command/3`, `handle_async/3`, `handle_info/2`,
  and `terminate/2`. Root stores opt in with `use Musubi.Store, root: true`
  and may also implement `mount/2` to receive client mount params before
  `init/1` runs.

  `render/1` returns the resolved Elixir-shaped term; wire conversion happens
  separately via `Musubi.Wire.to_wire/1`.

  ## Runtime facade

  `use Musubi.Store` blanket-imports this module so every helper below is
  available bare inside a store's callbacks. Each helper is a `defdelegate`
  to the underlying implementation module (`Musubi.Socket`, `Musubi.Stream`,
  `Musubi.Lifecycle`, `Musubi.Child`, `Musubi.DSL.Render`) or a `defmacro`
  that lowers to a runtime call (the async lifecycle helpers).

  | Surface          | Helpers                                                                                            |
  | :--------------- | :------------------------------------------------------------------------------------------------- |
  | Socket           | `assign/2,3`, `assign_new/3`, `update/3`, `changed?/2`, `get_private/2,3`, `put_private/3`         |
  | Streams          | `stream/3,4`, `stream_configure/3`, `stream_insert/3,4`, `stream_delete/3`, `stream_delete_by_item_key/3` |
  | Lifecycle        | `attach_hook/4`, `detach_hook/3`                                                                   |
  | Async            | `assign_async/3,4`, `start_async/3,4`, `stream_async/3,4`, `cancel_async/2,3`                      |
  | Render builders  | `child/2`, `stream/1`, `async_stream/1`                                                            |

  The fully-qualified module forms remain available — the facade is purely
  additive — so `Musubi.Socket.assign(...)` keeps working alongside the bare
  `assign(...)` form preferred inside store modules.
  """

  alias Musubi.Async
  alias Musubi.Lifecycle
  alias Musubi.Socket
  alias Musubi.Stream

  @type value() ::
          nil
          | boolean()
          | number()
          | String.t()
          | atom()
          | pid()
          | reference()
          | port()
          | tuple()
          | [value()]
          | %{optional(value()) => value()}

  @type assigns() :: %{optional(Socket.assign_key()) => value()}
  @type async_name() :: Async.name_arg()
  @type async_result() :: {:ok, value()} | {:exit, value()}
  @type command_name() :: atom()
  @type command_payload() :: %{optional(String.t() | atom()) => value()}
  @type command_reply() :: map()
  @type message() :: value()
  @type rendered() ::
          nil
          | boolean()
          | number()
          | String.t()
          | atom()
          | [rendered()]
          | %{optional(String.t() | atom()) => rendered()}
  @type root_params() :: %{optional(String.t()) => value()}
  @type terminate_reason() :: :normal | :shutdown | {:shutdown, value()} | value()

  @doc """
  Mounts a root store with client-supplied params.

  Only modules declared with `use Musubi.Store, root: true` receive this
  callback. Child stores use `init/1`.
  """
  @callback mount(params :: root_params(), socket :: Socket.t()) :: {:ok, Socket.t()}

  @doc """
  Initializes a freshly-created store socket.
  """
  @callback init(socket :: Socket.t()) :: {:ok, Socket.t()}

  @doc """
  Initializes a freshly-mounted store socket.

  This callback is kept for compatibility with pre-session stores. Prefer
  `init/1` for child stores and `mount/2` for root-only client params.
  """
  @callback mount(socket :: Socket.t()) :: {:ok, Socket.t()}

  @doc """
  Produces the resolved Elixir-shaped render output for the current store.

  The returned term is still in Musubi's Elixir form. The runtime converts it
  to wire form later with `Musubi.Wire.to_wire/1`.
  """
  @callback render(socket :: Socket.t()) :: rendered()

  @doc """
  Updates a mounted store socket from new parent-supplied assigns.
  """
  @callback update(assigns :: assigns(), socket :: Socket.t()) :: {:ok, Socket.t()}

  @doc """
  Handles a declared command for the current store.
  """
  @callback handle_command(
              name :: command_name(),
              payload :: command_payload(),
              socket :: Socket.t()
            ) ::
              {:noreply, Socket.t()} | {:reply, command_reply(), Socket.t()}

  @doc """
  Handles an async result routed to the current store.
  """
  @callback handle_async(
              name :: async_name(),
              async_fun_result :: async_result(),
              socket :: Socket.t()
            ) ::
              {:noreply, Socket.t()}

  @doc """
  Handles an in-process message routed to the current store.
  """
  @callback handle_info(message :: message(), socket :: Socket.t()) :: {:noreply, Socket.t()}

  @doc """
  Handles store teardown after the page runtime begins terminating.
  """
  @callback terminate(reason :: terminate_reason(), socket :: Socket.t()) :: :ok

  @doc """
  Optional. Invoked when a chunk has been received (channel mode) or a
  client `upload_progress` message arrived (external mode) for an
  upload-tracked entry.
  """
  @callback handle_progress(
              name :: atom(),
              entry :: Musubi.Upload.Entry.t(),
              socket :: Socket.t()
            ) :: {:noreply, Socket.t()}

  @doc """
  Optional. When defined for an upload name, the preflight switches to
  external mode and forwards the returned `meta` map to the client
  uploader. Musubi treats `meta` opaquely.
  """
  @callback upload_external(
              name :: atom(),
              entry :: Musubi.Upload.Entry.t(),
              socket :: Socket.t()
            ) :: {:ok, map(), Socket.t()}

  @optional_callbacks mount: 2,
                      init: 1,
                      mount: 1,
                      update: 2,
                      handle_async: 3,
                      handle_info: 2,
                      terminate: 2,
                      handle_progress: 3,
                      upload_external: 3

  # ---------------------------------------------------------------------------
  # Socket helpers (defdelegate -> Musubi.Socket)
  # ---------------------------------------------------------------------------

  @doc "See `Musubi.Socket.assign/2`."
  defdelegate assign(socket, attrs), to: Socket

  @doc "See `Musubi.Socket.assign/3`."
  defdelegate assign(socket, key, value), to: Socket

  @doc "See `Musubi.Socket.assign_new/3`."
  defdelegate assign_new(socket, key, fun), to: Socket

  @doc "See `Musubi.Socket.update/3`."
  defdelegate update(socket, key, fun), to: Socket

  @doc "See `Musubi.Socket.changed?/2`."
  defdelegate changed?(socket, key), to: Socket

  @doc "See `Musubi.Socket.get_private/3`."
  defdelegate get_private(socket, key, default \\ nil), to: Socket

  @doc "See `Musubi.Socket.put_private/3`."
  defdelegate put_private(socket, key, value), to: Socket

  # ---------------------------------------------------------------------------
  # Stream helpers (defdelegate -> Musubi.Stream)
  # ---------------------------------------------------------------------------

  @doc "See `Musubi.Stream.stream/4`."
  defdelegate stream(socket, name, items, opts \\ []), to: Stream

  @doc "See `Musubi.Stream.stream_configure/3`."
  defdelegate stream_configure(socket, name, opts), to: Stream

  @doc "See `Musubi.Stream.stream_insert/4`."
  defdelegate stream_insert(socket, name, item, opts \\ []), to: Stream

  @doc "See `Musubi.Stream.stream_delete/3`."
  defdelegate stream_delete(socket, name, item), to: Stream

  @doc "See `Musubi.Stream.stream_delete_by_item_key/3`."
  defdelegate stream_delete_by_item_key(socket, name, item_key), to: Stream

  # ---------------------------------------------------------------------------
  # Push-event helper (defdelegate -> Musubi.Event)
  # ---------------------------------------------------------------------------

  @doc "See `Musubi.Event.push_event/3`."
  defdelegate push_event(socket, name, payload), to: Musubi.Event

  # ---------------------------------------------------------------------------
  # Upload helpers (defdelegate -> Musubi.Upload)
  # ---------------------------------------------------------------------------

  @doc """
  Returns `{completed, in_progress}` for the upload named `name`.

  ## Examples

      {completed, _in_progress} = uploaded_entries(socket, :avatar)
  """
  defdelegate uploaded_entries(socket, name), to: Musubi.Upload

  @doc """
  Consumes the completed upload entries for `name` via `fun`.

  `fun` receives `(meta, entry)` where `meta` is `%{path: path}` in
  channel mode or `%{external: meta}` in external mode. Returns
  `{:ok, val}` to consume the entry (removing it from internal state)
  or `{:postpone, val}` to leave it in place.

  Returns the updated socket and the list of `val`s produced.

  Must be called from a command handler.
  """
  defdelegate consume_uploaded_entries(socket, name, fun), to: Musubi.Upload

  @doc """
  Cancels a single upload entry by `ref`, emitting `{op: cancel}`.
  """
  defdelegate cancel_upload(socket, name, ref), to: Musubi.Upload

  # ---------------------------------------------------------------------------
  # Lifecycle helpers (defdelegate -> Musubi.Lifecycle)
  # ---------------------------------------------------------------------------

  @doc "See `Musubi.Lifecycle.attach_hook/4`."
  defdelegate attach_hook(socket, name, stage, fun), to: Lifecycle

  @doc "See `Musubi.Lifecycle.detach_hook/3`."
  defdelegate detach_hook(socket, name, stage), to: Lifecycle

  # ---------------------------------------------------------------------------
  # Async helpers — defmacros that ferry `__CALLER__` into
  # `Musubi.Async.__<name>__/5` so the socket-capture lint runs at compile
  # time before lowering to the runtime call.
  # ---------------------------------------------------------------------------

  @doc "See `Musubi.Async.assign_async/3,4`."
  defmacro assign_async(socket, key_or_keys, fun) do
    Musubi.Async.__assign_async__(socket, key_or_keys, fun, [], __CALLER__)
  end

  defmacro assign_async(socket, key_or_keys, fun, opts) do
    Musubi.Async.__assign_async__(socket, key_or_keys, fun, opts, __CALLER__)
  end

  @doc "See `Musubi.Async.start_async/3,4`."
  defmacro start_async(socket, name, fun) do
    Musubi.Async.__start_async__(socket, name, fun, [], __CALLER__)
  end

  defmacro start_async(socket, name, fun, opts) do
    Musubi.Async.__start_async__(socket, name, fun, opts, __CALLER__)
  end

  @doc "See `Musubi.Async.stream_async/3,4`."
  defmacro stream_async(socket, name, fun) do
    Musubi.Async.__stream_async__(socket, name, fun, [], __CALLER__)
  end

  defmacro stream_async(socket, name, fun, opts) do
    Musubi.Async.__stream_async__(socket, name, fun, opts, __CALLER__)
  end

  @doc "See `Musubi.Async.cancel_async/2,3`."
  defdelegate cancel_async(socket, target), to: Async
  defdelegate cancel_async(socket, target, reason), to: Async

  # ---------------------------------------------------------------------------
  # Render builders (defdelegate / defmacro -> Musubi.Child / Musubi.DSL.Render)
  # ---------------------------------------------------------------------------

  @doc "See `Musubi.Child.child/2`."
  defdelegate child(module, opts), to: Musubi.Child

  @doc "See `Musubi.DSL.Render.stream/1`. Render-time placeholder."
  defmacro stream(name) when is_atom(name) do
    quote do
      %Musubi.Stream.Placeholder{name: unquote(name)}
    end
  end

  @doc "See `Musubi.DSL.Render.async_stream/1`. Render-time placeholder."
  defmacro async_stream(name) when is_atom(name) do
    quote do
      %Musubi.Stream.AsyncPlaceholder{name: unquote(name)}
    end
  end

  @doc false
  @spec __before_compile__(Macro.Env.t()) :: Macro.t()
  defmacro __before_compile__(env) do
    root? = Module.get_attribute(env.module, :__musubi_root__) || false

    if not root? and Module.defines?(env.module, {:mount, 2}, :def) do
      raise CompileError,
        file: env.file,
        line: env.line,
        description:
          "mount/2 is only allowed on root Musubi stores; " <>
            "declare `use Musubi.Store, root: true` before defining it"
    end

    validate_uploads_against_fields!(env)

    quote(do: :ok)
  end

  @spec validate_uploads_against_fields!(Macro.Env.t()) :: :ok
  defp validate_uploads_against_fields!(%Macro.Env{} = env) do
    uploads = Module.get_attribute(env.module, :__musubi_uploads__) || []
    fields = Module.get_attribute(env.module, :__musubi_fields__) || []
    field_names = MapSet.new(fields, & &1.name)

    Enum.each(uploads, fn {name, _config, file, line} ->
      if MapSet.member?(field_names, name) do
        raise CompileError,
          file: file,
          line: line,
          description:
            "upload :#{name} name collides with state field :#{name} on " <>
              "#{inspect(env.module)}; uploads and state fields share the " <>
              "`page.<name>` namespace and must be uniquely named"
      end
    end)

    :ok
  end

  @spec __using__(keyword()) :: Macro.t()
  defmacro __using__(opts) do
    root? = Keyword.get(opts, :root, false)

    quote bind_quoted: [root?: root?] do
      use TypedStructor
      @behaviour Musubi.Store

      # LV-style single-facade import: every runtime helper a store calls
      # (assign, stream, attach_hook, assign_async, child, …) flows through
      # `Musubi.Store`. The compile-time DSL macros stay filtered because they
      # are only valid in specific syntactic positions.
      import Musubi.Store
      import Musubi.DSL.Command, only: [command: 1, command: 2]
      import Musubi.DSL.Event, only: [event: 1, event: 2]
      import Musubi.DSL.State, only: [state: 1]
      import Musubi.DSL.Attr, only: [attr: 2, attr: 3]
      import Musubi.DSL.Upload, only: [upload: 2]

      Module.register_attribute(__MODULE__, :__musubi_fields__, accumulate: false)
      Module.register_attribute(__MODULE__, :__musubi_commands__, accumulate: true)
      Module.register_attribute(__MODULE__, :__musubi_command_payload_fields__, accumulate: true)
      Module.register_attribute(__MODULE__, :__musubi_command_reply_fields__, accumulate: true)
      Module.register_attribute(__MODULE__, :__musubi_command_field_target__, accumulate: false)
      Module.register_attribute(__MODULE__, :__musubi_events__, accumulate: true)
      Module.register_attribute(__MODULE__, :__musubi_event_payload_fields__, accumulate: true)
      Module.register_attribute(__MODULE__, :__musubi_attrs__, accumulate: true)
      Module.register_attribute(__MODULE__, :__musubi_uploads__, accumulate: false)
      Module.put_attribute(__MODULE__, :__musubi_kind__, :store)
      Module.put_attribute(__MODULE__, :__musubi_root__, root?)

      @after_verify {Musubi.Type, :verify_module!}

      @doc false
      @spec __musubi_runtime_module__() :: boolean()
      def __musubi_runtime_module__, do: false

      @doc false
      @spec __musubi_kind__() :: :store
      def __musubi_kind__, do: :store

      if @__musubi_root__ do
        @impl Musubi.Store
        @doc false
        @spec mount(Musubi.Store.root_params(), Musubi.Socket.t()) :: {:ok, Musubi.Socket.t()}
        def mount(_params, socket) do
          {:ok, socket}
        end
      end

      @impl Musubi.Store
      @doc false
      @spec init(Musubi.Socket.t()) :: {:ok, Musubi.Socket.t()}
      def init(socket) do
        if function_exported?(__MODULE__, :mount, 1) do
          apply(__MODULE__, :mount, [socket])
        else
          {:ok, socket}
        end
      end

      @impl Musubi.Store
      @doc false
      @spec terminate(Musubi.Store.terminate_reason(), Musubi.Socket.t()) :: :ok
      def terminate(_reason, _socket), do: :ok

      if @__musubi_root__ do
        defoverridable mount: 2, init: 1, terminate: 2
      else
        defoverridable init: 1, terminate: 2
      end

      @before_compile Musubi.Store
      @before_compile Musubi.Plugin.Reflection
    end
  end
end
