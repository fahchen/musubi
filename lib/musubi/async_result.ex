defmodule Musubi.AsyncResult do
  @moduledoc """
  Three-field struct that tracks the lifecycle of an asynchronously-resolved
  socket assignment.

  Every assign written via `Musubi.Async.assign_async/3,4` (or seeded by
  `Musubi.Async.stream_async/3,4`) flows through one of the three statuses
  below so the client can pattern-match on a discriminated union.

  | `status`   | `result`                                     | `reason`                      |
  | :--------- | :------------------------------------------- | :---------------------------- |
  | `:loading` | prior `result` (or `nil`) — stale-while-load | `nil`                         |
  | `:ok`      | the value the user fun produced              | `nil`                         |
  | `:failed`  | prior `result` (or `nil`) — stale-while-fail | `{:error, _}` or `{:exit, _}` |

  Wire serialization preserves the three public fields and adds the
  runtime-only marker key `"__musubi_async__" => true` so clients can
  distinguish an AsyncResult payload from ordinary maps with similar keys.

  ## Compile-time type marker

  `Musubi.AsyncResult.of(t)` is also the typespec marker used inside `state do`
  field declarations (see `Musubi.DSL.State`). The runtime struct and the
  compile-time marker share a name on purpose: a field declared as
  `field :profile, AsyncResult.of(UserProfileState.t())` accepts an
  `%Musubi.AsyncResult{}` at runtime.

  ## Examples

      iex> Musubi.AsyncResult.loading()
      %Musubi.AsyncResult{status: :loading, result: nil, reason: nil}

      iex> prior = %Musubi.AsyncResult{status: :ok, result: "snapshot", reason: nil}
      iex> Musubi.AsyncResult.loading(prior)
      %Musubi.AsyncResult{status: :loading, result: "snapshot", reason: nil}

      iex> Musubi.AsyncResult.ok(nil, %{name: "ada"})
      %Musubi.AsyncResult{status: :ok, result: %{name: "ada"}, reason: nil}

      iex> Musubi.AsyncResult.failed("snapshot", {:error, :timeout})
      %Musubi.AsyncResult{status: :failed, result: "snapshot", reason: {:error, :timeout}}
  """

  use TypedStructor

  @typedoc "Discriminated-union status enum surfaced on the wire."
  @type status() :: :loading | :ok | :failed

  @typedoc "Failure classification returned by the runtime."
  @type failure_reason() ::
          {:error, term()}
          | {:exit, term()}

  @typedoc "Compile-time field-type marker used inside `state do` declarations."
  @type of(value) :: t(value)

  @typedoc "An AsyncResult whose result type is unconstrained."
  @type t() :: t(term())

  typed_structor do
    parameter :value

    field :status, status(),
      default: :loading,
      doc:
        "Discriminated-union tag the client pattern-matches on. Serialized as a string on the wire."

    field :result, value,
      doc:
        "The value produced by the user function (when `status: :ok`), or the prior `:ok` result preserved for stale-while-loading/failed UX."

    field :reason, failure_reason() | nil,
      default: nil,
      doc:
        "Failure classification when `status: :failed`. `nil` while `:loading` or `:ok`. `{:error, term}` for explicit user errors; `{:exit, term}` for cancels, timeouts, exceptions, throws, exits."
  end

  @doc """
  Returns a fresh `%AsyncResult{}` in the `:loading` status with no prior result.

  ## Examples

      iex> Musubi.AsyncResult.loading()
      %Musubi.AsyncResult{status: :loading, result: nil, reason: nil}
  """
  @spec loading() :: t()
  def loading, do: %__MODULE__{status: :loading, result: nil, reason: nil}

  @doc """
  Returns a `:loading` `%AsyncResult{}` that preserves the prior `result` for
  stale-while-loading UX.

  Accepts either a previously-resolved `%AsyncResult{}` (its `result` is
  carried forward) or any raw value (used directly as the prior `result`).

  ## Examples

      iex> Musubi.AsyncResult.loading(%Musubi.AsyncResult{status: :ok, result: "snapshot"})
      %Musubi.AsyncResult{status: :loading, result: "snapshot", reason: nil}

      iex> Musubi.AsyncResult.loading("raw")
      %Musubi.AsyncResult{status: :loading, result: "raw", reason: nil}

      iex> Musubi.AsyncResult.loading(nil)
      %Musubi.AsyncResult{status: :loading, result: nil, reason: nil}
  """
  @spec loading(t() | term()) :: t()
  def loading(%__MODULE__{result: prior}), do: %__MODULE__{status: :loading, result: prior}
  def loading(prior), do: %__MODULE__{status: :loading, result: prior}

  @doc """
  Returns a fresh `:ok` `%AsyncResult{}` carrying the produced value, with no
  prior state to carry forward.

  ## Examples

      iex> Musubi.AsyncResult.ok(42)
      %Musubi.AsyncResult{status: :ok, result: 42, reason: nil}
  """
  @spec ok(term()) :: t()
  def ok(value), do: %__MODULE__{status: :ok, result: value, reason: nil}

  @doc """
  Returns an `:ok` `%AsyncResult{}` carrying the produced value.

  When `prior` is an existing `%AsyncResult{}`, it is updated in place — status
  flipped to `:ok`, `reason` cleared, `result` replaced — mirroring
  `Phoenix.LiveView.AsyncResult.ok/2`. Any other `prior` (e.g. `nil`) builds a
  fresh struct.

  ## Examples

      iex> prior = %Musubi.AsyncResult{status: :failed, result: "stale", reason: {:error, :boom}}
      iex> Musubi.AsyncResult.ok(prior, %{name: "ada"})
      %Musubi.AsyncResult{status: :ok, result: %{name: "ada"}, reason: nil}

      iex> Musubi.AsyncResult.ok(nil, 42)
      %Musubi.AsyncResult{status: :ok, result: 42, reason: nil}
  """
  @spec ok(t() | term(), term()) :: t()
  def ok(%__MODULE__{} = prior, value),
    do: %{prior | status: :ok, result: value, reason: nil}

  def ok(_prior, value), do: %__MODULE__{status: :ok, result: value, reason: nil}

  @doc """
  Returns a `:failed` `%AsyncResult{}` that preserves the prior `result` for
  stale-while-failed UX. `reason` must be `{:error, term}` or `{:exit, term}`.

  ## Examples

      iex> Musubi.AsyncResult.failed("snapshot", {:error, :unauthorized})
      %Musubi.AsyncResult{status: :failed, result: "snapshot", reason: {:error, :unauthorized}}

      iex> prior = %Musubi.AsyncResult{status: :ok, result: "snapshot"}
      iex> Musubi.AsyncResult.failed(prior, {:exit, :timeout})
      %Musubi.AsyncResult{status: :failed, result: "snapshot", reason: {:exit, :timeout}}
  """
  @spec failed(t() | term(), failure_reason()) :: t()
  def failed(%__MODULE__{result: prior}, reason),
    do: %__MODULE__{status: :failed, result: prior, reason: reason}

  def failed(prior, reason),
    do: %__MODULE__{status: :failed, result: prior, reason: reason}

  defimpl Musubi.Wire do
    @moduledoc false
    @spec to_wire(Musubi.AsyncResult.t()) :: %{required(String.t()) => term()}
    def to_wire(%Musubi.AsyncResult{status: status, result: result, reason: reason}) do
      %{
        "__musubi_async__" => true,
        "status" => Atom.to_string(status),
        "result" => Musubi.Wire.to_wire(result),
        "reason" => reason_to_wire(reason)
      }
    end

    defp reason_to_wire(nil), do: nil

    defp reason_to_wire({kind, payload}) when kind in [:error, :exit] do
      %{"kind" => Atom.to_string(kind), "value" => safe_wire(payload)}
    end

    defp reason_to_wire(other), do: safe_wire(other)

    # The `reason` payload may carry arbitrary terms — exception structs,
    # tuples (`{kind, reason, stacktrace}`), pids, refs — none of which are
    # required to implement `Musubi.Wire`. Inspect everything that does not
    # round-trip through the protocol so the wire stays JSON-safe.
    defp safe_wire(value) do
      Musubi.Wire.to_wire(value)
    rescue
      Protocol.UndefinedError -> inspect(value)
    end
  end
end
