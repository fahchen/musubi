defmodule Musubi.Diff do
  @moduledoc """
  Computes the JSON Patch (RFC 6902) diff between two wire-form terms.

  Wraps `Jsonpatch.diff/2` and post-filters the result:
  Musubi only emits `add`, `remove`, and `replace` ops. `move`, `copy`, and
  `test` are filtered out unconditionally. There is no op-count threshold or
  byte threshold and no subtree-replace fallback — the structural minimal diff
  is what reaches the wire.

  Reorders surface as per-index `replace` ops (whatever `Jsonpatch.diff/2`
  produces). JSON Pointer escaping for `/` and `~` is delegated to the
  `jsonpatch` library; store authors must not assemble paths manually.

  ## Examples

      iex> Musubi.Diff.diff(%{"a" => 1}, %{"a" => 2})
      [%{op: "replace", path: "/a", value: 2}]

      iex> Musubi.Diff.diff(%{"a/b" => 1}, %{"a/b" => 2})
      [%{op: "replace", path: "/a~1b", value: 2}]

      iex> Musubi.Diff.diff(%{"a" => 1}, %{"a" => 1})
      []
  """

  alias Musubi.Telemetry

  @typedoc "RFC 6902 op kind that Musubi emits — `move`/`copy`/`test` are excluded."
  @type op_kind() :: :add | :remove | :replace

  @typedoc """
  An op map matching the wire envelope shape. Atom keys; the `op` value is the
  RFC 6902 op kind as a binary so client materializers do not need to map
  atoms.
  """
  @type op() :: %{
          required(:op) => String.t(),
          required(:path) => String.t(),
          optional(:value) => term()
        }

  @disallowed_ops ~w(move copy test)

  @doc """
  Returns the Musubi-allowed RFC 6902 ops describing the change from `previous`
  to `current`.

  Both inputs must be wire-form terms (string keys, plain maps, atoms encoded
  as strings) — pass values that have already gone through `Musubi.Wire.to_wire/1`
  in the page server's render cycle. Function values raise `ArgumentError`
  because RFC 6902 op `value`s must be JSON-serializable.

  ## Examples

      iex> Musubi.Diff.diff(%{"title" => "Inbox"}, %{"title" => "Outbox"})
      [%{op: "replace", path: "/title", value: "Outbox"}]

      iex> Musubi.Diff.diff(%{"items" => [1, 2, 3]}, %{"items" => [1, 9, 3]})
      [%{op: "replace", path: "/items/1", value: 9}]
  """
  @spec diff(term(), term()) :: [op()]
  def diff(previous, current) do
    started_at = System.monotonic_time()
    sanity_check!(previous)
    sanity_check!(current)

    ops =
      previous
      |> Jsonpatch.diff(current)
      |> Enum.flat_map(&filter_op/1)

    Telemetry.emit(
      [:musubi, :diff, :stop],
      %{duration: System.monotonic_time() - started_at, count: length(ops)},
      %{}
    )

    ops
  end

  @spec filter_op(map()) :: [op()]
  defp filter_op(%{op: op}) when op in @disallowed_ops, do: []

  defp filter_op(%{op: "remove", path: path}) when is_binary(path),
    do: [%{op: "remove", path: path}]

  defp filter_op(%{op: kind, path: path, value: value})
       when kind in ["add", "replace"] and is_binary(path),
       do: [%{op: kind, path: path, value: value}]

  defp filter_op(other) do
    raise ArgumentError, "unexpected op shape from Jsonpatch.diff/2: #{inspect(other)}"
  end

  @spec sanity_check!(term()) :: :ok
  defp sanity_check!(value) when is_function(value) do
    raise ArgumentError,
          "Musubi.Diff received a function value: #{inspect(value)}. " <>
            "Functions must be rejected by render-output validation before reaching the diff engine."
  end

  defp sanity_check!(value) when is_map(value) and not is_struct(value) do
    Enum.each(value, fn {_k, v} -> sanity_check!(v) end)
    :ok
  end

  defp sanity_check!(value) when is_list(value) do
    Enum.each(value, &sanity_check!/1)
    :ok
  end

  defp sanity_check!(_value), do: :ok
end
