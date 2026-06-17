defmodule Musubi do
  @moduledoc """
  Server-authoritative, page-scoped runtime for Elixir/Phoenix with a
  framework-agnostic JavaScript client.

  ## The name

  `Musubi` (Japanese 結び) means *knot*, *bond*, or *connector* — the act
  of tying threads together at a point. In this runtime, every store is
  a musubi: a node that binds parent assigns to child renders, holds its
  own state, and lets reactive changes propagate through the bonds.

  ## What it does

  One BEAM process per connected page owns a tree of composable
  **stores**. Each store renders a piece of resolved state. When state
  changes, the runtime computes a structural diff against the previous
  wire output and pushes an RFC 6902 JSON Patch to the client over a
  `Phoenix.Channel`. The client materializes the patch into an
  immutable snapshot tree without prescribing a UI framework.

  Change tracking mirrors `Phoenix.LiveView`: per-key `__changed__`
  flags drive per-store memoization, so unchanged subtrees are reused
  verbatim and never re-rendered, re-serialized, or re-diffed.

  ## Core building blocks

    * `Musubi.Store` — define a store with `state do`, command
      handlers, optional async, and a `render/1` callback.
    * `Musubi.State` / `Musubi.Input` — typed state and command-input
      schemas.
    * `Musubi.Socket` — per-store state carrier with LV-aligned
      `assigns.__changed__` dirty tracking.
    * `Musubi.Page.Server` — the per-page runtime process.
    * `Musubi.Resolver` / `Musubi.Reconciler` — tree resolution and
      mount/update/reuse decisions.
    * `Musubi.Diff` — RFC 6902 add/remove/replace patch generator.
    * `Musubi.Wire` — protocol turning resolved Elixir terms into wire
      form.
    * `Musubi.Async` — `assign_async/3,4`, `start_async/3,4`,
      `cancel_async/2,3`, `stream_async/3,4`.
    * `Musubi.Stream` — LV-aligned streams with stable wire markers.
    * `Musubi.Testing` — test harness for stores.

  See `docs/PRD.md` and `spec/decisions/` for the design rationale.
  """

  @type store_id() :: Musubi.Socket.store_id()

  @doc """
  Targets a mounted child store with new assigns, aligned with
  `Phoenix.LiveView.send_update/2`.

  The `assigns` map is delivered to the addressed store's `update/2`
  callback (or merged directly when the store does not export it). The
  store's socket goes dirty and only that subtree re-renders; a clean
  root short-circuits its own `render/1` (BDR-0023). One coalesced patch
  envelope ships for the cycle. A `store_id` that no longer resolves to a
  mounted store is a no-op (LV-aligned) and emits
  `[:musubi, :send_update, :no_target]` telemetry.

  This two-arity form sends to `self()` — call it from inside the root
  store's `handle_info/2`, where `self()` is the page process. It is the
  intra-page last hop for cross-connection fan-out coordinated over
  `Phoenix.PubSub` (BDR-0005 / BDR-0030); Musubi owns the targeting, the
  application owns the broadcast.

  `store_id` may address any mounted store, including the root (`[]`).

  Push keys the **child owns**, not keys the parent passes it — e.g. an
  internal trigger like `%{reload_token: ref}` the child's `update/2`
  reacts to by reloading. A key the parent also supplies is reconciled
  back to the parent's value on the very next resolve (parent props win,
  LiveView-aligned) and re-invokes `update/2`, so it has no net effect.

  ## Examples

      iex> me = self()
      iex> Musubi.send_update(["comments"], %{reload_token: :ref})
      :ok
      iex> receive do {:musubi_send_update, _, _} = msg -> msg end
      {:musubi_send_update, ["comments"], %{reload_token: :ref}}
      iex> me == self()
      true
  """
  @spec send_update(store_id(), map()) :: :ok
  def send_update(store_id, assigns) when is_list(store_id) and is_map(assigns) do
    send(self(), {:musubi_send_update, store_id, assigns})
    :ok
  end

  @doc """
  Targets a mounted child store on `page_pid` with new assigns, aligned
  with `Phoenix.LiveView.send_update/3`.

  Same semantics as `send_update/2` but addressed at an explicit page
  process — for an in-node caller holding the page pid (e.g. a release
  `rpc` task) rather than running inside the page's own `handle_info/2`.

  ## Examples

      iex> Musubi.send_update(self(), ["comments"], %{reload_token: :ref})
      :ok
      iex> receive do {:musubi_send_update, _, _} = msg -> msg end
      {:musubi_send_update, ["comments"], %{reload_token: :ref}}
  """
  @spec send_update(pid(), store_id(), map()) :: :ok
  def send_update(page_pid, store_id, assigns)
      when is_pid(page_pid) and is_list(store_id) and is_map(assigns) do
    send(page_pid, {:musubi_send_update, store_id, assigns})
    :ok
  end
end
