---
id: BDR-0030
title: Server can target a mounted child store with new assigns via send_update
status: accepted
date: 2026-06-17
summary: Add `Musubi.send_update/2,3`, aligned with `Phoenix.LiveView.send_update`, delivering an assigns map to one addressed child store's `update/2`; render scoping reuses the existing `subtree_dirty?` gate and the BDR-0023 root short-circuit, ordering follows the page mailbox FIFO, and a missing target is a no-op plus telemetry.
---

**Feature**: domains/runtime/features/command-routing.feature
**Rule**: The server can target a mounted child store with new assigns

## Context

Musubi is server-authoritative and page-scoped: one GenServer per connected
page owns a tree of stores. Only the root store has a delivery path for server
messages — `Phoenix.PubSub` broadcasts land in the page mailbox and route to the
root's `handle_info/2` (the catch-all in `lib/musubi/page/server.ex`). Child
stores are not processes; they are declared in a parent's `render/1` and have
`init/1` + `update/2`.

The only existing way to refresh one specific mounted child from the server was
**rev-prop threading**: the root bumps a child-consumed assign and threads it
through every intermediate parent's `render/1` so `Reconciler.reconcile_child/4`
sees a changed consumed prop. This is brittle (per-target rev bookkeeping, deep
threading) and pays a full root `render/1` on every event, because dirtying a
root assign defeats the BDR-0023 short-circuit.

A key enabling fact already exists in the render machinery:
`Reconciler.reconcile_child/4` re-renders a child when `subtree_dirty?` is true —
i.e. the child's own socket is dirty — independent of parent props
(`lib/musubi/reconciler.ex`). The only missing primitive is a way to dirty one
addressed child's socket directly from the server.

## Behaviours Considered

### Option A: `send_update(store_id, assigns)` → child `update/2` (chosen)

Mirror `Phoenix.LiveView.send_update`. A page-mailbox message
`{:musubi_send_update, store_id, assigns}` addresses one mounted store by its
`store_id` path. The runtime runs the child's `update/2` (or merges assigns when
`update/2` is not exported) via the existing `Reconciler.update_store/2`, puts
the mutated socket back in the store table, and runs the normal render cycle.

### Option B: `notify`/child-`handle_info` — an imperative payload-less signal

Add a per-child `handle_info/2`-style callback so the server can send a message
to a specific child that the child reacts to imperatively.

## Decision

Adopt Option A. `Musubi.send_update/2,3` delivers an `assigns` map to the
addressed child store's `update/2`. Addressing is by `store_id` path (the same
addressing scheme as `command/4` and `peek/2`), intra-page only.

- `send_update/2` sends to `self()` — called from inside the root store's
  `handle_info/2`, where `self()` is the page process.
- `send_update/3` sends to an explicit `page_pid` — an in-node caller holding a
  page pid (e.g. a release `rpc` task).

Both return `:ok` and enqueue `{:musubi_send_update, store_id, assigns}` on the
page mailbox.

**Render scoping reuses existing machinery** — no new render or reconcile
machinery is added. After `update_store/2` runs, the child's socket is dirty, so
`subtree_dirty?` fires in the next render cycle and only that subtree
re-renders. The clean root short-circuits its own `render/1` per BDR-0023. One
coalesced patch envelope ships for the cycle.

**Ordering** follows the page mailbox FIFO: a `send_update` message is
serialized with commands, PubSub messages, and async results; none preempts
another mid-cycle.

**Missing target** is a no-op (LV-identical): when `store_id` does not resolve to
a mounted store, the runtime emits `[:musubi, :send_update, :no_target]`
telemetry and pushes no envelope.

**Pushed assigns are passed raw** to `update/2` — no attr normalization, matching
LiveView which passes the `send_update` map straight through. Caveat (LiveView-
identical): a later parent re-render that passes the same key overrides the
pushed value.

### Relationship to BDR-0005 (no built-in PubSub) — explicit non-conflict

This is a **targeting/delivery primitive, not a pub/sub abstraction**. Musubi
still defines no subscribe macro or broadcast helper. Cross-connection fan-out
stays application-owned via `Phoenix.PubSub`: each source broadcasts, every
page's root `handle_info/2` receives the broadcast and calls `send_update` as the
**intra-page last hop**. No page registry is introduced; Musubi owns only the
intra-page targeting, the application owns the cross-connection broadcast.

## Rejected Alternatives

### Option B: `notify`/child-`handle_info` (imperative message)

Rejected to keep LiveView parity. Musubi's child-update contract is
assigns-merge through `update/2`, and `send_update` matches it exactly, so a
caller's mental model carries over from LiveView unchanged. An imperative
payload-less `notify` is the right tool only for side-effect signals that carry
no assigns to merge; this need (reload a child's data, push new assigns) is an
assigns-merge, so the imperative variant would add a second, divergent delivery
shape for no gain.
