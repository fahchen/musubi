---
id: BDR-0031
title: assign_async/stream_async re-assign never kills the prior task; lazy-discard by ref (LiveView parity)
status: accepted
date: 2026-06-21
summary: Re-assigning assign_async/stream_async for a name already in flight (including with :reset) does NOT kill the prior task. The runtime drops its tracking; the prior task runs to completion and its result/`:DOWN` lazy-discards by ref. Only cancel_async/2,3 and :timeout actively kill. Extends BDR-0019's start_async rule to all three async APIs and matches Phoenix.LiveView, which never exits a producer on re-assign.
---

**Feature**: domains/async/features/lifecycle.feature, stream-async.feature
**Rule**: assign_async/stream_async re-assign re-emits loading without killing the prior task

## Context

`Musubi.Async.assign_async/3,4` and `stream_async/3,4` previously called
`cancel_prior_for_reassign` on every re-assign of a name already in flight —
`Process.exit(pid, {:shutdown, :reassign})` (`lib/musubi/async.ex`). This ran
unconditionally, for plain re-assign and `:reset` alike.

Tasks spawn via `Task.Supervisor.async_nolink` and do NOT trap exits, so any
termination reason kills them immediately — before `consume`/DB checkin. When a
prior task is mid-DB-call under an Ecto sandbox in `shared: true` mode (one
shared connection), killing it tears down that connection: the DBConnection
ownership proxy logs `disconnected: client #PID exited` and subsequent queries
see `:CONNECTION_DEAD`. This is `db_connection` ownership behavior, adapter-
agnostic (reproduced identically on Exqlite and Postgrex); Postgres test suites
only escape it via `async: true` per-test connections, which SQLite cannot do.
See `docs/review-store-async-sqlite-problem.md`.

Phoenix LiveView (v1.2.3, `lib/phoenix_live_view/async.ex`) **never** exits a
producer on re-assign: `run_async_task` (`async.ex:279`) `Map.put`s the new
`{ref, pid, kind}` over the old; `prune_current_async` (`async.ex:416`) discards
only a stale RESULT by ref mismatch and never sends an exit. Only `cancel_async`
(`async.ex:316`) calls `Process.exit`. `start_async` in Musubi already matched
this (BDR-0019). `assign_async`/`stream_async` re-assign did not.

## Decision

`assign_async`/`stream_async` re-assign of an in-flight name calls
`drop_tracking` instead of killing:

- The prior task is **not** killed. It runs to completion; its result message
  and `:DOWN` arrive with a ref no longer in the tracking map and lazy-discard,
  emitting `[:musubi, :async, :lazy_discard]`.
- `:reset` still re-emits `loading()` for the managed keys (visible behavior
  unchanged); it no longer kills the prior task.
- Only `cancel_async/2,3` (explicit) and `:timeout` (expiry) actively
  `Process.exit` a task. Those are intentional and out of scope here.

This extends the BDR-0019 silent-overwrite-+-lazy-discard rule from `start_async`
to all three async APIs, so the whole async surface matches LiveView.

## Behaviours Considered

### Option A: drop_tracking on re-assign (chosen)
Re-assign drops tracking; prior task runs to completion, result lazy-discards by
ref. Matches LiveView and BDR-0019. No task is killed mid-DB-call, so the shared
sandbox connection is never torn down.

### Option B: keep killing on re-assign
Status quo. Predictable resource cleanup, but kills tasks mid-transaction →
tears down shared Ecto sandbox connections, and diverges from LiveView and from
`start_async` (BDR-0019).

## Decision Drivers / Rejected Alternatives

Option B rejected: the SQLite-sandbox teardown is a direct consequence, and the
divergence from LiveView and `start_async` was unintended. The cost of Option A
— a prior task occasionally running to completion with its result discarded — is
the same cost already accepted for `start_async` in BDR-0019 (a fast double-call
runs both tasks and drops the loser). Applications needing deterministic
cancellation call `cancel_async/2,3` explicitly.
