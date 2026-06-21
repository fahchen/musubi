---
id: BDR-0022
title: Refresh primitives — stream/4 with reset: true (silent) vs stream_async/4 with reset: true (with loading flash)
status: accepted
date: 2026-05-09
summary: Musubi exposes no dedicated reload mechanism (no `reload_stream` helper, no `reload_stream/2` callback). To refresh a stream's contents the application calls `stream(socket, name, fresh_items, reset: true)` directly (silent — AsyncResult untouched) or, when a loading indicator is desired, re-runs LV-parity `stream_async(socket, name, fun, reset: true)` (re-emits AsyncResult.loading then re-seeds on completion).
---

## Scope

**Feature**: domains/streams/features/lifecycle.feature
**Feature**: domains/async/features/stream-async.feature
**Rule**: Refresh paths — silent vs. loading-flash

## Reason

A separate `reload_stream/2` callback would add a third path (alongside `stream/4` and `stream_async/4`) without giving the application any new capability. Both refresh patterns are already expressible:

- **Silent refresh**: the application has fresh items in hand (from a webhook, a user click, an external query) and calls `stream(socket, name, fresh_items, reset: true)`. The stream slot is cleared and re-seeded; `AsyncResult` (if any) stays at `:ok`. No loading flash.
- **Refresh with loading flash**: the application calls `stream_async(socket, name, fun, reset: true)`. The runtime leaves the prior async task running (its result lazy-discards by ref — LiveView parity, see BDR-0031), sets the AsyncResult to `:loading` (preserving the prior result for stale-while-loading UX), and re-seeds the stream when the task completes.

Both forms compose with the standard stream and async APIs and need no special primitives. Authors who want a "refresh now" handler simply pattern-match in their own `handle_command` or `handle_info` and call the appropriate helper. The runtime has no auto-invocation surface — recovery is the reconnect path (BDR-0015) for runtime restart, and explicit application calls otherwise.

## Rejected Alternatives

A dedicated `reload_stream/2` callback paired with a `reload_stream(socket, name)` helper was considered and removed. It added a callback name to learn, an envelope shape to document, and a third refresh path with no capability gain over `stream(reset: true)`. Removing it shrinks the API surface by one callback and one helper.
