---
id: BDR-0032
title: Transient server-to-client push events ride the patch envelope
date: 2026-06-30
status: accepted
summary: Add the bare `push_event/3` store helper (LV-aligned, delegating to `Musubi.Event`) queuing transient, fire-and-forget events on the socket; the page server drains them per render cycle and folds them into `PatchEnvelope.events`, so one consolidated `"patch"` push carries diff + events. Events have no version of their own, no ack, no retry, and are not replayed on reconnect. An event-only cycle still emits an envelope and bumps `version`.
---

**Feature**: domains/runtime/features/command-routing.feature
**Rule**: The server can push a transient event to the connected client

## Context

Musubi's only server-to-client channel is the versioned `PatchEnvelope`
(`lib/musubi/page/patch_envelope.ex`): a render cycle's RFC 6902 `ops` plus
`stream_ops`/`upload_ops`, sequenced by `base_version`/`version`. The client
applies it in order and triggers reconnect recovery on a version gap
(`packages/client/src/runtime.ts`). Everything the server sends today is
**materialized state** — it survives in the store tree and is replayed on
reconnect via the initial patch (BDR-0015).

Applications also need to deliver **transient signals** that are *not* state: a
toast, a "scroll to bottom", a one-off "playSound". LiveView covers this with
`push_event/3` + client `handleEvent` — a fire-and-forget event that the client
consumes once and that owns no server state.

BDR-0005 deliberately keeps Musubi out of the **server-side** pub/sub business
(apps use `Phoenix.PubSub` + `handle_info/2` directly). This BDR is the
orthogonal **client-delivery** side: a one-way server→client emit primitive,
the analog of LV `push_event`, not a subscribe/broadcast abstraction.

## Behaviours Considered

### Option A: Separate `"event"` channel message, sent per event

`push_event` sends directly to the transport pid; the channel pushes a distinct
`"event"` frame. Rejected: (1) it bypasses the server-owned egress — every other
server→client message (patch/stream/upload) is drained from the store sockets by
the page server and sent from `State.transport`, never from a socket field, and
child sockets do not even carry `transport_pid`; (2) a cycle producing both a
diff and N events would emit N+1 separate frames instead of one consolidated
push.

### Option B: Fold events into the patch envelope (chosen)

`push_event(socket, name, payload)` queues `{name, payload}` on the socket
(same accumulate-on-socket pattern as `stream_insert`). The page server drains
all store sockets each render cycle (`flush_all_events`, mirroring
`flush_all_stream_ops`) and folds the events into a new
`PatchEnvelope.events` field. One `"patch"` push carries diff + stream + upload +
events. Egress stays server-owned: events are wire-serialized
(`Musubi.Wire.to_wire`) at drain and ship from `State.transport` like every
other op.

## Decision

Adopt Option B.

**API.** `Musubi.push_event(socket, name, payload) :: socket`, exposed bare in
store callbacks via `Musubi.Store` (defdelegate to `Musubi.Event`). `name` is a
string or atom (stringified); `payload` is any wire-encodable term. Returns the
socket for pipe-chaining (LV-aligned). Callable from any store callback (mount,
command, `handle_info`, `handle_async`) on the root or any child socket — the
runtime does not special-case the calling phase.

**Typed declaration.** Events are declared in the **root store** with an `event`
DSL (payload-only, mirroring `command`): `event :toast do field :msg, String.t()
end`. The declaration is root-only (a child-store `event` is a compile-time
error) because events are root-scoped on the wire. It drives codegen — `StoreDef`
gains a 4th type param `Events` (default `{}`, so existing 3-arg references stay
valid), yielding TS `EventName<M,R>` / `EventPayload<M,K,R>` that type
`handleEvent` / `useMusubiEvent`. On a child proxy `EventName` is `never`, so
events are subscribed off the root proxy.

**Runtime validation (dev-correctness only).** The declared payload schema is
validated at drain (`Musubi.Event.validate_events!/2`), mirroring
`Musubi.Hooks.ValidateReplySchema`: a command *reply* is server-generated yet
still validated against its declared `reply_fields` to catch handler mistakes,
and a push event is the same kind of trusted-but-fallible server output, so it
gets the same treatment. A declared event whose `push_event` payload is missing
a field or has a type mismatch raises `ArgumentError` (BDR-0003 let-it-crash);
an *undeclared* event name is skipped (not validated). This is **not** a
security boundary — events are server-pushed, so there is no untrusted input to
guard, unlike commands (`ValidateCommandSchema` on client input). Validation
runs in the same render cycle as the queuing handler, so a bad `push_event` in a
command handler surfaces synchronously from `dispatch_command`.

**Wire shape.** `PatchEnvelope` gains an `events` field. One event is
`%{"name" => string, "payload" => wire}`. The envelope keeps `type: "patch"` —
events are a field on the existing message, not a new frame:

```json
{ "type": "patch", "base_version": 4, "version": 5,
  "ops": [...], "stream_ops": [...], "upload_ops": [...],
  "events": [ {"name": "toast", "payload": {"msg": "saved"}} ] }
```

**Emit rule.** `PatchEnvelope.build` emits an envelope when *any* of `ops`,
`stream_ops`, `upload_ops`, or `events` is non-empty (was: the first three).
A cycle with only events therefore emits an envelope and **bumps `version`**
(`version = base_version + 1`). `version` is the envelope/message sequence, not
strictly a state version; an event-only bump applies empty `ops` (a client
no-op) and advances the sequence. This is cheap (no server-side history; reset
to 0 on reconnect) and avoids special-casing the client's strict
`version == base_version + 1` gate.

**Events are not versioned state.** They ride the envelope but own no recovery
semantics:

- **No ack, no retry.** Delivered once with the envelope over the channel; the
  application never acks.
- **No replay on reconnect.** Reconnect re-mounts the root and replays *state*
  via the initial patch (BDR-0015); the server keeps no event history, so
  past events are gone. If no client is connected when `push_event` runs
  (`transport_pid == nil`, e.g. a detached/test socket), the event is dropped.
- **Dropped on version mismatch.** When `handlePatch` rejects an envelope for a
  version gap and triggers recovery, that envelope's events are discarded with
  it. The client dispatches events only inside `acceptEnvelope`, after a clean
  apply.
- **No special-casing of the mount phase.** Events queued during `mount` are
  drained the same way as any other cycle and ride the *initial* envelope, so
  `PatchEnvelope.initial` carries an `events` field too. Codex review noted the
  cold-client limitation: a `mountStore` caller cannot register a handler until
  the initial patch resolves, so mount-time events can be missed (and mount
  re-runs on reconnect, re-firing them). We accept this rather than add a
  deferral/buffer mechanism — buffering a transient, dispatch-once signal
  contradicts its semantics, and a one-cycle deferral still loses React's
  effect-timed registration. The documented guidance is to use **state**
  (replayed per BDR-0015) for data that must exist at mount, and `push_event`
  for transient signals the client is already listening for.

**Client consumption.** The client owns the consumption logic. `acceptEnvelope`
dispatches `envelope.events` to a per-`RootConnection` handler registry after
applying `ops`/`stream_ops`/`upload_ops`. `root.handleEvent(name, cb)` registers
a handler and returns an unsubscribe thunk; multiple handlers per name are
allowed; an event with no registered handler is dropped. The registry lives on
the `RootConnection` (not the channel) so it survives reconnect.

**Scope is root-level.** The wire event carries no `store_id`. Events queued on
any store socket (root or child) are flattened in `store_id` order into the one
root envelope and dispatched by `name` alone. Per-store scoping is deferred
until a concrete need appears.

**Ordering is not guaranteed** beyond "within one envelope, events are
dispatched after that envelope's state ops are applied." No ordering is promised
across envelopes relative to other transient signals.

### Relationship to BDR-0005 (no built-in PubSub) — explicit non-conflict

`push_event` is a **client-delivery** primitive, not a server-side pub/sub
abstraction. Musubi still defines no subscribe macro or broadcast helper.
Cross-connection fan-out stays application-owned via `Phoenix.PubSub`: a source
broadcasts, each page's root `handle_info/2` receives it and calls `push_event`
as the intra-page last hop to the client. Musubi owns only the intra-page emit.

## Rejected Alternatives

Option A (separate `"event"` frame, per-event send) was rejected for breaking
server-owned egress and defeating the consolidation requirement (one push per
cycle). Folding into the envelope reuses the existing drain/egress machinery and
ships a single coalesced frame.
