---
id: BDR-0033
title: Clients surface a local mount status (connecting | live | reconnecting); servers are not involved
date: 2026-09-04
status: accepted
summary: Both clients expose the socket-liveness signal they already hold internally as a per-surface status — Rust `MountStatus { Connecting, Live, Reconnecting }` per mounted root (`Mounted::status()` / `Mounted::status_updates()`, fed by a new `PhoenixSocket` status watch), TS `MusubiSocketStatus = "connecting" | "ready" | "reconnecting"` per connection (`connection.status()` / `connection.onStatusChange()`, fed by phoenix.js socket onOpen/onError/onClose). Client-local only, no wire change, servers uninvolved. Terminal outcomes (rejected join, decode failure, unmount, disconnect) stay on the existing mount error path; the status never carries an error arm. While reconnecting the client MUST keep rendering the last-good tree — BDR-0015's obligation restated as a status-surface contract.
---

**Feature**: domains/replication/features/json-patch-diff.feature
**Rule**: There is no application-level resync command; reconnect is the recovery path

## Context

BDR-0015 makes reconnect the only recovery path and obliges clients to keep
rendering the last-good tree through the window. Both clients honor that —
`Mounted::snapshot()` is never cleared on reconnect
(`crates/musubi-client/src/mounted.rs`), and the TS runtime keeps the last-good
root through `handleRootDisconnect` (`packages/client/src/runtime.ts`) — but
neither *surfaced* the liveness signal it derives that behavior from. The
`Option` returned by `snapshot()` answers "have I ever loaded", never "am I
current".

The consequence was the gap `docs/rust-gpui-example.md` recorded as open
question 1: an idle client whose socket dies (peer close, IO failure, missed
heartbeat) has no way to notice until a command fails. The shipped desktop
example worked around it with a `stale` flag set by the first command that
failed with `NotConnected` / `Disconnected` / `Transport` — so its connection
pill read "live" until the user pressed Send. Every non-React embedder would
re-derive the same workaround. On the TS side,
`useMusubiConnectionStatus` covers only the connect handshake
(`connecting | ready | error`) and never moves again; the socket lifecycle
(phoenix.js `onOpen` / `onError` / `onClose`, channel rejoin) was consumed for
recovery but not exposed.

The signal already exists in both socket layers: the Rust `phoenix-channel`
actor owns the backoff/rejoin loop and tears the socket down on a missed
heartbeat (`ChannelErrorReason::HeartbeatTimeout`); phoenix.js fires socket
lifecycle callbacks and auto-rejoins channels. Surfacing it is a client-local
projection — the server is not involved and nothing changes on the wire.

## Behaviours Considered

### Option A: Server-driven presence/status message

A wire-level "am I current" signal (server ping, status frame, resync marker).
Rejected outright: BDR-0015 removed exactly this class of machinery. The server
cannot tell a client "your socket is dead" over the dead socket, and the client
already owns the authoritative local signal (heartbeat timeout, close, rejoin).

### Option B: Keep the per-embedder workaround (status quo)

Leave the signal internal; embedders infer staleness from failed commands.
Rejected: the desktop example's `stale` flag demonstrates the failure mode — an
idle disconnect is invisible, and every non-React client re-derives the same
wrong-by-one-command workaround.

### Option C: Client-local status surface derived from the socket layer (chosen)

Each client exposes its own socket-liveness projection, with no wire change.

## Decision

Adopt Option C.

**Shape.** Three states, no error arm:

- *connecting* — the surface has never been live: the transport/first accepted
  initial patch has not arrived yet. Socket churn before first success stays
  here; it never reads as "reconnecting" something that was never connected.
- *live / ready* — the happy state.
- *reconnecting* — liveness was lost after the surface had been live
  (socket drop, heartbeat timeout, version-gap recovery) and the client's own
  reconnect machinery is working its way back. The state ends when recovery
  completes (fresh initial patch / socket reopen), not before.

**Terminal outcomes stay on the error path.** A rejected join, a decode
failure, unmount, and `disconnect()` already surface as mount errors / stream
ends. The status deliberately has no `Error`/`Closed` arm: it is a liveness
surface, not an error surface, and a second error channel would race the first.

**Rendering obligation (BDR-0015 restated).** While the status is
*reconnecting* the client MUST keep rendering the last-good tree. The status
exists so an embedder can *annotate* stale rendering (dim it, show a pill) —
never so it can blank it. `snapshot()` staying `Some` through the window is
part of the contract, not an accident of implementation.

**Rust naming.** `phoenix-channel` exposes the connection-wide watch:
`pub enum SocketStatus { Connecting, Connected, Reconnecting, Closed }`,
`PhoenixSocket::status() -> SocketStatus` and
`PhoenixSocket::status_updates() -> SocketStatusUpdates` (a `Stream`; dropping
it unsubscribes). `musubi-client` maps the same signal — delivered per topic as
the `ChannelEvent`s the socket actor already emits from the identical
transitions — per mounted root into
`pub enum MountStatus { Connecting, Live, Reconnecting }` with
`Mounted::status() -> MountStatus` and
`Mounted::status_updates() -> impl Stream<Item = MountStatus>` (`#[must_use]`,
same mpsc-sender pattern as `updates()`/`events()`; dropping unsubscribes; no
replay — read `status()` first). Per-root semantics: `Connecting` until the
first **accepted** initial patch (a cache seed does not count — it renders
data, it does not make the root live); `Live` after; `Reconnecting` from
socket-drop / heartbeat-timeout / version-gap recovery until the rejoin's
fresh initial patch lands. A root that was never live cannot enter
`Reconnecting`.

**TS naming.** The status is per **connection** (one socket, many roots — the
socket lifecycle is connection-scoped, and per-root recovery already rides
behind it): `type MusubiSocketStatus = "connecting" | "ready" | "reconnecting"`
in `@musubi/client`, exposed as `connection.status()` and
`connection.onStatusChange(listener)` (returns an unsubscribe thunk, the
package's existing subscription idiom). `"ready"` rather than `"live"` because
it extends the vocabulary `useMusubiConnectionStatus` already established.
Driven by optional `SocketLike.onOpen`/`onError`/`onClose` hooks (phoenix.js
provides all three); a socket without them degrades to a constant `"ready"`
after `connect()`, which is exactly the pre-BDR information content. The hooks
feed the status surface only — recovery stays channel-driven, per the existing
contract.

**No cross-client symmetry requirement.** Rust surfaces per-root status (one
`Mounted` is the natural unit of a native view); TS surfaces per-connection
status (the React tree renders per-store from proxies and needed the
connection-level gap filled). Both are projections of the same socket signal;
neither adds wire traffic.

## Rejected Alternatives

Option A re-adds the resync-class wire machinery BDR-0015 removed, for a signal
the client already owns locally. Option B leaves the idle-disconnect blind spot
in every embedder and makes the desktop example's `stale`-flag workaround the
de-facto API. A fourth arm (`Unmounted` / `Closed`, as the original
`docs/rust-gpui-example.md` proposal sketched) was dropped from the mount-level
enum because teardown already has an unambiguous surface — the subscription
streams end and commands fail with `Unmounted`/`Disconnected` — and a terminal
status arm would duplicate it.
