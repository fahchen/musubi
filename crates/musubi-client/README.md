# musubi-client

The runtime-free core of the Musubi Rust client: the socket seam, the
mount/command/event lifecycle, reconnect and recovery, uploads (both planes),
the stale-while-revalidate mount cache, the per-root mount status, and the
retained reactive state tree every envelope is applied to (`musubi-state`,
re-exported here). It also carries the support types the generated bundle
(`mix compile.musubi_rust`) refers to.

One envelope is **one transaction** against a retained tree, and only the nodes
whose value actually changed wake their subscribers — see
`docs/rust-reactive-state.md` for the design.

The crate depends on no executor: the socket, the task spawner and the clock
are the `Connector` / `Spawner` / `Timer` traits of `phoenix-channel`. Tokio
embedders add `musubi-client-tokio`, which supplies all three.

```rust,ignore
let connection = Connection::builder()
    .url("wss://example.test/socket")
    .connector(connector)
    .spawner(spawner)
    .timer(timer)
    .build()?;

let cart: Mounted<CartStore> = connection.mount("cart:page", Params {}).await?;
let state = cart.state();

// `x.prop()` gives a handle, `handle.value()` gives a value,
// `handle.subscribe(cb)` gives a subscription, and dropping it unsubscribes.
let title = state.title().value();
let _watch = state.items().subscribe(|_change, _edits| redraw());

let reply = cart.command(AddItem { sku: "ABC".into() }).await?;
```

## Using it

The crate is unpublished: it ships inside the Hex `musubi` package, so a
consumer path-depends on the copy in its `deps/`.

```toml
[dependencies]
musubi-client = { path = "../deps/musubi/crates/musubi-client" }
```

Version is the Hex `musubi` version — one release stream, no pairing table.
MSRV 1.85 (edition 2024). Licensed MIT, see `LICENSE`.

## `AsyncResult` uses the wire names, not the TypeScript names

`packages/client` normalizes an async node to `{status, data, error}`. This
crate keeps the server's own field names instead:

```rust,ignore
match feed {
    AsyncResult::Loading { result, .. } => /* previous value, if any */,
    AsyncResult::Ok { result, .. } => /* the value */,
    AsyncResult::Failed { reason, .. } => /* the failure */,
}
```

`result` / `reason` are what `Musubi.AsyncResult` (`lib/musubi/async_result.ex`)
and the wire carry, so the three variants line up 1:1 with the server struct and
the `Deserialize` derive needs no hand-written impl. Every variant carries
`reason` — the server always renders the key, `null` when not failed — so a
consumer matching only on the payload writes `AsyncResult::Ok { result, .. }`.

There are deliberately no `Default` / `unwrap_or_default` conveniences:
`Loading { result: None }` and `Ok` are different states and the app must
branch.

## Scope

Uploads are both observed and driven. `upload_ops` are folded into per-store
handles you reach in one step from the slot node on the tree —
`Mounted::upload_at(&state.avatar())`, which reads both halves of the
`(store_id, name)` key off the node — and the handle carries the same
`value()` / `subscribe()` pair every other handle does, plus `into_stream()`
for a consumer whose shape is a loop. That stream is a queue of per-envelope
values, not a latest-value cell. The same handle carries `select()`, `start()`,
`cancel()` and `reset()`. The slot on the tree is an **inert leaf**: the server
re-renders the same marker every cycle, so a pure-upload envelope wakes no state
subscriber at all. The crate reads no files: you hand it an `UploadFile` (bytes
plus name and content type), and an external destination is your own `Uploader`
registered on the builder.

Reconnect is reconnect-only: a version gap or a rejoin keeps the last-good
rendering and waits for a fresh initial envelope, and an upload in flight fails
rather than resuming. The rejoin's initial patch is *reconciled* into the same
tree, so a subtree the server re-sent unchanged keeps its identity and notifies
nobody. The reconnect window itself is renderable state: `Mounted::status()`
hands back a handle whose `value()` / `subscribe()` / `into_stream()` report
`MountStatus { Connecting, Live, Reconnecting }` (BDR-0033) while the tree keeps
serving the last-good rendering — the status annotates stale rendering, it never
blanks it.

Mounts can be stale-while-revalidate. `ConnectionBuilder::cache` takes a
`CacheStore` — `MemoryCacheStore` ships here, a durable one is yours — and every
mount then publishes the last-known wire tree for its `(module, id, params)`
while the live join revalidates in the background; the real initial patch
replaces the seed in one whole-root op. Writes are throttled, entries carry a
`buster` you set to your build version, and a seeded root queues command
dispatches behind its in-flight initial patch instead of rejecting them.

See `docs/rust-reactive-state.md` for the state tree, `docs/rust-client.md`
§6.4 and §9–§10 for the rest of the contract, and `docs/rust-codegen.md` for the
generated bundle.
