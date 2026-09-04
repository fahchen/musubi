# musubi-client

The runtime-free core of the Musubi Rust client: the patch engine (RFC 6902
subset over a shadow `serde_json::Value`), client-owned stream materialization,
hydration of the wire markers, mount/command/event lifecycle, uploads (both
planes), the stale-while-revalidate mount cache, the per-root mount status,
and the support types the generated bundle (`mix compile.musubi_rust`) refers
to.

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
let state = cart.snapshot();
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
handles you read with `Mounted::upload(&store_id, name)` — the same
`snapshot()`/`updates()` pair as state, though an upload's stream is a queue of
per-envelope handles rather than a latest-value cell — and the same handle
carries `select()`, `start()`, `cancel()` and `reset()`. The state slot itself
stays the inert `UploadSlot`, which carries the handle's name. The crate reads no files: you hand it an
`UploadFile` (bytes plus name and content type), and an external destination is
your own `Uploader` registered on the builder. Reconnect is reconnect-only: a
version gap or a rejoin keeps the last-good rendering and waits for a fresh
initial envelope, and an upload in flight fails rather than resuming. The
reconnect window itself is renderable state:
`Mounted::status()` / `status_updates()` report
`MountStatus { Connecting, Live, Reconnecting }` (BDR-0033) while `snapshot()`
keeps serving the last-good tree — the status annotates stale rendering, it
never blanks it.

Mounts can be stale-while-revalidate. `ConnectionBuilder::cache` takes a
`CacheStore` — `MemoryCacheStore` ships here, a durable one is yours — and every
mount then publishes the last-known wire tree for its `(module, id, params)`
while the live join revalidates in the background; the real initial patch
replaces the seed in one whole-root op. Writes are throttled, entries carry a
`buster` you set to your build version, and a seeded root queues command
dispatches behind its in-flight initial patch instead of rejecting them.

See `docs/rust-client.md` §6.4 and §9–§10 in the Musubi repository for the full
contract, and `docs/rust-codegen.md` for the generated bundle.
