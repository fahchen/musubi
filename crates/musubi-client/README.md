# musubi-client

The runtime-free core of the Musubi Rust client: the patch engine (RFC 6902
subset over a shadow `serde_json::Value`), client-owned stream materialization,
hydration of the wire markers, mount/command/event lifecycle, and the support
types the generated bundle (`mix compile.musubi_rust`) refers to.

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

let cart: Mounted<CartStore> = connection.mount("cart:page", json!({})).await?;
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

## Scope of v1

Uploads are parsed but not driven (`UploadSlot` is inert), and reconnect is
reconnect-only: a version gap or a rejoin keeps the last-good rendering and
waits for a fresh initial envelope. See `docs/rust-client.md` §9–§10 in the
Musubi repository for the full contract, and `docs/rust-codegen.md` for the
generated bundle.
