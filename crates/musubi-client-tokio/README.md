# musubi-client-tokio

The tokio transport for the Musubi Rust client: `TungsteniteConnector`,
`TokioSpawner`, `TokioTimer`, and a `builder()` one-liner that pre-fills all
three. Everything in `musubi-client` is re-exported, so a tokio embedder
depends on this crate alone.

```rust,ignore
let connection = musubi_client_tokio::builder("wss://example.test/socket").build()?;
let cart: Mounted<CartStore> = connection.mount("cart:page", json!({})).await?;
```

Choosing a runtime is a crate choice, not a feature flag: depending on
`musubi-client` alone keeps tokio out of the dependency tree entirely, which is
what a GUI embedder (gpui, egui) needs.

## Using it

The crate is unpublished: it ships inside the Hex `musubi` package, so a
consumer path-depends on the copy in its `deps/`.

```toml
[dependencies]
musubi-client-tokio = { path = "../deps/musubi/crates/musubi-client-tokio" }
```

Version is the Hex `musubi` version — one release stream, no pairing table.
MSRV 1.85 (edition 2024). Licensed MIT, see `LICENSE`.

Design: `docs/rust-client.md` §2.3 in the Musubi repository.
