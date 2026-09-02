# phoenix-channel

A Phoenix Channel client for Rust: serializer v2 framing, joins, pushes,
heartbeats and reconnect, over a socket the caller supplies.

Not Musubi-aware and not runtime-aware. Everything executor-specific sits
behind four seams — `Socket`, `Connector`, `Spawner`, `Timer` — so the same
protocol code runs on tokio, on a GUI executor, or on a test's manual pump.

```rust,ignore
let socket = PhoenixSocket::builder()
    .url("wss://example.test/socket")
    .connector(connector)
    .spawner(spawner)
    .timer(timer)
    .build()?;

let (channel, mut events) = socket.channel("room:lobby", json!({})).await?;
let reply = channel.push("new_msg", json!({"body": "hi"})).await?;
```

## Using it

The crate is unpublished: it ships inside the Hex `musubi` package, so a
consumer path-depends on the copy in its `deps/`.

```toml
[dependencies]
phoenix-channel = { path = "../deps/musubi/crates/phoenix-channel" }
```

Version is the Hex `musubi` version — one release stream, no pairing table.
MSRV 1.85 (edition 2024). Licensed MIT, see `LICENSE`.

Design: `docs/rust-client.md` §3 in the Musubi repository.
