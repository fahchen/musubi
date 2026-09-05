# musubi-state

The retained reactive state tree of the Musubi client: nodes with client-local
identity, recursive semantic equality, transactional patch reconciliation,
keyed collection reconciliation for streams, and per-node RAII subscriptions.

```text
PatchEnvelope
  ->  one transaction against the retained tree
        ops        -> pointer-addressed reconciliation
        stream_ops -> key-addressed collection reconciliation
  ->  recursive semantic equality, bottom-up over the dirty set
  ->  ChangeSet<NodeId> (+ per-collection keyed edits)
  ->  the subscribers of exactly the changed nodes
  ->  RAII-managed callbacks
```

A patch is only *input*. Whether anyone is notified is decided by comparing
each node's semantic value from before the whole transaction with the one it
settles on, so a field a transaction changed and changed back wakes nobody, and
a sibling nothing touched is never even compared.

```rust,ignore
let state: State<AppState> = mounted.state();

let rows = state.messages();               // a handle: zero cost, no read
let first = rows.by_key("msg-1").unwrap(); // still a handle
let body = first.body().value();           // the one materialization point

let subscription = rows.subscribe(|change, edits| {
    // `edits` is the keyed diff — the one thing re-reading cannot recover.
});
```

## Scope

No network, no UI, no runtime: `serde`, `serde_json`, `slotmap`, `thiserror`,
and nothing else. The socket, the envelope, uploads and events are
`musubi-client`'s; the gpui adapter is `musubi-gpui`'s.

The wire vocabulary the tree itself names — `StoreId`, `PatchOp`, `StreamOp`,
and the snapshot types the handles return (`UploadSlot`, `StoreField`,
`AsyncResult`, `AsyncError`, `AsyncErrorKind`) — lives here because
`musubi-client` depends on this crate and the reverse would be a cycle. Every
one of them is re-exported verbatim from `musubi_client::generated`, so no
consumer path changes.

## Using it

The crate is unpublished: it ships inside the Hex `musubi` package, so a
consumer path-depends on the copy in its `deps/`.

```toml
[dependencies]
musubi-state = { path = "../deps/musubi/crates/musubi-state" }
```

Version is the Hex `musubi` version — one release stream, no pairing table.
MSRV 1.85 (edition 2024). Licensed MIT, see `LICENSE`.

## The design

`docs/rust-reactive-state.md` is normative: §2 signs the five interfaces, §3
the wire integration, and §9 is the semantics appendix this crate's test module
mirrors row for row.
