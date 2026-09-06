# musubi-gpui

The gpui adapter for `musubi-state`: the per-view subscription hop, and an
incremental `ListState` driver over keyed collection edits.

Two things, and deliberately nothing else. No widgets, no theme, no rendering.

```rust,ignore
let subs = vec![
    // Redraw on change — the common case.
    musubi_gpui::observe(&state.last_send_status(), cx),
    // Read the new value out of the handle.
    musubi_gpui::observe_with(&state.current_user().name(), window, cx,
        |view, name, window, cx| view.set_draft(&name.value(), window, cx)),
    // The bare hop, for the handles that live outside the tree.
    chat.status().subscribe(musubi_gpui::to_view(window, cx,
        |view, status, _window, cx| { view.status = status; cx.notify(); })),
];

// And the keyed collection, spliced instead of reset.
let driver = musubi_gpui::drive_list(&rows, &self.list, cx);
```

## Why it exists

1. **The `!Send` hop.** A subscription callback is `Fn(Change) + Send + Sync`;
   a gpui entity is `!Send` and thread-affine. Every subscription therefore
   needs the same hop — carry the value to the foreground executor, update the
   entity, `cx.notify()`, branch on the entity having gone. Under per-node
   subscription that hop is written once per view per field rather than once per
   window, which is exactly the repetitive, subtly-wrong-able glue an adapter
   exists to take over.
2. **Keyed edits become list splices.** `ChangeSet::collection_edits` names the
   item keys inserted, removed and moved, and their positions — the input a
   virtualized list wants. `drive_list` replays them onto `ListState::splice`
   instead of wiping every cached row height with `reset(count)`.

`observe` / `observe_with` / `to_view` / `drive_list` is the whole surface.
`UploadSlotState` gets no `observe`: its subscription never fires, and a token
for something that can never ring is worse than no token.

## Scope

Depends on `musubi-state` and gpui, and **never on `musubi-client`** — gpui
cannot reach the client's dependency graph even transitively. `to_view` is
generic over the notified *value* and never over the handle, which is what lets
the client's own out-of-tree handles (`StatusState`, `Upload`) use it from the
far side of that line: their call site is word-for-word the tree handles'.

The crate carries its own `[workspace]` table and its own `Cargo.lock`, and the
root manifest excludes it. Both halves are required, or gpui enters the root
lockfile and `cargo test --workspace` starts building it. Same precedent as
`examples/chat_room/desktop`.

## Two deviations from the design, recorded

`docs/rust-reactive-state.md` §5.1 signs `to_view(cx, apply)` and §6.3 sketches
`drive_list` capturing `cx.to_async()` inside the subscription callback. §10.2
flags both as unverified against gpui 0.2.2. Verified now:

* **`to_view` and `observe_with` take a `&Window`.** Their `apply` takes
  `&mut Window`, and the only route from a background notification to one is
  `Context::spawn_in(window, ..)` → `AsyncWindowContext` →
  `WeakEntity::update_in`; `AsyncWindowContext::new_context` is `pub(crate)` and
  `Context<V>` carries no window handle. `observe` and `drive_list`, whose
  bodies need no window, keep their signed signatures.
* **The hop is a channel, not a captured context.** `AsyncApp` holds an
  `rc::Weak` and a `ForegroundExecutor` marked `!Send` with a `PhantomData<Rc>`,
  so the sketch cannot compile. The *value* is sent down an unbounded channel
  and the foreground drains it. Behaviour, ordering and RAII lifetime are
  unchanged: dropping the `Subscription` drops the sender, which ends the task.

§10.2's other question closes the good way: **`ListState::splice(Range, usize)`
does exist in gpui 0.2.2**, so `drive_list` is genuinely incremental and the
`reset(len)` degrade path §6.3 describes survives only as the arm for
`CollectionEdit`'s `#[non_exhaustive]` future variants.

## Using it

Unpublished, like every crate here: it ships inside the Hex `musubi` package, so
a consumer path-depends on the copy in its `deps/`.

```toml
[dependencies]
musubi-gpui = { path = "../deps/musubi/crates/musubi-gpui" }
```

Version is the Hex `musubi` version — one release stream, no pairing table.
MSRV 1.85 (edition 2024). Licensed MIT, see `LICENSE`.

Design: `docs/rust-reactive-state.md` §5.1 and §6.3 in the Musubi repository.
