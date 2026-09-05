//! What is testable without a real window: the callback plumbing over
//! `musubi-state`'s cells and nodes.
//!
//! `#[gpui::test]` builds a `TestAppContext` on gpui's in-process test
//! platform, so these are headless — the same rig
//! `examples/chat_room/desktop` uses. Nothing here paints; what is under test
//! is which notifications reach a view, which do not, and what the
//! [`ListState`] holds afterwards.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use gpui::{
    AppContext as _, Context, Empty, Entity, IntoElement, ListAlignment, ListOffset, ListState,
    Render, TestAppContext, VisualTestContext, Window, px,
};
use musubi_state::{
    Notify, PatchOp, State, StateTree, StoreId, StreamOp, StreamState, Subscription,
};
use serde_json::json;

// ---------------------------------------------------------------------------
// Rig
// ---------------------------------------------------------------------------

/// A view with nowhere to draw: every observation writes into a field the test
/// reads back, and `render` is deliberately empty so nothing but the adapter is
/// under test.
struct Probe {
    seen: Vec<String>,
    list: ListState,
    subs: Vec<Subscription>,
}

impl Probe {
    fn new() -> Self {
        Self {
            seen: Vec::new(),
            list: ListState::new(0, ListAlignment::Top, px(200.0)),
            subs: Vec::new(),
        }
    }
}

impl Render for Probe {
    fn render(&mut self, _window: &mut Window, _cx: &mut Context<Self>) -> impl IntoElement {
        Empty
    }
}

/// A probe in a window, and the visual context the rest of the test drives.
fn probe(cx: &mut TestAppContext) -> (Entity<Probe>, &mut VisualTestContext) {
    cx.add_window_view(|_window, _cx| Probe::new())
}

/// Counts `cx.notify()` on the probe — the only thing [`crate::observe`] does,
/// and not otherwise observable from outside the view.
fn notifications(probe: &Entity<Probe>, cx: &mut VisualTestContext) -> Arc<AtomicUsize> {
    let count = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&count);

    cx.update(|_window, cx| {
        cx.observe(probe, move |_probe, _cx| {
            seen.fetch_add(1, Ordering::SeqCst);
        })
        .detach();
    });

    count
}

/// A root with two independent leaves and one stream slot.
fn seeded() -> StateTree {
    let tree = StateTree::new();

    apply(&tree, |tree| {
        tree.apply(
            &[PatchOp::Replace {
                path: String::new(),
                value: json!({
                    "name": "ada",
                    "count": 0,
                    "messages": {"__musubi_stream__": "messages"},
                }),
            }],
            &[],
        )
    });

    tree
}

/// Runs one transaction and lets its `Notify` drop, which is what invokes the
/// callbacks.
fn apply(
    tree: &StateTree,
    run: impl FnOnce(&StateTree) -> Result<Notify, musubi_state::TreeError>,
) {
    drop(run(tree).expect("the transaction is well formed"));
}

fn set(tree: &StateTree, field: &str, value: serde_json::Value) {
    apply(tree, |tree| {
        tree.apply(
            &[PatchOp::Replace {
                path: format!("/{field}"),
                value,
            }],
            &[],
        )
    });
}

fn insert(tree: &StateTree, item_key: &str, at: i64, body: &str) {
    apply(tree, |tree| {
        tree.apply(
            &[],
            &[StreamOp::Insert {
                stream: "messages".into(),
                store_id: StoreId::root(),
                item_key: item_key.into(),
                at,
                item: json!({"body": body}),
                limit: None,
            }],
        )
    });
}

fn delete(tree: &StateTree, item_key: &str) {
    apply(tree, |tree| {
        tree.apply(
            &[],
            &[StreamOp::Delete {
                stream: "messages".into(),
                store_id: StoreId::root(),
                item_key: item_key.into(),
            }],
        )
    });
}

fn reset(tree: &StateTree) {
    apply(tree, |tree| {
        tree.apply(
            &[],
            &[StreamOp::Reset {
                stream: "messages".into(),
                store_id: StoreId::root(),
            }],
        )
    });
}

fn rows(tree: &StateTree) -> StreamState<serde_json::Value> {
    StreamState::from(
        tree.root::<serde_json::Value>()
            .field("messages")
            .expect("the stream slot is in the seeded root"),
    )
}

// ---------------------------------------------------------------------------
// observe / observe_with / to_view
// ---------------------------------------------------------------------------

/// The hop's whole job: a `Send + Sync` callback fired off the view's thread
/// ends up as one `cx.notify()` on the view.
#[gpui::test]
fn observe_notifies_the_view_when_its_own_node_changes(cx: &mut TestAppContext) {
    let (probe, cx) = probe(cx);
    let tree = seeded();
    let name: State<String> = tree.root::<serde_json::Value>().field("name").unwrap();
    let notified = notifications(&probe, cx);

    probe.update(cx, |view, cx| view.subs.push(crate::observe(&name, cx)));
    cx.run_until_parked();
    notified.store(0, Ordering::SeqCst);

    set(&tree, "name", json!("grace"));
    cx.run_until_parked();

    assert_eq!(notified.load(Ordering::SeqCst), 1);
}

/// The reason per-node subscription is worth an adapter: a sibling's change is
/// not this view's business, and no hop is made for it.
#[gpui::test]
fn observe_stays_silent_when_a_sibling_changes(cx: &mut TestAppContext) {
    let (probe, cx) = probe(cx);
    let tree = seeded();
    let name: State<String> = tree.root::<serde_json::Value>().field("name").unwrap();
    let notified = notifications(&probe, cx);

    probe.update(cx, |view, cx| view.subs.push(crate::observe(&name, cx)));
    cx.run_until_parked();
    notified.store(0, Ordering::SeqCst);

    set(&tree, "count", json!(7));
    cx.run_until_parked();

    assert_eq!(notified.load(Ordering::SeqCst), 0);
}

/// A transaction that changed nothing owes nobody a callback, so the hop is
/// never made either.
#[gpui::test]
fn observe_stays_silent_when_the_value_settles_back(cx: &mut TestAppContext) {
    let (probe, cx) = probe(cx);
    let tree = seeded();
    let name: State<String> = tree.root::<serde_json::Value>().field("name").unwrap();
    let notified = notifications(&probe, cx);

    probe.update(cx, |view, cx| view.subs.push(crate::observe(&name, cx)));
    cx.run_until_parked();
    notified.store(0, Ordering::SeqCst);

    apply(&tree, |tree| {
        tree.apply(
            &[
                PatchOp::Replace {
                    path: "/name".into(),
                    value: json!("grace"),
                },
                PatchOp::Replace {
                    path: "/name".into(),
                    value: json!("ada"),
                },
            ],
            &[],
        )
    });
    cx.run_until_parked();

    assert_eq!(notified.load(Ordering::SeqCst), 0);
}

/// `observe_with` feeds the **handle**, not a value: the body reads through it
/// and sees the settled state.
#[gpui::test]
fn observe_with_feeds_the_handle_and_the_body_reads_through_it(cx: &mut TestAppContext) {
    let (probe, cx) = probe(cx);
    let tree = seeded();
    let name: State<String> = tree.root::<serde_json::Value>().field("name").unwrap();

    probe.update_in(cx, |view, window, cx| {
        view.subs.push(crate::observe_with(
            &name,
            window,
            cx,
            |view, handle, _window, cx| {
                view.seen.push(handle.value());
                cx.notify();
            },
        ));
    });

    set(&tree, "name", json!("grace"));
    cx.run_until_parked();
    set(&tree, "name", json!("hopper"));
    cx.run_until_parked();

    assert_eq!(
        probe.read_with(cx, |view, _| view.seen.clone()),
        vec!["grace".to_string(), "hopper".to_string()],
        "one body run per notification, in transaction order"
    );
}

/// The consequence of the hop being deferred: the body reads the tree as it is
/// **when the body runs**, not as it was when the transaction committed. Two
/// transactions that land before the foreground drains therefore both read the
/// settled value.
///
/// This is the `Change`-carries-no-old/new rule (§2.3) showing through, and it
/// is the right answer for a view: what gets painted is the current state. A
/// consumer that needs per-transaction values wants the value in the
/// notification, which is what [`crate::to_view`] carries.
#[gpui::test]
fn observe_with_reads_the_settled_value_not_the_one_at_commit(cx: &mut TestAppContext) {
    let (probe, cx) = probe(cx);
    let tree = seeded();
    let name: State<String> = tree.root::<serde_json::Value>().field("name").unwrap();

    probe.update_in(cx, |view, window, cx| {
        view.subs.push(crate::observe_with(
            &name,
            window,
            cx,
            |view, handle, _window, cx| {
                view.seen.push(handle.value());
                cx.notify();
            },
        ));
    });

    set(&tree, "name", json!("grace"));
    set(&tree, "name", json!("hopper"));
    cx.run_until_parked();

    assert_eq!(
        probe.read_with(cx, |view, _| view.seen.clone()),
        vec!["hopper".to_string(), "hopper".to_string()]
    );
}

/// RAII, end to end: dropping the token stops the updates, and the foreground
/// task the hop spawned goes with it.
#[gpui::test]
fn dropping_the_subscription_stops_the_updates(cx: &mut TestAppContext) {
    let (probe, cx) = probe(cx);
    let tree = seeded();
    let name: State<String> = tree.root::<serde_json::Value>().field("name").unwrap();

    probe.update_in(cx, |view, window, cx| {
        view.subs.push(crate::observe_with(
            &name,
            window,
            cx,
            |view, handle, _window, cx| {
                view.seen.push(handle.value());
                cx.notify();
            },
        ));
    });

    set(&tree, "name", json!("grace"));
    cx.run_until_parked();

    probe.update(cx, |view, _| view.subs.clear());

    set(&tree, "name", json!("hopper"));
    cx.run_until_parked();

    assert_eq!(
        probe.read_with(cx, |view, _| view.seen.clone()),
        vec!["grace".to_string()]
    );
}

/// `to_view` is generic over the notified **value**, never over the handle —
/// which is what lets `musubi-client`'s out-of-tree `StatusState` and `Upload`
/// use it without this crate depending on that one (§5.1). This test stands in
/// for them with a bare `Send` value, delivered from another thread to prove
/// the returned closure really is `Send + Sync`.
#[gpui::test]
fn to_view_carries_a_value_this_crate_has_never_heard_of(cx: &mut TestAppContext) {
    #[derive(Debug)]
    struct OffTree(&'static str);

    let (probe, cx) = probe(cx);

    let hop = probe.update_in(cx, |_view, window, cx| {
        crate::to_view(
            window,
            cx,
            |view: &mut Probe, status: OffTree, _window, cx| {
                view.seen.push(status.0.to_string());
                cx.notify();
            },
        )
    });

    let hop = Arc::new(hop);
    let sender = Arc::clone(&hop);
    std::thread::spawn(move || sender(OffTree("live")))
        .join()
        .expect("the hop's closure is Send + Sync");
    hop(OffTree("reconnecting"));

    cx.run_until_parked();

    assert_eq!(
        probe.read_with(cx, |view, _| view.seen.clone()),
        vec!["live".to_string(), "reconnecting".to_string()]
    );
}

/// The view is gone: the hop notices once and stops draining rather than
/// spinning for the rest of the app's life, and a notification arriving after
/// the release is simply dropped.
#[gpui::test]
fn a_notification_for_a_released_view_is_dropped(cx: &mut TestAppContext) {
    let tree = seeded();
    let name: State<String> = tree.root::<serde_json::Value>().field("name").unwrap();

    let subscription = cx.update(|cx| {
        // Not in a window, so dropping the handle is what releases it.
        let orphan = cx.new(|_cx| Vec::<String>::new());
        let subscription = orphan.update(cx, |_view, cx| crate::observe(&name, cx));

        drop(orphan);
        subscription
    });

    cx.run_until_parked();
    set(&tree, "name", json!("grace"));
    cx.run_until_parked();

    // What is under test is the absence of a panic and of a spin: the hop's
    // task saw `Err` from `update` and finished instead of looping forever.
    drop(subscription);
}

// ---------------------------------------------------------------------------
// drive_list
// ---------------------------------------------------------------------------

/// Installing the driver aligns the list with whatever the collection already
/// holds, so a caller never has to remember to seed the row count.
#[gpui::test]
fn drive_list_aligns_the_list_when_it_is_installed(cx: &mut TestAppContext) {
    let (probe, cx) = probe(cx);
    let tree = seeded();
    insert(&tree, "a", -1, "one");
    insert(&tree, "b", -1, "two");

    probe.update(cx, |view, cx| {
        let driver = crate::drive_list(&rows(&tree), &view.list, cx);
        view.subs.push(driver);
    });

    assert_eq!(probe.read_with(cx, |view, _| view.list.item_count()), 2);
}

/// The four edit kinds, replayed onto the row count.
#[gpui::test]
fn drive_list_replays_inserts_moves_removals_and_resets(cx: &mut TestAppContext) {
    let (probe, cx) = probe(cx);
    let tree = seeded();
    let rows = rows(&tree);

    probe.update(cx, |view, cx| {
        let driver = crate::drive_list(&rows, &view.list, cx);
        view.subs.push(driver);
    });

    let count = |cx: &mut VisualTestContext| probe.read_with(cx, |view, _| view.list.item_count());

    insert(&tree, "a", -1, "one");
    insert(&tree, "b", -1, "two");
    insert(&tree, "c", -1, "three");
    cx.run_until_parked();
    assert_eq!(count(cx), 3);
    assert_eq!(rows.keys().len(), 3);

    // A move: same key, new position. The row count does not change, and the
    // tree emits `Moved` rather than a remove/insert pair.
    insert(&tree, "c", 0, "three");
    cx.run_until_parked();
    assert_eq!(count(cx), 3);
    assert_eq!(&*rows.keys()[0], "c");

    delete(&tree, "a");
    cx.run_until_parked();
    assert_eq!(count(cx), 2);

    reset(&tree);
    cx.run_until_parked();
    assert_eq!(count(cx), 0);

    // A reset immediately followed by re-inserts is the common wire refresh:
    // `Reset` then two `Inserted`, in one transaction.
    apply(&tree, |tree| {
        tree.apply(
            &[],
            &[
                StreamOp::Reset {
                    stream: "messages".into(),
                    store_id: StoreId::root(),
                },
                StreamOp::Insert {
                    stream: "messages".into(),
                    store_id: StoreId::root(),
                    item_key: "x".into(),
                    at: -1,
                    item: json!({"body": "x"}),
                    limit: None,
                },
                StreamOp::Insert {
                    stream: "messages".into(),
                    store_id: StoreId::root(),
                    item_key: "y".into(),
                    at: -1,
                    item: json!({"body": "y"}),
                    limit: None,
                },
            ],
        )
    });
    cx.run_until_parked();
    assert_eq!(count(cx), 2);
}

/// A change confined to one item's own fields is not a list edit (§6.3): the
/// driver splices nothing, and the row count is untouched.
#[gpui::test]
fn drive_list_splices_nothing_for_a_change_inside_a_row(cx: &mut TestAppContext) {
    let (probe, cx) = probe(cx);
    let tree = seeded();
    let rows = rows(&tree);

    probe.update(cx, |view, cx| {
        let driver = crate::drive_list(&rows, &view.list, cx);
        view.subs.push(driver);
    });

    insert(&tree, "a", -1, "one");
    insert(&tree, "b", -1, "two");
    cx.run_until_parked();

    let notified = notifications(&probe, cx);

    // Same key, same position, new body: the collection is notified with an
    // empty edit slice.
    insert(&tree, "a", 0, "edited");
    cx.run_until_parked();

    assert_eq!(probe.read_with(cx, |view, _| view.list.item_count()), 2);
    assert_eq!(
        notified.load(Ordering::SeqCst),
        1,
        "the view is still told, so a parent-drawn row can repaint"
    );
}

/// The claim §10.2 left open, made falsifiable: an insert is a **splice**, not
/// a `reset`. gpui's `splice` shifts the logical scroll position past the new
/// row; `reset` drops it. Watching the scroll offset is therefore a direct
/// observation of which of the two ran.
#[gpui::test]
fn drive_list_splices_rather_than_resetting(cx: &mut TestAppContext) {
    let (probe, cx) = probe(cx);
    let tree = seeded();
    let rows = rows(&tree);

    for key in ["a", "b", "c"] {
        insert(&tree, key, -1, key);
    }

    probe.update(cx, |view, cx| {
        let driver = crate::drive_list(&rows, &view.list, cx);
        view.subs.push(driver);
        view.list.scroll_to(ListOffset {
            item_ix: 2,
            offset_in_item: px(0.0),
        });
    });

    insert(&tree, "z", 0, "prepended");
    cx.run_until_parked();

    let top = probe.read_with(cx, |view, _| view.list.logical_scroll_top());

    assert_eq!(probe.read_with(cx, |view, _| view.list.item_count()), 4);
    assert_eq!(
        top.item_ix, 3,
        "a prepend shifted the scrolled-to row by one; a reset would have \
         dropped the offset back to 0"
    );
}

/// The driver keeps the row count right even when the collection is edited
/// between `subscribe` and the install-time alignment — the one window the
/// batch's own length exists to close.
#[gpui::test]
fn drive_list_self_corrects_a_batch_that_races_the_install(cx: &mut TestAppContext) {
    let (probe, cx) = probe(cx);
    let tree = seeded();
    let rows = rows(&tree);

    // Standing in for the race: the driver is installed, and a batch that was
    // already in flight is replayed onto a list the alignment has since moved.
    probe.update(cx, |view, cx| {
        let driver = crate::drive_list(&rows, &view.list, cx);
        view.subs.push(driver);
        view.list.reset(9);
    });

    insert(&tree, "a", -1, "one");
    cx.run_until_parked();

    assert_eq!(probe.read_with(cx, |view, _| view.list.item_count()), 1);
}

/// One `Vec<Subscription>` holds every observation a view has, tree or not
/// (§2.4). Nothing here needs a second token type or a `Task<()>` field.
#[gpui::test]
fn every_observation_is_the_same_token(cx: &mut TestAppContext) {
    let (probe, cx) = probe(cx);
    let tree = seeded();
    let root = tree.root::<serde_json::Value>();
    let rows = rows(&tree);

    probe.update_in(cx, |view, window, cx| {
        let name: State<String> = root.field("name").unwrap();
        let count: State<i64> = root.field("count").unwrap();
        let driver = crate::drive_list(&rows, &view.list, cx);

        view.subs = vec![
            crate::observe(&name, cx),
            crate::observe(&rows, cx),
            crate::observe_with(&count, window, cx, |view, handle, _window, cx| {
                view.seen.push(handle.value().to_string());
                cx.notify();
            }),
            driver,
        ];
    });

    set(&tree, "count", json!(3));
    insert(&tree, "a", -1, "one");
    cx.run_until_parked();

    assert_eq!(probe.read_with(cx, |view, _| view.seen.clone()), vec!["3"]);
    assert_eq!(probe.read_with(cx, |view, _| view.list.item_count()), 1);
    assert_eq!(probe.read_with(cx, |view, _| view.subs.len()), 4);
}
