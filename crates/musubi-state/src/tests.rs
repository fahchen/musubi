//! The semantics appendix (`docs/rust-reactive-state.md` §9), as tests.
//!
//! Every row of §9.1's equality table, §9.2's transaction rules, §9.3's
//! revision and notification rules, and both worked examples (§9.4, §9.5) has a
//! test here. The module-level ones that follow cover what the appendix leans
//! on: the pointer walk, structural sharing, the carry-over table and the
//! lock discipline.

use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde::Deserialize;
use serde_json::{Value, json};

use crate::arena::MAX_DEPTH;
use crate::change::CollectionEdit;
use crate::error::{ReadError, TreeError};
use crate::node::{AsyncStatus, NodeId, NodeKind, Semantic, SemanticValue};
use crate::state::{AsyncState, State, StoreState, StreamState, UploadSlotState};
use crate::subscription::Subscription;
use crate::tree::StateTree;
use crate::wire::{AsyncError, AsyncResult, PatchOp, StoreId, StreamOp, UploadSlot};

// ------------------------------------------------------------------ helpers

fn replace(path: &str, value: Value) -> PatchOp {
    PatchOp::Replace {
        path: path.to_owned(),
        value,
    }
}

fn add(path: &str, value: Value) -> PatchOp {
    PatchOp::Add {
        path: path.to_owned(),
        value,
    }
}

fn remove(path: &str) -> PatchOp {
    PatchOp::Remove {
        path: path.to_owned(),
    }
}

fn store_id(segments: &[&str]) -> StoreId {
    serde_json::from_value(json!(segments)).expect("a store id is an array of strings")
}

fn insert_op(item_key: &str, at: i64, item: Value, limit: Option<i64>) -> StreamOp {
    StreamOp::Insert {
        stream: "messages".to_owned(),
        store_id: StoreId::root(),
        item_key: item_key.to_owned(),
        at,
        item,
        limit,
    }
}

/// An append onto the `messages` stream of one **child** store.
fn row_insert_op(segments: &[&str], item_key: &str, item: Value) -> StreamOp {
    StreamOp::Insert {
        stream: "messages".to_owned(),
        store_id: store_id(segments),
        item_key: item_key.to_owned(),
        at: -1,
        item,
        limit: None,
    }
}

fn reset_op() -> StreamOp {
    StreamOp::Reset {
        stream: "messages".to_owned(),
        store_id: StoreId::root(),
    }
}

fn delete_op(item_key: &str) -> StreamOp {
    StreamOp::Delete {
        stream: "messages".to_owned(),
        store_id: StoreId::root(),
        item_key: item_key.to_owned(),
    }
}

/// A tree with one committed root value, and its notifications already drained.
fn seeded(value: Value) -> StateTree {
    let tree = StateTree::new();

    commit(&tree, &[replace("", value)], &[]);

    tree
}

/// Applies and lets the notifications run.
fn commit(tree: &StateTree, ops: &[PatchOp], stream_ops: &[StreamOp]) {
    drop(
        tree.apply(ops, stream_ops)
            .expect("the transaction was expected to apply"),
    );
}

/// Records which labelled subscribers were told about a transaction.
#[derive(Clone, Default)]
struct Log(Arc<Mutex<Vec<&'static str>>>);

impl Log {
    fn watch<T>(&self, label: &'static str, state: &State<T>) -> Subscription {
        let log = self.0.clone();

        state.subscribe(move |_| log.lock().expect("test log").push(label))
    }

    fn taken(&self) -> Vec<&'static str> {
        let mut taken = std::mem::take(&mut *self.0.lock().expect("test log"));

        taken.sort_unstable();

        taken
    }
}

/// Applies on a worker thread, so a regression that spins **holding the arena
/// lock** — the shape a parent cycle used to have — fails this test instead of
/// wedging the whole run.
fn apply_within(tree: &StateTree, ops: &[PatchOp]) -> Result<(), TreeError> {
    let (sender, receiver) = std::sync::mpsc::channel();
    let worker = tree.clone();
    let ops = ops.to_vec();

    std::thread::spawn(move || {
        let outcome = worker.apply(&ops, &[]).map(drop);

        let _ = sender.send(outcome);
    });

    receiver
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("the transaction was expected to finish rather than spin under the lock")
}

/// Every node reachable from the root, and the three assertions §3.2 rests on:
/// the tree is a tree — no node is reachable twice, none is its own ancestor,
/// and the arena holds nothing the root cannot reach.
fn assert_is_a_tree(tree: &StateTree) -> Vec<NodeId> {
    let arena = tree.inner().lock();
    let mut seen = HashSet::new();
    let mut stack = vec![arena.root];
    let mut nodes = Vec::new();

    while let Some(id) = stack.pop() {
        assert!(seen.insert(id), "node {id:?} is reachable from two parents");
        assert!(nodes.len() < 10_000, "the walk from the root does not end");

        nodes.push(id);
        stack.extend(arena.children(id));
    }

    for id in &nodes {
        let mut cursor = arena.nodes[*id].parent;
        let mut steps = 0;

        while let Some(current) = cursor {
            assert_ne!(current, *id, "node {id:?} is its own ancestor");

            steps += 1;

            assert!(steps <= MAX_DEPTH + 1, "node {id:?} has no root above it");

            cursor = arena.nodes[current].parent;
        }
    }

    // A committed transaction frees everything it detached, the null
    // placeholders adoption left behind included.
    assert_eq!(
        nodes.len(),
        arena.nodes.len(),
        "the arena holds nodes the root cannot reach"
    );

    nodes
}

/// One node's cached semantic value.
fn semantic(tree: &StateTree, id: NodeId) -> SemanticValue {
    tree.node(id).expect("the node is live").semantic
}

/// The semantic value a parent holds for one of its object keys.
fn field_semantic(tree: &StateTree, parent: NodeId, key: &str) -> SemanticValue {
    let semantic = semantic(tree, parent);
    let fields = match semantic.get() {
        Semantic::Object(fields) | Semantic::Store { fields, .. } => fields.clone(),
        other => panic!("not an object: {other:?}"),
    };

    fields
        .into_iter()
        .find(|(name, _)| &**name == key)
        .map(|(_, value)| value)
        .expect("no such key")
}

// -------------------------------------------------------------- §9.1 equality

#[test]
fn null_is_always_equal() {
    let tree = seeded(json!({"a": null}));
    let node = tree.root::<Value>().field::<Value>("a").expect("a").node();
    let before = tree.node(node).expect("live").revision;

    commit(&tree, &[replace("/a", json!(null))], &[]);

    assert_eq!(tree.node(node).expect("live").revision, before);
}

#[test]
fn scalars_compare_by_value_and_a_retyped_number_is_a_change() {
    let tree = seeded(json!({"n": 1, "s": "a", "b": true}));
    let root = tree.root::<Value>();
    let log = Log::default();
    let number = root.field::<i64>("n").expect("n");
    let _watch = log.watch("n", &number);

    // Same value, no notification.
    commit(&tree, &[replace("/n", json!(1))], &[]);

    assert!(log.taken().is_empty());

    // `1` and `1.0` are different `serde_json::Number`s, so this is a change.
    commit(&tree, &[replace("/n", json!(1.0))], &[]);

    assert_eq!(log.taken(), ["n"]);

    commit(&tree, &[replace("/s", json!("b"))], &[]);
    commit(&tree, &[replace("/b", json!(false))], &[]);

    assert!(!root.field::<bool>("b").expect("b").value());
    assert_eq!(root.field::<String>("s").expect("s").value(), "b");
}

#[test]
fn an_object_compares_by_key_set_and_key_order_is_not_part_of_the_value() {
    let tree = seeded(json!({"o": {"a": 1, "b": 2}}));
    let log = Log::default();
    let object = tree.root::<Value>().field::<Value>("o").expect("o");
    let _watch = log.watch("o", &object);

    // The same pairs in the other order: `NodeKind::Object` is a `BTreeMap`, so
    // the value is identical and nobody is told.
    commit(&tree, &[replace("/o", json!({"b": 2, "a": 1}))], &[]);

    assert!(log.taken().is_empty());

    // A different key set is a change even when the shared keys are equal.
    commit(
        &tree,
        &[replace("/o", json!({"a": 1, "b": 2, "c": 3}))],
        &[],
    );

    assert_eq!(log.taken(), ["o"]);
}

#[test]
fn a_store_node_compares_by_store_id_and_by_its_fields() {
    let tree = seeded(json!({"__musubi_store_id__": [], "total": 1}));
    let log = Log::default();
    let root = tree.root::<Value>();
    let _watch = log.watch("root", &root);

    commit(
        &tree,
        &[replace("", json!({"__musubi_store_id__": [], "total": 1}))],
        &[],
    );

    assert!(log.taken().is_empty());

    // A different store id at the same position is a different value.
    commit(
        &tree,
        &[replace(
            "",
            json!({"__musubi_store_id__": ["other"], "total": 1}),
        )],
        &[],
    );

    assert_eq!(log.taken(), ["root"]);
    assert_eq!(tree.store_ids(), [store_id(&["other"])]);
}

#[test]
fn an_array_compares_by_length_and_by_index() {
    let tree = seeded(json!({"list": [1, 2]}));
    let log = Log::default();
    let list = tree
        .root::<Value>()
        .field::<Vec<i64>>("list")
        .expect("list");
    let _watch = log.watch("list", &list.clone().cast::<Value>());

    commit(&tree, &[replace("/list", json!([1, 2]))], &[]);

    assert!(log.taken().is_empty());

    commit(&tree, &[replace("/list", json!([1, 2, 3]))], &[]);

    assert_eq!(log.taken(), ["list"]);
    assert_eq!(list.len(), 3);
    assert_eq!(list.at(2).expect("index 2").value(), 3);
    assert_eq!(list.first().expect("first").value(), 1);
    assert_eq!(list.last().expect("last").value(), 3);
    assert_eq!(
        list.iter().map(|item| item.value()).collect::<Vec<i64>>(),
        [1, 2, 3]
    );
}

#[test]
fn a_collection_compares_as_an_ordered_key_sequence() {
    let tree = streaming_tree();
    let messages = messages_state(&tree);
    let log = Log::default();
    let _watch = log.watch("messages", &messages.as_state().cast::<Value>());

    commit(
        &tree,
        &[],
        &[
            insert_op("a", -1, json!({"id": "a"}), None),
            insert_op("b", -1, json!({"id": "b"}), None),
        ],
    );

    assert_eq!(log.taken(), ["messages"]);
    assert_eq!(keys(&messages), ["a", "b"]);

    // Re-inserting the same items at the same positions with the same values
    // changes nothing.
    commit(
        &tree,
        &[],
        &[
            insert_op("a", 0, json!({"id": "a"}), None),
            insert_op("b", -1, json!({"id": "b"}), None),
        ],
    );

    assert!(log.taken().is_empty());

    // A pure reorder is a change: order is part of the collection's value.
    commit(&tree, &[], &[insert_op("b", 0, json!({"id": "b"}), None)]);

    assert_eq!(log.taken(), ["messages"]);
    assert_eq!(keys(&messages), ["b", "a"]);
}

#[test]
fn an_async_node_compares_by_status_result_and_reason() {
    let tree = seeded(json!({
        "feed": {"__musubi_async__": true, "status": "loading", "result": null, "reason": null}
    }));
    let feed = AsyncState::from(tree.root::<Value>().field::<i64>("feed").expect("feed"));
    let log = Log::default();
    let watched = feed.clone();
    let hits = Arc::new(AtomicUsize::new(0));
    let seen = hits.clone();
    let _watch = watched.subscribe(move |_| {
        seen.fetch_add(1, Ordering::SeqCst);
    });

    assert_eq!(feed.status(), AsyncStatus::Loading);
    assert!(feed.result().is_none());

    commit(
        &tree,
        &[replace(
            "/feed",
            json!({"__musubi_async__": true, "status": "ok", "result": 7, "reason": null}),
        )],
        &[],
    );

    assert_eq!(hits.load(Ordering::SeqCst), 1);
    assert_eq!(feed.status(), AsyncStatus::Ok);
    assert_eq!(feed.result().expect("a result").value(), 7);
    assert_eq!(
        feed.value(),
        AsyncResult::Ok {
            result: 7,
            reason: None
        }
    );

    // A status flip alone is still a change to *this* node (§3.3) — and the
    // result subtree, which kept its value, is not told.
    let result = feed.result().expect("a result");
    let result_revision = result.revision();

    commit(
        &tree,
        &[replace(
            "/feed",
            json!({"__musubi_async__": true, "status": "loading", "result": 7, "reason": null}),
        )],
        &[],
    );

    assert_eq!(hits.load(Ordering::SeqCst), 2);
    assert_eq!(feed.status(), AsyncStatus::Loading);
    assert_eq!(result.revision(), result_revision);
    let _ = log;
}

#[test]
fn an_op_addressing_an_async_node_writes_the_field_it_names() {
    // The shape every `async_result` cycle arrives in: the server patches
    // `/feed/status` and `/feed/reason` as separate ops, so the walk has to
    // treat `status` as the node's own semantics and `reason` as a child
    // (§3.3).
    let tree = seeded(json!({
        "feed": {"__musubi_async__": true, "status": "loading", "result": 7, "reason": null}
    }));
    let feed = AsyncState::from(tree.root::<Value>().field::<i64>("feed").expect("feed"));
    let result = feed.result().expect("a result");
    let result_revision = result.revision();

    commit(
        &tree,
        &[
            replace("/feed/status", json!("failed")),
            replace("/feed/reason", json!({"kind": "error", "value": "nope"})),
        ],
        &[],
    );

    assert_eq!(feed.status(), AsyncStatus::Failed);
    assert_eq!(
        result.revision(),
        result_revision,
        "the preserved result is untouched by either op"
    );
    assert!(matches!(
        feed.value(),
        AsyncResult::Failed {
            result: Some(7),
            reason: Some(AsyncError::Structured { .. })
        }
    ));

    // Writing the same status back is not a change.
    let revision = feed.revision();

    commit(&tree, &[replace("/feed/status", json!("failed"))], &[]);

    assert_eq!(feed.revision(), revision);

    // A status outside the three the wire defines is an application failure,
    // and the transaction that carried it is rolled back whole.
    let error = tree
        .apply(&[replace("/feed/status", json!("queued"))], &[])
        .expect_err("an unknown status is refused");

    assert!(matches!(error, TreeError::Pointer { .. }));
    assert_eq!(feed.status(), AsyncStatus::Failed);
}

#[test]
fn an_upload_slot_is_inert_and_carries_both_halves_of_its_key() {
    let tree = seeded(json!({
        "__musubi_store_id__": [],
        "panel": {"__musubi_store_id__": ["panel"], "avatar": {"__musubi_upload__": "avatar"}}
    }));
    let panel = tree.root::<Value>().field::<Value>("panel").expect("panel");
    let slot = UploadSlotState::from(panel.field::<UploadSlot>("avatar").expect("avatar"));
    let hits = Arc::new(AtomicUsize::new(0));
    let seen = hits.clone();
    let _watch = slot.subscribe(move |_| {
        seen.fetch_add(1, Ordering::SeqCst);
    });

    // The owner is the *nearest enclosing store*, not the root — the half a
    // call site used to spell by hand, wrongly, for a slot in a child store.
    let (owner, name) = slot.key().expect("a live slot");

    assert_eq!(owner, store_id(&["panel"]));
    assert_eq!(&*name, "avatar");
    assert_eq!(
        slot.value(),
        UploadSlot {
            name: "avatar".to_owned()
        }
    );

    let revision = slot.revision();

    // The server re-renders the same marker every cycle; the slot never fires.
    commit(
        &tree,
        &[replace(
            "/panel/avatar",
            json!({"__musubi_upload__": "avatar"}),
        )],
        &[],
    );

    assert_eq!(hits.load(Ordering::SeqCst), 0);
    assert_eq!(slot.revision(), revision);
}

#[test]
fn two_kinds_are_never_equal() {
    let tree = seeded(json!({"a": []}));
    let log = Log::default();
    let node = tree.root::<Value>().field::<Value>("a").expect("a");
    let _watch = log.watch("a", &node);

    // An empty array and an empty object project differently and compare
    // differently, even though both are "empty".
    commit(&tree, &[replace("/a", json!({}))], &[]);

    assert_eq!(log.taken(), ["a"]);
}

// ---------------------------------------------------------- §9.2 transactions

#[test]
fn one_to_two_to_one_inside_a_transaction_notifies_nobody() {
    let tree = seeded(json!({"count": 1}));
    let count = tree.root::<Value>().field::<i64>("count").expect("count");
    let log = Log::default();
    let _watch = log.watch("count", &count);
    let revision = count.revision();

    commit(
        &tree,
        &[replace("/count", json!(2)), replace("/count", json!(1))],
        &[],
    );

    assert!(log.taken().is_empty());
    assert_eq!(count.revision(), revision);
    assert_eq!(count.value(), 1);
}

#[test]
fn one_to_two_to_one_across_two_apply_calls_of_one_transaction_notifies_nobody() {
    let tree = seeded(json!({"count": 1}));
    let count = tree.root::<Value>().field::<i64>("count").expect("count");
    let log = Log::default();
    let _watch = log.watch("count", &count);
    let revision = count.revision();

    let mut transaction = tree.begin();

    transaction
        .apply(&[replace("/count", json!(2))], &[])
        .expect("applies");
    transaction
        .apply(&[replace("/count", json!(1))], &[])
        .expect("applies");

    drop(transaction.commit());

    assert!(log.taken().is_empty());
    assert_eq!(count.revision(), revision);
}

#[test]
fn a_failed_op_rolls_the_whole_transaction_back() {
    let tree = seeded(json!({"count": 1, "items": [{"name": "foo"}]}));
    let root = tree.root::<Value>();
    let count = root.field::<i64>("count").expect("count");
    let name = root
        .field::<Value>("items")
        .and_then(|items| items.cast::<Vec<Value>>().at(0))
        .and_then(|item| item.field::<String>("name"))
        .expect("items[0].name");
    let log = Log::default();
    let _watch = log.watch("count", &count);
    let before = (
        tree.len(),
        count.revision(),
        name.node(),
        tree.to_wire(tree.root_id()),
    );

    let error = tree
        .apply(
            &[
                replace("/count", json!(9)),
                add("/items/-", json!({"name": "new"})),
                // Nothing resolves here: the transaction unwinds.
                replace("/missing/deep", json!(1)),
            ],
            &[],
        )
        .expect_err("the third op cannot resolve");

    assert!(matches!(error, TreeError::Pointer { .. }));
    assert!(log.taken().is_empty());
    assert_eq!(
        (
            tree.len(),
            count.revision(),
            name.node(),
            tree.to_wire(tree.root_id())
        ),
        before
    );
    assert_eq!(count.value(), 1);
}

#[test]
fn a_dropped_transaction_rolls_back_and_the_tree_is_untouched() {
    let tree = seeded(json!({"count": 1}));
    let count = tree.root::<Value>().field::<i64>("count").expect("count");
    let before = tree.to_wire(tree.root_id());

    {
        let mut transaction = tree.begin();

        transaction
            .apply(&[replace("/count", json!(2))], &[])
            .expect("applies");

        // Mid-transaction the projection already shows the new value.
        assert_eq!(
            transaction
                .to_hydrated(transaction.root_id())
                .expect("a live root"),
            json!({"count": 2})
        );
    }

    assert_eq!(tree.to_wire(tree.root_id()), before);
    assert_eq!(count.value(), 1);
}

#[test]
fn ops_apply_left_to_right_and_before_stream_ops() {
    let tree = StateTree::new();

    // The `replace ""` is what creates the slot the insert in the same envelope
    // fills; the other order could not work (§3.1).
    commit(
        &tree,
        &[replace(
            "",
            json!({"__musubi_store_id__": [], "messages": {"__musubi_stream__": "messages"}}),
        )],
        &[insert_op("a", -1, json!({"id": "a"}), None)],
    );

    let messages = StreamState::from(
        tree.root::<Value>()
            .field::<Vec<Value>>("messages")
            .expect("messages"),
    );

    assert_eq!(keys(&messages), ["a"]);
}

// -------------------------------------------------- §9.3 revisions and notice

#[test]
fn a_fresh_root_has_revision_zero_until_the_first_patch_lands() {
    let tree = StateTree::new();

    assert_eq!(tree.root::<Value>().revision(), 0);

    commit(&tree, &[replace("", json!({"count": 1}))], &[]);

    assert_eq!(tree.root::<Value>().revision(), 1);
}

#[test]
fn only_the_dirty_path_is_recomputed_and_untouched_siblings_keep_everything() {
    let tree = seeded(json!({"count": 1, "items": [{"name": "foo"}]}));
    let root = tree.root::<Value>();
    let root_id = root.node();
    let count = root.field::<i64>("count").expect("count");
    let before_revision = count.revision();
    let before_semantic = semantic(&tree, count.node());
    let before_in_parent = field_semantic(&tree, root_id, "count");

    assert!(before_semantic.is_shared_with(&before_in_parent));

    commit(&tree, &[replace("/items/0/name", json!("bar"))], &[]);

    let after_semantic = semantic(&tree, count.node());
    let after_in_parent = field_semantic(&tree, root_id, "count");

    // The sibling was never compared, never recomputed and never bumped: the
    // root's recompute copied the very `Arc` it already held.
    assert_eq!(count.revision(), before_revision);
    assert!(before_semantic.is_shared_with(&after_semantic));
    assert!(before_semantic.is_shared_with(&after_in_parent));
}

#[test]
fn a_removed_node_is_notified_once_and_then_reads_as_dead() {
    let tree = seeded(json!({"a": {"deep": 1}}));
    let a = tree.root::<Value>().field::<Value>("a").expect("a");
    let deep = a.field::<i64>("deep").expect("deep");
    let log = Log::default();
    let _watch_a = log.watch("a", &a);
    let _watch_deep = log.watch("deep", &deep);

    commit(&tree, &[remove("/a")], &[]);

    // The whole freed subtree is told once, so a view bound anywhere inside it
    // learns to tear itself down.
    assert_eq!(log.taken(), ["a", "deep"]);
    assert!(!a.is_live());
    assert!(!deep.is_live());
    assert!(matches!(a.try_value(), Err(ReadError::Gone)));
    assert_eq!(a.revision(), 0);

    // And nothing fires a second time.
    commit(&tree, &[replace("", json!({"b": 1}))], &[]);

    assert!(log.taken().is_empty());
}

#[test]
fn a_replace_reconciles_rather_than_destroying_the_nodes_it_did_not_change() {
    let tree = seeded(json!({"count": 1, "items": [{"name": "foo"}]}));
    let root = tree.root::<Value>();
    let root_id = root.node();
    let count = root.field::<i64>("count").expect("count");
    let name = root
        .field::<Vec<Value>>("items")
        .and_then(|items| items.at(0))
        .and_then(|item| item.field::<String>("name"))
        .expect("items[0].name");
    let log = Log::default();
    let _watch = log.watch("name", &name);
    let ids = (root_id, count.node(), name.node());

    // A whole-root replace, with one leaf different.
    commit(
        &tree,
        &[replace("", json!({"count": 1, "items": [{"name": "bar"}]}))],
        &[],
    );

    assert_eq!(
        (
            tree.root_id(),
            count.node(),
            root.field::<Vec<Value>>("items")
                .and_then(|items| items.at(0))
                .and_then(|item| item.field::<String>("name"))
                .expect("items[0].name")
                .node()
        ),
        ids
    );
    // The subscriber registered before the replace is still the node's own.
    assert_eq!(log.taken(), ["name"]);
    assert_eq!(name.value(), "bar");
    assert!(name.is_live());
}

// --------------------------------------------------------- §9.4 worked example

#[test]
fn worked_example_of_handoff_section_31() {
    let tree = seeded(json!({"count": 1, "items": [{"name": "foo"}]}));
    let root = tree.root::<Value>();
    let count = root.field::<i64>("count").expect("count");
    let items = root.field::<Vec<Value>>("items").expect("items");
    let item = items.at(0).expect("items[0]");
    let name = item.field::<String>("name").expect("name");
    let log = Log::default();

    let _a = log.watch("A", &count);
    let _b = log.watch("B", &items.clone().cast::<Value>());
    let _c = log.watch("C", &item);
    let _d = log.watch("D", &name);
    let _e = log.watch("E", &root);

    let notify = tree
        .apply(&[replace("/items/0/name", json!("bar"))], &[])
        .expect("applies");

    // Children before parents, and `count` was never even compared.
    assert_eq!(
        notify.changes().changed(),
        [name.node(), item.node(), items.node(), root.node()]
    );
    assert!(!notify.changes().contains(count.node()));

    drop(notify);

    assert_eq!(log.taken(), ["B", "C", "D", "E"]);
}

// --------------------------------------------------------- §9.5 worked example

/// The §9.5 tree: a root store whose `feed.messages` is a stream slot.
fn streaming_tree() -> StateTree {
    seeded(json!({
        "__musubi_store_id__": [],
        "title": "Inbox",
        "current_user": {"name": "me"},
        "feed": {"messages": {"__musubi_stream__": "messages"}}
    }))
}

fn messages_state(tree: &StateTree) -> StreamState<Value> {
    let feed = tree.root::<Value>().field::<Value>("feed").expect("feed");

    StreamState::from(feed.field::<Vec<Value>>("messages").expect("messages"))
}

fn keys(messages: &StreamState<Value>) -> Vec<String> {
    messages
        .keys()
        .into_iter()
        .map(|key| key.to_string())
        .collect()
}

#[test]
fn worked_example_of_a_stream_insert() {
    let tree = streaming_tree();
    let messages = messages_state(&tree);

    commit(
        &tree,
        &[],
        &[
            insert_op("msg-2", -1, json!({"id": "2", "body": "two"}), None),
            insert_op("msg-1", -1, json!({"id": "1", "body": "one"}), None),
        ],
    );

    let root = tree.root::<Value>();
    let feed = root.field::<Value>("feed").expect("feed");
    let title = root.field::<String>("title").expect("title");
    let message_one = messages.by_key("msg-1").expect("msg-1");
    let log = Log::default();

    let _a = log.watch("A", &title);
    let _b = log.watch("B", &feed);
    let _c = log.watch("C", &messages.as_state().cast::<Value>());
    let _d = log.watch("D", &message_one);
    let _e = log.watch("E", &root);

    let (revision_one, node_one) = (message_one.revision(), message_one.node());
    let node_two = messages.by_key("msg-2").expect("msg-2").node();

    let notify = tree
        .apply(
            &[],
            &[insert_op(
                "msg-3",
                0,
                json!({"id": "3", "body": "hi"}),
                Some(-100),
            )],
        )
        .expect("applies");

    let edits = notify.changes().collection_edits(messages.node()).to_vec();

    drop(notify);

    // Notified: C, B, E. Not notified: A (a sibling field), D (an item whose
    // own value did not change).
    assert_eq!(log.taken(), ["B", "C", "E"]);
    assert_eq!(keys(&messages), ["msg-3", "msg-2", "msg-1"]);
    assert_eq!(message_one.revision(), revision_one);
    assert_eq!(message_one.node(), node_one);
    assert_eq!(messages.by_key("msg-2").expect("msg-2").node(), node_two);
    assert_eq!(
        edits,
        [CollectionEdit::Inserted {
            item_key: Arc::from("msg-3"),
            index: 0,
            node: messages.by_key("msg-3").expect("msg-3").node(),
        }]
    );
}

#[test]
fn a_pure_reposition_moves_the_row_and_leaves_it_unnotified() {
    let tree = streaming_tree();
    let messages = messages_state(&tree);

    commit(
        &tree,
        &[],
        &[
            insert_op("msg-2", -1, json!({"id": "2"}), None),
            insert_op("msg-1", -1, json!({"id": "1"}), None),
        ],
    );

    let message_one = messages.by_key("msg-1").expect("msg-1");
    let log = Log::default();
    let _c = log.watch("C", &messages.as_state().cast::<Value>());
    let _d = log.watch("D", &message_one);
    let node = message_one.node();

    let notify = tree
        .apply(&[], &[insert_op("msg-1", 0, json!({"id": "1"}), None)])
        .expect("applies");
    let edits = notify.changes().collection_edits(messages.node()).to_vec();

    drop(notify);

    assert_eq!(log.taken(), ["C"]);
    assert_eq!(keys(&messages), ["msg-1", "msg-2"]);
    assert_eq!(messages.by_key("msg-1").expect("msg-1").node(), node);
    assert_eq!(
        edits,
        [CollectionEdit::Moved {
            item_key: Arc::from("msg-1"),
            from: 1,
            to: 0,
        }]
    );
}

#[test]
fn a_limit_trim_removes_the_overflow_row_and_kills_its_node() {
    let tree = streaming_tree();
    let messages = messages_state(&tree);

    commit(
        &tree,
        &[],
        &[
            insert_op("a", -1, json!({"id": "a"}), Some(-2)),
            insert_op("b", -1, json!({"id": "b"}), Some(-2)),
        ],
    );

    let dropped = messages.by_key("a").expect("a");
    let log = Log::default();
    let _watch = log.watch("a", &dropped);

    let notify = tree
        .apply(&[], &[insert_op("c", -1, json!({"id": "c"}), Some(-2))])
        .expect("applies");
    let edits = notify.changes().collection_edits(messages.node()).to_vec();

    drop(notify);

    assert_eq!(log.taken(), ["a"]);
    assert!(!dropped.is_live());
    assert_eq!(keys(&messages), ["b", "c"]);
    assert_eq!(
        edits,
        [
            CollectionEdit::Inserted {
                item_key: Arc::from("c"),
                index: 2,
                node: messages.by_key("c").expect("c").node(),
            },
            CollectionEdit::Removed {
                item_key: Arc::from("a"),
                index: 0,
            }
        ]
    );
}

#[test]
fn a_trim_of_several_rows_reports_them_in_removal_order_from_either_end() {
    // The overflow leaves in one `drain` / `split_off` rather than one
    // `Vec::remove` per row — the row-by-row form cost O(n·k) and took 116 ms
    // for a single op against a 20 000-row list. What a list adapter replays is
    // the edit sequence (§6.3), so it is the edit sequence that has to be
    // identical: each index is the one its row held at the moment it was taken
    // out, which for a front trim is always `0` and for a back trim counts down.
    let front = {
        let tree = streaming_tree();
        let messages = messages_state(&tree);

        commit(
            &tree,
            &[],
            &[
                insert_op("a", -1, json!({"id": "a"}), None),
                insert_op("b", -1, json!({"id": "b"}), None),
                insert_op("c", -1, json!({"id": "c"}), None),
                insert_op("d", -1, json!({"id": "d"}), None),
            ],
        );

        let notify = tree
            .apply(&[], &[insert_op("e", -1, json!({"id": "e"}), Some(-2))])
            .expect("applies");
        let edits = notify.changes().collection_edits(messages.node()).to_vec();

        drop(notify);

        assert_eq!(keys(&messages), ["d", "e"]);

        edits
    };

    assert_eq!(
        front[1..],
        [
            CollectionEdit::Removed {
                item_key: Arc::from("a"),
                index: 0,
            },
            CollectionEdit::Removed {
                item_key: Arc::from("b"),
                index: 0,
            },
            CollectionEdit::Removed {
                item_key: Arc::from("c"),
                index: 0,
            },
        ]
    );

    let back = {
        let tree = streaming_tree();
        let messages = messages_state(&tree);

        commit(
            &tree,
            &[],
            &[
                insert_op("a", -1, json!({"id": "a"}), None),
                insert_op("b", -1, json!({"id": "b"}), None),
                insert_op("c", -1, json!({"id": "c"}), None),
                insert_op("d", -1, json!({"id": "d"}), None),
            ],
        );

        let notify = tree
            .apply(&[], &[insert_op("e", 0, json!({"id": "e"}), Some(-2))])
            .expect("applies");
        let edits = notify.changes().collection_edits(messages.node()).to_vec();

        drop(notify);

        assert_eq!(keys(&messages), ["e", "a"]);

        edits
    };

    assert_eq!(
        back[1..],
        [
            CollectionEdit::Removed {
                item_key: Arc::from("d"),
                index: 4,
            },
            CollectionEdit::Removed {
                item_key: Arc::from("c"),
                index: 3,
            },
            CollectionEdit::Removed {
                item_key: Arc::from("b"),
                index: 2,
            },
        ]
    );
}

// -------------------------------------------------------- keyed reconciliation

#[test]
fn a_reset_and_reinsert_refresh_carries_every_row_node_over() {
    let tree = streaming_tree();
    let messages = messages_state(&tree);

    commit(
        &tree,
        &[],
        &[
            insert_op("a", -1, json!({"id": "a", "body": "one"}), None),
            insert_op("b", -1, json!({"id": "b", "body": "two"}), None),
        ],
    );

    let row_a = messages.by_key("a").expect("a");
    let row_b = messages.by_key("b").expect("b");
    let body_a = row_a.field::<String>("body").expect("body");
    let (node_a, node_b, revision_b) = (row_a.node(), row_b.node(), row_b.revision());
    let log = Log::default();
    let _a = log.watch("a", &row_a);
    let _b = log.watch("b", &row_b);
    let _body = log.watch("body-a", &body_a);

    // The most common refresh on the wire: `[reset] ++ inserts` in one envelope.
    commit(
        &tree,
        &[],
        &[
            reset_op(),
            insert_op("a", -1, json!({"id": "a", "body": "edited"}), None),
            insert_op("b", -1, json!({"id": "b", "body": "two"}), None),
        ],
    );

    // Both rows kept their nodes and their subscribers; only the row whose
    // value moved was told, and only through the field that moved.
    assert_eq!(messages.by_key("a").expect("a").node(), node_a);
    assert_eq!(messages.by_key("b").expect("b").node(), node_b);
    assert_eq!(log.taken(), ["a", "body-a"]);
    assert_eq!(row_b.revision(), revision_b);
    assert_eq!(body_a.value(), "edited");
    assert!(row_a.is_live());
}

#[test]
fn a_carried_row_that_nothing_reinserts_is_freed_at_commit() {
    let tree = streaming_tree();
    let messages = messages_state(&tree);

    commit(&tree, &[], &[insert_op("a", -1, json!({"id": "a"}), None)]);

    let row = messages.by_key("a").expect("a");
    let log = Log::default();
    let _watch = log.watch("a", &row);

    commit(&tree, &[], &[reset_op()]);

    assert_eq!(log.taken(), ["a"]);
    assert!(!row.is_live());
    assert!(messages.is_empty());
}

#[test]
fn an_upsert_keeps_the_row_node_and_pushes_only_the_field_that_moved() {
    let tree = streaming_tree();
    let messages = messages_state(&tree);

    commit(
        &tree,
        &[],
        &[insert_op("a", -1, json!({"id": "a", "body": "hi"}), None)],
    );

    let row = messages.by_key("a").expect("a");
    let id = row.field::<String>("id").expect("id");
    let body = row.field::<String>("body").expect("body");
    let (node, id_revision) = (row.node(), id.revision());
    let log = Log::default();
    let _id = log.watch("id", &id);
    let _body = log.watch("body", &body);

    commit(
        &tree,
        &[],
        &[insert_op(
            "a",
            -1,
            json!({"id": "a", "body": "edited"}),
            None,
        )],
    );

    assert_eq!(messages.by_key("a").expect("a").node(), node);
    assert_eq!(log.taken(), ["body"]);
    assert_eq!(id.revision(), id_revision);
}

#[test]
fn a_brand_new_row_notifies_the_list_and_neither_the_row_nor_its_siblings() {
    // Inserting an item is a change to the **container** and to nothing else:
    // the item node did not change, it did not exist. Nothing dirties a node
    // `build` created, so a fresh node never enters the settle set and carries
    // no `Change` at all; a sibling that only kept its slot carries none
    // either (§9.3). The same statement for a plain array, whose insert takes
    // the other write path.
    let tree = seeded(json!({
        "__musubi_store_id__": [],
        "feed": {"messages": {"__musubi_stream__": "messages"}},
        "list": [{"n": 1}]
    }));
    let messages = messages_state(&tree);

    commit(
        &tree,
        &[],
        &[insert_op("old", -1, json!({"id": "old"}), None)],
    );

    let list = tree
        .root::<Value>()
        .field::<Vec<Value>>("list")
        .expect("list");
    let sibling = messages.by_key("old").expect("old");
    let element = list.at(0).expect("list[0]");
    let log = Log::default();
    let _collection = log.watch("collection", &messages.as_state());
    let _sibling = log.watch("sibling", &sibling);
    let _array = log.watch("array", &list);
    let _element = log.watch("element", &element);

    let notify = tree
        .apply(
            &[add("/list/-", json!({"n": 2}))],
            &[insert_op("new", -1, json!({"id": "new"}), None)],
        )
        .expect("applies");
    let fresh_row = messages.by_key("new").expect("new").node();
    let fresh_element = list.at(1).expect("list[1]").node();

    assert!(notify.changes().contains(messages.node()));
    assert!(notify.changes().contains(list.node()));
    assert!(!notify.changes().contains(fresh_row));
    assert!(!notify.changes().contains(fresh_element));

    drop(notify);

    // Exactly the two containers, exactly once each.
    assert_eq!(log.taken(), ["array", "collection"]);
    // A node a transaction created starts at revision `1` — it *was* touched by
    // a transaction — and is not bumped again for arriving.
    assert_eq!(tree.node(fresh_row).expect("the new row").revision, 1);
    assert_eq!(
        tree.node(fresh_element).expect("the new element").revision,
        1
    );
}

#[test]
fn a_stream_row_adopted_out_by_a_patch_records_the_removal_its_delete_no_longer_can() {
    // Patch ops run before stream ops (§3.6), so "the render moves a store out
    // of a stream row, and the same envelope deletes that row" arrives as an
    // adoption followed by a delete that finds nothing left to delete. The
    // adoption is what took the row out of the list, so the adoption is what
    // has to record it: a list adapter replays `collection_edits` and nothing
    // else (§6.3), and a `Removed` nobody recorded leaves a stale row on screen.
    let tree = seeded(json!({
        "__musubi_store_id__": [],
        "feed": {"messages": {"__musubi_stream__": "messages"}},
        "slot": null
    }));
    let messages = messages_state(&tree);
    let row = json!({
        "__musubi_store_id__": ["a"],
        "inner": {"__musubi_stream__": "messages"}
    });

    commit(
        &tree,
        &[],
        &[
            insert_op("a", -1, row.clone(), None),
            insert_op("b", -1, json!({"id": "b"}), None),
            row_insert_op(&["a"], "a1", json!({"id": "a1"})),
        ],
    );

    let store = StoreState::from(messages.by_key("a").expect("a"));
    let node = store.node();

    let notify = tree
        .apply(&[replace("/slot", row.clone())], &[delete_op("a")])
        .expect("applies");
    let edits = notify.changes().collection_edits(messages.node()).to_vec();

    drop(notify);

    assert_is_a_tree(&tree);
    assert_eq!(keys(&messages), ["b"]);
    assert_eq!(
        edits,
        [CollectionEdit::Removed {
            item_key: Arc::from("a"),
            index: 0,
        }]
    );
    // The row left by moving, not by dying: it keeps its node, its items, and
    // its item key is **not** carried over — a node that left deliberately is
    // not one an insert for the same key may claim back.
    assert_eq!(
        tree.root::<Value>()
            .field::<Value>("slot")
            .expect("slot")
            .node(),
        node
    );
    assert_eq!(
        store.fields().value(),
        json!({"__musubi_store_id__": ["a"], "inner": [{"id": "a1"}]})
    );
}

#[test]
fn a_stream_row_that_stops_being_the_store_it_was_does_not_keep_its_node() {
    // The upsert keeps the **node** for a key the list already holds (§3.1) —
    // but a store id is that node's own identity (§3.2), so a row rendered as a
    // different child store is a different node, and the handle that read the
    // old id when it was made reads as gone rather than addressing the new one.
    let tree = streaming_tree();
    let messages = messages_state(&tree);

    commit(
        &tree,
        &[],
        &[insert_op(
            "a",
            -1,
            json!({"__musubi_store_id__": ["row", "a"], "n": 1}),
            None,
        )],
    );

    let row = StoreState::from(messages.by_key("a").expect("a"));
    let node = row.node();

    assert_eq!(tree.store_node(&store_id(&["row", "a"])), Some(node));

    commit(
        &tree,
        &[],
        &[insert_op(
            "a",
            -1,
            json!({"__musubi_store_id__": ["row", "b"], "n": 1}),
            None,
        )],
    );

    let fresh = StoreState::from(messages.by_key("a").expect("a"));

    assert_is_a_tree(&tree);
    assert_ne!(fresh.node(), node);
    assert!(!row.fields().is_live());
    assert_eq!(tree.store_node(&store_id(&["row", "a"])), None);
    assert_eq!(
        tree.store_node(&store_id(&["row", "b"])),
        Some(fresh.node())
    );
    assert_eq!(keys(&messages), ["a"]);
}

#[test]
fn a_delete_drops_the_row_and_a_stream_op_for_an_unknown_slot_is_ignored() {
    let tree = streaming_tree();
    let messages = messages_state(&tree);

    commit(
        &tree,
        &[],
        &[
            insert_op("a", -1, json!({"id": "a"}), None),
            insert_op("b", -1, json!({"id": "b"}), None),
        ],
    );

    let notify = tree.apply(&[], &[delete_op("a")]).expect("applies");
    let edits = notify.changes().collection_edits(messages.node()).to_vec();

    drop(notify);

    assert_eq!(keys(&messages), ["b"]);
    assert_eq!(
        edits,
        [CollectionEdit::Removed {
            item_key: Arc::from("a"),
            index: 0
        }]
    );

    // A slot this render does not have: dropped, and nothing else in the
    // envelope suffers for it.
    let before = tree.len();

    commit(
        &tree,
        &[],
        &[StreamOp::Insert {
            stream: "absent".to_owned(),
            store_id: StoreId::root(),
            item_key: "x".to_owned(),
            at: -1,
            item: json!({"id": "x"}),
            limit: None,
        }],
    );

    assert_eq!(tree.len(), before);
}

#[test]
fn a_rewrite_into_the_same_list_changes_nothing_and_edits_nothing() {
    let tree = streaming_tree();
    let messages = messages_state(&tree);

    commit(
        &tree,
        &[],
        &[
            insert_op("a", -1, json!({"id": "a"}), None),
            insert_op("b", -1, json!({"id": "b"}), None),
        ],
    );

    let notify = tree
        .apply(
            &[],
            &[
                reset_op(),
                insert_op("a", -1, json!({"id": "a"}), None),
                insert_op("b", -1, json!({"id": "b"}), None),
            ],
        )
        .expect("applies");

    assert!(notify.changes().is_empty());
    assert!(
        notify
            .changes()
            .collection_edits(messages.node())
            .is_empty()
    );

    drop(notify);
}

#[test]
fn insert_positioning_and_trimming_match_the_typescript_client() {
    // The rules of §3.1, op for op: `-1` appends, `0` and any other negative
    // prepend, a positive `at` clamps to the post-removal length, and the trim
    // direction is chosen by `at`, never by the sign of `limit`.
    let cases: Vec<(Vec<StreamOp>, Vec<&str>)> = vec![
        (
            vec![
                insert_op("a", -1, json!({}), None),
                insert_op("b", -1, json!({}), None),
                insert_op("c", 0, json!({}), None),
            ],
            vec!["c", "a", "b"],
        ),
        (
            vec![
                insert_op("a", -1, json!({}), None),
                insert_op("b", -2, json!({}), None),
                insert_op("c", -7, json!({}), None),
            ],
            vec!["c", "b", "a"],
        ),
        (
            vec![
                insert_op("a", -1, json!({}), None),
                insert_op("b", -1, json!({}), None),
                insert_op("c", 99, json!({}), None),
            ],
            vec!["a", "b", "c"],
        ),
        (
            vec![
                insert_op("a", -1, json!({}), None),
                insert_op("b", -1, json!({}), None),
                insert_op("c", -1, json!({}), None),
                // Post-removal length is 2, so `at: 2` lands last.
                insert_op("a", 2, json!({}), None),
            ],
            vec!["b", "c", "a"],
        ),
        (
            vec![
                insert_op("a", -1, json!({}), None),
                insert_op("b", -1, json!({}), Some(0)),
            ],
            vec![],
        ),
        (
            vec![
                insert_op("a", -1, json!({}), Some(-2)),
                insert_op("b", -1, json!({}), Some(-2)),
                insert_op("c", -1, json!({}), Some(-2)),
            ],
            vec!["b", "c"],
        ),
        (
            vec![
                insert_op("a", 0, json!({}), Some(-2)),
                insert_op("b", 0, json!({}), Some(-2)),
                insert_op("c", 0, json!({}), Some(-2)),
            ],
            vec!["c", "b"],
        ),
        (
            vec![
                insert_op("a", -1, json!({}), Some(2)),
                insert_op("b", -1, json!({}), Some(2)),
                insert_op("c", 1, json!({}), Some(2)),
            ],
            vec!["c", "b"],
        ),
    ];

    for (ops, expected) in cases {
        let tree = streaming_tree();
        let messages = messages_state(&tree);

        commit(&tree, &[], &ops);

        assert_eq!(keys(&messages), expected, "for {ops:?}");
    }
}

// -------------------------------------------------------------- child stores

#[test]
fn a_child_store_that_moved_keeps_its_node_its_subtree_and_its_subscribers() {
    let tree = seeded(json!({
        "__musubi_store_id__": [],
        "panel": {"__musubi_store_id__": ["panel"], "total": 1}
    }));
    let panel = StoreState::from(tree.root::<Value>().field::<Value>("panel").expect("panel"));
    let total = panel.fields().field::<i64>("total").expect("total");
    let (node, total_node) = (panel.node(), total.node());
    let log = Log::default();
    let _watch = log.watch("total", &total);

    assert_eq!(panel.store_id(), Some(store_id(&["panel"])));

    // The same store, rendered somewhere else entirely.
    commit(
        &tree,
        &[replace(
            "",
            json!({
                "__musubi_store_id__": [],
                "rows": [{"__musubi_store_id__": ["panel"], "total": 1}]
            }),
        )],
        &[],
    );

    let moved = tree
        .root::<Value>()
        .field::<Vec<Value>>("rows")
        .and_then(|rows| rows.at(0))
        .expect("rows[0]");

    assert_eq!(moved.node(), node);
    assert_eq!(total.node(), total_node);
    assert_eq!(total.value(), 1);
    assert!(log.taken().is_empty());
    assert_eq!(tree.store_node(&store_id(&["panel"])), Some(node));
}

#[test]
fn an_unmounted_store_takes_its_subtree_and_its_collections_with_it() {
    let tree = seeded(json!({
        "__musubi_store_id__": [],
        "panel": {
            "__musubi_store_id__": ["panel"],
            "rows": {"__musubi_stream__": "rows"}
        }
    }));

    commit(
        &tree,
        &[],
        &[StreamOp::Insert {
            stream: "rows".to_owned(),
            store_id: store_id(&["panel"]),
            item_key: "r1".to_owned(),
            at: -1,
            item: json!({"id": "r1"}),
            limit: None,
        }],
    );

    let panel = tree.root::<Value>().field::<Value>("panel").expect("panel");
    let rows = StreamState::from(panel.field::<Vec<Value>>("rows").expect("rows"));

    assert_eq!(rows.len(), 1);
    assert_eq!(tree.store_ids().len(), 2);

    commit(&tree, &[remove("/panel")], &[]);

    assert!(!panel.is_live());
    assert_eq!(tree.store_ids(), [StoreId::root()]);

    // A store that comes back starts empty — BDR-0011's fresh-mount semantics,
    // reached structurally rather than by a pruning walk.
    commit(
        &tree,
        &[add(
            "/panel",
            json!({"__musubi_store_id__": ["panel"], "rows": {"__musubi_stream__": "rows"}}),
        )],
        &[],
    );

    let fresh = tree.root::<Value>().field::<Value>("panel").expect("panel");
    let fresh_rows = StreamState::from(fresh.field::<Vec<Value>>("rows").expect("rows"));

    assert!(fresh_rows.is_empty());
    assert_ne!(fresh.node(), panel.node());
}

#[test]
fn an_add_that_shifts_a_store_one_slot_right_moves_its_node_rather_than_rebuilding_it() {
    let tree = seeded(json!({
        "__musubi_store_id__": [],
        "rows": [{"__musubi_store_id__": ["panel"], "total": 1}]
    }));
    let rows = tree
        .root::<Value>()
        .field::<Vec<Value>>("rows")
        .expect("rows");
    let panel = StoreState::from(rows.at(0).expect("rows[0]"));
    let total = panel.fields().field::<i64>("total").expect("total");
    let (node, total_node) = (panel.node(), total.node());

    // The shift rewrites index 0 with a wholly different value. The store that
    // was there belongs at index 1 now, with everything under it (§3.2).
    commit(&tree, &[add("/rows/0", json!({"kind": "banner"}))], &[]);

    assert_eq!(rows.len(), 2);
    assert_eq!(rows.at(1).expect("rows[1]").node(), node);
    assert_eq!(total.node(), total_node);
    assert!(total.is_live());
    assert_eq!(total.value(), 1);
    assert_eq!(tree.store_node(&store_id(&["panel"])), Some(node));
    assert_eq!(
        rows.at(0).expect("rows[0]").value(),
        json!({"kind": "banner"})
    );
}

#[test]
fn a_store_that_swaps_places_with_its_sibling_keeps_both_nodes() {
    let tree = seeded(json!({
        "__musubi_store_id__": [],
        "rows": [
            {"__musubi_store_id__": ["a"], "n": 1},
            {"__musubi_store_id__": ["b"], "n": 2}
        ]
    }));
    let rows = tree
        .root::<Value>()
        .field::<Vec<Value>>("rows")
        .expect("rows");
    let (first, second) = (
        rows.at(0).expect("rows[0]").node(),
        rows.at(1).expect("rows[1]").node(),
    );

    commit(
        &tree,
        &[replace(
            "/rows",
            json!([
                {"__musubi_store_id__": ["b"], "n": 2},
                {"__musubi_store_id__": ["a"], "n": 1}
            ]),
        )],
        &[],
    );

    // Store identity beats position: each node followed its own store id, and
    // neither was reconciled into the other's value on the way.
    assert_eq!(rows.at(0).expect("rows[0]").node(), second);
    assert_eq!(rows.at(1).expect("rows[1]").node(), first);
    assert_eq!(
        rows.value(),
        [
            json!({"__musubi_store_id__": ["b"], "n": 2}),
            json!({"__musubi_store_id__": ["a"], "n": 1})
        ]
    );
}

#[test]
fn one_store_id_rendered_under_two_keys_becomes_two_nodes() {
    // A server bug (`spec/domains/runtime/features/render-contract.feature`
    // raises on it), and the tree's answer is structural: the second sighting
    // is a new node, never a second parent for the first (§3.2).
    let tree = seeded(json!({
        "__musubi_store_id__": [],
        "x": {"__musubi_store_id__": ["dup"], "label": "A"},
        "y": {"__musubi_store_id__": ["dup"], "label": "B"}
    }));
    let root = tree.root::<Value>();
    let (x, y) = (
        root.field::<Value>("x").expect("x"),
        root.field::<Value>("y").expect("y"),
    );

    assert_ne!(x.node(), y.node());
    assert_eq!(
        x.value(),
        json!({"__musubi_store_id__": ["dup"], "label": "A"})
    );
    assert_eq!(
        y.value(),
        json!({"__musubi_store_id__": ["dup"], "label": "B"})
    );

    // Removing one of them frees only its own node.
    commit(&tree, &[remove("/x")], &[]);

    assert!(!x.is_live());
    assert!(y.is_live());
}

#[test]
fn a_node_reparented_after_it_was_dirtied_settles_before_its_new_ancestors() {
    // The store is dirtied under `/p`, then adopted three levels deeper by a
    // whole-root replace in the same transaction. Its new ancestors have to
    // settle *after* it, or they cache a value built from the one it left.
    let tree = seeded(json!({
        "__musubi_store_id__": [],
        "p": {"__musubi_store_id__": ["p"], "total": 1},
        "a": {"b": {"c": {"old": true}}}
    }));

    commit(
        &tree,
        &[
            replace("/p/total", json!(5)),
            replace(
                "",
                json!({
                    "__musubi_store_id__": [],
                    "a": {"b": {"c": {"__musubi_store_id__": ["p"], "total": 5}}}
                }),
            ),
        ],
        &[],
    );

    assert_eq!(
        tree.root::<Value>().value(),
        json!({
            "__musubi_store_id__": [],
            "a": {"b": {"c": {"__musubi_store_id__": ["p"], "total": 5}}}
        })
    );
}

// ------------------------------------------------- adoption keeps a tree a tree

#[test]
fn a_store_rendered_under_its_own_descendant_gets_a_node_of_its_own() {
    // Adoption reparents; adopting a node under something it already contains
    // would close a parent cycle, and every walk up the tree — `mark_dirty`'s
    // included, which runs holding the arena lock — would then never reach a
    // root. §3.2's duplicate rule is the answer: a new node, not a new parent.
    let tree = seeded(json!({
        "__musubi_store_id__": [],
        "a": {"__musubi_store_id__": ["X"], "inner": {"k": 1}}
    }));
    let outer = tree.root::<Value>().field::<Value>("a").expect("a");

    apply_within(
        &tree,
        &[add(
            "/a/inner/self",
            json!({"__musubi_store_id__": ["X"], "inner": {"k": 2}}),
        )],
    )
    .expect("the op applies rather than spinning");

    assert_is_a_tree(&tree);

    let nested = outer
        .field::<Value>("inner")
        .and_then(|inner| inner.field::<Value>("self"))
        .expect("/a/inner/self");

    assert_ne!(nested.node(), outer.node());
    assert!(outer.is_live());
    assert_eq!(
        nested.value(),
        json!({"__musubi_store_id__": ["X"], "inner": {"k": 2}})
    );
}

#[test]
fn a_store_rendered_as_a_child_of_itself_gets_a_node_of_its_own() {
    // The one-level form of the same cycle: the parent the value lands under
    // *is* the node its id names.
    let tree = seeded(json!({
        "__musubi_store_id__": [],
        "a": {"__musubi_store_id__": ["p"], "n": 1}
    }));
    let panel = tree.root::<Value>().field::<Value>("a").expect("a");

    apply_within(
        &tree,
        &[
            replace("/a/n", json!(2)),
            add("/a/child", json!({"__musubi_store_id__": ["p"], "n": 3})),
        ],
    )
    .expect("the ops apply rather than spinning");

    assert_is_a_tree(&tree);

    let child = panel.field::<Value>("child").expect("/a/child");

    assert_ne!(child.node(), panel.node());
    assert_eq!(
        panel.value(),
        json!({
            "__musubi_store_id__": ["p"],
            "n": 2,
            "child": {"__musubi_store_id__": ["p"], "n": 3}
        })
    );
}

#[test]
fn a_bare_marker_rendered_under_its_own_subtree_leaves_that_subtree_standing() {
    // The variant that did not hang: adoption reparented the ancestor under its
    // own descendant and then reconciled it into an empty store, taking the
    // whole subtree — and the root's key with it — while reporting success.
    let tree = seeded(json!({
        "__musubi_store_id__": [],
        "a": {"__musubi_store_id__": ["X"], "inner": {"k": 1}}
    }));

    apply_within(
        &tree,
        &[add("/a/inner/self", json!({"__musubi_store_id__": ["X"]}))],
    )
    .expect("the op applies");

    assert_is_a_tree(&tree);
    assert_eq!(
        tree.root::<Value>().value(),
        json!({
            "__musubi_store_id__": [],
            "a": {
                "__musubi_store_id__": ["X"],
                "inner": {"k": 1, "self": {"__musubi_store_id__": ["X"]}}
            }
        })
    );
}

#[test]
fn one_stream_rendered_under_two_keys_of_one_store_becomes_two_collections() {
    // A collection is adopted by `(owner, name)` the way a store is adopted by
    // its id, so the duplicate rule has to hold for it too: one render naming
    // the same stream twice is a server bug, and the second sighting gets a node
    // of its own rather than becoming a second parent for the first.
    let tree = seeded(json!({
        "__musubi_store_id__": [],
        "panel": {
            "__musubi_store_id__": ["panel"],
            "messages": {"__musubi_stream__": "messages"}
        }
    }));

    commit(
        &tree,
        &[],
        &[row_insert_op(&["panel"], "m1", json!({"id": "m1"}))],
    );

    let panel = tree.root::<Value>().field::<Value>("panel").expect("panel");
    let messages = panel
        .field::<Vec<Value>>("messages")
        .expect("messages")
        .node();

    commit(
        &tree,
        &[add(
            "/panel/mirror",
            json!({"__musubi_stream__": "messages"}),
        )],
        &[],
    );

    assert_is_a_tree(&tree);
    assert_ne!(
        panel.field::<Vec<Value>>("mirror").expect("mirror").node(),
        messages
    );
    assert_eq!(
        panel.value(),
        json!({
            "__musubi_store_id__": ["panel"],
            "messages": [{"id": "m1"}],
            "mirror": []
        })
    );
}

// --------------------------------------------------------------- the depth cap

/// `levels` levels of nesting: `nest(1)` is a scalar, `nest(2)` is `{"n": 1}`.
fn nest(levels: usize) -> Value {
    nest_around(levels, json!(1))
}

/// `levels` levels of nesting around `leaf`, the leaf itself counted.
fn nest_around(levels: usize, leaf: Value) -> Value {
    let mut value = leaf;

    for _ in 1..levels {
        value = json!({ "n": value });
    }

    value
}

/// The pointer that walks `levels` levels down a [`nest`] chain rooted at
/// `field`: `descend("/deep", 2)` is `/deep/n/n`, the node `2` levels below the
/// one `/deep` addresses.
fn descend(field: &str, levels: usize) -> String {
    let mut path = field.to_owned();

    for _ in 0..levels {
        path.push_str("/n");
    }

    path
}

#[test]
fn a_value_that_would_nest_past_the_depth_cap_is_refused_and_the_tree_stays_usable() {
    // The cap is on the **tree**, not on one document: `serde_json`'s own
    // nesting limit bounds a single `value`, and an `add` at a successively
    // deeper path composes depth across ops and across envelopes. Past the stack
    // the recursive walks run on, that is a `SIGABRT` — not a panic, so nothing
    // catches it and the journal never rolls back.
    let tree = seeded(json!({"count": 1}));
    let mut path = String::new();

    // Exactly at the cap: the deepest node sits `MAX_DEPTH` levels below the
    // root, one op at a time.
    for _ in 0..MAX_DEPTH {
        path.push_str("/n");

        commit(&tree, &[add(&path, json!({}))], &[]);
    }

    let before = tree.to_wire(tree.root_id());
    let nodes = tree.len();

    path.push_str("/n");

    let error = tree
        .apply(&[add(&path, json!({}))], &[])
        .expect_err("one level past the cap is refused");

    assert!(matches!(error, TreeError::Depth { .. }));
    // Refusing rolls the transaction back like any other failure, and the tree
    // is exactly what it was.
    assert_eq!(tree.to_wire(tree.root_id()), before);
    assert_eq!(tree.len(), nodes);

    commit(&tree, &[replace("/count", json!(2))], &[]);

    assert_eq!(
        tree.root::<Value>()
            .field::<i64>("count")
            .expect("count")
            .value(),
        2
    );
}

#[test]
fn one_value_is_measured_against_the_same_cap_as_a_run_of_ops() {
    let tree = StateTree::new();

    // `nest(MAX_DEPTH + 1)` puts its deepest node exactly at the cap: the
    // outermost level is the root, at depth 0.
    commit(&tree, &[replace("", nest(MAX_DEPTH + 1))], &[]);

    assert!(
        tree.apply(&[replace("", nest(MAX_DEPTH + 2))], &[])
            .is_err()
    );
    assert!(
        tree.apply(&[], &[insert_op("a", -1, nest(MAX_DEPTH + 2), None)],)
            .is_ok(),
        "a stream op for a slot this tree does not have is still dropped, not refused"
    );
}

#[test]
fn a_stream_item_is_measured_against_the_depth_cap_too() {
    let tree = streaming_tree();

    // The collection sits two levels down, so its items start at three.
    let error = tree
        .apply(&[], &[insert_op("a", -1, nest(MAX_DEPTH), None)])
        .expect_err("an item deeper than the cap is refused");

    assert!(matches!(error, TreeError::Depth { .. }));
    assert!(messages_state(&tree).is_empty());

    commit(&tree, &[], &[insert_op("a", -1, json!({"id": "a"}), None)]);

    assert_eq!(keys(&messages_state(&tree)), ["a"]);
}

/// A tree holding one tall child store and two chains to land it in: `near`
/// bottoms out inside the cap, `far` does not.
///
/// The store's own node sits at depth 1 and its deepest descendant at 201, so
/// the subtree is 200 levels tall — legal where it stands, and legal to move
/// anywhere at depth 56 or above.
fn tall_store_tree() -> StateTree {
    seeded(json!({
        "__musubi_store_id__": [],
        "count": 1,
        "tall": {"__musubi_store_id__": ["tall"], "n": nest(200)},
        "near": nest(50),
        "far": nest(120)
    }))
}

#[test]
fn an_adoption_that_would_compose_past_the_depth_cap_is_refused() {
    // `build` measures what it creates, but a **matching** subtree never
    // reaches `build`: adoption re-parents it whole and reconciliation walks
    // down it through the unchanged fast path. Destination depth plus subtree
    // height therefore composed straight past the cap, and every recursive read
    // and every `Drop` below it ran on a chain the cap was supposed to bound.
    let tree = tall_store_tree();
    let render = json!({"__musubi_store_id__": ["tall"], "n": nest(200)});
    let node = tree.store_node(&store_id(&["tall"])).expect("tall");
    let before = tree.to_wire(tree.root_id());
    let nodes = tree.len();

    // Landing it 120 levels down would put its deepest node at 320.
    let error = tree
        .apply(&[replace(&descend("/far", 119), render.clone())], &[])
        .expect_err("an adoption past the cap is refused");

    assert!(matches!(error, TreeError::Depth { .. }));
    assert_eq!(tree.to_wire(tree.root_id()), before);
    assert_eq!(tree.len(), nodes);
    assert_eq!(tree.store_node(&store_id(&["tall"])), Some(node));

    commit(&tree, &[replace("/count", json!(2))], &[]);

    assert_eq!(
        tree.root::<Value>()
            .field::<i64>("count")
            .expect("count")
            .value(),
        2
    );
}

#[test]
fn an_adoption_that_carries_an_already_dirty_node_past_the_cap_is_refused_before_commit() {
    // The same composition, with a node inside the moving subtree dirtied by an
    // earlier op of the same envelope. That node's parent chain is walked again
    // at commit — after `commit` has taken the arena guard and the journal, so
    // the rollback is already disarmed and there is nothing left to unwind
    // into. The refusal has to happen while the transaction can still be
    // rolled back, which is to say at the write, not at the settle.
    let tree = tall_store_tree();
    let before = tree.to_wire(tree.root_id());
    let nodes = tree.len();

    let error = tree
        .apply(
            &[
                replace(&descend("/tall", 200), json!(2)),
                replace(
                    &descend("/far", 119),
                    json!({"__musubi_store_id__": ["tall"], "n": nest_around(200, json!(2))}),
                ),
            ],
            &[],
        )
        .expect_err("an adoption past the cap is refused");

    assert!(matches!(error, TreeError::Depth { .. }));
    assert_eq!(tree.to_wire(tree.root_id()), before);
    assert_eq!(tree.len(), nodes);
}

#[test]
fn re_adopting_one_subtree_across_envelopes_is_measured_against_the_cap_every_time() {
    // The move that fits is taken, and taking it does not buy the next one any
    // slack: every adoption is measured against the tree as it stands, so a
    // subtree cannot be walked past the cap one legal envelope at a time.
    let tree = tall_store_tree();
    let render = json!({"__musubi_store_id__": ["tall"], "n": nest(200)});
    let node = tree.store_node(&store_id(&["tall"])).expect("tall");

    // 50 levels down: the deepest node lands at 250, inside the cap.
    commit(
        &tree,
        &[replace(&descend("/near", 49), render.clone())],
        &[],
    );

    assert_is_a_tree(&tree);
    assert_eq!(tree.store_node(&store_id(&["tall"])), Some(node));
    // The key it left keeps the addressable `Null` `detach` puts there (§3.2).
    assert_eq!(
        tree.root::<Value>()
            .field::<Value>("tall")
            .expect("tall")
            .value(),
        Value::Null
    );

    let before = tree.to_wire(tree.root_id());

    let error = tree
        .apply(&[replace(&descend("/far", 119), render)], &[])
        .expect_err("the second move is measured against the tree it now has");

    assert!(matches!(error, TreeError::Depth { .. }));
    assert_eq!(tree.to_wire(tree.root_id()), before);
    assert_eq!(tree.store_node(&store_id(&["tall"])), Some(node));
}

// ------------------------------------------------- landing before vacating

#[test]
fn a_store_that_lands_before_its_old_key_is_vacated_keeps_its_node() {
    // The literal `Musubi.Diff` output for a child store that moved between two
    // keys that both exist: the op that **lands** it comes first, and the one
    // that vacates its old slot comes second. Detaching the node used to delete
    // the source key outright, so the second op could not resolve and a
    // legitimate server frame was rejected whole.
    let tree = seeded(json!({
        "__musubi_store_id__": [],
        "a": {"__musubi_store_id__": ["p"], "n": 1},
        "b": {}
    }));
    let panel = StoreState::from(tree.root::<Value>().field::<Value>("a").expect("a"));
    let node = panel.node();
    let hits = Arc::new(AtomicUsize::new(0));
    let seen = hits.clone();
    let _watch = panel.subscribe(move |_| {
        seen.fetch_add(1, Ordering::SeqCst);
    });

    commit(
        &tree,
        &[
            add("/b/w", json!({"__musubi_store_id__": ["p"], "n": 1})),
            replace("/a", json!(null)),
        ],
        &[],
    );

    assert_is_a_tree(&tree);
    assert_eq!(tree.store_node(&store_id(&["p"])), Some(node));
    assert_eq!(
        tree.root::<Value>().value(),
        json!({
            "__musubi_store_id__": [],
            "a": null,
            "b": {"w": {"__musubi_store_id__": ["p"], "n": 1}}
        })
    );
    assert!(panel.fields().is_live());
    assert_eq!(hits.load(Ordering::SeqCst), 0);

    // The subscription registered before the move is still the moved node's.
    commit(&tree, &[replace("/b/w/n", json!(2))], &[]);

    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[test]
fn a_vacated_key_the_render_drops_altogether_is_removable() {
    // The other half of the same shape: when the new render has no key at all
    // where the store used to be, `Musubi.Diff` emits `remove`, and that has to
    // resolve against the placeholder the move left behind.
    let tree = seeded(json!({
        "__musubi_store_id__": [],
        "a": {"__musubi_store_id__": ["p"], "n": 1},
        "b": {}
    }));
    let node = tree.root::<Value>().field::<Value>("a").expect("a").node();

    commit(
        &tree,
        &[
            add("/b/w", json!({"__musubi_store_id__": ["p"], "n": 1})),
            remove("/a"),
        ],
        &[],
    );

    assert_is_a_tree(&tree);
    assert_eq!(tree.store_node(&store_id(&["p"])), Some(node));
    assert_eq!(
        tree.root::<Value>().value(),
        json!({
            "__musubi_store_id__": [],
            "b": {"w": {"__musubi_store_id__": ["p"], "n": 1}}
        })
    );
}

// ----------------------------------------------- replace is a child-level write

#[test]
fn a_replace_that_lands_a_store_living_elsewhere_moves_its_node() {
    // §3.2's headline promise, on the op shape the server actually emits for a
    // child store that moved between two slots: `replace` used to reconcile the
    // node the pointer resolved to *directly*, so it never consulted the store
    // index and built a second node for a store that already had one.
    let tree = seeded(json!({
        "__musubi_store_id__": [],
        "panel": {"__musubi_store_id__": ["x"], "n": 1},
        "slot": null
    }));
    let panel = StoreState::from(tree.root::<Value>().field::<Value>("panel").expect("panel"));
    let total = panel.fields().field::<i64>("n").expect("n");
    let (node, total_node, nodes) = (panel.node(), total.node(), tree.len());
    let log = Log::default();
    let _watch = log.watch("n", &total);

    commit(
        &tree,
        &[
            replace("/slot", json!({"__musubi_store_id__": ["x"], "n": 1})),
            replace("/panel", json!(null)),
        ],
        &[],
    );

    let moved = tree.root::<Value>().field::<Value>("slot").expect("slot");

    assert_is_a_tree(&tree);
    assert_eq!(moved.node(), node);
    assert_eq!(total.node(), total_node);
    assert!(log.taken().is_empty());
    assert_eq!(tree.store_node(&store_id(&["x"])), Some(node));
    assert_eq!(tree.store_ids().len(), 2);
    assert_eq!(tree.len(), nodes);
    assert_eq!(
        tree.root::<Value>().value(),
        json!({
            "__musubi_store_id__": [],
            "panel": null,
            "slot": {"__musubi_store_id__": ["x"], "n": 1}
        })
    );
}

#[test]
fn a_store_removed_and_landed_again_in_one_envelope_keeps_its_node() {
    let tree = seeded(json!({
        "__musubi_store_id__": [],
        "left": {"__musubi_store_id__": ["x"], "n": 1},
        "right": null
    }));
    let panel = StoreState::from(tree.root::<Value>().field::<Value>("left").expect("left"));
    let node = panel.node();
    let hits = Arc::new(AtomicUsize::new(0));
    let seen = hits.clone();
    let _watch = panel.subscribe(move |_| {
        seen.fetch_add(1, Ordering::SeqCst);
    });

    commit(
        &tree,
        &[
            remove("/left"),
            replace("/right", json!({"__musubi_store_id__": ["x"], "n": 1})),
        ],
        &[],
    );

    assert_is_a_tree(&tree);
    assert_eq!(
        tree.root::<Value>()
            .field::<Value>("right")
            .expect("right")
            .node(),
        node
    );
    assert_eq!(tree.store_node(&store_id(&["x"])), Some(node));
    // The node moved; it was never removed, so nothing was told.
    assert_eq!(hits.load(Ordering::SeqCst), 0);
}

#[test]
fn a_slot_given_a_different_store_id_does_not_reuse_the_old_store_node() {
    // A store node's identity *is* its id: reusing the node would leave a live
    // `StoreState` — which reads its id once, at handle creation (§3.2) —
    // pointed at a node that has since become some other store, so
    // `command_on` would dispatch at the wrong target and read the wrong
    // fields.
    let tree = seeded(json!({
        "__musubi_store_id__": [],
        "panel": {"__musubi_store_id__": ["a"], "n": 1}
    }));
    let panel = StoreState::from(tree.root::<Value>().field::<Value>("panel").expect("panel"));

    assert_eq!(panel.store_id(), Some(store_id(&["a"])));

    commit(
        &tree,
        &[replace(
            "/panel",
            json!({"__musubi_store_id__": ["b"], "n": 2}),
        )],
        &[],
    );

    let fresh = StoreState::from(tree.root::<Value>().field::<Value>("panel").expect("panel"));

    assert!(!panel.fields().is_live());
    assert_ne!(fresh.node(), panel.node());
    assert_eq!(fresh.store_id(), Some(store_id(&["b"])));
    assert_eq!(tree.store_node(&store_id(&["a"])), None);
    assert_eq!(tree.store_node(&store_id(&["b"])), Some(fresh.node()));

    // The same id is the same store, and keeps everything.
    commit(
        &tree,
        &[replace(
            "/panel",
            json!({"__musubi_store_id__": ["b"], "n": 3}),
        )],
        &[],
    );

    assert_eq!(
        tree.root::<Value>()
            .field::<Value>("panel")
            .expect("panel")
            .node(),
        fresh.node()
    );
    assert!(fresh.fields().is_live());
}

#[test]
fn a_stream_item_may_take_a_store_an_earlier_patch_op_placed() {
    // Two things at once. The claimed set is scoped to **one op**, patch or
    // stream: a stream op that inherited the previous patch op's claims would
    // refuse to adopt and build a second node for a store that already has one.
    // And the key the adoption vacates stays in the render rather than being
    // deleted out of its parent behind the server's back.
    let tree = seeded(json!({
        "__musubi_store_id__": [],
        "slot": null,
        "messages": {"__musubi_stream__": "messages"}
    }));

    commit(
        &tree,
        &[replace(
            "/slot",
            json!({"__musubi_store_id__": ["x"], "n": 1}),
        )],
        &[insert_op(
            "a",
            -1,
            json!({"id": "a", "panel": {"__musubi_store_id__": ["x"], "n": 1}}),
            None,
        )],
    );

    let nodes = assert_is_a_tree(&tree);
    let messages = StreamState::from(
        tree.root::<Value>()
            .field::<Vec<Value>>("messages")
            .expect("messages"),
    );
    let row = messages.by_key("a").expect("a");
    let panel = row.field::<Value>("panel").expect("panel");

    assert_eq!(tree.store_node(&store_id(&["x"])), Some(panel.node()));
    assert_eq!(tree.store_ids().len(), 2);
    assert_eq!(nodes.len(), tree.len());
    assert_eq!(
        tree.root::<Value>().value(),
        json!({
            "__musubi_store_id__": [],
            "slot": null,
            "messages": [{"id": "a", "panel": {"__musubi_store_id__": ["x"], "n": 1}}]
        })
    );
}

// ------------------------------------------------- positional rewrites

#[test]
fn an_add_that_renders_a_store_a_second_time_gives_the_newcomer_its_own_node() {
    // `[{store a}]` + `add /rows/1 {store a}` is what `Musubi.Diff` emits when
    // a plain row is prepended before a store row. The prefix keeps the node it
    // has, so the positional rewrite must not also hand it to the new position:
    // one `NodeId` in two slots is a tree that is not a tree.
    let tree = seeded(json!({
        "__musubi_store_id__": [],
        "rows": [{"__musubi_store_id__": ["a"], "n": 1}]
    }));
    let rows = tree
        .root::<Value>()
        .field::<Vec<Value>>("rows")
        .expect("rows");
    let first = rows.at(0).expect("rows[0]").node();

    commit(
        &tree,
        &[add(
            "/rows/1",
            json!({"__musubi_store_id__": ["a"], "n": 1}),
        )],
        &[],
    );

    assert_is_a_tree(&tree);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows.at(0).expect("rows[0]").node(), first);
    assert_ne!(rows.at(1).expect("rows[1]").node(), first);
    assert_eq!(
        rows.value(),
        [
            json!({"__musubi_store_id__": ["a"], "n": 1}),
            json!({"__musubi_store_id__": ["a"], "n": 1})
        ]
    );
}

#[test]
fn an_add_before_a_store_row_shifts_it_right_and_keeps_its_node() {
    // The right-to-left rewrite (§3.2): every position takes its predecessor's
    // value, so the store has to be claimed by the position it moves *into*
    // before the position it moves out of is rewritten.
    let tree = seeded(json!({
        "__musubi_store_id__": [],
        "rows": [
            {"kind": "banner"},
            {"__musubi_store_id__": ["a"], "n": 1},
            {"__musubi_store_id__": ["b"], "n": 2}
        ]
    }));
    let rows = tree
        .root::<Value>()
        .field::<Vec<Value>>("rows")
        .expect("rows");
    let panel = StoreState::from(rows.at(1).expect("rows[1]"));
    let (first, second) = (panel.node(), rows.at(2).expect("rows[2]").node());

    commit(&tree, &[add("/rows/0", json!({"kind": "header"}))], &[]);

    assert_is_a_tree(&tree);
    assert_eq!(rows.at(2).expect("rows[2]").node(), first);
    assert_eq!(rows.at(3).expect("rows[3]").node(), second);
    assert!(panel.fields().is_live());
    assert_eq!(
        rows.value(),
        [
            json!({"kind": "header"}),
            json!({"kind": "banner"}),
            json!({"__musubi_store_id__": ["a"], "n": 1}),
            json!({"__musubi_store_id__": ["b"], "n": 2})
        ]
    );
}

#[test]
fn a_remove_before_a_store_row_shifts_it_left_and_keeps_its_node() {
    let tree = seeded(json!({
        "__musubi_store_id__": [],
        "rows": [{"kind": "header"}, {"kind": "banner"}, {"__musubi_store_id__": ["a"], "n": 1}]
    }));
    let rows = tree
        .root::<Value>()
        .field::<Vec<Value>>("rows")
        .expect("rows");
    let panel = StoreState::from(rows.at(2).expect("rows[2]"));
    let node = panel.node();

    commit(&tree, &[remove("/rows/0")], &[]);

    assert_is_a_tree(&tree);
    assert_eq!(rows.at(1).expect("rows[1]").node(), node);
    assert!(panel.fields().is_live());
    assert_eq!(
        rows.value(),
        [
            json!({"kind": "banner"}),
            json!({"__musubi_store_id__": ["a"], "n": 1})
        ]
    );
}

#[test]
fn a_position_rendered_as_a_plain_value_does_not_destroy_the_store_a_later_one_adopts() {
    // A whole-list `replace` where a plain row is prepended: position 0's value
    // is not a store, and rewriting the store node standing there in place
    // would unregister it — so position 1 would find nothing to adopt and build
    // a copy, while a live handle read the banner out of what used to be its
    // store.
    let tree = seeded(json!({
        "__musubi_store_id__": [],
        "rows": [{"__musubi_store_id__": ["panel"], "n": 1}]
    }));
    let rows = tree
        .root::<Value>()
        .field::<Vec<Value>>("rows")
        .expect("rows");
    let panel = StoreState::from(rows.at(0).expect("rows[0]"));
    let node = panel.node();

    commit(
        &tree,
        &[replace(
            "/rows",
            json!([{"banner": true}, {"__musubi_store_id__": ["panel"], "n": 1}]),
        )],
        &[],
    );

    assert_is_a_tree(&tree);
    assert_eq!(rows.at(1).expect("rows[1]").node(), node);
    assert_ne!(rows.at(0).expect("rows[0]").node(), node);
    assert!(panel.fields().is_live());
    assert_eq!(tree.store_node(&store_id(&["panel"])), Some(node));
    assert_eq!(
        rows.value(),
        [
            json!({"banner": true}),
            json!({"__musubi_store_id__": ["panel"], "n": 1})
        ]
    );
}

#[test]
fn a_position_rewritten_as_a_plain_value_exchanges_with_the_copy_that_took_its_id() {
    // The same identity exchange as a marker release, reached without a marker
    // op: `add` lands a second render of store `a`, and the op that follows
    // rewrites position 0 as a plain value. Whatever spelling strips
    // store-ness off the node holding the id, the id stays on the node that
    // was carrying it (§3.2).
    let tree = seeded(json!({
        "__musubi_store_id__": [],
        "rows": [{"__musubi_store_id__": ["a"], "n": 1}]
    }));
    let rows = tree
        .root::<Value>()
        .field::<Vec<Value>>("rows")
        .expect("rows");
    let panel = StoreState::from(rows.at(0).expect("rows[0]"));
    let node = panel.node();

    commit(
        &tree,
        &[
            add("/rows/1", json!({"__musubi_store_id__": ["a"], "n": 1})),
            replace("/rows/0", json!({"kind": "banner"})),
        ],
        &[],
    );

    assert_is_a_tree(&tree);
    assert_eq!(rows.at(1).expect("rows[1]").node(), node);
    assert!(panel.fields().is_live());
    assert_eq!(tree.store_node(&store_id(&["a"])), Some(node));
    assert_eq!(
        rows.value(),
        [
            json!({"kind": "banner"}),
            json!({"__musubi_store_id__": ["a"], "n": 1})
        ]
    );
}

#[test]
fn a_store_and_a_plain_row_that_swap_places_keep_the_store_node() {
    let tree = seeded(json!({
        "__musubi_store_id__": [],
        "rows": [{"__musubi_store_id__": ["panel"], "n": 1}, {"banner": true}]
    }));
    let rows = tree
        .root::<Value>()
        .field::<Vec<Value>>("rows")
        .expect("rows");
    let node = rows.at(0).expect("rows[0]").node();

    commit(
        &tree,
        &[replace(
            "/rows",
            json!([{"banner": true}, {"__musubi_store_id__": ["panel"], "n": 1}]),
        )],
        &[],
    );

    assert_is_a_tree(&tree);
    assert_eq!(rows.at(1).expect("rows[1]").node(), node);
    assert_eq!(tree.store_node(&store_id(&["panel"])), Some(node));
}

// ------------------------------------------------- settling what was adopted

#[test]
fn a_parent_built_around_an_adopted_child_settles_after_it() {
    // The adopted node arrives with the value it had *before* this transaction
    // — its cache is deliberately stale until commit settles it — and the
    // parent being built around it computes its own value from that cache. If
    // the fresh parent never enters the dirty set, it keeps the stale value for
    // good: the kinds say `total: 5` and every read says `total: 1`.
    let tree = seeded(json!({
        "__musubi_store_id__": [],
        "p": {"__musubi_store_id__": ["p"], "total": 1}
    }));

    commit(
        &tree,
        &[
            replace("/p/total", json!(5)),
            add(
                "/q",
                json!({"w": {"__musubi_store_id__": ["p"], "total": 5}}),
            ),
            remove("/p"),
        ],
        &[],
    );

    assert_is_a_tree(&tree);
    assert_eq!(
        tree.root::<Value>().value(),
        json!({
            "__musubi_store_id__": [],
            "q": {"w": {"__musubi_store_id__": ["p"], "total": 5}}
        })
    );
}

#[test]
fn a_node_dirtied_after_it_was_adopted_settles_every_one_of_its_new_ancestors() {
    // The mark_dirty walk runs to the root every time rather than stopping at
    // the first node already in the dirty set: `inner` is dirtied *after* the
    // store was adopted, so its new ancestors — the store's own already-dirty
    // node among them — are the only thing standing between the settled value
    // and the root.
    let tree = seeded(json!({
        "__musubi_store_id__": [],
        "p": {"__musubi_store_id__": ["p"], "total": 1, "inner": {"n": 1}}
    }));

    commit(
        &tree,
        &[
            replace("/p/total", json!(5)),
            add(
                "/q",
                json!({
                    "w": {"__musubi_store_id__": ["p"], "total": 5, "inner": {"n": 2}}
                }),
            ),
            remove("/p"),
        ],
        &[],
    );

    assert_is_a_tree(&tree);
    assert_eq!(
        tree.root::<Value>().value(),
        json!({
            "__musubi_store_id__": [],
            "q": {"w": {"__musubi_store_id__": ["p"], "total": 5, "inner": {"n": 2}}}
        })
    );
}

// ------------------------------------------------------------- the store marker

#[test]
fn a_reordered_list_of_child_stores_arrives_as_marker_ops_and_keeps_both_nodes() {
    // `Jsonpatch.diff/2` descends into `__musubi_store_id__` like any other
    // key, so a reorder of two child stores arrives as ops *into the marker*.
    // The tree keeps the id on the node rather than in the field map, so these
    // have to be read as what they are — a change of identity — and routed
    // through adoption, or a legitimate server frame is rejected whole.
    let tree = seeded(json!({
        "__musubi_store_id__": [],
        "rows": [{"__musubi_store_id__": ["a"]}, {"__musubi_store_id__": ["b"]}]
    }));
    let rows = tree
        .root::<Value>()
        .field::<Vec<Value>>("rows")
        .expect("rows");
    let (first, second) = (
        rows.at(0).expect("rows[0]").node(),
        rows.at(1).expect("rows[1]").node(),
    );

    commit(
        &tree,
        &[
            replace("/rows/1/__musubi_store_id__/0", json!("a")),
            replace("/rows/0/__musubi_store_id__/0", json!("b")),
        ],
        &[],
    );

    assert_is_a_tree(&tree);
    assert_eq!(rows.at(0).expect("rows[0]").node(), second);
    assert_eq!(rows.at(1).expect("rows[1]").node(), first);
    assert_eq!(
        rows.value(),
        [
            json!({"__musubi_store_id__": ["b"]}),
            json!({"__musubi_store_id__": ["a"]})
        ]
    );
    assert_eq!(tree.store_node(&store_id(&["a"])), Some(first));
    assert_eq!(tree.store_node(&store_id(&["b"])), Some(second));
}

#[test]
fn a_reordered_list_of_child_stores_with_fields_keeps_both_nodes_and_both_values() {
    // The same reorder when the rows carry fields: four ops, the field writes
    // interleaved with the marker writes, exactly as `Jsonpatch.diff/2` emits
    // them.
    let tree = seeded(json!({
        "__musubi_store_id__": [],
        "rows": [
            {"__musubi_store_id__": ["a"], "label": "A"},
            {"__musubi_store_id__": ["b"], "label": "B"}
        ]
    }));
    let rows = tree
        .root::<Value>()
        .field::<Vec<Value>>("rows")
        .expect("rows");
    let (first, second) = (
        rows.at(0).expect("rows[0]").node(),
        rows.at(1).expect("rows[1]").node(),
    );

    commit(
        &tree,
        &[
            replace("/rows/1/label", json!("A")),
            replace("/rows/1/__musubi_store_id__/0", json!("a")),
            replace("/rows/0/label", json!("B")),
            replace("/rows/0/__musubi_store_id__/0", json!("b")),
        ],
        &[],
    );

    assert_is_a_tree(&tree);
    assert_eq!(rows.at(0).expect("rows[0]").node(), second);
    assert_eq!(rows.at(1).expect("rows[1]").node(), first);
    assert_eq!(
        rows.value(),
        [
            json!({"__musubi_store_id__": ["b"], "label": "B"}),
            json!({"__musubi_store_id__": ["a"], "label": "A"})
        ]
    );
    assert_eq!(tree.store_node(&store_id(&["a"])), Some(first));
    assert_eq!(tree.store_node(&store_id(&["b"])), Some(second));
}

#[test]
fn a_row_prepended_before_a_child_store_arrives_as_a_marker_removal() {
    // The literal `Jsonpatch.diff/2` output for `[{store a}]` becoming
    // `[{banner}, {store a}]`: the added row lands first, then the marker and
    // the fields of row 0 are rewritten in place. What comes out is the render:
    // a plain banner at 0, store `a` at 1, and one id in the index. *Which
    // node* carries the id is the next test over — a store node is never
    // rewritten into a non-store, so the marker removal exchanges the two
    // rather than rewriting either (§3.2).
    let tree = seeded(json!({
        "__musubi_store_id__": [],
        "rows": [{"__musubi_store_id__": ["a"], "n": 1}]
    }));
    let rows = tree
        .root::<Value>()
        .field::<Vec<Value>>("rows")
        .expect("rows");

    commit(
        &tree,
        &[
            add("/rows/1", json!({"__musubi_store_id__": ["a"], "n": 1})),
            remove("/rows/0/n"),
            remove("/rows/0/__musubi_store_id__"),
            add("/rows/0/kind", json!("banner")),
        ],
        &[],
    );

    assert_is_a_tree(&tree);
    assert_eq!(
        rows.value(),
        [
            json!({"kind": "banner"}),
            json!({"__musubi_store_id__": ["a"], "n": 1})
        ]
    );
    assert_eq!(
        tree.store_node(&store_id(&["a"])),
        Some(rows.at(1).expect("rows[1]").node())
    );
    assert_eq!(tree.store_ids().len(), 2);
}

#[test]
fn a_row_prepended_before_a_child_store_keeps_the_store_node_and_its_subscribers() {
    // The same four ops, read as what they say about **identity**. Store `a` is
    // rendered in both frames — it never unmounts, so §3.2's promise applies to
    // it: the node that carried the id keeps it, with its subtree and its
    // subscribers. The `add` lands a second copy of the id because the marker
    // that releases the first one has not arrived yet; when it does, the copy
    // and the original exchange slots, exactly as a reorder does (§3.2).
    let tree = seeded(json!({
        "__musubi_store_id__": [],
        "rows": [{"__musubi_store_id__": ["a"], "n": 1}]
    }));
    let rows = tree
        .root::<Value>()
        .field::<Vec<Value>>("rows")
        .expect("rows");
    let panel = StoreState::from(rows.at(0).expect("rows[0]"));
    let node = panel.node();
    let hits = Arc::new(AtomicUsize::new(0));
    let seen = hits.clone();
    let _watch = panel.subscribe(move |_| {
        seen.fetch_add(1, Ordering::SeqCst);
    });

    commit(
        &tree,
        &[
            add("/rows/1", json!({"__musubi_store_id__": ["a"], "n": 1})),
            remove("/rows/0/n"),
            remove("/rows/0/__musubi_store_id__"),
            add("/rows/0/kind", json!("banner")),
        ],
        &[],
    );

    assert_is_a_tree(&tree);
    assert_eq!(rows.at(1).expect("rows[1]").node(), node);
    assert!(panel.fields().is_live());
    assert_eq!(tree.store_node(&store_id(&["a"])), Some(node));
    // Store `a`'s own value came out of the envelope as it went in, so the
    // subscriber that survived is not woken at all: `n: 1` left and came back
    // inside one transaction (§9.2).
    assert_eq!(hits.load(Ordering::SeqCst), 0);
    assert_eq!(
        panel.fields().value(),
        json!({"__musubi_store_id__": ["a"], "n": 1})
    );
}

#[test]
fn a_refused_op_after_an_identity_exchange_puts_both_nodes_back() {
    // The exchange is two parent writes and a re-registration, so it has to
    // journal like every other mutation: a later op that cannot apply rolls the
    // whole envelope back, exchange included (§9.2).
    let tree = seeded(json!({
        "__musubi_store_id__": [],
        "rows": [{"__musubi_store_id__": ["a"], "n": 1}]
    }));
    let rows = tree
        .root::<Value>()
        .field::<Vec<Value>>("rows")
        .expect("rows");
    let node = rows.at(0).expect("rows[0]").node();
    let before = tree.to_wire(tree.root_id());
    let nodes = tree.len();

    let error = tree
        .apply(
            &[
                add("/rows/1", json!({"__musubi_store_id__": ["a"], "n": 1})),
                remove("/rows/0/n"),
                remove("/rows/0/__musubi_store_id__"),
                remove("/rows/9"),
            ],
            &[],
        )
        .expect_err("the last op cannot apply");

    assert!(matches!(error, TreeError::Index { .. }));
    assert_is_a_tree(&tree);
    assert_eq!(tree.to_wire(tree.root_id()), before);
    assert_eq!(tree.len(), nodes);
    assert_eq!(rows.at(0).expect("rows[0]").node(), node);
    assert_eq!(tree.store_node(&store_id(&["a"])), Some(node));
}

#[test]
fn a_marker_release_that_exchanges_two_keys_of_one_object_keeps_both_nodes() {
    // The exchange between two slots of the **same** parent, which is where a
    // write that thinks it displaced the node it wrote over gets it wrong: the
    // node is still that parent's, one key along. A store rendered twice under
    // one object gives the second sighting a node of its own (§3.2), and the
    // marker removal that follows is what says which of the two is the store.
    let tree = seeded(json!({
        "__musubi_store_id__": [],
        "obj": {"a": {"__musubi_store_id__": ["x"], "n": 1}, "b": null}
    }));
    let obj = tree.root::<Value>().field::<Value>("obj").expect("obj");
    let panel = StoreState::from(obj.field::<Value>("a").expect("a"));
    let node = panel.node();

    commit(
        &tree,
        &[
            replace(
                "/obj",
                json!({
                    "a": {"__musubi_store_id__": ["x"], "n": 1},
                    "b": {"__musubi_store_id__": ["x"], "n": 1}
                }),
            ),
            remove("/obj/a/__musubi_store_id__"),
        ],
        &[],
    );

    assert_is_a_tree(&tree);
    assert_eq!(obj.field::<Value>("b").expect("b").node(), node);
    assert!(panel.fields().is_live());
    assert_eq!(tree.store_node(&store_id(&["x"])), Some(node));
    assert_eq!(
        obj.value(),
        json!({"a": {"n": 1}, "b": {"__musubi_store_id__": ["x"], "n": 1}})
    );
}

#[test]
fn a_reordered_list_of_stream_bearing_child_stores_keeps_every_item_of_both_streams() {
    // The reorder shape again, with a stream under each row. A marker op
    // re-expresses the identity change as a value write built from
    // `semantic_deep().to_wire()`, and a `Collection`'s wire projection is its
    // bare marker — items travel in `stream_ops` and never in a value (§3.1).
    // Both rows move by adoption, so the bare marker lands on the collection
    // that is already there and says nothing about what it holds.
    let tree = seeded(json!({
        "__musubi_store_id__": [],
        "rows": [
            {"__musubi_store_id__": ["a"], "messages": {"__musubi_stream__": "messages"}},
            {"__musubi_store_id__": ["b"], "messages": {"__musubi_stream__": "messages"}}
        ]
    }));
    let rows = tree
        .root::<Value>()
        .field::<Vec<Value>>("rows")
        .expect("rows");
    let (first, second) = (
        rows.at(0).expect("rows[0]").node(),
        rows.at(1).expect("rows[1]").node(),
    );

    commit(
        &tree,
        &[],
        &[
            row_insert_op(&["a"], "a1", json!({"id": "a1"})),
            row_insert_op(&["a"], "a2", json!({"id": "a2"})),
            row_insert_op(&["b"], "b1", json!({"id": "b1"})),
        ],
    );

    commit(
        &tree,
        &[
            replace("/rows/1/__musubi_store_id__/0", json!("a")),
            replace("/rows/0/__musubi_store_id__/0", json!("b")),
        ],
        &[],
    );

    assert_is_a_tree(&tree);
    assert_eq!(rows.at(0).expect("rows[0]").node(), second);
    assert_eq!(rows.at(1).expect("rows[1]").node(), first);
    assert_eq!(
        rows.value(),
        [
            json!({"__musubi_store_id__": ["b"], "messages": [{"id": "b1"}]}),
            json!({
                "__musubi_store_id__": ["a"],
                "messages": [{"id": "a1"}, {"id": "a2"}]
            })
        ]
    );
    assert_eq!(tree.store_node(&store_id(&["a"])), Some(first));
    assert_eq!(tree.store_node(&store_id(&["b"])), Some(second));

    // The collection index is keyed by `(store_id, stream)`, so the next item
    // for store `a` has to land in the list that moved rather than in one
    // nothing can reach.
    commit(
        &tree,
        &[],
        &[row_insert_op(&["a"], "a3", json!({"id": "a3"}))],
    );

    assert_eq!(
        rows.at(1).expect("rows[1]").value(),
        json!({
            "__musubi_store_id__": ["a"],
            "messages": [{"id": "a1"}, {"id": "a2"}, {"id": "a3"}]
        })
    );
}

#[test]
fn a_row_prepended_before_a_stream_bearing_child_store_keeps_its_node_and_its_items() {
    // The prepend shape with a stream under the row that moves. Store `a` is
    // rendered in both frames — it never unmounts — so BDR-0011's fresh-mount
    // reset does not apply to it: it keeps its node, and its items stay on it.
    let tree = seeded(json!({
        "__musubi_store_id__": [],
        "rows": [{"__musubi_store_id__": ["a"], "messages": {"__musubi_stream__": "messages"}}]
    }));
    let rows = tree
        .root::<Value>()
        .field::<Vec<Value>>("rows")
        .expect("rows");
    let node = rows.at(0).expect("rows[0]").node();

    commit(
        &tree,
        &[],
        &[
            row_insert_op(&["a"], "a1", json!({"id": "a1"})),
            row_insert_op(&["a"], "a2", json!({"id": "a2"})),
        ],
    );

    commit(
        &tree,
        &[
            add(
                "/rows/1",
                json!({
                    "__musubi_store_id__": ["a"],
                    "messages": {"__musubi_stream__": "messages"}
                }),
            ),
            remove("/rows/0/messages"),
            remove("/rows/0/__musubi_store_id__"),
            add("/rows/0/kind", json!("banner")),
        ],
        &[],
    );

    assert_is_a_tree(&tree);
    assert_eq!(
        rows.value(),
        [
            json!({"kind": "banner"}),
            json!({
                "__musubi_store_id__": ["a"],
                "messages": [{"id": "a1"}, {"id": "a2"}]
            })
        ]
    );
    assert_eq!(rows.at(1).expect("rows[1]").node(), node);
    assert_eq!(tree.store_node(&store_id(&["a"])), Some(node));
    assert_eq!(tree.store_ids().len(), 2);

    commit(
        &tree,
        &[],
        &[row_insert_op(&["a"], "a3", json!({"id": "a3"}))],
    );

    assert_eq!(
        rows.at(1).expect("rows[1]").value(),
        json!({
            "__musubi_store_id__": ["a"],
            "messages": [{"id": "a1"}, {"id": "a2"}, {"id": "a3"}]
        })
    );
}

#[test]
fn adding_a_marker_mounts_a_child_store_and_removing_it_unmounts_one() {
    let tree = seeded(json!({"__musubi_store_id__": [], "panel": {"n": 1}}));
    let root = tree.root::<Value>();
    let plain = root.field::<Value>("panel").expect("panel").node();

    commit(
        &tree,
        &[add("/panel/__musubi_store_id__", json!(["panel"]))],
        &[],
    );

    let mounted = StoreState::from(root.field::<Value>("panel").expect("panel"));

    assert_ne!(mounted.node(), plain);
    assert_eq!(mounted.store_id(), Some(store_id(&["panel"])));
    assert_eq!(tree.store_node(&store_id(&["panel"])), Some(mounted.node()));

    // A segment appended to the id is a different store.
    commit(
        &tree,
        &[add("/panel/__musubi_store_id__/-", json!("0"))],
        &[],
    );

    assert_eq!(tree.store_node(&store_id(&["panel"])), None);
    assert_eq!(tree.store_ids().len(), 2);

    commit(&tree, &[remove("/panel/__musubi_store_id__")], &[]);

    assert_eq!(tree.store_ids(), [StoreId::root()]);
    assert_eq!(
        tree.root::<Value>().value(),
        json!({"__musubi_store_id__": [], "panel": {"n": 1}})
    );
}

#[test]
fn a_malformed_store_marker_op_is_refused_and_rolls_the_envelope_back() {
    let tree = seeded(json!({
        "__musubi_store_id__": [],
        "panel": {"__musubi_store_id__": ["a"], "n": 1},
        "scalar": 1
    }));
    let before = tree.to_wire(tree.root_id());

    for op in [
        // A store id is an array of strings, and one of its elements is a
        // string.
        replace("/panel/__musubi_store_id__", json!("a")),
        replace("/panel/__musubi_store_id__/0", json!(7)),
        // Out of range, and one level too deep.
        replace("/panel/__musubi_store_id__/3", json!("b")),
        remove("/panel/__musubi_store_id__/3"),
        replace("/panel/__musubi_store_id__/0/deeper", json!("b")),
        // Nothing but an object or a child store carries one.
        add("/scalar/__musubi_store_id__", json!(["s"])),
    ] {
        assert!(
            tree.apply(std::slice::from_ref(&op), &[]).is_err(),
            "expected {op:?} to be refused"
        );
    }

    assert_eq!(tree.to_wire(tree.root_id()), before);
}

// ---------------------------------------------------------------- the pointer

#[test]
fn the_pointer_walk_handles_escaping_and_the_array_rules() {
    let tree = seeded(json!({"a/b": {"c~d": 1}, "list": [10, 20]}));
    let root = tree.root::<Value>();

    // `~1` is a `/` and `~0` is a `~`.
    commit(&tree, &[replace("/a~1b/c~0d", json!(2))], &[]);

    assert_eq!(
        root.field::<Value>("a/b")
            .and_then(|node| node.field::<i64>("c~d"))
            .expect("a/b.c~d")
            .value(),
        2
    );

    // `-` appends, and `add` accepts `index == len`.
    commit(&tree, &[add("/list/-", json!(30))], &[]);
    commit(&tree, &[add("/list/3", json!(40))], &[]);

    assert_eq!(
        root.field::<Vec<i64>>("list").expect("list").value(),
        [10, 20, 30, 40]
    );

    // Out of range, malformed, and `-` where only a real index will do.
    for op in [
        add("/list/9", json!(1)),
        add("/list/01", json!(1)),
        remove("/list/-"),
        remove("/list/4"),
    ] {
        assert!(
            matches!(
                tree.apply(std::slice::from_ref(&op), &[]),
                Err(TreeError::Index { .. })
            ),
            "expected an index error for {op:?}"
        );
    }
}

#[test]
fn the_pointer_walk_refuses_what_it_cannot_address() {
    let tree = seeded(json!({
        "__musubi_store_id__": [],
        "scalar": 1,
        "messages": {"__musubi_stream__": "messages"}
    }));

    commit(&tree, &[], &[insert_op("a", -1, json!({"id": "a"}), None)]);

    // A scalar has nothing under it; a stream item has no pointer at all.
    for op in [
        replace("/scalar/deep", json!(1)),
        replace("/messages/0", json!(1)),
        replace("/missing", json!(1)),
        remove("/missing"),
        // A pointer that is not empty must start with `/`.
        replace("scalar", json!(1)),
        // The document root cannot be removed.
        remove(""),
    ] {
        assert!(
            matches!(
                tree.apply(std::slice::from_ref(&op), &[]),
                Err(TreeError::Pointer { .. })
            ),
            "expected a pointer error for {op:?}"
        );
    }
}

#[test]
fn an_add_at_an_index_inserts_and_moves_the_elements_after_it() {
    // §9.1 reads a plain array's identity off the index, and a whole-list
    // `replace` still honours that to the letter (the tests just above). An
    // `add /list/0` is a different statement: RFC 6902 defines it as an
    // insertion, so the tail is moved rather than rewritten — the element that
    // was at 0 keeps its node, its value and its subscribers, and lands at 1.
    //
    // This is what the rewrite-the-values shift was traded for: that one copied
    // every element from the index to the end through a JSON round-trip, twice,
    // under the arena lock, and dropped the items of any stream slot it moved
    // (see `a_stream_slot_an_array_shift_moves_keeps_its_items`).
    let tree = seeded(json!({"list": ["a", "b"]}));
    let list = tree
        .root::<Value>()
        .field::<Vec<String>>("list")
        .expect("list");
    let first = list.at(0).expect("index 0");
    let second = list.at(1).expect("index 1");
    let log = Log::default();
    let _zero = log.watch("0", &first);
    let _one = log.watch("1", &second);
    let _list = log.watch("list", &list);

    commit(&tree, &[add("/list/0", json!("new"))], &[]);

    // The list changed — its semantic is the ordered sequence of its children's
    // (§9.1) — and no element did, because none of them holds a different value
    // than it held.
    assert_eq!(log.taken(), ["list"]);
    assert_eq!(list.value(), ["new", "a", "b"]);
    assert_eq!(first.node(), list.at(1).expect("index 1").node());
    assert_eq!(second.node(), list.at(2).expect("index 2").node());
    assert_eq!(first.value(), "a");
    assert_eq!(second.value(), "b");
    assert_ne!(list.at(0).expect("index 0").node(), first.node());
    assert_eq!(list.at(0).expect("index 0").value(), "new");
}

#[test]
fn a_remove_at_an_index_kills_that_node_and_moves_the_rest() {
    // The mirror: `remove /list/0` takes out the node at 0, and the elements
    // after it keep theirs.
    let tree = seeded(json!({"list": ["a", "b", "c"]}));
    let list = tree
        .root::<Value>()
        .field::<Vec<String>>("list")
        .expect("list");
    let first = list.at(0).expect("index 0");
    let last = list.at(2).expect("index 2");

    commit(&tree, &[remove("/list/0")], &[]);

    assert_is_a_tree(&tree);
    assert_eq!(list.value(), ["b", "c"]);
    assert!(!first.is_live());
    assert!(last.is_live());
    assert_eq!(last.node(), list.at(1).expect("index 1").node());
    assert_eq!(list.at(0).expect("index 0").value(), "b");
}

#[test]
fn the_elements_an_array_shift_moves_keep_their_nodes_and_their_subscribers() {
    let tree = seeded(json!({"list": ["a", "b", "c"]}));
    let list = tree
        .root::<Value>()
        .field::<Vec<String>>("list")
        .expect("list");
    let watched = list.at(2).expect("index 2");
    let node = watched.node();
    let log = Log::default();
    let _c = log.watch("c", &watched);

    commit(&tree, &[add("/list/0", json!("new"))], &[]);

    // A move is not a change: the row still holds "c".
    assert!(log.taken().is_empty());
    assert_eq!(list.at(3).expect("index 3").node(), node);
    assert_eq!(watched.value(), "c");

    // And the subscription taken before the shift is still live on that row.
    commit(&tree, &[replace("/list/3", json!("c2"))], &[]);

    assert_eq!(log.taken(), ["c"]);
    assert_eq!(watched.value(), "c2");
    assert_eq!(list.value(), ["new", "a", "b", "c2"]);
}

#[test]
fn a_stream_slot_an_array_shift_moves_keeps_its_items() {
    // The lossy half of the old value-rewrite shift. A `Collection`'s wire
    // projection is its bare marker — stream contents travel in `stream_ops`
    // and never in a value (§3.1) — so a stream slot pushed one slot right
    // through `to_wire()` came out the other side as an *empty* collection, and
    // the collection index pointed at the empty one. Shifting the node instead
    // is what makes the slot survive being moved.
    let tree = seeded(json!({"rows": [{"__musubi_stream__": "messages"}]}));
    let rows = tree
        .root::<Value>()
        .field::<Vec<Value>>("rows")
        .expect("rows");

    commit(
        &tree,
        &[],
        &[insert_op(
            "msg-1",
            -1,
            json!({"id": "1", "body": "one"}),
            None,
        )],
    );

    let slot = rows.at(0).expect("rows[0]").node();

    commit(&tree, &[add("/rows/0", json!({"kind": "header"}))], &[]);

    assert_is_a_tree(&tree);
    assert_eq!(rows.at(1).expect("rows[1]").node(), slot);
    assert_eq!(
        rows.value(),
        [
            json!({"kind": "header"}),
            json!([{"id": "1", "body": "one"}])
        ]
    );

    // The collection index still resolves to the moved node, so the next stream
    // op lands in the same list rather than in a slot nothing can reach.
    commit(
        &tree,
        &[],
        &[insert_op(
            "msg-2",
            -1,
            json!({"id": "2", "body": "two"}),
            None,
        )],
    );

    assert_eq!(
        rows.value(),
        [
            json!({"kind": "header"}),
            json!([{"id": "1", "body": "one"}, {"id": "2", "body": "two"}])
        ]
    );

    // And the same, shifting back left.
    commit(&tree, &[remove("/rows/0")], &[]);

    assert_is_a_tree(&tree);
    assert_eq!(rows.at(0).expect("rows[0]").node(), slot);
    assert_eq!(
        rows.value(),
        [json!([{"id": "1", "body": "one"}, {"id": "2", "body": "two"}])]
    );
}

// ------------------------------------------------------------- subscriptions

#[test]
fn dropping_a_subscription_unsubscribes() {
    let tree = seeded(json!({"count": 1}));
    let count = tree.root::<Value>().field::<i64>("count").expect("count");
    let hits = Arc::new(AtomicUsize::new(0));
    let seen = hits.clone();
    let subscription = count.subscribe(move |_| {
        seen.fetch_add(1, Ordering::SeqCst);
    });

    assert_eq!(tree.node(count.node()).expect("live").subscribers, 1);

    commit(&tree, &[replace("/count", json!(2))], &[]);

    assert_eq!(hits.load(Ordering::SeqCst), 1);

    drop(subscription);

    assert_eq!(tree.node(count.node()).expect("live").subscribers, 0);

    commit(&tree, &[replace("/count", json!(3))], &[]);

    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[test]
fn closing_a_tree_empties_it_notifies_once_and_refuses_every_later_transaction() {
    let tree = seeded(json!({"count": 1}));
    let root = tree.root::<Value>();
    let count = root.field::<i64>("count").expect("count");
    let log = Log::default();
    let _root = log.watch("root", &root);
    let _count = log.watch("count", &count);

    drop(tree.close());

    assert_eq!(log.taken(), ["count", "root"]);
    assert!(tree.is_closed());
    assert!(!root.is_live());
    assert!(!count.is_live());
    assert_eq!(tree.to_wire(tree.root_id()), Some(Value::Null));
    assert!(matches!(
        tree.apply(&[replace("", json!(1))], &[]),
        Err(TreeError::Closed)
    ));

    // Subscribing afterwards hands back an inert token rather than failing, and
    // nothing ever calls it.
    let hits = Arc::new(AtomicUsize::new(0));
    let seen = hits.clone();
    let subscription = root.subscribe(move |_| {
        seen.fetch_add(1, Ordering::SeqCst);
    });

    drop(tree.close());
    drop(subscription);

    assert_eq!(hits.load(Ordering::SeqCst), 0);
}

#[test]
fn a_callback_may_read_subscribe_and_even_apply_without_deadlocking() {
    let tree = seeded(json!({"count": 1, "other": 0}));
    let count = tree.root::<Value>().field::<i64>("count").expect("count");
    let reader = count.clone();
    let inner_tree = tree.clone();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let recorded = seen.clone();
    let held: Arc<Mutex<Vec<Subscription>>> = Arc::new(Mutex::new(Vec::new()));
    let parked = held.clone();

    let _watch = count.subscribe(move |change| {
        // Re-read through the handle — the whole point of `Change` carrying
        // nothing but a revision.
        recorded
            .lock()
            .expect("test log")
            .push((change.revision, reader.value()));

        // Subscribe from inside a callback…
        parked
            .lock()
            .expect("test log")
            .push(reader.subscribe(|_| {}));

        // …and open a nested transaction from inside one, which is legal and
        // produces a nested `Notify` dropped inside the outer notification.
        drop(
            inner_tree
                .apply(&[replace("/other", json!(1))], &[])
                .expect("applies"),
        );
    });

    commit(&tree, &[replace("/count", json!(2))], &[]);

    assert_eq!(*seen.lock().expect("test log"), [(2, 2)]);
    assert_eq!(held.lock().expect("test log").len(), 1);
}

#[test]
fn a_subscription_outliving_its_tree_drops_without_touching_anything() {
    let tree = seeded(json!({"count": 1}));
    let count = tree.root::<Value>().field::<i64>("count").expect("count");
    let subscription = count.subscribe(|_| {});

    drop(tree);
    drop(count);
    // The token holds a `Weak`: there is nothing left to unregister from.
    drop(subscription);
}

// ------------------------------------------------------- reads and projections

#[derive(Debug, Deserialize, PartialEq)]
struct Row {
    id: String,
    body: String,
}

#[test]
fn value_materializes_and_try_value_reports_what_value_would_raise() {
    let tree = seeded(json!({"row": {"id": "a", "body": "hi"}}));
    let row = tree.root::<Value>().field::<Row>("row").expect("row");

    assert_eq!(
        row.value(),
        Row {
            id: "a".to_owned(),
            body: "hi".to_owned()
        }
    );

    // A shape that does not match is drift, and `try_value` says so.
    let wrong = row.cast::<Vec<i64>>();

    assert!(matches!(wrong.try_value(), Err(ReadError::Shape(_))));

    commit(&tree, &[remove("/row")], &[]);

    assert!(matches!(row.try_value(), Err(ReadError::Gone)));
}

#[test]
#[should_panic(expected = "reading node")]
fn value_panics_on_a_node_that_is_gone() {
    let tree = seeded(json!({"row": 1}));
    let row = tree.root::<Value>().field::<i64>("row").expect("row");

    commit(&tree, &[remove("/row")], &[]);

    let _ = row.value();
}

#[test]
fn navigation_never_fails_and_an_absent_key_yields_a_handle_that_reads_as_gone() {
    // §2.4's handle law: `x.prop()` is zero-cost, reads no value and cannot
    // fail. That has to hold on a root that has not been patched yet, on one
    // teardown has emptied, and on a key the server simply did not render.
    let fresh = StateTree::new();
    let before_any_patch = fresh.root::<Value>().child::<i64>("count");

    assert!(!before_any_patch.is_live());
    assert!(matches!(before_any_patch.try_value(), Err(ReadError::Gone)));

    let tree = seeded(json!({"row": 1}));
    let root = tree.root::<Value>();
    let missing = root.child::<i64>("nope");

    assert!(!missing.is_live());
    assert!(matches!(missing.try_value(), Err(ReadError::Gone)));
    assert_eq!(missing.revision(), 0);
    // Chaining off a dead handle is dead, not a panic.
    assert!(!missing.child::<i64>("deeper").is_live());
    assert_eq!(root.child::<i64>("row").value(), 1);

    // The null key is a slot no node can ever occupy, so subscribing to it
    // registers an inert token: nothing ever calls it, and dropping it finds
    // nothing to unregister from.
    let hits = Arc::new(AtomicUsize::new(0));
    let seen = hits.clone();
    let subscription = missing.subscribe(move |_| {
        seen.fetch_add(1, Ordering::SeqCst);
    });

    commit(&tree, &[replace("", json!({"row": 2, "nope": 3}))], &[]);

    assert_eq!(hits.load(Ordering::SeqCst), 0);
    // A key the tree has since grown does not resurrect a handle bound to the
    // null slot; it is a fresh `child()` that finds the new node.
    assert!(!missing.is_live());
    assert!(root.child::<i64>("nope").is_live());

    drop(subscription);

    drop(tree.close());

    // A closed tree keeps its root node, so `Gone` has to come from the tree's
    // state rather than from the node having been freed (§2.5).
    assert!(!root.is_live());
    assert!(matches!(root.try_value(), Err(ReadError::Gone)));
    assert!(matches!(
        root.child::<i64>("row").try_value(),
        Err(ReadError::Gone)
    ));
}

#[test]
fn a_panicking_subscriber_loses_only_its_own_notification() {
    let tree = seeded(json!({"a": 1, "b": 1}));
    let root = tree.root::<Value>();
    let a = root.field::<i64>("a").expect("a");
    let b = root.field::<i64>("b").expect("b");
    let log = Log::default();
    let _boom = a.subscribe(|_| panic!("this subscriber is broken"));
    let _watch = log.watch("b", &b);

    // The panic hook still reports it; what it must not do is skip `b` or
    // unwind out of the drop and onto the actor task (§4.4).
    let hook = std::panic::take_hook();

    std::panic::set_hook(Box::new(|_| {}));
    commit(
        &tree,
        &[replace("/a", json!(2)), replace("/b", json!(2))],
        &[],
    );
    std::panic::set_hook(hook);

    assert_eq!(log.taken(), ["b"]);
    assert_eq!(b.value(), 2);
}

#[test]
fn the_two_projections_differ_only_in_how_a_stream_slot_is_rendered() {
    let tree = seeded(json!({
        "__musubi_store_id__": [],
        "attachment": {"__musubi_upload__": "avatar"},
        "feed": {"__musubi_async__": true, "status": "ok", "result": 1, "reason": null},
        "messages": {"__musubi_stream__": "messages"}
    }));

    commit(&tree, &[], &[insert_op("a", -1, json!({"id": "a"}), None)]);

    let root = tree.root_id();

    assert_eq!(
        tree.to_hydrated(root).expect("a live root"),
        json!({
            "__musubi_store_id__": [],
            "attachment": {"__musubi_upload__": "avatar"},
            "feed": {"__musubi_async__": true, "status": "ok", "result": 1, "reason": null},
            "messages": [{"id": "a"}]
        })
    );
    assert_eq!(
        tree.to_wire(root).expect("a live root"),
        json!({
            "__musubi_store_id__": [],
            "attachment": {"__musubi_upload__": "avatar"},
            "feed": {"__musubi_async__": true, "status": "ok", "result": 1, "reason": null},
            "messages": {"__musubi_stream__": "messages"}
        })
    );
}

#[test]
fn a_wire_projection_seeds_an_identical_tree() {
    let tree = seeded(json!({
        "__musubi_store_id__": [],
        "panel": {"__musubi_store_id__": ["panel"], "rows": [1, 2]},
        "messages": {"__musubi_stream__": "messages"}
    }));

    commit(&tree, &[], &[insert_op("a", -1, json!({"id": "a"}), None)]);

    let wire = tree.to_wire(tree.root_id()).expect("a live root");
    let reseeded = seeded(wire.clone());

    // The cache round-trip is lossless for everything but stream *contents*,
    // which the server re-delivers as `stream_ops`.
    assert_eq!(reseeded.to_wire(reseeded.root_id()), Some(wire));
    assert_eq!(reseeded.store_ids().len(), 2);
}

#[test]
fn an_optional_node_reads_as_none_only_when_it_is_null() {
    let tree = seeded(json!({"maybe": null}));
    let maybe = tree
        .root::<Value>()
        .field::<Option<i64>>("maybe")
        .expect("maybe");

    assert!(maybe.is_none());
    assert!(maybe.as_some().is_none());

    commit(&tree, &[replace("/maybe", json!(7))], &[]);

    assert!(!maybe.is_none());
    assert_eq!(maybe.as_some().expect("a value").value(), 7);
}

#[test]
fn a_stream_async_result_is_a_keyed_collection_of_its_own() {
    let tree = seeded(json!({
        "__musubi_store_id__": [],
        "feed": {
            "__musubi_async__": true,
            "status": "ok",
            "result": {"__musubi_stream__": "messages"},
            "reason": null
        }
    }));

    commit(&tree, &[], &[insert_op("a", -1, json!({"id": "a"}), None)]);

    let feed = AsyncState::from(
        tree.root::<Value>()
            .field::<Vec<Value>>("feed")
            .expect("feed"),
    );
    let rows = feed.ok_stream().expect("a materialized result");

    assert_eq!(keys(&rows), ["a"]);

    let row = rows.by_key("a").expect("a");

    // A reconnect drops the value back to `loading` while keeping the rows: the
    // async node is told, the row is not.
    let log = Log::default();
    let _row = log.watch("row", &row);

    commit(
        &tree,
        &[replace(
            "/feed",
            json!({
                "__musubi_async__": true,
                "status": "loading",
                "result": {"__musubi_stream__": "messages"},
                "reason": null
            }),
        )],
        &[],
    );

    assert!(log.taken().is_empty());
    assert_eq!(feed.status(), AsyncStatus::Loading);
    assert_eq!(rows.len(), 1);
    assert_eq!(row.node(), rows.by_key("a").expect("a").node());

    // What a consumer of a generated bundle materializes: the collection
    // projects back to a JSON array, so the snapshot type a `stream_async`
    // field renders as — `AsyncResult<Vec<T>>` — deserializes straight off it
    // (§4.3). The projection is what carries this, with no hydration pass.
    #[derive(Debug, Deserialize, PartialEq)]
    struct Message {
        id: String,
    }

    let typed = AsyncState::from(
        tree.root::<Value>()
            .field::<Vec<Message>>("feed")
            .expect("feed"),
    );

    assert_eq!(
        typed.value(),
        AsyncResult::Loading {
            result: Some(vec![Message { id: "a".to_owned() }]),
            reason: None
        }
    );
}

#[test]
fn the_debug_of_a_view_prints_identity_and_never_materializes() {
    let tree = seeded(json!({"count": "a-value-nobody-should-see"}));
    let count = tree
        .root::<Value>()
        .field::<String>("count")
        .expect("count");
    let printed = format!("{count:?}");

    assert!(printed.starts_with("State { node:"), "{printed}");
    assert!(printed.contains("revision: 1"), "{printed}");
    assert!(printed.contains("live: true"), "{printed}");
    assert!(!printed.contains("a-value-nobody-should-see"), "{printed}");
}

#[test]
fn a_node_copy_reports_the_kind_the_arena_holds() {
    let tree = seeded(json!({"list": [1], "count": 2}));
    let node = tree.node(tree.root_id()).expect("a live root");

    assert_eq!(node.parent, None);
    assert_eq!(node.revision, 1);
    assert_eq!(node.subscribers, 0);

    let NodeKind::Object(fields) = node.kind else {
        panic!("the root is a plain object");
    };
    let keys: BTreeMap<String, NodeId> = fields
        .into_iter()
        .map(|(key, id)| (key.to_string(), id))
        .collect();

    assert_eq!(keys.keys().collect::<Vec<_>>(), ["count", "list"]);
    assert!(!tree.is_empty());
    assert_eq!(tree.len(), 4);
}

// --------------------------------------------------------- §2.6 the type table

#[test]
fn every_handle_is_send_and_sync_even_for_a_payload_that_is_neither() {
    fn assert_send_sync<T: Send + Sync>() {}
    fn assert_send<T: Send>() {}

    // `PhantomData<fn() -> T>` is what buys this: a row type that is neither
    // `Send` nor `Sync` still crosses to the UI thread as a handle.
    #[allow(dead_code)]
    struct Neither(*const ());

    assert_send_sync::<State<Neither>>();
    assert_send_sync::<StreamState<Neither>>();
    assert_send_sync::<StoreState<Neither>>();
    assert_send_sync::<AsyncState<Neither>>();
    assert_send_sync::<UploadSlotState>();
    assert_send_sync::<StateTree>();
    assert_send_sync::<Subscription>();
    assert_send_sync::<crate::change::ChangeSet>();
    assert_send_sync::<crate::node::Node>();
    assert_send_sync::<SemanticValue>();

    // `Notify` holds `Arc<dyn Fn + Send + Sync>` callbacks, so it is `Send`;
    // `Transaction` holds a `MutexGuard` and is neither, which is what keeps a
    // transaction on the task that opened it.
    assert_send::<crate::change::Notify>();
}

#[test]
fn subscribers_do_not_run_until_the_notify_is_dropped() {
    // §3.6 steps 5–9: `commit` settles and releases the lock, the client does
    // its upload fold and its version bump, and only then does dropping the
    // guard wake the state subscribers.
    let tree = seeded(json!({"count": 1}));
    let count = tree.root::<Value>().field::<i64>("count").expect("count");
    let hits = Arc::new(AtomicUsize::new(0));
    let seen = hits.clone();
    let _watch = count.subscribe(move |_| {
        seen.fetch_add(1, Ordering::SeqCst);
    });

    let notify = tree
        .apply(&[replace("/count", json!(2))], &[])
        .expect("applies");

    assert_eq!(hits.load(Ordering::SeqCst), 0);
    assert!(!notify.changes().is_empty());
    // The tree is readable while the notification is still owed: the lock went
    // away with `commit`, not with the drop.
    assert_eq!(count.value(), 2);

    drop(notify);

    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[test]
fn a_rolled_back_transaction_restores_a_collection_it_had_already_folded() {
    let tree = streaming_tree();
    let messages = messages_state(&tree);

    commit(&tree, &[], &[insert_op("a", -1, json!({"id": "a"}), None)]);

    let node = messages.by_key("a").expect("a").node();
    let before = tree.len();

    let mut transaction = tree.begin();

    transaction
        .apply(
            &[],
            &[reset_op(), insert_op("b", -1, json!({"id": "b"}), None)],
        )
        .expect("the fold applies");

    let error = transaction
        .apply(&[replace("/missing", json!(1))], &[])
        .expect_err("nothing resolves there");

    drop(transaction);

    assert!(matches!(error, TreeError::Pointer { .. }));
    // The carry-over table, the item list and the node the reset had detached
    // are all exactly as they were.
    assert_eq!(keys(&messages), ["a"]);
    assert_eq!(messages.by_key("a").expect("a").node(), node);
    assert_eq!(tree.len(), before);
}
