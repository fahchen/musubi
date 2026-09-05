//! The semantics appendix (`docs/rust-reactive-state.md` §9), as tests.
//!
//! Every row of §9.1's equality table, §9.2's transaction rules, §9.3's
//! revision and notification rules, and both worked examples (§9.4, §9.5) has a
//! test here. The module-level ones that follow cover what the appendix leans
//! on: the pointer walk, structural sharing, the carry-over table and the
//! lock discipline.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde::Deserialize;
use serde_json::{Value, json};

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
fn an_add_at_an_index_shifts_the_values_because_index_is_the_identity() {
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

    commit(&tree, &[add("/list/0", json!("new"))], &[]);

    // §9.1's owner decision: the client does not infer that "a" moved to index
    // 1. Index *is* the identity, so every later index changed value and every
    // later index is told.
    assert_eq!(log.taken(), ["0", "1"]);
    assert_eq!(list.value(), ["new", "a", "b"]);
    assert_eq!(first.node(), list.at(0).expect("index 0").node());
    assert_eq!(first.value(), "new");
    assert_eq!(second.value(), "a");
}

#[test]
fn a_remove_at_an_index_shifts_the_values_back_and_kills_the_last_slot() {
    let tree = seeded(json!({"list": ["a", "b", "c"]}));
    let list = tree
        .root::<Value>()
        .field::<Vec<String>>("list")
        .expect("list");
    let last = list.at(2).expect("index 2");

    commit(&tree, &[remove("/list/0")], &[]);

    assert_eq!(list.value(), ["b", "c"]);
    assert!(!last.is_live());
    assert_eq!(list.at(0).expect("index 0").value(), "b");
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
    // Chaining off a dead handle is dead, not a panic.
    assert!(!missing.child::<i64>("deeper").is_live());
    assert_eq!(root.child::<i64>("row").value(), 1);

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
