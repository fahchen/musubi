//! The list driver (`docs/rust-reactive-state.md` §5.1 capability 2, §6.3).

use futures_channel::mpsc;
use futures_util::StreamExt as _;
use gpui::{Context, ListState};
use musubi_state::{CollectionEdit, StreamState, Subscription};

/// Translates a keyed `ChangeSet` into list splices. The one place
/// `musubi-state`'s vocabulary and gpui's meet.
///
/// ```rust,ignore
/// self._list_driver = Some(musubi_gpui::drive_list(&rows, &self.list, cx));
/// ```
///
/// The driver owns the alignment between the collection node and the
/// [`ListState`]: it resets the list to the collection's current length when it
/// is installed, and from then on replays each transaction's
/// [`CollectionEdit`]s in order. Row heights survive every edit except the ones
/// the wire genuinely reset, which is the whole point of not calling
/// `reset(len)` per envelope the way `examples/chat_room/desktop` does today.
///
/// A change confined to an item's own fields arrives with an **empty** edit
/// slice (§6.3): nothing is spliced, and the view is notified so a parent-drawn
/// row can repaint. A view whose rows are their own entities subscribes per row
/// instead and ignores the collection.
///
/// # `splice` is real in gpui 0.2.2
///
/// §10.2 left this unverified. It is verified now: `ListState::splice(Range,
/// usize)` and `ListState::item_count()` both exist in the pinned 0.2.2, and
/// `splice` marks only the spliced rows unmeasured. So this is the incremental
/// implementation, not the `reset(rows.len())` degrade §6.3 describes —
/// capability (2) is a present-tense argument for the crate, not a prospective
/// one. The degrade path survives in exactly one arm: [`CollectionEdit`] is
/// `#[non_exhaustive]`, so an edit kind a future `musubi-state` adds falls back
/// to a reset rather than to a silently wrong row count.
///
/// # Threading
///
/// As [`to_view`](crate::to_view): the subscription callback is `Send + Sync`
/// and `ListState` is `Rc<RefCell<..>>`, so the list cannot be captured by the
/// callback. It is owned by the foreground task instead, and what crosses the
/// thread boundary is the edit slice.
pub fn drive_list<T, V>(
    rows: &StreamState<T>,
    list: &ListState,
    cx: &mut Context<V>,
) -> Subscription
where
    T: 'static,
    V: 'static,
{
    let driven = list.clone();
    let (sender, mut receiver) = mpsc::unbounded::<(usize, Vec<CollectionEdit>)>();

    cx.spawn(async move |view, cx| {
        while let Some((len, edits)) = receiver.next().await {
            let applied = view.update(cx, |_view, cx| {
                splice(&driven, len, &edits);
                cx.notify();
            });

            if applied.is_err() {
                break;
            }
        }
    })
    .detach();

    let source = rows.clone();
    let subscription = rows.subscribe(move |_change, edits| {
        // The length is read here, not in the task: `Notify`'s drop runs one
        // transaction's callbacks before the next transaction is applied
        // (§3.6), so this is exactly the length the edits settle on.
        let _ = sender.unbounded_send((source.len(), edits.to_vec()));
    });

    // Subscribe first, align second. The reverse order can drop an edit that
    // lands in between; this order can only duplicate one, and the length each
    // batch carries makes a duplicate self-correcting.
    list.reset(rows.len());

    subscription
}

/// Replays one transaction's edits onto the list, then checks the row count it
/// was supposed to land on.
///
/// Every index is the one valid at the moment its edit was applied, in edit
/// order (§2.3), so no index fixing is needed here — that is the half-ounce of
/// convenience `CollectionEdit` exists to buy.
fn splice(list: &ListState, len: usize, edits: &[CollectionEdit]) {
    for edit in edits {
        match edit {
            CollectionEdit::Inserted { index, .. } => list.splice(*index..*index, 1),
            CollectionEdit::Removed { index, .. } => list.splice(*index..*index + 1, 0),
            // A move is resolved against the post-removal list on the tree side
            // too, so `to` needs no adjustment for the row just taken out.
            CollectionEdit::Moved { from, to, .. } => {
                list.splice(*from..*from + 1, 0);
                list.splice(*to..*to, 1);
            }
            CollectionEdit::Reset => list.reset(0),
            // The `#[non_exhaustive]` arm, and the only surviving use of §6.3's
            // degrade path: right row count, lost row heights.
            _ => {
                list.reset(len);
                return;
            }
        }
    }

    // Steady state never reaches the body: replaying the edits already lands on
    // `len`. It catches the install race above, and any future edit whose
    // splice translation is off — cheaply, and without a second code path.
    if list.item_count() != len {
        list.reset(len);
    }
}
