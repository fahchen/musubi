//! The hop, and the `observe` pair built on it
//! (`docs/rust-reactive-state.md` §5.1, capability 1).

use futures_channel::mpsc;
use futures_util::StreamExt as _;
use gpui::{Context, Window};
use musubi_state::{AsyncState, Change, State, StoreState, StreamState, Subscription};

/// A `musubi-state` handle that can be observed from a gpui view.
///
/// The one thing [`observe`] and [`observe_with`] need from a handle: register
/// a `Send + Sync` callback on the node it is rooted at, hand back the RAII
/// token. Implemented for exactly the four navigation handles that notify —
/// [`State`], [`StreamState`], [`StoreState`], [`AsyncState`].
///
/// [`UploadSlotState`](musubi_state::UploadSlotState) is **absent on purpose**
/// (§3.4, §5.1): the server re-renders the same marker every cycle, so its
/// subscription never fires. Handing it an `observe` would hand out a token for
/// something that can never ring.
///
/// The trait exists because Rust has no overloading and §6.5.2 spells the call
/// site `musubi_gpui::observe(&handle, cx)` for every handle. It is not a seam:
/// generated code hands out these four types and nothing else.
pub trait Observe: Clone + Send + Sync + 'static {
    /// Registers `on_change` on this handle's node. RAII, as everywhere.
    fn observe_node(&self, on_change: Box<dyn Fn(Change) + Send + Sync>) -> Subscription;
}

impl<T: 'static> Observe for State<T> {
    fn observe_node(&self, on_change: Box<dyn Fn(Change) + Send + Sync>) -> Subscription {
        self.subscribe(on_change)
    }
}

impl<T: 'static> Observe for StreamState<T> {
    /// The collection node, seen through the one-argument `subscribe`: a view
    /// that only wants "the list changed" does not need the keyed diff, and the
    /// one that does uses [`drive_list`](crate::drive_list) instead. Same node,
    /// same notification occasions — only what the callback is *shown* differs.
    fn observe_node(&self, on_change: Box<dyn Fn(Change) + Send + Sync>) -> Subscription {
        self.as_state().subscribe(on_change)
    }
}

impl<S: 'static> Observe for StoreState<S> {
    fn observe_node(&self, on_change: Box<dyn Fn(Change) + Send + Sync>) -> Subscription {
        self.subscribe(on_change)
    }
}

impl<T: 'static> Observe for AsyncState<T> {
    fn observe_node(&self, on_change: Box<dyn Fn(Change) + Send + Sync>) -> Subscription {
        self.subscribe(on_change)
    }
}

/// The hop, on its own: takes a callback body written against the view, hands
/// back the `Send + Sync` closure every `subscribe` in the API asks for.
///
/// Generic over the notified **value**, never over the handle — which is
/// exactly what lets it serve `musubi-client`'s `StatusState` and `Upload`
/// (§2.4) without this crate depending on `musubi-client`:
///
/// ```rust,ignore
/// chat.status().subscribe(musubi_gpui::to_view(window, cx, |view, status, _window, cx| {
///     view.status = status;
///     cx.notify();
/// }));
/// ```
///
/// # How the hop is actually made
///
/// Not by capturing a context. gpui's `AsyncApp` holds an `rc::Weak<AppCell>`
/// and a `ForegroundExecutor` that is `!Send` by an explicit marker field, so a
/// `Send + Sync` closure cannot hold one — the sketch in §5.1/§6.3 that clones
/// `cx.to_async()` into the callback does not compile against gpui 0.2.2. What
/// crosses the thread boundary is the **value**: the returned closure owns an
/// `UnboundedSender<E>` (`Send + Sync` for `E: Send`), and a foreground task
/// spawned here drains the receiver and runs `apply` on the entity's own
/// thread.
///
/// Ordering is the channel's, so notifications arrive in the order the
/// transactions produced them. The queue is unbounded because dropping a state
/// notification would desynchronize a view, and because the task that drains it
/// is scheduled by the same executor that repaints — a backlog is a busy frame,
/// not a leak.
///
/// # Lifetime
///
/// The task ends when the returned closure is dropped (the sender goes, the
/// receiver terminates) or when the entity is released. Dropping the
/// [`Subscription`] the closure was handed to therefore tears the task down
/// too: one RAII token still owns the whole observation.
///
/// # Deviation from the signed signature
///
/// §5.1 signs `to_view(cx, apply)`. `apply` takes `&mut Window`, and in gpui
/// 0.2.2 the only way to reach a `&mut Window` from a background notification
/// is `Context::spawn_in(window, ..)` → `AsyncWindowContext` →
/// `WeakEntity::update_in`; `AsyncWindowContext::new_context` is `pub(crate)`,
/// and `Context<V>` carries no window handle of its own. So the window is a
/// parameter here, placed where gpui itself places it (immediately before
/// `cx`). Every call site in §6.5.2 already has `window` in scope.
///
/// `apply` is a named parameter rather than an `impl Trait` one only because
/// edition 2024's `use<..>` — needed to keep the returned closure from
/// capturing `window`'s and `cx`'s lifetimes — must name every type parameter
/// in scope. The call site is unchanged.
#[must_use = "the returned closure owns the foreground task; dropping it ends the hop"]
pub fn to_view<E, V, A>(
    window: &Window,
    cx: &mut Context<V>,
    apply: A,
) -> impl Fn(E) + Send + Sync + 'static + use<E, V, A>
where
    E: Send + 'static,
    V: 'static,
    A: Fn(&mut V, E, &mut Window, &mut Context<V>) + Send + Sync + 'static,
{
    let (sender, mut receiver) = mpsc::unbounded::<E>();

    cx.spawn_in(window, async move |view, cx| {
        while let Some(value) = receiver.next().await {
            // `Err` is "the entity was released": stop draining rather than
            // spin once per notification for the rest of the app's life.
            if view
                .update_in(cx, |view, window, cx| apply(view, value, window, cx))
                .is_err()
            {
                break;
            }
        }
    })
    .detach();

    move |value| {
        // `Err` is "the view is gone and the task has finished". The
        // subscription may outlive it by a moment; a dropped notification for a
        // dropped view is the correct outcome.
        let _ = sender.unbounded_send(value);
    }
}

/// The same hop for a body that needs no window: `Context<V>` alone is enough,
/// so `observe` keeps the signature §5.1 signs.
fn to_view_windowless<E, V, A>(
    cx: &mut Context<V>,
    apply: A,
) -> impl Fn(E) + Send + Sync + 'static + use<E, V, A>
where
    E: Send + 'static,
    V: 'static,
    A: Fn(&mut V, E, &mut Context<V>) + 'static,
{
    let (sender, mut receiver) = mpsc::unbounded::<E>();

    cx.spawn(async move |view, cx| {
        while let Some(value) = receiver.next().await {
            if view.update(cx, |view, cx| apply(view, value, cx)).is_err() {
                break;
            }
        }
    })
    .detach();

    move |value| {
        let _ = sender.unbounded_send(value);
    }
}

/// Redraw this view whenever this handle's node changes. The whole of the
/// common case.
///
/// ```rust,ignore
/// let subs = vec![
///     musubi_gpui::observe(&state.last_send_status(), cx),
///     musubi_gpui::observe(&state.online_users(), cx),
/// ];
/// ```
///
/// The special case of [`observe_with`] whose body is one `cx.notify()`, and
/// the reason it needs no window.
pub fn observe<H, V>(handle: &H, cx: &mut Context<V>) -> Subscription
where
    H: Observe,
    V: 'static,
{
    handle.observe_node(Box::new(to_view_windowless(
        cx,
        |_view, _change: Change, cx| cx.notify(),
    )))
}

/// [`observe`], with the handle itself fed to the body — the form for a view
/// that wants to read the new value, not merely repaint.
///
/// ```rust,ignore
/// musubi_gpui::observe_with(&state.current_user().name(), window, cx, |view, name, window, cx| {
///     view.set_draft(&name.value(), window, cx);
/// });
/// ```
///
/// What arrives is the **handle**, not a value: §2.3's `Change` carries no
/// old/new pair by design, and re-reading through the handle is what makes the
/// body see the settled state rather than an intermediate one. Nothing is
/// materialized unless the body asks.
///
/// Two consequences of that, both correct for a view and both worth stating:
///
/// * The body runs **once per notification**, but reads the tree as it is when
///   it runs. Two transactions that land before the foreground drains produce
///   two runs that both read the second one's value. What gets painted is the
///   current state, which is the point; a consumer that needs the value each
///   transaction settled on puts that value in the notification and uses
///   [`to_view`] directly.
/// * The body may run once after the returned [`Subscription`] is dropped — see
///   [`Subscription`]'s own contract — and by then the handle may be dead.
///   `is_live()` and `try_value()` are the checked reads.
///
/// # Deviation from the signed signature
///
/// The `window` parameter, for the reason [`to_view`] documents.
pub fn observe_with<H, V>(
    handle: &H,
    window: &Window,
    cx: &mut Context<V>,
    apply: impl Fn(&mut V, H, &mut Window, &mut Context<V>) + Send + Sync + 'static,
) -> Subscription
where
    H: Observe,
    V: 'static,
{
    let observed = handle.clone();
    let forward = to_view(window, cx, apply);

    handle.observe_node(Box::new(move |_change| forward(observed.clone())))
}
