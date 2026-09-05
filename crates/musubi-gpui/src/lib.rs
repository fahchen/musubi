//! The gpui adapter for [`musubi-state`](musubi_state).
//!
//! Two things, and deliberately nothing else
//! (`docs/rust-reactive-state.md` §5.1):
//!
//! 1. **The `!Send` hop.** A subscription callback is
//!    `Fn(Change) + Send + Sync`; a gpui entity is `!Send` and thread-affine.
//!    Every subscription therefore needs the same hop — carry the value to the
//!    foreground executor, update the entity, `cx.notify()`, branch on the
//!    entity having gone. Under per-node subscription that hop is written once
//!    per view per field rather than once per window, which is exactly the
//!    repetitive, subtly-wrong-able glue an adapter exists to take over.
//! 2. **Keyed edits become list splices.** `ChangeSet::collection_edits` names
//!    the item keys inserted, removed and moved, and their positions. That is
//!    the input a virtualized list wants: update the affected row ranges instead
//!    of wiping every cached row height with `ListState::reset(count)`.
//!
//! No widgets, no theme, no rendering. [`observe`] / [`observe_with`] /
//! [`to_view`] / [`drive_list`], and that is the whole surface.
//!
//! ```rust,ignore
//! let subs = vec![
//!     // Redraw on change — the common case.
//!     musubi_gpui::observe(&state.last_send_status(), cx),
//!     // Read the new value out of the handle.
//!     musubi_gpui::observe_with(&state.current_user().name(), window, cx,
//!         |view, name, window, cx| view.set_draft(&name.value(), window, cx)),
//!     // The bare hop, for the handles that live outside the tree.
//!     chat.status().subscribe(musubi_gpui::to_view(window, cx,
//!         |view, status, _window, cx| { view.status = status; cx.notify(); })),
//! ];
//!
//! // And the keyed collection, spliced instead of reset.
//! let driver = musubi_gpui::drive_list(&rows, &self.list, cx);
//! ```
//!
//! # Scope
//!
//! Depends on `musubi-state` and gpui, and **never on `musubi-client`** — gpui
//! cannot reach the client's dependency graph even transitively. [`to_view`] is
//! generic over the notified *value* and never over the handle, which is what
//! lets the client's own out-of-tree handles (`StatusState`, `Upload`) use it
//! from the far side of that line: the call site is word-for-word the one the
//! tree handles use.
//!
//! [`UploadSlotState`](musubi_state::UploadSlotState) gets no `observe`. Its
//! subscription never fires (§3.4), and a token for something that can never
//! ring is worse than no token.
//!
//! The crate carries its own `[workspace]` table and its own `Cargo.lock`, and
//! the root manifest excludes it: gpui stays out of the root lockfile and out of
//! the runtime-free gates.
//!
//! # Two recorded deviations from the design
//!
//! * [`to_view`] and [`observe_with`] take a `&Window`. Their signed `apply`
//!   takes `&mut Window`, and in gpui 0.2.2 the only route from a background
//!   notification to one is `Context::spawn_in(window, ..)` →
//!   `AsyncWindowContext` → `WeakEntity::update_in`. [`observe`] and
//!   [`drive_list`], whose bodies need no window, keep their signatures exactly.
//! * The hop is a channel, not a captured context. `AsyncApp` holds an
//!   `rc::Weak` and a `ForegroundExecutor` marked `!Send`, so §5.1's sketch —
//!   clone `cx.to_async()` into the `Send + Sync` callback — cannot compile. The
//!   value is sent instead, and the foreground drains it. Behaviour, ordering
//!   and RAII lifetime are unchanged.
//!
//! §10.2's other open question is closed the other way: `ListState::splice`
//! **does** exist in gpui 0.2.2, so [`drive_list`] is incremental and the reset
//! degrade path survives only as the `#[non_exhaustive]` arm.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod list;
mod observe;
#[cfg(test)]
mod tests;

pub use crate::list::drive_list;
pub use crate::observe::{Observe, observe, observe_with, to_view};
