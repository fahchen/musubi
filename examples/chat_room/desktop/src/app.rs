//! `ChatWindow` — the single `Render` entity (`docs/rust-gpui-example.md` §4).
//!
//! One window, one root entity, one root store. Every component here maps to a
//! Musubi feature the `chat_room` server already ships; nothing was added to
//! the backend to make the native client work, which is the point of the
//! example.
//!
//! | Component | Musubi feature |
//! | :-- | :-- |
//! | [`ChatWindow`] | root store mount — join *is* mount |
//! | Message list | `stream_async :messages`, materialized to a `Vec` |
//! | Loading / failed panels | `AsyncResult` `loading \| ok \| failed` |
//! | Composer | `send_message`, reply `{queued: true}` |
//! | Delivery receipt | `last_send_status`, a three-arm tagged union |
//! | Identity + rename | `set_name`, reply `{ok, name}` |
//! | Online panel | `assign_async :online_users` + PubSub |
//! | Connection pill | reconnect (BDR-0015) |
//!
//! The one rule the whole file is organized around: **state renders from
//! [`Mounted::updates`], never from a command reply.** A reply resolves before
//! the patch it caused is applied (BDR-0009), so replies only ever write the
//! one-line `feedback` string; every field the user reads comes off the
//! snapshot.
//!
//! # Parity with `ui/`
//!
//! The layout, the copy and the palette are ported from `ui/src/App.tsx` and
//! `ui/src/App.css` so the two clients read as one app: sidebar (room card,
//! identity card, rename form, presence panel) beside a chat column (header
//! with activity pills, message bubbles, composer dock). The connection pill is
//! the one piece of chrome the browser client has no equivalent for — it
//! reports the reconnect state described in §4.6.

use std::sync::Arc;

use futures::StreamExt;
use gpui::{
    AnyElement, AppContext, Context, Div, Entity, FontWeight, InteractiveElement, IntoElement,
    ListAlignment, ListState, ParentElement, Render, SharedString, StatefulInteractiveElement,
    Styled, Subscription, Task, Window, div, linear_color_stop, linear_gradient, list, px,
    relative,
};
// `when` — the conditional-builder combinator gpui blanket-implements for
// every element.
use gpui::prelude::FluentBuilder;
use gpui_component::button::{Button, ButtonVariants};
use gpui_component::input::{Input, InputEvent, InputState};
use gpui_component::{Disableable, Sizable};
use musubi_client::{Connection, Mounted, MusubiError};
use serde_json::json;

use crate::generated::chat_room::stores::chat_room_store::{
    ChatRoomStore, ChatRoomStoreLastSendStatus as SendStatus, SendMessage, SetName, State,
};
use crate::generated::chat_room::{MessageState, OnlineUser};
use crate::generated::musubi::{AsyncError, AsyncResult, Command};
use crate::theme::{
    BORDER_CARD, BORDER_SOFT, BORDER_STRONG, BUBBLE, CANVAS, CARD, DOCK, EMPTY, EYEBROW,
    FONT_FAMILY, GOLD, INK, MUTED, ON_TEAL_MUTED, PAPER, RADIUS, ROW, RUST, SAND, SAND_WASH, STAT,
    TEAL, color,
};

/// The room every client of this example joins — the same one `ui/` mounts, so
/// the browser and the native window share a presence list and a message
/// stream.
const ROOM_ID: &str = "general";

// -----------------------------------------------------------------------------
// Sidebar geometry
//
// These are pixel constants rather than a stretched layout for one specific
// reason. gpui 0.2.2's `truncate()` only produces an ellipsis when the element
// carrying it has a **definite** width at the moment taffy first measures it:
// taffy probes this subtree under `AvailableSpace::MinContent` before any
// stretch is resolved, and `StyledText` caches the line it shapes on that first
// call for the rest of the frame (the cache is keyed on wrap width, which
// `truncate()`'s `white_space: nowrap` pins to `None` forever). A stretched or
// percentage width is indefinite in that probe, so the line is shaped unclipped
// and then hard-clipped by `overflow: hidden`, with no ellipsis — and a
// `min_w_0()` anywhere up the chain makes the probe hand down a *definite* 0 px
// instead, which shapes the whole label down to a bare "…".
//
// The browser client gets the same effect from `grid-template-columns:
// auto minmax(0, 1fr)`, which is a definite track. The numbers below are that
// grid, resolved: they reproduce the browser's 199.4 px and 192.2 px text
// columns exactly.
// -----------------------------------------------------------------------------

/// `.chat-shell { grid-template-columns: minmax(260px, 320px) ... }`, at the
/// 880 px default window the max applies.
const SIDEBAR_W: f32 = 320.0;
/// `.sidebar { padding: 1rem }`.
const SIDEBAR_PAD: f32 = 16.0;
/// `border-right: 1px` on the sidebar.
const SIDEBAR_BORDER: f32 = 1.0;
/// Outer width of every card in the sidebar.
const CARD_W: f32 = SIDEBAR_W - 2.0 * SIDEBAR_PAD - SIDEBAR_BORDER;
/// `.identity-card, .presence-panel { padding: 0.9rem }`.
const CARD_PAD: f32 = 14.4;
/// `1px` card border.
const CARD_BORDER: f32 = 1.0;
/// Content width inside a sidebar card.
const CARD_INNER_W: f32 = CARD_W - 2.0 * (CARD_PAD + CARD_BORDER);
/// `.avatar { width: 36px }`.
const AVATAR: f32 = 36.0;
/// `.self-avatar { width: 44px }`.
const SELF_AVATAR: f32 = 44.0;
/// `.identity-card { gap: 0.8rem }`.
const IDENTITY_GAP: f32 = 12.8;
/// `.identity-copy` — the ellipsized "Posting as" column.
const IDENTITY_TEXT_W: f32 = CARD_INNER_W - SELF_AVATAR - IDENTITY_GAP;
/// `.users li { padding: 0.55rem }`.
const USER_PAD: f32 = 8.8;
/// `.users li { gap: 0.65rem }`.
const USER_GAP: f32 = 10.4;
/// `.user-meta` — the ellipsized name/id column.
const USER_TEXT_W: f32 = CARD_INNER_W - 2.0 * USER_PAD - AVATAR - USER_GAP;
/// `.users { max-height: min(390px, 42vh) }`, at the default window height.
const USERS_MAX_H: f32 = 260.0;

// -----------------------------------------------------------------------------
// Type sizes, straight off `App.css`
// -----------------------------------------------------------------------------

/// `.eyebrow { font-size: 0.72rem }`.
const TEXT_EYEBROW: f32 = 11.52;
/// `.user-meta small`, `.bubble small` — `font-size: small` at a 16 px root.
const TEXT_SMALL: f32 = 13.33;
/// `.chat-stats span { font-size: 0.82rem }`.
const TEXT_STAT: f32 = 13.12;
/// `.send-state { font-size: 0.86rem }`.
const TEXT_STATUS: f32 = 13.76;
/// `:root` body copy.
const TEXT_BODY: f32 = 16.0;
/// `h2 { font-size: clamp(1.3rem, 2vw, 1.8rem) }`.
const TEXT_H2: f32 = 20.8;
/// `h1 { font-size: clamp(1.9rem, 4vw, 3rem) }`, at the default window width.
const TEXT_H1: f32 = 35.2;
/// `.room-mark, .empty-mark { font-size: 1.5rem }`.
const TEXT_MARK: f32 = 24.0;

/// `.room-mark, .empty-mark { width: 48px }`.
const MARK: f32 = 48.0;
/// `.bubble p { line-height: 1.45 }`.
const BODY_LINE_HEIGHT: f32 = TEXT_BODY * 1.45;

/// `button, input { min-height: 44px }` — the stylesheet's one control height.
///
/// gpui-component's `Size::Large` gives `Input` exactly this (`input_h` maps
/// `Large` to `h_11`), but a *labelled* `Button` ignores the size above
/// `Small`: `button.rs` falls through to `h_8().px_4()` for both `Medium` and
/// `Large`, so `.large()` alone leaves the button 12 px shorter than the input
/// beside it. Pinning the height afterwards is the fix — `Button` keeps its
/// `Styled` refinement in a separate field and applies it *after* its own
/// sizing, so this overrides `h_8` rather than being overridden by it.
const CONTROL_H: f32 = 44.0;

/// Which command is in flight. The window allows one at a time, so the button
/// labels ("Sending" / "Saving", as in `App.tsx`) need to know which.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pending {
    Send,
    Rename,
}

/// The single `Render` entity.
pub struct ChatWindow {
    /// The socket base, for the "connecting to ..." line.
    url: SharedString,
    /// Set when the join is rejected. The window stays open showing why rather
    /// than the app exiting silently.
    mount_error: Option<SharedString>,
    /// The last good state. Only ever assigned `Some`: a reconnect keeps the
    /// previous tree rendered (BDR-0015), and the fresh one replaces it
    /// atomically when the re-seeded initial patch lands.
    snapshot: Option<Arc<State>>,
    /// Set when a command fails because the socket went away, cleared by the
    /// next accepted patch. The §4.6 workaround: the crate publishes no mount
    /// status, and `Mounted::snapshot()` is never cleared on reconnect, so a
    /// failed command is the only evidence the view ever gets.
    stale: bool,
    /// `None` until the join succeeds; commands are refused before that.
    mounted: Option<Mounted<ChatRoomStore>>,
    /// The one-line reply/receipt channel. Written by command replies only, and
    /// rendered in place of `last_send_status` when non-empty — the same
    /// `feedback || renderSendStatus(...)` precedence `App.tsx` uses.
    feedback: SharedString,
    /// One command at a time: both buttons read it, and `_in_flight` holds
    /// exactly one task.
    busy: Option<Pending>,
    composer: Entity<InputState>,
    name_input: Entity<InputState>,
    /// Row heights for the message list. `list` measures lazily and caches, so
    /// the count has to be pushed in whenever the stream changes length.
    messages: ListState,
    /// Held rather than detached: dropping the task cancels the update loop,
    /// which is the right teardown when the window closes. A detached loop
    /// would keep the `Mounted` — and so the server-side page — alive.
    _updates: Task<()>,
    /// Held: one command at a time, cancelled with the window.
    _in_flight: Option<Task<()>>,
    /// Held: dropping a `Subscription` unsubscribes.
    _subscriptions: Vec<Subscription>,
}

impl ChatWindow {
    /// Builds the view and starts the mount.
    ///
    /// The window already exists by the time this runs, so a failed join is a
    /// rendered panel rather than a silent exit.
    pub fn new(
        connection: Connection,
        url: SharedString,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        let composer = cx.new(|cx| InputState::new(window, cx).placeholder("Write a message"));
        let name_input = cx.new(|cx| InputState::new(window, cx).placeholder("Display name"));

        // Enter submits, in both fields. `InputEvent::PressEnter` is the only
        // arm either one cares about.
        let subscriptions = vec![
            cx.subscribe_in(&composer, window, |this, _input, event, window, cx| {
                if matches!(event, InputEvent::PressEnter { .. }) {
                    this.send_message(window, cx);
                }
            }),
            cx.subscribe_in(&name_input, window, |this, _input, event, window, cx| {
                if matches!(event, InputEvent::PressEnter { .. }) {
                    this.set_name(window, cx);
                }
            }),
        ];

        composer.update(cx, |state, cx| state.focus(window, cx));

        // §5.3: the crossing from the socket task to the UI thread is a
        // `Stream` consumed by a foreground `cx.spawn`. `Entity`/`Context` are
        // `!Send` and never cross; the `WeakEntity` + `AsyncApp` this hands out
        // are what may.
        let updates = cx.spawn_in(window, async move |this, cx| {
            // The store declares `attr(:room_id, String.t(), required: true)`
            // and its `mount/2` does `Map.fetch!(params, "room_id")`, so
            // joining with `{}` is rejected server-side.
            let mounted = match connection
                .mount::<ChatRoomStore>(ROOM_ID, json!({ "room_id": ROOM_ID }))
                .await
            {
                Ok(mounted) => mounted,
                Err(error) => {
                    this.update(cx, |view, cx| {
                        view.mount_error = Some(format!("mount failed: {error}").into());
                        cx.notify();
                    })
                    .ok();

                    return;
                }
            };

            // Subscribe *before* reading the snapshot: `updates()` does not
            // replay, so taking the snapshot afterwards is what closes the gap
            // between the two.
            let mut updates = mounted.updates();
            let initial = mounted.snapshot();

            if this
                .update_in(cx, |view, window, cx| {
                    view.adopt(initial, window, cx);
                    view.mounted = Some(mounted);
                    cx.notify();
                })
                .is_err()
            {
                return;
            }

            while let Some(snapshot) = updates.next().await {
                // A closed window is a normal exit, not an error.
                let alive = this.update_in(cx, |view, window, cx| {
                    view.adopt(Some(snapshot), window, cx);
                    view.stale = false;
                    // `cx.notify()` is the only thing that schedules a repaint;
                    // mutating the view without it renders nothing.
                    cx.notify();
                });

                if alive.is_err() {
                    break;
                }
            }
        });

        Self {
            url,
            mount_error: None,
            snapshot: None,
            stale: false,
            mounted: None,
            feedback: "".into(),
            busy: None,
            composer,
            name_input,
            // `Top` because index 0 is the newest message: the "latest" end of
            // this list is its head, not its tail.
            messages: ListState::new(0, ListAlignment::Top, px(200.0)),
            _updates: updates,
            _in_flight: None,
            _subscriptions: subscriptions,
        }
    }

    /// Installs a new snapshot and keeps the pieces that are derived from it in
    /// step: the list's row count, and the rename field's draft.
    fn adopt(&mut self, snapshot: Option<Arc<State>>, window: &mut Window, cx: &mut Context<Self>) {
        let Some(state) = snapshot else {
            return;
        };

        // `App.tsx` re-seeds `nameDraft` from `room.current_user.name` in a
        // `useEffect`, so the field always shows the name the server settled
        // on — including a rename made from the browser client.
        let name = state.current_user.name.clone();

        if self.name_input.read(cx).value().as_ref() != name.as_str() {
            self.name_input
                .update(cx, |input, cx| input.set_value(&name, window, cx));
        }

        // `ListState` caches a height per row, so it has to be told when the
        // row count moves. Rows are prepended and trimmed at 100 server-side,
        // so a reset — which also parks the viewport at the newest message — is
        // the honest answer to any change.
        let count = stale_or_fresh(&state.messages).len();

        if self.messages.item_count() != count {
            self.messages.reset(count);
        }

        self.snapshot = Some(state);
    }

    // -------------------------------------------------------------------------
    // Commands
    // -------------------------------------------------------------------------

    /// Dispatches `send_message` with the composer's contents.
    ///
    /// The reply is `{queued: true}` and arrives **before** the row does
    /// (BDR-0009: reply, then the `"patch"` push, then the `start_async` task's
    /// own patch). That is the contract, not a bug to paper over, so the
    /// feedback line says "queued" and the row shows up one envelope later.
    /// There is no `command_and_wait_for_patch` helper, by design.
    fn send_message(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.dispatch(
            window,
            cx,
            self.composer.clone(),
            (
                Pending::Send,
                "Message body cannot be empty.",
                "Message send",
            ),
            |body| SendMessage { body },
            |reply| {
                if reply.queued {
                    "Message queued for async delivery."
                } else {
                    "Send request returned."
                }
                .into()
            },
        );
    }

    /// Dispatches `set_name`. The reply carries the server-normalized name —
    /// trimmed, truncated to 40 characters, and defaulted when blank — but the
    /// name on screen still comes from the patch that follows it.
    fn set_name(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        self.dispatch(
            window,
            cx,
            self.name_input.clone(),
            (Pending::Rename, "Name cannot be empty.", "Name update"),
            |name| SetName { name },
            |reply| format!("Name updated to {}.", reply.name).into(),
        );
    }

    /// The body both commands share: refuse when unmounted or busy, trim the
    /// field, dispatch, then write the reply into `feedback` — the screen
    /// renders from the snapshot, never from a reply. The tuple is the pending
    /// marker, the empty-field refusal, and the failure label.
    fn dispatch<C: Command<ChatRoomStore>>(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
        input: Entity<InputState>,
        (pending, empty, label): (Pending, &str, &'static str),
        cmd_of: impl FnOnce(String) -> C,
        on_reply: impl FnOnce(C::Reply) -> SharedString + 'static,
    ) {
        let Some(mounted) = self.mounted.clone() else {
            return self.reject("not connected yet", cx);
        };

        if self.busy.is_some() {
            return;
        }

        let value = input.read(cx).value().trim().to_owned();

        if value.is_empty() {
            return self.reject(empty, cx);
        }

        // `App.tsx` leaves the previous line on screen while a command is in
        // flight; the button label is what reports the pending state.
        self.busy = Some(pending);
        cx.notify();

        let command = cmd_of(value);
        // Only the composer is cleared on success; the rename field is a
        // *draft of an existing value*, and `adopt` refills it from the patch.
        let clear_on_reply = pending == Pending::Send;

        self._in_flight = Some(cx.spawn_in(window, async move |this, cx| {
            let result = mounted.command(command).await;

            this.update_in(cx, |view, window, cx| {
                view.busy = None;

                match result {
                    Ok(reply) => {
                        view.feedback = on_reply(reply);

                        if clear_on_reply {
                            input.update(cx, |state, cx| state.set_value("", window, cx));
                        }
                    }
                    Err(error) => view.note_failure(label, &error),
                }

                cx.notify();
            })
            .ok();
        }));
    }

    /// Refuses a dispatch locally, without a round trip.
    fn reject(&mut self, why: &str, cx: &mut Context<Self>) {
        self.feedback = why.to_owned().into();
        cx.notify();
    }

    /// Records a command failure, and marks the view stale when the reason was
    /// the socket rather than the server (§4.6).
    fn note_failure(&mut self, label: &str, error: &MusubiError) {
        if matches!(
            error,
            MusubiError::NotConnected | MusubiError::Disconnected | MusubiError::Transport(_)
        ) {
            self.stale = true;
        }

        self.feedback = format!("{label} failed: {error}").into();
    }

    // -------------------------------------------------------------------------
    // Derived state
    // -------------------------------------------------------------------------

    /// The materialized stream. `stream_async` arrives as an ordinary
    /// `Vec<MessageState>` on the snapshot — the client resolves the stream
    /// markers before serde runs — so there is no stream type to unwrap.
    ///
    /// The server inserts `at: 0` with `limit: -100`, so index 0 is the
    /// **newest** message and the list never exceeds 100 rows. The view renders
    /// newest-first: no reversal, no scroll-to-bottom bookkeeping, which is
    /// also what `App.tsx` does.
    fn messages(&self) -> &[MessageState] {
        self.snapshot
            .as_deref()
            .map(|state| stale_or_fresh(&state.messages))
            .unwrap_or_default()
    }

    /// The name in the identity card.
    ///
    /// Before the first patch there is no identity yet, and the browser client
    /// renders its whole shell as `Connecting...` for exactly that window.
    pub fn poster(&self) -> SharedString {
        match self.snapshot.as_deref() {
            Some(state) => state.current_user.name.clone().into(),
            None => "Connecting...".into(),
        }
    }
}

// -----------------------------------------------------------------------------
// Render
// -----------------------------------------------------------------------------

impl Render for ChatWindow {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        // `:root` is a tinted desk; `.chat-shell` is the rounded card on it,
        // inset by the stylesheet's `margin: 1rem`. The top inset is deeper
        // than the stylesheet's: the window is frameless (`main.rs`), so this
        // band is where the traffic lights sit and where AppKit's drag region
        // is — it stays empty desk on purpose.
        div()
            .size_full()
            .p(px(12.0))
            .pt(px(34.0))
            .bg(color(CANVAS))
            .font_family(FONT_FAMILY)
            .text_size(px(TEXT_BODY))
            .text_color(color(INK))
            .child(
                div()
                    .flex()
                    .flex_row()
                    .size_full()
                    .min_h_0()
                    .rounded(px(RADIUS))
                    .border_1()
                    .border_color(color(BORDER_STRONG))
                    .bg(color(PAPER))
                    .overflow_hidden()
                    .child(self.sidebar(cx))
                    .child(self.chat_pane(cx)),
            )
    }
}

impl ChatWindow {
    /// `<aside class="sidebar">`: room identity, who you are, rename, presence.
    fn sidebar(&self, cx: &Context<Self>) -> AnyElement {
        div()
            .flex()
            .flex_col()
            .w(px(SIDEBAR_W))
            .flex_shrink_0()
            .h_full()
            .gap(px(16.0))
            .p(px(SIDEBAR_PAD))
            .border_r_1()
            .border_color(color(BORDER_CARD))
            .bg(linear_gradient(
                135.0,
                linear_color_stop(color(SAND_WASH), 0.0),
                linear_color_stop(color(SAND), 0.34),
            ))
            .child(room_card())
            .child(self.identity_card())
            .child(self.name_form(cx))
            .child(self.online_panel())
            .into_any_element()
    }

    /// `<section class="identity-card">`: avatar, "Posting as", display name.
    fn identity_card(&self) -> AnyElement {
        let name = self.poster();

        card()
            .flex_row()
            .items_center()
            .gap(px(IDENTITY_GAP))
            .child(avatar(&name, SELF_AVATAR, RUST, PAPER))
            .child(
                div()
                    .flex()
                    .flex_col()
                    .w(px(IDENTITY_TEXT_W))
                    .child(div().text_color(color(MUTED)).child("Posting as"))
                    .child(
                        div()
                            // Definite, not stretched: see the geometry block.
                            .w(px(IDENTITY_TEXT_W))
                            .font_weight(FontWeight::BOLD)
                            .truncate()
                            .debug_selector(|| "identity-name".into())
                            .child(name),
                    ),
            )
            .into_any_element()
    }

    /// `<form class="name-form">`: the rename draft plus its submit button.
    fn name_form(&self, cx: &Context<Self>) -> AnyElement {
        div()
            .flex()
            .flex_row()
            .gap(px(8.0))
            .child(
                div()
                    .flex_1()
                    .debug_selector(|| "name-input".into())
                    .child(Input::new(&self.name_input).large()),
            )
            .child(
                Button::new("rename")
                    .primary()
                    .large()
                    .h(px(CONTROL_H))
                    .label(if self.busy == Some(Pending::Rename) {
                        "Saving"
                    } else {
                        "Rename"
                    })
                    .disabled(self.busy.is_some())
                    .on_click(cx.listener(|this, _event, window, cx| {
                        this.set_name(window, cx);
                    })),
            )
            .debug_selector(|| "rename-button".into())
            .into_any_element()
    }

    /// `assign_async :online_users` — the same three-arm `AsyncResult` match as
    /// the message list, on a field that is a plain list rather than a stream.
    /// PubSub keeps it current: a rename in the browser client moves a row
    /// here.
    fn online_panel(&self) -> AnyElement {
        let status = self.snapshot.as_deref().map(|state| &state.online_users);

        // `.status-dot` and its two overrides. `waiting` shares the default
        // rust dot with `failed`, exactly as the browser class does.
        let dot = match status {
            Some(AsyncResult::Ok { .. }) => TEAL,
            Some(AsyncResult::Loading { .. }) => GOLD,
            _ => RUST,
        };

        let panel = card().flex_col().child(
            div()
                .flex()
                .flex_row()
                .items_center()
                .justify_between()
                .mb(px(12.8))
                .child(heading("Online"))
                .child(status_dot(dot)),
        );

        let body = match status {
            None => side_note("Waiting for the first patch"),
            Some(AsyncResult::Loading { result: None, .. }) => side_note("Loading presence"),
            Some(AsyncResult::Failed {
                result: None,
                reason,
            }) => error_note("Presence unavailable", reason_text(reason)),
            // Stale rows, dimmed, beat blanking the panel while a reconnect
            // re-seeds it — the browser client drops straight to the note.
            Some(AsyncResult::Loading { result, .. } | AsyncResult::Failed { result, .. }) => {
                user_rows(result.as_deref().unwrap_or_default(), true)
            }
            Some(AsyncResult::Ok { result, .. }) => user_rows(result, false),
        };

        panel.child(body).into_any_element()
    }

    /// `<section class="chatbox">`: header, message viewport, composer dock.
    fn chat_pane(&self, cx: &Context<Self>) -> AnyElement {
        div()
            .flex()
            .flex_col()
            .flex_1()
            .min_w_0()
            .h_full()
            .child(self.chat_header())
            .child(self.message_list())
            .child(self.composer_dock(cx))
            .into_any_element()
    }

    /// `<header class="chat-header">`: title on the left, activity on the right.
    fn chat_header(&self) -> AnyElement {
        let state = self.snapshot.as_deref();

        let online = match state.map(|state| &state.online_users) {
            Some(AsyncResult::Ok { result, .. }) => result.len(),
            _ => 0,
        };
        let messages = self.messages().len();

        // `.status-dot` again, this time for the history AsyncResult.
        let history = match state.map(|state| &state.messages) {
            Some(AsyncResult::Ok { .. }) => TEAL,
            Some(AsyncResult::Loading { .. }) => GOLD,
            _ => RUST,
        };

        div()
            .flex()
            .flex_row()
            .items_center()
            .justify_between()
            .gap(px(16.0))
            .px(px(18.4))
            .py(px(16.0))
            .border_b_1()
            .border_color(color(BORDER_SOFT))
            .child(
                div()
                    .flex()
                    .flex_col()
                    .child(eyebrow("Live chat"))
                    .child(heading("Chat room")),
            )
            .child(
                div()
                    .flex()
                    .flex_row()
                    .flex_wrap()
                    .justify_end()
                    .items_center()
                    .gap(px(7.2))
                    .child(stat_pill(format!("{online} online"), None))
                    .child(stat_pill(format!("{messages} messages"), None))
                    .child(status_dot(history))
                    .child(self.connection_pill()),
            )
            .into_any_element()
    }

    /// §4.6. The pill flips without the message list blanking, because the
    /// last-good snapshot is kept (BDR-0015).
    ///
    /// `stale` is the *only* reconnect signal v1 has, and it is set by a
    /// command that failed on a dead socket. `Mounted::snapshot()` is never
    /// cleared once the first patch lands — not by a reconnect either — so a
    /// socket that drops while the app is idle keeps saying "live" until the
    /// next command. Fixing that needs a `MountStatus` on the crate; see
    /// `docs/rust-gpui-example.md` open question 1.
    ///
    /// The browser client has no equivalent: `@musubi/react` resubscribes
    /// silently, so there is nothing there to port.
    fn connection_pill(&self) -> AnyElement {
        let (label, tint) = self.connection_state();

        stat_pill(label, Some(tint))
            .debug_selector(|| "connection-pill".into())
            .into_any_element()
    }

    /// The pill's copy and tint, split out so it can be asserted on directly.
    pub fn connection_state(&self) -> (&'static str, u32) {
        if self.mount_error.is_some() {
            ("offline", RUST)
        } else if self.mounted.is_none() {
            ("connecting", GOLD)
        } else if self.stale {
            ("reconnecting", GOLD)
        } else if self.snapshot.is_some() {
            ("live", TEAL)
        } else {
            ("joining", GOLD)
        }
    }

    /// §4.2 + §4.3. The server's ~1.5 s history-seed delay makes the
    /// `loading -> ok` flip visible on every mount *and* every reconnect, which
    /// is exactly the state a native client has to get right.
    fn message_list(&self) -> AnyElement {
        let viewport = div()
            .flex()
            .flex_col()
            .flex_1()
            .min_h_0()
            .p(px(19.2))
            .debug_selector(|| "message-list".into());

        // Before the first patch there is nothing to render but the mount
        // itself — including its failure, which is a panel rather than a
        // silent exit because the window is already open by then.
        let Some(state) = self.snapshot.clone() else {
            let panel = match &self.mount_error {
                Some(reason) => error_panel("Could not join the room.", reason.clone()),
                None => empty_panel("…", format!("Connecting to {}", self.url)),
            };

            return viewport.child(panel).into_any_element();
        };

        // Field names are the *wire* names, and every variant carries both
        // `result` and `reason` (`docs/rust-client.md` §6.1) — hence the `..`.
        // The `Loading { result: Some(_) }` arm is the one that matters on
        // reconnect: the async value goes back to `loading` while still
        // carrying the previous payload, and rendering those rows dimmed beats
        // blanking the list.
        let body = match &state.messages {
            AsyncResult::Loading { result: None, .. } => empty_panel("…", "Loading history"),
            AsyncResult::Failed {
                result: None,
                reason,
            } => error_panel("Could not load history.", reason_text(reason)),
            AsyncResult::Ok { result, .. } if result.is_empty() => {
                empty_panel("+", "No messages yet.")
            }
            AsyncResult::Loading { .. } | AsyncResult::Failed { .. } => self.rows(&state, true),
            AsyncResult::Ok { .. } => self.rows(&state, false),
        };

        viewport.child(body).into_any_element()
    }

    /// `<ol class="messages">`, virtualized.
    ///
    /// `gpui::list` rather than `uniform_list` because bubbles wrap: the row
    /// height depends on the body, so there is no single height to measure
    /// once (`docs/rust-gpui-example.md` §4.2 named this swap).
    fn rows(&self, state: &Arc<State>, dimmed: bool) -> AnyElement {
        let state = Arc::clone(state);

        list(self.messages.clone(), move |index, _window, _cx| {
            message_row(&state, index, dimmed)
        })
        .flex_1()
        .into_any_element()
    }

    /// `<footer class="composer-dock">`: delivery line, then the composer.
    fn composer_dock(&self, cx: &Context<Self>) -> AnyElement {
        div()
            .flex()
            .flex_col()
            .px(px(16.0))
            .pt(px(13.6))
            .pb(px(16.0))
            .border_t_1()
            .border_color(color(BORDER_SOFT))
            .bg(color(DOCK))
            .child(
                div()
                    .min_h(px(20.0))
                    .mb(px(7.2))
                    .text_size(px(TEXT_STATUS))
                    .text_color(color(MUTED))
                    .child(self.send_state()),
            )
            .child(
                div()
                    .flex()
                    .flex_row()
                    .gap(px(8.8))
                    .child(
                        div()
                            .flex_1()
                            .debug_selector(|| "composer".into())
                            .child(Input::new(&self.composer).large()),
                    )
                    .child(
                        Button::new("send")
                            .primary()
                            .large()
                            .h(px(CONTROL_H))
                            .label(if self.busy == Some(Pending::Send) {
                                "Sending"
                            } else {
                                "Send"
                            })
                            .disabled(self.busy.is_some())
                            .on_click(cx.listener(|this, _event, window, cx| {
                                this.send_message(window, cx);
                            }))
                            .debug_selector(|| "send-button".into()),
                    ),
            )
            .into_any_element()
    }

    /// §4.5. `last_send_status` is written only by `handle_async/3`, so this
    /// line flips on a *second*, independent patch with no command reply
    /// attached — the tail of the
    /// command → reply → patch → async-completion → patch sequence the reply
    /// feedback starts.
    ///
    /// Rust is nominal, so the three-arm union in `state do` is hoisted to a
    /// named enum and this `match` is exhaustive by compiler force. The
    /// TypeScript bundle writes the same union inline, and `App.tsx` renders
    /// exactly these three strings — with the same `feedback ||` precedence.
    fn send_state(&self) -> SharedString {
        if !self.feedback.is_empty() {
            return self.feedback.clone();
        }

        match self
            .snapshot
            .as_deref()
            .map(|state| &state.last_send_status)
        {
            None | Some(SendStatus::Idle) => "idle".into(),
            Some(SendStatus::Ok { id }) => format!("ok ({id})").into(),
            Some(SendStatus::Failed { reason }) => format!("failed ({reason})").into(),
        }
    }
}

// -----------------------------------------------------------------------------
// Shared bits of chrome
// -----------------------------------------------------------------------------

/// `<div class="room-card">`: the `#` mark beside the room's name.
fn room_card() -> AnyElement {
    card()
        .flex_row()
        .items_center()
        .gap(px(14.4))
        .p(px(16.0))
        .child(mark("#"))
        .child(
            div().flex().flex_col().child(eyebrow("Room")).child(
                div()
                    .text_size(px(TEXT_H1))
                    .line_height(px(TEXT_H1))
                    .font_weight(FontWeight::BOLD)
                    .child(ROOM_ID),
            ),
        )
        .into_any_element()
}

/// The `.room-card / .identity-card / .presence-panel` shell: one fixed-width
/// tinted card with a hairline border.
fn card() -> Div {
    div()
        .flex()
        .w(px(CARD_W))
        .flex_shrink_0()
        .p(px(CARD_PAD))
        .rounded(px(RADIUS))
        .border_1()
        .border_color(color(BORDER_CARD))
        .bg(color(CARD))
}

/// `.room-mark` / `.empty-mark`: a teal square holding one glyph.
fn mark(glyph: impl Into<SharedString>) -> AnyElement {
    div()
        .flex()
        .items_center()
        .justify_center()
        .size(px(MARK))
        .flex_shrink_0()
        .rounded(px(RADIUS))
        .bg(color(TEAL))
        .text_color(color(PAPER))
        .text_size(px(TEXT_MARK))
        .font_weight(FontWeight::BLACK)
        .child(glyph.into())
        .into_any_element()
}

/// `.avatar` / `.self-avatar`: a tinted square of initials.
fn avatar(name: &str, size: f32, background: u32, foreground: u32) -> AnyElement {
    div()
        .flex()
        .items_center()
        .justify_center()
        .size(px(size))
        .flex_shrink_0()
        .rounded(px(RADIUS))
        .bg(color(background))
        .text_color(color(foreground))
        .text_size(px(12.48))
        .font_weight(FontWeight::BLACK)
        .child(initials(name))
        .into_any_element()
}

/// `.eyebrow`: an uppercase, rust-colored section label. gpui has no
/// `text-transform`, so the case change happens here.
fn eyebrow(text: &str) -> AnyElement {
    div()
        .mb(px(3.2))
        .text_size(px(TEXT_EYEBROW))
        .font_weight(FontWeight::BLACK)
        .text_color(color(EYEBROW))
        .child(text.to_uppercase())
        .into_any_element()
}

/// `h2`.
fn heading(text: impl Into<SharedString>) -> AnyElement {
    div()
        .text_size(px(TEXT_H2))
        .line_height(px(TEXT_H2))
        .font_weight(FontWeight::BOLD)
        .child(text.into())
        .into_any_element()
}

/// `.chat-stats span`: a bordered capsule. `tint` colors the text and border
/// for the connection pill; the activity counters take the ink default.
fn stat_pill(text: impl Into<SharedString>, tint: Option<u32>) -> Div {
    div()
        .px(px(10.4))
        .py(px(6.4))
        .rounded_full()
        .border_1()
        .border_color(color(tint.unwrap_or(BORDER_CARD)))
        .bg(color(STAT))
        .text_size(px(TEXT_STAT))
        .font_weight(FontWeight::EXTRA_BOLD)
        .text_color(color(tint.unwrap_or(INK)))
        .flex_shrink_0()
        .child(text.into())
}

/// `.status-dot`: a 12 px disc, tinted by the `AsyncResult` it reports.
fn status_dot(tint: u32) -> AnyElement {
    div()
        .size(px(12.0))
        .flex_shrink_0()
        .rounded_full()
        .bg(color(tint))
        .into_any_element()
}

/// `.side-note`: the presence panel's one-line fallback.
fn side_note(text: impl Into<SharedString>) -> AnyElement {
    div()
        .text_color(color(MUTED))
        .child(text.into())
        .into_any_element()
}

/// A `.side-note` with the verbatim cause under it. The browser client shows
/// only the headline; the reason is worth keeping in a client you debug with.
fn error_note(text: impl Into<SharedString>, detail: impl Into<SharedString>) -> AnyElement {
    div()
        .flex()
        .flex_col()
        .child(div().text_color(color(RUST)).child(text.into()))
        .child(
            div()
                .text_size(px(TEXT_SMALL))
                .text_color(color(MUTED))
                .child(detail.into()),
        )
        .into_any_element()
}

/// `.empty-state`: a centered mark over one line of copy.
fn empty_panel(glyph: &'static str, text: impl Into<SharedString>) -> AnyElement {
    div()
        .flex()
        .flex_1()
        .flex_col()
        .items_center()
        .justify_center()
        .gap(px(12.8))
        .text_color(color(EMPTY))
        .child(mark(glyph))
        .child(div().child(text.into()))
        .into_any_element()
}

/// `.empty-state` for a failure, with the verbatim cause underneath.
fn error_panel(text: impl Into<SharedString>, detail: impl Into<SharedString>) -> AnyElement {
    div()
        .flex()
        .flex_1()
        .flex_col()
        .items_center()
        .justify_center()
        .gap(px(12.8))
        .text_color(color(EMPTY))
        .debug_selector(|| "error-panel".into())
        .child(mark("!"))
        .child(div().text_color(color(RUST)).child(text.into()))
        .child(
            div()
                .text_size(px(TEXT_SMALL))
                .text_color(color(MUTED))
                .child(detail.into()),
        )
        .into_any_element()
}

/// `<ul class="users">`. Short enough not to need virtualizing, capped and
/// scrollable like the browser client's `max-height`.
fn user_rows(users: &[OnlineUser], dimmed: bool) -> AnyElement {
    div()
        .id("online-users")
        .flex()
        .flex_col()
        .gap(px(8.8))
        .max_h(px(USERS_MAX_H))
        .overflow_y_scroll()
        .when(dimmed, |column| column.opacity(0.55))
        .children(users.iter().map(|user| {
            div()
                .flex()
                .flex_row()
                .items_center()
                .gap(px(USER_GAP))
                .p(px(USER_PAD))
                .rounded(px(RADIUS))
                .bg(color(ROW))
                .child(avatar(&user.name, AVATAR, GOLD, INK))
                .child(
                    div()
                        .flex()
                        .flex_col()
                        .w(px(USER_TEXT_W))
                        .child(
                            div()
                                // Definite, not stretched: see the geometry
                                // block at the top of this file.
                                .w(px(USER_TEXT_W))
                                .font_weight(FontWeight::BOLD)
                                .truncate()
                                .child(user.name.clone()),
                        )
                        .child(
                            div()
                                .w(px(USER_TEXT_W))
                                .text_size(px(TEXT_SMALL))
                                .text_color(color(MUTED))
                                .truncate()
                                .child(user.id.clone()),
                        ),
                )
        }))
        .into_any_element()
}

/// `<li class="message">`: avatar plus bubble, mirrored for your own messages.
///
/// Bodies wrap (`.bubble p { overflow-wrap: anywhere }`), so nothing here
/// truncates and the row height is whatever the text needs.
fn message_row(state: &State, index: usize, dimmed: bool) -> AnyElement {
    let messages = stale_or_fresh(&state.messages);

    let Some(message) = messages.get(index) else {
        return div().into_any_element();
    };

    let from_self = message.sender == state.current_user.name;
    let (bubble_bg, bubble_fg, id_fg) = if from_self {
        (TEAL, PAPER, ON_TEAL_MUTED)
    } else {
        (BUBBLE, INK, MUTED)
    };

    let bubble = div()
        .flex()
        .flex_col()
        .min_w_0()
        .px(px(14.4))
        .py(px(12.48))
        .rounded(px(RADIUS))
        // `border-radius: 8px 8px 8px 2px` — the tail corner points at the
        // avatar, so it swaps sides with the row.
        .when(from_self, |bubble| bubble.rounded_br(px(2.0)))
        .when(!from_self, |bubble| bubble.rounded_bl(px(2.0)))
        .border_1()
        .border_color(color(BORDER_SOFT))
        .bg(color(bubble_bg))
        .text_color(color(bubble_fg))
        .child(
            div()
                .flex()
                .flex_row()
                .items_center()
                .justify_between()
                .gap(px(16.0))
                .child(
                    div()
                        .font_weight(FontWeight::BOLD)
                        .child(message.sender.clone()),
                )
                .child(
                    div()
                        .text_size(px(TEXT_SMALL))
                        .text_color(color(id_fg))
                        .child(short_message_id(&message.id)),
                ),
        )
        .child(
            div()
                .mt(px(7.2))
                .line_height(px(BODY_LINE_HEIGHT))
                .child(message.body.clone()),
        );

    let row = div()
        .flex()
        .flex_row()
        .items_end()
        .gap(px(USER_GAP))
        .max_w(relative(0.92))
        .when(from_self, |row| row.flex_row_reverse())
        .child(avatar(&message.sender, AVATAR, GOLD, INK))
        .child(bubble);

    // `.messages { gap: 0.9rem }` lives on the row because `list` owns the
    // spacing between its children.
    div()
        .flex()
        .flex_row()
        .w_full()
        .pb(px(14.4))
        .when(from_self, |wrapper| wrapper.justify_end())
        .when(dimmed, |wrapper| wrapper.opacity(0.55))
        .child(row)
        .into_any_element()
}

/// `initials()` from `App.tsx`: the first letter of up to two words, uppercased.
fn initials(name: &str) -> SharedString {
    let letters: String = name
        .split_whitespace()
        .take(2)
        .filter_map(|part| part.chars().next())
        .flat_map(char::to_uppercase)
        .collect();

    if letters.is_empty() {
        "?".into()
    } else {
        letters.into()
    }
}

/// `shortMessageId()` from `App.tsx`: the last ten characters, or all of them.
fn short_message_id(id: &str) -> SharedString {
    let chars = id.chars().count();

    if chars > 10 {
        id.chars().skip(chars - 10).collect::<String>().into()
    } else {
        id.to_owned().into()
    }
}

/// The payload an [`AsyncResult`] currently has, whatever its status: the
/// resolved value when it is `ok`, the preserved previous value while it is
/// `loading` or `failed`, and nothing when there has never been one.
fn stale_or_fresh<T>(value: &AsyncResult<Vec<T>>) -> &[T] {
    match value {
        AsyncResult::Loading { result, .. } | AsyncResult::Failed { result, .. } => {
            result.as_deref().unwrap_or_default()
        }
        AsyncResult::Ok { result, .. } => result,
    }
}

/// Renders an [`AsyncError`] for a human.
fn reason_text(reason: &Option<AsyncError>) -> String {
    match reason {
        None => "no reason reported".to_owned(),
        Some(AsyncError::Structured { kind, value }) => format!("{kind:?}: {value}"),
        Some(AsyncError::Opaque(value)) => value.to_string(),
    }
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

/// UI tests over gpui's in-process test platform.
///
/// Nothing here opens a real window, dials a real socket or needs an
/// accessibility grant: `#[gpui::test]` builds a `TestAppContext` whose
/// `BackgroundExecutor` is a deterministic single-threaded dispatcher, so the
/// *real* [`GpuiSpawner`](crate::transport::GpuiSpawner) and
/// [`GpuiTimer`](crate::transport::GpuiTimer) are used as-is and
/// `run_until_parked()` is the only synchronization primitive needed. Only the
/// [`Connector`] seam is swapped, for a pair of in-memory channels.
///
/// The frames are the ones the real server sends, captured off the browser
/// client's websocket.
#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::VecDeque;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::sync::Mutex;
    use std::task::{Context as TaskContext, Poll};

    use futures::channel::mpsc::{self, UnboundedReceiver, UnboundedSender};
    use futures::future::BoxFuture;
    use futures::{FutureExt, Sink, Stream, StreamExt};
    use gpui::{Entity, Modifiers, Point, TestAppContext, VisualTestContext};
    use gpui_component::Root;
    use musubi_client::{Connector, Frame, Socket, TransportError};
    use serde_json::{Value, json};

    use super::*;
    use crate::transport::{GpuiSpawner, GpuiTimer};

    /// The topic `Connection::mount` derives from the module and the id.
    const TOPIC: &str = "musubi:connection:ChatRoom.Stores.ChatRoomStore:general";
    /// The `root_id` every patch envelope is addressed to.
    const ROOT: &str = "ChatRoom.Stores.ChatRoomStore:general";
    /// Who the canned snapshot says you are.
    const ME: &str = "Ada Lovelace";

    // -------------------------------------------------------------------------
    // The scripted transport
    // -------------------------------------------------------------------------

    /// One serializer-v2 five-tuple, `[join_ref, ref, topic, event, payload]`.
    ///
    /// Spelled out rather than imported: `phoenix-channel` is one layer below
    /// the seams an embedder is meant to know about, and the whole point of
    /// this example is that it depends on `musubi-client` alone.
    #[derive(Debug, Clone)]
    struct Wire {
        join_ref: Option<String>,
        msg_ref: Option<String>,
        topic: String,
        event: String,
        payload: Value,
    }

    impl Wire {
        fn decode(frame: &Frame) -> Self {
            let Frame::Text(text) = frame else {
                panic!("the client only sends text frames");
            };

            let tuple: (Option<String>, Option<String>, String, String, Value) =
                serde_json::from_str(text).expect("client frames are five-tuples");

            Self {
                join_ref: tuple.0,
                msg_ref: tuple.1,
                topic: tuple.2,
                event: tuple.3,
                payload: tuple.4,
            }
        }

        fn frame(&self) -> Frame {
            Frame::Text(
                json!([
                    self.join_ref,
                    self.msg_ref,
                    self.topic,
                    self.event,
                    self.payload
                ])
                .to_string(),
            )
        }
    }

    /// The client half of a scripted socket: two unbounded channels.
    struct TestSocket {
        inbound: UnboundedReceiver<Result<Frame, TransportError>>,
        outbound: UnboundedSender<Frame>,
    }

    impl Stream for TestSocket {
        type Item = Result<Frame, TransportError>;

        fn poll_next(
            mut self: Pin<&mut Self>,
            cx: &mut TaskContext<'_>,
        ) -> Poll<Option<Self::Item>> {
            self.inbound.poll_next_unpin(cx)
        }
    }

    impl Sink<Frame> for TestSocket {
        type Error = TransportError;

        fn poll_ready(
            self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, frame: Frame) -> Result<(), Self::Error> {
            self.outbound
                .unbounded_send(frame)
                .map_err(|_| TransportError::Closed)
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    /// Hands out the sockets a test queued, one per connect attempt. A
    /// reconnect with nothing queued fails, which is what keeps a dropped
    /// socket dropped.
    struct TestConnector(Arc<Mutex<VecDeque<TestSocket>>>);

    impl Connector for TestConnector {
        fn connect(
            &self,
            _url: &str,
        ) -> BoxFuture<'static, Result<Box<dyn Socket>, TransportError>> {
            let next = self.0.lock().unwrap().pop_front();

            async move {
                next.map(|socket| Box::new(socket) as Box<dyn Socket>)
                    .ok_or_else(|| TransportError::connect("no socket queued"))
            }
            .boxed()
        }
    }

    /// The server side of the wire: what the client wrote, and a sender for
    /// what it should read next.
    struct Server {
        sockets: Arc<Mutex<VecDeque<TestSocket>>>,
        to_client: Option<UnboundedSender<Result<Frame, TransportError>>>,
        from_client: UnboundedReceiver<Frame>,
    }

    impl Server {
        /// Builds the connector and the first socket's server end.
        fn new() -> (TestConnector, Self) {
            let sockets = Arc::new(Mutex::new(VecDeque::new()));
            let mut server = Self {
                sockets: Arc::clone(&sockets),
                to_client: None,
                from_client: mpsc::unbounded().1,
            };

            server.queue_socket();

            (TestConnector(sockets), server)
        }

        /// Queues one more socket and takes over its server end.
        fn queue_socket(&mut self) {
            let (to_client, inbound) = mpsc::unbounded();
            let (outbound, from_client) = mpsc::unbounded();

            self.sockets
                .lock()
                .unwrap()
                .push_back(TestSocket { inbound, outbound });

            self.to_client = Some(to_client);
            self.from_client = from_client;
        }

        /// Everything the client wrote since the last call.
        fn sent(&mut self) -> Vec<Wire> {
            let mut frames = Vec::new();

            while let Ok(frame) = self.from_client.try_recv() {
                frames.push(Wire::decode(&frame));
            }

            frames
        }

        /// The one frame the client wrote for `event`, panicking otherwise.
        fn only(&mut self, event: &str) -> Wire {
            let mut matching: Vec<Wire> = self
                .sent()
                .into_iter()
                .filter(|wire| wire.event == event)
                .collect();

            assert_eq!(matching.len(), 1, "expected exactly one {event} frame");

            matching.remove(0)
        }

        fn push(&self, wire: Wire) {
            if let Some(to_client) = &self.to_client {
                let _ = to_client.unbounded_send(Ok(wire.frame()));
            }
        }

        fn reply(&self, to: &Wire, status: &str, response: Value) {
            self.push(Wire {
                join_ref: to.join_ref.clone(),
                msg_ref: to.msg_ref.clone(),
                topic: to.topic.clone(),
                event: "phx_reply".to_owned(),
                payload: json!({"status": status, "response": response}),
            });
        }

        fn push_patch(&self, join: &Wire, envelope: Value) {
            self.push(Wire {
                join_ref: join.msg_ref.clone(),
                msg_ref: None,
                topic: join.topic.clone(),
                event: "patch".to_owned(),
                payload: envelope,
            });
        }

        /// Ends the inbound stream, which is how a transport reports a drop.
        fn disconnect(&mut self) {
            self.to_client = None;
        }
    }

    /// The initial `v1` envelope: two messages already seeded, two users online.
    ///
    /// Shape copied from a live `mix server` frame — `stream_async` renders the
    /// stream marker *inside* the `AsyncResult`'s `result`, and the rows arrive
    /// as `stream_ops`, never as JSON-patch ops.
    fn initial_envelope() -> Value {
        json!({
            "type": "patch",
            "root_id": ROOT,
            "base_version": 0,
            "version": 1,
            "ops": [{
                "op": "replace",
                "path": "",
                "value": {
                    "__musubi_store_id__": [],
                    "current_user": {"id": "user-7", "name": ME},
                    "last_send_status": {"type": "idle"},
                    "messages": {
                        "__musubi_async__": true,
                        "status": "ok",
                        "result": {"__musubi_stream__": "messages"},
                        "reason": null
                    },
                    "online_users": {
                        "__musubi_async__": true,
                        "status": "ok",
                        "result": [
                            {"id": "user-7", "name": ME},
                            {"id": "user-9", "name": "Grace Hopper"}
                        ],
                        "reason": null
                    }
                }
            }],
            "stream_ops": [
                {"op": "reset", "stream": "messages", "ref": "0", "store_id": []},
                {
                    "op": "insert", "stream": "messages", "ref": "0", "store_id": [],
                    "item_key": "msg-msg-1", "at": -1, "limit": null,
                    "item": {"id": "msg-1", "body": "first", "sender": "Grace Hopper"}
                },
                {
                    "op": "insert", "stream": "messages", "ref": "0", "store_id": [],
                    "item_key": "msg-msg-2", "at": -1, "limit": null,
                    "item": {"id": "msg-2", "body": "second", "sender": ME}
                }
            ],
            "upload_ops": [],
            "events": []
        })
    }

    // -------------------------------------------------------------------------
    // Rig
    // -------------------------------------------------------------------------

    /// Opens the real window tree — `Root` on the outside, or gpui-component
    /// panics looking for it — over a scripted socket.
    fn boot(cx: &mut TestAppContext) -> (Server, Entity<ChatWindow>, &mut VisualTestContext) {
        cx.update(|cx| {
            gpui_component::init(cx);
            crate::theme::apply_paper_theme(cx);
        });

        let (connector, server) = Server::new();
        let executor = cx.executor();
        let connection = Connection::builder()
            .url("ws://test.invalid/socket")
            .connector(connector)
            .spawner(GpuiSpawner(executor.clone()))
            .timer(GpuiTimer(executor))
            .build()
            .expect("every connection seam is supplied above");

        let chat: Rc<RefCell<Option<Entity<ChatWindow>>>> = Rc::new(RefCell::new(None));
        let slot = Rc::clone(&chat);

        let (_root, cx) = cx.add_window_view(move |window, cx| {
            let view = cx.new(|cx| {
                ChatWindow::new(connection, "ws://test.invalid/socket".into(), window, cx)
            });
            *slot.borrow_mut() = Some(view.clone());

            Root::new(view, window, cx)
        });

        let chat = chat.borrow_mut().take().expect("the view was built");

        (server, chat, cx)
    }

    /// Answers the join and seeds the initial patch. Returns the join frame,
    /// which every later server push has to echo the `join_ref` of.
    fn mount(server: &mut Server, cx: &mut VisualTestContext) -> Wire {
        cx.run_until_parked();

        let join = server.only("phx_join");
        assert_eq!(join.topic, TOPIC);
        assert_eq!(join.payload["params"], json!({"room_id": "general"}));

        server.reply(&join, "ok", json!({"root_id": ROOT}));
        cx.run_until_parked();
        server.push_patch(&join, initial_envelope());
        cx.run_until_parked();

        join
    }

    /// The center of a `debug_selector`ed element, for `simulate_click`.
    fn center(cx: &mut VisualTestContext, selector: &'static str) -> Point<gpui::Pixels> {
        cx.debug_bounds(selector)
            .unwrap_or_else(|| panic!("{selector} is on screen"))
            .center()
    }

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    /// A successful mount paints the snapshot, and the identity label keeps the
    /// definite width that makes `truncate()` produce an ellipsis instead of
    /// collapsing the whole name to one (see the geometry block above).
    #[gpui::test]
    fn a_successful_mount_renders_the_snapshot(cx: &mut TestAppContext) {
        let (mut server, chat, cx) = boot(cx);
        mount(&mut server, cx);

        assert_eq!(chat.update(cx, |chat, _| chat.poster()), ME);
        assert_eq!(chat.update(cx, |chat, _| chat.messages().len()), 2);
        assert_eq!(chat.update(cx, |chat, _| chat.connection_state().0), "live");

        assert!(cx.debug_bounds("message-list").is_some());
        assert!(cx.debug_bounds("error-panel").is_none());
        // The invariant the ellipsis depends on: this element's width is the
        // definite one the geometry block computes, not a stretched or
        // content-derived one. gpui rounds layout to device pixels, hence the
        // tolerance.
        let label = cx
            .debug_bounds("identity-name")
            .expect("the identity label is on screen");

        assert!(
            (f32::from(label.size.width) - IDENTITY_TEXT_W).abs() < 1.0,
            "identity label is {} wide, expected ~{IDENTITY_TEXT_W}",
            label.size.width
        );
    }

    /// A rejected join is a rendered panel, not a silent exit.
    #[gpui::test]
    fn a_rejected_join_renders_the_error_panel(cx: &mut TestAppContext) {
        let (mut server, chat, cx) = boot(cx);
        cx.run_until_parked();

        let join = server.only("phx_join");
        server.reply(&join, "error", json!({"reason": "unauthorized"}));
        cx.run_until_parked();

        assert_eq!(
            chat.update(cx, |chat, _| chat.connection_state().0),
            "offline"
        );
        assert!(cx.debug_bounds("error-panel").is_some());
    }

    /// Typing into the composer and sending dispatches `send_message` with the
    /// typed body; the reply clears the draft.
    ///
    /// The send is a click rather than an Enter keystroke: gpui's test IME
    /// gives a bare `enter` a `key_char` of `"\n"`, and gpui-component's
    /// single-line input calls `cx.propagate()` from its `Enter` action, so the
    /// harness would then type that newline into the field. Both paths land on
    /// [`ChatWindow::send_message`].
    #[gpui::test]
    fn sending_the_composer_dispatches_the_body_and_the_reply_clears_it(cx: &mut TestAppContext) {
        let (mut server, chat, cx) = boot(cx);
        mount(&mut server, cx);

        cx.simulate_input("ship it");
        cx.run_until_parked();

        assert_eq!(
            chat.update(cx, |chat, cx| chat.composer.read(cx).value()),
            "ship it"
        );

        let send = center(cx, "send-button");
        cx.simulate_click(send, Modifiers::none());
        cx.run_until_parked();

        let command = server.only("command");
        assert_eq!(command.topic, TOPIC);
        assert_eq!(command.payload["name"], json!("send_message"));
        assert_eq!(command.payload["payload"], json!({"body": "ship it"}));

        assert_eq!(
            chat.update(cx, |chat, cx| chat.composer.read(cx).value()),
            "ship it",
            "the draft survives until the reply lands"
        );

        server.reply(&command, "ok", json!({"queued": true}));
        cx.run_until_parked();

        assert_eq!(
            chat.update(cx, |chat, cx| chat.composer.read(cx).value()),
            ""
        );
        assert_eq!(
            chat.update(cx, |chat, _| chat.send_state()),
            "Message queued for async delivery."
        );
    }

    /// One command at a time: clicking Send while the first is in flight sends
    /// nothing.
    #[gpui::test]
    fn a_send_while_one_is_in_flight_is_refused(cx: &mut TestAppContext) {
        let (mut server, chat, cx) = boot(cx);
        mount(&mut server, cx);

        cx.simulate_input("first");
        let send = center(cx, "send-button");
        cx.simulate_click(send, Modifiers::none());
        cx.run_until_parked();

        let command = server.only("command");
        assert_eq!(command.payload["payload"], json!({"body": "first"}));

        cx.simulate_click(send, Modifiers::none());
        cx.run_until_parked();

        assert!(
            server.sent().is_empty(),
            "the disabled button swallows the second click"
        );

        // The same refusal one layer down, which is the path Enter takes.
        cx.update(|window, cx| chat.update(cx, |chat, cx| chat.send_message(window, cx)));
        cx.run_until_parked();

        assert!(
            server.sent().is_empty(),
            "the in-flight guard refuses a second dispatch"
        );

        server.reply(&command, "ok", json!({"queued": true}));
        cx.run_until_parked();

        assert!(chat.update(cx, |chat, _| chat.busy.is_none()));
    }

    /// A socket that goes away flips the pill on the next failed command, and
    /// the last good snapshot keeps rendering (BDR-0015).
    #[gpui::test]
    fn a_dropped_socket_flips_the_pill_but_keeps_the_rows(cx: &mut TestAppContext) {
        let (mut server, chat, cx) = boot(cx);
        mount(&mut server, cx);

        assert_eq!(chat.update(cx, |chat, _| chat.connection_state().0), "live");

        server.disconnect();
        cx.run_until_parked();

        cx.simulate_input("into the void");
        let send = center(cx, "send-button");
        cx.simulate_click(send, Modifiers::none());
        cx.run_until_parked();

        assert_eq!(
            chat.update(cx, |chat, _| chat.connection_state().0),
            "reconnecting"
        );
        assert_eq!(
            chat.update(cx, |chat, _| chat.messages().len()),
            2,
            "the last good snapshot is kept"
        );
        assert!(cx.debug_bounds("message-list").is_some());
        assert!(cx.debug_bounds("connection-pill").is_some());
    }
}
