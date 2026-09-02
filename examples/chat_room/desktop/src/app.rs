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

use std::ops::Range;
use std::sync::Arc;

use futures::StreamExt;
use gpui::{
    AnyElement, App, AppContext, Context, Entity, FontWeight, Hsla, InteractiveElement,
    IntoElement, ParentElement, Render, SharedString, StatefulInteractiveElement, Styled,
    Subscription, Task, Window, div, px, uniform_list,
};
// `when` — the conditional-builder combinator gpui blanket-implements for
// every element.
use gpui::prelude::FluentBuilder;
use gpui_component::button::{Button, ButtonVariants};
use gpui_component::input::{Input, InputEvent, InputState};
use gpui_component::spinner::Spinner;
use gpui_component::{ActiveTheme, Disableable, Sizable};
use musubi_client::{Connection, Mounted, MusubiError};
use serde_json::json;

use crate::generated::chat_room::stores::chat_room_store::{
    ChatRoomStore, ChatRoomStoreLastSendStatus as SendStatus, SendMessage, SetName, State,
};
use crate::generated::chat_room::{MessageState, OnlineUser};
use crate::generated::musubi::{AsyncError, AsyncResult, Command};

/// The room every client of this example joins — the same one `ui/` mounts, so
/// the browser and the native window share a presence list and a message
/// stream.
const ROOM_ID: &str = "general";

/// `uniform_list` virtualizes on a fixed row height, so bodies truncate rather
/// than wrap. The variable-height upgrade is `gpui::list` + `ListState`, a swap
/// of one element (`docs/rust-gpui-example.md` §4.2).
const ROW_HEIGHT: f32 = 44.0;

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
    /// The one-line reply/receipt channel. Written by command replies only.
    feedback: SharedString,
    /// One command at a time: both buttons read it, and `_in_flight` holds
    /// exactly one task.
    busy: bool,
    composer: Entity<InputState>,
    name_input: Entity<InputState>,
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
        let updates = cx.spawn(async move |this, cx| {
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
                .update(cx, |view, cx| {
                    view.snapshot = initial;
                    view.mounted = Some(mounted);
                    cx.notify();
                })
                .is_err()
            {
                return;
            }

            while let Some(snapshot) = updates.next().await {
                // A closed window is a normal exit, not an error.
                let alive = this.update(cx, |view, cx| {
                    view.snapshot = Some(snapshot);
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
            busy: false,
            composer,
            name_input,
            _updates: updates,
            _in_flight: None,
            _subscriptions: subscriptions,
        }
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
            ("sending", "Message body cannot be empty.", "Message send"),
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
            ("renaming", "Name cannot be empty.", "Name update"),
            |name| SetName { name },
            |reply| format!("Name updated to {}.", reply.name).into(),
        );
    }

    /// The body both commands share: refuse when unmounted or busy, trim the
    /// field, dispatch, then write the reply into `feedback` and clear the
    /// draft — the screen renders from the snapshot, never from a reply. The
    /// three strings are the busy feedback, the empty refusal, and the failure
    /// label.
    fn dispatch<C: Command<ChatRoomStore>>(
        &mut self,
        window: &mut Window,
        cx: &mut Context<Self>,
        input: Entity<InputState>,
        (pending, empty, label): (&str, &str, &'static str),
        cmd_of: impl FnOnce(String) -> C,
        on_reply: impl FnOnce(C::Reply) -> SharedString + 'static,
    ) {
        let Some(mounted) = self.mounted.clone() else {
            return self.reject("not connected yet", cx);
        };

        if self.busy {
            return;
        }

        let value = input.read(cx).value().trim().to_owned();

        if value.is_empty() {
            return self.reject(empty, cx);
        }

        self.busy = true;
        self.feedback = pending.to_owned().into();
        cx.notify();

        let command = cmd_of(value);

        self._in_flight = Some(cx.spawn_in(window, async move |this, cx| {
            let result = mounted.command(command).await;

            this.update_in(cx, |view, window, cx| {
                view.busy = false;

                match result {
                    Ok(reply) => {
                        view.feedback = on_reply(reply);
                        input.update(cx, |state, cx| state.set_value("", window, cx));
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
    /// newest-first: no reversal, no scroll-to-bottom bookkeeping.
    fn messages(&self) -> &[MessageState] {
        self.snapshot
            .as_deref()
            .map(|state| stale_or_fresh(&state.messages))
            .unwrap_or_default()
    }
}

// -----------------------------------------------------------------------------
// Render
// -----------------------------------------------------------------------------

impl Render for ChatWindow {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .flex()
            .flex_col()
            .size_full()
            .bg(cx.theme().background)
            .text_color(cx.theme().foreground)
            .text_sm()
            .child(self.header(cx))
            .child(
                div()
                    .flex()
                    .flex_row()
                    .flex_1()
                    .min_h_0()
                    .child(self.sidebar(cx))
                    .child(self.chat_pane(cx)),
            )
    }
}

impl ChatWindow {
    /// Room identity on the left, connection pill on the right.
    fn header(&self, cx: &Context<Self>) -> AnyElement {
        div()
            .flex()
            .flex_row()
            .items_center()
            .justify_between()
            .h(px(60.0))
            .px_5()
            .border_b_1()
            .border_color(cx.theme().border)
            .child(
                div().flex().flex_col().child(eyebrow("Room", cx)).child(
                    div()
                        .text_lg()
                        .font_weight(FontWeight::SEMIBOLD)
                        .child(ROOM_ID),
                ),
            )
            .child(self.connection_pill(cx))
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
    fn connection_pill(&self, cx: &Context<Self>) -> AnyElement {
        let theme = cx.theme();

        let (label, color) = if self.mount_error.is_some() {
            ("offline", theme.danger)
        } else if self.mounted.is_none() {
            ("connecting", theme.warning)
        } else if self.stale {
            ("reconnecting", theme.warning)
        } else if self.snapshot.is_some() {
            ("live", theme.success)
        } else {
            ("joining", theme.warning)
        };

        pill(label, color)
    }

    /// Identity, rename, and the `assign_async` presence panel.
    fn sidebar(&self, cx: &Context<Self>) -> AnyElement {
        div()
            .flex()
            .flex_col()
            .w(px(260.0))
            .flex_shrink_0()
            .h_full()
            .gap_4()
            .p_4()
            .border_r_1()
            .border_color(cx.theme().border)
            .bg(cx.theme().sidebar)
            .child(
                div()
                    .flex()
                    .flex_col()
                    .min_w_0()
                    .child(eyebrow("Posting as", cx))
                    .child(
                        div().font_weight(FontWeight::SEMIBOLD).truncate().child(
                            self.snapshot
                                .as_deref()
                                .map_or("…", |state| state.current_user.name.as_str())
                                .to_owned(),
                        ),
                    ),
            )
            .child(
                div()
                    .flex()
                    .flex_row()
                    .gap_2()
                    .child(div().flex_1().child(Input::new(&self.name_input).small()))
                    .child(
                        Button::new("rename")
                            .outline()
                            .small()
                            .label("Rename")
                            .disabled(self.busy)
                            .on_click(cx.listener(|this, _event, window, cx| {
                                this.set_name(window, cx);
                            })),
                    ),
            )
            .child(self.online_panel(cx))
            .into_any_element()
    }

    /// `assign_async :online_users` — the same three-arm `AsyncResult` match as
    /// the message list, on a field that is a plain list rather than a stream.
    /// PubSub keeps it current: a rename in the browser client moves a row
    /// here.
    fn online_panel(&self, cx: &Context<Self>) -> AnyElement {
        let status = match self.snapshot.as_deref().map(|state| &state.online_users) {
            None => "waiting",
            Some(AsyncResult::Loading { .. }) => "loading",
            Some(AsyncResult::Ok { .. }) => "ok",
            Some(AsyncResult::Failed { .. }) => "failed",
        };

        let column = div().flex().flex_col().flex_1().min_h_0().gap_2().child(
            div()
                .flex()
                .flex_row()
                .items_center()
                .justify_between()
                .child(eyebrow("Online", cx))
                .child(muted(status, cx)),
        );

        let Some(state) = self.snapshot.as_deref() else {
            return column
                .child(muted("Waiting for the first patch", cx))
                .into_any_element();
        };

        let body = match &state.online_users {
            AsyncResult::Loading { result: None, .. } => loading_panel("Loading presence", cx),
            AsyncResult::Failed {
                result: None,
                reason,
            } => error_panel("Presence unavailable", reason_text(reason), cx),
            AsyncResult::Loading { result, .. } | AsyncResult::Failed { result, .. } => {
                user_rows(result.as_deref().unwrap_or_default(), true, cx)
            }
            AsyncResult::Ok { result, .. } => user_rows(result, false, cx),
        };

        column.child(body).into_any_element()
    }

    /// Message viewport plus the composer dock.
    fn chat_pane(&self, cx: &Context<Self>) -> AnyElement {
        div()
            .flex()
            .flex_col()
            .flex_1()
            .min_w_0()
            .h_full()
            .child(self.message_list(cx))
            .child(self.composer_dock(cx))
            .into_any_element()
    }

    /// §4.2 + §4.3. The server's ~1.5 s history-seed delay makes the
    /// `loading -> ok` flip visible on every mount *and* every reconnect, which
    /// is exactly the state a native client has to get right.
    fn message_list(&self, cx: &Context<Self>) -> AnyElement {
        let viewport = div().flex().flex_col().flex_1().min_h_0().p_4();

        // Before the first patch there is nothing to render but the mount
        // itself — including its failure, which is a panel rather than a
        // silent exit because the window is already open by then.
        let Some(state) = self.snapshot.as_deref() else {
            let panel = match &self.mount_error {
                Some(reason) => error_panel("Could not join the room.", reason.clone(), cx),
                None => loading_panel(format!("Connecting to {}", self.url), cx),
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
            AsyncResult::Loading { result: None, .. } => loading_panel("Loading history", cx),
            AsyncResult::Failed {
                result: None,
                reason,
            } => error_panel("Could not load history.", reason_text(reason), cx),
            AsyncResult::Ok { result, .. } if result.is_empty() => div()
                .flex()
                .flex_1()
                .items_center()
                .justify_center()
                .text_color(cx.theme().muted_foreground)
                .child("No messages yet.")
                .into_any_element(),
            AsyncResult::Loading { .. } | AsyncResult::Failed { .. } => self.rows(true, cx),
            AsyncResult::Ok { .. } => self.rows(false, cx),
        };

        viewport.child(body).into_any_element()
    }

    /// The virtualized list itself. Rows are a fixed 44 px so `uniform_list`
    /// can measure once; long bodies truncate.
    fn rows(&self, dimmed: bool, cx: &Context<Self>) -> AnyElement {
        let count = self.messages().len();

        uniform_list(
            "messages",
            count,
            cx.processor(move |this, range: Range<usize>, _window, _cx| {
                range
                    .map(|index| this.message_row(index, dimmed))
                    .collect::<Vec<_>>()
            }),
        )
        .flex_1()
        .into_any_element()
    }

    /// One row: sender, then body.
    fn message_row(&self, index: usize, dimmed: bool) -> AnyElement {
        let row = div().h(px(ROW_HEIGHT)).flex().flex_col().justify_center();

        let Some(message) = self.messages().get(index) else {
            return row.into_any_element();
        };

        row.when(dimmed, |row| row.opacity(0.55))
            .child(
                div()
                    .font_weight(FontWeight::SEMIBOLD)
                    .text_xs()
                    .child(message.sender.clone()),
            )
            .child(div().truncate().child(message.body.clone()))
            .into_any_element()
    }

    /// Delivery receipt, reply feedback, and the composer.
    fn composer_dock(&self, cx: &Context<Self>) -> AnyElement {
        div()
            .flex()
            .flex_col()
            .gap_2()
            .p_4()
            .border_t_1()
            .border_color(cx.theme().border)
            .child(
                div()
                    .flex()
                    .flex_row()
                    .items_center()
                    .gap_3()
                    .child(self.send_status_pill(cx))
                    .child(muted(self.feedback.clone(), cx)),
            )
            .child(
                div()
                    .flex()
                    .flex_row()
                    .gap_2()
                    .child(div().flex_1().child(Input::new(&self.composer)))
                    .child(
                        Button::new("send")
                            .primary()
                            .label("Send")
                            .disabled(self.busy)
                            .on_click(cx.listener(|this, _event, window, cx| {
                                this.send_message(window, cx);
                            })),
                    ),
            )
            .into_any_element()
    }

    /// §4.5. `last_send_status` is written only by `handle_async/3`, so this
    /// pill flips on a *second*, independent patch with no command reply
    /// attached — the tail of the
    /// command → reply → patch → async-completion → patch sequence the feedback
    /// line above starts.
    ///
    /// Rust is nominal, so the three-arm union in `state do` is hoisted to a
    /// named enum and this `match` is exhaustive by compiler force. The
    /// TypeScript bundle writes the same union inline.
    fn send_status_pill(&self, cx: &Context<Self>) -> AnyElement {
        let theme = cx.theme();

        let Some(state) = self.snapshot.as_deref() else {
            return pill("idle", theme.muted_foreground);
        };

        match &state.last_send_status {
            SendStatus::Idle => pill("idle", theme.muted_foreground),
            SendStatus::Ok { id } => pill(format!("delivered {id}"), theme.success),
            SendStatus::Failed { reason } => pill(format!("failed: {reason}"), theme.danger),
        }
    }
}

// -----------------------------------------------------------------------------
// Shared bits of chrome
// -----------------------------------------------------------------------------

/// A small status capsule, tinted by its subject.
fn pill(text: impl Into<SharedString>, color: Hsla) -> AnyElement {
    div()
        .px_2()
        .py_0p5()
        .rounded_full()
        .border_1()
        .border_color(color)
        .text_color(color)
        .text_xs()
        .flex_shrink_0()
        .child(text.into())
        .into_any_element()
}

/// An uppercase section label.
fn eyebrow(text: impl Into<SharedString>, cx: &App) -> AnyElement {
    div()
        .text_xs()
        .text_color(cx.theme().muted_foreground)
        .child(text.into())
        .into_any_element()
}

/// Secondary body text.
fn muted(text: impl Into<SharedString>, cx: &App) -> AnyElement {
    div()
        .text_xs()
        .text_color(cx.theme().muted_foreground)
        .truncate()
        .child(text.into())
        .into_any_element()
}

/// `AsyncResult::Loading` with nothing to show yet.
fn loading_panel(text: impl Into<SharedString>, cx: &App) -> AnyElement {
    div()
        .flex()
        .flex_1()
        .flex_row()
        .items_center()
        .justify_center()
        .gap_2()
        .text_color(cx.theme().muted_foreground)
        .child(Spinner::new().small())
        .child(text.into())
        .into_any_element()
}

/// A headline plus the verbatim cause underneath it.
fn error_panel(
    text: impl Into<SharedString>,
    detail: impl Into<SharedString>,
    cx: &App,
) -> AnyElement {
    div()
        .flex()
        .flex_1()
        .flex_col()
        .items_center()
        .justify_center()
        .gap_1()
        .child(div().text_color(cx.theme().danger).child(text.into()))
        .child(muted(detail, cx))
        .into_any_element()
}

/// The presence list. Short enough not to need virtualizing.
fn user_rows(users: &[OnlineUser], dimmed: bool, cx: &App) -> AnyElement {
    div()
        .id("online-users")
        .flex()
        .flex_col()
        .flex_1()
        .min_h_0()
        .gap_2()
        .overflow_y_scroll()
        .when(dimmed, |column| column.opacity(0.55))
        .children(users.iter().map(|user| {
            div()
                .flex()
                .flex_col()
                .min_w_0()
                .child(div().text_xs().truncate().child(user.name.clone()))
                .child(
                    div()
                        .text_xs()
                        .text_color(cx.theme().muted_foreground)
                        .truncate()
                        .child(user.id.clone()),
                )
        }))
        .into_any_element()
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
