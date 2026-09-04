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
//! | Attach button | `upload :attachment` in channel mode + `attach` |
//! | Connection pill | `Mounted::status_updates` (BDR-0033) |
//! | Instant relaunch | SWR mount cache (§6.4) over `cache_store::FileCacheStore` |
//!
//! The one rule the whole file is organized around: **state renders from
//! [`Mounted::updates`], never from a command reply.** A reply is not gated on
//! the patch it caused and carries no ordering relationship to it — BDR-0009
//! orders the *server's* frames, not the client's inbox — so replies only ever
//! write the one-line `feedback` string; every field the user reads comes off
//! the snapshot.
//!
//! # Parity with `ui/`
//!
//! The layout, the copy and the palette are ported from `ui/src/App.tsx` and
//! `ui/src/App.css` so the two clients read as one app: sidebar (room card,
//! identity card, rename form, presence panel) beside a chat column (header
//! with activity pills, message bubbles, composer dock). The connection pill is
//! the one piece of chrome the browser client has no equivalent for — it
//! renders the crate's [`MountStatus`] stream (BDR-0033), so an idle
//! disconnect flips it without a command.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures::StreamExt;
use gpui::{
    AnyElement, AppContext, AsyncWindowContext, Context, Div, Entity, FontWeight,
    InteractiveElement, IntoElement, ListAlignment, ListState, ParentElement, PathPromptOptions,
    Render, SharedString, StatefulInteractiveElement, Styled, Subscription, Task, Window, div,
    linear_color_stop, linear_gradient, list, px, relative,
};
// `when` — the conditional-builder combinator gpui blanket-implements for
// every element.
use gpui::prelude::FluentBuilder;
use gpui_component::button::{Button, ButtonVariants};
use gpui_component::input::{Input, InputEvent, InputState};
use gpui_component::{Disableable, Sizable};
use musubi_client::{
    Connection, MountStatus, Mounted, MusubiError, Upload, UploadAccept, UploadEntry, UploadFile,
    UploadHandle,
};

use crate::generated::chat_room::stores::chat_room_store::{
    Attach, AttachReply, ChatRoomStore, ChatRoomStoreLastSendStatus as SendStatus, Params,
    SendMessage, SetName, State,
};
use crate::generated::chat_room::{AttachmentState, MessageState, OnlineUser};
use crate::generated::musubi::{AsyncError, AsyncResult, Command, StoreId};
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

/// `.attach-button { min-height: 34px }` — the composer's secondary control.
const ATTACH_H: f32 = 34.0;

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

/// Sent as `client_type` when the extension is not one of the declared ones.
///
/// `accept` is enforced against the **extension** at preflight and never
/// against the MIME type (BDR-0026), so a wrong guess here cannot reject a
/// file — it only shows up in `Content-Type` when the example serves the blob
/// back.
const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";

/// Which command is in flight. The window allows one at a time, so the button
/// labels ("Sending" / "Saving", as in `App.tsx`) need to know which.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pending {
    Send,
    Rename,
    Attach,
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
    /// The crate's own liveness signal (BDR-0033), fed by
    /// [`Mounted::status_updates`]. `Reconnecting` flips the pill the moment
    /// the client notices a dead socket — idle or not — with no command
    /// involved.
    status: MountStatus,
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
    /// The upload's control plane, taken once the first snapshot names the
    /// slot. `None` until then, which is what refuses an attach before mount.
    upload: Option<Upload>,
    /// The last handle the upload's own updates stream produced. Upload state
    /// is *not* part of the state tree (BDR-0028) — it arrives on a separate
    /// `upload_ops` channel and lands here, so progress repaints without the
    /// message list re-rendering.
    attachment: Option<UploadHandle>,
    /// Held rather than detached: dropping the task cancels the update loop,
    /// which is the right teardown when the window closes. A detached loop
    /// would keep the `Mounted` — and so the server-side page — alive.
    _updates: Task<()>,
    /// Held: the [`Mounted::status_updates`] loop, started once the mount
    /// resolves.
    _status_updates: Option<Task<()>>,
    /// Held: the upload handle's update loop, started with the first snapshot.
    _upload_updates: Option<Task<()>>,
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
            // `attr(:room_id, String.t(), required: true)` on the store is
            // generated as a plain `Params` field, so the required param
            // cannot be forgotten here.
            let mounted = match connection
                .mount::<ChatRoomStore>(
                    ROOM_ID,
                    Params {
                        room_id: ROOM_ID.to_owned(),
                    },
                )
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

            // Both streams are latest-value and open with what the root
            // already holds, so there is no subscribe-before-read window to
            // close here. The two reads below are the synchronous seed for the
            // first paint; the loops would arrive at the same values a tick
            // later.
            let mut updates = mounted.updates();
            let mut statuses = mounted.status_updates();
            let initial = mounted.snapshot();
            let status = mounted.status();

            if this
                .update_in(cx, |view, window, cx| {
                    view.adopt(initial, window, cx);
                    view.status = status;
                    view.mounted = Some(mounted);
                    view.watch_upload(window, cx);
                    // The crate's own liveness stream drives the pill
                    // (BDR-0033): a socket drop or heartbeat timeout flips it
                    // with no command involved, and the rejoin's fresh initial
                    // patch flips it back.
                    view._status_updates = Some(cx.spawn_in(window, async move |this, cx| {
                        while let Some(status) = statuses.next().await {
                            let alive = this.update(cx, |view, cx| {
                                view.status = status;
                                cx.notify();
                            });

                            if alive.is_err() {
                                break;
                            }
                        }
                    }));
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
                    view.watch_upload(window, cx);
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
            status: MountStatus::Connecting,
            mounted: None,
            upload: None,
            attachment: None,
            feedback: "".into(),
            busy: None,
            composer,
            name_input,
            // `Top` because index 0 is the newest message: the "latest" end of
            // this list is its head, not its tail.
            messages: ListState::new(0, ListAlignment::Top, px(200.0)),
            _updates: updates,
            _status_updates: None,
            _upload_updates: None,
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
    /// The reply is `{queued: true}` and says **nothing** about the row. It is
    /// not gated on the patch it caused: BDR-0009 orders the frames the server
    /// writes (reply, then the `"patch"` push, then the `start_async` task's
    /// own patch), but replies and patches reach this client through separate
    /// tasks, so either can be observed first. That is the contract, not a bug
    /// to paper over, so the feedback line says "queued" and the row shows up
    /// when its snapshot does. There is no `command_and_wait_for_patch`
    /// helper, by design.
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

    /// Subscribes to the upload handle, once the first snapshot names it.
    ///
    /// Idempotent: called on every snapshot, it does its work exactly once.
    /// `State::attachment` is an inert [`UploadSlot`](musubi_client::generated::UploadSlot) —
    /// the framework injects it into the render output and it carries only the
    /// declared name, which is the key the live handle is reached by
    /// (`docs/rust-client.md` §10).
    fn watch_upload(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.upload.is_some() {
            return;
        }

        let (Some(mounted), Some(state)) = (self.mounted.clone(), self.snapshot.clone()) else {
            return;
        };

        let upload = mounted.upload(&StoreId::root(), &state.attachment.name);
        // Subscribe before snapshotting: an upload's `updates()` is a queue of
        // per-envelope handles, not a latest-value cell like the two streams
        // the mount path takes, so it does not replay and a progress op
        // landing between these two lines would otherwise be missed.
        let mut updates = upload.updates();

        self.attachment = Some(upload.snapshot());
        self.upload = Some(upload);

        self._upload_updates = Some(cx.spawn_in(window, async move |this, cx| {
            while let Some(handle) = updates.next().await {
                let alive = this.update(cx, |view, cx| {
                    view.attachment = Some(handle);
                    cx.notify();
                });

                if alive.is_err() {
                    break;
                }
            }
        }));
    }

    /// The whole channel-mode upload, end to end.
    ///
    /// `path` is `None` for the button — the native dialog supplies one — and
    /// `Some` when a caller already has the file, which is how the tests drive
    /// this without a modal on screen.
    ///
    /// The three steps after the file is read are the flow `docs/uploads.md`
    /// specifies: [`Upload::select`] preflights and the server signs one token
    /// per accepted entry, [`Upload::start`] joins `musubi_upload:<ref>` and
    /// pushes the bytes as binary frames, and the `attach` command is what
    /// consumes the finished entry server-side. The message row that announces
    /// it arrives afterwards on the ordinary stream — never out of the reply.
    fn attach(&mut self, path: Option<PathBuf>, window: &mut Window, cx: &mut Context<Self>) {
        let Some(mounted) = self.mounted.clone() else {
            return self.reject("not connected yet", cx);
        };

        let Some(upload) = self.upload.clone() else {
            return self.reject("the upload slot has not arrived yet", cx);
        };

        if self.busy.is_some() {
            return;
        }

        self.busy = Some(Pending::Attach);
        cx.notify();

        self._in_flight = Some(cx.spawn_in(window, async move |this, cx| {
            let picked = match path {
                Some(path) => Some(path),
                None => pick_file(cx).await,
            };

            let Some(path) = picked else {
                this.update(cx, |view, cx| {
                    view.busy = None;
                    view.feedback = "Attachment cancelled.".into();
                    cx.notify();
                })
                .ok();

                return;
            };

            // `musubi-client` never touches a filesystem — the embedder reads
            // the file and hands the bytes over — so the read happens here, off
            // the UI thread.
            let read = path.clone();
            let bytes = cx
                .background_executor()
                .spawn(async move { std::fs::read(read) })
                .await;

            let outcome = match bytes {
                Ok(bytes) => transfer(&upload, &mounted, &path, bytes).await,
                Err(error) => Err(format!("could not read {}: {error}", path.display())),
            };

            this.update(cx, |view, cx| {
                view.busy = None;

                view.feedback = match outcome {
                    Ok(feedback) => feedback,
                    Err(reason) => format!("Attachment failed: {reason}").into(),
                };

                cx.notify();
            })
            .ok();
        }));
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

    /// Records a command failure on the feedback line. A failure caused by a
    /// dead socket needs no special handling here: the crate's status stream
    /// has already flipped the pill to "reconnecting" (BDR-0033), so the two
    /// signals coincide instead of the command being the only evidence.
    fn note_failure(&mut self, label: &str, error: &MusubiError) {
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

    /// The pill flips without the message list blanking, because the
    /// last-good snapshot is kept (BDR-0015).
    ///
    /// It renders [`Mounted::status_updates`] (BDR-0033): a socket that drops
    /// while the app is idle flips to "reconnecting" the moment the client
    /// notices — bounded by the heartbeat window — with no command involved,
    /// and the rejoin's fresh initial patch flips it back to "live".
    ///
    /// The browser client has no equivalent piece of chrome, though the same
    /// signal exists there as `connection.status()` / `onStatusChange()`.
    fn connection_pill(&self) -> AnyElement {
        let (label, tint) = self.connection_state();

        stat_pill(label, Some(tint))
            .debug_selector(|| "connection-pill".into())
            .into_any_element()
    }

    /// The upload's one entry, while there is one.
    ///
    /// `max_entries` is 1, so the handle never holds more; a `reset` (which
    /// consuming the entry emits) empties it again.
    pub fn attach_entry(&self) -> Option<&UploadEntry> {
        self.attachment
            .as_ref()
            .and_then(|handle| handle.entries.first())
    }

    /// The pill's copy and tint, split out so it can be asserted on directly.
    ///
    /// `mount_error` keeps its own arm — a rejected join is terminal and never
    /// enters the status stream (BDR-0033) — and before the mount resolves
    /// there is no handle to read a status from, hence "connecting". Past
    /// that, the copy is the [`MountStatus`] verbatim.
    ///
    /// A cache-seeded mount (`docs/rust-client.md` §6.4) lands in the
    /// `Connecting` arm *with* a rendered snapshot: `snapshot` and `status` are
    /// independent on purpose, so last-session state paints under a "joining"
    /// pill until the accepted live initial patch flips it — a seed never
    /// counts as `Live`.
    pub fn connection_state(&self) -> (&'static str, u32) {
        if self.mount_error.is_some() {
            return ("offline", RUST);
        }

        if self.mounted.is_none() {
            return ("connecting", GOLD);
        }

        match self.status {
            MountStatus::Connecting => ("joining", GOLD),
            MountStatus::Live => ("live", TEAL),
            MountStatus::Reconnecting => ("reconnecting", GOLD),
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
            .child(self.attach_row(cx))
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

    /// `<div class="attach-row">`: the picker, then whatever the upload has to
    /// say — its live entry, a rejection, or the declared limits.
    ///
    /// Everything rendered here comes off the handle, which the server drives
    /// over `upload_ops`. Progress repaints without the message list
    /// re-rendering: upload state is not part of the state tree, so a
    /// `{op: progress}` marks no `socket.assigns` key changed.
    fn attach_row(&self, cx: &Context<Self>) -> AnyElement {
        div()
            .flex()
            .flex_row()
            .items_center()
            .gap(px(8.8))
            .mb(px(7.2))
            .child(
                Button::new("attach")
                    .large()
                    .h(px(ATTACH_H))
                    .label(if self.busy == Some(Pending::Attach) {
                        "Uploading"
                    } else {
                        "Attach file"
                    })
                    .disabled(self.busy.is_some())
                    .on_click(cx.listener(|this, _event, window, cx| {
                        this.attach(None, window, cx);
                    }))
                    .debug_selector(|| "attach-button".into()),
            )
            .child(self.attach_state())
            .into_any_element()
    }

    /// The line beside the picker.
    fn attach_state(&self) -> AnyElement {
        let Some(handle) = self.attachment.as_ref() else {
            return note(TEXT_STATUS, MUTED, "waiting for the upload config");
        };

        // A rejected file produces no entry at all (BDR-0024), so a
        // handle-level error is the only thing left to show for it.
        if let Some(error) = handle.errors.first() {
            return note(TEXT_STATUS, RUST, error.message.clone());
        }

        match self.attach_entry() {
            Some(entry) => stat_pill(
                format!("{} — {}%", entry.client_name, entry.progress),
                Some(TEAL),
            )
            .debug_selector(|| "attach-progress".into())
            .into_any_element(),
            None => note(
                TEXT_STATUS,
                MUTED,
                format!(
                    "{} up to {}",
                    accept_text(&handle.config.accept),
                    byte_text(handle.config.max_file_size)
                ),
            ),
        }
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
// The upload path
// -----------------------------------------------------------------------------

/// The native file dialog. `None` means the user cancelled.
async fn pick_file(cx: &mut AsyncWindowContext) -> Option<PathBuf> {
    let paths = cx
        .update(|_window, app| {
            app.prompt_for_paths(PathPromptOptions {
                files: true,
                directories: false,
                multiple: false,
                prompt: Some("Attach".into()),
            })
        })
        .ok()?;

    // `Canceled` (the window went away), then the platform's own error, then
    // the user pressing Cancel — all three mean the same thing here.
    paths.await.ok()?.ok()??.into_iter().next()
}

/// `select` → `start` → `attach`, in order, with the feedback line each step
/// would produce.
///
/// The command is not optional: a completed entry sits in the server's index
/// until something consumes it, and `consume_uploaded_entries/3` may only run
/// inside a command handler (`docs/uploads.md`).
async fn transfer(
    upload: &Upload,
    mounted: &Mounted<ChatRoomStore>,
    path: &Path,
    bytes: Vec<u8>,
) -> Result<SharedString, String> {
    let name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "attachment".to_owned());

    let file = UploadFile::new(name, content_type(path), bytes);

    let entries = upload
        .select(vec![file])
        .await
        .map_err(|error| error.to_string())?;

    if entries.is_empty() {
        return Err(upload
            .snapshot()
            .errors
            .first()
            .map(|error| error.message.clone())
            .unwrap_or_else(|| "the server accepted no entry".to_owned()));
    }

    upload.start().await.map_err(|error| error.to_string())?;

    let reply = mounted
        .command(Attach {})
        .await
        .map_err(|error| error.to_string())?;

    Ok(match reply {
        AttachReply {
            attached: true,
            name: Some(name),
        } => format!("Attached {name}.").into(),
        _ => "Nothing to attach.".into(),
    })
}

/// The `client_type` the server is told about, guessed from the extension.
///
/// The declared `accept` list is checked against the extension and never
/// against this, so a wrong guess cannot reject a file.
fn content_type(path: &Path) -> &'static str {
    let extension = path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase);

    match extension.as_deref() {
        Some("png") => "image/png",
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("txt") => "text/plain",
        Some("md") => "text/markdown",
        _ => DEFAULT_CONTENT_TYPE,
    }
}

/// `formatAccept()` from `App.tsx`.
fn accept_text(accept: &UploadAccept) -> String {
    match accept {
        UploadAccept::Any => "Any file".to_owned(),
        UploadAccept::Extensions(extensions) => extensions.join(" "),
    }
}

/// `formatBytes()` from `App.tsx`, to the decimal place and all.
fn byte_text(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * 1024;

    match bytes {
        bytes if bytes < KB => format!("{bytes} B"),
        bytes if bytes < MB => format!("{} kB", bytes.div_ceil(KB)),
        bytes => format!("{:.1} MB", bytes as f64 / MB as f64),
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

/// One tinted line of copy at a given size — the composer dock's hint, its
/// rejection message, and anything else that is text and nothing else.
fn note(size: f32, tint: u32, text: impl Into<SharedString>) -> AnyElement {
    div()
        .text_size(px(size))
        .text_color(color(tint))
        .child(text.into())
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
        )
        // `attachment` is an ordinary `Option` field on the message, not upload
        // state: by the time this row exists the entry has been consumed and
        // the handle is back to idle.
        .children(message.attachment.as_ref().map(attachment_chip));

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

/// `<a class="attachment">`: what the `attach` command moved into the example's
/// agent, as the message row reports it.
///
/// The browser client renders an `<img>` preview off `attachment.url`; this one
/// shows the name and size, because the Musubi transport carries no images and
/// the example is not going to grow an HTTP client to fetch one.
fn attachment_chip(attachment: &AttachmentState) -> AnyElement {
    div()
        .flex()
        .flex_row()
        .items_center()
        .gap(px(8.8))
        .mt(px(8.8))
        .p(px(7.2))
        .rounded(px(RADIUS))
        .border_1()
        .border_color(color(BORDER_SOFT))
        .bg(color(STAT))
        .text_color(color(INK))
        .debug_selector(|| "attachment-chip".into())
        .child(
            div()
                .flex()
                .items_center()
                .justify_center()
                .size(px(AVATAR))
                .flex_shrink_0()
                .rounded(px(RADIUS))
                .bg(color(GOLD))
                .text_size(px(TEXT_EYEBROW))
                .font_weight(FontWeight::BLACK)
                .child("FILE"),
        )
        .child(
            div()
                .flex()
                .flex_col()
                .min_w_0()
                .child(
                    div()
                        .font_weight(FontWeight::BOLD)
                        .child(attachment.name.clone()),
                )
                .child(
                    div()
                        .text_size(px(TEXT_SMALL))
                        .text_color(color(MUTED))
                        .child(byte_text(attachment.size.max(0) as u64)),
                ),
        )
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
    use musubi_client::{
        BinaryPush, CacheEntry, CacheStore, ConnectionBuilder, Connector, Frame, MemoryCacheStore,
        Socket, TransportError, cache_key, now_ms,
    };
    use serde_json::{Value, json};

    use super::*;
    use crate::transport::{GpuiSpawner, GpuiTimer};

    /// The topic `Connection::mount` derives from the module and the id.
    const TOPIC: &str = "musubi:connection:ChatRoom.Stores.ChatRoomStore:general";
    /// The `root_id` every patch envelope is addressed to.
    const ROOT: &str = "ChatRoom.Stores.ChatRoomStore:general";
    /// Who the canned snapshot says you are.
    const ME: &str = "Ada Lovelace";
    /// The entry ref the scripted preflight hands out.
    const ENTRY: &str = "u_1";
    /// The per-entry chunk topic that ref implies.
    const UPLOAD_TOPIC: &str = "musubi_upload:u_1";
    /// The bytes the attach test uploads.
    const FILE: &[u8] = b"musubi upload demo\n";

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

    /// The text `phx_reply` shape a chunk is answered with; Phoenix never
    /// answers a binary push with a binary frame.
    fn as_reply_target(push: &BinaryPush) -> Wire {
        Wire {
            join_ref: Some(push.join_ref.clone()),
            msg_ref: Some(push.msg_ref.clone()),
            topic: push.topic.clone(),
            event: push.event.clone(),
            payload: Value::Null,
        }
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
        /// Frames drained off the socket but not yet claimed by a reader.
        pending: Vec<Frame>,
    }

    impl Server {
        /// Builds the connector and the first socket's server end.
        fn new() -> (TestConnector, Self) {
            let sockets = Arc::new(Mutex::new(VecDeque::new()));
            let mut server = Self {
                sockets: Arc::clone(&sockets),
                to_client: None,
                from_client: mpsc::unbounded().1,
                pending: Vec::new(),
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
            self.pending.clear();
        }

        /// The text frames the client wrote since the last call.
        ///
        /// Binary frames stay queued for [`sent_binary`](Self::sent_binary):
        /// a chunk transfer interleaves the two, and a reader that discarded
        /// the kind it was not asked for would silently eat them.
        fn sent(&mut self) -> Vec<Wire> {
            self.take(|frame| match frame {
                Frame::Text(_) => Some(Wire::decode(frame)),
                Frame::Binary(_) => None,
            })
        }

        /// The binary pushes the client wrote since the last call.
        fn sent_binary(&mut self) -> Vec<BinaryPush> {
            self.take(|frame| match frame {
                Frame::Binary(bytes) => {
                    Some(BinaryPush::decode(bytes).expect("chunks are binary pushes"))
                }
                Frame::Text(_) => None,
            })
        }

        /// Drains the socket into the pending buffer, then removes and decodes
        /// every frame `decode` claims, leaving the rest in order.
        fn take<T>(&mut self, decode: impl Fn(&Frame) -> Option<T>) -> Vec<T> {
            while let Ok(frame) = self.from_client.try_recv() {
                self.pending.push(frame);
            }

            let mut taken = Vec::new();

            self.pending.retain(|frame| match decode(frame) {
                Some(decoded) => {
                    taken.push(decoded);
                    false
                }
                None => true,
            });

            taken
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
                    // The framework injects one marker per declared upload
                    // after `render/1` returns; it is inert, and carries only
                    // the name the live handle is keyed by.
                    "attachment": {"__musubi_upload__": "attachment"},
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
                    "item": {
                        "id": "msg-1", "body": "first", "sender": "Grace Hopper",
                        "attachment": null
                    }
                },
                {
                    "op": "insert", "stream": "messages", "ref": "0", "store_id": [],
                    "item_key": "msg-msg-2", "at": -1, "limit": null,
                    "item": {"id": "msg-2", "body": "second", "sender": ME, "attachment": null}
                }
            ],
            // What the mount emits for a declared upload: one `config` op with
            // the limits the store declared, and no entries yet.
            "upload_ops": [{
                "op": "config",
                "upload": "attachment",
                "store_id": [],
                "config": {
                    "accept": [".png", ".jpg", ".jpeg", ".gif", ".txt", ".md"],
                    "max_entries": 1,
                    "max_file_size": 2_000_000,
                    "chunk_size": 64_000
                }
            }],
            "events": []
        })
    }

    /// An envelope that carries nothing but `upload_ops` — the shape a
    /// transfer produces, since upload state marks no `socket.assigns` key
    /// changed and so drives no JSON patch of its own.
    fn upload_envelope(base: u64, version: u64, ops: Value) -> Value {
        json!({
            "type": "patch",
            "root_id": ROOT,
            "base_version": base,
            "version": version,
            "ops": [],
            "stream_ops": [],
            "upload_ops": ops,
            "events": []
        })
    }

    fn add_op(progress: u64) -> Value {
        json!({
            "op": "add", "upload": "attachment", "store_id": [], "ref": ENTRY,
            "entry": {
                "ref": ENTRY,
                "client_name": "musubi-attach.txt",
                "client_size": FILE.len(),
                "client_type": "text/plain",
                "progress": progress,
                "status": "pending",
                "errors": []
            }
        })
    }

    fn progress_op(progress: u64) -> Value {
        json!({
            "op": "progress", "upload": "attachment", "store_id": [],
            "ref": ENTRY, "progress": progress
        })
    }

    fn complete_op() -> Value {
        json!({"op": "complete", "upload": "attachment", "store_id": [], "ref": ENTRY})
    }

    /// The envelope the `attach` command produces: the row the server appended,
    /// plus the `reset` that consuming the last entry emits.
    fn attachment_envelope(base: u64, version: u64) -> Value {
        json!({
            "type": "patch",
            "root_id": ROOT,
            "base_version": base,
            "version": version,
            "ops": [],
            "stream_ops": [{
                "op": "insert", "stream": "messages", "ref": "0", "store_id": [],
                "item_key": "msg-msg-3", "at": 0, "limit": -100,
                "item": {
                    "id": "msg-3",
                    "body": "shared musubi-attach.txt",
                    "sender": ME,
                    "attachment": {
                        "name": "musubi-attach.txt",
                        "content_type": "text/plain",
                        "size": FILE.len(),
                        "url": "/attachments/att-1"
                    }
                }
            }],
            "upload_ops": [{"op": "reset", "upload": "attachment", "store_id": []}],
            "events": []
        })
    }

    // -------------------------------------------------------------------------
    // Rig
    // -------------------------------------------------------------------------

    /// Opens the real window tree — `Root` on the outside, or gpui-component
    /// panics looking for it — over a scripted socket.
    fn boot(cx: &mut TestAppContext) -> (Server, Entity<ChatWindow>, &mut VisualTestContext) {
        boot_with(cx, |builder| builder)
    }

    /// [`boot`], with a hook to finish the [`ConnectionBuilder`] — how the
    /// seeded-mount test turns the cache on without a second rig.
    fn boot_with(
        cx: &mut TestAppContext,
        tune: impl FnOnce(ConnectionBuilder) -> ConnectionBuilder,
    ) -> (Server, Entity<ChatWindow>, &mut VisualTestContext) {
        cx.update(|cx| {
            gpui_component::init(cx);
            crate::theme::apply_paper_theme(cx);
        });

        let (connector, server) = Server::new();
        let executor = cx.executor();
        let builder = Connection::builder()
            .url("ws://test.invalid/socket")
            .connector(connector)
            .spawner(GpuiSpawner(executor.clone()))
            .timer(GpuiTimer(executor));
        let connection = tune(builder)
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

    /// A cache-seeded mount renders last-session state before any server frame
    /// — under a "joining" pill, because a seed never counts as `Live`
    /// (BDR-0033) — and the live initial patch then replaces the seed with the
    /// server's tree in one whole-root op (`docs/rust-client.md` §6.4).
    ///
    /// The crate's in-memory store stands in for
    /// [`FileCacheStore`](crate::cache_store::FileCacheStore): the seeding path
    /// is identical behind the `CacheStore` trait, and the file layer has its
    /// own round-trip and corrupt-file tests in `cache_store.rs`.
    #[gpui::test]
    fn a_seeded_mount_renders_cached_state_before_any_server_frame(cx: &mut TestAppContext) {
        // What the last session's throttled write left behind: the wire tree
        // out of `initial_envelope`, with a name only the cache could know.
        let mut cached = initial_envelope()["ops"][0]["value"].clone();
        cached["current_user"]["name"] = json!("Cached Ada");

        let store = Arc::new(MemoryCacheStore::new());
        futures::executor::block_on(store.put(
            &cache_key(
                "ChatRoom.Stores.ChatRoomStore",
                ROOM_ID,
                &json!({"room_id": ROOM_ID}),
            ),
            CacheEntry {
                data: cached,
                updated_at: now_ms(),
                buster: String::new(),
            },
        ));

        let (mut server, chat, cx) = boot_with(cx, |builder| builder.cache(Arc::clone(&store)));
        cx.run_until_parked();

        // Nothing has been replied to yet, so everything on screen is the
        // seed: cached identity and cached presence rows under a "joining"
        // pill — `snapshot` and `status` are independent on purpose.
        assert_eq!(chat.update(cx, |chat, _| chat.poster()), "Cached Ada");
        assert_eq!(
            chat.update(cx, |chat, _| chat.connection_state().0),
            "joining"
        );
        assert!(cx.debug_bounds("message-list").is_some());
        // Streams are not cached (`stream_ops` are not part of the tree), so
        // the seeded messages slot hydrates empty until the live envelope
        // refills it.
        assert_eq!(chat.update(cx, |chat, _| chat.messages().len()), 0);

        // The join went out concurrently — the seed raced the network, it did
        // not replace it. Answering it swaps the seed for the server's tree.
        let join = server.only("phx_join");
        server.reply(&join, "ok", json!({"root_id": ROOT}));
        cx.run_until_parked();
        server.push_patch(&join, initial_envelope());
        cx.run_until_parked();

        assert_eq!(chat.update(cx, |chat, _| chat.poster()), ME);
        assert_eq!(chat.update(cx, |chat, _| chat.messages().len()), 2);
        assert_eq!(chat.update(cx, |chat, _| chat.connection_state().0), "live");
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

    /// The whole channel-mode upload, driven over the scripted socket:
    /// preflight, the signed sub-channel, the chunk, then the `config` /
    /// `add` / `progress` / `complete` ops and the message row that follows.
    ///
    /// The path is passed in rather than picked: a native file dialog cannot
    /// be driven from a test, and everything after the path is identical
    /// either way.
    #[gpui::test]
    fn attaching_a_file_uploads_it_and_renders_the_message_it_produces(cx: &mut TestAppContext) {
        let (mut server, chat, cx) = boot(cx);
        let join = mount(&mut server, cx);

        // Written under the OS temp dir rather than the repo: the app reads it
        // with `std::fs::read`, so it has to be a real file.
        let path = std::env::temp_dir().join(format!("musubi-attach-{}.txt", std::process::id()));
        std::fs::write(&path, FILE).expect("the temp file is writable");

        cx.update(|window, cx| {
            chat.update(cx, |chat, cx| chat.attach(Some(path.clone()), window, cx))
        });
        cx.run_until_parked();

        // 1. Preflight. The size is the byte length the client read, and the
        //    type is guessed from the extension.
        let allow = server.only("allow_upload");
        assert_eq!(allow.topic, TOPIC);
        assert_eq!(allow.payload["name"], json!("attachment"));
        assert_eq!(
            allow.payload["entries"],
            json!([{
                "client_ref": "0",
                "name": path.file_name().unwrap().to_string_lossy(),
                "size": FILE.len(),
                "type": "text/plain",
            }])
        );

        server.reply(
            &allow,
            "ok",
            json!({
                "ref": "attachment",
                "config": {
                    "accept": [".png", ".jpg", ".jpeg", ".gif", ".txt", ".md"],
                    "max_entries": 1,
                    "max_file_size": 2_000_000,
                    "chunk_size": 64_000
                },
                "entries": {"0": {"type": "channel", "entry_ref": ENTRY, "token": "tok"}},
                "errors": []
            }),
        );
        cx.run_until_parked();

        // 2. The sub-channel, joined with the stateless preflight token.
        let upload_join = server.only("phx_join");
        assert_eq!(upload_join.topic, UPLOAD_TOPIC);
        assert_eq!(upload_join.payload, json!({"token": "tok"}));

        server.reply(&upload_join, "ok", json!({}));
        cx.run_until_parked();

        // 3. One chunk: the file is far below the 64 kB slice size.
        let chunks = server.sent_binary();
        assert!(
            matches!(
                chunks.as_slice(),
                [BinaryPush { topic, event, payload, .. }]
                    if topic == UPLOAD_TOPIC && event == "chunk" && payload == FILE
            ),
            "expected one whole-file chunk, got {chunks:?}"
        );

        // 4. Progress renders off the handle's updates stream, not off a reply.
        server.reply(&as_reply_target(&chunks[0]), "ok", json!({"progress": 100}));
        server.push_patch(
            &join,
            upload_envelope(1, 2, json!([add_op(0), progress_op(60)])),
        );
        cx.run_until_parked();

        assert_eq!(
            chat.update(cx, |chat, _| chat
                .attach_entry()
                .map(|entry| entry.progress)),
            Some(60),
            "the progress op repaints the composer dock"
        );
        assert!(cx.debug_bounds("attach-progress").is_some());

        // 5. `complete` is the authoritative finish, and the command that
        //    consumes the entry goes out only after `start` resolves.
        server.push_patch(&join, upload_envelope(2, 3, json!([complete_op()])));
        cx.run_until_parked();

        let command = server.only("command");
        assert_eq!(command.payload["name"], json!("attach"));
        assert_eq!(command.payload["payload"], json!({}));

        server.reply(
            &command,
            "ok",
            json!({"attached": true, "name": "musubi-attach.txt"}),
        );
        cx.run_until_parked();

        assert_eq!(
            chat.update(cx, |chat, _| chat.send_state()),
            "Attached musubi-attach.txt."
        );

        // 6. The row itself arrives on the ordinary stream, one envelope later
        //    and carrying the consumed attachment as plain state — never out of
        //    the reply (BDR-0009). Consuming the last entry empties the index,
        //    so the same envelope resets the handle to idle.
        server.push_patch(&join, attachment_envelope(3, 4));
        cx.run_until_parked();

        assert_eq!(chat.update(cx, |chat, _| chat.messages().len()), 3);
        assert_eq!(
            chat.update(cx, |chat, _| chat
                .attach_entry()
                .map(|entry| entry.progress)),
            None,
            "the reset op empties the handle"
        );
        assert!(cx.debug_bounds("attachment-chip").is_some());

        std::fs::remove_file(&path).ok();
    }

    /// A socket that goes away flips the pill **on its own** (BDR-0033): the
    /// crate's status stream reports the drop with no command involved, and
    /// the last good snapshot keeps rendering (BDR-0015).
    #[gpui::test]
    fn a_dropped_socket_flips_the_pill_without_a_command_and_keeps_the_rows(
        cx: &mut TestAppContext,
    ) {
        let (mut server, chat, cx) = boot(cx);
        mount(&mut server, cx);

        assert_eq!(chat.update(cx, |chat, _| chat.connection_state().0), "live");

        server.disconnect();
        cx.run_until_parked();

        assert_eq!(
            chat.update(cx, |chat, _| chat.connection_state().0),
            "reconnecting",
            "the pill flips from the status stream alone"
        );
        assert!(
            server.sent().is_empty(),
            "nothing was dispatched to notice the drop"
        );
        assert_eq!(
            chat.update(cx, |chat, _| chat.messages().len()),
            2,
            "the last good snapshot is kept"
        );
        assert!(cx.debug_bounds("message-list").is_some());
        assert!(cx.debug_bounds("connection-pill").is_some());

        // A command sent into the dead window still fails onto the feedback
        // line, coinciding with the pill rather than being its only source.
        cx.simulate_input("into the void");
        let send = center(cx, "send-button");
        cx.simulate_click(send, Modifiers::none());
        cx.run_until_parked();

        assert_eq!(
            chat.update(cx, |chat, _| chat.connection_state().0),
            "reconnecting"
        );
    }
}
