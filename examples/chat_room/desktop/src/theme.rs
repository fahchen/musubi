//! The palette, ported value-for-value from `examples/chat_room/ui/src/App.css`.
//!
//! The browser client hard-codes its colors in one stylesheet; there is no
//! design-token file to share, so this module is the second copy and the CSS
//! rule each value comes from is named next to it. Two consumers:
//!
//! * [`apply_paper_theme`] repaints gpui-component's `Theme` so `Input` and
//!   `Button` — the two widgets this example does not draw itself — land on the
//!   same ink-on-paper palette as everything around them.
//! * [`crate::app`] passes the constants to [`color`] for the chrome it does
//!   draw.
//!
//! Every constant is `0xRRGGBBAA`, alpha included, because gpui has no
//! `color-mix`: `rgba(32, 33, 29, 0.14)` is `0x20211d24` (0.14 × 255 ≈ 0x24).

use gpui::{App, Hsla, px, rgba};
use gpui_component::{Theme, ThemeMode};

/// `#20211d` — `:root { color }`. Every foreground that is not on a tint.
pub const INK: u32 = 0x20211dff;
/// `#fffaf1` — `.chat-shell { background }`. The page surface.
pub const PAPER: u32 = 0xfffaf1ff;
/// `#f3efe5` — `:root { background }`. The desk the shell sits on.
pub const CANVAS: u32 = 0xf3efe5ff;
/// `#efe6d0` — `.sidebar { background }`.
pub const SAND: u32 = 0xefe6d0ff;
/// `#2b796b` — `.room-mark`, `.status-ok`, `.message-self .bubble`.
pub const TEAL: u32 = 0x2b796bff;
/// `#d75a35` — `.self-avatar`, `.status-dot`, `input:focus`.
pub const RUST: u32 = 0xd75a35ff;
/// `#ebbc3f` — `.avatar`, `.status-loading`.
pub const GOLD: u32 = 0xebbc3fff;
/// `#8a3d2a` — `.eyebrow { color }`.
pub const EYEBROW: u32 = 0x8a3d2aff;
/// `#ffffff` — `.bubble { background }`, the one pure white in the design.
pub const BUBBLE: u32 = 0xffffffff;

/// `rgba(32, 33, 29, 0.18)` — `.chat-shell` and `input` borders.
pub const BORDER_STRONG: u32 = 0x20211d2e;
/// `rgba(32, 33, 29, 0.14)` — sidebar card borders and `.chat-stats span`.
pub const BORDER_CARD: u32 = 0x20211d24;
/// `rgba(32, 33, 29, 0.12)` — `.chat-header`, `.composer-dock`, `.bubble`.
pub const BORDER_SOFT: u32 = 0x20211d1f;
/// `rgba(32, 33, 29, 0.58)` — every secondary label.
pub const MUTED: u32 = 0x20211d94;
/// `rgba(32, 33, 29, 0.62)` — `.empty-state { color }`.
pub const EMPTY: u32 = 0x20211d9e;

/// `rgba(255, 250, 241, 0.72)` — `.room-card`, `.identity-card`,
/// `.presence-panel`.
pub const CARD: u32 = 0xfffaf1b8;
/// `rgba(255, 250, 241, 0.62)` — `.users li`.
pub const ROW: u32 = 0xfffaf19e;
/// `rgba(255, 250, 241, 0.74)` — `.chat-stats span`.
pub const STAT: u32 = 0xfffaf1bd;
/// `rgba(255, 250, 241, 0.84)` — `.composer-dock`.
pub const DOCK: u32 = 0xfffaf1d6;
/// `rgba(255, 250, 241, 0.72)` — `.message-self .bubble small`.
pub const ON_TEAL_MUTED: u32 = 0xfffaf1b8;

/// The far end of `.sidebar`'s gold wash:
/// `linear-gradient(135deg, rgba(235, 188, 63, 0.2), transparent 34%)` composited
/// over [`SAND`]. gpui takes two stops rather than a stack of layers, so the
/// blend is pre-computed here.
///
/// The stylesheet's other two washes — `.chatbox`'s pair of `radial-gradient`s —
/// have no gpui 0.2.2 equivalent (`linear_gradient` is the only gradient
/// constructor) and are dropped; that pane is flat [`PAPER`].
pub const SAND_WASH: u32 = 0xeeddb3ff;

/// `"Avenir Next"` heads the `:root { font-family }` stack.
///
/// Set once on the view's root element, which is what makes it cascade into the
/// gpui-component widgets nested inside it. gpui falls back to the system UI
/// font if the family is missing.
pub const FONT_FAMILY: &str = "Avenir Next";

/// `8px` — every `border-radius` in the stylesheet except the pills.
pub const RADIUS: f32 = 8.0;

/// `0xRRGGBBAA` as the `Hsla` gpui styles and the theme both take.
pub fn color(hex: u32) -> Hsla {
    rgba(hex).into()
}

/// Repaints gpui-component's global `Theme` with the stylesheet's palette.
///
/// Only the fields `Input` and `Button` actually read are overridden; the rest
/// of the light theme is left alone, because nothing in this example renders a
/// widget that reads them.
pub fn apply_paper_theme(cx: &mut App) {
    Theme::change(ThemeMode::Light, None, cx);

    let theme = Theme::global_mut(cx);

    theme.radius = px(RADIUS);
    theme.radius_lg = px(RADIUS);
    // The stylesheet does give buttons a drop shadow, but gpui-component's is a
    // different shape and reads as a different design; the flat fill is closer.
    theme.shadow = false;

    theme.background = color(PAPER);
    theme.foreground = color(INK);
    theme.muted_foreground = color(MUTED);
    theme.muted = color(CARD);

    // `input { border, background }` plus `input:focus { border-color }`.
    theme.input = color(BORDER_STRONG);
    theme.border = color(BORDER_STRONG);
    theme.ring = color(RUST);
    theme.caret = color(INK);
    theme.selection = color(0xebbc3f66);

    // `button { background, color }` — the only button style the sheet has, so
    // both buttons in the window are `.primary()`.
    theme.primary = color(INK);
    theme.primary_foreground = color(PAPER);
    theme.primary_hover = color(0x3a3b34ff);
    theme.primary_active = color(0x14150fff);

    theme.success = color(TEAL);
    theme.warning = color(GOLD);
    theme.danger = color(RUST);
    theme.accent = color(GOLD);
    theme.sidebar = color(SAND);
    theme.sidebar_border = color(BORDER_CARD);
}
