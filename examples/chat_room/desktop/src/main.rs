//! The gpui chat-room client (`docs/rust-gpui-example.md` §6).
//!
//! This file is the startup path only: paint the palette `ui/src/App.css`
//! defines onto the gpui-component theme, build a [`Connection`] over the
//! gpui-backed seams in [`transport`], open one window, and hand the connection
//! to [`ChatWindow`], which owns the mount and every pixel after that.
//!
//! Run it with `mix desktop` from `examples/chat_room/`, against a `mix server`
//! in another terminal.

mod app;
mod generated;
mod theme;
mod transport;

use std::io::{IsTerminal, Read};

use gpui::{
    App, AppContext, Application, Bounds, TitlebarOptions, WindowBounds, WindowOptions, point, px,
    size,
};
use gpui_component::Root;
use musubi_client::Connection;

use crate::app::ChatWindow;
use crate::transport::{GpuiSpawner, GpuiTimer, SmolConnector};

/// The socket base; `/websocket` and `vsn=2.0.0` are appended by the client.
///
/// `MUSUBI_URL` overrides it. There is no config file.
const DEFAULT_URL: &str = "ws://127.0.0.1:4002/socket";

fn main() {
    Application::new().run(|cx: &mut App| {
        // Must be the first call inside `run`: it installs the theme registry,
        // the key bindings and the action context every gpui-component widget
        // needs — `Input` included.
        gpui_component::init(cx);
        // The browser client is a light, warm-paper design; the native window
        // uses the same ink-on-paper palette rather than gpui-component's
        // default greys, so the two clients are recognizably one app.
        theme::apply_paper_theme(cx);

        // `mix desktop` shells out to `cargo run`, so killing mix kills the
        // BEAM and the `cargo` port — but never this grandchild. Closing the
        // port's stdio is the only signal that reaches here.
        quit_when_stdin_closes(cx);

        // The three runtime seams, all over gpui's own executor (§5). No tokio
        // runtime is started, and `musubi-client-tokio` is not a dependency.
        let executor = cx.background_executor().clone();
        let url = std::env::var("MUSUBI_URL").unwrap_or_else(|_| DEFAULT_URL.to_owned());
        let connection = Connection::builder()
            .url(url.clone())
            .connector(SmolConnector)
            .spawner(GpuiSpawner(executor.clone()))
            .timer(GpuiTimer(executor))
            .build()
            .expect("every connection seam is supplied above");

        let bounds = Bounds::centered(None, size(px(880.0), px(620.0)), cx);

        cx.open_window(
            WindowOptions {
                window_bounds: Some(WindowBounds::Windowed(bounds)),
                // Frameless, the way Zed is: no drawn title bar, but the window
                // is still `NSTitled`, so the close/minimize/zoom buttons are
                // the real ones and AppKit still starts a window drag anywhere
                // in the top band `ChatWindow::render` leaves empty. Only the
                // chrome moves; nothing here draws a title bar of its own.
                titlebar: Some(TitlebarOptions {
                    title: None,
                    appears_transparent: true,
                    traffic_light_position: Some(point(px(12.0), px(10.0))),
                }),
                ..Default::default()
            },
            |window, cx| {
                // Mounting is async and needs somewhere to report failure, so
                // it starts inside the view rather than out here: a rejected
                // join becomes a rendered panel, not a silent exit.
                let view = cx.new(|cx| ChatWindow::new(connection, url.into(), window, cx));

                // The window's first-level view must be a `Root`, or popovers,
                // notifications and dialogs have nowhere to render.
                cx.new(|cx| Root::new(view, window, cx))
            },
        )
        .expect("the window opens");

        cx.activate(true);
    });
}

/// Quits when the parent process closes our stdin.
///
/// `mix desktop` runs `cargo run` through `Mix.shell().cmd/2`, i.e. an Erlang
/// port two processes above this binary. When the BEAM dies the port's stdio is
/// closed, but no signal is delivered to the grandchild, so without this the
/// window survives the `mix` it was launched from. Reading stdin to EOF is the
/// one event that does cross that gap.
///
/// A terminal `cargo run` is left alone: stdin is a tty there, the read would
/// block on the user's keyboard, and the first typed newline would look like a
/// quit request.
fn quit_when_stdin_closes(cx: &mut App) {
    if std::io::stdin().is_terminal() {
        return;
    }

    let (closed, wait) = futures::channel::oneshot::channel::<()>();

    // A plain OS thread, not a background task: `Stdin::read` blocks, and gpui's
    // background executor is a fixed-size pool that must not be parked on IO.
    std::thread::spawn(move || {
        let mut stdin = std::io::stdin().lock();
        let mut sink = [0_u8; 256];

        // Anything the parent writes is discarded; only the end of the stream
        // is a message. A read error means the same thing as EOF here.
        while matches!(stdin.read(&mut sink), Ok(1..)) {}

        // The receiver is gone if the app already quit, which is fine.
        let _ = closed.send(());
    });

    // The quit itself has to happen on the foreground thread, so the thread
    // above only signals and this task does the work.
    cx.spawn(async move |cx| {
        if wait.await.is_ok() {
            cx.update(|cx| cx.quit()).ok();
        }
    })
    .detach();
}
