//! The gpui chat-room client (`docs/rust-gpui-example.md` §6).
//!
//! This file is the startup path only: build a [`Connection`] over the
//! gpui-backed seams in [`transport`], open one window, and hand the connection
//! to [`ChatWindow`], which owns the mount and every pixel after that.
//!
//! Run it with `mix desktop` from `examples/chat_room/`, against a `mix server`
//! in another terminal.

mod app;
mod generated;
mod transport;

use gpui::{App, AppContext, Application, Bounds, WindowBounds, WindowOptions, px, size};
use gpui_component::{Root, Theme, ThemeMode};
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
        Theme::change(ThemeMode::Dark, None, cx);

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
