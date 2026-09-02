//! The executor and clock seams, backed by the ambient tokio runtime.

use std::time::Duration;

use futures_core::future::BoxFuture;
use musubi_client::{Spawner, Timer};

/// Spawns detached tasks with [`tokio::spawn`].
///
/// Like `tokio::spawn` itself, this panics when no runtime is entered — which
/// happens on the thread that calls `build()`, not inside the client.
///
/// ```
/// use musubi_client::Spawner;
/// use musubi_client_tokio::TokioSpawner;
///
/// let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
/// let (tx, rx) = std::sync::mpsc::channel();
///
/// runtime.block_on(async move {
///     TokioSpawner.spawn(Box::pin(async move { tx.send("ran").unwrap() }));
///     tokio::task::yield_now().await;
/// });
///
/// assert_eq!(rx.recv().unwrap(), "ran");
/// ```
#[derive(Debug, Default, Clone, Copy)]
pub struct TokioSpawner;

impl Spawner for TokioSpawner {
    fn spawn(&self, fut: BoxFuture<'static, ()>) {
        // The client owns every task's lifetime through its own channels, so
        // the join handle carries nothing a caller could use.
        drop(tokio::spawn(fut));
    }
}

/// Sleeps with [`tokio::time::sleep`], driving heartbeats, push deadlines and
/// the reconnect backoff ladder.
///
/// ```
/// use std::time::Duration;
///
/// use musubi_client::Timer;
/// use musubi_client_tokio::TokioTimer;
///
/// let runtime = tokio::runtime::Builder::new_current_thread()
///     .enable_time()
///     .build()
///     .unwrap();
///
/// runtime.block_on(async {
///     let started = tokio::time::Instant::now();
///     TokioTimer.sleep(Duration::from_millis(10)).await;
///
///     assert!(started.elapsed() >= Duration::from_millis(10));
/// });
/// ```
#[derive(Debug, Default, Clone, Copy)]
pub struct TokioTimer;

impl Timer for TokioTimer {
    fn sleep(&self, dur: Duration) -> BoxFuture<'static, ()> {
        Box::pin(tokio::time::sleep(dur))
    }
}
