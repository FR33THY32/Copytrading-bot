//! Async logging utility for hot paths.
//!
//! Uses a bounded channel to send log messages to a background task,
//! ensuring the hot path never blocks on logging.

use log::info;
use std::sync::OnceLock;
use tokio::sync::mpsc::{self, Sender};

/// Channel capacity - if full, logs are dropped rather than blocking
const CHANNEL_CAPACITY: usize = 1024;

static LOG_SENDER: OnceLock<Sender<String>> = OnceLock::new();

/// Initialize the async logger. Call once at startup.
/// Returns a handle to the background logging task.
pub fn init_async_logger() -> tokio::task::JoinHandle<()> {
    let (tx, mut rx) = mpsc::channel::<String>(CHANNEL_CAPACITY);
    LOG_SENDER
        .set(tx)
        .expect("async logger already initialized");

    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            info!("{}", msg);
        }
    })
}

/// Log a message asynchronously. Non-blocking - drops if channel is full.
#[inline]
pub fn info_async(msg: String) {
    if let Some(sender) = LOG_SENDER.get() {
        // try_send is non-blocking - if channel full, log is dropped
        let _ = sender.try_send(msg);
    }
}

/// Convenience macro for async info logging with format support
#[macro_export]
macro_rules! info_async {
    ($($arg:tt)*) => {
        $crate::async_log::info_async(format!($($arg)*))
    };
}
