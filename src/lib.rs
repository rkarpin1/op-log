// -------------------------------------------------------------------------------------------------
//   Copyright 2024-2025 (c) Robert Karpiński
// -------------------------------------------------------------------------------------------------

mod add_to_log;
mod clean_up;
mod clean_up_delete_files;
pub mod messages;
mod service;
mod write_to_file;

use crate::messages::OpLogMessage;
pub use crate::messages::{
    OpLogBundle, OpLogCleanUpDefinition, OpLogData, OpLogDefinition, OpLogInfo, OpLogOption,
    OpLogType,
};
use chrono::{DateTime, Utc};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;

// -------------------------------------------------------------------------------------------------
// log crate integration
// -------------------------------------------------------------------------------------------------

struct OpLogSystemLogger {
    inner: std::sync::Arc<OpLogInner>,
    name: String,
}

impl log::Log for OpLogSystemLogger {
    fn enabled(&self, _metadata: &log::Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &log::Record<'_>) {

        if record.level() > log::Level::Info {
            return;
        }

        let text = format!("[{}] {}", record.level(), record.args());
        self.inner.send_log(OpLogData {
            log_name: self.name.clone(),
            log: text,
            date: Utc::now(),
        });
    }

    fn flush(&self) {
        let _ = self.inner.tx.try_send(OpLogMessage::Flush);
    }
}

// -------------------------------------------------------------------------------------------------
// Internal worker types
// -------------------------------------------------------------------------------------------------

struct LogDefinition {
    last_time_use: Instant,
    auto_remove_definition: bool,
    header: String,
    path: String,
    flush_interval: Duration,
    log_type: OpLogType,
    options: HashSet<OpLogOption>,
    files: HashMap<String, LogFile>,
}

struct LogFile {
    last_time_use: Instant,
    time_of_first_addition_of_log_after_write: Option<Instant>,
    log_name: String,
    header: String,
    path: String,
    logs: VecDeque<String>,
    /// True after a failed disk write was reported — keeps the error message
    /// to one per failure episode instead of one per writer tick.
    write_error_logged: bool,
}

struct LogCleanUpDefinition {
    path: String,
    delete_after_days: u32,
}

struct OpLogWorker {
    definitions: HashMap<String, LogDefinition>,
    clean_up_definitions: Vec<LogCleanUpDefinition>,
    rx_channel: Receiver<OpLogMessage>,
}

impl OpLogWorker {
    fn new(rx: Receiver<OpLogMessage>) -> OpLogWorker {
        OpLogWorker {
            definitions: HashMap::new(),
            clean_up_definitions: Vec::new(),
            rx_channel: rx,
        }
    }
}

// -------------------------------------------------------------------------------------------------
// User-facing handle
// -------------------------------------------------------------------------------------------------

/// Capacity of the worker channel. Shared by every log the process writes;
/// bursts of large entries from several concurrent producers can outpace the
/// worker while it is busy with disk I/O — a too-small buffer silently drops
/// entries.
const CHANNEL_CAPACITY: usize = 1024;

struct OpLogInner {
    tx: Sender<OpLogMessage>,
    handle: Mutex<Option<JoinHandle<()>>>,
    /// Log entries dropped because the channel was full; reported back into
    /// the log as a notice entry on the next successful send.
    dropped_logs: AtomicU64,
}

impl OpLogInner {
    /// Sends a log entry. A full channel does not lose entries silently:
    /// drops are counted and a notice entry is written to the same log once
    /// the channel has room again.
    fn send_log(&self, data: OpLogData) {
        let pending = self.dropped_logs.swap(0, Ordering::Relaxed);
        if pending > 0 {
            let notice = OpLogMessage::Log(OpLogData {
                log_name: data.log_name.clone(),
                log: format!("[op-log] dropped {pending} log entries (channel full)"),
                date: data.date,
            });
            if self.tx.try_send(notice).is_err() {
                self.dropped_logs.fetch_add(pending, Ordering::Relaxed);
            }
        }
        if self.tx.try_send(OpLogMessage::Log(data)).is_err() {
            self.dropped_logs.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Thread-safe handle to the OpLog background worker.
/// Clone freely — all clones share the same worker.
#[derive(Clone)]
pub struct OpLog(std::sync::Arc<OpLogInner>);

impl OpLog {
    /// Spawns the background worker. Must be called inside a Tokio runtime.
    pub fn new() -> OpLog {
        let (tx, rx) = mpsc::channel::<OpLogMessage>(CHANNEL_CAPACITY);
        let worker = OpLogWorker::new(rx);
        let handle = tokio::spawn(async move {
            let mut w = worker;
            w.run().await;
        });
        OpLog(std::sync::Arc::new(OpLogInner {
            tx,
            handle: Mutex::new(Some(handle)),
            dropped_logs: AtomicU64::new(0),
        }))
    }

    /// Registers a log definition. Non-blocking; drops silently if the channel is full.
    pub fn def(&self, def: OpLogDefinition) -> &OpLog {
        let _ = self.0.tx.try_send(OpLogMessage::LogDefinition(def));
        self
    }

    /// Registers a log definition and installs `OpLog` as the global `log` crate logger.
    /// After this call, macros such as `info!`, `warn!`, and `error!` route their output
    /// to `OpLog::log` using `def.log_name` as the log name.
    /// If a global logger is already set this call is a no-op for the logger part but still
    /// registers the definition.
    pub fn def_system_log(&self, def: OpLogDefinition) -> &OpLog {
        let name = def.log_name.clone();
        self.def(def);

        let logger = Box::new(OpLogSystemLogger {
            inner: self.0.clone(),
            name,
        });
        if log::set_logger(Box::leak(logger)).is_ok() {
            log::set_max_level(log::LevelFilter::Trace);
        }

        self
    }

    /// Sends a log entry. Non-blocking; if the channel is full the entry is
    /// dropped, but the drop is counted and reported into the log as a
    /// notice entry once the channel has room again.
    pub fn log(&self, name: &str, date: DateTime<Utc>, text: &str) -> &OpLog {
        self.0.send_log(OpLogData {
            log_name: name.to_string(),
            log: text.to_string(),
            date,
        });

        self
    }

    /// Sends a bundle of definitions and log entries atomically.
    pub fn log_bundle(&self, bundle: OpLogBundle) -> &OpLog {
        let _ = self.0.tx.try_send(OpLogMessage::LogBundle(bundle));
        self
    }

    /// Signals the worker to flush pending writes. Fire-and-forget.
    pub fn flush(&self) {
        let _ = self.0.tx.try_send(OpLogMessage::Flush);
    }

    /// Flushes pending writes and returns current stats.
    pub async fn get_info(&self) -> Option<OpLogInfo> {
        let (tx, rx) = oneshot::channel();
        let _ = self.0.tx.try_send(OpLogMessage::GetInfoAndFlush(tx));
        rx.await.ok()
    }

    /// Stops the worker and waits for it to finish (including a final flush).
    /// Idempotent — safe to call from any clone; subsequent calls are no-ops.
    pub async fn shutdown(&self) {
        // Wait for channel room rather than `try_send`: a full channel would
        // drop the stop signal and leave `h.await` below waiting forever. If
        // the worker is already gone the send fails immediately.
        let _ = self.0.tx.send(OpLogMessage::StopService).await;
        let handle = self.0.handle.lock().unwrap().take();
        if let Some(h) = handle {
            let _ = h.await;
        }
    }

    /// Registers or updates a cleanup rule for a log path.
    pub fn clean_up_definition(&self, def: OpLogCleanUpDefinition) -> &OpLog {
        let _ = self.0.tx.try_send(OpLogMessage::CleanUpDefinition(def));
        self
    }

    /// Replaces all cleanup rules at once.
    pub fn clean_up_bundle(&self, bundle: Vec<OpLogCleanUpDefinition>) -> &OpLog {
        let _ = self.0.tx.try_send(OpLogMessage::CleanUpBundle(bundle));
        self
    }

    /// Removes all registered cleanup rules.
    pub fn clean_up_remove_all_definitions(&self) -> &OpLog {
        let _ = self
            .0
            .tx
            .try_send(OpLogMessage::CleanUpRemoveAllDefinitions);
        self
    }
}

impl Default for OpLog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use std::time::Duration;

    #[tokio::test]
    async fn user_facing_api() {
        let op = OpLog::new();

        let def = OpLogDefinition::new("test", ".")
            .log_type(OpLogType::PerHour)
            .flush_interval(Duration::from_secs(60));

        op.def(def);

        let op2 = op.clone();

        op2.log("test", Utc::now(), "test log from clone");
        op.flush();

        let info = op.get_info().await;
        assert!(info.is_some());

        op.shutdown().await;

        // second shutdown call must be a no-op (idempotent)
        op2.shutdown().await;
    }

    // Exercises send_log directly against a tiny channel with no worker:
    // overflow is counted, and the first send after the channel has room
    // emits a notice entry carrying the dropped count.
    #[tokio::test]
    async fn send_log_counts_drops_and_reports_notice() {
        let (tx, mut rx) = mpsc::channel::<OpLogMessage>(1);
        let inner = OpLogInner {
            tx,
            handle: Mutex::new(None),
            dropped_logs: AtomicU64::new(0),
        };
        let entry = |text: &str| OpLogData {
            log_name: "test".to_string(),
            log: text.to_string(),
            date: Utc::now(),
        };

        // capacity 1: first fills the channel, next two are dropped and counted
        inner.send_log(entry("1"));
        inner.send_log(entry("2"));
        inner.send_log(entry("3"));
        assert_eq!(inner.dropped_logs.load(Ordering::Relaxed), 2);

        // drain one slot: the next send spends it on the notice entry,
        // so the fresh entry is dropped and counted again
        let first = rx.recv().await.unwrap();
        match first {
            OpLogMessage::Log(data) => assert_eq!(data.log, "1"),
            _ => panic!("expected a Log message"),
        }
        inner.send_log(entry("4"));
        let notice = rx.recv().await.unwrap();
        match notice {
            OpLogMessage::Log(data) => {
                assert_eq!(data.log, "[op-log] dropped 2 log entries (channel full)")
            }
            _ => panic!("expected a Log message"),
        }
        assert_eq!(inner.dropped_logs.load(Ordering::Relaxed), 1);
    }

    // One test covers both: definition registration and log routing via info!.
    // Merging avoids the constraint that log::set_logger can be called only once per process —
    // a second def_system_log call in the same process would install on a dead OpLog.
    #[tokio::test]
    async fn def_system_log_registers_definition_and_routes_log() {
        let op = OpLog::new();

        op.def_system_log(
            OpLogDefinition::new("log", ".")
                .log_type(OpLogType::PerHour)
                .flush_interval(Duration::from_secs(60)),
        );

        log::info!("hello from info! macro");

        // Give the async worker time to dequeue the channel message.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // get_info captures number_of_logs before it flushes to disk.
        let info = op.get_info().await.unwrap();
        assert_eq!(info.number_of_definitions, 1);
        assert!(info.number_of_logs >= 1, "info! must route to OpLog");

        op.shutdown().await;
    }

    // The worker can be busy (a slow disk write, a flush) long enough for
    // producers to fill the channel. `shutdown()` must still deliver its stop
    // signal — a dropped stop leaves the caller awaiting a worker that never
    // ends. The current_thread test runtime doesn't poll the worker until we
    // await, so the channel is guaranteed full when shutdown is called.
    #[tokio::test]
    async fn shutdown_completes_even_when_channel_is_full() {
        let op = OpLog::new();
        for i in 0..(CHANNEL_CAPACITY + 16) {
            op.log("nobody", Utc::now(), &format!("entry {i}"));
        }

        tokio::time::timeout(Duration::from_secs(2), op.shutdown())
            .await
            .expect("shutdown must finish even if the channel was full when called");
    }
}
