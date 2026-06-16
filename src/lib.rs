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
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::Instant;

// -------------------------------------------------------------------------------------------------
// Internal worker types
// -------------------------------------------------------------------------------------------------

struct LogDefinition {
    last_time_use: Instant,
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

struct OpLogInner {
    tx: Sender<OpLogMessage>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

/// Thread-safe handle to the OpLog background worker.
/// Clone freely — all clones share the same worker.
#[derive(Clone)]
pub struct OpLog(std::sync::Arc<OpLogInner>);

impl OpLog {
    /// Spawns the background worker. Must be called inside a Tokio runtime.
    pub fn new() -> OpLog {
        let (tx, rx) = mpsc::channel::<OpLogMessage>(32);
        let worker = OpLogWorker::new(rx);
        let handle = tokio::spawn(async move {
            let mut w = worker;
            w.run().await;
        });
        OpLog(std::sync::Arc::new(OpLogInner {
            tx,
            handle: Mutex::new(Some(handle)),
        }))
    }

    /// Registers a log definition. Non-blocking; drops silently if the channel is full.
    pub fn def(&self, def: OpLogDefinition) -> &OpLog {
        let _ = self.0.tx.try_send(OpLogMessage::LogDefinition(def));
        self
    }

    /// Sends a log entry. Non-blocking; drops silently if the channel is full.
    pub fn log(&self, name: &str, date: DateTime<Utc>, text: &str) -> &OpLog {
        let _ = self.0.tx.try_send(OpLogMessage::Log(OpLogData {
            log_name: name.to_string(),
            log: text.to_string(),
            date,
        }));

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
        let _ = self.0.tx.try_send(OpLogMessage::StopService);
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
}
