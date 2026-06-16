// -------------------------------------------------------------------------------------------------
//   Copyright 2024-2025 (c) Robert Karpiński
// -------------------------------------------------------------------------------------------------

use chrono::{DateTime, Utc};
use std::collections::HashSet;
use std::time::Duration;
use tokio::sync::oneshot;

pub struct OpLogInfo {
    pub number_of_definitions: usize,
    pub number_of_logs: usize,
}

#[derive(Clone, Copy)]
pub enum OpLogType {
    NoSplit,
    PerHour,
    PerDay,
    PerMonth,
}

#[derive(Clone, Copy, Hash, Eq, PartialEq)]
pub enum OpLogOption {
    NoAddDateToLog,
    UseSubDirectories,
}

pub struct OpLogDefinition {
    pub log_name: String,
    pub log_type: OpLogType,
    pub header: String,
    pub path: String,
    pub options: HashSet<OpLogOption>,
    pub flush_interval: Duration,
}

impl OpLogDefinition {
    pub fn new(log_name: &str, path: &str) -> OpLogDefinition {
        OpLogDefinition {
            log_name: log_name.to_string(),
            path: path.to_string(),
            log_type: OpLogType::PerDay,
            header: "".to_string(),
            options: HashSet::new(),
            flush_interval: Duration::from_secs(1),
        }
    }

    pub fn header(mut self, header: &str) -> OpLogDefinition {
        self.header = header.to_string();
        self
    }

    pub fn log_type(mut self, log_type: OpLogType) -> OpLogDefinition {
        self.log_type = log_type;
        self
    }

    pub fn options(mut self, options: HashSet<OpLogOption>) -> OpLogDefinition {
        self.options = options;
        self
    }

    pub fn flush_interval(mut self, timeout: Duration) -> OpLogDefinition {
        self.flush_interval = timeout;
        self
    }

    pub fn path(mut self, path: &str) -> OpLogDefinition {
        self.path = path.to_string();
        self
    }
}

pub struct OpLogData {
    pub log_name: String,
    pub log: String,
    pub date: DateTime<Utc>,
}

#[derive(Default)]
pub struct OpLogBundle {
    pub definitions: Vec<OpLogDefinition>,
    pub logs: Vec<OpLogData>,
}

impl OpLogBundle {
    pub fn new() -> OpLogBundle {
        Default::default()
    }

    pub fn add_definition(mut self, def: OpLogDefinition) -> OpLogBundle {
        self.definitions.push(def);
        self
    }

    pub fn add_log(mut self, log: OpLogData) -> OpLogBundle {
        self.logs.push(log);
        self
    }
}

pub struct OpLogCleanUpDefinition {
    pub path: String,
    pub delete_after_days: u32,
}

#[allow(dead_code)]
pub enum OpLogMessage {
    Flush,
    GetInfoAndFlush(oneshot::Sender<OpLogInfo>),
    StopService,
    LogBundle(OpLogBundle),
    LogDefinition(OpLogDefinition),
    Log(OpLogData),

    CleanUpRemoveAllDefinitions,
    CleanUpDefinition(OpLogCleanUpDefinition),
    CleanUpBundle(Vec<OpLogCleanUpDefinition>),
}
