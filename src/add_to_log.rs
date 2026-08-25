// -------------------------------------------------------------------------------------------------
//   Copyright 2024-2025 (c) Robert Karpiński
// -------------------------------------------------------------------------------------------------

use crate::{LogDefinition, LogFile, OpLogWorker};
use crate::messages::{OpLogOption, OpLogType};
use chrono::{DateTime, Utc};
use chrono_tz::Europe::Warsaw;
use chrono_tz::Tz;
use std::collections::{hash_map::Entry, HashMap, HashSet, VecDeque};
use std::time::Duration;
use tokio::time::Instant;

fn format_date_in_log<'a>(log_type: &OpLogType) -> &'a str {
    match log_type {
        OpLogType::NoSplit => "",
        OpLogType::PerHour => "%H:%M:%S.%3f",
        OpLogType::PerDay => "%H:%M:%S.%3f",
        OpLogType::PerMonth => "%d %H:%M:%S.%3f",
    }
}

/// Ceiling on the bytes queued for one file. Entries pile up in memory
/// while writes fail (no space left, a wedged volume) — the channel only
/// buffers the way in, the worker drains it into the queue regardless. A
/// long episode would otherwise grow the process without bound. Above the
/// cap the OLDEST entries go: the newest ones describe what the process is
/// doing now. Drops are counted and reported into the log itself once a
/// write succeeds again, like `dropped_logs` does for a full channel.
pub(crate) const QUEUE_MAX_BYTES: usize = 8 * 1024 * 1024;

/// Formats an entry the way it is stored in the queue and written to disk:
/// with the period's timestamp prefix, unless the definition opts out.
pub(crate) fn format_entry(
    log_type: &OpLogType,
    no_date: bool,
    date: &DateTime<Tz>,
    text: &str,
) -> String {
    let date_in_log = date.format(format_date_in_log(log_type)).to_string();
    if date_in_log.is_empty() || no_date {
        text.trim().to_string()
    } else {
        format!("{date_in_log} {text}")
    }
}

fn format_date_in_path<'a>(log_type: &OpLogType) -> &'a str {
    match log_type {
        OpLogType::NoSplit => "",
        OpLogType::PerHour => "%Y_%m_%d_%H",
        OpLogType::PerDay => "%Y_%m_%d",
        OpLogType::PerMonth => "%Y_%m",
    }
}

impl OpLogWorker {
    pub(crate) fn def(
        &mut self,
        log_name: &str,
        path: &str,
        log_type: OpLogType,
        options: &HashSet<OpLogOption>,
        flush_interval: Duration,
        header: &str,
        auto_remove_definition: bool,
    ) {
        match self.definitions.entry(log_name.to_string()) {
            Entry::Occupied(mut e) => {
                let file = e.get_mut();
                file.path = path.to_string();
                file.log_type = log_type;
                file.options = options.clone();
                file.flush_interval = flush_interval;
                file.header = header.to_string();
                file.auto_remove_definition = auto_remove_definition;
            }
            Entry::Vacant(e) => {
                e.insert(LogDefinition {
                    last_time_use: Instant::now(),
                    auto_remove_definition,
                    path: path.to_string(),
                    flush_interval,
                    log_type,
                    options: options.clone(),
                    header: header.to_string(),
                    files: HashMap::new(),
                });
            }
        }
    }

    pub(crate) fn log(&mut self, log_name: &str, date: DateTime<Utc>, log: &str) {
        let Some(def) = self.definitions.get_mut(log_name) else {
            return;
        };

        def.last_time_use = Instant::now();

        let date = date.with_timezone(&Warsaw);

        let no_date = def.options.contains(&OpLogOption::NoAddDateToLog);
        let log = format_entry(&def.log_type, no_date, &date, log);

        let date_in_path_format = format_date_in_path(&def.log_type);
        let date_in_path = date.format(date_in_path_format).to_string();

        let (log_name, path) = if date_in_path.is_empty() {
            (format!("{}.log", log_name), def.path.to_string())
        } else {
            if def.options.contains(&OpLogOption::UseSubDirectories) {
                (
                    format!("{}.log", log_name),
                    format!("{}/{}", def.path, date_in_path),
                )
            } else {
                (
                    format!("{}_{}.log", log_name, date_in_path),
                    def.path.to_string(),
                )
            }
        };

        def.add_to_log(log_name, path, def.header.clone(), log)
    }
}

impl LogDefinition {
    fn get_log_file(&mut self, log_name: String, path: String, header: String) -> &mut LogFile {
        let file_name = format!("{path}/{log_name}");

        match self.files.entry(file_name) {
            Entry::Occupied(e) => {
                let file = e.into_mut();
                if file.header != header {
                    file.header = header;
                }
                file
            }
            Entry::Vacant(e) => e.insert(LogFile {
                last_time_use: Instant::now(),
                time_of_first_addition_of_log_after_write: None,
                log_name,
                path,
                header,
                logs: VecDeque::new(),
                queued_bytes: 0,
                dropped_logs: 0,
                write_error_logged: false,
                tail_verified: false,
            }),
        }
    }

    fn add_to_log(&mut self, log_name: String, path: String, header: String, log: String) {
        let log_file = self.get_log_file(log_name, path, header);

        log_file.last_time_use = Instant::now();

        if log_file.time_of_first_addition_of_log_after_write.is_none() {
            log_file.time_of_first_addition_of_log_after_write = Some(Instant::now());
        }

        log_file.queued_bytes += log.len();
        log_file.logs.push_back(log);

        // Stay under the cap by dropping the oldest entries. The entry just
        // added always survives, even if it is larger than the cap alone.
        while log_file.queued_bytes > QUEUE_MAX_BYTES && log_file.logs.len() > 1 {
            if let Some(dropped) = log_file.logs.pop_front() {
                log_file.queued_bytes -= dropped.len();
                log_file.dropped_logs += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::QUEUE_MAX_BYTES;
    use crate::OpLogWorker;
    use crate::messages::{OpLogOption, OpLogType};
    use chrono::Utc;
    use std::collections::HashSet;
    use std::time::Duration;

    fn define(worker: &mut OpLogWorker, options: &HashSet<OpLogOption>, auto_remove: bool) {
        worker.def(
            "app",
            "logs",
            OpLogType::PerDay,
            options,
            Duration::from_secs(1),
            "",
            auto_remove,
        );
    }

    // `NoAddDateToLog` is documented as "does not prepend a timestamp to each
    // log entry" — the queued entry must be the caller's text as is.
    #[tokio::test]
    async fn no_add_date_option_leaves_the_entry_without_timestamp() {
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let mut worker = OpLogWorker::new(rx);
        define(&mut worker, &HashSet::from([OpLogOption::NoAddDateToLog]), false);

        worker.log("app", Utc::now(), "entry without a timestamp");

        let queued: Vec<&String> = worker.definitions["app"]
            .files
            .values()
            .flat_map(|f| f.logs.iter())
            .collect();
        assert_eq!(queued, vec!["entry without a timestamp"]);
    }

    // While writes fail the queue must not grow without bound: above the cap
    // the oldest entries are dropped and counted, the newest are kept.
    #[tokio::test]
    async fn queue_stays_under_the_cap_by_dropping_the_oldest_entries() {
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let mut worker = OpLogWorker::new(rx);
        define(&mut worker, &HashSet::new(), false);

        let payload = "x".repeat(1024 * 1024);
        for i in 0..10 {
            worker.log("app", Utc::now(), &format!("{i}:{payload}"));
        }

        let file = worker.definitions["app"].files.values().next().unwrap();
        assert!(
            file.queued_bytes <= QUEUE_MAX_BYTES,
            "queued bytes {} must stay under the cap {QUEUE_MAX_BYTES}",
            file.queued_bytes
        );
        assert_eq!(
            file.queued_bytes,
            file.logs.iter().map(String::len).sum::<usize>(),
            "the byte count must match the queue"
        );
        assert_eq!(file.dropped_logs, 3, "three 1 MB entries plus prefixes exceed 8 MB");
        assert_eq!(file.logs.len(), 7);
        assert!(file.logs.front().unwrap().contains(" 3:"), "the oldest entries go first");
        assert!(file.logs.back().unwrap().contains(" 9:"), "the newest entry always survives");
    }

    // `def()` on an existing name is documented as an update: every field of
    // the definition must follow the latest call, not just some of them.
    #[tokio::test]
    async fn redefining_updates_options_and_auto_remove() {
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let mut worker = OpLogWorker::new(rx);
        define(&mut worker, &HashSet::from([OpLogOption::UseSubDirectories]), false);
        define(&mut worker, &HashSet::new(), true);

        let def = &worker.definitions["app"];
        assert!(def.options.is_empty(), "options must follow the latest definition");
        assert!(
            def.auto_remove_definition,
            "auto_remove_definition must follow the latest definition"
        );

        worker.log("app", Utc::now(), "entry");
        let keys: Vec<&String> = worker.definitions["app"].files.keys().collect();
        assert_eq!(keys.len(), 1);
        assert!(
            keys[0].starts_with("logs/app_"),
            "without UseSubDirectories the date belongs in the file name: {}",
            keys[0]
        );
    }
}
