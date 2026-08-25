// -------------------------------------------------------------------------------------------------
//   Copyright 2024-2025 (c) Robert Karpiński
// -------------------------------------------------------------------------------------------------

use crate::{LogDefinition, LogFile, OpLogWorker};
use crate::messages::{OpLogOption, OpLogType};
use chrono::{DateTime, Utc};
use chrono_tz::Europe::Warsaw;
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

        let date_in_log_format = format_date_in_log(&def.log_type);
        let date_in_log = date.format(date_in_log_format).to_string();
        let log = if date_in_log.is_empty() || def.options.contains(&OpLogOption::NoAddDateToLog) {
            log.trim().to_string()
        } else {
            format!("{date_in_log} {log}")
        };

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

        log_file.logs.push_back(log);
    }
}

#[cfg(test)]
mod tests {
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
