// -------------------------------------------------------------------------------------------------
//   Copyright 2024-2025 (c) Robert Karpiński
// -------------------------------------------------------------------------------------------------

use crate::{LogDefinition, LogFile, OpLogWorker};
use crate::messages::{OpLogDefinition, OpLogOption, OpLogType};
use chrono::format::{Fixed, Item, Numeric, Pad};
use chrono::{DateTime, Utc};
use chrono_tz::Europe::Warsaw;
use chrono_tz::Tz;
use std::collections::{hash_map::Entry, HashMap, VecDeque};
use tokio::time::Instant;

// The date formats below are the `strftime` strings "%H:%M:%S.%3f",
// "%d %H:%M:%S.%3f", "%Y_%m_%d_%H", "%Y_%m_%d" and "%Y_%m", already parsed
// into chrono's items. chrono parses a format string on every call, and
// that parse (twice per entry: the timestamp and the file period) was a
// quarter of the worker's cost per entry. The output is the same byte for
// byte; `Fixed::Nanosecond3` prints the dot that "%3f" leaves to the
// literal before it.
const TIME_IN_LOG: &[Item<'static>] = &[
    Item::Numeric(Numeric::Hour, Pad::Zero),
    Item::Literal(":"),
    Item::Numeric(Numeric::Minute, Pad::Zero),
    Item::Literal(":"),
    Item::Numeric(Numeric::Second, Pad::Zero),
    Item::Fixed(Fixed::Nanosecond3),
];

const DAY_AND_TIME_IN_LOG: &[Item<'static>] = &[
    Item::Numeric(Numeric::Day, Pad::Zero),
    Item::Space(" "),
    Item::Numeric(Numeric::Hour, Pad::Zero),
    Item::Literal(":"),
    Item::Numeric(Numeric::Minute, Pad::Zero),
    Item::Literal(":"),
    Item::Numeric(Numeric::Second, Pad::Zero),
    Item::Fixed(Fixed::Nanosecond3),
];

const HOUR_IN_PATH: &[Item<'static>] = &[
    Item::Numeric(Numeric::Year, Pad::Zero),
    Item::Literal("_"),
    Item::Numeric(Numeric::Month, Pad::Zero),
    Item::Literal("_"),
    Item::Numeric(Numeric::Day, Pad::Zero),
    Item::Literal("_"),
    Item::Numeric(Numeric::Hour, Pad::Zero),
];

const DAY_IN_PATH: &[Item<'static>] = &[
    Item::Numeric(Numeric::Year, Pad::Zero),
    Item::Literal("_"),
    Item::Numeric(Numeric::Month, Pad::Zero),
    Item::Literal("_"),
    Item::Numeric(Numeric::Day, Pad::Zero),
];

const MONTH_IN_PATH: &[Item<'static>] = &[
    Item::Numeric(Numeric::Year, Pad::Zero),
    Item::Literal("_"),
    Item::Numeric(Numeric::Month, Pad::Zero),
];

fn format_date_in_log(log_type: &OpLogType) -> &'static [Item<'static>] {
    match log_type {
        OpLogType::NoSplit => &[],
        OpLogType::PerHour => TIME_IN_LOG,
        OpLogType::PerDay => TIME_IN_LOG,
        OpLogType::PerMonth => DAY_AND_TIME_IN_LOG,
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
    let date_format = format_date_in_log(log_type);
    if date_format.is_empty() || no_date {
        return text.trim().to_string();
    }
    // One allocation for the whole entry: the timestamp is written straight
    // into it. Going through `Display` (`format!("{} {text}", ..)`) would
    // format the timestamp into a String of its own first, then grow the
    // entry from the size of the literal pieces.
    let mut entry = String::with_capacity(text.len() + 16);
    write_date(&mut entry, date, date_format);
    entry.push(' ');
    entry.push_str(text);
    entry
}

/// Appends `date` formatted with `format` to `out`. The formats used here
/// are constants without an error item, so the formatting cannot fail.
fn write_date(out: &mut String, date: &DateTime<Tz>, format: &[Item<'static>]) {
    date.format_with_items(format.iter()).write_to(out).expect("constant date format")
}

fn format_date_in_path(log_type: &OpLogType) -> &'static [Item<'static>] {
    match log_type {
        OpLogType::NoSplit => &[],
        OpLogType::PerHour => HOUR_IN_PATH,
        OpLogType::PerDay => DAY_IN_PATH,
        OpLogType::PerMonth => MONTH_IN_PATH,
    }
}

impl OpLogWorker {
    pub(crate) fn def(&mut self, def: OpLogDefinition) {
        let OpLogDefinition {
            log_name,
            log_type,
            header,
            path,
            options,
            flush_interval,
            auto_remove_definition,
        } = def;
        match self.definitions.entry(log_name) {
            Entry::Occupied(mut e) => {
                let file = e.get_mut();
                file.path = path;
                file.log_type = log_type;
                file.options = options;
                file.flush_interval = flush_interval;
                file.header = header;
                file.auto_remove_definition = auto_remove_definition;
            }
            Entry::Vacant(e) => {
                e.insert(LogDefinition {
                    last_time_use: Instant::now(),
                    auto_remove_definition,
                    path,
                    flush_interval,
                    log_type,
                    options,
                    header,
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

        let (log_name, path) = if date_in_path_format.is_empty() {
            (format!("{}.log", log_name), def.path.to_string())
        } else {
            // The period is written straight into the name or the path
            // (see `format_entry`), not through `Display`.
            if def.options.contains(&OpLogOption::UseSubDirectories) {
                let mut path = String::with_capacity(def.path.len() + 16);
                path.push_str(&def.path);
                path.push('/');
                write_date(&mut path, &date, date_in_path_format);
                (format!("{}.log", log_name), path)
            } else {
                let mut log_name_in_period = String::with_capacity(log_name.len() + 20);
                log_name_in_period.push_str(log_name);
                log_name_in_period.push('_');
                write_date(&mut log_name_in_period, &date, date_in_path_format);
                log_name_in_period.push_str(".log");
                (log_name_in_period, def.path.to_string())
            }
        };

        def.add_to_log(log_name, path, log)
    }
}

impl LogDefinition {
    fn get_log_file(&mut self, log_name: String, path: String) -> &mut LogFile {
        let file_name = format!("{path}/{log_name}");

        match self.files.entry(file_name) {
            Entry::Occupied(e) => {
                let file = e.into_mut();
                if file.header != self.header {
                    file.header = self.header.clone();
                }
                file
            }
            Entry::Vacant(e) => e.insert(LogFile {
                last_time_use: Instant::now(),
                time_of_first_addition_of_log_after_write: None,
                log_name,
                path,
                header: self.header.clone(),
                logs: VecDeque::new(),
                queued_bytes: 0,
                dropped_logs: 0,
                write_error_logged: false,
                tail_verified: false,
            }),
        }
    }

    fn add_to_log(&mut self, log_name: String, path: String, log: String) {
        let log_file = self.get_log_file(log_name, path);

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
    use crate::messages::{OpLogDefinition, OpLogOption, OpLogType};
    use chrono::Utc;
    use std::collections::HashSet;
    use std::time::Duration;

    fn define(worker: &mut OpLogWorker, options: &HashSet<OpLogOption>, auto_remove: bool) {
        worker.def(
            OpLogDefinition::new("app", "logs")
                .log_type(OpLogType::PerDay)
                .options(options.clone())
                .flush_interval(Duration::from_secs(1))
                .auto_remove_definition(auto_remove),
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
