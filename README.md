# op-log

An async file logging library for Rust built on Tokio. Log entries are compressed (zlib) and obfuscated before being written to a binary file in the `OPLog 1.0` format.

## Features

- Async I/O via Tokio
- Thread-safe `OpLog` handle — clone freely, all clones share one background worker
- zlib compression + lightweight XOR obfuscation of stored data
- Automatic log file splitting by time period: hour, day, month, or no split
- Optional subdirectories per period (`UseSubDirectories`)
- Configurable write interval (`flush_interval`)
- File header written once when a new log file is created
- Timestamps in the Europe/Warsaw timezone
- Bounded memory while the disk is unavailable: at most 8 MB of pending entries per file, the oldest are dropped, counted and reported into the log once writes succeed again
- Optional automatic removal of inactive definitions (`auto_remove_definition`, unused for >10 min)
- Automatic deletion of old files after N days

## Dependencies

```toml
[dependencies]
op-log = { path = "..." }
tokio = { version = "1", features = ["full"] }
```

## Quick start

Create an `OpLog` handle — the background worker is spawned automatically. The handle is cheaply cloneable; all clones share the same worker.

```rust
use op_log::{OpLog, OpLogDefinition, OpLogType};
use chrono::Utc;
use std::time::Duration;

#[tokio::main]
async fn main() {
    let log = OpLog::new();

    // Register a log definition
    let def = OpLogDefinition::new("app", "./logs")
        .log_type(OpLogType::PerDay)
        .flush_interval(Duration::from_secs(1))
        .header("timestamp, message");

    log.def(def)
       .log("app", Utc::now(), "application started");

    // Flush and shut down, waiting for pending writes to complete
    log.shutdown().await;
}
```

## API (`OpLog`)

| Method | Description |
|---|---|
| `OpLog::new()` | Spawns the background worker and returns a handle |
| `def(OpLogDefinition)` | Registers or updates a log definition |
| `log(name, date, text)` | Appends a single entry to a log |
| `log_bundle(OpLogBundle)` | Batch: definitions + entries in one call |
| `flush()` | Signals the worker to flush pending writes (fire-and-forget) |
| `get_info()` | Flushes and returns current stats (`Option<OpLogInfo>`) |
| `shutdown()` | Flushes, shuts down the worker, and waits for it to finish |
| `clean_up_definition(def)` | Registers or updates a cleanup rule for a log path |
| `clean_up_bundle(rules)` | Replaces the full set of cleanup rules at once |
| `clean_up_remove_all_definitions()` | Removes all cleanup rules |

All methods except `get_info` and `shutdown` are non-blocking and return `&OpLog` for chaining.

## Log configuration (`OpLogDefinition`)

```rust
use op_log::{OpLogDefinition, OpLogType, OpLogOption};
use std::collections::HashSet;
use std::time::Duration;

let def = OpLogDefinition::new("my_log", "/path/to/logs")
    .log_type(OpLogType::PerDay)
    .flush_interval(Duration::from_secs(2))
    .header("col1, col2, col3")
    .options(HashSet::from([OpLogOption::UseSubDirectories]));
```

### File split types (`OpLogType`)

| Type | File name |
|---|---|
| `NoSplit` | `name.log` |
| `PerHour` | `name_2025_01_15_14.log` |
| `PerDay` | `name_2025_01_15.log` (default) |
| `PerMonth` | `name_2025_01.log` |

### Options (`OpLogOption`)

| Option | Description |
|---|---|
| `UseSubDirectories` | Groups files in subdirectories by period instead of a name suffix |
| `NoAddDateToLog` | Does not prepend a timestamp to each log entry |

## Automatic file cleanup

```rust
use op_log::OpLogCleanUpDefinition;

log.clean_up_definition(OpLogCleanUpDefinition {
    path: "./logs".to_string(),
    delete_after_days: 30,
});
```

The worker runs cleanup every 5 minutes, deleting files (and empty directories) older than the specified number of days.

## File format

Each `.log` file starts with the ASCII header `OPLog 1.0\n`, followed by data blocks:

```
[0xFF] [rnd] [checksum] [size (VLQ, XOR 0xC5)] [zlib+XOR data]
```

The data inside each block is UTF-8 text compressed with zlib, then obfuscated with XOR using a pseudorandom key derived from `rnd` and the data length.

A write cut short (disk full mid-block, a crash between the prefix and the data) can leave a file ending inside a block. Before appending to an existing file, the worker walks its blocks and cuts such an incomplete tail back to the last block boundary, so readers never meet a block followed by frames it claims to contain. Files larger than 64 MB are appended to without this check.

## License

Copyright 2024-2025 Robert Karpiński
