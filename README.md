# op-log

An async file logging library for Rust built on Tokio. Log entries are compressed (zlib) and obfuscated before being written to a binary file in the `OPLog 1.0` format.

## Features

- Async I/O via Tokio
- Message-passing via `mpsc` channel — thread-safe by design
- zlib compression + lightweight XOR obfuscation of stored data
- Automatic log file splitting by time period: hour, day, month, or no split
- Optional subdirectories per period (`UseSubDirectories`)
- Configurable write interval (`flush_interval`)
- File header written once when a new log file is created
- Timestamps in the Europe/Warsaw timezone
- Automatic cleanup of inactive definitions (unused for >10 min)
- Automatic deletion of old files after N days

## Dependencies

```toml
[dependencies]
op-log = { path = "..." }
tokio = { version = "1", features = ["full"] }
```

## Quick start

`op-log` works through a message channel. Spawn the worker in the background and send commands to it.

```rust
use op_log::messages::{
    OpLogBundle, OpLogData, OpLogDefinition, OpLogMessage, OpLogOption, OpLogType,
};
use chrono::Utc;
use std::collections::HashSet;
use std::time::Duration;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() {
    let (tx, rx) = mpsc::channel::<OpLogMessage>(1024);

    // Spawn the worker in the background
    tokio::spawn(async move {
        let mut worker = op_log::OpLogWorker::new(rx);
        worker.run().await;
    });

    // Register a log definition
    let mut def = OpLogDefinition::new("app", "./logs");
    def.log_type(OpLogType::PerDay)
       .flush_interval(Duration::from_secs(1))
       .header("timestamp, message");

    tx.send(OpLogMessage::LogDefinition(def)).await.unwrap();

    // Write a log entry
    tx.send(OpLogMessage::Log(OpLogData {
        log_name: "app".to_string(),
        log: "application started".to_string(),
        date: Utc::now(),
    })).await.unwrap();

    // Stop the service
    tx.send(OpLogMessage::StopService).await.unwrap();
}
```

## Messages (`OpLogMessage`)

| Message | Description |
|---|---|
| `LogDefinition(OpLogDefinition)` | Registers or updates a log definition |
| `Log(OpLogData)` | Appends a single entry to a log |
| `LogBundle(OpLogBundle)` | Batch: definitions + entries in one message |
| `Flush` | Forces all buffered logs to be written to disk |
| `GetInfoAndFlush(sender)` | Returns stats and performs a flush |
| `StopService` | Flushes and shuts down the worker |
| `CleanUpDefinition(...)` | Adds a rule for deleting old files |
| `CleanUpBundle(...)` | Replaces the full set of cleanup rules |
| `CleanUpRemoveAllDefinitions` | Removes all cleanup rules |

## Log configuration (`OpLogDefinition`)

```rust
let mut def = OpLogDefinition::new("my_log", "/path/to/logs");
def.log_type(OpLogType::PerDay)
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
| `UseSubDirectories` | Groups files in subdirectories by period instead of using a name suffix |
| `NoAddDateToLog` | Does not prepend a timestamp to each log entry |

## Automatic file cleanup

```rust
use op_log::messages::{OpLogCleanUpDefinition, OpLogMessage};

tx.send(OpLogMessage::CleanUpDefinition(OpLogCleanUpDefinition {
    path: "./logs".to_string(),
    delete_after_days: 30,
})).await.unwrap();
```

The worker runs cleanup every 5 minutes, deleting files (and empty directories) older than the specified number of days.

## File format

Each `.log` file starts with the ASCII header `OPLog 1.0\n`, followed by data blocks:

```
[0xFF] [rnd] [checksum] [size (VLQ, XOR 0xC5)] [zlib+XOR data]
```

The data inside each block is UTF-8 text compressed with zlib, then obfuscated with XOR using a pseudorandom key derived from `rnd` and the data length.

## License

Copyright 2024-2025 Robert Karpiński