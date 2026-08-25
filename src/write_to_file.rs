// -------------------------------------------------------------------------------------------------
//   Copyright 2024-2025 (c) Robert Karpiński
// -------------------------------------------------------------------------------------------------

use crate::messages::OpLogInfo;
use crate::{LogDefinition, LogFile, OpLogWorker};
use bytes::{BufMut, BytesMut};
use flate2::write::ZlibEncoder;
use flate2::Compression;
use rand::random;
use std::io;
use std::io::prelude::*;
use std::path::PathBuf;
use std::time::Duration;
use log::info;
use tokio::fs;
use tokio::fs::{create_dir_all, File};
use tokio::io::AsyncWriteExt;
use tokio::sync::oneshot;
use tokio::time::timeout;

/// Ceiling on a single file write (create_dir_all + open + write + flush).
/// Without this, a stuck syscall (e.g. a volume that briefly wedges) blocks
/// the `.await` forever — and this is the ONE shared task handling every
/// defined log at once, so its hang kills logging for good, until the
/// process is restarted. Measured as the cause of a 10-day logging outage
/// in a consumer of this library (x-ai, incident 2026-08-15..08-25): the
/// process stayed up, the disk had room, no panic in the journal — the
/// worker simply never came back from one `.await`.
///
/// Residual risk, accepted deliberately: `tokio::time::timeout` does not
/// cancel the underlying blocking-pool task, it only stops waiting on it.
/// A timed-out write may still complete later, in the background, and could
/// in principle interleave with a subsequent attempt's append to the same
/// path — the on-disk format is a sequential stream of length-prefixed
/// frames, so an interleaved write could corrupt everything from that point
/// on. This is judged acceptable: it trades an unmeasured, narrow-window
/// risk for the certainty of the alternative (the worker hangs forever,
/// exactly as measured above). Closing it fully would mean rotating to a
/// fresh file after every timeout, which changes the on-disk file layout
/// that downstream decoders (e.g. the `emi-oplog` reader) rely on — judged
/// out of scope for this fix.
const WRITE_TIMEOUT: Duration = Duration::from_secs(15);

impl LogDefinition {
    async fn write_to_file(&mut self, flush_interval: &Duration) {
        for file in self.files.values_mut() {
            // A disk error (no space, revoked permissions) must not kill the
            // worker — a panic here would silently stop ALL logging until
            // restart. Report once per failure episode on stderr and keep
            // going; pending entries stay queued and retry on the next tick.
            //
            // A HUNG write (stuck syscall, never returns) is the same class
            // of risk but doesn't go through `Result` at all — wrap the call
            // in a timeout so it always resolves to one.
            let result = match timeout(WRITE_TIMEOUT, file.write_to_file(flush_interval)).await {
                Ok(result) => result,
                Err(_) => Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("write did not complete within {WRITE_TIMEOUT:?}"),
                )),
            };
            match result {
                Ok(()) => file.write_error_logged = false,
                Err(e) => {
                    if !file.write_error_logged {
                        eprintln!(
                            "[op-log] write error for {}/{}: {e}",
                            file.path, file.log_name
                        );
                        file.write_error_logged = true;
                    }
                }
            }
        }
    }

    fn log_count(&self) -> usize {
        self.files.values().map(|f| f.log_count()).sum()
    }
}

impl OpLogWorker {
    fn log_count(&self) -> usize {
        self.definitions.values().map(|def| def.log_count()).sum()
    }

    pub(crate) async fn write_to_files(&mut self) {
        for def in self.definitions.values_mut() {
            let flush_interval = def.flush_interval;
            def.write_to_file(&flush_interval).await
        }
    }

    pub(crate) async fn get_info_and_flush(&mut self, sender: oneshot::Sender<OpLogInfo>) {
        let info = OpLogInfo {
            number_of_definitions: self.definitions.len(),
            number_of_logs: self.log_count(),
        };

        self.flush().await;

        let _ = sender.send(info);
    }

    pub(crate) async fn flush(&mut self) {
        info!(target: "opLog", "flush()");

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);

        loop {
            for def in self.definitions.values_mut() {
                def.write_to_file(&Duration::from_millis(0)).await
            }

            if self.log_count() == 0 || tokio::time::Instant::now() >= deadline {
                break;
            }
        }
    }
}

impl LogFile {
    async fn write_to_file(&mut self, flush_interval: &Duration) -> io::Result<()> {
        if self.logs.is_empty() {
            self.time_of_first_addition_of_log_after_write = None;
            return Ok(());
        }

        if let Some(time) = self.time_of_first_addition_of_log_after_write {
            let diff = time.elapsed();
            if diff < *flush_interval {
                return Ok(());
            }
        }

        let mut bytes = BytesMut::with_capacity(1024);
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());

        let mut path = PathBuf::from(&self.path);
        let _ = create_dir_all(&path).await;

        path.push(&self.log_name);

        // A zero-length file cannot be a valid log (every file starts with
        // the magic) — a crash or a full disk right after `File::create`
        // leaves exactly that behind. Treat it as new, otherwise frames get
        // appended to a file without its magic and readers reject it whole.
        let has_content = fs::metadata(&path).await.map(|m| m.len() > 0).unwrap_or(false);
        let mut f = if has_content {
            File::options().append(true).open(&path).await?
        } else {
            let mut f = File::create(&path).await?;

            f.write_all(b"OPLog 1.0\n").await?;

            if !self.header.is_empty() {
                bytes.put(self.header.as_bytes());
                bytes.put_u8(0x0a);

                encoder.write_all(&bytes)?;
                bytes.clear();
            }

            f
        };

        // Encode by INDEX, not by draining `self.logs` — the disk write below
        // can still be interrupted by the caller's `tokio::time::timeout`. If
        // entries were popped here, a timed-out write would lose them for
        // good (they'd exist only in `bytes`/`encoder`, dropped with the
        // future). Only remove what was actually written once the write
        // below has succeeded (see `self.logs.drain(..consumed)` further
        // down) — a timeout before that point leaves `self.logs` untouched,
        // so the next tick retries the same entries in full.
        let mut consumed = 0usize;
        loop {
            bytes.clear();

            while let Some(log) = self.logs.get(consumed) {
                bytes.put(log.as_bytes());
                bytes.put_u8(0x0a);
                consumed += 1;

                if bytes.len() > 64000 {
                    break;
                }
            }

            if bytes.is_empty() {
                break;
            }
            let b = bytes.split();

            encoder.write_all(&b)?;
            if encoder.get_ref().len() > 2 * 1024 * 1024 {
                break;
            }
        }

        let mut a = encoder.finish()?;
        let mut size = a.len();

        let rnd: u8 = random();

        let mut sum = 0u32;
        let mut xor: u32 = (rnd as u32 * size as u32) & 0xFFF;

        // encrypt
        for byte in a.iter_mut() {
            sum += *byte as u32;
            sum &= 0xff;

            xor *= 2903;
            xor += 71;

            xor &= 0xfff;

            *byte ^= (xor & 0xff) as u8;
        }

        bytes.clear();
        bytes.put_u8(0xff);
        bytes.put_u8(rnd);
        bytes.put_u8((sum as u8) ^ 0x5c);

        loop {
            let mut a: u8 = (size & 0x7F) as u8;
            size >>= 7;
            if size != 0 {
                a |= 0x80
            };

            bytes.put_u8(a ^ 0xc5);
            if size == 0 {
                break;
            }
        }

        f.write_all(&bytes).await?;
        f.write_all(&a).await?;
        f.flush().await?;

        // Only now, after the write is confirmed on disk, drop the entries
        // that were actually encoded above — a timeout anywhere before this
        // line leaves them in `self.logs` for the next attempt.
        self.logs.drain(..consumed);

        if self.logs.is_empty() {
            self.time_of_first_addition_of_log_after_write = None;
        }

        Ok(())
    }

    fn log_count(&self) -> usize {
        self.logs.len()
    }
}

#[cfg(test)]
mod tests {
    use crate::OpLogWorker;
    use crate::messages::OpLogType;
    use chrono::Utc;
    use std::collections::HashSet;
    use std::io::Read;
    use std::path::PathBuf;
    use std::time::Duration;

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("op-log-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn worker_with_no_split_definition(dir: &std::path::Path, header: &str) -> OpLogWorker {
        worker_with_flush_interval(dir, header, Duration::from_secs(10))
    }

    fn worker_with_flush_interval(
        dir: &std::path::Path,
        header: &str,
        flush_interval: Duration,
    ) -> OpLogWorker {
        let (_tx, rx) = tokio::sync::mpsc::channel(32);
        let mut worker = OpLogWorker::new(rx);
        worker.def(
            "test",
            dir.to_str().unwrap(),
            OpLogType::NoSplit,
            &HashSet::new(),
            flush_interval,
            header,
            false,
        );
        worker
    }

    fn queued_file(worker: &OpLogWorker) -> &crate::LogFile {
        let files = &worker.definitions["test"].files;
        assert_eq!(files.len(), 1, "the definition must have exactly one file");
        files.values().next().unwrap()
    }

    // Independent decoder of the on-disk format, written from the README's
    // description (the same contract the external `emi-oplog` reader relies
    // on): magic, then `[0xFF][rnd][checksum][size VLQ ^ 0xC5][zlib ^ key]`.
    // Returns the text of every block.
    fn decode_oplog_file(raw: &[u8]) -> Vec<String> {
        const MAGIC: &[u8] = b"OPLog 1.0
";
        assert!(raw.starts_with(MAGIC), "file must start with the OPLog 1.0 magic");
        let mut pos = MAGIC.len();
        let mut blocks = Vec::new();
        while pos < raw.len() {
            assert_eq!(raw[pos], 0xff, "block marker expected at offset {pos}");
            let rnd = raw[pos + 1];
            let checksum = raw[pos + 2] ^ 0x5c;
            pos += 3;

            let mut size = 0usize;
            let mut shift = 0;
            loop {
                let b = raw[pos] ^ 0xc5;
                pos += 1;
                size |= ((b & 0x7f) as usize) << shift;
                if b & 0x80 == 0 {
                    break;
                }
                shift += 7;
            }

            let mut payload = raw[pos..pos + size].to_vec();
            pos += size;

            let mut xor: u32 = (rnd as u32 * size as u32) & 0xfff;
            let mut sum = 0u32;
            for byte in payload.iter_mut() {
                xor = (xor * 2903 + 71) & 0xfff;
                *byte ^= (xor & 0xff) as u8;
                sum = (sum + *byte as u32) & 0xff;
            }
            assert_eq!(sum as u8, checksum, "block checksum must match");

            let mut text = String::new();
            flate2::read::ZlibDecoder::new(&payload[..])
                .read_to_string(&mut text)
                .expect("block must be a valid zlib stream of UTF-8 text");
            blocks.push(text);
        }
        blocks
    }

    // Round trip through the real write path: the file on disk must decode
    // back to the header and the entry, and the queue must be drained.
    #[tokio::test]
    async fn write_to_file() {
        let dir = temp_dir("write");
        let header = "header, jest długi bez z półskimi liter ŻĄŁ";
        let entry = "log, to ładny i ŻAŁOŚĆ to słowo";
        let mut op_log = worker_with_no_split_definition(&dir, header);

        op_log.log("test", Utc::now(), entry);
        assert_eq!(op_log.log_count(), 1);
        op_log.flush().await;
        assert_eq!(op_log.log_count(), 0, "flush must drain the queue");

        let raw = std::fs::read(dir.join("test.log")).unwrap();
        assert_eq!(decode_oplog_file(&raw), vec![format!("{header}
{entry}
")]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    // A zero-length file at the log's path is what a crash or a full disk
    // right after `File::create` leaves behind. It must be treated as a new
    // file: appending frames to it would produce a file without the magic,
    // which downstream readers reject as a whole. A non-empty file must still
    // be appended to, never truncated.
    #[tokio::test]
    async fn empty_existing_file_is_rewritten_with_the_magic() {
        let dir = temp_dir("empty");
        std::fs::write(dir.join("test.log"), b"").unwrap();
        let mut op_log = worker_with_no_split_definition(&dir, "");

        op_log.log("test", Utc::now(), "first");
        op_log.flush().await;
        let raw = std::fs::read(dir.join("test.log")).unwrap();
        assert_eq!(decode_oplog_file(&raw), vec!["first
".to_string()]);

        op_log.log("test", Utc::now(), "second");
        op_log.flush().await;
        let raw = std::fs::read(dir.join("test.log")).unwrap();
        assert_eq!(
            decode_oplog_file(&raw),
            vec!["first
".to_string(), "second
".to_string()],
            "a non-empty file must be appended to"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    // The promise behind `write_error_logged`: a write that fails (no space,
    // revoked permissions, a path that cannot be created) must neither kill
    // the worker nor lose the entries — they stay queued, the error is
    // reported once, and the first successful write after the cause is gone
    // delivers them and ends the episode. The failure is injected by making
    // the log directory's parent a regular FILE, which fails on every
    // platform; removing that file is "the cause is gone".
    #[tokio::test]
    async fn write_error_keeps_entries_and_the_next_successful_write_delivers_them() {
        let dir = temp_dir("retry");
        let blocker = dir.join("blocker");
        std::fs::write(&blocker, b"not a directory").unwrap();
        let log_dir = blocker.join("logs");
        let mut op_log = worker_with_flush_interval(&log_dir, "", Duration::from_millis(0));

        op_log.log("test", Utc::now(), "entry");

        // one writer tick: the write fails, the entry survives, the episode is open
        op_log.write_to_files().await;
        assert_eq!(op_log.log_count(), 1, "a failed write must keep the entry queued");
        assert!(queued_file(&op_log).write_error_logged, "the failure must open an error episode");

        // a second failing tick must not lose the entry either
        op_log.write_to_files().await;
        assert_eq!(op_log.log_count(), 1, "repeated failures must keep the entry queued");

        // the cause is gone: the next tick writes and closes the episode
        std::fs::remove_file(&blocker).unwrap();
        op_log.write_to_files().await;
        assert_eq!(op_log.log_count(), 0, "the first successful write must drain the queue");
        assert!(!queued_file(&op_log).write_error_logged, "a successful write must close the episode");

        let raw = std::fs::read(log_dir.join("test.log")).unwrap();
        assert_eq!(decode_oplog_file(&raw), vec!["entry\n".to_string()]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    // Pins the constant's value: a future change to the ceiling must be a
    // deliberate decision, not an accidental side effect of a refactor.
    #[test]
    fn write_timeout_constant_matches_documented_value() {
        assert_eq!(super::WRITE_TIMEOUT, Duration::from_secs(15));
    }

    // Simulates a stuck write syscall: a future that never completes. A
    // genuinely stuck disk can't be simulated deterministically without
    // mocking the filesystem — this checks the mechanism the whole fix
    // relies on, with a short local duration so the test stays fast.
    #[tokio::test]
    async fn timeout_wraps_a_stuck_operation_and_errors_out() {
        let stuck = std::future::pending::<std::io::Result<()>>();
        let result = tokio::time::timeout(Duration::from_millis(20), stuck).await;
        assert!(
            result.is_err(),
            "timeout must interrupt an operation that never completes, \
             otherwise the worker hangs forever"
        );
    }
}
