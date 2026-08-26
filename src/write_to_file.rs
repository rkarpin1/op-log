// -------------------------------------------------------------------------------------------------
//   Copyright 2024-2025 (c) Robert Karpiński
// -------------------------------------------------------------------------------------------------

use crate::add_to_log::format_entry;
use crate::messages::{OpLogInfo, OpLogOption};
use crate::{LogDefinition, LogFile, OpLogWorker};
use chrono::Utc;
use chrono_tz::Europe::Warsaw;
use flate2::write::ZlibEncoder;
use flate2::Compression;
use rand::random;
use std::cmp::min;
use std::collections::VecDeque;
use std::io;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
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

/// Every log file starts with this line; frames follow it.
const MAGIC: &[u8] = b"OPLog 1.0\n";

/// Walks the frames of an existing log file and returns the offset of the
/// frame the file ends in the middle of — the trace of a write that was
/// cut short (no space left mid-payload, a crash between the prefix and the
/// payload). Returns `None` when the file ends exactly on a frame boundary,
/// and also when the layout is not one this walker understands (no magic,
/// a byte other than the marker where a frame should start, an oversized
/// size field): such a file is not ours to judge and is left untouched.
fn truncated_frame_start<R: Read + Seek>(reader: &mut R, len: u64) -> io::Result<Option<u64>> {
    if len < MAGIC.len() as u64 {
        return Ok(None);
    }
    let mut magic = [0u8; MAGIC.len()];
    reader.read_exact(&mut magic)?;
    if magic != MAGIC {
        return Ok(None);
    }

    let mut pos = MAGIC.len() as u64;
    while pos < len {
        // prefix: marker, rnd, checksum, then the size as a VLQ of up to
        // five bytes (each XOR 0xC5)
        let mut prefix = [0u8; 8];
        let available = min(prefix.len() as u64, len - pos) as usize;
        reader.read_exact(&mut prefix[..available])?;
        if prefix[0] != 0xff {
            return Ok(None);
        }

        let mut size = 0u64;
        let mut shift = 0;
        let mut i = 3;
        loop {
            if i >= available {
                // the file ends inside the prefix
                return Ok(Some(pos));
            }
            let b = prefix[i] ^ 0xc5;
            i += 1;
            size |= ((b & 0x7f) as u64) << shift;
            if b & 0x80 == 0 {
                break;
            }
            shift += 7;
            if shift > 28 {
                return Ok(None);
            }
        }

        let next = pos + i as u64 + size;
        if next > len {
            // the file ends inside the payload
            return Ok(Some(pos));
        }
        // the reader sits at `pos + available`; a relative seek lets a
        // buffered reader skip small payloads without touching the disk
        reader.seek_relative(next as i64 - (pos + available as u64) as i64)?;
        pos = next;
    }
    Ok(None)
}

/// Files above this size are appended to without the tail check. The walk
/// reads the file front to back (frames carry no trailer to walk backwards
/// from) and has to stay far below `WRITE_TIMEOUT` even on a cold disk: a
/// check that timed out would repeat on every tick and never let the write
/// through. Per-period files stay well under this; only a `NoSplit` file
/// that has grown for a long time crosses it, and loses the check.
const TAIL_CHECK_MAX_LEN: u64 = 64 * 1024 * 1024;

/// Cuts an interrupted frame off the end of the file, if there is one, so
/// that the next frame is appended on a frame boundary. Runs on the
/// blocking pool: the walk is a buffered read of the prefixes, far cheaper
/// there than as a round trip through `tokio::fs` per frame.
async fn cut_interrupted_tail(path: &Path) -> io::Result<()> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || cut_interrupted_tail_up_to(&path, TAIL_CHECK_MAX_LEN))
        .await
        .map_err(io::Error::other)?
}

fn cut_interrupted_tail_up_to(path: &Path, max_len: u64) -> io::Result<()> {
    let file = std::fs::File::options().read(true).write(true).open(path)?;
    let len = file.metadata()?.len();
    if len > max_len {
        return Ok(());
    }

    let mut reader = io::BufReader::with_capacity(64 * 1024, &file);
    let Some(cut) = truncated_frame_start(&mut reader, len)? else {
        return Ok(());
    };

    // A write that timed out earlier may still be running in the background
    // (see `WRITE_TIMEOUT`). If the file changed under the walk, the offset
    // no longer describes it — leave it alone rather than cut a live frame.
    if file.metadata()?.len() != len {
        return Ok(());
    }
    file.set_len(cut)?;
    eprintln!(
        "[op-log] {}: cut {} bytes of an interrupted frame before appending",
        path.display(),
        len - cut
    );
    Ok(())
}

impl LogDefinition {
    async fn write_to_file(&mut self, flush_interval: &Duration) {
        let log_type = self.log_type;
        let no_date = self.options.contains(&OpLogOption::NoAddDateToLog);
        for file in self.files.values_mut() {
            // Entries dropped to stay under the queue cap are reported into
            // the log itself, ahead of the entries that outlived them. The
            // count is cleared only by a successful write, so a failed or
            // timed-out attempt reports it again next time.
            let backlog_notice = (file.dropped_logs > 0).then(|| {
                let text = format!(
                    "[op-log] dropped {} log entries (write backlog)",
                    file.dropped_logs
                );
                format_entry(&log_type, no_date, &Utc::now().with_timezone(&Warsaw), &text)
            });

            // A disk error (no space, revoked permissions) must not kill the
            // worker — a panic here would silently stop ALL logging until
            // restart. Report once per failure episode on stderr and keep
            // going; pending entries stay queued and retry on the next tick.
            //
            // A HUNG write (stuck syscall, never returns) is the same class
            // of risk but doesn't go through `Result` at all — wrap the call
            // in a timeout so it always resolves to one.
            let write = file.write_to_file(flush_interval, backlog_notice.as_deref());
            let result = match timeout(WRITE_TIMEOUT, write).await {
                Ok(result) => result,
                Err(_) => Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("write did not complete within {WRITE_TIMEOUT:?}"),
                )),
            };
            match result {
                Ok(()) => file.write_error_logged = false,
                Err(e) => {
                    file.tail_verified = false;
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
}

impl OpLogWorker {
    fn log_count(&self) -> usize {
        self.definitions.values().flat_map(|d| d.files.values()).map(|f| f.logs.len()).sum()
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

/// Compresses `header` (a new file's first frame only), `notice` (a
/// dropped-backlog notice, if any) and as many of `logs` as fit — chunked
/// the same way production does, up to 64 000 B per `write_all` into the
/// encoder and a 2 MB cap on the compressed payload — then obfuscates the
/// result and prefixes it with the frame header (marker, random byte,
/// checksum, VLQ size): the inverse of `truncated_frame_start`'s prefix
/// parsing. Returns the prefix, the payload, and how many entries were
/// consumed, so the caller can drain exactly those from `logs` once the
/// write to disk succeeds.
///
/// Pulled out of `write_to_file` so the framing can be exercised at any
/// compression level in a test; production always calls it with
/// `Compression::default()`.
fn encode_frame(
    logs: &VecDeque<String>,
    header: Option<&str>,
    notice: Option<&str>,
    level: Compression,
) -> io::Result<(Vec<u8>, Vec<u8>, usize)> {
    let mut bytes: Vec<u8> = Vec::with_capacity(1024);
    let mut encoder = ZlibEncoder::new(Vec::new(), level);

    if let Some(header) = header {
        encoder.write_all(header.as_bytes())?;
        encoder.write_all(b"\n")?;
    }
    if let Some(notice) = notice {
        encoder.write_all(notice.as_bytes())?;
        encoder.write_all(b"\n")?;
    }

    let mut consumed = 0usize;
    loop {
        bytes.clear();

        while let Some(log) = logs.get(consumed) {
            bytes.extend_from_slice(log.as_bytes());
            bytes.push(0x0a);
            consumed += 1;

            if bytes.len() > 64000 {
                break;
            }
        }

        if bytes.is_empty() {
            break;
        }

        encoder.write_all(&bytes)?;
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

    let mut prefix = Vec::with_capacity(8);
    prefix.push(0xff);
    prefix.push(rnd);
    prefix.push((sum as u8) ^ 0x5c);

    loop {
        let mut b: u8 = (size & 0x7F) as u8;
        size >>= 7;
        if size != 0 {
            b |= 0x80
        };

        prefix.push(b ^ 0xc5);
        if size == 0 {
            break;
        }
    }

    Ok((prefix, a, consumed))
}

impl LogFile {
    async fn write_to_file(
        &mut self,
        flush_interval: &Duration,
        backlog_notice: Option<&str>,
    ) -> io::Result<()> {
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

        let mut path = PathBuf::from(&self.path);
        let _ = create_dir_all(&path).await;

        path.push(&self.log_name);

        // A zero-length file cannot be a valid log (every file starts with
        // the magic) — a crash or a full disk right after `File::create`
        // leaves exactly that behind. Treat it as new, otherwise frames get
        // appended to a file without its magic and readers reject it whole.
        let has_content = fs::metadata(&path).await.map(|m| m.len() > 0).unwrap_or(false);
        let mut f = if has_content {
            // An earlier write may have been cut short (no space left in the
            // middle of the payload, a crash between the prefix and the
            // payload) and left the file ending inside a frame. Appending
            // after that would make the whole file unreadable: readers
            // recover a truncated LAST frame, but a truncated frame followed
            // by more frames fails its checksum and takes every later frame
            // down with it. Checked before the first write through this
            // handle and again after every failed write.
            if !self.tail_verified {
                cut_interrupted_tail(&path).await?;
            }
            File::options().append(true).open(&path).await?
        } else {
            let mut f = File::create(&path).await?;
            f.write_all(MAGIC).await?;
            f
        };

        // Encode by INDEX, not by draining `self.logs` — the disk write below
        // can still be interrupted by the caller's `tokio::time::timeout`. If
        // entries were popped here, a timed-out write would lose them for
        // good (they'd exist only in the encoder, dropped with the future).
        // Only remove what was actually written once the write below has
        // succeeded (see `self.logs.drain(..consumed)` further down) — a
        // timeout before that point leaves `self.logs` untouched, so the
        // next tick retries the same entries in full.
        let header = (!has_content && !self.header.is_empty()).then_some(self.header.as_str());
        let (prefix, payload, consumed) =
            encode_frame(&self.logs, header, backlog_notice, Compression::default())?;

        f.write_all(&prefix).await?;
        f.write_all(&payload).await?;
        f.flush().await?;

        // Only now, after the write is confirmed on disk, drop the entries
        // that were actually encoded above — a timeout anywhere before this
        // line leaves them in `self.logs` for the next attempt.
        for written in self.logs.drain(..consumed) {
            self.queued_bytes -= written.len();
        }
        // The notice above carried the count to disk.
        self.dropped_logs = 0;
        // The file now ends on the boundary of the frame just written.
        self.tail_verified = true;

        if self.logs.is_empty() {
            self.time_of_first_addition_of_log_after_write = None;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::OpLogWorker;
    use crate::messages::{OpLogDefinition, OpLogType};
    use chrono::Utc;
    use flate2::Compression;
    use std::collections::VecDeque;
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
            OpLogDefinition::new("test", dir.to_str().unwrap())
                .log_type(OpLogType::NoSplit)
                .flush_interval(flush_interval)
                .header(header),
        );
        worker
    }

    fn queued_file(worker: &OpLogWorker) -> &crate::LogFile {
        let files = &worker.definitions["test"].files;
        assert_eq!(files.len(), 1, "the definition must have exactly one file");
        files.values().next().unwrap()
    }

    fn queued_file_mut(worker: &mut OpLogWorker) -> &mut crate::LogFile {
        let files = &mut worker.definitions.get_mut("test").unwrap().files;
        assert_eq!(files.len(), 1, "the definition must have exactly one file");
        files.values_mut().next().unwrap()
    }

    // Entries dropped to keep the queue under its cap are not lost silently:
    // the first write that succeeds again starts with a notice carrying the
    // count, ahead of the entries that outlived the drops. A failed write
    // must keep the count for the next attempt.
    #[tokio::test]
    async fn dropped_backlog_is_reported_by_the_first_successful_write() {
        let dir = temp_dir("backlog");
        let blocker = dir.join("blocker");
        std::fs::write(&blocker, b"not a directory").unwrap();
        let log_dir = blocker.join("logs");
        let mut op_log = worker_with_flush_interval(&log_dir, "", Duration::from_millis(0));

        op_log.log("test", Utc::now(), "kept");
        queued_file_mut(&mut op_log).dropped_logs = 3;

        op_log.write_to_files().await;
        assert_eq!(op_log.log_count(), 1, "a failed write must keep the entry queued");
        assert_eq!(queued_file(&op_log).dropped_logs, 3, "a failed write must keep the drop count");

        std::fs::remove_file(&blocker).unwrap();
        op_log.write_to_files().await;
        assert_eq!(op_log.log_count(), 0, "the first successful write must drain the queue");
        assert_eq!(queued_file(&op_log).dropped_logs, 0, "a successful write reports and clears the count");
        assert_eq!(queued_file(&op_log).queued_bytes, 0, "the byte count must follow the drain");

        let raw = std::fs::read(log_dir.join("test.log")).unwrap();
        assert_eq!(
            decode_oplog_file(&raw),
            vec!["[op-log] dropped 3 log entries (write backlog)\nkept\n".to_string()]
        );

        // nothing left to report: the next write carries no notice
        op_log.log("test", Utc::now(), "later");
        op_log.write_to_files().await;
        let raw = std::fs::read(log_dir.join("test.log")).unwrap();
        assert_eq!(
            decode_oplog_file(&raw),
            vec![
                "[op-log] dropped 3 log entries (write backlog)\nkept\n".to_string(),
                "later\n".to_string()
            ]
        );

        let _ = std::fs::remove_dir_all(&dir);
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

        let file = log_dir.join("test.log");
        let raw = std::fs::read(&file).unwrap();
        assert_eq!(decode_oplog_file(&raw), vec!["entry\n".to_string()]);
        assert!(queued_file(&op_log).tail_verified, "a successful write leaves the tail verified");

        // a failure AFTER a success: a directory in place of the log file
        // fails the write on every platform; the failure must invalidate
        // the tail check, since an interrupted write may have left a
        // partial frame behind
        std::fs::remove_file(&file).unwrap();
        std::fs::create_dir(&file).unwrap();
        op_log.log("test", Utc::now(), "later");
        op_log.write_to_files().await;
        assert_eq!(op_log.log_count(), 1, "a failed write must keep the entry queued");
        assert!(!queued_file(&op_log).tail_verified, "a failed write must invalidate the tail check");

        std::fs::remove_dir(&file).unwrap();
        std::fs::write(&file, &raw).unwrap();
        op_log.write_to_files().await;
        assert_eq!(op_log.log_count(), 0, "the first successful write must drain the queue");
        assert!(queued_file(&op_log).tail_verified, "a successful write leaves the tail verified");
        let raw = std::fs::read(&file).unwrap();
        assert_eq!(decode_oplog_file(&raw), vec!["entry\n".to_string(), "later\n".to_string()]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    // A write cut short — no space left in the middle of the payload, a
    // crash between the prefix and the payload — leaves the file ending
    // inside a frame. Readers recover a truncated LAST frame, but once more
    // frames are appended after it, the truncated frame's declared size
    // swallows the following bytes, its checksum fails and the whole file
    // is rejected. A fresh writer must therefore cut the file back to the
    // last frame boundary before appending; a file that ends on a boundary
    // must be appended to untouched.
    #[tokio::test]
    async fn interrupted_frame_at_the_tail_is_cut_before_appending() {
        let dir = temp_dir("tail");
        let path = dir.join("test.log");

        // the second entry is incompressible and long, so its frame carries
        // a multi-byte size field — the walker must decode it exactly, or
        // it would cut a valid file
        let mut state = 12345u32;
        let second: String = (0..40_000)
            .map(|_| {
                state = state.wrapping_mul(1_103_515_245).wrapping_add(12_345);
                char::from(b'a' + ((state >> 16) % 26) as u8)
            })
            .collect();

        let mut op_log = worker_with_no_split_definition(&dir, "");
        op_log.log("test", Utc::now(), "first");
        op_log.flush().await;
        let len1 = std::fs::metadata(&path).unwrap().len();
        op_log.log("test", Utc::now(), &second);
        op_log.flush().await;
        let clean = std::fs::read(&path).unwrap();
        let frame2 = clean.len() as u64 - len1;
        assert!(frame2 > 16_384, "the second frame must need a three-byte size field: {frame2}");

        // bytes of the second frame kept on disk — the whole frame (no cut),
        // 1..=4 (cut inside the prefix: marker, rnd, checksum, size), and
        // cuts inside the payload
        for kept in [frame2, 1, 2, 3, 4, frame2 / 2, frame2 - 1] {
            std::fs::write(&path, &clean[..(len1 + kept) as usize]).unwrap();

            let mut fresh = worker_with_no_split_definition(&dir, "");
            fresh.log("test", Utc::now(), "third");
            fresh.flush().await;
            assert_eq!(fresh.log_count(), 0, "kept={kept}: the write must succeed");

            let raw = std::fs::read(&path).unwrap();
            let expected: Vec<String> = if kept == frame2 {
                vec!["first\n".into(), format!("{second}\n"), "third\n".into()]
            } else {
                vec!["first\n".into(), "third\n".into()]
            };
            assert_eq!(decode_oplog_file(&raw), expected, "kept={kept} of {frame2}");
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    // The walker may only report a cut for a file it fully understands up to
    // the truncated frame. Anything else — no magic, a byte other than the
    // marker where a frame should start — must be left alone: cutting a file
    // whose layout is unknown would destroy data instead of repairing it.
    #[test]
    fn tail_walker_leaves_unrecognised_layouts_alone() {
        // Every case goes through a plain cursor and through a buffered
        // reader whose tiny buffer forces refills inside prefixes and
        // payloads — the production walk is buffered.
        let walk = |bytes: &[u8]| {
            let mut cursor = std::io::Cursor::new(bytes.to_vec());
            let plain = super::truncated_frame_start(&mut cursor, bytes.len() as u64).unwrap();
            let mut buffered =
                std::io::BufReader::with_capacity(3, std::io::Cursor::new(bytes.to_vec()));
            let via_buffer =
                super::truncated_frame_start(&mut buffered, bytes.len() as u64).unwrap();
            assert_eq!(plain, via_buffer, "buffering must not change the verdict");
            plain
        };
        let magic = b"OPLog 1.0\n".as_slice();
        // marker, rnd, checksum, size 2 (VLQ 0x02 ^ 0xC5), two payload bytes
        let frame = [0xff, 0x01, 0x02, 0x02 ^ 0xc5, 0xaa, 0xbb].as_slice();

        assert_eq!(walk(b""), None);
        assert_eq!(walk(b"plain text, not a log\n"), None);
        assert_eq!(walk(magic), None, "magic only: nothing to cut");
        assert_eq!(walk(&[magic, b"XYZ"].concat()), None, "bad marker: not ours to judge");
        assert_eq!(walk(&[magic, frame].concat()), None, "complete frame: nothing to cut");
        assert_eq!(walk(&[magic, frame, frame].concat()), None, "two complete frames");
        assert_eq!(
            walk(&[magic, frame, b"garbage"].concat()),
            None,
            "a good frame followed by a bad marker: not ours to judge"
        );
        assert_eq!(
            walk(&[magic, frame, &frame[..5]].concat()),
            Some((magic.len() + frame.len()) as u64),
            "the second frame is short of its last byte"
        );
        assert_eq!(
            walk(&[magic, &frame[..2]].concat()),
            Some(magic.len() as u64),
            "the only frame ends inside its prefix"
        );

        // sizes that need two and three VLQ bytes, encoded the way the
        // writer does it (7 bits per byte, low group first, 0x80 continues)
        for size in [200usize, 20_000] {
            let mut big = vec![0xff, 0x01, 0x02];
            let mut rest = size;
            loop {
                let mut a = (rest & 0x7f) as u8;
                rest >>= 7;
                if rest != 0 {
                    a |= 0x80;
                }
                big.push(a ^ 0xc5);
                if rest == 0 {
                    break;
                }
            }
            big.extend(std::iter::repeat_n(0x55u8, size));
            assert_eq!(walk(&[magic, &big].concat()), None, "complete {size}-byte frame");
            assert_eq!(walk(&[magic, &big, frame].concat()), None, "followed by a complete frame");
            assert_eq!(
                walk(&[magic, frame, &big[..big.len() - 1]].concat()),
                Some((magic.len() + frame.len()) as u64),
                "{size}-byte frame short of its last byte"
            );
            assert_eq!(
                walk(&[magic, &big, &frame[..1]].concat()),
                Some((magic.len() + big.len()) as u64),
                "a marker alone after a complete {size}-byte frame"
            );
        }
    }

    // The check walks the whole file, so it is skipped above a size cap —
    // a walk that outlived `WRITE_TIMEOUT` would repeat on every tick and
    // never let the write through. Below the cap the tail is cut.
    #[test]
    fn tail_check_is_skipped_above_the_size_cap() {
        let dir = temp_dir("cap");
        let path = dir.join("test.log");
        let frame = [0xffu8, 0x01, 0x02, 0x02 ^ 0xc5, 0xaa, 0xbb];
        let content = [b"OPLog 1.0\n".as_slice(), &frame, &frame[..4]].concat();
        std::fs::write(&path, &content).unwrap();
        let len = content.len() as u64;

        super::cut_interrupted_tail_up_to(&path, len - 1).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), len, "above the cap: untouched");

        super::cut_interrupted_tail_up_to(&path, len).unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            len - 4,
            "at the cap: the interrupted frame is cut"
        );

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

    // Every zlib level (0 = store, 9 = best compression) must round-trip
    // through the frame `encode_frame` builds: the VLQ size prefix and the
    // XOR keystream depend on the compressed payload's length, which varies
    // with the level, so a bug at one length wouldn't necessarily show at
    // another. Three corpora exercise different compressed-size regimes:
    // a short entry set (production's common case), a long, highly
    // repetitive entry (small output even at level 0), and a long entry
    // with little redundancy (output close to input size at every level).
    #[test]
    fn every_zlib_level_round_trips_through_the_frame() {
        let short: VecDeque<String> = ["first entry", "second entry", "third, with more text"]
            .into_iter()
            .map(String::from)
            .collect();

        let repetitive: VecDeque<String> =
            VecDeque::from([format!("{} ", "same word ").repeat(20_000)]);

        // A linear congruential generator gives a deterministic, hard-to-
        // compress byte stream without pulling in a `rand` distribution.
        let mut state = 88172645463325252u64;
        let mut hard_to_compress = String::with_capacity(50_000);
        while hard_to_compress.len() < 50_000 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            hard_to_compress.push_str(&format!("{:016x}", state));
        }
        let random_like: VecDeque<String> = VecDeque::from([hard_to_compress]);

        for level in 0..=9u32 {
            for (label, logs) in [("short", &short), ("repetitive", &repetitive), ("random-like", &random_like)] {
                let (prefix, payload, consumed) =
                    super::encode_frame(logs, Some("a header"), Some("a backlog notice"), Compression::new(level))
                        .unwrap_or_else(|e| panic!("level {level}, {label}: encode failed: {e}"));
                assert_eq!(consumed, logs.len(), "level {level}, {label}: every entry must be consumed");

                let mut raw = super::MAGIC.to_vec();
                raw.extend_from_slice(&prefix);
                raw.extend_from_slice(&payload);

                let decoded = decode_oplog_file(&raw);
                assert_eq!(decoded.len(), 1, "level {level}, {label}: one entry was written as one frame");
                let mut expected = String::from("a header\na backlog notice\n");
                for log in logs {
                    expected.push_str(log);
                    expected.push('\n');
                }
                assert_eq!(decoded[0], expected, "level {level}, {label}: decoded text must match what was written");
            }
        }
    }
}

#[cfg(test)]
#[path = "bench.rs"]
mod bench;
