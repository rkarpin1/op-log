// -------------------------------------------------------------------------------------------------
//   Copyright 2024-2025 (c) Robert Karpiński
// -------------------------------------------------------------------------------------------------

use crate::messages::OpLogCleanUpDefinition;
use crate::OpLogWorker;
use log::info;
use std::cmp::min;
use std::future::Future;
use std::path::Path;
use std::time::{Duration, SystemTime};
use tokio::fs::remove_dir;
use tokio::fs::{read_dir, remove_file, DirEntry};
use tokio::io;
use tokio::time::timeout;

/// Ceiling on one pass of the old-file cleanup. The pass runs in the same
/// task as the writer, so every second it takes is a second without log
/// writes — and a stuck syscall inside it (`read_dir`, `metadata`,
/// `remove_file`) would hang the worker for good, the same way a stuck
/// write did before `WRITE_TIMEOUT`. A healthy pass over a consumer's tree
/// takes milliseconds; a pass that does not finish is cut off and starts
/// over on the next cleanup tick — deleting is idempotent and every partial
/// pass has already removed something, so it converges.
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(60);

/// Runs one cleanup pass under `CLEANUP_TIMEOUT`; returns whether it
/// completed.
async fn run_bounded<F: Future<Output = ()>>(pass: F) -> bool {
    timeout(CLEANUP_TIMEOUT, pass).await.is_ok()
}

impl OpLogWorker {
    /// One pass of the old-file cleanup, cut off after `CLEANUP_TIMEOUT`.
    pub(crate) async fn clean_up_delete_files_bounded(&mut self) {
        let completed = run_bounded(self.clean_up_delete_files()).await;
        self.note_cleanup_outcome(completed);
    }

    fn note_cleanup_outcome(&mut self, completed: bool) {
        if completed {
            self.cleanup_timed_out = false;
        } else if !self.cleanup_timed_out {
            eprintln!(
                "[op-log] file cleanup did not complete within {CLEANUP_TIMEOUT:?}; \
                 it resumes on the next cleanup tick"
            );
            self.cleanup_timed_out = true;
        }
    }

    pub(crate) fn clean_up_remove_all_definitions(&mut self) {
        self.clean_up_definitions.clear();
    }

    pub(crate) fn clean_up_bundle(&mut self, bundle: Vec<OpLogCleanUpDefinition>) {
        self.clean_up_definitions = bundle;
    }

    pub(crate) fn clean_up_definition(&mut self, def: OpLogCleanUpDefinition) {
        let a = self
            .clean_up_definitions
            .iter_mut()
            .find(|d| d.path.eq(&def.path));

        if let Some(a) = a {
            a.delete_after_days = def.delete_after_days;
        } else {
            self.clean_up_definitions.push(def)
        }
    }

    async fn is_for_deletion(&self, dir_entry: &DirEntry, min_days: u32) -> io::Result<bool> {
        let metadata = dir_entry.metadata().await?;

        let modified_time = metadata.modified().unwrap_or(SystemTime::now());
        let created_time = metadata.created().unwrap_or(SystemTime::now());

        let time = min(modified_time, created_time);

        let duration = SystemTime::now()
            .duration_since(time)
            .unwrap_or(Duration::from_secs(0));

        let days = duration.as_secs() / (60 * 60 * 24);
        Ok(days >= min_days as u64)
    }

    async fn delete_files_in_path(&self, path: &Path, delete_after_days: u32) {
        let Ok(mut dir) = read_dir(path).await else {
            return;
        };
        let mut deleted_count = 0;

        loop {
            let Ok(Some(entry)) = dir.next_entry().await else {
                break;
            };

            if let Ok(file_type) = entry.file_type().await {
                if file_type.is_dir() {
                    let dir_path = entry.path();
                    Box::pin(self.delete_files_in_path(&dir_path, delete_after_days)).await;

                    if remove_dir(&dir_path).await.is_ok() {
                        info!(target: "log", "remove dir:{}, after-days:{}",
                            dir_path.display(),
                        delete_after_days)
                    }

                    continue;
                }

                if !file_type.is_file() {
                    continue;
                }

                if self
                    .is_for_deletion(&entry, delete_after_days)
                    .await
                    .unwrap_or(false)
                    && remove_file(&entry.path()).await.is_ok()
                {
                    deleted_count += 1;
                }
            }
        }

        if deleted_count > 0 {
            info!(target: "log", "deleted:{} - path:{}, after-days:{}",
            deleted_count, path.display(),
            delete_after_days, );
        }
    }

    pub(crate) async fn clean_up_delete_files(&self) {
        for def in self.clean_up_definitions.iter() {
            if def.delete_after_days > 0 {
                self.delete_files_in_path(Path::new(&def.path), def.delete_after_days)
                    .await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::OpLogWorker;
    use std::ffi::OsString;
    use std::time::Duration;

    // A name the OS accepts but that is not valid UTF-8 — the case that used
    // to panic the shared worker task via `.to_str().unwrap()`. Each platform
    // produces one differently: arbitrary bytes on Unix, an unpaired UTF-16
    // surrogate on Windows.
    #[cfg(unix)]
    fn non_utf8_name() -> OsString {
        use std::os::unix::ffi::OsStringExt;
        // "fo\xFFo" — 0xFF is never a valid UTF-8 byte.
        OsString::from_vec(vec![0x66, 0x6f, 0xff, 0x6f])
    }

    #[cfg(windows)]
    fn non_utf8_name() -> OsString {
        use std::os::windows::ffi::OsStringExt;
        // "fo\u{D800}o" — a lone high surrogate has no UTF-8 encoding.
        OsString::from_wide(&[0x66, 0x6f, 0xd800, 0x6f])
    }

    // The cleaner formats a path in two places: the directory it has just
    // emptied and every subdirectory it removed — so the bad name must be a
    // SUBDIRECTORY holding an old file, not just a file. `info!` evaluates
    // its arguments only when the level is enabled, so enable it: otherwise
    // even the old `.unwrap()` would never run here.
    #[tokio::test]
    async fn non_utf8_filename_does_not_panic_the_cleaner() {
        log::set_max_level(log::LevelFilter::Info);
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let worker = OpLogWorker::new(rx);

        let dir = std::env::temp_dir().join(format!("op-log-test-{}", std::process::id()));
        let bad_dir = dir.join(non_utf8_name());
        let bad_file = bad_dir.join(non_utf8_name());
        tokio::fs::create_dir_all(&bad_dir).await.unwrap();
        tokio::fs::write(&bad_file, b"x").await.unwrap();
        assert!(bad_dir.to_str().is_none(), "the test premise needs a non-UTF-8 path");

        // delete_after_days: 0 makes every file eligible immediately.
        worker.delete_files_in_path(&dir, 0).await;

        assert!(!bad_file.exists(), "file with a non-UTF-8 name should have been deleted");
        assert!(!bad_dir.exists(), "emptied directory with a non-UTF-8 name should be gone");

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    // A future change of the ceiling must be a deliberate decision, not a
    // side effect of a refactor.
    #[test]
    fn cleanup_timeout_constant_matches_documented_value() {
        assert_eq!(super::CLEANUP_TIMEOUT, Duration::from_secs(60));
    }

    // A cleanup pass that never finishes (a stuck syscall) must be cut off
    // at the ceiling instead of hanging the shared worker task; a pass that
    // finishes is left alone. Paused time makes the 60 s virtual.
    #[tokio::test(start_paused = true)]
    async fn stuck_cleanup_pass_is_cut_off_at_the_ceiling() {
        let started = tokio::time::Instant::now();
        let completed = super::run_bounded(std::future::pending::<()>()).await;
        assert!(!completed, "a pass that never finishes must be reported as cut off");
        assert_eq!(started.elapsed(), super::CLEANUP_TIMEOUT, "cut off exactly at the ceiling");

        let completed = super::run_bounded(async {}).await;
        assert!(completed, "a finished pass must not be reported as cut off");
    }

    // The cut-off is reported once per episode and the episode closes with
    // the first pass that completes again.
    #[tokio::test]
    async fn cleanup_cut_off_is_reported_once_per_episode() {
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let mut worker = OpLogWorker::new(rx);

        worker.note_cleanup_outcome(false);
        assert!(worker.cleanup_timed_out, "the first cut-off opens an episode");
        worker.note_cleanup_outcome(false);
        assert!(worker.cleanup_timed_out, "a repeated cut-off keeps the episode open");
        worker.note_cleanup_outcome(true);
        assert!(!worker.cleanup_timed_out, "a completed pass closes the episode");
    }
}
