// -------------------------------------------------------------------------------------------------
//   Copyright 2024-2025 (c) Robert Karpiński
// -------------------------------------------------------------------------------------------------

use crate::OpLogWorker;
use crate::messages::OpLogCleanUpDefinition;
use log::info;
use std::future::Future;
use std::path::Path;
use std::time::{Duration, SystemTime};
use tokio::fs::remove_dir;
use tokio::fs::{DirEntry, read_dir, remove_file};
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

/// Decides whether a file has gone unwritten for `min_days` whole days.
///
/// Age is measured from the LAST MODIFICATION, so a file that is still
/// being appended to is never old, however long ago it was created. The
/// creation time is only a fallback for filesystems that do not report a
/// modification time; when neither is available the file counts as brand
/// new, because cleanup must not delete a file it cannot date.
///
/// The timestamps and `now` are arguments rather than reads of the file and
/// the clock so that the rule can be pinned by a test on every platform:
/// modification times can be set portably, creation times cannot.
fn is_older_than(
    modified: io::Result<SystemTime>,
    created: io::Result<SystemTime>,
    now: SystemTime,
    min_days: u32,
) -> bool {
    let time = modified.or(created).unwrap_or(now);
    let duration = now.duration_since(time).unwrap_or(Duration::from_secs(0));
    duration.as_secs() / (60 * 60 * 24) >= min_days as u64
}

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
        Ok(is_older_than(
            metadata.modified(),
            metadata.created(),
            SystemTime::now(),
            min_days,
        ))
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
    use std::time::SystemTime;
    use tokio::io;

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
        assert!(
            bad_dir.to_str().is_none(),
            "the test premise needs a non-UTF-8 path"
        );

        // delete_after_days: 0 makes every file eligible immediately.
        worker.delete_files_in_path(&dir, 0).await;

        assert!(
            !bad_file.exists(),
            "file with a non-UTF-8 name should have been deleted"
        );
        assert!(
            !bad_dir.exists(),
            "emptied directory with a non-UTF-8 name should be gone"
        );

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
        assert!(
            !completed,
            "a pass that never finishes must be reported as cut off"
        );
        assert_eq!(
            started.elapsed(),
            super::CLEANUP_TIMEOUT,
            "cut off exactly at the ceiling"
        );

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
        assert!(
            worker.cleanup_timed_out,
            "the first cut-off opens an episode"
        );
        worker.note_cleanup_outcome(false);
        assert!(
            worker.cleanup_timed_out,
            "a repeated cut-off keeps the episode open"
        );
        worker.note_cleanup_outcome(true);
        assert!(
            !worker.cleanup_timed_out,
            "a completed pass closes the episode"
        );
    }

    // Retention counts days since the file was last WRITTEN, not since it
    // was created: a `NoSplit` log created months ago but appended to today
    // is not old. The creation time only stands in when the filesystem
    // reports no modification time, and a file that can be dated by neither
    // is never deleted.
    #[test]
    fn retention_age_is_measured_from_the_last_modification() {
        let now = SystemTime::now();
        let days_ago = |d: u64| Ok(now - Duration::from_secs(d * 24 * 60 * 60));
        let unavailable = || Err(io::Error::other("no timestamp on this filesystem"));

        assert!(
            !super::is_older_than(days_ago(0), days_ago(400), now, 30),
            "a file created long ago but written to today must survive"
        );
        assert!(
            super::is_older_than(days_ago(40), days_ago(0), now, 30),
            "a file left unwritten for 40 days is old, however recently it was created"
        );
        assert!(
            super::is_older_than(days_ago(30), days_ago(30), now, 30),
            "the threshold counts whole days and is inclusive"
        );
        assert!(
            !super::is_older_than(days_ago(29), days_ago(29), now, 30),
            "a day short of the threshold is not old yet"
        );
        assert!(
            super::is_older_than(unavailable(), days_ago(40), now, 30),
            "without a modification time the creation time decides"
        );
        assert!(
            !super::is_older_than(unavailable(), unavailable(), now, 30),
            "a file that cannot be dated must never be deleted"
        );
        assert!(
            !super::is_older_than(Ok(now + Duration::from_secs(3600)), days_ago(400), now, 30),
            "a modification time in the future must not underflow into a huge age"
        );
    }

    // The rule above has to be the one the cleaner actually applies: this
    // walks a real directory, so a future change reading the creation time
    // again would show up here. Only modification times are set — creation
    // times cannot be set portably, which is why the rule itself is pinned
    // by the truth table above.
    #[tokio::test]
    async fn cleaner_deletes_by_modification_time() {
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let worker = OpLogWorker::new(rx);

        let dir = std::env::temp_dir().join(format!("op-log-retention-{}", std::process::id()));
        let _ = tokio::fs::remove_dir_all(&dir).await;
        tokio::fs::create_dir_all(&dir).await.unwrap();

        let stale = dir.join("stale.log");
        let active = dir.join("active.log");
        for path in [&stale, &active] {
            std::fs::write(path, b"x").unwrap();
        }
        // Both files were just created; only their modification times differ.
        std::fs::File::options()
            .write(true)
            .open(&stale)
            .unwrap()
            .set_modified(SystemTime::now() - Duration::from_secs(40 * 24 * 60 * 60))
            .unwrap();

        worker.delete_files_in_path(&dir, 30).await;

        assert!(
            !stale.exists(),
            "a file unwritten for 40 days must be deleted at 30"
        );
        assert!(
            active.exists(),
            "a file written just now must survive, though it is as new as the other"
        );

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }
}
