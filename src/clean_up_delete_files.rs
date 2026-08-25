// -------------------------------------------------------------------------------------------------
//   Copyright 2024-2025 (c) Robert Karpiński
// -------------------------------------------------------------------------------------------------

use crate::messages::OpLogCleanUpDefinition;
use crate::{LogCleanUpDefinition, OpLogWorker};
use log::info;
use std::cmp::min;
use std::path::Path;
use std::time::{Duration, SystemTime};
use tokio::fs::remove_dir;
use tokio::fs::{read_dir, remove_file, DirEntry};
use tokio::io;

impl OpLogWorker {
    pub(crate) fn clean_up_remove_all_definitions(&mut self) {
        self.clean_up_definitions.clear();
    }

    pub(crate) fn clean_up_bundle(&mut self, bundle: Vec<OpLogCleanUpDefinition>) {
        self.clean_up_definitions = bundle
            .into_iter()
            .map(|d| LogCleanUpDefinition {
                path: d.path,
                delete_after_days: d.delete_after_days,
            })
            .collect();
    }

    pub(crate) fn clean_up_definition(&mut self, def: OpLogCleanUpDefinition) {
        let a = self
            .clean_up_definitions
            .iter_mut()
            .find(|d| d.path.eq(&def.path));

        if let Some(a) = a {
            a.delete_after_days = def.delete_after_days;
        } else {
            self.clean_up_definitions.push(LogCleanUpDefinition {
                path: def.path.clone(),
                delete_after_days: def.delete_after_days,
            })
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
        let dir = read_dir(path).await;
        if dir.is_err() {
            return;
        }

        let mut dir = dir.unwrap();
        let mut deleted_count = 0;

        loop {
            let Ok(Some(entry)) = dir.next_entry().await else {
                break;
            };

            if let Ok(file_type) = entry.file_type().await {
                if file_type.is_dir() {
                    Box::pin(self.delete_files_in_path(&entry.path(), delete_after_days)).await;

                    if remove_dir(&entry.path()).await.is_ok() {
                        info!(target: "log", "remove dir:{}, after-days:{}",
                            entry.path().display(),
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
}
