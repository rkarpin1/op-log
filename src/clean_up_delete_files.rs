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
    // A filename that isn't valid UTF-8 must not panic the shared worker
    // task — it used to, via `.to_str().unwrap()` (see the diff this test
    // was added alongside). Non-UTF-8 bytes in a filename are a Unix-only
    // concept — OsStr on Windows can't carry arbitrary bytes the same way —
    // so this only runs where the risk actually exists.
    #[cfg(unix)]
    #[tokio::test]
    async fn non_utf8_filename_does_not_panic_the_cleaner() {
        use super::OpLogWorker;
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;

        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let worker = OpLogWorker::new(rx);

        let dir = std::env::temp_dir().join(format!("op-log-test-{}", std::process::id()));
        tokio::fs::create_dir_all(&dir).await.unwrap();

        // "fo\xFFo" — 0xFF is not a valid UTF-8 continuation or start byte.
        let bad_name = OsStr::from_bytes(&[0x66, 0x6f, 0xff, 0x6f]);
        let bad_path = dir.join(bad_name);
        tokio::fs::write(&bad_path, b"x").await.unwrap();

        // delete_after_days: 0 makes every file eligible immediately.
        worker.delete_files_in_path(&dir, 0).await;

        assert!(
            !bad_path.exists(),
            "file with a non-UTF-8 name should have been deleted without panicking"
        );

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }
}
