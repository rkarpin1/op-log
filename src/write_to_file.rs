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

/// Sufit czasu na pojedynczy zapis pliku (create_dir_all + open + write + flush).
/// Bez tego, zawieszony syscall (np. chwilowe zacięcie wolumenu) blokuje
/// `.await` w nieskończoność — a to jest jedyny wspólny task obsługujący
/// WSZYSTKIE zdefiniowane logi naraz, więc jego zawieszenie ubija logowanie
/// na stałe, do restartu procesu. Zmierzone jako przyczyna 10-dniowej przerwy
/// w logach w konsumencie tej biblioteki (x-ai, incydent 2026-08-15..08-25):
/// proces żył, dysk miał miejsce, brak paniki w journalu — worker po prostu
/// nigdy nie wrócił z jednego `.await`.
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

        let mut f = if fs::metadata(&path).await.is_ok() {
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

        loop {
            bytes.clear();

            loop {
                if self.logs.is_empty() {
                    break;
                }
                let log = self.logs.pop_front().unwrap();
                bytes.put(log.as_bytes());
                bytes.put_u8(0x0a);

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
    use std::time::Duration;

    #[tokio::test]
    async fn write_to_file() {
        let (_tx, rx) = tokio::sync::mpsc::channel(32);
        let mut op_log = OpLogWorker::new(rx);

        op_log.def(
            "test",
            ".",
            OpLogType::PerHour,
            &HashSet::new(),
            Duration::from_secs(10),
            "header, jest długi bez z półskimi liter ŻĄŁ",
            false
        );

        op_log.log("test", Utc::now(), "log, to ładny i ŻAŁOŚĆ to słowo");

        let c = op_log.log_count();
        println!("{}", c);
        op_log.flush().await;
    }

    // Przypina wartość stałej: przyszła zmiana sufitu ma być świadomą
    // decyzją, nie przypadkowym skutkiem refaktoru.
    #[test]
    fn write_timeout_constant_matches_documented_value() {
        assert_eq!(super::WRITE_TIMEOUT, Duration::from_secs(15));
    }

    // Symuluje zawieszony syscall zapisu: future, która nigdy się nie kończy.
    // Prawdziwego zawieszonego dysku nie da się deterministycznie zasymulować
    // bez mockowania systemu plików — to sprawdza sam mechanizm, na którym
    // stoi cała poprawka, z krótkim lokalnym czasem, żeby test pozostał szybki.
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
