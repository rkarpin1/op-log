// -------------------------------------------------------------------------------------------------
//   Copyright 2024-2025 (c) Robert Karpiński
// -------------------------------------------------------------------------------------------------

use crate::messages::{OpLogBundle, OpLogData, OpLogDefinition, OpLogMessage};
use crate::OpLogWorker;
use std::time::Duration;
use tokio::time::{interval, MissedTickBehavior};

impl OpLogWorker {
    pub(crate) async fn run(&mut self) {
        let mut writer_tick = interval(Duration::from_millis(500));
        let mut cleanup_tick = interval(Duration::from_secs(5 * 60));
        writer_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        cleanup_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                biased;
                _ = writer_tick.tick() => self.write_to_files().await,
                _ = cleanup_tick.tick() => {
                    self.clean_up();
                    self.clean_up_delete_files().await;
                }
                msg = self.rx_channel.recv() => {
                    match msg {
                        None => break,
                        Some(OpLogMessage::StopService) => break,
                        Some(OpLogMessage::Flush) => self.flush().await,
                        Some(OpLogMessage::GetInfoAndFlush(sender)) => self.get_info_and_flush(sender).await,
                        Some(OpLogMessage::LogBundle(bundle)) => self.process_bundle(&bundle),
                        Some(OpLogMessage::LogDefinition(def)) => self.process_definition(&def),
                        Some(OpLogMessage::Log(log)) => self.process_log(&log),
                        Some(OpLogMessage::CleanUpRemoveAllDefinitions) => {
                            self.clean_up_remove_all_definitions()
                        }
                        Some(OpLogMessage::CleanUpDefinition(def)) => self.clean_up_definition(def),
                        Some(OpLogMessage::CleanUpBundle(bundle)) => self.clean_up_bundle(bundle),
                    }
                }
            }
        }

        self.flush().await;
    }

    fn process_bundle(&mut self, bundle: &OpLogBundle) {
        bundle
            .definitions
            .iter()
            .for_each(|def| self.process_definition(def));

        bundle.logs.iter().for_each(|log| self.process_log(log));
    }

    fn process_definition(&mut self, def: &OpLogDefinition) {
        self.def(
            def.log_name.as_str(),
            def.path.as_str(),
            def.log_type,
            &def.options,
            def.flush_interval,
            def.header.as_str(),
            def.auto_remove_definition,
        )
    }

    fn process_log(&mut self, log: &OpLogData) {
        self.log(log.log_name.as_str(), log.date, log.log.as_str())
    }
}
