// -------------------------------------------------------------------------------------------------
//   Copyright 2024-2025 (c) Robert Karpiński
// -------------------------------------------------------------------------------------------------

use crate::OpLogWorker;
use std::time::Duration;

impl OpLogWorker {
    pub(crate) fn clean_up(&mut self) {
        self.definitions.values_mut().for_each(|d| {
            d.files.retain(|_, f| {
                !f.logs.is_empty() || f.last_time_use.elapsed() < Duration::from_secs(10 * 60)
            })
        });

        self.definitions.retain(|_, d| {
            !d.auto_remove_definition
                || (!d.files.is_empty() || d.last_time_use.elapsed() < Duration::from_secs(10 * 60))
        });
    }
}

#[cfg(test)]
mod tests {
    use crate::OpLogWorker;
    use crate::messages::{OpLogDefinition, OpLogType};
    use chrono::Utc;
    use std::time::Duration;

    const IDLE: Duration = Duration::from_secs(11 * 60);

    fn worker_with_definition(auto_remove_definition: bool) -> OpLogWorker {
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let mut worker = OpLogWorker::new(rx);
        worker.def(
            OpLogDefinition::new("quiet", ".")
                .log_type(OpLogType::PerDay)
                .flush_interval(Duration::from_secs(1))
                .auto_remove_definition(auto_remove_definition),
        );
        worker
    }

    fn queued_entries(worker: &OpLogWorker, name: &str) -> usize {
        worker
            .definitions
            .get(name)
            .map(|d| d.files.values().map(|f| f.logs.len()).sum())
            .unwrap_or(0)
    }

    // Consumers register their definitions once at startup and then only call
    // `log()`. A definition that is quiet for ten minutes (a night without
    // traffic) must still be there afterwards — otherwise every later entry
    // for that name is discarded silently, for the rest of the process's life.
    #[tokio::test(start_paused = true)]
    async fn idle_definition_is_kept_unless_auto_remove_is_requested() {
        let mut worker = worker_with_definition(false);

        tokio::time::advance(IDLE).await;
        worker.clean_up();

        assert!(
            worker.definitions.contains_key("quiet"),
            "a definition without auto_remove_definition must survive an idle period"
        );
        worker.log("quiet", Utc::now(), "first entry after a quiet period");
        assert_eq!(
            queued_entries(&worker, "quiet"),
            1,
            "entry must be queued, not dropped"
        );
    }

    // `auto_remove_definition(true)` opts into the idle cleanup: the definition
    // stays while it is in use and goes away once it has been idle for >10 min.
    #[tokio::test(start_paused = true)]
    async fn auto_remove_definition_goes_away_only_after_idle_period() {
        let mut worker = worker_with_definition(true);

        tokio::time::advance(Duration::from_secs(60)).await;
        worker.clean_up();
        assert!(
            worker.definitions.contains_key("quiet"),
            "a recently used auto-remove definition must not be removed yet"
        );

        tokio::time::advance(IDLE).await;
        worker.clean_up();
        assert!(
            !worker.definitions.contains_key("quiet"),
            "an auto-remove definition idle for >10 min must be removed"
        );
    }
}
