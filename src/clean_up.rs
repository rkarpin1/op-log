// -------------------------------------------------------------------------------------------------
//   Copyright 2024-2025 (c) Robert Karpiński
// -------------------------------------------------------------------------------------------------

use crate::{LogDefinition, OpLogWorker};
use std::time::Duration;

impl OpLogWorker {
    pub(crate) fn clean_up(&mut self) {
        self.definitions.values_mut().for_each(|d| d.clean_up());

        self.definitions.retain(|_, d| {
            !d.files.is_empty() || d.last_time_use.elapsed() < Duration::from_secs(10 * 60)
        });
    }
}

impl LogDefinition {
    pub fn clean_up(&mut self) {
        self.files.retain(|_, f| {
            !f.logs.is_empty() || f.last_time_use.elapsed() < Duration::from_secs(10 * 60)
        });
    }
}