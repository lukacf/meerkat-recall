//! Pipeline worker infrastructure.

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Worker configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    /// Maximum queue depth.
    pub max_queue_depth: usize,
    /// Retry delay on failure.
    pub retry_delay: Duration,
    /// Maximum retries before dead-letter.
    pub max_retries: u32,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            max_queue_depth: 1000,
            retry_delay: Duration::from_secs(5),
            max_retries: 3,
        }
    }
}

impl WorkerConfig {
    /// Creates a new worker configuration with default values.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the maximum queue depth.
    #[must_use]
    pub const fn with_max_queue_depth(mut self, depth: usize) -> Self {
        self.max_queue_depth = depth;
        self
    }

    /// Sets the retry delay.
    #[must_use]
    pub const fn with_retry_delay(mut self, delay: Duration) -> Self {
        self.retry_delay = delay;
        self
    }

    /// Sets the maximum retries.
    #[must_use]
    pub const fn with_max_retries(mut self, retries: u32) -> Self {
        self.max_retries = retries;
        self
    }
}

/// Worker status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerStatus {
    /// Worker is idle.
    Idle,
    /// Worker is processing.
    Processing,
    /// Worker is backing off due to errors.
    BackingOff,
    /// Worker is stopped.
    Stopped,
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::mixed_attributes_style,
        clippy::similar_names,
        clippy::type_complexity,
        clippy::float_cmp,
        clippy::const_is_empty,
        clippy::expect_used,
        clippy::panic,
        unused_imports,
        dead_code,
        clippy::too_many_lines,
        clippy::cognitive_complexity
    )]
    use super::*;

    #[test]
    fn test_worker_config_default() {
        let config = WorkerConfig::default();
        assert_eq!(config.max_queue_depth, 1000);
        assert_eq!(config.max_retries, 3);
    }

    #[test]
    fn test_worker_config_serde_roundtrip() {
        let config = WorkerConfig {
            max_queue_depth: 500,
            retry_delay: Duration::from_millis(1500),
            max_retries: 5,
        };

        let json = serde_json::to_string(&config).expect("serialize");
        let decoded: WorkerConfig = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(decoded.max_queue_depth, 500);
        assert_eq!(decoded.retry_delay, Duration::from_millis(1500));
        assert_eq!(decoded.max_retries, 5);
    }

    #[test]
    fn test_worker_status_variants() {
        let statuses = [
            WorkerStatus::Idle,
            WorkerStatus::Processing,
            WorkerStatus::BackingOff,
            WorkerStatus::Stopped,
        ];

        for status in &statuses {
            let count = statuses.iter().filter(|s| *s == status).count();
            assert_eq!(count, 1, "Each status variant should be unique");
        }
    }

    #[test]
    fn test_worker_status_serde() {
        let test_cases = [
            (WorkerStatus::Idle, "\"idle\""),
            (WorkerStatus::Processing, "\"processing\""),
            (WorkerStatus::BackingOff, "\"backing_off\""),
            (WorkerStatus::Stopped, "\"stopped\""),
        ];

        for (status, expected) in &test_cases {
            let json = serde_json::to_string(status).expect("serialize");
            assert_eq!(&json, expected);

            let decoded: WorkerStatus = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(&decoded, status);
        }
    }

    #[test]
    fn test_worker_config_zero_values() {
        let config = WorkerConfig {
            max_queue_depth: 0,
            retry_delay: Duration::ZERO,
            max_retries: 0,
        };

        let json = serde_json::to_string(&config).expect("serialize");
        let decoded: WorkerConfig = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(decoded.max_queue_depth, 0);
        assert_eq!(decoded.retry_delay, Duration::ZERO);
        assert_eq!(decoded.max_retries, 0);
    }

    #[test]
    fn test_worker_config_large_values() {
        let config = WorkerConfig {
            max_queue_depth: usize::MAX,
            retry_delay: Duration::from_secs(86400), // 1 day
            max_retries: u32::MAX,
        };

        let json = serde_json::to_string(&config).expect("serialize");
        let decoded: WorkerConfig = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(decoded.max_queue_depth, usize::MAX);
        assert_eq!(decoded.retry_delay, Duration::from_secs(86400));
        assert_eq!(decoded.max_retries, u32::MAX);
    }

    #[test]
    fn test_worker_config_new() {
        let config = WorkerConfig::new();
        assert_eq!(config.max_queue_depth, 1000);
        assert_eq!(config.retry_delay, Duration::from_secs(5));
        assert_eq!(config.max_retries, 3);
    }

    #[test]
    fn test_worker_config_builder() {
        let config = WorkerConfig::new()
            .with_max_queue_depth(500)
            .with_retry_delay(Duration::from_millis(2500))
            .with_max_retries(5);

        assert_eq!(config.max_queue_depth, 500);
        assert_eq!(config.retry_delay, Duration::from_millis(2500));
        assert_eq!(config.max_retries, 5);
    }
}
