//! Reprocessing and backfill types.
//!
//! Per spec §16, these types manage replay when extractors or semantics improve.

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

use crate::error::TypeError;
use crate::records::base::BaseFields;

/// Status of a reprocess job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReprocessStatus {
    /// Job is queued.
    #[default]
    Pending,
    /// Job is running.
    Running,
    /// Job is paused (e.g., for budget).
    Paused,
    /// Job completed successfully.
    Completed,
    /// Job failed.
    Failed,
    /// Job was cancelled.
    Cancelled,
}

impl std::fmt::Display for ReprocessStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "pending"),
            Self::Running => write!(f, "running"),
            Self::Paused => write!(f, "paused"),
            Self::Completed => write!(f, "completed"),
            Self::Failed => write!(f, "failed"),
            Self::Cancelled => write!(f, "cancelled"),
        }
    }
}

impl FromStr for ReprocessStatus {
    type Err = TypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pending" => Ok(Self::Pending),
            "running" => Ok(Self::Running),
            "paused" => Ok(Self::Paused),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            _ => Err(TypeError::InvalidEnumVariant {
                field: "reprocess_status".to_string(),
                value: s.to_string(),
            }),
        }
    }
}

impl<'de> Deserialize<'de> for ReprocessStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, Visitor};
        use std::fmt;

        struct ReprocessStatusVisitor;

        impl Visitor<'_> for ReprocessStatusVisitor {
            type Value = ReprocessStatus;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string for ReprocessStatus")
            }

            fn visit_str<E>(self, value: &str) -> Result<ReprocessStatus, E>
            where
                E: de::Error,
            {
                ReprocessStatus::from_str(value).map_err(|e| de::Error::custom(e.to_string()))
            }
        }

        deserializer.deserialize_any(ReprocessStatusVisitor)
    }
}

/// Target stage for a reprocess job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ReprocessTargetStage {
    /// Re-render source content.
    ReRender,
    /// Re-chunk rendered content.
    ReChunk,
    /// Re-extract assertions.
    ReExtract,
    /// Re-resolve assertions.
    ReResolve,
    /// Re-run truth maintenance.
    ReMaintain,
}

impl ReprocessTargetStage {
    /// Returns all reprocess target stages.
    #[must_use]
    pub fn all() -> Vec<Self> {
        vec![
            Self::ReRender,
            Self::ReChunk,
            Self::ReExtract,
            Self::ReResolve,
            Self::ReMaintain,
        ]
    }
}

/// Default target stages for reprocess jobs.
#[must_use]
pub fn default_reprocess_target_stages() -> Vec<ReprocessTargetStage> {
    ReprocessTargetStage::all()
}

/// Criteria for selecting sources to reprocess.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct SelectionCriteria {
    /// Source types to include.
    #[serde(default)]
    pub source_types: Vec<String>,

    /// Date range start (inclusive).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub date_from: Option<NaiveDate>,

    /// Date range end (inclusive).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub date_to: Option<NaiveDate>,

    /// Reprocess sources with extractor version before this.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extractor_version_before: Option<String>,

    /// Reprocess sources using these predicate IDs.
    #[serde(default)]
    pub predicate_ids: Vec<String>,

    /// Space IDs to include (empty = all spaces).
    #[serde(default)]
    pub space_ids: Vec<String>,

    /// Only reprocess sources with specific status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_status: Option<String>,

    /// Custom filter expression.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_filter: Option<String>,
}

/// Progress tracking for a reprocess job.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ReprocessProgress {
    /// Total sources to process.
    pub total_sources: u64,

    /// Sources processed so far.
    pub processed_sources: u64,

    /// Sources that succeeded.
    pub succeeded_sources: u64,

    /// Sources that failed.
    pub failed_sources: u64,

    /// Sources skipped (e.g., no changes needed).
    pub skipped_sources: u64,

    /// Progress percentage (0.0 to 100.0).
    pub progress_percent: f32,

    /// Estimated time remaining in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub eta_seconds: Option<u64>,

    /// Cost spent so far in euros.
    pub cost_spent_euros: f64,
}

/// A job to reprocess a batch of sources.
///
/// Per spec §16.2: backfill is driven by reprocess jobs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReprocessJob {
    /// Base fields common to all records.
    #[serde(flatten)]
    pub base: BaseFields,

    /// Job name/description.
    pub name: String,

    /// Selection criteria for sources.
    pub selection_criteria: SelectionCriteria,

    /// Target pipeline stages to re-run.
    #[serde(default = "default_reprocess_target_stages")]
    pub target_stages: Vec<ReprocessTargetStage>,

    /// Current status.
    #[serde(default)]
    pub status: ReprocessStatus,

    /// Progress tracking.
    #[serde(default)]
    pub progress: ReprocessProgress,

    /// Priority (higher = more urgent).
    #[serde(default)]
    pub priority: i32,

    /// Budget limit in euros (None = no limit).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget_limit_euros: Option<f64>,

    /// Max sources per day (for pacing).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_sources_per_day: Option<u32>,

    /// When the job was started.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,

    /// When the job was completed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,

    /// Error message (if failed).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,

    /// Reason for creation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        unused_imports,
        dead_code
    )]
    use super::*;

    #[test]
    fn test_reprocess_status_roundtrip() {
        for status in [
            ReprocessStatus::Pending,
            ReprocessStatus::Running,
            ReprocessStatus::Paused,
            ReprocessStatus::Completed,
            ReprocessStatus::Failed,
            ReprocessStatus::Cancelled,
        ] {
            let s = status.to_string();
            let parsed = ReprocessStatus::from_str(&s).unwrap();
            assert_eq!(status, parsed);
        }
    }

    #[test]
    fn test_reprocess_status_serde() {
        let status = ReprocessStatus::Running;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, "\"running\"");

        let parsed: ReprocessStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, status);
    }

    #[test]
    fn test_selection_criteria_default() {
        let criteria = SelectionCriteria::default();
        assert!(criteria.source_types.is_empty());
        assert!(criteria.date_from.is_none());
        assert!(criteria.predicate_ids.is_empty());
    }

    #[test]
    fn test_progress_default() {
        let progress = ReprocessProgress::default();
        assert_eq!(progress.total_sources, 0);
        assert_eq!(progress.processed_sources, 0);
        assert!((progress.progress_percent - 0.0).abs() < f32::EPSILON);
    }
}
