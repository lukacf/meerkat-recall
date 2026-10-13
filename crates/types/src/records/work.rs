//! Work scheduler types for durable job execution.
//!
//! Per spec §5, the work scheduler provides a durable queue + DAG executor.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;

use crate::error::TypeError;
use crate::records::base::BaseFields;
use crate::Link;

/// Status of a work item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkItemStatus {
    /// Waiting for dependencies.
    #[default]
    Pending,
    /// Dependencies satisfied, ready to run.
    Runnable,
    /// Currently executing (leased).
    Running,
    /// Completed successfully.
    Done,
    /// Failed after all retries.
    Failed,
    /// Cancelled by operator.
    Cancelled,
}

impl std::fmt::Display for WorkItemStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "pending"),
            Self::Runnable => write!(f, "runnable"),
            Self::Running => write!(f, "running"),
            Self::Done => write!(f, "done"),
            Self::Failed => write!(f, "failed"),
            Self::Cancelled => write!(f, "cancelled"),
        }
    }
}

impl FromStr for WorkItemStatus {
    type Err = TypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pending" => Ok(Self::Pending),
            "runnable" => Ok(Self::Runnable),
            "running" => Ok(Self::Running),
            "done" => Ok(Self::Done),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            _ => Err(TypeError::InvalidEnumVariant {
                field: "work_item_status".to_string(),
                value: s.to_string(),
            }),
        }
    }
}

impl<'de> Deserialize<'de> for WorkItemStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, Visitor};
        use std::fmt;

        struct WorkItemStatusVisitor;

        impl Visitor<'_> for WorkItemStatusVisitor {
            type Value = WorkItemStatus;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string for WorkItemStatus")
            }

            fn visit_str<E>(self, value: &str) -> Result<WorkItemStatus, E>
            where
                E: de::Error,
            {
                WorkItemStatus::from_str(value).map_err(|e| de::Error::custom(e.to_string()))
            }
        }

        deserializer.deserialize_any(WorkItemStatusVisitor)
    }
}

/// Determinism level of a worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Determinism {
    /// Fully deterministic (same inputs → same outputs).
    #[default]
    Deterministic,
    /// Single LLM call, bounded but non-deterministic.
    LlmSingleShot,
    /// Internal agentic loop, bounded by time/tokens.
    AgenticLoop,
}

impl std::fmt::Display for Determinism {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Deterministic => write!(f, "deterministic"),
            Self::LlmSingleShot => write!(f, "llm_single_shot"),
            Self::AgenticLoop => write!(f, "agentic_loop"),
        }
    }
}

impl FromStr for Determinism {
    type Err = TypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "deterministic" => Ok(Self::Deterministic),
            "llm_single_shot" => Ok(Self::LlmSingleShot),
            "agentic_loop" => Ok(Self::AgenticLoop),
            _ => Err(TypeError::InvalidEnumVariant {
                field: "determinism".to_string(),
                value: s.to_string(),
            }),
        }
    }
}

impl<'de> Deserialize<'de> for Determinism {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, Visitor};
        use std::fmt;

        struct DeterminismVisitor;

        impl Visitor<'_> for DeterminismVisitor {
            type Value = Determinism;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string for Determinism")
            }

            fn visit_str<E>(self, value: &str) -> Result<Determinism, E>
            where
                E: de::Error,
            {
                Determinism::from_str(value).map_err(|e| de::Error::custom(e.to_string()))
            }
        }

        deserializer.deserialize_any(DeterminismVisitor)
    }
}

/// Status of a work artifact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactStatus {
    /// Staged but not yet validated.
    #[default]
    Draft,
    /// Passed validation.
    Validated,
    /// Commit has been claimed and is in progress.
    Committing,
    /// Written to canonical tables.
    Committed,
    /// Validation or commit failed.
    Rejected,
}

impl std::fmt::Display for ArtifactStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Draft => write!(f, "draft"),
            Self::Validated => write!(f, "validated"),
            Self::Committing => write!(f, "committing"),
            Self::Committed => write!(f, "committed"),
            Self::Rejected => write!(f, "rejected"),
        }
    }
}

impl FromStr for ArtifactStatus {
    type Err = TypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "draft" => Ok(Self::Draft),
            "validated" => Ok(Self::Validated),
            "committing" => Ok(Self::Committing),
            "committed" => Ok(Self::Committed),
            "rejected" => Ok(Self::Rejected),
            _ => Err(TypeError::InvalidEnumVariant {
                field: "artifact_status".to_string(),
                value: s.to_string(),
            }),
        }
    }
}

impl<'de> Deserialize<'de> for ArtifactStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, Visitor};
        use std::fmt;

        struct ArtifactStatusVisitor;

        impl Visitor<'_> for ArtifactStatusVisitor {
            type Value = ArtifactStatus;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string for ArtifactStatus")
            }

            fn visit_str<E>(self, value: &str) -> Result<ArtifactStatus, E>
            where
                E: de::Error,
            {
                ArtifactStatus::from_str(value).map_err(|e| de::Error::custom(e.to_string()))
            }
        }

        deserializer.deserialize_any(ArtifactStatusVisitor)
    }
}

/// Retry policy for a worker.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// Maximum number of attempts.
    pub max_attempts: u32,

    /// Initial backoff duration in seconds.
    pub initial_backoff_secs: u32,

    /// Maximum backoff duration in seconds.
    pub max_backoff_secs: u32,

    /// Backoff multiplier.
    #[serde(default = "default_backoff_multiplier")]
    pub backoff_multiplier: f32,

    /// Error patterns that are retryable.
    #[serde(default)]
    pub retryable_errors: Vec<String>,
}

const fn default_backoff_multiplier() -> f32 {
    2.0
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff_secs: 60,
            max_backoff_secs: 3600,
            backoff_multiplier: 2.0,
            retryable_errors: Vec::new(),
        }
    }
}

/// Plugin manifest declaring worker capabilities.
///
/// Per spec §2.4a: every plugin must declare its manifest.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PluginManifest {
    /// Unique plugin identifier.
    pub plugin_id: String,

    /// Semantic version.
    pub version: String,

    /// Plugin type.
    pub plugin_type: String,

    /// ABAC scopes required.
    #[serde(default)]
    pub required_scopes: Vec<String>,

    /// Tools the plugin may call.
    #[serde(default)]
    pub tool_access: Vec<String>,

    /// Work kinds this plugin handles.
    #[serde(default)]
    pub work_kinds: Vec<String>,

    /// Determinism level.
    #[serde(default)]
    pub determinism: Determinism,

    /// Maximum tokens (for LLM/agentic workers).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,

    /// Maximum wall time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_wall_time_secs: Option<u32>,

    /// Input record types.
    #[serde(default)]
    pub input_types: Vec<String>,

    /// Output artifact types.
    #[serde(default)]
    pub output_types: Vec<String>,

    /// Validator profile for outputs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validator_profile: Option<String>,

    /// Configuration schema (JSON Schema).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_schema: Option<serde_json::Value>,

    /// Health check endpoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health_check: Option<String>,

    /// Retry policy.
    #[serde(default)]
    pub retry_policy: RetryPolicy,
}

/// Metrics collected during a work run.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct WorkRunMetrics {
    /// Total tokens used (for LLM workers).
    #[serde(default)]
    pub tokens_used: u32,

    /// Number of internal steps (for agentic loops).
    #[serde(default)]
    pub steps_taken: u32,

    /// Wall time in milliseconds.
    #[serde(default)]
    pub wall_time_ms: u64,

    /// Estimated cost in euros.
    #[serde(default)]
    pub estimated_cost_euros: f64,

    /// Custom metrics from the worker.
    #[serde(default)]
    pub custom: HashMap<String, serde_json::Value>,
}

/// A durable work item in the scheduler queue.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkItem {
    /// Base fields common to all records.
    #[serde(flatten)]
    pub base: BaseFields,

    /// Kind of work to perform.
    pub kind: String,

    /// Payload references (record IDs, not data).
    pub payload: serde_json::Value,

    /// Current status.
    #[serde(default)]
    pub status: WorkItemStatus,

    /// Priority (higher = more urgent).
    #[serde(default)]
    pub priority: i32,

    /// Idempotency key for deduplication.
    pub idempotency_key: String,

    /// Number of attempts made.
    #[serde(default)]
    pub attempts: u32,

    /// When the current lease expires (if running).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_expires_at: Option<DateTime<Utc>>,

    /// ID of the worker holding the lease.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leased_by: Option<String>,

    /// When to run next (for scheduled/delayed items).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_after: Option<DateTime<Utc>>,

    /// Last error message (if failed).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

/// Durable cold copy of a terminal work item, retaining its public identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkItemArchive {
    /// Archive identity and the original security/space scope.
    #[serde(flatten)]
    pub base: BaseFields,
    /// Original public work item ID.
    pub original_id: crate::RecordId,
    /// When the terminal record left the hot queue.
    pub archived_at: DateTime<Utc>,
    /// Complete original record, including its original ID.
    pub record: WorkItem,
}

/// Durable cold copy of a completed execution attempt.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkRunArchive {
    /// Archive identity and the original security/space scope.
    #[serde(flatten)]
    pub base: BaseFields,
    /// Original public work run ID.
    pub original_id: crate::RecordId,
    /// When the completed attempt left the hot table.
    pub archived_at: DateTime<Utc>,
    /// Complete original record, including provenance and usage metrics.
    pub record: WorkRun,
}

/// Dependency edge between work items.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkEdge {
    /// Base fields common to all records.
    #[serde(flatten)]
    pub base: BaseFields,

    /// The work item that must complete first.
    pub from: Link<WorkItem>,

    /// The work item that depends on the first.
    pub to: Link<WorkItem>,

    /// Type of dependency.
    #[serde(default = "default_edge_type")]
    pub edge_type: String,
}

fn default_edge_type() -> String {
    "depends_on".to_string()
}

/// A single execution attempt of a work item.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkRun {
    /// Base fields common to all records.
    #[serde(flatten)]
    pub base: BaseFields,

    /// The work item being executed.
    pub work_item: Link<WorkItem>,

    /// Attempt number (1-indexed).
    pub attempt: u32,

    /// When the run started.
    pub started_at: DateTime<Utc>,

    /// When the run completed (if finished).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,

    /// Whether the run succeeded.
    #[serde(default)]
    pub success: bool,

    /// Error message (if failed).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,

    /// Metrics collected during the run.
    #[serde(default)]
    pub metrics: WorkRunMetrics,

    /// Worker that executed this run.
    pub worker_id: String,

    /// Worker version.
    pub worker_version: String,

    /// Model version (for LLM workers).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_version: Option<String>,

    /// Prompt identifier (template/version) used by the worker.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_id: Option<String>,

    /// Config hash (for reproducibility).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_hash: Option<String>,
}

/// A staged artifact produced by a work run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkArtifact {
    /// Base fields common to all records.
    #[serde(flatten)]
    pub base: BaseFields,

    /// The work run that produced this artifact.
    pub work_run: Link<WorkRun>,

    /// Artifact type (e.g., `knowledge_candidates`, `resolution_patch_set`).
    pub artifact_type: String,

    /// The staged data.
    pub data: serde_json::Value,

    /// Current status.
    #[serde(default)]
    pub status: ArtifactStatus,

    /// Validation errors (if rejected).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validation_errors: Option<Vec<String>>,

    /// When the artifact was validated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validated_at: Option<DateTime<Utc>>,

    /// When the artifact was committed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub committed_at: Option<DateTime<Utc>>,
}

/// A failed work item moved to the dead letter queue.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkDeadletter {
    /// Base fields common to all records.
    #[serde(flatten)]
    pub base: BaseFields,

    /// The original work item.
    pub work_item: Link<WorkItem>,

    /// All runs that were attempted.
    #[serde(default)]
    pub runs: Vec<Link<WorkRun>>,

    /// Final error message.
    pub final_error: String,

    /// When the item was moved to deadletter.
    pub deadlettered_at: DateTime<Utc>,

    /// Whether an operator has acknowledged this failure.
    #[serde(default)]
    pub acknowledged: bool,

    /// Operator notes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::float_cmp,
        unused_imports,
        dead_code
    )]
    use super::*;

    #[test]
    fn test_work_item_status_roundtrip() {
        for status in [
            WorkItemStatus::Pending,
            WorkItemStatus::Runnable,
            WorkItemStatus::Running,
            WorkItemStatus::Done,
            WorkItemStatus::Failed,
            WorkItemStatus::Cancelled,
        ] {
            let s = status.to_string();
            let parsed = WorkItemStatus::from_str(&s).unwrap();
            assert_eq!(status, parsed);
        }
    }

    #[test]
    fn test_determinism_roundtrip() {
        for det in [
            Determinism::Deterministic,
            Determinism::LlmSingleShot,
            Determinism::AgenticLoop,
        ] {
            let s = det.to_string();
            let parsed = Determinism::from_str(&s).unwrap();
            assert_eq!(det, parsed);
        }
    }

    #[test]
    fn test_artifact_status_serde() {
        let status = ArtifactStatus::Validated;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, "\"validated\"");

        let parsed: ArtifactStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, status);
    }

    #[test]
    fn test_retry_policy_default() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.max_attempts, 3);
        assert_eq!(policy.initial_backoff_secs, 60);
        assert_eq!(policy.backoff_multiplier, 2.0);
    }
}
