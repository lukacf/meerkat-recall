//! Progressive ingestion and budget types.
//!
//! Per spec §6, these types manage ingestion modes and cost enforcement.

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;

use crate::error::TypeError;
use crate::records::base::BaseFields;
use crate::records::source::Source;
use crate::{Link, RecordId};

/// Ingestion mode for a source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IngestMode {
    /// Store only optional metrics.
    Skip,
    /// Store source + minimal doc metadata + external refs.
    #[default]
    Metadata,
    /// metadata + `doc_revision` + `doc_node`+chunk + embeddings.
    Retrieval,
    /// retrieval + assertions + resolution + candidate `facts/rels` (including event-like rels).
    Graph,
}

/// Worker kind used for budget guard records in cost ledger.
pub const BUDGET_GUARD_WORKER_KIND: &str = "__budget_guard__";

impl std::fmt::Display for IngestMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Skip => write!(f, "skip"),
            Self::Metadata => write!(f, "metadata"),
            Self::Retrieval => write!(f, "retrieval"),
            Self::Graph => write!(f, "graph"),
        }
    }
}

impl FromStr for IngestMode {
    type Err = TypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "skip" => Ok(Self::Skip),
            "metadata" => Ok(Self::Metadata),
            "retrieval" => Ok(Self::Retrieval),
            "graph" => Ok(Self::Graph),
            _ => Err(TypeError::InvalidEnumVariant {
                field: "ingest_mode".to_string(),
                value: s.to_string(),
            }),
        }
    }
}

impl<'de> Deserialize<'de> for IngestMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, Visitor};
        use std::fmt;

        struct IngestModeVisitor;

        impl Visitor<'_> for IngestModeVisitor {
            type Value = IngestMode;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string for IngestMode")
            }

            fn visit_str<E>(self, value: &str) -> Result<IngestMode, E>
            where
                E: de::Error,
            {
                IngestMode::from_str(value).map_err(|e| de::Error::custom(e.to_string()))
            }
        }

        deserializer.deserialize_any(IngestModeVisitor)
    }
}

/// Policy for handling budget overruns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DowngradePolicy {
    /// Reject over-budget items.
    Strict,
    /// Downgrade to lower mode.
    DegradeMode,
    /// Downgrade and queue for later retry.
    #[default]
    DegradeModeWithQueue,
    /// Alert only, don't reject.
    AlertOnly,
}

impl std::fmt::Display for DowngradePolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Strict => write!(f, "strict"),
            Self::DegradeMode => write!(f, "degrade_mode"),
            Self::DegradeModeWithQueue => write!(f, "degrade_mode_with_queue"),
            Self::AlertOnly => write!(f, "alert_only"),
        }
    }
}

impl FromStr for DowngradePolicy {
    type Err = TypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "strict" => Ok(Self::Strict),
            "degrade_mode" => Ok(Self::DegradeMode),
            "degrade_mode_with_queue" => Ok(Self::DegradeModeWithQueue),
            "alert_only" => Ok(Self::AlertOnly),
            _ => Err(TypeError::InvalidEnumVariant {
                field: "downgrade_policy".to_string(),
                value: s.to_string(),
            }),
        }
    }
}

impl<'de> Deserialize<'de> for DowngradePolicy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, Visitor};
        use std::fmt;

        struct DowngradePolicyVisitor;

        impl Visitor<'_> for DowngradePolicyVisitor {
            type Value = DowngradePolicy;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string for DowngradePolicy")
            }

            fn visit_str<E>(self, value: &str) -> Result<DowngradePolicy, E>
            where
                E: de::Error,
            {
                DowngradePolicy::from_str(value).map_err(|e| de::Error::custom(e.to_string()))
            }
        }

        deserializer.deserialize_any(DowngradePolicyVisitor)
    }
}

/// Budget configuration for a space.
///
/// Per spec §6.2: budgets are enforced at scheduling and commit time.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Budget {
    /// Daily euro budget for retrieval mode.
    #[serde(default)]
    pub daily_euros_retrieval: f64,

    /// Daily euro budget for graph mode.
    #[serde(default)]
    pub daily_euros_graph: f64,

    /// Max items per day for retrieval mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub daily_items_retrieval: Option<u32>,

    /// Max items per day for graph mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub daily_items_graph: Option<u32>,

    /// Max tokens per item.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub per_item_tokens: Option<u32>,

    /// Max wall time per item in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub per_item_wall_time_secs: Option<u32>,

    /// Concurrent worker limits per work kind.
    #[serde(default)]
    pub concurrent_workers: HashMap<String, u32>,

    /// Policy for handling budget overruns.
    #[serde(default)]
    pub downgrade_policy: DowngradePolicy,
}

impl Default for Budget {
    fn default() -> Self {
        Self {
            daily_euros_retrieval: 10.0,
            daily_euros_graph: 50.0,
            daily_items_retrieval: Some(1000),
            daily_items_graph: Some(10_000),
            per_item_tokens: Some(100_000),
            per_item_wall_time_secs: Some(300),
            concurrent_workers: HashMap::new(),
            downgrade_policy: DowngradePolicy::default(),
        }
    }
}

/// Snapshot of budget state at decision time.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BudgetSnapshot {
    /// Remaining euro budget for retrieval.
    pub remaining_euros_retrieval: f64,

    /// Remaining euro budget for graph.
    pub remaining_euros_graph: f64,

    /// Remaining items for retrieval.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remaining_items_retrieval: Option<u32>,

    /// Remaining items for graph.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remaining_items_graph: Option<u32>,

    /// Snapshot timestamp.
    pub snapshot_at: DateTime<Utc>,
}

/// Result of triage for a source.
///
/// Per spec §6.4: stores the decision from a triage plugin.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IngestDecision {
    /// Base fields common to all records.
    #[serde(flatten)]
    pub base: BaseFields,

    /// The source being triaged.
    pub source: Link<Source>,

    /// Decided ingestion mode.
    pub decision: IngestMode,

    /// Reason codes from the plugin (opaque strings).
    #[serde(default)]
    pub reason_codes: Vec<String>,

    /// Confidence in the decision (0.0 to 1.0).
    #[serde(default)]
    pub confidence: f32,

    /// Estimated cost in euros.
    #[serde(default)]
    pub estimated_cost: f64,

    /// Reserved cost ledger entry (if any).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_ledger_id: Option<RecordId>,

    /// Budget snapshot at decision time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget_snapshot: Option<BudgetSnapshot>,

    /// Version of the policy used.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy_version: Option<String>,

    /// Reason for downgrade (if mode was downgraded).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub downgrade_reason: Option<String>,

    /// Original mode before downgrade (if downgraded).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub original_mode: Option<IngestMode>,
}

/// Daily cost ledger entry.
///
/// Per spec §17: tracks actual costs by space, date, mode, and worker kind.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CostLedgerDaily {
    /// Base fields common to all records.
    #[serde(flatten)]
    pub base: BaseFields,

    /// Date of this ledger entry.
    pub date: NaiveDate,

    /// Ingestion mode.
    pub mode: IngestMode,

    /// Worker kind.
    pub worker_kind: String,

    /// Total euros spent.
    #[serde(default)]
    pub total_euros: f64,

    /// Total items processed.
    #[serde(default)]
    pub total_items: u32,

    /// Total tokens used.
    #[serde(default)]
    pub total_tokens: u64,

    /// Total runs recorded.
    #[serde(default)]
    pub total_runs: u32,

    /// Total failures recorded.
    #[serde(default)]
    pub total_failures: u32,

    /// Reserved (uncommitted) cost.
    #[serde(default)]
    pub reserved_euros: f64,

    /// Reconciled (committed) cost.
    #[serde(default)]
    pub reconciled_euros: f64,
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
    fn test_ingest_mode_roundtrip() {
        for mode in [
            IngestMode::Skip,
            IngestMode::Metadata,
            IngestMode::Retrieval,
            IngestMode::Graph,
        ] {
            let s = mode.to_string();
            let parsed = IngestMode::from_str(&s).unwrap();
            assert_eq!(mode, parsed);
        }
    }

    #[test]
    fn test_downgrade_policy_roundtrip() {
        for policy in [
            DowngradePolicy::Strict,
            DowngradePolicy::DegradeMode,
            DowngradePolicy::DegradeModeWithQueue,
            DowngradePolicy::AlertOnly,
        ] {
            let s = policy.to_string();
            let parsed = DowngradePolicy::from_str(&s).unwrap();
            assert_eq!(policy, parsed);
        }
    }

    #[test]
    fn test_budget_default() {
        let budget = Budget::default();
        assert!((budget.daily_euros_retrieval - 10.0).abs() < f64::EPSILON);
        assert!((budget.daily_euros_graph - 50.0).abs() < f64::EPSILON);
        assert_eq!(budget.daily_items_retrieval, Some(1000));
        assert_eq!(budget.daily_items_graph, Some(10_000));
        assert_eq!(
            budget.downgrade_policy,
            DowngradePolicy::DegradeModeWithQueue
        );
    }

    #[test]
    fn test_ingest_mode_serde() {
        let mode = IngestMode::Graph;
        let json = serde_json::to_string(&mode).unwrap();
        assert_eq!(json, "\"graph\"");

        let parsed: IngestMode = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, mode);
    }
}
