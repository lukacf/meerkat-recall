//! Entity merge strategies.

use elephant_types::RecordId;
use serde::{Deserialize, Serialize};

/// Merge strategy options.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum MergeStrategy {
    /// Keep entity A, B becomes alias.
    #[default]
    KeepA,
    /// Keep entity B, A becomes alias.
    KeepB,
    /// Create new entity C, A and B become aliases.
    Combine,
}

/// Result of a merge operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeResult {
    /// The surviving entity.
    pub survivor: RecordId,
    /// Entities that were merged (now tombstones).
    pub merged: Vec<RecordId>,
    /// Strategy that was applied.
    pub strategy: MergeStrategy,
    /// Number of references rewritten.
    #[serde(default)]
    pub refs_rewritten: usize,
}

/// Merge lineage record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeLineage {
    /// Entity that was merged from.
    pub from: RecordId,
    /// Entity that was merged into.
    pub into: RecordId,
    /// Strategy used.
    pub strategy: MergeStrategy,
    /// Actor who performed the merge.
    pub decided_by: String,
    /// Run ID if part of a batch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
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
    use elephant_types::ids::TableName;

    #[test]
    fn test_merge_strategy() {
        let a = RecordId::new_v4(TableName::Entity);
        let b = RecordId::new_v4(TableName::Entity);

        let result = MergeResult {
            survivor: a,
            merged: vec![b],
            strategy: MergeStrategy::KeepA,
            refs_rewritten: 0,
        };

        assert_eq!(result.strategy, MergeStrategy::KeepA);
        assert_eq!(result.merged.len(), 1);
    }
}
