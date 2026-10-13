//! Transactional invalidation of staged embedding coverage.

use elephant_types::records::SpaceConfig;
use elephant_types::TableName;

use crate::{ElephantDbError, ElephantDbTx, Result};

impl ElephantDbTx<'_> {
    /// Called by the kernel's combined mutation hook. Config writes do not
    /// recurse, and rollback restores the epoch together with the content row.
    pub(super) fn bump_index_generation(&mut self, table: TableName, space_id: &str) -> Result<()> {
        if !matches!(
            table,
            TableName::Source
                | TableName::Doc
                | TableName::DocRevision
                | TableName::DocNode
                | TableName::Entity
        ) {
            return Ok(());
        }
        self.bump_space_index_generation(space_id)
    }

    /// Control visibility participates in the same existing coverage authority.
    pub(super) fn bump_space_index_generation(&mut self, space_id: &str) -> Result<()> {
        for mut config in self.scan::<SpaceConfig>()? {
            if config.base.space_id != space_id || config.base.deleted_at.is_some() {
                continue;
            }
            let Some(processing) = config.processing.as_mut() else {
                continue;
            };
            processing.index_generation =
                processing.index_generation.checked_add(1).ok_or_else(|| {
                    ElephantDbError::DataIntegrity("space index generation exhausted".into())
                })?;
            config.base.updated_at = chrono::Utc::now();
            let _ = self.replace(config)?;
        }
        Ok(())
    }
}

/// These controls can change read eligibility without changing indexed text.
pub fn control_visibility_changed(
    table: &str,
    before: Option<&serde_json::Value>,
    after: Option<&serde_json::Value>,
) -> bool {
    if !matches!(
        table,
        "source_head"
            | "source_effect_support"
            | "source_revision_mapping"
            | "source_publication_completion"
            | "source_publication_manifest"
            | "source_publication_manifest_history"
    ) {
        return false;
    }
    let (Some(before), Some(after)) = (before, after) else {
        return true;
    };
    if before
        .get("payload")
        .is_some_and(serde_json::Value::is_string)
        && after
            .get("payload")
            .is_some_and(serde_json::Value::is_string)
    {
        before.get("space_id") != after.get("space_id")
            || before.get("payload") != after.get("payload")
            || before.get("state") != after.get("state")
    } else {
        before != after
    }
}
