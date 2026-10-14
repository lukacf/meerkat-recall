//! Current read of an originally committed generated definition and its inputs.
use super::{
    async_trait, wiki_source_dependency_matches, BaseFields, CurrentRead, ReadAuthorization,
    ReadEligibility, Repository, SourceRepository, StorageResult, TableName,
};
use crate::canonical_commit::{CanonicalRecord, CanonicalRepository};
use elephant_types::records::{KnowledgeViewDefinition, KnowledgeViewPublicationStatus};
use std::collections::BTreeSet;
use std::str::FromStr;

#[async_trait]
impl CurrentRead for KnowledgeViewDefinition {
    fn read_base(&self) -> &BaseFields {
        &self.base
    }

    #[allow(
        clippy::too_many_lines,
        reason = "Keep the original definition, input and dependency authorization checks in their existing single fenced read traversal."
    )]
    async fn retain_current(
        &mut self,
        reader: &ReadEligibility,
        authorize: &ReadAuthorization<'_>,
    ) -> StorageResult<bool> {
        if self.publication_status == KnowledgeViewPublicationStatus::Hidden {
            return Ok(false);
        }
        // Direct intrinsic configuration keeps its existing strict lineage path.
        // Any computed chain requires its exact original private decision proof;
        // a still-current source support ledger cannot substitute for that proof.
        if self.base.provenance.extractor.is_none()
            && self.base.provenance.work_run_id.is_none()
            && self.base.provenance.chain.is_empty()
        {
            return reader.base_support(&self.base, authorize).await;
        }
        let Some(inputs) = CanonicalRepository::new(reader.storage.clone())
            .definition_decision_inputs(self)
            .await?
        else {
            return Ok(false);
        };
        if inputs.is_empty() {
            return Ok(false);
        }
        // The actual sealed finite inventory bounds this traversal. All
        // references must be original captured inputs; never expand from a new
        // current row or truncate a legitimate full-graph inventory by prefix.
        let original_ids = inputs
            .iter()
            .map(|row| row.base().id.clone())
            .collect::<BTreeSet<_>>();
        if original_ids.len() != inputs.len() {
            return Ok(false);
        }
        let mut pending = inputs;
        let mut visited = BTreeSet::new();
        while let Some(input) = pending.pop() {
            let original = input.base().clone();
            if original.space_id != self.base.space_id || original.deleted_at.is_some() {
                return Ok(false);
            }
            if !visited.insert(original.id.clone()) {
                continue;
            }
            let effective = crate::canonical_definition::input_envelope(&input)?;
            let root_allowed = authorize(&effective);
            let scoped = |base: &BaseFields| {
                if base.id == original.id && base.space_id == original.space_id {
                    *base == original && root_allowed
                } else {
                    authorize(base)
                }
            };
            if !root_allowed {
                return Ok(false);
            }
            macro_rules! project {
                ($row:expr) => {{
                    let row = $row;
                    let Some(current) = reader.project((*row).clone(), &scoped).await? else {
                        return Ok(false);
                    };
                    if current != *row {
                        return Ok(false);
                    }
                }};
            }
            let mut references = Vec::new();
            match input {
                CanonicalRecord::Source(row) => {
                    // The private decision returns its exact sealed published
                    // input. The live Source may already carry desired R2;
                    // register/authorize that actual row before projecting R1.
                    let Some(raw) = SourceRepository::new(reader.storage.clone())
                        .get(&row.base.space_id, &row.base.id)
                        .await?
                    else {
                        return Ok(false);
                    };
                    let current_envelope = crate::canonical_definition::input_envelope(
                        &CanonicalRecord::Source(Box::new(raw.clone())),
                    )?;
                    if !authorize(&current_envelope) {
                        return Ok(false);
                    }
                    let raw_base = raw.base.clone();
                    let source_scope = |base: &BaseFields| {
                        if base.id == original.id && base.space_id == original.space_id {
                            (*base == original || *base == raw_base) && root_allowed
                        } else {
                            authorize(base)
                        }
                    };
                    let Some(current) = reader.project(raw, &source_scope).await? else {
                        return Ok(false);
                    };
                    if !wiki_source_dependency_matches(Some(row.base.updated_at), &row, &current) {
                        return Ok(false);
                    }
                }
                CanonicalRecord::Doc(row) => project!(row),
                CanonicalRecord::DocRevision(row) => project!(row),
                CanonicalRecord::DocNode(row) => {
                    // The existing projection checks the exact stored row, then
                    // may adorn node text from its verified immutable revision.
                    if reader.project(*row, &scoped).await?.is_none() {
                        return Ok(false);
                    }
                }
                CanonicalRecord::Entity(row) => project!(row),
                CanonicalRecord::Assertion(row) => project!(row),
                CanonicalRecord::Rel(row) => project!(row),
                CanonicalRecord::Attribute(row) => project!(row),
                CanonicalRecord::Event(row) => project!(row),
                CanonicalRecord::TruthSlot(row) => project!(row),
                CanonicalRecord::Freshness(row) => project!(row),
                CanonicalRecord::EntityIdentityKey(row) => {
                    if !reader.matches_content_support(&row.base, &row).await?
                        || !reader.base_support(&row.base, &scoped).await?
                    {
                        return Ok(false);
                    }
                    references.push(row.entity.id().clone());
                    references.extend(row.evidence.iter().map(|source| source.id().clone()));
                }
                CanonicalRecord::ConflictGroup(row) => {
                    if !reader.matches_content_support(&row.base, &row).await?
                        || !reader.base_support(&row.base, &scoped).await?
                        || row.claim_ids.is_empty()
                    {
                        return Ok(false);
                    }
                    for claim in &row.claim_ids {
                        let Ok(id) = elephant_types::RecordId::from_str(claim) else {
                            return Ok(false);
                        };
                        if !matches!(
                            id.table(),
                            TableName::Rel | TableName::Attribute | TableName::Event
                        ) {
                            return Ok(false);
                        }
                        references.push(id);
                    }
                }
                CanonicalRecord::PredicateRegistry(row) => {
                    if !reader.matches_content_support(&row.base, &row).await?
                        || !reader.base_support(&row.base, &scoped).await?
                    {
                        return Ok(false);
                    }
                }
                CanonicalRecord::PredicateAlias(row) => {
                    if !reader.matches_content_support(&row.base, &row).await?
                        || !reader.base_support(&row.base, &scoped).await?
                    {
                        return Ok(false);
                    }
                }
                // Definitions, views, operational rows and audit records cannot
                // recursively certify another definition's semantic inventory.
                _ => return Ok(false),
            }
            if references.iter().any(|id| !original_ids.contains(id)) {
                return Ok(false);
            }
        }
        {
            let mut observed = reader.observed_definitions.lock().map_err(|_| {
                crate::StorageError::Conflict("Read definition fence unavailable".into())
            })?;
            let key = (self.base.space_id.clone(), self.base.id.to_string());
            if observed.get(&key).is_some_and(|before| before != self) {
                return Err(crate::StorageError::Conflict(
                    "Definition changed during read; retry".into(),
                ));
            }
            observed.insert(key, self.clone());
        }
        reader.verify_current().await?;
        Ok(true)
    }
}

#[cfg(test)]
#[path = "read_definition_tests.rs"]
mod tests;
