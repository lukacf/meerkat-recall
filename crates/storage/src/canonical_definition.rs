//! Original canonical authority for generated graph-view definitions.
//!
//! The chain entry is an address, never a certificate. Only the private payload
//! written with the actual canonical effects proves the original output and
//! material input rows. Retrieval must also authorize these inputs recursively.

use elephant_types::publication::{SourceAuthorizationEnvelope, SourceRevision};
use elephant_types::records::base::{BaseFields, ProvenanceStep, SubjectsState};
use elephant_types::records::{
    HasSecurity, KnowledgeViewDefinition, KnowledgeViewPublicationStatus, KnowledgeViewSelector,
};
use serde::{Deserialize, Serialize};

use crate::canonical_commit::{
    CanonicalCommit, CanonicalCommitReceipt, CanonicalMaintenanceOrigin, CanonicalRecord,
    CanonicalRepository, PublishedSourceFence,
};
use crate::source_decision::{SourceDecisionReceipt, DECISION_TABLE};
use crate::source_publication::{integrity, stable_key, SourcePublicationRepository};
use crate::{EmbeddedMutation, Repository, StorageClient, StorageResult};

const STEP: &str = "canonical_definition_decision_v1";
const FIELD: &str = "_definition_lineage";
const RECEIPT_TABLE: &str = "source_commit_receipt";

/// Addresses the original canonical decision. This does not grant authority;
/// the canonical transaction must seal the exact output and original inputs.
pub fn bind_definition_decision(
    definition: &mut KnowledgeViewDefinition,
    commit_id: &str,
) -> StorageResult<()> {
    if commit_id.trim().is_empty() || definition.base.space_id.trim().is_empty() {
        return Err(integrity("definition decision identity required"));
    }
    crate::knowledge_view_definition_repository::validate_definition(definition)?;
    definition
        .base
        .provenance
        .chain
        .retain(|step| step.step != STEP);
    definition.base.provenance.chain.push(ProvenanceStep {
        step: STEP.into(),
        input: format!(
            "{RECEIPT_TABLE}:{}",
            stable_key(&[&definition.base.space_id, commit_id])
        ),
        output: definition.base.id.to_string(),
    });
    Ok(())
}

fn pointer(definition: &KnowledgeViewDefinition) -> Option<&str> {
    let mut entries = definition
        .base
        .provenance
        .chain
        .iter()
        .filter(|step| step.step == STEP);
    let entry = entries.next()?;
    if entries.next().is_some() || entry.output != definition.base.id.to_string() {
        return None;
    }
    entry
        .input
        .strip_prefix("source_commit_receipt:")
        .filter(|key| !key.is_empty())
}

/// Semantic inputs of the generated definition.
///
/// Before-images of definitions
/// are CAS/inverse data, not authority for the newly generated content. Queue,
/// audit, and lifecycle bookkeeping similarly cannot certify an inventory.
const fn material_input(row: &CanonicalRecord) -> bool {
    matches!(
        row,
        CanonicalRecord::Entity(_)
            | CanonicalRecord::EntityIdentityKey(_)
            | CanonicalRecord::Assertion(_)
            | CanonicalRecord::Rel(_)
            | CanonicalRecord::Attribute(_)
            | CanonicalRecord::Event(_)
            | CanonicalRecord::Freshness(_)
            | CanonicalRecord::Source(_)
            | CanonicalRecord::Doc(_)
            | CanonicalRecord::DocRevision(_)
            | CanonicalRecord::DocNode(_)
            | CanonicalRecord::TruthSlot(_)
            | CanonicalRecord::ConflictGroup(_)
            | CanonicalRecord::PredicateRegistry(_)
            | CanonicalRecord::PredicateAlias(_)
    )
}

/// Effective typed input policy for definition derivation; original rows remain
/// unchanged in the canonical CAS and private receipt. Operational records are
/// not semantic inventory inputs.
pub fn input_envelope(row: &CanonicalRecord) -> StorageResult<BaseFields> {
    fn typed<T: HasSecurity>(base: &BaseFields, row: &T) -> BaseFields {
        let mut scope = base.clone();
        scope.subjects = row.subjects();
        scope.subjects_state = row.subjects_state();
        scope
    }
    Ok(match row {
        CanonicalRecord::Entity(row) => typed(&row.base, row.as_ref()),
        CanonicalRecord::Assertion(row) => typed(&row.base, row.as_ref()),
        CanonicalRecord::Rel(row) => typed(&row.base, row.as_ref()),
        CanonicalRecord::Attribute(row) => typed(&row.base, row.as_ref()),
        CanonicalRecord::Event(row) => typed(&row.base, row.as_ref()),
        CanonicalRecord::Freshness(row) => typed(&row.base, row.as_ref()),
        CanonicalRecord::Source(row) => typed(&row.base, row.as_ref()),
        CanonicalRecord::Doc(row) => typed(&row.base, row.as_ref()),
        CanonicalRecord::DocNode(row) => typed(&row.base, row.as_ref()),
        CanonicalRecord::TruthSlot(row) => typed(&row.base, row.as_ref()),
        CanonicalRecord::PredicateRegistry(row) => typed(&row.base, row.as_ref()),
        CanonicalRecord::PredicateAlias(row) => typed(&row.base, row.as_ref()),
        CanonicalRecord::EntityIdentityKey(_)
        | CanonicalRecord::DocRevision(_)
        | CanonicalRecord::ConflictGroup(_) => row.base().clone(),
        _ => return Err(integrity("record is not a definition inventory input")),
    })
}

/// Withdrawal carries no new content authority. A narrow visibility-only
/// transition keeps the original pointer but cannot make its changed row pass
/// the original receipt's exact after-image proof.
fn withdrawal_only(effect: &crate::canonical_commit::CanonicalEffect) -> bool {
    let (
        Some(CanonicalRecord::KnowledgeViewDefinition(before)),
        Some(CanonicalRecord::KnowledgeViewDefinition(after)),
    ) = (&effect.before, &effect.after)
    else {
        return false;
    };
    if after.publication_status != KnowledgeViewPublicationStatus::Hidden {
        return false;
    }
    let mut normalized = (**after).clone();
    normalized.publication_status = before.publication_status;
    normalized.base.updated_at = before.base.updated_at;
    normalized == **before
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DefinitionLineage {
    version: u8,
    definitions: Vec<KnowledgeViewDefinition>,
    inputs: Vec<CanonicalRecord>,
    // Exact published semantics, distinct from the raw current rows above which
    // remain the canonical transaction's original planning/CAS observations.
    #[serde(default)]
    published_sources: std::collections::BTreeMap<String, SourceRevision>,
}

impl DefinitionLineage {
    fn prepare(request: &CanonicalCommit) -> StorageResult<Option<Self>> {
        let definitions = request
            .plan
            .effects
            .iter()
            .filter(|effect| !withdrawal_only(effect))
            .filter_map(|effect| match &effect.after {
                Some(CanonicalRecord::KnowledgeViewDefinition(row)) => Some((**row).clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        if definitions.is_empty() {
            return Ok(None);
        }
        let derived = request
            .maintenance
            .as_ref()
            .is_some_and(|context| context.origin == CanonicalMaintenanceOrigin::Derived);
        if !derived {
            if definitions.iter().any(|row| {
                row.base
                    .provenance
                    .chain
                    .iter()
                    .any(|step| step.step == STEP)
            }) {
                return Err(integrity(
                    "definition decision requires derived canonical authority",
                ));
            }
            return Ok(None);
        }
        let inputs = request
            .plan
            .dependencies
            .iter()
            .filter(|row| material_input(row))
            .cloned()
            .collect::<Vec<_>>();
        if !inputs
            .iter()
            .any(|row| matches!(row, CanonicalRecord::Entity(_)))
        {
            return Err(integrity(
                "definition decision requires original entity inventory",
            ));
        }
        let key = stable_key(&[&request.space_id, &request.commit_id]);
        let mut ids = std::collections::BTreeSet::new();
        for input in &inputs {
            if input.base().space_id != request.space_id
                || input.base().deleted_at.is_some()
                || !ids.insert(input.base().id.clone())
            {
                return Err(integrity(
                    "definition inventory must be live, distinct and same-space",
                ));
            }
        }
        validate_definition_inputs(&definitions, &inputs, &key)?;
        Ok(Some(Self {
            version: 2,
            definitions,
            inputs,
            published_sources: std::collections::BTreeMap::new(),
        }))
    }
}

fn validate_definition_inputs(
    definitions: &[KnowledgeViewDefinition],
    inputs: &[CanonicalRecord],
    key: &str,
) -> StorageResult<()> {
    for definition in definitions {
        crate::knowledge_view_definition_repository::validate_definition(definition)?;
        if definition.base.deleted_at.is_some() || pointer(definition) != Some(key) {
            return Err(integrity(
                "definition decision pointer differs from actual commit",
            ));
        }
        for input in inputs {
            if !contains_scope(&input_envelope(input)?, &definition.base)
                || input_sources(input)
                    .iter()
                    .any(|source| !definition.base.provenance.sources.contains(source))
            {
                return Err(integrity("definition omits original inventory authority"));
            }
        }
        let roots = match &definition.selector {
            KnowledgeViewSelector::Topic { topic, .. } => vec![topic.id()],
            KnowledgeViewSelector::Community { seeds, .. } => {
                seeds.iter().map(elephant_types::Link::id).collect()
            }
            KnowledgeViewSelector::Query { .. } => Vec::new(),
        };
        if roots
            .into_iter()
            .chain(
                definition
                    .related_articles
                    .iter()
                    .map(elephant_types::Link::id),
            )
            .any(|id| {
                !inputs.iter().any(|input| {
                    matches!(input, CanonicalRecord::Entity(_)) && input.base().id == *id
                })
            })
        {
            return Err(integrity("definition references an unobserved entity"));
        }
    }
    Ok(())
}

fn input_sources(input: &CanonicalRecord) -> std::collections::BTreeSet<elephant_types::RecordId> {
    input
        .base()
        .provenance
        .sources
        .iter()
        .cloned()
        .chain(matches!(input, CanonicalRecord::Source(_)).then(|| input.base().id.clone()))
        .collect()
}

fn contains_scope(input: &BaseFields, output: &BaseFields) -> bool {
    input.space_id == output.space_id
        && input.security.policy_id == output.security.policy_id
        && input.security.level <= output.security.level
        && input
            .security
            .labels
            .iter()
            .all(|value| output.security.labels.contains(value))
        && input
            .security
            .handling
            .iter()
            .all(|value| output.security.handling.contains(value))
        && input
            .subjects
            .iter()
            .all(|value| output.subjects.contains(value))
        && !matches!(
            (input.subjects_state, output.subjects_state),
            (
                SubjectsState::Unknown,
                SubjectsState::Partial | SubjectsState::Complete
            ) | (SubjectsState::Partial, SubjectsState::Complete)
        )
}

pub(crate) fn validate_commit(request: &CanonicalCommit) -> StorageResult<()> {
    DefinitionLineage::prepare(request).map(|_| ())
}

/// Prepared once before either backend transaction.
///
/// Revision payloads are read
/// from the existing immutable source store and guarded in that same mutation.
/// This creates no new source-head or publication state authority.
pub(crate) struct PreparedDefinitionLineage {
    lineage: Option<DefinitionLineage>,
    controls: crate::source_contact_lineage::OriginalControls,
}

#[derive(Deserialize)]
struct PublishedRevisionRecord {
    revision: SourceRevision,
}

impl PreparedDefinitionLineage {
    pub(crate) async fn prepare(
        client: &StorageClient,
        request: &CanonicalCommit,
    ) -> StorageResult<Self> {
        let mut prepared = Self {
            lineage: DefinitionLineage::prepare(request)?,
            controls: crate::source_contact_lineage::OriginalControls::default(),
        };
        let Some(lineage) = &mut prepared.lineage else {
            return Ok(prepared);
        };
        let fences = &request
            .maintenance
            .as_ref()
            .ok_or_else(|| integrity("definition maintenance missing"))?
            .sources;
        for input in &lineage.inputs {
            let CanonicalRecord::Source(raw) = input else {
                continue;
            };
            let fence = fences
                .iter()
                .find(|fence| fence.source_id() == &raw.base.id)
                .ok_or_else(|| integrity("definition source publication authority missing"))?;
            match fence {
                PublishedSourceFence::Legacy { source } if source.as_ref() == raw.as_ref() => {}
                PublishedSourceFence::Head {
                    published_revision, ..
                } => {
                    let key = crate::source_publication::revision_key(
                        &request.space_id,
                        &raw.base.id,
                        published_revision,
                    );
                    let stored = prepared
                        .controls
                        .read::<PublishedRevisionRecord>(
                            client,
                            &request.space_id,
                            crate::source_publication::REVISION_TABLE,
                            &key,
                        )
                        .await?
                        .ok_or_else(|| integrity("definition published source revision missing"))?;
                    if !revision_identity(
                        &stored.revision,
                        &request.space_id,
                        &raw.base.id,
                        published_revision,
                    ) {
                        return Err(integrity(
                            "definition published source revision identity changed",
                        ));
                    }
                    let published =
                        CanonicalRecord::Source(Box::new(stored.revision.source.clone()));
                    for definition in &lineage.definitions {
                        if !input_contained(&published, definition)? {
                            return Err(integrity("definition omits published source authority"));
                        }
                    }
                    lineage
                        .published_sources
                        .insert(raw.base.id.to_string(), stored.revision);
                }
                _ => return Err(integrity("definition requires published source input")),
            }
        }
        Ok(prepared)
    }

    pub(crate) fn check_tx(&self, tx: &EmbeddedMutation<'_>, space: &str) -> StorageResult<()> {
        self.controls.check_tx(tx, space)
    }

    pub(crate) fn append_guard_sql(
        &self,
        sql: &mut String,
        bindings: &mut std::collections::BTreeMap<String, surrealdb::sql::Value>,
        space: &str,
    ) {
        self.controls
            .append_guard_sql(sql, bindings, space, "definition_source");
    }

    pub(crate) fn seal_receipt(
        &self,
        mut payload: serde_json::Value,
    ) -> StorageResult<serde_json::Value> {
        if let Some(lineage) = &self.lineage {
            payload
                .as_object_mut()
                .ok_or_else(|| integrity("canonical receipt is not an object"))?
                .insert(
                    FIELD.into(),
                    serde_json::to_value(lineage).map_err(|error| integrity(&error.to_string()))?,
                );
        }
        Ok(payload)
    }
}

fn revision_identity(
    revision: &SourceRevision,
    space: &str,
    source: &elephant_types::RecordId,
    id: &str,
) -> bool {
    revision.space_id == space
        && revision.source_id == *source
        && revision.revision_id == id
        && revision.source.base.space_id == space
        && revision.source.base.id == *source
        && revision.source.base.deleted_at.is_none()
        && revision.source.hash == revision.content_hash
}

fn input_contained(
    input: &CanonicalRecord,
    definition: &KnowledgeViewDefinition,
) -> StorageResult<bool> {
    Ok(contains_scope(&input_envelope(input)?, &definition.base)
        && input_sources(input)
            .iter()
            .all(|source| definition.base.provenance.sources.contains(source)))
}

// Pending desired text/metadata may change. Current and original authorization
// envelopes may not silently diverge; the read consumer also authorizes the
// actual current raw Source and the sealed published projection independently.
fn same_source_authorization(
    original: &elephant_types::records::Source,
    current: &elephant_types::records::Source,
) -> bool {
    current.base.deleted_at.is_none()
        && SourceAuthorizationEnvelope::from(original) == SourceAuthorizationEnvelope::from(current)
}

impl CanonicalRepository {
    /// Returns the actual decision's material inputs only for its exact current
    /// output and unchanged original rows.
    ///
    /// Callers must additionally apply their
    /// bounded recursive current-read policy to every input in the same read
    /// context. No mutation, latest-decision scan, or marker-only grant occurs.
    #[allow(
        clippy::too_many_lines,
        reason = "Retain one ordered verification of the original definition decision and every current input without substituting support authority."
    )]
    pub async fn definition_decision_inputs(
        &self,
        definition: &KnowledgeViewDefinition,
    ) -> StorageResult<Option<Vec<CanonicalRecord>>> {
        let Some(key) = pointer(definition) else {
            return Ok(None);
        };
        if definition.base.deleted_at.is_some() {
            return Ok(None);
        }
        let space = &definition.base.space_id;
        let publications = SourcePublicationRepository::new(self.client.clone());
        let Some(payload) = publications
            .read_control::<serde_json::Value>(RECEIPT_TABLE, key, space)
            .await?
        else {
            return Ok(None);
        };
        let receipt: CanonicalCommitReceipt = serde_json::from_value(payload.clone())
            .map_err(|error| integrity(&error.to_string()))?;
        if receipt.space_id != *space
            || stable_key(&[space, &receipt.commit_id]) != key
            || receipt.publication_authority.is_some()
            || !receipt.effect_ids.contains(&definition.base.id)
        {
            return Ok(None);
        }
        let authority_ids = receipt
            .maintenance_authority
            .iter()
            .map(|fence| fence.source_id().clone())
            .collect::<std::collections::BTreeSet<_>>();
        if authority_ids != receipt.source_ids.iter().cloned().collect() {
            return Ok(None);
        }
        for fence in &receipt.maintenance_authority {
            let Some(source) = crate::SourceRepository::new(self.client.clone())
                .get(space, fence.source_id())
                .await?
            else {
                return Ok(None);
            };
            if source.base.deleted_at.is_some() {
                return Ok(None);
            }
            let head = publications.head(space, fence.source_id()).await?;
            let current = match fence {
                PublishedSourceFence::PendingControl { .. } => false,
                PublishedSourceFence::Legacy { source: original } => {
                    head.is_none() && source == **original
                }
                PublishedSourceFence::Head {
                    source_id,
                    published_revision,
                    ..
                } => head.as_ref().is_some_and(|head| {
                    head.space_id == *space
                        && head.source_id == *source_id
                        && head.lifecycle == elephant_types::publication::SourceVisibility::Active
                        && head.published_revision.as_deref() == Some(published_revision.as_str())
                }),
            };
            // A newer desired generation does not unpublish unchanged original
            // inputs. This is current content authority, not payload replay.
            if !current {
                return Ok(None);
            }
        }
        let Some(lineage) = payload.get(FIELD) else {
            return Ok(None);
        };
        let lineage: DefinitionLineage = serde_json::from_value(lineage.clone())
            .map_err(|error| integrity(&error.to_string()))?;
        if lineage.version != 2
            || !lineage.definitions.iter().any(|row| row == definition)
            || !lineage
                .inputs
                .iter()
                .any(|row| matches!(row, CanonicalRecord::Entity(_)))
        {
            return Ok(None);
        }
        let Some(decision) = publications
            .read_control::<SourceDecisionReceipt>(DECISION_TABLE, key, space)
            .await?
        else {
            return Ok(None);
        };
        if decision.space_id != *space
            || decision.commit_id != receipt.commit_id
            || decision.source_ids != receipt.source_ids
            || !decision.effects.iter().any(|effect| {
                matches!(&effect.after,
                Some(CanonicalRecord::KnowledgeViewDefinition(row)) if row.as_ref() == definition)
            })
        {
            return Ok(None);
        }
        let current = crate::KnowledgeViewDefinitionRepository::new(self.client.clone())
            .get_including_deleted(space, &definition.base.id)
            .await?;
        if current.as_ref() != Some(definition) {
            return Ok(None);
        }
        let mut projected_inputs = Vec::with_capacity(lineage.inputs.len());
        let mut used_published = std::collections::BTreeSet::new();
        for input in &lineage.inputs {
            if !material_input(input)
                || input.base().space_id != *space
                || input.base().deleted_at.is_some()
                || !decision.dependency_ids.contains(&input.base().id)
                || !contains_scope(&input_envelope(input)?, &definition.base)
                || input_sources(input)
                    .iter()
                    .any(|source| !definition.base.provenance.sources.contains(source))
            {
                return Ok(None);
            }
            let Some(current) =
                CanonicalRecord::load(&self.client, space, &input.base().id).await?
            else {
                return Ok(None);
            };
            if let CanonicalRecord::Source(original) = input {
                if let Some(sealed) = lineage.published_sources.get(&original.base.id.to_string()) {
                    let Some(PublishedSourceFence::Head {
                        published_revision, ..
                    }) = receipt
                        .maintenance_authority
                        .iter()
                        .find(|fence| fence.source_id() == &original.base.id)
                    else {
                        return Ok(None);
                    };
                    let CanonicalRecord::Source(current_source) = &current else {
                        return Ok(None);
                    };
                    if !revision_identity(sealed, space, &original.base.id, published_revision)
                        || !same_source_authorization(original, current_source)
                        || !input_contained(&current, definition)?
                    {
                        return Ok(None);
                    }
                    let Some(actual) = publications
                        .revision(space, &original.base.id, published_revision)
                        .await?
                    else {
                        return Ok(None);
                    };
                    if serde_json::to_value(&actual)
                        .map_err(|error| integrity(&error.to_string()))?
                        != serde_json::to_value(sealed)
                            .map_err(|error| integrity(&error.to_string()))?
                    {
                        return Ok(None);
                    }
                    let published = CanonicalRecord::Source(Box::new(sealed.source.clone()));
                    if !input_contained(&published, definition)? {
                        return Ok(None);
                    }
                    used_published.insert(original.base.id.to_string());
                    projected_inputs.push(published);
                    continue;
                }
                // No inferred historical snapshot for an unsupported/missing
                // private proof. Only exact retained pre-head sources qualify.
                if !receipt.maintenance_authority.iter().any(|fence| {
                    matches!(fence,
                    PublishedSourceFence::Legacy { source } if source.base.id == original.base.id)
                }) {
                    return Ok(None);
                }
            }
            // Non-Source inputs and retained legacy Source rows remain exact,
            // including bookkeeping; no same-hash historical substitution.
            if serde_json::to_value(&current).map_err(|error| integrity(&error.to_string()))?
                != serde_json::to_value(input).map_err(|error| integrity(&error.to_string()))?
            {
                return Ok(None);
            }
            projected_inputs.push(input.clone());
        }
        if used_published != lineage.published_sources.keys().cloned().collect() {
            return Ok(None);
        }
        drop(publications);
        Ok(Some(projected_inputs))
    }
}

#[cfg(test)]
#[path = "canonical_definition_tests.rs"]
mod tests;
