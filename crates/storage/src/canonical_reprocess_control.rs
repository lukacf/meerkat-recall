//! Restricted job bookkeeping under current input authority, including a source
//! whose desired revision is not published. No factual support is granted here.

use std::collections::BTreeSet;

use elephant_types::publication::SourceVisibility;
use elephant_types::records::{AuditAction, ReprocessJob};
use elephant_types::RecordId;

use crate::canonical_commit::{
    canonical_json, CanonicalCommit, CanonicalRecord, CanonicalRepository, PublishedSourceFence,
};
use crate::source_decision::{SourceDecisionReceipt, DECISION_TABLE};
use crate::source_publication::{conflict, integrity, SourcePublicationRepository};
use crate::{Repository, SourceRepository, StorageResult};

impl CanonicalRepository {
    /// Captures original source authority for an existing reprocess job's
    /// status/progress change.
    ///
    /// Call after the original planning epoch is read.
    /// This method never certifies graph output or a published source revision.
    pub async fn capture_reprocess_control_support(
        &self,
        space: &str,
        job: &ReprocessJob,
    ) -> StorageResult<Vec<PublishedSourceFence>> {
        if job.base.space_id != space || job.base.deleted_at.is_some() {
            return Err(integrity("invalid reprocess control job"));
        }
        let observed = CanonicalRecord::ReprocessJob(Box::new(job.clone()));
        let current = CanonicalRecord::load(&self.client, space, &job.base.id).await?;
        if current
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(|error| integrity(&error.to_string()))?
            .map(canonical_json)
            != Some(canonical_json(
                serde_json::to_value(&observed).map_err(|error| integrity(&error.to_string()))?,
            ))
        {
            return Err(conflict("reprocess_control_job_changed"));
        }
        let publication = SourcePublicationRepository::new(self.client.clone());
        let mut sources: BTreeSet<_> = job.base.provenance.sources.iter().cloned().collect();
        sources.extend(
            publication
                .support_for_record(space, &job.base.id)
                .await?
                .into_iter()
                .map(|support| support.source_id),
        );
        let decisions: Vec<SourceDecisionReceipt> = publication
            .list_controls(DECISION_TABLE, Some(space))
            .await?;
        for decision in decisions {
            if decision.effects.iter().any(|effect| {
                effect
                    .before
                    .iter()
                    .chain(effect.after.iter())
                    .any(|row| row.base().id == job.base.id)
            }) {
                sources.extend(decision.source_ids);
            }
        }
        let mut fences = Vec::new();
        for source_id in sources {
            let source = SourceRepository::new(self.client.clone())
                .get(space, &source_id)
                .await?
                .ok_or_else(|| conflict("maintenance_source_missing"))?;
            match publication.head(space, &source_id).await? {
                Some(head) => {
                    if head.lifecycle != SourceVisibility::Active {
                        return Err(conflict("maintenance_source_not_active"));
                    }
                    fences.push(PublishedSourceFence::PendingControl {
                        head: Box::new(head),
                        source: Box::new(source),
                    });
                }
                None => fences.push(PublishedSourceFence::Legacy {
                    source: Box::new(source),
                }),
            }
        }
        Ok(fences)
    }
}

pub fn validate_control_commit(request: &CanonicalCommit) -> StorageResult<()> {
    if !request.plan.extension_effects.is_empty() {
        return Err(integrity("reprocess control cannot mutate extensions"));
    }
    let jobs: Vec<_> = request
        .plan
        .effects
        .iter()
        .filter_map(|effect| match (&effect.before, &effect.after) {
            (
                Some(CanonicalRecord::ReprocessJob(before)),
                Some(CanonicalRecord::ReprocessJob(after)),
            ) => Some((before.as_ref(), after.as_ref())),
            _ => None,
        })
        .collect();
    let [(before, after)] = jobs.as_slice() else {
        return Err(integrity("reprocess control requires one existing job"));
    };
    if before.base.deleted_at.is_some() || after.base.deleted_at.is_some() {
        return Err(integrity("reprocess control cannot change lifecycle"));
    }
    let mut allowed: ReprocessJob = (**before).clone();
    allowed.status = after.status;
    allowed.progress = after.progress.clone();
    allowed.started_at = after.started_at;
    allowed.completed_at = after.completed_at;
    allowed.error.clone_from(&after.error);
    allowed.base.updated_at = after.base.updated_at;
    if allowed != **after {
        return Err(integrity(
            "reprocess control changed immutable job input or authority",
        ));
    }
    for dependency in &request.plan.dependencies {
        if !matches!(
            dependency,
            CanonicalRecord::Source(_)
                | CanonicalRecord::WorkItem(_)
                | CanonicalRecord::WorkRun(_)
                | CanonicalRecord::WorkArtifact(_)
        ) && !matches!(dependency, CanonicalRecord::ReprocessJob(job) if job.as_ref() == *before)
        {
            return Err(integrity("reprocess control has non-control dependencies"));
        }
    }
    let context = request
        .maintenance
        .as_ref()
        .ok_or_else(|| integrity("control context missing"))?;
    for fence in &context.sources {
        match fence {
            PublishedSourceFence::PendingControl { head, source }
                if head.space_id == request.space_id
                    && source.base.space_id == request.space_id
                    && head.source_id == source.base.id
                    && source.base.deleted_at.is_none()
                    && head.lifecycle == SourceVisibility::Active => {}
            PublishedSourceFence::Legacy { source }
                if source.base.space_id == request.space_id && source.base.deleted_at.is_none() => {
            }
            _ => return Err(integrity("invalid reprocess input control fence")),
        }
    }
    let mut affected: BTreeSet<RecordId> = BTreeSet::from([before.base.id.clone()]);
    for effect in &request.plan.effects {
        match (&effect.before, &effect.after) {
            (Some(CanonicalRecord::ReprocessJob(_)), Some(CanonicalRecord::ReprocessJob(_))) => {}
            (None, Some(CanonicalRecord::AuditLog(audit))) => {
                if audit.record != before.base.id
                    || audit.action != AuditAction::Update
                    || audit.base.deleted_at.is_some()
                    || audit.base.security != after.base.security
                    || audit.base.subjects != after.base.subjects
                    || audit.base.subjects_state != after.base.subjects_state
                    || audit.base.provenance != after.base.provenance
                    || audit.before.as_ref().map(|row| canonical_json(row.clone()))
                        != Some(canonical_json(
                            serde_json::to_value(before)
                                .map_err(|error| integrity(&error.to_string()))?,
                        ))
                    || audit.after.as_ref().map(|row| canonical_json(row.clone()))
                        != Some(canonical_json(
                            serde_json::to_value(after)
                                .map_err(|error| integrity(&error.to_string()))?,
                        ))
                {
                    return Err(integrity(
                        "reprocess control audit does not certify its job",
                    ));
                }
                affected.insert(audit.base.id.clone());
            }
            _ => return Err(integrity("reprocess control cannot mutate canonical graph")),
        }
    }
    validate_control_outbox(request, &affected)?;
    Ok(())
}

fn validate_control_outbox(
    request: &CanonicalCommit,
    affected: &BTreeSet<RecordId>,
) -> StorageResult<()> {
    for event in &request.plan.outbox {
        let records = event
            .payload
            .get("records")
            .and_then(serde_json::Value::as_array)
            .filter(|rows| !rows.is_empty())
            .ok_or_else(|| integrity("reprocess control event lacks affected records"))?;
        for record in records {
            let id: RecordId = serde_json::from_value(
                record
                    .get("id")
                    .cloned()
                    .ok_or_else(|| integrity("reprocess control event lacks record ID"))?,
            )
            .map_err(|error| integrity(&error.to_string()))?;
            if !affected.contains(&id) {
                return Err(integrity("reprocess control event targets another record"));
            }
        }
    }
    Ok(())
}
