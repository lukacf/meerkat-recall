//! Explicit owner action closure. Original claims and timeline rows stay intact.
//! A canonical receipt and inherited audit distinguish closure from truth status.
use crate::canonical_commit::{
    source_support_fingerprint, CanonicalCommit, CanonicalEffect, CanonicalMaintenanceContext,
    CanonicalMaintenanceOrigin, CanonicalPlan, CanonicalRecord, CanonicalRepository,
};
use crate::read_eligibility::{ReadAuthorization, ReadEligibility};
use crate::source_publication::{conflict, integrity, stable_key};
use crate::{AssertionRepository, AuditLogRepository, Repository, StorageClient, StorageResult};
use chrono::{DateTime, Utc};
use elephant_types::records::assertion::EvidenceSpan;
use elephant_types::records::{AuditAction, AuditLog};
use elephant_types::{RecordId, TableName};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Explicit owner decision for one exact action; actor authority comes from the caller.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CloseActionInput {
    /// Exact `requires_action` assertion to close or whose closure is recorded.
    pub action_id: RecordId,
    /// Resolved entity that must match the exact target action.
    pub subject_entity: RecordId,
    /// Verified original evidence span supporting the explicit owner decision.
    pub owner_statement: EvidenceSpan,
    /// Nonempty owner closure explanation, bounded to 4096 UTF-8 bytes.
    pub reason: String,
    /// Explicit effective timestamp of the owner decision.
    pub effective_at: DateTime<Utc>,
}
/// Durable owner closure receipt, independent of the original claim's truth or status.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActionDisposition {
    /// Version of the serialized historical proof.
    pub version: u32,
    /// Recorded `closed_by_owner` disposition, separate from truth and resolution status.
    pub state: String,
    /// Exact `requires_action` assertion to close or whose closure is recorded.
    pub action_id: RecordId,
    /// Resolved entity that must match the exact target action.
    pub subject_entity: RecordId,
    /// Nonempty owner closure explanation, bounded to 4096 UTF-8 bytes.
    pub reason: String,
    /// Explicit effective timestamp of the owner decision.
    pub effective_at: DateTime<Utc>,
    /// Actual timestamp of the canonical closure transaction.
    pub recorded_at: DateTime<Utc>,
    /// Comes from the authenticated principal, never source metadata.
    pub actor: String,
    /// Verified original evidence span supporting the explicit owner decision.
    pub owner_statement: EvidenceSpan,
    /// Canonical audit record identity created with this closure.
    pub audit_id: RecordId,
    /// Fingerprint of the exact audit row retained by the closure receipt.
    pub audit_fingerprint: String,
    /// Fingerprint of the authenticated actor and complete explicit request.
    pub request_fingerprint: String,
    /// Exact authorized target and evidence dependencies checked atomically.
    pub dependency_fingerprints: BTreeMap<RecordId, String>,
}
/// Canonical, currently authorized owner closure and receipt reads.
#[derive(Debug, Clone)]
pub struct ActionDispositionRepository {
    client: StorageClient,
}
impl ActionDispositionRepository {
    /// Bind owner-action operations to an existing storage client.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }
    /// Begin before the adapter authorizes any input. Caller must separately
    /// require explicit action-management authority; metadata is not a grant.
    pub async fn begin(&self, space: &str) -> StorageResult<String> {
        CanonicalRepository::new(self.client.clone())
            .observe_epoch(space)
            .await?
            .ok_or_else(|| conflict("action space epoch absent"))
    }
    /// Close one exactly authorized action and atomically retain its audit and receipt.
    /// Refuses changed epochs, dependencies, target identity or conflicting prior closure.
    pub async fn close(
        &self,
        space: &str,
        epoch: String,
        actor: &str,
        input: CloseActionInput,
        authorize: &ReadAuthorization<'_>,
    ) -> StorageResult<ActionDisposition> {
        validate_close_input(actor, &input)?;
        let canonical = CanonicalRepository::new(self.client.clone());
        if canonical.observe_epoch(space).await?.as_deref() != Some(epoch.as_str()) {
            return Err(conflict("action authorization changed"));
        }
        let reader = ReadEligibility::new(self.client.clone());
        let target = AssertionRepository::new(self.client.clone())
            .get(space, &input.action_id)
            .await?
            .ok_or_else(|| conflict("action unavailable"))?;
        if !action_target_matches(&target, &input.subject_entity)
            || reader.project(target.clone(), authorize).await?.is_none()
        {
            return Err(conflict("action target differs or is not authorized"));
        }
        let subject = crate::EntityRepository::new(self.client.clone())
            .get(space, &input.subject_entity)
            .await?
            .ok_or_else(|| conflict("action subject unavailable"))?;
        if reader.project(subject.clone(), authorize).await?.is_none() {
            return Err(conflict("action subject denied"));
        }
        let evidence = reader
            .evidence(space, &input.owner_statement, authorize)
            .await?
            .ok_or_else(|| conflict("owner statement unavailable or denied"))?;
        let fingerprint = request_fingerprint(space, actor, &input)?;
        if let Some(existing) = self.current(space, &input.action_id, authorize).await? {
            if existing.request_fingerprint != fingerprint {
                return Err(conflict("action is already closed by another disposition"));
            }
            if canonical.observe_epoch(space).await?.as_deref() != Some(epoch.as_str()) {
                return Err(conflict("action replay authorization changed"));
            }
            return Ok(existing);
        }
        let mut dependencies = vec![
            CanonicalRecord::Assertion(Box::new(target.clone())),
            CanonicalRecord::Entity(Box::new(subject)),
            CanonicalRecord::Doc(Box::new(evidence.doc.clone())),
            CanonicalRecord::DocRevision(Box::new(evidence.revision.clone())),
        ];
        if let Some(node) = evidence.node {
            dependencies.push(
                CanonicalRecord::load(&self.client, space, &node.base.id)
                    .await?
                    .ok_or_else(|| conflict("owner evidence node changed"))?,
            );
        }
        let (sources, observed_sources) = reader.action_source_fences(space).await?;
        dependencies.extend(observed_sources);
        let (dependencies, dependency_fingerprints) = unique_dependencies(dependencies)?;
        let (commit_id, result, audit) = make_disposition(
            space,
            actor,
            input,
            &target,
            &dependencies,
            dependency_fingerprints,
            &fingerprint,
        )?;
        reader.verify_current().await?;
        drop(reader);
        let receipt = canonical
            .commit(CanonicalCommit {
                space_id: space.into(),
                commit_id,
                input_fingerprint: fingerprint,
                publication: None,
                maintenance: Some(CanonicalMaintenanceContext {
                    origin: CanonicalMaintenanceOrigin::IndependentMutation,
                    attempt: None,
                    sources,
                }),
                finalization: None,
                replay_output: Some(
                    serde_json::to_value(&result)
                        .map_err(|_| integrity("action disposition encoding"))?,
                ),
                plan: CanonicalPlan {
                    observed_epoch: epoch,
                    dependencies,
                    effects: vec![CanonicalEffect {
                        before: None,
                        after: Some(CanonicalRecord::AuditLog(Box::new(audit))),
                    }],
                    extension_effects: vec![],
                    outbox: vec![],
                },
            })
            .await?;
        drop(canonical);
        serde_json::from_value(
            receipt
                .replay_output
                .ok_or_else(|| integrity("action disposition result absent"))?,
        )
        .map_err(|_| integrity("action disposition result invalid"))
    }
    /// A retained receipt is not authority by shape alone: require its actual
    /// canonical commit, audit and every exact live authorized dependency.
    pub async fn current(
        &self,
        space: &str,
        action: &RecordId,
        authorize: &ReadAuthorization<'_>,
    ) -> StorageResult<Option<ActionDisposition>> {
        let canonical = CanonicalRepository::new(self.client.clone());
        let epoch = canonical.observe_epoch(space).await?;
        let Some(receipt) = canonical
            .receipt_read_only(space, &commit_id(space, action))
            .await?
        else {
            return Ok(None);
        };
        let result: ActionDisposition = serde_json::from_value(
            receipt
                .replay_output
                .clone()
                .ok_or_else(|| integrity("action disposition result absent"))?,
        )
        .map_err(|_| integrity("action disposition result invalid"))?;
        validate_current_receipt(action, &receipt, &result)?;
        let audit = AuditLogRepository::new(self.client.clone())
            .get(space, &result.audit_id)
            .await?
            .ok_or_else(|| integrity("action audit absent"))?;
        if source_support_fingerprint(&audit)? != result.audit_fingerprint
            || audit.base.deleted_at.is_some()
            || audit.timestamp != result.recorded_at
            || audit.record != *action
            || audit.actor != result.actor
            || audit.reason.as_deref()
                != Some(format!("owner-action-disposition:{}", receipt.commit_id).as_str())
            || !authorize(&audit.base)
        {
            return Ok(None);
        }
        if !result.dependency_fingerprints.contains_key(action)
            || !result
                .dependency_fingerprints
                .contains_key(&result.subject_entity)
            || !result
                .dependency_fingerprints
                .contains_key(result.owner_statement.revision.id())
            || result.dependency_fingerprints.len() > 8192
        {
            return Err(integrity("action dependency closure invalid"));
        }
        let mut dependencies = Vec::new();
        for (id, fingerprint) in &result.dependency_fingerprints {
            let Some(row) = CanonicalRecord::load(&self.client, space, id).await? else {
                return Ok(None);
            };
            if row.base().deleted_at.is_some()
                || !authorize(row.base())
                || row.fingerprint()? != *fingerprint
            {
                return Ok(None);
            }
            dependencies.push(row);
        }
        let input = CloseActionInput {
            action_id: result.action_id.clone(),
            subject_entity: result.subject_entity.clone(),
            owner_statement: result.owner_statement.clone(),
            reason: result.reason.clone(),
            effective_at: result.effective_at,
        };
        if request_fingerprint(space, &result.actor, &input)? != result.request_fingerprint {
            return Err(integrity("action disposition input differs"));
        }
        let reader = ReadEligibility::new(self.client.clone());
        let target = AssertionRepository::new(self.client.clone())
            .get(space, action)
            .await?
            .ok_or_else(|| conflict("action no longer available"))?;
        if !action_target_matches(&target, &result.subject_entity)
            || reader.project(target, authorize).await?.is_none()
            || reader
                .evidence(space, &result.owner_statement, authorize)
                .await?
                .is_none()
        {
            return Ok(None);
        }
        verify_original_plan(
            space,
            &receipt,
            &result,
            epoch.as_ref(),
            dependencies,
            audit,
        )?;
        reader.verify_current().await?;
        drop(reader);
        if canonical.observe_epoch(space).await? != epoch {
            return Err(conflict("action disposition dependencies changed"));
        }
        drop(canonical);
        Ok(Some(result))
    }
}
fn action_target_matches(target: &elephant_types::records::Assertion, subject: &RecordId) -> bool {
    target
        .predicate_id
        .as_deref()
        .unwrap_or(&target.predicate_raw)
        == "requires_action"
        && target.subject_entity.as_ref().map(elephant_types::Link::id) == Some(subject)
}
fn validate_close_input(actor: &str, input: &CloseActionInput) -> StorageResult<()> {
    if actor.is_empty()
        || actor.len() > 1024
        || input.action_id.table() != TableName::Assertion
        || input.subject_entity.table() != TableName::Entity
        || input.reason.trim().is_empty()
        || input.reason.len() > 4096
        || input.owner_statement.start >= input.owner_statement.end
    {
        return Err(integrity(
            "action disposition identity/reason/evidence invalid",
        ));
    }
    Ok(())
}
fn unique_dependencies(
    dependencies: Vec<CanonicalRecord>,
) -> StorageResult<(Vec<CanonicalRecord>, BTreeMap<RecordId, String>)> {
    let mut unique = BTreeMap::new();
    for dependency in dependencies {
        unique.insert(dependency.base().id.clone(), dependency);
    }
    let dependencies: Vec<_> = unique.into_values().collect();
    let dependency_fingerprints = dependencies
        .iter()
        .map(|row| Ok((row.base().id.clone(), row.fingerprint()?)))
        .collect::<StorageResult<_>>()?;
    Ok((dependencies, dependency_fingerprints))
}
fn validate_current_receipt(
    action: &RecordId,
    receipt: &crate::canonical_commit::CanonicalCommitReceipt,
    result: &ActionDisposition,
) -> StorageResult<()> {
    if result.version != 1
        || result.state != "closed_by_owner"
        || &result.action_id != action
        || receipt.historical_import.is_some()
        || receipt.publication_authority.is_some()
        || receipt.input_fingerprint != result.request_fingerprint
        || receipt.effect_ids != [result.audit_id.clone()]
        || !receipt.extension_ids.is_empty()
        || !receipt.outbox_ids.is_empty()
    {
        return Err(integrity("action disposition commit differs"));
    }
    Ok(())
}
fn make_disposition(
    space: &str,
    actor: &str,
    input: CloseActionInput,
    target: &elephant_types::records::Assertion,
    dependencies: &[CanonicalRecord],
    dependency_fingerprints: BTreeMap<RecordId, String>,
    fingerprint: &str,
) -> StorageResult<(String, ActionDisposition, AuditLog)> {
    let commit_id = commit_id(space, &input.action_id);
    let audit_id = RecordId::new(
        TableName::AuditLog,
        uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_OID,
            format!("action-disposition-audit:{commit_id}").as_bytes(),
        ),
    );
    let now = Utc::now();
    let mut result = ActionDisposition {
        version: 1,
        state: "closed_by_owner".into(),
        action_id: input.action_id.clone(),
        subject_entity: input.subject_entity.clone(),
        reason: input.reason,
        effective_at: input.effective_at,
        recorded_at: now,
        actor: actor.into(),
        owner_statement: input.owner_statement,
        audit_id: audit_id.clone(),
        audit_fingerprint: String::new(),
        request_fingerprint: fingerprint.to_owned(),
        dependency_fingerprints,
    };
    let mut base = elephant_types::records::BaseFields::new(audit_id, space.into(), actor.into());
    base.created_at = now;
    base.updated_at = now;
    base.security
        .policy_id
        .clone_from(&target.base.security.policy_id);
    // This new audit is authored now. Original extractor/work-run metadata
    // stays on the original claim; every causal input restriction/source is
    // inherited so owner-evidence erasure also reaches the new receipt/audit.
    for dependency in dependencies {
        crate::public_mutation::inherit_public_envelope(&mut base, dependency.base())?;
    }
    let audit = AuditLog {
        base,
        action: AuditAction::Update,
        actor: actor.into(),
        record: target.base.id.clone(),
        before: None,
        after: None,
        timestamp: now,
        reason: Some(format!("owner-action-disposition:{commit_id}")),
    };
    result.audit_fingerprint = source_support_fingerprint(&audit)?;
    Ok((commit_id, result, audit))
}

fn verify_original_plan(
    space: &str,
    receipt: &crate::canonical_commit::CanonicalCommitReceipt,
    result: &ActionDisposition,
    epoch: Option<&String>,
    dependencies: Vec<CanonicalRecord>,
    audit: AuditLog,
) -> StorageResult<()> {
    let request = CanonicalCommit {
        space_id: space.into(),
        commit_id: receipt.commit_id.clone(),
        input_fingerprint: result.request_fingerprint.clone(),
        publication: None,
        maintenance: Some(CanonicalMaintenanceContext {
            origin: CanonicalMaintenanceOrigin::IndependentMutation,
            attempt: None,
            sources: receipt.maintenance_authority.clone(),
        }),
        finalization: None,
        replay_output: Some(
            serde_json::to_value(result).map_err(|_| integrity("action disposition encoding"))?,
        ),
        plan: CanonicalPlan {
            observed_epoch: epoch
                .cloned()
                .ok_or_else(|| conflict("action epoch absent"))?,
            dependencies,
            effects: vec![CanonicalEffect {
                before: None,
                after: Some(CanonicalRecord::AuditLog(Box::new(audit))),
            }],
            extension_effects: vec![],
            outbox: vec![],
        },
    };
    if crate::canonical_commit::canonical_plan_fingerprint(&request)? != receipt.plan_fingerprint
        || crate::canonical_commit::causal_source_ids(&request) != receipt.source_ids
        || receipt.source_id.is_some()
        || receipt.source_revision.is_some()
    {
        return Err(integrity("action disposition original plan differs"));
    }
    Ok(())
}

fn request_fingerprint(
    space: &str,
    actor: &str,
    input: &CloseActionInput,
) -> StorageResult<String> {
    source_support_fingerprint(
        &serde_json::json!({"version":1,"space":space,"actor":actor,"input":input}),
    )
}
fn commit_id(space: &str, action: &RecordId) -> String {
    stable_key(&["owner-action-disposition-v1", space, &action.to_string()])
}

#[cfg(test)]
#[path = "action_disposition_tests.rs"]
mod tests;

/// Current scope needed to expose a disposition's dependency/reason.
#[must_use]
pub const fn dependency_scope(table: TableName) -> &'static str {
    match table {
        TableName::Doc | TableName::DocRevision | TableName::DocNode => "read:doc:raw",
        TableName::Source => "read:source",
        TableName::Entity => "read:entity",
        _ => "read:assertion",
    }
}
