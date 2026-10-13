//! Trusted affected-image authority, independent of batch-wide causal inputs.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use super::*;
use crate::canonical_commit::{
    CanonicalCommit, CanonicalEffect, CanonicalMaintenanceContext, CanonicalMaintenanceOrigin,
    CanonicalRepository, PublishedSourceFence,
};
use crate::{Repository, StorageClient, StorageConfig};
use chrono::Utc;
use elephant_types::records::base::SubjectsState;
use elephant_types::records::{
    entity::EntityType, AuditAction, Entity, Source, SourceType, SpaceConfig,
};
use elephant_types::{Link, RecordId, TableName};

const SPACE: &str = "audit-authority";

fn entity() -> CanonicalRecord {
    let mut base = BaseFields::new(
        RecordId::new_v4(TableName::Entity),
        SPACE.into(),
        "test".into(),
    );
    base.subjects_state = SubjectsState::Complete;
    CanonicalRecord::Entity(Box::new(Entity::new(
        base,
        EntityType::new("project"),
        "target".into(),
    )))
}

fn plan(before: Option<CanonicalRecord>, after: Option<CanonicalRecord>) -> CanonicalPlan {
    CanonicalPlan {
        observed_epoch: "captured".into(),
        dependencies: Vec::new(),
        effects: vec![CanonicalEffect { before, after }],
        extension_effects: Vec::new(),
        outbox: Vec::new(),
    }
}

fn snapshot(row: &CanonicalRecord) -> serde_json::Value {
    serde_json::to_value(row).unwrap()["record"].clone()
}

fn audit(plan: &CanonicalPlan) -> AuditLog {
    let effect = &plan.effects[0];
    AuditLog {
        base: BaseFields::new(
            RecordId::new_v4(TableName::AuditLog),
            SPACE.into(),
            "test".into(),
        ),
        action: AuditAction::Update,
        actor: "test".into(),
        record: effect
            .after
            .as_ref()
            .or(effect.before.as_ref())
            .unwrap()
            .base()
            .id
            .clone(),
        before: effect.before.as_ref().map(snapshot),
        after: effect.after.as_ref().map(snapshot),
        timestamp: Utc::now(),
        reason: None,
    }
}

fn append_audit(plan: &mut CanonicalPlan, audit: AuditLog) {
    plan.effects.push(CanonicalEffect {
        before: None,
        after: Some(CanonicalRecord::AuditLog(Box::new(audit))),
    });
}

#[test]
fn audit_union_retains_before_only_sources_and_both_authorization_envelopes() {
    let first = RecordId::new_v4(TableName::Source);
    let second = RecordId::new_v4(TableName::Source);
    let mut before = entity();
    before.base_mut().provenance.sources = vec![first.clone(), second.clone()];
    before.base_mut().security.labels = vec!["original".into()];
    before.base_mut().subjects = vec![Link::new(RecordId::new_v4(TableName::Entity))];
    before.base_mut().subjects_state = SubjectsState::Unknown;
    let mut after = before.clone();
    after.base_mut().provenance.sources = vec![second.clone()];
    after.base_mut().security.level = 3;
    after.base_mut().security.labels = vec!["current".into()];
    after
        .base_mut()
        .subjects
        .push(Link::new(RecordId::new_v4(TableName::Entity)));
    after.base_mut().subjects_state = SubjectsState::Complete;
    let mut plan = plan(Some(before.clone()), Some(after.clone()));
    let mut audit = audit(&plan);
    let payload = (audit.before.clone(), audit.after.clone());
    inherit_maintenance_audit_authority(&plan, &mut audit).unwrap();
    assert_eq!(
        source_ids(&audit.base),
        sorted([first.to_string(), second.to_string()].into_iter())
    );
    assert_eq!(audit.base.security.level, 3);
    assert_eq!(
        sorted(audit.base.security.labels.iter().cloned()),
        vec!["current", "original"]
    );
    assert_eq!(audit.base.subjects.len(), 2);
    assert_eq!(audit.base.subjects_state, SubjectsState::Unknown);
    assert_eq!((audit.before.clone(), audit.after.clone()), payload);
    append_audit(&mut plan, audit);
    validate(&plan).unwrap();
}

#[test]
fn audit_rejects_missing_or_forged_snapshot_and_staged_authority() {
    let source = RecordId::new_v4(TableName::Source);
    let mut target = entity();
    target.base_mut().provenance.sources.push(source);
    let mut plan = plan(Some(target.clone()), Some(target));
    let mut original = audit(&plan);
    inherit_maintenance_audit_authority(&plan, &mut original).unwrap();
    for field in [
        "provenance",
        "subjects",
        "subjects_state",
        "security",
        "space_id",
        "id",
    ] {
        let mut malformed = original.clone();
        malformed
            .before
            .as_mut()
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove(field);
        assert!(
            inherit_maintenance_audit_authority(&plan, &mut malformed).is_err(),
            "{field}"
        );
    }
    let mut forged = original.clone();
    forged.before.as_mut().unwrap()["provenance"]["sources"] =
        serde_json::json!([RecordId::new_v4(TableName::Source)]);
    assert!(inherit_maintenance_audit_authority(&plan, &mut forged).is_err());
    let mut forged = original.clone();
    forged.after.as_mut().unwrap()["security"]["policy_id"] = serde_json::json!("foreign");
    assert!(inherit_maintenance_audit_authority(&plan, &mut forged).is_err());
    original.base.provenance.sources.clear();
    append_audit(&mut plan, original);
    assert!(
        validate(&plan).is_err(),
        "commit must not trust an omitted audit source"
    );
}

#[test]
fn audit_create_delete_observed_intrinsic_and_absent_targets_are_distinct() {
    let mut target = entity();
    target
        .base_mut()
        .provenance
        .sources
        .push(RecordId::new_v4(TableName::Source));
    for (before, after) in [(None, Some(target.clone())), (Some(target.clone()), None)] {
        let plan = plan(before, after);
        let mut audit = audit(&plan);
        inherit_maintenance_audit_authority(&plan, &mut audit).unwrap();
        assert_eq!(
            audit.base.provenance.sources,
            target.base().provenance.sources
        );
    }
    let intrinsic = entity();
    let mut observed = plan(Some(intrinsic.clone()), Some(intrinsic.clone()));
    let mut compact = audit(&observed);
    compact.before = None;
    compact.after = None;
    observed.effects.clear();
    observed.dependencies.push(intrinsic);
    inherit_maintenance_audit_authority(&observed, &mut compact).unwrap();
    assert!(compact.base.provenance.sources.is_empty());
    observed.dependencies.clear();
    assert!(inherit_maintenance_audit_authority(&observed, &mut compact).is_err());
}

#[test]
fn audit_rejects_cross_space_or_policy_target_and_security_weakening() {
    let before = entity();
    let mut after = before.clone();
    after.base_mut().security.policy_id = "another-domain".into();
    let cross_policy = plan(Some(before.clone()), Some(after));
    assert!(inherit_maintenance_audit_authority(&cross_policy, &mut audit(&cross_policy)).is_err());
    let mut after = before.clone();
    after.base_mut().space_id = "another-space".into();
    let cross_space = plan(Some(before.clone()), Some(after));
    assert!(inherit_maintenance_audit_authority(&cross_space, &mut audit(&cross_space)).is_err());
    let mut restricted = before;
    restricted.base_mut().security.level = 5;
    let mut plan = plan(None, Some(restricted));
    let mut audit = audit(&plan);
    inherit_maintenance_audit_authority(&plan, &mut audit).unwrap();
    audit.base.security.level = 0;
    append_audit(&mut plan, audit);
    assert!(validate(&plan).is_err());
}

#[test]
fn intermediate_snapshot_is_bounded_by_trusted_target_union_without_rewriting_payload() {
    let mut before = entity();
    before
        .base_mut()
        .provenance
        .sources
        .push(RecordId::new_v4(TableName::Source));
    let mut after = before.clone();
    after
        .base_mut()
        .provenance
        .sources
        .push(RecordId::new_v4(TableName::Source));
    let plan = plan(Some(before.clone()), Some(after));
    let mut audit = audit(&plan);
    audit.after = Some(snapshot(&before));
    audit.after.as_mut().unwrap()["confidence"] = serde_json::json!(0.980_000_019_073_486_4);
    let original = audit.after.clone();
    inherit_maintenance_audit_authority(&plan, &mut audit).unwrap();
    assert_eq!(audit.base.provenance.sources.len(), 2);
    assert_eq!(
        audit.after, original,
        "the guard validates authority, not floating payload encoding"
    );
}

async fn client(embedded: bool) -> StorageClient {
    let client = if embedded {
        StorageClient::new_embedded().await
    } else {
        StorageClient::new(StorageConfig::default()).await
    }
    .unwrap();
    if !embedded {
        client
            .query_checked(include_str!(
                "../../migrations/sql/031_source_publication.surql"
            ))
            .await
            .unwrap();
    }
    let space: SpaceConfig = serde_json::from_value(serde_json::json!({
        "id":RecordId::new_v4(TableName::SpaceConfig),"space_id":SPACE,
        "created_at":Utc::now(),"updated_at":Utc::now(),"created_by":"test",
        "security":{"policy_id":"default","level":0,"labels":[],"handling":[]},
        "provenance":{},"subjects":[],"subjects_state":"complete",
        "embedding_provider":"fake","embedding_model":"test","embedding_dims":8
    }))
    .unwrap();
    crate::SpaceConfigRepository::new(client.clone())
        .create(space)
        .await
        .unwrap();
    client
}

#[expect(
    clippy::too_many_lines,
    reason = "One complete atomic refusal, success and causal inverse scenario."
)]
async fn atomic_audit_authority_and_causal_inverse(embedded: bool) {
    let client = client(embedded).await;
    let source_repository = crate::SourceRepository::new(client.clone());
    let mut sources = Vec::new();
    for name in ["before-source", "after-source", "decision-only-source"] {
        sources.push(
            source_repository
                .create(Source::new(
                    BaseFields::new(
                        RecordId::new_v4(TableName::Source),
                        SPACE.into(),
                        "test".into(),
                    ),
                    SourceType::Manual,
                    name.into(),
                ))
                .await
                .unwrap(),
        );
    }
    let mut baseline = Entity::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Entity),
            SPACE.into(),
            "test".into(),
        ),
        EntityType::new("project"),
        "target".into(),
    );
    baseline.base.provenance.sources = sources[..2]
        .iter()
        .map(|source| source.base.id.clone())
        .collect();
    let baseline = crate::EntityRepository::new(client.clone())
        .create(baseline)
        .await
        .unwrap();
    let mut after = baseline.clone();
    after.base.provenance.sources = vec![sources[1].base.id.clone()];
    after.base.deleted_at = Some(Utc::now());
    let repository = CanonicalRepository::new(client.clone());
    let mut plan = plan(
        Some(CanonicalRecord::Entity(Box::new(baseline.clone()))),
        Some(CanonicalRecord::Entity(Box::new(after))),
    );
    plan.observed_epoch = repository.epoch(SPACE).await.unwrap();
    plan.dependencies = sources
        .iter()
        .cloned()
        .map(|source| CanonicalRecord::Source(Box::new(source)))
        .collect();
    let original_epoch = plan.observed_epoch.clone();
    let mut audit = audit(&plan);
    inherit_maintenance_audit_authority(&plan, &mut audit).unwrap();
    let audit_id = audit.base.id.clone();
    assert_eq!(
        audit.base.provenance.sources.len(),
        2,
        "decision-only source is not row support"
    );
    append_audit(&mut plan, audit);
    let request = CanonicalCommit {
        space_id: SPACE.into(),
        commit_id: "audit-authority-decision".into(),
        input_fingerprint: "same-original-plan".into(),
        replay_output: None,
        publication: None,
        finalization: None,
        maintenance: Some(CanonicalMaintenanceContext {
            origin: CanonicalMaintenanceOrigin::Derived,
            attempt: None,
            sources: sources
                .iter()
                .cloned()
                .map(|source| PublishedSourceFence::Legacy {
                    source: Box::new(source),
                })
                .collect(),
        }),
        plan,
    };
    let mut invalid = request.clone();
    invalid.plan.effects[1]
        .after
        .as_mut()
        .unwrap()
        .base_mut()
        .provenance
        .sources
        .clear();
    assert!(repository.commit(invalid).await.is_err());
    assert_eq!(repository.epoch(SPACE).await.unwrap(), original_epoch);
    assert!(crate::AuditLogRepository::new(client.clone())
        .get(SPACE, &audit_id)
        .await
        .unwrap()
        .is_none());
    assert!(crate::EntityRepository::new(client.clone())
        .get(SPACE, &baseline.base.id)
        .await
        .unwrap()
        .is_some());
    let receipt = repository.commit(request.clone()).await.unwrap();
    assert_eq!(
        receipt.source_ids.len(),
        3,
        "full conjunctive decision inputs stay intact"
    );
    for source in [&sources[0], &sources[2]] {
        let inverse = crate::source_decision::SourceDecisionRepository::new(client.clone())
            .invalidation(SPACE, &source.base.id)
            .await
            .unwrap();
        assert!(inverse.effects.iter().any(|effect| effect
            .before
            .as_ref()
            .is_some_and(|row| row.base().id == audit_id)
            && effect.after.is_none()));
        assert!(inverse
            .effects
            .iter()
            .any(
                |effect| effect.after.as_ref().is_some_and(
                    |row| row.base().id == baseline.base.id && row.base().deleted_at.is_none()
                )
            ));
    }
    source_repository
        .delete(SPACE, &sources[2].base.id)
        .await
        .unwrap();
    drop(source_repository);
    assert!(repository
        .receipt(SPACE, "audit-authority-decision")
        .await
        .is_err());
    assert!(
        repository.commit(request).await.is_err(),
        "stale source cannot authorize replay"
    );
    drop(repository);
}

#[tokio::test]
async fn embedded_audit_authority_is_atomic_and_keeps_causal_inverse() {
    Box::pin(atomic_audit_authority_and_causal_inverse(true)).await;
}

#[tokio::test]
async fn surreal_audit_authority_is_atomic_and_keeps_causal_inverse() {
    Box::pin(atomic_audit_authority_and_causal_inverse(false)).await;
}
