//! Audit support follows the affected record, while decisions retain all inputs.

use super::*;
use std::collections::BTreeSet;

fn fence(source: &RecordId, revision: &str) -> PublishedSourceFence {
    PublishedSourceFence::Head {
        source_id: source.clone(),
        generation: format!("generation-{revision}"),
        published_revision: revision.into(),
    }
}

fn audit_effect(before: &Entity, after: &Entity) -> CanonicalEffect {
    let mut base = after.base.clone();
    base.id = RecordId::new_v4(TableName::AuditLog);
    CanonicalEffect {
        before: None,
        after: Some(CanonicalRecord::AuditLog(Box::new(AuditLog {
            base,
            action: AuditAction::Update,
            actor: "maintenance_worker".into(),
            record: after.base.id.clone(),
            before: Some(serde_json::to_value(before).expect("original snapshot")),
            after: Some(serde_json::to_value(after).expect("successor snapshot")),
            timestamp: Utc::now(),
            reason: Some("Recompute the affected record".into()),
        }))),
    }
}

#[test]
fn audit_support_does_not_multiply_unrelated_batch_sources() {
    let sources: Vec<_> = (0..12)
        .map(|_| RecordId::new_v4(TableName::Source))
        .collect();
    let context = CanonicalMaintenanceContext {
        origin: CanonicalMaintenanceOrigin::Derived,
        attempt: None,
        sources: sources.iter().map(|source| fence(source, "r1")).collect(),
    };
    let mut effects = Vec::new();
    let mut support_count = 0;
    // The observed failure contained 48 independent audit snapshots in a
    // twelve-source batch. Every snapshot declared exactly its affected source.
    for source in &sources {
        for _ in 0..4 {
            let mut record = entity("unchanged source attribution");
            record.base.provenance.sources = vec![source.clone()];
            let effect = audit_effect(&record, &record);
            let original = serde_json::to_value(&effect.after).expect("original audit");
            let support = maintenance_supports(&context, &effect);
            assert_eq!(support, vec![(source.clone(), "r1".into())]);
            support_count += support.len();
            assert_eq!(
                serde_json::to_value(&effect.after).expect("retained audit"),
                original,
                "Selecting support must not rewrite audit snapshots or envelopes"
            );
            effects.push(effect);
        }
    }
    assert_eq!(effects.len(), 48);
    assert_eq!(support_count, 48);
    let request = CanonicalCommit {
        space_id: "named-a".into(),
        commit_id: "original-multi-source-decision".into(),
        input_fingerprint: "all-original-causal-inputs".into(),
        replay_output: None,
        publication: None,
        maintenance: Some(context),
        finalization: None,
        plan: CanonicalPlan {
            observed_epoch: "original-epoch".into(),
            dependencies: Vec::new(),
            extension_effects: Vec::new(),
            effects,
            outbox: Vec::new(),
        },
    };
    assert_eq!(
        causal_source_ids(&request)
            .iter()
            .map(ToString::to_string)
            .collect::<BTreeSet<_>>(),
        sources
            .iter()
            .map(ToString::to_string)
            .collect::<BTreeSet<_>>(),
        "Decision receipts and inverse authority must retain the entire causal union"
    );
}

#[test]
fn audit_support_preserves_real_conjunction_and_published_revisions() {
    let first = RecordId::new_v4(TableName::Source);
    let second = RecordId::new_v4(TableName::Source);
    let unrelated = RecordId::new_v4(TableName::Source);
    let mut before = entity("two-source decision");
    before.base.provenance.sources = vec![first.clone()];
    let mut after = before.clone();
    after.base.provenance.sources.push(second.clone());
    let effect = audit_effect(&before, &after);
    let mut context = CanonicalMaintenanceContext {
        origin: CanonicalMaintenanceOrigin::Derived,
        attempt: None,
        sources: vec![
            fence(&unrelated, "unrelated-r9"),
            fence(&second, "second-r3"),
            fence(&first, "first-r2"),
        ],
    };
    let expected = BTreeSet::from([
        (first.to_string(), "first-r2".to_string()),
        (second.to_string(), "second-r3".to_string()),
    ]);
    assert_eq!(
        maintenance_supports(&context, &effect)
            .into_iter()
            .map(|(source, revision)| (source.to_string(), revision))
            .collect::<BTreeSet<_>>(),
        expected
    );
    context.sources.reverse();
    assert_eq!(
        maintenance_supports(&context, &effect)
            .into_iter()
            .map(|(source, revision)| (source.to_string(), revision))
            .collect::<BTreeSet<_>>(),
        expected,
        "Source order cannot replace the genuine per-effect conjunction"
    );
}

#[test]
fn absent_audit_provenance_cannot_invent_batch_support() {
    let source = RecordId::new_v4(TableName::Source);
    let record = entity("independent record");
    let effect = audit_effect(&record, &record);
    let context = CanonicalMaintenanceContext {
        origin: CanonicalMaintenanceOrigin::Derived,
        attempt: None,
        sources: vec![fence(&source, "unrelated-r1")],
    };
    assert!(maintenance_supports(&context, &effect).is_empty());
}

#[test]
fn control_only_and_deleted_audits_do_not_publish_new_support() {
    let source = RecordId::new_v4(TableName::Source);
    let mut record = entity("source-backed record");
    record.base.provenance.sources = vec![source.clone()];
    let mut effect = audit_effect(&record, &record);
    let mut context = CanonicalMaintenanceContext {
        origin: CanonicalMaintenanceOrigin::ControlOnly,
        attempt: None,
        sources: vec![fence(&source, "r1")],
    };
    assert!(maintenance_supports(&context, &effect).is_empty());
    context.origin = CanonicalMaintenanceOrigin::Derived;
    if let Some(CanonicalRecord::AuditLog(audit)) = &mut effect.after {
        audit.base.deleted_at = Some(Utc::now());
    }
    assert!(maintenance_supports(&context, &effect).is_empty());
    effect.before = effect.after.take();
    assert!(maintenance_supports(&context, &effect).is_empty());
}
