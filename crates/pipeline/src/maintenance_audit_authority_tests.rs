//! Exercise planning and canonical audit commits with real storage; no provider.

use super::*;

#[tokio::test]
async fn planning_audit_preserves_sources_removed_from_aggregate_after_image() {
    let client = setup(true).await;
    let planning = PlanningTruthMaintenanceStorage::new(client, SPACE, None)
        .await
        .unwrap();
    let first = RecordId::new_v4(TableName::Source);
    let second = RecordId::new_v4(TableName::Source);
    let mut before = ConflictGroup {
        base: base(TableName::ConflictGroup),
        slot_key: "shrinking-audit-authority".into(),
        slot_type: "rel".into(),
        claim_ids: vec![],
        resolved: false,
        resolution_rationale: None,
        detected_at: Utc::now(),
        resolved_at: None,
        adjudicated_claim_set_hash: None,
        adjudicated_at: None,
    };
    before.base.provenance.sources = vec![first.clone(), second.clone()];
    let mut after = before.clone();
    after.base.provenance.sources = vec![second.clone()];
    planning.stage(Some(before.clone()), after.clone()).unwrap();
    let audit = planning
        .create_audit_log(AuditLog {
            base: base(TableName::AuditLog),
            action: AuditAction::Update,
            actor: "test".into(),
            record: before.base.id.clone(),
            before: Some(serde_json::to_value(&before).unwrap()),
            after: Some(serde_json::to_value(&after).unwrap()),
            timestamp: Utc::now(),
            reason: None,
        })
        .await
        .unwrap();
    assert_eq!(audit.base.provenance.sources.len(), 2);
    assert!(audit.base.provenance.sources.contains(&first));
    assert!(audit.base.provenance.sources.contains(&second));
    assert_eq!(audit.base.security, before.base.security);
    assert_eq!(audit.before, Some(serde_json::to_value(&before).unwrap()));
    assert_eq!(audit.after, Some(serde_json::to_value(&after).unwrap()));
    assert_eq!(planning.prepared_plan().unwrap().effects.len(), 2);
}

#[tokio::test]
async fn planning_missing_target_or_forged_snapshot_does_not_stage_audit() {
    let client = setup(true).await;
    let planning = PlanningTruthMaintenanceStorage::new(client, SPACE, None)
        .await
        .unwrap();
    let target = Entity::new(
        base(TableName::Entity),
        EntityType::new("project"),
        "target".into(),
    );
    let log = AuditLog {
        base: base(TableName::AuditLog),
        action: AuditAction::Create,
        actor: "test".into(),
        record: target.base.id.clone(),
        before: None,
        after: Some(serde_json::to_value(&target).unwrap()),
        timestamp: Utc::now(),
        reason: None,
    };
    assert!(planning.create_audit_log(log.clone()).await.is_err());
    assert!(planning.prepared_plan().unwrap().effects.is_empty());
    planning.stage(None, target).unwrap();
    let mut forged = log;
    forged.after.as_mut().unwrap()["provenance"]["sources"] =
        serde_json::json!([RecordId::new_v4(TableName::Source)]);
    assert!(planning.create_audit_log(forged).await.is_err());
    assert_eq!(planning.prepared_plan().unwrap().effects.len(), 1);
}

#[tokio::test]
async fn coalesced_aggregate_audits_commit_intermediate_source_subsets_without_losing_union() {
    let client = setup(true).await;
    let mut sources = Vec::new();
    for label in ["first", "second"] {
        sources.push(
            elephant_storage::SourceRepository::new(client.clone())
                .create(Source::new(
                    base(TableName::Source),
                    SourceType::Manual,
                    label.into(),
                ))
                .await
                .unwrap(),
        );
    }
    let mut group_base = base(TableName::ConflictGroup);
    group_base.provenance.sources = sources
        .iter()
        .map(|source| source.base.id.clone())
        .collect();
    let initial = elephant_storage::ConflictGroupRepository::new(client.clone())
        .create(ConflictGroup {
            base: group_base,
            slot_key: "coalesced-audit-union".into(),
            slot_type: "rel".into(),
            claim_ids: Vec::new(),
            resolved: false,
            resolution_rationale: None,
            detected_at: Utc::now(),
            resolved_at: None,
            adjudicated_claim_set_hash: None,
            adjudicated_at: None,
        })
        .await
        .unwrap();
    let planning = PlanningTruthMaintenanceStorage::new(client.clone(), SPACE, None)
        .await
        .unwrap();
    let mut previous = initial.clone();
    let mut audit_ids = Vec::new();
    for source in &sources {
        let mut after = previous.clone();
        after.base.provenance.sources = vec![source.base.id.clone()];
        planning
            .stage(Some(previous.clone()), after.clone())
            .unwrap();
        let audit = planning
            .create_audit_log(AuditLog {
                base: base(TableName::AuditLog),
                action: AuditAction::Update,
                actor: "test".into(),
                record: initial.base.id.clone(),
                before: Some(serde_json::to_value(&previous).unwrap()),
                after: Some(serde_json::to_value(&after).unwrap()),
                timestamp: Utc::now(),
                reason: None,
            })
            .await
            .unwrap();
        assert_eq!(audit.base.provenance.sources.len(), 2);
        audit_ids.push(audit.base.id);
        previous = after;
    }
    planning
        .commit("audit_union_test", "coalesced", &serde_json::json!({}), &())
        .await
        .unwrap();
    let stored = elephant_storage::ConflictGroupRepository::new(client.clone())
        .get(SPACE, &initial.base.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stored.base.provenance.sources,
        vec![sources[1].base.id.clone()]
    );
    for id in audit_ids {
        let stored = elephant_storage::AuditLogRepository::new(client.clone())
            .get(SPACE, &id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.base.provenance.sources.len(), 2);
        assert!(stored.base.provenance.sources.contains(&sources[0].base.id));
        assert!(stored.base.provenance.sources.contains(&sources[1].base.id));
    }
}

#[tokio::test]
async fn intermediate_only_source_refuses_coalesced_audit_without_publication() {
    for embedded in [true, false] {
        Box::pin(intermediate_only_source_refuses_coalesced_audit(embedded)).await;
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "Complete atomic coalescing refusal scenario."
)]
async fn intermediate_only_source_refuses_coalesced_audit(embedded: bool) {
    let client = setup(embedded).await;
    let sources = elephant_storage::SourceRepository::new(client.clone());
    let first = sources
        .create(Source::new(
            base(TableName::Source),
            SourceType::Manual,
            "first".into(),
        ))
        .await
        .unwrap();
    let second = sources
        .create(Source::new(
            base(TableName::Source),
            SourceType::Manual,
            "second".into(),
        ))
        .await
        .unwrap();
    drop(sources);
    let mut group_base = base(TableName::ConflictGroup);
    group_base.provenance.sources = vec![first.base.id.clone()];
    let groups = elephant_storage::ConflictGroupRepository::new(client.clone());
    let original = groups
        .create(ConflictGroup {
            base: group_base,
            slot_key: "intermediate-only-audit-source".into(),
            slot_type: "rel".into(),
            claim_ids: Vec::new(),
            resolved: false,
            resolution_rationale: None,
            detected_at: Utc::now(),
            resolved_at: None,
            adjudicated_claim_set_hash: None,
            adjudicated_at: None,
        })
        .await
        .unwrap();
    let canonical = CanonicalRepository::new(client.clone());
    let epoch = canonical.epoch(SPACE).await.unwrap();
    let planning = PlanningTruthMaintenanceStorage::new(client.clone(), SPACE, None)
        .await
        .unwrap();
    // Even a legitimately observed decision input does not authenticate its
    // presence in this target after the intermediate target image is gone.
    planning.observe(&second).unwrap();
    let mut intermediate = original.clone();
    intermediate
        .base
        .provenance
        .sources
        .push(second.base.id.clone());
    let intermediate = planning
        .update_conflict_group(SPACE, &original.base.id, intermediate)
        .await
        .unwrap();
    let after_snapshot = serde_json::to_value(&intermediate).unwrap();
    let audit = planning
        .create_audit_log(AuditLog {
            base: base(TableName::AuditLog),
            action: AuditAction::Update,
            actor: "test".into(),
            record: original.base.id.clone(),
            before: Some(serde_json::to_value(&original).unwrap()),
            after: Some(after_snapshot.clone()),
            timestamp: Utc::now(),
            reason: None,
        })
        .await
        .unwrap();
    assert!(audit.base.provenance.sources.contains(&second.base.id));
    planning
        .update_conflict_group(SPACE, &original.base.id, original.clone())
        .await
        .unwrap();
    let error = planning
        .commit(
            "audit_union_test",
            "intermediate-only",
            &serde_json::json!({}),
            &(),
        )
        .await
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("maintenance audit snapshot authority mismatch"));
    assert_eq!(canonical.epoch(SPACE).await.unwrap(), epoch);
    drop(canonical);
    let stored = groups.get(SPACE, &original.base.id).await.unwrap().unwrap();
    drop(groups);
    assert_eq!(
        serde_json::to_value(stored).unwrap(),
        serde_json::to_value(original).unwrap()
    );
    assert!(elephant_storage::AuditLogRepository::new(client)
        .get(SPACE, &audit.base.id)
        .await
        .unwrap()
        .is_none());
    let prepared = planning.prepared_plan().unwrap();
    let retained = prepared
        .effects
        .iter()
        .find_map(|effect| match &effect.after {
            Some(CanonicalRecord::AuditLog(row)) if row.base.id == audit.base.id => Some(row),
            _ => None,
        })
        .unwrap();
    assert_eq!(retained.after, Some(after_snapshot));
    assert!(retained.base.provenance.sources.contains(&second.base.id));
}
