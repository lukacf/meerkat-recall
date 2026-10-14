//! Empty-slot acknowledgement uses the real canonical transaction on both
//! backends, including refusal after a competing canonical write.
use super::*;
use crate::truth_maintenance::{
    compute_record_partition, compute_slot_key, execute_tm_recompute, RecomputePriority, TmContext,
};
use elephant_storage::TruthSlotRepository;
use elephant_types::records::truth::{CurrentValue, TruthSlot};

#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete competing-commit, refusal and recovery sequence together"
)]
async fn empty_projection_is_atomic(embedded: bool) {
    let client = setup(embedded).await;
    let deleted = claim(&client).await;
    let mut slot_base = deleted.base.clone();
    slot_base.id = RecordId::new_v4(TableName::TruthSlot);
    let partition = compute_record_partition(&deleted.base);
    let key = compute_slot_key(
        &deleted.subject.id().to_string(),
        Some(&deleted.object.id().to_string()),
        &deleted.rel_type,
        None,
        &partition,
    );
    let original = TruthSlot {
        base: slot_base,
        slot_key: key,
        slot_type: "rel".into(),
        subject_entity: deleted.subject.id().to_string(),
        predicate_id: deleted.rel_type.clone(),
        current_values: vec![CurrentValue {
            claim_id: deleted.base.id.to_string(),
            confidence: deleted.confidence,
            status: deleted.status,
            evidence: deleted.evidence.clone(),
        }],
        conflict_group: None,
        confidence: deleted.confidence,
        rationale: None,
        semantics_version: 0,
        alias_version: 0,
        stale: true,
        stale_reason: Some("last_claim_deleted".into()),
        computed_at: Utc::now(),
        security_hash: partition,
    };
    let slots = TruthSlotRepository::new(client.clone());
    slots.create(original.clone()).await.unwrap();
    RelRepository::new(client.clone())
        .delete(SPACE, &deleted.base.id)
        .await
        .unwrap();
    let planning = PlanningTruthMaintenanceStorage::new(client.clone(), SPACE, None)
        .await
        .unwrap();
    let result = execute_tm_recompute(
        &TmContext::none(),
        SPACE,
        RecomputePriority::Normal,
        10,
        &planning,
    )
    .await
    .unwrap();
    assert_eq!(result.slots_recomputed, 1);
    assert!(result.pending_conflict_groups.is_empty());
    let live = slots.get(SPACE, &original.base.id).await.unwrap().unwrap();
    assert_eq!(
        serde_json::to_value(&live).unwrap(),
        serde_json::to_value(&original).unwrap(),
        "empty acknowledgement escaped preparation"
    );

    // A distinct canonical transaction advances the epoch after preparation.
    // The old empty result must not acknowledge the now-stale read set.
    let unrelated = claim(&client).await;
    let competitor = prepare_approval(&client, &unrelated).await;
    competitor
        .commit(
            "empty_slot_competitor",
            "other-claim",
            &serde_json::json!({}),
            &(),
        )
        .await
        .unwrap();
    assert!(planning
        .commit(
            "empty_slot_test",
            "original",
            &serde_json::json!({}),
            &result
        )
        .await
        .is_err());
    let live = slots.get(SPACE, &original.base.id).await.unwrap().unwrap();
    assert!(live.stale);
    assert_eq!(live.current_values.len(), 1);

    let current = PlanningTruthMaintenanceStorage::new(client.clone(), SPACE, None)
        .await
        .unwrap();
    let result = execute_tm_recompute(
        &TmContext::none(),
        SPACE,
        RecomputePriority::Normal,
        10,
        &current,
    )
    .await
    .unwrap();
    assert_eq!(result.slots_recomputed, 1);
    current
        .commit("empty_slot_test", "fresh", &serde_json::json!({}), &result)
        .await
        .unwrap();
    let settled = slots.get(SPACE, &original.base.id).await.unwrap().unwrap();
    assert!(!settled.stale);
    assert!(settled.current_values.is_empty());
    assert_eq!(settled.confidence.to_bits(), 0.0_f32.to_bits());
    assert_eq!(
        serde_json::to_value(&settled.base.security).unwrap(),
        serde_json::to_value(&original.base.security).unwrap()
    );
    assert_eq!(settled.base.provenance, original.base.provenance);
    assert_eq!(settled.base.subjects, original.base.subjects);
    let unchanged = RelRepository::new(client.clone())
        .get(SPACE, &unrelated.base.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(unchanged.status, ClaimStatus::Approved);
    let repeat = PlanningTruthMaintenanceStorage::new(client.clone(), SPACE, None)
        .await
        .unwrap();
    assert_eq!(
        execute_tm_recompute(
            &TmContext::none(),
            SPACE,
            RecomputePriority::Normal,
            10,
            &repeat
        )
        .await
        .unwrap()
        .stale_slots_found,
        0
    );
}

#[tokio::test]
async fn embedded_empty_slot_acknowledgement_is_atomic_and_epoch_fenced() {
    Box::pin(empty_projection_is_atomic(true)).await;
}

#[tokio::test]
async fn surreal_empty_slot_acknowledgement_is_atomic_and_epoch_fenced() {
    Box::pin(empty_projection_is_atomic(false)).await;
}
