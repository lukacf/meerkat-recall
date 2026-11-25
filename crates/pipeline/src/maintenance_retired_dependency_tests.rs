//! Retired inputs remain transaction guards, but cannot supply live evidence.
use super::*;
use crate::truth_maintenance::{execute_tm_detect_conflicts, TriggerEvent};
use elephant_storage::{
    AssertionRepository, AttributeRepository, EventRepository, TruthSlotRepository,
};

async fn retired_claims(client: &StorageClient) -> (Rel, Attribute, Event) {
    let row = claim(client).await;
    let (rel, source) = legacy_claim(client, row, "Ada works at Research").await;
    let mut attr_base = rel.base.clone();
    attr_base.id = RecordId::new_v4(TableName::Attribute);
    let attr = AttributeRepository::new(client.clone())
        .create(Attribute {
            base: attr_base,
            subject: rel.subject.clone(),
            attr_type: "role".into(),
            value: Literal::String("researcher".into()),
            status: ClaimStatus::Approved,
            confidence: 0.9,
            validity_window: None,
            evidence: rel.evidence.clone(),
            temporal_audit: None,
        })
        .await
        .unwrap();
    let mut event_base = rel.base.clone();
    event_base.id = RecordId::new_v4(TableName::Event);
    let event = EventRepository::new(client.clone())
        .create(
            Event::new(
                event_base,
                "Ada joins Research",
                vec![rel.evidence[0].assertion.clone()],
                vec![Link::new(source.base.id)],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    RelRepository::new(client.clone())
        .delete(SPACE, &rel.base.id)
        .await
        .unwrap();
    AttributeRepository::new(client.clone())
        .delete(SPACE, &attr.base.id)
        .await
        .unwrap();
    EventRepository::new(client.clone())
        .delete(SPACE, &event.base.id)
        .await
        .unwrap();
    AssertionRepository::new(client.clone())
        .delete(SPACE, rel.evidence[0].assertion.id())
        .await
        .unwrap();
    (rel, attr, event)
}

#[tokio::test]
async fn mixed_retired_and_live_detection_commits_the_live_claim() {
    // Both actual storage backends traverse the production detection/commit path.
    // Separate retained sources isolate the missing-old-assertion failure; live
    // whole-source replacement is additionally qualified by installed acceptance.
    for embedded in [true, false] {
        let client = setup(embedded).await;
        let (old_rel, old_attr, old_event) = retired_claims(&client).await;
        let row = claim(&client).await;
        let (live, _) = legacy_claim(&client, row, "Ada works at Research").await;
        let storage = RepositoryTruthMaintenanceStorage::new(client.clone());
        let events = [
            TriggerEvent::Rel(old_rel.base.id.clone()),
            TriggerEvent::Attribute(old_attr.base.id.clone()),
            TriggerEvent::Event(old_event.base.id.clone()),
            TriggerEvent::Rel(live.base.id.clone()),
        ];
        let result = execute_tm_detect_conflicts(&events, SPACE, &storage)
            .await
            .unwrap();
        assert!(result.direct_commits.is_empty());
        let stored = RelRepository::new(client.clone())
            .get(SPACE, &live.base.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.status, ClaimStatus::Approved);
        let slots = TruthSlotRepository::new(client.clone())
            .list(SPACE, 100, None)
            .await
            .unwrap();
        assert!(slots.iter().any(|slot| slot
            .current_values
            .iter()
            .any(|value| value.claim_id == live.base.id.to_string())));
        for id in [&old_rel.base.id, &old_attr.base.id, &old_event.base.id] {
            assert!(!slots.iter().any(|slot| slot
                .current_values
                .iter()
                .any(|value| value.claim_id == id.to_string())));
        }
        assert!(storage
            .get_rel(SPACE, &old_rel.base.id)
            .await
            .unwrap()
            .is_none());
        assert!(storage
            .get_attribute(SPACE, &old_attr.base.id)
            .await
            .unwrap()
            .is_none());
        assert!(storage
            .get_event(SPACE, &old_event.base.id)
            .await
            .unwrap()
            .is_none());
    }
}

#[tokio::test]
async fn retired_dependencies_still_fence_changes_and_cannot_be_restored_without_evidence() {
    for embedded in [true, false] {
        let client = setup(embedded).await;
        let (old, _, _) = retired_claims(&client).await;
        let row = claim(&client).await;
        let (live, _) = legacy_claim(&client, row, "Ada works at Research").await;
        let planning = prepare_approval(&client, &live).await;
        let tombstone = planning
            .get_rel_with_deleted(SPACE, &old.base.id)
            .await
            .unwrap()
            .unwrap();
        let request = planning
            .prepare_request("retired_guard", "race", &serde_json::json!({}), &())
            .await
            .unwrap();
        assert!(request
            .plan
            .dependencies
            .iter()
            .any(|row| row.base().id == old.base.id && row.base().deleted_at.is_some()));

        let restoration = PlanningTruthMaintenanceStorage::new(client.clone(), SPACE, None)
            .await
            .unwrap();
        restoration.stage(Some(tombstone), old.clone()).unwrap();
        assert!(restoration
            .prepare_request("retired_guard", "restore", &serde_json::json!({}), &())
            .await
            .is_err());

        // A repository mutation does not advance the canonical epoch: the exact
        // retained before-image itself must prevent the prepared commit.
        RelRepository::new(client.clone())
            .restore(SPACE, &old.base.id)
            .await
            .unwrap();
        let canonical = CanonicalRepository::new(client.clone());
        assert_eq!(
            canonical.epoch(SPACE).await.unwrap(),
            request.plan.observed_epoch
        );
        let error = canonical
            .commit(request)
            .await
            .expect_err("changed tombstone must invalidate the whole transaction");
        assert!(
            error.to_string().contains("canonical_dependency_changed")
                || error.to_string().contains("canonical_row_changed"),
            "refusal must come from the retained row guard: {error}"
        );
        assert_eq!(
            RelRepository::new(client.clone())
                .get(SPACE, &live.base.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            ClaimStatus::Candidate
        );

        // Once active, this same row must still fail for its missing assertion.
        let storage = RepositoryTruthMaintenanceStorage::new(client.clone());
        assert!(
            execute_tm_detect_conflicts(&[TriggerEvent::Rel(old.base.id)], SPACE, &storage)
                .await
                .is_err()
        );
    }
}
