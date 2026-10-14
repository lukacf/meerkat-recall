//! Original row checks must precede effects with native graph side effects.

use super::*;

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete ordered conformance scenario and its failure assertions together."
)]
async fn original_edge_guard_precedes_endpoint_deletion(embedded: bool) {
    let client = client(embedded).await;
    let entities = crate::EntityRepository::new(client.clone());
    let removed = entities.create(entity("withdrawn endpoint")).await.unwrap();
    let retained = entities.create(entity("retained endpoint")).await.unwrap();
    let independent = entities
        .create(entity("independent endpoint"))
        .await
        .unwrap();
    let relations = crate::RelRepository::new(client.clone());
    let edge = |subject: &Entity| Rel {
        base: BaseFields::new(
            RecordId::new_v4(TableName::Rel),
            "named-a".into(),
            "test".into(),
        ),
        subject: elephant_types::Link::new(subject.base.id.clone()),
        object: elephant_types::Link::new(retained.base.id.clone()),
        rel_type: "works_at".into(),
        status: elephant_types::records::ClaimStatus::Approved,
        confidence: 0.95,
        validity_window: None,
        evidence: Vec::new(),
        temporal_audit: None,
    };
    let removed_edge = relations.create(edge(&removed)).await.unwrap();
    let retained_edge = relations.create(edge(&independent)).await.unwrap();
    let event = SystemEvent {
        base: BaseFields::new(
            RecordId::new_v4(TableName::SystemEvent),
            "named-a".into(),
            "test".into(),
        ),
        seq: 0,
        event_type: "records.changed".into(),
        payload: serde_json::json!({"records":[
            {"id": removed.base.id, "op":"delete"},
            {"id": removed_edge.base.id, "op":"delete"}
        ]}),
    };
    let canonical = CanonicalRepository::new(client.clone());
    let request = CanonicalCommit {
        space_id: "named-a".into(),
        commit_id: "endpoint-before-incident-edge".into(),
        input_fingerprint: "original-endpoint-and-edge".into(),
        replay_output: None,
        publication: None,
        maintenance: None,
        finalization: None,
        plan: CanonicalPlan {
            observed_epoch: canonical.epoch("named-a").await.unwrap(),
            dependencies: Vec::new(),
            extension_effects: Vec::new(),
            // Preserve this order: native DELETE of the first row cascades to
            // the second before the explicit edge deletion executes.
            effects: vec![
                CanonicalEffect {
                    before: Some(CanonicalRecord::Entity(Box::new(removed.clone()))),
                    after: None,
                },
                CanonicalEffect {
                    before: Some(CanonicalRecord::Rel(Box::new(removed_edge.clone()))),
                    after: None,
                },
            ],
            outbox: vec![event.clone()],
        },
    };

    // A current epoch cannot authorize a stale second before-image. The whole
    // original write set still has to match before the first node is removed.
    let mut stale = request.clone();
    stale.commit_id = "stale-edge-current-epoch".into();
    let mut changed_edge = removed_edge.clone();
    changed_edge.confidence = 0.5;
    stale.plan.effects[1].before = Some(CanonicalRecord::Rel(Box::new(changed_edge)));
    assert!(canonical.commit(stale).await.is_err());
    assert!(canonical
        .receipt("named-a", "stale-edge-current-epoch")
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        entities.get("named-a", &removed.base.id).await.unwrap(),
        Some(removed.clone())
    );
    assert_eq!(
        relations
            .get("named-a", &removed_edge.base.id)
            .await
            .unwrap(),
        Some(removed_edge.clone())
    );
    assert!(CanonicalRecord::load(&client, "named-a", &event.base.id)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        canonical.epoch("named-a").await.unwrap(),
        request.plan.observed_epoch
    );

    // Native cascade and its graph indices must roll back together with the
    // first endpoint write, receipt, outbox and canonical epoch.
    assert!(canonical
        .commit_with_fault(request.clone(), CanonicalFault::AfterFirstEffect)
        .await
        .is_err());
    assert_eq!(
        entities.get("named-a", &removed.base.id).await.unwrap(),
        Some(removed.clone())
    );
    assert_eq!(
        relations
            .get("named-a", &removed_edge.base.id)
            .await
            .unwrap(),
        Some(removed_edge.clone())
    );
    assert!(relations
        .get_entities_within_n_hops("named-a", &removed.base.id, 1)
        .await
        .unwrap()
        .iter()
        .any(|neighbor| neighbor.entity_id == retained.base.id));
    assert!(canonical
        .receipt("named-a", &request.commit_id)
        .await
        .unwrap()
        .is_none());
    assert!(CanonicalRecord::load(&client, "named-a", &event.base.id)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        canonical.epoch("named-a").await.unwrap(),
        request.plan.observed_epoch
    );

    let receipt = canonical.commit(request.clone()).await.unwrap();
    assert_eq!(
        receipt.effect_ids,
        vec![removed.base.id.clone(), removed_edge.base.id.clone()]
    );
    assert!(entities
        .get("named-a", &removed.base.id)
        .await
        .unwrap()
        .is_none());
    assert!(relations
        .get("named-a", &removed_edge.base.id)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        entities.get("named-a", &retained.base.id).await.unwrap(),
        Some(retained.clone())
    );
    assert_eq!(
        entities.get("named-a", &independent.base.id).await.unwrap(),
        Some(independent.clone())
    );
    assert_eq!(
        relations
            .get("named-a", &retained_edge.base.id)
            .await
            .unwrap(),
        Some(retained_edge)
    );
    assert!(relations
        .get_entities_within_n_hops("named-a", &retained.base.id, 1)
        .await
        .unwrap()
        .iter()
        .all(|neighbor| neighbor.entity_id != removed.base.id));
    assert!(CanonicalRecord::load(&client, "named-a", &event.base.id)
        .await
        .unwrap()
        .is_some());
    assert_eq!(
        canonical
            .receipt("named-a", &request.commit_id)
            .await
            .unwrap(),
        Some(receipt.clone())
    );
    assert_eq!(canonical.commit(request).await.unwrap(), receipt);
}

#[tokio::test]
async fn embedded_original_edge_guard_precedes_endpoint_deletion() {
    original_edge_guard_precedes_endpoint_deletion(true).await;
}

#[tokio::test]
async fn native_original_edge_guard_precedes_endpoint_deletion() {
    original_edge_guard_precedes_endpoint_deletion(false).await;
}
