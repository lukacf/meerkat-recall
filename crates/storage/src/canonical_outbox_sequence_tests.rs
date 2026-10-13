//! Actual transaction oracles for retained, retired and restored event sequences.

use super::*;

async fn sequence_client(embedded: bool) -> StorageClient {
    let client = Box::pin(client(embedded)).await;
    if !embedded {
        // Canonical retirement uses the same private reference guard as normal
        // native writes. Install its real migration, including the table events.
        client
            .query_checked(include_str!(
                "../../migrations/sql/039_private_reference_fence.surql"
            ))
            .await
            .expect("production private reference fence schema");
    }
    client
}

fn event(space: &str, label: &str) -> SystemEvent {
    SystemEvent {
        base: BaseFields::new(
            RecordId::new_v4(TableName::SystemEvent),
            space.into(),
            "sequence-regression".into(),
        ),
        seq: 0,
        event_type: "records.changed".into(),
        payload: serde_json::json!({"label":label}),
    }
}

async fn request(
    repository: &CanonicalRepository,
    space: &str,
    key: &str,
    effects: Vec<CanonicalEffect>,
    outbox: Vec<SystemEvent>,
) -> CanonicalCommit {
    CanonicalCommit {
        space_id: space.into(),
        commit_id: key.into(),
        input_fingerprint: key.into(),
        publication: None,
        maintenance: None,
        finalization: None,
        replay_output: None,
        plan: CanonicalPlan {
            observed_epoch: repository.epoch(space).await.unwrap(),
            effects,
            outbox,
            ..CanonicalPlan::default()
        },
    }
}

// Read physical membership, including any tombstones, without the repository's
// public visibility projection. Mandatory native fields have explicit types.
#[derive(Debug, PartialEq, Eq)]
struct PhysicalEvent {
    id: RecordId,
    seq: i64,
}

#[derive(serde::Deserialize)]
struct PhysicalEventDb {
    id: surrealdb::sql::Thing,
    seq: i64,
}

async fn physical_events(client: &StorageClient, space: &str) -> Vec<PhysicalEvent> {
    let mut rows = if let Some(store) = client.embedded_store() {
        store
            .scan::<SystemEvent>()
            .await
            .unwrap()
            .into_iter()
            .filter(|event| event.base.space_id == space)
            .map(|event| PhysicalEvent {
                id: event.base.id,
                seq: event.seq,
            })
            .collect::<Vec<_>>()
    } else {
        let mut response = client
            .query_bind_checked(
                "SELECT id,seq FROM system_event WHERE space_id=$space",
                crate::bindings! { "space" => space },
            )
            .await
            .unwrap();
        response
            .take::<Vec<PhysicalEventDb>>(0)
            .unwrap()
            .into_iter()
            .map(|row| PhysicalEvent {
                id: crate::surreal::record_id_from_thing(&row.id).unwrap(),
                seq: row.seq,
            })
            .collect()
    };
    rows.sort_by_key(|row| (row.seq, row.id.clone()));
    rows
}

async fn stored_event(client: &StorageClient, space: &str, id: &RecordId) -> SystemEvent {
    crate::SystemEventRepository::new(client.clone())
        .get(space, id)
        .await
        .unwrap()
        .unwrap()
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete ordered conformance scenario and its failure assertions together."
)]
async fn deleting_tail_preserves_original_sequence_floor(embedded: bool) {
    let client = Box::pin(sequence_client(embedded)).await;
    let repository = CanonicalRepository::new(client.clone());
    let original = [
        event("named-a", "retained"),
        event("named-a", "retired-2"),
        event("named-a", "retired-3"),
    ];
    let initial = Box::pin(request(
        &repository,
        "named-a",
        "original-events",
        vec![],
        original.to_vec(),
    ))
    .await;
    Box::pin(repository.commit(initial)).await.unwrap();
    let retained = Box::pin(stored_event(&client, "named-a", &original[0].base.id)).await;
    let retired_second = Box::pin(stored_event(&client, "named-a", &original[1].base.id)).await;
    let retired_tail = Box::pin(stored_event(&client, "named-a", &original[2].base.id)).await;
    assert_eq!(
        (retained.seq, retired_second.seq, retired_tail.seq),
        (1, 2, 3)
    );
    let before = Box::pin(physical_events(&client, "named-a")).await;
    assert_eq!(before.len(), 3);

    let other_config: SpaceConfig = serde_json::from_value(serde_json::json!({
        "id":RecordId::new_v4(TableName::SpaceConfig),"space_id":"named-b",
        "created_at":Utc::now(),"updated_at":Utc::now(),"created_by":"test",
        "security":{"policy_id":"default","level":0,"labels":[],"handling":[]},
        "provenance":{},"subjects":[],"subjects_state":"complete",
        "embedding_provider":"fake","embedding_model":"test","embedding_dims":8
    }))
    .unwrap();
    Box::pin(SpaceConfigRepository::new(client.clone()).create(other_config))
        .await
        .unwrap();
    let other = event("named-b", "separate sequence");
    let mut other_tail = event("named-b", "higher foreign-space tail");
    other_tail.seq = 100;
    let other_request = Box::pin(request(
        &repository,
        "named-b",
        "original-events",
        vec![CanonicalEffect {
            before: None,
            after: Some(CanonicalRecord::SystemEvent(Box::new(other_tail))),
        }],
        vec![other.clone()],
    ))
    .await;
    Box::pin(repository.commit(other_request)).await.unwrap();
    let other_before = Box::pin(stored_event(&client, "named-b", &other.base.id)).await;
    assert_eq!(other_before.seq, 101);

    let new_events = [event("named-a", "new-4"), event("named-a", "new-5")];
    let replacement = Box::pin(request(
        &repository,
        "named-a",
        "retire-original-tail",
        vec![
            CanonicalEffect {
                before: Some(CanonicalRecord::SystemEvent(Box::new(retired_tail))),
                after: None,
            },
            CanonicalEffect {
                before: Some(CanonicalRecord::SystemEvent(Box::new(retired_second))),
                after: None,
            },
        ],
        new_events.to_vec(),
    ))
    .await;
    for (fault, expected_error) in [
        (
            CanonicalFault::BeforeOutbox,
            "injected canonical fault before outbox",
        ),
        (
            CanonicalFault::BeforeReceipt,
            "injected canonical fault before receipt",
        ),
    ] {
        let error = Box::pin(repository.commit_with_fault(replacement.clone(), fault))
            .await
            .unwrap_err();
        assert!(error.to_string().contains(expected_error), "{error}");
        assert_eq!(Box::pin(physical_events(&client, "named-a")).await, before);
        assert_eq!(
            Box::pin(repository.epoch("named-a")).await.unwrap(),
            replacement.plan.observed_epoch
        );
        assert!(
            Box::pin(repository.receipt("named-a", &replacement.commit_id))
                .await
                .unwrap()
                .is_none()
        );
        for row in &original {
            let actual = Box::pin(stored_event(&client, "named-a", &row.base.id)).await;
            let mut expected = row.clone();
            expected.seq = before
                .iter()
                .find(|entry| entry.id == row.base.id)
                .unwrap()
                .seq;
            assert_eq!(
                serde_json::to_value(actual).unwrap(),
                serde_json::to_value(expected).unwrap()
            );
        }
    }
    let receipt = Box::pin(repository.commit(replacement.clone()))
        .await
        .unwrap();
    assert_eq!(
        receipt.outbox_ids,
        new_events
            .iter()
            .map(|row| row.base.id.clone())
            .collect::<Vec<_>>()
    );
    let after = Box::pin(physical_events(&client, "named-a")).await;
    assert_eq!(
        after,
        vec![
            PhysicalEvent {
                id: original[0].base.id.clone(),
                seq: 1
            },
            PhysicalEvent {
                id: new_events[0].base.id.clone(),
                seq: 4
            },
            PhysicalEvent {
                id: new_events[1].base.id.clone(),
                seq: 5
            },
        ]
    );
    assert_eq!(
        serde_json::to_value(Box::pin(stored_event(&client, "named-a", &retained.base.id)).await)
            .unwrap(),
        serde_json::to_value(retained).unwrap()
    );
    assert_eq!(
        Box::pin(repository.commit(replacement)).await.unwrap(),
        receipt
    );
    assert_eq!(Box::pin(physical_events(&client, "named-a")).await, after);
    assert_eq!(
        serde_json::to_value(Box::pin(stored_event(&client, "named-b", &other.base.id)).await)
            .unwrap(),
        serde_json::to_value(other_before).unwrap()
    );
    assert_eq!(Box::pin(physical_events(&client, "named-b")).await.len(), 2);
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete ordered conformance scenario and its failure assertions together."
)]
async fn restored_sequence_and_overflow_are_atomic(embedded: bool) {
    let client = Box::pin(sequence_client(embedded)).await;
    let repository = CanonicalRepository::new(client.clone());
    let mut restored = event("named-a", "restored explicit sequence");
    restored.seq = 20;
    let next = event("named-a", "after restored sequence");
    let publish = Box::pin(request(
        &repository,
        "named-a",
        "restored-floor",
        vec![CanonicalEffect {
            before: None,
            after: Some(CanonicalRecord::SystemEvent(Box::new(restored.clone()))),
        }],
        vec![next.clone()],
    ))
    .await;
    Box::pin(repository.commit(publish)).await.unwrap();
    assert_eq!(
        Box::pin(physical_events(&client, "named-a")).await,
        vec![
            PhysicalEvent {
                id: restored.base.id.clone(),
                seq: 20
            },
            PhysicalEvent {
                id: next.base.id.clone(),
                seq: 21
            },
        ]
    );
    let before = Box::pin(physical_events(&client, "named-a")).await;
    let mut near_limit = event("named-a", "insufficient complete-batch capacity");
    near_limit.seq = i64::MAX - 1;
    let overflow = Box::pin(request(
        &repository,
        "named-a",
        "sequence-overflow",
        vec![CanonicalEffect {
            before: None,
            after: Some(CanonicalRecord::SystemEvent(Box::new(near_limit))),
        }],
        vec![
            event("named-a", "would-fit-alone"),
            event("named-a", "would-overflow"),
        ],
    ))
    .await;
    assert!(Box::pin(repository.commit(overflow.clone())).await.is_err());
    assert_eq!(Box::pin(physical_events(&client, "named-a")).await, before);
    assert_eq!(
        Box::pin(repository.epoch("named-a")).await.unwrap(),
        overflow.plan.observed_epoch
    );
    assert!(Box::pin(repository.receipt("named-a", &overflow.commit_id))
        .await
        .unwrap()
        .is_none());

    // An existing maximum cannot be retired to make apparent room. A batch
    // that emits no new outbox rows does not need a further sequence number.
    let mut maximum = event("named-a", "original signed maximum");
    maximum.seq = i64::MAX;
    let install_maximum = Box::pin(request(
        &repository,
        "named-a",
        "no-new-outbox-at-maximum",
        vec![CanonicalEffect {
            before: None,
            after: Some(CanonicalRecord::SystemEvent(Box::new(maximum.clone()))),
        }],
        vec![],
    ))
    .await;
    Box::pin(repository.commit(install_maximum)).await.unwrap();
    let maximum_before = Box::pin(physical_events(&client, "named-a")).await;
    let retire_maximum = Box::pin(request(
        &repository,
        "named-a",
        "cannot-recycle-original-maximum",
        vec![CanonicalEffect {
            before: Some(CanonicalRecord::SystemEvent(Box::new(maximum))),
            after: None,
        }],
        vec![event("named-a", "cannot-recycle")],
    ))
    .await;
    assert!(Box::pin(repository.commit(retire_maximum.clone()))
        .await
        .is_err());
    assert_eq!(
        Box::pin(physical_events(&client, "named-a")).await,
        maximum_before
    );
    assert_eq!(
        Box::pin(repository.epoch("named-a")).await.unwrap(),
        retire_maximum.plan.observed_epoch
    );
    assert!(
        Box::pin(repository.receipt("named-a", &retire_maximum.commit_id))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn embedded_canonical_retired_event_tail_keeps_original_sequence_floor() {
    Box::pin(deleting_tail_preserves_original_sequence_floor(true)).await;
}

#[tokio::test]
async fn surreal_canonical_retired_event_tail_keeps_original_sequence_floor() {
    Box::pin(deleting_tail_preserves_original_sequence_floor(false)).await;
}

#[tokio::test]
async fn embedded_canonical_restored_event_floor_and_overflow_are_atomic() {
    Box::pin(restored_sequence_and_overflow_are_atomic(true)).await;
}

#[tokio::test]
async fn surreal_canonical_restored_event_floor_and_overflow_are_atomic() {
    Box::pin(restored_sequence_and_overflow_are_atomic(false)).await;
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete ordered conformance scenario and its failure assertions together."
)]
async fn separate_retirement_preserves_floor_and_failed_reservation_rolls_back(embedded: bool) {
    let client = Box::pin(sequence_client(embedded)).await;
    let repository = CanonicalRepository::new(client.clone());
    let originals = [
        event("named-a", "original-one"),
        event("named-a", "original-two"),
        event("named-a", "original-tail"),
    ];
    let initial = Box::pin(request(
        &repository,
        "named-a",
        "before-separate-retirement",
        vec![],
        originals.to_vec(),
    ))
    .await;
    Box::pin(repository.commit(initial)).await.unwrap();
    let original_tail = Box::pin(stored_event(&client, "named-a", &originals[2].base.id))
        .await
        .seq;
    assert_eq!(original_tail, 3);
    let mut effects = Vec::new();
    for original in &originals {
        let stored = Box::pin(stored_event(&client, "named-a", &original.base.id)).await;
        effects.push(CanonicalEffect {
            before: Some(CanonicalRecord::SystemEvent(Box::new(stored))),
            after: None,
        });
    }
    // This successful transaction emits no event. Its original tail must still
    // survive the deletion of every physical event until a later publication.
    let retirement = Box::pin(request(
        &repository,
        "named-a",
        "retire-every-event-without-append",
        effects,
        vec![],
    ))
    .await;
    let retired = Box::pin(repository.commit(retirement.clone()))
        .await
        .unwrap();
    assert!(retired.outbox_ids.is_empty());
    assert!(Box::pin(physical_events(&client, "named-a"))
        .await
        .is_empty());

    // Recreate the repository, not the database. Process reopen durability is
    // exercised independently by the lifecycle tests on the durable backend.
    let repository = CanonicalRepository::new(client.clone());
    let mut never_committed = event("named-a", "rolled-back-high-floor");
    never_committed.seq = 100;
    let failed = Box::pin(request(
        &repository,
        "named-a",
        "failed-higher-reservation",
        vec![CanonicalEffect {
            before: None,
            after: Some(CanonicalRecord::SystemEvent(Box::new(never_committed))),
        }],
        vec![event("named-a", "rolled-back-event")],
    ))
    .await;
    let error =
        Box::pin(repository.commit_with_fault(failed.clone(), CanonicalFault::BeforeReceipt))
            .await
            .unwrap_err();
    assert!(error
        .to_string()
        .contains("injected canonical fault before receipt"));
    assert!(Box::pin(physical_events(&client, "named-a"))
        .await
        .is_empty());
    assert_eq!(
        Box::pin(repository.epoch("named-a")).await.unwrap(),
        failed.plan.observed_epoch
    );
    assert!(Box::pin(repository.receipt("named-a", &failed.commit_id))
        .await
        .unwrap()
        .is_none());

    let next = event("named-a", "after-separate-retirement");
    let publication = Box::pin(request(
        &repository,
        "named-a",
        "publication-after-empty-history",
        vec![],
        vec![next.clone()],
    ))
    .await;
    let receipt = Box::pin(repository.commit(publication.clone()))
        .await
        .unwrap();
    let actual = Box::pin(stored_event(&client, "named-a", &next.base.id)).await;
    assert_eq!(actual.seq, original_tail + 1);
    assert_eq!(receipt.outbox_ids, vec![next.base.id.clone()]);
    assert_eq!(
        Box::pin(repository.commit(publication)).await.unwrap(),
        receipt
    );
    assert_eq!(
        Box::pin(repository.commit(retirement)).await.unwrap(),
        retired
    );
    assert_eq!(
        Box::pin(physical_events(&client, "named-a")).await,
        vec![PhysicalEvent {
            id: next.base.id,
            seq: original_tail + 1
        }]
    );
}

#[tokio::test]
async fn embedded_canonical_separate_retirement_and_failed_reservation_keep_floor() {
    Box::pin(separate_retirement_preserves_floor_and_failed_reservation_rolls_back(true)).await;
}

#[tokio::test]
async fn surreal_canonical_separate_retirement_and_failed_reservation_keep_floor() {
    Box::pin(separate_retirement_preserves_floor_and_failed_reservation_rolls_back(false)).await;
}
