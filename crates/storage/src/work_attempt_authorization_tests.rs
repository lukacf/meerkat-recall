#![allow(clippy::unwrap_used)]
use super::*;
use crate::{Repository, WorkItemRepository};
use elephant_types::{records::base::BaseFields, TableName};

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep repository in the original fixture scope through assertions and cleanup."
)]
async fn contract(client: StorageClient) {
    let repository = WorkItemRepository::new(client.clone());
    let original = repository
        .create(WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                "guarded".into(),
                "fixture".into(),
            ),
            kind: "authorized-attempt".into(),
            payload: serde_json::json!({"input":"original"}),
            status: WorkItemStatus::Runnable,
            priority: 1,
            idempotency_key: "authorized-attempt".into(),
            attempts: 0,
            leased_by: None,
            lease_expires_at: None,
            run_after: None,
            last_error: None,
        })
        .await
        .unwrap();
    let mut reclassified = original.clone();
    reclassified.base.security.level = 2;
    let reclassified = repository
        .update("guarded", &original.base.id, reclassified)
        .await
        .unwrap();
    let until = Utc::now() + chrono::Duration::minutes(5);
    assert!(repository
        .claim_authorized(&original, "worker", until)
        .await
        .unwrap()
        .is_none());
    let untouched = repository
        .get("guarded", &original.base.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(untouched.attempts, 0);
    assert_eq!(untouched.status, WorkItemStatus::Runnable);
    let claimed = repository
        .claim_authorized(&reclassified, "worker", until)
        .await
        .unwrap()
        .unwrap();
    let token = claimed.leased_by.as_deref().unwrap();
    // Inject a concurrent stored-envelope reclassification between authorization
    // and completion; the completion itself must check the observed snapshot.
    if let Some(store) = client.embedded_store() {
        store
            .mutate(|tx| {
                let mut changed = tx
                    .get::<WorkItem>("guarded", &claimed.base.id, false)?
                    .unwrap();
                changed.base.security.level = 7;
                tx.replace(changed)?;
                Ok(())
            })
            .await
            .unwrap();
    } else {
        client
            .query_bind_checked(
                "UPDATE $id SET security.level = 7;",
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&claimed.base.id),
                },
            )
            .await
            .unwrap();
    }
    assert!(repository
        .finish_authorized(&claimed, token, WorkItemStatus::Done, None)
        .await
        .unwrap()
        .is_none());
    let current = repository
        .get("guarded", &claimed.base.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.status, WorkItemStatus::Running);
    assert_eq!(current.base.security.level, 7);
    assert!(
        repository
            .finish_authorized(&current, "worker", WorkItemStatus::Done, None)
            .await
            .unwrap()
            .is_none(),
        "caller worker label is not its original opaque token"
    );
    let done = repository
        .finish_authorized(&current, token, WorkItemStatus::Done, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(done.status, WorkItemStatus::Done);
    assert_eq!(done.base.security.level, 7);
}

#[tokio::test]
async fn embedded_claim_and_completion_compare_authorized_envelope_atomically() {
    contract(StorageClient::new_embedded().await.unwrap()).await;
}
#[tokio::test]
async fn surreal_claim_and_completion_compare_authorized_envelope_atomically() {
    contract(StorageClient::new_memory().await.unwrap()).await;
}

// Change one authorization field without changing updated_at: a timestamp-only
// check must not make these controls pass accidentally.
async fn replace_envelope(client: &StorageClient, record: WorkItem) {
    if let Some(store) = client.embedded_store() {
        store
            .mutate(|tx| tx.replace(record).map_err(crate::StorageError::from))
            .await
            .unwrap();
    } else {
        client
            .query_bind_checked(
                "UPDATE $id CONTENT $record",
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&record.base.id),
                    "record" => WorkItemRepository::work_item_to_value(&record).unwrap(),
                },
            )
            .await
            .unwrap();
    }
}

#[allow(
    clippy::significant_drop_tightening,
    reason = "Repositories share the fixture store through both claim and completion assertions."
)]
async fn changed_envelope_contract(client: StorageClient) {
    let repository = WorkItemRepository::new(client.clone());
    for field in ["security", "provenance", "payload"] {
        let original = repository
            .create(WorkItem {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::WorkItem),
                    "guarded".into(),
                    "fixture".into(),
                ),
                kind: "authorized-attempt".into(),
                payload: serde_json::json!({"nested":{"input":"original","explicit_null":null}}),
                status: WorkItemStatus::Runnable,
                priority: 1,
                idempotency_key: uuid::Uuid::new_v4().to_string(),
                attempts: 0,
                leased_by: None,
                lease_expires_at: None,
                run_after: None,
                last_error: None,
            })
            .await
            .unwrap();
        let mutate = |row: &mut WorkItem| match field {
            "security" => row.base.security.level += 1,
            "provenance" => row.base.provenance.run_id = Some(uuid::Uuid::new_v4().to_string()),
            "payload" => row.payload["nested"]["input"] = uuid::Uuid::new_v4().to_string().into(),
            _ => unreachable!(),
        };
        let mut changed = original.clone();
        mutate(&mut changed);
        assert_eq!(changed.base.updated_at, original.base.updated_at);
        replace_envelope(&client, changed.clone()).await;
        let until = Utc::now() + chrono::Duration::minutes(5);
        assert!(
            repository
                .claim_authorized(&original, "worker", until)
                .await
                .unwrap()
                .is_none(),
            "stale {field} claim"
        );
        assert_eq!(
            repository
                .get("guarded", &original.base.id)
                .await
                .unwrap()
                .unwrap(),
            changed
        );
        let claimed = repository
            .claim_authorized(&changed, "worker", until)
            .await
            .unwrap()
            .unwrap();
        let token = claimed.leased_by.as_deref().unwrap();
        let mut current = claimed.clone();
        mutate(&mut current);
        replace_envelope(&client, current.clone()).await;
        assert!(
            repository
                .finish_authorized(&claimed, token, WorkItemStatus::Done, None)
                .await
                .unwrap()
                .is_none(),
            "stale {field} completion"
        );
        assert_eq!(
            repository
                .get("guarded", &current.base.id)
                .await
                .unwrap()
                .unwrap(),
            current
        );
        assert!(
            repository
                .finish_authorized(&current, token, WorkItemStatus::Done, None)
                .await
                .unwrap()
                .is_some(),
            "current {field} completion"
        );
    }
}

#[tokio::test]
async fn embedded_changed_nested_envelope_refuses_claim_and_completion() {
    changed_envelope_contract(StorageClient::new_embedded().await.unwrap()).await;
}

#[tokio::test]
async fn native_changed_nested_envelope_refuses_claim_and_completion() {
    changed_envelope_contract(StorageClient::new_memory().await.unwrap()).await;
}

#[tokio::test]
#[allow(
    clippy::significant_drop_tightening,
    reason = "Repository retains the fixture store until both strict native comparisons finish."
)]
async fn native_unknown_nested_fields_are_not_discarded_by_claim_guard() {
    let client = StorageClient::new_memory().await.unwrap();
    let repository = WorkItemRepository::new(client.clone());
    for field in ["security", "provenance"] {
        let original = repository
            .create(WorkItem {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::WorkItem),
                    "guarded".into(),
                    "fixture".into(),
                ),
                kind: "authorized-attempt".into(),
                payload: serde_json::json!({"input":"original"}),
                status: WorkItemStatus::Runnable,
                priority: 1,
                idempotency_key: uuid::Uuid::new_v4().to_string(),
                attempts: 0,
                leased_by: None,
                lease_expires_at: None,
                run_after: None,
                last_error: None,
            })
            .await
            .unwrap();
        client
            .query_bind_checked(
                &format!("UPDATE $id SET {field}.unrecognized = 'retain-me';"),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&original.base.id),
                },
            )
            .await
            .unwrap();
        let read = || async {
            let mut response = client
                .query_bind_checked(
                    "SELECT VALUE type::string($this) FROM $id; SELECT VALUE type::string($this) FROM work_attempt WHERE work_item_id = $id;",
                    crate::bindings! {
                        "id" => crate::surreal::thing_from_record_id(&original.base.id),
                    },
                )
                .await
                .unwrap();
            let work: Vec<String> = response.take(0).unwrap();
            let mut attempts: Vec<String> = response.take(1).unwrap();
            // Keep every native row and field; ordering is only a comparison detail.
            attempts.sort();
            (work, attempts)
        };
        let before = read().await;
        assert!(repository
            .claim_with_lease(
                "guarded",
                &original.base.id,
                "worker",
                Utc::now() + chrono::Duration::minutes(5)
            )
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            read().await,
            before,
            "unknown {field} must survive an inert claim"
        );
    }
}
