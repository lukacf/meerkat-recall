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
