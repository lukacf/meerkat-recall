//! Real queue claims and managed registrations retained across publication.
use super::*;
use crate::{PreparedKnowledgeViewAuthority, WorkItemRepository};
use elephant_types::publication::AttemptFence;
use elephant_types::records::{WorkItem, WorkItemStatus};

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep works in the original fixture scope through assertions and cleanup."
)]
pub(super) async fn original_claim(client: &StorageClient) -> AttemptFence {
    let work = WorkItem {
        base: BaseFields::new(
            RecordId::new_v4(TableName::WorkItem),
            "wiki-guarded".into(),
            "original-manager".into(),
        ),
        kind: "compile_knowledge_view".into(),
        payload: serde_json::json!({}),
        status: WorkItemStatus::Runnable,
        priority: 0,
        idempotency_key: uuid::Uuid::new_v4().to_string(),
        attempts: 0,
        lease_expires_at: None,
        leased_by: None,
        run_after: None,
        last_error: None,
    };
    let works = WorkItemRepository::new(client.clone());
    let created = works.create(work).await.unwrap();
    let claimed = works
        .claim_with_lease(
            "wiki-guarded",
            &created.base.id,
            "original-worker",
            Utc::now() + chrono::Duration::minutes(5),
        )
        .await
        .unwrap()
        .unwrap();
    crate::work_attempt_repository::fence_from_claim(&claimed).unwrap()
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep views in the original fixture scope through assertions and cleanup."
)]
async fn no_publication(
    client: &StorageClient,
    view: &KnowledgeView,
    revision: &KnowledgeViewRevision,
    proof: &PreparedKnowledgeViewAuthority,
) {
    // Deliberately capture a NEW content epoch after authority changed. Content
    // freshness must not silently restore this original invocation's authority.
    let epoch = CanonicalRepository::new(client.clone())
        .epoch("wiki-guarded")
        .await
        .unwrap();
    let views = KnowledgeViewRepository::new(client.clone());
    assert!(matches!(
        views
            .publish_at_epoch_with_authority(view.clone(), revision.clone(), &epoch, proof)
            .await,
        Err(StorageError::Conflict(_))
    ));
    assert!(views
        .get_view("wiki-guarded", &view.base.id)
        .await
        .unwrap()
        .is_none());
    assert!(views
        .get_revision("wiki-guarded", &revision.base.id)
        .await
        .unwrap()
        .is_none());
    assert!(views
        .list_dependency_indexes("wiki-guarded", &view.base.id)
        .await
        .unwrap()
        .is_empty());
    assert!(SystemEventRepository::new(client.clone())
        .find_recent_by_type_since(
            "knowledge_view.published",
            Utc::now() - chrono::Duration::minutes(1),
            10,
        )
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        CanonicalRepository::new(client.clone())
            .epoch("wiki-guarded")
            .await
            .unwrap(),
        epoch,
    );
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep views, proof in the original fixture scope through assertions and cleanup."
)]
async fn original_claim_and_sync_grant_each_authorize_publication() {
    for embedded in [true, false] {
        for queued in [true, false] {
            let (client, _, view, revision) = fixture(embedded).await;
            let views = KnowledgeViewRepository::new(client.clone());
            assert!(views
                .prepare_managed_publication("wiki-guarded", None, None)
                .await
                .is_err());
            let (attempt, grant) = if queued {
                (Some(original_claim(&client).await), None)
            } else {
                (
                    None,
                    Some(
                        WorkItemRepository::new(client.clone())
                            .create_managed_wiki_grant("wiki-guarded", "manager")
                            .await
                            .unwrap(),
                    ),
                )
            };
            let proof = views
                .prepare_managed_publication("wiki-guarded", attempt.as_ref(), grant.as_ref())
                .await
                .unwrap();
            assert!(views
                .prepare_managed_publication("foreign-space", attempt.as_ref(), grant.as_ref())
                .await
                .is_err());
            proof
                .verify_current_for_space("wiki-guarded")
                .await
                .unwrap();
            assert!(proof
                .verify_current_for_space("foreign-space")
                .await
                .is_err());
            assert_eq!(
                proof.original_grant_rows().len(),
                if queued { 0 } else { 2 }
            );
            let epoch = CanonicalRepository::new(client.clone())
                .epoch("wiki-guarded")
                .await
                .unwrap();
            views
                .publish_at_epoch_with_authority(view.clone(), revision.clone(), &epoch, &proof)
                .await
                .unwrap();
            proof.verify_current().await.unwrap();
            assert_eq!(
                views.get_view("wiki-guarded", &view.base.id).await.unwrap(),
                Some(view)
            );
            assert_eq!(
                views
                    .get_revision("wiki-guarded", &revision.base.id)
                    .await
                    .unwrap(),
                Some(revision)
            );
        }
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep works, proof, fresh in the original fixture scope through assertions and cleanup."
)]
async fn revoked_or_rotated_original_grant_cannot_adopt_a_fresh_epoch() {
    for embedded in [true, false] {
        let (client, _, view, revision) = fixture(embedded).await;
        let works = WorkItemRepository::new(client.clone());
        let grant = works
            .create_managed_wiki_grant("wiki-guarded", "first-manager")
            .await
            .unwrap();
        let views = KnowledgeViewRepository::new(client.clone());
        let proof = views
            .prepare_managed_publication("wiki-guarded", None, Some(&grant))
            .await
            .unwrap();
        proof.verify_current().await.unwrap();
        works
            .revoke_managed_wiki_grants("wiki-guarded")
            .await
            .unwrap();
        assert!(proof.verify_current().await.is_err());
        no_publication(&client, &view, &revision, &proof).await;
        let replacement = works
            .create_managed_wiki_grant("wiki-guarded", "replacement-manager")
            .await
            .unwrap();
        assert_ne!(grant, replacement);
        assert!(proof.verify_current().await.is_err());
        no_publication(&client, &view, &revision, &proof).await;
        let fresh = views
            .prepare_managed_publication("wiki-guarded", None, Some(&replacement))
            .await
            .unwrap();
        fresh.verify_current().await.unwrap();
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep works, proof in the original fixture scope through assertions and cleanup."
)]
async fn original_attempt_cannot_adopt_reclaim_even_with_original_active_grant() {
    for embedded in [true, false] {
        let (client, _, view, revision) = fixture(embedded).await;
        let works = WorkItemRepository::new(client.clone());
        let original = original_claim(&client).await;
        let grant = works
            .create_managed_wiki_grant("wiki-guarded", "manager")
            .await
            .unwrap();
        let views = KnowledgeViewRepository::new(client.clone());
        let proof = views
            .prepare_managed_publication("wiki-guarded", Some(&original), Some(&grant))
            .await
            .unwrap();
        works
            .requeue_with_delay(
                "wiki-guarded",
                &original.work_item_id,
                &original.lease_token,
                0,
                Some("actual worker retry".into()),
            )
            .await
            .unwrap()
            .unwrap();
        let replacement = works
            .claim_with_lease(
                "wiki-guarded",
                &original.work_item_id,
                "replacement-worker",
                Utc::now() + chrono::Duration::minutes(5),
            )
            .await
            .unwrap()
            .unwrap();
        let replacement = crate::work_attempt_repository::fence_from_claim(&replacement).unwrap();
        assert!(replacement.generation > original.generation);
        assert_ne!(replacement.lease_token, original.lease_token);
        assert!(works
            .managed_wiki_grant_is_active("wiki-guarded", &grant)
            .await
            .unwrap());
        assert!(proof.verify_current().await.is_err());
        no_publication(&client, &view, &revision, &proof).await;
        views
            .prepare_managed_publication("wiki-guarded", Some(&replacement), Some(&grant))
            .await
            .unwrap()
            .verify_current()
            .await
            .unwrap();
    }
}
