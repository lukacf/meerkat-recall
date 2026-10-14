//! All original sibling inputs and every effect share the same transaction.
use super::*;
use crate::{PreparedKnowledgeViewAuthority, WorkItemRepository};
use elephant_types::records::HasSecurity;

type Publications = Vec<(KnowledgeView, KnowledgeViewRevision)>;

async fn siblings(embedded: bool) -> (StorageClient, Entity, Publications) {
    let (client, root, view, revision) = fixture(embedded).await;
    let mut second_root = root;
    second_root.base.id = RecordId::new_v4(TableName::Entity);
    second_root.display_name = "Second independent project".into();
    EntityRepository::new(client.clone())
        .create(second_root.clone())
        .await
        .unwrap();
    let mut second_view = view.clone();
    let mut second_revision = revision.clone();
    second_view.base.id = RecordId::new_v4(TableName::KnowledgeView);
    second_revision.base.id = RecordId::new_v4(TableName::KnowledgeViewRevision);
    second_view.key = format!("view:{}", second_root.base.id);
    second_view.root_record = second_root.base.id.to_string();
    second_view.title = second_root.display_name.clone();
    second_view.slug = "second-project".into();
    second_view.current_revision = Some(second_revision.base.id.to_string());
    second_view.base.subjects = second_root.subjects();
    second_view.base.subjects_state = second_root.subjects_state();
    second_revision.view_id = second_view.base.id.to_string();
    second_revision.root_record = second_root.base.id.to_string();
    second_revision.rendered_markdown = second_root.display_name.clone();
    second_revision.dependencies[0].record_id = second_root.base.id.to_string();
    second_revision.dependencies[0].observed_updated_at = Some(second_root.base.updated_at);
    second_revision.citations[0].record_id = second_root.base.id.to_string();
    second_revision.base.subjects = second_root.subjects();
    second_revision.base.subjects_state = second_root.subjects_state();
    (
        client,
        second_root,
        vec![(view, revision), (second_view, second_revision)],
    )
}
async fn epoch(client: &StorageClient) -> String {
    CanonicalRepository::new(client.clone())
        .epoch("wiki-guarded")
        .await
        .unwrap()
}
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep repo in the original fixture scope through assertions and cleanup."
)]
async fn absent(client: &StorageClient, rows: &Publications) {
    let repo = KnowledgeViewRepository::new(client.clone());
    for (view, revision) in rows {
        assert!(repo
            .get_view("wiki-guarded", &view.base.id)
            .await
            .unwrap()
            .is_none());
        assert!(repo
            .get_revision("wiki-guarded", &revision.base.id)
            .await
            .unwrap()
            .is_none());
        assert!(repo
            .list_dependency_indexes("wiki-guarded", &view.base.id)
            .await
            .unwrap()
            .is_empty());
    }
}
async fn grant(client: &StorageClient) -> (RecordId, PreparedKnowledgeViewAuthority) {
    let id = WorkItemRepository::new(client.clone())
        .create_managed_wiki_grant("wiki-guarded", "batch-manager")
        .await
        .unwrap();
    let proof = KnowledgeViewRepository::new(client.clone())
        .prepare_managed_publication("wiki-guarded", None, Some(&id))
        .await
        .unwrap();
    (id, proof)
}
async fn events(client: &StorageClient) -> Vec<elephant_types::records::SystemEvent> {
    SystemEventRepository::new(client.clone())
        .find_recent_by_type_since(
            "knowledge_view.published",
            Utc::now() - chrono::Duration::minutes(1),
            10,
        )
        .await
        .unwrap()
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Poll the original conformance future directly without changing its allocation or await shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep repo in the original fixture scope through assertions and cleanup."
)]
async fn two_original_siblings_publish_with_complete_receipts_and_empty_is_read_only() {
    for embedded in [true, false] {
        let (client, _, rows) = siblings(embedded).await;
        let (_, proof) = grant(&client).await;
        let before = epoch(&client).await;
        let repo = KnowledgeViewRepository::new(client.clone());
        assert!(repo
            .publish_batch_at_epoch(vec![], &before, Some(&proof))
            .await
            .unwrap()
            .is_empty());
        assert_eq!(epoch(&client).await, before);
        assert_eq!(
            repo.publish_batch_at_epoch(rows.clone(), &before, Some(&proof))
                .await
                .unwrap(),
            rows
        );
        for (view, revision) in &rows {
            assert_eq!(
                repo.get_view("wiki-guarded", &view.base.id).await.unwrap(),
                Some(view.clone())
            );
            assert_eq!(
                repo.get_revision("wiki-guarded", &revision.base.id)
                    .await
                    .unwrap(),
                Some(revision.clone())
            );
            assert_eq!(
                repo.list_dependency_indexes("wiki-guarded", &view.base.id)
                    .await
                    .unwrap()
                    .len(),
                3
            );
        }
        let mut receipt_events = events(&client).await;
        receipt_events.sort_by_key(|event| event.seq);
        assert_eq!(receipt_events.len(), 2);
        assert_eq!(receipt_events[0].seq + 1, receipt_events[1].seq);
        assert_ne!(epoch(&client).await, before);
        proof.verify_current().await.unwrap();
    }
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Poll the original conformance future directly without changing its allocation or await shape."
)]
async fn second_sibling_outbox_collision_rolls_back_first_sibling_and_entire_batch() {
    for embedded in [true, false] {
        let (client, _, rows) = siblings(embedded).await;
        let (_, proof) = grant(&client).await;
        // A real preexisting unique event identity makes the second event write
        // fail AFTER the first sibling's view/revision/index/event statements.
        let mut collision = publication::publication_event(&rows[1].0, &rows[1].1);
        collision.seq = 1;
        collision.event_type = "fixture.existing_event".into();
        collision.payload = serde_json::json!({"original":"must survive unchanged"});
        let collision = SystemEventRepository::new(client.clone())
            .create(collision)
            .await
            .unwrap();
        let before = epoch(&client).await;
        assert!(KnowledgeViewRepository::new(client.clone())
            .publish_batch_at_epoch(rows.clone(), &before, Some(&proof))
            .await
            .is_err());
        absent(&client, &rows).await;
        assert!(events(&client).await.is_empty());
        assert_eq!(epoch(&client).await, before);
        assert_eq!(
            SystemEventRepository::new(client.clone())
                .get("wiki-guarded", &collision.base.id)
                .await
                .unwrap(),
            Some(collision)
        );
    }
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Poll the original conformance future directly without changing its allocation or await shape."
)]
async fn original_external_input_change_or_revocation_refuses_all_siblings() {
    for embedded in [true, false] {
        let (client, mut root, rows) = siblings(embedded).await;
        let (_, proof) = grant(&client).await;
        let original = epoch(&client).await;
        root.display_name = "External canonical edit after preparation".into();
        EntityRepository::new(client.clone())
            .update("wiki-guarded", &root.base.id, root.clone())
            .await
            .unwrap();
        let changed = epoch(&client).await;
        assert_ne!(original, changed);
        assert!(KnowledgeViewRepository::new(client.clone())
            .publish_batch_at_epoch(rows.clone(), &original, Some(&proof))
            .await
            .is_err());
        absent(&client, &rows).await;
        assert!(events(&client).await.is_empty());
        assert_eq!(epoch(&client).await, changed);

        let (client, _, rows) = siblings(embedded).await;
        let (_, proof) = grant(&client).await;
        WorkItemRepository::new(client.clone())
            .revoke_managed_wiki_grants("wiki-guarded")
            .await
            .unwrap();
        let after_revocation = epoch(&client).await;
        assert!(KnowledgeViewRepository::new(client.clone())
            .publish_batch_at_epoch(rows.clone(), &after_revocation, Some(&proof))
            .await
            .is_err());
        absent(&client, &rows).await;
        assert!(events(&client).await.is_empty());
        assert_eq!(epoch(&client).await, after_revocation);
        assert!(KnowledgeViewRepository::new(client)
            .publish_batch_at_epoch(vec![], &after_revocation, Some(&proof))
            .await
            .is_err());
    }
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Poll the original conformance future directly without changing its allocation or await shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep repo, works in the original fixture scope through assertions and cleanup."
)]
async fn replaced_original_attempt_refuses_entire_sibling_batch() {
    for embedded in [true, false] {
        let (client, _, rows) = siblings(embedded).await;
        let attempt = super::authority_tests::original_claim(&client).await;
        let repo = KnowledgeViewRepository::new(client.clone());
        let proof = repo
            .prepare_managed_publication("wiki-guarded", Some(&attempt), None)
            .await
            .unwrap();
        let works = WorkItemRepository::new(client.clone());
        works
            .requeue_with_delay(
                "wiki-guarded",
                &attempt.work_item_id,
                &attempt.lease_token,
                0,
                None,
            )
            .await
            .unwrap()
            .unwrap();
        works
            .claim_with_lease(
                "wiki-guarded",
                &attempt.work_item_id,
                "new-worker",
                Utc::now() + chrono::Duration::minutes(5),
            )
            .await
            .unwrap()
            .unwrap();
        let current_epoch = epoch(&client).await;
        assert!(repo
            .publish_batch_at_epoch(rows.clone(), &current_epoch, Some(&proof))
            .await
            .is_err());
        absent(&client, &rows).await;
        assert!(events(&client).await.is_empty());
        assert_eq!(epoch(&client).await, current_epoch);
    }
}
