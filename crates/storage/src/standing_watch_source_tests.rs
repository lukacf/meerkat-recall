#![allow(clippy::expect_used)]
use super::{capture_tasks, Task};
use crate::source_publication::{AcceptSourceRevision, SourcePublicationRepository};
use crate::{Repository, StorageClient};
use elephant_types::publication::{PublicationFence, SourcePrecondition};
use elephant_types::records::{BaseFields, Source, SourceType, WorkItem, WorkItemStatus};
use elephant_types::{RecordId, TableName};

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep storage in the original fixture scope through assertions and cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep this ordered conformance scenario and its failure assertions in one function."
)]
async fn convergent_original_references(embedded: bool) {
    let storage = if embedded {
        Box::pin(StorageClient::new_embedded()).await
    } else {
        Box::pin(StorageClient::new_memory()).await
    }
    .expect("actual storage");
    if !embedded {
        for schema in [
            include_str!("../../migrations/sql/031_source_publication.surql"),
            include_str!("../../migrations/sql/032_work_recovery.surql"),
            include_str!("../../migrations/sql/041_work_producer_origin.surql"),
        ] {
            storage
                .query_checked(schema)
                .await
                .expect("explicit original schema");
        }
        crate::reference_fence::install_native_reference_guards(&storage, &[])
            .await
            .expect("actual private and physical guard schema before original records");
    }
    let space = "convergent-original-authority";
    crate::SpaceConfigRepository::new(storage.clone())
        .create(elephant_types::records::SpaceConfig {
            base: BaseFields::new(
                RecordId::new_v4(TableName::SpaceConfig),
                space.into(),
                "original caller".into(),
            ),
            embedding_provider: "unconfigured".into(),
            embedding_model: "unconfigured".into(),
            embedding_dims: 1,
            processing: None,
            purpose: None,
            skill: None,
        })
        .await
        .expect("actual named space before independent source admission");
    let accepted = SourcePublicationRepository::new(storage.clone())
        .accept(AcceptSourceRevision {
            source: Source::new(
                BaseFields::new(
                    RecordId::new_v4(TableName::Source),
                    space.into(),
                    "original caller".into(),
                ),
                SourceType::Manual,
                "actual accepted input".into(),
            ),
            revision_id: "original-revision".into(),
            expected: SourcePrecondition::Absent,
            idempotency_key: "original-admission".into(),
            request_fingerprint: "original-admission-fingerprint".into(),
            reactivate: false,
            recreate_after_erasure: false,
            authorized: None,
            authorized_document: None,
        })
        .await
        .expect("actual persisted independent source operation");
    let original = PublicationFence {
        source_id: accepted.source_id.clone(),
        generation: accepted.source_generation.clone(),
        revision_id: accepted
            .desired_revision
            .clone()
            .expect("original accepted revision"),
        operation_id: accepted.operation_id.clone(),
    };
    let proof = Box::pin(capture_tasks(
        &storage,
        space,
        (0..300).map(|_| Task::Source(original.clone())).collect(),
    ))
    .await
    .expect("identical original convergence does not consume distinct node budget");
    assert_eq!(proof.controls.len(), 1);
    assert_eq!(proof.source_ids.len(), 1);
    proof
        .verify_current(&storage)
        .await
        .expect("actual original still current");
    let mut wrong_source = original.clone();
    wrong_source.source_id = RecordId::new_v4(TableName::Source);
    let mut wrong_generation = original.clone();
    wrong_generation.generation = "another-generation".into();
    let mut wrong_revision = original.clone();
    wrong_revision.revision_id = "another-revision".into();
    for changed in [wrong_source, wrong_generation, wrong_revision] {
        // Last-in-first-out: first validate the real persisted original, then
        // encounter a branch that reuses its ID with an inconsistent fence.
        assert!(
            Box::pin(capture_tasks(
                &storage,
                space,
                vec![Task::Source(changed), Task::Source(original.clone()),]
            ))
            .await
            .is_err(),
            "completed operation ID cannot authorize a changed original reference"
        );
    }
    let work = crate::WorkItemRepository::new(storage.clone())
        .create(WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                space.into(),
                "original caller".into(),
            ),
            kind: "stage".into(),
            payload: serde_json::json!({}),
            status: WorkItemStatus::Runnable,
            priority: 1,
            idempotency_key: "original-work-intent".into(),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        })
        .await
        .expect("actual direct queue root and private origin");
    let proof = Box::pin(capture_tasks(
        &storage,
        space,
        (0..300)
            .map(|_| Task::Work(Box::new(work.clone())))
            .collect(),
    ))
    .await
    .expect("identical original Work convergence counts once");
    assert_eq!(proof.controls.len(), 1);
    assert_eq!(proof.records.len(), 1);
    let mut changed = work.clone();
    changed.base.security.level += 1;
    assert!(
        Box::pin(capture_tasks(
            &storage,
            space,
            vec![
                Task::Work(Box::new(changed)),
                Task::Work(Box::new(work.clone())),
            ]
        ))
        .await
        .is_err(),
        "completed Work identity cannot authorize a different original envelope"
    );
    proof
        .verify_current(&storage)
        .await
        .expect("rejected branch did not mutate original proof");
}

#[tokio::test]
async fn embedded_standing_convergence_requires_identical_original_fences_and_work() {
    Box::pin(convergent_original_references(true)).await;
}

#[tokio::test]
async fn native_standing_convergence_requires_identical_original_fences_and_work() {
    Box::pin(convergent_original_references(false)).await;
}
