//! Independent boundary checks through actual persisted producer evidence.
//! Included as a child of reprocess::budget by its implementation owner.
//! No provider, fabricated producer row, synthetic completion or new authority.

use super::{guard_physical_dispatch, guard_physical_dispatch_in_walk, ProducerNode, ProducerWalk};
use elephant_storage::source_publication::{AcceptSourceRevision, SourcePublicationRepository};
use elephant_storage::space_grant_repository::SpaceGrantRepository;
use elephant_storage::work_producer_repository::WorkProducerRepository;
use elephant_storage::{Repository, StorageClient, WorkRunRepository};
use elephant_types::publication::{PublicationFence, SourcePrecondition};
use elephant_types::records::ingest::IngestMode;
use elephant_types::records::{Source, SourceType, WorkItem, WorkItemStatus, WorkRun};
use elephant_types::{RecordId, TableName};
use std::time::{Duration, Instant};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

async fn successor(
    storage: &StorageClient,
    parent: &WorkItem,
    run: &WorkRun,
    ordinal: usize,
) -> TestResult<(WorkItem, WorkRun)> {
    let mut child = parent.clone();
    child.base.id = RecordId::new_v4(TableName::WorkItem);
    child.kind = "extract_assertions".into();
    child.payload = serde_json::json!({"independent_ancestry_ordinal": ordinal});
    child.status = WorkItemStatus::Runnable;
    child.idempotency_key = format!("independent-original-successor-{ordinal}");
    child.attempts = 0;
    child.lease_expires_at = None;
    child.leased_by = None;
    child.run_after = None;
    child.last_error = None;
    let accepted = Box::pin(
        WorkProducerRepository::new(storage.clone()).enqueue_successor(parent, run, child, false),
    )
    .await?;
    let (work, run, _) = Box::pin(super::standing_dispatch_tests::claim_run(
        storage,
        &accepted.base.id,
    ))
    .await;
    Ok((work, run))
}

async fn direct_operation(
    storage: &StorageClient,
    original: &Source,
    ordinal: usize,
) -> TestResult<PublicationFence> {
    let mut base = original.base.clone();
    base.id = RecordId::new_v4(TableName::Source);
    let mut source = Source::new(base, SourceType::Manual, format!("direct-input-{ordinal}"));
    source.uri = Some(format!("https://example.invalid/ancestry/{ordinal}"));
    source.set_requested_ingest_mode(IngestMode::Metadata);
    let receipt = Box::pin(SourcePublicationRepository::new(storage.clone()).accept(
        AcceptSourceRevision {
            source,
            revision_id: format!("original-direct-revision-{ordinal}"),
            expected: SourcePrecondition::Absent,
            idempotency_key: format!("original-direct-intent-{ordinal}"),
            request_fingerprint: format!("original-direct-input-{ordinal}"),
            reactivate: false,
            recreate_after_erasure: false,
            authorized: None,
            authorized_document: None,
        },
    ))
    .await?;
    Ok(PublicationFence {
        source_id: receipt.source_id,
        generation: receipt.source_generation,
        revision_id: receipt
            .desired_revision
            .ok_or_else(|| std::io::Error::other("accepted direct revision missing"))?,
        operation_id: receipt.operation_id,
    })
}

#[expect(
    clippy::expect_used,
    reason = "Unexpected acceptance must fail the original persisted Source, Work and successor boundary assertions."
)]
async fn original_boundaries(embedded: bool) -> TestResult {
    let started = Instant::now();
    let mark = |phase: &str, nodes: usize| {
        eprintln!(
            "independent_ancestry backend={} phase={phase} nodes={nodes} elapsed_ms={}",
            if embedded {
                "elephant-db"
            } else {
                "surreal-db"
            },
            started.elapsed().as_millis()
        );
    };
    mark("fixture_start", 0);
    let (storage, source, root, root_run, grant) =
        Box::pin(super::standing_dispatch_tests::fixture(embedded)).await;
    mark("fixture_complete", 1);
    let space = root.base.space_id.clone();
    let mut current = root;
    let mut run = root_run;
    // 1 actual signed root + 255 actual private successor edges = 256 Works.
    // Each edge uses its actual persisted claim/run; no private row is edited.
    for ordinal in 1..256 {
        (current, run) = Box::pin(successor(&storage, &current, &run, ordinal)).await?;
        if (ordinal + 1) % 32 == 0 {
            mark("work_construction", ordinal + 1);
        }
    }
    mark("work_walk_start", 256);
    Box::pin(guard_physical_dispatch(&storage, &current)).await?;
    mark("work_walk_complete", 256);

    let mut walk = ProducerWalk::default();
    let mut sources = Vec::new();
    for ordinal in 0..257 {
        let publication = Box::pin(direct_operation(&storage, &source, ordinal)).await?;
        // Resolve actual admitted Source operation descriptors, not counter-only
        // stand-ins. All 257 Source nodes share the same walk as all 256 Works.
        Box::pin(super::super::source_job::guard_source_dispatch_in_walk(
            &storage,
            &space,
            &publication,
            &mut walk,
        ))
        .await?;
        sources.push(publication);
        if sources.len() % 32 == 0 || sources.len() == 257 {
            mark("source_construction_and_walk", sources.len());
        }
    }
    mark("shared_work_walk_start", 513);
    Box::pin(guard_physical_dispatch_in_walk(
        &storage, &current, &mut walk,
    ))
    .await?;
    mark("shared_work_walk_complete", 513);
    // Converged, already verified branches cannot consume the limit again.
    for (index, publication) in sources.iter().rev().enumerate() {
        Box::pin(super::super::source_job::guard_source_dispatch_in_walk(
            &storage,
            &space,
            publication,
            &mut walk,
        ))
        .await?;
        if (index + 1) % 32 == 0 || index + 1 == 257 {
            mark("source_replay_walk", index + 1);
        }
    }
    mark("shared_work_replay_start", 513);
    Box::pin(guard_physical_dispatch_in_walk(
        &storage, &current, &mut walk,
    ))
    .await?;
    mark("shared_work_replay_complete", 513);

    mark("source_overflow_start", 258);
    let extra_source = Box::pin(direct_operation(&storage, &source, 257)).await?;
    let error = Box::pin(super::super::source_job::guard_source_dispatch_in_walk(
        &storage,
        &space,
        &extra_source,
        &mut walk,
    ))
    .await
    .expect_err("258th distinct actual Source operation must exceed the original bound");
    assert!(
        error
            .to_string()
            .contains("exceeds its original execution bound"),
        "wrong refusal for Source boundary: {error}"
    );
    mark("source_overflow_refused", 258);

    mark("work_overflow_construction_start", 257);
    let (extra_work, extra_run) = Box::pin(successor(&storage, &current, &run, 256)).await?;
    mark("work_overflow_walk_start", 257);
    let error = Box::pin(guard_physical_dispatch_in_walk(
        &storage,
        &extra_work,
        &mut ProducerWalk::default(),
    ))
    .await
    .expect_err("257th actual Work must exceed the original bound");
    assert!(
        error
            .to_string()
            .contains("exceeds its original execution bound"),
        "wrong refusal for Work boundary: {error}"
    );
    assert!(Box::pin(guard_physical_dispatch(&storage, &extra_work))
        .await
        .is_err());
    mark("work_overflow_refused", 257);

    // Admission must also refuse the genuine 257-node parent chain. Reuse its
    // actual original Run; do not build another chain or extend the deadline.
    let error = Box::pin(successor(&storage, &extra_work, &extra_run, 257))
        .await
        .expect_err("a successor of the actual 257-Work chain must not be admitted");
    assert!(
        error
            .to_string()
            .contains("work_producer_origin_changed_or_unknown"),
        "wrong refusal for original producer observation depth: {error}"
    );
    let key = elephant_storage::canonical_commit::source_support_fingerprint(&serde_json::json!({
        "format": "work-producer-child-key/v1",
        "space_id": extra_work.base.space_id,
        "parent_work_item_id": extra_work.base.id,
        "original_parent_work_run_id": extra_run.base.id,
        "kind": "extract_assertions",
        "semantic_key": "independent-original-successor-257",
    }))?;
    let attempted_id = RecordId::new(
        TableName::WorkItem,
        uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, key.as_bytes()),
    );
    assert!(elephant_storage::WorkItemRepository::new(storage.clone())
        .get(&space, &attempted_id)
        .await?
        .is_none());
    if let Some(store) = storage.embedded_store() {
        let origin_key = format!("work_producer_origin:{}", attempted_id.id());
        assert!(
            store
                .mutate(|tx| {
                    Ok(!tx
                        .scan_extension_rows("work_producer_origin")?
                        .iter()
                        .any(|(key, _)| key == &origin_key))
                })
                .await?
        );
    } else {
        let origin_id = surrealdb::sql::Thing::from((
            "work_producer_origin",
            surrealdb::sql::Id::String(attempted_id.id().to_string()),
        ));
        let mut response = storage
            .query_bind_checked(
                "RETURN array::len((SELECT VALUE id FROM $origin));",
                elephant_storage::bindings! {"origin" => origin_id},
            )
            .await?;
        let count: Option<u64> = response.take(0)?;
        assert_eq!(count, Some(0), "no private origin may survive refusal");
    }
    mark("producer_observation_overflow_refused", 257);

    // A fresh dispatch must reread original authority even after the exact
    // 256-node chain previously passed; no cached completed branch grants it.
    let mut revoked = grant.clone();
    revoked.active = false;
    mark("grant_revocation_start", 256);
    SpaceGrantRepository::new(storage.clone())
        .replace(revoked, Some(&grant.generation))
        .await?;
    assert!(Box::pin(guard_physical_dispatch(&storage, &current))
        .await
        .is_err());
    mark("grant_revocation_refused", 256);
    Ok(())
}

#[expect(
    clippy::expect_used,
    reason = "Unexpected acceptance must fail the original active-ancestor refusal assertion without changing the traversal."
)]
async fn failed_branch_and_active_path(embedded: bool) -> TestResult {
    let (storage, _, root, original_run, _) =
        Box::pin(super::standing_dispatch_tests::fixture(embedded)).await;
    let (child, _) = Box::pin(successor(&storage, &root, &original_run, 1)).await?;
    Box::pin(guard_physical_dispatch(&storage, &child)).await?;

    let runs = WorkRunRepository::new(storage.clone());
    let mut corrupted = original_run.clone();
    assert!(corrupted.metrics.custom.remove("attempt_fence").is_some());
    runs.update(&root.base.space_id, &original_run.base.id, corrupted)
        .await?;
    let mut failed_walk = ProducerWalk::default();
    assert!(Box::pin(guard_physical_dispatch_in_walk(
        &storage,
        &child,
        &mut failed_walk
    ))
    .await
    .is_err());
    // Only exact original evidence is restored. A failed branch must be retried
    // and verified, never recorded as successful or left permanently active.
    runs.update(
        &root.base.space_id,
        &original_run.base.id,
        original_run.clone(),
    )
    .await?;
    Box::pin(guard_physical_dispatch_in_walk(
        &storage,
        &child,
        &mut failed_walk,
    ))
    .await?;
    Box::pin(guard_physical_dispatch(&storage, &child)).await?;

    let mut active_walk = ProducerWalk::default();
    let root_key = crate::maintenance_commit::request_fingerprint(&(
        "work",
        &root.base.space_id,
        &root.base.id,
    ))?;
    let entered = active_walk
        .enter(ProducerNode::Work, root_key)?
        .ok_or_else(|| std::io::Error::other("fresh original root unexpectedly complete"))?;
    // This is an explicitly active traversal ancestor, not a forged cyclic
    // producer record. The actual child traversal must detect its re-entry.
    let error = Box::pin(guard_physical_dispatch_in_walk(
        &storage,
        &child,
        &mut active_walk,
    ))
    .await
    .expect_err("actual traversal must reject an already active original ancestor");
    assert!(
        error.to_string().contains("cyclic"),
        "wrong active-path refusal: {error}"
    );
    active_walk.leave(&entered, false);
    Box::pin(guard_physical_dispatch_in_walk(
        &storage,
        &child,
        &mut active_walk,
    ))
    .await?;
    Ok(())
}

#[tokio::test]
async fn embedded_actual_256_work_and_257_source_boundaries() -> TestResult {
    tokio::time::timeout(
        Duration::from_secs(240),
        Box::pin(original_boundaries(true)),
    )
    .await?
}

#[tokio::test]
async fn native_actual_256_work_and_257_source_boundaries() -> TestResult {
    tokio::time::timeout(
        Duration::from_secs(240),
        Box::pin(original_boundaries(false)),
    )
    .await?
}

#[tokio::test]
async fn embedded_original_failed_branch_and_active_path_are_distinct() -> TestResult {
    tokio::time::timeout(
        Duration::from_secs(60),
        Box::pin(failed_branch_and_active_path(true)),
    )
    .await?
}

#[tokio::test]
async fn native_original_failed_branch_and_active_path_are_distinct() -> TestResult {
    tokio::time::timeout(
        Duration::from_secs(60),
        Box::pin(failed_branch_and_active_path(false)),
    )
    .await?
}
