//! Independent writer controls: original emitted sequences survive real retirement.
//! No private floor helper supplies an expected value or replaces an actual writer.
use std::time::Duration;

use anyhow::{ensure, Context};
use chrono::Utc;
use elephant_outbox::{OutboxPayload, OutboxWriter};
use elephant_pipeline::retention::RetentionWorker;
use elephant_storage::canonical_commit::CanonicalRepository;
use elephant_storage::{
    EntityRepository, KnowledgeViewRepository, OutboxCheckpointRepository, Repository,
    SpaceConfigRepository, StorageClient, SystemEventRepository,
};
use elephant_types::records::base::BaseFields;
use elephant_types::records::entity::EntityType;
use elephant_types::records::{
    CitationKind, CompilerReceipt, Entity, HasSecurity, KnowledgeCitation, KnowledgeDependency,
    KnowledgeView, KnowledgeViewKind, KnowledgeViewPublicationStatus, KnowledgeViewRevision,
    KnowledgeViewSurface, ProjectionFreshness, ProjectionState, SpaceConfig, SystemEvent,
    ValidationReceipt,
};
use elephant_types::{RecordId, TableName};

use super::spy::{self, Backend};

const SPACE: &str = "original-event-floor-household";
const OTHER_SPACE: &str = "original-event-floor-business";
const CONSUMER: &str = "independent-original-consumer";

async fn fixture(backend: Backend) -> anyhow::Result<StorageClient> {
    let storage = spy::storage(backend).await?;
    for space in [SPACE, OTHER_SPACE] {
        let config: SpaceConfig = serde_json::from_value(serde_json::json!({
            "id": RecordId::new_v4(TableName::SpaceConfig), "space_id": space,
            "created_at": Utc::now(), "updated_at": Utc::now(), "created_by": "sequence-control",
            "security": {"policy_id":"default", "level":0, "labels":[], "handling":[]},
            "provenance": {}, "subjects": [], "subjects_state": "complete",
            "embedding_provider":"fake", "embedding_model":"unused", "embedding_dims":8
        }))?;
        SpaceConfigRepository::new(storage.clone())
            .create(config)
            .await?;
    }
    Ok(storage)
}

fn payload(space: &str, label: &str) -> OutboxPayload {
    OutboxPayload::new(space.into(), label.into(), "sequence-control".into())
}

async fn events(storage: &StorageClient, space: &str) -> anyhow::Result<Vec<SystemEvent>> {
    Ok(SystemEventRepository::new(storage.clone())
        .get_after_seq(space, 0, 100)
        .await?)
}

async fn emit(
    storage: &StorageClient,
    space: &str,
    count: usize,
) -> anyhow::Result<Vec<SystemEvent>> {
    let writer = OutboxWriter::new(storage.clone());
    let mut original = Vec::new();
    let mut previous = events(storage, space)
        .await?
        .last()
        .map_or(0, |event| event.seq);
    for index in 0..count {
        let event = writer
            .write(payload(space, &format!("original.{index}")))
            .await?;
        ensure!(
            event.seq == previous + 1,
            "actual writer skipped original sequence"
        );
        previous = event.seq;
        original.push(event);
    }
    Ok(original)
}

async fn age(storage: &StorageClient, event: &SystemEvent) -> anyhow::Result<SystemEvent> {
    let mut old = event.clone();
    old.base.created_at = Utc::now() - chrono::Duration::days(8);
    Ok(SystemEventRepository::new(storage.clone())
        .update(&event.base.space_id, &event.base.id, old)
        .await?)
}

async fn cleanup(storage: &StorageClient, expected: usize) -> anyhow::Result<()> {
    let result = RetentionWorker::new(storage.clone(), Duration::from_secs(7 * 24 * 60 * 60))
        .process_cleanup()
        .await?;
    ensure!(
        result.total_deleted == expected,
        "actual retention removed an unexpected population"
    );
    Ok(())
}

async fn require_absent(storage: &StorageClient, originals: &[SystemEvent]) -> anyhow::Result<()> {
    let repo = SystemEventRepository::new(storage.clone());
    for original in originals {
        ensure!(
            repo.get_with_deleted(&original.base.space_id, &original.base.id)
                .await?
                .is_none(),
            "retention must physically remove original event, not just hide it"
        );
    }
    Ok(())
}

/// Retire earlier original rows, lower the remaining tail through the actual
/// repository, then retire that row too. Neither action may rewind the writer.
async fn retire_and_lower(storage: &StorageClient) -> anyhow::Result<i64> {
    let original = emit(storage, SPACE, 3).await?;
    let tail = original.last().context("three original events")?.seq;
    OutboxCheckpointRepository::new(storage.clone())
        .advance(SPACE, CONSUMER, 0, tail)
        .await?;
    for event in &original[..2] {
        age(storage, event).await?;
    }
    cleanup(storage, 2).await?;
    require_absent(storage, &original[..2]).await?;
    let mut lowered = original[2].clone();
    lowered.seq = original[0].seq;
    let lowered = SystemEventRepository::new(storage.clone())
        .update(SPACE, &original[2].base.id, lowered)
        .await?;
    ensure!(lowered.seq == original[0].seq && lowered.seq < tail);
    age(storage, &lowered).await?;
    cleanup(storage, 1).await?;
    require_absent(storage, &original).await?;
    ensure!(
        OutboxCheckpointRepository::new(storage.clone())
            .load(SPACE, CONSUMER)
            .await?
            == tail,
        "producer retirement changed the original consumer acknowledgement"
    );
    Ok(tail)
}

async fn writer_after_retirement(backend: Backend) -> anyhow::Result<()> {
    let storage = fixture(backend).await?;
    let other = emit(&storage, OTHER_SPACE, 7).await?;
    let other_before = events(&storage, OTHER_SPACE).await?;
    let tail = retire_and_lower(&storage).await?;
    let next = OutboxWriter::new(storage.clone())
        .write(payload(SPACE, "after.retention"))
        .await?;
    ensure!(
        next.seq == tail + 1,
        "retired original tail was reused or foreign floor leaked"
    );
    ensure!(events(&storage, OTHER_SPACE).await? == other_before);
    let foreign = OutboxWriter::new(storage.clone())
        .write(payload(OTHER_SPACE, "foreign.next"))
        .await?;
    ensure!(foreign.seq == other.last().context("foreign originals")?.seq + 1);
    // Concurrent real writes must serialize on the same space's producer floor.
    let left = OutboxWriter::new(storage.clone());
    let right = OutboxWriter::new(storage.clone());
    let (left, right) = tokio::join!(
        left.write(payload(SPACE, "concurrent.left")),
        right.write(payload(SPACE, "concurrent.right")),
    );
    let (left, right) = (left?, right?);
    let mut sequences = [left.seq, right.seq];
    sequences.sort();
    ensure!(sequences == [tail + 2, tail + 3]);
    ensure!(left.base.id != right.base.id);
    let checkpoints = OutboxCheckpointRepository::new(storage.clone());
    ensure!(checkpoints.load(SPACE, CONSUMER).await? == tail);
    ensure!(checkpoints.load(OTHER_SPACE, CONSUMER).await? == 0);
    ensure!(
        events(&storage, SPACE)
            .await?
            .iter()
            .filter(|event| event.seq > tail)
            .count()
            == 3
    );
    Ok(())
}

type Publication = (KnowledgeView, KnowledgeViewRevision);

async fn publication(storage: &StorageClient, label: &str) -> anyhow::Result<Publication> {
    let root = Entity::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Entity),
            SPACE.into(),
            "sequence-control".into(),
        ),
        EntityType::new("project"),
        format!("Independent sequence project {label}"),
    );
    EntityRepository::new(storage.clone())
        .create(root.clone())
        .await?;
    let now = Utc::now();
    let view_id = RecordId::new_v4(TableName::KnowledgeView);
    let revision_id = RecordId::new_v4(TableName::KnowledgeViewRevision);
    let mut view = KnowledgeView {
        base: BaseFields::new(view_id.clone(), SPACE.into(), "sequence-control".into()),
        key: format!("sequence:{label}:{}", root.base.id),
        kind: KnowledgeViewKind::Entity,
        surface: KnowledgeViewSurface::Inspector,
        publication_status: KnowledgeViewPublicationStatus::Published,
        root_record: root.base.id.to_string(),
        title: root.display_name.clone(),
        slug: label.into(),
        security_fingerprint: "original-sequence-policy".into(),
        current_revision: Some(revision_id.to_string()),
        revision_count: 1,
        state: ProjectionState::Current,
        state_reason: None,
        published_at: Some(now),
    };
    let mut revision = KnowledgeViewRevision {
        base: BaseFields::new(revision_id, SPACE.into(), "sequence-control".into()),
        view_id: view_id.to_string(),
        revision: 1,
        kind: KnowledgeViewKind::Entity,
        root_record: root.base.id.to_string(),
        blocks: vec![],
        rendered_markdown: root.display_name.clone(),
        citations: vec![KnowledgeCitation {
            id: "original-root".into(),
            kind: CitationKind::GraphRecord,
            record_id: root.base.id.to_string(),
            assertion_id: None,
            source_id: None,
            evidence_span: None,
        }],
        dependencies: vec![KnowledgeDependency {
            record_id: root.base.id.to_string(),
            record_type: "entity".into(),
            role: "root".into(),
            observed_updated_at: Some(root.base.updated_at),
        }],
        freshness: ProjectionFreshness {
            state: ProjectionState::Current,
            minimum_claim_score: None,
            stale_truth_slots: 0,
            unresolved_conflicts: 0,
            unsupported_claims: 0,
            expired_claims: 0,
            selection_truncated: false,
            valid_until: None,
        },
        validation: ValidationReceipt {
            citations_resolve: true,
            factual_blocks_cited: true,
            statements_supported: true,
            prose_is_structured: true,
            factual_block_count: 0,
            citation_count: 1,
            statement_count: 0,
            warnings: vec![],
        },
        compiler: CompilerReceipt {
            compiler: "independent-sequence-fixture".into(),
            version: 1,
            dependency_hash: "original-entity".into(),
            policy_fingerprint: "original-sequence-policy".into(),
        },
        compiled_at: now,
        supersedes: None,
    };
    view.base.subjects = root.subjects();
    view.base.subjects_state = root.subjects_state();
    revision.base.subjects = root.subjects();
    revision.base.subjects_state = root.subjects_state();
    Ok((view, revision))
}

async fn require_unpublished(storage: &StorageClient, rows: &[Publication]) -> anyhow::Result<()> {
    let repo = KnowledgeViewRepository::new(storage.clone());
    for (view, revision) in rows {
        ensure!(repo.get_view(SPACE, &view.base.id).await?.is_none());
        ensure!(repo.get_revision(SPACE, &revision.base.id).await?.is_none());
        ensure!(repo
            .list_dependency_indexes(SPACE, &view.base.id)
            .await?
            .is_empty());
    }
    Ok(())
}

async fn require_published(storage: &StorageClient, rows: &[Publication]) -> anyhow::Result<()> {
    let repo = KnowledgeViewRepository::new(storage.clone());
    for (view, revision) in rows {
        ensure!(repo.get_view(SPACE, &view.base.id).await? == Some(view.clone()));
        ensure!(repo.get_revision(SPACE, &revision.base.id).await? == Some(revision.clone()));
        ensure!(
            repo.list_dependency_indexes(SPACE, &view.base.id)
                .await?
                .len()
                == 1
        );
    }
    Ok(())
}

async fn views_after_retirement(backend: Backend) -> anyhow::Result<()> {
    let storage = fixture(backend).await?;
    let first = publication(&storage, "first").await?;
    let second = publication(&storage, "second").await?;
    let stale = publication(&storage, "stale").await?;
    emit(&storage, OTHER_SPACE, 7).await?;
    let foreign = events(&storage, OTHER_SPACE).await?;
    let tail = retire_and_lower(&storage).await?;
    let epoch = CanonicalRepository::new(storage.clone())
        .epoch(SPACE)
        .await?;
    let repo = KnowledgeViewRepository::new(storage.clone());
    let originals = vec![first, second];
    let result = Box::pin(repo.publish_batch_at_epoch(originals.clone(), &epoch, None)).await?;
    ensure!(result == originals);
    require_published(&storage, &originals).await?;
    let emitted = SystemEventRepository::new(storage.clone())
        .get_after_seq(SPACE, tail, 10)
        .await?;
    ensure!(emitted.len() == 2);
    ensure!(emitted.iter().map(|event| event.seq).collect::<Vec<_>>() == [tail + 1, tail + 2]);
    ensure!(emitted
        .iter()
        .all(|event| event.event_type == "knowledge_view.published"));
    let after = CanonicalRepository::new(storage.clone())
        .epoch(SPACE)
        .await?;
    ensure!(after != epoch);
    ensure!(
        Box::pin(repo.publish_batch_at_epoch(vec![stale.clone()], &epoch, None))
            .await
            .is_err(),
        "original preparation epoch must remain fenced after publication"
    );
    require_unpublished(&storage, &[stale]).await?;
    ensure!(
        CanonicalRepository::new(storage.clone())
            .epoch(SPACE)
            .await?
            == after
    );
    ensure!(
        SystemEventRepository::new(storage.clone())
            .get_after_seq(SPACE, tail, 10)
            .await?
            == emitted
    );
    ensure!(events(&storage, OTHER_SPACE).await? == foreign);
    ensure!(
        OutboxCheckpointRepository::new(storage)
            .load(SPACE, CONSUMER)
            .await?
            == tail
    );
    Ok(())
}

async fn overflow_is_atomic(backend: Backend) -> anyhow::Result<()> {
    let storage = fixture(backend).await?;
    let rows = vec![
        publication(&storage, "overflow-first").await?,
        publication(&storage, "overflow-second").await?,
    ];
    let mut maximum = SystemEvent {
        base: BaseFields::new(
            RecordId::new_v4(TableName::SystemEvent),
            SPACE.into(),
            "sequence-control".into(),
        ),
        seq: i64::MAX - 1,
        event_type: "original.restored.maximum".into(),
        payload: serde_json::json!({"original":"restored event"}),
    };
    maximum.base.created_at = Utc::now() - chrono::Duration::days(8);
    let maximum = SystemEventRepository::new(storage.clone())
        .create(maximum)
        .await?;
    cleanup(&storage, 1).await?;
    require_absent(&storage, &[maximum]).await?;
    let epoch = CanonicalRepository::new(storage.clone())
        .epoch(SPACE)
        .await?;
    let original_events = events(&storage, SPACE).await?;
    let foreign = events(&storage, OTHER_SPACE).await?;
    let repo = KnowledgeViewRepository::new(storage.clone());
    ensure!(
        Box::pin(repo.publish_batch_at_epoch(rows.clone(), &epoch, None))
            .await
            .is_err(),
        "two-row publication must refuse when only one sequence remains"
    );
    require_unpublished(&storage, &rows).await?;
    ensure!(
        CanonicalRepository::new(storage.clone())
            .epoch(SPACE)
            .await?
            == epoch
    );
    ensure!(events(&storage, SPACE).await? == original_events);
    // A failed batch cannot consume even its first otherwise valid sequence.
    let last = OutboxWriter::new(storage.clone())
        .write(payload(SPACE, "original.last.available"))
        .await?;
    ensure!(last.seq == i64::MAX);
    let mut lowered = last.clone();
    lowered.seq = 42;
    lowered.base.created_at = Utc::now() - chrono::Duration::days(8);
    SystemEventRepository::new(storage.clone())
        .update(SPACE, &last.base.id, lowered)
        .await?;
    cleanup(&storage, 1).await?;
    require_absent(&storage, &[last]).await?;
    let epoch = CanonicalRepository::new(storage.clone())
        .epoch(SPACE)
        .await?;
    ensure!(OutboxWriter::new(storage.clone())
        .write(payload(SPACE, "overflow.refused"))
        .await
        .is_err());
    ensure!(
        Box::pin(repo.publish_batch_at_epoch(vec![rows[0].clone()], &epoch, None))
            .await
            .is_err()
    );
    require_unpublished(&storage, &rows).await?;
    ensure!(
        CanonicalRepository::new(storage.clone())
            .epoch(SPACE)
            .await?
            == epoch
    );
    ensure!(events(&storage, SPACE).await? == original_events);
    ensure!(events(&storage, OTHER_SPACE).await? == foreign);
    ensure!(
        OutboxCheckpointRepository::new(storage)
            .load(SPACE, CONSUMER)
            .await?
            == 0
    );
    Ok(())
}

async fn install_native_refusal(storage: &StorageClient) -> anyhow::Result<()> {
    storage
        .query_checked(
            "DEFINE EVENT independent_sequence_refusal ON TABLE system_event \
         WHEN $event = 'CREATE' THEN { THROW 'independent_sequence_write_refused'; };",
        )
        .await?;
    Ok(())
}

async fn remove_native_refusal(storage: &StorageClient) -> anyhow::Result<()> {
    storage
        .query_checked("REMOVE EVENT independent_sequence_refusal ON TABLE system_event;")
        .await?;
    Ok(())
}

#[tokio::test]
async fn native_outbox_create_failure_rolls_back_floor_reservation() -> anyhow::Result<()> {
    let storage = fixture(Backend::SurrealMemory).await?;
    let tail = retire_and_lower(&storage).await?;
    let original = events(&storage, SPACE).await?;
    let foreign = events(&storage, OTHER_SPACE).await?;
    install_native_refusal(&storage).await?;
    let result = OutboxWriter::new(storage.clone())
        .write(payload(SPACE, "refused.actual.write"))
        .await;
    let error = result
        .err()
        .context("actual CREATE must hit native refusal")?;
    ensure!(error
        .to_string()
        .contains("independent_sequence_write_refused"));
    ensure!(events(&storage, SPACE).await? == original);
    remove_native_refusal(&storage).await?;
    let next = OutboxWriter::new(storage.clone())
        .write(payload(SPACE, "original.retry"))
        .await?;
    ensure!(
        next.seq == tail + 1,
        "failed native CREATE consumed the reserved sequence"
    );
    ensure!(events(&storage, OTHER_SPACE).await? == foreign);
    ensure!(
        OutboxCheckpointRepository::new(storage)
            .load(SPACE, CONSUMER)
            .await?
            == tail
    );
    Ok(())
}

#[tokio::test]
async fn native_view_event_failure_rolls_back_floor_and_publication() -> anyhow::Result<()> {
    let storage = fixture(Backend::SurrealMemory).await?;
    let rows = vec![
        publication(&storage, "native-fault-first").await?,
        publication(&storage, "native-fault-second").await?,
    ];
    let tail = retire_and_lower(&storage).await?;
    let epoch = CanonicalRepository::new(storage.clone())
        .epoch(SPACE)
        .await?;
    let original = events(&storage, SPACE).await?;
    let foreign = events(&storage, OTHER_SPACE).await?;
    install_native_refusal(&storage).await?;
    let repo = KnowledgeViewRepository::new(storage.clone());
    let result = Box::pin(repo.publish_batch_at_epoch(rows.clone(), &epoch, None)).await;
    let error = result
        .err()
        .context("actual publication event CREATE must hit native refusal")?;
    ensure!(error
        .to_string()
        .contains("independent_sequence_write_refused"));
    require_unpublished(&storage, &rows).await?;
    ensure!(
        CanonicalRepository::new(storage.clone())
            .epoch(SPACE)
            .await?
            == epoch
    );
    ensure!(events(&storage, SPACE).await? == original);
    remove_native_refusal(&storage).await?;
    let published = Box::pin(repo.publish_batch_at_epoch(rows.clone(), &epoch, None)).await?;
    ensure!(published == rows);
    require_published(&storage, &rows).await?;
    let emitted = SystemEventRepository::new(storage.clone())
        .get_after_seq(SPACE, tail, 10)
        .await?;
    ensure!(emitted.len() == 2);
    ensure!(emitted.iter().map(|event| event.seq).collect::<Vec<_>>() == [tail + 1, tail + 2]);
    ensure!(events(&storage, OTHER_SPACE).await? == foreign);
    ensure!(
        OutboxCheckpointRepository::new(storage)
            .load(SPACE, CONSUMER)
            .await?
            == tail
    );
    Ok(())
}

#[tokio::test]
async fn embedded_outbox_retirement_preserves_original_floor() -> anyhow::Result<()> {
    Box::pin(writer_after_retirement(Backend::ElephantDb)).await
}
#[tokio::test]
async fn native_outbox_retirement_preserves_original_floor() -> anyhow::Result<()> {
    Box::pin(writer_after_retirement(Backend::SurrealMemory)).await
}
#[tokio::test]
async fn embedded_knowledge_view_retirement_preserves_original_floor() -> anyhow::Result<()> {
    Box::pin(views_after_retirement(Backend::ElephantDb)).await
}
#[tokio::test]
async fn native_knowledge_view_retirement_preserves_original_floor() -> anyhow::Result<()> {
    Box::pin(views_after_retirement(Backend::SurrealMemory)).await
}
#[tokio::test]
async fn embedded_retired_sequence_overflow_refuses_entire_publication() -> anyhow::Result<()> {
    Box::pin(overflow_is_atomic(Backend::ElephantDb)).await
}
#[tokio::test]
async fn native_retired_sequence_overflow_refuses_entire_publication() -> anyhow::Result<()> {
    Box::pin(overflow_is_atomic(Backend::SurrealMemory)).await
}
