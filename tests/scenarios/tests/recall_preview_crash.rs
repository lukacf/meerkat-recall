//! Actual SIGKILL/reopen gates, distinct from same-process transaction faults.
//! ElephantDB persistent destination only; no claim about remote Surreal server crashes.
#![cfg(unix)]

#[path = "../../recall_preview/crash_support.rs"]
mod crash_support;
#[path = "../../recall_preview/graph_support.rs"]
mod graph_support;
#[path = "../../recall_preview/outbox_support.rs"]
mod outbox_support;
#[path = "../../recall_preview/publication_support.rs"]
mod publication_support;
#[path = "../../recall_preview/support.rs"]
mod support;

use crash_support::SPACE;
use elephant_pipeline::scheduler::{work_queue::WorkQueueConfig, WorkQueue};
use elephant_storage::canonical_commit::CanonicalRepository;
use elephant_storage::source_lifecycle::ErasureFault;
use elephant_storage::source_publication::{
    AcceptSourceRevision, SourceMutation, SourcePublicationRepository,
};
use elephant_storage::work_attempt_repository::fence_from_claim;
use elephant_storage::{
    EntityRepository, OutboxCheckpointRepository, Repository, SourceRepository,
    SpaceConfigRepository, StorageClient, WorkItemRepository,
};
use elephant_types::publication::{
    OperationReceipt, OperationState, SourcePrecondition, SourceVisibility,
};
use elephant_types::records::base::BaseFields;
use elephant_types::records::entity::{Entity, EntityType};
use elephant_types::records::work::{WorkItem, WorkItemStatus};
use elephant_types::records::{
    Assertion, Doc, DocNode, DocRevision, KnowledgeView, KnowledgeViewRevision, ManualDocTemplate,
    NodeEmbedding, Rel, Source, SourceType, SpaceConfig, SystemEvent, WorkArtifact,
    WorkItemArchive, WorkRun, WorkRunArchive,
};
use elephant_types::{RecordId, TableName};
use serde_json::{json, Value};

async fn accepted_source(client: &StorageClient) -> anyhow::Result<(Source, OperationReceipt)> {
    let mut config = serde_json::to_value(BaseFields::new(
        RecordId::new_v4(TableName::SpaceConfig),
        SPACE.into(),
        "qualification".into(),
    ))?;
    config["embedding_provider"] = json!("qualification-no-dispatch");
    config["embedding_model"] = json!("qualification-no-dispatch");
    config["embedding_dims"] = json!(3);
    config["processing"] = Value::Null;
    SpaceConfigRepository::new(client.clone())
        .create(serde_json::from_value::<SpaceConfig>(config)?)
        .await?;
    let mut source = Source::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Source),
            SPACE.into(),
            "qualification".into(),
        ),
        SourceType::Manual,
        support::digest(support::TEXT),
    );
    source.external_id = Some("acknowledged-source".into());
    source.set_manual_doc_template(&ManualDocTemplate {
        doc_id: RecordId::new_v4(TableName::Doc).to_string(),
        title: "Acknowledged immutable input".into(),
        text: support::TEXT.into(),
        sidecar_text: None,
        media_attachments: vec![],
        rendition_key: "acknowledged-text".into(),
        metadata: json!({}),
    })?;
    let receipt = SourcePublicationRepository::new(client.clone())
        .accept(AcceptSourceRevision {
            source: source.clone(),
            revision_id: "accepted-r1".into(),
            expected: SourcePrecondition::Absent,
            idempotency_key: "accepted-before-kill".into(),
            request_fingerprint: support::digest(support::TEXT),
            reactivate: false,
            recreate_after_erasure: false,
            authorized: None,
            authorized_document: None,
        })
        .await?;
    Ok((source, receipt))
}

fn ordered<T: serde::Serialize>(rows: Vec<T>) -> anyhow::Result<Value> {
    let mut rows = rows
        .into_iter()
        .map(serde_json::to_value)
        .collect::<Result<Vec<_>, _>>()?;
    rows.sort_by_cached_key(Value::to_string);
    Ok(Value::Array(rows))
}

async fn graph_observation(client: &StorageClient) -> anyhow::Result<Value> {
    let store = client
        .embedded_store()
        .ok_or_else(|| anyhow::anyhow!("crash oracle requires physical embedded inventory"))?;
    Ok(json!({
        "entities": ordered(store.scan::<Entity>().await?)?,
        "assertions": ordered(store.scan::<Assertion>().await?)?,
        "relationships": ordered(store.scan::<Rel>().await?)?,
        "artifacts": ordered(store.scan::<WorkArtifact>().await?)?,
        "documents": ordered(store.scan::<Doc>().await?)?,
        "revisions": ordered(store.scan::<DocRevision>().await?)?,
        "nodes": ordered(store.scan::<DocNode>().await?)?,
        "embeddings": ordered(store.scan::<NodeEmbedding>().await?)?,
        "work_items": ordered(store.scan::<WorkItem>().await?)?,
        "work_runs": ordered(store.scan::<WorkRun>().await?)?,
        "archived_items": ordered(store.scan::<WorkItemArchive>().await?)?,
        "archived_runs": ordered(store.scan::<WorkRunArchive>().await?)?,
        "views": ordered(store.scan::<KnowledgeView>().await?)?,
        "view_revisions": ordered(store.scan::<KnowledgeViewRevision>().await?)?,
        "outbox": ordered(store.scan::<SystemEvent>().await?)?,
    }))
}

async fn child_checkpoint(mode: &str, directory: &std::path::Path) -> anyhow::Result<()> {
    let client = crash_support::open(directory).await?;
    let observation = if mode == "accepted" || mode == "claimed" {
        let (source, receipt) = accepted_source(&client).await?;
        let claimed = if mode == "claimed" {
            let queue = WorkQueue::new(
                client.clone(),
                WorkQueueConfig {
                    lease_duration_secs: 2,
                    worker_id: "crashed-worker".into(),
                    ..WorkQueueConfig::default()
                },
            );
            let item = WorkItem {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::WorkItem),
                    SPACE.into(),
                    "qualification".into(),
                ),
                kind: "triage_source".into(),
                payload: json!({"source_id": source.base.id}),
                status: WorkItemStatus::Runnable,
                priority: 0,
                idempotency_key: "durable-claim".into(),
                attempts: 0,
                lease_expires_at: None,
                leased_by: None,
                run_after: None,
                last_error: None,
            };
            queue.enqueue(item).await?;
            Some(
                queue
                    .claim()
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("new work not claimable"))?,
            )
        } else {
            None
        };
        json!({"mode":mode,"source":source,"operation":receipt,"claimed":claimed})
    } else if mode == "outbox-failed" || mode == "outbox-complete" {
        let event = outbox_support::seed(&client, SPACE).await?;
        let result = outbox_support::deliver(&client, SPACE, mode == "outbox-failed").await;
        if mode == "outbox-failed" {
            let failure = result
                .err()
                .ok_or_else(|| anyhow::anyhow!("outbox failure swallowed"))?;
            anyhow::ensure!(failure
                .to_string()
                .contains("qualification failure after durable queue effect"));
        } else {
            anyhow::ensure!(result? == 1);
        }
        json!({"mode":mode,"event":event})
    } else if mode == "restored" {
        use elephant_storage::snapshot::{export_named, restore_named, SnapshotRetention};
        accepted_source(&client).await?;
        let entities = EntityRepository::new(client.clone());
        let original = entities
            .create(Entity::new(
                BaseFields::new(
                    RecordId::new_v4(TableName::Entity),
                    SPACE.into(),
                    "qualification".into(),
                ),
                EntityType::new("topic"),
                "Original backup authority".into(),
            ))
            .await?;
        let backup = directory.join("registered-backup");
        export_named(
            &client,
            &backup,
            "before-later-wal",
            &SnapshotRetention::default(),
        )
        .await?;
        let mut changed = original.clone();
        changed.display_name = "Must not replay old WAL after restore".into();
        entities.update(SPACE, &original.base.id, changed).await?;
        let later = entities
            .create(Entity::new(
                BaseFields::new(
                    RecordId::new_v4(TableName::Entity),
                    SPACE.into(),
                    "qualification".into(),
                ),
                EntityType::new("topic"),
                "Later WAL row must disappear".into(),
            ))
            .await?;
        restore_named(&client, &backup, client.erasure_journal()?).await?;
        anyhow::ensure!(entities.get(SPACE, &original.base.id).await? == Some(original.clone()));
        anyhow::ensure!(entities.get(SPACE, &later.base.id).await?.is_none());
        json!({"mode":mode,"original":original,"later_id":later.base.id})
    } else {
        let fixture = graph_support::GraphFixture::new(client.clone(), SPACE).await?;
        fixture.index_fixture_vectors().await?;
        let committed = fixture.publication.finalize().await?;
        if mode == "published" {
            json!({"mode":mode,"source":fixture.lineage.source,"operation":fixture.publication.accepted,
                "receipt":committed,"graph":graph_observation(&client).await?})
        } else {
            let sentinel = EntityRepository::new(client.clone())
                .create(Entity::new(
                    BaseFields::new(
                        RecordId::new_v4(TableName::Entity),
                        SPACE.into(),
                        "qualification".into(),
                    ),
                    EntityType::new("topic"),
                    "Independent source-less authority".into(),
                ))
                .await?;
            let repository = SourcePublicationRepository::new(client.clone());
            let authorized = support::authorized_source_snapshot(
                &repository,
                SPACE,
                &fixture.lineage.source.base.id,
            )
            .await?;
            let mutation = SourceMutation {
                authorized_document: None,
                space_id: SPACE.into(),
                source_id: fixture.lineage.source.base.id.clone(),
                expected: SourcePrecondition::Generation(
                    fixture.publication.accepted.source_generation.clone(),
                ),
                idempotency_key: "erase-before-kill".into(),
                request_fingerprint: "erase-qualification-v1".into(),
                actor: "qualification".into(),
                authorized,
            };
            let fault = match mode {
                "erase-reserved" => ErasureFault::AfterReservation,
                "erase-journaled" => ErasureFault::AfterJournal,
                "erase-admission-reserved" => ErasureFault::AfterAdmissionReservation,
                "erase-reference-fsynced" => ErasureFault::AfterReferenceMembership,
                "erase-in-purge" => ErasureFault::DuringPurge,
                "erase-purged" => ErasureFault::AfterPurge,
                "erase-persisted" => ErasureFault::AfterPersistence,
                "erase-complete" => ErasureFault::None,
                _ => anyhow::bail!("unknown crash scenario {mode}"),
            };
            let result = repository.erase_live_with_fault(mutation, fault).await;
            if fault == ErasureFault::None {
                anyhow::ensure!(result?.state == OperationState::Erased);
            } else {
                let error = result
                    .err()
                    .ok_or_else(|| anyhow::anyhow!("erasure fault not reached"))?;
                anyhow::ensure!(
                    error.to_string().contains("injected"),
                    "unrelated erasure failure: {error}"
                );
                anyhow::ensure!(
                    !client.recovery_status().ready,
                    "interrupted erasure remained ready"
                );
            }
            let intents = client.erasure_journal()?.intents()?;
            anyhow::ensure!(
                intents
                    .iter()
                    .any(|i| i.source_id == fixture.lineage.source.base.id)
                    == (mode != "erase-reserved"),
                "journal ordering differs from the tested boundary"
            );
            json!({"mode":mode,"source":fixture.lineage.source,"sentinel":sentinel})
        }
    };
    crash_support::acknowledge(directory, &observation)?;
    // Keep the active client alive. No snapshot/export/drop/shutdown is called.
    loop {
        std::hint::black_box(&client);
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    }
}

/// Infrastructure entry point; no fixture work occurs unless the parent supplied mode+root.
#[test]
fn crash_fixture_child() -> anyhow::Result<()> {
    let Ok(mode) = std::env::var(crash_support::CHILD_MODE) else {
        return Ok(());
    };
    let root = std::env::var_os(crash_support::CHILD_ROOT)
        .ok_or_else(|| anyhow::anyhow!("child root absent"))?;
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(child_checkpoint(&mode, std::path::Path::new(&root)))
}

#[tokio::test]
async fn acknowledged_source_acceptance_survives_sigkill_without_snapshot() -> anyhow::Result<()> {
    let directory = crash_support::CrashDirectory::new()?;
    let ack = crash_support::acknowledge_then_sigkill(&directory.0, "accepted")?;
    let client = crash_support::open(&directory.0).await?;
    let repository = SourcePublicationRepository::new(client.clone());
    let source: Source = serde_json::from_value(ack["source"].clone())?;
    let accepted: OperationReceipt = serde_json::from_value(ack["operation"].clone())?;
    anyhow::ensure!(client.recovery_status().ready);
    anyhow::ensure!(
        repository.operation(SPACE, &accepted.operation_id).await? == Some(accepted.clone()),
        "acknowledged operation vanished across SIGKILL"
    );
    let revision = repository
        .revision(SPACE, &source.base.id, "accepted-r1")
        .await?
        .ok_or_else(|| anyhow::anyhow!("acknowledged immutable input vanished across SIGKILL"))?;
    anyhow::ensure!(
        revision.source.hash == source.hash
            && revision
                .source
                .manual_doc_template()?
                .is_some_and(|template| template.text == support::TEXT)
    );
    let head = repository
        .head(SPACE, &source.base.id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("accepted head lost"))?;
    anyhow::ensure!(
        head.generation == accepted.source_generation
            && head.desired_revision.as_deref() == Some("accepted-r1")
    );
    Ok(())
}

#[tokio::test]
async fn acknowledged_queue_claim_survives_sigkill_and_reclaim_advances_attempt(
) -> anyhow::Result<()> {
    let directory = crash_support::CrashDirectory::new()?;
    let ack = crash_support::acknowledge_then_sigkill(&directory.0, "claimed")?;
    let client = crash_support::open(&directory.0).await?;
    let original: WorkItem = serde_json::from_value(ack["claimed"].clone())?;
    let retained = WorkItemRepository::new(client.clone())
        .get(SPACE, &original.base.id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("acknowledged queue item vanished across SIGKILL"))?;
    let first = fence_from_claim(&original)?;
    anyhow::ensure!(
        fence_from_claim(&retained)? == first && retained.status == WorkItemStatus::Running
    );
    if let Some(expiry) = retained.lease_expires_at {
        if let Ok(remaining) = expiry.signed_duration_since(chrono::Utc::now()).to_std() {
            tokio::time::sleep(remaining + std::time::Duration::from_millis(25)).await;
        }
    }
    let queue = WorkQueue::new(
        client.clone(),
        WorkQueueConfig {
            worker_id: "replacement-worker".into(),
            ..WorkQueueConfig::default()
        },
    );
    let replacement = queue
        .claim()
        .await?
        .ok_or_else(|| anyhow::anyhow!("quiet persisted queue failed to recover"))?;
    let second = fence_from_claim(&replacement)?;
    anyhow::ensure!(
        replacement.base.id == original.base.id
            && second.generation == first.generation + 1
            && second.lease_token != first.lease_token
    );
    anyhow::ensure!(
        queue
            .for_attempt(&original)?
            .complete(SPACE, &original.base.id)
            .await
            .is_err(),
        "pre-crash attempt completed replacement work"
    );
    queue
        .for_attempt(&replacement)?
        .complete(SPACE, &replacement.base.id)
        .await?;
    Ok(())
}

#[tokio::test]
async fn acknowledged_whole_source_publication_survives_sigkill_with_exact_effects_and_outbox(
) -> anyhow::Result<()> {
    let directory = crash_support::CrashDirectory::new()?;
    let ack = crash_support::acknowledge_then_sigkill(&directory.0, "published")?;
    let client = crash_support::open(&directory.0).await?;
    anyhow::ensure!(
        graph_observation(&client).await? == ack["graph"],
        "durable graph contents/evidence/artifacts/outbox differ from acknowledged publication"
    );
    let commit_id = ack["receipt"]["commit_id"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("receipt identity missing"))?;
    let receipt = CanonicalRepository::new(client.clone())
        .receipt(SPACE, commit_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("durable canonical receipt absent"))?;
    anyhow::ensure!(serde_json::to_value(receipt)? == ack["receipt"]);
    let source: Source = serde_json::from_value(ack["source"].clone())?;
    let head = SourcePublicationRepository::new(client.clone())
        .head(SPACE, &source.base.id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("durable published head absent"))?;
    anyhow::ensure!(head.published_revision.as_deref() == Some("oriole-revision-1"));
    let completed = SourcePublicationRepository::new(client)
        .operation(SPACE, &head.operation_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("durable completed operation absent"))?;
    anyhow::ensure!(completed.state == OperationState::Completed);
    Ok(())
}

#[tokio::test]
async fn every_erasure_boundary_recovers_before_ready_after_real_sigkill() -> anyhow::Result<()> {
    for mode in [
        "erase-reserved",
        "erase-journaled",
        "erase-admission-reserved",
        "erase-reference-fsynced",
        "erase-in-purge",
        "erase-purged",
        "erase-persisted",
        "erase-complete",
    ] {
        let directory = crash_support::CrashDirectory::new()?;
        let ack = crash_support::acknowledge_then_sigkill(&directory.0, mode)?;
        if mode == "erase-complete" {
            crash_support::assert_no_live_payload(&directory.0, b"opened the Oriole laboratory")?;
        }
        let client = crash_support::open(&directory.0).await?;
        anyhow::ensure!(
            client.recovery_status().ready,
            "{mode}: startup did not finish replay"
        );
        let source: Source = serde_json::from_value(ack["source"].clone())?;
        let sentinel: Entity = serde_json::from_value(ack["sentinel"].clone())?;
        let publications = SourcePublicationRepository::new(client.clone());
        let head = publications
            .head(SPACE, &source.base.id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("{mode}: durable erasure reservation vanished"))?;
        anyhow::ensure!(
            head.lifecycle == SourceVisibility::Erased && head.published_revision.is_none(),
            "{mode}: ready store retained erased visibility"
        );
        anyhow::ensure!(SourceRepository::new(client.clone())
            .get_with_deleted(SPACE, &source.base.id)
            .await?
            .is_none());
        anyhow::ensure!(publications
            .revision(SPACE, &source.base.id, "oriole-revision-1")
            .await?
            .is_none());
        let operation = publications
            .operation(SPACE, &head.operation_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("erasure operation absent"))?;
        anyhow::ensure!(
            operation.state == OperationState::Erased && operation.completed_at.is_some()
        );
        anyhow::ensure!(client
            .erasure_journal()?
            .intents()?
            .iter()
            .any(|i| i.source_id == source.base.id));
        anyhow::ensure!(
            EntityRepository::new(client.clone())
                .get(SPACE, &sentinel.base.id)
                .await?
                == Some(sentinel)
        );
        let graph = graph_observation(&client).await?;
        anyhow::ensure!(graph["entities"]
            .as_array()
            .is_some_and(|rows| rows.len() == 1));
        for table in [
            "assertions",
            "relationships",
            "artifacts",
            "documents",
            "revisions",
            "nodes",
            "embeddings",
            "work_items",
            "work_runs",
            "archived_items",
            "archived_runs",
            "views",
            "view_revisions",
        ] {
            anyhow::ensure!(
                graph[table] == json!([]),
                "{mode}: residual exclusive source payload in {table}"
            );
        }
        anyhow::ensure!(
            !serde_json::to_string(&graph)?.contains(support::TEXT),
            "{mode}: erased quote retained in outbox/view"
        );
        crash_support::assert_no_live_payload(&directory.0, b"opened the Oriole laboratory")?;
        // Ready must also mean that recovery released its shared write
        // reservation. A stale reservation cannot make every negative pass.
        let entities = EntityRepository::new(client.clone());
        entities
            .create(Entity::new(
                BaseFields::new(
                    RecordId::new_v4(TableName::Entity),
                    SPACE.into(),
                    "qualification".into(),
                ),
                EntityType::new("topic"),
                "Independent write after recovered erasure".into(),
            ))
            .await?;
        let mut late = Entity::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Entity),
                SPACE.into(),
                "qualification".into(),
            ),
            EntityType::new("topic"),
            "Late source-reference-only result".into(),
        );
        late.metadata = json!({"input":source.base.id});
        let late_id = late.base.id.clone();
        let error = entities
            .create(late)
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("{mode}: recovered store forgot erasure fence"))?;
        anyhow::ensure!(
            error.to_string().contains("source_erased"),
            "{mode}: wrong failure instead of recovered erasure fence: {error}"
        );
        anyhow::ensure!(entities.get_with_deleted(SPACE, &late_id).await?.is_none());
    }
    Ok(())
}

#[tokio::test]
async fn restored_generation_cannot_replay_pre_restore_wal_after_sigkill() -> anyhow::Result<()> {
    let directory = crash_support::CrashDirectory::new()?;
    let ack = crash_support::acknowledge_then_sigkill(&directory.0, "restored")?;
    let client = crash_support::open(&directory.0).await?;
    anyhow::ensure!(client.recovery_status().ready);
    let original: Entity = serde_json::from_value(ack["original"].clone())?;
    let later: RecordId = serde_json::from_value(ack["later_id"].clone())?;
    let entities = EntityRepository::new(client);
    anyhow::ensure!(
        entities.get(SPACE, &original.base.id).await? == Some(original),
        "reopen reapplied pre-restore modification"
    );
    anyhow::ensure!(
        entities.get(SPACE, &later).await?.is_none(),
        "reopen resurrected pre-restore WAL insertion"
    );
    Ok(())
}

#[tokio::test]
async fn failed_and_successful_outbox_acknowledgments_recover_truthfully_after_sigkill(
) -> anyhow::Result<()> {
    for mode in ["outbox-failed", "outbox-complete"] {
        let directory = crash_support::CrashDirectory::new()?;
        let ack = crash_support::acknowledge_then_sigkill(&directory.0, mode)?;
        let client = crash_support::open(&directory.0).await?;
        let event: SystemEvent = serde_json::from_value(ack["event"].clone())?;
        let checkpoints = OutboxCheckpointRepository::new(client.clone());
        let expected = if mode == "outbox-failed" {
            0
        } else {
            event.seq
        };
        anyhow::ensure!(checkpoints.load(SPACE, outbox_support::CONSUMER).await? == expected);
        anyhow::ensure!(
            WorkItemRepository::new(client.clone())
                .list(SPACE, 100, None)
                .await?
                .len()
                == 1,
            "acknowledged effect vanished before retry"
        );
        anyhow::ensure!(
            outbox_support::deliver(&client, SPACE, false).await?
                == usize::from(mode == "outbox-failed")
        );
        anyhow::ensure!(checkpoints.load(SPACE, outbox_support::CONSUMER).await? == event.seq);
        anyhow::ensure!(
            WorkItemRepository::new(client)
                .list(SPACE, 100, None)
                .await?
                .len()
                == 1,
            "durable effect duplicated across process retry"
        );
    }
    Ok(())
}
