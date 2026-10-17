//! Synthetic persistent transaction/restart controls; no household input or provider.
#![allow(clippy::expect_used)]
use super::*;
use crate::historical_adoption::catchup::{
    HistoricalControlEffect, HistoricalControlImage, PreparedHistoricalEffects,
};
use crate::historical_delta::{runtime_row_fingerprint, HistoricalChangeKind, HistoricalDeltaRow};
use crate::historical_delta_storage::{commit_batch_inner, BatchFault};
use crate::source_publication::{control_thing, control_value, stable_key, write_control_tx};
use crate::transfer_batch::TransferRecord;
use crate::{Repository, StorageBackendKind, StorageConfig};
use elephant_types::records::{BaseFields, Entity};
use elephant_types::{RecordId, TableName};

async fn open(root: &std::path::Path, embedded: bool) -> StorageClient {
    let config = if embedded {
        StorageConfig {
            backend: StorageBackendKind::ElephantDb,
            ..StorageConfig::default()
        }
    } else {
        StorageConfig {
            url: format!("rocksdb://{}", root.join("database").display()),
            ..StorageConfig::default()
        }
    };
    StorageClient::new_with_durability(
        config,
        embedded.then(|| root.join("live.snapshot")),
        Some(root.join("journal")),
    )
    .await
    .expect("persistent client")
}

async fn install(client: &StorageClient) {
    if client.embedded_store().is_none() {
        for sql in [
            include_str!("../../migrations/sql/028_work_queue_indexes.surql"),
            include_str!("../../migrations/sql/029_work_history_archives.surql"),
            include_str!("../../migrations/sql/031_source_publication.surql"),
            include_str!("../../migrations/sql/033_space_processing.surql"),
            include_str!("../../migrations/sql/035_source_publication_manifest.surql"),
            include_str!("../../migrations/sql/037_source_publication_manifest_history.surql"),
            include_str!("../../migrations/sql/038_source_index_visibility.surql"),
            include_str!("../../migrations/sql/039_private_reference_fence.surql"),
            include_str!("../../migrations/sql/041_work_producer_origin.surql"),
            include_str!("../../migrations/sql/043_native_read_fence.surql"),
            include_str!("../../migrations/sql/044_source_head_semantic_epoch.surql"),
            include_str!("../../migrations/sql/045_canonical_read_semantic_epoch.surql"),
        ] {
            client
                .query_checked(sql)
                .await
                .expect("actual current schema");
        }
        crate::reference_fence::install_native_reference_guards(client, &[])
            .await
            .expect("erasure guards");
    }
    client
        .initialize_journal_identity()
        .await
        .expect("original journal binding");
}

fn entity() -> Entity {
    Entity::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Entity),
            "historical".into(),
            "original-agent".into(),
        ),
        "person".into(),
        "original graph name".into(),
    )
}

async fn put_head(client: &StorageClient, head: &SourceHead) {
    let key = crate::source_publication::head_key(&head.space_id, &head.source_id);
    if let Some(store) = client.embedded_store() {
        store
            .mutate(|tx| {
                write_control_tx(
                    tx,
                    "source_head",
                    &key,
                    &head.space_id,
                    &head.source_id,
                    head,
                )
            })
            .await
            .expect("head");
    } else {
        client.query_bind_checked("CREATE $id CONTENT $record",crate::bindings!{"id"=>control_thing("source_head",&key),"record"=>control_value(&head.space_id,&head.source_id,head).expect("value")}).await.expect("head");
    }
}

async fn image(client: &StorageClient, head: &SourceHead) -> HistoricalControlImage {
    let key = crate::source_publication::head_key(&head.space_id, &head.source_id);
    crate::historical_adoption::catchup::full_control(client, "source_head", &key, &head.space_id)
        .await
        .expect("whole head")
}

async fn insert_original(client: &StorageClient, row: &TransferRecord) {
    if let Some(store) = client.embedded_store() {
        store
            .mutate(|tx| match row {
                TransferRecord::WorkItem(record) => tx
                    .create(record.clone())
                    .map(|_| ())
                    .map_err(StorageError::from),
                TransferRecord::SystemEvent(record) => tx
                    .create(record.clone())
                    .map(|_| ())
                    .map_err(StorageError::from),
                TransferRecord::Entity(record) => tx
                    .create(record.clone())
                    .map(|_| ())
                    .map_err(StorageError::from),
                TransferRecord::Rel(record) => tx
                    .create(record.clone())
                    .map(|_| ())
                    .map_err(StorageError::from),
                TransferRecord::WorkItemArchive(record) => tx
                    .create(record.clone())
                    .map(|_| ())
                    .map_err(StorageError::from),
                _ => Err(StorageError::QueryError("unexpected fixture type".into())),
            })
            .await
            .expect("original typed row");
    } else if let TransferRecord::Rel(rel) = row {
        client.query_bind_checked("RELATE $subject->$id->$object CONTENT $record",crate::bindings!{"subject"=>crate::surreal::thing_from_record_id(rel.subject.id()),"object"=>crate::surreal::thing_from_record_id(rel.object.id()),"id"=>crate::surreal::thing_from_record_id(row.id()),"record"=>crate::historical_delta_storage::full_native(row).expect("native")}).await.expect("original native relation");
    } else {
        client.query_bind_checked("CREATE $id CONTENT $record",crate::bindings!{"id"=>crate::surreal::thing_from_record_id(row.id()),"record"=>crate::historical_delta_storage::full_native(row).expect("native")}).await.expect("original native archive");
    }
}

async fn assert_exact_row(client: &StorageClient, expected: &TransferRecord) {
    if let Some(store) = client.embedded_store() {
        let actual = store
            .mutate(|tx| Ok(tx.raw_row_by_id(expected.id().table(), expected.id())))
            .await
            .expect("physical row");
        assert_eq!(actual, Some(expected.json().expect("json")));
    } else {
        let mut response = client
            .query_bind_checked(
                "SELECT * FROM ONLY $id",
                crate::bindings! {"id"=>crate::surreal::thing_from_record_id(expected.id())},
            )
            .await
            .expect("full native row");
        let actual: surrealdb::Value = response.take(0).expect("native decode");
        assert_eq!(
            actual.into_inner(),
            crate::historical_delta_storage::full_native(expected).expect("native full image")
        );
    }
}

async fn put_extension(
    client: &StorageClient,
    table: &str,
    key: &str,
    mut value: serde_json::Value,
) {
    value["id"] = format!("{table}:{key}").into();
    if let Some(store) = client.embedded_store() {
        store
            .mutate(|tx| {
                tx.upsert_extension_row(table, value.clone())
                    .map(|_| ())
                    .map_err(StorageError::from)
            })
            .await
            .expect("extension fixture");
    } else {
        let surrealdb::sql::Value::Object(mut fields) =
            crate::surreal::json_to_surreal(value).expect("native fields")
        else {
            unreachable!()
        };
        fields.insert("id".into(), control_thing(table, key).into());
        if table == "work_producer_origin" {
            fields.insert(
                "work_item_id".into(),
                control_thing("work_item", key).into(),
            );
        }
        client.query_bind_checked("CREATE $id CONTENT $row",crate::bindings!{"id"=>control_thing(table,key),"row"=>surrealdb::sql::Value::Object(fields)}).await.expect("native extension fixture");
    }
}
async fn origin_fixture(client: &StorageClient, work: &elephant_types::records::WorkItem) {
    let payload=serde_json::json!({"version":1,"space_id":work.base.space_id,"work_item_id":work.base.id,
        "intent_fingerprint":crate::work_producer_repository::work_intent_fingerprint(work).expect("intent"),"source_ids":[],
        "origin":crate::work_producer_repository::WorkProducerOrigin::Unknown,
        "historical_transfer":{"version":1,"snapshot_sha256":"b".repeat(64),"original_row_sha256":"a".repeat(64),"hash_domain":"homecore-python-json-v1","source_space_id":"default","target_space_id":"historical"}}).to_string();
    let key = work.base.id.id().to_string();
    let row = serde_json::json!({"space_id":"historical","work_item_id":work.base.id,"source_ids":[],"payload":payload,"created_at":chrono::Utc::now()});
    put_extension(client, "work_producer_origin", &key, row).await;
}
async fn checkpoint_fixtures(client: &StorageClient) {
    for consumer in [
        "knowledge_views",
        "truth_maintenance",
        crate::event_sequence::RESERVED_CONSUMER,
    ] {
        let mut row = serde_json::json!({"space_id":"historical","consumer":consumer,"seq":5,"updated_at":chrono::Utc::now()});
        if consumer != crate::event_sequence::RESERVED_CONSUMER {
            row["historical_baseline"] = serde_json::json!({"version":1,"snapshot_sha256":"b".repeat(64),"journal_id":client.erasure_journal().expect("journal").identity(),"through_seq":5,"disposition":"historical_preserved_not_delivered"});
        }
        put_extension(
            client,
            "outbox_checkpoint",
            &crate::outbox_checkpoint_repository::key("historical", consumer),
            row,
        )
        .await;
    }
}
async fn put_vector(client: &StorageClient, row: &elephant_types::records::NodeEmbedding) {
    if let Some(store) = client.embedded_store() {
        store
            .mutate(|tx| {
                tx.create(row.clone())
                    .map(|_| ())
                    .map_err(StorageError::from)
            })
            .await
            .expect("original vector");
    } else {
        let (partition, id, value) =
            crate::historical_delta_vectors::native(row).expect("partition");
        client
            .query_checked(&elephant_embeddings::PartitionManager::create_partition_ddl(&partition))
            .await
            .expect("partition DDL");
        client
            .query_bind_checked(
                "CREATE $id CONTENT $row",
                crate::bindings! {"id"=>id,"row"=>value},
            )
            .await
            .expect("original vector");
    }
}
async fn exact_vector(client: &StorageClient, row: &elephant_types::records::NodeEmbedding) {
    if let Some(store) = client.embedded_store() {
        let actual = store
            .mutate(|tx| Ok(tx.raw_row_by_id(TableName::ChunkEmbedding, &row.base.id)))
            .await
            .expect("raw vector");
        assert_eq!(actual, Some(serde_json::to_value(row).expect("vector")));
    } else {
        let (_, id, expected) = crate::historical_delta_vectors::native(row).expect("native");
        let mut r = client
            .query_bind_checked("SELECT * FROM ONLY $id", crate::bindings! {"id"=>id})
            .await
            .expect("native vector");
        let actual: surrealdb::Value = r.take(0).expect("native value");
        assert_eq!(actual.into_inner(), expected);
    }
}

fn change(before: TransferRecord, after: TransferRecord) -> HistoricalDeltaRow {
    HistoricalDeltaRow {
        id: before.id().clone(),
        kind: HistoricalChangeKind::Update,
        before_fingerprint: Some(runtime_row_fingerprint(&before).expect("before")),
        after_fingerprint: Some(runtime_row_fingerprint(&after).expect("after")),
        before: Some(before),
        after: Some(after),
        destructive_intent: false,
    }
}

fn receipt(
    marker: &CatchupMarker,
    source: &RecordId,
    row: &RecordId,
    before: &str,
    after: &str,
) -> CanonicalCommitReceipt {
    let state = HistoricalCatchupBatchReceipt {
        version: 1,
        authority_binding_sha256: marker.authority_binding_sha256.clone(),
        ordinal: 0,
        batch_sha256: "f".repeat(64),
        previous_receipt_sha256: None,
        before_epoch: before.into(),
        after_epoch: after.into(),
        operational_effects: Vec::new(),
        retained_controls: Vec::new(),
    };
    CanonicalCommitReceipt {
        historical_import: None,
        space_id: marker.space_id.clone(),
        commit_id: batch_commit_id(marker, 0),
        input_fingerprint: marker.authority_binding_sha256.clone(),
        plan_fingerprint: state.batch_sha256.clone(),
        replay_output: Some(serde_json::to_value(state).expect("state")),
        source_ids: vec![source.clone()],
        publication_authority: None,
        maintenance_authority: Vec::new(),
        source_id: None,
        source_revision: None,
        effect_ids: vec![row.clone()],
        extension_ids: Vec::new(),
        outbox_ids: Vec::new(),
        committed_at: chrono::Utc::now(),
    }
}

struct NativeReplayProof {
    binding: crate::historical_native_effects::HistoricalNativeEffectBinding,
    inventory: Vec<(String, surrealdb::sql::Thing)>,
    baseline_sha256: String,
}

struct RestartProof {
    native_replay: Option<NativeReplayProof>,
    marker: CatchupMarker,
    before_epoch: String,
    receipt: CanonicalCommitReceipt,
    before: Entity,
    after: Entity,
    head: SourceHead,
    after_image: HistoricalControlImage,
    extra_after: Vec<TransferRecord>,
    vector_after: elephant_types::records::NodeEmbedding,
    operational: Vec<crate::historical_delta_operations::OperationalEffect>,
}

#[allow(
    clippy::significant_drop_tightening,
    reason = "Explicit custody and all handles must close before reopening the persistent store."
)]
#[allow(
    clippy::too_many_lines,
    reason = "Keep the complete ordered fixture and its failure/recovery assertions together."
)]
async fn write_and_interrupt(root: &std::path::Path, embedded: bool) -> RestartProof {
    let client = open(root, embedded).await;
    install(&client).await;
    let configuration = elephant_types::records::SpaceConfig {
        base: BaseFields::new(
            RecordId::new_v4(TableName::SpaceConfig),
            "historical".into(),
            "fixture".into(),
        ),
        embedding_provider: "fake".into(),
        embedding_model: "fixture".into(),
        embedding_dims: 3,
        purpose: None,
        skill: None,
        processing: Some(elephant_types::provider_binding::SpaceProcessingConfig {
            generation: 1,
            index_generation: 0,
            policy_id: "default".into(),
            connections: std::collections::BTreeMap::new(),
            profiles: std::collections::BTreeMap::new(),
            bindings: std::collections::BTreeMap::new(),
            active_embedding_profile_id: None,
            pending_reindex: None,
            completed_embedding_reindex: None,
        }),
    };
    crate::SpaceConfigRepository::new(client.clone())
        .create(configuration.clone())
        .await
        .expect("configuration");
    let mut before = entity();
    before.metadata = serde_json::json!({"name_embedding":[0.5,-0.25,0.0],"name_embedding_model":"original-model","name_embedding_text_hash":"original-text-hash","name_embedding_dims":3});
    insert_original(&client, &TransferRecord::Entity(before.clone())).await;
    assert_exact_row(&client, &TransferRecord::Entity(before.clone())).await;
    let target = entity();
    crate::EntityRepository::new(client.clone())
        .create(target.clone())
        .await
        .expect("relation endpoint");
    let relation = elephant_types::records::Rel {
        base: BaseFields::new(
            RecordId::new_v4(TableName::Rel),
            "historical".into(),
            "original-agent".into(),
        ),
        subject: elephant_types::Link::new(before.base.id.clone()),
        object: elephant_types::Link::new(target.base.id),
        rel_type: "knows".into(),
        status: elephant_types::records::ClaimStatus::Approved,
        confidence: 0.75,
        validity_window: None,
        evidence: Vec::new(),
        temporal_audit: None,
    };
    let mut relation_after = relation.clone();
    relation_after.rel_type = "collaborates_with".into();
    let work = elephant_types::records::WorkItem {
        base: BaseFields::new(
            RecordId::new_v4(TableName::WorkItem),
            "historical".into(),
            "original-agent".into(),
        ),
        kind: "historical".into(),
        payload: serde_json::json!({"original":true}),
        status: elephant_types::records::WorkItemStatus::Failed,
        priority: 1,
        idempotency_key: "archive-fixture".into(),
        attempts: 3,
        leased_by: None,
        lease_expires_at: None,
        run_after: None,
        last_error: Some("old failure".into()),
    };
    let mut archive_base = work.base.clone();
    archive_base.id = crate::work_retention::archive_id(&work.base.id);
    let archive = elephant_types::records::WorkItemArchive {
        base: archive_base,
        original_id: work.base.id.clone(),
        archived_at: chrono::Utc::now(),
        record: work.clone(),
    };
    let mut archive_after = archive.clone();
    archive_after.record.last_error = Some("captured S1 failure".into());
    let event = elephant_types::records::SystemEvent {
        base: BaseFields::new(
            RecordId::new_v4(TableName::SystemEvent),
            "historical".into(),
            "original".into(),
        ),
        seq: 5,
        event_type: "original_history".into(),
        payload: serde_json::json!({}),
    };
    let mut event_after = event.clone();
    event_after.seq = 7;
    let extra_before = vec![
        TransferRecord::Rel(relation),
        TransferRecord::WorkItem(work.clone()),
        TransferRecord::SystemEvent(event),
    ];
    let extra_after = vec![
        TransferRecord::Rel(relation_after),
        TransferRecord::WorkItemArchive(archive_after),
        TransferRecord::SystemEvent(event_after),
    ];
    for row in &extra_before {
        insert_original(&client, row).await;
        assert_exact_row(&client, row).await;
    }
    origin_fixture(&client, &work).await;
    checkpoint_fixtures(&client).await;
    let vector = elephant_types::records::NodeEmbedding {
        base: BaseFields::new(
            RecordId::new_v4(TableName::ChunkEmbedding),
            "historical".into(),
            "original".into(),
        ),
        node: elephant_types::Link::new(RecordId::new_v4(TableName::DocNode)),
        model: "original".into(),
        dims: 3,
        vector: vec![0.5, 0.25, -0.0],
    };
    let mut vector_after = vector.clone();
    vector_after.vector[0] = 0.75;
    put_vector(&client, &vector).await;
    let source = RecordId::new_v4(TableName::Source);
    let head = SourceHead {
        space_id: "historical".into(),
        source_id: source.clone(),
        generation: "original-generation".into(),
        desired_revision: Some("original-revision".into()),
        published_revision: Some("original-revision".into()),
        operation_id: "original-operation".into(),
        lifecycle: elephant_types::publication::SourceVisibility::Active,
        updated_at: chrono::Utc::now(),
    };
    put_head(&client, &head).await;
    let config_before = crate::SpaceConfigRepository::new(client.clone())
        .get("historical", &configuration.base.id)
        .await
        .expect("configuration")
        .expect("exists");
    let before_epoch = CanonicalRepository::new(client.clone())
        .observe_epoch("historical")
        .await
        .expect("epoch")
        .expect("existing");
    let mut marker = super::tests::fixture_marker(&client);
    marker.batches = 2;
    journal::begin(&client.journal, &marker).expect("original pending marker");
    let guard = client
        .begin_maintenance(RecoveryPhase::Restoring, std::time::Duration::from_secs(5))
        .await
        .expect("exclusive");
    let exclusive = client.maintenance_client_with_guard(&guard);
    let authority = PreparedHistoricalCatchupAuthority {
        client: exclusive.clone(),
        marker: marker.clone(),
        next_batch: 0,
        last_receipt: None,
        epoch: before_epoch.clone(),
        original_epoch: before_epoch.clone(),
        recovered_regeneration: Vec::new(),
        original_sources: Vec::new(),
        retained_operational: tokio::sync::OnceCell::new(),
        archive_receipt: None,
    };
    let before_image = image(&exclusive, &head).await;
    let mut after_head = head.clone();
    after_head.published_revision = None;
    let after_image = before_image
        .replace(&after_head)
        .expect("projected complete control");
    let mut after = before.clone();
    after.display_name = "agent graph edit".into();
    let before_row = TransferRecord::Entity(before.clone());
    let after_row = TransferRecord::Entity(after.clone());
    let mut effects = PreparedHistoricalEffects {
        controls: vec![HistoricalControlEffect {
            table: "source_head",
            key: crate::source_publication::head_key("historical", &source),
            before: before_image.clone(),
            after: Some(after_image.clone()),
        }],
        projected_rows: vec![HistoricalDeltaRow {
            id: before.base.id.clone(),
            kind: HistoricalChangeKind::Update,
            before_fingerprint: Some(runtime_row_fingerprint(&before_row).expect("hash")),
            after_fingerprint: Some(runtime_row_fingerprint(&after_row).expect("hash")),
            before: Some(before_row),
            after: Some(after_row),
            destructive_intent: false,
        }],
        original_receipts: Vec::new(),
        projected_embeddings: Vec::new(),
        batch_digest: "f".repeat(64),
        expected_epoch: before_epoch.clone(),
    };
    effects
        .projected_rows
        .push(change(extra_before[0].clone(), extra_after[0].clone()));
    effects.projected_rows.push(HistoricalDeltaRow {
        id: work.base.id.clone(),
        kind: HistoricalChangeKind::Remove,
        before_fingerprint: Some(runtime_row_fingerprint(&extra_before[1]).expect("hot before")),
        after_fingerprint: None,
        before: Some(extra_before[1].clone()),
        after: None,
        destructive_intent: true,
    });
    effects.projected_rows.push(HistoricalDeltaRow {
        id: extra_after[1].id().clone(),
        kind: HistoricalChangeKind::Create,
        before_fingerprint: None,
        after_fingerprint: Some(runtime_row_fingerprint(&extra_after[1]).expect("archive after")),
        before: None,
        after: Some(extra_after[1].clone()),
        destructive_intent: false,
    });
    effects
        .projected_rows
        .push(change(extra_before[2].clone(), extra_after[2].clone()));
    let projected_vector = crate::historical_delta::HistoricalEmbeddingDelta {
        id: vector.base.id.clone(),
        kind: HistoricalChangeKind::Update,
        before_fingerprint: Some(
            crate::historical_delta::runtime_embedding_fingerprint(&vector).expect("vector before"),
        ),
        after_fingerprint: Some(
            crate::historical_delta::runtime_embedding_fingerprint(&vector_after)
                .expect("vector after"),
        ),
        before: Some(vector.clone()),
        after: Some(vector_after.clone()),
        destructive_intent: false,
    };
    effects.projected_embeddings.push(projected_vector);
    let mut raw_rows = effects.projected_rows.clone();
    for row in &mut raw_rows {
        for image in [&mut row.before, &mut row.after].into_iter().flatten() {
            image
                .relocate_space("historical", "default")
                .expect("original namespace");
        }
        row.before_fingerprint = row
            .before
            .as_ref()
            .map(runtime_row_fingerprint)
            .transpose()
            .expect("raw before");
        row.after_fingerprint = row
            .after
            .as_ref()
            .map(runtime_row_fingerprint)
            .transpose()
            .expect("raw after");
    }
    let batch = crate::historical_delta::HistoricalDeltaBatch {
        version: 1,
        migration_id: marker.migration_id.clone(),
        source_space_id: marker.source_space_id.clone(),
        space_id: marker.space_id.clone(),
        before_snapshot_sha256: marker.before_snapshot_sha256.clone(),
        after_snapshot_sha256: marker.after_snapshot_sha256.clone(),
        planner_delta_sha256: marker.delta_sha256.clone(),
        batch_ordinal: 0,
        previous_batch_receipt: None,
        expected_epoch: before_epoch.clone(),
        rows: raw_rows,
        embeddings: Vec::new(),
        retained_controls: Vec::new(),
    }
    .prepare()
    .expect("original batch");
    let operational =
        crate::historical_delta_operations::prepare(&authority, &batch, &effects.projected_rows)
            .await
            .expect("hot archive and event effects");
    assert_eq!(operational.len(), 4);
    let floor_key = crate::outbox_checkpoint_repository::key(
        "historical",
        crate::event_sequence::RESERVED_CONSUMER,
    );
    if let Some(store) = exclusive.embedded_store() {
        store
            .mutate(|tx| {
                let key = format!("outbox_checkpoint:{floor_key}");
                let mut row = tx
                    .extension_row_by_key("outbox_checkpoint", &key)?
                    .expect("floor");
                row["seq"] = 6.into();
                tx.replace_extension_row("outbox_checkpoint", &key, row)?;
                Ok(())
            })
            .await
            .expect("target-owned event floor");
    } else {
        exclusive
            .query_bind_checked(
                "UPDATE $id SET seq=6",
                crate::bindings! {"id"=>control_thing("outbox_checkpoint",&floor_key)},
            )
            .await
            .expect("target-owned event floor");
    }
    assert!(
        crate::historical_delta_operations::prepare(&authority, &batch, &effects.projected_rows)
            .await
            .is_err(),
        "must not acknowledge an undelivered target event"
    );
    if let Some(store) = exclusive.embedded_store() {
        store
            .mutate(|tx| {
                let key = format!("outbox_checkpoint:{floor_key}");
                let mut row = tx
                    .extension_row_by_key("outbox_checkpoint", &key)?
                    .expect("floor");
                row["seq"] = 5.into();
                tx.replace_extension_row("outbox_checkpoint", &key, row)?;
                Ok(())
            })
            .await
            .expect("restore exact floor");
    } else {
        exclusive
            .query_bind_checked(
                "UPDATE $id SET seq=5",
                crate::bindings! {"id"=>control_thing("outbox_checkpoint",&floor_key)},
            )
            .await
            .expect("restore exact floor");
    }

    let after_epoch = uuid::Uuid::new_v4().to_string();
    let mut receipt = receipt(
        &marker,
        &source,
        &before.base.id,
        &before_epoch,
        &after_epoch,
    );
    let mut state: HistoricalCatchupBatchReceipt =
        serde_json::from_value(receipt.replay_output.clone().expect("state")).expect("state");
    state.operational_effects = crate::historical_delta_operations::receipt_effects(&operational);
    receipt.replay_output = Some(serde_json::to_value(state).expect("state"));
    // This fault occurs after row and source-control writes, before epoch/receipt.
    assert!(commit_batch_inner(
        &authority,
        &effects,
        &operational,
        &receipt,
        &after_epoch,
        BatchFault::BeforeReceipt
    )
    .await
    .is_err());
    assert_eq!(
        crate::EntityRepository::new(exclusive.clone())
            .get("historical", &before.base.id)
            .await
            .expect("row"),
        Some(before.clone())
    );
    assert_exact_row(&exclusive, &TransferRecord::Entity(before.clone())).await;
    exact_vector(&exclusive, &vector).await;
    for row in &extra_before {
        assert_exact_row(&exclusive, row).await;
    }
    assert_eq!(image(&exclusive, &head).await, before_image);
    assert_eq!(
        crate::SpaceConfigRepository::new(exclusive.clone())
            .get("historical", &configuration.base.id)
            .await
            .expect("rollback config"),
        Some(config_before.clone())
    );
    assert_eq!(
        CanonicalRepository::new(exclusive.clone())
            .observe_epoch("historical")
            .await
            .expect("epoch"),
        Some(before_epoch.clone())
    );
    let key = stable_key(&["historical", &receipt.commit_id]);
    assert!(SourcePublicationRepository::new(exclusive.clone())
        .read_control::<CanonicalCommitReceipt>("source_commit_receipt", &key, "historical")
        .await
        .expect("read")
        .is_none());
    // Retain the original planned identity inventory and pre-commit native
    // hashes. No ledger effect is consumed before all DB handles are closed.
    let native_replay = if embedded {
        None
    } else {
        let planned = crate::historical_delta_storage::inventory::begin(
            &authority,
            &effects,
            &operational,
            &receipt,
            &source,
            &mut String::new(),
            &mut std::collections::BTreeMap::new(),
        )
        .await
        .expect("independently planned native inventory");
        let baseline = root.join("original-expected.sqlite");
        let db = rusqlite::Connection::open(&baseline).expect("original ledger");
        db.execute_batch("CREATE TABLE expected(t TEXT,k TEXT,h TEXT,seq INTEGER,PRIMARY KEY(t,k),UNIQUE(t,seq));").expect("schema");
        let mut identities = planned.identities.clone();
        identities.push((
            "source_erasure_guard".into(),
            control_thing("source_erasure_guard", "historical"),
        ));
        identities.sort_by(|a, b| a.1.cmp(&b.1));
        identities.dedup();
        for (seq, (table, id)) in identities.iter().enumerate() {
            let mut response = exclusive
                .query_bind_checked(
                    "SELECT VALUE crypto::sha256(type::string($this)) FROM ONLY $id",
                    crate::bindings! {"id"=>id.clone()},
                )
                .await
                .expect("original exact native hash");
            let hash: Option<String> = response.take(0).expect("optional hash");
            if let Some(hash) = hash {
                db.execute(
                    "INSERT INTO expected VALUES(?,?,?,?)",
                    rusqlite::params![
                        table,
                        id.to_string(),
                        hash,
                        i64::try_from(seq)
                            .expect("bounded planned native inventory sequence fits i64")
                    ],
                )
                .expect("baseline row");
            }
        }
        drop(db);
        let baseline_sha256 = hex::encode(Sha256::digest(
            std::fs::read(&baseline).expect("closed baseline"),
        ));
        Some(NativeReplayProof {
            binding: planned.binding,
            inventory: planned.identities,
            baseline_sha256,
        })
    };
    commit_batch_inner(
        &authority,
        &effects,
        &operational,
        &receipt,
        &after_epoch,
        BatchFault::None,
    )
    .await
    .expect("atomic batch");
    assert_exact_row(&exclusive, &TransferRecord::Entity(after.clone())).await;
    exact_vector(&exclusive, &vector_after).await;
    for control in &operational {
        let actual = crate::historical_delta_operations::full_optional(
            &exclusive,
            control.table,
            &control.key,
            "historical",
        )
        .await
        .expect("operational after");
        assert_eq!(actual, control.after);
    }
    for row in &extra_after {
        assert_exact_row(&exclusive, row).await;
    }
    assert_eq!(image(&exclusive, &head).await, after_image);
    let config_after = crate::SpaceConfigRepository::new(exclusive.clone())
        .get("historical", &configuration.base.id)
        .await
        .expect("configuration")
        .expect("exists");
    assert!(
        config_after
            .processing
            .as_ref()
            .expect("processing")
            .index_generation
            > config_before
                .processing
                .as_ref()
                .expect("processing")
                .index_generation
    );
    assert!(commit_batch_inner(
        &authority,
        &effects,
        &operational,
        &receipt,
        &after_epoch,
        BatchFault::None
    )
    .await
    .is_err());
    let recovered = recover_chain(&exclusive, &marker, &before_epoch)
        .await
        .expect("one contiguous receipt");
    assert_eq!(recovered.next_batch, 1);
    assert_eq!(recovered.epoch, after_epoch);
    // Real disk durability and handle teardown, not a second in-memory database.
    exclusive
        .persist_restored_state()
        .await
        .expect("durable state");
    drop(authority);
    drop(exclusive);
    drop(guard);
    drop(client);
    RestartProof {
        native_replay,
        marker,
        before_epoch,
        receipt,
        before,
        after,
        head,
        after_image,
        extra_after,
        vector_after,
        operational,
    }
}

#[allow(
    clippy::significant_drop_tightening,
    reason = "Resume custody is dropped explicitly to verify Failed state."
)]
#[allow(
    clippy::too_many_lines,
    reason = "Keep the complete ordered fixture and its failure/recovery assertions together."
)]
async fn reopen_and_verify(root: &std::path::Path, embedded: bool, proof: RestartProof) {
    let RestartProof {
        native_replay,
        marker,
        before_epoch,
        receipt,
        before,
        after,
        head,
        after_image,
        extra_after,
        vector_after,
        operational,
    } = proof;
    let reopened = open(root, embedded).await;
    assert!(!reopened.recovery_status().ready);
    assert!(crate::EntityRepository::new(reopened.clone())
        .get("historical", &before.base.id)
        .await
        .is_err());
    let resume_guard = reopened
        .begin_maintenance(RecoveryPhase::Restoring, std::time::Duration::from_secs(5))
        .await
        .expect("resume custody");
    let resume = reopened.maintenance_client_with_guard(&resume_guard);
    let recovered = recover_chain(&resume, &marker, &before_epoch)
        .await
        .expect("persistent chain");
    assert_eq!(recovered.next_batch, 1);
    assert_eq!(
        recovered.last_receipt,
        Some(hex::encode(Sha256::digest(
            serde_json::to_vec(&receipt).expect("receipt")
        )))
    );
    assert_eq!(
        crate::EntityRepository::new(resume.clone())
            .get("historical", &before.base.id)
            .await
            .expect("persisted after-image"),
        Some(after.clone())
    );
    assert_exact_row(&resume, &TransferRecord::Entity(after.clone())).await;
    exact_vector(&resume, &vector_after).await;
    for control in &operational {
        let actual = crate::historical_delta_operations::full_optional(
            &resume,
            control.table,
            &control.key,
            "historical",
        )
        .await
        .expect("persistent operational after");
        assert_eq!(actual, control.after);
    }
    for row in &extra_after {
        assert_exact_row(&resume, row).await;
    }
    assert_eq!(image(&resume, &head).await, after_image);
    if let Some(proof) = native_replay {
        let baseline = root.join("original-expected.sqlite");
        assert_eq!(
            hex::encode(Sha256::digest(
                std::fs::read(&baseline).expect("retained original")
            )),
            proof.baseline_sha256
        );
        let source = receipt.source_ids.first().expect("source");
        let recovered = crate::historical_native_effects::recover_verified(
            &resume,
            source,
            &proof.binding,
            &receipt,
            &proof.inventory,
        )
        .await
        .expect("committed DB manifest recovered after store/runtime restart");
        let expected = recovered.effects().to_vec();
        assert!(
            expected
                .iter()
                .any(|e| e.before_sha256.is_some() && e.after_sha256.is_none()),
            "hot row deletion retained"
        );
        let replay_path = root.join("resumed-expected.sqlite");
        std::fs::copy(&baseline, &replay_path).expect("fresh original ledger copy");
        let mut ledger=crate::historical_catchup_ledger::HistoricalCatchupLedger::fixture_open_retained_baseline(
            &replay_path,&proof.binding.authority_binding_sha256,
        ).expect("retained ledger");
        ledger
            .apply(recovered)
            .expect("exact DB-commit/CLI-ledger gap replay");
        drop(ledger);
        let db = rusqlite::Connection::open(&replay_path).expect("replayed ledger");
        for effect in &expected {
            use rusqlite::OptionalExtension as _;
            let actual: Option<String> = db
                .query_row(
                    "SELECT h FROM expected WHERE t=? AND k=?",
                    rusqlite::params![effect.table, effect.id],
                    |r| r.get(0),
                )
                .optional()
                .expect("ledger row");
            assert_eq!(actual, effect.after_sha256, "{}", effect.id);
        }
        assert_eq!(
            hex::encode(Sha256::digest(
                std::fs::read(&baseline).expect("unchanged original")
            )),
            proof.baseline_sha256
        );
        // A stored field matching the former projection alias must not vanish.
        let manifest = expected.last().expect("manifest self effect");
        let id = control_thing(
            "source_commit_receipt",
            &stable_key(&[
                "historical",
                &stable_key(&[
                    "historical-native-effects-v1",
                    &proof.binding.authority_binding_sha256,
                    "batch",
                    "0",
                ]),
            ]),
        );
        assert_eq!(id.to_string(), manifest.id);
        resume
            .query_bind_checked(
                "UPDATE $id SET __historical_native_sha256='unknown stored field'",
                crate::bindings! {"id"=>id},
            )
            .await
            .expect("unknown wrapper fixture");
        assert!(crate::historical_native_effects::recover_verified(
            &resume,
            source,
            &proof.binding,
            &receipt,
            &proof.inventory
        )
        .await
        .is_err());
        let primary_id = control_thing(
            "source_commit_receipt",
            &stable_key(&["historical", &receipt.commit_id]),
        );
        exact_receipt(&resume, "historical", &receipt.commit_id)
            .await
            .expect("exact original primary wrapper");
        resume
            .query_bind_checked(
                "UPDATE $id SET unknown_historical_wrapper='preserve and refuse'",
                crate::bindings! {"id"=>primary_id},
            )
            .await
            .expect("unknown primary field");
        assert!(exact_receipt(&resume, "historical", &receipt.commit_id)
            .await
            .is_err());
    }
    assert_eq!(
        journal::read(&reopened.journal).expect("pending"),
        Some(marker)
    );
    drop(resume);
    drop(resume_guard);
    assert_eq!(reopened.recovery_status().phase, RecoveryPhase::Failed);
}

fn persistent_control(embedded: bool) {
    let root = tempfile::tempdir().expect("directory");
    let writer = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("writer runtime");
    let proof = writer.block_on(write_and_interrupt(root.path(), embedded));
    // Native SDK router owns the engine until its runtime closes.
    drop(writer);
    let reader = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("reader runtime");
    reader.block_on(reopen_and_verify(root.path(), embedded, proof));
}

#[test]
fn embedded_batch_rolls_back_and_persistent_receipt_chain_resumes() {
    persistent_control(true);
}
#[cfg(feature = "rocksdb")]
#[test]
fn native_batch_rolls_back_and_persistent_receipt_chain_resumes() {
    persistent_control(false);
}
