//! Independent active graph lifecycle and inverse-decision oracles on both backends.
//! Fixed interpretations qualify storage/lifecycle semantics, never model quality.

#[path = "../../recall_preview/crash_support.rs"]
mod crash_support;
#[path = "../../recall_preview/graph_support.rs"]
mod graph_support;
#[path = "../../recall_preview/publication_support.rs"]
mod publication_support;
#[path = "../../recall_preview/support.rs"]
mod support;

use elephant_mcp::{ToolContext, ToolDispatcher};
use elephant_storage::canonical_commit::{
    CanonicalCommit, CanonicalEffect, CanonicalMaintenanceContext, CanonicalMaintenanceOrigin,
    CanonicalPlan, CanonicalRecord, CanonicalRepository,
};
use elephant_storage::snapshot::{export_named, restore_named, SnapshotRetention};
use elephant_storage::source_publication::{SourceMutation, SourcePublicationRepository};
use elephant_storage::{
    AssertionRepository, EntityRepository, Repository, SourceRepository, StorageBackendKind,
    StorageClient, StorageConfig,
};
use elephant_types::publication::{OperationState, SourcePrecondition, SourceVisibility};
use elephant_types::records::base::BaseFields;
use elephant_types::records::entity::EntityType;
use elephant_types::records::*;
use elephant_types::{RecordId, TableName};
use graph_support::GraphFixture;
use serde_json::{json, Value};

const SPACE: &str = "erasure-qualification";

/// Test-only native extension fixture; actual writes traverse each backend's
/// shared mutation/event guards. Object IDs exercise the embedded type bridge.
async fn write_native_fixture(
    client: &StorageClient,
    space: &str,
    id: &surrealdb::sql::Thing,
    owner: Option<&RecordId>,
) -> anyhow::Result<()> {
    use surrealdb::sql::{Id, Value as NativeValue};
    let encoded_id = match &id.id {
        Id::String(value) => json!({"tb":id.tb,"id":value}),
        Id::Number(value) => json!({"tb":id.tb,"id":value}),
        // Canonical native spelling preserves composite ID types and references
        // inside their keys without treating an array as an ordinary string key.
        Id::Array(_) | Id::Object(_) => json!(id.to_string()),
        _ => anyhow::bail!("unsupported fixture identity type"),
    };
    let mut row = json!({"id":encoded_id, "space_id":space,
        "fixture_id":id.to_string(), "text":"Declared native reference fixture"});
    if let Some(source) = owner {
        row["source_id"] = json!(source);
    }
    if let Some(store) = client.embedded_store() {
        store
            .mutate(|transaction| {
                transaction
                    .upsert_extension_row(&id.tb, row)
                    .map_err(Into::into)
            })
            .await?;
    } else {
        let mut row = elephant_storage::surreal::json_to_surreal(row)?;
        if let NativeValue::Object(object) = &mut row {
            object.insert("id".into(), NativeValue::Thing(id.clone()));
        }
        client
            .query_bind_checked(
                "UPSERT $record CONTENT $row",
                elephant_storage::bindings! { "record" => id.clone(), "row" => row },
            )
            .await?;
    }
    Ok(())
}

async fn native_fixture_exists(
    client: &StorageClient,
    space: &str,
    id: &surrealdb::sql::Thing,
) -> anyhow::Result<bool> {
    if let Some(store) = client.embedded_store() {
        return Ok(store
            .extension_rows_page(&id.tb, space, None, 100)
            .await?
            .iter()
            .any(|(_, row)| row["fixture_id"] == id.to_string()));
    }
    #[derive(serde::Deserialize)]
    struct Count {
        n: u64,
    }
    let mut response = client
        .query_bind_checked(
            "SELECT count() AS n FROM $record WHERE space_id = $space GROUP ALL",
            elephant_storage::bindings! { "record" => id.clone(), "space" => space },
        )
        .await?;
    let rows: Vec<Count> = response.take(0)?;
    Ok(rows.first().is_some_and(|row| row.n == 1))
}

async fn physical_artifact_exists(client: &StorageClient, id: &RecordId) -> anyhow::Result<bool> {
    if let Some(store) = client.embedded_store() {
        return Ok(store.get::<WorkArtifact>(SPACE, id, true).await?.is_some());
    }
    #[derive(serde::Deserialize)]
    struct Count {
        n: u64,
    }
    let mut response = client.query_bind_checked(
        "SELECT count() AS n FROM $record WHERE space_id = $space GROUP ALL",
        elephant_storage::bindings! { "record" => elephant_storage::surreal::thing_from_record_id(id), "space" => SPACE }).await?;
    let rows: Vec<Count> = response.take(0)?;
    Ok(rows.first().is_some_and(|row| row.n == 1))
}

fn reference_artifact(run: &WorkRun, data: Value) -> WorkArtifact {
    WorkArtifact {
        base: BaseFields::new(
            RecordId::new_v4(TableName::WorkArtifact),
            SPACE.into(),
            "qualification".into(),
        ),
        work_run: elephant_types::Link::new(run.base.id.clone()),
        artifact_type: "qualification_reference_fixture".into(),
        data,
        status: ArtifactStatus::Draft,
        validation_errors: None,
        validated_at: None,
        committed_at: None,
    }
}

/// Claim an actual source-free producer; only artifact references may later
/// establish ownership. The caller records success after authoring its artifacts.
async fn reference_producer(
    client: &StorageClient,
) -> anyhow::Result<(elephant_pipeline::scheduler::WorkQueue, WorkItem, WorkRun)> {
    use elephant_pipeline::scheduler::WorkQueue;
    use elephant_storage::WorkRunRepository;
    let queue = WorkQueue::with_defaults(client.clone());
    let pending = queue
        .enqueue(WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                SPACE.into(),
                "qualification".into(),
            ),
            kind: "qualification_reference_fixture".into(),
            payload: json!({}),
            status: WorkItemStatus::Runnable,
            priority: 0,
            idempotency_key: format!("native-reference-{}", uuid::Uuid::new_v4()),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        })
        .await?;
    let claimed = queue
        .claim()
        .await?
        .ok_or_else(|| anyhow::anyhow!("reference producer not claimed"))?;
    anyhow::ensure!(claimed.base.id == pending.base.id);
    let runs = WorkRunRepository::new(client.clone());
    let run = runs
        .create(WorkRun {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkRun),
                SPACE.into(),
                "qualification".into(),
            ),
            work_item: elephant_types::Link::new(claimed.base.id.clone()),
            attempt: claimed.attempts,
            started_at: chrono::Utc::now(),
            completed_at: None,
            success: false,
            error: None,
            metrics: Default::default(),
            worker_id: "qualification".into(),
            worker_version: "fixture-v1".into(),
            model_version: None,
            prompt_id: None,
            config_hash: None,
        })
        .await?;
    Ok((queue, claimed, run))
}

/// Native expressions and partial updates must be checked after evaluation.
/// A matching allowed transaction establishes that refusal is not a syntax or
/// unavailable-route accident; the earlier write in the rejected batch rolls back.
async fn native_partial_update_oracle(
    client: &StorageClient,
    target: &surrealdb::sql::Thing,
    other: &surrealdb::sql::Thing,
    allowed: &surrealdb::sql::Thing,
    erased: &[surrealdb::sql::Thing],
) -> anyhow::Result<()> {
    use surrealdb::sql::{Id, Value as NativeValue};
    if client.embedded_store().is_some() {
        return Ok(());
    }
    #[derive(Debug, PartialEq, serde::Deserialize)]
    struct Probe {
        qualification_marker: Option<String>,
        qualification_reference: Option<surrealdb::sql::Thing>,
    }
    async fn read(client: &StorageClient, id: &surrealdb::sql::Thing) -> anyhow::Result<Probe> {
        let mut response = client
            .query_bind_checked(
                "SELECT qualification_marker, qualification_reference FROM $record",
                elephant_storage::bindings! { "record" => id.clone() },
            )
            .await?;
        let mut rows: Vec<Probe> = response.take(0)?;
        anyhow::ensure!(rows.len() == 1, "native update probe row disappeared");
        Ok(rows.remove(0))
    }
    const SQL: &str = "BEGIN TRANSACTION;
        UPDATE $other SET qualification_marker = $marker;
        LET $evaluated_reference = type::thing($reference_table, $reference_key);
        UPDATE $target SET qualification_marker = $marker,
            qualification_reference = $evaluated_reference;
        COMMIT TRANSACTION;";
    for (reference, marker, must_refuse) in std::iter::once((allowed, "allowed", false))
        .chain(erased.iter().map(|id| (id, "must-rollback", true)))
    {
        let key = match &reference.id {
            Id::String(key) => NativeValue::from(key.clone()),
            Id::Number(key) => NativeValue::from(*key),
            Id::Array(key) => NativeValue::Array(key.clone()),
            Id::Object(key) => NativeValue::Object(key.clone()),
            _ => anyhow::bail!("unsupported probe key"),
        };
        let target_before = read(client, target).await?;
        let other_before = read(client, other).await?;
        let result = client
            .query_bind_checked(
                SQL,
                elephant_storage::bindings! {
                    "other" => other.clone(), "target" => target.clone(),
                    "marker" => marker, "reference_table" => reference.tb.clone(),
                    "reference_key" => key
                },
            )
            .await;
        if must_refuse {
            let error = match result {
                Err(error) => error,
                Ok(_) => anyhow::bail!("partial UPDATE accepted evaluated erased native reference"),
            };
            anyhow::ensure!(
                error.to_string().contains("source_erased"),
                "wrong refusal instead of actual erasure fence: {error}"
            );
            anyhow::ensure!(read(client, target).await? == target_before);
            anyhow::ensure!(
                read(client, other).await? == other_before,
                "earlier ordinary write escaped rejected native transaction"
            );
        } else {
            result?;
            let actual = read(client, target).await?;
            anyhow::ensure!(actual.qualification_marker.as_deref() == Some("allowed"));
            anyhow::ensure!(actual.qualification_reference.as_ref() == Some(allowed));
        }
    }
    Ok(())
}

#[tokio::test]
async fn native_erasure_preserves_key_types_and_independent_neighbors_but_fences_exact_late_references_after_restore(
) -> anyhow::Result<()> {
    use elephant_storage::{WorkArtifactRepository, WorkRunRepository};
    use surrealdb::sql::{Id, Thing};
    for backend in support::BACKENDS {
        let directory = crash_support::CrashDirectory::new()?;
        let client = storage(backend, &directory.0).await?;
        let graph = GraphFixture::new(client.clone(), SPACE).await?;
        publish(&graph).await?;
        elephant_storage::canonical_commit::ensure_extension_epoch_guards(
            &client,
            &["consumer_note".into()],
        )
        .await?;
        let owned = [
            Thing::from(("consumer_note", "42")),
            Thing::from(("consumer_note", Id::Number(7))),
            Thing::from(("consumer_note", "qualification-native-private")),
            Thing::from(("consumer_note", "élève 'quoted' \\ key")),
            surrealdb::sql::thing("consumer_note:[consumer_note:42, 'owned-array']")?,
            surrealdb::sql::thing(
                "consumer_note:{ peer: consumer_note:42, label: 'owned-object' }",
            )?,
        ];
        let independent = [
            Thing::from(("consumer_note", Id::Number(42))),
            Thing::from(("consumer_note", "7")),
            Thing::from(("consumer_note", "qualification-native-private-neighbor")),
        ];
        for id in &owned {
            write_native_fixture(&client, SPACE, id, Some(&graph.lineage.source.base.id)).await?;
        }
        for id in &independent {
            write_native_fixture(&client, SPACE, id, None).await?;
        }
        for id in owned.iter().chain(&independent) {
            anyhow::ensure!(
                native_fixture_exists(&client, SPACE, id).await?,
                "native typed/composite identity missing before erasure: {id}"
            );
        }

        // An actual separately claimed producer writes the artifacts below. Its
        // queue/run carries no source reference, so native-reference closure is
        // the only reason these particular artifacts may be removed.
        let (queue, claimed, mut run) = reference_producer(&client).await?;
        let runs = WorkRunRepository::new(client.clone());
        let artifacts = WorkArtifactRepository::new(client.clone());
        let mut removed = Vec::new();
        for id in &owned {
            let reference = id.to_string();
            let object = serde_json::Map::from_iter([(reference.clone(), json!("owned input"))]);
            for data in [
                json!({"reference":reference}),
                Value::Object(object),
                json!({"encoded":serde_json::to_string(&json!({"reference":reference}))?}),
            ] {
                removed.push(artifacts.create(reference_artifact(&run, data)).await?);
            }
        }
        let mut retained = Vec::new();
        for id in &independent {
            retained.push(
                artifacts
                    .create(reference_artifact(
                        &run,
                        json!({"reference":id.to_string()}),
                    ))
                    .await?,
            );
        }
        retained.push(artifacts.create(reference_artifact(&run, json!({"annotation":format!("Operator discussed {} as an identifier; this is independent prose.", owned[2])}))).await?);
        run.success = true;
        run.completed_at = Some(chrono::Utc::now());
        let run_id = run.base.id.clone();
        run = runs.update(SPACE, &run_id, run).await?;
        queue
            .for_attempt(&claimed)?
            .complete(SPACE, &claimed.base.id)
            .await?;
        let backup = directory.0.join("native-reference-before-erase");
        export_named(
            &client,
            &backup,
            "native-reference-before-erase",
            &SnapshotRetention::default(),
        )
        .await?;
        let publication = SourcePublicationRepository::new(client.clone());
        let result = publication
            .erase_live(mutation(&graph, "erase-native-reference").await?)
            .await?;
        anyhow::ensure!(result.state == OperationState::Erased);
        for restore in [false, true] {
            if restore {
                restore_named(&client, &backup, client.erasure_journal()?).await?;
            }
            anyhow::ensure!(client.recovery_status().ready);
            for id in &owned {
                anyhow::ensure!(
                    !native_fixture_exists(&client, SPACE, id).await?,
                    "erased native row survived {id}"
                );
            }
            for id in &independent {
                anyhow::ensure!(
                    native_fixture_exists(&client, SPACE, id).await?,
                    "independent typed neighbor removed {id}"
                );
            }
            for artifact in &removed {
                anyhow::ensure!(
                    !physical_artifact_exists(&client, &artifact.base.id).await?,
                    "native-reference-only payload survived erase/replay"
                );
            }
            for artifact in &retained {
                anyhow::ensure!(
                    artifacts.get(SPACE, &artifact.base.id).await?.as_ref() == Some(artifact),
                    "independent neighbor/prose changed"
                );
            }
            for id in &owned {
                anyhow::ensure!(
                    write_native_fixture(&client, SPACE, id, None)
                        .await
                        .is_err(),
                    "erased native identity recreated without source reference"
                );
                for data in [
                    json!({"reference":id.to_string()}),
                    json!({"encoded":serde_json::to_string(&json!({"reference":id.to_string()}))?}),
                ] {
                    let late = reference_artifact(&run, data);
                    let late_id = late.base.id.clone();
                    anyhow::ensure!(
                        artifacts.create(late).await.is_err(),
                        "late native-reference-only payload accepted after completed erase"
                    );
                    anyhow::ensure!(!physical_artifact_exists(&client, &late_id).await?);
                }
            }
            // The array/object owned IDs themselves mention the independent
            // numeric 42 record. Tombstoning a deleted identity must not also
            // tombstone that nested reference or prohibit its future producers.
            for id in &independent {
                let independent_late =
                    reference_artifact(&run, json!({"reference":id.to_string()}));
                artifacts.create(independent_late).await?;
            }
            native_partial_update_oracle(
                &client,
                &independent[0],
                &independent[1],
                &independent[2],
                &owned,
            )
            .await?;
        }
    }
    Ok(())
}

async fn storage(
    backend: support::Backend,
    directory: &std::path::Path,
) -> anyhow::Result<StorageClient> {
    let embedded = matches!(backend, support::Backend::ElephantDb);
    let config = StorageConfig {
        backend: if embedded {
            StorageBackendKind::ElephantDb
        } else {
            StorageBackendKind::SurrealDb
        },
        url: "mem://".into(),
        namespace: format!("erase_{}", uuid::Uuid::new_v4().simple()),
        database: "qualification".into(),
        ..StorageConfig::default()
    };
    let client = StorageClient::new_with_durability(
        config,
        embedded.then(|| directory.join("live.snapshot")),
        Some(directory.join("journal")),
    )
    .await?;
    if !embedded {
        elephant_migrations::apply_schema(&client)
            .await
            .map_err(anyhow::Error::msg)?;
    }
    Ok(client)
}

#[tokio::test]
async fn restoring_backup_predating_source_keeps_erased_descendant_and_native_membership_fences(
) -> anyhow::Result<()> {
    use elephant_storage::{SpaceConfigRepository, WorkArtifactRepository, WorkRunRepository};
    use surrealdb::sql::Thing;
    for backend in support::BACKENDS {
        let directory = crash_support::CrashDirectory::new()?;
        let client = storage(backend, &directory.0).await?;
        SpaceConfigRepository::new(client.clone())
            .create(graph_support::configuration(SPACE))
            .await?;
        elephant_storage::canonical_commit::ensure_extension_epoch_guards(
            &client,
            &["consumer_note".into()],
        )
        .await?;
        let entities = EntityRepository::new(client.clone());
        let baseline = entities
            .create(Entity::new(
                BaseFields::new(
                    RecordId::new_v4(TableName::Entity),
                    SPACE.into(),
                    "qualification".into(),
                ),
                EntityType::new("topic"),
                "Independent pre-source baseline".into(),
            ))
            .await?;
        let native = Thing::from(("consumer_note", "private-reference-created-after-backup"));
        anyhow::ensure!(!native_fixture_exists(&client, SPACE, &native).await?);
        let (queue, claimed, mut run) = reference_producer(&client).await?;
        let artifacts = WorkArtifactRepository::new(client.clone());
        // The old backup has this dangling opaque reference, but neither its
        // eventual source nor its eventual target. Replay needs the original
        // private membership to discover it; the source UUID is absent here.
        let historical_reference = artifacts
            .create(reference_artifact(
                &run,
                json!({"encoded":serde_json::to_string(&json!({"reference":native.to_string()}))?,
                       "text":"Historical reference-only payload requiring physical replay cleanup"}),
            ))
            .await?;
        let independent_artifact = artifacts
            .create(reference_artifact(
                &run,
                json!({"reference":baseline.base.id.to_string(),"text":"Independent historical artifact"}),
            ))
            .await?;
        run.success = true;
        run.completed_at = Some(chrono::Utc::now());
        let run_id = run.base.id.clone();
        WorkRunRepository::new(client.clone())
            .update(SPACE, &run_id, run)
            .await?;
        queue
            .for_attempt(&claimed)?
            .complete(SPACE, &claimed.base.id)
            .await?;
        // The backup cannot contain the later source or its accepted descendants.
        let backup = directory.0.join("before-source-existed");
        export_named(
            &client,
            &backup,
            "before-source-existed",
            &SnapshotRetention::default(),
        )
        .await?;
        let lineage = support::lineage_for_registered_space(
            &client,
            SPACE,
            elephant_types::records::base::SubjectsState::Complete,
            "qualification/oriole/opening",
            "oriole-revision-1",
        )
        .await?;
        let graph = GraphFixture::from_lineage(client.clone(), SPACE, lineage).await?;
        publish(&graph).await?;
        write_native_fixture(&client, SPACE, &native, Some(&graph.lineage.source.base.id)).await?;
        let assertion = AssertionRepository::new(client.clone())
            .list(SPACE, 100, None)
            .await?
            .into_iter()
            .find(|row| row.evidence.revision.id() == &graph.lineage.revision.base.id)
            .ok_or_else(|| anyhow::anyhow!("published assertion absent"))?;
        let references = [
            graph.lineage.source.base.id.to_string(),
            graph.lineage.doc.base.id.to_string(),
            graph.lineage.revision.base.id.to_string(),
            graph.lineage.node.base.id.to_string(),
            assertion.base.id.to_string(),
            native.to_string(),
        ];
        let repository = SourcePublicationRepository::new(client.clone());
        let receipt = repository
            .erase_live(mutation(&graph, "erase-after-older-backup").await?)
            .await?;
        anyhow::ensure!(receipt.state == OperationState::Erased);
        anyhow::ensure!(
            !physical_artifact_exists(&client, &historical_reference.base.id).await?,
            "original erase missed the existing reference-only historical payload"
        );
        restore_named(&client, &backup, client.erasure_journal()?).await?;
        anyhow::ensure!(client.recovery_status().ready);
        anyhow::ensure!(
            !physical_artifact_exists(&client, &historical_reference.base.id).await?,
            "old backup restored erased-reference payload without a source UUID"
        );
        anyhow::ensure!(
            artifacts
                .get(SPACE, &independent_artifact.base.id)
                .await?
                .as_ref()
                == Some(&independent_artifact),
            "historical replay removed or changed independent support"
        );
        anyhow::ensure!(entities.get(SPACE, &baseline.base.id).await?.as_ref() == Some(&baseline));
        let mut independent = Entity::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Entity),
                SPACE.into(),
                "qualification".into(),
            ),
            EntityType::new("topic"),
            "Independent post-restore write".into(),
        );
        independent.metadata = json!({"input":baseline.base.id});
        entities.create(independent).await?;
        for reference in &references {
            let mut late = Entity::new(
                BaseFields::new(
                    RecordId::new_v4(TableName::Entity),
                    SPACE.into(),
                    "qualification".into(),
                ),
                EntityType::new("topic"),
                "Late reference-only result".into(),
            );
            late.metadata = json!({"input":reference});
            let id = late.base.id.clone();
            anyhow::ensure!(entities.create(late).await.is_err(),
                "{backend:?}: restore lost erased identity membership absent from old backup: {reference}");
            anyhow::ensure!(entities.get_with_deleted(SPACE, &id).await?.is_none());
        }
        anyhow::ensure!(
            write_native_fixture(&client, SPACE, &native, None)
                .await
                .is_err(),
            "restoring pre-source backup forgot erased native identity itself"
        );
    }
    Ok(())
}

async fn publish(fixture: &GraphFixture) -> anyhow::Result<()> {
    fixture.index_fixture_vectors().await?;
    fixture.publication.finalize().await?;
    fixture
        .publication
        .queue
        .for_attempt(&fixture.publication.claimed)?
        .complete(
            &fixture.publication.space,
            &fixture.publication.claimed.base.id,
        )
        .await?;
    Ok(())
}

async fn mutation(fixture: &GraphFixture, key: &str) -> anyhow::Result<SourceMutation> {
    let repository = SourcePublicationRepository::new(fixture.publication.client.clone());
    let head = repository
        .head(&fixture.publication.space, &fixture.lineage.source.base.id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("source head missing"))?;
    Ok(SourceMutation {
        authorized_document: None,
        space_id: fixture.publication.space.clone(),
        source_id: fixture.lineage.source.base.id.clone(),
        expected: SourcePrecondition::Generation(head.generation),
        idempotency_key: key.into(),
        request_fingerprint: format!("qualification-{key}"),
        actor: "qualification".into(),
        authorized: support::authorized_source_snapshot(
            &repository,
            &fixture.publication.space,
            &fixture.lineage.source.base.id,
        )
        .await?,
    })
}

async fn search(fixture: &GraphFixture) -> anyhow::Result<Value> {
    let principal = elephant_policy::Principal {
        space_id: fixture.publication.space.clone(),
        scopes: ["read:doc:raw".into()].into(),
        clearances: vec![elephant_policy::Clearance {
            policy_id: "qualification-policy".into(),
            max_level: 7,
            labels: ["research".into()].into(),
            handling_allow: ["no-external-processing".into()].into(),
        }],
        subject_allowlist: None,
        purpose: Default::default(),
    };
    let mut dispatcher = ToolDispatcher::new(ToolContext::new(
        fixture.publication.client.clone(),
        principal,
    ));
    dispatcher.register(elephant_mcp::scoped_search::ScopedSearchHandler);
    let external = fixture
        .lineage
        .source
        .external_id
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("external identity absent"))?;
    let constraints = std::collections::BTreeMap::from([(
        external,
        fixture.publication.request.revision_id.as_str(),
    )]);
    support::mcp_success_data(dispatcher.execute("search_scoped",json!({"query":"Oriole","limit":3,"use_vector":false,"source_revision_constraints":constraints})).await?)
}

/// Raw counts include soft-deleted rows; a visibility filter cannot prove erasure.
async fn payload_counts(client: &StorageClient, space: &str) -> anyhow::Result<Value> {
    #[derive(serde::Deserialize)]
    struct Count {
        n: u64,
    }
    macro_rules! count {
        ($record:ty, $table:literal) => {{
            if let Some(store) = client.embedded_store() {
                store
                    .scan::<$record>()
                    .await?
                    .into_iter()
                    .filter(|row| row.base.space_id == space)
                    .count() as u64
            } else {
                let mut result = client
                    .query_bind_checked(
                        concat!(
                            "SELECT count() AS n FROM ",
                            $table,
                            " WHERE space_id=$space GROUP ALL"
                        ),
                        elephant_storage::bindings! {"space"=>space},
                    )
                    .await?;
                let rows: Vec<Count> = result.take(0)?;
                anyhow::ensure!(rows.len() <= 1, "malformed physical count");
                rows.first().map_or(0, |row| row.n)
            }
        }};
    }
    let mut partition_rows = 0_u64;
    if client.embedded_store().is_none() {
        for table in client.list_tables().await? {
            if !table.starts_with("emb__") {
                continue;
            }
            anyhow::ensure!(
                table
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_'),
                "unexpected partition identifier"
            );
            let mut response = client
                .query_bind_checked(
                    &format!("SELECT count() AS n FROM {table} WHERE space_id=$space GROUP ALL"),
                    elephant_storage::bindings! {"space"=>space},
                )
                .await?;
            let rows: Vec<Count> = response.take(0)?;
            anyhow::ensure!(rows.len() <= 1, "malformed physical vector count");
            partition_rows += rows.first().map_or(0, |row| row.n);
        }
    }
    Ok(json!({
        "sources":count!(Source,"source"), "documents":count!(Doc,"doc"),
        "revisions":count!(DocRevision,"doc_revision"), "nodes":count!(DocNode,"doc_node"),
        "embeddings":count!(NodeEmbedding,"chunk_embedding"), "vector_partitions":partition_rows,
        "assertions":count!(Assertion,"assertion"),
        "relations":count!(Rel,"rel"), "artifacts":count!(WorkArtifact,"work_artifact"),
        "work_items":count!(WorkItem,"work_item"), "work_runs":count!(WorkRun,"work_run"),
        "archived_items":count!(WorkItemArchive,"work_item_archive"), "archived_runs":count!(WorkRunArchive,"work_run_archive"),
        "views":count!(KnowledgeView,"knowledge_view"), "view_revisions":count!(KnowledgeViewRevision,"knowledge_view_revision"),
    }))
}

#[tokio::test]
async fn retract_hides_published_graph_and_old_finalization_cannot_reactivate_it(
) -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        let fixture = GraphFixture::new(support::storage(backend).await?, SPACE).await?;
        publish(&fixture).await?;
        anyhow::ensure!(search(&fixture).await?["results"]
            .as_array()
            .is_some_and(|rows| !rows.is_empty()));
        let repository = SourcePublicationRepository::new(fixture.publication.client.clone());
        let request = mutation(&fixture, "retract-published").await?;
        let retracted = repository.retract(request.clone()).await?;
        anyhow::ensure!(retracted.state == OperationState::Retracted);
        let response = search(&fixture).await?;
        anyhow::ensure!(response["results"] == json!([]));
        anyhow::ensure!(
            response["coverage"]["qualification/oriole/opening"]["state"] == "retracted"
        );
        anyhow::ensure!(
            fixture.publication.finalize().await.is_err(),
            "old completed finalizer revived a retracted source"
        );
        anyhow::ensure!(repository.retract(request).await? == retracted);
        let mut implicit = fixture.publication.request.clone();
        implicit.revision_id = "implicit-revival-r2".into();
        implicit.idempotency_key = "implicit-revival".into();
        implicit.request_fingerprint = "implicit-revival".into();
        implicit.expected = SourcePrecondition::Generation(retracted.source_generation.clone());
        implicit.authorized = Some(
            support::authorized_source_snapshot(
                &repository,
                SPACE,
                &fixture.lineage.source.base.id,
            )
            .await?,
        );
        anyhow::ensure!(
            repository.accept(implicit).await.is_err(),
            "ordinary replacement silently reactivated tombstone"
        );
    }
    Ok(())
}

#[tokio::test]
async fn active_graph_erasure_and_old_backup_restore_preserve_other_space_and_source_less_authority(
) -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        let directory = crash_support::CrashDirectory::new()?;
        let client = storage(backend, &directory.0).await?;
        let fixture = GraphFixture::new(client.clone(), SPACE).await?;
        publish(&fixture).await?;
        let other = GraphFixture::new(client.clone(), "erasure-independent-space").await?;
        publish(&other).await?;
        let other_before = search(&other).await?;
        let baseline = EntityRepository::new(client.clone())
            .create(Entity::new(
                BaseFields::new(
                    RecordId::new_v4(TableName::Entity),
                    SPACE.into(),
                    "qualification".into(),
                ),
                EntityType::new("topic"),
                "Independent household authority".into(),
            ))
            .await?;
        let backup = directory.0.join("registered-before-erase");
        let saved = export_named(
            &client,
            &backup,
            "before-active-erase",
            &SnapshotRetention::default(),
        )
        .await?;
        anyhow::ensure!(saved.expires_at > saved.created_at);
        let repository = SourcePublicationRepository::new(client.clone());
        let request = mutation(&fixture, "erase-published-graph").await?;
        let erased = repository.erase_live(request.clone()).await?;
        anyhow::ensure!(erased.state == OperationState::Erased && erased.completed_at.is_some());
        anyhow::ensure!(
            repository.erase_live(request.clone()).await? == erased,
            "same live erasure replay changed receipt"
        );
        for phase in ["after erase", "after restoring old backup"] {
            if phase == "after restoring old backup" {
                restore_named(&client, &backup, client.erasure_journal()?).await?;
            }
            anyhow::ensure!(client.recovery_status().ready);
            let counts = payload_counts(&client, SPACE).await?;
            for (table, count) in counts
                .as_object()
                .ok_or_else(|| anyhow::anyhow!("counts shape"))?
            {
                anyhow::ensure!(
                    *count == json!(0),
                    "{backend:?}/{phase}: residual physical payload in {table}: {count}"
                );
            }
            anyhow::ensure!(SourceRepository::new(client.clone())
                .get_with_deleted(SPACE, &fixture.lineage.source.base.id)
                .await?
                .is_none());
            anyhow::ensure!(repository
                .revision(SPACE, &fixture.lineage.source.base.id, "oriole-revision-1")
                .await?
                .is_none());
            let head = repository
                .head(SPACE, &fixture.lineage.source.base.id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("tombstone absent"))?;
            anyhow::ensure!(head.lifecycle == SourceVisibility::Erased);
            anyhow::ensure!(
                EntityRepository::new(client.clone())
                    .get(SPACE, &baseline.base.id)
                    .await?
                    == Some(baseline.clone())
            );
            anyhow::ensure!(
                EntityRepository::new(client.clone())
                    .list(SPACE, 100, None)
                    .await?
                    .len()
                    == 1
            );
            anyhow::ensure!(
                search(&other).await? == other_before,
                "erasure crossed named-space boundary"
            );
            anyhow::ensure!(search(&fixture).await?["results"] == json!([]));
        }
    }
    Ok(())
}

async fn maintenance_commit(
    client: &StorageClient,
    key: &str,
    dependencies: Vec<CanonicalRecord>,
    before: CanonicalRecord,
    after: CanonicalRecord,
    derived: bool,
) -> anyhow::Result<()> {
    let repository = CanonicalRepository::new(client.clone());
    let observed_epoch = repository.epoch(SPACE).await?;
    let mut observed = dependencies.clone();
    observed.push(before.clone());
    let maintenance = Some(CanonicalMaintenanceContext {
        attempt: None,
        origin: if derived {
            CanonicalMaintenanceOrigin::Derived
        } else {
            CanonicalMaintenanceOrigin::IndependentMutation
        },
        sources: repository
            .capture_maintenance_support(SPACE, &observed)
            .await?,
    });
    repository
        .commit(CanonicalCommit {
            space_id: SPACE.into(),
            commit_id: key.into(),
            input_fingerprint: format!("qualification-{key}"),
            replay_output: None,
            publication: None,
            finalization: None,
            maintenance,
            plan: CanonicalPlan {
                observed_epoch,
                dependencies,
                effects: vec![CanonicalEffect {
                    before: Some(before),
                    after: Some(after),
                }],
                outbox: vec![],
                ..CanonicalPlan::default()
            },
        })
        .await?;
    Ok(())
}

#[tokio::test]
async fn erasing_published_b_reopens_a_hidden_by_cross_source_decision() -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        let directory = crash_support::CrashDirectory::new()?;
        let client = storage(backend, &directory.0).await?;
        let a = GraphFixture::new(client.clone(), SPACE).await?;
        publish(&a).await?;
        let lineage = support::lineage_for_registered_space(
            &client,
            SPACE,
            SubjectsState::Complete,
            "qualification/oriole/corroboration",
            "corroboration-r1",
        )
        .await?;
        let b = GraphFixture::from_lineage(client.clone(), SPACE, lineage).await?;
        publish(&b).await?;
        let assertions = AssertionRepository::new(client.clone());
        let original = assertions
            .list(SPACE, 100, None)
            .await?
            .into_iter()
            .find(|row| row.evidence.revision.id() == &a.lineage.revision.base.id)
            .ok_or_else(|| anyhow::anyhow!("A's independently published assertion absent"))?;
        let mut hidden = original.clone();
        hidden.base.deleted_at = Some(chrono::Utc::now());
        let source_b = SourceRepository::new(client.clone())
            .get(SPACE, &b.lineage.source.base.id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("published B absent"))?;
        maintenance_commit(
            &client,
            "B-supersedes-A",
            vec![CanonicalRecord::Source(Box::new(source_b))],
            CanonicalRecord::Assertion(Box::new(original.clone())),
            CanonicalRecord::Assertion(Box::new(hidden)),
            true,
        )
        .await?;
        anyhow::ensure!(assertions.get(SPACE, &original.base.id).await?.is_none());
        SourcePublicationRepository::new(client.clone())
            .erase_live(mutation(&b, "erase-causal-B").await?)
            .await?;
        let restored = assertions
            .get(SPACE, &original.base.id)
            .await?
            .ok_or_else(|| {
                anyhow::anyhow!("erasing B did not restore independently supported A")
            })?;
        anyhow::ensure!(
            restored.base.deleted_at.is_none()
                && restored.evidence == original.evidence
                && restored.subject_text == original.subject_text
        );
        let support = SourcePublicationRepository::new(client.clone())
            .support_for_record(SPACE, &restored.base.id)
            .await?;
        anyhow::ensure!(support
            .iter()
            .any(|row| row.source_id == a.lineage.source.base.id));
        anyhow::ensure!(support
            .iter()
            .all(|row| row.source_id != b.lineage.source.base.id));
        anyhow::ensure!(!search(&a).await?["results"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("search shape"))?
            .is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn erase_reverses_nested_decision_fields_while_preserving_independent_later_metadata(
) -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        let directory = crash_support::CrashDirectory::new()?;
        let client = storage(backend, &directory.0).await?;
        let b = GraphFixture::new(client.clone(), SPACE).await?;
        publish(&b).await?;
        let entities = EntityRepository::new(client.clone());
        let mut baseline = Entity::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Entity),
                SPACE.into(),
                "qualification".into(),
            ),
            EntityType::new("topic"),
            "Independent authoritative baseline".into(),
        );
        baseline.metadata = json!({"context":{"value":"baseline"}});
        let baseline = entities.create(baseline).await?;
        let source_b = SourceRepository::new(client.clone())
            .get(SPACE, &b.lineage.source.base.id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("published B absent"))?;
        let mut derived = baseline.clone();
        derived.metadata["context"]["value"] = json!("B-private-contribution");
        maintenance_commit(
            &client,
            "B-changes-nested-baseline",
            vec![CanonicalRecord::Source(Box::new(source_b))],
            CanonicalRecord::Entity(Box::new(baseline.clone())),
            CanonicalRecord::Entity(Box::new(derived)),
            true,
        )
        .await?;
        let current = entities
            .get(SPACE, &baseline.base.id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("changed baseline absent"))?;
        let mut independent = current.clone();
        independent.metadata["context"]["independent"] = json!("retain-later-authored-field");
        maintenance_commit(
            &client,
            "independent-later-edit",
            vec![],
            CanonicalRecord::Entity(Box::new(current)),
            CanonicalRecord::Entity(Box::new(independent)),
            false,
        )
        .await?;
        SourcePublicationRepository::new(client.clone())
            .erase_live(mutation(&b, "erase-nested-B").await?)
            .await?;
        let restored = entities
            .get(SPACE, &baseline.base.id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("independent baseline erased"))?;
        anyhow::ensure!(
            restored.metadata
                == json!({"context":{"value":"baseline","independent":"retain-later-authored-field"}}),
            "nested inverse retained erased contribution or destroyed independently authored field"
        );
    }
    Ok(())
}
