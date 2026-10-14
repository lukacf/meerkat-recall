//! Populated hot/cold history through real queue attempts and archival, followed
//! by actual source erasure. No archive, source head or completion is fabricated.
//! Fixed graph interpretations exercise storage completeness, not model quality.

#[path = "../../recall_preview/crash_support.rs"]
mod crash_support;
#[path = "../../recall_preview/graph_support.rs"]
mod graph_support;
#[path = "../../recall_preview/publication_support.rs"]
mod publication_support;
#[path = "../../recall_preview/support.rs"]
mod support;

use chrono::Utc;
use elephant_mcp::handlers::identity::MergeEntitiesHandler;
use elephant_mcp::handlers::knowledge_views::RebuildKnowledgeViewHandler;
use elephant_mcp::{ToolContext, ToolDispatcher};
use elephant_pipeline::scheduler::WorkQueue;
use elephant_policy::{Clearance, Principal};
use elephant_storage::source_publication::{
    AcceptSourceRevision, SourceMutation, SourcePublicationRepository,
};
use elephant_storage::work_attempt_repository::fence_from_claim;
use elephant_storage::work_retention::archive_terminal_work;
use elephant_storage::{
    EntityRepository, KnowledgeViewRepository, MediaAssetRepository, Repository, SourceRepository,
    SpaceConfigRepository, StorageBackendKind, StorageClient, StorageConfig,
    WorkArtifactRepository, WorkItemRepository, WorkRunRepository,
};
use elephant_types::publication::{OperationState, SourcePrecondition};
use elephant_types::records::base::SubjectsState;
use elephant_types::records::work::{
    ArtifactStatus, WorkArtifact, WorkItem, WorkItemArchive, WorkItemStatus, WorkRun,
    WorkRunArchive, WorkRunMetrics,
};
use elephant_types::records::{
    DocMediaKind, KnowledgeView, KnowledgeViewDependencyIndex, KnowledgeViewRevision, MediaAsset,
    Source, SourceType,
};
use elephant_types::{Link, RecordId, TableName};
use graph_support::GraphFixture;
use serde_json::json;
use std::collections::BTreeSet;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::str::FromStr as _;

const SPACE: &str = "qualification-populated-erasure";

async fn storage(backend: support::Backend, directory: &Path) -> anyhow::Result<StorageClient> {
    let embedded = matches!(backend, support::Backend::ElephantDb);
    let client = StorageClient::new_with_durability(
        StorageConfig {
            backend: if embedded {
                StorageBackendKind::ElephantDb
            } else {
                StorageBackendKind::SurrealDb
            },
            url: "mem://".into(),
            namespace: format!("closure_{}", uuid::Uuid::new_v4().simple()),
            database: "qualification".into(),
            ..StorageConfig::default()
        },
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

struct History {
    item: WorkItem,
    run: WorkRun,
    artifact: WorkArtifact,
}

async fn completed_history(
    client: &StorageClient,
    space: &str,
    source: Option<&RecordId>,
    label: &str,
) -> anyhow::Result<History> {
    let queue = WorkQueue::with_defaults(client.clone());
    let mut base = support::classified_base(TableName::WorkItem, space);
    base.provenance.sources = source.cloned().into_iter().collect();
    let item = queue
        .enqueue(WorkItem {
            base,
            kind: "qualification_history".into(),
            payload: json!({"private_canary":label,"nested":{"original":true}}),
            status: WorkItemStatus::Runnable,
            priority: 1,
            idempotency_key: format!("{space}/{label}"),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        })
        .await?;
    let claimed = queue
        .claim_matching(|candidate| candidate.base.id == item.base.id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("original history attempt not claimed"))?;
    let fence = fence_from_claim(&claimed)?;
    let mut base = claimed.base.clone();
    base.id = RecordId::new_v4(TableName::WorkRun);
    let runs = WorkRunRepository::new(client.clone());
    let run = runs
        .create(WorkRun {
            base,
            work_item: Link::new(claimed.base.id.clone()),
            attempt: claimed.attempts,
            started_at: Utc::now(),
            completed_at: None,
            success: false,
            error: None,
            metrics: WorkRunMetrics {
                custom: [
                    ("attempt_fence".into(), serde_json::to_value(&fence)?),
                    ("original_canary".into(), json!(label)),
                ]
                .into(),
                ..WorkRunMetrics::default()
            },
            worker_id: fence.lease_token.clone(),
            worker_version: "qualification-no-provider".into(),
            model_version: None,
            prompt_id: None,
            config_hash: None,
        })
        .await?;
    let mut base = run.base.clone();
    base.id = RecordId::new_v4(TableName::WorkArtifact);
    let artifact = WorkArtifactRepository::new(client.clone())
        .create(WorkArtifact {
            base,
            work_run: Link::new(run.base.id.clone()),
            artifact_type: "qualification_history_payload".into(),
            data: json!({"original_canary":label,"retained_fields":[1,2,3]}),
            status: ArtifactStatus::Draft,
            validated_at: None,
            validation_errors: None,
            committed_at: None,
        })
        .await?;
    let mut finished = run.clone();
    finished.completed_at = Some(Utc::now());
    finished.success = true;
    let run = runs.update(space, &run.base.id, finished).await?;
    let item = queue
        .for_attempt(&claimed)?
        .complete(space, &claimed.base.id)
        .await?;
    anyhow::ensure!(item.status == WorkItemStatus::Done && item.attempts == 1);
    Ok(History {
        item,
        run,
        artifact,
    })
}

/// Actual physical identities, including soft-deleted rows. Repository history
/// reads alone cannot distinguish an archive from an invisible retained hot row.
async fn physical_ids(
    client: &StorageClient,
    space: &str,
    table: TableName,
) -> anyhow::Result<BTreeSet<RecordId>> {
    if let Some(store) = client.embedded_store() {
        macro_rules! scan {
            ($record:ty) => {
                store
                    .scan::<$record>()
                    .await?
                    .into_iter()
                    .filter(|row| row.base.space_id == space)
                    .map(|row| row.base.id)
                    .collect()
            };
        }
        return Ok(match table {
            TableName::WorkItem => scan!(WorkItem),
            TableName::WorkRun => scan!(WorkRun),
            TableName::WorkItemArchive => scan!(WorkItemArchive),
            TableName::WorkRunArchive => scan!(WorkRunArchive),
            TableName::WorkArtifact => scan!(WorkArtifact),
            TableName::MediaAsset => scan!(MediaAsset),
            TableName::KnowledgeView => scan!(KnowledgeView),
            TableName::KnowledgeViewRevision => scan!(KnowledgeViewRevision),
            TableName::KnowledgeViewDependency => scan!(KnowledgeViewDependencyIndex),
            _ => anyhow::bail!("unreviewed physical history table"),
        });
    }
    anyhow::ensure!(matches!(
        table,
        TableName::WorkItem
            | TableName::WorkRun
            | TableName::WorkItemArchive
            | TableName::WorkRunArchive
            | TableName::WorkArtifact
            | TableName::MediaAsset
            | TableName::KnowledgeView
            | TableName::KnowledgeViewRevision
            | TableName::KnowledgeViewDependency
    ));
    let mut response = client
        .query_bind_checked(
            &format!(
                "SELECT VALUE id FROM {} WHERE space_id=$space",
                table.as_str()
            ),
            elephant_storage::bindings! { "space" => space },
        )
        .await?;
    let ids: Vec<surrealdb::sql::Thing> = response.take(0)?;
    ids.iter()
        .map(elephant_storage::surreal::record_id_from_thing)
        .collect::<Result<_, _>>()
        .map_err(Into::into)
}

async fn assert_history_unchanged(client: &StorageClient, row: &History) -> anyhow::Result<()> {
    let space = &row.item.base.space_id;
    // Full domain equality includes original payload, errors, metrics, times,
    // security and provenance; no implementation fingerprint is used as oracle.
    anyhow::ensure!(
        WorkItemRepository::new(client.clone())
            .get(space, &row.item.base.id)
            .await?
            == Some(row.item.clone()),
        "independent hot/cold work changed"
    );
    anyhow::ensure!(
        WorkRunRepository::new(client.clone())
            .get(space, &row.run.base.id)
            .await?
            == Some(row.run.clone()),
        "independent hot/cold execution changed"
    );
    anyhow::ensure!(
        WorkArtifactRepository::new(client.clone())
            .get(space, &row.artifact.base.id)
            .await?
            == Some(row.artifact.clone()),
        "independent artifact changed"
    );
    Ok(())
}

async fn erase(client: &StorageClient, source: &RecordId) -> anyhow::Result<()> {
    let repository = SourcePublicationRepository::new(client.clone());
    let head = repository
        .head(SPACE, source)
        .await?
        .ok_or_else(|| anyhow::anyhow!("original source head missing"))?;
    let erased = Box::pin(repository.erase_live(SourceMutation {
        space_id: SPACE.into(),
        source_id: source.clone(),
        expected: SourcePrecondition::Generation(head.generation),
        idempotency_key: format!("erase-closure:{source}"),
        request_fingerprint: format!("erase-closure:{source}"),
        actor: "qualification".into(),
        authorized: support::authorized_source_snapshot(&repository, SPACE, source).await?,
        authorized_document: None,
    }))
    .await?;
    anyhow::ensure!(erased.state == OperationState::Erased && erased.completed_at.is_some());
    anyhow::ensure!(client.recovery_status().ready);
    Ok(())
}

#[tokio::test]
async fn erasure_physically_removes_populated_hot_and_cold_history_without_changing_independent_rows(
) -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        let directory = crash_support::CrashDirectory::new()?;
        let client = storage(backend, &directory.0).await?;
        let a = GraphFixture::new(client.clone(), SPACE).await?;
        a.index_fixture_vectors().await?;
        a.publication.finalize().await?;
        let b = GraphFixture::from_lineage(
            client.clone(),
            SPACE,
            support::lineage_for_registered_space(
                &client,
                SPACE,
                SubjectsState::Complete,
                "independent-history-source",
                "independent-history-r1",
            )
            .await?,
        )
        .await?;
        b.index_fixture_vectors().await?;
        b.publication.finalize().await?;
        let a_id = &a.lineage.source.base.id;
        let b_id = &b.lineage.source.base.id;
        let cold_a = completed_history(&client, SPACE, Some(a_id), "erase-cold-canary").await?;
        let cold_b = completed_history(&client, SPACE, Some(b_id), "retain-cold-canary").await?;
        let independent =
            completed_history(&client, SPACE, None, "source-free-cold-canary").await?;
        let moved =
            archive_terminal_work(&client, Utc::now() + chrono::Duration::seconds(1), 1000).await?;
        anyhow::ensure!(
            moved.items_archived >= 3 && moved.runs_archived >= 3,
            "cold-history fixture did not execute actual archival"
        );
        for history in [&cold_a, &cold_b, &independent] {
            for (hot, cold, id) in [
                (
                    TableName::WorkItem,
                    TableName::WorkItemArchive,
                    &history.item.base.id,
                ),
                (
                    TableName::WorkRun,
                    TableName::WorkRunArchive,
                    &history.run.base.id,
                ),
            ] {
                anyhow::ensure!(!physical_ids(&client, SPACE, hot).await?.contains(id));
                anyhow::ensure!(
                    physical_ids(&client, SPACE, cold)
                        .await?
                        .contains(&RecordId::new(cold, id.id())),
                    "archive payload never populated"
                );
            }
            assert_history_unchanged(&client, history).await?;
        }
        let hot_a = completed_history(&client, SPACE, Some(a_id), "erase-hot-canary").await?;
        let hot_b = completed_history(&client, SPACE, Some(b_id), "retain-hot-canary").await?;
        SpaceConfigRepository::new(client.clone())
            .create(graph_support::configuration("independent-other-space"))
            .await?;
        let other = completed_history(
            &client,
            "independent-other-space",
            None,
            "other-space-canary",
        )
        .await?;
        for history in [&hot_a, &hot_b] {
            anyhow::ensure!(physical_ids(&client, SPACE, TableName::WorkItem)
                .await?
                .contains(&history.item.base.id));
            anyhow::ensure!(physical_ids(&client, SPACE, TableName::WorkRun)
                .await?
                .contains(&history.run.base.id));
        }
        erase(&client, a_id).await?;
        for history in [&cold_a, &hot_a] {
            for (hot, cold, id) in [
                (
                    TableName::WorkItem,
                    TableName::WorkItemArchive,
                    &history.item.base.id,
                ),
                (
                    TableName::WorkRun,
                    TableName::WorkRunArchive,
                    &history.run.base.id,
                ),
            ] {
                anyhow::ensure!(
                    !physical_ids(&client, SPACE, hot).await?.contains(id),
                    "erased hot payload remains"
                );
                anyhow::ensure!(
                    !physical_ids(&client, SPACE, cold)
                        .await?
                        .contains(&RecordId::new(cold, id.id())),
                    "erased cold payload remains"
                );
            }
            anyhow::ensure!(
                !physical_ids(&client, SPACE, TableName::WorkArtifact)
                    .await?
                    .contains(&history.artifact.base.id),
                "erased staged payload remains"
            );
            anyhow::ensure!(WorkItemRepository::new(client.clone())
                .get(SPACE, &history.item.base.id)
                .await?
                .is_none());
            anyhow::ensure!(WorkRunRepository::new(client.clone())
                .get(SPACE, &history.run.base.id)
                .await?
                .is_none());
        }
        for history in [&cold_b, &hot_b, &independent, &other] {
            assert_history_unchanged(&client, history).await?;
        }
        for history in [&cold_b, &independent] {
            for (hot, cold, id) in [
                (
                    TableName::WorkItem,
                    TableName::WorkItemArchive,
                    &history.item.base.id,
                ),
                (
                    TableName::WorkRun,
                    TableName::WorkRunArchive,
                    &history.run.base.id,
                ),
            ] {
                anyhow::ensure!(!physical_ids(&client, SPACE, hot).await?.contains(id));
                anyhow::ensure!(
                    physical_ids(&client, SPACE, cold)
                        .await?
                        .contains(&RecordId::new(cold, id.id())),
                    "independent archive physically removed"
                );
            }
        }
        anyhow::ensure!(physical_ids(&client, SPACE, TableName::WorkItem)
            .await?
            .contains(&hot_b.item.base.id));
        anyhow::ensure!(physical_ids(&client, SPACE, TableName::WorkRun)
            .await?
            .contains(&hot_b.run.base.id));
        anyhow::ensure!(SourceRepository::new(client.clone())
            .get_with_deleted(SPACE, a_id)
            .await?
            .is_none());
        anyhow::ensure!(SourceRepository::new(client.clone())
            .get(SPACE, b_id)
            .await?
            .is_some());
    }
    Ok(())
}

/// Own only files created with exclusive creation; never change the process
/// asset-root environment, delete a shared directory, or remove existing files.
#[derive(Default)]
struct OwnedAssetFiles(Vec<PathBuf>);

impl OwnedAssetFiles {
    fn write(&mut self, asset: &MediaAsset, bytes: &[u8]) -> anyhow::Result<PathBuf> {
        let path = elephant_types::records::media_asset::managed_asset_path(&asset.base.id);
        let parent = path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("asset parent absent"))?;
        std::fs::create_dir_all(parent)?;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        self.0.push(path.clone());
        file.write_all(bytes)?;
        file.sync_all()?;
        Ok(path)
    }
}

impl Drop for OwnedAssetFiles {
    fn drop(&mut self) {
        for path in &self.0 {
            let _ = std::fs::remove_file(path);
        }
    }
}

fn asset(label: &str, payload: &str, encoded: &str) -> MediaAsset {
    let base = support::classified_base(TableName::MediaAsset, SPACE);
    MediaAsset {
        storage_path: elephant_types::records::media_asset::managed_asset_relative_path(&base.id),
        base,
        kind: DocMediaKind::Image,
        media_type: "image/png".into(),
        size_bytes: payload.len() as u64,
        sha256: support::digest(payload),
        payload_base64: Some(encoded.into()),
        duration_ms: None,
        label: Some(label.into()),
    }
}

async fn accepted_asset_owner(
    client: &StorageClient,
    name: &str,
    assets: &[MediaAsset],
) -> anyhow::Result<RecordId> {
    let mut source = Source::new(
        support::classified_base(TableName::Source, SPACE),
        SourceType::Manual,
        support::digest(name),
    );
    source.external_id = Some(name.into());
    // Trusted imported sharing is intentional. PreparedSourceIntake copies
    // assets per source; this does not claim public intake pools media blobs.
    source.metadata = json!({"_manual_doc":{"_intake_assets":assets},"original_owner":name});
    let accepted = SourcePublicationRepository::new(client.clone())
        .accept(AcceptSourceRevision {
            source,
            revision_id: "shared-asset-r1".into(),
            expected: SourcePrecondition::Absent,
            idempotency_key: format!("asset-owner:{name}"),
            request_fingerprint: support::digest(name),
            reactivate: false,
            recreate_after_erasure: false,
            authorized: None,
            authorized_document: None,
        })
        .await?;
    Ok(accepted.source_id)
}

#[tokio::test]
async fn trusted_shared_media_preserves_exact_surviving_owner_and_bytes_until_last_owner_erased(
) -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        let directory = crash_support::CrashDirectory::new()?;
        let client = storage(backend, &directory.0).await?;
        SpaceConfigRepository::new(client.clone())
            .create(graph_support::configuration(SPACE))
            .await?;
        let assets = MediaAssetRepository::new(client.clone());
        // Storage canaries, not decodable-image or multimodal semantic fixtures.
        let unique = asset("unique-A", "D-A!", "RC1BIQ==");
        let shared = asset("shared-A-B", "D-S!", "RC1TIQ==");
        let independent = asset("independent-B", "D-B!", "RC1CIQ==");
        let mut owned = OwnedAssetFiles::default();
        let mut paths = Vec::new();
        for (row, bytes) in [
            (&unique, b"D-A!"),
            (&shared, b"D-S!"),
            (&independent, b"D-B!"),
        ] {
            assets.create(row.clone()).await?;
            paths.push(owned.write(row, bytes)?);
            anyhow::ensure!(
                assets.get_with_payload(SPACE, &row.base.id).await? == Some(row.clone())
            );
        }
        let a = accepted_asset_owner(&client, "asset-owner-a", &[unique.clone(), shared.clone()])
            .await?;
        let b = accepted_asset_owner(
            &client,
            "asset-owner-b",
            &[shared.clone(), independent.clone()],
        )
        .await?;
        let publication = SourcePublicationRepository::new(client.clone());
        let before_b = SourceRepository::new(client.clone())
            .get(SPACE, &b)
            .await?
            .ok_or_else(|| anyhow::anyhow!("independent accepted source absent"))?;
        let before_revision = publication
            .revision(SPACE, &b, "shared-asset-r1")
            .await?
            .ok_or_else(|| anyhow::anyhow!("independent immutable source revision absent"))?;
        let before_ids = physical_ids(&client, SPACE, TableName::MediaAsset).await?;
        anyhow::ensure!([&unique, &shared, &independent]
            .iter()
            .all(|row| before_ids.contains(&row.base.id)));

        erase(&client, &a).await?;
        let after_ids = physical_ids(&client, SPACE, TableName::MediaAsset).await?;
        anyhow::ensure!(
            !after_ids.contains(&unique.base.id) && !paths[0].exists(),
            "unique media row or file survived physical erasure"
        );
        anyhow::ensure!(assets
            .get_with_payload(SPACE, &unique.base.id)
            .await?
            .is_none());
        for (row, path, bytes) in [
            (&shared, &paths[1], b"D-S!"),
            (&independent, &paths[2], b"D-B!"),
        ] {
            anyhow::ensure!(
                after_ids.contains(&row.base.id),
                "independently owned media physically removed"
            );
            anyhow::ensure!(
                assets.get_with_payload(SPACE, &row.base.id).await? == Some(row.clone()),
                "independent media metadata or inline bytes changed"
            );
            anyhow::ensure!(
                std::fs::read(path)? == bytes,
                "independent managed file bytes changed"
            );
        }
        anyhow::ensure!(
            SourceRepository::new(client.clone()).get(SPACE, &b).await? == Some(before_b)
        );
        let after_revision = publication
            .revision(SPACE, &b, "shared-asset-r1")
            .await?
            .ok_or_else(|| anyhow::anyhow!("independent immutable revision disappeared"))?;
        // SourceRevision has no PartialEq; compare every serialized domain
        // value, preserving payload, classification and all timestamps.
        anyhow::ensure!(
            serde_json::to_value(&after_revision)? == serde_json::to_value(&before_revision)?,
            "independent immutable source revision changed"
        );
        anyhow::ensure!(SourceRepository::new(client.clone())
            .get_with_deleted(SPACE, &a)
            .await?
            .is_none());

        // Surviving reference is the reason for retention, not a permanent leak.
        erase(&client, &b).await?;
        let final_ids = physical_ids(&client, SPACE, TableName::MediaAsset).await?;
        for row in [&unique, &shared, &independent] {
            anyhow::ensure!(!final_ids.contains(&row.base.id));
            anyhow::ensure!(assets
                .get_with_payload(SPACE, &row.base.id)
                .await?
                .is_none());
        }
        anyhow::ensure!(paths.iter().all(|path| !path.exists()));
    }
    Ok(())
}

struct Projection {
    view: KnowledgeView,
    revision: KnowledgeViewRevision,
    indexes: Vec<KnowledgeViewDependencyIndex>,
}

fn projection_future<F: std::future::Future>(
    phase: &'static str,
    future: F,
) -> std::pin::Pin<Box<F>> {
    eprintln!(
        "erasure projection phase={phase} future_bytes={}",
        std::mem::size_of::<F>()
    );
    Box::pin(future)
}

fn projection_principal() -> Principal {
    Principal {
        space_id: SPACE.into(),
        scopes: [
            "read:source",
            "write:source",
            "read:entity",
            "write:entity",
            "read:relationship",
            "write:relationship",
            "read:event",
            "write:event",
            "read:doc:raw",
            "read:assertion",
            "write:assertion",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect(),
        clearances: vec![Clearance {
            policy_id: "qualification-policy".into(),
            max_level: 7,
            labels: ["research".into()].into(),
            handling_allow: ["no-external-processing".into()].into(),
        }],
        subject_allowlist: None,
        purpose: Default::default(),
    }
}

async fn merge_published_subjects(
    client: &StorageClient,
    source_a: &RecordId,
    source_b: &RecordId,
) -> anyhow::Result<RecordId> {
    let entities = EntityRepository::new(client.clone())
        .list(SPACE, 100, None)
        .await?;
    let select = |source: &RecordId| -> anyhow::Result<RecordId> {
        let matching: Vec<_> = entities
            .iter()
            .filter(|entity| {
                entity.display_name == "Élodie"
                    && entity.base.provenance.sources == [source.clone()]
            })
            .collect();
        anyhow::ensure!(
            matching.len() == 1,
            "expected one independently published subject in the exact source partition"
        );
        Ok(matching[0].base.id.clone())
    };
    let subject_a = select(source_a)?;
    let subject_b = select(source_b)?;
    anyhow::ensure!(subject_a != subject_b);
    let context = ToolContext::new(client.clone(), projection_principal());
    anyhow::ensure!(!context.authz.is_skip_auth());
    let mut dispatcher = ToolDispatcher::new(context);
    dispatcher.register(MergeEntitiesHandler::new(client.clone()));
    let response = support::mcp_success_data(
        projection_future(
            "merge_dispatch",
            dispatcher.execute(
                "merge_entities",
                json!({"entity_a":subject_a,"entity_b":subject_b,"strategy":"prefer_b"}),
            ),
        )
        .await?,
    )?;
    anyhow::ensure!(response["merged_id"] == subject_a.to_string());
    anyhow::ensure!(response["survivor"]["id"] == subject_b.to_string());
    Ok(subject_b)
}

async fn rebuild_projection(
    client: &StorageClient,
    root: serde_json::Value,
) -> anyhow::Result<Projection> {
    let context = ToolContext::new(client.clone(), projection_principal());
    anyhow::ensure!(!context.authz.is_skip_auth());
    let mut dispatcher = ToolDispatcher::new(context);
    dispatcher.register(RebuildKnowledgeViewHandler::new(client.clone()));
    let response = support::mcp_success_data(
        projection_future(
            "rebuild_dispatch",
            dispatcher.execute("rebuild_knowledge_view", root),
        )
        .await?,
    )?;
    anyhow::ensure!(response["changed"] == true && response["stale_fallback"] == false);
    let id = RecordId::from_str(
        response["view"]["id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("published view identity missing"))?,
    )?;
    let repository = KnowledgeViewRepository::new(client.clone());
    let view = repository
        .get_view(SPACE, &id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("actual view publication missing"))?;
    let revision = repository
        .get_current_revision(&view)
        .await?
        .ok_or_else(|| anyhow::anyhow!("actual view revision missing"))?;
    let indexes = repository.list_dependency_indexes(SPACE, &id).await?;
    anyhow::ensure!(!indexes.is_empty() && !revision.dependencies.is_empty());
    anyhow::ensure!(
        view.base.security == support::classified_base(TableName::KnowledgeView, SPACE).security
    );
    anyhow::ensure!(revision.base.security == view.base.security);
    Ok(Projection {
        view,
        revision,
        indexes,
    })
}

#[tokio::test]
async fn erased_evidence_removes_source_and_mixed_view_history_but_preserves_independent_projection(
) -> anyhow::Result<()> {
    Box::pin(erased_projection_history()).await
}

async fn erased_projection_history() -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        eprintln!("erasure projection phase=backend_begin backend={backend:?}");
        let directory = crash_support::CrashDirectory::new()?;
        let client = projection_future("storage", storage(backend, &directory.0)).await?;
        let a = projection_future("graph_a", GraphFixture::new(client.clone(), SPACE)).await?;
        projection_future("index_a", a.index_fixture_vectors()).await?;
        projection_future("finalize_a", a.publication.finalize()).await?;
        let lineage_b = projection_future(
            "lineage_b",
            support::lineage_for_registered_space(
                &client,
                SPACE,
                SubjectsState::Complete,
                "view-owner-b",
                "view-owner-b-r1",
            ),
        )
        .await?;
        let b = projection_future(
            "graph_b",
            GraphFixture::from_lineage(client.clone(), SPACE, lineage_b),
        )
        .await?;
        projection_future("index_b", b.index_fixture_vectors()).await?;
        projection_future("finalize_b", b.publication.finalize()).await?;
        eprintln!("erasure projection phase=both_original_publications_complete");
        let a_id = &a.lineage.source.base.id;
        let b_id = &b.lineage.source.base.id;
        // Publication intentionally partitions entities by original source.
        // Establish shared identity with the actual authorized merge operation
        // before freezing any source-only survivor view or history baseline.
        let subject = projection_future(
            "merge_subjects",
            merge_published_subjects(&client, a_id, b_id),
        )
        .await?;
        eprintln!("erasure projection phase=original_merge_complete");
        let a_old = projection_future(
            "source_a_old",
            rebuild_projection(&client, json!({"source_id":a_id})),
        )
        .await?;
        let a_new = projection_future(
            "source_a_new",
            rebuild_projection(&client, json!({"source_id":a_id})),
        )
        .await?;
        let b_old = projection_future(
            "source_b_old",
            rebuild_projection(&client, json!({"source_id":b_id})),
        )
        .await?;
        let b_new = projection_future(
            "source_b_new",
            rebuild_projection(&client, json!({"source_id":b_id})),
        )
        .await?;
        anyhow::ensure!(
            a_old.view.base.id == a_new.view.base.id
                && a_old.revision.base.id != a_new.revision.base.id
        );
        anyhow::ensure!(
            b_old.view.base.id == b_new.view.base.id
                && b_old.revision.base.id != b_new.revision.base.id
        );
        let mixed_old = projection_future(
            "mixed_old",
            rebuild_projection(&client, json!({"entity_id":subject})),
        )
        .await?;
        let mixed_new = projection_future(
            "mixed_new",
            rebuild_projection(&client, json!({"entity_id":subject})),
        )
        .await?;
        eprintln!("erasure projection phase=all_original_projections_complete");
        for projection in [&mixed_old, &mixed_new] {
            for original in [&a.lineage.revision.base.id, &b.lineage.revision.base.id] {
                anyhow::ensure!(
                    projection.revision.citations.iter().any(|citation| citation
                        .evidence_span
                        .as_ref()
                        .is_some_and(|span| span.revision.id() == original)),
                    "mixed-view precondition lacks one independently published source's evidence"
                );
            }
        }
        for projection in [&b_old, &b_new] {
            anyhow::ensure!(projection
                .revision
                .citations
                .iter()
                .any(|citation| citation.source_id.as_deref() == Some(b_id.to_string().as_str())));
            anyhow::ensure!(!projection
                .revision
                .citations
                .iter()
                .any(|citation| citation.source_id.as_deref() == Some(a_id.to_string().as_str())));
        }
        let revisions_before =
            physical_ids(&client, SPACE, TableName::KnowledgeViewRevision).await?;
        for projection in [&a_old, &a_new, &b_old, &b_new, &mixed_old, &mixed_new] {
            anyhow::ensure!(revisions_before.contains(&projection.revision.base.id));
        }
        projection_future("erase_original_a", erase(&client, a_id)).await?;
        eprintln!("erasure projection phase=original_erasure_complete");
        let views_after = physical_ids(&client, SPACE, TableName::KnowledgeView).await?;
        let revisions_after =
            physical_ids(&client, SPACE, TableName::KnowledgeViewRevision).await?;
        let indexes_after =
            physical_ids(&client, SPACE, TableName::KnowledgeViewDependency).await?;
        let repository = KnowledgeViewRepository::new(client.clone());
        for projection in [&a_old, &a_new, &mixed_old, &mixed_new] {
            anyhow::ensure!(
                !views_after.contains(&projection.view.base.id),
                "view containing erased evidence retained its head"
            );
            anyhow::ensure!(
                !revisions_after.contains(&projection.revision.base.id),
                "immutable view history retained erased evidence"
            );
            anyhow::ensure!(projection
                .indexes
                .iter()
                .all(|edge| !indexes_after.contains(&edge.base.id)));
            anyhow::ensure!(repository
                .get_view(SPACE, &projection.view.base.id)
                .await?
                .is_none());
            anyhow::ensure!(repository
                .get_revision(SPACE, &projection.revision.base.id)
                .await?
                .is_none());
        }
        anyhow::ensure!(views_after.contains(&b_new.view.base.id));
        anyhow::ensure!(
            repository.get_view(SPACE, &b_new.view.base.id).await? == Some(b_new.view.clone()),
            "independent projection changed"
        );
        for projection in [&b_old, &b_new] {
            anyhow::ensure!(revisions_after.contains(&projection.revision.base.id));
            anyhow::ensure!(
                repository
                    .get_revision(SPACE, &projection.revision.base.id)
                    .await?
                    == Some(projection.revision.clone()),
                "independent immutable view history changed"
            );
        }
        anyhow::ensure!(
            repository
                .list_dependency_indexes(SPACE, &b_new.view.base.id)
                .await?
                == b_new.indexes
        );
        anyhow::ensure!(b_new
            .indexes
            .iter()
            .all(|edge| indexes_after.contains(&edge.base.id)));
    }
    Ok(())
}

#[test]
fn erasure_projection_child_futures_fit_default_stack_budget() {
    fn bytes<Args, F, Fut>(_: F) -> usize
    where
        F: FnOnce(Args) -> Fut,
        Fut: std::future::Future,
    {
        std::mem::size_of::<Fut>()
    }
    // Infer actual future types without constructing/polling storage, handlers,
    // source publications or the fixture. No larger thread stack is requested.
    for (phase, size) in [
        (
            "projection history",
            bytes(|()| erased_projection_history()),
        ),
        (
            "merge published subjects",
            bytes(
                |(storage, a, b): (
                    &'static StorageClient,
                    &'static RecordId,
                    &'static RecordId,
                )| { merge_published_subjects(storage, a, b) },
            ),
        ),
        (
            "rebuild projection",
            bytes(
                |(storage, root): (&'static StorageClient, serde_json::Value)| {
                    rebuild_projection(storage, root)
                },
            ),
        ),
        (
            "erase original source",
            bytes(
                |(storage, source): (&'static StorageClient, &'static RecordId)| {
                    erase(storage, source)
                },
            ),
        ),
    ] {
        assert!(size <= 128 * 1024, "{phase} future stores {size} bytes");
    }
}
