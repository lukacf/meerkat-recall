//! Independent whole-source expected-set oracles against production finalization.
//! Metadata mode tests completion mechanics; it does not establish graph search.

#[path = "../../recall_preview/publication_support.rs"]
mod publication_support;
#[path = "../../recall_preview/support.rs"]
mod support;

use elephant_storage::source_publication::SourcePublicationRepository;
use elephant_storage::source_publication_manifest::SourcePublicationManifestRepository;
use elephant_storage::{
    Repository, SourceRepository, SpaceConfigRepository, WorkArtifactRepository,
};
use elephant_types::publication::OperationState;
use elephant_types::records::ingest::IngestMode;
use elephant_types::records::source::SourceLifecycleStatus;
use elephant_types::records::work::ArtifactStatus;
use elephant_types::{RecordId, TableName};
use publication_support::MetadataFixture;
use std::collections::BTreeSet;

async fn no_completion(fixture: &MetadataFixture) -> anyhow::Result<()> {
    let sources = SourcePublicationRepository::new(fixture.client.clone());
    let head = sources
        .head(&fixture.space, &fixture.accepted.source_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("head missing"))?;
    anyhow::ensure!(head.published_revision.is_none());
    let operation = sources
        .operation(&fixture.space, &fixture.accepted.operation_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("operation missing"))?;
    anyhow::ensure!(
        operation.state != OperationState::Completed && operation.completed_at.is_none()
    );
    anyhow::ensure!(
        SourcePublicationManifestRepository::new(fixture.client.clone())
            .completion(&fixture.space, &fixture.publication)
            .await?
            .is_none()
    );
    Ok(())
}

#[tokio::test]
async fn complete_metadata_requires_every_declared_bundle_then_replays_one_receipt(
) -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        let client = support::storage(backend).await?;
        let fixture = MetadataFixture::new(client.clone(), "finalization-business", 2, 1).await?;
        let manifests = SourcePublicationManifestRepository::new(client.clone());
        // A sealed completion proof captures every original metadata bundle.
        // The missing declared sibling is therefore refused during capture,
        // before a ready proof can reach the canonical transaction check.
        let requirements = manifests
            .requirements(&fixture.space, &fixture.publication)
            .await;
        anyhow::ensure!(
            matches!(requirements, Err(elephant_storage::StorageError::Conflict(ref code))
                if code == "source_publication_incomplete:original_metadata_bundle_missing"),
            "unfinished declared sibling disappeared from readiness"
        );
        anyhow::ensure!(
            fixture.finalize().await.is_err(),
            "finished subset published the entire source"
        );
        no_completion(&fixture).await?;
        let artifacts = WorkArtifactRepository::new(client.clone());
        let first = artifacts
            .get(&fixture.space, &fixture.artifacts[0].1.base.id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("first bundle disappeared"))?;
        anyhow::ensure!(first.status == ArtifactStatus::Validated && first.committed_at.is_none());
        fixture.stage_pair(1).await?;
        let requirements = manifests
            .requirements(&fixture.space, &fixture.publication)
            .await?;
        manifests.check_ready(&requirements).await?;
        let receipt = fixture.finalize().await?;
        let source = SourceRepository::new(client.clone())
            .get(&fixture.space, &fixture.accepted.source_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("source disappeared"))?;
        anyhow::ensure!(source.current_ingest_mode == Some(IngestMode::Metadata));
        anyhow::ensure!(source.lifecycle_status == SourceLifecycleStatus::MetadataReady);
        anyhow::ensure!(
            source.metadata["_normalized_source_metadata"]["normalized_title"] == "Oriole metadata",
            "metadata completion omitted its normalized source projection"
        );
        let operation = SourcePublicationRepository::new(client.clone())
            .operation(&fixture.space, &fixture.accepted.operation_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("operation missing"))?;
        anyhow::ensure!(
            operation.state == OperationState::Completed && operation.completed_at.is_some()
        );
        anyhow::ensure!(operation.published_revision.as_deref() == Some("metadata-r1"));
        for (candidate, bundle) in &fixture.artifacts {
            for original in [candidate, bundle] {
                let row = artifacts
                    .get(&fixture.space, &original.base.id)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("declared artifact missing after completion"))?;
                anyhow::ensure!(
                    row.status == ArtifactStatus::Committed && row.committed_at.is_some()
                );
                anyhow::ensure!(receipt.effect_ids.contains(&row.base.id));
            }
        }
        anyhow::ensure!(receipt.effect_ids.contains(&fixture.accepted.source_id));
        anyhow::ensure!(
            fixture.finalize().await? == receipt,
            "retry produced a different semantic receipt"
        );
        anyhow::ensure!(
            manifests.pending_all().await?.is_empty(),
            "completed operation remained pending"
        );
    }
    Ok(())
}

#[tokio::test]
async fn sealed_expected_work_cannot_shrink_grow_or_change_after_partial_progress(
) -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        let client = support::storage(backend).await?;
        let fixture = MetadataFixture::new(client.clone(), "finalization-household", 2, 1).await?;
        let manifests = SourcePublicationManifestRepository::new(client);
        anyhow::ensure!(
            manifests
                .set_operation_plan(fixture.manifest.clone())
                .await?
                == fixture.manifest
        );
        for grow in [false, true] {
            let mut changed = fixture.manifest.clone();
            if grow {
                changed
                    .metadata_artifact_ids
                    .push(RecordId::new_v4(TableName::WorkArtifact));
            } else {
                changed.metadata_artifact_ids.pop();
            }
            anyhow::ensure!(
                manifests.set_operation_plan(changed).await.is_err(),
                "sealed fanout changed after a child finished"
            );
            anyhow::ensure!(
                manifests.get(&fixture.space, &fixture.publication).await?
                    == Some(fixture.manifest.clone())
            );
        }
        no_completion(&fixture).await?;
    }
    Ok(())
}

#[tokio::test]
async fn bundle_cannot_complete_after_its_validated_candidate_was_changed() -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        let client = support::storage(backend).await?;
        let fixture = MetadataFixture::new(client.clone(), "finalization-business", 1, 1).await?;
        let manifests = SourcePublicationManifestRepository::new(client.clone());
        manifests
            .check_ready(
                &manifests
                    .requirements(&fixture.space, &fixture.publication)
                    .await?,
            )
            .await?;
        let artifacts = WorkArtifactRepository::new(client);
        let mut candidate = artifacts
            .get(&fixture.space, &fixture.artifacts[0].0.base.id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("candidate missing"))?;
        candidate.data["normalized_title"] = serde_json::json!("Unrelated metadata");
        let candidate_id = candidate.base.id.clone();
        artifacts
            .update(&fixture.space, &candidate_id, candidate)
            .await?;
        anyhow::ensure!(
            fixture.finalize().await.is_err(),
            "bundle ignored changed validated semantics"
        );
        no_completion(&fixture).await?;
    }
    Ok(())
}

#[tokio::test]
async fn quiet_named_spaces_remain_discoverable_when_all_queue_items_are_done() -> anyhow::Result<()>
{
    for backend in support::BACKENDS {
        let client = support::storage(backend).await?;
        let mut expected = BTreeSet::new();
        for space in ["quiet-household", "quiet-business"] {
            let fixture = MetadataFixture::new(client.clone(), space, 1, 0).await?;
            fixture
                .queue
                .for_attempt(&fixture.claimed)?
                .complete(space, &fixture.claimed.base.id)
                .await?;
            expected.insert((space.to_string(), fixture.accepted.operation_id.clone()));
            no_completion(&fixture).await?;
        }
        let pending = SourcePublicationManifestRepository::new(client)
            .pending_all()
            .await?;
        let actual: BTreeSet<_> = pending
            .into_iter()
            .map(|manifest| (manifest.space_id, manifest.publication.operation_id))
            .collect();
        anyhow::ensure!(
            actual == expected,
            "unfinished source recovery depended on a runnable/default-space item"
        );
    }
    Ok(())
}

#[tokio::test]
async fn deleted_configuration_revokes_a_previously_ready_source_finalizer() -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        let client = support::storage(backend).await?;
        let fixture = MetadataFixture::new(client.clone(), "finalization-business", 1, 1).await?;
        let manifests = SourcePublicationManifestRepository::new(client.clone());
        manifests
            .check_ready(
                &manifests
                    .requirements(&fixture.space, &fixture.publication)
                    .await?,
            )
            .await?;
        SpaceConfigRepository::new(client)
            .delete(&fixture.space, &fixture.configuration.base.id)
            .await?;
        anyhow::ensure!(
            fixture.finalize().await.is_err(),
            "deleted space authority completed publication"
        );
        no_completion(&fixture).await?;
    }
    Ok(())
}

#[tokio::test]
async fn registered_bundle_waits_for_its_original_commit_stage_to_finish() -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        let client = support::storage(backend).await?;
        let fixture = MetadataFixture::new(client.clone(), "finalization-business", 1, 0).await?;
        let stage = fixture.stage_pair_before_queue_completion(0).await?;
        anyhow::ensure!(stage.base.id != fixture.claimed.base.id);
        anyhow::ensure!(
            fixture.finalize().await.is_err(),
            "registered bundle bypassed predecessor completion"
        );
        no_completion(&fixture).await?;
        fixture
            .queue
            .for_attempt(&stage)?
            .complete(&fixture.space, &stage.base.id)
            .await?;
        let receipt = fixture.finalize().await?;
        anyhow::ensure!(receipt.source_revision.as_deref() == Some("metadata-r1"));
    }
    Ok(())
}
