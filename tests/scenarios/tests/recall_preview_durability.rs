//! D-owned checks of the actual named backup/restore library, never a mock store.
//! Requires the coherent durability + integrity replay join. No installed artifact claim.

use std::path::{Path, PathBuf};
use std::time::Duration;

use elephant_migrations::apply_schema;
use elephant_storage::snapshot::{
    cleanup_expired, export_named, restore_named, verify, SnapshotRetention,
};
use elephant_storage::{
    EntityRepository, Repository, SpaceConfigRepository, StorageBackendKind, StorageClient,
    StorageConfig,
};
use elephant_types::records::base::BaseFields;
use elephant_types::records::entity::{Entity, EntityType};
use elephant_types::records::SpaceConfig;
use elephant_types::{RecordId, TableName};

const BACKENDS: [StorageBackendKind; 2] = [
    StorageBackendKind::SurrealDb,
    StorageBackendKind::ElephantDb,
];
const SPACE: &str = "durability-qualification";

struct FixtureDirectory(PathBuf);

impl FixtureDirectory {
    fn new() -> anyhow::Result<Self> {
        let path =
            std::env::temp_dir().join(format!("recall-qualification-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&path)?;
        Ok(Self(path))
    }
}

impl Drop for FixtureDirectory {
    fn drop(&mut self) {
        // Only the uniquely created test directory is owned by this fixture.
        let _cleanup = std::fs::remove_dir_all(&self.0);
    }
}

async fn client(backend: StorageBackendKind, directory: &Path) -> anyhow::Result<StorageClient> {
    let config = StorageConfig {
        backend,
        url: "mem://".into(),
        namespace: format!("qualification_{}", uuid::Uuid::new_v4().simple()),
        database: "snapshot_contract".into(),
        ..StorageConfig::default()
    };
    let destination =
        (backend == StorageBackendKind::ElephantDb).then(|| directory.join("live.snapshot"));
    let client =
        StorageClient::new_with_durability(config, destination, Some(directory.join("journal")))
            .await?;
    if backend == StorageBackendKind::SurrealDb {
        apply_schema(&client).await.map_err(anyhow::Error::msg)?;
    }
    let mut fields = serde_json::to_value(BaseFields::new(
        RecordId::new_v4(TableName::SpaceConfig),
        SPACE.into(),
        "qualification".into(),
    ))?;
    fields["embedding_provider"] = serde_json::json!("fixture-durability");
    fields["embedding_model"] = serde_json::json!("fixture-no-dispatch");
    fields["embedding_dims"] = serde_json::json!(384);
    fields["processing"] = serde_json::Value::Null;
    let config: SpaceConfig = serde_json::from_value(fields)?;
    SpaceConfigRepository::new(client.clone())
        .create(config)
        .await?;
    Ok(client)
}

fn entity(name: &str) -> Entity {
    Entity::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Entity),
            SPACE.into(),
            "qualification".into(),
        ),
        EntityType::new("topic"),
        name.into(),
    )
}

#[tokio::test]
async fn named_restore_replaces_complete_dataset_and_preserves_source_less_authority(
) -> anyhow::Result<()> {
    for backend in BACKENDS {
        let directory = FixtureDirectory::new()?;
        let client = client(backend, &directory.0).await?;
        let repository = EntityRepository::new(client.clone());
        let original = repository
            .create(entity("Authoritative source-less note"))
            .await?;
        let backup = directory.0.join("before-change");
        let manifest = export_named(
            &client,
            &backup,
            "before-change",
            &SnapshotRetention::default(),
        )
        .await?;
        anyhow::ensure!(manifest.backend == backend && manifest.bytes > 0);
        anyhow::ensure!(manifest.expires_at > manifest.created_at);
        anyhow::ensure!(verify(&backup).await?.sha256 == manifest.sha256);
        let mut changed = original.clone();
        changed.display_name = "Post-backup changed note".into();
        repository.update(SPACE, &original.base.id, changed).await?;
        let later = repository
            .create(entity("Post-backup additional row"))
            .await?;

        let restored = restore_named(&client, &backup, client.erasure_journal()?).await?;
        anyhow::ensure!(restored.sha256 == manifest.sha256);
        anyhow::ensure!(
            client.recovery_status().ready,
            "successful restore never released readiness"
        );
        let row = repository
            .get(SPACE, &original.base.id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("original row lost"))?;
        anyhow::ensure!(
            row.display_name == original.display_name,
            "restore did not replace changed content"
        );
        anyhow::ensure!(
            repository.get(SPACE, &later.base.id).await?.is_none(),
            "restore appended rather than replacing dataset"
        );
        let new_row = repository.create(entity("Post-restore write")).await?;
        anyhow::ensure!(
            repository.get(SPACE, &new_row.base.id).await?.is_some(),
            "restored store cannot accept writes"
        );
    }
    Ok(())
}

#[tokio::test]
async fn corrupted_backup_is_refused_before_live_dataset_changes() -> anyhow::Result<()> {
    for backend in BACKENDS {
        let directory = FixtureDirectory::new()?;
        let client = client(backend, &directory.0).await?;
        let repository = EntityRepository::new(client.clone());
        let original = repository
            .create(entity("Live state must survive invalid restore"))
            .await?;
        let backup = directory.0.join("corrupt-input");
        export_named(
            &client,
            &backup,
            "corrupt-input",
            &SnapshotRetention::default(),
        )
        .await?;
        let payload = backup.join("data.snapshot");
        let mut bytes = std::fs::read(&payload)?;
        anyhow::ensure!(!bytes.is_empty(), "fixture exported no bytes");
        bytes[0] ^= 1;
        std::fs::write(&payload, bytes)?;
        anyhow::ensure!(
            verify(&backup).await.is_err(),
            "checksum corruption accepted"
        );
        anyhow::ensure!(restore_named(&client, &backup, client.erasure_journal()?)
            .await
            .is_err());
        let still_live = repository
            .get(SPACE, &original.base.id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("invalid backup removed live row"))?;
        anyhow::ensure!(still_live == original, "invalid backup changed live state");
        anyhow::ensure!(
            client.recovery_status().ready,
            "preflight failure poisoned an unchanged store"
        );
    }
    Ok(())
}

#[tokio::test]
async fn existing_named_backup_generation_cannot_be_overwritten() -> anyhow::Result<()> {
    for backend in BACKENDS {
        let directory = FixtureDirectory::new()?;
        let client = client(backend, &directory.0).await?;
        let repository = EntityRepository::new(client.clone());
        repository
            .create(entity("First complete generation"))
            .await?;
        let backup = directory.0.join("retained");
        let first =
            export_named(&client, &backup, "retained", &SnapshotRetention::default()).await?;
        repository.create(entity("Later generation")).await?;
        anyhow::ensure!(
            export_named(&client, &backup, "retained", &SnapshotRetention::default())
                .await
                .is_err(),
            "existing backup silently replaced"
        );
        anyhow::ensure!(
            verify(&backup).await?.sha256 == first.sha256,
            "failed export destroyed previous good generation"
        );
    }
    Ok(())
}

#[tokio::test]
async fn expired_registered_backup_is_physically_deleted_and_cannot_restore() -> anyhow::Result<()>
{
    for backend in BACKENDS {
        let directory = FixtureDirectory::new()?;
        let client = client(backend, &directory.0).await?;
        EntityRepository::new(client.clone())
            .create(entity("Synthetic retention fixture"))
            .await?;
        let backup = directory.0.join("finite-retention");
        let retention = SnapshotRetention {
            retention_seconds: 2,
            quiesce_timeout_seconds: 30,
        };
        let manifest = export_named(&client, &backup, "finite-retention", &retention).await?;
        let remaining = manifest
            .expires_at
            .signed_duration_since(chrono::Utc::now())
            .to_std()
            .unwrap_or(Duration::ZERO);
        // Exercise the production clock and cleanup path; no handcrafted expired registry.
        tokio::time::sleep(remaining + Duration::from_millis(50)).await;
        anyhow::ensure!(
            verify(&backup).await.is_err(),
            "expired snapshot still accepted"
        );
        cleanup_expired(client.erasure_journal()?)?;
        anyhow::ensure!(
            !backup.exists(),
            "retention refused reads but left backup payload physically present"
        );
        anyhow::ensure!(restore_named(&client, &backup, client.erasure_journal()?)
            .await
            .is_err());
    }
    Ok(())
}

#[tokio::test]
async fn backup_cannot_be_restored_with_unrelated_empty_erasure_journal() -> anyhow::Result<()> {
    for backend in BACKENDS {
        let original_directory = FixtureDirectory::new()?;
        let original = client(backend, &original_directory.0).await?;
        EntityRepository::new(original.clone())
            .create(entity("Original journal binding"))
            .await?;
        let backup = original_directory.0.join("bound-journal");
        export_named(
            &original,
            &backup,
            "bound-journal",
            &SnapshotRetention::default(),
        )
        .await?;

        let target_directory = FixtureDirectory::new()?;
        let target = client(backend, &target_directory.0).await?;
        let target_repo = EntityRepository::new(target.clone());
        let sentinel = target_repo
            .create(entity("Unchanged target sentinel"))
            .await?;
        anyhow::ensure!(
            restore_named(&target, &backup, target.erasure_journal()?)
                .await
                .is_err(),
            "old snapshot accepted with empty replacement journal"
        );
        anyhow::ensure!(target_repo.get(SPACE, &sentinel.base.id).await?.is_some());
        anyhow::ensure!(target.recovery_status().ready);
    }
    Ok(())
}
