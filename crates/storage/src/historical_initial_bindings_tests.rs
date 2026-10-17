//! Actual native receipt and archive controls for bounded initial bindings.

use super::{
    HistoricalAdoptionReceipt, HistoricalAdoptionRepository, MAX_INITIAL_BINDING_BATCH_BYTES,
    MAX_INITIAL_BINDING_BATCH_RECEIPTS,
};
use crate::canonical_commit::CanonicalRepository;
use crate::durability_io::io_error;
use crate::historical_adoption::{
    backend_tests, tests, HistoricalGroupRows, HistoricalOriginalDocument,
};
use crate::historical_archive_lifecycle::reserve_initial_archive;
use crate::historical_operational_archive::{ArchiveBinding, ArchiveBuilder, ArchiveRow};
use crate::{Repository, StorageClient, StorageResult};
use elephant_types::records::doc_revision::{RendererMetadata, RevisionStatus};
use elephant_types::records::{BaseFields, DocRevision, SystemEvent};
use elephant_types::{Link, RecordId, TableName};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

async fn initialize_fixture(client: &StorageClient) -> StorageResult<()> {
    for sql in [
        include_str!("../../migrations/sql/031_source_publication.surql"),
        include_str!("../../migrations/sql/033_space_processing.surql"),
        include_str!("../../migrations/sql/034_storage_durability.surql"),
        include_str!("../../migrations/sql/035_source_publication_manifest.surql"),
        include_str!("../../migrations/sql/037_source_publication_manifest_history.surql"),
        include_str!("../../migrations/sql/038_source_index_visibility.surql"),
        include_str!("../../migrations/sql/039_private_reference_fence.surql"),
        include_str!("../../migrations/sql/043_native_read_fence.surql"),
        include_str!("../../migrations/sql/044_source_head_semantic_epoch.surql"),
        include_str!("../../migrations/sql/045_canonical_read_semantic_epoch.surql"),
        include_str!("../../migrations/sql/046_source_support_index_fence.surql"),
    ] {
        client.query_checked(sql).await?;
    }
    client.initialize_journal_identity().await?;
    HistoricalAdoptionRepository::new(client.clone())
        .bootstrap_configuration(&"b".repeat(64), backend_tests::config())
        .await?;
    Ok(())
}

async fn fixture(
    client: &StorageClient,
    count: usize,
) -> StorageResult<(Vec<HistoricalAdoptionReceipt>, PathBuf)> {
    initialize_fixture(client).await?;
    let mut groups = Vec::new();
    let mut rows = Vec::new();
    for index in 0..count {
        let mut document = tests::document();
        document.rendition_key = format!("binding-original-{index}");
        let original_document = serde_json::to_value(&document).map_err(io_error)?;
        let mut declared = tests::declaration(&document);
        declared.external_source_id = format!("binding-group-{index:04}");
        let mut group = tests::plan(vec![declared]).groups.remove(0);
        group.external_source_id = format!("binding-group-{index:04}");
        groups.push(group);
        let revision = DocRevision {
            base: BaseFields::new(
                RecordId::new_v4(TableName::DocRevision),
                "history-test".into(),
                "original".into(),
            ),
            doc: Link::new(document.base.id.clone()),
            text: document.text.clone(),
            text_hash: super::super::sha256(document.text.as_bytes()),
            renderer: RendererMetadata {
                name: "original".into(),
                version: "1".into(),
                config_hash: None,
            },
            manifest: None,
            supersedes: None,
            status: RevisionStatus::Active,
            rendered_at: chrono::Utc::now(),
        };
        crate::DocRepository::new(client.clone())
            .create(document.clone())
            .await?;
        crate::DocRevisionRepository::new(client.clone())
            .create(revision.clone())
            .await?;
        rows.push(HistoricalGroupRows {
            documents: vec![HistoricalOriginalDocument {
                document,
                original_document,
                revision,
                nodes: vec![],
            }],
            original_sources: vec![],
            graph: vec![],
        });
    }
    let mut input = tests::plan(Vec::new());
    input.groups = groups;
    input.documents_sha256 =
        super::super::jsonl_sha256(input.groups.iter().flat_map(|group| &group.documents))?;
    input.groups_sha256 = super::super::jsonl_sha256(input.groups.iter().map(|group| serde_json::json!({
        "external_source_id":group.external_source_id,"documents":group.documents.len(),"document_mapping_sha256":group.document_mapping_sha256
    })))?;
    let group_names = input
        .groups
        .iter()
        .map(|group| group.external_source_id.clone())
        .collect::<Vec<_>>();
    let authority = HistoricalAdoptionRepository::new(client.clone()).prepare_authority(input)?;
    let mut receipts = Vec::new();
    for (name, rows) in group_names.iter().zip(rows) {
        receipts.push(Box::pin(authority.adopt_group(name, rows)).await?);
    }
    drop(authority);
    Ok((receipts, create_archive(client).await?))
}

async fn create_archive(client: &StorageClient) -> StorageResult<PathBuf> {
    let event = SystemEvent {
        base: BaseFields::new(
            RecordId::new_v4(TableName::SystemEvent),
            "history-test".into(),
            "original".into(),
        ),
        seq: 7,
        event_type: "original.event".into(),
        payload: serde_json::json!({"retained":"binding-byte-control"}),
    };
    let lexical = serde_json::to_vec(&event).map_err(io_error)?;
    let binding = ArchiveBinding {
        version: 1,
        snapshot_sha256: "b".repeat(64),
        spool_sha256: "c".repeat(64),
        plan_id: "d".repeat(64),
        source_space_id: "history-test".into(),
        target_space_id: "history-test".into(),
        journal_identity: client.erasure_journal()?.identity().into(),
        disposition_sha256: ArchiveBinding::disposition_digest(),
        parent_generation_sha256: None,
        original_table_declarations: BTreeMap::from([("system_event".into(), 1)]),
    };
    let pending = reserve_initial_archive(client, &binding)?;
    let path = pending.path().to_owned();
    let mut writer = ArchiveBuilder::create(&path, binding)?;
    writer.append(&[ArchiveRow {
        table: "system_event".into(),
        id: event.base.id.to_string(),
        lexical_sha256: super::super::sha256(&lexical),
        lexical,
    }])?;
    let archive = pending.seal(client, writer.seal()?)?;
    let epoch = CanonicalRepository::new(client.clone())
        .current_epoch("history-test")
        .await?;
    archive.commit(client, &epoch).await?;
    Ok(path)
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original native client through transfer completion and explicit awaited owned close; dropping it before assertions would weaken custody and cleanup coverage."
)]
async fn native_binding_batches_match_standalone_with_bounded_full_checks() -> StorageResult<()> {
    let client = StorageClient::new_memory().await?;
    let receipts = Box::pin(client.with_transfer_barrier(
        &"b".repeat(64),
        |exclusive| async move {
            let (receipts, _) =
                Box::pin(fixture(&exclusive, MAX_INITIAL_BINDING_BATCH_RECEIPTS + 1)).await?;
            let repo = HistoricalAdoptionRepository::new(exclusive.clone());
            let mut expected = Vec::new();
            for receipt in &receipts {
                expected.push(repo.initial_catchup_binding(receipt).await?);
            }
            let checks = AtomicUsize::new(0);
            let mut actual = Vec::new();
            // Two real native batches: every batch runs both full verifiers, while
            // each receipt retains actual canonical/head validation and ordering.
            for page in receipts.chunks(MAX_INITIAL_BINDING_BATCH_RECEIPTS) {
                actual.extend(
                    repo.initial_catchup_bindings_checked(
                        page,
                        std::future::ready(Ok(())),
                        &checks,
                    )
                    .await?,
                );
            }
            assert_eq!(checks.load(Ordering::Relaxed), 4);
            assert_eq!(
                serde_json::to_value(actual).map_err(io_error)?,
                serde_json::to_value(expected).map_err(io_error)?
            );
            assert!(repo.initial_catchup_bindings(&[]).await.is_err());
            assert!(repo
                .initial_catchup_bindings(&[receipts[0].clone(), receipts[0].clone()])
                .await
                .is_err());
            assert!(repo
                .initial_catchup_bindings(&vec![
                    receipts[0].clone();
                    MAX_INITIAL_BINDING_BATCH_RECEIPTS + 1
                ])
                .await
                .is_err());
            let mut excessive = receipts[0].clone();
            excessive.external_source_id = "x".repeat(MAX_INITIAL_BINDING_BATCH_BYTES + 1);
            assert!(repo.initial_catchup_bindings(&[excessive]).await.is_err());
            let mut byte_excess = receipts[..3].to_vec();
            for receipt in &mut byte_excess {
                receipt.external_source_id = "x".repeat(6 * 1024 * 1024);
            }
            let denied_checks = AtomicUsize::new(0);
            assert!(repo
                .initial_catchup_bindings_checked(
                    &byte_excess,
                    std::future::ready(Ok(())),
                    &denied_checks,
                )
                .await
                .is_err());
            assert_eq!(denied_checks.load(Ordering::Relaxed), 0);
            drop(byte_excess);
            let mut wrong_snapshot = receipts[0].clone();
            wrong_snapshot.original_snapshot_sha256 = "a".repeat(64);
            assert!(repo
                .initial_catchup_bindings(&[wrong_snapshot])
                .await
                .is_err());
            let mut foreign_space = receipts[0].clone();
            foreign_space.space_id = "foreign".into();
            assert!(repo
                .initial_catchup_bindings(&[foreign_space])
                .await
                .is_err());
            let mut wrong_manifest = receipts[0].clone();
            wrong_manifest.manifest_digest = "a".repeat(64);
            assert!(repo
                .initial_catchup_bindings(&[wrong_manifest])
                .await
                .is_err());
            drop(repo);
            Ok(receipts)
        },
    ))
    .await?;
    assert!(HistoricalAdoptionRepository::new(client.clone())
        .initial_catchup_bindings(&receipts[..1])
        .await
        .is_err());
    client.close_owned_command_connection(60_000).await?;
    Ok(())
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original native client through transfer completion and explicit awaited owned close; dropping it before assertions would weaken custody and cleanup coverage."
)]
async fn native_binding_batch_refuses_archive_mutation_and_preserves_standalone_guard(
) -> StorageResult<()> {
    let client = StorageClient::new_memory().await?;
    Box::pin(
        client.with_transfer_barrier(&"b".repeat(64), |exclusive| async move {
            let (receipts, path) = Box::pin(fixture(&exclusive, 3)).await?;
            let original = std::fs::read(&path).map_err(io_error)?;
            let mut changed = original.clone();
            let last = changed.len() - 1;
            changed[last] ^= 1;
            let repo = HistoricalAdoptionRepository::new(exclusive.clone());
            let checks = AtomicUsize::new(0);
            let result = repo
                .initial_catchup_bindings_checked(
                    &receipts,
                    async {
                        std::fs::write(&path, &changed).map_err(io_error)?;
                        Ok(())
                    },
                    &checks,
                )
                .await;
            assert!(result.is_err());
            assert!(exclusive.erasure_journal()?.transfer_marker()?.is_some());
            assert_eq!(checks.load(Ordering::Relaxed), 1);
            assert!(repo.initial_catchup_binding(&receipts[0]).await.is_err());
            std::fs::write(&path, &original).map_err(io_error)?;
            std::fs::File::open(&path)
                .map_err(io_error)?
                .sync_all()
                .map_err(io_error)?;
            assert!(repo.initial_catchup_bindings(&receipts).await.is_ok());
            drop(repo);
            Ok(())
        }),
    )
    .await?;
    client.close_owned_command_connection(60_000).await?;
    Ok(())
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original native client through transfer completion and explicit awaited owned close; dropping it before assertions would weaken custody and cleanup coverage."
)]
async fn native_binding_batch_refuses_same_epoch_marker_replacement_and_epoch_drift(
) -> StorageResult<()> {
    let client = StorageClient::new_memory().await?;
    Box::pin(client.with_transfer_barrier(&"b".repeat(64), |exclusive| async move {
        let (receipts, _) = Box::pin(fixture(&exclusive, 3)).await?;
        let repo = HistoricalAdoptionRepository::new(exclusive.clone());
        let marker = exclusive.erasure_journal()?.path().join("transfer-pending.json");
        let original = std::fs::read(&marker).map_err(io_error)?;
        let mut replaced: serde_json::Value = serde_json::from_slice(&original).map_err(io_error)?;
        replaced["started_at"] = serde_json::json!("2001-01-01T00:00:00Z");
        let result = repo.initial_catchup_bindings_checked(&receipts, async {
            std::fs::write(&marker, serde_json::to_vec(&replaced).map_err(io_error)?).map_err(io_error)?;
            Ok(())
        }, &AtomicUsize::new(0)).await;
        assert!(result.is_err());
        std::fs::write(&marker, &original).map_err(io_error)?;
        let epoch = CanonicalRepository::new(exclusive.clone()).current_epoch("history-test").await?;
        let result = repo.initial_catchup_bindings_checked(&receipts, async {
            exclusive.query_checked("UPDATE canonical_epoch:`history-test` SET epoch='changed-batch-epoch'").await?;
            Ok(())
        }, &AtomicUsize::new(0)).await;
        assert!(result.is_err());
        exclusive.query_bind_checked("UPDATE canonical_epoch:`history-test` SET epoch=$epoch", crate::bindings! {"epoch"=>epoch}).await?;
        assert!(repo.initial_catchup_bindings(&receipts).await.is_ok());
        drop(repo);
        Ok(())
    })).await?;
    client.close_owned_command_connection(60_000).await?;
    Ok(())
}

// Publish an independently coherent native/registry successor at the same epoch.
// This test intentionally uses crate-private fixture custody to exercise the
// exact identity comparison; it does not add a production publication bypass.
async fn replace_archive_same_epoch(client: &StorageClient) -> StorageResult<()> {
    use crate::historical_archive_lifecycle::{native_id, seal_evidence, NativeArchive};
    use crate::historical_operational_archive::VerifiedArchive;
    let journal = client.erasure_journal()?;
    let original = journal.operational_archive_heads()?.remove(0);
    let evidence = original
        .evidence()
        .ok_or_else(|| io_error("fixture archive unsealed"))?;
    let old = VerifiedArchive::open(
        &journal.archive_payload_path(&original.registration.generation_id)?,
        &evidence.binding,
        &evidence.seal,
    )?;
    let mut binding = evidence.binding.clone();
    binding.parent_generation_sha256 = Some(evidence.file_sha256.clone());
    let mut registration = original.registration.clone();
    registration.parent_generation_id = Some(registration.generation_id);
    registration.generation_id = uuid::Uuid::new_v4().to_string();
    registration.binding_sha256 = binding.digest()?;
    let path = journal.reserve_operational_archive(registration.clone())?;
    let mut writer = ArchiveBuilder::create(&path, binding.clone())?;
    writer.append(&old.page("system_event", None, 256)?)?;
    let verified = writer.seal()?;
    let entry = journal.seal_operational_archive(
        &journal.operational_archive_entry(&registration.generation_id)?,
        seal_evidence(&binding, verified.seal())?,
    )?;
    let payload = serde_json::to_string(&NativeArchive {
        version: 1,
        registration,
        binding,
        seal: verified.seal().clone(),
    })
    .map_err(io_error)?;
    let mut response = client.query_bind_checked(
        "UPDATE $id SET archive=$archive; SELECT VALUE crypto::sha256(type::string($this)) FROM ONLY $id;",
        crate::bindings! {"id"=>native_id("history-test"), "archive"=>payload},
    ).await?;
    let digest: Option<String> = response.take(1)?;
    journal.commit_operational_archive(
        &entry,
        &digest.ok_or_else(|| io_error("fixture native digest missing"))?,
    )?;
    Box::pin(client.verify_journal_identity()).await?;
    Ok(())
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original native client through transfer completion and explicit awaited owned close; dropping it before assertions would weaken custody and cleanup coverage."
)]
async fn native_binding_batch_refuses_coherent_same_epoch_archive_successor() -> StorageResult<()> {
    let client = StorageClient::new_memory().await?;
    Box::pin(
        client.with_transfer_barrier(&"b".repeat(64), |exclusive| async move {
            let (receipts, _) = Box::pin(fixture(&exclusive, 3)).await?;
            let epoch = CanonicalRepository::new(exclusive.clone())
                .current_epoch("history-test")
                .await?;
            let repo = HistoricalAdoptionRepository::new(exclusive.clone());
            let checks = AtomicUsize::new(0);
            let result = repo
                .initial_catchup_bindings_checked(
                    &receipts,
                    async {
                        Box::pin(replace_archive_same_epoch(&exclusive)).await?;
                        assert_eq!(
                            CanonicalRepository::new(exclusive.clone())
                                .current_epoch("history-test")
                                .await?,
                            epoch
                        );
                        Ok(())
                    },
                    &checks,
                )
                .await;
            assert!(result.is_err());
            assert!(exclusive.erasure_journal()?.transfer_marker()?.is_some());
            assert_eq!(checks.load(Ordering::Relaxed), 1);
            // Both endpoint populations independently pass the original strong
            // guard, but the in-flight batch must not cross between them.
            assert!(repo.initial_catchup_binding(&receipts[0]).await.is_ok());
            assert!(repo.initial_catchup_bindings(&receipts).await.is_ok());
            drop(repo);
            Ok(())
        }),
    )
    .await?;
    client.close_owned_command_connection(60_000).await?;
    Ok(())
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original native client through transfer completion and explicit awaited owned close; dropping it before assertions would weaken custody and cleanup coverage."
)]
async fn native_binding_batch_refuses_native_archive_wrapper_mutation() -> StorageResult<()> {
    use crate::historical_archive_lifecycle::native_id;
    let client = StorageClient::new_memory().await?;
    Box::pin(
        client.with_transfer_barrier(&"b".repeat(64), |exclusive| async move {
            let (receipts, _) = Box::pin(fixture(&exclusive, 3)).await?;
            let repo = HistoricalAdoptionRepository::new(exclusive.clone());
            let checks = AtomicUsize::new(0);
            let result = repo
                .initial_catchup_bindings_checked(
                    &receipts,
                    async {
                        exclusive
                            .query_bind_checked(
                                "UPDATE $id SET unexpected_wrapper=true",
                                crate::bindings! {"id"=>native_id("history-test")},
                            )
                            .await?;
                        Ok(())
                    },
                    &checks,
                )
                .await;
            assert!(result.is_err());
            assert!(exclusive.erasure_journal()?.transfer_marker()?.is_some());
            assert_eq!(checks.load(Ordering::Relaxed), 1);
            assert!(repo.initial_catchup_binding(&receipts[0]).await.is_err());
            exclusive
                .query_bind_checked(
                    "UPDATE $id UNSET unexpected_wrapper",
                    crate::bindings! {"id"=>native_id("history-test")},
                )
                .await?;
            assert!(repo.initial_catchup_bindings(&receipts).await.is_ok());
            drop(repo);
            Ok(())
        }),
    )
    .await?;
    client.close_owned_command_connection(60_000).await?;
    Ok(())
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original native client through transfer completion and explicit awaited owned close; dropping it before assertions would weaken custody and cleanup coverage."
)]
async fn native_binding_batch_checks_actual_source_head_after_admission() -> StorageResult<()> {
    use crate::source_publication::{control_thing, head_key, SourcePublicationRepository};
    let client = StorageClient::new_memory().await?;
    Box::pin(client.with_transfer_barrier(&"b".repeat(64), |exclusive| async move {
        let (receipts, _) = Box::pin(fixture(&exclusive, 3)).await?;
        let head = SourcePublicationRepository::new(exclusive.clone()).head("history-test", &receipts[0].source_id).await?
            .ok_or_else(|| io_error("fixture head missing"))?;
        let original = serde_json::to_string(&head).map_err(io_error)?;
        let mut changed = head;
        changed.lifecycle = elephant_types::publication::SourceVisibility::Retracted;
        let id = control_thing("source_head", &head_key("history-test", &receipts[0].source_id));
        exclusive.query_bind_checked("UPDATE $id SET payload=$payload", crate::bindings! {"id"=>id.clone(), "payload"=>serde_json::to_string(&changed).map_err(io_error)?}).await?;
        let checks = AtomicUsize::new(0);
        assert!(HistoricalAdoptionRepository::new(exclusive.clone()).initial_catchup_bindings_checked(
            &receipts, std::future::ready(Ok(())), &checks,
        ).await.is_err());
        assert_eq!(checks.load(Ordering::Relaxed), 1);
        exclusive.query_bind_checked("UPDATE $id SET payload=$payload", crate::bindings! {"id"=>id, "payload"=>original}).await?;
        assert!(HistoricalAdoptionRepository::new(exclusive).initial_catchup_bindings(&receipts).await.is_ok());
        Ok(())
    })).await?;
    client.close_owned_command_connection(60_000).await?;
    Ok(())
}
