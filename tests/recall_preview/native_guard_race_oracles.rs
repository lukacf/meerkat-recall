//! Deterministic native overlap through the actual PreparedDocGuards SQL.
//!
//! Included as a child of source_document_guards_tests by its storage owner.
//! No polling sleeps, fabricated completion, refreshed proof or external traffic.

use super::*;
use crate::source_publication_manifest::SourcePublicationManifest;
use crate::{SpaceConfigRepository, StorageClient, StorageConfig};
use elephant_types::provider_binding::{SpaceConfigToken, SpaceProcessingConfig};
use elephant_types::publication::{PublicationFence, SourceHead};
use elephant_types::records::ingest::IngestMode;
use elephant_types::records::SpaceConfig;
use std::collections::BTreeMap;
use std::future::Future;
use std::net::Ipv4Addr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::{timeout, timeout_at, Instant};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

struct Fixture {
    storage: StorageClient,
    listener: TcpListener,
    source: RecordId,
    head: SourceHead,
    config: SpaceConfig,
    // Keep the original journal alive until the database and listener drop.
    _directory: tempfile::TempDir,
}

async fn fixture() -> TestResult<Fixture> {
    let directory = tempfile::tempdir()?;
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let storage = StorageClient::new_native_with_test_barrier(
        StorageConfig {
            database: format!("independent_guard_race_{}", uuid::Uuid::new_v4().simple()),
            query_timeout: Duration::from_secs(8),
            transaction_timeout: Duration::from_secs(8),
            ..StorageConfig::default()
        },
        directory.path().join("original-journal"),
        listener.local_addr()?,
    )
    .await?;
    for migration in [
        concat!(
            include_str!("../../crates/migrations/sql/031_source_publication.surql"),
            "\n",
            include_str!("../../crates/migrations/sql/046_source_support_index_fence.surql")
        ),
        include_str!("../../crates/migrations/sql/035_source_publication_manifest.surql"),
        include_str!("../../crates/migrations/sql/037_source_publication_manifest_history.surql"),
        include_str!("../../crates/migrations/sql/039_private_reference_fence.surql"),
        include_str!("../../crates/migrations/sql/043_native_read_fence.surql"),
    ] {
        storage.query_checked(migration).await?;
    }
    let mut config: SpaceConfig = serde_json::from_value(serde_json::json!({
        "id":RecordId::new_v4(TableName::SpaceConfig), "space_id":"business-a",
        "created_at":chrono::Utc::now(), "updated_at":chrono::Utc::now(),
        "created_by":"independent-native-guard-oracle", "deleted_at":null,
        "security":{"policy_id":"default","level":0,"labels":[],"handling":[]},
        "provenance":{}, "subjects":[], "subjects_state":"complete",
        "embedding_provider":"disabled", "embedding_model":"no-provider-called", "embedding_dims":8
    }))?;
    config.processing = Some(SpaceProcessingConfig {
        generation: 1,
        index_generation: 0,
        policy_id: "independent-metadata-policy".into(),
        connections: BTreeMap::new(),
        profiles: BTreeMap::new(),
        bindings: BTreeMap::new(),
        active_embedding_profile_id: None,
        pending_reindex: None,
        completed_embedding_reindex: None,
    });
    let config = SpaceConfigRepository::new(storage.clone())
        .create(config)
        .await?;
    let source = RecordId::new_v4(TableName::Source);
    let mut request = tests::request(&source, "original-race-r1", SourcePrecondition::Absent);
    request
        .source
        .set_requested_ingest_mode(IngestMode::Metadata);
    let repository = SourcePublicationRepository::new(storage.clone());
    let original = Box::pin(repository.accept(request)).await?;
    let head = repository
        .head("business-a", &source)
        .await?
        .ok_or_else(|| std::io::Error::other("actual accepted source head is missing"))?;
    assert_eq!(head.operation_id, original.operation_id);
    assert_eq!(head.generation, original.source_generation);
    Ok(Fixture {
        storage,
        listener,
        source,
        head,
        config,
        _directory: directory,
    })
}

/// A domain effect in a THIRD space cannot create an accidental same-space
/// canonical-epoch conflict with the competing writer.
///
/// The original prepared
/// readset/fence must itself exclude that writer. No test-private schema event.
fn effect_document(source: &RecordId) -> Doc {
    let mut effect = document(source, "independent-transaction-effect");
    effect.base.space_id = "independent-effect-space".into();
    effect.source = None;
    effect
}

async fn physical_doc(storage: &StorageClient, id: &RecordId) -> TestResult<surrealdb::sql::Value> {
    let mut response = storage
        .query_bind_checked(
            "SELECT * FROM ONLY $id;",
            crate::bindings! {"id" => crate::surreal::thing_from_record_id(id)},
        )
        .await?;
    let raw: surrealdb::Value = response.take(0)?;
    Ok(raw.into_inner())
}

/// Own both futures directly: no spawned task can outlive the test.
///
/// The bound
/// listener and accepted stream are RAII-owned; cancellation closes both. Each
/// operation uses the SAME absolute deadline, and the engine's original 8s
/// query/transaction timeout is shorter than the 9s enclosing race deadline.
#[expect(
    clippy::items_after_statements,
    reason = "Keep the local fixture helper beside the original scenario phase that uses it."
)]
async fn overlap<F>(
    fixture: &Fixture,
    original: &PreparedDocGuards,
    competing_writer: F,
    expect_conflict: bool,
) -> TestResult
where
    F: Future<Output = TestResult>,
{
    let effect = effect_document(&fixture.source);
    let address = fixture.listener.local_addr()?;
    let path = format!("/original-guard/{}", uuid::Uuid::new_v4());
    let mut sql = String::from("BEGIN TRANSACTION;\n");
    let mut bindings = crate::bindings! {"space" => fixture.head.space_id.as_str()};
    original.append_guard_sql(&mut sql, &mut bindings)?;
    bindings.extend(crate::bindings! {
        "oracle_barrier" => format!("http://{address}{path}"),
        "oracle_effect_id" => crate::surreal::thing_from_record_id(&effect.base.id),
        "oracle_effect" => crate::surreal::doc_to_value(&effect)?,
    });
    sql.push_str("LET $oracle_ack=http::get($oracle_barrier);\nIF $oracle_ack != true { THROW 'independent_barrier_ack_invalid'; };\nCREATE $oracle_effect_id CONTENT $oracle_effect;\nCOMMIT TRANSACTION;");
    let deadline = Instant::now() + Duration::from_secs(9);
    // Observe the ORIGINAL commit result once. The checked repository wrapper
    // retries native conflicts and would incorrectly issue a second barrier GET.
    let query = timeout_at(deadline, fixture.storage.query_bind(&sql, bindings));
    let barrier = timeout_at(deadline, async {
        let (mut stream, peer) = fixture.listener.accept().await?;
        if peer.ip() != Ipv4Addr::LOCALHOST {
            return Err(std::io::Error::other("unexpected barrier peer").into());
        }
        let mut request = Vec::new();
        let mut chunk = [0_u8; 256];
        while !request.windows(4).any(|part| part == b"\r\n\r\n") {
            let count = stream.read(&mut chunk).await?;
            if count == 0 || request.len() + count > 4096 {
                return Err(
                    std::io::Error::other("incomplete or oversized barrier request").into(),
                );
            }
            request.extend_from_slice(&chunk[..count]);
        }
        let expected = format!("GET {path} HTTP/1.1\r\n");
        if !request.starts_with(expected.as_bytes()) {
            return Err(std::io::Error::other("barrier method or original nonce mismatch").into());
        }
        // HTTP reached only AFTER the actual original row reads and changing
        // bucket writes. Commit the independent writer while first TX is open.
        let competing = competing_writer.await;
        // Always release the first TX after a competing error, then propagate
        // that original error. An HTTP/timeout failure cannot count as conflict.
        let release = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 4\r\nConnection: close\r\n\r\ntrue").await;
        drop(stream);
        competing?;
        release?;
        TestResult::Ok(())
    });
    let (query, barrier) = tokio::join!(query, barrier);
    barrier??;
    let mut response = query??;
    // Rollback markers can precede the actual failure. Inspect ALL errors from
    // this one original query, without the repository's conflict retry loop.
    let errors: BTreeMap<_, _> = response
        .take_errors()
        .into_iter()
        .map(|(statement, error)| (statement, error.to_string()))
        .collect();
    const ROLLBACK: &str = "The query was not executed due to a failed transaction";
    const CONFLICT: &str = "Failed to commit transaction due to a read or write conflict. This transaction can be retried";
    let detailed_conflict = format!("{ROLLBACK}. {CONFLICT}");
    let is_conflict = |message: &str| message == CONFLICT || message == detailed_conflict;
    if expect_conflict {
        assert!(
            errors.values().any(|message| is_conflict(message)),
            "original transaction must report the exact pinned commit conflict: {errors:?}"
        );
        assert!(
            errors
                .values()
                .all(|message| message == ROLLBACK || is_conflict(message)),
            "no unrelated original query error can qualify as a conflict: {errors:?}"
        );
        assert_eq!(
            physical_doc(&fixture.storage, &effect.base.id).await?,
            surrealdb::sql::Value::None,
            "the entire original transaction effect must roll back"
        );
    } else {
        assert!(
            errors.is_empty(),
            "positive original query failed: {errors:?}"
        );
        assert_eq!(
            DocRepository::new(fixture.storage.clone())
                .get(&effect.base.space_id, &effect.base.id)
                .await?,
            Some(effect)
        );
    }
    Ok(())
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_original_guard_barrier_commits_without_a_competing_writer() -> TestResult {
    timeout(Duration::from_secs(45), async {
        let fixture = Box::pin(fixture()).await?;
        let original = PreparedDocGuards::from_authorized_documents(
            "business-a",
            &fixture.source,
            Some(&fixture.head),
            &[],
        )?;
        overlap(&fixture, &original, async { Ok(()) }, false).await
    })
    .await?
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_original_absent_doc_guard_conflicts_with_foreign_space_same_key_create(
) -> TestResult {
    timeout(Duration::from_secs(45), async {
        let fixture = Box::pin(fixture()).await?;
        let mut foreign = document(&fixture.source, "foreign-space-physical-identity");
        foreign.base.space_id = "independent-foreign-space".into();
        foreign.source = None;
        let docs = DocRepository::new(fixture.storage.clone());
        assert_eq!(
            physical_doc(&fixture.storage, &foreign.base.id).await?,
            surrealdb::sql::Value::None
        );
        let original = PreparedDocGuards::from_authorized_documents(
            "business-a",
            &fixture.source,
            Some(&fixture.head),
            &[],
        )?
        .with_absent_documents(&[foreign.base.id.clone()])?;
        overlap(
            &fixture,
            &original,
            async {
                let actual = docs.create(foreign.clone()).await?;
                assert_eq!(actual, foreign);
                Ok(())
            },
            true,
        )
        .await?;
        assert_eq!(
            docs.get(&foreign.base.space_id, &foreign.base.id).await?,
            Some(foreign)
        );
        assert_eq!(
            SourcePublicationRepository::new(fixture.storage.clone())
                .head("business-a", &fixture.source)
                .await?,
            Some(fixture.head)
        );
        Ok(())
    })
    .await?
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_original_doc_policy_guard_conflicts_with_actual_policy_update() -> TestResult {
    timeout(Duration::from_secs(45), async {
        let fixture = Box::pin(fixture()).await?;
        let docs = DocRepository::new(fixture.storage.clone());
        let original_doc = docs
            .create(document(&fixture.source, "original-policy"))
            .await?;
        let original = PreparedDocGuards::from_authorized_documents(
            "business-a",
            &fixture.source,
            Some(&fixture.head),
            std::slice::from_ref(&original_doc),
        )?;
        let mut restricted = original_doc.clone();
        restricted.base.security.level = original_doc.base.security.level + 1;
        overlap(
            &fixture,
            &original,
            async {
                docs.update("business-a", &restricted.base.id, restricted.clone())
                    .await?;
                Ok(())
            },
            true,
        )
        .await?;
        let current = docs
            .get("business-a", &original_doc.base.id)
            .await?
            .ok_or_else(|| std::io::Error::other("competing policy record disappeared"))?;
        assert_eq!(current.base.security, restricted.base.security);
        assert_eq!(current.text, original_doc.text);
        assert_eq!(current.source, original_doc.source);
        Ok(())
    })
    .await?
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_original_head_guard_conflicts_with_actual_next_source_acceptance() -> TestResult {
    timeout(Duration::from_secs(45), async {
        let fixture = Box::pin(fixture()).await?;
        let repository = SourcePublicationRepository::new(fixture.storage.clone());
        let original = PreparedDocGuards::from_authorized_documents(
            "business-a",
            &fixture.source,
            Some(&fixture.head),
            &[],
        )?;
        let mut next = tests::request(
            &fixture.source,
            "actual-overlapping-r2",
            SourcePrecondition::Generation(fixture.head.generation.clone()),
        );
        next.source.set_requested_ingest_mode(IngestMode::Metadata);
        next.authorized = repository
            .authorization_snapshot("business-a", &fixture.source)
            .await?;
        overlap(
            &fixture,
            &original,
            async {
                Box::pin(repository.accept(next.clone())).await?;
                Ok(())
            },
            true,
        )
        .await?;
        let current = repository
            .head("business-a", &fixture.source)
            .await?
            .ok_or_else(|| std::io::Error::other("competing source head disappeared"))?;
        assert_ne!(current.generation, fixture.head.generation);
        assert_eq!(
            current.desired_revision.as_deref(),
            Some("actual-overlapping-r2")
        );
        assert!(repository
            .revision("business-a", &fixture.source, "original-race-r1")
            .await?
            .is_some());
        assert!(repository
            .revision("business-a", &fixture.source, "actual-overlapping-r2")
            .await?
            .is_some());
        Ok(())
    })
    .await?
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
)]
async fn native_original_manifest_guard_conflicts_with_actual_observed_payload_update() -> TestResult
{
    timeout(Duration::from_secs(45), async {
        let fixture = Box::pin(fixture()).await?;
        let publication = PublicationFence {
            source_id: fixture.source.clone(), generation: fixture.head.generation.clone(),
            revision_id: "original-race-r1".into(), operation_id: fixture.head.operation_id.clone(),
        };
        let processing = fixture.config.processing.as_ref()
            .ok_or_else(|| std::io::Error::other("original metadata configuration missing"))?;
        // This is an observed private manifest row for storage guard testing.
        // It does NOT claim a valid completed extraction plan or a worker receipt.
        let manifest = SourcePublicationManifest {
        historical_adoption: None,
            space_id: "business-a".into(), publication: publication.clone(), mode: IngestMode::Metadata,
            configuration: SpaceConfigToken { space_id: "business-a".into(),
                config_id: fixture.config.base.id.to_string(), generation: processing.generation,
                policy_id: processing.policy_id.clone() },
            embedding_profile: None, documents: vec![], metadata_artifact_ids: vec![],
            planning_artifact_ids: vec![], bundles: vec![], embeddings: vec![], embedding_work_item_ids: vec![],
        };
        persist_manifest_observation_fixture(&fixture.storage, &manifest).await;
        let original = PreparedDocGuards::from_authorized_documents(
            "business-a", &fixture.source, Some(&fixture.head), &[])?
            .with_manifest_observation(&publication, Some(&manifest))?;
        let mut changed = manifest.clone();
        changed.metadata_artifact_ids.push(RecordId::new_v4(TableName::WorkArtifact));
        overlap(&fixture, &original, async {
            let table = crate::source_publication_manifest::MANIFEST_TABLE;
            let key = crate::source_publication_manifest::manifest_key("business-a", &publication);
            fixture.storage.query_bind_checked("UPDATE $id SET payload=$payload;", crate::bindings! {
                "id" => control_thing(table, &key), "payload" => encode_payload(&changed)?,
            }).await?;
            Ok(())
        }, true).await?;
        let actual = crate::source_publication_manifest::SourcePublicationManifestRepository::new(fixture.storage.clone())
            .get("business-a", &publication).await?;
        assert_eq!(actual, Some(changed));
        assert_eq!(SourcePublicationRepository::new(fixture.storage.clone())
            .head("business-a", &fixture.source).await?, Some(fixture.head));
        Ok(())
    }).await?
}
