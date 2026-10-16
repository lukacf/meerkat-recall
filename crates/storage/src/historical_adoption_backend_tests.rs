//! Real custody, publication and read regressions on both storage backends.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;
use crate::canonical_commit::{CanonicalRecord, CanonicalRepository};
use crate::historical_adoption::tests::{declaration, document, plan};
use crate::source_publication::SourcePublicationRepository;
use crate::{Repository, StorageClient, StorageConfig};
use elephant_types::provider_binding::{
    ModelOperation, ModelProfile, OperationBinding, ProviderConnection, SpaceProcessingConfig,
};
use elephant_types::publication::{OperationState, SourceAuthorizationSnapshot};
use elephant_types::records::base::BaseFields;
use elephant_types::records::doc_node::{NodeRole, TextSpan};
use elephant_types::records::doc_revision::{RendererMetadata, RevisionStatus};
use elephant_types::records::entity::EntityType;
use elephant_types::records::{Entity, SpaceConfig};
use elephant_types::Link;

pub(super) fn config() -> SpaceConfig {
    let mut base = BaseFields::new(
        RecordId::new_v4(TableName::SpaceConfig),
        "history-test".into(),
        "fixture".into(),
    );
    base.security.policy_id = "default".into();
    SpaceConfig {
        base,
        embedding_provider: "openai".into(),
        embedding_model: "fixture".into(),
        embedding_dims: 3,
        purpose: None,
        skill: None,
        processing: Some(SpaceProcessingConfig {
            generation: 1,
            index_generation: 0,
            policy_id: "default".into(),
            connections: BTreeMap::from([(
                "fixture".into(),
                ProviderConnection {
                    id: "fixture".into(),
                    provider: "openai".into(),
                    account_id: "unused".into(),
                    endpoint: "https://example.test/v1".into(),
                    credential_ref: Some("UNUSED".into()),
                    credential_generation: 1,
                    revoked: false,
                },
            )]),
            profiles: BTreeMap::from([(
                "embedding".into(),
                ModelProfile {
                    id: "embedding".into(),
                    connection_id: "fixture".into(),
                    model: "fixture".into(),
                    model_version: "fixture-v1".into(),
                    dimensions: Some(3),
                    preprocessing: "text-full-v2".into(),
                    summary: None,
                },
            )]),
            bindings: [
                ModelOperation::DocumentEmbedding,
                ModelOperation::QueryEmbedding,
            ]
            .into_iter()
            .map(|op| {
                (
                    op,
                    OperationBinding {
                        id: format!("{op:?}"),
                        profile_id: "embedding".into(),
                        revoked: false,
                    },
                )
            })
            .collect(),
            active_embedding_profile_id: Some("embedding".into()),
            pending_reindex: None,
            completed_embedding_reindex: None,
        }),
    }
}
#[allow(
    clippy::too_many_lines,
    reason = "Keep the complete ordered fixture and its failure/recovery assertions together."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain the original adoption authority through omitted-node, duplicate-node, commit-effects and exact replay assertions before transfer closure."
)]
async fn exercise(embedded: bool, root: &std::path::Path) -> Option<s1_tests::InterruptedS1> {
    let storage = StorageClient::new_with_durability(
        StorageConfig {
            backend: if embedded {
                crate::client::StorageBackendKind::ElephantDb
            } else {
                crate::client::StorageBackendKind::SurrealDb
            },
            url: if embedded {
                "mem://".into()
            } else {
                format!("rocksdb://{}", root.join("database").display())
            },
            database: "historical_s1".into(),
            ..StorageConfig::default()
        },
        None,
        Some(root.join("journal")),
    )
    .await
    .unwrap();
    let mut original = document();
    original.base.space_id = "default".into();
    let mut input = plan(vec![declaration(&original)]);
    input.source_space_id = "default".into();
    assert!(HistoricalAdoptionRepository::new(storage.clone())
        .prepare_authority(input.clone())
        .is_err());
    let digest = input.snapshot_sha256.clone();
    let bootstrap_digest = digest.clone();
    let (receipt, doc_id, node_id, entity_id, original_rows) = storage
        .with_transfer_barrier(&digest, move |exclusive| async move {
            if !embedded {
                for sql in [
                    include_str!("../../migrations/sql/031_source_publication.surql"),
                    include_str!("../../migrations/sql/033_space_processing.surql"),
                    include_str!("../../migrations/sql/034_storage_durability.surql"),
                    include_str!("../../migrations/sql/035_source_publication_manifest.surql"),
                    include_str!(
                        "../../migrations/sql/037_source_publication_manifest_history.surql"
                    ),
                    include_str!("../../migrations/sql/038_source_index_visibility.surql"),
                    include_str!("../../migrations/sql/039_private_reference_fence.surql"),
                    include_str!("../../migrations/sql/043_native_read_fence.surql"),
                    include_str!("../../migrations/sql/044_source_head_semantic_epoch.surql"),
                    include_str!("../../migrations/sql/045_canonical_read_semantic_epoch.surql"),
                    include_str!("../../migrations/sql/046_source_support_index_fence.surql"),
                ] {
                    exclusive.query_checked(sql).await?;
                }
            }
            // Fresh fixture initialization stays inside exclusive transfer custody.
            // Final transfer verification still requires this retained journal identity.
            exclusive.initialize_journal_identity().await?;
            let bootstrap = HistoricalAdoptionRepository::new(exclusive.clone())
                .bootstrap_configuration(&bootstrap_digest, config())
                .await?;
            assert_eq!(bootstrap.len(), if embedded { 0 } else { 3 });
            let raw = serde_json::to_value(&original).unwrap();
            let mut doc = original;
            doc.base.space_id = "history-test".into();
            let revision = DocRevision {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::DocRevision),
                    "history-test".into(),
                    "original-renderer".into(),
                ),
                doc: Link::new(doc.base.id.clone()),
                text: doc.text.clone(),
                text_hash: sha256(doc.text.as_bytes()),
                renderer: RendererMetadata {
                    name: "original".into(),
                    version: "legacy".into(),
                    config_hash: None,
                },
                manifest: None,
                supersedes: None,
                status: RevisionStatus::Active,
                rendered_at: Utc::now(),
            };
            let node = DocNode {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::DocNode),
                    "history-test".into(),
                    "original".into(),
                ),
                revision: Link::new(revision.base.id.clone()),
                role: NodeRole::Chunk,
                span: TextSpan::new(0, u32::try_from(doc.text.len()).unwrap()),
                node_key: "original-node".into(),
                text: None,
                parent: None,
                position: 0,
                depth: 0,
                embedding_model: Some("legacy-model-unproven".into()),
                collection: None,
            };
            let entity = Entity::new(
                BaseFields::new(
                    RecordId::new_v4(TableName::Entity),
                    "history-test".into(),
                    "legacy".into(),
                ),
                EntityType::new("person"),
                "Original entity".into(),
            );
            let assertion: elephant_types::records::Assertion = serde_json::from_value(serde_json::json!({
                "id": RecordId::new_v4(TableName::Assertion), "space_id":"history-test",
                "created_at":Utc::now(),"updated_at":Utc::now(),"created_by":"original-agent",
                "provenance":{},"security":{"policy_id":"default","level":0,"labels":[],"handling":[]},
                "subject_text":"Original entity","predicate_raw":"attended", "object_text":"Original event",
                "evidence":{"revision":revision.base.id,"node":node.base.id,"start":0,"end":doc.text.len(),
                    "quote_hash":sha256(doc.text.as_bytes())},
                "observed_at":Utc::now(),"asserted_at":Utc::now(),"assertion_key":"original-assertion",
                "subject_entity":entity.base.id,"confidence":0.8
            })).unwrap();
            let event = elephant_types::records::Event::new(
                BaseFields::new(RecordId::new_v4(TableName::Event), "history-test".into(), "original-agent".into()),
                "Original event", vec![Link::new(assertion.base.id.clone())], vec![]).unwrap();
            let truth: elephant_types::records::TruthSlot = serde_json::from_value(serde_json::json!({
                "id":RecordId::new_v4(TableName::TruthSlot),"space_id":"history-test",
                "created_at":Utc::now(),"updated_at":Utc::now(),"created_by":"original-agent",
                "provenance":{},"security":{"policy_id":"default","level":0,"labels":[],"handling":[]},
                "slot_key":"original-slot","slot_type":"event","subject_entity":entity.base.id.to_string(),
                "predicate_id":"attended","current_values":[{"claim_id":event.base.id.to_string(),"confidence":0.8,"status":"approved","evidence":[]}],
                "computed_at":Utc::now(),"security_hash":"original-security"
            })).unwrap();
            crate::AssertionRepository::new(exclusive.clone()).create(assertion.clone()).await?;
            crate::EventRepository::new(exclusive.clone()).create(event.clone()).await?;
            crate::TruthSlotRepository::new(exclusive.clone()).create(truth.clone()).await?;
            crate::DocRepository::new(exclusive.clone())
                .create(doc.clone())
                .await?;
            crate::DocRevisionRepository::new(exclusive.clone())
                .create(revision.clone())
                .await?;
            crate::DocNodeRepository::new(exclusive.clone())
                .create(node.clone())
                .await?;
            crate::EntityRepository::new(exclusive.clone())
                .create(entity.clone())
                .await?;
            let authority =
                HistoricalAdoptionRepository::new(exclusive.clone()).prepare_authority(input)?;
            let rows = HistoricalGroupRows {
                documents: vec![HistoricalOriginalDocument {
                    document: doc.clone(),
                    original_document: raw,
                    revision,
                    nodes: vec![node.clone()],
                }],
                original_sources: vec![],
                graph: vec![CanonicalRecord::Entity(Box::new(entity.clone())),
                    CanonicalRecord::Assertion(Box::new(assertion)), CanonicalRecord::Event(Box::new(event)),
                    CanonicalRecord::TruthSlot(Box::new(truth))],
            };
            // The population index must prove closure, not merely validate
            // the nodes supplied by a caller. A failed preparation writes nothing.
            let epoch_before = CanonicalRepository::new(exclusive.clone())
                .observe_epoch("history-test")
                .await?;
            let mut omitted = rows.clone();
            omitted.documents[0].nodes.clear();
            assert!(authority
                .adopt_group("hc-legacy-original", omitted)
                .await
                .is_err());
            let mut duplicate = rows.clone();
            duplicate.documents[0].nodes.push(node.clone());
            assert!(authority
                .adopt_group("hc-legacy-original", duplicate)
                .await
                .is_err());
            assert_eq!(
                epoch_before,
                CanonicalRepository::new(exclusive.clone())
                    .observe_epoch("history-test")
                    .await?
            );
            let committed = authority
                .adopt_group_with_effects("hc-legacy-original", rows.clone())
                .await?;
            if embedded {
                assert!(committed.native_effects.is_empty());
            } else {
                for table in [
                    "space_config",
                    "canonical_epoch",
                    "source_erasure_guard",
                    "native_read_fence",
                    "doc",
                    "source_head",
                ] {
                    assert!(
                        committed
                            .native_effects
                            .iter()
                            .any(|effect| effect.table == table),
                        "missing generated effect: {table}"
                    );
                }
                assert!(!committed
                    .native_effects
                    .iter()
                    .any(|effect| effect.table == "source_erasure_membership"));
            }
            let receipt = committed.receipt;
            assert_eq!(
                receipt,
                authority
                    .adopt_group("hc-legacy-original", rows.clone())
                    .await?
            );
            assert_eq!(receipt.operation.state, OperationState::Processing);
            assert!(receipt.operation.completed_at.is_none());
            Ok((receipt, doc.base.id, node.base.id, entity.base.id, rows))
        })
        .await
        .unwrap();
    assert!(storage.recovery_status().ready);
    let publications = SourcePublicationRepository::new(storage.clone());
    let source = crate::SourceRepository::new(storage.clone())
        .get("history-test", &receipt.source_id)
        .await
        .unwrap()
        .unwrap();
    let doc = crate::DocRepository::new(storage.clone())
        .get("history-test", &doc_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(doc.source.as_ref().unwrap().id(), &receipt.source_id);
    let node = crate::DocNodeRepository::new(storage.clone())
        .get("history-test", &node_id)
        .await
        .unwrap()
        .unwrap();
    let reader = crate::read_eligibility::ReadEligibility::new(storage.clone());
    assert!(reader
        .project(node.clone(), &|_| true)
        .await
        .unwrap()
        .is_some());
    assert!(reader.project(node, &|_| false).await.unwrap().is_none());
    let supports = publications
        .support_for_record("history-test", &entity_id)
        .await
        .unwrap();
    // A current-only authority must be retained, not treated as its own replacement.
    let current_entity = crate::EntityRepository::new(storage.clone())
        .get("history-test", &entity_id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        crate::read_eligibility::ReadEligibility::new(storage.clone())
            .project(current_entity, &|_| true)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(supports.len(), 1);
    assert!(!supports[0].independent);
    assert!(supports[0].historical_unknown.is_some());
    let stored_operation: crate::source_publication::StoredOperation = publications
        .read_control(
            "source_operation",
            &receipt.operation.operation_id,
            "history-test",
        )
        .await
        .unwrap()
        .unwrap();
    let source_input = uuid::Uuid::parse_str(&stored_operation.input_fingerprint).unwrap();
    assert_eq!(source_input.get_version_num(), 5);
    assert_eq!(source_input.to_string(), stored_operation.input_fingerprint);
    assert_eq!(
        stored_operation.request_fingerprint,
        receipt.accepted_request_fingerprint
    );
    assert_eq!(receipt.accepted_request_fingerprint.len(), 64);
    let revision_key = crate::source_publication::revision_key(
        "history-test",
        &source.base.id,
        &receipt.accepted_revision,
    );
    let stored_revision: crate::source_publication::StoredRevision = publications
        .read_control("source_revision", &revision_key, "history-test")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stored_revision.input_fingerprint,
        stored_operation.input_fingerprint
    );
    let epoch = CanonicalRepository::new(storage.clone())
        .observe_epoch("history-test")
        .await
        .unwrap();
    let request = HistoricalNoopRequest {
        space_id: "history-test".into(),
        source_id: source.base.id.clone(),
        external_source_id: receipt.external_source_id.clone(),
        expected_generation: receipt.operation.source_generation.clone(),
        accepted_revision: receipt.accepted_revision.clone(),
        accepted_request_fingerprint: receipt.accepted_request_fingerprint.clone(),
        original_commit_id: receipt.commit_id.clone(),
        document_content_sha256: receipt.document_content_sha256.clone(),
        authorization_epoch: epoch.clone(),
        authorized_document_fingerprints: BTreeMap::from([(
            doc.base.id.clone(),
            source_support_fingerprint(&doc).unwrap(),
        )]),
        authorized: SourceAuthorizationSnapshot::from(&source),
    };
    let repository = HistoricalAdoptionRepository::new(storage.clone());
    assert!(
        repository
            .verify_unchanged(&request)
            .await
            .unwrap()
            .unchanged
    );
    assert_eq!(
        epoch,
        CanonicalRepository::new(storage.clone())
            .observe_epoch("history-test")
            .await
            .unwrap()
    );
    let mut changed = request.clone();
    changed.document_content_sha256.clear();
    assert!(repository.verify_unchanged(&changed).await.is_err());
    let mut changed = request.clone();
    changed
        .authorized_document_fingerprints
        .insert(doc_id, "wrong".into());
    assert!(repository.verify_unchanged(&changed).await.is_err());
    let mut changed = request;
    changed.authorization_epoch = None;
    assert!(repository.verify_unchanged(&changed).await.is_err());
    assert!(crate::WorkItemRepository::new(storage.clone())
        .find_by_status_limited(
            "history-test",
            elephant_types::records::work::WorkItemStatus::Pending,
            1
        )
        .await
        .unwrap()
        .is_empty());
    drop(reader);
    drop(repository);
    drop(publications);
    exercise_s1(storage, original_rows, receipt, embedded, root).await
}
#[path = "historical_adoption_s1_tests.rs"]
mod s1_tests;
use s1_tests::exercise_s1;
#[tokio::test]
async fn historical_adoption_embedded_custody_noop_read_and_unproven_vectors() {
    let directory = tempfile::tempdir().unwrap();
    assert!(Box::pin(exercise(true, directory.path())).await.is_none());
}
#[cfg(feature = "rocksdb")]
#[test]
fn historical_adoption_native_custody_noop_read_and_unproven_vectors() {
    let directory = tempfile::tempdir().unwrap();
    let first = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let interrupted = first.block_on(exercise(false, directory.path())).unwrap();
    drop(first); // Native router/store owners must close before reopening.
    let second = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    second.block_on(s1_tests::resume_s1(interrupted, directory.path()));
}

#[path = "historical_snapshot_graph_tests.rs"]
mod snapshot_graph_tests;
