//! Actual repository import -> explicit reindex -> physical coverage controls.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;
use crate::historical_adoption::{
    document_mapping_sha256, original_document_content_sha256, HistoricalAdoptionInput,
    HistoricalAdoptionRepository, HistoricalDocumentInput, HistoricalGroupRows,
    HistoricalOriginalDocument, HistoricalSourceGroup,
};
use crate::{Repository, StorageConfig};
use elephant_types::provider_binding::{
    ModelProfile, OperationBinding, ProviderConnection, SpaceProcessingConfig,
};
use elephant_types::records::base::BaseFields;
use elephant_types::records::doc_node::TextSpan;
use elephant_types::records::doc_revision::{RendererMetadata, RevisionStatus};
use elephant_types::records::entity::EntityType;
use elephant_types::records::{Doc, DocRevision};
use elephant_types::{Link, TableName};

fn canonical_hash(value: &impl Serialize) -> String {
    digest(
        &serde_json::to_vec(&crate::canonical_commit::canonical_json(
            serde_json::to_value(value).unwrap(),
        ))
        .unwrap(),
    )
}
fn jsonl_hash(values: impl IntoIterator<Item = serde_json::Value>) -> String {
    let mut hash = Sha256::new();
    for value in values {
        hash.update(serde_json::to_vec(&crate::canonical_commit::canonical_json(value)).unwrap());
        hash.update(b"\n");
    }
    hex::encode(hash.finalize())
}
fn config() -> SpaceConfig {
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
                ModelOperation::ReindexEmbedding,
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

struct Fixture {
    storage: StorageClient,
    _directory: tempfile::TempDir,
    receipt: HistoricalAdoptionReceipt,
    publication: PublicationFence,
    nodes: Vec<DocNode>,
    entity: Entity,
    referenced: Option<Entity>,
    shared_snapshot: Option<PublicationFence>,
    second_receipt: Option<HistoricalAdoptionReceipt>,
}
async fn fixture(embedded: bool) -> Fixture {
    fixture_kind(embedded, false).await
}
async fn fixture_kind(embedded: bool, snapshot_group: bool) -> Fixture {
    fixture_flags(embedded, snapshot_group, false).await
}
async fn fixture_population(embedded: bool, snapshot_group: bool, successive: bool) -> Fixture {
    fixture_options(embedded, snapshot_group, false, successive).await
}
async fn fixture_flags(embedded: bool, snapshot_group: bool, shared_node: bool) -> Fixture {
    fixture_options(embedded, snapshot_group, shared_node, false).await
}
#[allow(
    clippy::too_many_lines,
    reason = "Keep the complete ordered fixture and its failure/recovery assertions together."
)]
#[allow(
    clippy::fn_params_excessive_bools,
    reason = "Independent fixture axes select backend, snapshot ownership, shared nodes, and succession."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Preserve initial transfer authority through document and snapshot-group adoption and the final immutable fixture return."
)]
async fn fixture_options(
    embedded: bool,
    snapshot_group: bool,
    shared_node: bool,
    successive: bool,
) -> Fixture {
    let directory = tempfile::tempdir().unwrap();
    let storage = StorageClient::new_with_durability(
        StorageConfig {
            backend: if embedded {
                crate::client::StorageBackendKind::ElephantDb
            } else {
                crate::client::StorageBackendKind::SurrealDb
            },
            database: format!("coverage_{}", uuid::Uuid::new_v4().simple()),
            ..StorageConfig::default()
        },
        None,
        Some(directory.path().join("journal")),
    )
    .await
    .unwrap();
    let (receipt,nodes,entity,referenced,shared_snapshot,second_receipt)=storage.with_transfer_barrier(&"b".repeat(64),move|exclusive| async move {
        if !embedded {
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
                include_str!("../../migrations/sql/047_historical_node_revision_lookup.surql"),
                include_str!("../../migrations/sql/048_historical_coverage_visibility.surql"),
            ] { exclusive.query_checked(sql).await?; }
        }
        exclusive.initialize_journal_identity().await?;
        HistoricalAdoptionRepository::new(exclusive.clone()).bootstrap_configuration(&"b".repeat(64),config()).await?;
        let doc=Doc {base:BaseFields::new(RecordId::new_v4(TableName::Doc),"history-test".into(),"original".into()),title:"Original".into(),text:"Original αβ evidence".into(),sidecar_text:None,source:None,media_attachments:vec![],rendition_key:"original".into(),metadata:serde_json::json!({})};
        let declaration=HistoricalDocumentInput {
            legacy_doc_id:doc.base.id.clone(),external_source_id:"original".into(),original_row_sha256:"a".repeat(64),
            doc_content_sha256:original_document_content_sha256(&doc)?,original_text_sha256:digest(doc.text.as_bytes()),original_text_utf8_bytes:doc.text.len(),title_sha256:digest(doc.title.as_bytes()),security_sha256:canonical_hash(&doc.base.security),provenance_sha256:canonical_hash(&doc.base.provenance),accepted_request_fingerprint:None,accepted_revision:None,
        };
        let group=HistoricalSourceGroup {external_source_id:"original".into(),document_mapping_sha256:document_mapping_sha256([(&declaration.legacy_doc_id,declaration.original_row_sha256.as_str())])?,documents:vec![declaration]};
        let input=HistoricalAdoptionInput {source_space_id:"history-test".into(),space_id:"history-test".into(),snapshot_sha256:"b".repeat(64),idmap_sha256:"c".repeat(64),plan_id:"d".repeat(64),documents_sha256:jsonl_hash(group.documents.iter().map(|d|serde_json::to_value(d).unwrap())),groups_sha256:jsonl_hash([serde_json::json!({"external_source_id":group.external_source_id,"documents":1,"document_mapping_sha256":group.document_mapping_sha256})]),groups:vec![group],snapshot_groups:vec![]};
        let revision=DocRevision {base:BaseFields::new(RecordId::new_v4(TableName::DocRevision),"history-test".into(),"original".into()),doc:Link::new(doc.base.id.clone()),text:doc.text.clone(),text_hash:digest(doc.text.as_bytes()),renderer:RendererMetadata{name:"original".into(),version:"1".into(),config_hash:None},manifest:None,supersedes:None,status:RevisionStatus::Active,rendered_at:Utc::now()};
        let mut nodes=Vec::new();
        for (index,role) in [NodeRole::Chunk,NodeRole::Paragraph,NodeRole::Summary].into_iter().enumerate() {
            let summary=role==NodeRole::Summary;
            nodes.push(DocNode {base:BaseFields::new(RecordId::new_v4(TableName::DocNode),"history-test".into(),"original".into()),revision:Link::new(revision.base.id.clone()),role,span:TextSpan::new(0,u32::try_from(doc.text.len()).unwrap()),node_key:format!("original-{index}"),text:summary.then(||"Original summary input".into()),parent:None,position:i32::try_from(index).unwrap(),depth:0,embedding_model:if index==0 {None} else {Some("legacy-unproven".into())},collection:None});
        }
        let mut entity=Entity::new(BaseFields::new(RecordId::new_v4(TableName::Entity),"history-test".into(),"original".into()),EntityType::new("person"),"Original person".into());
        let referenced = (snapshot_group || successive).then(||Entity::new(BaseFields::new(RecordId::new_v4(TableName::Entity),"history-test".into(),"original-reference".into()),EntityType::new("person"),"Referenced original person".into()));
        if let Some(reference) = &referenced {
            if snapshot_group { entity.base.subjects.push(Link::new(reference.base.id.clone())); }
            crate::EntityRepository::new(exclusive.clone()).create(reference.clone()).await?;
        }
        crate::DocRepository::new(exclusive.clone()).create(doc.clone()).await?;
        crate::DocRevisionRepository::new(exclusive.clone()).create(revision.clone()).await?;
        for node in &nodes { crate::DocNodeRepository::new(exclusive.clone()).create(node.clone()).await?; }
        crate::EntityRepository::new(exclusive.clone()).create(entity.clone()).await?;
        let mut merged=Entity::new(BaseFields::new(RecordId::new_v4(TableName::Entity),"history-test".into(),"original".into()),EntityType::new("person"),"Merged original".into());
        merged.status=elephant_types::records::entity::EntityStatus::Merged;
        let inventory=Entity::new(BaseFields::new(RecordId::new_v4(TableName::Entity),"history-test".into(),"original".into()),EntityType::new("document"),"Source document retained".into());
        crate::EntityRepository::new(exclusive.clone()).create(merged.clone()).await?;
        crate::EntityRepository::new(exclusive.clone()).create(inventory.clone()).await?;
        if snapshot_group {
            let record = CanonicalRecord::Entity(Box::new(entity.clone()));
            let original = serde_json::to_value(&entity).unwrap();
            let original_hash = canonical_hash(&original);
            let group = crate::historical_adoption::HistoricalSnapshotGraphGroup::from_rows(BTreeMap::from([(entity.base.id.clone(),original_hash.clone())]))?;
            let input = HistoricalAdoptionInput {source_space_id:"history-test".into(),space_id:"history-test".into(),snapshot_sha256:"b".repeat(64),idmap_sha256:"c".repeat(64),plan_id:"d".repeat(64),documents_sha256:digest(b""),groups_sha256:digest(b""),groups:vec![],snapshot_groups:vec![group]};
            let authority = HistoricalAdoptionRepository::new(exclusive.clone()).prepare_authority(input)?;
            let committed = Box::pin(authority.adopt_snapshot_graph(vec![crate::historical_adoption::HistoricalSnapshotGraphRow {record,original_row:original,original_row_sha256:original_hash}])).await?;
            return Ok((committed.receipt,nodes,entity,referenced,None,None));
        }
        let mut input=input;
        let snapshot_row=if shared_node {
            let mut base=BaseFields::new(RecordId::new_v4(TableName::Assertion),"history-test".into(),"retained-agent".into());
            base.provenance.work_run_id=Some(RecordId::new_v4(TableName::WorkRun));
            let mut raw=serde_json::to_value(&base).unwrap();
            raw.as_object_mut().unwrap().extend(serde_json::json!({"subject_text":"Original person","predicate_raw":"remembered","object_text":"Original evidence","subject_entity":entity.base.id,"evidence":{"revision":revision.base.id,"node":nodes[0].base.id,"start":0,"end":doc.text.len(),"quote_hash":revision.text_hash},"asserted_at":Utc::now(),"observed_at":Utc::now(),"assertion_key":"shared-original-node"}).as_object().unwrap().clone());
            let assertion:elephant_types::records::Assertion=serde_json::from_value(raw.clone()).unwrap();
            crate::AssertionRepository::new(exclusive.clone()).create(assertion.clone()).await?;
            let row=crate::historical_adoption::HistoricalSnapshotGraphRow {record:CanonicalRecord::Assertion(Box::new(assertion)),original_row_sha256:canonical_hash(&raw),original_row:raw};
            input.snapshot_groups.push(crate::historical_adoption::HistoricalSnapshotGraphGroup::from_rows(BTreeMap::from([(row.record.base().id.clone(),row.original_row_sha256.clone())]))?);
            Some(row)
        } else {None};
        let authority=HistoricalAdoptionRepository::new(exclusive.clone()).prepare_authority(input)?;
        let receipt=authority.adopt_group("original",HistoricalGroupRows {documents:vec![HistoricalOriginalDocument {original_document:serde_json::to_value(&doc).unwrap(),document:doc,revision,nodes:nodes.clone()}],original_sources:vec![],graph:vec![CanonicalRecord::Entity(Box::new(entity.clone())),CanonicalRecord::Entity(Box::new(merged)),CanonicalRecord::Entity(Box::new(inventory))]}).await?;
        let shared_snapshot=if let Some(row)=snapshot_row {
            let result=Box::pin(authority.adopt_snapshot_graph(vec![row])).await?;
            Some(PublicationFence {source_id:result.receipt.source_id,generation:result.receipt.operation.source_generation,revision_id:result.receipt.accepted_revision,operation_id:result.receipt.operation.operation_id})
        } else {None};
        let second_receipt = if successive {
            let second = referenced.as_ref().unwrap();
            let original = serde_json::to_value(second).unwrap();
            let original_hash = canonical_hash(&original);
            let group = crate::historical_adoption::HistoricalSnapshotGraphGroup::from_rows(BTreeMap::from([(second.base.id.clone(),original_hash.clone())]))?;
            let input = HistoricalAdoptionInput {source_space_id:"history-test".into(),space_id:"history-test".into(),snapshot_sha256:"b".repeat(64),idmap_sha256:"c".repeat(64),plan_id:"d".repeat(64),documents_sha256:digest(b""),groups_sha256:digest(b""),groups:vec![],snapshot_groups:vec![group]};
            Some(Box::pin(HistoricalAdoptionRepository::new(exclusive.clone()).prepare_authority(input)?.adopt_snapshot_graph(vec![crate::historical_adoption::HistoricalSnapshotGraphRow {record:CanonicalRecord::Entity(Box::new(second.clone())),original_row:original,original_row_sha256:original_hash}])).await?.receipt)
        } else { None };
        Ok((receipt,nodes,entity,referenced,shared_snapshot,second_receipt))
    }).await.unwrap();
    let publication = PublicationFence {
        source_id: receipt.source_id.clone(),
        generation: receipt.operation.source_generation.clone(),
        revision_id: receipt.accepted_revision.clone(),
        operation_id: receipt.operation.operation_id.clone(),
    };
    Fixture {
        storage,
        _directory: directory,
        receipt,
        publication,
        nodes,
        entity,
        referenced,
        shared_snapshot,
        second_receipt,
    }
}
async fn activate(f: &Fixture) -> SpaceConfig {
    let repo = crate::SpaceConfigRepository::new(f.storage.clone());
    let current = repo.get_by_space_id("history-test").await.unwrap().unwrap();
    let staged = repo
        .rebuild_active_embedding_profile(&current, "embedding")
        .await
        .unwrap();
    let id = staged
        .processing
        .as_ref()
        .unwrap()
        .pending_reindex
        .as_ref()
        .unwrap()
        .id
        .clone();
    // Deliberately complete durable counters without rows: certificate must
    // independently refuse this, and later prove the exact actual vectors.
    let nodes = repo
        .record_embedding_reindex_page(&staged, None, 3, true)
        .await
        .unwrap();
    let entities = repo
        .record_embedding_reindex_page(&nodes, None, 1, true)
        .await
        .unwrap();
    repo.activate_embedding_reindex(&entities, &id)
        .await
        .unwrap()
}
async fn store_vectors(f: &Fixture, config: &SpaceConfig, model: &str, nodes: usize, entity: bool) {
    // Real explicit reindex writer is used by the positive path in stage_vectors.
    let manager = elephant_embeddings::PartitionManager::new();
    let vectors = f
        .nodes
        .iter()
        .take(nodes)
        .map(|node| NodeEmbedding {
            base: node.base.clone(),
            node: Link::new(node.base.id.clone()),
            model: model.into(),
            dims: 3,
            vector: vec![1.0, 0.0, 0.0],
        })
        .collect();
    crate::NodeEmbeddingRepository::new(f.storage.clone(), manager)
        .store_reindex_embeddings(config, vectors)
        .await
        .unwrap();
    if entity {
        crate::entity_profile_embedding_repository::EntityProfileEmbeddingRepository::new(
            f.storage.clone(),
        )
        .store(
            config,
            &f.entity,
            model,
            &crate::entity_profile_embedding_repository::entity_input_hash(&f.entity),
            vec![1.0, 0.0, 0.0],
        )
        .await
        .unwrap();
    }
}
async fn stage_vectors(f: &Fixture, nodes: usize, entity: bool) -> SpaceConfig {
    stage_vectors_with_reference(f, nodes, entity, false).await
}
async fn stage_vectors_with_reference(
    f: &Fixture,
    nodes: usize,
    entity: bool,
    include_reference: bool,
) -> SpaceConfig {
    let repo = crate::SpaceConfigRepository::new(f.storage.clone());
    let current = repo.get_by_space_id("history-test").await.unwrap().unwrap();
    let staged = repo
        .rebuild_active_embedding_profile(&current, "embedding")
        .await
        .unwrap();
    let binding = repo
        .authorize_model_operation("history-test", ModelOperation::ReindexEmbedding, None)
        .await
        .unwrap();
    let policy = elephant_embeddings::governed::policy_for_binding(&binding).unwrap();
    store_vectors(f, &staged, &policy.model, nodes, entity).await;
    if include_reference {
        let reference = f.referenced.as_ref().unwrap();
        crate::entity_profile_embedding_repository::EntityProfileEmbeddingRepository::new(
            f.storage.clone(),
        )
        .store(
            &staged,
            reference,
            &policy.model,
            &crate::entity_profile_embedding_repository::entity_input_hash(reference),
            vec![1.0, 0.0, 0.0],
        )
        .await
        .unwrap();
    }
    let id = staged
        .processing
        .as_ref()
        .unwrap()
        .pending_reindex
        .as_ref()
        .unwrap()
        .id
        .clone();
    let page = repo
        .record_embedding_reindex_page(&staged, None, 3, true)
        .await
        .unwrap();
    let page = repo
        .record_embedding_reindex_page(&page, None, 1, true)
        .await
        .unwrap();
    repo.activate_embedding_reindex(&page, &id).await.unwrap()
}
#[allow(
    clippy::too_many_lines,
    reason = "Keep the complete ordered fixture and its failure/recovery assertions together."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture storage and its coverage, completion and config readers alive through successful certification and every subsequent invalidation/refusal assertion."
)]
async fn exercise_coverage(embedded: bool) {
    let f = fixture(embedded).await;
    let repo = HistoricalEmbeddingCoverageRepository::new(f.storage.clone());
    if !embedded {
        let revision = crate::surreal::thing_from_record_id(f.nodes[0].revision.id());
        let mut response = f
            .storage
            .query_bind_checked(
                &format!("{COVERAGE_NODE_LOOKUP} EXPLAIN"),
                crate::bindings! {"revision"=>revision},
            )
            .await
            .unwrap();
        let plan: surrealdb::Value = response.take(0).unwrap();
        let plan = plan.into_inner().to_string();
        assert!(
            plan.contains("Iterate Index") && plan.contains("historical_node_revision_lookup"),
            "{plan}"
        );
        assert!(!plan.contains("Iterate Table"), "{plan}");
    }
    let completion_repo = SourcePublicationManifestRepository::new(f.storage.clone());
    let original = completion_repo
        .completion("history-test", &f.publication)
        .await
        .unwrap()
        .unwrap();
    assert!(Box::pin(repo.certify("history-test", &f.publication))
        .await
        .is_err());
    let _ = activate(&f).await;
    assert!(
        Box::pin(repo.certify("history-test", &f.publication))
            .await
            .is_err(),
        "counters alone cannot complete historical operation"
    );
    let _ = stage_vectors(&f, 2, true).await;
    assert!(
        Box::pin(repo.certify("history-test", &f.publication))
            .await
            .is_err(),
        "summary vector must cover its independent summary text"
    );
    let _ = stage_vectors(&f, 3, true).await;
    let prepared = Box::pin(PreparedCoverage::prepare(
        &f.storage,
        "history-test",
        &f.publication,
    ))
    .await
    .unwrap();
    // Change the complete configuration after preparation; unchanged content
    // counters alone cannot authorize a stale completion transaction.
    let configs = crate::SpaceConfigRepository::new(f.storage.clone());
    let before = configs
        .get_by_space_id("history-test")
        .await
        .unwrap()
        .unwrap();
    let mut changed = before.clone();
    changed.purpose = Some("updated purpose".into());
    changed.processing.as_mut().unwrap().generation += 1;
    configs.replace_authorized(&before, changed).await.unwrap();
    assert!(prepared.commit(&f.storage).await.is_err());
    assert!(completion_repo
        .completion("history-test", &f.publication)
        .await
        .unwrap()
        .unwrap()
        .historical_embedding_coverage
        .is_none());
    let _ = stage_vectors(&f, 3, true).await;
    let done = Box::pin(repo.certify("history-test", &f.publication))
        .await
        .unwrap();
    assert_eq!(done.state, OperationState::Completed);
    assert!(done.completed_at.is_some());
    let completion = completion_repo
        .completion("history-test", &f.publication)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completion.completed_at, original.completed_at);
    assert_eq!(completion.commit_id, original.commit_id);
    let certificate = completion.historical_embedding_coverage.as_ref().unwrap();
    assert_eq!(certificate.node_inputs.len(), 3);
    assert_eq!(certificate.entity_inputs.len(), 1);
    assert_eq!(certificate.import_commit_id, f.receipt.commit_id);
    for node in &f.nodes {
        assert_eq!(
            &crate::DocNodeRepository::new(f.storage.clone())
                .get("history-test", &node.base.id)
                .await
                .unwrap()
                .unwrap(),
            node
        );
    }
    let replay = Box::pin(repo.certify("history-test", &f.publication))
        .await
        .unwrap();
    assert_eq!(replay, done);
    let replay_completion = completion_repo
        .completion("history-test", &f.publication)
        .await
        .unwrap()
        .unwrap();
    assert!(same(&completion, &replay_completion).unwrap());
    if !embedded {
        let node = &f.nodes[0];
        let table = PartitionKey::new(
            node.base.security.policy_id.clone(),
            level_bucket(node.base.security.level),
            format!("recall:{}", encode_payload(&certificate.partition).unwrap()),
            certificate.partition.dimensions,
        )
        .unwrap()
        .table_name();
        f.storage
            .query_checked(&format!("REMOVE INDEX {table}_hnsw ON TABLE {table};"))
            .await
            .unwrap();
        assert!(
            Box::pin(repo.certify("history-test", &f.publication))
                .await
                .is_err(),
            "a certificate replay still validates current physical readiness"
        );
    }
}
#[tokio::test]
async fn historical_coverage_embedded_requires_actual_vectors_and_exact_atomic_authority() {
    Box::pin(exercise_coverage(true)).await;
}
#[tokio::test]
async fn historical_coverage_native_requires_physical_duplicates_summary_and_current_authority() {
    Box::pin(exercise_coverage(false)).await;
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the fixture and its physical vectors alive through the final eligible-entity refusal."
)]
async fn historical_coverage_requires_eligible_entity_even_with_every_node_vector() {
    for embedded in [true, false] {
        let f = fixture(embedded).await;
        let _ = stage_vectors(&f, 3, false).await;
        assert!(
            Box::pin(
                HistoricalEmbeddingCoverageRepository::new(f.storage.clone())
                    .certify("history-test", &f.publication)
            )
            .await
            .is_err(),
            "eligible entity vector is required despite complete node vectors"
        );
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original fixture and borrowed revision through node insertion and complete population comparison."
)]
async fn historical_coverage_indexed_population_detects_added_nodes() {
    for embedded in [true, false] {
        let f = fixture(embedded).await;
        let revision = f.nodes[0].revision.id();
        let original: BTreeSet<_> = f.nodes.iter().map(|node| node.base.id.clone()).collect();
        assert_eq!(
            coverage_node_ids(&f.storage, "history-test", revision)
                .await
                .unwrap()
                .into_iter()
                .collect::<BTreeSet<_>>(),
            original
        );
        let mut extra = f.nodes[0].clone();
        extra.base.id = RecordId::new_v4(TableName::DocNode);
        extra.node_key = "extra-unproven-node".into();
        extra.position = 9;
        crate::DocNodeRepository::new(f.storage.clone())
            .create(extra.clone())
            .await
            .unwrap();
        let observed = coverage_node_ids(&f.storage, "history-test", revision)
            .await
            .unwrap();
        assert_eq!(observed.len(), original.len() + 1);
        assert!(observed.contains(&extra.base.id));
        assert_ne!(observed.into_iter().collect::<BTreeSet<_>>(), original);
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the native fixture and configuration reader through physical row replacement and exact before-image refusal."
)]
async fn historical_coverage_native_typed_read_pair_refuses_replaced_image() {
    let f = fixture(false).await;
    let repo = crate::SpaceConfigRepository::new(f.storage.clone());
    let config = repo.get_by_space_id("history-test").await.unwrap().unwrap();
    let id = crate::surreal::thing_from_record_id(&config.base.id);
    let before = native_row(&f.storage, &id).await.unwrap();
    validate_native_typed_image(
        &id,
        &before,
        crate::surreal::space_config_to_value(&config).unwrap(),
    )
    .unwrap();
    // An extra field is retained in full native CAS, not projected away.
    f.storage
        .query_bind_checked(
            "UPDATE ONLY $id SET coverage_extra='kept'",
            crate::bindings! {"id"=>id.clone()},
        )
        .await
        .unwrap();
    let extra = native_row(&f.storage, &id).await.unwrap();
    validate_native_typed_image(
        &id,
        &extra,
        crate::surreal::space_config_to_value(&config).unwrap(),
    )
    .unwrap();
    assert_ne!(before, extra);
    f.storage
        .query_bind_checked(
            "UPDATE ONLY $id SET purpose='replacement between reads'",
            crate::bindings! {"id"=>id.clone()},
        )
        .await
        .unwrap();
    let after = native_row(&f.storage, &id).await.unwrap();
    assert!(validate_native_typed_image(
        &id,
        &after,
        crate::surreal::space_config_to_value(&config).unwrap()
    )
    .is_err());

    let node = CanonicalRecord::DocNode(Box::new(f.nodes[0].clone()));
    let id = crate::surreal::thing_from_record_id(&node.base().id);
    let before = native_row(&f.storage, &id).await.unwrap();
    validate_native_typed_image(&id, &before, node.native().unwrap()).unwrap();
    f.storage
        .query_bind_checked(
            "UPDATE ONLY $id SET node_key='replacement between reads'",
            crate::bindings! {"id"=>id.clone()},
        )
        .await
        .unwrap();
    let after = native_row(&f.storage, &id).await.unwrap();
    assert!(validate_native_typed_image(&id, &after, node.native().unwrap()).is_err());
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain original fixture/control readers through paired control corruption, restoration and final commit refusal."
)]
async fn historical_coverage_control_read_pair_refuses_replaced_receipt_and_revision() {
    for embedded in [true, false] {
        let f = fixture(embedded).await;
        let _ = stage_vectors(&f, 3, true).await;
        let mut prepared = Box::pin(PreparedCoverage::prepare(
            &f.storage,
            "history-test",
            &f.publication,
        ))
        .await
        .unwrap();
        let repo = SourcePublicationRepository::new(f.storage.clone());
        for (table, key, field) in [
            (
                "source_commit_receipt",
                stable_key(&["history-test", &f.receipt.commit_id]),
                "input_fingerprint",
            ),
            (
                "source_revision",
                crate::source_publication::revision_key(
                    "history-test",
                    &f.publication.source_id,
                    &f.publication.revision_id,
                ),
                "input_fingerprint",
            ),
        ] {
            let original: serde_json::Value = prepared
                .control(&repo, &f.storage, table, key.clone())
                .await
                .unwrap();
            let mut changed = original.clone();
            changed[field] = serde_json::json!("different captured authority");
            let payload = encode_payload(&changed).unwrap();
            if let Some(store) = f.storage.embedded_store() {
                store
                    .mutate(|tx| {
                        let mut row = tx
                            .extension_row_by_key(table, &format!("{table}:{key}"))?
                            .unwrap();
                        row["payload"] = serde_json::Value::String(payload.clone());
                        tx.upsert_extension_row(table, row)?;
                        Ok(())
                    })
                    .await
                    .unwrap();
            } else {
                f.storage
                    .query_bind_checked(
                        "UPDATE ONLY $id SET payload=$payload",
                        crate::bindings! {"id"=>control_thing(table,&key),"payload"=>payload},
                    )
                    .await
                    .unwrap();
            }
            let captured: serde_json::Value = prepared
                .control(&repo, &f.storage, table, key)
                .await
                .unwrap();
            assert!(!same(&original, &captured).unwrap());
        }
        assert!(prepared.commit(&f.storage).await.is_err());
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain the original publication/configuration fixture through all binding negatives, certification and exact replay."
)]
async fn historical_coverage_bound_original_population_and_activation_are_exact() {
    for embedded in [true, false] {
        let f = fixture(embedded).await;
        let publication = SourcePublicationRepository::new(f.storage.clone());
        let head = publication
            .head("history-test", &f.publication.source_id)
            .await
            .unwrap()
            .unwrap();
        let receipt = CanonicalRepository::new(f.storage.clone())
            .receipt_read_only("history-test", &f.receipt.commit_id)
            .await
            .unwrap()
            .unwrap();
        let original = crate::client::HistoricalCatchupSource {
            source_id: f.publication.source_id.clone(),
            commit_id: receipt.commit_id.clone(),
            receipt_sha256: digest(&serde_json::to_vec(&receipt).unwrap()),
            head,
        };
        let active = stage_vectors(&f, 3, true).await;
        let reindex = active
            .processing
            .as_ref()
            .unwrap()
            .completed_embedding_reindex
            .as_ref()
            .unwrap()
            .reindex_id
            .clone();
        let repo = HistoricalEmbeddingCoverageRepository::new(f.storage.clone());
        for mode in 0..4 {
            let mut wrong = original.clone();
            let mut config = active.base.id.clone();
            let mut id = reindex.clone();
            match mode {
                0 => wrong.receipt_sha256 = "0".repeat(64),
                1 => wrong.commit_id.push_str("replacement"),
                2 => config = RecordId::new_v4(TableName::SpaceConfig),
                _ => id.push_str("replacement"),
            }
            assert!(
                Box::pin(repo.certify_bound("history-test", &wrong, &config, &id))
                    .await
                    .is_err()
            );
            let operation: StoredOperation = publication
                .read_control(
                    "source_operation",
                    &f.publication.operation_id,
                    "history-test",
                )
                .await
                .unwrap()
                .unwrap();
            assert_eq!(operation.receipt.state, OperationState::Processing);
        }
        let done =
            Box::pin(repo.certify_bound("history-test", &original, &active.base.id, &reindex))
                .await
                .unwrap();
        assert_eq!(done.state, OperationState::Completed);
        let replay =
            Box::pin(repo.certify_bound("history-test", &original, &active.base.id, &reindex))
                .await
                .unwrap();
        assert!(same(&done, &replay).unwrap());
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain the zero-document fixture and coverage reader through missing-reference refusal, complete vector proof and later invalidation."
)]
async fn historical_coverage_snapshot_zero_documents_requires_full_reference_vectors() {
    for embedded in [true, false] {
        let f = fixture_kind(embedded, true).await;
        let repo = HistoricalEmbeddingCoverageRepository::new(f.storage.clone());
        stage_vectors(&f, 0, true).await;
        assert!(
            Box::pin(repo.certify("history-test", &f.publication))
                .await
                .is_err(),
            "root vector alone does not cover its referenced entity"
        );
        // Stage every required vector before the matching activation; a later
        // vector write invalidates the completed activation's index generation.
        stage_vectors_with_reference(&f, 0, true, true).await;
        let reference = f.referenced.as_ref().unwrap();
        let done = Box::pin(repo.certify("history-test", &f.publication))
            .await
            .unwrap();
        assert_eq!(done.state, OperationState::Completed);
        let completion = SourcePublicationManifestRepository::new(f.storage.clone())
            .completion("history-test", &f.publication)
            .await
            .unwrap()
            .unwrap();
        let coverage = completion.historical_embedding_coverage.unwrap();
        assert!(coverage.node_inputs.is_empty());
        assert_eq!(coverage.entity_inputs.len(), 2);
        assert!(coverage.entity_inputs.contains_key(&reference.base.id));
        assert!(coverage.entity_inputs.contains_key(&f.entity.base.id));
        let prepared = Box::pin(PreparedCoverage::prepare(
            &f.storage,
            "history-test",
            &f.publication,
        ))
        .await
        .unwrap();
        let mut changed = reference.clone();
        changed.display_name.push_str(" changed");
        crate::EntityRepository::new(f.storage.clone())
            .update("history-test", &reference.base.id, changed)
            .await
            .unwrap();
        assert!(
            prepared.commit(&f.storage).await.is_err(),
            "referenced original image remains an atomic dependency"
        );
        assert!(Box::pin(repo.certify("history-test", &f.publication))
            .await
            .is_err());
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain original fixture authority through exact repeated dependency, changed-node refusal and final commit."
)]
async fn historical_coverage_repeated_node_dependency_is_exact_and_deduplicated() {
    for embedded in [true, false] {
        let f = fixture(embedded).await;
        stage_vectors(&f, 3, true).await;
        let mut prepared = Box::pin(PreparedCoverage::prepare(
            &f.storage,
            "history-test",
            &f.publication,
        ))
        .await
        .unwrap();
        let before = prepared.nodes.len();
        let guards = prepared.native.len();
        let mut hashes = BTreeMap::new();
        prepared
            .node_vector(&f.storage, f.nodes[0].clone(), &mut hashes)
            .await
            .unwrap();
        assert_eq!(prepared.nodes.len(), before);
        assert_eq!(prepared.native.len(), guards);
        assert!(hashes.is_empty());
        let mut changed = f.nodes[0].clone();
        changed.node_key.push_str("changed");
        assert!(prepared
            .node_vector(&f.storage, changed, &mut hashes)
            .await
            .is_err());
        assert_eq!(
            prepared.commit(&f.storage).await.unwrap().state,
            OperationState::Completed
        );
    }
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "Keep the complete ordered fixture and its failure/recovery assertions together."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the shared document/snapshot fixture and all original readers through the complete authority-mutation matrix and final revision invalidation."
)]
async fn historical_coverage_document_and_snapshot_share_exact_node_revision_and_vector() {
    for embedded in [true, false] {
        let f = fixture_flags(embedded, false, true).await;
        let snapshot = f.shared_snapshot.as_ref().unwrap();
        let repo = HistoricalEmbeddingCoverageRepository::new(f.storage.clone());
        let manifest = SourcePublicationManifestRepository::new(f.storage.clone())
            .get("history-test", snapshot)
            .await
            .unwrap()
            .unwrap();
        let proof = manifest.historical_adoption.as_ref().unwrap();
        let root_id = proof
            .graph_fingerprints
            .keys()
            .find(|id| id.table() == TableName::Assertion)
            .unwrap();
        let assertion = crate::AssertionRepository::new(f.storage.clone())
            .get("history-test", root_id)
            .await
            .unwrap()
            .unwrap();
        let reader = crate::read_eligibility::ReadEligibility::new(f.storage.clone());
        assert!(reader
            .project(assertion.clone(), &|_| true)
            .await
            .unwrap()
            .is_some());
        reader.verify_current().await.unwrap();
        assert!(
            crate::read_eligibility::ReadEligibility::new(f.storage.clone())
                .project(assertion.clone(), &|base| base.id != f.receipt.source_id)
                .await
                .unwrap()
                .is_none(),
            "document source denial is independent of snapshot anchor authorization"
        );
        assert!(
            crate::read_eligibility::ReadEligibility::new(f.storage.clone())
                .project(assertion, &|base| base.id != snapshot.source_id)
                .await
                .unwrap()
                .is_none(),
            "snapshot anchor denial is independent of document source authorization"
        );
        // A real source-less assertion references the same retained node/revision
        // already adopted by the document group. The two proofs stay distinct.
        assert!(Box::pin(repo.certify("history-test", snapshot))
            .await
            .is_err());
        let configs = crate::SpaceConfigRepository::new(f.storage.clone());
        let current = configs
            .get_by_space_id("history-test")
            .await
            .unwrap()
            .unwrap();
        let staged = configs
            .rebuild_active_embedding_profile(&current, "embedding")
            .await
            .unwrap();
        let manager = elephant_embeddings::PartitionManager::new();
        let embeddings = crate::NodeEmbeddingRepository::new(f.storage.clone(), manager);
        let wrong = NodeEmbedding {
            base: f.nodes[0].base.clone(),
            node: Link::new(f.nodes[0].base.id.clone()),
            model: "legacy-unproven".into(),
            dims: 3,
            vector: vec![1.0, 0.0, 0.0],
        };
        assert!(
            embeddings
                .store_reindex_embeddings(&staged, vec![wrong])
                .await
                .is_err(),
            "current profile never accepts relabelled legacy vectors"
        );
        // Finish this empty generation; certificate still independently refuses
        // missing vectors, regardless of durable page counters.
        let reindex = staged
            .processing
            .as_ref()
            .unwrap()
            .pending_reindex
            .as_ref()
            .unwrap()
            .id
            .clone();
        let nodes = configs
            .record_embedding_reindex_page(&staged, None, 3, true)
            .await
            .unwrap();
        let entities = configs
            .record_embedding_reindex_page(&nodes, None, 1, true)
            .await
            .unwrap();
        configs
            .activate_embedding_reindex(&entities, &reindex)
            .await
            .unwrap();
        assert!(Box::pin(repo.certify("history-test", snapshot))
            .await
            .is_err());
        stage_vectors(&f, 3, true).await;
        let doc_prepared = Box::pin(PreparedCoverage::prepare(
            &f.storage,
            "history-test",
            &f.publication,
        ))
        .await
        .unwrap();
        let snapshot_prepared = Box::pin(PreparedCoverage::prepare(
            &f.storage,
            "history-test",
            snapshot,
        ))
        .await
        .unwrap();
        assert_eq!(doc_prepared.nodes.len(), 3);
        assert_eq!(snapshot_prepared.nodes.len(), 1);
        // Both certificates capture the exact same physical vector; no second
        // vector row or new model invocation is created by certification.
        assert_eq!(
            Box::pin(repo.certify("history-test", &f.publication))
                .await
                .unwrap()
                .state,
            OperationState::Completed
        );
        assert_eq!(
            Box::pin(repo.certify("history-test", snapshot))
                .await
                .unwrap()
                .state,
            OperationState::Completed
        );
        let completions = SourcePublicationManifestRepository::new(f.storage.clone());
        let doc = completions
            .completion("history-test", &f.publication)
            .await
            .unwrap()
            .unwrap()
            .historical_embedding_coverage
            .unwrap();
        let graph = completions
            .completion("history-test", snapshot)
            .await
            .unwrap()
            .unwrap()
            .historical_embedding_coverage
            .unwrap();
        assert_eq!(doc.node_inputs.len(), 3);
        assert_eq!(graph.node_inputs.len(), 1);
        assert_eq!(
            doc.node_inputs.get(&f.nodes[0].base.id),
            graph.node_inputs.get(&f.nodes[0].base.id)
        );
        assert_eq!(
            doc.entity_inputs.get(&f.entity.base.id),
            graph.entity_inputs.get(&f.entity.base.id)
        );
        assert_eq!(doc.partition, graph.partition);
        assert_eq!(
            snapshot_prepared.nodes[0].embedding_id,
            doc_prepared
                .nodes
                .iter()
                .find(|entry| entry.node.base.id == f.nodes[0].base.id)
                .unwrap()
                .embedding_id
        );
        assert_eq!(
            snapshot_prepared.nodes[0].table,
            doc_prepared
                .nodes
                .iter()
                .find(|entry| entry.node.base.id == f.nodes[0].base.id)
                .unwrap()
                .table
        );
        assert_eq!(
            snapshot_prepared.nodes[0].embedded,
            doc_prepared
                .nodes
                .iter()
                .find(|entry| entry.node.base.id == f.nodes[0].base.id)
                .unwrap()
                .embedded
        );
        if !embedded {
            let node = &snapshot_prepared.nodes[0];
            let graph_row = snapshot_prepared
                .native
                .iter()
                .find(|guard| guard.id == node.native_id)
                .unwrap();
            let doc_row = doc_prepared
                .native
                .iter()
                .find(|guard| guard.id == node.native_id)
                .unwrap();
            assert_eq!(graph_row.row, doc_row.row);
        }
        // Stored original revision remains part of atomic snapshot closure,
        // even when its vector bytes and node row do not change.
        let prepared = Box::pin(PreparedCoverage::prepare(
            &f.storage,
            "history-test",
            snapshot,
        ))
        .await
        .unwrap();
        let revisions = crate::DocRevisionRepository::new(f.storage.clone());
        let mut revision = revisions
            .get("history-test", f.nodes[0].revision.id())
            .await
            .unwrap()
            .unwrap();
        revision.renderer.version.push_str("-changed");
        revisions
            .update("history-test", f.nodes[0].revision.id(), revision)
            .await
            .unwrap();
        assert!(prepared.commit(&f.storage).await.is_err());
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the same configuration and coverage readers through every source certification/replay and exact activation assertion."
)]
async fn historical_coverage_successive_sources_and_replays_keep_exact_activation() {
    for embedded in [true, false] {
        let f = fixture_population(embedded, false, true).await;
        let active = stage_vectors_with_reference(&f, 3, true, true).await;
        let configs = crate::SpaceConfigRepository::new(f.storage.clone());
        let repo = HistoricalEmbeddingCoverageRepository::new(f.storage.clone());
        let second = f.second_receipt.as_ref().unwrap();
        let second_publication = PublicationFence {
            source_id: second.source_id.clone(),
            generation: second.operation.source_generation.clone(),
            revision_id: second.accepted_revision.clone(),
            operation_id: second.operation.operation_id.clone(),
        };
        for publication in [
            &f.publication,
            &second_publication,
            &f.publication,
            &second_publication,
        ] {
            assert_eq!(
                Box::pin(repo.certify("history-test", publication))
                    .await
                    .unwrap()
                    .state,
                OperationState::Completed
            );
            let current = configs
                .get_by_space_id("history-test")
                .await
                .unwrap()
                .unwrap();
            assert!(
                same(&active, &current).unwrap(),
                "certificate metadata must preserve exact completed activation"
            );
        }
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain original native client through all invalidation transition assertions and response decoding."
)]
async fn historical_coverage_invalidation_native_embedded_exact_parity() {
    let before_payload = r#"{"space_id":"history-test","publication":{"generation":"1"},"manifest_digest":"m","commit_id":"c","completed_at":"t","historical_embedding_coverage":null}"#;
    let after_payload = before_payload.replace("null}", r#"{"reindex_id":"r"}}"#);
    let client = StorageClient::new(StorageConfig::default()).await.unwrap();
    let before = serde_json::json!({"id":"source_publication_completion:test","space_id":"history-test","payload":before_payload});
    let after = serde_json::json!({"id":"source_publication_completion:test","space_id":"history-test","payload":after_payload});
    let mut cases = vec![
        (before.clone(), after.clone(), true),
        (after.clone(), after.clone(), true),
    ];
    for key in ["id", "space_id"] {
        let mut changed = after.clone();
        changed[key] = "changed".into();
        cases.push((before.clone(), changed, false));
    }
    // Actual transaction-fence addition, replacement and removal preserve
    // visibility, but cannot hide any simultaneous semantic wrapper change.
    let mut fenced = before.clone();
    fenced["fence_nonce"] = "first transaction".into();
    let mut refenced = fenced.clone();
    refenced["fence_nonce"] = "second transaction".into();
    cases.extend([
        (before.clone(), fenced.clone(), true),
        (fenced.clone(), refenced, true),
        (fenced.clone(), before.clone(), true),
    ]);
    for key in ["id", "space_id", "state", "unknown", "read_nonce"] {
        let mut changed = fenced.clone();
        changed[key] = "changed".into();
        cases.push((before.clone(), changed, false));
    }
    let mut removed = fenced.clone();
    removed.as_object_mut().unwrap().remove("id");
    cases.push((before.clone(), removed, false));
    let mut malformed = fenced;
    malformed["payload"] = "{".into();
    cases.push((before.clone(), malformed, false));
    // Unchanged payload must not hide a wrapper-only authority mutation.
    for key in ["id", "space_id", "state", "unknown"] {
        let mut changed = before.clone();
        changed[key] = "changed".into();
        cases.push((before.clone(), changed, false));
    }
    let mut removed = before.clone();
    removed.as_object_mut().unwrap().remove("id");
    cases.push((before.clone(), removed, false));
    for payload in [
        after_payload.replace("\"commit_id\":\"c\"", "\"commit_id\":\"changed\""),
        after_payload.replace("\"generation\":\"1\"", "\"generation\":\"2\""),
        after_payload.replace(
            "\"generation\":\"1\"",
            "\"generation\":\"1\",\"generation\":\"1\"",
        ),
        after_payload.replace(
            "\"reindex_id\":\"r\"",
            "\"reindex_id\":\"r\",\"reindex_id\":\"r\"",
        ),
        "{".into(),
        "[]".into(),
    ] {
        let mut changed = after.clone();
        changed["payload"] = payload.into();
        cases.push((before.clone(), changed, false));
    }
    let mut sibling = after.clone();
    sibling["unknown"] = 1.into();
    cases.push((before.clone(), sibling, false));
    for (before, after, expected) in cases {
        assert_eq!(
            !elephant_db::control_visibility_changed(
                "source_publication_completion",
                Some(&before),
                Some(&after)
            ),
            expected
        );
        let mut response = client.query_bind_checked("RETURN !recall::completion_visibility_changed($before,$after);", crate::bindings! {"before"=>crate::surreal::json_to_surreal(before).unwrap(),"after"=>crate::surreal::json_to_surreal(after).unwrap()}).await.unwrap();
        assert_eq!(response.take::<Option<bool>>(0).unwrap(), Some(expected));
    }
    assert!(elephant_db::control_visibility_changed(
        "source_publication_completion",
        Some(&after),
        None
    ));
    assert!(elephant_db::control_visibility_changed(
        "source_publication_completion",
        None,
        Some(&after)
    ));
    let mut response = client
        .query_bind_checked(
            "RETURN recall::completion_coverage_only($before,NONE);",
            crate::bindings! {"before"=>crate::surreal::json_to_surreal(after).unwrap()},
        )
        .await
        .unwrap();
    assert_eq!(response.take::<Option<bool>>(0).unwrap(), Some(false));
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain the original native datastore through every physical-index corruption, restore and final refusal; completed transactions are dropped explicitly."
)]
async fn historical_coverage_native_physical_graph_and_identity_corruption_refuse() {
    use std::fmt::Write as _;
    use surrealdb::dbs::Session;
    use surrealdb::kvs::{Datastore, LockType::Optimistic, TransactionType::Write};
    use surrealdb::sql::Value;
    async fn check(ds: &Datastore, session: &Session, query: &str) -> Value {
        ds.execute(query, session, None)
            .await
            .unwrap()
            .remove(0)
            .result
            .unwrap()
    }
    let ds = Datastore::new("memory").await.unwrap();
    let session = Session::owner().with_ns("coverage").with_db("physical");
    let mut sql = "DEFINE INDEX knn ON points FIELDS vector HNSW DIMENSION 2 TYPE F32 DIST COSINE;"
        .to_owned();
    for n in 0..12 {
        write!(sql, "CREATE points:{n} SET vector=[1,0];").unwrap();
    }
    for response in ds.execute(&sql, &session, None).await.unwrap() {
        response.result.unwrap();
    }
    let inputs = (0..12)
        .map(|n| format!("{{id:points:{n},vector:[1,0]}}"))
        .collect::<Vec<_>>()
        .join(",");
    let query = format!("RETURN recall::hnsw_coverage('points','knn',[{inputs}]);");
    assert_eq!(check(&ds, &session, &query).await, Value::Bool(true));
    // Exact pinned native index-key prefix, as covered by the vendored key
    // codec fixtures. Inspect only this isolated synthetic index's physical keys.
    let prefix = b"/*coverage\0*physical\0*points\0+knn\0!h".to_vec();
    let tx = ds.transaction(Write, Optimistic).await.unwrap();
    let keys = tx.getp(prefix.clone()).await.unwrap();
    tx.cancel().await.unwrap();
    drop(tx);
    let families = keys
        .iter()
        .map(|(k, _)| k[prefix.len()])
        .collect::<std::collections::BTreeSet<_>>();
    for family in b"idvesn" {
        assert!(
            families.contains(family),
            "missing physical family {family}"
        );
    }
    assert!(keys.len() < 100, "bounded12-identity fixture");
    for (key, original) in &keys {
        if !b"idvesn".contains(&key[prefix.len()])
            || (key[prefix.len()] == b'd' && key.get(prefix.len() + 1) != Some(&1))
        {
            continue;
        }
        let tx = ds.transaction(Write, Optimistic).await.unwrap();
        tx.del(key.clone()).await.unwrap();
        tx.commit().await.unwrap();
        drop(tx);
        assert_eq!(
            check(&ds, &session, &query).await,
            Value::Bool(false),
            "removed physical key {key:?}"
        );
        let tx = ds.transaction(Write, Optimistic).await.unwrap();
        tx.set(key.clone(), original.clone(), None).await.unwrap();
        tx.commit().await.unwrap();
        drop(tx);
        assert_eq!(check(&ds, &session, &query).await, Value::Bool(true));
    }
    let reverse = keys
        .iter()
        .filter(|(k, _)| k[prefix.len()] == b'd' && k.get(prefix.len() + 1) == Some(&1))
        .collect::<Vec<_>>();
    assert!(reverse.len() >= 2);
    let tx = ds.transaction(Write, Optimistic).await.unwrap();
    tx.set(reverse[0].0.clone(), reverse[1].1.clone(), None)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    drop(tx);
    assert_eq!(check(&ds, &session, &query).await, Value::Bool(false));
    let tx = ds.transaction(Write, Optimistic).await.unwrap();
    tx.set(reverse[0].0.clone(), reverse[0].1.clone(), None)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    drop(tx);
    assert_eq!(
        check(
            &ds,
            &session,
            &query.replace("vector:[1,0]", "vector:[0,1]")
        )
        .await,
        Value::Bool(false)
    );
    for invalid in [
        "RETURN recall::hnsw_coverage('points','absent',[{id:points:0,vector:[1,0]}]);",
        "RETURN recall::hnsw_coverage('points','knn',[{id:points:0,vector:[1,0]},{id:points:0,vector:[1,0]}]);",
        "RETURN recall::hnsw_coverage('points','knn',[{id:points:0,vector:[1e-100f,0]}]);",
    ] { assert!(ds.execute(invalid,&session,None).await.unwrap().remove(0).result.is_err()); }
    for response in ds
        .execute("DELETE points:11;", &session, None)
        .await
        .unwrap()
    {
        response.result.unwrap();
    }
    assert_eq!(check(&ds, &session, &query).await, Value::Bool(false));
    for response in ds
        .execute("REMOVE INDEX knn ON points;", &session, None)
        .await
        .unwrap()
    {
        response.result.unwrap();
    }
    assert!(ds
        .execute(&query, &session, None)
        .await
        .unwrap()
        .remove(0)
        .result
        .is_err());
}
