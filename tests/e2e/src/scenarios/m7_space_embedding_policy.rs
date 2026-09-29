#![allow(
    clippy::unwrap_used,
    clippy::mixed_attributes_style,
    clippy::similar_names,
    clippy::type_complexity,
    clippy::float_cmp,
    clippy::const_is_empty,
    clippy::expect_used,
    clippy::panic,
    unused_imports,
    dead_code,
    clippy::too_many_lines,
    clippy::too_many_arguments,
    clippy::cognitive_complexity
)]
use chrono::Utc;
use elephant_embeddings::{
    level_bucket, EmbeddingProvider, FakeEmbeddingProvider, PartitionKey, PartitionManager,
};
use elephant_pipeline::EmbedWorker;
use elephant_storage::{
    DocNodeRepository, DocRepository, DocRevisionRepository, NodeEmbeddingRepository, Repository,
    SpaceConfigRepository,
};
use elephant_types::ids::TableName;
use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
use elephant_types::records::doc_node::{DocNode, NodeRole, TextSpan};
use elephant_types::records::doc_revision::{DocRevision, RendererMetadata, RevisionStatus};
use elephant_types::records::{Doc, SpaceConfig};
use elephant_types::{Link, RecordId};

use crate::Harness;

fn make_doc(space_id: &str, policy_id: &str, title: &str, text: &str) -> Doc {
    let mut base = BaseFields::new(
        RecordId::new_v4(TableName::Doc),
        space_id.to_string(),
        "scenario".to_string(),
    );
    base.security = Security {
        policy_id: policy_id.to_string(),
        level: 0,
        labels: vec![],
        handling: vec![],
    };
    base.provenance = Provenance::default();
    base.subjects_state = SubjectsState::Complete;

    Doc {
        base,
        title: title.to_string(),
        text: text.to_string(),
        sidecar_text: None,
        source: None,
        media_attachments: Vec::new(),
        rendition_key: format!("rendition-{title}"),
        metadata: serde_json::json!({}),
    }
}

fn make_space_config(space_id: &str, provider: &str, model: &str, dims: i32) -> SpaceConfig {
    let mut base = BaseFields::new(
        RecordId::new_v4(TableName::SpaceConfig),
        space_id.to_string(),
        "scenario".to_string(),
    );
    base.security = Security {
        policy_id: "default".to_string(),
        level: 0,
        labels: vec![],
        handling: vec![],
    };
    base.provenance = Provenance::default();
    base.subjects_state = SubjectsState::Complete;

    SpaceConfig {
        base,
        embedding_provider: provider.to_string(),
        embedding_model: model.to_string(),
        embedding_dims: dims,
        purpose: None,
        skill: None,
    }
}

async fn ingest_and_embed(
    doc_repo: &DocRepository,
    node_repo: &DocNodeRepository,
    revision_repo: &DocRevisionRepository,
    embedding_repo: &NodeEmbeddingRepository,
    space_id: &str,
    policy_id: &str,
    model: &str,
    dims: usize,
) -> anyhow::Result<RecordId> {
    let doc = make_doc(
        space_id,
        policy_id,
        &format!("Doc-{space_id}"),
        &format!("This document belongs to {space_id}."),
    );
    let doc_id = doc.base.id.clone();
    let doc_text = doc.text.clone();
    doc_repo.create(doc).await?;

    // Create a revision
    let revision_id = RecordId::new_v4(TableName::DocRevision);
    let mut rev_base = BaseFields::new(
        revision_id.clone(),
        space_id.to_string(),
        "scenario".to_string(),
    );
    rev_base.security = Security {
        policy_id: policy_id.to_string(),
        level: 0,
        labels: vec![],
        handling: vec![],
    };
    rev_base.provenance = Provenance::default();
    rev_base.subjects_state = SubjectsState::Complete;

    let revision = DocRevision {
        base: rev_base,
        doc: Link::new(doc_id.clone()),
        text: doc_text.clone(),
        text_hash: format!("sha256:hash_{space_id}"),
        renderer: RendererMetadata {
            name: "passthrough".to_string(),
            version: "1.0.0".to_string(),
            config_hash: None,
        },
        manifest: None,
        supersedes: None,
        status: RevisionStatus::Active,
        rendered_at: Utc::now(),
    };
    revision_repo.create(revision).await?;

    // Create a chunk node for embedding
    let node_id = RecordId::new_v4(TableName::DocNode);
    let mut node_base = BaseFields::new(
        node_id.clone(),
        space_id.to_string(),
        "scenario".to_string(),
    );
    node_base.security = Security {
        policy_id: policy_id.to_string(),
        level: 0,
        labels: vec![],
        handling: vec![],
    };
    node_base.provenance = Provenance::default();
    node_base.subjects_state = SubjectsState::Complete;

    let node = DocNode {
        base: node_base,
        revision: Link::new(revision_id.clone()),
        role: NodeRole::Chunk,
        span: TextSpan::new(0, doc_text.len() as u32),
        node_key: format!("chunk-0-{space_id}"),
        text: Some(doc_text),
        parent: None,
        position: 0,
        depth: 0,
        embedding_model: None,
        collection: Some("default".to_string()),
    };
    node_repo.create(node).await?;

    let provider = FakeEmbeddingProvider::new(model, dims);
    let embed_worker = EmbedWorker::new(
        node_repo.clone(),
        revision_repo.clone(),
        embedding_repo.clone(),
        provider,
    );
    let _result = embed_worker.process_pending(space_id, 10).await?;

    Ok(doc_id)
}

#[tokio::test]
#[ignore]
async fn scenario_m7_space_embeddings_isolated() -> anyhow::Result<()> {
    let harness = Harness::new().await?;

    let doc_repo = DocRepository::new(harness.client.clone());
    let node_repo = DocNodeRepository::new(harness.client.clone());
    let revision_repo = DocRevisionRepository::new(harness.client.clone());
    let embedding_repo =
        NodeEmbeddingRepository::new(harness.client.clone(), PartitionManager::new());
    let space_config_repo = SpaceConfigRepository::new(harness.client.clone());

    let space_a = "space_alpha";
    let space_b = "space_beta";

    let cfg_a = make_space_config(space_a, "fake", "model-a", 3);
    let cfg_b = make_space_config(space_b, "fake", "model-b", 5);
    space_config_repo.create(cfg_a).await?;
    space_config_repo.create(cfg_b).await?;

    let doc_a = ingest_and_embed(
        &doc_repo,
        &node_repo,
        &revision_repo,
        &embedding_repo,
        space_a,
        "policy-a",
        "model-a",
        3,
    )
    .await?;
    let doc_b = ingest_and_embed(
        &doc_repo,
        &node_repo,
        &revision_repo,
        &embedding_repo,
        space_b,
        "policy-b",
        "model-b",
        5,
    )
    .await?;

    let provider_a = FakeEmbeddingProvider::new("model-a", 3);
    let query_a = provider_a
        .embed(std::slice::from_ref(&"belongs to alpha".to_string()))
        .await?
        .pop()
        .expect("vector");
    let key_a = PartitionKey::new(
        "policy-a".to_string(),
        level_bucket(0),
        "model-a".to_string(),
        3,
    )?;
    let hits_a = embedding_repo
        .search_partition(&key_a, space_a, &query_a, 5, None, false)
        .await?;
    assert!(!hits_a.is_empty(), "expected hits for space A");

    let provider_b = FakeEmbeddingProvider::new("model-b", 5);
    let query_b = provider_b
        .embed(std::slice::from_ref(&"belongs to beta".to_string()))
        .await?
        .pop()
        .expect("vector");
    let key_b = PartitionKey::new(
        "policy-b".to_string(),
        level_bucket(0),
        "model-b".to_string(),
        5,
    )?;
    let hits_b = embedding_repo
        .search_partition(&key_b, space_b, &query_b, 5, None, false)
        .await?;
    assert!(!hits_b.is_empty(), "expected hits for space B");

    let doc_a_result = doc_repo.get(space_a, &doc_a).await?.expect("doc a");
    assert_eq!(doc_a_result.base.space_id, space_a);

    let doc_b_result = doc_repo.get(space_b, &doc_b).await?.expect("doc b");
    assert_eq!(doc_b_result.base.space_id, space_b);

    Ok(())
}
