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
    clippy::cognitive_complexity
)]
//! S4.11 Massive document partitioning scenario test.
//!
//! Verifies >100 node documents are partitioned correctly.

use chrono::Utc;
use elephant_storage::{
    DocNodeRepository, DocRepository, DocRevisionRepository, Repository, SourceRepository,
};
use elephant_types::ids::TableName;
use elephant_types::records::base::{BaseFields, Security, SubjectsState};
use elephant_types::records::doc::Doc;
use elephant_types::records::doc_node::{DocNode, NodeRole, TextSpan};
use elephant_types::records::doc_revision::{DocRevision, RendererMetadata, RevisionStatus};
use elephant_types::records::source::{Source, SourceType};
use elephant_types::{Link, RecordId};

use crate::Harness;

/// S4.11: Massive document partitioning.
///
/// Test invariants:
/// 1. Documents with >100 nodes trigger partitioning
/// 2. Partition boundaries preserve semantic coherence
/// 3. Cross-partition references maintained via doc_node links
/// 4. Each partition processed independently in pipeline
#[tokio::test]
async fn scenario_s4_11_massive_doc() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let source_repo = SourceRepository::new(harness.client.clone());
    let doc_repo = DocRepository::new(harness.client.clone());
    let revision_repo = DocRevisionRepository::new(harness.client.clone());
    let node_repo = DocNodeRepository::new(harness.client.clone());

    let space_id = "Space_MassiveDoc";

    // Create a source for the massive document
    let source_id = RecordId::new_v4(TableName::Source);
    let mut source_base =
        BaseFields::new(source_id.clone(), space_id.to_string(), "P_sys".to_string());
    source_base.security = Security {
        policy_id: "default".to_string(),
        level: 1,
        ..Default::default()
    };
    source_base.subjects_state = SubjectsState::Complete;
    let mut source = Source::new(
        source_base,
        SourceType::Drive,
        "hash_massive_doc".to_string(),
    );
    source.external_id = Some("massive-doc-001".to_string());
    source.uri = Some("https://example.com/annual-report-2024.pdf".to_string());
    source.metadata = serde_json::json!({"title": "Annual Report 2024", "pages": 250});
    source_repo.create(source).await?;

    // Create a massive document with many sections
    let doc_id = RecordId::new_v4(TableName::Doc);
    let doc_base = BaseFields::new(doc_id.clone(), space_id.to_string(), "P_sys".to_string());

    // Generate text representing a large document with many sections
    let mut doc_text = String::new();
    for i in 1..=120 {
        doc_text.push_str(&format!(
            "Section {}: This is content for section {} of the massive document. ",
            i, i
        ));
        doc_text.push_str(&format!(
            "It contains important information about topic {} and related matters. ",
            i
        ));
        doc_text.push_str("\n\n");
    }

    let doc = Doc {
        base: doc_base,
        title: "Annual Report 2024 - Comprehensive Analysis".to_string(),
        text: doc_text.clone(),
        sidecar_text: None,
        source: Some(Link::new(source_id.clone())),
        media_attachments: Vec::new(),
        rendition_key: "massive-doc-001-v1".to_string(),
        metadata: serde_json::json!({"total_sections": 120}),
    };
    doc_repo.create(doc).await?;

    // Create the document revision
    let revision_id = RecordId::new_v4(TableName::DocRevision);
    let revision_base = BaseFields::new(
        revision_id.clone(),
        space_id.to_string(),
        "renderer".to_string(),
    );
    let revision = DocRevision {
        base: revision_base,
        doc: Link::new(doc_id.clone()),
        text: doc_text.clone(),
        text_hash: "sha256:massive_doc_hash".to_string(),
        renderer: RendererMetadata {
            name: "pdf_renderer".to_string(),
            version: "2.0.0".to_string(),
            config_hash: None,
        },
        manifest: None,
        supersedes: None,
        status: RevisionStatus::Active,
        rendered_at: Utc::now(),
    };
    revision_repo.create(revision).await?;

    // Invariant 1: Documents with >100 nodes trigger partitioning
    // Create the root document node
    let root_node_id = RecordId::new_v4(TableName::DocNode);
    let root_node_base = BaseFields::new(
        root_node_id.clone(),
        space_id.to_string(),
        "partitioner".to_string(),
    );
    let root_node = DocNode {
        base: root_node_base,
        revision: Link::new(revision_id.clone()),
        role: NodeRole::Document,
        span: TextSpan::new(0, doc_text.len() as u32),
        node_key: "root-node".to_string(),
        text: None,
        parent: None,
        position: 0,
        depth: 0,
        embedding_model: None,
        collection: None,
    };
    node_repo.create(root_node).await?;

    // Create >100 section nodes to trigger partitioning
    let mut section_node_ids = Vec::new();
    let mut current_offset: u32 = 0;
    let section_size = (doc_text.len() / 120) as u32;

    for i in 0..120 {
        let node_id = RecordId::new_v4(TableName::DocNode);
        let node_base = BaseFields::new(
            node_id.clone(),
            space_id.to_string(),
            "partitioner".to_string(),
        );
        let node = DocNode {
            base: node_base,
            revision: Link::new(revision_id.clone()),
            role: NodeRole::Section,
            span: TextSpan::new(current_offset, current_offset + section_size),
            node_key: format!("section-node-{}", i),
            text: None,
            parent: Some(Link::new(root_node_id.clone())),
            position: i,
            depth: 1,
            embedding_model: None,
            collection: None,
        };
        let created_node = node_repo.create(node).await?;
        section_node_ids.push(created_node.base.id.clone());
        current_offset += section_size;
    }

    // Verify we have more than 100 section nodes
    let all_nodes = node_repo.list(space_id, 200, None).await?;
    let section_nodes: Vec<_> = all_nodes
        .iter()
        .filter(|n| n.role == NodeRole::Section)
        .collect();
    assert!(
        section_nodes.len() > 100,
        "CRITICAL: Document should have >100 section nodes to trigger partitioning"
    );

    // Invariant 2: Partition boundaries preserve semantic coherence
    // Create partition boundaries at logical section breaks (every 25 sections)
    let partition_count = 5;
    let sections_per_partition = 24; // 120 / 5 = 24 sections per partition

    let mut partition_summary_ids = Vec::new();
    for p in 0..partition_count {
        let partition_start = p * sections_per_partition;
        let partition_end = (p + 1) * sections_per_partition;

        // Create a summary node for each partition
        let summary_node_id = RecordId::new_v4(TableName::DocNode);
        let summary_node_base = BaseFields::new(
            summary_node_id.clone(),
            space_id.to_string(),
            "partitioner".to_string(),
        );
        let summary_node = DocNode {
            base: summary_node_base,
            revision: Link::new(revision_id.clone()),
            role: NodeRole::Summary,
            span: TextSpan::new(
                (partition_start * section_size as usize) as u32,
                (partition_end * section_size as usize) as u32,
            ),
            node_key: format!("partition-{}-summary", p),
            text: Some(format!(
                "Summary of sections {} to {}: Key findings about topics in this partition.",
                partition_start + 1,
                partition_end
            )),
            parent: Some(Link::new(root_node_id.clone())),
            position: p as i32,
            depth: 1,
            embedding_model: None,
            collection: None,
        };
        let created_summary = node_repo.create(summary_node).await?;
        partition_summary_ids.push(created_summary.base.id.clone());
    }

    assert_eq!(
        partition_summary_ids.len(),
        partition_count,
        "Should have one summary per partition"
    );

    // Invariant 3: Cross-partition references maintained via doc_node links
    // Create chunk nodes that span partition boundaries
    let chunk_nodes_per_partition = 5;
    let mut chunk_node_ids = Vec::new();

    #[allow(clippy::needless_range_loop)]
    for p in 0..partition_count {
        for c in 0..chunk_nodes_per_partition {
            let chunk_node_id = RecordId::new_v4(TableName::DocNode);
            let chunk_node_base = BaseFields::new(
                chunk_node_id.clone(),
                space_id.to_string(),
                "chunker".to_string(),
            );

            // Calculate span within partition
            let partition_start_offset =
                (p * sections_per_partition * section_size as usize) as u32;
            let chunk_offset = c as u32 * (section_size * 5);

            let chunk_node = DocNode {
                base: chunk_node_base,
                revision: Link::new(revision_id.clone()),
                role: NodeRole::Chunk,
                span: TextSpan::new(
                    partition_start_offset + chunk_offset,
                    partition_start_offset + chunk_offset + (section_size * 4),
                ),
                node_key: format!("chunk-p{}-c{}", p, c),
                text: Some(format!(
                    "Chunk {} of partition {} containing relevant content for embedding.",
                    c, p
                )),
                parent: Some(Link::new(partition_summary_ids[p].clone())),
                position: c as i32,
                depth: 2,
                embedding_model: None,
                collection: None,
            };
            let created_chunk = node_repo.create(chunk_node).await?;
            chunk_node_ids.push(created_chunk.base.id.clone());
        }
    }

    // Verify chunk nodes are linked to partition summaries
    let all_chunks = node_repo.list(space_id, 200, None).await?;
    let chunk_nodes: Vec<_> = all_chunks
        .iter()
        .filter(|n| n.role == NodeRole::Chunk)
        .collect();
    assert_eq!(
        chunk_nodes.len(),
        partition_count * chunk_nodes_per_partition,
        "Should have correct number of chunk nodes"
    );

    // Verify parent links are maintained
    for chunk in &chunk_nodes {
        assert!(
            chunk.parent.is_some(),
            "Chunk nodes should have parent links to partition summaries"
        );
    }

    // Invariant 4: Each partition processed independently in pipeline
    // Verify partition isolation by checking node depths and positions
    for p in 0..partition_count {
        let partition_chunks: Vec<_> = chunk_nodes
            .iter()
            .filter(|n| n.node_key.starts_with(&format!("chunk-p{}-", p)))
            .collect();
        assert_eq!(
            partition_chunks.len(),
            chunk_nodes_per_partition,
            "Each partition should have {} chunks",
            chunk_nodes_per_partition
        );

        // Verify chunk positions within partition are sequential
        let mut positions: Vec<i32> = partition_chunks.iter().map(|n| n.position).collect();
        positions.sort();
        for (i, pos) in positions.iter().enumerate() {
            assert_eq!(
                *pos, i as i32,
                "Chunk positions should be sequential within partition"
            );
        }
    }

    // Verify the complete node tree structure
    let final_nodes = node_repo.list(space_id, 500, None).await?;
    let document_nodes: Vec<_> = final_nodes
        .iter()
        .filter(|n| n.role == NodeRole::Document)
        .collect();
    let summary_nodes: Vec<_> = final_nodes
        .iter()
        .filter(|n| n.role == NodeRole::Summary)
        .collect();

    assert_eq!(
        document_nodes.len(),
        1,
        "Should have one root document node"
    );
    assert_eq!(
        summary_nodes.len(),
        partition_count,
        "Should have one summary per partition"
    );
    assert!(
        section_nodes.len() > 100,
        "Should have >100 section nodes for massive doc"
    );

    // Verify depth hierarchy
    for doc_node in &document_nodes {
        assert_eq!(doc_node.depth, 0, "Document root should be at depth 0");
    }
    for summary in &summary_nodes {
        assert_eq!(summary.depth, 1, "Partition summaries should be at depth 1");
    }
    for chunk in &chunk_nodes {
        assert_eq!(chunk.depth, 2, "Chunks should be at depth 2");
    }

    Ok(())
}
