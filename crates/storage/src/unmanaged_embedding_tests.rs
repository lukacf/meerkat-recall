//! Ownership regressions exercise the same guards as the active vector transaction.
#![allow(clippy::expect_used, clippy::unwrap_used)]
use super::*;
use crate::{NodeEmbeddingRepository, StorageConfig};
use chrono::{Duration, Utc};
use elephant_embeddings::PartitionManager;
use elephant_types::records::base::BaseFields;
use elephant_types::records::doc_node::{NodeRole, TextSpan};
use elephant_types::records::doc_revision::{RendererMetadata, RevisionStatus};
use elephant_types::records::NodeEmbedding;
use elephant_types::{Link, TableName};

const SPACE: &str = "named-embedding-space";
const MODEL: &str = "ownership-test";

async fn client(embedded: bool) -> StorageClient {
    let client = if embedded {
        StorageClient::new_embedded().await
    } else {
        StorageClient::new(StorageConfig {
            database: format!("embedding_ownership_{}", uuid::Uuid::new_v4().simple()),
            ..StorageConfig::default()
        })
        .await
    }
    .expect("backend");
    if !embedded {
        client
            .query_checked(include_str!(
                "../../migrations/sql/031_source_publication.surql"
            ))
            .await
            .expect("explicit publication migration");
    }
    client
}

async fn fixture(
    client: &StorageClient,
    source: Option<RecordId>,
    position: i32,
) -> (Doc, DocRevision, DocNode) {
    let doc = Doc {
        base: BaseFields::new(
            RecordId::new_v4(TableName::Doc),
            SPACE.into(),
            "test".into(),
        ),
        title: "Standalone or managed".into(),
        text: "hello".into(),
        sidecar_text: None,
        source: source.map(Link::new),
        media_attachments: vec![],
        rendition_key: format!("fixture-{position}"),
        metadata: serde_json::json!({}),
    };
    let revision = DocRevision {
        base: BaseFields::new(
            RecordId::new_v4(TableName::DocRevision),
            SPACE.into(),
            "test".into(),
        ),
        doc: Link::new(doc.base.id.clone()),
        text: "hello".into(),
        text_hash: "hash".into(),
        renderer: RendererMetadata {
            name: "test".into(),
            version: "1".into(),
            config_hash: None,
        },
        manifest: None,
        supersedes: None,
        status: RevisionStatus::Active,
        rendered_at: Utc::now(),
    };
    let mut node = DocNode {
        base: BaseFields::new(
            RecordId::new_v4(TableName::DocNode),
            SPACE.into(),
            "test".into(),
        ),
        revision: Link::new(revision.base.id.clone()),
        role: NodeRole::Chunk,
        span: TextSpan::new(0, 5),
        node_key: format!("node-{position}"),
        text: Some("hello".into()),
        parent: None,
        position,
        depth: 0,
        embedding_model: None,
        collection: None,
    };
    // Stable ordering without clocks/sleeps deciding which row consumes a page.
    node.base.created_at = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap()
        + Duration::seconds(i64::from(position));
    DocRepository::new(client.clone())
        .create(doc.clone())
        .await
        .expect("doc");
    DocRevisionRepository::new(client.clone())
        .create(revision.clone())
        .await
        .expect("revision");
    DocNodeRepository::new(client.clone())
        .create(node.clone())
        .await
        .expect("node");
    (doc, revision, node)
}

async fn insert_head(client: &StorageClient, source: &RecordId, stored_space: &str) {
    let key = crate::source_publication::head_key(SPACE, source);
    // Deliberately not a decodable publication: physical ownership cannot fall
    // through to standalone merely because the payload or stored space is bad.
    if let Some(store) = client.embedded_store() {
        store
            .mutate(|tx| {
                tx.upsert_extension_row(
                    "source_head",
                    serde_json::json!({
                        "id": format!("source_head:{key}"), "space_id": stored_space,
                        "source_id": source.to_string(), "payload": "malformed"
                    }),
                )?;
                Ok(())
            })
            .await
            .expect("physical head");
    } else {
        client
            .query_bind_checked(
                "UPSERT $id SET space_id=$space, source_id=$source, payload='malformed';",
                crate::bindings! {
                    "id" => crate::source_publication::control_thing("source_head", &key),
                    "space" => stored_space, "source" => source.to_string(),
                },
            )
            .await
            .expect("physical head");
    }
}

async fn write(client: &StorageClient, node: &DocNode) -> StorageResult<()> {
    let vector = NodeEmbedding {
        base: node.base.clone(),
        node: Link::new(node.base.id.clone()),
        model: MODEL.into(),
        dims: 3,
        vector: vec![1.0, 0.0, 0.0],
    };
    NodeEmbeddingRepository::new(client.clone(), PartitionManager::new())
        .store_embeddings_and_mark_nodes(
            SPACE,
            std::slice::from_ref(&node.base.id),
            MODEL,
            Utc::now(),
            vec![vector],
        )
        .await
}

async fn marker(client: &StorageClient, id: &RecordId) -> Option<String> {
    DocNodeRepository::new(client.clone())
        .get(SPACE, id)
        .await
        .unwrap()
        .unwrap()
        .embedding_model
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep repo in the original fixture scope through assertions and cleanup."
)]
async fn pending_embedding_pages_past_managed_heads_on_both_backends() {
    for embedded in [true, false] {
        let client = client(embedded).await;
        for position in 0..3 {
            let source = RecordId::new_v4(TableName::Source);
            fixture(&client, Some(source.clone()), position).await;
            insert_head(
                &client,
                &source,
                if position == 1 { "foreign" } else { SPACE },
            )
            .await;
        }
        let (deleted, _, _) = fixture(&client, None, 3).await;
        DocRepository::new(client.clone())
            .delete(SPACE, &deleted.base.id)
            .await
            .unwrap();
        let (_, _, standalone) = fixture(&client, None, 4).await;
        let repo = DocNodeRepository::new(client.clone());
        assert!(repo
            .find_pending_embedding(SPACE, 0)
            .await
            .unwrap()
            .is_empty());
        let pending = repo.find_pending_embedding(SPACE, 1).await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].base.id, standalone.base.id);
        assert!(repo
            .find_pending_embedding("quiet-other-space", 1)
            .await
            .unwrap()
            .is_empty());
    }
}

#[tokio::test]
async fn generic_embedding_refuses_managed_heads_but_keeps_standalone_writes() {
    for embedded in [true, false] {
        let client = client(embedded).await;
        let source = RecordId::new_v4(TableName::Source);
        let (_, _, managed) = fixture(&client, Some(source.clone()), 0).await;
        insert_head(&client, &source, SPACE).await;
        assert!(write(&client, &managed).await.is_err());
        assert!(DocNodeRepository::new(client.clone())
            .mark_unmanaged_embedding_failed(
                SPACE,
                std::slice::from_ref(&managed.base.id),
                "dead-letter"
            )
            .await
            .is_err());
        assert_eq!(marker(&client, &managed.base.id).await, None);
        for (position, source) in [(1, None), (2, Some(RecordId::new_v4(TableName::Source)))] {
            let (_, _, standalone) = fixture(&client, source, position).await;
            write(&client, &standalone)
                .await
                .expect("unmanaged vector transaction");
            assert_eq!(
                marker(&client, &standalone.base.id).await.as_deref(),
                Some(MODEL)
            );
        }
    }
}

async fn guarded_marker_write(
    client: &StorageClient,
    node: &DocNode,
    guards: &[Ownership],
) -> StorageResult<()> {
    if let Some(store) = client.embedded_store() {
        store
            .mutate(|tx| {
                // Change a row first: a subsequent guard refusal must roll it back.
                let mut row = tx.get_by_id::<DocNode>(&node.base.id, true)?.unwrap();
                row.embedding_model = Some("must-rollback".into());
                tx.replace(row)?;
                check_tx(tx, SPACE, guards)
            })
            .await
    } else {
        let mut sql = String::from(
            "BEGIN TRANSACTION; UPDATE $test_node SET embedding_model='must-rollback';\n",
        );
        let mut bindings = BTreeMap::new();
        bindings.insert(
            "test_node".into(),
            crate::surreal::thing_from_record_id(&node.base.id).into(),
        );
        append_sql(&mut sql, &mut bindings, SPACE, guards);
        sql.push_str("COMMIT TRANSACTION;");
        client.query_bind_checked(&sql, bindings).await.map(|_| ())
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep client in the original fixture scope through assertions and cleanup."
)]
async fn ownership_guard_rejects_head_arrival_after_preparation_atomically() {
    for embedded in [true, false] {
        for stored_space in [SPACE, "foreign"] {
            let client = client(embedded).await;
            let source = RecordId::new_v4(TableName::Source);
            let (_, _, node) = fixture(&client, Some(source.clone()), 0).await;
            let guards = prepare(&client, SPACE, std::slice::from_ref(&node.base.id))
                .await
                .unwrap();
            insert_head(&client, &source, stored_space).await;
            assert!(guarded_marker_write(&client, &node, &guards).await.is_err());
            assert_eq!(marker(&client, &node.base.id).await, None);
        }
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep client in the original fixture scope through assertions and cleanup."
)]
async fn ownership_guard_rejects_changed_document_revision_or_deletion() {
    for embedded in [true, false] {
        for change in ["source", "document", "deleted"] {
            let client = client(embedded).await;
            let (mut doc, mut revision, node) = fixture(&client, None, 0).await;
            let guards = prepare(&client, SPACE, std::slice::from_ref(&node.base.id))
                .await
                .unwrap();
            if change == "document" {
                revision.doc = Link::new(RecordId::new_v4(TableName::Doc));
                DocRevisionRepository::new(client.clone())
                    .update(SPACE, &revision.base.id.clone(), revision)
                    .await
                    .unwrap();
            } else if change == "source" {
                doc.source = Some(Link::new(RecordId::new_v4(TableName::Source)));
                DocRepository::new(client.clone())
                    .update(SPACE, &doc.base.id.clone(), doc)
                    .await
                    .unwrap();
            } else {
                DocRepository::new(client.clone())
                    .delete(SPACE, &doc.base.id)
                    .await
                    .unwrap();
            }
            assert!(
                guarded_marker_write(&client, &node, &guards).await.is_err(),
                "{change}"
            );
            assert_eq!(marker(&client, &node.base.id).await, None);
        }
    }
}
