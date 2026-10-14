//! Native staged-vector transaction custody; no model or transport is invoked.
//! This checks a real writer, not semantic relevance or deterministic overlap.
use super::spy;
use anyhow::Context;
use elephant_embeddings::governed::policy_for_binding;
use elephant_embeddings::{PartitionKey, PartitionManager};
use elephant_storage::canonical_commit::CanonicalRepository;
use elephant_storage::{
    DocNodeRepository, DocRepository, DocRevisionRepository, NodeEmbeddingRepository, Repository,
    SpaceConfigRepository, StorageClient,
};
use elephant_types::provider_binding::{ModelOperation, OperationBinding};
use elephant_types::records::base::BaseFields;
use elephant_types::records::doc_node::{NodeRole, TextSpan};
use elephant_types::records::doc_revision::{DocRevision, RendererMetadata, RevisionStatus};
use elephant_types::records::{Doc, DocNode, NodeEmbedding};
use elephant_types::{Link, RecordId, TableName};
use sha2::{Digest, Sha256};

async fn stored_vectors(
    client: &StorageClient,
    partition: &PartitionKey,
    space: &str,
) -> anyhow::Result<Vec<Vec<f32>>> {
    let mut result = client
        .query_bind_checked(
            &format!(
                "SELECT VALUE vector FROM {} WHERE space_id = $space",
                partition.table_name()
            ),
            elephant_storage::bindings! { "space" => space },
        )
        .await?;
    Ok(result.take(0)?)
}

#[tokio::test]
async fn native_reindex_rotates_only_its_existing_epoch_and_preserves_approval_snapshot(
) -> anyhow::Result<()> {
    Box::pin(exercise_native_reindex_writer()).await
}

#[allow(clippy::too_many_lines)]
async fn exercise_native_reindex_writer() -> anyhow::Result<()> {
    let client = Box::pin(spy::storage(spy::Backend::SurrealMemory)).await?;
    let configs = SpaceConfigRepository::new(client.clone());
    for space in spy::SPACES {
        // The endpoint is inert: this test only calls repositories.
        Box::pin(spy::configure(&configs, space, "http://127.0.0.1:1/v1", 8)).await?;
    }
    let space = spy::SPACES[0];
    let text = "Original repository transaction input.";
    let doc = DocRepository::new(client.clone())
        .create(Doc {
            base: BaseFields::new(
                RecordId::new_v4(TableName::Doc),
                space.into(),
                "test".into(),
            ),
            title: "Native vector custody".into(),
            text: text.into(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "native-vector-custody".into(),
            metadata: serde_json::json!({}),
        })
        .await?;
    let revision = DocRevisionRepository::new(client.clone())
        .create(DocRevision {
            base: BaseFields::new(
                RecordId::new_v4(TableName::DocRevision),
                space.into(),
                "test".into(),
            ),
            doc: Link::new(doc.base.id),
            text: text.into(),
            text_hash: format!("{:x}", Sha256::digest(text.as_bytes())),
            renderer: RendererMetadata {
                name: "repository-fixture".into(),
                version: "1".into(),
                config_hash: None,
            },
            manifest: None,
            supersedes: None,
            status: RevisionStatus::Active,
            rendered_at: chrono::Utc::now(),
        })
        .await?;
    let node = DocNodeRepository::new(client.clone())
        .create(DocNode {
            base: BaseFields::new(
                RecordId::new_v4(TableName::DocNode),
                space.into(),
                "test".into(),
            ),
            revision: Link::new(revision.base.id),
            role: NodeRole::Chunk,
            span: TextSpan::new(0, u32::try_from(text.len())?),
            node_key: "native-vector-custody".into(),
            text: Some(text.into()),
            parent: None,
            position: 0,
            depth: 0,
            embedding_model: None,
            collection: None,
        })
        .await?;
    let target = format!("{space}-target");
    let configured = Box::pin(spy::edit(&configs, space, |processing| {
        let mut profile = processing
            .profiles
            .get(&format!("{space}-embedding"))
            .context("original embedding profile")?
            .clone();
        profile.id.clone_from(&target);
        profile.model_version = "target-v2".into();
        processing.profiles.insert(target.clone(), profile);
        processing.bindings.insert(
            ModelOperation::ReindexEmbedding,
            OperationBinding {
                id: format!("{space}-reindex"),
                profile_id: target.clone(),
                revoked: false,
            },
        );
        Ok(())
    }))
    .await?;
    let observed = configs
        .start_embedding_reindex(&configured, &target)
        .await?;
    let binding = configs
        .authorize_model_operation(space, ModelOperation::ReindexEmbedding, None)
        .await?;
    let policy = policy_for_binding(&binding)?;
    let partition = PartitionKey::new("default".into(), "L0".into(), policy.model.clone(), 8)?;
    let mut embedding = NodeEmbedding {
        base: BaseFields::new(
            RecordId::new_v4(TableName::DocNode),
            space.into(),
            "test".into(),
        ),
        node: Link::new(node.base.id.clone()),
        model: policy.model,
        dims: 8,
        vector: vec![1.0; 8],
    };
    let epochs = CanonicalRepository::new(client.clone());
    let before = epochs.epoch(space).await?;
    let foreign_before = epochs.epoch(spy::SPACES[1]).await?;
    let vectors = NodeEmbeddingRepository::new(client.clone(), PartitionManager::new());
    vectors
        .store_reindex_embeddings(&observed, vec![embedding.clone()])
        .await?;
    anyhow::ensure!(
        epochs.epoch(space).await? != before,
        "native effect lacked shared epoch write"
    );
    anyhow::ensure!(epochs.epoch(spy::SPACES[1]).await? == foreign_before);
    anyhow::ensure!(configs.get_by_space_id(space).await?.as_ref() == Some(&observed));
    anyhow::ensure!(
        DocNodeRepository::new(client.clone())
            .get(space, &node.base.id)
            .await?
            .as_ref()
            == Some(&node)
    );
    anyhow::ensure!(stored_vectors(&client, &partition, space).await? == vec![vec![1.0; 8]]);

    Box::pin(spy::edit(&configs, space, |processing| {
        processing
            .bindings
            .get_mut(&ModelOperation::ReindexEmbedding)
            .context("reindex binding")?
            .revoked = true;
        Ok(())
    }))
    .await?;
    embedding.vector = vec![2.0; 8];
    let revoked_epoch = epochs.epoch(space).await?;
    anyhow::ensure!(vectors
        .store_reindex_embeddings(&observed, vec![embedding.clone()])
        .await
        .is_err());
    anyhow::ensure!(epochs.epoch(space).await? == revoked_epoch);
    anyhow::ensure!(stored_vectors(&client, &partition, space).await? == vec![vec![1.0; 8]]);

    let restored = Box::pin(spy::edit(&configs, space, |processing| {
        processing
            .bindings
            .get_mut(&ModelOperation::ReindexEmbedding)
            .context("reindex binding")?
            .revoked = false;
        Ok(())
    }))
    .await?;
    // Losing the existing shared writer must refuse, not create another
    // authority while persisting the target vector.
    client
        .query_bind_checked(
            "DELETE type::thing('canonical_epoch', $space);",
            elephant_storage::bindings! { "space" => space },
        )
        .await?;
    anyhow::ensure!(vectors
        .store_reindex_embeddings(&restored, vec![embedding])
        .await
        .is_err());
    anyhow::ensure!(stored_vectors(&client, &partition, space).await? == vec![vec![1.0; 8]]);
    let mut missing = client
        .query_bind_checked(
            "SELECT VALUE epoch FROM ONLY type::thing('canonical_epoch', $space);",
            elephant_storage::bindings! { "space" => space },
        )
        .await?;
    let epoch: Option<String> = missing.take(0)?;
    anyhow::ensure!(
        epoch.is_none(),
        "refused vector write recreated the missing epoch"
    );
    Ok(())
}
