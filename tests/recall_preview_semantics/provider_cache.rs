//! Warm the retained entity-vector API through real governed embedding transport.
//! The provider successor removes reusable TTL payloads; this checks the behavior
//! through the same compatibility cache handle before and after storage changes.
use super::spy;
use anyhow::Context;
use elephant_embeddings::governed::policy_for_binding;
use elephant_mcp::handlers::{SearchEntitiesHandler, ToolContext, ToolHandler};
use elephant_pipeline::provider_dispatch::{with_dispatch_credentials, ProviderDispatchContext};
use elephant_policy::{Clearance, Principal};
use elephant_storage::entity_profile_embedding_repository::{entity_input_hash, entity_input_text};
use elephant_storage::entity_repository::EntityVectorCache;
use elephant_storage::{EntityRepository, Repository, SpaceConfigRepository, StorageClient};
use elephant_types::provider_binding::ModelOperation;
use elephant_types::records::base::BaseFields;
use elephant_types::records::entity::EntityType;
use elephant_types::records::Entity;
use elephant_types::{RecordId, TableName};
use spy::{BACKENDS, SPACES};
use std::collections::HashSet;
use std::sync::Arc;

fn reader(client: &StorageClient, space: &str) -> ToolContext {
    ToolContext::new(
        client.clone(),
        Principal {
            space_id: space.into(),
            scopes: HashSet::from(["read:entity".into()]),
            clearances: vec![Clearance {
                policy_id: "default".into(),
                max_level: 0,
                labels: HashSet::new(),
                handling_allow: HashSet::new(),
            }],
            subject_allowlist: None,
            purpose: HashSet::new(),
        },
    )
}

#[tokio::test]
async fn actual_entity_search_handler_preserves_space_and_rechecks_warm_approval(
) -> anyhow::Result<()> {
    for backend in BACKENDS {
        let client = spy::storage(backend).await?;
        let config = SpaceConfigRepository::new(client.clone());
        let home = spy::Spy::start().await?;
        let business = spy::Spy::start().await?;
        let handler = SearchEntitiesHandler::new(client.clone());
        // Both inventories exist before either query, at identical dimensions.
        // Names deliberately do not match the original query lexically.
        let mut entities = Vec::new();
        for (space, endpoint) in [(SPACES[0], &home), (SPACES[1], &business)] {
            spy::configure(&config, space, &endpoint.endpoint, 8).await?;
            entities.push(
                embed_entity(&client, &spy::context(&config, space), "Synthetic entity").await?,
            );
        }
        let query = "unmatched original public query";
        for (index, (space, endpoint)) in [(SPACES[0], &home), (SPACES[1], &business)]
            .into_iter()
            .enumerate()
        {
            let context = reader(&client, space);
            let params = serde_json::json!({"query":query,"use_vector":true,"limit":3});
            for _ in 0..2 {
                let before = endpoint.count()?;
                let response = with_dispatch_credentials(
                    Arc::new(spy::SpyCredentials),
                    handler.execute(&context, params.clone()),
                )
                .await?;
                anyhow::ensure!(response["ok"] == true);
                let items = response["data"]["items"]
                    .as_array()
                    .context("public entity search omitted items")?;
                anyhow::ensure!(items.len() == 1);
                let entity: Entity = serde_json::from_value(items[0].clone())?;
                anyhow::ensure!(entity.base.id == entities[index].base.id);
                anyhow::ensure!(entity.base.space_id == space);
                let requests = endpoint.requests()?;
                anyhow::ensure!(requests.len() == before + 1);
                let request = requests.last().context("missing public query transport")?;
                anyhow::ensure!(request.body["input"][0] == query);
            }
            let before_home = home.count()?;
            let before_business = business.count()?;
            let denied = with_dispatch_credentials(
                Arc::new(spy::SpyCredentials),
                handler.execute(
                    &context,
                    serde_json::json!({"query":query,"filters":{"space_id":SPACES[1-index]}}),
                ),
            )
            .await;
            anyhow::ensure!(denied.as_ref().map_or(true, |value| value["ok"] == false));
            anyhow::ensure!(home.count()? == before_home && business.count()? == before_business);
            spy::edit(&config, space, |processing| {
                processing
                    .bindings
                    .get_mut(&ModelOperation::QueryEmbedding)
                    .context("missing query binding")?
                    .revoked = true;
                Ok(())
            })
            .await?;
            let denied = with_dispatch_credentials(
                Arc::new(spy::SpyCredentials),
                handler.execute(&context, params),
            )
            .await;
            anyhow::ensure!(
                denied.as_ref().map_or(true, |value| value["ok"] == false),
                "revoked public semantic query silently succeeded"
            );
            anyhow::ensure!(home.count()? == before_home && business.count()? == before_business);
        }
    }
    Ok(())
}

async fn embed_entity(
    client: &StorageClient,
    context: &ProviderDispatchContext,
    name: &str,
) -> anyhow::Result<Entity> {
    let repo = EntityRepository::new(client.clone());
    let entity = repo
        .create(Entity::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Entity),
                context.space_id.clone(),
                "D-spy-input".into(),
            ),
            EntityType::new("topic"),
            name.into(),
        ))
        .await?;
    let (policy, provider, _) = context.embedding(ModelOperation::DocumentEmbedding).await?;
    let mut vectors = provider.embed(&[entity_input_text(&entity)]).await?;
    anyhow::ensure!(vectors.len() == 1);
    let vector = vectors.pop().context("missing spy vector")?;
    repo.set_name_embedding(
        &context.space_id,
        &entity.base.id,
        &vector,
        &policy.model,
        &entity_input_hash(&entity),
    )
    .await?;
    Ok(entity)
}

#[tokio::test]
async fn warm_entity_handle_does_not_cross_same_dimension_spaces_or_profile_identity(
) -> anyhow::Result<()> {
    for backend in BACKENDS {
        let client = spy::storage(backend).await?;
        let config = SpaceConfigRepository::new(client.clone());
        let repo = EntityRepository::new(client.clone());
        let cache = EntityVectorCache::new();
        for space in SPACES {
            let endpoint = spy::Spy::start().await?;
            spy::configure(&config, space, &endpoint.endpoint, 8).await?;
            let context = spy::context(&config, space);
            let entity =
                embed_entity(&client, &context, &format!("Synthetic input for {space}")).await?;
            let (policy, provider, _) = context.embedding(ModelOperation::QueryEmbedding).await?;
            let vectors = provider
                .embed(&["original cache qualification query".into()])
                .await?;
            for _ in 0..2 {
                let hits = repo
                    .vector_search_entities_for_policy(&cache, space, &policy, &vectors[0], 3)
                    .await?;
                anyhow::ensure!(
                    hits.len() == 1 && hits[0].entity_id == entity.base.id,
                    "{backend:?}: same dimension cache leaked another space"
                );
            }
            let binding = config
                .authorize_model_operation(space, ModelOperation::QueryEmbedding, None)
                .await?;
            for field in ["profile", "account", "endpoint", "version", "preprocessing"] {
                let mut unapproved = binding.clone();
                match field {
                    "profile" => unapproved.profile.id = "not-current-profile".into(),
                    "account" => unapproved.connection.account_id = "not-current-account".into(),
                    "endpoint" => unapproved.connection.endpoint = "http://127.0.0.1:1/v1".into(),
                    "version" => unapproved.profile.model_version = "not-current-version".into(),
                    _ => unapproved.profile.preprocessing = "not-current-preprocessing".into(),
                }
                let foreign_policy = policy_for_binding(&unapproved)?;
                anyhow::ensure!(
                    repo.vector_search_entities_for_policy(
                        &cache,
                        space,
                        &foreign_policy,
                        &vectors[0],
                        3
                    )
                    .await
                    .is_err(),
                    "{backend:?}: warm vector handle ignored {field} identity"
                );
            }
            let before = endpoint.count()?;
            spy::edit(&config, space, |processing| {
                processing
                    .bindings
                    .get_mut(&ModelOperation::QueryEmbedding)
                    .context("missing query binding")?
                    .revoked = true;
                Ok(())
            })
            .await?;
            anyhow::ensure!(provider
                .embed(&["retry on warmed cache".into()])
                .await
                .is_err());
            anyhow::ensure!(
                endpoint.count()? == before,
                "revoked cached query provider emitted outbound call"
            );
            anyhow::ensure!(
                repo.vector_search_entities_for_policy(&cache, space, &policy, &vectors[0], 3)
                    .await
                    .is_err(),
                "warm entity handle bypassed current repository approval"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn physical_entity_deletion_is_visible_immediately_on_warmed_handle() -> anyhow::Result<()> {
    for backend in BACKENDS {
        let client = spy::storage(backend).await?;
        let config = SpaceConfigRepository::new(client.clone());
        let repo = EntityRepository::new(client.clone());
        let cache = EntityVectorCache::new();
        for space in SPACES {
            let endpoint = spy::Spy::start().await?;
            spy::configure(&config, space, &endpoint.endpoint, 8).await?;
            let context = spy::context(&config, space);
            let entity = embed_entity(&client, &context, "Synthetic exclusive entity").await?;
            let (policy, provider, _) = context.embedding(ModelOperation::QueryEmbedding).await?;
            let vectors = provider
                .embed(&["original physical deletion query".into()])
                .await?;
            anyhow::ensure!(
                repo.vector_search_entities_for_policy(&cache, space, &policy, &vectors[0], 3)
                    .await?
                    .len()
                    == 1
            );
            repo.hard_delete(space, &entity.base.id).await?;
            anyhow::ensure!(
                repo.vector_search_entities_for_policy(&cache, space, &policy, &vectors[0], 3)
                    .await?
                    .is_empty(),
                "{backend:?}: physically deleted entity survived in warm handle"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn supported_embedded_snapshot_restore_cannot_reuse_pre_restore_vector_payload(
) -> anyhow::Result<()> {
    // Raw embedded restore is retained. SurrealDB named persistent restore is D's
    // durability lane; this test does not substitute an in-memory database for it.
    let client = spy::storage(spy::Backend::ElephantDb).await?;
    let config = SpaceConfigRepository::new(client.clone());
    let endpoint = spy::Spy::start().await?;
    spy::configure(&config, SPACES[0], &endpoint.endpoint, 8).await?;
    let path = std::env::temp_dir().join(format!(
        "recall-semantic-cache-{}.snapshot",
        uuid::Uuid::new_v4()
    ));
    client.export_snapshot(path.clone()).await?;
    let context = spy::context(&config, SPACES[0]);
    let _entity = embed_entity(&client, &context, "Entity created after saved snapshot").await?;
    let repo = EntityRepository::new(client.clone());
    let cache = EntityVectorCache::new();
    let (policy, provider, _) = context.embedding(ModelOperation::QueryEmbedding).await?;
    let vectors = provider.embed(&["original snapshot query".into()]).await?;
    anyhow::ensure!(
        repo.vector_search_entities_for_policy(&cache, SPACES[0], &policy, &vectors[0], 3)
            .await?
            .len()
            == 1
    );
    client.import_snapshot(path.clone()).await?;
    anyhow::ensure!(
        repo.vector_search_entities_for_policy(&cache, SPACES[0], &policy, &vectors[0], 3)
            .await?
            .is_empty(),
        "restored entity inventory was bypassed by warmed vector handle"
    );
    tokio::fs::remove_file(path).await?;
    Ok(())
}
