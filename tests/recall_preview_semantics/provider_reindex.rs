//! Actual claimed reindex pages and controlled HTTP vectors, never relevance evidence.
use super::spy;
use anyhow::Context;
use elephant_embeddings::governed::policy_for_binding;
use elephant_pipeline::embedding_reindex::{
    process_embedding_reindex_with_attempt, ReindexProgress,
};
use elephant_pipeline::provider_dispatch::with_dispatch_credentials;
use elephant_pipeline::PipelineError;
use elephant_storage::entity_profile_embedding_repository::EntityProfileEmbeddingRepository;
use elephant_storage::work_attempt_repository::fence_from_claim;
use elephant_storage::{
    EntityRepository, Repository, SpaceConfigRepository, StorageClient, WorkItemRepository,
};
use elephant_types::provider_binding::{
    EmbeddingReindex, EmbeddingReindexPhase, ModelOperation, OperationBinding,
};
use elephant_types::publication::AttemptFence;
use elephant_types::records::base::BaseFields;
use elephant_types::records::entity::EntityType;
use elephant_types::records::{Entity, SpaceConfig, WorkItem, WorkItemStatus};
use elephant_types::{RecordId, TableName};
use sha2::{Digest, Sha256};
use std::sync::Arc;

struct Fixture {
    space: String,
    target: String,
    entity: Entity,
    attempt: AttemptFence,
    reindex_id: String,
    dimensions: usize,
}

fn pending(config: &SpaceConfig) -> anyhow::Result<&EmbeddingReindex> {
    config
        .processing
        .as_ref()
        .and_then(|p| p.pending_reindex.as_ref())
        .context("actual staged reindex missing")
}

async fn current(repo: &SpaceConfigRepository, space: &str) -> anyhow::Result<SpaceConfig> {
    repo.get_by_space_id(space)
        .await?
        .context("configured reindex space missing")
}

async fn claim(storage: &StorageClient, space: &str, id: &str) -> anyhow::Result<AttemptFence> {
    let jobs = WorkItemRepository::new(storage.clone());
    let item = jobs
        .create(WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                space.into(),
                "D-reindex-spy".into(),
            ),
            kind: "reindex_space_embeddings".into(),
            payload: serde_json::json!({"reindex_id": id}),
            status: WorkItemStatus::Runnable,
            priority: 0,
            idempotency_key: format!("D-reindex-spy-{id}"),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        })
        .await?;
    let claimed = jobs
        .claim_with_lease(
            space,
            &item.base.id,
            "D-original-reindex-claim",
            chrono::Utc::now() + chrono::Duration::minutes(5),
        )
        .await?
        .context("original reindex claim was refused")?;
    // Only repository-returned claim authority. No synthetic fence or progress.
    Ok(fence_from_claim(&claimed)?)
}

async fn prepare(
    storage: &StorageClient,
    repo: &SpaceConfigRepository,
    space: &str,
    endpoint: &spy::Spy,
    dimensions: usize,
) -> anyhow::Result<Fixture> {
    spy::configure(repo, space, &endpoint.endpoint, dimensions - 4).await?;
    let entity = EntityRepository::new(storage.clone())
        .create(Entity::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Entity),
                space.into(),
                "D-reindex-spy".into(),
            ),
            EntityType::new("topic"),
            format!("Original intrinsic reindex input in {space}"),
        ))
        .await?;
    let target = format!("{space}-reindex-target");
    let configured = spy::edit(repo, space, |processing| {
        let mut connection = processing
            .connections
            .get("embed")
            .context("embedding connection missing")?
            .clone();
        connection.id = "reindex".into();
        connection.account_id = format!("{space}-reindex-account");
        connection.credential_ref = Some(format!("SEMANTICS_SPY_{dimensions}_REINDEX"));
        processing
            .connections
            .insert(connection.id.clone(), connection);
        let mut profile = processing
            .profiles
            .get(&format!("{space}-embedding"))
            .context("active profile missing")?
            .clone();
        profile.id.clone_from(&target);
        profile.connection_id = "reindex".into();
        profile.model = format!("text-embedding-spy-reindex-{dimensions}");
        profile.model_version = "spy-reindex-v1".into();
        profile.dimensions = Some(dimensions);
        processing.profiles.insert(target.clone(), profile);
        Ok(())
    })
    .await?;
    anyhow::ensure!(
        repo.start_embedding_reindex(&configured, &target)
            .await
            .is_err(),
        "staging accepted a target without ReindexEmbedding approval"
    );
    anyhow::ensure!(current(repo, space).await?.processing == configured.processing);
    let approved = spy::edit(repo, space, |processing| {
        processing.bindings.insert(
            ModelOperation::ReindexEmbedding,
            OperationBinding {
                id: format!("{space}-reindex-approval"),
                profile_id: target.clone(),
                revoked: false,
            },
        );
        Ok(())
    })
    .await?;
    let staged = repo.start_embedding_reindex(&approved, &target).await?;
    let reindex_id = pending(&staged)?.id.clone();
    let attempt = claim(storage, space, &reindex_id).await?;
    Ok(Fixture {
        space: space.into(),
        target,
        entity,
        attempt,
        reindex_id,
        dimensions,
    })
}

async fn page(
    storage: &StorageClient,
    space: &str,
    id: &str,
    attempt: &AttemptFence,
) -> anyhow::Result<Result<ReindexProgress, PipelineError>> {
    Ok(tokio::time::timeout(
        std::time::Duration::from_secs(10),
        with_dispatch_credentials(
            Arc::new(spy::SpyCredentials),
            Box::pin(process_embedding_reindex_with_attempt(
                storage, space, 8, id, attempt,
            )),
        ),
    )
    .await?)
}

fn counts(endpoints: &[spy::Spy; 2]) -> anyhow::Result<[usize; 2]> {
    Ok([endpoints[0].count()?, endpoints[1].count()?])
}

async fn assert_unchanged(
    repo: &SpaceConfigRepository,
    original: &SpaceConfig,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        current(repo, &original.base.space_id).await?.processing == original.processing,
        "refused original page changed reindex progress or processing authority"
    );
    Ok(())
}

async fn refusal_matrix(
    storage: &StorageClient,
    repo: &SpaceConfigRepository,
    endpoints: &[spy::Spy; 2],
    fixture: &Fixture,
    foreign: &Fixture,
) -> anyhow::Result<()> {
    let before = counts(endpoints)?;
    let original = current(repo, &fixture.space).await?;
    for (space, id, attempt) in [
        (
            fixture.space.as_str(),
            fixture.reindex_id.as_str(),
            &foreign.attempt,
        ),
        (
            fixture.space.as_str(),
            foreign.reindex_id.as_str(),
            &fixture.attempt,
        ),
        (
            "semantics-unregistered",
            fixture.reindex_id.as_str(),
            &fixture.attempt,
        ),
    ] {
        anyhow::ensure!(
            page(storage, space, id, attempt).await?.is_err(),
            "reindex accepted foreign claim, original target or selected space"
        );
        anyhow::ensure!(
            counts(endpoints)? == before,
            "unauthorized reindex dispatched"
        );
        assert_unchanged(repo, &original).await?;
    }
    let mut binding = None;
    let missing = spy::edit(repo, &fixture.space, |processing| {
        binding = processing
            .bindings
            .remove(&ModelOperation::ReindexEmbedding);
        Ok(())
    })
    .await?;
    anyhow::ensure!(page(
        storage,
        &fixture.space,
        &fixture.reindex_id,
        &fixture.attempt
    )
    .await?
    .is_err());
    anyhow::ensure!(
        counts(endpoints)? == before,
        "missing current reindex approval dispatched"
    );
    assert_unchanged(repo, &missing).await?;
    let binding = binding.context("reindex binding absent before removal")?;
    spy::edit(repo, &fixture.space, |processing| {
        processing
            .bindings
            .insert(ModelOperation::ReindexEmbedding, binding);
        Ok(())
    })
    .await?;
    Ok(())
}

async fn approved_page(
    storage: &StorageClient,
    repo: &SpaceConfigRepository,
    endpoints: &[spy::Spy; 2],
    fixture: &Fixture,
    index: usize,
) -> anyhow::Result<()> {
    let before = counts(endpoints)?;
    let nodes = page(
        storage,
        &fixture.space,
        &fixture.reindex_id,
        &fixture.attempt,
    )
    .await??;
    anyhow::ensure!(
        nodes.embedded_nodes == 0 && nodes.embedded_entities == 0 && !nodes.ready_to_activate
    );
    anyhow::ensure!(
        counts(endpoints)? == before,
        "empty node scan must not call a provider"
    );
    let staged = current(repo, &fixture.space).await?;
    anyhow::ensure!(pending(&staged)?.phase == EmbeddingReindexPhase::Entities);
    let progress = page(
        storage,
        &fixture.space,
        &fixture.reindex_id,
        &fixture.attempt,
    )
    .await??;
    let mut expected = before;
    expected[index] += 1;
    anyhow::ensure!(
        counts(endpoints)? == expected,
        "actual entity reindex must dispatch exactly once in its own space"
    );
    anyhow::ensure!(
        progress.reindex_id == fixture.reindex_id
            && progress.embedded_nodes == 0
            && progress.embedded_entities == 1
            && !progress.ready_to_activate
    );
    let request = endpoints[index]
        .requests()?
        .pop()
        .context("actual reindex request missing")?;
    anyhow::ensure!(
        request.path == "/v1/embeddings"
            && request.body["model"]
                == format!("text-embedding-spy-reindex-{}", fixture.dimensions)
            && request.body["dimensions"] == fixture.dimensions
            && request.body["input"] == serde_json::json!([&fixture.entity.display_name])
            && request.credential
                == format!(
                    "Bearer spy-only-SEMANTICS_SPY_{}_REINDEX",
                    fixture.dimensions
                ),
        "reindex lost its target profile, credential, dimension or original input"
    );
    assert_staged_vector(storage, repo, fixture, index).await?;
    let revoked = spy::edit(repo, &fixture.space, |processing| {
        processing
            .bindings
            .get_mut(&ModelOperation::ReindexEmbedding)
            .context("reindex binding missing")?
            .revoked = true;
        Ok(())
    })
    .await?;
    anyhow::ensure!(
        page(
            storage,
            &fixture.space,
            &fixture.reindex_id,
            &fixture.attempt
        )
        .await?
        .is_err(),
        "revoked continuation was treated as an approved completed page"
    );
    anyhow::ensure!(
        counts(endpoints)? == expected,
        "revoked reindex continuation dispatched"
    );
    assert_unchanged(repo, &revoked).await?;
    Ok(())
}

async fn assert_staged_vector(
    storage: &StorageClient,
    repo: &SpaceConfigRepository,
    fixture: &Fixture,
    index: usize,
) -> anyhow::Result<()> {
    let binding = repo
        .authorize_model_operation(&fixture.space, ModelOperation::ReindexEmbedding, None)
        .await?;
    anyhow::ensure!(
        binding.profile.id == fixture.target
            && binding.connection.account_id == format!("{}-reindex-account", fixture.space)
    );
    let policy = policy_for_binding(&binding)?;
    let vectors = EntityProfileEmbeddingRepository::new(storage.clone());
    let hashes = vectors
        .text_hashes(&fixture.space, &policy.model, policy.dims)
        .await?;
    // This original entity has no aliases/summary. Derive the expected digest
    // from its literal name, independently of the production preprocessing helper.
    let expected_hash = format!(
        "{:x}",
        Sha256::digest(fixture.entity.display_name.as_bytes())
    );
    anyhow::ensure!(
        hashes.len() == 1
            && hashes.get(&fixture.entity.base.id.to_string()) == Some(&expected_hash),
        "successful page did not persist the exact target entity input"
    );
    anyhow::ensure!(vectors
        .text_hashes(spy::SPACES[1 - index], &policy.model, policy.dims)
        .await?
        .is_empty());
    // Staged vectors cannot silently replace the active query profile.
    anyhow::ensure!(vectors
        .load(&fixture.space, &policy.model, policy.dims)
        .await
        .is_err());
    let after = current(repo, &fixture.space).await?;
    let processing = after
        .processing
        .as_ref()
        .context("processing authority missing")?;
    anyhow::ensure!(
        processing.active_embedding_profile_id.as_deref()
            == Some(format!("{}-embedding", fixture.space).as_str())
            && pending(&after)?.cursor.as_deref()
                == Some(fixture.entity.base.id.to_string().as_str())
    );
    Ok(())
}

#[tokio::test]
async fn actual_claimed_reindex_uses_current_target_and_refuses_revoked_continuation(
) -> anyhow::Result<()> {
    Box::pin(reindex_contract()).await
}

async fn reindex_contract() -> anyhow::Result<()> {
    for backend in spy::BACKENDS {
        let storage = spy::storage(backend).await?;
        let repo = SpaceConfigRepository::new(storage.clone());
        let endpoints = [spy::Spy::start().await?, spy::Spy::start().await?];
        let mut fixtures = Vec::new();
        for (index, space) in spy::SPACES.into_iter().enumerate() {
            fixtures
                .push(prepare(&storage, &repo, space, &endpoints[index], 12 + index * 4).await?);
        }
        anyhow::ensure!(
            counts(&endpoints)? == [0, 0],
            "staging or claims dispatched"
        );
        for index in 0..2 {
            refusal_matrix(
                &storage,
                &repo,
                &endpoints,
                &fixtures[index],
                &fixtures[1 - index],
            )
            .await?;
            approved_page(&storage, &repo, &endpoints, &fixtures[index], index).await?;
        }
    }
    Ok(())
}
