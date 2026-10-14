//! Retained direct adapters in a dependency build, where cfg(test) substitutes
//! in elephant-pipeline are absent. No scripted claims or model-quality oracle.
use super::spy;
use anyhow::Context;
use elephant_pipeline::extraction::{
    DefaultExtractor, ExtractionBudget, ExtractionContext, Extractor,
};
use elephant_pipeline::provider_dispatch::with_dispatch_credentials;
use elephant_pipeline::scheduler::triage::{
    AgenticTriage, RuleBasedTriage, TriageContext, TriagePlugin,
};
use elephant_storage::{SpaceConfigRepository, StorageClient};
use elephant_types::provider_binding::{ModelOperation, OperationBinding, SpaceProcessingConfig};
use elephant_types::records::base::{BaseFields, Security};
use elephant_types::records::ingest::BudgetSnapshot;
use elephant_types::records::source::{Source, SourceType};
use elephant_types::{RecordId, TableName};
use std::sync::Arc;

fn counts(endpoints: &[spy::Spy; 2]) -> anyhow::Result<[usize; 2]> {
    Ok([endpoints[0].count()?, endpoints[1].count()?])
}

async fn remove_binding(
    repo: &SpaceConfigRepository,
    space: &str,
    operation: ModelOperation,
) -> anyhow::Result<OperationBinding> {
    let mut removed = None;
    spy::edit(repo, space, |config| {
        removed = config.bindings.remove(&operation);
        Ok(())
    })
    .await?;
    removed.context("original direct-adapter binding absent")
}

async fn restore_binding(
    repo: &SpaceConfigRepository,
    space: &str,
    operation: ModelOperation,
    binding: OperationBinding,
) -> anyhow::Result<()> {
    spy::edit(repo, space, |config| {
        config.bindings.insert(operation, binding);
        Ok(())
    })
    .await?;
    Ok(())
}

async fn revoke(
    repo: &SpaceConfigRepository,
    space: &str,
    operation: ModelOperation,
) -> anyhow::Result<()> {
    spy::edit(repo, space, |config| {
        config
            .bindings
            .get_mut(&operation)
            .context("binding to revoke absent")?
            .revoked = true;
        Ok(())
    })
    .await?;
    Ok(())
}

fn triage_input(space: &str) -> TriageContext {
    let mut source = Source::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Source),
            space.into(),
            "D-direct-triage-spy".into(),
        ),
        SourceType::Manual,
        "unpersisted-transport-fixture".into(),
    );
    source.metadata =
        serde_json::json!({"description": format!("original triage input for {space}")});
    TriageContext {
        source,
        budget_snapshot: BudgetSnapshot {
            remaining_euros_retrieval: 1.0,
            remaining_euros_graph: 1.0,
            remaining_items_retrieval: Some(10),
            remaining_items_graph: Some(10),
            snapshot_at: chrono::Utc::now(),
        },
        space_id: space.into(),
        policy_version: "direct-triage-spy-v1".into(),
    }
}

#[tokio::test]
async fn actual_agentic_triage_checks_input_space_and_current_approval() -> anyhow::Result<()> {
    for backend in spy::BACKENDS {
        let repo = SpaceConfigRepository::new(spy::storage(backend).await?);
        let endpoints = [spy::Spy::start().await?, spy::Spy::start().await?];
        for (index, space) in spy::SPACES.into_iter().enumerate() {
            spy::configure(&repo, space, &endpoints[index].endpoint, 8 + index * 4).await?;
        }
        for (index, space) in spy::SPACES.into_iter().enumerate() {
            let adapter = AgenticTriage::new().with_provider_context(spy::context(&repo, space));
            let before = counts(&endpoints)?;
            let other = spy::SPACES[1 - index];
            let wrong_context = adapter.triage(triage_input(other)).await;
            anyhow::ensure!(
                wrong_context.is_err(),
                "{backend:?}/{space}: wrong selected space admitted"
            );
            let mut wrong_record = triage_input(space);
            wrong_record.source.base.space_id = other.into();
            anyhow::ensure!(
                adapter.triage(wrong_record).await.is_err(),
                "{backend:?}/{space}: foreign source record admitted"
            );
            let binding = remove_binding(&repo, space, ModelOperation::Triage).await?;
            anyhow::ensure!(
                adapter.triage(triage_input(space)).await.is_err(),
                "{backend:?}/{space}: missing approval became a rule fallback"
            );
            anyhow::ensure!(
                counts(&endpoints)? == before,
                "unauthorized triage dispatched"
            );
            restore_binding(&repo, space, ModelOperation::Triage, binding).await?;

            let input = triage_input(space);
            let expected_fallback = RuleBasedTriage::new().triage(input.clone()).await?;
            let result =
                tokio::time::timeout(std::time::Duration::from_secs(10), adapter.triage(input))
                    .await??;
            let mut after = before;
            after[index] += 1;
            anyhow::ensure!(counts(&endpoints)? == after,
                "{backend:?}/{space}: approved triage must dispatch exactly once to its own transport");
            // The documented local fallback is allowed only after that actual
            // approved failed call. It is not a generated model decision.
            anyhow::ensure!(
                result.decision == expected_fallback.decision
                    && result.reason_codes == expected_fallback.reason_codes,
                "controlled provider refusal changed the retained rule fallback"
            );
            let request = endpoints[index]
                .requests()?
                .pop()
                .context("triage request absent")?;
            anyhow::ensure!(request
                .body
                .to_string()
                .contains(&format!("original triage input for {space}")));
            anyhow::ensure!(
                request.body["model"]
                    == if index == 0 {
                        "claude-spy-a"
                    } else {
                        "gpt-spy-b"
                    }
            );
            revoke(&repo, space, ModelOperation::Triage).await?;
            anyhow::ensure!(
                adapter.triage(triage_input(space)).await.is_err(),
                "{backend:?}/{space}: revoked triage became a rule fallback"
            );
            anyhow::ensure!(
                counts(&endpoints)? == after,
                "revoked triage dispatched again"
            );
        }
    }
    Ok(())
}

fn extraction_input(storage: &StorageClient, space: &str) -> ExtractionContext {
    ExtractionContext::new(
        RecordId::new_v4(TableName::DocNode),
        format!("Original direct extractor input for {space}."),
        RecordId::new_v4(TableName::DocRevision),
        space.into(),
        Security::default(),
    )
    .with_storage(Some(storage.clone()))
    .with_budget(ExtractionBudget {
        max_tokens: 256,
        max_wall_time_secs: 5,
    })
}

#[tokio::test]
async fn actual_default_extractor_checks_storage_space_and_current_approval() -> anyhow::Result<()>
{
    for backend in spy::BACKENDS {
        let storage = spy::storage(backend).await?;
        let repo = SpaceConfigRepository::new(storage.clone());
        let endpoints = [spy::Spy::start().await?, spy::Spy::start().await?];
        for (index, space) in spy::SPACES.into_iter().enumerate() {
            spy::configure(&repo, space, &endpoints[index].endpoint, 8 + index * 4).await?;
        }
        let adapter = DefaultExtractor::new();
        for (index, space) in spy::SPACES.into_iter().enumerate() {
            let before = counts(&endpoints)?;
            for input in [
                extraction_input(&storage, "semantics-unregistered"),
                extraction_input(&storage, space).with_storage(None),
            ] {
                let result = with_dispatch_credentials(
                    Arc::new(spy::SpyCredentials),
                    adapter.extract(input),
                )
                .await;
                anyhow::ensure!(
                    result.is_err(),
                    "{backend:?}/{space}: unauthorized extraction admitted"
                );
            }
            let binding = remove_binding(&repo, space, ModelOperation::Extraction).await?;
            let missing = with_dispatch_credentials(
                Arc::new(spy::SpyCredentials),
                adapter.extract(extraction_input(&storage, space)),
            )
            .await;
            anyhow::ensure!(
                missing.is_err() && counts(&endpoints)? == before,
                "{backend:?}/{space}: missing approval dispatched or produced claims"
            );
            restore_binding(&repo, space, ModelOperation::Extraction, binding).await?;

            let result = with_dispatch_credentials(
                Arc::new(spy::SpyCredentials),
                adapter.extract(extraction_input(&storage, space)),
            )
            .await;
            let mut after = before;
            after[index] += 1;
            anyhow::ensure!(
                result.is_err() && counts(&endpoints)? == after,
                "{backend:?}/{space}: real direct extractor must fail after one approved dispatch"
            );
            let request = endpoints[index]
                .requests()?
                .pop()
                .context("direct extraction request absent")?;
            anyhow::ensure!(request
                .body
                .to_string()
                .contains(&format!("Original direct extractor input for {space}.")));
            anyhow::ensure!(
                request.body["model"]
                    == if index == 0 {
                        "claude-spy-a"
                    } else {
                        "gpt-spy-b"
                    }
            );
            revoke(&repo, space, ModelOperation::Extraction).await?;
            let revoked = with_dispatch_credentials(
                Arc::new(spy::SpyCredentials),
                adapter.extract(extraction_input(&storage, space)),
            )
            .await;
            anyhow::ensure!(
                revoked.is_err() && counts(&endpoints)? == after,
                "{backend:?}/{space}: revoked direct extractor dispatched or returned claims"
            );
        }
    }
    Ok(())
}

fn require_generation_identity(
    original: &SpaceProcessingConfig,
    updated: &SpaceProcessingConfig,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        updated.profiles == original.profiles && updated.bindings == original.bindings,
        "credential rotation changed immutable model identity or operation approval"
    );
    anyhow::ensure!(
        updated.active_embedding_profile_id == original.active_embedding_profile_id,
        "generation rotation changed the active embedding identity"
    );
    let old_connection = original
        .connections
        .get("generate")
        .context("original connection missing")?;
    let new_connection = updated
        .connections
        .get("generate")
        .context("rotated connection missing")?;
    anyhow::ensure!(
        old_connection.provider == new_connection.provider
            && old_connection.account_id == new_connection.account_id
            && old_connection.endpoint == new_connection.endpoint,
        "credential rotation changed provider/account/endpoint"
    );
    Ok(())
}

#[tokio::test]
async fn actual_default_extractor_rotation_preserves_profile_and_uses_current_credential(
) -> anyhow::Result<()> {
    for backend in spy::BACKENDS {
        let storage = spy::storage(backend).await?;
        let repo = SpaceConfigRepository::new(storage.clone());
        let endpoints = [spy::Spy::start().await?, spy::Spy::start().await?];
        let mut configurations = Vec::new();
        for (index, space) in spy::SPACES.into_iter().enumerate() {
            configurations.push(
                spy::configure(&repo, space, &endpoints[index].endpoint, 8 + index * 4).await?,
            );
        }
        let adapter = DefaultExtractor::new();
        for (index, space) in spy::SPACES.into_iter().enumerate() {
            let before = counts(&endpoints)?;
            let original = configurations[index]
                .processing
                .as_ref()
                .context("processing missing")?;
            let initial = with_dispatch_credentials(
                Arc::new(spy::SpyCredentials),
                adapter.extract(extraction_input(&storage, space)),
            )
            .await;
            let mut expected = before;
            expected[index] += 1;
            anyhow::ensure!(
                initial.is_err() && counts(&endpoints)? == expected,
                "{backend:?}/{space}: initial approved generation did not reach its transport once"
            );
            let old_request = endpoints[index]
                .requests()?
                .pop()
                .context("original request missing")?;
            let reference = format!("SEMANTICS_SPY_{}_GENERATE_ROTATED", 8 + index * 4);
            let rotated = spy::edit(&repo, space, |processing| {
                let connection = processing
                    .connections
                    .get_mut("generate")
                    .context("generation connection missing")?;
                connection.credential_generation += 1;
                connection.credential_ref = Some(reference.clone());
                Ok(())
            })
            .await?;
            let updated = rotated.processing.context("rotated processing missing")?;
            require_generation_identity(original, &updated)?;
            let repeated = with_dispatch_credentials(
                Arc::new(spy::SpyCredentials),
                adapter.extract(extraction_input(&storage, space)),
            )
            .await;
            expected[index] += 1;
            anyhow::ensure!(
                repeated.is_err() && counts(&endpoints)? == expected,
                "{backend:?}/{space}: rotated adapter did not dispatch exactly once"
            );
            let new_request = endpoints[index]
                .requests()?
                .pop()
                .context("rotated request missing")?;
            let expected_header = if index == 0 {
                format!("spy-only-{reference}")
            } else {
                format!("Bearer spy-only-{reference}")
            };
            anyhow::ensure!(
                new_request.credential == expected_header
                    && new_request.credential != old_request.credential,
                "next generation dispatch reused stale credentials"
            );
            anyhow::ensure!(
                new_request.body["model"] == old_request.body["model"]
                    && new_request
                        .body
                        .to_string()
                        .contains(&format!("Original direct extractor input for {space}.")),
                "credential rotation changed the original model/input"
            );
            revoke(&repo, space, ModelOperation::Extraction).await?;
            let revoked = with_dispatch_credentials(
                Arc::new(spy::SpyCredentials),
                adapter.extract(extraction_input(&storage, space)),
            )
            .await;
            anyhow::ensure!(
                revoked.is_err() && counts(&endpoints)? == expected,
                "current approval was bypassed after credential rotation"
            );
        }
    }
    Ok(())
}
