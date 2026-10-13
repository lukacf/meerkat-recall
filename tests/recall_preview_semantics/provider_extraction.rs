//! Actual AgenticExtractor run, production SDKs and independent support operation.
//! Model responses are scripted SPIES for conformance, not relevance acceptance.
use super::spy;
use anyhow::Context;
use async_trait::async_trait;
use elephant_pipeline::extraction::{
    AgenticExtractor, AgenticExtractorConfig, ExtractionBudget, ExtractionContext,
};
use elephant_pipeline::provider_dispatch::with_dispatch_credentials;
use elephant_storage::SpaceConfigRepository;
use elephant_types::provider_binding::ModelOperation;
use elephant_types::records::base::Security;
use elephant_types::{RecordId, TableName};
use meerkat::{AgentError, AgentLlmClient, AgentToolDispatcher, LlmStreamResult, Message, ToolDef};
use meerkat_core::lifecycle::run_primitive::ProviderParamsOverride;
use serde_json::json;
use spy::{BACKENDS, SPACES};
use std::sync::Arc;

struct NeverAmbientModel;
#[async_trait]
impl AgentLlmClient for NeverAmbientModel {
    async fn stream_response(
        &self,
        _messages: &[Message],
        _tools: &[Arc<ToolDef>],
        _max_tokens: u32,
        _temperature: Option<f32>,
        _provider_params: Option<&ProviderParamsOverride>,
    ) -> Result<LlmStreamResult, AgentError> {
        Err(AgentError::ConfigError(
            "ambient test model must never be invoked; production governed HTTP transport required"
                .into(),
        ))
    }
    fn provider(&self) -> meerkat::Provider {
        meerkat::Provider::Other
    }
    fn model(&self) -> &str {
        "never-ambient-test-model"
    }
}

struct NoExternalTools;
#[async_trait]
impl AgentToolDispatcher for NoExternalTools {
    fn tools(&self) -> Arc<[Arc<ToolDef>]> {
        Vec::<Arc<ToolDef>>::new().into()
    }
    async fn dispatch(
        &self,
        _call: meerkat_core::ToolCallView<'_>,
    ) -> Result<meerkat_core::ops::ToolDispatchOutcome, meerkat::ToolError> {
        Err(meerkat::ToolError::Other(
            "scripted extraction requested an unexpected external tool".into(),
        ))
    }
}

const TEXT: &str = "The acquisition of Moss Ltd was discussed but never approved. No acquisition agreement was signed and Moss Ltd remains independent.";

fn unsupported_output() -> serde_json::Value {
    // This exact source says the opposite. The separate scripted verifier has
    // one fixed false verdict for this counterexample, never blanket approval.
    json!({"assertions":[{
        "subject_text":"The company", "predicate_text":"acquired", "object_text":"Moss Ltd",
        "object_literal":null, "evidence":{"start":0,"end":TEXT.len(),"quote":TEXT},
        "confidence":0.99,"source_event_time":null,"source_event_time_precision":null,
        "event_title":"Moss acquisition completed","subject_entity_id":null,
        "subject_entity_type":"org","object_entity_id":null,"object_entity_type":"org","significance":"high"
    }]})
}

async fn run(
    extractor: &AgenticExtractor,
    context: ExtractionContext,
) -> anyhow::Result<
    elephant_pipeline::PipelineResult<elephant_pipeline::extraction::ExtractionResult>,
> {
    Ok(tokio::time::timeout(
        std::time::Duration::from_secs(20),
        with_dispatch_credentials(
            Arc::new(spy::SpyCredentials),
            Box::pin(extractor.run_agent(
                context,
                Arc::new(NoExternalTools),
                Arc::new(NeverAmbientModel),
            )),
        ),
    )
    .await?)
}

#[tokio::test]
async fn actual_extractor_and_independent_support_use_current_distinct_approvals(
) -> anyhow::Result<()> {
    Box::pin(extraction_dispatch_contract()).await
}

async fn extraction_dispatch_contract() -> anyhow::Result<()> {
    for backend in BACKENDS {
        eprintln!("extraction dispatch phase=storage backend={backend:?}");
        let storage = Box::pin(spy::storage(backend)).await?;
        let repo = SpaceConfigRepository::new(storage.clone());
        for (space, dims) in [(SPACES[0], 8), (SPACES[1], 12)] {
            eprintln!("extraction dispatch phase=configure backend={backend:?} space={space}");
            let endpoint = spy::Spy::start().await?;
            let configured =
                Box::pin(spy::configure(&repo, space, &endpoint.endpoint, dims)).await?;
            let model = configured
                .processing
                .as_ref()
                .context("processing missing")?
                .profiles
                .get(&format!("{space}-generation"))
                .context("generation profile missing")?
                .model
                .clone();
            let support_model = format!("{model}-independent-support");
            Box::pin(spy::edit(&repo, space, |processing| {
                let mut profile = processing
                    .profiles
                    .get(&format!("{space}-generation"))
                    .context("generation profile missing")?
                    .clone();
                profile.id = format!("{space}-support");
                profile.model = support_model.clone();
                processing
                    .bindings
                    .get_mut(&ModelOperation::SupportVerification)
                    .context("support binding missing")?
                    .profile_id = profile.id.clone();
                processing.profiles.insert(profile.id.clone(), profile);
                Ok(())
            }))
            .await?;
            endpoint.unsupported_extraction(unsupported_output())?;
            let budget = ExtractionBudget {
                max_tokens: 64_000,
                max_wall_time_secs: 15,
            };
            let extractor = AgenticExtractor::new(
                AgenticExtractorConfig::new("http://127.0.0.1:1/unused-mcp", &model)
                    .with_connect_on_init(false)
                    .with_budget(budget.clone()),
            );
            let mut context = ExtractionContext::new(
                RecordId::new_v4(TableName::DocNode),
                TEXT.into(),
                RecordId::new_v4(TableName::DocRevision),
                space.into(),
                Security::default(),
            );
            context.storage = Some(storage.clone());
            context.budget = budget;
            eprintln!("extraction dispatch phase=approved-run backend={backend:?} space={space}");
            let result = Box::pin(run(&extractor, context.clone())).await??;
            eprintln!(
                "extraction dispatch phase=inspect-support backend={backend:?} space={space}"
            );
            anyhow::ensure!(
                result.raw_assertions.len() == 1,
                "scripted candidate unexpectedly lost before support inspection"
            );
            anyhow::ensure!(
                result.raw_assertions[0].support_verification.is_none(),
                "exact quote falsely certified an unsupported acquisition"
            );
            let requests = endpoint.requests()?;
            anyhow::ensure!(
                requests.iter().any(|r| r.body["model"] == model),
                "{backend:?}: extraction did not use production governed HTTP"
            );
            let support_count = requests
                .iter()
                .filter(|r| r.body["model"] == support_model)
                .count();
            anyhow::ensure!(
                support_count > 0,
                "independent support operation never reached its approved model"
            );
            // A pinned extractor retry must still re-resolve support approval.
            Box::pin(spy::edit(&repo, space, |processing| {
                processing
                    .bindings
                    .get_mut(&ModelOperation::SupportVerification)
                    .context("support binding missing")?
                    .revoked = true;
                Ok(())
            }))
            .await?;
            eprintln!(
                "extraction dispatch phase=revoked-support backend={backend:?} space={space}"
            );
            anyhow::ensure!(Box::pin(run(&extractor, context.clone())).await?.is_err());
            anyhow::ensure!(
                endpoint
                    .requests()?
                    .iter()
                    .filter(|r| r.body["model"] == support_model)
                    .count()
                    == support_count,
                "revoked support verification escaped through extraction approval"
            );
            Box::pin(spy::edit(&repo, space, |processing| {
                processing.bindings.remove(&ModelOperation::Extraction);
                Ok(())
            }))
            .await?;
            let before = endpoint.count()?;
            eprintln!(
                "extraction dispatch phase=missing-extraction backend={backend:?} space={space}"
            );
            anyhow::ensure!(Box::pin(run(&extractor, context)).await?.is_err());
            anyhow::ensure!(
                endpoint.count()? == before,
                "missing extraction binding emitted outbound request"
            );
        }
    }
    Ok(())
}
