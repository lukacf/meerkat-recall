//! Actual retained adapters, with production clients reaching deterministic spies.
use super::spy;
use anyhow::Context;
use elephant_pipeline::provider_dispatch::with_dispatch_credentials;
use elephant_pipeline::resolution::{
    AgenticDisambiguator, CandidateScore, CandidateSummary, DisambiguationBudget,
    DisambiguationContext, ResolutionDisambiguator,
};
use elephant_pipeline::scheduler::promotion_disambiguation::{
    AgenticPromotionDisambiguator, CandidateEntity, DisambiguationRequest,
    PromotionDisambiguationConfig, PromotionDisambiguator,
};
use elephant_pipeline::summarizer::{
    AgenticSummarizer, SummarizationBudget, SummarizationContext, SummarizationOptions, Summarizer,
};
use elephant_pipeline::truth_maintenance::adjudicator::{
    AdjudicationClaim, AdjudicationConfig, AdjudicationInput, AgenticSlotAdjudicator, ClaimValue,
    SlotAdjudicator,
};
use elephant_pipeline::wiki_editorial::{
    MeerkatWikiEditorialLlm, WikiEditorialConfig, WikiEditorialLlm,
};
use elephant_storage::SpaceConfigRepository;
use elephant_types::provider_binding::ModelOperation;
use elephant_types::records::assertion::{AssertionRetryState, EvidenceSpan, ResolutionStatus};
use elephant_types::records::{Assertion, BaseFields};
use elephant_types::{Link, RecordId, TableName};
use spy::{BACKENDS, SPACES};
use std::sync::Arc;

fn identity_input(space: &str) -> DisambiguationContext {
    // An unresolved input candidate, never a persisted canonical winner.
    let base = BaseFields::new(
        RecordId::new_v4(TableName::Assertion),
        space.into(),
        "D-unresolved-identity-spy".into(),
    );
    let evidence = EvidenceSpan {
        revision: Link::new(RecordId::new_v4(TableName::DocRevision)),
        node: None,
        start: 0,
        end: 9,
        quote_hash: "unpersisted-transport-input".into(),
    };
    let assertion = Assertion {
        observed_at: base.created_at,
        asserted_at: base.created_at,
        base,
        subject_text: "Alex Chen".into(),
        predicate_raw: "works with".into(),
        predicate_id: None,
        object_text: Some("Finance".into()),
        object_literal: None,
        evidence: evidence.clone(),
        source_event_time: None,
        assertion_key: "unpersisted-spy-input".into(),
        resolution_status: ResolutionStatus::Pending,
        resolution_method: None,
        resolution_confidence: None,
        retry_state: AssertionRetryState::default(),
        subject_entity: None,
        object_entity: None,
        confidence: 0.5,
        subject_entity_type: Some("person".into()),
        object_entity_type: Some("org".into()),
        significance: None,
        lifecycle_tier: "active".into(),
    };
    let summaries: Vec<_> = ["Finance employee F-204", "Researcher R-991"]
        .into_iter()
        .map(|description| CandidateSummary {
            entity_id: RecordId::new_v4(TableName::Entity),
            display_name: "Alex Chen".into(),
            summary: Some(description.into()),
            aliases: Vec::new(),
        })
        .collect();
    DisambiguationContext {
        assertion,
        candidates: summaries
            .iter()
            .map(|candidate| CandidateScore {
                entity_id: candidate.entity_id.clone(),
                score: 0.5,
                features: vec![("equal synthetic ambiguity".into(), 0.5)],
            })
            .collect(),
        evidence,
        candidate_summaries: summaries,
        budget: DisambiguationBudget {
            max_tokens: 256,
            max_wall_time_secs: 5,
        },
    }
}

#[tokio::test]
async fn actual_identity_resolution_uses_assertion_space_and_current_approval() -> anyhow::Result<()>
{
    for backend in BACKENDS {
        let client = spy::storage(backend).await?;
        let repo = SpaceConfigRepository::new(client.clone());
        let adapter =
            AgenticDisambiguator::new("unapproved-identity-model", client).with_max_iterations(1);
        for (space, dims) in [(SPACES[0], 8), (SPACES[1], 12)] {
            let endpoint = spy::Spy::start().await?;
            spy::configure(&repo, space, &endpoint.endpoint, dims).await?;
            let result = with_dispatch_credentials(
                Arc::new(spy::SpyCredentials),
                adapter.disambiguate(identity_input("semantics-unregistered")),
            )
            .await;
            anyhow::ensure!(result.is_err() && endpoint.count()? == 0,
                "{backend:?}/{space}: wrong-space resolution must fail without dispatch; result={result:?}");
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                with_dispatch_credentials(
                    Arc::new(spy::SpyCredentials),
                    adapter.disambiguate(identity_input(space)),
                ),
            )
            .await?;
            anyhow::ensure!(result.is_err() && endpoint.count()? == 1,
                "{backend:?}/{space}: approved provider refusal must fail after exactly one dispatch; result={result:?}, calls={}", endpoint.count()?);
            let request = endpoint
                .requests()?
                .pop()
                .context("missing identity request")?;
            anyhow::ensure!(request.body.to_string().contains("Alex Chen"));
            anyhow::ensure!(request.body["model"] != "unapproved-identity-model");
            revoke(&repo, space, ModelOperation::IdentityDisambiguation).await?;
            let result = with_dispatch_credentials(
                Arc::new(spy::SpyCredentials),
                adapter.disambiguate(identity_input(space)),
            )
            .await;
            anyhow::ensure!(result.is_err() && endpoint.count()? == 1,
                "{backend:?}/{space}: revoked resolution must fail without another dispatch; result={result:?}, calls={}", endpoint.count()?);
        }
    }
    Ok(())
}

#[test]
fn actual_identity_sweep_requires_approval_and_keeps_a_revoked_retry_pending() -> anyhow::Result<()>
{
    super::run_bounded_adapter_scenario("identity-sweep-adapter", || async {
        Box::pin(identity_sweep_dispatch_contract()).await
    })
}

async fn identity_sweep_dispatch_contract() -> anyhow::Result<()> {
    use elephant_pipeline::maintenance::{identity_sweep::run_identity_sweep, MaintenanceConfig};
    use elephant_storage::{EntityRepository, IdentityCandidateRepository, Repository};
    use elephant_types::records::entity::EntityType;
    use elephant_types::records::{Entity, IdentityCandidateStatus, SubjectsState};
    for backend in BACKENDS {
        eprintln!("identity sweep dispatch phase=storage backend={backend:?}");
        let client = Box::pin(spy::storage(backend)).await?;
        let configurations = SpaceConfigRepository::new(client.clone());
        let entities = EntityRepository::new(client.clone());
        let candidates = IdentityCandidateRepository::new(client.clone());
        for (space, dims) in [(SPACES[0], 8), (SPACES[1], 12)] {
            eprintln!("identity sweep dispatch phase=configure backend={backend:?} space={space}");
            let endpoint = spy::Spy::start().await?;
            let approved = Box::pin(spy::configure(
                &configurations,
                space,
                &endpoint.endpoint,
                dims,
            ))
            .await?;
            let approved_binding = approved
                .processing
                .context("processing configuration absent")?
                .bindings
                .get(&ModelOperation::IdentityDisambiguation)
                .cloned()
                .context("identity binding absent")?;
            let canonical = format!("Alex {space}");
            endpoint.pending_identity(&canonical)?;
            let mut input_ids = Vec::new();
            for (name, summary) in [
                (
                    canonical.clone(),
                    "Employee context with independent identifier.",
                ),
                (
                    format!("{canonical} account"),
                    "Account context requiring identity review.",
                ),
            ] {
                let mut base = BaseFields::new(
                    RecordId::new_v4(TableName::Entity),
                    space.into(),
                    "D-identity-sweep-spy".into(),
                );
                base.security.policy_id = format!("policy-{space}");
                base.subjects_state = SubjectsState::Complete;
                let mut entity = Entity::new(base, EntityType::new("person"), name.clone());
                entity.summary = Some(summary.into());
                entity.identity_keys = vec![name.to_lowercase()];
                input_ids.push(entities.create(entity).await?.base.id);
            }
            let config = MaintenanceConfig {
                model: "unapproved-sweep-model".into(),
                auto_merge_threshold: 0.95,
                max_candidates_per_sweep: 4,
                ..MaintenanceConfig::default()
            };
            spy::edit(&configurations, space, |processing| {
                processing
                    .bindings
                    .remove(&ModelOperation::IdentityDisambiguation)
                    .context("identity binding absent")?;
                Ok(())
            })
            .await?;
            eprintln!(
                "identity sweep dispatch phase=missing-approval backend={backend:?} space={space}"
            );
            let blocked = with_dispatch_credentials(
                Arc::new(spy::SpyCredentials),
                Box::pin(run_identity_sweep(&client, space, &config)),
            )
            .await?;
            anyhow::ensure!(blocked.skipped == 1 && endpoint.count()? == 0);
            anyhow::ensure!(candidates.list(space, 10, None).await?.is_empty());
            spy::edit(&configurations, space, |processing| {
                processing
                    .bindings
                    .insert(ModelOperation::IdentityDisambiguation, approved_binding);
                Ok(())
            })
            .await?;
            eprintln!(
                "identity sweep dispatch phase=approved-run backend={backend:?} space={space}"
            );
            let proposed = tokio::time::timeout(
                std::time::Duration::from_secs(15),
                with_dispatch_credentials(
                    Arc::new(spy::SpyCredentials),
                    Box::pin(run_identity_sweep(&client, space, &config)),
                ),
            )
            .await??;
            eprintln!(
                "identity sweep dispatch phase=approved-return backend={backend:?} space={space}"
            );
            anyhow::ensure!(
                proposed.proposed == 1 && proposed.auto_merged == 0 && endpoint.count()? == 1
            );
            let request = endpoint
                .requests()?
                .pop()
                .context("identity sweep did not dispatch")?;
            anyhow::ensure!(request.body.to_string().contains(&canonical));
            anyhow::ensure!(request.body["model"] != "unapproved-sweep-model");
            anyhow::ensure!(
                request.credential == format!("Bearer spy-only-SEMANTICS_SPY_{dims}_GENERATE")
                    || request.credential == format!("spy-only-SEMANTICS_SPY_{dims}_GENERATE")
            );
            let rows = candidates.list(space, 10, None).await?;
            anyhow::ensure!(rows.len() == 1 && rows[0].status == IdentityCandidateStatus::Pending);
            let before = serde_json::to_value(&rows[0])?;
            revoke(
                &configurations,
                space,
                ModelOperation::IdentityDisambiguation,
            )
            .await?;
            eprintln!(
                "identity sweep dispatch phase=revoked-run backend={backend:?} space={space}"
            );
            let revoked = with_dispatch_credentials(
                Arc::new(spy::SpyCredentials),
                Box::pin(run_identity_sweep(&client, space, &config)),
            )
            .await?;
            anyhow::ensure!(
                revoked.skipped == 1 && revoked.auto_merged == 0 && endpoint.count()? == 1,
                "pending candidate retry ignored current revocation"
            );
            anyhow::ensure!(
                serde_json::to_value(
                    candidates
                        .get(space, &rows[0].base.id)
                        .await?
                        .context("pending candidate disappeared")?
                )? == before
            );
            for id in input_ids {
                let entity = entities
                    .get(space, &id)
                    .await?
                    .context("input identity disappeared")?;
                anyhow::ensure!(
                    entity.redirect_to.is_none()
                        && entity.status == elephant_types::records::entity::EntityStatus::Active
                );
            }
        }
    }
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
            .context("missing binding")?
            .revoked = true;
        Ok(())
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn active_long_summary_dispatch_and_revocation_are_governed() -> anyhow::Result<()> {
    for backend in BACKENDS {
        let repo = SpaceConfigRepository::new(spy::storage(backend).await?);
        for (space, dims) in [(SPACES[0], 8), (SPACES[1], 12)] {
            let endpoint = spy::Spy::start().await?;
            spy::configure(&repo, space, &endpoint.endpoint, dims).await?;
            let summarizer =
                AgenticSummarizer::new().with_provider_context(spy::context(&repo, space));
            let input = SummarizationContext::new(
                format!("Synthetic summary input for {space}. ").repeat(50),
                SummarizationBudget {
                    max_output_tokens: 16,
                    max_summary_length: 64,
                    tokens_per_char: 0.25,
                },
                SummarizationOptions {
                    min_text_length: 1,
                    model: "unapproved-summary-model".into(),
                },
            );
            // On the controlled provider 400, a truncated fallback is allowed by
            // the product. It does not constitute a successful model summary.
            let _ = summarizer.summarize(&input).await;
            anyhow::ensure!(
                endpoint.count()? == 1,
                "{backend:?}: active summarizer bypassed governed transport"
            );
            revoke(&repo, space, ModelOperation::Summary).await?;
            anyhow::ensure!(summarizer.summarize(&input).await.is_err());
            anyhow::ensure!(
                endpoint.count()? == 1,
                "summary retry ignored revoked approval"
            );
        }
    }
    Ok(())
}

fn wiki_config() -> WikiEditorialConfig {
    WikiEditorialConfig {
        writer_model: "unapproved-writer".into(),
        verifier_model: "unapproved-verifier".into(),
        max_output_tokens: 32,
        structured_output_retries: 0,
        timeout_secs: 5,
        max_context_chars: 4096,
        max_editorial_attempts: 1,
    }
}

async fn wiki_call(adapter: &MeerkatWikiEditorialLlm, role: &str) -> anyhow::Result<()> {
    let prompt =
        format!("Synthetic {role} transport input. No semantic verdict requested from spy.");
    let failed = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        match role {
            "planner" => adapter.plan_wiki(prompt).await.is_err(),
            "organizer" => adapter.organize_wiki(prompt).await.is_err(),
            "writer" => adapter.write_article(prompt).await.is_err(),
            _ => adapter.verify_article(prompt).await.is_err(),
        }
    })
    .await?;
    anyhow::ensure!(
        failed,
        "controlled HTTP 400 cannot produce valid wiki output"
    );
    Ok(())
}

#[test]
fn actual_wiki_planner_organizer_writer_and_verifier_check_current_approval() -> anyhow::Result<()>
{
    super::run_bounded_adapter_scenario("wiki-editorial-adapter", wiki_dispatch_contract)
}

async fn wiki_dispatch_contract() -> anyhow::Result<()> {
    for backend in BACKENDS {
        for (space, dims) in [(SPACES[0], 8), (SPACES[1], 12)] {
            for (role, operation) in [
                ("planner", ModelOperation::WikiPlanner),
                ("organizer", ModelOperation::WikiPlanner),
                ("writer", ModelOperation::WikiWriter),
                ("verifier", ModelOperation::WikiVerifier),
            ] {
                let repo = SpaceConfigRepository::new(spy::storage(backend).await?);
                let endpoint = spy::Spy::start().await?;
                spy::configure(&repo, space, &endpoint.endpoint, dims).await?;
                let adapter = MeerkatWikiEditorialLlm::new(wiki_config())
                    .with_provider_context(spy::context(&repo, space))
                    .await?;
                wiki_call(&adapter, role).await?;
                anyhow::ensure!(
                    endpoint.count()? == 1,
                    "{backend:?}/{role}: real editorial adapter did not dispatch once"
                );
                revoke(&repo, space, operation).await?;
                wiki_call(&adapter, role).await?;
                anyhow::ensure!(
                    endpoint.count()? == 1,
                    "{role}: cached editorial adapter ignored revocation"
                );
            }
        }
    }
    Ok(())
}

fn tm_input(space: &str) -> anyhow::Result<AdjudicationInput> {
    let timestamp =
        chrono::DateTime::parse_from_rfc3339("2027-03-20T00:00:00Z")?.with_timezone(&chrono::Utc);
    let claims =
        ["Leena", "Omar"]
            .into_iter()
            .map(|name| AdjudicationClaim {
                claim_id: format!("synthetic-candidate-{name}"),
                value: ClaimValue::EntityRef {
                    id: format!("synthetic-person-{name}"),
                    display: name.into(),
                },
                confidence: 0.8,
                evidence_count: 1,
                timestamp,
                time_basis: "fixture effective date".into(),
                sources: vec!["business/ownership".into()],
                evidence_notes: vec![
                    "Leena coordinates through March; Omar takes over April 1.".into()
                ],
            })
            .collect();
    Ok(AdjudicationInput {
        space_id: space.into(),
        slot_key: "synthetic-coordinator".into(),
        slot_type: "rel_set".into(),
        subject_display: "Incident response".into(),
        predicate_id: "coordinated_by".into(),
        semantics: None,
        claims,
    })
}

#[tokio::test]
async fn actual_tm_adapter_refuses_wrong_space_and_revoked_retry() -> anyhow::Result<()> {
    for backend in BACKENDS {
        let repo = SpaceConfigRepository::new(spy::storage(backend).await?);
        for (space, dims, other) in [(SPACES[0], 8, SPACES[1]), (SPACES[1], 12, SPACES[0])] {
            let endpoint = spy::Spy::start().await?;
            spy::configure(&repo, space, &endpoint.endpoint, dims).await?;
            let adapter = AgenticSlotAdjudicator::new(AdjudicationConfig {
                model: "unapproved-tm-model".into(),
                max_tokens: 32,
                max_wall_time_secs: 5,
            })
            .with_provider_context(spy::context(&repo, space))
            .await?;
            // Typed cross-space input must be checked at the actual adapter,
            // not merely at a self-consistent ProviderDispatchContext wrapper.
            let _ = adapter.adjudicate(&tm_input(other)?).await;
            anyhow::ensure!(
                endpoint.count()? == 0,
                "{backend:?}: foreign TM input dispatched under another space approval"
            );
            let _ = adapter.adjudicate(&tm_input(space)?).await;
            anyhow::ensure!(endpoint.count()? == 1, "active TM adapter did not dispatch");
            revoke(&repo, space, ModelOperation::TruthMaintenance).await?;
            let _ = adapter.adjudicate(&tm_input(space)?).await;
            anyhow::ensure!(endpoint.count()? == 1, "active TM retry ignored revocation");
        }
    }
    Ok(())
}

#[tokio::test]
async fn actual_identity_promotion_adapter_dispatch_and_retry_are_governed() -> anyhow::Result<()> {
    for backend in BACKENDS {
        let repo = SpaceConfigRepository::new(spy::storage(backend).await?);
        for (space, dims) in [(SPACES[0], 8), (SPACES[1], 12)] {
            let endpoint = spy::Spy::start().await?;
            spy::configure(&repo, space, &endpoint.endpoint, dims).await?;
            let adapter = AgenticPromotionDisambiguator::new(PromotionDisambiguationConfig {
                model: "unapproved-identity-model".into(),
            })
            .with_provider_context(spy::context(&repo, space))
            .await?;
            let input = DisambiguationRequest {
                mention: "Alex Chen".into(),
                normalized_type: "person".into(),
                context_snippet: "Finance F-204 and researcher R-991 are different people.".into(),
                candidates: ["F-204", "R-991"]
                    .into_iter()
                    .map(|id| CandidateEntity {
                        id: id.into(),
                        display_name: "Alex Chen".into(),
                        entity_type: "person".into(),
                        summary: None,
                    })
                    .collect(),
            };
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                adapter.disambiguate(&input),
            )
            .await?;
            anyhow::ensure!(
                endpoint.count()? == 1,
                "{backend:?}: actual identity promotion adapter did not dispatch"
            );
            revoke(&repo, space, ModelOperation::PromotionDisambiguation).await?;
            let _ = adapter.disambiguate(&input).await;
            anyhow::ensure!(
                endpoint.count()? == 1,
                "promotion identity retry ignored revocation"
            );
        }
    }
    Ok(())
}
