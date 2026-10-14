//! Identity detection sweep.
//!
//! Finds potential duplicate entities using cheap heuristics (shared identity
//! key tokens, same source document), builds rich context dossiers, and uses
//! an LLM to decide whether to merge.
//!
//! The LLM gets pre-built context with both entities' relationships, source
//! text excerpts, and shared provenance — enabling single-call decisions
//! without expensive multi-turn tool use.

use std::collections::{BTreeMap, HashMap, HashSet};

use elephant_storage::{
    AssertionRepository, EntityRepository, IdentityCandidateRepository, RelRepository, Repository,
    SourceRepository, StorageClient,
};
use elephant_types::ids::TableName;
use elephant_types::records::base::{BaseFields, SubjectsState};
use elephant_types::records::entity::{Entity, EntityStatus};
use elephant_types::records::{IdentityCandidate, IdentityCandidateStatus};
use elephant_types::{Link, RecordId};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tracing::debug;

use super::{IdentitySweepResult, MaintenanceConfig};
use crate::error::PipelineError;
use crate::maintenance_commit::{PlanRecord, PlanningTruthMaintenanceStorage};
use crate::truth_maintenance::TruthMaintenanceStorage;

const MAX_IDENTITY_TOKEN_FANOUT: usize = 40;
#[cfg(feature = "meerkat-extraction")]
const IDENTITY_LLM_MAX_OUTPUT_TOKENS: u32 = 12_000;
#[cfg(feature = "meerkat-extraction")]
const IDENTITY_LLM_BUDGET_TOKENS: u64 = 16_000;

/// A candidate pair of potentially duplicate entities.
#[derive(Debug, Clone)]
pub struct CandidatePair {
    /// First entity.
    pub entity_a: Entity,
    /// Second entity.
    pub entity_b: Entity,
    /// Shared identity key tokens.
    pub shared_tokens: Vec<String>,
    /// Whether both entities were extracted from the same document.
    pub same_source_doc: bool,
}

/// Pre-built context dossier for an LLM identity decision.
#[derive(Debug, Clone)]
pub struct MergeDossier {
    /// The candidate pair.
    pub pair: CandidatePair,
    /// Entity A's relationships with counterpart names.
    pub a_relationships: Vec<RelSummary>,
    /// Entity B's relationships with counterpart names.
    pub b_relationships: Vec<RelSummary>,
    /// Source text excerpts mentioning entity A.
    pub a_excerpts: Vec<String>,
    /// Source text excerpts mentioning entity B.
    pub b_excerpts: Vec<String>,
    /// Shared source document titles.
    pub shared_doc_titles: Vec<String>,
    /// Provenance source summaries for entity A.
    pub a_sources: Vec<String>,
    /// Provenance source summaries for entity B.
    pub b_sources: Vec<String>,
}

/// Summary of a relationship for the dossier.
#[derive(Debug, Clone)]
pub struct RelSummary {
    /// Direction: "outgoing" or "incoming".
    pub direction: String,
    /// Relationship type.
    pub rel_type: String,
    /// Counterpart entity display name.
    pub counterpart_name: String,
}

/// LLM's structured decision about an entity pair.
#[derive(Debug, Clone, Deserialize)]
pub struct IdentityDecision {
    /// Whether the entities refer to the same real-world entity.
    pub same_entity: bool,
    /// Preferred canonical display name.
    pub canonical_display_name: String,
    /// Confidence score (0.0 to 1.0).
    pub confidence: f64,
    /// Reasoning for the decision.
    pub reasoning: String,
}

#[cfg(any(feature = "meerkat-extraction", test))]
#[derive(Debug, Deserialize)]
struct PartialIdentityDecision {
    same_entity: bool,
    canonical_display_name: String,
    confidence: f64,
    reasoning: Option<String>,
}

/// Runs the identity detection sweep for a space.
///
/// 1. Finds candidate pairs via token overlap in identity keys
/// 2. Filters pairs that already have an IdentityCandidate record
/// 3. Builds context dossiers for each pair
/// 4. Calls LLM for each pair (structured output, single turn)
/// 5. Auto-merges at high confidence, proposes at medium confidence
///
/// # Errors
///
/// Returns an error if storage operations fail fatally.
pub async fn run_identity_sweep(
    storage: &StorageClient,
    space_id: &str,
    config: &MaintenanceConfig,
) -> Result<IdentitySweepResult, PipelineError> {
    // Register extension epoch guards before capturing the one decision epoch.
    let registry = elephant_extensions::load_runtime_reference_registry_from_env()
        .map_err(|error| PipelineError::ConfigError(error.to_string()))?;
    let mut tables = registry.tables().map(str::to_owned).collect::<Vec<_>>();
    tables.sort();
    Box::pin(elephant_storage::canonical_commit::ensure_extension_epoch_guards(storage, &tables))
        .await?;
    let planning = Box::pin(PlanningTruthMaintenanceStorage::new(
        storage.clone(),
        space_id,
        None,
    ))
    .await?;
    let input = serde_json::json!({"space_id":space_id,"model":config.model,
        "auto_merge_threshold":config.auto_merge_threshold,
        "max_candidates_per_sweep":config.max_candidates_per_sweep});
    let result = Box::pin(prepare_identity_sweep(&planning, storage, space_id, config)).await?;
    Box::pin(planning.commit("identity_sweep", space_id, &input, &result)).await?;
    Ok(result)
}

/// Prepare all identity effects in the caller's original maintenance overlay.
/// No repository mutation or nested canonical commit occurs in this function.
pub(crate) async fn prepare_identity_sweep(
    planning: &PlanningTruthMaintenanceStorage,
    storage: &StorageClient,
    space_id: &str,
    config: &MaintenanceConfig,
) -> Result<IdentitySweepResult, PipelineError> {
    if space_id.trim().is_empty()
        || !config.auto_merge_threshold.is_finite()
        || !(0.0..=1.0).contains(&config.auto_merge_threshold)
    {
        return Err(PipelineError::InvalidInput(
            "invalid identity sweep space or confidence threshold".into(),
        ));
    }
    let mut result = IdentitySweepResult::default();
    let mut pending = read_rows(
        planning,
        &IdentityCandidateRepository::new(storage.clone()),
        space_id,
    )
    .await?;
    pending.retain(|candidate| candidate.status == IdentityCandidateStatus::Pending);
    pending.truncate(pending_recheck_budget(config.max_candidates_per_sweep));
    for selected in pending {
        let Some(candidate) = current_record(
            planning,
            &IdentityCandidateRepository::new(storage.clone()),
            space_id,
            &selected.base.id,
        )
        .await?
        else {
            result.skipped += 1;
            continue;
        };
        if candidate.status != IdentityCandidateStatus::Pending {
            continue;
        }
        let Some(pair) = current_pair(
            planning,
            space_id,
            candidate.entity_a.id(),
            candidate.entity_b.id(),
        )
        .await?
        else {
            result.skipped += 1;
            continue;
        };
        Box::pin(evaluate_pair(
            planning,
            storage,
            space_id,
            config,
            pair,
            Some(candidate),
            &mut result,
        ))
        .await?;
    }
    let remaining = config
        .max_candidates_per_sweep
        .saturating_sub(result.candidates_evaluated);
    if remaining == 0 {
        return Ok(result);
    }
    let candidates = find_candidate_pairs_in_plan(planning, storage, space_id, remaining).await?;
    for selected in candidates {
        // Prior pairs and preceding maintenance phases may have redirected or
        // enriched either side. Never reuse the discovery-time entity clone.
        let Some(pair) = current_pair(
            planning,
            space_id,
            &selected.entity_a.base.id,
            &selected.entity_b.base.id,
        )
        .await?
        else {
            result.skipped += 1;
            continue;
        };
        Box::pin(evaluate_pair(
            planning,
            storage,
            space_id,
            config,
            pair,
            None,
            &mut result,
        ))
        .await?;
    }
    Ok(result)
}

async fn current_pair(
    planning: &PlanningTruthMaintenanceStorage,
    space: &str,
    left: &RecordId,
    right: &RecordId,
) -> Result<Option<CandidatePair>, PipelineError> {
    if left == right {
        return Ok(None);
    }
    let (Some(entity_a), Some(entity_b)) = (
        planning.get_entity(space, left).await?,
        planning.get_entity(space, right).await?,
    ) else {
        return Ok(None);
    };
    if entity_a.status != EntityStatus::Active
        || entity_b.status != EntityStatus::Active
        || entity_a.redirect_to.is_some()
        || entity_b.redirect_to.is_some()
    {
        return Ok(None);
    }
    let mut shared_tokens = entity_identity_tokens(&entity_a)
        .intersection(&entity_identity_tokens(&entity_b))
        .cloned()
        .collect::<Vec<_>>();
    shared_tokens.sort();
    Ok(Some(CandidatePair {
        entity_a,
        entity_b,
        shared_tokens,
        same_source_doc: false,
    }))
}

async fn evaluate_pair(
    planning: &PlanningTruthMaintenanceStorage,
    storage: &StorageClient,
    space: &str,
    config: &MaintenanceConfig,
    pair: CandidatePair,
    existing: Option<IdentityCandidate>,
    result: &mut IdentitySweepResult,
) -> Result<(), PipelineError> {
    let mut context = Vec::new();
    let indistinguishable =
        crate::scheduler::inline_promotion::candidates_are_indistinguishable(&[
            pair.entity_a.clone(),
            pair.entity_b.clone(),
        ]);
    let decision = if indistinguishable {
        IdentityDecision { same_entity:true, confidence:1.0,
            canonical_display_name:pair.entity_a.display_name.clone(),
            reasoning:"identical display name and type without a distinguishing summary, alias or handle; deterministic identity decision".into() }
    } else {
        let (dossier, contributors) =
            build_dossier_in_plan(planning, storage, space, &pair).await?;
        context = contributors;
        match Box::pin(evaluate_with_llm(
            storage,
            space,
            config,
            &format_dossier_prompt(&dossier),
        ))
        .await
        {
            Ok(decision) => decision,
            Err(error @ PipelineError::StorageError(_)) => return Err(error),
            Err(error) => {
                debug!("Skipping identity candidate because its approved model is unavailable: {error}");
                result.skipped += 1;
                return Ok(());
            }
        }
    };
    validate_decision(&decision)?;
    result.candidates_evaluated += 1;
    if decision.same_entity && decision.confidence >= config.auto_merge_threshold {
        if let Some(candidate) = existing {
            approve_existing_candidate_and_merge(
                planning, storage, space, candidate, &pair, &decision, &context,
            )
            .await?;
        } else {
            auto_merge(planning, storage, space, &pair, &decision, &context).await?;
        }
        result.auto_merged += 1;
    } else {
        let status = if decision.same_entity {
            IdentityCandidateStatus::Pending
        } else {
            IdentityCandidateStatus::Rejected
        };
        if let Some(candidate) = existing {
            update_existing_candidate_decision(
                planning, space, candidate, &pair, &decision, status, &context,
            )?;
        } else {
            stage_candidate(planning, space, &pair, &decision, status, &context)?;
        }
        if decision.same_entity {
            result.proposed += 1;
        } else {
            result.rejected += 1;
        }
    }
    Ok(())
}

fn validate_decision(decision: &IdentityDecision) -> Result<(), PipelineError> {
    if !decision.confidence.is_finite()
        || !(0.0..=1.0).contains(&decision.confidence)
        || decision.canonical_display_name.trim().is_empty()
    {
        return Err(PipelineError::ValidationError(
            "identity decision has invalid confidence or display name".into(),
        ));
    }
    Ok(())
}

/// Page real rows and apply prior effects without making unrelated discovery
/// rows causal contributors. The original epoch fences the discovery predicate;
/// selected decision inputs are observed through `current_record`/`get_entity`.
/// Pagination and storage errors abort preparation.
async fn read_rows<T, R>(
    planning: &PlanningTruthMaintenanceStorage,
    repository: &R,
    space: &str,
) -> Result<Vec<T>, PipelineError>
where
    T: PlanRecord + Serialize + DeserializeOwned,
    R: Repository<T>,
{
    let mut rows = BTreeMap::new();
    let mut cursor = None;
    loop {
        let page = repository.list(space, 1000, cursor.as_ref()).await?;
        let count = page.len();
        let next = page.last().map(|row| row.base().id.clone());
        for row in page {
            rows.insert(row.base().id.to_string(), row);
        }
        if count < 1000 {
            break;
        }
        if next == cursor || next.is_none() {
            return Err(PipelineError::InvalidState(
                "identity sweep pagination did not advance".into(),
            ));
        }
        cursor = next;
    }
    // Include records created earlier in this plan, and remove staged deletes.
    for effect in planning.prepared_plan()?.effects {
        let Some(row) = effect
            .after
            .as_ref()
            .or(effect.before.as_ref())
            .and_then(T::from_record)
        else {
            continue;
        };
        let id = row.base().id.clone();
        rows.remove(&id.to_string());
        if let Some(current) = effect.after.as_ref().and_then(T::from_record) {
            if current.base().deleted_at.is_none() {
                rows.insert(id.to_string(), current);
            }
        }
    }
    Ok(rows.into_values().collect())
}

async fn current_record<T, R>(
    planning: &PlanningTruthMaintenanceStorage,
    repository: &R,
    space: &str,
    id: &RecordId,
) -> Result<Option<T>, PipelineError>
where
    T: PlanRecord + Serialize + DeserializeOwned,
    R: Repository<T>,
{
    planning.current_row(repository.get(space, id).await?, id, false)
}

/// Finds candidate pairs by tokenizing identity keys and finding overlaps.
///
/// Returns pairs where at least one identity key token is shared.
/// Filters out pairs that already have an IdentityCandidate record.
pub async fn find_candidate_pairs(
    storage: &StorageClient,
    space_id: &str,
    max_pairs: usize,
) -> Result<Vec<CandidatePair>, PipelineError> {
    let planning = PlanningTruthMaintenanceStorage::new(storage.clone(), space_id, None).await?;
    find_candidate_pairs_in_plan(&planning, storage, space_id, max_pairs).await
}

async fn find_candidate_pairs_in_plan(
    planning: &PlanningTruthMaintenanceStorage,
    storage: &StorageClient,
    space_id: &str,
    max_pairs: usize,
) -> Result<Vec<CandidatePair>, PipelineError> {
    if max_pairs == 0 {
        return Ok(Vec::new());
    }
    let mut entities =
        read_rows(planning, &EntityRepository::new(storage.clone()), space_id).await?;
    entities.retain(|entity| entity.status == EntityStatus::Active && entity.redirect_to.is_none());
    if entities.len() < 2 {
        return Ok(Vec::new());
    }

    // Build token → entity index. Candidate discovery should not depend on
    // extractors pre-splitting identity keys perfectly: email-shaped keys,
    // handles, aliases, and display names all provide useful generic signals.
    let mut token_index: HashMap<String, Vec<usize>> = HashMap::new();
    let mut entity_tokens = Vec::with_capacity(entities.len());
    for (idx, entity) in entities.iter().enumerate() {
        let tokens = entity_identity_tokens(entity);
        for token in &tokens {
            token_index.entry(token.clone()).or_default().push(idx);
        }
        entity_tokens.push(tokens);
    }

    // Find pairs with shared tokens
    let mut seen_pairs: HashSet<(usize, usize)> = HashSet::new();
    let mut pairs = Vec::new();

    for indices in token_index.values() {
        if indices.len() < 2 {
            continue;
        }
        if indices.len() > MAX_IDENTITY_TOKEN_FANOUT {
            continue;
        }
        for i in 0..indices.len() {
            for j in (i + 1)..indices.len() {
                let a = indices[i];
                let b = indices[j];
                let key = if a < b { (a, b) } else { (b, a) };
                if !seen_pairs.insert(key) {
                    continue;
                }

                // Skip if same identity keys (already handled by inline promotion)
                if entities[a].identity_keys == entities[b].identity_keys {
                    continue;
                }

                // Collect all shared tokens for this pair
                let mut shared: Vec<String> = entity_tokens[a]
                    .intersection(&entity_tokens[b])
                    .cloned()
                    .collect();
                shared.sort();

                pairs.push(CandidatePair {
                    entity_a: entities[a].clone(),
                    entity_b: entities[b].clone(),
                    shared_tokens: shared,
                    same_source_doc: false, // populated in build_dossier
                });
            }
        }
    }

    pairs.sort_by(|left, right| {
        candidate_pair_priority(right)
            .cmp(&candidate_pair_priority(left))
            .then_with(|| {
                left.entity_a
                    .display_name
                    .cmp(&right.entity_a.display_name)
                    .then_with(|| left.entity_b.display_name.cmp(&right.entity_b.display_name))
                    .then_with(|| candidate_log_pair_key(left).cmp(&candidate_log_pair_key(right)))
            })
    });

    let existing_candidate_pairs = read_rows(
        planning,
        &IdentityCandidateRepository::new(storage.clone()),
        space_id,
    )
    .await?
    .into_iter()
    .map(|candidate| ordered_pair_key(candidate.entity_a.id(), candidate.entity_b.id()))
    .collect::<HashSet<_>>();
    let mut selected = Vec::new();
    for pair in pairs {
        if existing_candidate_pairs.contains(&candidate_log_pair_key(&pair)) {
            continue;
        }
        selected.push(pair);
        if selected.len() >= max_pairs {
            break;
        }
    }

    Ok(selected)
}

fn candidate_log_pair_key(pair: &CandidatePair) -> (String, String) {
    ordered_pair_key(&pair.entity_a.base.id, &pair.entity_b.base.id)
}

fn ordered_pair_key(left: &RecordId, right: &RecordId) -> (String, String) {
    let a = left.to_string();
    let b = right.to_string();
    if a <= b {
        (a, b)
    } else {
        (b, a)
    }
}

fn pending_recheck_budget(max_candidates_per_sweep: usize) -> usize {
    if max_candidates_per_sweep <= 1 {
        return max_candidates_per_sweep;
    }
    max_candidates_per_sweep / 2
}

fn candidate_pair_priority(pair: &CandidatePair) -> i32 {
    if email_localpart_name_match(&pair.entity_a, &pair.entity_b) {
        return 1_000;
    }

    if person_name_token_set_match(&pair.entity_a, &pair.entity_b) {
        return 900;
    }

    if person_name_token_subset_match(&pair.entity_a, &pair.entity_b) {
        return 850;
    }

    if pair.entity_a.entity_type == pair.entity_b.entity_type {
        let a = pair.entity_a.display_name.to_lowercase();
        let b = pair.entity_b.display_name.to_lowercase();
        if a.contains(&b) || b.contains(&a) {
            return 500;
        }
    }

    0
}

fn entity_identity_tokens(entity: &Entity) -> HashSet<String> {
    let mut tokens = HashSet::new();
    add_identity_tokens(&mut tokens, &entity.display_name);
    for alias in &entity.aliases {
        add_identity_tokens(&mut tokens, alias);
    }
    for handle in &entity.handles {
        add_identity_tokens(&mut tokens, &handle.platform);
        add_identity_tokens(&mut tokens, &handle.username);
    }
    for key in &entity.identity_keys {
        add_identity_tokens(&mut tokens, key);
    }
    tokens
}

fn add_identity_tokens(tokens: &mut HashSet<String>, value: &str) {
    let normalized = value.trim().to_lowercase();
    if normalized.is_empty() {
        return;
    }

    // Keep the full key as a high-specificity token, then add conservative
    // sub-tokens so "name@example.com" can match "Name" without extractor
    // authors needing provider-specific identity-key formatting.
    if normalized.len() >= 3 && !is_low_signal_identity_token(&normalized) {
        tokens.insert(normalized.clone());
    }

    for token in normalized
        .split(|ch: char| !ch.is_alphanumeric())
        .filter(|token| token.len() >= 3)
        .filter(|token| !is_low_signal_identity_token(token))
    {
        tokens.insert(token.to_string());
    }
}

fn is_low_signal_identity_token(token: &str) -> bool {
    matches!(
        token,
        "www"
            | "com"
            | "org"
            | "net"
            | "edu"
            | "gov"
            | "mail"
            | "email"
            | "the"
            | "and"
            | "for"
            | "with"
    )
}

/// Builds a rich context dossier for an entity pair.
///
/// Loads relationships, source text excerpts, and shared provenance
/// so the LLM has everything it needs for a single-call decision.
pub async fn build_dossier(
    storage: &StorageClient,
    space_id: &str,
    pair: &CandidatePair,
) -> Result<MergeDossier, PipelineError> {
    let planning = PlanningTruthMaintenanceStorage::new(storage.clone(), space_id, None).await?;
    let pair = current_pair(
        &planning,
        space_id,
        &pair.entity_a.base.id,
        &pair.entity_b.base.id,
    )
    .await?
    .ok_or_else(|| PipelineError::InvalidState("identity pair is no longer active".into()))?;
    Ok(build_dossier_in_plan(&planning, storage, space_id, &pair)
        .await?
        .0)
}

async fn build_dossier_in_plan(
    planning: &PlanningTruthMaintenanceStorage,
    storage: &StorageClient,
    space: &str,
    pair: &CandidatePair,
) -> Result<(MergeDossier, Vec<BaseFields>), PipelineError> {
    let rel_repo = RelRepository::new(storage.clone());
    let assertion_repo = AssertionRepository::new(storage.clone());
    let mut context = Vec::new();
    let mut a_relationships = Vec::new();
    let mut b_relationships = Vec::new();
    for selected in read_rows(planning, &rel_repo, space).await? {
        if ![selected.subject.id(), selected.object.id()]
            .iter()
            .any(|id| **id == pair.entity_a.base.id || **id == pair.entity_b.base.id)
        {
            continue;
        }
        let Some(rel) = current_record(planning, &rel_repo, space, &selected.base.id).await? else {
            continue;
        };
        for (id, summaries) in [
            (&pair.entity_a.base.id, &mut a_relationships),
            (&pair.entity_b.base.id, &mut b_relationships),
        ] {
            for (direction, counterpart) in [
                (
                    "outgoing",
                    (rel.subject.id() == id).then_some(rel.object.id()),
                ),
                (
                    "incoming",
                    (rel.object.id() == id).then_some(rel.subject.id()),
                ),
            ] {
                let Some(counterpart) = counterpart else {
                    continue;
                };
                let name = match planning.get_entity(space, counterpart).await? {
                    Some(entity) => {
                        context.push(entity.base.clone());
                        entity.display_name
                    }
                    None => counterpart.to_string(),
                };
                context.push(rel.base.clone());
                summaries.push(RelSummary {
                    direction: direction.into(),
                    rel_type: rel.rel_type.clone(),
                    counterpart_name: name,
                });
            }
        }
    }
    let assertions = read_rows(planning, &assertion_repo, space).await?;
    let mut a_assertions = Vec::new();
    let mut b_assertions = Vec::new();
    for selected in assertions {
        let mentions = |assertion: &elephant_types::records::Assertion, id: &RecordId| {
            assertion
                .subject_entity
                .as_ref()
                .is_some_and(|link| link.id() == id)
                || assertion
                    .object_entity
                    .as_ref()
                    .is_some_and(|link| link.id() == id)
        };
        if !(a_assertions.len() < 10 && mentions(&selected, &pair.entity_a.base.id)
            || b_assertions.len() < 10 && mentions(&selected, &pair.entity_b.base.id))
        {
            continue;
        }
        let Some(assertion) =
            current_record(planning, &assertion_repo, space, &selected.base.id).await?
        else {
            continue;
        };
        if a_assertions.len() < 10 && mentions(&assertion, &pair.entity_a.base.id) {
            context.push(assertion.base.clone());
            a_assertions.push(assertion.clone());
        }
        if b_assertions.len() < 10 && mentions(&assertion, &pair.entity_b.base.id) {
            context.push(assertion.base.clone());
            b_assertions.push(assertion);
        }
    }
    let a_revisions = a_assertions
        .iter()
        .map(|a| a.evidence.revision.id().to_string())
        .collect::<HashSet<_>>();
    let b_revisions = b_assertions
        .iter()
        .map(|a| a.evidence.revision.id().to_string())
        .collect::<HashSet<_>>();
    let mut shared_doc_titles = a_revisions
        .intersection(&b_revisions)
        .cloned()
        .collect::<Vec<_>>();
    shared_doc_titles.sort();
    let a_sources =
        load_source_context_for_entity(planning, storage, space, &pair.entity_a, &mut context)
            .await?;
    let b_sources =
        load_source_context_for_entity(planning, storage, space, &pair.entity_b, &mut context)
            .await?;
    Ok((
        MergeDossier {
            pair: CandidatePair {
                same_source_doc: !shared_doc_titles.is_empty(),
                ..pair.clone()
            },
            a_relationships,
            b_relationships,
            a_excerpts: a_assertions.iter().map(format_assertion_excerpt).collect(),
            b_excerpts: b_assertions.iter().map(format_assertion_excerpt).collect(),
            shared_doc_titles,
            a_sources,
            b_sources,
        },
        context,
    ))
}

async fn load_source_context_for_entity(
    planning: &PlanningTruthMaintenanceStorage,
    storage: &StorageClient,
    space: &str,
    entity: &Entity,
    context: &mut Vec<BaseFields>,
) -> Result<Vec<String>, PipelineError> {
    let repository = SourceRepository::new(storage.clone());
    let mut summaries = Vec::new();
    let mut seen = HashSet::new();
    for id in &entity.base.provenance.sources {
        if summaries.len() >= 8 {
            break;
        }
        if !seen.insert(id.clone()) {
            continue;
        }
        if let Some(source) = current_record(planning, &repository, space, id).await? {
            summaries.push(format_source_context(&source));
            context.push(source.base);
        }
    }
    Ok(summaries)
}

fn format_source_context(source: &elephant_types::records::source::Source) -> String {
    let mut parts = Vec::new();
    parts.push(format!("type={}", source_type_label(source.type_)));
    if let Some(uri) = source
        .uri
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        parts.push(format!("uri={uri}"));
    }
    if let Some(external_id) = source
        .external_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        parts.push(format!("external_id={external_id}"));
    }
    if let Some(timestamp) = source.timestamp {
        parts.push(format!("timestamp={}", timestamp.to_rfc3339()));
    }
    if let Some(metadata) = source.metadata.as_object() {
        for key in ["title", "snippet", "collection"] {
            if let Some(value) = metadata.get(key).and_then(|value| value.as_str()) {
                let value = value.trim();
                if !value.is_empty() {
                    parts.push(format!("{key}={value}"));
                }
            }
        }
    }
    parts.join("; ")
}

fn source_type_label(source_type: elephant_types::records::source::SourceType) -> &'static str {
    match source_type {
        elephant_types::records::source::SourceType::Email => "email",
        elephant_types::records::source::SourceType::Calendar => "calendar",
        elephant_types::records::source::SourceType::Web => "web",
        elephant_types::records::source::SourceType::Drive => "drive",
        elephant_types::records::source::SourceType::Chat => "chat",
        elephant_types::records::source::SourceType::DeviceFrame => "device_frame",
        elephant_types::records::source::SourceType::DeviceVideo => "device_video",
        elephant_types::records::source::SourceType::Transcript => "transcript",
        elephant_types::records::source::SourceType::Manual => "manual",
    }
}

/// Formats an assertion as a human-readable excerpt for the dossier.
fn format_assertion_excerpt(assertion: &elephant_types::records::assertion::Assertion) -> String {
    let obj = assertion
        .object_text
        .as_deref()
        .or_else(|| assertion.object_literal.as_ref().map(|_| "<literal>"))
        .unwrap_or("?");
    format!(
        "{} {} {}",
        assertion.subject_text, assertion.predicate_raw, obj
    )
}

/// Formats a merge dossier as a structured prompt for the LLM.
pub fn format_dossier_prompt(dossier: &MergeDossier) -> String {
    let mut prompt = String::new();

    prompt.push_str("Are the following two entities the same real-world entity?\n\n");

    // Entity A
    prompt.push_str(&format!(
        "## Entity A: \"{}\" ({})\n",
        dossier.pair.entity_a.display_name, dossier.pair.entity_a.entity_type,
    ));
    if let Some(summary) = dossier.pair.entity_a.summary.as_deref() {
        if !summary.trim().is_empty() {
            prompt.push_str(&format!("- Summary: {summary}\n"));
        }
    }
    if !dossier.pair.entity_a.aliases.is_empty() {
        prompt.push_str(&format!("- Aliases: {:?}\n", dossier.pair.entity_a.aliases));
    }
    prompt.push_str(&format!(
        "- Identity keys: {:?}\n",
        dossier.pair.entity_a.identity_keys
    ));
    if !dossier.a_relationships.is_empty() {
        prompt.push_str("- Relationships:\n");
        for rel in &dossier.a_relationships {
            prompt.push_str(&format!(
                "  - {} {} {} ({})\n",
                dossier.pair.entity_a.display_name,
                if rel.direction == "outgoing" {
                    "→"
                } else {
                    "←"
                },
                rel.counterpart_name,
                rel.rel_type,
            ));
        }
    }
    if !dossier.a_excerpts.is_empty() {
        prompt.push_str("- Source text excerpts:\n");
        for excerpt in &dossier.a_excerpts {
            prompt.push_str(&format!("  > \"{excerpt}\"\n"));
        }
    }
    if !dossier.a_sources.is_empty() {
        prompt.push_str("- Provenance sources:\n");
        for source in &dossier.a_sources {
            prompt.push_str(&format!("  - {source}\n"));
        }
    }

    prompt.push('\n');

    // Entity B
    prompt.push_str(&format!(
        "## Entity B: \"{}\" ({})\n",
        dossier.pair.entity_b.display_name, dossier.pair.entity_b.entity_type,
    ));
    if let Some(summary) = dossier.pair.entity_b.summary.as_deref() {
        if !summary.trim().is_empty() {
            prompt.push_str(&format!("- Summary: {summary}\n"));
        }
    }
    if !dossier.pair.entity_b.aliases.is_empty() {
        prompt.push_str(&format!("- Aliases: {:?}\n", dossier.pair.entity_b.aliases));
    }
    prompt.push_str(&format!(
        "- Identity keys: {:?}\n",
        dossier.pair.entity_b.identity_keys
    ));
    if !dossier.b_relationships.is_empty() {
        prompt.push_str("- Relationships:\n");
        for rel in &dossier.b_relationships {
            prompt.push_str(&format!(
                "  - {} {} {} ({})\n",
                dossier.pair.entity_b.display_name,
                if rel.direction == "outgoing" {
                    "→"
                } else {
                    "←"
                },
                rel.counterpart_name,
                rel.rel_type,
            ));
        }
    }
    if !dossier.b_excerpts.is_empty() {
        prompt.push_str("- Source text excerpts:\n");
        for excerpt in &dossier.b_excerpts {
            prompt.push_str(&format!("  > \"{excerpt}\"\n"));
        }
    }
    if !dossier.b_sources.is_empty() {
        prompt.push_str("- Provenance sources:\n");
        for source in &dossier.b_sources {
            prompt.push_str(&format!("  - {source}\n"));
        }
    }

    prompt.push('\n');

    // Shared signals
    prompt.push_str("## Shared Signals\n");
    prompt.push_str(&format!(
        "- Shared identity key tokens: {:?}\n",
        dossier.pair.shared_tokens
    ));
    if dossier.pair.same_source_doc {
        prompt.push_str("- Both entities were extracted from the SAME source document\n");
    }
    if !dossier.shared_doc_titles.is_empty() {
        prompt.push_str(&format!(
            "- Shared source revisions: {:?}\n",
            dossier.shared_doc_titles
        ));
    }

    prompt.push_str(
        "\nReturn exactly one JSON object and no markdown. Required fields:\n\
         {\"same_entity\": boolean, \"canonical_display_name\": string, \
         \"confidence\": number, \"reasoning\": string}\n",
    );

    prompt
}

fn person_name_token_set_match(a: &Entity, b: &Entity) -> bool {
    if a.entity_type != b.entity_type || a.entity_type.to_string() != "person" {
        return false;
    }
    let a_tokens = display_name_tokens(&a.display_name);
    let b_tokens = display_name_tokens(&b.display_name);
    a_tokens.len() >= 2 && a_tokens == b_tokens
}

fn person_name_token_subset_match(a: &Entity, b: &Entity) -> bool {
    if a.entity_type != b.entity_type || a.entity_type.to_string() != "person" {
        return false;
    }
    let a_tokens = display_name_tokens(&a.display_name);
    let b_tokens = display_name_tokens(&b.display_name);
    let shared_count = a_tokens.intersection(&b_tokens).count();
    shared_count >= 2 && (a_tokens.is_subset(&b_tokens) || b_tokens.is_subset(&a_tokens))
}

fn display_name_tokens(display_name: &str) -> HashSet<String> {
    display_name
        .to_lowercase()
        .split(|ch: char| !ch.is_alphanumeric())
        .filter(|token| token.len() >= 2)
        .filter(|token| !is_low_signal_identity_token(token))
        .map(ToString::to_string)
        .collect()
}

fn email_localpart_name_match(a: &Entity, b: &Entity) -> bool {
    if a.entity_type != b.entity_type || a.entity_type.to_string() != "person" {
        return false;
    }

    let a_name_tokens = entity_identity_tokens(a);
    let b_name_tokens = entity_identity_tokens(b);
    email_localparts(a)
        .iter()
        .any(|localpart| b_name_tokens.contains(localpart))
        || email_localparts(b)
            .iter()
            .any(|localpart| a_name_tokens.contains(localpart))
}

fn email_localparts(entity: &Entity) -> HashSet<String> {
    let mut localparts = HashSet::new();

    collect_email_localparts(&mut localparts, &entity.display_name);
    for alias in &entity.aliases {
        collect_email_localparts(&mut localparts, alias);
    }
    for handle in &entity.handles {
        collect_email_localparts(&mut localparts, &handle.username);
    }
    for key in &entity.identity_keys {
        collect_email_localparts(&mut localparts, key);
    }

    localparts
}

fn collect_email_localparts(localparts: &mut HashSet<String>, value: &str) {
    for segment in value.split_whitespace() {
        if let Some((localpart, _domain)) = segment.split_once('@') {
            let cleaned = localpart
                .trim_matches(|ch: char| !ch.is_alphanumeric())
                .to_lowercase();
            if cleaned.len() >= 3 && !is_low_signal_identity_token(&cleaned) {
                localparts.insert(cleaned);
            }
        }
    }
}

/// Evaluates a candidate pair using the LLM via meerkat.
///
/// Builds an agent with structured output and runs a single-turn evaluation
/// with the pre-built dossier prompt.
///
/// Returns an error if the LLM is unavailable (no API key, connection failure).
/// The caller skips the candidate in that case; merge decisions must not fall
/// back to local string heuristics.
#[cfg(feature = "meerkat-extraction")]
fn identity_decision_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "same_entity": { "type": "boolean" },
            "canonical_display_name": { "type": "string" },
            "confidence": { "type": "number" },
            "reasoning": { "type": "string" }
        },
        "required": ["same_entity", "canonical_display_name", "confidence", "reasoning"],
        "additionalProperties": false
    })
}

#[cfg(feature = "meerkat-extraction")]
fn identity_system_prompt() -> &'static str {
    "You are an identity resolution agent for a knowledge graph. \
     You decide whether two entities refer to the same real-world entity. \
     Use only the provided dossier; it contains the relevant names, \
     identity keys, relationships, and source excerpts. \
     Return only a JSON object matching the requested schema. \
     Never omit canonical_display_name; if the entities are different, \
     use the better-supported existing display name as canonical_display_name."
}

#[cfg(feature = "meerkat-extraction")]
fn identity_text_fallback_prompt(prompt: &str) -> String {
    format!(
        "{prompt}\n\n\
         The previous structured-output attempt returned no usable payload. \
         Re-evaluate the same dossier and return exactly one JSON object, \
         with no markdown or surrounding prose, using these keys: \
         same_entity (boolean), canonical_display_name (string), \
         confidence (number from 0 to 1), reasoning (string)."
    )
}

#[cfg(feature = "meerkat-extraction")]
fn identity_provider_params(
    model: &str,
) -> Option<meerkat_core::lifecycle::run_primitive::ProviderParamsOverride> {
    if let Ok(effort) = std::env::var("MAINTENANCE_REASONING_EFFORT") {
        return identity_provider_params_for_model(model, Some(effort.as_str()));
    }

    identity_provider_params_for_model(model, None)
}

#[cfg(any(feature = "meerkat-extraction", test))]
fn identity_provider_params_for_model(
    model: &str,
    explicit_reasoning_effort: Option<&str>,
) -> Option<meerkat_core::lifecycle::run_primitive::ProviderParamsOverride> {
    use meerkat_core::lifecycle::run_primitive::{
        OpenAiProviderTag, ProviderParamsOverride, ProviderTag, ReasoningEffort,
    };

    fn effort_from_str(value: &str) -> Option<ReasoningEffort> {
        match value.trim().to_ascii_lowercase().as_str() {
            "none" => Some(ReasoningEffort::None),
            "low" => Some(ReasoningEffort::Low),
            "medium" => Some(ReasoningEffort::Medium),
            "high" => Some(ReasoningEffort::High),
            "xhigh" => Some(ReasoningEffort::XHigh),
            _ => None,
        }
    }

    fn params(effort: ReasoningEffort) -> ProviderParamsOverride {
        ProviderParamsOverride {
            provider_tag: Some(ProviderTag::OpenAi(OpenAiProviderTag {
                reasoning_effort: Some(effort),
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    if let Some(effort) = explicit_reasoning_effort
        .map(str::trim)
        .filter(|effort| !effort.is_empty())
    {
        return effort_from_str(effort).map(params);
    }

    if model.starts_with("gpt-") || model.starts_with("o") {
        return Some(params(ReasoningEffort::Low));
    }

    None
}

#[cfg(feature = "meerkat-extraction")]
async fn evaluate_with_llm(
    _storage: &StorageClient,
    _space_id: &str,
    config: &MaintenanceConfig,
    prompt: &str,
) -> Result<IdentityDecision, PipelineError> {
    use std::path::PathBuf;
    use std::time::Duration;

    use meerkat::{
        AgentBuildConfig, AgentFactory, BudgetLimits, OutputSchema, ToolCategoryOverride,
    };
    use meerkat_core::types::ContentInput;
    use meerkat_core::Config;

    let budget_limits = BudgetLimits::default()
        .with_max_tokens(IDENTITY_LLM_BUDGET_TOKENS)
        .with_max_duration(Duration::from_secs(60));

    let output_schema = OutputSchema::new(identity_decision_schema())
        .map_err(|e| PipelineError::ExtractionError(format!("Invalid identity schema: {e}")))?
        .with_name("identity_decision")
        .strict();

    let factory = AgentFactory::new(PathBuf::from("/tmp/elephant-maintenance"));
    let meerkat_config = Config::default();
    let provider_params = identity_provider_params(&config.model);
    let mut build = AgentBuildConfig::new(config.model.clone());
    build.system_prompt =
        meerkat_core::SystemPromptOverride::Set(identity_system_prompt().to_string());
    build.max_tokens = Some(IDENTITY_LLM_MAX_OUTPUT_TOKENS);
    build.budget_limits = Some(budget_limits);
    build.output_schema = Some(output_schema);
    build.structured_output_retries = Some(2);
    build.override_shell = ToolCategoryOverride::Disable;
    build.override_mob = ToolCategoryOverride::Disable;
    build.provider_params = provider_params.clone();

    crate::provider_dispatch::govern_agent_build(
        elephant_storage::SpaceConfigRepository::new(_storage.clone()),
        _space_id,
        elephant_types::provider_binding::ModelOperation::IdentityDisambiguation,
        &mut build,
    )
    .await?;
    // Keep the factory/run state machines out of the sweep's inline future.
    // Boxing only the outer caller still constructs these nested states on
    // the runtime worker stack, including the retained fallback branch.
    let mut agent = Box::pin(factory.build_agent(build, &meerkat_config))
        .await
        .map_err(|e| PipelineError::ExtractionError(format!("Identity agent build failed: {e}")))?;

    let run_result = Box::pin(agent.run(ContentInput::Text(prompt.to_string())))
        .await
        .map_err(|e| PipelineError::ExtractionError(format!("Identity agent failed: {e}")))?;

    match parse_identity_decision_run_result(
        run_result.structured_output.as_ref(),
        run_result.text.trim(),
        run_result.extraction_error.as_ref(),
    ) {
        Ok(decision) => Ok(decision),
        Err(err) if is_empty_identity_decision_output_error(&err) => {
            tracing::warn!(
                model = %config.model,
                "Identity agent returned empty structured/text output; retrying once with text JSON fallback"
            );

            let fallback_budget_limits = BudgetLimits::default()
                .with_max_tokens(IDENTITY_LLM_BUDGET_TOKENS)
                .with_max_duration(Duration::from_secs(60));
            let mut fallback_build = AgentBuildConfig::new(config.model.clone());
            fallback_build.system_prompt =
                meerkat_core::SystemPromptOverride::Set(identity_system_prompt().to_string());
            fallback_build.max_tokens = Some(IDENTITY_LLM_MAX_OUTPUT_TOKENS);
            fallback_build.budget_limits = Some(fallback_budget_limits);
            fallback_build.override_shell = ToolCategoryOverride::Disable;
            fallback_build.override_mob = ToolCategoryOverride::Disable;
            fallback_build.provider_params = provider_params;

            crate::provider_dispatch::govern_agent_build(
                elephant_storage::SpaceConfigRepository::new(_storage.clone()),
                _space_id,
                elephant_types::provider_binding::ModelOperation::IdentityDisambiguation,
                &mut fallback_build,
            )
            .await?;
            let mut fallback_agent = Box::pin(factory.build_agent(fallback_build, &meerkat_config))
                .await
                .map_err(|build_err| {
                    PipelineError::ExtractionError(format!(
                        "{err}; identity fallback agent build failed: {build_err}"
                    ))
                })?;

            let fallback_result = Box::pin(
                fallback_agent.run(ContentInput::Text(identity_text_fallback_prompt(prompt))),
            )
            .await
            .map_err(|run_err| {
                PipelineError::ExtractionError(format!(
                    "{err}; identity fallback agent failed: {run_err}"
                ))
            })?;

            parse_identity_decision_run_result(
                fallback_result.structured_output.as_ref(),
                fallback_result.text.trim(),
                fallback_result.extraction_error.as_ref(),
            )
            .map_err(|fallback_err| {
                PipelineError::ExtractionError(format!(
                    "{err}; identity fallback also failed: {fallback_err}"
                ))
            })
        }
        Err(err) => Err(err),
    }
}

#[cfg(any(feature = "meerkat-extraction", test))]
fn parse_identity_decision_value(
    value: serde_json::Value,
) -> Result<IdentityDecision, PipelineError> {
    let decision: PartialIdentityDecision = serde_json::from_value(value).map_err(|e| {
        PipelineError::ExtractionError(format!("Failed to parse identity decision: {e}"))
    })?;
    Ok(decision.into_identity_decision())
}

#[cfg(any(feature = "meerkat-extraction", test))]
fn parse_identity_decision_text(text: &str) -> Result<IdentityDecision, PipelineError> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(PipelineError::ExtractionError(
            "Identity agent returned empty text and no structured output".to_string(),
        ));
    }

    let mut errors = Vec::new();
    for candidate in identity_decision_json_candidates(trimmed) {
        match serde_json::from_str::<PartialIdentityDecision>(candidate) {
            Ok(decision) => return Ok(decision.into_identity_decision()),
            Err(error) => errors.push(error.to_string()),
        }
    }

    Err(PipelineError::ExtractionError(format!(
        "Failed to parse identity decision from text: {}",
        errors
            .first()
            .cloned()
            .unwrap_or_else(|| "no JSON object found".to_string())
    )))
}

#[cfg(any(feature = "meerkat-extraction", test))]
fn parse_identity_decision_run_result(
    structured_output: Option<&serde_json::Value>,
    text: &str,
    extraction_error: Option<&meerkat_core::types::ExtractionError>,
) -> Result<IdentityDecision, PipelineError> {
    if let Some(structured) = structured_output {
        if let Ok(decision) = parse_identity_decision_value(structured.clone()) {
            return Ok(decision);
        }
    }

    parse_identity_decision_text(text).map_err(|err| {
        if is_empty_identity_decision_output_error(&err) {
            if let Some(extraction_error) = extraction_error {
                return PipelineError::ExtractionError(format!(
                    "{err}; structured extraction failed after {} attempts: {}; last_output_len={}",
                    extraction_error.attempts,
                    extraction_error.reason,
                    extraction_error.last_output.len(),
                ));
            }
        }
        err
    })
}

#[cfg(any(feature = "meerkat-extraction", test))]
fn is_empty_identity_decision_output_error(err: &PipelineError) -> bool {
    matches!(
        err,
        PipelineError::ExtractionError(message)
            if message.contains("Identity agent returned empty text and no structured output")
    )
}

#[cfg(any(feature = "meerkat-extraction", test))]
impl PartialIdentityDecision {
    fn into_identity_decision(self) -> IdentityDecision {
        IdentityDecision {
            same_entity: self.same_entity,
            canonical_display_name: self.canonical_display_name,
            confidence: self.confidence,
            reasoning: self
                .reasoning
                .filter(|reasoning| !reasoning.trim().is_empty())
                .unwrap_or_else(|| "LLM did not provide a separate reasoning field.".to_string()),
        }
    }
}

#[cfg(any(feature = "meerkat-extraction", test))]
fn identity_decision_json_candidates(text: &str) -> Vec<&str> {
    let mut candidates = Vec::new();
    let trimmed = text.trim();
    candidates.push(trimmed);

    if let Some(fenced) = extract_fenced_json(trimmed) {
        candidates.push(fenced);
    }

    if let (Some(start), Some(end)) = (trimmed.find('{'), trimmed.rfind('}')) {
        if start < end {
            candidates.push(&trimmed[start..=end]);
        }
    }

    candidates
}

#[cfg(any(feature = "meerkat-extraction", test))]
fn extract_fenced_json(text: &str) -> Option<&str> {
    let fence_start = text.find("```")?;
    let after_start = &text[fence_start + 3..];
    let after_language = after_start
        .strip_prefix("json")
        .or_else(|| after_start.strip_prefix("JSON"))
        .unwrap_or(after_start);
    let after_newline = after_language
        .strip_prefix('\n')
        .or_else(|| after_language.strip_prefix("\r\n"))
        .unwrap_or(after_language);
    let fence_end = after_newline.find("```")?;
    Some(after_newline[..fence_end].trim())
}

/// Fallback when meerkat-extraction feature is not enabled.
#[cfg(not(feature = "meerkat-extraction"))]
async fn evaluate_with_llm(
    _storage: &StorageClient,
    _space_id: &str,
    _config: &MaintenanceConfig,
    _prompt: &str,
) -> Result<IdentityDecision, PipelineError> {
    Err(PipelineError::ConfigError(
        "meerkat-extraction feature not enabled".to_string(),
    ))
}

/// Stage the candidate and its complete canonical merge in the same overlay.
async fn auto_merge(
    planning: &PlanningTruthMaintenanceStorage,
    storage: &StorageClient,
    space: &str,
    pair: &CandidatePair,
    decision: &IdentityDecision,
    context: &[BaseFields],
) -> Result<(), PipelineError> {
    observe_pair(planning, space, pair).await?;
    // Validate the decision while its exact contributor snapshots remain in
    // the overlay; a later chained merge may coalesce an intermediate entity.
    stage_candidate(
        planning,
        space,
        pair,
        decision,
        IdentityCandidateStatus::Approved,
        context,
    )?;
    merge_entities_for_decision(planning, storage, space, pair, decision, context).await
}

async fn approve_existing_candidate_and_merge(
    planning: &PlanningTruthMaintenanceStorage,
    storage: &StorageClient,
    space: &str,
    candidate: IdentityCandidate,
    pair: &CandidatePair,
    decision: &IdentityDecision,
    context: &[BaseFields],
) -> Result<(), PipelineError> {
    update_existing_candidate_decision(
        planning,
        space,
        candidate,
        pair,
        decision,
        IdentityCandidateStatus::Approved,
        context,
    )?;
    merge_entities_for_decision(planning, storage, space, pair, decision, context).await
}

fn update_existing_candidate_decision(
    planning: &PlanningTruthMaintenanceStorage,
    space: &str,
    mut candidate: IdentityCandidate,
    pair: &CandidatePair,
    decision: &IdentityDecision,
    status: IdentityCandidateStatus,
    context: &[BaseFields],
) -> Result<(), PipelineError> {
    let before = candidate.clone();
    configure_candidate(&mut candidate, space, pair, decision, status, context, true)?;
    planning.stage_inherited(
        Some(before),
        candidate,
        &decision_contributors(pair, context),
    )?;
    Ok(())
}

fn stage_candidate(
    planning: &PlanningTruthMaintenanceStorage,
    space: &str,
    pair: &CandidatePair,
    decision: &IdentityDecision,
    status: IdentityCandidateStatus,
    context: &[BaseFields],
) -> Result<(), PipelineError> {
    let mut base = BaseFields::new(
        RecordId::new_v4(TableName::IdentityCandidate),
        space.into(),
        "maintenance_worker".into(),
    );
    base.security = pair.entity_a.base.security.clone();
    base.subjects_state = SubjectsState::Complete;
    let mut candidate = IdentityCandidate {
        base,
        entity_a: Link::new(pair.entity_a.base.id.clone()),
        entity_b: Link::new(pair.entity_b.base.id.clone()),
        signals: serde_json::Value::Null,
        confidence: decision.confidence,
        status,
        proposed_by: "maintenance_worker".into(),
        decided_by: None,
        decided_at: None,
    };
    configure_candidate(
        &mut candidate,
        space,
        pair,
        decision,
        status,
        context,
        false,
    )?;
    planning.stage_inherited(None, candidate, &decision_contributors(pair, context))?;
    Ok(())
}

fn configure_candidate(
    candidate: &mut IdentityCandidate,
    space: &str,
    pair: &CandidatePair,
    decision: &IdentityDecision,
    status: IdentityCandidateStatus,
    context: &[BaseFields],
    rechecked: bool,
) -> Result<(), PipelineError> {
    validate_decision(decision)?;
    if candidate.base.space_id != space {
        return Err(PipelineError::InvalidState(
            "identity candidate crossed space".into(),
        ));
    }
    let (entity_a, entity_b) = if status == IdentityCandidateStatus::Approved {
        survivor_and_merged(pair, decision)
    } else {
        (&pair.entity_a, &pair.entity_b)
    };
    inherit_decision_context(&mut candidate.base, pair, context)?;
    candidate.base.updated_at = chrono::Utc::now();
    candidate.entity_a = Link::new(entity_a.base.id.clone());
    candidate.entity_b = Link::new(entity_b.base.id.clone());
    candidate.signals = identity_candidate_signals(
        decision,
        match status {
            IdentityCandidateStatus::Pending => "pending",
            IdentityCandidateStatus::Approved => "approved",
            IdentityCandidateStatus::Rejected => "rejected",
        },
        rechecked,
    );
    candidate.confidence = decision.confidence;
    candidate.status = status;
    candidate.proposed_by = "maintenance_worker".into();
    if status == IdentityCandidateStatus::Pending {
        candidate.decided_by = None;
        candidate.decided_at = None;
    } else {
        candidate.decided_by = Some("maintenance_worker".into());
        candidate.decided_at = Some(chrono::Utc::now());
    }
    Ok(())
}

fn decision_contributors(pair: &CandidatePair, context: &[BaseFields]) -> Vec<BaseFields> {
    [&pair.entity_a.base, &pair.entity_b.base]
        .into_iter()
        .chain(context)
        .cloned()
        .collect()
}

async fn observe_pair(
    planning: &PlanningTruthMaintenanceStorage,
    space: &str,
    pair: &CandidatePair,
) -> Result<(), PipelineError> {
    let current = current_pair(
        planning,
        space,
        &pair.entity_a.base.id,
        &pair.entity_b.base.id,
    )
    .await?
    .ok_or_else(|| PipelineError::InvalidState("identity pair no longer active".into()))?;
    if current.entity_a != pair.entity_a || current.entity_b != pair.entity_b {
        return Err(PipelineError::InvalidState(
            "identity pair changed before preparation".into(),
        ));
    }
    Ok(())
}

fn inherit_decision_context(
    base: &mut BaseFields,
    pair: &CandidatePair,
    context: &[BaseFields],
) -> Result<(), PipelineError> {
    for input in [&pair.entity_a.base, &pair.entity_b.base]
        .into_iter()
        .chain(context)
    {
        elephant_storage::public_mutation::inherit_public_envelope(base, input)?;
    }
    for id in [&pair.entity_a.base.id, &pair.entity_b.base.id] {
        let subject = Link::new(id.clone());
        if !base.subjects.contains(&subject) {
            base.subjects.push(subject);
        }
    }
    Ok(())
}

fn identity_candidate_signals(
    decision: &IdentityDecision,
    decision_label: &str,
    llm_rechecked: bool,
) -> serde_json::Value {
    serde_json::json!({
        "reasoning": decision.reasoning,
        "confidence": decision.confidence,
        "method": "maintenance_sweep",
        "decision": decision_label,
        "llm_rechecked": llm_rechecked,
    })
}

fn survivor_and_merged<'a>(
    pair: &'a CandidatePair,
    decision: &IdentityDecision,
) -> (&'a Entity, &'a Entity) {
    if pair.entity_b.display_name == decision.canonical_display_name {
        (&pair.entity_b, &pair.entity_a)
    } else {
        (&pair.entity_a, &pair.entity_b)
    }
}

async fn merge_entities_for_decision(
    planning: &PlanningTruthMaintenanceStorage,
    storage: &StorageClient,
    space: &str,
    pair: &CandidatePair,
    decision: &IdentityDecision,
    context: &[BaseFields],
) -> Result<(), PipelineError> {
    let (survivor, merged) = survivor_and_merged(pair, decision);
    // Preserve the sweep's retained risk/trust semantics. The shared planner
    // owns every other payload merge, reference rewrite, lineage and audit.
    let mut updated = survivor.clone();
    merge_sweep_risk_and_trust(&mut updated, merged);
    // The decision cannot expose more than any input sent to the model.
    for input in context {
        elephant_storage::public_mutation::inherit_public_envelope(&mut updated.base, input)?;
    }
    if updated != *survivor {
        updated.base.updated_at = chrono::Utc::now();
        planning.stage_inherited(Some(survivor.clone()), updated, context)?;
    }
    let registry = elephant_extensions::load_runtime_reference_registry_from_env()
        .map_err(|error| PipelineError::ConfigError(error.to_string()))?;
    let prior = planning.prepared_plan()?;
    // Newly approved decisions describe the exact pair observed by this sweep.
    // Keep those already validated records out of canonical reference rewrites:
    // rewriting them would erase the decision's original pair and authority.
    // Pending and existing stored candidates still use the shared merge planner.
    let mut merge_input = prior.clone();
    let mut decisions = Vec::new();
    merge_input.effects.retain(|effect| {
        let new_decision = effect.before.is_none()
            && matches!(
                effect.after.as_ref(),
                Some(elephant_storage::canonical_commit::CanonicalRecord::IdentityCandidate(candidate))
                    if candidate.status == IdentityCandidateStatus::Approved
            );
        if new_decision {
            decisions.push(effect.clone());
        }
        !new_decision
    });
    let prepared = elephant_identity::prepare_entity_merge_on_plan(
        storage,
        space,
        &survivor.base.id,
        &merged.base.id,
        elephant_storage::entity_merge_repository::MergeSelection::KeepA,
        "maintenance_worker",
        &registry,
        &merge_input,
    )
    .await
    .map_err(|error| {
        PipelineError::InvalidState(format!("identity merge preparation failed: {error}"))
    })?;
    let mut merged = prepared.into_prepared_plan()?;
    merged.effects.extend(decisions);
    planning.adopt_plan(&prior, merged)
}

fn merge_sweep_risk_and_trust(survivor: &mut Entity, merged: &Entity) {
    survivor.risk_level = match (survivor.risk_level, merged.risk_level) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (a, b) => a.or(b),
    };
    survivor.trust_level = match (survivor.trust_level, merged.trust_level) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (a, b) => a.or(b),
    };
    survivor.known_safe |= merged.known_safe;
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::float_cmp,
        unused_imports,
        dead_code,
        clippy::too_many_lines,
        clippy::cognitive_complexity
    )]

    use super::*;
    use elephant_types::records::assertion::{Assertion, EvidenceSpan, ResolutionStatus};
    use elephant_types::records::base::{Provenance, Security};
    use elephant_types::records::entity::{EntityStatus, EntityType};
    use std::str::FromStr;

    async fn fixture_storage(embedded: bool) -> StorageClient {
        let storage = if embedded {
            StorageClient::new_embedded().await
        } else {
            StorageClient::new_memory().await
        }
        .unwrap();
        if !embedded {
            storage
                .query_checked(include_str!(
                    "../../../migrations/sql/031_source_publication.surql"
                ))
                .await
                .unwrap();
        }
        let config: elephant_types::records::SpaceConfig = serde_json::from_value(serde_json::json!({
            "id":RecordId::new_v4(TableName::SpaceConfig),"space_id":"test_space",
            "created_at":chrono::Utc::now(),"updated_at":chrono::Utc::now(),"created_by":"identity-fixture",
            "security":{"policy_id":"default","level":0,"labels":[],"handling":[]},
            "provenance":{},"subjects":[],"subjects_state":"complete",
            "embedding_provider":"openai","embedding_model":"text-embedding-3-small","embedding_dims":1536
        })).unwrap();
        elephant_storage::SpaceConfigRepository::new(storage.clone())
            .create(config)
            .await
            .unwrap();
        storage
    }

    fn decision_for(entity: &Entity) -> IdentityDecision {
        IdentityDecision {
            same_entity: true,
            canonical_display_name: entity.display_name.clone(),
            confidence: 1.0,
            reasoning: "controlled identity fixture; no model acceptance".into(),
        }
    }

    async fn immutable_assertion_evidence(storage: &StorageClient) -> EvidenceSpan {
        use elephant_types::records::doc_revision::{RendererMetadata, RevisionStatus};
        use elephant_types::records::{Doc, DocRevision};
        use sha2::{Digest, Sha256};
        let text = "Riley is partner of Alex";
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::Doc),
            "test_space".into(),
            "identity-fixture".into(),
        );
        base.security.policy_id = "default".into();
        base.subjects_state = SubjectsState::Complete;
        let doc = elephant_storage::DocRepository::new(storage.clone())
            .create(Doc {
                base: base.clone(),
                title: "Independent assertion evidence".into(),
                text: text.into(),
                sidecar_text: None,
                source: None,
                media_attachments: vec![],
                rendition_key: "identity-assertion-fixture".into(),
                metadata: serde_json::json!({}),
            })
            .await
            .unwrap();
        base.id = RecordId::new_v4(TableName::DocRevision);
        let hash = format!("{:x}", Sha256::digest(text.as_bytes()));
        let revision = elephant_storage::DocRevisionRepository::new(storage.clone())
            .create(DocRevision {
                base,
                doc: Link::new(doc.base.id),
                text: text.into(),
                text_hash: hash.clone(),
                renderer: RendererMetadata {
                    name: "identity-fixture".into(),
                    version: "1".into(),
                    config_hash: None,
                },
                manifest: None,
                supersedes: None,
                status: RevisionStatus::Active,
                rendered_at: chrono::Utc::now(),
            })
            .await
            .unwrap();
        EvidenceSpan {
            revision: Link::new(revision.base.id),
            node: None,
            start: 0,
            end: u32::try_from(text.len()).unwrap(),
            quote_hash: hash,
        }
    }

    fn make_entity(display_name: &str, entity_type: EntityType) -> Entity {
        let id = RecordId::new_v4(TableName::Entity);
        let mut base = BaseFields::new(id, "test_space".to_string(), "test".to_string());
        base.security.policy_id = "default".into();
        base.subjects_state = SubjectsState::Complete;
        Entity {
            base,
            entity_type,
            display_name: display_name.to_string(),
            aliases: vec![],
            summary: None,
            handles: vec![],
            identity_keys: vec![display_name.to_lowercase()],
            risk_level: None,
            trust_level: None,
            known_safe: false,
            first_seen: None,
            last_seen: None,
            status: EntityStatus::Active,
            redirect_to: None,
            metadata: serde_json::Value::Object(serde_json::Map::new()),
        }
    }

    fn make_assertion(
        space_id: &str,
        subject_id: &RecordId,
        object_id: &RecordId,
        evidence: EvidenceSpan,
    ) -> Assertion {
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::Assertion),
            space_id.to_string(),
            "test".to_string(),
        );
        base.security.policy_id = "default".into();
        base.provenance = Provenance::default();
        base.subjects = vec![Link::new(subject_id.clone())];
        base.subjects_state = SubjectsState::Complete;
        Assertion {
            base,
            subject_text: "Jordan".to_string(),
            predicate_raw: "is partner of".to_string(),
            predicate_id: Some("is partner of".to_string()),
            object_text: Some("Alex".to_string()),
            object_literal: None,
            evidence,
            source_event_time: None,
            observed_at: chrono::Utc::now(),
            asserted_at: chrono::Utc::now(),
            assertion_key: format!("{subject_id}:is_partner_of:{object_id}"),
            resolution_status: ResolutionStatus::Resolved,
            resolution_method: Some("test".to_string()),
            resolution_confidence: Some(0.95),
            retry_state: Default::default(),
            subject_entity: Some(Link::new(subject_id.clone())),
            object_entity: Some(Link::new(object_id.clone())),
            confidence: 0.95,
            subject_entity_type: Some("person".to_string()),
            object_entity_type: Some("person".to_string()),
            significance: Some("high".to_string()),
            lifecycle_tier: "active".to_string(),
        }
    }

    #[test]
    fn test_identity_tokens_split_email_shaped_keys() {
        let mut entity = make_entity("alex@example.com", EntityType::new("person"));
        entity.identity_keys = vec!["email:alex@example.com".to_string()];

        let tokens = entity_identity_tokens(&entity);

        assert!(tokens.contains("alex"));
        assert!(tokens.contains("example"));
        assert!(!tokens.contains("com"));
    }

    #[tokio::test]
    async fn sweep_merge_preserves_metadata_and_risk_on_both_backends() {
        for embedded in [false, true] {
            let storage = fixture_storage(embedded).await;
            let repository = EntityRepository::new(storage.clone());
            let mut a = make_entity("Alice Chen", EntityType::new("person"));
            a.metadata =
                serde_json::json!({"angel_protectees":["child-b","child-a"],"note":"keep"});
            a.risk_level = Some(2.0);
            a.trust_level = Some(4.0);
            let a = repository.create(a).await.unwrap();
            let mut b = make_entity("Alice", EntityType::new("person"));
            b.metadata = serde_json::json!({"angel_protectees":["child-c","child-a"]});
            b.risk_level = Some(5.0);
            b.trust_level = Some(1.0);
            b.known_safe = true;
            let b = repository.create(b).await.unwrap();
            let pair = CandidatePair {
                entity_a: a.clone(),
                entity_b: b,
                shared_tokens: vec!["alice".into()],
                same_source_doc: false,
            };
            let planning =
                PlanningTruthMaintenanceStorage::new(storage.clone(), "test_space", None)
                    .await
                    .unwrap();
            let mut earlier = a.clone();
            earlier.aliases.push("earlier hygiene alias".into());
            planning.stage(Some(a.clone()), earlier).unwrap();
            let current = current_pair(&planning, "test_space", &a.base.id, &pair.entity_b.base.id)
                .await
                .unwrap()
                .unwrap();
            auto_merge(
                &planning,
                &storage,
                "test_space",
                &current,
                &decision_for(&a),
                &[],
            )
            .await
            .unwrap();
            assert_eq!(
                repository
                    .get("test_space", &a.base.id)
                    .await
                    .unwrap()
                    .unwrap(),
                a,
                "preparation cannot write"
            );
            let prepared = planning
                .get_entity("test_space", &a.base.id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                prepared.metadata["angel_protectees"],
                serde_json::json!(["child-a", "child-b", "child-c"])
            );
            assert_eq!(prepared.metadata["note"], "keep");
            assert!(prepared.aliases.contains(&"earlier hygiene alias".into()));
            assert_eq!(prepared.risk_level, Some(5.0));
            assert_eq!(prepared.trust_level, Some(4.0));
            assert!(prepared.known_safe);
            planning
                .commit(
                    "identity_fixture",
                    "metadata",
                    &serde_json::json!({"case":"metadata"}),
                    &serde_json::json!({"merged":1}),
                )
                .await
                .unwrap();
            assert_eq!(
                repository
                    .get("test_space", &a.base.id)
                    .await
                    .unwrap()
                    .unwrap(),
                prepared
            );
        }
    }

    #[test]
    fn invalid_decision_scores_cannot_stage_candidates() {
        for confidence in [f64::NAN, f64::INFINITY, -0.1, 1.1] {
            let mut decision = decision_for(&make_entity("Alice", EntityType::new("person")));
            decision.confidence = confidence;
            assert!(validate_decision(&decision).is_err());
        }
    }

    #[test]
    fn test_pending_recheck_budget_leaves_room_for_new_candidates() {
        assert_eq!(pending_recheck_budget(0), 0);
        assert_eq!(pending_recheck_budget(1), 1);
        assert_eq!(pending_recheck_budget(2), 1);
        assert_eq!(pending_recheck_budget(50), 25);
    }

    #[test]
    fn test_parse_identity_decision_text_accepts_fenced_json() {
        let text = r#"Here is the decision:
```json
{
  "same_entity": true,
  "canonical_display_name": "Vilma Jönsson",
  "confidence": 0.97,
  "reasoning": "The names and evidence refer to the same person."
}
```"#;

        let decision = parse_identity_decision_text(text).expect("parse fenced decision");

        assert!(decision.same_entity);
        assert_eq!(decision.canonical_display_name, "Vilma Jönsson");
        assert_eq!(decision.confidence, 0.97);
    }

    #[test]
    fn test_parse_identity_decision_value_accepts_structured_json() {
        let value = serde_json::json!({
            "same_entity": false,
            "canonical_display_name": "Alice Chen",
            "confidence": 0.22,
            "reasoning": "The dossiers describe different people."
        });

        let decision = parse_identity_decision_value(value).expect("parse structured decision");

        assert!(!decision.same_entity);
        assert_eq!(decision.canonical_display_name, "Alice Chen");
        assert_eq!(decision.confidence, 0.22);
    }

    #[test]
    fn test_parse_identity_decision_text_rejects_empty_output() {
        let error = parse_identity_decision_text("")
            .expect_err("empty model output should be a structured error");

        assert!(error.to_string().contains("empty text"));
        assert!(is_empty_identity_decision_output_error(&error));
    }

    #[test]
    fn test_identity_provider_params_default_openai_reasoning_effort_low() {
        let params = identity_provider_params_for_model("gpt-5.5", None)
            .expect("openai identity maintenance should default to bounded reasoning");

        assert_eq!(
            params
                .provider_tag
                .and_then(|tag| match tag {
                    meerkat_core::lifecycle::run_primitive::ProviderTag::OpenAi(tag) =>
                        tag.reasoning_effort,
                    _ => None,
                })
                .map(|effort| effort.as_legacy_str()),
            Some("low")
        );
    }

    #[test]
    fn test_identity_provider_params_explicit_effort_overrides_default() {
        let params = identity_provider_params_for_model("gpt-5.5", Some("high"))
            .expect("explicit effort should be preserved");

        assert_eq!(
            params
                .provider_tag
                .and_then(|tag| match tag {
                    meerkat_core::lifecycle::run_primitive::ProviderTag::OpenAi(tag) =>
                        tag.reasoning_effort,
                    _ => None,
                })
                .map(|effort| effort.as_legacy_str()),
            Some("high")
        );
    }

    #[test]
    fn test_identity_run_result_empty_output_preserves_extraction_error_reason() {
        let extraction_error = meerkat_core::types::ExtractionError {
            last_output: String::new(),
            attempts: 3,
            reason: "Invalid JSON after structured retries".to_string(),
        };
        let error = parse_identity_decision_run_result(None, "", Some(&extraction_error))
            .expect_err("empty run result should expose extraction diagnostics");
        let message = error.to_string();

        assert!(is_empty_identity_decision_output_error(&error));
        assert!(message.contains("empty text"));
        assert!(message.contains("Invalid JSON after structured retries"));
        assert!(message.contains("3 attempts"));
    }

    #[test]
    fn test_identity_empty_output_classifier_rejects_other_parse_errors() {
        let error = parse_identity_decision_text("not json")
            .expect_err("non-json output should be a parse error");

        assert!(!is_empty_identity_decision_output_error(&error));
    }

    #[test]
    fn test_format_dossier_prompt_includes_context() {
        let mut entity_a = make_entity("Alice Chen", EntityType::new("person"));
        entity_a.summary = Some("Product lead at Acme Corp.".to_string());
        entity_a.aliases = vec!["alice@example.com".to_string()];
        let pair = CandidatePair {
            entity_a,
            entity_b: make_entity("Alice", EntityType::new("person")),
            shared_tokens: vec!["alice".to_string()],
            same_source_doc: true,
        };

        let dossier = MergeDossier {
            pair,
            a_relationships: vec![RelSummary {
                direction: "outgoing".to_string(),
                rel_type: "works_at".to_string(),
                counterpart_name: "Acme Corp".to_string(),
            }],
            b_relationships: vec![],
            a_excerpts: vec!["Alice Chen joined the team".to_string()],
            b_excerpts: vec!["Alice will lead the project".to_string()],
            shared_doc_titles: vec!["doc_revision:abc".to_string()],
            a_sources: vec!["type=email; title=Welcome Alice".to_string()],
            b_sources: vec![],
        };

        let prompt = format_dossier_prompt(&dossier);

        assert!(prompt.contains("Alice Chen"));
        assert!(prompt.contains("Alice"));
        assert!(prompt.contains("Product lead at Acme Corp."));
        assert!(prompt.contains("alice@example.com"));
        assert!(prompt.contains("works_at"));
        assert!(prompt.contains("Acme Corp"));
        assert!(prompt.contains("Alice Chen joined the team"));
        assert!(prompt.contains("Welcome Alice"));
        assert!(prompt.contains("Alice will lead the project"));
        assert!(prompt.contains("SAME source document"));
    }

    #[tokio::test]
    async fn test_find_candidate_pairs_with_shared_tokens() {
        let storage = fixture_storage(false).await;

        let entity_repo = EntityRepository::new(storage.clone());

        // Create entities with overlapping identity keys
        let mut alice_chen = make_entity("Alice Chen", EntityType::new("person"));
        alice_chen.base.space_id = "test_space".to_string();
        alice_chen.identity_keys = vec!["alice chen".to_string()];
        entity_repo.create(alice_chen).await.unwrap();

        let mut alice = make_entity("Alice", EntityType::new("person"));
        alice.base.space_id = "test_space".to_string();
        alice.identity_keys = vec!["alice".to_string()];
        entity_repo.create(alice).await.unwrap();

        let mut bob = make_entity("Bob Martinez", EntityType::new("person"));
        bob.base.space_id = "test_space".to_string();
        bob.identity_keys = vec!["bob martinez".to_string()];
        entity_repo.create(bob).await.unwrap();

        let pairs = find_candidate_pairs(&storage, "test_space", 50)
            .await
            .unwrap();

        // "alice" token appears in both "alice chen" and "alice"
        assert_eq!(
            pairs.len(),
            1,
            "Should find one candidate pair (Alice/Alice Chen)"
        );

        let pair = &pairs[0];
        let names: HashSet<&str> = [
            pair.entity_a.display_name.as_str(),
            pair.entity_b.display_name.as_str(),
        ]
        .into_iter()
        .collect();
        assert!(names.contains("Alice"));
        assert!(names.contains("Alice Chen"));
        assert!(pair.shared_tokens.contains(&"alice".to_string()));
    }

    #[tokio::test]
    async fn test_find_candidate_pairs_skips_existing_candidate_log_entry() {
        let storage = fixture_storage(false).await;

        let entity_repo = EntityRepository::new(storage.clone());

        let mut alice_chen = make_entity("Alice Chen", EntityType::new("person"));
        alice_chen.base.space_id = "test_space".to_string();
        alice_chen.identity_keys = vec!["alice chen".to_string()];
        let alice_chen = entity_repo.create(alice_chen).await.unwrap();

        let mut alice = make_entity("Alice", EntityType::new("person"));
        alice.base.space_id = "test_space".to_string();
        alice.identity_keys = vec!["alice".to_string()];
        let alice = entity_repo.create(alice).await.unwrap();

        let pair = CandidatePair {
            entity_a: alice_chen,
            entity_b: alice,
            shared_tokens: vec!["alice".to_string()],
            same_source_doc: false,
        };
        let decision = IdentityDecision {
            same_entity: true,
            canonical_display_name: "Alice Chen".to_string(),
            confidence: 0.75,
            reasoning: "existing candidate".to_string(),
        };
        let planning = PlanningTruthMaintenanceStorage::new(storage.clone(), "test_space", None)
            .await
            .unwrap();
        observe_pair(&planning, "test_space", &pair).await.unwrap();
        stage_candidate(
            &planning,
            "test_space",
            &pair,
            &decision,
            IdentityCandidateStatus::Pending,
            &[],
        )
        .unwrap();

        let pairs = find_candidate_pairs_in_plan(&planning, &storage, "test_space", 50)
            .await
            .unwrap();

        assert!(pairs.is_empty());
    }

    #[tokio::test]
    async fn test_find_candidate_pairs_splits_email_identity_key_tokens() {
        let storage = fixture_storage(false).await;

        let entity_repo = EntityRepository::new(storage.clone());

        let mut alex = make_entity("Alex", EntityType::new("person"));
        alex.base.space_id = "test_space".to_string();
        alex.identity_keys = vec!["alex".to_string()];
        entity_repo.create(alex).await.unwrap();

        let mut email = make_entity("alex@example.test", EntityType::new("person"));
        email.base.space_id = "test_space".to_string();
        email.identity_keys = vec!["email:alex@example.test".to_string()];
        entity_repo.create(email).await.unwrap();

        let pairs = find_candidate_pairs(&storage, "test_space", 50)
            .await
            .unwrap();

        assert_eq!(pairs.len(), 1);
        assert!(pairs[0].shared_tokens.contains(&"alex".to_string()));
    }

    #[tokio::test]
    async fn test_find_candidate_pairs_prioritizes_email_localpart_matches() {
        let storage = fixture_storage(false).await;

        let entity_repo = EntityRepository::new(storage.clone());

        for idx in 0..80 {
            let mut entity = make_entity(&format!("kap {idx}"), EntityType::new("topic"));
            entity.base.space_id = "test_space".to_string();
            entity.identity_keys = vec![format!("kap {idx}")];
            entity_repo.create(entity).await.unwrap();
        }

        let mut person = make_entity("Alex", EntityType::new("person"));
        person.base.space_id = "test_space".to_string();
        person.identity_keys = vec!["alex".to_string()];
        entity_repo.create(person).await.unwrap();

        let mut email = make_entity("alex@example.test", EntityType::new("person"));
        email.base.space_id = "test_space".to_string();
        email.identity_keys = vec!["email:alex@example.test".to_string()];
        entity_repo.create(email).await.unwrap();

        let pairs = find_candidate_pairs(&storage, "test_space", 1)
            .await
            .unwrap();

        assert_eq!(pairs.len(), 1);
        let names: HashSet<&str> = [
            pairs[0].entity_a.display_name.as_str(),
            pairs[0].entity_b.display_name.as_str(),
        ]
        .into_iter()
        .collect();
        assert!(names.contains("Alex"));
        assert!(names.contains("alex@example.test"));
    }

    #[tokio::test]
    async fn test_find_candidate_pairs_prioritizes_person_name_subset_matches() {
        let storage = fixture_storage(false).await;

        let entity_repo = EntityRepository::new(storage.clone());

        for idx in 0..80 {
            let mut entity = make_entity(&format!("shared topic {idx}"), EntityType::new("topic"));
            entity.base.space_id = "test_space".to_string();
            entity.identity_keys = vec![format!("shared topic {idx}")];
            entity_repo.create(entity).await.unwrap();
        }

        let mut full_name = make_entity("Vilma Engström Jönsson", EntityType::new("person"));
        full_name.base.space_id = "test_space".to_string();
        full_name.identity_keys = vec!["vilma engström jönsson".to_string()];
        entity_repo.create(full_name).await.unwrap();

        let mut shorter_name = make_entity("Vilma Jönsson", EntityType::new("person"));
        shorter_name.base.space_id = "test_space".to_string();
        shorter_name.identity_keys = vec!["vilma jönsson".to_string()];
        entity_repo.create(shorter_name).await.unwrap();

        let pairs = find_candidate_pairs(&storage, "test_space", 1)
            .await
            .unwrap();

        assert_eq!(pairs.len(), 1);
        let names: HashSet<&str> = [
            pairs[0].entity_a.display_name.as_str(),
            pairs[0].entity_b.display_name.as_str(),
        ]
        .into_iter()
        .collect();
        assert!(names.contains("Vilma Engström Jönsson"));
        assert!(names.contains("Vilma Jönsson"));
    }

    #[tokio::test]
    async fn test_find_candidate_pairs_scans_past_first_entity_page() {
        let storage = fixture_storage(false).await;

        let entity_repo = EntityRepository::new(storage.clone());

        for idx in 0..1000 {
            let mut entity = make_entity(&format!("Topic {idx}"), EntityType::new("topic"));
            entity.base.id =
                RecordId::from_str(&format!("entity:00000000-0000-0000-0000-{idx:012x}")).unwrap();
            entity.identity_keys = vec![format!("topic-{idx}")];
            entity_repo.create(entity).await.unwrap();
        }

        let mut full_name = make_entity("Elena Rossi", EntityType::new("person"));
        full_name.base.id =
            RecordId::from_str("entity:ffffffff-ffff-ffff-ffff-fffffffffff0").unwrap();
        full_name.identity_keys = vec!["elena rossi".to_string()];
        entity_repo.create(full_name).await.unwrap();

        let mut short_name = make_entity("Elena", EntityType::new("person"));
        short_name.base.id =
            RecordId::from_str("entity:ffffffff-ffff-ffff-ffff-fffffffffff1").unwrap();
        short_name.identity_keys = vec!["elena".to_string()];
        entity_repo.create(short_name).await.unwrap();

        let pairs = find_candidate_pairs(&storage, "test_space", 50)
            .await
            .unwrap();

        assert_eq!(pairs.len(), 1);
        let names: HashSet<&str> = [
            pairs[0].entity_a.display_name.as_str(),
            pairs[0].entity_b.display_name.as_str(),
        ]
        .into_iter()
        .collect();
        assert!(names.contains("Elena"));
        assert!(names.contains("Elena Rossi"));
    }

    #[tokio::test]
    async fn test_indistinguishable_duplicates_merge_without_an_llm() {
        // Synthetic duplicate records must merge without a provider. A differing
        // summary or display name must still reach the identity agent.
        let a = make_entity("Morgan Sjöberg", EntityType::new("person"));
        let b = make_entity("Morgan Sjöberg", EntityType::new("person"));
        assert!(
            crate::scheduler::inline_promotion::candidates_are_indistinguishable(&[
                a.clone(),
                b.clone()
            ]),
            "two bare same-name person records carry nothing to tell them apart"
        );

        // A pair that DOES carry a distinction must not take the deterministic path.
        let mut c = make_entity("Morgan Sjöberg", EntityType::new("person"));
        c.summary = Some("caretaker at a different school".to_string());
        assert!(
            !crate::scheduler::inline_promotion::candidates_are_indistinguishable(&[
                a.clone(),
                c.clone()
            ]),
            "a summary is a real distinction and must reach the identity agent"
        );
        // BOTH ORDERS. The predicate checks the first candidate separately from the
        // rest, and my first version of this test only put the distinction on the
        // SECOND one - so a mutation deleting the first-candidate check passed. An
        // asymmetric predicate needs an asymmetric test.
        assert!(
            !crate::scheduler::inline_promotion::candidates_are_indistinguishable(&[c, a.clone()]),
            "a distinction on the FIRST candidate must count too"
        );

        // And a differing display name is a distinction even with the same key.
        let d = make_entity("M. Sjöberg", EntityType::new("person"));
        assert!(
            !crate::scheduler::inline_promotion::candidates_are_indistinguishable(&[a, d]),
            "different display names must reach the identity agent"
        );
    }

    #[tokio::test]
    async fn test_auto_merge_marks_merged_entity_and_preserves_identity_keys() {
        let storage = fixture_storage(false).await;

        let entity_repo = EntityRepository::new(storage.clone());

        let mut survivor = make_entity("Alex", EntityType::new("person"));
        survivor.base.space_id = "test_space".to_string();
        survivor.identity_keys = vec!["alex".to_string()];
        let survivor = entity_repo.create(survivor).await.unwrap();

        let mut merged = make_entity("alex@example.test", EntityType::new("person"));
        merged.base.space_id = "test_space".to_string();
        merged.identity_keys = vec!["alex@example.test".to_string()];
        let merged = entity_repo.create(merged).await.unwrap();

        let pair = CandidatePair {
            entity_a: survivor.clone(),
            entity_b: merged.clone(),
            shared_tokens: vec!["alex".to_string()],
            same_source_doc: false,
        };
        let decision = IdentityDecision {
            same_entity: true,
            canonical_display_name: survivor.display_name.clone(),
            confidence: 0.95,
            reasoning: "test merge".to_string(),
        };

        let planning = PlanningTruthMaintenanceStorage::new(storage.clone(), "test_space", None)
            .await
            .unwrap();
        auto_merge(&planning, &storage, "test_space", &pair, &decision, &[])
            .await
            .unwrap();
        assert_eq!(
            entity_repo
                .get("test_space", &merged.base.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            EntityStatus::Active
        );
        planning
            .commit(
                "identity_fixture",
                "merge",
                &serde_json::json!({"case":"merge"}),
                &serde_json::json!({"merged":1}),
            )
            .await
            .unwrap();

        let decisions = IdentityCandidateRepository::new(storage.clone())
            .list("test_space", 10, None)
            .await
            .unwrap();
        assert_eq!(decisions.len(), 1);
        let recorded = &decisions[0];
        assert_eq!(recorded.status, IdentityCandidateStatus::Approved);
        assert_eq!(recorded.entity_a.id(), &survivor.base.id);
        assert_eq!(recorded.entity_b.id(), &merged.base.id);
        assert!(recorded
            .base
            .subjects
            .contains(&Link::new(survivor.base.id.clone())));
        assert!(recorded
            .base
            .subjects
            .contains(&Link::new(merged.base.id.clone())));

        let updated_survivor = entity_repo
            .get("test_space", &survivor.base.id)
            .await
            .unwrap()
            .unwrap();
        assert!(
            updated_survivor
                .identity_keys
                .contains(&"alex@example.test".to_string()),
            "survivor should inherit merged identity keys"
        );
        assert!(
            updated_survivor
                .aliases
                .contains(&"alex@example.test".to_string()),
            "survivor should keep merged display name as alias"
        );

        let merged_tombstone = entity_repo
            .get("test_space", &merged.base.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(merged_tombstone.status, EntityStatus::Merged);
        assert_eq!(
            merged_tombstone.redirect_to.as_ref().map(Link::id),
            Some(&survivor.base.id)
        );
        assert!(merged_tombstone.base.deleted_at.is_none());
    }

    #[tokio::test]
    async fn test_auto_merge_rewrites_object_side_assertion_entities() {
        let storage = fixture_storage(false).await;

        let entity_repo = EntityRepository::new(storage.clone());
        let assertion_repo = AssertionRepository::new(storage.clone());

        let mut third_party = make_entity("Riley", EntityType::new("person"));
        third_party.base.space_id = "test_space".to_string();
        let third_party = entity_repo.create(third_party).await.unwrap();

        let mut survivor = make_entity("Alex Example", EntityType::new("person"));
        survivor.base.space_id = "test_space".to_string();
        let survivor = entity_repo.create(survivor).await.unwrap();

        let mut merged = make_entity("Alex", EntityType::new("person"));
        merged.base.space_id = "test_space".to_string();
        let merged = entity_repo.create(merged).await.unwrap();

        let evidence = immutable_assertion_evidence(&storage).await;
        let assertion = assertion_repo
            .create(make_assertion(
                "test_space",
                &third_party.base.id,
                &merged.base.id,
                evidence,
            ))
            .await
            .unwrap();

        let pair = CandidatePair {
            entity_a: survivor.clone(),
            entity_b: merged.clone(),
            shared_tokens: vec!["alex".to_string()],
            same_source_doc: true,
        };
        let decision = IdentityDecision {
            same_entity: true,
            canonical_display_name: survivor.display_name.clone(),
            confidence: 0.95,
            reasoning: "test merge".to_string(),
        };

        let planning = PlanningTruthMaintenanceStorage::new(storage.clone(), "test_space", None)
            .await
            .unwrap();
        auto_merge(&planning, &storage, "test_space", &pair, &decision, &[])
            .await
            .unwrap();
        assert_eq!(
            entity_repo
                .get("test_space", &merged.base.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            EntityStatus::Active
        );
        planning
            .commit(
                "identity_fixture",
                "merge",
                &serde_json::json!({"case":"merge"}),
                &serde_json::json!({"merged":1}),
            )
            .await
            .unwrap();

        let updated_assertion = assertion_repo
            .get("test_space", &assertion.base.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            updated_assertion.object_entity.as_ref().map(Link::id),
            Some(&survivor.base.id)
        );
        assert_eq!(
            updated_assertion.subject_entity.as_ref().map(Link::id),
            Some(&third_party.base.id)
        );
    }
    #[tokio::test]
    async fn sweep_preparation_reads_current_overlay_without_committing() {
        for embedded in [false, true] {
            let storage = fixture_storage(embedded).await;
            let repository = EntityRepository::new(storage.clone());
            let mut a = make_entity("Morgan Vale", EntityType::new("person"));
            a.identity_keys = vec!["morgan first".into()];
            let a = repository.create(a).await.unwrap();
            let mut b = make_entity("Morgan Vale", EntityType::new("person"));
            b.identity_keys = vec!["morgan second".into()];
            let b = repository.create(b).await.unwrap();
            let unrelated = repository
                .create(make_entity("Other Person", EntityType::new("person")))
                .await
                .unwrap();
            let planning =
                PlanningTruthMaintenanceStorage::new(storage.clone(), "test_space", None)
                    .await
                    .unwrap();
            let result = prepare_identity_sweep(
                &planning,
                &storage,
                "test_space",
                &MaintenanceConfig::default(),
            )
            .await
            .unwrap();
            assert_eq!(result.auto_merged, 1);
            assert_eq!(
                repository
                    .get("test_space", &a.base.id)
                    .await
                    .unwrap()
                    .unwrap(),
                a
            );
            assert_eq!(
                repository
                    .get("test_space", &b.base.id)
                    .await
                    .unwrap()
                    .unwrap(),
                b
            );
            assert!(IdentityCandidateRepository::new(storage.clone())
                .list("test_space", 10, None)
                .await
                .unwrap()
                .is_empty());
            let plan = planning.prepared_plan().unwrap();
            assert!(plan.effects.iter().any(|effect| matches!(
                effect.after,
                Some(elephant_storage::canonical_commit::CanonicalRecord::EntityMerge(_))
            )));
            assert!(
                !plan
                    .dependencies
                    .iter()
                    .any(|row| row.base().id == unrelated.base.id),
                "discovery alone must not attach unrelated source authority"
            );
            assert!(
                current_pair(&planning, "test_space", &a.base.id, &b.base.id)
                    .await
                    .unwrap()
                    .is_none(),
                "later pairs see the staged redirect"
            );
            let again = prepare_identity_sweep(
                &planning,
                &storage,
                "test_space",
                &MaintenanceConfig::default(),
            )
            .await
            .unwrap();
            assert_eq!(again.auto_merged, 0);
            planning
                .commit(
                    "identity_fixture",
                    "prepare",
                    &serde_json::json!({"case":"prepare"}),
                    &result,
                )
                .await
                .unwrap();
            assert_eq!(
                IdentityCandidateRepository::new(storage.clone())
                    .list("test_space", 10, None)
                    .await
                    .unwrap()
                    .len(),
                1
            );
        }
    }

    #[tokio::test]
    async fn chained_sweep_merges_keep_original_decisions_in_one_atomic_plan() {
        for embedded in [false, true] {
            let storage = fixture_storage(embedded).await;
            let repository = EntityRepository::new(storage.clone());
            let mut originals = Vec::new();
            for (index, suffix) in ["first", "second", "third"].iter().enumerate() {
                let mut entity = make_entity("Morgan Vale", EntityType::new("person"));
                // Fixed IDs ensure the candidate order is A/B, A/C, B/C and
                // the equal-name decision successively selects B then C.
                entity.base.id =
                    RecordId::new(TableName::Entity, uuid::Uuid::from_u128(index as u128 + 1));
                entity.identity_keys = vec![format!("morgan {suffix}")];
                entity.base.security.level = index as i32 + 1;
                entity.base.security.labels = vec![format!("original-{index}")];
                originals.push(repository.create(entity).await.unwrap());
            }
            let planning =
                PlanningTruthMaintenanceStorage::new(storage.clone(), "test_space", None)
                    .await
                    .unwrap();
            let result = prepare_identity_sweep(
                &planning,
                &storage,
                "test_space",
                &MaintenanceConfig::default(),
            )
            .await
            .unwrap();
            assert_eq!(result.auto_merged, 2);
            for original in &originals {
                assert_eq!(
                    repository
                        .get("test_space", &original.base.id)
                        .await
                        .unwrap()
                        .as_ref(),
                    Some(original),
                    "the whole chained sweep must remain uncommitted"
                );
            }
            planning
                .commit(
                    "identity_fixture",
                    "chained",
                    &serde_json::json!({"case":"three_entities"}),
                    &result,
                )
                .await
                .unwrap();
            let decisions = IdentityCandidateRepository::new(storage.clone())
                .list("test_space", 10, None)
                .await
                .unwrap();
            assert_eq!(decisions.len(), 2);
            for (winner, merged) in [(1, 0), (2, 1)] {
                let decision = decisions
                    .iter()
                    .find(|row| row.entity_b.id() == &originals[merged].base.id)
                    .expect("original merge decision retained");
                assert_eq!(decision.status, IdentityCandidateStatus::Approved);
                assert_eq!(decision.entity_a.id(), &originals[winner].base.id);
                assert_eq!(decision.base.security.level, winner as i32 + 1);
                for original_index in 0..=winner {
                    assert!(decision
                        .base
                        .security
                        .labels
                        .contains(&format!("original-{original_index}")));
                }
                for subject in [winner, merged] {
                    assert!(decision
                        .base
                        .subjects
                        .contains(&Link::new(originals[subject].base.id.clone())));
                }
            }
        }
    }

    #[tokio::test]
    async fn late_sweep_failure_leaves_prior_prepared_merge_uncommitted() {
        for embedded in [false, true] {
            let storage = fixture_storage(embedded).await;
            let repository = EntityRepository::new(storage.clone());
            let a = repository
                .create(make_entity("Alex", EntityType::new("person")))
                .await
                .unwrap();
            let b = repository
                .create(make_entity("Alex Example", EntityType::new("person")))
                .await
                .unwrap();
            let c = repository
                .create(make_entity("Pat", EntityType::new("person")))
                .await
                .unwrap();
            let mut incompatible = make_entity("Pat Example", EntityType::new("person"));
            incompatible.base.security.policy_id = "different-policy".into();
            let d = repository.create(incompatible).await.unwrap();
            let planning =
                PlanningTruthMaintenanceStorage::new(storage.clone(), "test_space", None)
                    .await
                    .unwrap();
            let pair = CandidatePair {
                entity_a: a.clone(),
                entity_b: b.clone(),
                shared_tokens: vec![],
                same_source_doc: false,
            };
            auto_merge(
                &planning,
                &storage,
                "test_space",
                &pair,
                &decision_for(&a),
                &[],
            )
            .await
            .unwrap();
            let late_pair = CandidatePair {
                entity_a: c.clone(),
                entity_b: d.clone(),
                shared_tokens: vec![],
                same_source_doc: false,
            };
            assert!(auto_merge(
                &planning,
                &storage,
                "test_space",
                &late_pair,
                &decision_for(&c),
                &[]
            )
            .await
            .is_err());
            // A caller propagating this error drops the whole overlay. Neither
            // the prior successful preparation nor the failed pair wrote live.
            drop(planning);
            for expected in [&a, &b, &c, &d] {
                assert_eq!(
                    repository
                        .get("test_space", &expected.base.id)
                        .await
                        .unwrap()
                        .as_ref(),
                    Some(expected)
                );
            }
            assert!(IdentityCandidateRepository::new(storage.clone())
                .list("test_space", 10, None)
                .await
                .unwrap()
                .is_empty());
            assert!(
                elephant_storage::EntityMergeRepository::new(storage.clone())
                    .list("test_space", 10, None)
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
    }

    #[tokio::test]
    async fn candidate_notes_inherit_both_entities_and_actual_context() {
        for embedded in [false, true] {
            let storage = fixture_storage(embedded).await;
            let mut a = make_entity("Alice", EntityType::new("person"));
            a.base.security.labels = vec!["home".into()];
            let mut b = make_entity("Alice Example", EntityType::new("person"));
            b.base.security.level = 3;
            b.base.security.handling = vec!["restricted".into()];
            b.base.subjects_state = SubjectsState::Partial;
            let mut evidence = make_entity("Context", EntityType::new("person")).base;
            evidence.security.labels = vec!["context-only".into()];
            let mut source_base = evidence.clone();
            source_base.id = RecordId::new_v4(TableName::Source);
            let source = SourceRepository::new(storage.clone())
                .create(elephant_types::records::Source::new(
                    source_base,
                    elephant_types::records::SourceType::Manual,
                    "context fixture".into(),
                ))
                .await
                .unwrap()
                .base
                .id;
            evidence.provenance.sources = vec![source.clone()];
            let repository = EntityRepository::new(storage.clone());
            let a = repository.create(a).await.unwrap();
            let b = repository.create(b).await.unwrap();
            let context_row = repository
                .create(Entity {
                    base: evidence.clone(),
                    ..make_entity("Context", EntityType::new("person"))
                })
                .await
                .unwrap();
            evidence = context_row.base.clone();
            let pair = CandidatePair {
                entity_a: a.clone(),
                entity_b: b.clone(),
                shared_tokens: vec![],
                same_source_doc: false,
            };
            let planning = PlanningTruthMaintenanceStorage::new(storage, "test_space", None)
                .await
                .unwrap();
            observe_pair(&planning, "test_space", &pair).await.unwrap();
            planning
                .get_entity("test_space", &context_row.base.id)
                .await
                .unwrap()
                .unwrap();
            stage_candidate(
                &planning,
                "test_space",
                &pair,
                &decision_for(&a),
                IdentityCandidateStatus::Pending,
                &[evidence],
            )
            .unwrap();
            let plan = planning.prepared_plan().unwrap();
            let candidate = plan
                .effects
                .iter()
                .find_map(|effect| {
                    effect
                        .after
                        .as_ref()
                        .and_then(IdentityCandidate::from_record)
                })
                .unwrap();
            assert_eq!(candidate.base.security.level, 3);
            assert!(candidate.base.security.labels.contains(&"home".into()));
            assert!(candidate
                .base
                .security
                .labels
                .contains(&"context-only".into()));
            assert_eq!(candidate.base.security.handling, ["restricted"]);
            assert_eq!(candidate.base.subjects_state, SubjectsState::Partial);
            assert_eq!(candidate.base.provenance.sources, [source]);
            assert!(candidate.base.subjects.contains(&Link::new(a.base.id)));
            assert!(candidate.base.subjects.contains(&Link::new(b.base.id)));
            // Preparation inherits real observed rows. This test does not
            // submit the unprocessed source as a completed publication.
        }
    }

    #[tokio::test]
    async fn dossiers_use_staged_relationships_entities_and_assertions() {
        use elephant_types::records::rel::{ClaimStatus, Rel};
        for embedded in [false, true] {
            let storage = fixture_storage(embedded).await;
            let repository = EntityRepository::new(storage.clone());
            let a = repository
                .create(make_entity("Alice", EntityType::new("person")))
                .await
                .unwrap();
            let b = repository
                .create(make_entity("Alice Example", EntityType::new("person")))
                .await
                .unwrap();
            let counterpart = repository
                .create(make_entity("Old Counterpart", EntityType::new("person")))
                .await
                .unwrap();
            let mut rel_base = make_entity("unused", EntityType::new("person")).base;
            rel_base.id = RecordId::new_v4(TableName::Rel);
            let rel = RelRepository::new(storage.clone())
                .create(Rel {
                    base: rel_base,
                    subject: Link::new(a.base.id.clone()),
                    object: Link::new(counterpart.base.id.clone()),
                    rel_type: "old relation".into(),
                    status: ClaimStatus::Approved,
                    confidence: 1.0,
                    validity_window: None,
                    evidence: vec![],
                    temporal_audit: None,
                })
                .await
                .unwrap();
            let evidence = immutable_assertion_evidence(&storage).await;
            let planning =
                PlanningTruthMaintenanceStorage::new(storage.clone(), "test_space", None)
                    .await
                    .unwrap();
            let mut updated_counterpart = counterpart.clone();
            updated_counterpart.display_name = "Current Counterpart".into();
            planning
                .stage(Some(counterpart), updated_counterpart)
                .unwrap();
            let mut updated_rel = rel.clone();
            updated_rel.rel_type = "current relation".into();
            planning.stage(Some(rel), updated_rel).unwrap();
            let mut assertion = make_assertion("test_space", &a.base.id, &b.base.id, evidence);
            assertion.subject_text = "Riley".into();
            planning.stage(None, assertion).unwrap();
            let pair = CandidatePair {
                entity_a: a,
                entity_b: b,
                shared_tokens: vec![],
                same_source_doc: false,
            };
            let (dossier, context) =
                build_dossier_in_plan(&planning, &storage, "test_space", &pair)
                    .await
                    .unwrap();
            assert_eq!(dossier.a_relationships.len(), 1);
            assert_eq!(dossier.a_relationships[0].rel_type, "current relation");
            assert_eq!(
                dossier.a_relationships[0].counterpart_name,
                "Current Counterpart"
            );
            assert_eq!(dossier.a_excerpts, ["Riley is partner of Alex"]);
            assert_eq!(dossier.b_excerpts, ["Riley is partner of Alex"]);
            assert!(dossier.pair.same_source_doc);
            assert!(context
                .iter()
                .any(|base| base.id.table() == TableName::Assertion));
            assert!(AssertionRepository::new(storage)
                .list("test_space", 10, None)
                .await
                .unwrap()
                .is_empty());
        }
    }
}
