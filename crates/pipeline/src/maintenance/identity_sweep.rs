//! Identity detection sweep.
//!
//! Finds potential duplicate entities using cheap heuristics (shared identity
//! key tokens, same source document), builds rich context dossiers, and uses
//! an LLM to decide whether to merge.
//!
//! The LLM gets pre-built context with both entities' relationships, source
//! text excerpts, and shared provenance — enabling single-call decisions
//! without expensive multi-turn tool use.

use std::collections::{HashMap, HashSet};

use elephant_storage::{
    AssertionRepository, EntityRepository, IdentityCandidateRepository, RelRepository, Repository,
    SearchCriteria, SearchRepository, SourceRepository, StorageClient,
};
use elephant_types::ids::TableName;
use elephant_types::records::base::{BaseFields, SubjectsState};
use elephant_types::records::entity::{Entity, EntityStatus};
use elephant_types::records::rel::Rel;
use elephant_types::records::{IdentityCandidate, IdentityCandidateStatus};
use elephant_types::{Link, RecordId};
use serde::Deserialize;
use tracing::{debug, info, warn};

use super::{IdentitySweepResult, MaintenanceConfig};
use crate::error::PipelineError;
use crate::scheduler::inline_promotion::{
    angel_protectees_from_metadata, union_angel_protectees_into_metadata,
};

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
    let mut result = IdentitySweepResult::default();

    let rechecked = recheck_pending_identity_candidates(
        storage,
        space_id,
        pending_recheck_budget(config.max_candidates_per_sweep),
        config,
    )
    .await?;
    result.candidates_evaluated += rechecked.candidates_evaluated;
    result.auto_merged += rechecked.auto_merged;
    result.proposed += rechecked.proposed;
    result.rejected += rechecked.rejected;
    result.skipped += rechecked.skipped;

    let remaining_budget = config
        .max_candidates_per_sweep
        .saturating_sub(result.candidates_evaluated);
    if remaining_budget == 0 {
        return Ok(result);
    }

    // Step 1: Find candidate pairs
    let candidates = find_candidate_pairs(storage, space_id, remaining_budget).await?;

    if candidates.is_empty() {
        debug!("No identity candidates found for space {space_id}");
        return Ok(result);
    }

    info!(
        "Found {} identity candidate pairs for space {space_id}",
        candidates.len()
    );

    // Step 2: Build dossiers and evaluate each pair
    for pair in candidates {
        result.candidates_evaluated += 1;

        let dossier = match build_dossier(storage, space_id, &pair).await {
            Ok(d) => d,
            Err(e) => {
                warn!(
                    "Failed to build dossier for {} / {}: {e}",
                    pair.entity_a.display_name, pair.entity_b.display_name
                );
                continue;
            }
        };

        // Step 3: Format dossier as prompt text
        let prompt = format_dossier_prompt(&dossier);

        // Identical records with no distinguishing summary, alias or handle can
        // be merged deterministically. Records with meaningful differences still
        // require the context-rich identity agent.
        if crate::scheduler::inline_promotion::candidates_are_indistinguishable(&[
            pair.entity_a.clone(),
            pair.entity_b.clone(),
        ]) {
            let decision = IdentityDecision {
                same_entity: true,
                confidence: 1.0,
                canonical_display_name: pair.entity_a.display_name.clone(),
                reasoning: "identical display name and type with no summary, alias or \
                            handle to distinguish them; merged deterministically without \
                            consulting the identity agent"
                    .to_string(),
            };
            match auto_merge(storage, space_id, &pair, &decision).await {
                Ok(()) => {
                    result.auto_merged += 1;
                    info!(
                        "Merged indistinguishable duplicate '{}' into '{}' without an LLM call",
                        pair.entity_b.display_name, pair.entity_a.display_name,
                    );
                }
                Err(e) => {
                    warn!(
                        "Failed to merge indistinguishable duplicates {} / {}: {e}",
                        pair.entity_a.display_name, pair.entity_b.display_name
                    );
                }
            }
            continue;
        }

        // Step 4: Call the LLM for the identity decision. Candidate discovery
        // is intentionally broad and cheap, but merge decisions must come from
        // the context-rich identity agent rather than local string heuristics.
        let decision = match evaluate_with_llm(storage, config, &prompt).await {
            Ok(d) => d,
            Err(e) => {
                debug!(
                    "Skipping identity candidate {} / {} because LLM evaluation is unavailable: {e}",
                    pair.entity_a.display_name, pair.entity_b.display_name
                );
                result.skipped += 1;
                continue;
            }
        };

        // Step 5: Act on decision. Only auto-merge above the configured
        // threshold; lower-confidence positive decisions remain auditable
        // pending candidates for later review.
        if decision.same_entity {
            if decision.confidence >= config.auto_merge_threshold {
                match auto_merge(storage, space_id, &pair, &decision).await {
                    Ok(()) => {
                        result.auto_merged += 1;
                        info!(
                            "Merged '{}' into '{}' (confidence: {:.2}, reason: {})",
                            pair.entity_b.display_name,
                            decision.canonical_display_name,
                            decision.confidence,
                            decision.reasoning,
                        );
                    }
                    Err(e) => {
                        warn!(
                            "Failed to merge {} / {}: {e}",
                            pair.entity_a.display_name, pair.entity_b.display_name
                        );
                    }
                }
            } else {
                if let Err(e) = log_pending(storage, space_id, &pair, &decision).await {
                    warn!(
                        "Failed to log pending candidate for {} / {}: {e}",
                        pair.entity_a.display_name, pair.entity_b.display_name
                    );
                }
                result.proposed += 1;
            }
        } else {
            // Log rejection as a candidate record so the decision is discoverable
            // and can be revisited if new evidence arrives later.
            if let Err(e) = log_rejection(storage, space_id, &pair, &decision).await {
                warn!(
                    "Failed to log rejection for {} / {}: {e}",
                    pair.entity_a.display_name, pair.entity_b.display_name
                );
            }
            result.rejected += 1;
        }
    }

    Ok(result)
}

async fn recheck_pending_identity_candidates(
    storage: &StorageClient,
    space_id: &str,
    max_candidates: usize,
    config: &MaintenanceConfig,
) -> Result<IdentitySweepResult, PipelineError> {
    let candidate_repo = IdentityCandidateRepository::new(storage.clone());
    let pending = candidate_repo
        .get_pending(space_id, max_candidates)
        .await
        .map_err(PipelineError::StorageError)?;
    let mut result = IdentitySweepResult::default();

    for candidate in pending {
        let pair = match candidate_pair_from_identity_candidate(storage, space_id, &candidate).await
        {
            Ok(Some(pair)) => pair,
            Ok(None) => {
                result.skipped += 1;
                continue;
            }
            Err(e) => {
                warn!(
                    "Failed to load pending identity candidate {}: {e}",
                    candidate.base.id
                );
                result.skipped += 1;
                continue;
            }
        };

        let dossier = match build_dossier(storage, space_id, &pair).await {
            Ok(dossier) => dossier,
            Err(e) => {
                warn!(
                    "Failed to build pending identity dossier for {} / {}: {e}",
                    pair.entity_a.display_name, pair.entity_b.display_name
                );
                result.skipped += 1;
                continue;
            }
        };
        // Candidate discovery skips previously logged pairs. Recheck pending
        // indistinguishable pairs here so those duplicates can converge too.
        if crate::scheduler::inline_promotion::candidates_are_indistinguishable(&[
            pair.entity_a.clone(),
            pair.entity_b.clone(),
        ]) {
            let decision = IdentityDecision {
                same_entity: true,
                confidence: 1.0,
                canonical_display_name: pair.entity_a.display_name.clone(),
                reasoning: "identical display name and type with no summary, alias or \
                            handle to distinguish them; merged deterministically on \
                            recheck without consulting the identity agent"
                    .to_string(),
            };
            result.candidates_evaluated += 1;
            match approve_existing_candidate_and_merge(
                storage, space_id, candidate, &pair, &decision,
            )
            .await
            {
                Ok(()) => {
                    result.auto_merged += 1;
                    info!(
                        "Merged pending indistinguishable duplicate '{}' into '{}' without an LLM call",
                        pair.entity_b.display_name, pair.entity_a.display_name,
                    );
                }
                Err(e) => {
                    warn!(
                        "Failed to merge pending indistinguishable duplicates {} / {}: {e}",
                        pair.entity_a.display_name, pair.entity_b.display_name
                    );
                }
            }
            continue;
        }

        let prompt = format_dossier_prompt(&dossier);
        let decision = match evaluate_with_llm(storage, config, &prompt).await {
            Ok(decision) => decision,
            Err(e) => {
                debug!(
                    "Skipping pending identity recheck for {} / {} because LLM evaluation is unavailable: {e}",
                    pair.entity_a.display_name, pair.entity_b.display_name
                );
                result.skipped += 1;
                continue;
            }
        };

        result.candidates_evaluated += 1;
        if decision.same_entity {
            if decision.confidence >= config.auto_merge_threshold {
                match approve_existing_candidate_and_merge(
                    storage, space_id, candidate, &pair, &decision,
                )
                .await
                {
                    Ok(()) => {
                        result.auto_merged += 1;
                        info!(
                            "Merged pending identity candidate '{}' / '{}' (confidence: {:.2}, reason: {})",
                            pair.entity_a.display_name,
                            pair.entity_b.display_name,
                            decision.confidence,
                            decision.reasoning,
                        );
                    }
                    Err(e) => {
                        warn!(
                            "Failed to merge pending identity candidate {} / {}: {e}",
                            pair.entity_a.display_name, pair.entity_b.display_name
                        );
                    }
                }
            } else {
                if let Err(e) = update_existing_candidate_decision(
                    storage,
                    space_id,
                    candidate,
                    &pair,
                    &decision,
                    IdentityCandidateStatus::Pending,
                )
                .await
                {
                    warn!(
                        "Failed to update pending identity candidate for {} / {}: {e}",
                        pair.entity_a.display_name, pair.entity_b.display_name
                    );
                }
                result.proposed += 1;
            }
        } else {
            if let Err(e) = update_existing_candidate_decision(
                storage,
                space_id,
                candidate,
                &pair,
                &decision,
                IdentityCandidateStatus::Rejected,
            )
            .await
            {
                warn!(
                    "Failed to reject pending identity candidate for {} / {}: {e}",
                    pair.entity_a.display_name, pair.entity_b.display_name
                );
            }
            result.rejected += 1;
        }
    }

    Ok(result)
}

async fn candidate_pair_from_identity_candidate(
    storage: &StorageClient,
    space_id: &str,
    candidate: &IdentityCandidate,
) -> Result<Option<CandidatePair>, PipelineError> {
    let entity_repo = EntityRepository::new(storage.clone());
    let entity_a = entity_repo
        .get(space_id, candidate.entity_a.id())
        .await
        .map_err(PipelineError::StorageError)?;
    let entity_b = entity_repo
        .get(space_id, candidate.entity_b.id())
        .await
        .map_err(PipelineError::StorageError)?;
    let (Some(entity_a), Some(entity_b)) = (entity_a, entity_b) else {
        return Ok(None);
    };
    if entity_a.status == EntityStatus::Merged || entity_b.status == EntityStatus::Merged {
        return Ok(None);
    }

    let shared_tokens: Vec<String> = entity_identity_tokens(&entity_a)
        .intersection(&entity_identity_tokens(&entity_b))
        .cloned()
        .collect();

    Ok(Some(CandidatePair {
        entity_a,
        entity_b,
        shared_tokens,
        same_source_doc: false,
    }))
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
    let entity_repo = EntityRepository::new(storage.clone());
    let candidate_repo = IdentityCandidateRepository::new(storage.clone());

    let entities = load_active_entities_for_identity_sweep(&entity_repo, space_id).await?;

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
                let shared: Vec<String> = entity_tokens[a]
                    .intersection(&entity_tokens[b])
                    .cloned()
                    .collect();

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
            })
    });

    let existing_candidate_pairs = candidate_repo
        .existing_pair_keys(space_id)
        .await
        .unwrap_or_default();
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

async fn load_active_entities_for_identity_sweep(
    entity_repo: &EntityRepository,
    space_id: &str,
) -> Result<Vec<Entity>, PipelineError> {
    let page_limit = 1000;
    let mut entities = Vec::new();
    let mut cursor: Option<String> = None;
    let mut seen_cursors = HashSet::new();

    loop {
        let mut search = SearchCriteria::new()
            .with_space_id(space_id)
            .with_limit(page_limit);
        if let Some(current_cursor) = cursor {
            search = search.with_cursor(current_cursor);
        }

        let page = entity_repo.search(&search).await?;
        entities.extend(page.results);

        if !page.has_more {
            break;
        }

        let Some(next_cursor) = page.cursor else {
            return Err(PipelineError::StorageError(
                elephant_storage::StorageError::QueryError(
                    "Entity pagination reported more identity-sweep rows without a cursor"
                        .to_string(),
                ),
            ));
        };
        if !seen_cursors.insert(next_cursor.clone()) {
            return Err(PipelineError::StorageError(
                elephant_storage::StorageError::QueryError(
                    "Entity pagination cursor repeated during identity sweep".to_string(),
                ),
            ));
        }
        cursor = Some(next_cursor);
    }

    Ok(entities)
}

fn candidate_log_pair_key(pair: &CandidatePair) -> (String, String) {
    let a = pair.entity_a.base.id.to_string();
    let b = pair.entity_b.base.id.to_string();
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
    let rel_repo = RelRepository::new(storage.clone());
    let assertion_repo = AssertionRepository::new(storage.clone());
    let entity_repo = EntityRepository::new(storage.clone());
    let source_repo = SourceRepository::new(storage.clone());

    // Load relationships for both entities
    let a_out = rel_repo
        .find_outgoing(space_id, &pair.entity_a.base.id)
        .await
        .unwrap_or_default();
    let a_in = rel_repo
        .find_incoming(space_id, &pair.entity_a.base.id)
        .await
        .unwrap_or_default();
    let b_out = rel_repo
        .find_outgoing(space_id, &pair.entity_b.base.id)
        .await
        .unwrap_or_default();
    let b_in = rel_repo
        .find_incoming(space_id, &pair.entity_b.base.id)
        .await
        .unwrap_or_default();

    // Convert to summaries with counterpart names
    let a_relationships = build_rel_summaries(&entity_repo, space_id, &a_out, &a_in).await;
    let b_relationships = build_rel_summaries(&entity_repo, space_id, &b_out, &b_in).await;

    // Load assertion context for both entities via subjects field
    let a_assertions =
        load_assertions_for_entity(&assertion_repo, space_id, &pair.entity_a.base.id, 10).await;
    let b_assertions =
        load_assertions_for_entity(&assertion_repo, space_id, &pair.entity_b.base.id, 10).await;

    // Build excerpts from assertion subject/predicate/object text
    let a_excerpts: Vec<String> = a_assertions
        .iter()
        .map(|a| format_assertion_excerpt(a))
        .collect();
    let b_excerpts: Vec<String> = b_assertions
        .iter()
        .map(|a| format_assertion_excerpt(a))
        .collect();

    // Check for shared source documents via assertion evidence
    let a_revisions: HashSet<String> = a_assertions
        .iter()
        .map(|a| a.evidence.revision.id().to_string())
        .collect();
    let b_revisions: HashSet<String> = b_assertions
        .iter()
        .map(|a| a.evidence.revision.id().to_string())
        .collect();
    let shared_revisions: Vec<&String> = a_revisions.intersection(&b_revisions).collect();
    let same_source_doc = !shared_revisions.is_empty();
    let a_sources = load_source_context_for_entity(&source_repo, space_id, &pair.entity_a, 8).await;
    let b_sources = load_source_context_for_entity(&source_repo, space_id, &pair.entity_b, 8).await;

    Ok(MergeDossier {
        pair: CandidatePair {
            same_source_doc,
            ..pair.clone()
        },
        a_relationships,
        b_relationships,
        a_excerpts,
        b_excerpts,
        shared_doc_titles: shared_revisions.into_iter().cloned().collect(),
        a_sources,
        b_sources,
    })
}

/// Converts relationships to summaries with counterpart display names.
async fn build_rel_summaries(
    entity_repo: &EntityRepository,
    space_id: &str,
    outgoing: &[Rel],
    incoming: &[Rel],
) -> Vec<RelSummary> {
    let mut summaries = Vec::new();

    for rel in outgoing {
        let name = resolve_entity_name(entity_repo, space_id, rel.object.id()).await;
        summaries.push(RelSummary {
            direction: "outgoing".to_string(),
            rel_type: rel.rel_type.clone(),
            counterpart_name: name,
        });
    }

    for rel in incoming {
        let name = resolve_entity_name(entity_repo, space_id, rel.subject.id()).await;
        summaries.push(RelSummary {
            direction: "incoming".to_string(),
            rel_type: rel.rel_type.clone(),
            counterpart_name: name,
        });
    }

    summaries
}

/// Loads assertions that mention an entity as a resolved subject or object.
///
/// Best-effort — returns empty on error.
async fn load_assertions_for_entity(
    assertion_repo: &AssertionRepository,
    space_id: &str,
    entity_id: &RecordId,
    limit: usize,
) -> Vec<elephant_types::records::assertion::Assertion> {
    assertion_repo
        .find_by_resolved_entity(space_id, entity_id, limit)
        .await
        .unwrap_or_default()
}

async fn load_source_context_for_entity(
    source_repo: &SourceRepository,
    space_id: &str,
    entity: &Entity,
    limit: usize,
) -> Vec<String> {
    let mut contexts = Vec::new();
    let mut seen = HashSet::new();
    for source_id in &entity.base.provenance.sources {
        if contexts.len() >= limit {
            break;
        }
        if !seen.insert(source_id.to_string()) {
            continue;
        }
        let Ok(Some(source)) = source_repo.get(space_id, source_id).await else {
            continue;
        };
        contexts.push(format_source_context(&source));
    }
    contexts
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

/// Resolves an entity ID to its display name, or returns the ID string as fallback.
async fn resolve_entity_name(
    entity_repo: &EntityRepository,
    space_id: &str,
    entity_id: &RecordId,
) -> String {
    entity_repo
        .get(space_id, entity_id)
        .await
        .ok()
        .flatten()
        .map_or_else(|| entity_id.to_string(), |e| e.display_name)
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

    let mut agent = factory
        .build_agent(build, &meerkat_config)
        .await
        .map_err(|e| PipelineError::ExtractionError(format!("Identity agent build failed: {e}")))?;

    let run_result = agent
        .run(ContentInput::Text(prompt.to_string()))
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

            let mut fallback_agent = factory
                .build_agent(fallback_build, &meerkat_config)
                .await
                .map_err(|build_err| {
                    PipelineError::ExtractionError(format!(
                        "{err}; identity fallback agent build failed: {build_err}"
                    ))
                })?;

            let fallback_result = fallback_agent
                .run(ContentInput::Text(identity_text_fallback_prompt(prompt)))
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
    _config: &MaintenanceConfig,
    _prompt: &str,
) -> Result<IdentityDecision, PipelineError> {
    Err(PipelineError::ConfigError(
        "meerkat-extraction feature not enabled".to_string(),
    ))
}

/// Auto-merges two entities by creating an approved IdentityCandidate
/// and calling the merge logic.
async fn auto_merge(
    storage: &StorageClient,
    space_id: &str,
    pair: &CandidatePair,
    decision: &IdentityDecision,
) -> Result<(), PipelineError> {
    let candidate_repo = IdentityCandidateRepository::new(storage.clone());

    let (survivor, merged) = survivor_and_merged(pair, decision);

    // Create an approved identity candidate for audit trail
    let candidate_id = RecordId::new_v4(TableName::IdentityCandidate);
    let mut base = BaseFields::new(
        candidate_id,
        space_id.to_string(),
        "maintenance_worker".to_string(),
    );
    base.subjects = vec![
        Link::new(survivor.base.id.clone()),
        Link::new(merged.base.id.clone()),
    ];
    base.subjects_state = SubjectsState::Complete;

    let candidate = elephant_types::records::IdentityCandidate {
        base,
        entity_a: Link::new(survivor.base.id.clone()),
        entity_b: Link::new(merged.base.id.clone()),
        signals: identity_candidate_signals(decision, "approved", false),
        confidence: decision.confidence,
        status: IdentityCandidateStatus::Approved,
        proposed_by: "maintenance_worker".to_string(),
        decided_by: Some("maintenance_worker".to_string()),
        decided_at: Some(chrono::Utc::now()),
    };

    candidate_repo
        .create(candidate)
        .await
        .map_err(PipelineError::StorageError)?;

    merge_entities_for_decision(storage, space_id, pair, decision).await
}

async fn approve_existing_candidate_and_merge(
    storage: &StorageClient,
    space_id: &str,
    candidate: IdentityCandidate,
    pair: &CandidatePair,
    decision: &IdentityDecision,
) -> Result<(), PipelineError> {
    update_existing_candidate_decision(
        storage,
        space_id,
        candidate,
        pair,
        decision,
        IdentityCandidateStatus::Approved,
    )
    .await?;
    merge_entities_for_decision(storage, space_id, pair, decision).await
}

async fn update_existing_candidate_decision(
    storage: &StorageClient,
    space_id: &str,
    mut candidate: IdentityCandidate,
    pair: &CandidatePair,
    decision: &IdentityDecision,
    status: IdentityCandidateStatus,
) -> Result<(), PipelineError> {
    let candidate_repo = IdentityCandidateRepository::new(storage.clone());
    let candidate_id = candidate.base.id.clone();
    let (entity_a, entity_b) = if status == IdentityCandidateStatus::Approved {
        survivor_and_merged(pair, decision)
    } else {
        (&pair.entity_a, &pair.entity_b)
    };
    candidate.base.subjects = vec![
        Link::new(entity_a.base.id.clone()),
        Link::new(entity_b.base.id.clone()),
    ];
    candidate.base.subjects_state = SubjectsState::Complete;
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
        true,
    );
    candidate.confidence = decision.confidence;
    candidate.status = status;
    candidate.proposed_by = "maintenance_worker".to_string();
    if status == IdentityCandidateStatus::Pending {
        candidate.decided_by = None;
        candidate.decided_at = None;
    } else {
        candidate.decided_by = Some("maintenance_worker".to_string());
        candidate.decided_at = Some(chrono::Utc::now());
    }
    candidate_repo
        .update(space_id, &candidate_id, candidate)
        .await
        .map_err(PipelineError::StorageError)?;

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
    storage: &StorageClient,
    space_id: &str,
    pair: &CandidatePair,
    decision: &IdentityDecision,
) -> Result<(), PipelineError> {
    let entity_repo = EntityRepository::new(storage.clone());
    let rel_repo = RelRepository::new(storage.clone());
    let assertion_repo = AssertionRepository::new(storage.clone());
    let (survivor, merged) = survivor_and_merged(pair, decision);

    let mut updated_survivor = survivor.clone();
    merge_entity_fields(&mut updated_survivor, merged);
    updated_survivor.base.updated_at = chrono::Utc::now();
    entity_repo
        .update(space_id, &survivor.base.id, updated_survivor)
        .await
        .map_err(PipelineError::StorageError)?;

    // Repoint all relationships from merged entity to survivor
    let merged_out = rel_repo
        .find_outgoing(space_id, &merged.base.id)
        .await
        .map_err(PipelineError::StorageError)?;
    let merged_in = rel_repo
        .find_incoming(space_id, &merged.base.id)
        .await
        .map_err(PipelineError::StorageError)?;

    for mut rel in merged_out {
        let rel_id = rel.base.id.clone();
        rel.subject = Link::new(survivor.base.id.clone());
        rel_repo
            .update(space_id, &rel_id, rel)
            .await
            .map_err(PipelineError::StorageError)?;
    }

    for mut rel in merged_in {
        let rel_id = rel.base.id.clone();
        rel.object = Link::new(survivor.base.id.clone());
        rel_repo
            .update(space_id, &rel_id, rel)
            .await
            .map_err(PipelineError::StorageError)?;
    }

    // Repoint resolved assertions as well as relationships. Otherwise search
    // results can continue to expose the deleted entity as a canonical subject.
    for mut assertion in
        load_assertions_for_entity(&assertion_repo, space_id, &merged.base.id, 1000).await
    {
        let assertion_id = assertion.base.id.clone();
        if assertion
            .subject_entity
            .as_ref()
            .is_some_and(|link| link.id() == &merged.base.id)
        {
            assertion.subject_entity = Some(Link::new(survivor.base.id.clone()));
        }
        if assertion
            .object_entity
            .as_ref()
            .is_some_and(|link| link.id() == &merged.base.id)
        {
            assertion.object_entity = Some(Link::new(survivor.base.id.clone()));
        }
        for subject in &mut assertion.base.subjects {
            if subject.id() == &merged.base.id {
                *subject = Link::new(survivor.base.id.clone());
            }
        }
        assertion_repo
            .update(space_id, &assertion_id, assertion)
            .await
            .map_err(PipelineError::StorageError)?;
    }

    let mut merged_tombstone = merged.clone();
    merged_tombstone.status = EntityStatus::Merged;
    merged_tombstone.redirect_to = Some(Link::new(survivor.base.id.clone()));
    merged_tombstone.base.updated_at = chrono::Utc::now();
    entity_repo
        .update(space_id, &merged.base.id, merged_tombstone)
        .await
        .map_err(PipelineError::StorageError)?;

    Ok(())
}

fn merge_entity_fields(survivor: &mut Entity, merged: &Entity) {
    for alias in merged
        .aliases
        .iter()
        .chain(std::iter::once(&merged.display_name))
    {
        survivor.add_alias(alias.clone());
    }

    survivor
        .identity_keys
        .extend(merged.identity_keys.iter().cloned());
    survivor.identity_keys.sort();
    survivor.identity_keys.dedup();

    for handle in &merged.handles {
        let Some(existing) = survivor
            .handles
            .iter_mut()
            .find(|h| h.platform == handle.platform && h.username == handle.username)
        else {
            survivor.handles.push(handle.clone());
            continue;
        };

        let existing_conf = existing.confidence.unwrap_or(0.0);
        let incoming_conf = handle.confidence.unwrap_or(0.0);
        if incoming_conf > existing_conf {
            existing.confidence = handle.confidence;
            existing.verified = handle.verified;
        }

        existing.first_seen = match (existing.first_seen, handle.first_seen) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (None, Some(b)) => Some(b),
            (Some(a), None) => Some(a),
            (None, None) => None,
        };
        existing.last_seen = match (existing.last_seen, handle.last_seen) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (None, Some(b)) => Some(b),
            (Some(a), None) => Some(a),
            (None, None) => None,
        };

        existing.evidence.extend(handle.evidence.iter().cloned());
        let mut seen: HashSet<RecordId> = HashSet::new();
        existing.evidence.retain(|id| seen.insert(id.clone()));
    }

    // `merge_json_values` lets the merged entity's array values overwrite the
    // survivor's; angel_protectees attribution must union across both instead.
    let mut protectees = angel_protectees_from_metadata(&survivor.metadata);
    protectees.extend(angel_protectees_from_metadata(&merged.metadata));
    survivor.metadata = merge_json_values(&survivor.metadata, &merged.metadata);
    union_angel_protectees_into_metadata(&mut survivor.metadata, &protectees);

    survivor.risk_level = match (survivor.risk_level, merged.risk_level) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    };
    survivor.trust_level = match (survivor.trust_level, merged.trust_level) {
        (Some(a), Some(b)) => Some(a.max(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    };
    survivor.known_safe = survivor.known_safe || merged.known_safe;
}

fn merge_json_values(a: &serde_json::Value, b: &serde_json::Value) -> serde_json::Value {
    match (a, b) {
        (serde_json::Value::Object(a_obj), serde_json::Value::Object(b_obj)) => {
            let mut merged = a_obj.clone();
            for (key, b_val) in b_obj {
                match merged.get(key) {
                    Some(a_val) => {
                        merged.insert(key.clone(), merge_json_values(a_val, b_val));
                    }
                    None => {
                        merged.insert(key.clone(), b_val.clone());
                    }
                }
            }
            serde_json::Value::Object(merged)
        }
        (serde_json::Value::Null, value) => value.clone(),
        (value, serde_json::Value::Null) => value.clone(),
        (_a, b) => b.clone(),
    }
}

/// Logs a positive-but-below-threshold decision as a pending candidate.
async fn log_pending(
    storage: &StorageClient,
    space_id: &str,
    pair: &CandidatePair,
    decision: &IdentityDecision,
) -> Result<(), PipelineError> {
    let candidate_repo = IdentityCandidateRepository::new(storage.clone());

    let candidate_id = RecordId::new_v4(TableName::IdentityCandidate);
    let mut base = BaseFields::new(
        candidate_id,
        space_id.to_string(),
        "maintenance_worker".to_string(),
    );
    base.subjects = vec![
        Link::new(pair.entity_a.base.id.clone()),
        Link::new(pair.entity_b.base.id.clone()),
    ];
    base.subjects_state = SubjectsState::Complete;

    let candidate = elephant_types::records::IdentityCandidate {
        base,
        entity_a: Link::new(pair.entity_a.base.id.clone()),
        entity_b: Link::new(pair.entity_b.base.id.clone()),
        signals: identity_candidate_signals(decision, "pending", false),
        confidence: decision.confidence,
        status: IdentityCandidateStatus::Pending,
        proposed_by: "maintenance_worker".to_string(),
        decided_by: None,
        decided_at: None,
    };

    candidate_repo
        .create(candidate)
        .await
        .map_err(PipelineError::StorageError)?;

    Ok(())
}

/// Logs a rejection decision as an identity candidate for audit trail.
/// This makes the decision discoverable and revisitable if new evidence arrives.
async fn log_rejection(
    storage: &StorageClient,
    space_id: &str,
    pair: &CandidatePair,
    decision: &IdentityDecision,
) -> Result<(), PipelineError> {
    let candidate_repo = IdentityCandidateRepository::new(storage.clone());

    let candidate_id = RecordId::new_v4(TableName::IdentityCandidate);
    let mut base = BaseFields::new(
        candidate_id,
        space_id.to_string(),
        "maintenance_worker".to_string(),
    );
    base.subjects = vec![
        Link::new(pair.entity_a.base.id.clone()),
        Link::new(pair.entity_b.base.id.clone()),
    ];
    base.subjects_state = SubjectsState::Complete;

    let candidate = elephant_types::records::IdentityCandidate {
        base,
        entity_a: Link::new(pair.entity_a.base.id.clone()),
        entity_b: Link::new(pair.entity_b.base.id.clone()),
        signals: identity_candidate_signals(decision, "rejected", false),
        confidence: decision.confidence,
        status: IdentityCandidateStatus::Rejected,
        proposed_by: "maintenance_worker".to_string(),
        decided_by: Some("maintenance_worker".to_string()),
        decided_at: Some(chrono::Utc::now()),
    };

    candidate_repo
        .create(candidate)
        .await
        .map_err(PipelineError::StorageError)?;

    Ok(())
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

    fn make_entity(display_name: &str, entity_type: EntityType) -> Entity {
        let id = RecordId::new_v4(TableName::Entity);
        let base = BaseFields::new(id, "test_space".to_string(), "test".to_string());
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

    fn make_assertion(space_id: &str, subject_id: &RecordId, object_id: &RecordId) -> Assertion {
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::Assertion),
            space_id.to_string(),
            "test".to_string(),
        );
        base.security = Security::default();
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
            evidence: EvidenceSpan {
                revision: Link::new(RecordId::new_v4(TableName::DocRevision)),
                node: None,
                start: 0,
                end: 10,
                quote_hash: "sha256:test".to_string(),
            },
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

    #[test]
    fn test_merge_entity_fields_unions_angel_protectees() {
        let mut survivor = make_entity("Alice Chen", EntityType::new("person"));
        survivor.metadata =
            serde_json::json!({ "angel_protectees": ["child-b", "child-a"], "note": "keep" });
        let mut merged = make_entity("Alice", EntityType::new("person"));
        merged.metadata = serde_json::json!({ "angel_protectees": ["child-c", "child-a"] });

        merge_entity_fields(&mut survivor, &merged);

        // Union of both sides, deduped and sorted — not the merged entity's
        // array overwriting the survivor's.
        assert_eq!(
            survivor.metadata["angel_protectees"],
            serde_json::json!(["child-a", "child-b", "child-c"])
        );
        assert_eq!(survivor.metadata["note"], serde_json::json!("keep"));
    }

    #[test]
    fn test_merge_entity_fields_keeps_survivor_protectees_when_merged_has_none() {
        let mut survivor = make_entity("Alice Chen", EntityType::new("person"));
        survivor.metadata = serde_json::json!({ "angel_protectees": ["child-a"] });
        let merged = make_entity("Alice", EntityType::new("person"));

        merge_entity_fields(&mut survivor, &merged);

        assert_eq!(
            survivor.metadata["angel_protectees"],
            serde_json::json!(["child-a"])
        );
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
        let storage = StorageClient::new_memory().await.unwrap();
        storage
            .query("DEFINE TABLE entity SCHEMALESS; DEFINE TABLE identity_candidate SCHEMALESS;")
            .await
            .unwrap();

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
        let storage = StorageClient::new_memory().await.unwrap();
        storage
            .query("DEFINE TABLE entity SCHEMALESS; DEFINE TABLE identity_candidate SCHEMALESS;")
            .await
            .unwrap();

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
        log_pending(&storage, "test_space", &pair, &decision)
            .await
            .unwrap();

        let pairs = find_candidate_pairs(&storage, "test_space", 50)
            .await
            .unwrap();

        assert!(pairs.is_empty());
    }

    #[tokio::test]
    async fn test_find_candidate_pairs_splits_email_identity_key_tokens() {
        let storage = StorageClient::new_memory().await.unwrap();
        storage
            .query("DEFINE TABLE entity SCHEMALESS; DEFINE TABLE identity_candidate SCHEMALESS;")
            .await
            .unwrap();

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
        let storage = StorageClient::new_memory().await.unwrap();
        storage
            .query("DEFINE TABLE entity SCHEMALESS; DEFINE TABLE identity_candidate SCHEMALESS;")
            .await
            .unwrap();

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
        let storage = StorageClient::new_memory().await.unwrap();
        storage
            .query("DEFINE TABLE entity SCHEMALESS; DEFINE TABLE identity_candidate SCHEMALESS;")
            .await
            .unwrap();

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
        let storage = StorageClient::new_memory().await.unwrap();
        storage
            .query("DEFINE TABLE entity SCHEMALESS; DEFINE TABLE identity_candidate SCHEMALESS;")
            .await
            .unwrap();

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
        let storage = StorageClient::new_memory().await.unwrap();
        storage
            .query("DEFINE TABLE entity SCHEMALESS; DEFINE TABLE identity_candidate SCHEMALESS; DEFINE TABLE rel SCHEMALESS; DEFINE TABLE assertion SCHEMALESS;")
            .await
            .unwrap();

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

        auto_merge(&storage, "test_space", &pair, &decision)
            .await
            .unwrap();

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
        let storage = StorageClient::new_memory().await.unwrap();
        storage
            .query("DEFINE TABLE entity SCHEMALESS; DEFINE TABLE identity_candidate SCHEMALESS; DEFINE TABLE rel SCHEMALESS; DEFINE TABLE assertion SCHEMALESS;")
            .await
            .unwrap();

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

        let assertion = assertion_repo
            .create(make_assertion(
                "test_space",
                &third_party.base.id,
                &merged.base.id,
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

        auto_merge(&storage, "test_space", &pair, &decision)
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
}
