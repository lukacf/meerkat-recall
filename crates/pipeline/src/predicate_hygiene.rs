//! Predicate hygiene worker for semantic deduplication.
//!
//! This module implements a periodic background worker that detects semantically
//! equivalent predicates and proposes alias mappings for review. It uses
//! embedding-based cosine similarity to group predicates that likely mean the
//! same thing (e.g., "works_at" and "employed_by").
//!
//! Embedding similarity is a useful discovery signal, but it is not a safe
//! truth-canonicalization authority. By default this worker only creates
//! `WorkArtifact` proposals for review; active aliases must come from explicit
//! predicate governance.

use elephant_embeddings::EmbeddingProvider;
use elephant_storage::predicate_alias_repository::PredicateAliasRepository;
use elephant_storage::predicate_registry_repository::PredicateRegistryRepository;
use elephant_storage::work_artifact_repository::WorkArtifactRepository;
use elephant_storage::{Repository, StorageClient};
use elephant_types::ids::TableName;
use elephant_types::records::base::BaseFields;
use elephant_types::records::predicate::{
    AliasScope, AliasStatus, AliasType, PredicateAlias, PredicateRegistry,
};
use elephant_types::records::work::{ArtifactStatus, WorkArtifact};
use elephant_types::{Link, RecordId};
use std::collections::BTreeMap;
use tracing::{debug, info};

use crate::error::PipelineError;

/// Configuration for predicate hygiene sweeps.
#[derive(Debug, Clone)]
pub struct PredicateHygieneConfig {
    /// Similarity threshold for auto-aliasing (default 0.9).
    pub auto_alias_threshold: f64,
    /// Whether high-similarity pairs may be promoted directly to active aliases.
    ///
    /// Defaults to false because embedding similarity alone can be high for
    /// related-but-opposite or domain-adjacent predicates.
    pub auto_alias_enabled: bool,
    /// Similarity threshold for proposals (default 0.8).
    pub proposal_threshold: f64,
    /// Maximum predicates to process per sweep.
    pub max_predicates: usize,
}

impl Default for PredicateHygieneConfig {
    fn default() -> Self {
        Self {
            auto_alias_threshold: 0.9,
            auto_alias_enabled: false,
            proposal_threshold: 0.8,
            max_predicates: 1000,
        }
    }
}

impl PredicateHygieneConfig {
    /// Creates a new configuration with default values.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the auto-alias threshold.
    #[must_use]
    pub const fn with_auto_alias_threshold(mut self, threshold: f64) -> Self {
        self.auto_alias_threshold = threshold;
        self
    }

    /// Enables or disables direct active alias creation.
    #[must_use]
    pub const fn with_auto_alias_enabled(mut self, enabled: bool) -> Self {
        self.auto_alias_enabled = enabled;
        self
    }

    /// Sets the proposal threshold.
    #[must_use]
    pub const fn with_proposal_threshold(mut self, threshold: f64) -> Self {
        self.proposal_threshold = threshold;
        self
    }

    /// Sets the maximum predicates per sweep.
    #[must_use]
    pub const fn with_max_predicates(mut self, max: usize) -> Self {
        self.max_predicates = max;
        self
    }
}

/// Result of a predicate hygiene sweep.
#[derive(Debug, Clone)]
pub struct HygieneResult {
    /// Number of aliases auto-created.
    pub aliases_created: usize,
    /// Number of proposals created for review.
    pub proposals_created: usize,
    /// Number of predicates scanned.
    pub predicates_scanned: usize,
}

/// Artifact type identifier for hygiene proposals.
pub const HYGIENE_PROPOSAL_ARTIFACT_TYPE: &str = "predicate_hygiene_proposal";

/// Computes cosine similarity between two embedding vectors.
///
/// Returns 0.0 if either vector has zero norm.
fn cosine_similarity(a: &[f32], b: &[f32]) -> f64 {
    let dot: f64 = a
        .iter()
        .zip(b)
        .map(|(x, y)| f64::from(*x) * f64::from(*y))
        .sum();
    let norm_a: f64 = a
        .iter()
        .map(|x| f64::from(*x) * f64::from(*x))
        .sum::<f64>()
        .sqrt();
    let norm_b: f64 = b
        .iter()
        .map(|x| f64::from(*x) * f64::from(*x))
        .sum::<f64>()
        .sqrt();
    if norm_a == 0.0 || norm_b == 0.0 {
        return 0.0;
    }
    dot / (norm_a * norm_b)
}

/// A group of predicates that are semantically similar.
#[derive(Debug, Clone)]
struct SimilarityGroup {
    /// The canonical predicate (most used, or first alphabetically).
    canonical: String,
    /// The members of the group (excluding canonical) with their similarity scores.
    members: Vec<(String, f64)>,
}

/// Groups predicates by pairwise cosine similarity.
///
/// Returns groups where all members have similarity above `threshold` with
/// the canonical predicate. The canonical is chosen by highest `usage_count`,
/// breaking ties alphabetically.
fn group_predicates_by_similarity(
    predicates: &[PredicateRegistry],
    embeddings: &[Vec<f32>],
    threshold: f64,
) -> Vec<SimilarityGroup> {
    let n = predicates.len();
    // Track which predicates have been assigned to a group
    let mut assigned = vec![false; n];
    let mut groups = Vec::new();

    // Sort indices by usage_count descending, then alphabetically by predicate_id
    let mut indices: Vec<usize> = (0..n).collect();
    indices.sort_by(|&a, &b| {
        predicates[b]
            .usage_count
            .cmp(&predicates[a].usage_count)
            .then_with(|| predicates[a].predicate_id.cmp(&predicates[b].predicate_id))
    });

    for &i in &indices {
        if assigned[i] {
            continue;
        }
        assigned[i] = true;

        let mut members = Vec::new();
        for &j in &indices {
            if assigned[j] || i == j {
                continue;
            }
            let sim = cosine_similarity(&embeddings[i], &embeddings[j]);
            if sim >= threshold {
                members.push((predicates[j].predicate_id.clone(), sim));
                assigned[j] = true;
            }
        }

        if !members.is_empty() {
            groups.push(SimilarityGroup {
                canonical: predicates[i].predicate_id.clone(),
                members,
            });
        }
    }

    groups
}

/// Returns the text to embed for a predicate.
///
/// Uses the description if available, otherwise falls back to the predicate_id.
fn predicate_embed_text(predicate: &PredicateRegistry) -> String {
    predicate
        .description
        .as_deref()
        .unwrap_or(&predicate.predicate_id)
        .to_string()
}

async fn active_hygiene_proposal_pairs(
    artifact_repo: &WorkArtifactRepository,
    space_id: &str,
) -> Result<std::collections::HashSet<(String, String)>, PipelineError> {
    let artifacts = artifact_repo.list(space_id, 1000, None).await?;
    Ok(artifacts
        .into_iter()
        .filter(|artifact| {
            artifact.artifact_type == HYGIENE_PROPOSAL_ARTIFACT_TYPE
                && artifact.status != ArtifactStatus::Rejected
        })
        .filter_map(|artifact| {
            let alias = artifact
                .data
                .get("alias_predicate_id")
                .and_then(serde_json::Value::as_str)?;
            let canonical = artifact
                .data
                .get("canonical_predicate_id")
                .and_then(serde_json::Value::as_str)?;
            Some((alias.to_string(), canonical.to_string()))
        })
        .collect())
}

async fn create_hygiene_proposal(
    artifact_repo: &WorkArtifactRepository,
    space_id: &str,
    alias_predicate_id: &str,
    canonical_predicate_id: &str,
    similarity: f64,
    reason: &str,
) -> Result<(), PipelineError> {
    let artifact_id = RecordId::new_v4(TableName::WorkArtifact);
    let base = BaseFields::new(
        artifact_id,
        space_id.to_string(),
        "predicate_hygiene_worker".to_string(),
    );

    // We create a work artifact without a work_run link since this is a
    // background worker, not a pipeline work item. Use a sentinel link.
    let sentinel_run_id = RecordId::new_v4(TableName::WorkRun);

    let proposal_data = serde_json::json!({
        "type": "predicate_hygiene_proposal",
        "alias_predicate_id": alias_predicate_id,
        "canonical_predicate_id": canonical_predicate_id,
        "similarity": similarity,
        "reason": reason,
    });

    let artifact = WorkArtifact {
        base,
        work_run: Link::new(sentinel_run_id),
        artifact_type: HYGIENE_PROPOSAL_ARTIFACT_TYPE.to_string(),
        data: proposal_data,
        status: ArtifactStatus::Draft,
        validation_errors: None,
        validated_at: None,
        committed_at: None,
    };

    artifact_repo.create(artifact).await?;
    Ok(())
}

async fn deactivate_active_hygiene_aliases(
    alias_repo: &PredicateAliasRepository,
    artifact_repo: &WorkArtifactRepository,
    space_id: &str,
) -> Result<(usize, usize), PipelineError> {
    let mut existing_proposals = active_hygiene_proposal_pairs(artifact_repo, space_id).await?;
    let mut cursor: Option<RecordId> = None;
    let mut aliases_deactivated = 0;
    let mut proposals_created = 0;

    loop {
        let aliases = alias_repo.list(space_id, 1000, cursor.as_ref()).await?;
        if aliases.is_empty() {
            break;
        }
        cursor = aliases.last().map(|alias| alias.base.id.clone());

        for mut alias in aliases {
            if alias.status != AliasStatus::Active
                || alias.source_plugin.as_deref() != Some("predicate_hygiene")
            {
                continue;
            }

            let proposal_key = (alias.alias.clone(), alias.predicate_id.clone());
            if existing_proposals.insert(proposal_key) {
                create_hygiene_proposal(
                    artifact_repo,
                    space_id,
                    &alias.alias,
                    &alias.predicate_id,
                    f64::from(alias.confidence),
                    "active predicate_hygiene alias deactivated for review",
                )
                .await?;
                proposals_created += 1;
            }

            let alias_id = alias.base.id.clone();
            let alias_name = alias.alias.clone();
            let canonical_predicate_id = alias.predicate_id.clone();
            alias.status = AliasStatus::Deprecated;
            alias_repo.update(space_id, &alias_id, alias).await?;
            aliases_deactivated += 1;
            info!(
                "Deprecated predicate_hygiene active alias '{}' -> '{}' for review",
                alias_name, canonical_predicate_id
            );
        }
    }

    Ok((aliases_deactivated, proposals_created))
}

fn predicate_syntax_key(predicate_id: &str) -> Option<String> {
    let normalized = predicate_id
        .trim()
        .to_ascii_lowercase()
        .replace(['_', '-'], " ");
    let mut tokens: Vec<&str> = normalized
        .split_whitespace()
        .filter(|token| !matches!(*token, "a" | "an" | "the"))
        .collect();
    if tokens.first() == Some(&"is") {
        tokens.remove(0);
    }
    if tokens.len() < 2 {
        return None;
    }
    Some(tokens.join(" "))
}

fn predicate_syntax_canonical_score(predicate_id: &str) -> (u8, u8, usize) {
    let has_separator_noise = predicate_id.contains('_') || predicate_id.contains('-');
    let starts_with_is = predicate_id
        .trim()
        .to_ascii_lowercase()
        .split_whitespace()
        .next()
        == Some("is");
    (
        u8::from(!has_separator_noise),
        u8::from(!starts_with_is),
        usize::MAX - predicate_id.len(),
    )
}

async fn create_syntax_equivalence_aliases(
    alias_repo: &PredicateAliasRepository,
    space_id: &str,
    predicates: &[PredicateRegistry],
) -> Result<usize, PipelineError> {
    let mut groups: BTreeMap<String, Vec<&PredicateRegistry>> = BTreeMap::new();
    for predicate in predicates {
        if predicate.status != "active" {
            continue;
        }
        let Some(key) = predicate_syntax_key(&predicate.predicate_id) else {
            continue;
        };
        groups.entry(key).or_default().push(predicate);
    }

    let mut aliases_created = 0;
    for group in groups.values() {
        if group.len() < 2 {
            continue;
        }
        let Some(canonical) = group.iter().max_by(|left, right| {
            left.usage_count
                .cmp(&right.usage_count)
                .then_with(|| {
                    predicate_syntax_canonical_score(&left.predicate_id)
                        .cmp(&predicate_syntax_canonical_score(&right.predicate_id))
                })
                .then_with(|| right.predicate_id.cmp(&left.predicate_id))
        }) else {
            continue;
        };

        for predicate in group {
            if predicate.predicate_id == canonical.predicate_id {
                continue;
            }
            if alias_repo
                .find_active_by_raw(space_id, &predicate.predicate_id)
                .await?
                .is_some()
            {
                continue;
            }

            let alias_id = RecordId::new_v4(TableName::PredicateAlias);
            let base = BaseFields::new(
                alias_id,
                space_id.to_string(),
                "predicate_syntax_hygiene".to_string(),
            );
            let alias = PredicateAlias {
                base,
                alias: predicate.predicate_id.clone(),
                alias_type: AliasType::RawString,
                source_plugin: Some("predicate_syntax".to_string()),
                confidence: 1.0,
                status: AliasStatus::Active,
                scope: AliasScope::Global,
                predicate_id: canonical.predicate_id.clone(),
                confidence_version: 1,
                usage_count: 0,
            };
            alias_repo.create(alias).await?;
            aliases_created += 1;
            info!(
                "Syntax-aliased predicate '{}' -> '{}'",
                predicate.predicate_id, canonical.predicate_id
            );
        }
    }

    Ok(aliases_created)
}

/// Runs a predicate hygiene sweep for the given space.
///
/// Loads active predicates, embeds their descriptions, computes pairwise
/// similarity, and creates proposal artifacts for operator review. Direct
/// active alias creation is disabled by default and must be explicitly enabled.
///
/// # Errors
///
/// Returns `PipelineError` if storage or embedding operations fail.
pub async fn execute_predicate_hygiene_sweep(
    storage: &StorageClient,
    space_id: &str,
    config: &PredicateHygieneConfig,
    embedding_provider: &dyn EmbeddingProvider,
) -> Result<HygieneResult, PipelineError> {
    let registry_repo = PredicateRegistryRepository::new(storage.clone());
    let alias_repo = PredicateAliasRepository::new(storage.clone());
    let artifact_repo = WorkArtifactRepository::new(storage.clone());

    // 1. Load all active predicates for the space
    let predicates = registry_repo
        .list(space_id, config.max_predicates, None)
        .await?;

    let predicates_scanned = predicates.len();
    let mut aliases_created = 0;
    let mut proposals_created = 0;

    if !config.auto_alias_enabled {
        let (aliases_deactivated, review_proposals_created) =
            deactivate_active_hygiene_aliases(&alias_repo, &artifact_repo, space_id).await?;
        if aliases_deactivated > 0 {
            info!(
                "Deprecated {aliases_deactivated} active predicate_hygiene aliases; \
                 created {review_proposals_created} review proposals"
            );
        }
        proposals_created += review_proposals_created;
    }

    aliases_created +=
        create_syntax_equivalence_aliases(&alias_repo, space_id, &predicates).await?;

    if predicates_scanned < 2 {
        debug!("Fewer than 2 predicates in space {space_id}, skipping hygiene sweep");
        return Ok(HygieneResult {
            aliases_created,
            proposals_created,
            predicates_scanned,
        });
    }

    // 2. Embed predicate descriptions (or names if no description)
    let texts: Vec<String> = predicates.iter().map(predicate_embed_text).collect();
    let embeddings = embedding_provider.embed(&texts).await?;

    if embeddings.len() != predicates.len() {
        return Err(PipelineError::ExtractionError(format!(
            "Embedding count mismatch: expected {}, got {}",
            predicates.len(),
            embeddings.len()
        )));
    }

    // 3. Group predicates above proposal_threshold
    let groups =
        group_predicates_by_similarity(&predicates, &embeddings, config.proposal_threshold);

    let mut existing_proposals = active_hygiene_proposal_pairs(&artifact_repo, space_id).await?;

    // 4. For each group, create aliases or proposals
    for group in &groups {
        for (member_predicate_id, similarity) in &group.members {
            // Check if an alias already exists
            let existing = alias_repo
                .find_active_by_raw(space_id, member_predicate_id)
                .await?;
            if existing.is_some() {
                debug!("Alias already exists for '{member_predicate_id}' -> skipping");
                continue;
            }

            if config.auto_alias_enabled && *similarity >= config.auto_alias_threshold {
                // Auto-create alias
                let alias_id = RecordId::new_v4(TableName::PredicateAlias);
                let base = BaseFields::new(
                    alias_id,
                    space_id.to_string(),
                    "predicate_hygiene_worker".to_string(),
                );

                #[allow(clippy::cast_possible_truncation)]
                let confidence = *similarity as f32;

                let alias = PredicateAlias {
                    base,
                    alias: member_predicate_id.clone(),
                    alias_type: AliasType::RawString,
                    source_plugin: Some("predicate_hygiene".to_string()),
                    confidence,
                    status: AliasStatus::Active,
                    scope: AliasScope::Global,
                    predicate_id: group.canonical.clone(),
                    confidence_version: 1,
                    usage_count: 0,
                };

                alias_repo.create(alias).await?;
                aliases_created += 1;
                info!(
                    "Auto-aliased '{}' -> '{}' (similarity: {:.3})",
                    member_predicate_id, group.canonical, similarity
                );
            } else {
                let proposal_key = (member_predicate_id.clone(), group.canonical.clone());
                if existing_proposals.contains(&proposal_key) {
                    debug!(
                        "Predicate hygiene proposal already exists for '{}' -> '{}' -> skipping",
                        member_predicate_id, group.canonical
                    );
                    continue;
                }
                create_hygiene_proposal(
                    &artifact_repo,
                    space_id,
                    member_predicate_id,
                    &group.canonical,
                    *similarity,
                    "embedding similarity review",
                )
                .await?;
                existing_proposals.insert(proposal_key);
                proposals_created += 1;
                info!(
                    "Proposed alias '{}' -> '{}' (similarity: {:.3})",
                    member_predicate_id, group.canonical, similarity
                );
            }
        }
    }

    info!(
        "Predicate hygiene sweep complete: scanned={predicates_scanned}, \
         aliases_created={aliases_created}, proposals_created={proposals_created}"
    );

    Ok(HygieneResult {
        aliases_created,
        proposals_created,
        predicates_scanned,
    })
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::float_cmp,
        unused_imports,
        dead_code
    )]

    use super::*;
    use elephant_embeddings::FakeEmbeddingProvider;
    use elephant_types::records::predicate::SemanticsStatus;

    // =========================================================================
    // Helper: create a test predicate
    // =========================================================================

    fn make_predicate(
        predicate_id: &str,
        description: Option<&str>,
        usage_count: u64,
    ) -> PredicateRegistry {
        let id = RecordId::new_v4(TableName::PredicateRegistry);
        let base = BaseFields::new(id, "test_space".to_string(), "test".to_string());

        PredicateRegistry {
            base,
            predicate_id: predicate_id.to_string(),
            description: description.map(String::from),
            status: "active".to_string(),
            semantics: None,
            semantics_status: SemanticsStatus::Default,
            first_seen_at: Some(chrono::Utc::now()),
            usage_count,
            last_used_at: None,
        }
    }

    // =========================================================================
    // Unit tests for cosine_similarity
    // =========================================================================

    #[test]
    fn test_default_sweep_window_covers_large_tenant_predicate_sets() {
        let config = PredicateHygieneConfig::default();

        assert!(
            config.max_predicates >= 1000,
            "Tenant discovery can produce hundreds of source-derived predicates; \
             maintenance should not silently ignore predicates after the first page"
        );
    }

    #[test]
    fn test_cosine_similarity_identical() {
        let a = vec![1.0, 2.0, 3.0];
        let similarity = cosine_similarity(&a, &a);
        assert!(
            (similarity - 1.0).abs() < 1e-10,
            "Identical vectors should have similarity 1.0, got {similarity}"
        );
    }

    #[test]
    fn test_cosine_similarity_orthogonal() {
        let a = vec![1.0, 0.0];
        let b = vec![0.0, 1.0];
        let similarity = cosine_similarity(&a, &b);
        assert!(
            similarity.abs() < 1e-10,
            "Orthogonal vectors should have similarity 0.0, got {similarity}"
        );
    }

    #[test]
    fn test_cosine_similarity_zero_vector() {
        let a = vec![1.0, 2.0, 3.0];
        let zero = vec![0.0, 0.0, 0.0];
        let similarity = cosine_similarity(&a, &zero);
        assert!(
            similarity.abs() < 1e-10,
            "Zero vector should give similarity 0.0, got {similarity}"
        );
    }

    // =========================================================================
    // Unit tests for group_predicates_by_similarity
    // =========================================================================

    /// Test that predicates with identical embeddings are grouped together
    /// while dissimilar predicates remain separate.
    #[test]
    fn test_group_predicates_by_similarity() {
        // Create predicates with descriptions that hint at similarity
        let predicates = vec![
            make_predicate("works_at", Some("Employment relationship"), 100),
            make_predicate("employed_by", Some("Employment relationship"), 50),
            make_predicate("lives_in", Some("Residential location"), 80),
        ];

        // Use identical embeddings for works_at and employed_by (same description)
        // and a different embedding for lives_in
        let embeddings = vec![
            vec![1.0, 0.0, 0.0], // works_at
            vec![1.0, 0.0, 0.0], // employed_by -- identical to works_at
            vec![0.0, 1.0, 0.0], // lives_in -- orthogonal, different
        ];

        let groups = group_predicates_by_similarity(&predicates, &embeddings, 0.8);

        // Should have exactly one group: works_at + employed_by
        assert_eq!(
            groups.len(),
            1,
            "Should have 1 group (works_at + employed_by), got {}",
            groups.len()
        );

        let group = &groups[0];
        // works_at has higher usage (100 vs 50), so it should be canonical
        assert_eq!(
            group.canonical, "works_at",
            "Canonical should be 'works_at' (highest usage)"
        );
        assert_eq!(group.members.len(), 1);
        assert_eq!(group.members[0].0, "employed_by");

        // lives_in should NOT be in any group
        let all_member_ids: Vec<&str> = groups
            .iter()
            .flat_map(|g| {
                std::iter::once(g.canonical.as_str())
                    .chain(g.members.iter().map(|(id, _)| id.as_str()))
            })
            .collect();
        assert!(
            !all_member_ids.contains(&"lives_in"),
            "lives_in should not be grouped with employment predicates"
        );
    }

    /// Test that canonical selection prefers highest usage_count.
    #[test]
    fn test_group_canonical_by_usage() {
        let predicates = vec![
            make_predicate("low_usage", None, 10),
            make_predicate("high_usage", None, 200),
        ];

        // Identical embeddings so they group together
        let embeddings = vec![vec![1.0, 0.5], vec![1.0, 0.5]];

        let groups = group_predicates_by_similarity(&predicates, &embeddings, 0.8);

        assert_eq!(groups.len(), 1);
        assert_eq!(
            groups[0].canonical, "high_usage",
            "Canonical should be the predicate with highest usage"
        );
    }

    // =========================================================================
    // Integration-style tests for the sweep (using in-memory DB)
    // =========================================================================

    /// A custom embedding provider that returns controlled embeddings to
    /// simulate known similarity relationships.
    struct ControlledEmbeddingProvider {
        /// Map from text -> embedding vector.
        embeddings: std::collections::HashMap<String, Vec<f32>>,
        dimensions: usize,
    }

    impl ControlledEmbeddingProvider {
        fn new(mappings: Vec<(&str, Vec<f32>)>) -> Self {
            let dimensions = mappings.first().map_or(3, |(_, v)| v.len());
            let embeddings = mappings
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect();
            Self {
                embeddings,
                dimensions,
            }
        }
    }

    #[async_trait::async_trait]
    impl EmbeddingProvider for ControlledEmbeddingProvider {
        fn model(&self) -> &str {
            "controlled-test"
        }

        fn dimensions(&self) -> usize {
            self.dimensions
        }

        async fn embed(
            &self,
            texts: &[String],
        ) -> elephant_embeddings::EmbeddingResult<Vec<Vec<f32>>> {
            Ok(texts
                .iter()
                .map(|t| {
                    self.embeddings
                        .get(t)
                        .cloned()
                        .unwrap_or_else(|| vec![0.0; self.dimensions])
                })
                .collect())
        }
    }

    const HYGIENE_SCHEMA: &str = r"
        DEFINE TABLE predicate_registry SCHEMALESS;
        DEFINE TABLE predicate_alias SCHEMALESS;
        DEFINE TABLE work_artifact SCHEMALESS;
    ";

    async fn setup_hygiene_storage() -> StorageClient {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(HYGIENE_SCHEMA).await.expect("schema");
        storage
    }

    async fn insert_predicate(
        storage: &StorageClient,
        predicate_id: &str,
        description: &str,
        usage_count: u64,
    ) -> PredicateRegistry {
        let repo = PredicateRegistryRepository::new(storage.clone());
        let predicate = make_predicate(predicate_id, Some(description), usage_count);
        repo.create(predicate).await.expect("create predicate")
    }

    /// High embedding similarity should create a review artifact, not mutate
    /// active truth canonicalization by default.
    #[tokio::test]
    async fn test_high_confidence_defaults_to_proposal_not_active_alias() {
        let storage = setup_hygiene_storage().await;

        // Insert two predicates that mean the same thing
        insert_predicate(&storage, "works_at", "Employment relationship", 100).await;
        insert_predicate(&storage, "employed_by", "Employment relationship", 30).await;

        // Provider returns identical embeddings for identical descriptions
        let provider = ControlledEmbeddingProvider::new(vec![
            ("Employment relationship", vec![1.0, 0.0, 0.0]),
            ("Employment relationship", vec![1.0, 0.0, 0.0]),
        ]);

        // Similarity = 1.0 (identical) > auto_alias_threshold 0.9, but
        // automatic active aliases are disabled by default.
        let config = PredicateHygieneConfig::default();
        let result =
            execute_predicate_hygiene_sweep(&storage, "test_space", &config, &provider).await;

        let result = result.expect("sweep should succeed");
        assert_eq!(result.predicates_scanned, 2);
        assert_eq!(
            result.aliases_created, 0,
            "Embedding similarity alone must not create active predicate aliases"
        );
        assert_eq!(result.proposals_created, 1);

        // Verify no active alias was created in the DB.
        let alias_repo = PredicateAliasRepository::new(storage.clone());
        let alias = alias_repo
            .find_active_by_raw("test_space", "employed_by")
            .await
            .expect("query alias");
        assert!(alias.is_none(), "No active alias should exist in DB");

        let artifact_repo = WorkArtifactRepository::new(storage);
        let artifacts = artifact_repo
            .list("test_space", 10, None)
            .await
            .expect("list artifacts");
        assert_eq!(artifacts.len(), 1);
        assert_eq!(artifacts[0].artifact_type, HYGIENE_PROPOSAL_ARTIFACT_TYPE);
        assert_eq!(artifacts[0].status, ArtifactStatus::Draft);
    }

    /// Syntax-only predicate variants are safe to activate automatically:
    /// separator and article differences do not change the predicate meaning.
    #[tokio::test]
    async fn test_syntax_variants_are_active_aliased_by_default() {
        let storage = setup_hygiene_storage().await;

        insert_predicate(&storage, "scheduled for", "Schedule relation", 100).await;
        insert_predicate(&storage, "scheduled_for", "Schedule relation", 20).await;
        insert_predicate(&storage, "is the platform for", "Platform relation", 80).await;
        insert_predicate(&storage, "is_platform_for", "Platform relation", 10).await;

        let provider = FakeEmbeddingProvider::new("test-model", 3);
        let config = PredicateHygieneConfig::default();
        let result =
            execute_predicate_hygiene_sweep(&storage, "test_space", &config, &provider).await;
        let result = result.expect("sweep should succeed");

        assert_eq!(result.aliases_created, 2);

        let alias_repo = PredicateAliasRepository::new(storage);
        let scheduled = alias_repo
            .find_active_by_raw("test_space", "scheduled_for")
            .await
            .expect("query scheduled alias")
            .expect("scheduled_for should be active syntax alias");
        assert_eq!(scheduled.predicate_id, "scheduled for");
        assert_eq!(scheduled.source_plugin.as_deref(), Some("predicate_syntax"));

        let platform = alias_repo
            .find_active_by_raw("test_space", "is_platform_for")
            .await
            .expect("query platform alias")
            .expect("is_platform_for should be active syntax alias");
        assert_eq!(platform.predicate_id, "is the platform for");
        assert_eq!(platform.source_plugin.as_deref(), Some("predicate_syntax"));
    }

    /// Auto-aliasing still exists as an explicit opt-in for controlled
    /// deployments, but it is not the maintenance default.
    #[tokio::test]
    async fn test_auto_alias_requires_explicit_enablement() {
        let storage = setup_hygiene_storage().await;

        insert_predicate(&storage, "works_at", "Employment relationship", 100).await;
        insert_predicate(&storage, "employed_by", "Employment relationship", 30).await;

        let provider = ControlledEmbeddingProvider::new(vec![
            ("Employment relationship", vec![1.0, 0.0, 0.0]),
            ("Employment relationship", vec![1.0, 0.0, 0.0]),
        ]);

        let config = PredicateHygieneConfig::default().with_auto_alias_enabled(true);
        let result =
            execute_predicate_hygiene_sweep(&storage, "test_space", &config, &provider).await;

        let result = result.expect("sweep should succeed");
        assert_eq!(result.aliases_created, 1);
        assert_eq!(result.proposals_created, 0);

        let alias = PredicateAliasRepository::new(storage)
            .find_active_by_raw("test_space", "employed_by")
            .await
            .expect("query alias")
            .expect("explicit auto-alias should create alias");
        assert_eq!(alias.predicate_id, "works_at");
        assert_eq!(alias.status, AliasStatus::Active);
    }

    /// Regression for the production failure mode: very high embedding
    /// similarity between semantically unrelated predicates must not create an
    /// active alias that rewrites truth slots.
    #[tokio::test]
    async fn test_unrelated_high_similarity_predicates_are_not_active_aliased_by_default() {
        let storage = setup_hygiene_storage().await;

        insert_predicate(&storage, "agreed_to", "Agreement or consent", 500).await;
        insert_predicate(&storage, "has_email", "Email contact detail", 10).await;
        insert_predicate(&storage, "starts_on", "Event start date", 10).await;
        insert_predicate(&storage, "attends", "Attendance relationship", 10).await;

        let provider = ControlledEmbeddingProvider::new(vec![
            ("Agreement or consent", vec![1.0, 0.0, 0.0]),
            ("Email contact detail", vec![0.99, 0.01, 0.0]),
            ("Event start date", vec![0.98, 0.02, 0.0]),
            ("Attendance relationship", vec![0.97, 0.03, 0.0]),
        ]);

        let config = PredicateHygieneConfig::default();
        let result =
            execute_predicate_hygiene_sweep(&storage, "test_space", &config, &provider).await;
        let result = result.expect("sweep should succeed");

        assert_eq!(result.aliases_created, 0);
        assert!(
            result.proposals_created >= 1,
            "high-similarity pairs may be surfaced for review"
        );

        let alias_repo = PredicateAliasRepository::new(storage);
        for raw in ["has_email", "starts_on", "attends"] {
            let alias = alias_repo
                .find_active_by_raw("test_space", raw)
                .await
                .expect("query alias");
            assert!(
                alias.is_none(),
                "{raw} must not become an active alias for agreed_to"
            );
        }
    }

    /// When similarity is between 0.8 and 0.9, creates a proposal instead
    /// of auto-aliasing.
    #[tokio::test]
    async fn test_proposal_for_ambiguous() {
        let storage = setup_hygiene_storage().await;

        insert_predicate(&storage, "works_at", "Employment relationship", 100).await;
        insert_predicate(
            &storage,
            "affiliated_with",
            "Affiliated with organization",
            40,
        )
        .await;

        // Embeddings with similarity ~0.85 (between 0.8 and 0.9)
        // cos(a, b) = (1*0.9 + 0*0.4) / (1.0 * sqrt(0.81 + 0.16))
        //           = 0.9 / sqrt(0.97) = 0.9 / 0.9849 = 0.9137
        // Let's use vectors that yield exactly ~0.85:
        // a = [1.0, 0.0, 0.0], b = [0.85, 0.5268, 0.0]
        // cos = 0.85 / (1.0 * sqrt(0.7225 + 0.2775)) = 0.85 / 1.0 = 0.85
        let provider = ControlledEmbeddingProvider::new(vec![
            ("Employment relationship", vec![1.0, 0.0, 0.0]),
            ("Affiliated with organization", vec![0.85, 0.5268, 0.0]),
        ]);

        let config = PredicateHygieneConfig {
            auto_alias_threshold: 0.9,
            proposal_threshold: 0.8,
            ..Default::default()
        };

        let result =
            execute_predicate_hygiene_sweep(&storage, "test_space", &config, &provider).await;

        let result = result.expect("sweep should succeed");
        assert_eq!(result.predicates_scanned, 2);
        assert_eq!(
            result.aliases_created, 0,
            "Should NOT auto-create alias (similarity < 0.9)"
        );
        assert_eq!(
            result.proposals_created, 1,
            "Should create 1 proposal (similarity between 0.8 and 0.9)"
        );

        // Verify proposal artifact was created
        let artifact_repo = WorkArtifactRepository::new(storage.clone());
        let artifacts = artifact_repo
            .list("test_space", 10, None)
            .await
            .expect("list artifacts");
        assert_eq!(artifacts.len(), 1);
        assert_eq!(artifacts[0].artifact_type, HYGIENE_PROPOSAL_ARTIFACT_TYPE);
        assert_eq!(artifacts[0].status, ArtifactStatus::Draft);

        let second_result =
            execute_predicate_hygiene_sweep(&storage, "test_space", &config, &provider).await;
        let second_result = second_result.expect("second sweep should succeed");
        assert_eq!(
            second_result.proposals_created, 0,
            "Repeated sweeps should not create duplicate review proposals"
        );
        let artifacts = artifact_repo
            .list("test_space", 10, None)
            .await
            .expect("list artifacts after second sweep");
        assert_eq!(artifacts.len(), 1);
    }

    /// When similarity is below proposal_threshold, no action is taken.
    #[tokio::test]
    async fn test_no_action_below_threshold() {
        let storage = setup_hygiene_storage().await;

        insert_predicate(&storage, "works_at", "Employment relationship", 100).await;
        insert_predicate(&storage, "lives_in", "Residential location", 80).await;

        // Orthogonal embeddings: similarity = 0.0
        let provider = ControlledEmbeddingProvider::new(vec![
            ("Employment relationship", vec![1.0, 0.0, 0.0]),
            ("Residential location", vec![0.0, 1.0, 0.0]),
        ]);

        let config = PredicateHygieneConfig::default();
        let result =
            execute_predicate_hygiene_sweep(&storage, "test_space", &config, &provider).await;

        let result = result.expect("sweep should succeed");
        assert_eq!(result.predicates_scanned, 2);
        assert_eq!(result.aliases_created, 0);
        assert_eq!(result.proposals_created, 0);
    }

    /// Fewer than 2 predicates should skip the sweep.
    #[tokio::test]
    async fn test_skip_with_single_predicate() {
        let storage = setup_hygiene_storage().await;

        insert_predicate(&storage, "works_at", "Employment", 10).await;

        let provider = FakeEmbeddingProvider::new("test-model", 3);
        let config = PredicateHygieneConfig::default();

        let result =
            execute_predicate_hygiene_sweep(&storage, "test_space", &config, &provider).await;

        let result = result.expect("sweep should succeed");
        assert_eq!(result.predicates_scanned, 1);
        assert_eq!(result.aliases_created, 0);
        assert_eq!(result.proposals_created, 0);
    }

    /// Existing active aliases generated by the old unsafe hygiene worker are
    /// made non-active even when there are too few predicates for a normal
    /// similarity sweep.
    #[tokio::test]
    async fn test_deactivates_existing_active_hygiene_aliases_by_default() {
        let storage = setup_hygiene_storage().await;
        let alias_repo = PredicateAliasRepository::new(storage.clone());
        let alias_id = RecordId::new_v4(TableName::PredicateAlias);
        let base = BaseFields::new(alias_id, "test_space".to_string(), "test".to_string());
        let existing_alias = PredicateAlias {
            base,
            alias: "has_email".to_string(),
            alias_type: AliasType::RawString,
            source_plugin: Some("predicate_hygiene".to_string()),
            confidence: 0.99,
            status: AliasStatus::Active,
            scope: AliasScope::Global,
            predicate_id: "agreed_to".to_string(),
            confidence_version: 1,
            usage_count: 0,
        };
        alias_repo
            .create(existing_alias)
            .await
            .expect("create old hygiene alias");

        let provider = FakeEmbeddingProvider::new("test-model", 3);
        let config = PredicateHygieneConfig::default();
        let result =
            execute_predicate_hygiene_sweep(&storage, "test_space", &config, &provider).await;
        let result = result.expect("sweep should succeed");

        assert_eq!(result.predicates_scanned, 0);
        assert_eq!(result.aliases_created, 0);
        assert_eq!(result.proposals_created, 1);
        assert!(
            alias_repo
                .find_active_by_raw("test_space", "has_email")
                .await
                .expect("query active alias")
                .is_none(),
            "old predicate_hygiene alias should no longer be active"
        );

        let aliases = alias_repo
            .find_by_predicate_id("test_space", "agreed_to")
            .await
            .expect("query aliases");
        assert_eq!(aliases.len(), 1);
        assert_eq!(aliases[0].status, AliasStatus::Deprecated);

        let artifacts = WorkArtifactRepository::new(storage)
            .list("test_space", 10, None)
            .await
            .expect("list review artifacts");
        assert_eq!(artifacts.len(), 1);
        assert_eq!(
            artifacts[0]
                .data
                .get("reason")
                .and_then(serde_json::Value::as_str),
            Some("active predicate_hygiene alias deactivated for review")
        );
    }

    /// Existing aliases should not be duplicated.
    #[tokio::test]
    async fn test_skips_existing_alias() {
        let storage = setup_hygiene_storage().await;

        insert_predicate(&storage, "works_at", "Employment relationship", 100).await;
        insert_predicate(&storage, "employed_by", "Employment relationship", 30).await;

        // Pre-create an active alias for employed_by -> works_at
        let alias_repo = PredicateAliasRepository::new(storage.clone());
        let alias_id = RecordId::new_v4(TableName::PredicateAlias);
        let base = BaseFields::new(alias_id, "test_space".to_string(), "test".to_string());
        let existing_alias = PredicateAlias {
            base,
            alias: "employed_by".to_string(),
            alias_type: AliasType::RawString,
            source_plugin: None,
            confidence: 0.95,
            status: AliasStatus::Active,
            scope: AliasScope::Global,
            predicate_id: "works_at".to_string(),
            confidence_version: 1,
            usage_count: 0,
        };
        alias_repo
            .create(existing_alias)
            .await
            .expect("create existing alias");

        // Identical embeddings (would normally auto-alias)
        let provider = ControlledEmbeddingProvider::new(vec![
            ("Employment relationship", vec![1.0, 0.0, 0.0]),
            ("Employment relationship", vec![1.0, 0.0, 0.0]),
        ]);

        let config = PredicateHygieneConfig::default();
        let result =
            execute_predicate_hygiene_sweep(&storage, "test_space", &config, &provider).await;

        let result = result.expect("sweep should succeed");
        assert_eq!(
            result.aliases_created, 0,
            "Should not create duplicate alias"
        );
    }
}
