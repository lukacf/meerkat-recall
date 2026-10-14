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

mod atomic;
pub(crate) use atomic::prepare_predicate_hygiene_sweep;

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
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::error::PipelineError;

/// Configuration for predicate hygiene sweeps.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    let planning = crate::maintenance_commit::PlanningTruthMaintenanceStorage::new(
        storage.clone(),
        space_id,
        None,
    )
    .await?;
    let result = prepare_predicate_hygiene_sweep(
        &planning,
        storage,
        space_id,
        config,
        embedding_provider,
        None,
    )
    .await?;
    planning.audit_pending_effects("predicate_hygiene").await?;
    planning
        .commit(
            "predicate_hygiene",
            space_id,
            &serde_json::json!({"config":config}),
            &result,
        )
        .await?;
    Ok(result)
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

    async fn setup_hygiene_backend(embedded: bool) -> StorageClient {
        let storage = if embedded {
            StorageClient::new_embedded().await.expect("storage")
        } else {
            let client = StorageClient::new_memory().await.expect("storage");
            elephant_migrations::apply_schema(&client)
                .await
                .expect("schema");
            client
        };
        elephant_storage::SpaceConfigRepository::new(storage.clone())
            .create(elephant_types::records::SpaceConfig {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::SpaceConfig),
                    "test_space".into(),
                    "test".into(),
                ),
                embedding_provider: "unconfigured".into(),
                embedding_model: "unconfigured".into(),
                embedding_dims: 3,
                processing: None,
                purpose: None,
                skill: None,
            })
            .await
            .expect("space config");
        storage
    }
    async fn setup_hygiene_storage() -> StorageClient {
        setup_hygiene_backend(false).await
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
    struct IncompleteEmbeddingProvider;
    #[async_trait::async_trait]
    impl EmbeddingProvider for IncompleteEmbeddingProvider {
        fn model(&self) -> &str {
            "incomplete-offline-fixture"
        }
        fn dimensions(&self) -> usize {
            3
        }
        async fn embed(&self, _: &[String]) -> elephant_embeddings::EmbeddingResult<Vec<Vec<f32>>> {
            Ok(vec![])
        }
    }

    #[tokio::test]
    async fn later_provider_failure_keeps_prepared_syntax_aliases_unpublished() {
        for embedded in [true, false] {
            let storage = setup_hygiene_backend(embedded).await;
            insert_predicate(&storage, "scheduled for", "Schedule", 100).await;
            insert_predicate(&storage, "scheduled_for", "Schedule", 20).await;
            assert!(execute_predicate_hygiene_sweep(
                &storage,
                "test_space",
                &PredicateHygieneConfig::default(),
                &IncompleteEmbeddingProvider
            )
            .await
            .is_err());
            assert!(PredicateAliasRepository::new(storage.clone())
                .list("test_space", 10, None)
                .await
                .expect("aliases")
                .is_empty());
            assert!(WorkArtifactRepository::new(storage.clone())
                .list("test_space", 10, None)
                .await
                .expect("artifacts")
                .is_empty());
            assert!(elephant_storage::AuditLogRepository::new(storage)
                .list("test_space", 10, None)
                .await
                .expect("audit")
                .is_empty());
        }
    }

    #[tokio::test]
    async fn proposal_uses_real_pair_and_rejects_changed_operational_read_dependency() {
        use crate::maintenance_commit::PlanningTruthMaintenanceStorage;
        use elephant_storage::canonical_commit::{CanonicalRecord, CanonicalRepository};
        for embedded in [true, false] {
            for change_item in [false, true] {
                let storage = setup_hygiene_backend(embedded).await;
                insert_predicate(&storage, "works_at", "Employment", 100).await;
                insert_predicate(&storage, "employed_by", "Employment", 20).await;
                let planning =
                    PlanningTruthMaintenanceStorage::new(storage.clone(), "test_space", None)
                        .await
                        .expect("capture");
                let provider =
                    ControlledEmbeddingProvider::new(vec![("Employment", vec![1.0, 0.0, 0.0])]);
                let result = prepare_predicate_hygiene_sweep(
                    &planning,
                    &storage,
                    "test_space",
                    &PredicateHygieneConfig::default(),
                    &provider,
                    None,
                )
                .await
                .expect("prepare");
                assert_eq!(result.proposals_created, 1);
                let plan = planning.prepared_plan().expect("plan");
                let mut run = plan
                    .dependencies
                    .iter()
                    .find_map(|record| match record {
                        CanonicalRecord::WorkRun(row) => Some((**row).clone()),
                        _ => None,
                    })
                    .expect("actual run");
                let item = plan
                    .dependencies
                    .iter()
                    .find_map(|record| match record {
                        CanonicalRecord::WorkItem(row) => Some((**row).clone()),
                        _ => None,
                    })
                    .expect("actual item");
                assert_eq!(run.work_item.id(), &item.base.id);
                assert!(plan.effects.iter().all(|effect| !matches!(
                    &effect.after,
                    Some(CanonicalRecord::WorkRun(_) | CanonicalRecord::WorkItem(_))
                )));
                assert!(WorkArtifactRepository::new(storage.clone())
                    .list("test_space", 10, None)
                    .await
                    .expect("before")
                    .is_empty());
                planning
                    .audit_pending_effects("predicate_hygiene")
                    .await
                    .expect("audit");
                let request = planning
                    .prepare_request(
                        "predicate_hygiene",
                        "test_space",
                        &serde_json::json!({}),
                        &result,
                    )
                    .await
                    .expect("request");
                let key = request.commit_id.clone();
                if change_item {
                    elephant_storage::WorkItemRepository::new(storage.clone())
                        .delete("test_space", &item.base.id)
                        .await
                        .expect("delete observed item");
                } else {
                    run.worker_id = "changed-after-capture".into();
                    let id = run.base.id.clone();
                    elephant_storage::WorkRunRepository::new(storage.clone())
                        .update("test_space", &id, run)
                        .await
                        .expect("change observed run");
                }
                assert!(CanonicalRepository::new(storage.clone())
                    .commit(request)
                    .await
                    .is_err());
                assert!(WorkArtifactRepository::new(storage.clone())
                    .list("test_space", 10, None)
                    .await
                    .expect("after")
                    .is_empty());
                assert!(elephant_storage::AuditLogRepository::new(storage.clone())
                    .list("test_space", 10, None)
                    .await
                    .expect("audit")
                    .is_empty());
                assert!(CanonicalRepository::new(storage)
                    .receipt("test_space", &key)
                    .await
                    .expect("receipt")
                    .is_none());
            }
        }
    }
    #[tokio::test]
    async fn actual_proposal_context_survives_archive_without_resurrection() {
        use crate::maintenance_commit::PlanningTruthMaintenanceStorage;
        use elephant_storage::canonical_commit::{CanonicalRecord, CanonicalRepository};
        for embedded in [true, false] {
            let storage = setup_hygiene_backend(embedded).await;
            insert_predicate(&storage, "works_at", "Employment", 100).await;
            insert_predicate(&storage, "employed_by", "Employment", 20).await;
            let provider =
                ControlledEmbeddingProvider::new(vec![("Employment", vec![1.0, 0.0, 0.0])]);
            let planning =
                PlanningTruthMaintenanceStorage::new(storage.clone(), "test_space", None)
                    .await
                    .expect("capture");
            let result = prepare_predicate_hygiene_sweep(
                &planning,
                &storage,
                "test_space",
                &PredicateHygieneConfig::default(),
                &provider,
                None,
            )
            .await
            .expect("prepare");
            let plan = planning.prepared_plan().expect("plan");
            let run = plan
                .dependencies
                .iter()
                .find_map(|record| match record {
                    CanonicalRecord::WorkRun(row) => Some((**row).clone()),
                    _ => None,
                })
                .expect("run");
            let cutoff = chrono::Utc::now() + chrono::Duration::seconds(1);
            let archived =
                elephant_storage::work_retention::archive_terminal_work(&storage, cutoff, 10)
                    .await
                    .expect("archive");
            assert_eq!(archived.items_archived, 1);
            assert_eq!(archived.runs_archived, 1);
            // A second preparation at the same canonical epoch reuses the real
            // archived context; it cannot resurrect hot records or invent a run.
            let retry = PlanningTruthMaintenanceStorage::new(storage.clone(), "test_space", None)
                .await
                .expect("retry capture");
            prepare_predicate_hygiene_sweep(
                &retry,
                &storage,
                "test_space",
                &PredicateHygieneConfig::default(),
                &provider,
                None,
            )
            .await
            .expect("archive replay");
            assert!(retry
                .prepared_plan()
                .expect("retry plan")
                .dependencies
                .iter()
                .any(
                    |record| matches!(record, CanonicalRecord::WorkRun(row) if row.as_ref() == &run)
                ));
            planning
                .audit_pending_effects("predicate_hygiene")
                .await
                .expect("audit");
            let request = planning
                .prepare_request(
                    "predicate_hygiene",
                    "test_space",
                    &serde_json::json!({}),
                    &result,
                )
                .await
                .expect("request");
            CanonicalRepository::new(storage.clone())
                .commit(request)
                .await
                .expect("commit archived readset");
            assert_eq!(
                WorkArtifactRepository::new(storage.clone())
                    .list("test_space", 10, None)
                    .await
                    .expect("artifacts")
                    .len(),
                1
            );
            assert_eq!(
                elephant_storage::WorkRunRepository::new(storage.clone())
                    .get("test_space", &run.base.id)
                    .await
                    .expect("original run"),
                Some(run)
            );
            let second_archive =
                elephant_storage::work_retention::archive_terminal_work(&storage, cutoff, 10)
                    .await
                    .expect("no resurrection");
            assert_eq!(second_archive.items_archived, 0);
            assert_eq!(second_archive.runs_archived, 0);
        }
    }

    struct PartitionBatchProvider {
        requests: std::sync::Mutex<Vec<Vec<String>>>,
        malformed: Option<&'static str>,
    }
    impl PartitionBatchProvider {
        fn new(malformed: Option<&'static str>) -> Self {
            Self {
                requests: std::sync::Mutex::new(vec![]),
                malformed,
            }
        }
    }
    #[async_trait::async_trait]
    impl EmbeddingProvider for PartitionBatchProvider {
        fn model(&self) -> &str {
            "partition-batch-offline-fixture"
        }
        fn dimensions(&self) -> usize {
            3
        }
        async fn embed(
            &self,
            texts: &[String],
        ) -> elephant_embeddings::EmbeddingResult<Vec<Vec<f32>>> {
            self.requests.lock().expect("requests").push(texts.to_vec());
            let mut rows: Vec<_> = texts
                .iter()
                .map(|text| match text.as_str() {
                    "A" => vec![1.0, 0.0, 0.0],
                    "B" => vec![0.0, 1.0, 0.0],
                    "C" => vec![0.0, 0.0, 1.0],
                    "D" => vec![0.0, 1.0, 1.0],
                    _ => vec![1.0, 0.0, 0.0],
                })
                .collect();
            match self.malformed {
                Some("missing") => {
                    rows.pop();
                }
                Some("extra") => rows.push(vec![1.0, 0.0, 0.0]),
                Some("dimension") => rows[0] = vec![1.0, 0.0],
                Some("nonfinite") => rows[0][0] = f32::NAN,
                _ => {}
            }
            Ok(rows)
        }
    }

    #[tokio::test]
    async fn predicate_partition_batch_preserves_vector_mapping_and_omits_singletons() {
        let groups = vec![
            vec![
                make_predicate("a", Some("A"), 1),
                make_predicate("b", Some("B"), 1),
            ],
            vec![make_predicate("single", Some("never-dispatched"), 1)],
            vec![
                make_predicate("c", Some("C"), 1),
                make_predicate("d", Some("D"), 1),
            ],
        ];
        let provider = PartitionBatchProvider::new(None);
        let vectors = atomic::embed_partition_inputs(&groups, &provider)
            .await
            .expect("batch");
        assert_eq!(
            vectors,
            vec![
                vec![1.0, 0.0, 0.0],
                vec![0.0, 1.0, 0.0],
                vec![0.0, 0.0, 1.0],
                vec![0.0, 1.0, 1.0]
            ]
        );
        assert_eq!(
            *provider.requests.lock().expect("requests"),
            vec![vec!["A", "B", "C", "D"]]
        );
        let mut reversed = groups.clone();
        reversed.reverse();
        for group in &mut reversed {
            group.reverse();
        }
        let reverse_provider = PartitionBatchProvider::new(None);
        let reversed_vectors = atomic::embed_partition_inputs(&reversed, &reverse_provider)
            .await
            .expect("reverse batch");
        assert_eq!(
            reversed_vectors,
            vectors.into_iter().rev().collect::<Vec<_>>()
        );
        let no_pairs = PartitionBatchProvider::new(None);
        assert!(
            atomic::embed_partition_inputs(&[groups[1].clone(), vec![]], &no_pairs)
                .await
                .expect("singletons")
                .is_empty()
        );
        assert!(no_pairs.requests.lock().expect("requests").is_empty());
    }

    #[tokio::test]
    async fn predicate_partition_batch_keeps_distinct_envelope_proposals_on_both_backends() {
        for embedded in [true, false] {
            let storage = setup_hygiene_backend(embedded).await;
            let repo = PredicateRegistryRepository::new(storage.clone());
            for (label, canonical, alias) in [
                ("one", "works_at", "employed_by"),
                ("two", "located_at", "situated_in"),
            ] {
                for (name, usage) in [(canonical, 100), (alias, 1)] {
                    let mut predicate =
                        make_predicate(name, Some("Identical provider vector"), usage);
                    predicate.base.security.labels = vec![label.into()];
                    repo.create(predicate).await.expect("predicate");
                }
            }
            let provider = PartitionBatchProvider::new(None);
            let result = execute_predicate_hygiene_sweep(
                &storage,
                "test_space",
                &PredicateHygieneConfig::default(),
                &provider,
            )
            .await
            .expect("sweep");
            assert_eq!(provider.requests.lock().expect("requests").len(), 1);
            assert_eq!(result.proposals_created, 2);
            assert_eq!(result.aliases_created, 0);
            let proposals = WorkArtifactRepository::new(storage.clone())
                .list("test_space", 10, None)
                .await
                .expect("proposals");
            assert_eq!(proposals.len(), 2);
            let pairs: std::collections::BTreeSet<_> = proposals
                .into_iter()
                .map(|artifact| {
                    assert_eq!(artifact.status, ArtifactStatus::Draft);
                    (
                        artifact.base.security.labels,
                        artifact.data["alias_predicate_id"]
                            .as_str()
                            .expect("alias")
                            .to_owned(),
                        artifact.data["canonical_predicate_id"]
                            .as_str()
                            .expect("canonical")
                            .to_owned(),
                    )
                })
                .collect();
            assert_eq!(
                pairs,
                std::collections::BTreeSet::from([
                    (
                        vec!["one".to_owned()],
                        "employed_by".to_owned(),
                        "works_at".to_owned()
                    ),
                    (
                        vec!["two".to_owned()],
                        "situated_in".to_owned(),
                        "located_at".to_owned()
                    ),
                ])
            );
            assert!(PredicateAliasRepository::new(storage)
                .list("test_space", 10, None)
                .await
                .expect("aliases")
                .is_empty());
        }
    }

    #[tokio::test]
    async fn predicate_partition_batch_rejects_malformed_complete_response_before_effects() {
        use crate::maintenance_commit::PlanningTruthMaintenanceStorage;
        for malformed in ["missing", "extra", "dimension", "nonfinite"] {
            let storage = setup_hygiene_storage().await;
            let repo = PredicateRegistryRepository::new(storage.clone());
            for (label, names) in [
                ("one", ["scheduled for", "scheduled_for"]),
                ("two", ["works at", "works_at"]),
            ] {
                for name in names {
                    let mut predicate = make_predicate(name, Some("Identical provider vector"), 1);
                    predicate.base.security.labels = vec![label.into()];
                    repo.create(predicate).await.expect("predicate");
                }
            }
            let planning =
                PlanningTruthMaintenanceStorage::new(storage.clone(), "test_space", None)
                    .await
                    .expect("capture");
            let provider = PartitionBatchProvider::new(Some(malformed));
            assert!(prepare_predicate_hygiene_sweep(
                &planning,
                &storage,
                "test_space",
                &PredicateHygieneConfig::default(),
                &provider,
                None
            )
            .await
            .is_err());
            assert_eq!(provider.requests.lock().expect("requests").len(), 1);
            assert!(planning.prepared_plan().expect("plan").effects.is_empty());
            assert!(PredicateAliasRepository::new(storage.clone())
                .list("test_space", 10, None)
                .await
                .expect("aliases")
                .is_empty());
            assert!(WorkArtifactRepository::new(storage)
                .list("test_space", 10, None)
                .await
                .expect("artifacts")
                .is_empty());
        }
    }
}
