//! Maintenance workers for the Elephant knowledge graph.
//!
//! These periodic workers maintain graph quality using LLM-powered sweeps
//! with pre-built context dossiers. Each sweep gathers rich context upfront
//! so the LLM can decide in a single call without expensive tool round-trips.
//!
//! Three sweep types:
//! - **Identity sweep**: Find and merge duplicate entities
//! - **Entity type sweep**: Reclassify entities with default/unknown types
//! - **Predicate sweep**: Normalize semantic duplicate predicates

mod atomic_graph;

pub mod classification_backfill;
pub mod failed_work_recovery;
pub mod identity_sweep;

use crate::maintenance::classification_backfill::ClassificationBackfillStats;
use crate::maintenance::failed_work_recovery::FailedWorkRecoveryStats;

use elephant_embeddings::EmbeddingProvider;
use elephant_storage::{
    AssertionRepository, EntityRepository, RelRepository, Repository, SearchCriteria,
    SearchRepository,
};
use elephant_types::records::assertion::{Assertion, ResolutionStatus};
use elephant_types::records::entity::{Entity, EntityStatus};
use elephant_types::records::rel::{ClaimStatus, EvidenceRef, Rel};
use elephant_types::{RecordId, TableName};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::entity_type_consolidation::{self, EntityTypeConsolidationConfig};
use crate::predicate_hygiene::{self, PredicateHygieneConfig};

/// Combined result of all maintenance sweeps.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MaintenanceSweepResult {
    /// Entity label hygiene results.
    pub entity_hygiene: EntityHygieneSweepResult,
    /// Relationship exact-slot dedupe results.
    pub relationships: RelationshipSweepResult,
    /// Assertion resolved-slot dedupe results.
    pub assertions: AssertionSweepResult,
    /// Identity sweep results.
    pub identity: IdentitySweepResult,
    /// Entity type sweep results.
    pub entity_type: EntityTypeSweepResult,
    /// Predicate sweep results.
    pub predicate: PredicateSweepResult,
    /// Entity embedding sweep results.
    pub entity_embeddings: EntityEmbeddingSweepResult,
    /// Classification backfill results.
    pub classification_backfill: ClassificationBackfillStats,
    /// Failed-work recovery results.
    #[serde(skip)]
    pub failed_work_recovery: FailedWorkRecoveryStats,
}

/// Result of an entity embedding sweep.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EntityEmbeddingSweepResult {
    /// Number of candidate entities scanned.
    pub scanned: usize,
    /// Number of entities (re-)embedded.
    pub embedded: usize,
    /// Number of entities skipped (embedding up to date, or nothing to embed).
    pub skipped: usize,
    /// Number of stale entities deferred to the next sweep (per-cycle cap).
    pub deferred: usize,
    /// Number of embed/store failures.
    pub failed: usize,
}

/// Result of an entity label hygiene sweep.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EntityHygieneSweepResult {
    /// Number of active entities scanned.
    pub scanned: usize,
    /// Number of structurally invalid entity labels soft-deleted.
    pub retired: usize,
    /// Number of relationships pointing at retired entities soft-deleted.
    pub relationships_deleted: usize,
    /// Number of relationships repointed from merged entities to their survivor.
    pub relationships_repointed: usize,
    /// Number of assertions pointing at retired entities marked superseded.
    pub assertions_superseded: usize,
    /// Number of assertions repointed from merged entities to their survivor.
    pub assertions_repointed: usize,
}

/// Result of an exact relationship slot dedupe sweep.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RelationshipSweepResult {
    /// Number of active relationships scanned.
    pub scanned: usize,
    /// Number of duplicate `(subject, object, predicate)` slots found.
    pub duplicate_slots: usize,
    /// Number of duplicate relationship rows soft-deleted after evidence merge.
    pub merged: usize,
    /// Number of evidence references appended to winner relationships.
    pub evidence_added: usize,
}

/// Result of an exact resolved assertion slot dedupe sweep.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AssertionSweepResult {
    /// Number of active assertions scanned.
    pub scanned: usize,
    /// Number of duplicate resolved `(subject, object, predicate)` assertion slots found.
    pub duplicate_slots: usize,
    /// Number of duplicate assertion rows marked superseded.
    pub superseded: usize,
}

/// Result of an identity detection sweep.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IdentitySweepResult {
    /// Number of candidate pairs evaluated.
    pub candidates_evaluated: usize,
    /// Number of entities merged.
    pub auto_merged: usize,
    /// Number of likely matches left pending for review because confidence was below the auto-merge threshold.
    pub proposed: usize,
    /// Number of pairs determined to be distinct entities.
    pub rejected: usize,
    /// Number of pairs skipped (already have a candidate record).
    pub skipped: usize,
}

/// Result of an entity type consolidation sweep.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EntityTypeSweepResult {
    /// Number of entities scanned.
    pub scanned: usize,
    /// Number of entities reclassified.
    pub reclassified: usize,
    /// Number of entities skipped (no signal or already correct).
    pub skipped: usize,
}

/// Result of a predicate normalization sweep.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PredicateSweepResult {
    /// Number of predicates scanned.
    pub scanned: usize,
    /// Number of aliases created.
    pub aliases_created: usize,
    /// Number of alias proposals created for operator review.
    pub proposals_created: usize,
    /// Number of previously unseen predicates registered before hygiene ran.
    pub registered: usize,
}

impl std::fmt::Display for MaintenanceSweepResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "entity_hygiene({} scanned, {} retired, {} rels deleted, {} rels repointed, {} assertions superseded, {} assertions repointed) | rels({} scanned, {} slots, {} merged) | assertions({} scanned, {} slots, {} superseded) | identity({} eval, {} merged, {} proposed, {} rejected) | types({} scanned, {} reclassified) | predicates({} scanned, {} aliased, {} proposed, {} registered) | embeddings({} scanned, {} embedded, {} failed) | recovery({} examined, {} requeued, {} too recent, {} out of attempts)",
            self.entity_hygiene.scanned,
            self.entity_hygiene.retired,
            self.entity_hygiene.relationships_deleted,
            self.entity_hygiene.relationships_repointed,
            self.entity_hygiene.assertions_superseded,
            self.entity_hygiene.assertions_repointed,
            self.relationships.scanned,
            self.relationships.duplicate_slots,
            self.relationships.merged,
            self.assertions.scanned,
            self.assertions.duplicate_slots,
            self.assertions.superseded,
            self.identity.candidates_evaluated,
            self.identity.auto_merged,
            self.identity.proposed,
            self.identity.rejected,
            self.entity_type.scanned,
            self.entity_type.reclassified,
            self.predicate.scanned,
            self.predicate.aliases_created,
            self.predicate.proposals_created,
            self.predicate.registered,
            self.entity_embeddings.scanned,
            self.entity_embeddings.embedded,
            self.entity_embeddings.failed,
            self.failed_work_recovery.examined,
            self.failed_work_recovery.requeued,
            self.failed_work_recovery.too_recent,
            self.failed_work_recovery.attempts_exhausted,
        )
    }
}

/// Configuration for maintenance sweeps.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaintenanceConfig {
    /// MCP URL for the maintenance agent profile.
    pub mcp_url: String,
    /// LLM model to use for decisions.
    pub model: String,
    /// Confidence threshold for auto-merging entities.
    pub auto_merge_threshold: f64,
    /// Maximum candidate pairs to evaluate per sweep.
    pub max_candidates_per_sweep: usize,
}

impl Default for MaintenanceConfig {
    fn default() -> Self {
        Self {
            mcp_url: "http://127.0.0.1:3000/mcp/maintenance".to_string(),
            model: "claude-sonnet-4-5".to_string(),
            auto_merge_threshold: 0.9,
            max_candidates_per_sweep: 50,
        }
    }
}

/// Whether the operator has enabled the one-time classification repair pass.
///
/// Defaults to OFF. See the call site in [`run_maintenance_sweep`] for why a
/// wholesale-rewrite pass must not switch itself on when a new binary lands.
fn classification_backfill_enabled() -> bool {
    backfill_flag_is_on(
        std::env::var("PIPELINE_CLASSIFICATION_BACKFILL_ENABLED")
            .ok()
            .as_deref(),
    )
}

/// Parses the backfill opt-in flag. Split out so the default can be tested
/// without mutating process environment, which would race other tests sharing
/// the binary.
pub(crate) fn backfill_flag_is_on(value: Option<&str>) -> bool {
    matches!(value, Some("true") | Some("1"))
}

/// Run the retained semantic phases in one canonical transaction, followed by
/// the separately guarded vector-index and queue-recovery maintenance.
///
/// # Errors
/// Any semantic preparation or transaction failure aborts the complete graph
/// change. No successful earlier phase is published on a later phase failure.
pub async fn run_maintenance_sweep(
    storage: &elephant_storage::StorageClient,
    space_id: &str,
    config: &MaintenanceConfig,
    predicate_embedding_provider: &dyn EmbeddingProvider,
) -> Result<MaintenanceSweepResult, crate::error::PipelineError> {
    run_maintenance_sweep_with_attempt(
        storage,
        space_id,
        config,
        predicate_embedding_provider,
        None,
        None,
    )
    .await
}

/// Queue dispatch supplies the original attempt and its actual work run.
/// The retained periodic standalone caller uses neither.
pub async fn run_maintenance_sweep_with_attempt(
    storage: &elephant_storage::StorageClient,
    space_id: &str,
    config: &MaintenanceConfig,
    predicate_embedding_provider: &dyn EmbeddingProvider,
    attempt: Option<elephant_types::publication::AttemptFence>,
    work_run: Option<&RecordId>,
) -> Result<MaintenanceSweepResult, crate::error::PipelineError> {
    use crate::maintenance_commit::PlanningTruthMaintenanceStorage;
    use crate::PipelineError;

    // Native extension references belong to identity's owner registry. Install
    // its epoch guards before capturing the first decision snapshot.
    let registry = elephant_extensions::load_runtime_reference_registry_from_env()
        .map_err(|error| PipelineError::ConfigError(error.to_string()))?;
    let mut tables = registry.tables().map(str::to_owned).collect::<Vec<_>>();
    tables.sort();
    elephant_storage::canonical_commit::ensure_extension_epoch_guards(storage, &tables).await?;
    let planning =
        PlanningTruthMaintenanceStorage::new(storage.clone(), space_id, attempt.clone()).await?;
    if let Some(fence) = &attempt {
        let id = work_run.ok_or_else(|| {
            PipelineError::InvalidState("Queued maintenance requires its original work run".into())
        })?;
        capture_maintenance_run(&planning, storage, space_id, fence, id).await?;
    }
    let backfill = classification_backfill_enabled();
    let input = serde_json::json!({"config": config, "classification_backfill": backfill,
        "predicate_embedding_model": predicate_embedding_provider.model(),
        "predicate_embedding_dimensions": predicate_embedding_provider.dimensions()});
    let mut result = if let Some(replayed) = planning
        .replay("run_maintenance_sweep", space_id, &input)
        .await?
    {
        replayed
    } else {
        let mut result = MaintenanceSweepResult::default();
        // The opt-in repair runs first in the overlay so subsequent decisions
        // consume its inherited security envelope, never historical public defaults.
        if backfill {
            result.classification_backfill =
                classification_backfill::prepare_classification_backfill_sweep(
                    &planning, storage, space_id,
                )
                .await?;
        }
        result.entity_hygiene = atomic_graph::hygiene(&planning, storage, space_id).await?;
        result.relationships = atomic_graph::relationships(&planning, storage, space_id).await?;
        result.assertions = atomic_graph::assertions(&planning, storage, space_id).await?;
        result.identity =
            identity_sweep::prepare_identity_sweep(&planning, storage, space_id, config).await?;
        let consolidation = entity_type_consolidation::prepare_entity_type_sweep(
            &planning,
            storage,
            space_id,
            &EntityTypeConsolidationConfig::default(),
        )
        .await?;
        result.entity_type = EntityTypeSweepResult {
            scanned: consolidation.entities_scanned,
            reclassified: consolidation.entities_classified,
            skipped: consolidation.entities_skipped,
        };
        result.predicate = prepare_predicate_sweep(
            &planning,
            storage,
            space_id,
            predicate_embedding_provider,
            work_run,
        )
        .await?;
        planning
            .audit_pending_effects("run_maintenance_sweep")
            .await?;
        planning
            .commit("run_maintenance_sweep", space_id, &input, &result)
            .await?;
        result
    };

    // These owners maintain derived search vectors and operational queue state.
    // Neither is included in the semantic transaction's result receipt.
    match run_entity_embedding_sweep(storage, space_id, predicate_embedding_provider).await {
        Ok(embeddings) => result.entity_embeddings = embeddings,
        Err(error) => tracing::warn!("Entity embedding sweep failed: {error}"),
    }
    if let Some(cutoff) = failed_work_recovery::recovery_cutoff_from_env() {
        tracing::warn!("Starting failed-work recovery for space {space_id} (cutoff {cutoff})");
        match failed_work_recovery::run_failed_work_recovery(storage, space_id, cutoff).await {
            Ok(recovery) => result.failed_work_recovery = recovery,
            Err(error) => tracing::warn!("Failed-work recovery failed: {error}"),
        }
    }
    info!("Maintenance complete for space {space_id}: {result}");
    Ok(result)
}

/// The actual run is a decision input, not just another run for this work ID.
async fn capture_maintenance_run(
    planning: &crate::maintenance_commit::PlanningTruthMaintenanceStorage,
    storage: &elephant_storage::StorageClient,
    space: &str,
    fence: &elephant_types::publication::AttemptFence,
    run_id: &RecordId,
) -> Result<(), crate::PipelineError> {
    let run = elephant_storage::WorkRunRepository::new(storage.clone())
        .get(space, run_id)
        .await?
        .ok_or_else(|| crate::PipelineError::NotFound(run_id.to_string()))?;
    let saved = run.metrics.custom.get("attempt_fence").and_then(|value| {
        serde_json::from_value::<elephant_types::publication::AttemptFence>(value.clone()).ok()
    });
    if fence.space_id != space
        || run.work_item.id() != &fence.work_item_id
        || saved.as_ref() != Some(fence)
        || run.base.deleted_at.is_some()
    {
        return Err(crate::PipelineError::InvalidState(
            "Maintenance work run differs from the original attempt".into(),
        ));
    }
    planning.observe(&run)
}

/// Maximum entities (re-)embedded per sweep cycle: bounds provider cost per
/// tick; the next tick continues where this one left off.
const MAX_EMBEDS_PER_SWEEP: usize = 256;
/// Candidate scan bound per sweep cycle.
const MAX_EMBED_CANDIDATES_PER_SWEEP: usize = 4_096;
/// Texts per provider call.
const EMBED_BATCH_SIZE: usize = 64;

/// The text an entity's semantic-search embedding is computed from.
#[must_use]
pub fn entity_embedding_text(
    display_name: &str,
    aliases: &[String],
    summary: Option<&str>,
) -> String {
    let mut text = display_name.trim().to_string();
    if !aliases.is_empty() {
        text.push('\n');
        text.push_str(&aliases.join(", "));
    }
    if let Some(summary) = summary.map(str::trim).filter(|s| !s.is_empty()) {
        text.push('\n');
        text.push_str(summary);
    }
    text
}

/// Stable hash of the embedded text, stored alongside the vector so the
/// sweep can skip entities whose name/aliases/summary have not changed.
#[must_use]
pub fn entity_embedding_text_hash(text: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(text.as_bytes());
    format!("{digest:x}")
}

/// Embeds entities whose name/aliases/summary changed since their stored
/// embedding (or that have none), writing vectors through the storage layer.
/// This is the ONLY production writer of entity embeddings — the search read
/// path never embeds entities.
///
/// # Errors
///
/// Returns an error if listing candidates fails. Individual embed/store
/// failures are logged and counted, not fatal.
pub async fn run_entity_embedding_sweep(
    storage: &elephant_storage::StorageClient,
    space_id: &str,
    provider: &dyn EmbeddingProvider,
) -> Result<EntityEmbeddingSweepResult, crate::error::PipelineError> {
    let repo = EntityRepository::new(storage.clone());
    let model = provider.model().to_string();
    let candidates = if model.starts_with("recall:") {
        repo.list_embedding_candidates_for_policy(
            space_id,
            &model,
            provider.dimensions(),
            MAX_EMBED_CANDIDATES_PER_SWEEP,
        )
        .await?
    } else {
        repo.list_embedding_candidates(space_id, MAX_EMBED_CANDIDATES_PER_SWEEP)
            .await?
    };
    let mut result = EntityEmbeddingSweepResult {
        scanned: candidates.len(),
        ..EntityEmbeddingSweepResult::default()
    };

    let mut stale: Vec<(elephant_storage::EntityEmbeddingCandidate, String, String)> = Vec::new();
    for candidate in candidates {
        let text = entity_embedding_text(
            &candidate.display_name,
            &candidate.aliases,
            candidate.summary.as_deref(),
        );
        if text.is_empty() {
            result.skipped += 1;
            continue;
        }
        let hash = entity_embedding_text_hash(&text);
        let unchanged = candidate.embedding_model.as_deref() == Some(model.as_str())
            && candidate.embedding_text_hash.as_deref() == Some(hash.as_str())
            && candidate.embedding_dims == Some(provider.dimensions());
        if unchanged {
            result.skipped += 1;
            continue;
        }
        if stale.len() >= MAX_EMBEDS_PER_SWEEP {
            result.deferred += 1;
            continue;
        }
        stale.push((candidate, text, hash));
    }

    for chunk in stale.chunks(EMBED_BATCH_SIZE) {
        let texts: Vec<String> = chunk.iter().map(|(_, text, _)| text.clone()).collect();
        let vectors = match provider.embed(&texts).await {
            Ok(vectors) => vectors,
            Err(e) => {
                tracing::warn!("Entity embedding batch failed for space {space_id}: {e}");
                result.failed += chunk.len();
                continue;
            }
        };
        for ((candidate, _, hash), vector) in chunk.iter().zip(vectors) {
            match repo
                .set_name_embedding(space_id, &candidate.entity_id, &vector, &model, hash)
                .await
            {
                Ok(()) => result.embedded += 1,
                Err(e) => {
                    tracing::warn!("Failed to store embedding for {}: {e}", candidate.entity_id);
                    result.failed += 1;
                }
            }
        }
    }

    Ok(result)
}

#[cfg(test)]
async fn run_entity_hygiene_sweep(
    storage: &elephant_storage::StorageClient,
    space_id: &str,
) -> Result<EntityHygieneSweepResult, crate::error::PipelineError> {
    let planning = crate::maintenance_commit::PlanningTruthMaintenanceStorage::new(
        storage.clone(),
        space_id,
        None,
    )
    .await?;
    let result = atomic_graph::hygiene(&planning, storage, space_id).await?;
    planning
        .commit(
            "run_entity_hygiene_sweep",
            space_id,
            &serde_json::json!({"space":space_id}),
            &result,
        )
        .await?;
    Ok(result)
}

#[cfg(test)]
async fn run_assertion_dedupe_sweep(
    storage: &elephant_storage::StorageClient,
    space_id: &str,
) -> Result<AssertionSweepResult, crate::error::PipelineError> {
    let planning = crate::maintenance_commit::PlanningTruthMaintenanceStorage::new(
        storage.clone(),
        space_id,
        None,
    )
    .await?;
    let result = atomic_graph::assertions(&planning, storage, space_id).await?;
    planning
        .commit(
            "run_assertion_dedupe_sweep",
            space_id,
            &serde_json::json!({"space":space_id}),
            &result,
        )
        .await?;
    Ok(result)
}

#[cfg(test)]
async fn run_relationship_dedupe_sweep(
    storage: &elephant_storage::StorageClient,
    space_id: &str,
) -> Result<RelationshipSweepResult, crate::error::PipelineError> {
    let planning = crate::maintenance_commit::PlanningTruthMaintenanceStorage::new(
        storage.clone(),
        space_id,
        None,
    )
    .await?;
    let result = atomic_graph::relationships(&planning, storage, space_id).await?;
    planning
        .commit(
            "run_relationship_dedupe_sweep",
            space_id,
            &serde_json::json!({"space":space_id}),
            &result,
        )
        .await?;
    Ok(result)
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct AssertionSlot {
    subject: String,
    object: String,
    predicate: String,
}

impl AssertionSlot {
    fn from_assertion(assertion: &Assertion) -> Option<Self> {
        let subject = assertion.subject_entity.as_ref()?.id().to_string();
        let object = assertion_object_slot(assertion)?;
        let predicate = assertion
            .predicate_id
            .as_deref()
            .unwrap_or(assertion.predicate_raw.as_str())
            .trim()
            .to_string();
        if predicate.is_empty() {
            return None;
        }
        let predicate = predicate_slot_key(&predicate);
        if predicate.is_empty() {
            return None;
        }
        Some(Self {
            subject,
            object,
            predicate,
        })
    }
}

fn assertion_object_slot(assertion: &Assertion) -> Option<String> {
    let object_text = assertion.object_text.as_deref().map(str::trim);

    if let Some(value) = object_text.and_then(canonical_scalar_slot_value) {
        return Some(format!("scalar:{value}"));
    }

    if let Some(object_entity) = assertion.object_entity.as_ref() {
        return Some(format!("entity:{}", object_entity.id()));
    }

    if let Some(literal) = assertion.object_literal.as_ref() {
        let literal_value = canonical_literal_slot_value(literal)?;
        if let Some(label) = object_text
            .filter(|label| !label.is_empty())
            .filter(|label| canonical_scalar_slot_value(label).is_none())
        {
            let label = normalize_entity_label(label);
            if !label.is_empty() {
                return Some(format!("literal:{literal_value}|label:{label}"));
            }
        }
        return Some(format!("scalar:{literal_value}"));
    }

    object_text
        .filter(|label| !label.is_empty())
        .map(|label| format!("text:{}", normalize_entity_label(label)))
}

fn canonical_literal_slot_value(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::Null => None,
        serde_json::Value::Bool(value) => Some(value.to_string()),
        serde_json::Value::Number(value) => Some(value.to_string()),
        serde_json::Value::String(value) => canonical_scalar_slot_value(value),
        other => Some(other.to_string()),
    }
}

fn canonical_scalar_slot_value(value: &str) -> Option<String> {
    let normalized = value
        .trim()
        .trim_matches(|ch: char| ch.is_ascii_punctuation() || ch.is_whitespace())
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();
    if normalized.is_empty() || !looks_like_scalar_value(&normalized) {
        return None;
    }

    if normalized
        .chars()
        .all(|ch| ch.is_ascii_digit() || ch.is_whitespace())
    {
        return Some(normalized.split_whitespace().collect::<String>());
    }

    Some(normalized)
}

fn looks_like_scalar_value(value: &str) -> bool {
    if matches!(
        value,
        "true"
            | "false"
            | "yes"
            | "no"
            | "ja"
            | "nej"
            | "active"
            | "inactive"
            | "approved"
            | "candidate"
            | "confirmed"
            | "inferred"
            | "merged"
            | "pending"
            | "projected"
            | "resolved"
            | "superseded"
            | "uncertain"
            | "unknown"
    ) {
        return true;
    }
    let has_digit = value.chars().any(|ch| ch.is_ascii_digit());
    if !has_digit {
        return value.contains('@') && value.contains('.');
    }

    let lower = value.to_ascii_lowercase();
    if lower.contains('@') && lower.contains('.') {
        return true;
    }
    if is_clock_time_like(&lower)
        || is_ipv4_like(&lower)
        || is_iso_date_like(&lower)
        || is_semver_like(&lower)
    {
        return true;
    }
    if lower
        .chars()
        .all(|ch| ch.is_ascii_digit() || matches!(ch, ' ' | '-' | '+' | '(' | ')' | '.'))
    {
        return true;
    }
    let currency_tokens = ["kr", "sek", "usd", "eur", "gbp", "$", "€", "£"];
    if currency_tokens
        .iter()
        .any(|token| lower.split_whitespace().any(|part| part == *token) || lower.contains(token))
    {
        return true;
    }
    let compact = lower
        .chars()
        .filter(|ch| ch.is_ascii_hexdigit())
        .collect::<String>();
    compact.len() >= 16 && compact.len() == lower.chars().filter(|ch| !ch.is_whitespace()).count()
}

fn is_ipv4_like(value: &str) -> bool {
    let parts = value.split('.').collect::<Vec<_>>();
    parts.len() == 4
        && parts.iter().all(|part| {
            !part.is_empty()
                && part.len() <= 3
                && part.chars().all(|ch| ch.is_ascii_digit())
                && part.parse::<u8>().is_ok()
        })
}

fn is_iso_date_like(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() >= 10
        && bytes[0..4].iter().all(u8::is_ascii_digit)
        && bytes[4] == b'-'
        && bytes[5..7].iter().all(u8::is_ascii_digit)
        && bytes[7] == b'-'
        && bytes[8..10].iter().all(u8::is_ascii_digit)
}

fn is_semver_like(value: &str) -> bool {
    let parts = value.split('.').collect::<Vec<_>>();
    parts.len() >= 2
        && parts
            .iter()
            .all(|part| !part.is_empty() && part.chars().all(|ch| ch.is_ascii_digit()))
}

fn is_clock_time_like(value: &str) -> bool {
    let Some((hours, minutes)) = value.split_once(':') else {
        return false;
    };
    !hours.is_empty()
        && hours.len() <= 2
        && minutes.len() == 2
        && hours.chars().all(|ch| ch.is_ascii_digit())
        && minutes.chars().all(|ch| ch.is_ascii_digit())
        && hours.parse::<u8>().is_ok_and(|value| value <= 23)
        && minutes.parse::<u8>().is_ok_and(|value| value <= 59)
}

fn predicate_slot_key(value: &str) -> String {
    let normalized = value
        .replace(['_', '-'], " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();
    normalized
        .split_whitespace()
        .filter(|token| !matches!(*token, "is" | "a" | "an" | "the" | "of" | "or" | "to"))
        .collect::<Vec<_>>()
        .join(" ")
}

fn assertion_winner_order(left: &Assertion, right: &Assertion) -> std::cmp::Ordering {
    right
        .resolution_confidence
        .unwrap_or(0.0)
        .partial_cmp(&left.resolution_confidence.unwrap_or(0.0))
        .unwrap_or(std::cmp::Ordering::Equal)
        .then_with(|| {
            right
                .confidence
                .partial_cmp(&left.confidence)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .then_with(|| left.base.created_at.cmp(&right.base.created_at))
        .then_with(|| left.base.id.to_string().cmp(&right.base.id.to_string()))
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct RelationshipSlot {
    subject: String,
    object: String,
    rel_type: String,
}

impl RelationshipSlot {
    fn from_rel(rel: &Rel) -> Self {
        Self {
            subject: rel.subject.id().to_string(),
            object: rel.object.id().to_string(),
            rel_type: rel.rel_type.clone(),
        }
    }
}

fn relationship_winner_order(left: &Rel, right: &Rel) -> std::cmp::Ordering {
    claim_status_rank(right.status)
        .cmp(&claim_status_rank(left.status))
        .then_with(|| {
            right
                .confidence
                .partial_cmp(&left.confidence)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .then_with(|| left.base.created_at.cmp(&right.base.created_at))
        .then_with(|| left.base.id.to_string().cmp(&right.base.id.to_string()))
}

fn claim_status_rank(status: ClaimStatus) -> u8 {
    match status {
        ClaimStatus::Approved => 3,
        ClaimStatus::Candidate => 2,
        ClaimStatus::Disputed => 1,
        ClaimStatus::Superseded => 0,
    }
}

fn merged_claim_status(left: ClaimStatus, right: ClaimStatus) -> ClaimStatus {
    if claim_status_rank(left) >= claim_status_rank(right) {
        left
    } else {
        right
    }
}

fn evidence_identity_key(evidence: &EvidenceRef) -> String {
    format!(
        "{}:{}:{}:{}",
        evidence.assertion.id(),
        evidence.span.revision.id(),
        evidence.span.start,
        evidence.span.end,
    )
}

fn normalize_entity_label(value: &str) -> String {
    value
        .trim()
        .trim_matches(|ch: char| ch.is_ascii_punctuation() || ch.is_whitespace())
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

fn index_active_entity_for_deleted_survivor(
    entity: &Entity,
    active_identity_keys: &mut std::collections::HashMap<String, Option<RecordId>>,
    active_display_names: &mut std::collections::HashMap<String, Option<RecordId>>,
) {
    for key in entity
        .identity_keys
        .iter()
        .filter_map(|key| deleted_survivor_match_key(entity, key))
    {
        insert_unique_entity_match(active_identity_keys, key, entity.base.id.clone());
    }
    if let Some(key) = deleted_survivor_match_key(entity, &entity.display_name) {
        insert_unique_entity_match(active_display_names, key, entity.base.id.clone());
    }
}

fn deleted_entity_survivor(
    entity: &Entity,
    active_identity_keys: &std::collections::HashMap<String, Option<RecordId>>,
    active_display_names: &std::collections::HashMap<String, Option<RecordId>>,
) -> Option<RecordId> {
    for key in entity
        .identity_keys
        .iter()
        .filter_map(|key| deleted_survivor_match_key(entity, key))
    {
        if let Some(Some(survivor_id)) = active_identity_keys.get(&key) {
            if survivor_id != &entity.base.id {
                return Some(survivor_id.clone());
            }
        }
    }

    let key = deleted_survivor_match_key(entity, &entity.display_name)?;
    active_display_names
        .get(&key)
        .and_then(Clone::clone)
        .filter(|survivor_id| survivor_id != &entity.base.id)
}

fn insert_unique_entity_match(
    index: &mut std::collections::HashMap<String, Option<RecordId>>,
    key: String,
    entity_id: RecordId,
) {
    index
        .entry(key)
        .and_modify(|existing| {
            if existing.as_ref().is_some_and(|id| id != &entity_id) {
                *existing = None;
            }
        })
        .or_insert_with(|| Some(entity_id));
}

fn deleted_survivor_match_key(entity: &Entity, value: &str) -> Option<String> {
    let key = normalize_entity_label(value);
    if key.is_empty()
        || canonical_scalar_slot_value(&key).is_some()
        || should_ignore_deleted_survivor_key(entity, &key)
    {
        return None;
    }
    Some(key)
}

fn should_ignore_deleted_survivor_key(entity: &Entity, key: &str) -> bool {
    if key.len() <= 2
        || matches!(
            key,
            "unknown" | "untitled" | "none" | "null" | "n/a" | "na" | "test"
        )
    {
        return true;
    }
    let is_single_token = !key.chars().any(char::is_whitespace);
    let entity_type = normalize_entity_label(entity.entity_type.as_str());
    let person_like = matches!(
        entity_type.as_str(),
        "person" | "people" | "human" | "contact"
    );

    // A deleted person named only "Alex" should not silently steal edges to
    // "Alex Example"; require richer evidence before redirecting people.
    person_like && is_single_token
}

fn is_non_entity_composite_label(label: &str) -> bool {
    let lower = label.to_ascii_lowercase();
    lower.contains("<>")
        || lower.contains("->")
        || lower.contains("=>")
        || lower.contains(" from ")
            && lower.contains(" to ")
            && label.chars().any(|ch| ch == ':' || ch == '<' || ch == '>')
}

fn should_retire_entity_label(entity: &Entity) -> bool {
    if entity.known_safe {
        return false;
    }
    let label = entity.display_name.trim();
    if label.is_empty() {
        return true;
    }
    is_non_entity_composite_label(label) || canonical_scalar_slot_value(label).is_some()
}

async fn prepare_predicate_sweep(
    planning: &crate::maintenance_commit::PlanningTruthMaintenanceStorage,
    storage: &elephant_storage::StorageClient,
    space_id: &str,
    provider: &dyn EmbeddingProvider,
    work_run: Option<&RecordId>,
) -> crate::PipelineResult<PredicateSweepResult> {
    let mut result = atomic_graph::predicates(planning, storage, space_id).await?;
    let hygiene = predicate_hygiene::prepare_predicate_hygiene_sweep(
        planning,
        storage,
        space_id,
        &PredicateHygieneConfig::default(),
        provider,
        work_run,
    )
    .await?;
    result.scanned = hygiene.predicates_scanned;
    result.aliases_created = hygiene.aliases_created;
    result.proposals_created = hygiene.proposals_created;
    Ok(result)
}

#[cfg(test)]
async fn run_predicate_sweep(
    storage: &elephant_storage::StorageClient,
    space_id: &str,
    provider: &dyn EmbeddingProvider,
) -> crate::PipelineResult<PredicateSweepResult> {
    let planning = crate::maintenance_commit::PlanningTruthMaintenanceStorage::new(
        storage.clone(),
        space_id,
        None,
    )
    .await?;
    let result = prepare_predicate_sweep(&planning, storage, space_id, provider, None).await?;
    planning.audit_pending_effects("predicate_sweep").await?;
    planning
        .commit("predicate_sweep", space_id, &serde_json::json!({}), &result)
        .await?;
    Ok(result)
}

#[cfg(test)]
async fn sync_predicate_registry_from_graph(
    storage: &elephant_storage::StorageClient,
    space_id: &str,
) -> crate::PipelineResult<PredicateSweepResult> {
    let planning = crate::maintenance_commit::PlanningTruthMaintenanceStorage::new(
        storage.clone(),
        space_id,
        None,
    )
    .await?;
    let result = atomic_graph::predicates(&planning, storage, space_id).await?;
    planning
        .audit_pending_effects("predicate_registry_sync")
        .await?;
    planning
        .commit(
            "predicate_registry_sync",
            space_id,
            &serde_json::json!({}),
            &result,
        )
        .await?;
    Ok(result)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::collections::HashMap;

    use elephant_storage::{
        AssertionRepository, PredicateAliasRepository, PredicateRegistryRepository, RelRepository,
        Repository, StorageClient, WorkArtifactRepository,
    };
    use elephant_types::ids::{RecordId, TableName};
    use elephant_types::records::assertion::EvidenceSpan;
    use elephant_types::records::base::{BaseFields, Provenance, SubjectsState};
    use elephant_types::records::entity::{Entity, EntityType};
    use elephant_types::records::predicate::{PredicateRegistry, SemanticsStatus};
    use elephant_types::records::rel::{EvidenceRef, Rel, TimeBasis};
    use elephant_types::Link;

    use super::*;

    struct ControlledEmbeddingProvider {
        embeddings: HashMap<String, Vec<f32>>,
        dimensions: usize,
    }

    impl ControlledEmbeddingProvider {
        fn new(entries: Vec<(&str, Vec<f32>)>) -> Self {
            let dimensions = entries.first().map(|(_, vector)| vector.len()).unwrap_or(0);
            Self {
                embeddings: entries
                    .into_iter()
                    .map(|(text, vector)| (text.to_string(), vector))
                    .collect(),
                dimensions,
            }
        }
    }

    #[async_trait::async_trait]
    impl EmbeddingProvider for ControlledEmbeddingProvider {
        fn model(&self) -> &str {
            "controlled-maintenance-test"
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
                .map(|text| {
                    self.embeddings
                        .get(text)
                        .cloned()
                        .unwrap_or_else(|| vec![0.0; self.dimensions])
                })
                .collect())
        }
    }

    async fn setup_backend(space: &str, embedded: bool) -> StorageClient {
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
                    space.into(),
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

    async fn setup_storage(space: &str) -> StorageClient {
        setup_backend(space, false).await
    }

    async fn insert_predicate(
        storage: &StorageClient,
        space_id: &str,
        predicate_id: &str,
        description: &str,
        usage_count: u64,
    ) {
        let repo = PredicateRegistryRepository::new(storage.clone());
        let predicate = PredicateRegistry {
            base: BaseFields::new(
                RecordId::new_v4(TableName::PredicateRegistry),
                space_id.to_string(),
                "maintenance_test".to_string(),
            ),
            predicate_id: predicate_id.to_string(),
            description: Some(description.to_string()),
            status: "active".to_string(),
            semantics: None,
            semantics_status: SemanticsStatus::Default,
            first_seen_at: None,
            usage_count,
            last_used_at: None,
        };
        repo.create(predicate).await.expect("create predicate");
    }

    fn test_entity(space_id: &str, name: &str, entity_type: &str) -> Entity {
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::Entity),
            space_id.into(),
            "maintenance_test".into(),
        );
        base.subjects_state = SubjectsState::Complete;
        Entity::new(base, EntityType::new(entity_type), name.into())
    }

    async fn persist_test_endpoints(storage: &StorageClient, space: &str) -> (RecordId, RecordId) {
        let repo = EntityRepository::new(storage.clone());
        let subject = repo
            .create(test_entity(space, "Alice", "person"))
            .await
            .expect("actual subject");
        let object = repo
            .create(test_entity(space, "Acme", "organization"))
            .await
            .expect("actual object");
        (subject.base.id, object.base.id)
    }

    fn test_rel(
        space_id: &str,
        subject_id: &RecordId,
        object_id: &RecordId,
        rel_type: &str,
        status: ClaimStatus,
        confidence: f32,
    ) -> Rel {
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::Rel),
            space_id.to_string(),
            "maintenance_test".to_string(),
        );
        base.provenance = Provenance::default();
        base.subjects = vec![Link::new(subject_id.clone())];
        base.subjects_state = SubjectsState::Complete;

        Rel {
            base,
            subject: Link::new(subject_id.clone()),
            object: Link::new(object_id.clone()),
            rel_type: rel_type.to_string(),
            status,
            confidence,
            validity_window: None,
            // Independently authored relationship; evidence is added only by
            // tests that persist the actual supporting assertion.
            evidence: vec![],
            temporal_audit: None,
        }
    }

    fn test_assertion(
        space_id: &str,
        subject_id: &RecordId,
        object_id: &RecordId,
        predicate: &str,
        resolution_confidence: f32,
        confidence: f32,
    ) -> Assertion {
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::Assertion),
            space_id.to_string(),
            "maintenance_test".to_string(),
        );
        base.provenance = Provenance::default();
        base.subjects = vec![Link::new(subject_id.clone()), Link::new(object_id.clone())];
        base.subjects_state = SubjectsState::Complete;

        Assertion {
            base,
            subject_text: "Source Adult".to_string(),
            predicate_raw: predicate.to_string(),
            predicate_id: Some(predicate.to_string()),
            object_text: Some("Source Child".to_string()),
            object_literal: None,
            evidence: EvidenceSpan {
                revision: Link::new(RecordId::new_v4(TableName::DocRevision)),
                node: None,
                start: 0,
                end: 20,
                quote_hash: "sha256:test".to_string(),
            },
            source_event_time: None,
            observed_at: chrono::Utc::now(),
            asserted_at: chrono::Utc::now(),
            assertion_key: format!(
                "{}:{}:{}:{}",
                subject_id,
                predicate,
                object_id,
                RecordId::new_v4(TableName::Assertion)
            ),
            resolution_status: ResolutionStatus::Resolved,
            resolution_method: Some("test".to_string()),
            resolution_confidence: Some(resolution_confidence),
            retry_state: Default::default(),
            subject_entity: Some(Link::new(subject_id.clone())),
            object_entity: Some(Link::new(object_id.clone())),
            confidence,
            subject_entity_type: Some("person".to_string()),
            object_entity_type: Some("person".to_string()),
            significance: Some("high".to_string()),
            lifecycle_tier: "active".to_string(),
        }
    }

    #[tokio::test]
    async fn test_predicate_sync_registers_relationship_predicates() {
        let space_id = "maintenance_predicate_rel_space";
        let storage = setup_storage(space_id).await;
        let entity_repo = EntityRepository::new(storage.clone());
        let rel_repo = RelRepository::new(storage.clone());

        let subject = entity_repo
            .create(test_entity(space_id, "Ada Example", "person"))
            .await
            .expect("create subject");
        let object = entity_repo
            .create(test_entity(space_id, "Example School", "organization"))
            .await
            .expect("create object");

        rel_repo
            .create(test_rel(
                space_id,
                &subject.base.id,
                &object.base.id,
                "attends_school",
                ClaimStatus::Approved,
                0.9,
            ))
            .await
            .expect("create relationship");

        let result = sync_predicate_registry_from_graph(&storage, space_id)
            .await
            .expect("sync");
        assert_eq!(result.scanned, 1);
        assert_eq!(result.registered, 1);

        let predicate = PredicateRegistryRepository::new(storage)
            .find_by_predicate_id(space_id, "attends_school")
            .await
            .expect("find predicate");
        assert!(predicate.is_some());
    }

    #[tokio::test]
    async fn test_entity_hygiene_retires_non_entity_source_fragments() {
        let space_id = "maintenance_entity_hygiene_space";
        let storage = setup_storage(space_id).await;
        let entity_repo = EntityRepository::new(storage.clone());
        let rel_repo = RelRepository::new(storage.clone());
        let assertion_repo = AssertionRepository::new(storage.clone());

        let person = entity_repo
            .create(test_entity(space_id, "Ada Example", "person"))
            .await
            .expect("create person");
        let org = entity_repo
            .create(test_entity(space_id, "Smith, Jones and Co", "org"))
            .await
            .expect("create org");
        let routed_title = entity_repo
            .create(test_entity(
                space_id,
                "Video call: ProjectA <> ProjectB",
                "topic",
            ))
            .await
            .expect("create routed title");
        let clock_time = entity_repo
            .create(test_entity(space_id, "15:30", "topic"))
            .await
            .expect("create scalar clock time");
        let boolean_value = entity_repo
            .create(test_entity(space_id, "true", "topic"))
            .await
            .expect("create scalar boolean");
        let status_value = entity_repo
            .create(test_entity(space_id, "confirmed", "topic"))
            .await
            .expect("create scalar status");
        let datetime_value = entity_repo
            .create(test_entity(space_id, "2026-05-20T16:00:00Z", "topic"))
            .await
            .expect("create scalar datetime");
        let personal_id = entity_repo
            .create(test_entity(space_id, "123-45-6789", "person"))
            .await
            .expect("create scalar personal id");
        let ip_address = entity_repo
            .create(test_entity(space_id, "66.40.184.65", "topic"))
            .await
            .expect("create scalar ip address");
        let address = entity_repo
            .create(test_entity(
                space_id,
                "1200 Market Street, Springfield, IL",
                "location",
            ))
            .await
            .expect("create address");
        let stale_deleted = entity_repo
            .create(test_entity(space_id, "Alex", "person"))
            .await
            .expect("create stale deleted entity");
        entity_repo
            .delete(space_id, &stale_deleted.base.id)
            .await
            .expect("soft delete stale entity before hygiene");
        let mut active_duplicate_survivor =
            test_entity(space_id, "Example Regional Support", "org");
        active_duplicate_survivor.identity_keys = vec!["example regional support".to_string()];
        let active_duplicate_survivor = entity_repo
            .create(active_duplicate_survivor)
            .await
            .expect("create active duplicate survivor");
        let mut deleted_duplicate = test_entity(space_id, "Example Regional Support", "org");
        deleted_duplicate.identity_keys = vec!["example regional support".to_string()];
        let deleted_duplicate = entity_repo
            .create(deleted_duplicate)
            .await
            .expect("create deleted duplicate");
        entity_repo
            .delete(space_id, &deleted_duplicate.base.id)
            .await
            .expect("soft delete deleted duplicate before hygiene");

        let stale_rel = rel_repo
            .create(test_rel(
                space_id,
                &person.base.id,
                &stale_deleted.base.id,
                "related_to",
                ClaimStatus::Approved,
                0.8,
            ))
            .await
            .expect("create stale rel");
        let stale_assertion = assertion_repo
            .create(test_assertion(
                space_id,
                &stale_deleted.base.id,
                &person.base.id,
                "related_to",
                0.8,
                0.8,
            ))
            .await
            .expect("create stale assertion");
        let duplicate_rel = rel_repo
            .create(test_rel(
                space_id,
                &person.base.id,
                &deleted_duplicate.base.id,
                "connected_to",
                ClaimStatus::Approved,
                0.8,
            ))
            .await
            .expect("create duplicate rel");
        let duplicate_assertion = assertion_repo
            .create(test_assertion(
                space_id,
                &person.base.id,
                &deleted_duplicate.base.id,
                "connected_to",
                0.8,
                0.8,
            ))
            .await
            .expect("create duplicate assertion");

        let result = run_entity_hygiene_sweep(&storage, space_id)
            .await
            .expect("entity hygiene");

        assert_eq!(result.scanned, 11);
        assert_eq!(result.retired, 7);
        assert_eq!(result.relationships_deleted, 1);
        assert_eq!(result.relationships_repointed, 1);
        assert_eq!(result.assertions_superseded, 1);
        assert_eq!(result.assertions_repointed, 1);

        let entities = entity_repo
            .get_many(
                space_id,
                &[
                    person.base.id.clone(),
                    org.base.id.clone(),
                    routed_title.base.id.clone(),
                    clock_time.base.id.clone(),
                    boolean_value.base.id.clone(),
                    status_value.base.id.clone(),
                    datetime_value.base.id.clone(),
                    personal_id.base.id.clone(),
                    ip_address.base.id.clone(),
                    address.base.id.clone(),
                    active_duplicate_survivor.base.id.clone(),
                    deleted_duplicate.base.id.clone(),
                ],
                true,
            )
            .await
            .expect("get entities");
        let by_id = entities
            .into_iter()
            .map(|entity| (entity.base.id.clone(), entity))
            .collect::<std::collections::HashMap<_, _>>();

        assert!(by_id[&person.base.id].base.deleted_at.is_none());
        assert!(by_id[&org.base.id].base.deleted_at.is_none());
        assert!(by_id[&routed_title.base.id].base.deleted_at.is_some());
        assert!(by_id[&clock_time.base.id].base.deleted_at.is_some());
        assert!(by_id[&boolean_value.base.id].base.deleted_at.is_some());
        assert!(by_id[&status_value.base.id].base.deleted_at.is_some());
        assert!(by_id[&datetime_value.base.id].base.deleted_at.is_some());
        assert!(by_id[&personal_id.base.id].base.deleted_at.is_some());
        assert!(by_id[&ip_address.base.id].base.deleted_at.is_some());
        assert!(by_id[&address.base.id].base.deleted_at.is_none());
        assert!(by_id[&active_duplicate_survivor.base.id]
            .base
            .deleted_at
            .is_none());
        assert!(by_id[&deleted_duplicate.base.id].base.deleted_at.is_some());

        let stale_rel_after = rel_repo
            .get_with_deleted(space_id, &stale_rel.base.id)
            .await
            .expect("get stale rel")
            .expect("stale rel exists");
        assert!(stale_rel_after.base.deleted_at.is_some());

        let stale_assertion_after = assertion_repo
            .get(space_id, &stale_assertion.base.id)
            .await
            .expect("get stale assertion")
            .expect("stale assertion exists");
        assert_eq!(
            stale_assertion_after.resolution_status,
            ResolutionStatus::Superseded
        );

        let duplicate_rel_after = rel_repo
            .get_with_deleted(space_id, &duplicate_rel.base.id)
            .await
            .expect("get duplicate rel")
            .expect("duplicate rel exists");
        assert!(duplicate_rel_after.base.deleted_at.is_some());
        let neighbors = rel_repo
            .get_entities_within_n_hops(space_id, &person.base.id, 1)
            .await
            .expect("get graph neighbors");
        assert!(neighbors
            .iter()
            .any(|neighbor| neighbor.entity_id == active_duplicate_survivor.base.id));

        let duplicate_assertion_after = assertion_repo
            .get(space_id, &duplicate_assertion.base.id)
            .await
            .expect("get duplicate assertion")
            .expect("duplicate assertion exists");
        assert_eq!(
            duplicate_assertion_after
                .object_entity
                .as_ref()
                .map(Link::id),
            Some(&active_duplicate_survivor.base.id)
        );
    }

    #[tokio::test]
    async fn test_entity_hygiene_repoints_merged_entity_references() {
        let space_id = "maintenance_merged_reference_hygiene_space";
        let storage = setup_storage(space_id).await;
        let entity_repo = EntityRepository::new(storage.clone());
        let rel_repo = RelRepository::new(storage.clone());
        let assertion_repo = AssertionRepository::new(storage.clone());

        let survivor = entity_repo
            .create(test_entity(space_id, "Alex Example", "person"))
            .await
            .expect("create survivor");
        let mut merged = test_entity(space_id, "Casey Example", "person");
        merged.status = EntityStatus::Merged;
        merged.redirect_to = Some(Link::new(survivor.base.id.clone()));
        let merged = entity_repo.create(merged).await.expect("create merged");
        let other = entity_repo
            .create(test_entity(space_id, "Riley Example", "person"))
            .await
            .expect("create other");

        let rel = rel_repo
            .create(test_rel(
                space_id,
                &other.base.id,
                &merged.base.id,
                "spouse_or_partner_of",
                ClaimStatus::Approved,
                0.95,
            ))
            .await
            .expect("create rel");
        let assertion = assertion_repo
            .create(test_assertion(
                space_id,
                &other.base.id,
                &merged.base.id,
                "spouse_or_partner_of",
                0.95,
                0.95,
            ))
            .await
            .expect("create assertion");

        let result = run_entity_hygiene_sweep(&storage, space_id)
            .await
            .expect("entity hygiene");

        assert_eq!(result.retired, 0);
        assert_eq!(result.relationships_repointed, 1);
        assert_eq!(result.assertions_repointed, 1);

        let rel_after = rel_repo
            .get_with_deleted(space_id, &rel.base.id)
            .await
            .expect("get rel")
            .expect("old rel remains as tombstone only");
        assert!(rel_after.base.deleted_at.is_some());

        let assertion_after = assertion_repo
            .get(space_id, &assertion.base.id)
            .await
            .expect("get assertion")
            .expect("assertion exists");
        assert_eq!(
            assertion_after.object_entity.as_ref().map(Link::id),
            Some(&survivor.base.id)
        );
    }

    #[tokio::test]
    async fn test_predicate_sweep_runs_hygiene_not_only_registry_sync() {
        let space_id = "maintenance_predicate_hygiene_space";
        let storage = setup_storage(space_id).await;
        insert_predicate(
            &storage,
            space_id,
            "works_at",
            "Employment relationship",
            100,
        )
        .await;
        insert_predicate(
            &storage,
            space_id,
            "employed_by",
            "Employment relationship",
            30,
        )
        .await;

        let provider = ControlledEmbeddingProvider::new(vec![
            ("Employment relationship", vec![1.0, 0.0, 0.0]),
            ("Employment relationship", vec![1.0, 0.0, 0.0]),
        ]);

        let result = run_predicate_sweep(&storage, space_id, &provider)
            .await
            .expect("predicate sweep");

        assert_eq!(result.scanned, 2);
        assert_eq!(result.aliases_created, 0);
        assert_eq!(result.proposals_created, 1);

        let alias = PredicateAliasRepository::new(storage.clone())
            .find_active_by_raw(space_id, "employed_by")
            .await
            .expect("query alias");
        assert!(
            alias.is_none(),
            "maintenance predicate hygiene must not create active aliases from embedding similarity"
        );

        let artifacts = WorkArtifactRepository::new(storage)
            .list(space_id, 10, None)
            .await
            .expect("list predicate hygiene artifacts");
        assert_eq!(artifacts.len(), 1);
    }

    #[tokio::test]
    async fn test_relationship_dedupe_merges_exact_slot_evidence() {
        let space_id = "maintenance_rel_dedupe_space";
        let storage = setup_storage(space_id).await;
        let rel_repo = RelRepository::new(storage.clone());
        let (subject_id, object_id) = persist_test_endpoints(&storage, space_id).await;

        let mut candidate = test_rel(
            space_id,
            &subject_id,
            &object_id,
            "attends",
            ClaimStatus::Candidate,
            0.7,
        );
        let mut approved = test_rel(
            space_id,
            &subject_id,
            &object_id,
            "attends",
            ClaimStatus::Approved,
            0.95,
        );
        let unrelated = test_rel(
            space_id,
            &subject_id,
            &object_id,
            "has_role",
            ClaimStatus::Candidate,
            0.8,
        );

        for row in [&mut candidate, &mut approved] {
            let mut support = test_assertion(
                space_id,
                &subject_id,
                &object_id,
                "attends",
                1.0,
                row.confidence,
            );
            support.base.security = row.base.security.clone();
            support.base.subjects = row.base.subjects.clone();
            support.base.subjects_state = row.base.subjects_state;
            support = AssertionRepository::new(storage.clone())
                .create(support)
                .await
                .expect("actual support");
            row.evidence.push(EvidenceRef {
                assertion: Link::new(support.base.id.clone()),
                span: support.evidence,
                time_basis: TimeBasis::ObservedAt,
                time_value: support.observed_at,
            });
        }
        rel_repo.create(candidate).await.expect("create candidate");
        rel_repo.create(approved).await.expect("create approved");
        rel_repo
            .create(unrelated.clone())
            .await
            .expect("create unrelated");

        let result = run_relationship_dedupe_sweep(&storage, space_id)
            .await
            .expect("dedupe");

        assert_eq!(result.scanned, 3);
        assert_eq!(result.duplicate_slots, 1);
        assert_eq!(result.merged, 1);
        assert_eq!(result.evidence_added, 1);

        let attends = rel_repo
            .find_for_slot(space_id, &subject_id, &object_id, "attends")
            .await
            .expect("attends slot");
        assert_eq!(attends.len(), 1);
        assert_eq!(attends[0].status, ClaimStatus::Approved);
        assert_eq!(attends[0].confidence, 0.95);
        assert_eq!(attends[0].evidence.len(), 2);

        let has_role = rel_repo
            .find_for_slot(space_id, &subject_id, &object_id, "has_role")
            .await
            .expect("has role slot");
        assert_eq!(has_role.len(), 1);
        assert_eq!(has_role[0].base.id, unrelated.base.id);
    }

    #[tokio::test]
    async fn test_assertion_dedupe_supersedes_resolved_duplicate_slots() {
        let space_id = "maintenance_assertion_dedupe_space";
        let storage = setup_storage(space_id).await;
        let assertion_repo = AssertionRepository::new(storage.clone());
        let subject_id = RecordId::new_v4(TableName::Entity);
        let object_id = RecordId::new_v4(TableName::Entity);

        let weaker = test_assertion(space_id, &subject_id, &object_id, "reports_to", 0.72, 0.7);
        let stronger = test_assertion(space_id, &subject_id, &object_id, "reports_to", 0.95, 0.9);
        let unrelated = test_assertion(space_id, &subject_id, &object_id, "has_role", 0.8, 0.8);
        let predicate_variant =
            test_assertion(space_id, &subject_id, &object_id, "reports to", 0.78, 0.75);

        assertion_repo
            .create(weaker.clone())
            .await
            .expect("create weaker");
        assertion_repo
            .create(stronger.clone())
            .await
            .expect("create stronger");
        assertion_repo
            .create(unrelated.clone())
            .await
            .expect("create unrelated");
        assertion_repo
            .create(predicate_variant.clone())
            .await
            .expect("create predicate variant");

        let result = run_assertion_dedupe_sweep(&storage, space_id)
            .await
            .expect("dedupe assertions");

        assert_eq!(result.scanned, 4);
        assert_eq!(result.duplicate_slots, 1);
        assert_eq!(result.superseded, 2);

        let stronger_after = assertion_repo
            .get(space_id, &stronger.base.id)
            .await
            .expect("get stronger")
            .expect("stronger exists");
        assert_eq!(stronger_after.resolution_status, ResolutionStatus::Resolved);

        let weaker_after = assertion_repo
            .get(space_id, &weaker.base.id)
            .await
            .expect("get weaker")
            .expect("weaker exists");
        assert_eq!(weaker_after.resolution_status, ResolutionStatus::Superseded);

        let unrelated_after = assertion_repo
            .get(space_id, &unrelated.base.id)
            .await
            .expect("get unrelated")
            .expect("unrelated exists");
        assert_eq!(
            unrelated_after.resolution_status,
            ResolutionStatus::Resolved
        );

        let predicate_variant_after = assertion_repo
            .get(space_id, &predicate_variant.base.id)
            .await
            .expect("get predicate variant")
            .expect("predicate variant exists");
        assert_eq!(
            predicate_variant_after.resolution_status,
            ResolutionStatus::Superseded
        );
    }

    #[tokio::test]
    async fn test_assertion_dedupe_matches_scalar_entity_and_literal_objects() {
        let space_id = "maintenance_assertion_scalar_dedupe_space";
        let storage = setup_storage(space_id).await;
        let assertion_repo = AssertionRepository::new(storage.clone());
        let subject_id = RecordId::new_v4(TableName::Entity);
        let object_id = RecordId::new_v4(TableName::Entity);
        let distinct_object_id = RecordId::new_v4(TableName::Entity);

        let mut entity_object = test_assertion(
            space_id,
            &subject_id,
            &object_id,
            "published_from_ip",
            0.9,
            0.9,
        );
        entity_object.object_text = Some("66.40.184.65".to_string());
        entity_object.object_entity_type = Some("identifier".to_string());

        let mut literal_object = test_assertion(
            space_id,
            &subject_id,
            &distinct_object_id,
            "published from ip",
            0.95,
            0.95,
        );
        literal_object.object_entity = None;
        literal_object.object_text = None;
        literal_object.object_literal = Some(serde_json::json!("66.40.184.65"));
        literal_object.object_entity_type = None;
        // Scalar representations may dedupe only inside the same authorization
        // partition; the object shape must not accidentally change that scope.
        literal_object.base.subjects = entity_object.base.subjects.clone();

        let mut real_entity = test_assertion(
            space_id,
            &subject_id,
            &distinct_object_id,
            "published_from_ip",
            0.8,
            0.8,
        );
        real_entity.object_text = Some("Office network".to_string());

        assertion_repo
            .create(entity_object.clone())
            .await
            .expect("create entity object assertion");
        assertion_repo
            .create(literal_object.clone())
            .await
            .expect("create literal object assertion");
        assertion_repo
            .create(real_entity.clone())
            .await
            .expect("create real entity assertion");

        let result = run_assertion_dedupe_sweep(&storage, space_id)
            .await
            .expect("dedupe scalar assertions");

        assert_eq!(result.scanned, 3);
        assert_eq!(result.duplicate_slots, 1);
        assert_eq!(result.superseded, 1);

        let entity_object_after = assertion_repo
            .get(space_id, &entity_object.base.id)
            .await
            .expect("get entity object")
            .expect("entity object exists");
        assert_eq!(
            entity_object_after.resolution_status,
            ResolutionStatus::Superseded
        );

        let literal_object_after = assertion_repo
            .get(space_id, &literal_object.base.id)
            .await
            .expect("get literal object")
            .expect("literal object exists");
        assert_eq!(
            literal_object_after.resolution_status,
            ResolutionStatus::Resolved
        );

        let real_entity_after = assertion_repo
            .get(space_id, &real_entity.base.id)
            .await
            .expect("get real entity")
            .expect("real entity exists");
        assert_eq!(
            real_entity_after.resolution_status,
            ResolutionStatus::Resolved
        );
    }

    #[tokio::test]
    async fn test_entity_embedding_sweep_embeds_then_skips_unchanged() {
        let space_id = "maintenance_embedding_space";
        let storage = setup_storage(space_id).await;
        let repo = EntityRepository::new(storage.clone());

        let mut researcher = test_entity(space_id, "Riley Kim", "person");
        researcher.summary = Some("neural networks researcher".to_string());
        let researcher_id = researcher.base.id.clone();
        repo.create(researcher).await.expect("create researcher");
        let club = test_entity(space_id, "Chess Club", "organization");
        repo.create(club).await.expect("create club");

        let researcher_text =
            entity_embedding_text("Riley Kim", &[], Some("neural networks researcher"));
        let club_text = entity_embedding_text("Chess Club", &[], None);
        let provider = ControlledEmbeddingProvider::new(vec![
            (researcher_text.as_str(), vec![1.0, 0.0, 0.0]),
            (club_text.as_str(), vec![0.0, 1.0, 0.0]),
        ]);

        // First sweep embeds both entities.
        let result = run_entity_embedding_sweep(&storage, space_id, &provider)
            .await
            .expect("first sweep");
        assert_eq!(result.embedded, 2, "both entities embedded: {result:?}");
        assert_eq!(result.failed, 0);

        // The vectors are live for semantic lookup.
        let hits = repo
            .vector_search_entities(space_id, &[1.0, 0.0, 0.0], 5)
            .await
            .expect("vector search");
        assert_eq!(
            hits.first().map(|hit| hit.entity_id.clone()),
            Some(researcher_id.clone())
        );

        // Second sweep: nothing changed, nothing re-embedded (hash gate).
        let result = run_entity_embedding_sweep(&storage, space_id, &provider)
            .await
            .expect("second sweep");
        assert_eq!(result.embedded, 0, "unchanged entities skipped: {result:?}");
        assert_eq!(result.skipped, 2);

        // Changing the summary makes exactly that entity stale again.
        let mut updated = repo
            .get(space_id, &researcher_id)
            .await
            .expect("get researcher")
            .expect("researcher exists");
        updated.summary = Some("optimization theory researcher".to_string());
        repo.update(space_id, &researcher_id, updated)
            .await
            .expect("update researcher");

        let result = run_entity_embedding_sweep(&storage, space_id, &provider)
            .await
            .expect("third sweep");
        assert_eq!(result.embedded, 1, "only the changed entity: {result:?}");
        assert_eq!(result.skipped, 1);

        // A dims change under the SAME model string still re-embeds
        // everything: dims participate in the gate, not just model + hash.
        let updated_researcher_text =
            entity_embedding_text("Riley Kim", &[], Some("optimization theory researcher"));
        let wider_provider = ControlledEmbeddingProvider::new(vec![
            (updated_researcher_text.as_str(), vec![1.0, 0.0, 0.0, 0.0]),
            (club_text.as_str(), vec![0.0, 1.0, 0.0, 0.0]),
        ]);
        assert_eq!(
            wider_provider.model(),
            provider.model(),
            "same model string is the point of this step"
        );
        let result = run_entity_embedding_sweep(&storage, space_id, &wider_provider)
            .await
            .expect("dims-change sweep");
        assert_eq!(
            result.embedded, 2,
            "dims change re-embeds despite unchanged model string: {result:?}"
        );
        assert_eq!(result.skipped, 0);
    }
    #[tokio::test]
    async fn semantic_phases_roll_back_together_after_late_transaction_fault() {
        use crate::maintenance_commit::PlanningTruthMaintenanceStorage;
        use elephant_storage::canonical_commit::{
            CanonicalEffect, CanonicalRecord, CanonicalRepository,
        };
        for embedded in [true, false] {
            let space = "maintenance_atomic_fault";
            let storage = setup_backend(space, embedded).await;
            let entities = EntityRepository::new(storage.clone());
            let invalid = entities
                .create(test_entity(space, "15:30", "topic"))
                .await
                .expect("invalid label");
            let collision = entities
                .create(test_entity(space, "Existing Person", "person"))
                .await
                .expect("collision");
            let (subject, object) = persist_test_endpoints(&storage, space).await;
            let rels = RelRepository::new(storage.clone());
            for confidence in [0.7, 0.9] {
                rels.create(test_rel(
                    space,
                    &subject,
                    &object,
                    "knows",
                    ClaimStatus::Candidate,
                    confidence,
                ))
                .await
                .expect("rel");
            }
            let planning = PlanningTruthMaintenanceStorage::new(storage.clone(), space, None)
                .await
                .expect("capture");
            assert_eq!(
                atomic_graph::hygiene(&planning, &storage, space)
                    .await
                    .expect("hygiene")
                    .retired,
                1
            );
            assert_eq!(
                atomic_graph::relationships(&planning, &storage, space)
                    .await
                    .expect("dedupe")
                    .merged,
                1
            );
            assert_eq!(
                atomic_graph::predicates(&planning, &storage, space)
                    .await
                    .expect("registry")
                    .registered,
                1
            );
            planning
                .audit_pending_effects("run_maintenance_sweep")
                .await
                .expect("audits");
            assert_eq!(
                entities.get(space, &invalid.base.id).await.expect("read"),
                Some(invalid.clone())
            );
            assert_eq!(rels.list(space, 10, None).await.expect("rels").len(), 2);
            assert!(PredicateRegistryRepository::new(storage.clone())
                .list(space, 10, None)
                .await
                .expect("registry")
                .is_empty());
            let mut request = planning
                .prepare_request("run_maintenance_sweep", space, &serde_json::json!({}), &())
                .await
                .expect("request");
            let key = request.commit_id.clone();
            request.plan.effects.push(CanonicalEffect {
                before: None,
                after: Some(CanonicalRecord::Entity(Box::new(collision))),
            });
            assert!(CanonicalRepository::new(storage.clone())
                .commit(request)
                .await
                .is_err());
            assert_eq!(
                entities.get(space, &invalid.base.id).await.expect("read"),
                Some(invalid)
            );
            assert_eq!(rels.list(space, 10, None).await.expect("rels").len(), 2);
            assert!(PredicateRegistryRepository::new(storage.clone())
                .list(space, 10, None)
                .await
                .expect("registry")
                .is_empty());
            assert!(elephant_storage::AuditLogRepository::new(storage.clone())
                .list(space, 10, None)
                .await
                .expect("audit")
                .is_empty());
            assert!(
                elephant_storage::SystemEventRepository::new(storage.clone())
                    .list(space, 10, None)
                    .await
                    .expect("outbox")
                    .is_empty()
            );
            assert!(CanonicalRepository::new(storage)
                .receipt(space, &key)
                .await
                .expect("receipt")
                .is_none());
        }
    }

    #[tokio::test]
    async fn dedupe_never_collapses_distinct_security_or_subject_partitions() {
        use crate::maintenance_commit::PlanningTruthMaintenanceStorage;
        for embedded in [true, false] {
            let space = "maintenance_partitions";
            let storage = setup_backend(space, embedded).await;
            let (subject, object) = persist_test_endpoints(&storage, space).await;
            let mut first = test_rel(
                space,
                &subject,
                &object,
                "knows",
                ClaimStatus::Approved,
                0.9,
            );
            let mut second = first.clone();
            second.base.id = RecordId::new_v4(TableName::Rel);
            second.base.security.level = 2;
            let mut third = first.clone();
            third.base.id = RecordId::new_v4(TableName::Rel);
            third.base.subjects.push(Link::new(object));
            first.base.security.labels = vec!["restricted".into()];
            let repo = RelRepository::new(storage.clone());
            for row in [first, second, third] {
                repo.create(row).await.expect("create");
            }
            let planning = PlanningTruthMaintenanceStorage::new(storage.clone(), space, None)
                .await
                .expect("capture");
            let result = atomic_graph::relationships(&planning, &storage, space)
                .await
                .expect("prepare");
            assert_eq!(result.scanned, 3);
            assert_eq!(result.merged, 0);
            assert!(planning.prepared_plan().expect("plan").effects.is_empty());
            assert_eq!(repo.list(space, 10, None).await.expect("rows").len(), 3);
        }
    }
    #[tokio::test]
    async fn relationship_dedupe_retains_distinct_validity_windows() {
        use crate::maintenance_commit::PlanningTruthMaintenanceStorage;
        for embedded in [true, false] {
            let space = "maintenance_temporal_partitions";
            let storage = setup_backend(space, embedded).await;
            let (subject, object) = persist_test_endpoints(&storage, space).await;
            let repo = RelRepository::new(storage.clone());
            for days in [1, 2] {
                let mut rel = test_rel(
                    space,
                    &subject,
                    &object,
                    "works_at",
                    ClaimStatus::Approved,
                    0.9,
                );
                rel.validity_window = Some(elephant_types::records::rel::ValidityWindow {
                    start: Some(chrono::Utc::now() - chrono::Duration::days(days)),
                    end: None,
                });
                repo.create(rel)
                    .await
                    .expect("actual temporal relationship");
            }
            let originals = repo.list(space, 10, None).await.expect("originals");
            let planning = PlanningTruthMaintenanceStorage::new(storage.clone(), space, None)
                .await
                .expect("capture");
            let result = atomic_graph::relationships(&planning, &storage, space)
                .await
                .expect("prepare");
            assert_eq!(result.scanned, 2);
            assert_eq!(result.merged, 0);
            assert!(planning.prepared_plan().expect("plan").effects.is_empty());
            assert_eq!(
                repo.list(space, 10, None).await.expect("unchanged"),
                originals
            );
        }
    }

    #[tokio::test]
    async fn active_whole_sweep_commits_hygiene_dedupe_registry_and_audits() {
        for embedded in [true, false] {
            let space = "maintenance_whole_active";
            let storage = setup_backend(space, embedded).await;
            let invalid = EntityRepository::new(storage.clone())
                .create(test_entity(space, "15:30", "topic"))
                .await
                .expect("entity");
            let (subject, object) = persist_test_endpoints(&storage, space).await;
            let rels = RelRepository::new(storage.clone());
            for confidence in [0.7, 0.9] {
                rels.create(test_rel(
                    space,
                    &subject,
                    &object,
                    "knows",
                    ClaimStatus::Candidate,
                    confidence,
                ))
                .await
                .expect("rel");
            }
            // No candidate pairs and one predicate: the actual orchestration
            // executes without any model or network work.
            let config = MaintenanceConfig {
                max_candidates_per_sweep: 0,
                ..Default::default()
            };
            let result = run_maintenance_sweep(
                &storage,
                space,
                &config,
                &ControlledEmbeddingProvider::new(vec![]),
            )
            .await
            .expect("whole sweep");
            assert_eq!(result.entity_hygiene.retired, 1);
            assert_eq!(result.relationships.merged, 1);
            assert_eq!(result.predicate.registered, 1);
            assert!(EntityRepository::new(storage.clone())
                .get(space, &invalid.base.id)
                .await
                .expect("retired")
                .is_none());
            assert_eq!(rels.list(space, 10, None).await.expect("rels").len(), 1);
            assert_eq!(
                PredicateRegistryRepository::new(storage.clone())
                    .list(space, 10, None)
                    .await
                    .expect("registry")
                    .len(),
                1
            );
            assert!(!elephant_storage::AuditLogRepository::new(storage.clone())
                .list(space, 10, None)
                .await
                .expect("audits")
                .is_empty());
            assert!(!elephant_storage::SystemEventRepository::new(storage)
                .list(space, 10, None)
                .await
                .expect("outbox")
                .is_empty());
        }
    }
    #[tokio::test]
    async fn queued_sweep_binds_original_run_fence_and_captures_its_read_set() {
        use crate::maintenance_commit::PlanningTruthMaintenanceStorage;
        use elephant_types::records::{WorkItem, WorkItemStatus, WorkRun, WorkRunMetrics};
        for embedded in [true, false] {
            let space = "maintenance_original_run";
            let storage = setup_backend(space, embedded).await;
            let entities = EntityRepository::new(storage.clone());
            let original = entities
                .create(test_entity(space, "15:30", "topic"))
                .await
                .expect("original");
            let base =
                |table| BaseFields::new(RecordId::new_v4(table), space.into(), "oracle".into());
            let work_repo = elephant_storage::WorkItemRepository::new(storage.clone());
            let work = work_repo
                .create(WorkItem {
                    base: base(TableName::WorkItem),
                    kind: "maintenance_sweep".into(),
                    payload: serde_json::json!({}),
                    status: WorkItemStatus::Runnable,
                    priority: 1,
                    idempotency_key: "original-sweep".into(),
                    attempts: 0,
                    lease_expires_at: None,
                    leased_by: None,
                    run_after: None,
                    last_error: None,
                })
                .await
                .expect("work");
            let claimed = work_repo
                .claim_with_lease(
                    space,
                    &work.base.id,
                    "sweep-oracle",
                    chrono::Utc::now() + chrono::Duration::minutes(5),
                )
                .await
                .expect("claim")
                .expect("claimed");
            let fence = elephant_storage::work_attempt_repository::fence_from_claim(&claimed)
                .expect("fence");
            let mut metrics = WorkRunMetrics::default();
            metrics.custom.insert(
                "attempt_fence".into(),
                serde_json::to_value(&fence).expect("encode"),
            );
            let runs = elephant_storage::WorkRunRepository::new(storage.clone());
            let run = runs
                .create(WorkRun {
                    base: base(TableName::WorkRun),
                    work_item: Link::new(work.base.id),
                    attempt: claimed.attempts,
                    started_at: chrono::Utc::now(),
                    completed_at: None,
                    success: false,
                    error: None,
                    metrics,
                    worker_id: fence.lease_token.clone(),
                    worker_version: "oracle".into(),
                    model_version: None,
                    prompt_id: None,
                    config_hash: None,
                })
                .await
                .expect("actual run");
            let config = MaintenanceConfig {
                max_candidates_per_sweep: 0,
                ..Default::default()
            };
            let provider = ControlledEmbeddingProvider::new(vec![]);
            for missing in [true, false] {
                let mut changed = run.clone();
                if missing {
                    changed.metrics.custom.remove("attempt_fence");
                } else {
                    let mut other = fence.clone();
                    other.lease_token = "another-original-claim".into();
                    changed.metrics.custom.insert(
                        "attempt_fence".into(),
                        serde_json::to_value(other).expect("encode"),
                    );
                }
                runs.update(space, &run.base.id, changed)
                    .await
                    .expect("change run");
                let result = Box::pin(run_maintenance_sweep_with_attempt(
                    &storage,
                    space,
                    &config,
                    &provider,
                    Some(fence.clone()),
                    Some(&run.base.id),
                ))
                .await;
                assert!(
                    matches!(result, Err(crate::PipelineError::InvalidState(_))),
                    "{result:?}"
                );
                assert_eq!(
                    entities
                        .get(space, &original.base.id)
                        .await
                        .expect("unchanged"),
                    Some(original.clone())
                );
                assert!(elephant_storage::AuditLogRepository::new(storage.clone())
                    .list(space, 10, None)
                    .await
                    .expect("audits")
                    .is_empty());
            }
            let current = runs
                .update(space, &run.base.id, run.clone())
                .await
                .expect("restore real run");
            let planning =
                PlanningTruthMaintenanceStorage::new(storage.clone(), space, Some(fence.clone()))
                    .await
                    .expect("capture");
            capture_maintenance_run(&planning, &storage, space, &fence, &run.base.id)
                .await
                .expect("bind original");
            assert!(planning
                .prepared_plan()
                .expect("plan")
                .dependencies
                .iter()
                .any(|row| row.base().id == run.base.id));
            let mut after = original.clone();
            after.display_name = "Changed".into();
            planning
                .stage(Some(original.clone()), after)
                .expect("stage");
            let mut raced = current;
            raced
                .metrics
                .custom
                .insert("changed_after_capture".into(), serde_json::json!(true));
            runs.update(space, &run.base.id, raced)
                .await
                .expect("concurrent run update");
            assert!(Box::pin(planning.commit(
                "maintenance_run_oracle",
                space,
                &serde_json::json!({}),
                &()
            ))
            .await
            .is_err());
            assert_eq!(
                entities
                    .get(space, &original.base.id)
                    .await
                    .expect("unchanged after race"),
                Some(original.clone())
            );
            runs.update(space, &run.base.id, run.clone())
                .await
                .expect("restore original");
            let result = Box::pin(run_maintenance_sweep_with_attempt(
                &storage,
                space,
                &config,
                &provider,
                Some(fence),
                Some(&run.base.id),
            ))
            .await
            .expect("actual queued sweep");
            assert_eq!(result.entity_hygiene.retired, 1);
            assert!(entities
                .get(space, &original.base.id)
                .await
                .expect("retired")
                .is_none());
        }
    }
}
