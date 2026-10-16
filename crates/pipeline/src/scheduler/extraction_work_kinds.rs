//! Work kinds for the extraction layer (Phase 4).
//!
//! Per spec §8: These work kinds transform document nodes into assertions.
//!
//! Work kinds:
//! - `plan_extraction`: revision -> ExtractionPlan
//! - `extract_assertions`: node -> raw assertions (stored as work_artifact)

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Instant;

use elephant_storage::doc_node_repository::DocNodeRepository;
use elephant_storage::doc_repository::DocRepository;
use elephant_storage::doc_revision_repository::DocRevisionRepository;
use elephant_storage::source_repository::SourceRepository;
use elephant_storage::space_config_repository::SpaceConfigRepository;
use elephant_storage::work_artifact_repository::WorkArtifactRepository;
use elephant_storage::{AssertionRepository, Repository, StorageClient, WorkRunRepository};
use elephant_types::ids::TableName;
use elephant_types::pipeline_status::ExtractionGuidanceStatus;
use elephant_types::records::attribute::Literal;
use elephant_types::records::base::BaseFields;
use elephant_types::records::doc_node::{DocNode, NodeRole};
use elephant_types::records::doc_revision::{DocRevision, RenderManifestTimeRange, RevisionStatus};
use elephant_types::records::event::TimePrecision;
use elephant_types::records::source::{Source, SourceType};
use elephant_types::records::work::{ArtifactStatus, WorkArtifact};
use elephant_types::records::{Doc, SpaceConfig};
use elephant_types::{Link, RecordId};

use crate::error::PipelineError;
use crate::extraction::{
    AssertionValidator, ExtractionContext, ExtractionMediaAttachment, ExtractionPlan, Extractor,
    KnownEntity, NodeSelection, RawAssertion, StopConditions,
};
use crate::provenance::apply_work_run_provenance;

/// Work kind identifier for plan_extraction.
pub const PLAN_EXTRACTION_KIND: &str = "plan_extraction";
/// Artifact type for persisted extraction plans.
pub const EXTRACTION_PLAN_ARTIFACT_TYPE: &str = "extraction_plan";

/// Work kind identifier for extract_assertions.
pub const EXTRACT_ASSERTIONS_KIND: &str = "extract_assertions";
/// Artifact type emitted by extract_assertions before validation/commit.
pub const KNOWLEDGE_CANDIDATES_ARTIFACT_TYPE: &str = "knowledge_candidates";
/// Backward-compatible artifact type used by pre-rename staged extraction data.
pub const LEGACY_ASSERTION_BATCH_ARTIFACT_TYPE: &str = "assertion_batch";

/// Result of the `plan_extraction` work kind.
#[derive(Debug, Clone)]
pub struct PlanExtractionResult {
    /// The created extraction plan.
    pub plan: ExtractionPlan,
    /// Number of nodes selected for extraction.
    pub nodes_selected: usize,
    /// Total estimated tokens for the plan.
    pub estimated_tokens: u32,
    /// The work artifact ID containing the extraction plan.
    pub artifact_id: RecordId,
}

/// Result of the `extract_assertions` work kind.
#[derive(Debug, Clone)]
pub struct ExtractAssertionsResult {
    /// The work artifact ID containing the extraction results.
    pub artifact_id: RecordId,
    /// Number of assertions extracted.
    pub assertions_extracted: usize,
    /// Tokens used during extraction.
    pub tokens_used: u32,
    /// Model used for extraction.
    pub model: String,
}

/// Serialized extraction result for storage in work_artifact.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtractionArtifactData {
    /// Source identifier for stable lifecycle-aware commit behavior.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_id: Option<String>,
    /// The node that was extracted.
    pub node_id: String,
    /// The revision containing the node.
    pub revision_id: String,
    /// Raw assertions extracted.
    pub assertions: Vec<SerializedRawAssertion>,
    /// Tokens used during extraction.
    pub tokens_used: u32,
    /// Model used for extraction.
    pub model: String,
    /// Revision text length for validation.
    pub revision_text_len: u32,
}

/// Serialized raw assertion for storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializedRawAssertion {
    /// Independent support verification, never an extractor confidence claim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub support_verification: Option<crate::extraction::SupportVerification>,
    /// Raw subject mention text.
    pub subject_text: String,
    /// Raw predicate text.
    pub predicate_text: String,
    /// Raw object mention text.
    pub object_text: Option<String>,
    /// Literal value.
    pub object_literal: Option<serde_json::Value>,
    /// Typed literal retained so staging cannot change date/time/value semantics.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub typed_object_literal: Option<Literal>,
    /// Evidence span start.
    pub evidence_start: u32,
    /// Evidence span end.
    pub evidence_end: u32,
    /// Exact SHA-256 quote hash captured against immutable revision bytes.
    pub quote_hash: Option<String>,
    /// Confidence score.
    pub confidence: f64,
    /// Optional extractor-provided event time (when the claim occurred in real world).
    #[serde(default)]
    pub source_event_time: Option<DateTime<Utc>>,
    /// Optional precision for `source_event_time`.
    #[serde(default)]
    pub source_event_time_precision: Option<TimePrecision>,
    /// Optional extractor-provided title hint for event creation.
    #[serde(default)]
    pub event_title: Option<String>,
    /// Optional stable lineage key for event identity/refinement.
    #[serde(default)]
    pub event_lineage_key: Option<String>,
    /// Existing entity ID for the subject if found via search.
    #[serde(default)]
    pub subject_entity_id: Option<String>,
    /// Entity type classification for the subject.
    #[serde(default)]
    pub subject_entity_type: Option<String>,
    /// Existing entity ID for the object if found via search.
    #[serde(default)]
    pub object_entity_id: Option<String>,
    /// Entity type classification for the object.
    #[serde(default)]
    pub object_entity_type: Option<String>,
    /// Significance assessment: "skip", "low", "medium", "high".
    #[serde(default)]
    pub significance: Option<String>,
}

impl SerializedRawAssertion {
    /// Preserves a raw assertion without inventing evidence or support.
    pub fn from_raw(raw: &RawAssertion) -> Result<Self, PipelineError> {
        if raw.evidence_span.node.is_none() {
            return Err(PipelineError::InvalidInput(
                "evidence_span.node is required".to_string(),
            ));
        }

        let quote_hash = (!raw.evidence_span.quote_hash.is_empty())
            .then(|| raw.evidence_span.quote_hash.clone());

        // Serialize Literal to JSON value if present
        let object_literal = match raw.object_literal.as_ref() {
            Some(literal) => Some(literal_to_json_value(literal)?),
            None => None,
        };

        Ok(Self {
            support_verification: raw.support_verification.clone(),
            subject_text: raw.subject_text.clone(),
            predicate_text: raw.predicate_text.clone(),
            object_text: raw.object_text.clone(),
            object_literal,
            typed_object_literal: raw.object_literal.clone(),
            evidence_start: raw.evidence_span.start,
            evidence_end: raw.evidence_span.end,
            quote_hash,
            confidence: raw.confidence,
            source_event_time: raw.source_event_time,
            source_event_time_precision: raw.source_event_time_precision,
            event_title: raw.event_title.clone(),
            event_lineage_key: raw.event_lineage_key.clone(),
            subject_entity_id: raw.subject_entity_id.clone(),
            subject_entity_type: raw.subject_entity_type.clone(),
            object_entity_id: raw.object_entity_id.clone(),
            object_entity_type: raw.object_entity_type.clone(),
            significance: raw.significance.clone(),
        })
    }

    /// Rehydrates a stored raw assertion for later staged commit.
    pub fn to_raw(
        &self,
        revision_id: &RecordId,
        node_id: &RecordId,
    ) -> Result<RawAssertion, PipelineError> {
        let quote_hash =
            self.quote_hash
                .clone()
                .filter(|value| !value.trim().is_empty())
                .filter(|value| AssertionValidator::is_quote_hash(value))
                .ok_or_else(|| {
                    PipelineError::ValidationError(
                "An immutable source quote hash is required; synthetic evidence is forbidden".into()
            )
                })?;
        if let Some(literal) = &self.typed_object_literal {
            if self.object_literal.as_ref() != Some(&literal_to_json_value(literal)?) {
                return Err(PipelineError::ValidationError(
                    "Staged literal representations disagree".into(),
                ));
            }
        }
        Ok(RawAssertion {
            support_verification: self.support_verification.clone(),
            subject_text: self.subject_text.clone(),
            predicate_text: self.predicate_text.clone(),
            object_text: self.object_text.clone(),
            object_literal: self
                .typed_object_literal
                .clone()
                .or_else(|| self.object_literal.as_ref().map(json_value_to_literal)),
            evidence_span: elephant_types::records::assertion::EvidenceSpan {
                revision: Link::new(revision_id.clone()),
                node: Some(Link::new(node_id.clone())),
                start: self.evidence_start,
                end: self.evidence_end,
                quote_hash,
            },
            confidence: self.confidence,
            quote: None,
            source_event_time: self.source_event_time,
            source_event_time_precision: self.source_event_time_precision,
            event_title: self.event_title.clone(),
            event_lineage_key: self.event_lineage_key.clone(),
            subject_entity_id: self.subject_entity_id.clone(),
            subject_entity_type: self.subject_entity_type.clone(),
            object_entity_id: self.object_entity_id.clone(),
            object_entity_type: self.object_entity_type.clone(),
            significance: self.significance.clone(),
        })
    }
}

fn literal_to_json_value(literal: &Literal) -> Result<serde_json::Value, PipelineError> {
    if let Literal::Float(value) = literal {
        if !value.is_finite() {
            return Err(PipelineError::InvalidInput(
                "object_literal float must be finite".to_string(),
            ));
        }
    }

    // IMPORTANT: `Assertion.object_literal` is stored as a JSON literal (string/number/bool/array)
    // and wrapped by the storage layer for SurrealDB. Passing the `{type,value}` tagged enum
    // shape is rejected by `AssertionRepository` as a reserved wrapper.
    Ok(match literal {
        Literal::String(value)
        | Literal::Date(value)
        | Literal::DateTime(value)
        | Literal::Duration(value) => serde_json::Value::String(value.clone()),
        Literal::Integer(value) => serde_json::json!(value),
        Literal::Float(value) => serde_json::json!(value),
        Literal::Boolean(value) => serde_json::json!(value),
        Literal::Json(value) => value.clone(),
    })
}

fn json_value_to_literal(value: &serde_json::Value) -> Literal {
    match value {
        serde_json::Value::Bool(value) => Literal::Boolean(*value),
        serde_json::Value::Number(value) => {
            if let Some(integer) = value.as_i64() {
                Literal::Integer(integer)
            } else if let Some(float) = value.as_f64() {
                Literal::Float(float)
            } else {
                Literal::Json(serde_json::Value::Number(value.clone()))
            }
        }
        serde_json::Value::String(value) => Literal::String(value.clone()),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) | serde_json::Value::Null => {
            Literal::Json(value.clone())
        }
    }
}

/// Configuration for extraction planning.
#[derive(Debug, Clone)]
pub struct ExtractionPlanConfig {
    /// Stop conditions for extraction.
    pub stop_conditions: StopConditions,
    /// Preferred node roles for extraction (priority order).
    pub preferred_roles: Vec<NodeRole>,
    /// Estimated tokens per character ratio.
    pub tokens_per_char: f64,
    /// Maximum tokens per single extraction node. Nodes larger than this
    /// are skipped, falling back to smaller roles (e.g. Section, Paragraph).
    pub max_tokens_per_node: u32,
}

impl Default for ExtractionPlanConfig {
    fn default() -> Self {
        Self {
            stop_conditions: StopConditions::default(),
            // Prefer semantic document structure, then fall back to renderer chunks.
            // Extraction agents time out on multi-thousand-token spans more often
            // than they fail on smaller independent units, so keep each work item
            // bounded and let commit-time coalescing handle cross-chunk overlap.
            preferred_roles: vec![
                NodeRole::Document,
                NodeRole::Summary,
                NodeRole::Section,
                NodeRole::Paragraph,
                NodeRole::Chunk,
            ],
            tokens_per_char: 0.25, // ~4 chars per token
            max_tokens_per_node: 3_000,
        }
    }
}

/// Executes the `plan_extraction` work kind.
///
/// Creates an extraction plan with hierarchical node selection.
///
/// # Errors
///
/// Returns an error if node retrieval or planning fails.
pub async fn execute_plan_extraction(
    storage: &StorageClient,
    revision_id: &RecordId,
    policy_version: &str,
    config: &ExtractionPlanConfig,
    work_run_id: &RecordId,
    space_id: &str,
    actor: &str,
) -> Result<PlanExtractionResult, PipelineError> {
    if policy_version.trim().is_empty() {
        return Err(PipelineError::InvalidInput(
            "plan_extraction requires non-empty policy_version".to_string(),
        ));
    }

    let source_stage =
        super::source_stage::SourceStageContext::load(storage, space_id, work_run_id).await?;
    let node_repo = DocNodeRepository::new(storage.clone());
    let revision_repo = DocRevisionRepository::new(storage.clone());
    let artifact_repo = WorkArtifactRepository::new(storage.clone());

    // Fetch revision to get text length for token estimation. Point get by
    // id instead of loading every active revision just to .find() this one.
    let revision = revision_repo
        .get_by_id(revision_id)
        .await
        .map_err(PipelineError::StorageError)?
        .filter(|revision| revision.status == RevisionStatus::Active)
        .ok_or_else(|| PipelineError::NotFound(revision_id.to_string()))?;

    let doc = DocRepository::new(storage.clone())
        .get(space_id, revision.doc.id())
        .await?
        .ok_or_else(|| {
            PipelineError::ValidationError("Extraction plan document is missing".into())
        })?;
    let source_id = doc
        .source
        .as_ref()
        .ok_or_else(|| {
            PipelineError::ValidationError("Extraction plan document has no source".into())
        })?
        .id();
    let source = SourceRepository::new(storage.clone())
        .get(space_id, source_id)
        .await?
        .ok_or_else(|| {
            PipelineError::ValidationError("Extraction plan source is missing".into())
        })?;
    super::grounding::validate_revision_lineage(&source, &doc, &revision, space_id)?;
    if let Some(context) = &source_stage {
        context.require_source(&source.base.id)?;
    }
    let all_nodes = node_repo.find_by_revision(revision_id).await?;
    let plan = prepare_extraction_plan(&revision, &all_nodes, policy_version, config)?;
    let total_tokens = plan.total_estimated_tokens();

    // A configuration replan has separate attempt-local stage identities. A
    // retry may reuse its exact persisted plan; it cannot relabel older output.
    let artifact_id = match &source_stage {
        Some(context) => context.artifact_id(
            EXTRACTION_PLAN_ARTIFACT_TYPE,
            revision_id,
            None,
            work_run_id,
        )?,
        None => RecordId::new_v4(TableName::WorkArtifact),
    };
    if let Some(context) = &source_stage {
        if let Some(artifact) = context
            .replay(
                storage,
                &artifact_id,
                EXTRACTION_PLAN_ARTIFACT_TYPE,
                work_run_id,
            )
            .await?
        {
            let recorded: ExtractionPlan = serde_json::from_value(artifact.data)
                .map_err(|error| PipelineError::ValidationError(error.to_string()))?;
            if recorded != plan
                || artifact.base.security != revision.base.security
                || artifact.base.subjects != revision.base.subjects
                || artifact.base.subjects_state != revision.base.subjects_state
            {
                return Err(PipelineError::ValidationError(
                    "Persisted extraction plan changed within one producer attempt".into(),
                ));
            }
            return Ok(PlanExtractionResult {
                nodes_selected: plan.node_count(),
                estimated_tokens: total_tokens,
                plan,
                artifact_id,
            });
        }
    }
    let mut artifact_data = serde_json::to_value(&plan)
        .map_err(|error| PipelineError::ConfigError(error.to_string()))?;
    if let Some(context) = &source_stage {
        context.tag(&mut artifact_data)?;
        context.ensure_current(storage).await?;
    }
    // Persist the complete plan only after every validation succeeds.
    let mut base = BaseFields::new(artifact_id.clone(), space_id.to_string(), actor.to_string());
    base.security = revision.base.security.clone();
    base.subjects = revision.base.subjects.clone();
    base.subjects_state = revision.base.subjects_state;
    base.provenance = revision.base.provenance.clone();
    apply_work_run_provenance(storage, work_run_id, space_id, &mut base.provenance).await?;

    let artifact = WorkArtifact {
        base,
        work_run: Link::new(work_run_id.clone()),
        artifact_type: EXTRACTION_PLAN_ARTIFACT_TYPE.to_string(),
        data: artifact_data,
        status: ArtifactStatus::Draft,
        validation_errors: None,
        validated_at: None,
        committed_at: None,
    };

    artifact_repo
        .create(artifact)
        .await
        .map_err(PipelineError::StorageError)?;

    Ok(PlanExtractionResult {
        nodes_selected: plan.node_count(),
        estimated_tokens: total_tokens,
        plan,
        artifact_id,
    })
}

/// Prepare complete rendered coverage. Budget limits never authorize sealing a
/// successful partial plan. Lower-priority roles fill gaps left by larger nodes.
fn prepare_extraction_plan(
    revision: &DocRevision,
    nodes: &[DocNode],
    policy_version: &str,
    config: &ExtractionPlanConfig,
) -> Result<ExtractionPlan, PipelineError> {
    let text_len = u32::try_from(revision.text.len())
        .map_err(|error| PipelineError::InvalidInput(error.to_string()))?;
    if revision.text_hash != AssertionValidator::hash_quote(&revision.text) {
        return Err(PipelineError::ValidationError(
            "Extraction revision text and immutable hash disagree".into(),
        ));
    }
    if !config.tokens_per_char.is_finite()
        || config.tokens_per_char <= 0.0
        || config.max_tokens_per_node == 0
        || config.stop_conditions.max_nodes == 0
        || config.stop_conditions.max_tokens == 0
        || !config.stop_conditions.budget_cap_euros.is_finite()
        || config.stop_conditions.budget_cap_euros < 0.0
        || !config.stop_conditions.coverage_threshold.is_finite()
        || !(0.0..=1.0).contains(&config.stop_conditions.coverage_threshold)
    {
        return Err(PipelineError::InvalidInput(
            "Invalid extraction coverage or budget configuration".into(),
        ));
    }
    let mut plan = ExtractionPlan::new(revision.base.id.clone(), policy_version.into())
        .with_stop_conditions(config.stop_conditions.clone());
    let mut selected_nodes = 0_usize;
    for node in nodes {
        if node.base.space_id != revision.base.space_id
            || node.base.is_deleted()
            || node.revision.id() != &revision.base.id
            || !super::grounding::same_authorization_partition(&node.base, &revision.base)
            || node.span.start > node.span.end
            || revision
                .text
                .get(node.span.start as usize..node.span.end as usize)
                .is_none()
        {
            return Err(PipelineError::ValidationError(
                "Extraction plan contains invalid node lineage or spans".into(),
            ));
        }
    }
    for role in &config.preferred_roles {
        let mut candidates: Vec<_> = nodes.iter().filter(|node| node.role == *role).collect();
        candidates.sort_by_key(|node| (node.span.start, node.span.end, node.base.id.to_string()));
        let mut batch: Vec<&DocNode> = Vec::new();
        for node in candidates {
            if node.span.start == node.span.end
                || estimated_span_tokens(node.span.start, node.span.end, config)?
                    > config.max_tokens_per_node
            {
                continue;
            }
            let ranges = selected_ranges(&plan.node_selections, &batch);
            if !has_uncovered_text(&revision.text, node.span.start, node.span.end, &ranges) {
                continue;
            }
            if let Some(first) = batch.first() {
                let batch_end = batch
                    .iter()
                    .map(|node| node.span.end)
                    .max()
                    .unwrap_or(first.span.end)
                    .max(node.span.end);
                if estimated_span_tokens(first.span.start, batch_end, config)?
                    > config.max_tokens_per_node
                {
                    append_batch(&mut plan, &batch, config, &mut selected_nodes)?;
                    batch.clear();
                }
            }
            batch.push(node);
        }
        append_batch(&mut plan, &batch, config, &mut selected_nodes)?;
        if !has_uncovered_text(
            &revision.text,
            0,
            text_len,
            &selected_ranges(&plan.node_selections, &[]),
        ) {
            break;
        }
    }
    let ranges = selected_ranges(&plan.node_selections, &[]);
    if has_uncovered_text(&revision.text, 0, text_len, &ranges) {
        return Err(PipelineError::InvalidState("Extraction plan cannot cover the complete source revision within its configured node limits".into()));
    }
    let mut covered_bytes = 0_u32;
    let mut end = 0_u32;
    for (start, range_end) in ranges {
        let start = start.max(end);
        if range_end > start {
            covered_bytes += range_end - start;
            end = range_end;
        }
    }
    plan.estimated_cost_euros = f64::from(plan.total_estimated_tokens()) * 0.000_001;
    plan.coverage = Some(crate::extraction::ExtractionCoverage {
        revision_text_hash: revision.text_hash.clone(),
        revision_text_len: text_len,
        selected_byte_count: covered_bytes,
        complete: true,
        empty_reason: plan
            .node_selections
            .is_empty()
            .then(|| "empty_or_whitespace".into()),
    });
    Ok(plan)
}

fn estimated_span_tokens(
    start: u32,
    end: u32,
    config: &ExtractionPlanConfig,
) -> Result<u32, PipelineError> {
    let estimated = (f64::from(end - start) * config.tokens_per_char).ceil();
    if estimated > f64::from(u32::MAX) {
        return Err(PipelineError::BudgetExceeded(
            "Extraction span estimate exceeds supported token range".into(),
        ));
    }
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    Ok(estimated as u32)
}

fn selected_ranges(selections: &[NodeSelection], batch: &[&DocNode]) -> Vec<(u32, u32)> {
    let mut ranges: Vec<_> = selections
        .iter()
        .map(|selection| (selection.span_start, selection.span_end))
        .collect();
    if let (Some(first), Some(end)) = (batch.first(), batch.iter().map(|node| node.span.end).max())
    {
        ranges.push((first.span.start, end));
    }
    ranges.sort();
    ranges
}

fn has_uncovered_text(text: &str, start: u32, end: u32, ranges: &[(u32, u32)]) -> bool {
    let mut cursor = start;
    for &(range_start, range_end) in ranges {
        if range_end <= cursor {
            continue;
        }
        if range_start >= end {
            break;
        }
        if range_start > cursor
            && !text[cursor as usize..range_start.min(end) as usize]
                .trim()
                .is_empty()
        {
            return true;
        }
        cursor = cursor.max(range_end).min(end);
        if cursor == end {
            return false;
        }
    }
    cursor < end && !text[cursor as usize..end as usize].trim().is_empty()
}

fn append_batch(
    plan: &mut ExtractionPlan,
    nodes: &[&DocNode],
    config: &ExtractionPlanConfig,
    selected_nodes: &mut usize,
) -> Result<(), PipelineError> {
    let Some(first) = nodes.first() else {
        return Ok(());
    };
    let end = nodes
        .iter()
        .map(|node| node.span.end)
        .max()
        .unwrap_or(first.span.end);
    let tokens = estimated_span_tokens(first.span.start, end, config)?;
    let total = plan
        .total_estimated_tokens()
        .checked_add(tokens)
        .ok_or_else(|| PipelineError::BudgetExceeded("Extraction token count overflow".into()))?;
    // One coalesced window becomes one bounded work item. Counting its raw
    // structural members would reject large sources despite bounded fanout.
    let count = selected_nodes
        .checked_add(1)
        .ok_or_else(|| PipelineError::BudgetExceeded("Extraction node count overflow".into()))?;
    if tokens > config.max_tokens_per_node
        || total > config.stop_conditions.max_tokens
        || count > config.stop_conditions.max_nodes as usize
        || f64::from(total) * 0.000_001 > config.stop_conditions.budget_cap_euros
    {
        return Err(PipelineError::BudgetExceeded(
            "Complete source extraction exceeds token, node or estimated cost budget".into(),
        ));
    }
    let priority = u32::try_from(plan.node_count())
        .map_err(|error| PipelineError::InvalidInput(error.to_string()))?;
    plan.add_node(NodeSelection {
        node_id: first.base.id.clone(),
        node_ids: nodes.iter().map(|node| node.base.id.clone()).collect(),
        role: first.role,
        priority,
        estimated_tokens: tokens,
        span_start: first.span.start,
        span_end: end,
    });
    *selected_nodes = count;
    Ok(())
}

/// Read the actual installed asset. Explicit configuration never silently
/// substitutes another skill or continues with an absent/empty instruction body.
fn read_skill_body_from_directory(
    skill_id: &str,
    skills_dir: &std::path::Path,
) -> Result<String, PipelineError> {
    let canonical = match canonical_skill_id_from_directory(skill_id, skills_dir)? {
        Some(canonical) => Some(canonical),
        None => {
            let legacy = canonical_skill_id_legacy(skill_id);
            if legacy == skill_id {
                None
            } else {
                canonical_skill_id_from_directory(&legacy, skills_dir)?
            }
        }
    }
    .ok_or_else(|| {
        PipelineError::ConfigError(format!(
            "extraction_skill_missing: configured skill {skill_id:?} is not installed"
        ))
    })?;
    let root = skills_dir.canonicalize().map_err(|error| {
        PipelineError::ConfigError(format!("extraction_skill_unreadable: {error}"))
    })?;
    let path = root
        .join(&canonical)
        .join("SKILL.md")
        .canonicalize()
        .map_err(|error| {
            PipelineError::ConfigError(format!(
                "extraction_skill_unreadable: {canonical:?}: {error}"
            ))
        })?;
    if !path.starts_with(&root) {
        return Err(PipelineError::ConfigError(
            "extraction_skill_unreadable: asset is outside the configured skills directory".into(),
        ));
    }
    let content = std::fs::read_to_string(path).map_err(|error| {
        PipelineError::ConfigError(format!(
            "extraction_skill_unreadable: {canonical:?}: {error}"
        ))
    })?;
    let body = strip_yaml_frontmatter(&content);
    if body.trim().is_empty() {
        return Err(PipelineError::ConfigError(format!(
            "extraction_skill_empty: {canonical:?} has no instruction body"
        )));
    }
    Ok(body)
}

fn explicit_skill_source(source: &str) -> bool {
    matches!(
        source,
        "doc.metadata"
            | "source.metadata"
            | "space_config"
            | "env.source_type_map"
            | "env.default_skill"
    )
}

fn load_guidance_skill_body(
    guidance: &ExtractionGuidance,
    skills_dir: &std::path::Path,
) -> Result<Option<String>, PipelineError> {
    let Some(skill_id) = guidance.requested_skill_id.as_deref() else {
        return Ok(None);
    };
    match read_skill_body_from_directory(skill_id, skills_dir) {
        Ok(body) => Ok(Some(body)),
        Err(error) if explicit_skill_source(guidance.skill_source) => Err(error),
        Err(error) => {
            tracing::warn!(skill_id, %error, "Inferred extraction guidance asset is unavailable");
            Ok(None)
        }
    }
}

/// Strips YAML frontmatter (between `---` markers) from a SKILL.md file.
fn strip_yaml_frontmatter(content: &str) -> String {
    let trimmed = content.trim_start();
    if let Some(after_open) = trimmed.strip_prefix("---") {
        if let Some(end) = after_open.find("\n---") {
            let after_frontmatter = &after_open[end + 4..];
            return after_frontmatter.trim_start_matches('\n').to_string();
        }
    }
    content.to_string()
}

#[derive(Debug, Clone)]
struct ExtractionGuidance {
    purpose: Option<String>,
    purpose_source: &'static str,
    skill_id: Option<String>,
    requested_skill_id: Option<String>,
    skill_source: &'static str,
    resolution_trace: Vec<String>,
}

fn read_metadata_string(metadata: &Value, keys: &[&str]) -> Option<String> {
    let object = metadata.as_object()?;
    for key in keys {
        let Some(value) = object.get(*key) else {
            continue;
        };
        if let Some(raw) = value.as_str() {
            let trimmed = raw.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

fn repo_skills_dir() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../skills")
        .canonicalize()
        .unwrap_or_else(|_| {
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../skills")
        })
}

fn skills_dir_from_env() -> std::path::PathBuf {
    if let Ok(path) = std::env::var("PIPELINE_SKILLS_DIR") {
        return std::path::PathBuf::from(path);
    }

    let cwd_skills = std::path::PathBuf::from("./skills");
    if cwd_skills.exists() {
        return cwd_skills;
    }

    repo_skills_dir()
}

fn skill_alias_key(raw: &str) -> String {
    raw.chars().filter(char::is_ascii_alphanumeric).collect()
}

fn push_skill_alias(aliases: &mut std::collections::HashSet<String>, value: &str) {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return;
    }
    let normalized = trimmed.replace('\\', "/").to_ascii_lowercase();
    if normalized.is_empty() {
        return;
    }
    aliases.insert(normalized.clone());
    let compact = skill_alias_key(&normalized);
    if !compact.is_empty() {
        aliases.insert(compact);
    }
}

fn skill_alias_candidates(raw: &str) -> Vec<String> {
    let normalized = raw.trim().replace('\\', "/").to_ascii_lowercase();
    let mut aliases = std::collections::HashSet::new();
    push_skill_alias(&mut aliases, &normalized);
    push_skill_alias(&mut aliases, &normalized.replace('/', "_"));
    push_skill_alias(&mut aliases, &normalized.replace('/', "-"));
    push_skill_alias(&mut aliases, &normalized.replace('_', "-"));
    push_skill_alias(&mut aliases, &normalized.replace('-', "_"));

    if let Some(basename) = normalized.rsplit('/').next() {
        push_skill_alias(&mut aliases, basename);
        push_skill_alias(&mut aliases, &basename.replace('_', "-"));
        push_skill_alias(&mut aliases, &basename.replace('-', "_"));
    }

    if let Some(stripped) = normalized.strip_prefix("extraction/") {
        push_skill_alias(&mut aliases, stripped);
        push_skill_alias(&mut aliases, &format!("extraction_{stripped}"));
        push_skill_alias(&mut aliases, &format!("extraction-{stripped}"));
    }

    aliases.into_iter().collect()
}

fn collect_skill_ids_recursive(
    root: &std::path::Path,
    current: &std::path::Path,
    skill_ids: &mut Vec<String>,
) -> Result<(), PipelineError> {
    let entries = match std::fs::read_dir(current) {
        Ok(entries) => entries,
        Err(error) if current == root && error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(())
        }
        Err(error) => {
            return Err(PipelineError::ConfigError(format!(
                "extraction_skill_unreadable: skill catalog: {error}"
            )))
        }
    };
    let mut has_skill_md = false;
    let mut child_dirs = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| {
            PipelineError::ConfigError(format!(
                "extraction_skill_unreadable: skill catalog: {error}"
            ))
        })?;
        let kind = entry.file_type().map_err(|error| {
            PipelineError::ConfigError(format!(
                "extraction_skill_unreadable: skill catalog: {error}"
            ))
        })?;
        // Do not recurse through symlinks or accidentally index a different
        // asset tree under an alias. The configured root itself may be a link.
        if kind.is_dir() {
            child_dirs.push(entry.path());
        } else if kind.is_file() && entry.file_name() == "SKILL.md" {
            has_skill_md = true;
        }
    }
    if has_skill_md {
        if let Ok(relative) = current.strip_prefix(root) {
            let parts: Vec<_> = relative
                .components()
                .map(|part| part.as_os_str().to_string_lossy().into_owned())
                .collect();
            if !parts.is_empty() {
                skill_ids.push(parts.join("/"));
            }
        }
    }
    child_dirs.sort();
    for child in child_dirs {
        collect_skill_ids_recursive(root, &child, skill_ids)?;
    }
    Ok(())
}

type SkillAliases = std::collections::BTreeMap<String, std::collections::BTreeSet<String>>;

fn discovered_skill_aliases(skills_dir: &std::path::Path) -> Result<SkillAliases, PipelineError> {
    let mut skill_ids = Vec::new();
    collect_skill_ids_recursive(skills_dir, skills_dir, &mut skill_ids)?;
    let mut aliases: SkillAliases = Default::default();
    for canonical in skill_ids {
        for alias in skill_alias_candidates(&canonical) {
            aliases.entry(alias).or_default().insert(canonical.clone());
        }
    }
    Ok(aliases)
}

fn canonical_skill_id_from_directory(
    raw: &str,
    skills_dir: &std::path::Path,
) -> Result<Option<String>, PipelineError> {
    let normalized = raw.trim().replace('\\', "/");
    if normalized.is_empty()
        || std::path::Path::new(&normalized)
            .components()
            .any(|part| !matches!(part, std::path::Component::Normal(_)))
    {
        return Err(PipelineError::ConfigError(
            "extraction_skill_missing: skill ID must be a nonempty relative catalog name".into(),
        ));
    }
    let aliases = discovered_skill_aliases(skills_dir)?;
    // An exact canonical path wins even when its basename is shared. Preserve
    // the actual installed path's case on case-sensitive filesystems.
    if aliases.values().any(|ids| ids.contains(&normalized)) {
        return Ok(Some(normalized));
    }
    let key = normalized.to_ascii_lowercase();
    let compact = skill_alias_key(&key);
    let matches: std::collections::BTreeSet<_> = [key, compact]
        .iter()
        .filter_map(|key| aliases.get(key))
        .flatten()
        .cloned()
        .collect();
    match matches.len() {
        0 => Ok(None),
        1 => Ok(matches.into_iter().next()),
        _ => Err(PipelineError::ConfigError(format!("extraction_skill_ambiguous: {raw:?} names multiple installed skills; use their canonical relative path"))),
    }
}

fn canonical_skill_id_legacy(raw: &str) -> String {
    let normalized = raw.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "fiction" | "extraction_fiction" | "fiction_extraction" => "extraction/fiction".to_string(),
        "technical"
        | "technical-docs"
        | "technical_docs"
        | "docs"
        | "documentation"
        | "extraction_technical_docs" => "extraction/technical-docs".to_string(),
        _ => raw.trim().to_string(),
    }
}

fn canonical_skill_id(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    let skills_dir = skills_dir_from_env();
    match canonical_skill_id_from_directory(trimmed, &skills_dir) {
        Ok(Some(canonical)) => canonical,
        Ok(None) => canonical_skill_id_legacy(trimmed),
        // Preserve the requested ID for the strict execution/readiness check.
        // In particular, ambiguity must never become a legacy default alias.
        Err(_) => trimmed.to_owned(),
    }
}

fn domain_to_skill(domain: &str) -> Option<&'static str> {
    match domain.trim().to_ascii_lowercase().as_str() {
        "fiction" | "novel" | "narrative" | "story" => Some("extraction/fiction"),
        "technical" | "docs" | "documentation" | "research" | "regulatory" | "finance"
        | "operations" | "ops" => Some("extraction/technical-docs"),
        _ => None,
    }
}

fn source_type_key(source_type: SourceType) -> &'static str {
    match source_type {
        SourceType::Email => "email",
        SourceType::Calendar => "calendar",
        SourceType::Web => "web",
        SourceType::Drive => "drive",
        SourceType::Chat => "chat",
        SourceType::DeviceFrame => "device_frame",
        SourceType::DeviceVideo => "device_video",
        SourceType::Transcript => "transcript",
        SourceType::Manual => "manual",
    }
}

fn source_type_default_skill(source_type: SourceType) -> &'static str {
    match source_type {
        SourceType::Email | SourceType::Chat => "extraction/technical-docs",
        SourceType::Calendar
        | SourceType::Web
        | SourceType::Drive
        | SourceType::DeviceFrame
        | SourceType::DeviceVideo
        | SourceType::Transcript
        | SourceType::Manual => "extraction/technical-docs",
    }
}

fn source_type_skill_from_env(source_type: SourceType) -> Option<String> {
    use std::sync::OnceLock;
    static PARSED: OnceLock<Option<std::collections::HashMap<String, String>>> = OnceLock::new();
    let map = PARSED.get_or_init(|| {
        let raw = std::env::var("PIPELINE_SKILL_BY_SOURCE_TYPE_JSON").ok()?;
        serde_json::from_str(&raw).ok()
    });
    let parsed = map.as_ref()?;
    let key = source_type_key(source_type);
    parsed
        .get(key)
        .or_else(|| parsed.get("*"))
        .map(String::as_str)
        .map(ToString::to_string)
}

fn choose_with_trace<const N: usize>(
    trace: &mut Vec<String>,
    label: &'static str,
    candidates: [(&'static str, Option<String>); N],
    include_value_on_hit: bool,
) -> Option<(String, &'static str)> {
    for (source, candidate) in candidates {
        match candidate {
            Some(value) => {
                if include_value_on_hit {
                    // The trace reports the resolved skill just like skill_id;
                    // keep the original alias separately for strict asset lookup.
                    let resolved = canonical_skill_id(&value);
                    trace.push(format!("{label}:{source}=hit:{resolved}"));
                } else {
                    trace.push(format!("{label}:{source}=hit"));
                }
                return Some((value, source));
            }
            None => {
                trace.push(format!("{label}:{source}=miss"));
            }
        }
    }
    None
}

fn resolve_extraction_guidance(
    space_config: Option<&SpaceConfig>,
    doc: Option<&Doc>,
    source: Option<&Source>,
) -> ExtractionGuidance {
    let doc_metadata = doc.map(|d| &d.metadata);
    let source_metadata = source.map(|s| &s.metadata);
    let mut resolution_trace = Vec::new();

    let purpose = choose_with_trace(
        &mut resolution_trace,
        "purpose",
        [
            (
                "doc.metadata",
                doc_metadata
                    .and_then(|m| read_metadata_string(m, &["extraction_purpose", "purpose"])),
            ),
            (
                "source.metadata",
                source_metadata
                    .and_then(|m| read_metadata_string(m, &["extraction_purpose", "purpose"])),
            ),
            (
                "space_config",
                space_config
                    .and_then(|cfg| cfg.purpose.as_deref())
                    .map(str::trim)
                    .filter(|p| !p.is_empty())
                    .map(ToString::to_string),
            ),
            (
                "env.default_purpose",
                std::env::var("PIPELINE_DEFAULT_EXTRACTION_PURPOSE")
                    .ok()
                    .map(|p| p.trim().to_string())
                    .filter(|p| !p.is_empty()),
            ),
        ],
        false,
    );

    let skill = choose_with_trace(
        &mut resolution_trace,
        "skill",
        [
            (
                "doc.metadata",
                doc_metadata.and_then(|m| {
                    read_metadata_string(
                        m,
                        &[
                            "extraction_skill",
                            "extraction_skill_id",
                            "skill",
                            "skill_id",
                        ],
                    )
                }),
            ),
            (
                "source.metadata",
                source_metadata.and_then(|m| {
                    read_metadata_string(
                        m,
                        &[
                            "extraction_skill",
                            "extraction_skill_id",
                            "skill",
                            "skill_id",
                        ],
                    )
                }),
            ),
            (
                "space_config",
                space_config
                    .and_then(|cfg| cfg.skill.as_deref())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(ToString::to_string),
            ),
            (
                "doc.domain",
                doc_metadata
                    .and_then(|m| {
                        read_metadata_string(
                            m,
                            &["extraction_domain", "domain", "genre", "content_domain"],
                        )
                    })
                    .and_then(|domain| domain_to_skill(&domain).map(ToString::to_string)),
            ),
            (
                "source.domain",
                source_metadata
                    .and_then(|m| {
                        read_metadata_string(
                            m,
                            &["extraction_domain", "domain", "genre", "content_domain"],
                        )
                    })
                    .and_then(|domain| domain_to_skill(&domain).map(ToString::to_string)),
            ),
            (
                "env.source_type_map",
                source.and_then(|s| source_type_skill_from_env(s.type_)),
            ),
            (
                "source.type_default",
                source.map(|s| source_type_default_skill(s.type_).to_string()),
            ),
            (
                "env.default_skill",
                std::env::var("PIPELINE_DEFAULT_EXTRACTION_SKILL").ok(),
            ),
        ],
        true,
    );

    ExtractionGuidance {
        purpose: purpose.as_ref().map(|(p, _)| p.clone()),
        purpose_source: purpose.map(|(_, src)| src).unwrap_or("none"),
        skill_id: skill.as_ref().map(|(s, _)| canonical_skill_id(s)),
        requested_skill_id: skill.as_ref().map(|(s, _)| s.clone()),
        skill_source: skill.map(|(_, src)| src).unwrap_or("none"),
        resolution_trace,
    }
}

/// Resolves extraction guidance into a serializable status snapshot.
#[must_use]
pub fn resolve_extraction_guidance_status(
    space_config: Option<&SpaceConfig>,
    doc: Option<&Doc>,
    source: Option<&Source>,
) -> ExtractionGuidanceStatus {
    let guidance = resolve_extraction_guidance(space_config, doc, source);
    ExtractionGuidanceStatus {
        purpose: guidance.purpose,
        purpose_source: guidance.purpose_source.to_string(),
        skill_id: guidance.skill_id,
        skill_source: guidance.skill_source.to_string(),
        resolution_trace: guidance.resolution_trace,
    }
}

/// Validate selected explicit extraction guidance assets without a model call.
///
/// Uses the same installed catalog/body checks as active extraction. Missing,
/// ambiguous, unreadable or empty explicit assets are configuration errors.
/// Inferred defaults retain their optional behavior.
///
/// # Errors
/// Returns a configuration error when a selected explicit asset is unavailable.
pub fn validate_extraction_guidance_assets(
    space_config: Option<&SpaceConfig>,
    doc: Option<&Doc>,
    source: Option<&Source>,
) -> Result<(), PipelineError> {
    let guidance = resolve_extraction_guidance(space_config, doc, source);
    load_guidance_skill_body(&guidance, &skills_dir_from_env()).map(|_| ())
}

/// Loads entities already created from earlier chunks of the same document.
///
/// Queries assertions linked to this revision, extracts entity IDs from their
/// subjects, and loads the entities. This provides the extraction LLM with
/// context about entities already identified so it can reuse their IDs.
///
/// Best-effort: returns an empty vec on any error to avoid blocking extraction.
async fn load_known_entities_for_revision(
    storage: &StorageClient,
    space_id: &str,
    revision_id: &RecordId,
    expected_base: &BaseFields,
) -> Vec<KnownEntity> {
    use elephant_storage::EntityRepository;
    use elephant_types::records::entity::{Entity, EntityStatus};

    let assertion_repo = AssertionRepository::new(storage.clone());
    let entity_repo = EntityRepository::new(storage.clone());

    // Find assertions already created for this revision
    let assertions = match assertion_repo
        .find_by_revision(space_id, revision_id, 500)
        .await
    {
        Ok(a) => a,
        Err(e) => {
            tracing::debug!("Failed to load assertions for revision {revision_id}: {e}");
            return Vec::new();
        }
    };

    async fn load_canonical_entity(
        entity_repo: &EntityRepository,
        space_id: &str,
        entity_id: &RecordId,
        expected_base: &BaseFields,
    ) -> Option<Entity> {
        let entity = entity_repo.get(space_id, entity_id).await.ok().flatten()?;
        if !super::grounding::same_support_partition(&entity.base, expected_base) {
            return None;
        }
        if entity.status == EntityStatus::Merged {
            if let Some(redirect_to) = entity.redirect_to.as_ref() {
                return entity_repo
                    .get(space_id, redirect_to.id())
                    .await
                    .ok()
                    .flatten()
                    .filter(|target| {
                        target.status != EntityStatus::Merged
                            && super::grounding::same_support_partition(&target.base, expected_base)
                    });
            }
        }
        Some(entity)
    }

    async fn push_known_entity(
        entity_repo: &EntityRepository,
        space_id: &str,
        seen: &mut std::collections::HashSet<RecordId>,
        known: &mut Vec<KnownEntity>,
        entity_id: &RecordId,
        expected_base: &BaseFields,
    ) {
        let Some(entity) =
            load_canonical_entity(entity_repo, space_id, entity_id, expected_base).await
        else {
            return;
        };
        if !seen.insert(entity.base.id.clone()) {
            return;
        }
        known.push(KnownEntity {
            id: entity.base.id.to_string(),
            display_name: entity.display_name.clone(),
            entity_type: entity.entity_type.to_string(),
        });
    }

    // Collect unique entity IDs from assertion indexes and resolved endpoints.
    let mut seen = std::collections::HashSet::new();
    let mut known = Vec::new();

    for assertion in &assertions {
        for subject_link in &assertion.base.subjects {
            push_known_entity(
                &entity_repo,
                space_id,
                &mut seen,
                &mut known,
                subject_link.id(),
                expected_base,
            )
            .await;
        }
        if let Some(subject_entity) = assertion.subject_entity.as_ref() {
            push_known_entity(
                &entity_repo,
                space_id,
                &mut seen,
                &mut known,
                subject_entity.id(),
                expected_base,
            )
            .await;
        }
        if let Some(object_entity) = assertion.object_entity.as_ref() {
            push_known_entity(
                &entity_repo,
                space_id,
                &mut seen,
                &mut known,
                object_entity.id(),
                expected_base,
            )
            .await;
        }
    }

    known
}

// Keep concrete source-work child construction out of the producer poll frame.
// The caller polls the same future in its original task and authority scopes.
#[inline(never)]
fn boxed_extraction_work<F: std::future::Future>(
    make: impl FnOnce() -> F,
) -> std::pin::Pin<Box<F>> {
    Box::pin(make())
}

/// Executes the `extract_assertions` work kind.
///
/// Calls the extractor on a document node and stores results as a work artifact.
///
/// # Errors
///
/// Returns an error if extraction or artifact storage fails.
#[allow(clippy::too_many_arguments)]
pub async fn execute_extract_assertions<E: Extractor + ?Sized>(
    storage: &StorageClient,
    extractor: &E,
    node_id: &RecordId,
    revision_id: &RecordId,
    work_run_id: &RecordId,
    space_id: &str,
    actor: &str,
    span_override: Option<(u32, u32)>,
) -> Result<ExtractAssertionsResult, PipelineError> {
    boxed_extraction_work(|| {
        execute_extract_assertions_inner(
            storage,
            extractor,
            node_id,
            revision_id,
            work_run_id,
            space_id,
            actor,
            span_override,
            None,
        )
    })
    .await
}

/// Active source producer with one bounded, charged whole-batch repair policy.
///
/// # Errors
/// Returns typed repair, evidence, authority, provider or persistence failures.
#[allow(clippy::too_many_arguments)]
pub async fn execute_source_extraction<E: Extractor + ?Sized>(
    storage: &StorageClient,
    extractor: &E,
    node_id: &RecordId,
    revision_id: &RecordId,
    work_run_id: &RecordId,
    space_id: &str,
    actor: &str,
    span_override: Option<(u32, u32)>,
    configured_max_attempts: u32,
) -> Result<ExtractAssertionsResult, PipelineError> {
    boxed_extraction_work(|| {
        execute_extract_assertions_inner(
            storage,
            extractor,
            node_id,
            revision_id,
            work_run_id,
            space_id,
            actor,
            span_override,
            Some(configured_max_attempts),
        )
    })
    .await
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn execute_extract_assertions_inner<E: Extractor + ?Sized>(
    storage: &StorageClient,
    extractor: &E,
    node_id: &RecordId,
    revision_id: &RecordId,
    work_run_id: &RecordId,
    space_id: &str,
    actor: &str,
    span_override: Option<(u32, u32)>,
    configured_max_attempts: Option<u32>,
) -> Result<ExtractAssertionsResult, PipelineError> {
    let overall_started = Instant::now();
    let source_stage = boxed_extraction_work(|| {
        super::source_stage::SourceStageContext::load(storage, space_id, work_run_id)
    })
    .await?;
    if source_stage.is_some() && configured_max_attempts.is_some() {
        boxed_extraction_work(|| {
            super::extraction_repair::current_run(storage, space_id, work_run_id)
        })
        .await?;
    }
    let node_repo = DocNodeRepository::new(storage.clone());
    let doc_repo = DocRepository::new(storage.clone());
    let revision_repo = DocRevisionRepository::new(storage.clone());
    let source_repo = SourceRepository::new(storage.clone());
    let artifact_repo = WorkArtifactRepository::new(storage.clone());

    // Fetch the node
    let fetch_started = Instant::now();
    let node = node_repo
        .get(space_id, node_id)
        .await
        .map_err(PipelineError::StorageError)?
        .ok_or_else(|| PipelineError::NotFound(node_id.to_string()))?;

    // Fetch the revision to get text. Point get by id instead of loading
    // every active revision just to .find() this one.
    let revision = boxed_extraction_work(|| revision_repo.get_by_id(revision_id))
        .await
        .map_err(PipelineError::StorageError)?
        .filter(|revision| revision.status == RevisionStatus::Active)
        .ok_or_else(|| PipelineError::NotFound(revision_id.to_string()))?;
    let fetch_ms = fetch_started.elapsed().as_millis();

    // Extract text using span override (for batched nodes) or node span
    let (start, end) = if let Some((s, e)) = span_override {
        (s as usize, e as usize)
    } else {
        (node.span.start as usize, node.span.end as usize)
    };

    if end > revision.text.len()
        || start >= end
        || !revision.text.is_char_boundary(start)
        || !revision.text.is_char_boundary(end)
    {
        return Err(PipelineError::InvalidInput(format!(
            "Span [{}, {}) is out of bounds for revision text length {}",
            start,
            end,
            revision.text.len()
        )));
    }

    let node_text = revision
        .text
        .get(start..end)
        .ok_or_else(|| PipelineError::InvalidInput("Extraction span splits UTF-8".into()))?
        .to_string();
    let lineage = boxed_extraction_work(|| {
        super::grounding::load_grounding_lineage(storage, space_id, revision_id, node_id)
    })
    .await?;
    if lineage.node.revision.id() != revision_id {
        return Err(PipelineError::ValidationError(
            "Extraction node belongs to another revision".into(),
        ));
    }

    let artifact_id = match &source_stage {
        Some(context) => {
            context.require_source(&lineage.source.base.id)?;
            context.artifact_id(
                KNOWLEDGE_CANDIDATES_ARTIFACT_TYPE,
                revision_id,
                Some(node_id),
                work_run_id,
            )?
        }
        None => RecordId::new_v4(TableName::WorkArtifact),
    };
    if let Some(context) = &source_stage {
        if let Some(artifact) = boxed_extraction_work(|| {
            context.replay(
                storage,
                &artifact_id,
                KNOWLEDGE_CANDIDATES_ARTIFACT_TYPE,
                work_run_id,
            )
        })
        .await?
        {
            if artifact.data.get("_source_window") != Some(&serde_json::json!([start, end]))
                || artifact.base.security != node.base.security
                || artifact.base.subjects != node.base.subjects
                || artifact.base.subjects_state != node.base.subjects_state
            {
                return Err(PipelineError::ValidationError(
                    "Persisted extraction window or authorization changed".into(),
                ));
            }
            let data: ExtractionArtifactData = serde_json::from_value(artifact.data)
                .map_err(|error| PipelineError::ValidationError(error.to_string()))?;
            if data.source_id.as_deref() != Some(lineage.source.base.id.to_string().as_str())
                || data.node_id != node_id.to_string()
                || data.revision_id != revision_id.to_string()
                || usize::try_from(data.revision_text_len).ok() != Some(revision.text.len())
            {
                return Err(PipelineError::ValidationError(
                    "Persisted extraction belongs to another immutable input".into(),
                ));
            }
            if let Some(max_attempts) = configured_max_attempts {
                boxed_extraction_work(|| {
                    super::extraction_repair::validate_producer(
                        storage,
                        space_id,
                        work_run_id,
                        &artifact_id,
                        max_attempts,
                    )
                })
                .await?;
            }
            return Ok(ExtractAssertionsResult {
                artifact_id,
                assertions_extracted: data.assertions.len(),
                tokens_used: data.tokens_used,
                model: data.model,
            });
        }
    }

    // Load space config, then resolve extraction purpose/skill from
    // space config + doc/source metadata + source type defaults.
    let space_config_repo = SpaceConfigRepository::new(storage.clone());
    let space_config = boxed_extraction_work(|| space_config_repo.get_by_space_id(space_id))
        .await
        .map_err(PipelineError::StorageError)?;

    let doc = doc_repo
        .get(space_id, revision.doc.id())
        .await
        .map_err(PipelineError::StorageError)?;

    let source = if let Some(source_id) = doc.as_ref().and_then(|d| d.source.as_ref()) {
        source_repo
            .get(space_id, source_id.id())
            .await
            .map_err(PipelineError::StorageError)?
    } else {
        None
    };

    let guidance_started = Instant::now();
    let guidance =
        resolve_extraction_guidance(space_config.as_ref(), doc.as_ref(), source.as_ref());
    tracing::info!(
        space_id,
        revision_id = %revision_id,
        skill_id = guidance.skill_id.as_deref().unwrap_or("<none>"),
        skill_source = guidance.skill_source,
        purpose_source = guidance.purpose_source,
        "Resolved extraction guidance"
    );

    // Resolve and read explicit assets before any extractor/model dispatch.
    let skill_body = load_guidance_skill_body(&guidance, &skills_dir_from_env())?;

    // Load entities already created from earlier chunks of this document.
    // This enables the LLM to reuse entity IDs instead of creating duplicates.
    let mut expected_entity_base = lineage.source.base.clone();
    lineage.inherit(&mut expected_entity_base);
    let known_entities = boxed_extraction_work(|| {
        load_known_entities_for_revision(storage, space_id, revision_id, &expected_entity_base)
    })
    .await;
    let guidance_ms = guidance_started.elapsed().as_millis();
    let media_attachments = doc
        .as_ref()
        .map(|item| resolve_media_attachments_for_span(item, &revision, start as u32, end as u32))
        .unwrap_or_default();

    // Create extraction context
    let mut ctx = ExtractionContext::new(
        node_id.clone(),
        node_text,
        revision_id.clone(),
        space_id.to_string(),
        node.base.security.clone(),
    )
    .with_storage(Some(storage.clone()))
    .with_purpose(guidance.purpose)
    .with_skill_body(skill_body)
    .with_known_entities(known_entities)
    .with_doc_id(doc.as_ref().map(|item| item.base.id.to_string()))
    .with_doc_title(doc.as_ref().map(|item| item.title.clone()))
    .with_sidecar_text(doc.as_ref().and_then(|item| item.sidecar_text.clone()))
    .with_source_timestamp(source.as_ref().and_then(|item| item.timestamp))
    .with_media_attachments(media_attachments);

    if let (Some(context), Some(max_attempts)) = (&source_stage, configured_max_attempts) {
        boxed_extraction_work(|| {
            super::extraction_repair::prepare(
                storage,
                extractor,
                context,
                &lineage,
                work_run_id,
                [start, end],
                max_attempts,
                &mut ctx,
            )
        })
        .await?;
    }
    // Recheck the original plan after guidance reads and before model work.
    if let Some(context) = &source_stage {
        boxed_extraction_work(|| context.ensure_current(storage)).await?;
    }
    // Execute extraction
    let extract_started = Instant::now();
    let repairing = ctx.repair.is_some();
    let mut extraction_result = extractor.extract(ctx).await.map_err(|error| {
        if repairing && matches!(error, PipelineError::BudgetExceeded(_)) {
            PipelineError::GroundingRepairExhausted
        } else {
            error
        }
    })?;
    let extract_ms = extract_started.elapsed().as_millis();
    apply_revision_offsets(
        &mut extraction_result.raw_assertions,
        u32::try_from(start).map_err(|_| {
            PipelineError::InvalidInput("Extraction span start exceeds u32 range".to_string())
        })?,
        revision.text.len(),
    )?;
    for assertion in &mut extraction_result.raw_assertions {
        assertion.evidence_span.revision = Link::new(revision_id.clone());
        assertion.evidence_span.node = Some(Link::new(node_id.clone()));
    }

    if let Some(context) = &source_stage {
        boxed_extraction_work(|| context.ensure_current(storage)).await?;
    }
    let action_proposals = boxed_extraction_work(|| {
        super::action_disposition_proposals::prepare(
            storage,
            &lineage,
            [start, end],
            &extraction_result.action_disposition_proposals,
        )
    })
    .await?;
    let persistence_started = Instant::now();
    // Persist model/prompt identifiers on the work run for provenance.
    let manifest = extractor.manifest();
    let prompt_id = format!("{}@{}", manifest.plugin_id, manifest.version);
    let work_run_repo = WorkRunRepository::new(storage.clone());
    let mut work_run = work_run_repo
        .get(space_id, work_run_id)
        .await
        .map_err(PipelineError::StorageError)?
        .ok_or_else(|| PipelineError::NotFound(work_run_id.to_string()))?;
    work_run.model_version = Some(extraction_result.model.clone());
    work_run.prompt_id = Some(prompt_id);
    work_run_repo
        .update(space_id, work_run_id, work_run)
        .await
        .map_err(PipelineError::StorageError)?;

    // Serialize raw assertions for storage
    let serialized_assertions: Vec<SerializedRawAssertion> = extraction_result
        .raw_assertions
        .iter()
        .map(SerializedRawAssertion::from_raw)
        .collect::<Result<Vec<_>, _>>()?;

    let artifact_data = ExtractionArtifactData {
        source_id: source.as_ref().map(|source| source.base.id.to_string()),
        node_id: node_id.to_string(),
        revision_id: revision_id.to_string(),
        assertions: serialized_assertions,
        tokens_used: extraction_result.tokens_used,
        model: extraction_result.model.clone(),
        #[allow(clippy::cast_possible_truncation)]
        revision_text_len: revision.text.len() as u32,
    };

    let mut artifact_data = serde_json::to_value(&artifact_data)
        .map_err(|error| PipelineError::ConfigError(error.to_string()))?;
    if !action_proposals.is_empty() {
        artifact_data
            .as_object_mut()
            .ok_or_else(|| {
                PipelineError::ValidationError("Extraction payload is not an object".into())
            })?
            .insert(
                "_action_disposition_proposals".into(),
                serde_json::to_value(action_proposals).map_err(|_| {
                    PipelineError::ValidationError("Action proposal encoding failed".into())
                })?,
            );
    }
    if let Some(context) = &source_stage {
        context.tag(&mut artifact_data)?;
        artifact_data
            .as_object_mut()
            .ok_or_else(|| {
                PipelineError::ValidationError("Extraction payload is not an object".into())
            })?
            .insert("_source_window".into(), serde_json::json!([start, end]));
        boxed_extraction_work(|| context.ensure_current(storage)).await?;
    }
    // Create this producer's immutable work artifact.
    let mut base = BaseFields::new(artifact_id.clone(), space_id.to_string(), actor.to_string());
    base.security = node.base.security.clone();
    base.subjects = node.base.subjects.clone();
    base.subjects_state = node.base.subjects_state;
    base.provenance = node.base.provenance.clone();
    boxed_extraction_work(|| {
        apply_work_run_provenance(storage, work_run_id, space_id, &mut base.provenance)
    })
    .await?;

    let artifact = WorkArtifact {
        base,
        work_run: Link::new(work_run_id.clone()),
        artifact_type: KNOWLEDGE_CANDIDATES_ARTIFACT_TYPE.to_string(),
        data: artifact_data,
        status: ArtifactStatus::Draft,
        validation_errors: None,
        validated_at: None,
        committed_at: None,
    };

    artifact_repo
        .create(artifact)
        .await
        .map_err(PipelineError::StorageError)?;
    if source_stage.is_some() {
        if let Some(max_attempts) = configured_max_attempts {
            boxed_extraction_work(|| {
                super::extraction_repair::validate_producer(
                    storage,
                    space_id,
                    work_run_id,
                    &artifact_id,
                    max_attempts,
                )
            })
            .await?;
        }
    }
    let persistence_ms = persistence_started.elapsed().as_millis();
    tracing::info!(
        node_id = %node_id,
        revision_id = %revision_id,
        source_id = source.as_ref().map(|item| item.base.id.to_string()).unwrap_or_default(),
        skill_id = guidance.skill_id.as_deref().unwrap_or("<none>"),
        skill_source = guidance.skill_source,
        purpose_source = guidance.purpose_source,
        fetch_ms,
        guidance_ms,
        extract_ms,
        persistence_ms,
        total_ms = overall_started.elapsed().as_millis(),
        tokens_used = extraction_result.tokens_used,
        assertions_extracted = extraction_result.raw_assertions.len(),
        model = %extraction_result.model,
        "extract_assertions profile"
    );

    Ok(ExtractAssertionsResult {
        artifact_id,
        assertions_extracted: extraction_result.assertion_count(),
        tokens_used: extraction_result.tokens_used,
        model: extraction_result.model,
    })
}

fn apply_revision_offsets(
    assertions: &mut [RawAssertion],
    node_span_start: u32,
    revision_text_len: usize,
) -> Result<(), PipelineError> {
    for assertion in assertions {
        assertion.translate_evidence_offset(node_span_start, revision_text_len)?;
    }

    Ok(())
}

fn resolve_media_attachments_for_span(
    doc: &Doc,
    revision: &DocRevision,
    span_start: u32,
    span_end: u32,
) -> Vec<ExtractionMediaAttachment> {
    if doc.media_attachments.is_empty() {
        return Vec::new();
    }

    let Some(manifest) = revision.manifest.as_ref() else {
        return Vec::new();
    };

    let mut by_attachment =
        std::collections::BTreeMap::<String, Option<RenderManifestTimeRange>>::new();

    for binding in &manifest.span_bindings {
        if spans_overlap(span_start, span_end, binding.text_start, binding.text_end) {
            let merged = by_attachment
                .entry(binding.attachment_id.clone())
                .or_insert_with(|| None);
            *merged = merge_time_ranges(merged.take(), binding.time_range.clone());
        }
    }

    if by_attachment.is_empty() {
        return manifest
            .attachments
            .iter()
            .map(|attachment| ExtractionMediaAttachment::from_manifest_attachment(attachment, None))
            .collect();
    }

    manifest
        .attachments
        .iter()
        .filter_map(|attachment| {
            by_attachment
                .get(&attachment.attachment_id)
                .map(|time_range| {
                    ExtractionMediaAttachment::from_manifest_attachment(
                        attachment,
                        time_range.clone(),
                    )
                })
        })
        .collect()
}

fn spans_overlap(start_a: u32, end_a: u32, start_b: u32, end_b: u32) -> bool {
    start_a < end_b && start_b < end_a
}

fn merge_time_ranges(
    left: Option<RenderManifestTimeRange>,
    right: Option<RenderManifestTimeRange>,
) -> Option<RenderManifestTimeRange> {
    match (left, right) {
        (None, other) | (other, None) => other,
        (Some(left), Some(right)) => Some(RenderManifestTimeRange {
            start_ms: match (left.start_ms, right.start_ms) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            },
            end_ms: match (left.end_ms, right.end_ms) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            },
        }),
    }
}

#[cfg(test)]
mod routing_tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use async_trait::async_trait;
    use elephant_storage::work_item_repository::WorkItemRepository;
    use elephant_storage::EntityRepository;
    use elephant_types::records::assertion::{
        Assertion, AssertionRetryState, EvidenceSpan, ResolutionStatus,
    };
    use elephant_types::records::base::BaseFields;
    use elephant_types::records::doc::{DocMediaAttachment, DocMediaKind, DocMediaLocation};
    use elephant_types::records::doc_revision::{
        RenderManifest, RenderManifestAttachment, RenderManifestSpanBinding, RendererMetadata,
    };
    use elephant_types::records::entity::{Entity, EntityType};
    use elephant_types::records::work::{
        PluginManifest, WorkItem, WorkItemStatus, WorkRun, WorkRunMetrics,
    };
    use std::sync::{Arc, Mutex};

    fn base(table: TableName, space_id: &str) -> BaseFields {
        BaseFields::new(
            RecordId::new_v4(table),
            space_id.to_string(),
            "tester".to_string(),
        )
    }

    fn coverage_revision(text: &str) -> DocRevision {
        DocRevision {
            base: base(TableName::DocRevision, "coverage"),
            doc: Link::new(RecordId::new_v4(TableName::Doc)),
            text: text.into(),
            text_hash: AssertionValidator::hash_quote(text),
            renderer: RendererMetadata {
                name: "coverage-fixture".into(),
                version: "1".into(),
                config_hash: None,
            },
            manifest: None,
            supersedes: None,
            status: RevisionStatus::Active,
            rendered_at: Utc::now(),
        }
    }

    fn coverage_node(revision: &DocRevision, role: NodeRole, start: u32, end: u32) -> DocNode {
        let mut node_base = revision.base.clone();
        node_base.id = RecordId::new_v4(TableName::DocNode);
        DocNode {
            node_key: format!("{role}:{start}:{end}"),
            base: node_base,
            revision: Link::new(revision.base.id.clone()),
            role,
            span: elephant_types::records::doc_node::TextSpan::new(start, end),
            text: None,
            parent: None,
            position: 0,
            depth: 0,
            embedding_model: None,
            collection: None,
        }
    }

    fn coverage_config() -> ExtractionPlanConfig {
        ExtractionPlanConfig {
            preferred_roles: vec![NodeRole::Section, NodeRole::Chunk],
            tokens_per_char: 1.0,
            max_tokens_per_node: 8,
            ..ExtractionPlanConfig::default()
        }
    }

    #[test]
    fn coverage_falls_back_for_oversized_sections_without_silently_losing_tail() {
        let revision = coverage_revision("abcdefghijklmno");
        let nodes = vec![
            coverage_node(&revision, NodeRole::Section, 0, 5),
            coverage_node(&revision, NodeRole::Section, 5, 15),
            coverage_node(&revision, NodeRole::Chunk, 5, 10),
            coverage_node(&revision, NodeRole::Chunk, 10, 15),
        ];
        let plan = prepare_extraction_plan(&revision, &nodes, "v1", &coverage_config()).unwrap();
        assert_eq!(plan.node_selections.len(), 3);
        assert_eq!(plan.node_selections[2].span_end, 15);
        let proof = plan.coverage.unwrap();
        assert!(proof.complete);
        assert_eq!(proof.revision_text_hash, revision.text_hash);
        assert_eq!(proof.selected_byte_count, 15);
        assert_eq!(proof.revision_text_len, 15);
        assert_eq!(proof.empty_reason, None);
        assert!(prepare_extraction_plan(&revision, &nodes[..2], "v1", &coverage_config()).is_err());
    }

    #[test]
    fn coverage_node_token_and_cost_caps_never_return_partial_success() {
        let revision = coverage_revision("abcdefghijklmno");
        let nodes: Vec<_> = [(0, 5), (5, 10), (10, 15)]
            .into_iter()
            .map(|(start, end)| coverage_node(&revision, NodeRole::Chunk, start, end))
            .collect();
        let mut node_limited = coverage_config();
        node_limited.stop_conditions.max_nodes = 2;
        let mut token_limited = coverage_config();
        token_limited.stop_conditions.max_tokens = 14;
        let mut cost_limited = coverage_config();
        cost_limited.stop_conditions.budget_cap_euros = 0.000_014;
        for config in [node_limited, token_limited, cost_limited] {
            assert!(matches!(
                prepare_extraction_plan(&revision, &nodes, "v1", &config),
                Err(PipelineError::BudgetExceeded(_))
            ));
        }
    }

    #[test]
    fn coverage_counts_overlap_once_and_is_independent_of_repository_order() {
        let revision = coverage_revision("abcdefghijkl");
        let mut nodes = vec![
            coverage_node(&revision, NodeRole::Chunk, 0, 8),
            coverage_node(&revision, NodeRole::Chunk, 4, 12),
        ];
        let plan = prepare_extraction_plan(&revision, &nodes, "v1", &coverage_config()).unwrap();
        nodes.reverse();
        let reversed =
            prepare_extraction_plan(&revision, &nodes, "v1", &coverage_config()).unwrap();
        assert_eq!(plan, reversed);
        assert_eq!(plan.total_estimated_tokens(), 16);
        assert_eq!(plan.coverage.unwrap().selected_byte_count, 12);
    }

    #[test]
    fn coverage_accounts_for_full_coalesced_window_including_gaps() {
        let revision = coverage_revision("a  b");
        let nodes = vec![
            coverage_node(&revision, NodeRole::Chunk, 0, 1),
            coverage_node(&revision, NodeRole::Chunk, 3, 4),
        ];
        let plan = prepare_extraction_plan(&revision, &nodes, "v1", &coverage_config()).unwrap();
        assert_eq!(plan.node_count(), 1);
        assert_eq!(plan.total_estimated_tokens(), 4);
        assert_eq!(plan.coverage.unwrap().selected_byte_count, 4);
    }

    #[test]
    fn coverage_empty_plan_requires_explicit_whitespace_reason() {
        for text in ["", " \n\t", "\u{2003}"] {
            let revision = coverage_revision(text);
            let plan = prepare_extraction_plan(&revision, &[], "v1", &coverage_config()).unwrap();
            let proof = plan.coverage.unwrap();
            assert!(proof.complete);
            assert_eq!(proof.selected_byte_count, 0);
            assert_eq!(proof.empty_reason.as_deref(), Some("empty_or_whitespace"));
        }
        let revision = coverage_revision("not empty");
        assert!(prepare_extraction_plan(&revision, &[], "v1", &coverage_config()).is_err());
    }

    #[test]
    fn coverage_rejects_split_utf8_foreign_nodes_and_changed_revision() {
        let mut revision = coverage_revision("éabc");
        let invalid = coverage_node(&revision, NodeRole::Chunk, 1, 5);
        assert!(prepare_extraction_plan(&revision, &[invalid], "v1", &coverage_config()).is_err());
        let mut foreign = coverage_node(&revision, NodeRole::Chunk, 0, 5);
        foreign.base.space_id = "another-space".into();
        assert!(prepare_extraction_plan(&revision, &[foreign], "v1", &coverage_config()).is_err());
        let valid = coverage_node(&revision, NodeRole::Chunk, 0, 5);
        revision.text = "abcde".into();
        assert!(prepare_extraction_plan(&revision, &[valid], "v1", &coverage_config()).is_err());
    }

    #[test]
    fn complete_large_sources_fan_out_within_bounded_default_work_items() {
        for bytes in [1_130_000_u32, 2_000_000_u32] {
            let revision = coverage_revision(&"x".repeat(bytes as usize));
            let nodes: Vec<_> = (0..bytes)
                .step_by(462)
                .map(|start| {
                    coverage_node(&revision, NodeRole::Chunk, start, (start + 512).min(bytes))
                })
                .collect();
            assert!(
                nodes.len() > 1_000,
                "raw members exceed the former incorrect job count"
            );
            let config = ExtractionPlanConfig::default();
            let plan =
                prepare_extraction_plan(&revision, &nodes, "large-input-v1", &config).unwrap();
            assert!(plan.node_count() < 200);
            assert!(plan
                .node_selections
                .iter()
                .all(|selection| selection.estimated_tokens <= 3_000));
            assert!(plan.total_estimated_tokens() <= config.stop_conditions.max_tokens);
            let coverage = plan.coverage.unwrap();
            assert!(coverage.complete);
            assert_eq!(coverage.selected_byte_count, bytes);
        }
    }

    #[test]
    fn coverage_threshold_does_not_authorize_omitting_a_short_claim() {
        let revision = coverage_revision("abcdefgh z");
        let node = coverage_node(&revision, NodeRole::Chunk, 0, 8);
        let mut config = coverage_config();
        config.stop_conditions.coverage_threshold = 0.5;
        assert!(prepare_extraction_plan(&revision, &[node], "v1", &config).is_err());
    }

    fn make_doc(space_id: &str, metadata: Value) -> Doc {
        Doc {
            base: base(TableName::Doc, space_id),
            title: "doc".to_string(),
            text: "body".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "rendition".to_string(),
            metadata,
        }
    }

    fn make_source(space_id: &str, source_type: SourceType, metadata: Value) -> Source {
        let mut src = Source::new(
            base(TableName::Source, space_id),
            source_type,
            "hash".to_string(),
        );
        src.metadata = metadata;
        src
    }

    fn make_media_doc(space_id: &str) -> Doc {
        Doc {
            base: base(TableName::Doc, space_id),
            title: "media doc".to_string(),
            text: "Rendered projection".to_string(),
            sidecar_text: Some("Caller sidecar text".to_string()),
            source: None,
            media_attachments: vec![
                DocMediaAttachment {
                    attachment_id: "att-image".to_string(),
                    kind: DocMediaKind::Image,
                    media_type: "image/jpeg".to_string(),
                    location: DocMediaLocation::Path {
                        path: "/tmp/image.jpg".to_string(),
                    },
                    label: Some("frame".to_string()),
                    size_bytes: None,
                    duration_ms: None,
                    sha256: None,
                },
                DocMediaAttachment {
                    attachment_id: "att-video".to_string(),
                    kind: DocMediaKind::Video,
                    media_type: "video/mp4".to_string(),
                    location: DocMediaLocation::Path {
                        path: "/tmp/video.mp4".to_string(),
                    },
                    label: Some("segment".to_string()),
                    size_bytes: None,
                    duration_ms: Some(120_000),
                    sha256: None,
                },
            ],
            rendition_key: "media-rendition".to_string(),
            metadata: serde_json::Value::Null,
        }
    }

    fn make_media_revision(space_id: &str, doc_id: RecordId) -> DocRevision {
        DocRevision {
            base: base(TableName::DocRevision, space_id),
            doc: Link::new(doc_id),
            text: "Sidecar text:\nCaller sidecar text\n\nMedia attachment:\nAttachment id: att-image\nKind: image\nMedia type: image/jpeg\nLabel: frame\nDerived analysis:\nFrame details\n\nMedia attachment:\nAttachment id: att-video\nKind: video\nMedia type: video/mp4\nLabel: segment\nDuration ms: 120000\nDerived analysis:\n00:00-00:12 Alex works in Terminal\n00:12-00:24 Claude reviews code\n".to_string(),
            text_hash: "hash".to_string(),
            renderer: RendererMetadata {
                name: "renderer".to_string(),
                version: "1".to_string(),
                config_hash: None,
            },
            manifest: Some(RenderManifest {
                attachments: vec![
                    RenderManifestAttachment {
                        attachment_id: "att-image".to_string(),
                        kind: DocMediaKind::Image,
                        media_type: "image/jpeg".to_string(),
                        location: DocMediaLocation::Path {
                            path: "/tmp/frame.jpg".to_string(),
                        },
                        label: Some("frame".to_string()),
                        size_bytes: None,
                        duration_ms: None,
                        sha256: None,
                    },
                    RenderManifestAttachment {
                        attachment_id: "att-video".to_string(),
                        kind: DocMediaKind::Video,
                        media_type: "video/mp4".to_string(),
                        location: DocMediaLocation::Path {
                            path: "/tmp/segment.mp4".to_string(),
                        },
                        label: Some("segment".to_string()),
                        size_bytes: None,
                        duration_ms: Some(120_000),
                        sha256: None,
                    },
                ],
                span_bindings: vec![
                    RenderManifestSpanBinding {
                        attachment_id: "att-image".to_string(),
                        text_start: 0,
                        text_end: 40,
                        time_range: None,
                    },
                    RenderManifestSpanBinding {
                        attachment_id: "att-video".to_string(),
                        text_start: 40,
                        text_end: 120,
                        time_range: Some(RenderManifestTimeRange {
                            start_ms: Some(0),
                            end_ms: Some(12_000),
                        }),
                    },
                    RenderManifestSpanBinding {
                        attachment_id: "att-video".to_string(),
                        text_start: 120,
                        text_end: 220,
                        time_range: Some(RenderManifestTimeRange {
                            start_ms: Some(12_000),
                            end_ms: Some(24_000),
                        }),
                    },
                ],
            }),
            status: RevisionStatus::Active,
            supersedes: None,
            rendered_at: Utc::now(),
        }
    }

    fn make_work_item(space_id: &str) -> WorkItem {
        WorkItem {
            base: base(TableName::WorkItem, space_id),
            kind: EXTRACT_ASSERTIONS_KIND.to_string(),
            payload: serde_json::json!({}),
            status: WorkItemStatus::Running,
            priority: 0,
            idempotency_key: "extract-1".to_string(),
            attempts: 1,
            lease_expires_at: None,
            leased_by: Some("worker-1".to_string()),
            run_after: None,
            last_error: None,
        }
    }

    fn make_work_run(space_id: &str, work_item_id: RecordId) -> WorkRun {
        WorkRun {
            base: base(TableName::WorkRun, space_id),
            work_item: Link::new(work_item_id),
            attempt: 1,
            started_at: Utc::now(),
            completed_at: None,
            success: false,
            error: None,
            metrics: WorkRunMetrics::default(),
            worker_id: "worker-1".to_string(),
            worker_version: "test".to_string(),
            model_version: None,
            prompt_id: None,
            config_hash: None,
        }
    }

    fn make_entity(space_id: &str, display_name: &str, entity_type: &str) -> Entity {
        Entity::new(
            base(TableName::Entity, space_id),
            EntityType::new(entity_type),
            display_name.to_string(),
        )
    }

    fn make_assertion(
        space_id: &str,
        revision_id: RecordId,
        subject_entity: RecordId,
        object_entity: RecordId,
    ) -> Assertion {
        let mut assertion_base = base(TableName::Assertion, space_id);
        assertion_base.subjects = vec![Link::new(subject_entity.clone())];
        assertion_base.subjects_state = elephant_types::records::base::SubjectsState::Complete;
        Assertion {
            base: assertion_base,
            subject_text: "Ada".to_string(),
            predicate_raw: "attends".to_string(),
            predicate_id: Some("attends".to_string()),
            object_text: Some("Example School".to_string()),
            object_literal: None,
            evidence: EvidenceSpan {
                revision: Link::new(revision_id),
                node: None,
                start: 0,
                end: 10,
                quote_hash: "hash".to_string(),
            },
            source_event_time: None,
            observed_at: Utc::now(),
            asserted_at: Utc::now(),
            assertion_key: "assertion-known-entities".to_string(),
            resolution_status: ResolutionStatus::Resolved,
            resolution_method: Some("test".to_string()),
            resolution_confidence: Some(1.0),
            retry_state: AssertionRetryState::default(),
            subject_entity: Some(Link::new(subject_entity)),
            object_entity: Some(Link::new(object_entity)),
            confidence: 1.0,
            subject_entity_type: Some("person".to_string()),
            object_entity_type: Some("org".to_string()),
            significance: Some("high".to_string()),
            lifecycle_tier: "active".to_string(),
        }
    }

    #[test]
    fn test_extraction_plan_defaults_bound_work_items_and_fallback_to_chunks() {
        let config = ExtractionPlanConfig::default();

        assert!(config.max_tokens_per_node <= 3_000);
        assert_eq!(
            config.preferred_roles.last(),
            Some(&NodeRole::Chunk),
            "chunk fallback keeps large docs extractable without oversized agent runs"
        );
    }

    #[test]
    fn test_apply_revision_offsets_uses_selected_span_start() {
        let placeholder_revision =
            Link::<DocRevision>::new(RecordId::new_v4(TableName::DocRevision));
        let mut assertions = vec![RawAssertion {
            support_verification: None,
            subject_text: "Ada".to_string(),
            predicate_text: "attends".to_string(),
            object_text: Some("Example School".to_string()),
            object_literal: None,
            evidence_span: EvidenceSpan {
                revision: placeholder_revision,
                node: None,
                start: 5,
                end: 15,
                quote_hash: "hash".to_string(),
            },
            confidence: 0.9,
            quote: None,
            source_event_time: None,
            source_event_time_precision: None,
            event_title: None,
            event_lineage_key: None,
            subject_entity_id: None,
            subject_entity_type: Some("person".to_string()),
            object_entity_id: None,
            object_entity_type: Some("org".to_string()),
            significance: Some("high".to_string()),
        }];

        apply_revision_offsets(&mut assertions, 60, 200).expect("offsets apply");

        assert_eq!(assertions[0].evidence_span.start, 65);
        assert_eq!(assertions[0].evidence_span.end, 75);
    }

    #[tokio::test]
    async fn test_known_entities_include_resolved_assertion_objects() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let entity_repo = EntityRepository::new(storage.clone());
        let assertion_repo = AssertionRepository::new(storage.clone());
        let revision_id = RecordId::new_v4(TableName::DocRevision);

        let source_id = RecordId::new_v4(TableName::Source);
        let scoped_entity = |name: &str, kind: &str| {
            let mut entity = make_entity("space-1", name, kind);
            entity.base.provenance.sources = vec![source_id.clone()];
            entity
                .base
                .provenance
                .chain
                .push(elephant_types::records::base::ProvenanceStep {
                    step: "grounded_promotion".into(),
                    input: revision_id.to_string(),
                    output: entity.base.id.to_string(),
                });
            entity
        };
        let subject = entity_repo
            .create(scoped_entity("Ada", "person"))
            .await
            .expect("create subject");
        let object = entity_repo
            .create(scoped_entity("Example School", "org"))
            .await
            .expect("create object");
        assertion_repo
            .create(make_assertion(
                "space-1",
                revision_id.clone(),
                subject.base.id.clone(),
                object.base.id.clone(),
            ))
            .await
            .expect("create assertion");

        let known =
            load_known_entities_for_revision(&storage, "space-1", &revision_id, &subject.base)
                .await;

        assert_eq!(known.len(), 2);
        assert!(known.iter().any(|entity| entity.display_name == "Ada"));
        assert!(known
            .iter()
            .any(|entity| entity.display_name == "Example School"));
        let mut other_revision = subject.base.clone();
        other_revision.provenance.chain.clear();
        assert!(load_known_entities_for_revision(
            &storage,
            "space-1",
            &revision_id,
            &other_revision
        )
        .await
        .is_empty());
        let mut other_policy = subject.base.clone();
        other_policy.security.policy_id = "unrelated".into();
        assert!(
            load_known_entities_for_revision(&storage, "space-1", &revision_id, &other_policy)
                .await
                .is_empty()
        );
    }

    #[derive(Clone)]
    struct CapturingExtractor {
        seen: Arc<Mutex<Option<ExtractionContext>>>,
        manifest: PluginManifest,
    }

    impl CapturingExtractor {
        fn new() -> Self {
            Self {
                seen: Arc::new(Mutex::new(None)),
                manifest: PluginManifest {
                    plugin_id: "test.extractor.capture".to_string(),
                    version: "1.0.0".to_string(),
                    plugin_type: "extractor".to_string(),
                    required_scopes: vec![],
                    tool_access: vec![],
                    work_kinds: vec![EXTRACT_ASSERTIONS_KIND.to_string()],
                    determinism: elephant_types::records::work::Determinism::Deterministic,
                    max_tokens: None,
                    max_wall_time_secs: None,
                    input_types: vec!["doc_node".to_string()],
                    output_types: vec![KNOWLEDGE_CANDIDATES_ARTIFACT_TYPE.to_string()],
                    validator_profile: None,
                    config_schema: None,
                    health_check: None,
                    retry_policy: Default::default(),
                },
            }
        }
    }

    #[async_trait]
    impl Extractor for CapturingExtractor {
        fn manifest(&self) -> &PluginManifest {
            &self.manifest
        }

        async fn extract(
            &self,
            ctx: ExtractionContext,
        ) -> Result<crate::extraction::ExtractionResult, PipelineError> {
            *self.seen.lock().expect("extractor lock") = Some(ctx);
            Ok(crate::extraction::ExtractionResult::empty(
                "test-model".to_string(),
            ))
        }
    }

    fn make_space_config(
        space_id: &str,
        purpose: Option<&str>,
        skill: Option<&str>,
    ) -> SpaceConfig {
        SpaceConfig {
            processing: None,
            base: base(TableName::SpaceConfig, space_id),
            embedding_provider: "openai".to_string(),
            embedding_model: "text-embedding-3-large".to_string(),
            embedding_dims: 3072,
            purpose: purpose.map(ToString::to_string),
            skill: skill.map(ToString::to_string),
        }
    }

    fn make_temp_skills_dir() -> std::path::PathBuf {
        let mut dir = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock should be after unix epoch")
            .as_nanos();
        dir.push(format!(
            "elephant-skill-test-{}-{nanos}",
            std::process::id()
        ));
        std::fs::create_dir_all(dir.join("extraction/custom-ops")).expect("create temp skills dir");
        std::fs::write(
            dir.join("extraction/custom-ops/SKILL.md"),
            "# Custom Ops\n\nTest skill",
        )
        .expect("write skill file");
        dir
    }

    #[test]
    fn test_read_metadata_string_checks_multiple_keys() {
        let metadata = serde_json::json!({
            "other": "x",
            "extraction_skill": "extraction/technical-docs"
        });
        let found = read_metadata_string(&metadata, &["skill", "extraction_skill"]);
        assert_eq!(found.as_deref(), Some("extraction/technical-docs"));
    }

    #[test]
    fn test_doc_metadata_precedes_source_and_space_guidance() {
        let space = make_space_config(
            "space-1",
            Some("space purpose"),
            Some("extraction/custom-comms"),
        );
        let doc = make_doc(
            "space-1",
            serde_json::json!({
                "purpose": "doc purpose",
                "extraction_skill": "extraction/fiction"
            }),
        );
        let source = make_source(
            "space-1",
            SourceType::Web,
            serde_json::json!({
                "purpose": "source purpose",
                "extraction_skill": "extraction/technical-docs"
            }),
        );

        let resolved = resolve_extraction_guidance(Some(&space), Some(&doc), Some(&source));
        assert_eq!(resolved.purpose.as_deref(), Some("doc purpose"));
        assert_eq!(resolved.purpose_source, "doc.metadata");
        assert_eq!(resolved.skill_id.as_deref(), Some("extraction/fiction"));
        assert_eq!(resolved.skill_source, "doc.metadata");
        assert_eq!(
            resolved.resolution_trace,
            [
                "purpose:doc.metadata=hit",
                "skill:doc.metadata=hit:extraction/fiction"
            ]
        );
    }

    #[test]
    fn test_doc_metadata_skill_used_when_space_not_set() {
        let space = make_space_config("space-1", None, None);
        let doc = make_doc(
            "space-1",
            serde_json::json!({
                "extraction_skill": "fiction",
                "extraction_purpose": "extract story events"
            }),
        );

        let resolved = resolve_extraction_guidance(Some(&space), Some(&doc), None);
        assert_eq!(resolved.skill_id.as_deref(), Some("extraction/fiction"));
        assert_eq!(resolved.skill_source, "doc.metadata");
        assert_eq!(resolved.purpose.as_deref(), Some("extract story events"));
        assert_eq!(resolved.purpose_source, "doc.metadata");
    }

    #[test]
    fn test_source_guidance_precedes_space_when_document_override_is_blank() {
        let space = make_space_config("space-1", Some("space purpose"), Some("fiction"));
        let doc = make_doc(
            "space-1",
            serde_json::json!({
                "purpose": " ", "extraction_skill": "", "domain": "fiction"
            }),
        );
        let source = make_source(
            "space-1",
            SourceType::Web,
            serde_json::json!({
                "purpose": "source purpose", "extraction_skill": "technical-docs"
            }),
        );
        let resolved = resolve_extraction_guidance(Some(&space), Some(&doc), Some(&source));
        assert_eq!(resolved.purpose.as_deref(), Some("source purpose"));
        assert_eq!(resolved.purpose_source, "source.metadata");
        assert_eq!(
            resolved.skill_id.as_deref(),
            Some("extraction/technical-docs")
        );
        assert_eq!(resolved.skill_source, "source.metadata");
        assert_eq!(
            resolved.resolution_trace,
            [
                "purpose:doc.metadata=miss",
                "purpose:source.metadata=hit",
                "skill:doc.metadata=miss",
                "skill:source.metadata=hit:extraction/technical-docs"
            ]
        );
    }

    #[test]
    fn test_space_explicit_guidance_precedes_inferred_document_domain() {
        let space = make_space_config("space-1", Some("space purpose"), Some("technical-docs"));
        let doc = make_doc("space-1", serde_json::json!({"domain": "fiction"}));
        let source = make_source("space-1", SourceType::Web, serde_json::json!({}));
        let status = resolve_extraction_guidance_status(Some(&space), Some(&doc), Some(&source));
        assert_eq!(status.purpose.as_deref(), Some("space purpose"));
        assert_eq!(status.purpose_source, "space_config");
        assert_eq!(
            status.skill_id.as_deref(),
            Some("extraction/technical-docs")
        );
        assert_eq!(status.skill_source, "space_config");
        assert_eq!(
            status.resolution_trace,
            [
                "purpose:doc.metadata=miss",
                "purpose:source.metadata=miss",
                "purpose:space_config=hit",
                "skill:doc.metadata=miss",
                "skill:source.metadata=miss",
                "skill:space_config=hit:extraction/technical-docs"
            ]
        );
    }

    #[test]
    fn test_source_type_default_skill_used_when_no_config_or_metadata() {
        let source = make_source(
            "space-1",
            SourceType::Email,
            serde_json::Value::Object(serde_json::Map::new()),
        );

        let resolved = resolve_extraction_guidance(None, None, Some(&source));
        assert_eq!(
            resolved.skill_id.as_deref(),
            Some("extraction/technical-docs")
        );
        assert_eq!(resolved.skill_source, "source.type_default");
    }

    #[test]
    fn test_repo_skills_dir_contains_default_extraction_skill() {
        let resolved = repo_skills_dir();
        assert!(resolved.ends_with("skills"));
        assert!(resolved.join("extraction/technical-docs/SKILL.md").exists());
    }

    #[test]
    fn test_canonical_skill_id_is_derived_from_skills_directory() {
        let skills_dir = make_temp_skills_dir();
        let resolved = canonical_skill_id_from_directory("custom_ops", &skills_dir)
            .expect("unambiguous alias");
        assert_eq!(resolved.as_deref(), Some("extraction/custom-ops"));
        std::fs::remove_dir_all(skills_dir).expect("cleanup temp skills dir");
    }

    #[test]
    fn explicit_skill_reads_actual_body_and_missing_does_not_use_space_default() {
        let root = make_temp_skills_dir();
        let doc = make_doc(
            "space-1",
            serde_json::json!({"extraction_skill":"custom_ops"}),
        );
        let guidance = resolve_extraction_guidance(None, Some(&doc), None);
        assert_eq!(
            load_guidance_skill_body(&guidance, &root)
                .unwrap()
                .as_deref(),
            Some("# Custom Ops\n\nTest skill")
        );
        let missing = make_doc(
            "space-1",
            serde_json::json!({"extraction_skill":"missing-custom-skill"}),
        );
        let space = make_space_config("space-1", None, Some("extraction/custom-ops"));
        let guidance = resolve_extraction_guidance(Some(&space), Some(&missing), None);
        assert_eq!(guidance.skill_source, "doc.metadata");
        let error = load_guidance_skill_body(&guidance, &root).unwrap_err();
        assert!(
            matches!(error, PipelineError::ConfigError(ref message) if message.starts_with("extraction_skill_missing:"))
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn ambiguous_skill_alias_requires_canonical_path_and_rechecks_original_request() {
        let root = make_temp_skills_dir();
        let doc = make_doc(
            "space-1",
            serde_json::json!({"extraction_skill":"custom_ops"}),
        );
        let guidance = resolve_extraction_guidance(None, Some(&doc), None);
        std::fs::create_dir_all(root.join("another/custom-ops")).unwrap();
        std::fs::write(
            root.join("another/custom-ops/SKILL.md"),
            "Other explicit instructions",
        )
        .unwrap();
        assert!(
            matches!(canonical_skill_id_from_directory("custom_ops", &root), Err(PipelineError::ConfigError(message)) if message.starts_with("extraction_skill_ambiguous:"))
        );
        assert!(load_guidance_skill_body(&guidance, &root).is_err());
        assert_eq!(
            read_skill_body_from_directory("another/custom-ops", &root).unwrap(),
            "Other explicit instructions"
        );
        assert_eq!(
            canonical_skill_id_from_directory("extraction/custom-ops", &root)
                .unwrap()
                .as_deref(),
            Some("extraction/custom-ops")
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn explicit_skill_rejects_empty_unreadable_and_foreign_paths() {
        let root = make_temp_skills_dir();
        let asset = root.join("extraction/custom-ops/SKILL.md");
        std::fs::write(&asset, "---\nname: custom\n---\n  ").unwrap();
        assert!(
            matches!(read_skill_body_from_directory("custom_ops", &root), Err(PipelineError::ConfigError(message)) if message.starts_with("extraction_skill_empty:"))
        );
        std::fs::write(&asset, [0xff, 0xfe]).unwrap();
        assert!(
            matches!(read_skill_body_from_directory("custom_ops", &root), Err(PipelineError::ConfigError(message)) if message.starts_with("extraction_skill_unreadable:"))
        );
        assert!(read_skill_body_from_directory("../custom-ops", &root).is_err());
        assert!(read_skill_body_from_directory("/extraction/custom-ops", &root).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn absent_explicit_skill_keeps_optional_inferred_default() {
        let root = make_temp_skills_dir();
        let source = make_source("space-1", SourceType::Manual, serde_json::json!({}));
        let guidance = resolve_extraction_guidance(None, None, Some(&source));
        assert_eq!(guidance.skill_source, "source.type_default");
        assert!(load_guidance_skill_body(&guidance, &root)
            .unwrap()
            .is_none());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn missing_explicit_skill_blocks_active_extractor_and_artifact_creation() {
        let storage = StorageClient::new_embedded().await.unwrap();
        let mut doc = make_doc(
            "space-1",
            serde_json::json!({"extraction_skill": format!("missing-oracle-{}", uuid::Uuid::new_v4())}),
        );
        let hash = AssertionValidator::hash_quote(&doc.text);
        let source = SourceRepository::new(storage.clone())
            .create(Source::new(
                base(TableName::Source, "space-1"),
                SourceType::Manual,
                hash.clone(),
            ))
            .await
            .unwrap();
        doc.source = Some(Link::new(source.base.id.clone()));
        doc.base.provenance.sources = vec![source.base.id.clone()];
        let doc = DocRepository::new(storage.clone())
            .create(doc)
            .await
            .unwrap();
        let mut revision_base = base(TableName::DocRevision, "space-1");
        revision_base.provenance.sources = vec![source.base.id.clone()];
        revision_base
            .provenance
            .chain
            .push(elephant_types::records::base::ProvenanceStep {
                step: "source_content".into(),
                input: source.base.id.to_string(),
                output: hash.clone(),
            });
        let revision = DocRevisionRepository::new(storage.clone())
            .create(DocRevision {
                base: revision_base,
                doc: Link::new(doc.base.id),
                text: doc.text,
                text_hash: hash,
                renderer: RendererMetadata {
                    name: "retained".into(),
                    version: "1".into(),
                    config_hash: None,
                },
                manifest: None,
                status: RevisionStatus::Active,
                supersedes: None,
                rendered_at: chrono::Utc::now(),
            })
            .await
            .unwrap();
        let node = DocNodeRepository::new(storage.clone())
            .create(DocNode {
                base: base(TableName::DocNode, "space-1"),
                revision: Link::new(revision.base.id.clone()),
                role: NodeRole::Chunk,
                span: elephant_types::records::doc_node::TextSpan::new(0, 4),
                node_key: "body".into(),
                text: None,
                parent: None,
                position: 0,
                depth: 1,
                embedding_model: None,
                collection: None,
            })
            .await
            .unwrap();
        let item = WorkItemRepository::new(storage.clone())
            .create(make_work_item("space-1"))
            .await
            .unwrap();
        let run = WorkRunRepository::new(storage.clone())
            .create(make_work_run("space-1", item.base.id))
            .await
            .unwrap();
        let extractor = CapturingExtractor::new();
        let error = execute_extract_assertions(
            &storage,
            &extractor,
            &node.base.id,
            &revision.base.id,
            &run.base.id,
            "space-1",
            "oracle",
            None,
        )
        .await
        .unwrap_err();
        assert!(
            matches!(error, PipelineError::ConfigError(message) if message.starts_with("extraction_skill_missing:"))
        );
        assert!(extractor.seen.lock().unwrap().is_none());
        assert!(WorkArtifactRepository::new(storage)
            .list("space-1", 10, None)
            .await
            .unwrap()
            .is_empty());
    }

    #[test]
    fn test_guidance_resolution_includes_trace() {
        let doc = make_doc(
            "space-1",
            serde_json::json!({
                "extraction_skill": "fiction",
                "extraction_purpose": "extract timeline"
            }),
        );
        let resolved = resolve_extraction_guidance(None, Some(&doc), None);
        assert!(
            resolved
                .resolution_trace
                .iter()
                .any(|step| step.starts_with("skill:doc.metadata=hit")),
            "expected trace to include skill hit from doc metadata"
        );
        assert!(
            resolved
                .resolution_trace
                .iter()
                .all(|step| !step.contains("space_config")),
            "trace must stop at the document winner, before unused space defaults"
        );
    }

    #[test]
    fn test_doc_metadata_can_select_example_observations_skill() {
        let doc = make_doc(
            "space-1",
            serde_json::json!({
                "extraction_skill": "extraction/example-observations",
                "extraction_purpose": "extract observational evidence"
            }),
        );

        let resolved = resolve_extraction_guidance(None, Some(&doc), None);
        assert_eq!(
            resolved.skill_id.as_deref(),
            Some("extraction/example-observations")
        );
        assert_eq!(resolved.skill_source, "doc.metadata");
        assert_eq!(
            resolved.purpose.as_deref(),
            Some("extract observational evidence")
        );
    }

    #[test]
    fn test_resolve_media_attachments_for_span_prefers_overlapping_bindings() {
        let doc = make_media_doc("space-1");
        let revision = make_media_revision("space-1", doc.base.id.clone());

        let attachments = resolve_media_attachments_for_span(&doc, &revision, 60, 180);

        assert_eq!(attachments.len(), 1);
        assert_eq!(attachments[0].attachment_id, "att-video");
        assert_eq!(
            attachments[0].time_range,
            Some(RenderManifestTimeRange {
                start_ms: Some(0),
                end_ms: Some(24_000),
            })
        );
    }

    #[test]
    fn test_resolve_media_attachments_for_span_requires_manifest_snapshot() {
        let doc = make_media_doc("space-1");
        let mut revision = make_media_revision("space-1", doc.base.id.clone());
        revision.manifest = None;

        let attachments = resolve_media_attachments_for_span(&doc, &revision, 60, 180);

        assert!(attachments.is_empty());
    }

    #[tokio::test]
    async fn test_execute_extract_assertions_passes_media_context_to_extractor() {
        use crate::renderer::multimodal::{FakeMediaRender, MultimodalRenderer};
        use crate::renderer::RendererRegistry;
        let storage = StorageClient::new_memory().await.expect("storage");
        let doc_repo = DocRepository::new(storage.clone());
        let revision_repo = DocRevisionRepository::new(storage.clone());
        let node_repo = DocNodeRepository::new(storage.clone());
        let work_item_repo = WorkItemRepository::new(storage.clone());
        let work_run_repo = WorkRunRepository::new(storage.clone());

        let source = elephant_storage::SourceRepository::new(storage.clone())
            .create(Source::new(
                base(TableName::Source, "space-1"),
                SourceType::Manual,
                crate::extraction::AssertionValidator::hash_quote("original media fixture"),
            ))
            .await
            .expect("source");
        let mut input = make_media_doc("space-1");
        input.source = Some(Link::new(source.base.id));
        input.media_attachments[1].duration_ms = Some(24_000);
        let doc = doc_repo.create(input).await.expect("create doc");
        let mut renderers = RendererRegistry::new();
        renderers.register(Box::new(MultimodalRenderer::for_tests(vec![
            FakeMediaRender {
                attachment_id: "att-image".into(),
                derived_text: "Frame details".into(),
            },
            FakeMediaRender {
                attachment_id: "att-video".into(),
                derived_text: "Alex works in Terminal. Claude reviews code.".into(),
            },
        ])));
        let rendered = super::super::evidence_work_kinds::execute_render_doc(
            &storage,
            &renderers,
            &doc,
            Some("video/mp4"),
        )
        .await
        .expect("render immutable media revision");
        let revision = revision_repo
            .get_by_id(&rendered.revision_id)
            .await
            .expect("load rendered revision")
            .expect("revision");
        let binding = revision
            .manifest
            .as_ref()
            .expect("media manifest")
            .span_bindings
            .iter()
            .find(|binding| binding.attachment_id == "att-video")
            .expect("actual video span");
        let mut node_base = revision.base.clone();
        node_base.id = RecordId::new_v4(TableName::DocNode);
        let node = node_repo
            .create(DocNode {
                base: node_base,
                revision: Link::new(revision.base.id.clone()),
                role: NodeRole::Chunk,
                span: elephant_types::records::doc_node::TextSpan::new(
                    binding.text_start,
                    binding.text_end,
                ),
                node_key: "chunk-1".to_string(),
                text: None,
                parent: None,
                position: 0,
                depth: 1,
                embedding_model: None,
                collection: None,
            })
            .await
            .expect("create node");
        let work_item = work_item_repo
            .create(make_work_item("space-1"))
            .await
            .expect("create work item");
        let work_run = work_run_repo
            .create(make_work_run("space-1", work_item.base.id.clone()))
            .await
            .expect("create work run");

        let extractor = CapturingExtractor::new();
        let seen = extractor.seen.clone();

        let result = execute_extract_assertions(
            &storage,
            &extractor,
            &node.base.id,
            &revision.base.id,
            &work_run.base.id,
            "space-1",
            "tester",
            None,
        )
        .await
        .expect("execute extraction");

        assert_eq!(result.assertions_extracted, 0);
        let captured = seen
            .lock()
            .expect("seen lock")
            .clone()
            .expect("captured context");
        let expected_doc_id = doc.base.id.to_string();
        assert_eq!(captured.doc_id.as_deref(), Some(expected_doc_id.as_str()));
        assert_eq!(captured.doc_title.as_deref(), Some("media doc"));
        assert_eq!(
            captured.sidecar_text.as_deref(),
            Some("Caller sidecar text")
        );
        assert_eq!(captured.media_attachments.len(), 1);
        assert_eq!(captured.media_attachments[0].attachment_id, "att-video");
        assert_eq!(
            captured.media_attachments[0].time_range,
            Some(RenderManifestTimeRange {
                start_ms: Some(0),
                end_ms: Some(24_000),
            })
        );
    }
}
