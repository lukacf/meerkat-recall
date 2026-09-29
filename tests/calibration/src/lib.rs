//! Prompt calibration harness for Elephant's pure-data LLM entrypoints.
//!
//! Labeled case suites live in `cases/`; the runners in
//! `tests/calibration_llm.rs` feed them to the production adjudication and
//! disambiguation entrypoints, score the verdicts with the functions here,
//! and write machine-diffable results to
//! `<target>/calibration/<suite>/results.json`. Everything in this module is
//! offline (no network, no environment reads except [`results_path`] and
//! [`calibration_model`]) and covered by non-ignored unit tests.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Datelike, Utc};
use elephant_pipeline::scheduler::{
    CandidateEntity, DisambiguationRequest, PromotionDisambiguationDecision,
};
use elephant_pipeline::truth_maintenance::adjudicator::{
    AdjudicationClaim, AdjudicationDecision, AdjudicationInput, AdjudicationVerdict, ClaimValue,
};
use elephant_types::records::attribute::Literal;
use elephant_types::records::predicate::PredicateSemantics;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Embedded TM adjudication case suite (JSON).
pub const TM_ADJUDICATION_CASES: &str = include_str!("../cases/tm_adjudication.json");

/// Embedded entity disambiguation case suite (JSON).
pub const ENTITY_DISAMBIGUATION_CASES: &str = include_str!("../cases/entity_disambiguation.json");

/// TM adjudication prompt template bytes, embedded from the pipeline crate's
/// prompt file so results can record the exact prompt version under test.
pub const TM_PROMPT_TEMPLATE: &str =
    include_str!("../../../crates/pipeline/prompts/tm_adjudication.txt");

/// Promotion disambiguation prompt template bytes.
pub const DISAMBIGUATION_PROMPT_TEMPLATE: &str =
    include_str!("../../../crates/pipeline/prompts/promotion_disambiguation.txt");

/// Hard-pass rate floor asserted by both real-LLM suites.
pub const HARD_PASS_FLOOR: f64 = 0.8;

// ---------------------------------------------------------------------------
// TM adjudication case schema
// ---------------------------------------------------------------------------

/// One labeled TM adjudication case.
#[derive(Debug, Clone, Deserialize)]
pub struct TmCase {
    /// Unique case id within the suite.
    pub id: String,
    /// Human explanation of what the case exercises.
    pub description: String,
    /// The adjudication input in serde-friendly form.
    pub input: TmCaseInput,
    /// The primary expected verdict shape.
    pub expected: TmExpectation,
    /// Alternative verdict shapes that also count as a hard pass.
    #[serde(default)]
    pub acceptable_alternatives: Vec<TmExpectation>,
    /// Free-form coverage tags.
    #[serde(default)]
    pub tags: Vec<String>,
}

/// Serde mirror of [`AdjudicationInput`] (which does not derive
/// `Deserialize`); `space_id` and `slot_key` are synthesized per case.
#[derive(Debug, Clone, Deserialize)]
pub struct TmCaseInput {
    /// Display name of the subject entity.
    pub subject_display: String,
    /// Canonical predicate id.
    pub predicate_id: String,
    /// Slot type: `rel`, `attribute`, `event`, or `rel_set`.
    pub slot_type: String,
    /// Registered predicate semantics; omit unless the case is about them.
    #[serde(default)]
    pub semantics: Option<PredicateSemantics>,
    /// The competing claims.
    pub claims: Vec<TmCaseClaim>,
}

/// Serde mirror of [`AdjudicationClaim`].
#[derive(Debug, Clone, Deserialize)]
pub struct TmCaseClaim {
    /// Claim id, unique within the case.
    pub claim_id: String,
    /// The claim's value.
    pub value: CaseValue,
    /// Claim confidence (0.0 to 1.0).
    pub confidence: f32,
    /// Number of supporting evidence references.
    pub evidence_count: usize,
    /// Recency timestamp (RFC 3339).
    pub timestamp: DateTime<Utc>,
    /// Human label of the recency basis behind `timestamp`.
    pub time_basis: String,
    /// Source identifiers backing the claim.
    #[serde(default)]
    pub sources: Vec<String>,
    /// Rendered evidence assertions backing the claim (what the sources said).
    #[serde(default)]
    pub evidence_notes: Vec<String>,
}

/// Serde mirror of [`ClaimValue`], tagged by `kind`.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CaseValue {
    /// A string literal.
    String {
        /// The string value.
        value: String,
    },
    /// An integer literal.
    Integer {
        /// The integer value.
        value: i64,
    },
    /// A float literal.
    Float {
        /// The float value.
        value: f64,
    },
    /// A boolean literal.
    Boolean {
        /// The boolean value.
        value: bool,
    },
    /// An ISO 8601 date literal.
    Date {
        /// The date string.
        value: String,
    },
    /// A reference to an object entity (rel claims).
    EntityRef {
        /// Entity record id.
        id: String,
        /// Human-readable display name.
        display: String,
    },
    /// An event claim.
    Event {
        /// Event title.
        title: String,
        /// Event start time, if known.
        #[serde(default)]
        time_start: Option<DateTime<Utc>>,
        /// Participant display names.
        #[serde(default)]
        participants: Vec<String>,
        /// Event status label.
        status: String,
    },
}

/// Expected verdict shape; unspecified fields are not scored.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TmExpectation {
    /// Expected decision label (hard).
    pub decision: String,
    /// Expected winner claim id (hard when present).
    #[serde(default)]
    pub winner_id: Option<String>,
    /// Expected superseded claim id set (hard when present; `[]` asserts
    /// nothing was superseded).
    #[serde(default)]
    pub superseded_ids: Option<Vec<String>>,
    /// Expected equivalence partition as a set of sets (soft).
    #[serde(default)]
    pub equivalence_partition: Option<Vec<Vec<String>>>,
    /// Expected proposed cardinality, `one` or `many` (soft).
    #[serde(default)]
    pub proposed_cardinality: Option<String>,
    /// Expected `valid_until` year per superseded claim (soft).
    #[serde(default)]
    pub superseded_year_by_claim: Option<BTreeMap<String, i32>>,
}

impl CaseValue {
    /// Converts the serde mirror into the production [`ClaimValue`].
    #[must_use]
    pub fn to_claim_value(&self) -> ClaimValue {
        match self {
            Self::String { value } => ClaimValue::Literal(Literal::String(value.clone())),
            Self::Integer { value } => ClaimValue::Literal(Literal::Integer(*value)),
            Self::Float { value } => ClaimValue::Literal(Literal::Float(*value)),
            Self::Boolean { value } => ClaimValue::Literal(Literal::Boolean(*value)),
            Self::Date { value } => ClaimValue::Literal(Literal::Date(value.clone())),
            Self::EntityRef { id, display } => ClaimValue::EntityRef {
                id: id.clone(),
                display: display.clone(),
            },
            Self::Event {
                title,
                time_start,
                participants,
                status,
            } => ClaimValue::EventClaim {
                title: title.clone(),
                time_start: *time_start,
                participants: participants.clone(),
                status: status.clone(),
            },
        }
    }
}

impl TmCaseClaim {
    /// Converts the serde mirror into the production [`AdjudicationClaim`].
    #[must_use]
    pub fn to_adjudication_claim(&self) -> AdjudicationClaim {
        AdjudicationClaim {
            claim_id: self.claim_id.clone(),
            value: self.value.to_claim_value(),
            confidence: self.confidence,
            evidence_count: self.evidence_count,
            timestamp: self.timestamp,
            time_basis: self.time_basis.clone(),
            sources: self.sources.clone(),
            evidence_notes: self.evidence_notes.clone(),
        }
    }
}

impl TmCaseInput {
    /// Builds the production [`AdjudicationInput`] for this case, synthesizing
    /// the space id and slot key from the case id.
    #[must_use]
    pub fn to_adjudication_input(&self, case_id: &str) -> AdjudicationInput {
        AdjudicationInput {
            space_id: "space:calibration".to_string(),
            slot_key: format!("calibration::{case_id}"),
            slot_type: self.slot_type.clone(),
            subject_display: self.subject_display.clone(),
            predicate_id: self.predicate_id.clone(),
            semantics: self.semantics.clone(),
            claims: self
                .claims
                .iter()
                .map(TmCaseClaim::to_adjudication_claim)
                .collect(),
        }
    }
}

// ---------------------------------------------------------------------------
// Entity disambiguation case schema
// ---------------------------------------------------------------------------

/// One labeled entity disambiguation case.
#[derive(Debug, Clone, Deserialize)]
pub struct DisambiguationCase {
    /// Unique case id within the suite.
    pub id: String,
    /// Human explanation of what the case exercises.
    pub description: String,
    /// The disambiguation request in serde-friendly form.
    pub input: DisambiguationCaseInput,
    /// The primary expected decision.
    pub expected: DisambiguationExpectation,
    /// Alternative decisions that also count as a hard pass.
    #[serde(default)]
    pub acceptable_alternatives: Vec<DisambiguationExpectation>,
    /// Free-form coverage tags.
    #[serde(default)]
    pub tags: Vec<String>,
}

/// Serde mirror of [`DisambiguationRequest`].
#[derive(Debug, Clone, Deserialize)]
pub struct DisambiguationCaseInput {
    /// The mention text as extracted from the document.
    pub mention: String,
    /// The incoming entity type after normalization.
    pub normalized_type: String,
    /// Source-document context around the mention.
    pub context_snippet: String,
    /// Stored entities sharing the mention's identity key.
    pub candidates: Vec<CaseCandidate>,
}

/// Serde mirror of [`CandidateEntity`].
#[derive(Debug, Clone, Deserialize)]
pub struct CaseCandidate {
    /// Entity record id.
    pub id: String,
    /// Current display name.
    pub display_name: String,
    /// Stored entity type.
    pub entity_type: String,
    /// Entity summary, when one exists.
    #[serde(default)]
    pub summary: Option<String>,
}

/// Expected disambiguation decision.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DisambiguationExpectation {
    /// `reuse` or `create_new` (hard).
    pub action: String,
    /// Candidate id that must be reused (hard for `reuse`).
    #[serde(default)]
    pub entity_id: Option<String>,
}

impl DisambiguationCaseInput {
    /// Builds the production [`DisambiguationRequest`] for this case.
    #[must_use]
    pub fn to_request(&self) -> DisambiguationRequest {
        DisambiguationRequest {
            mention: self.mention.clone(),
            normalized_type: self.normalized_type.clone(),
            context_snippet: self.context_snippet.clone(),
            candidates: self
                .candidates
                .iter()
                .map(|candidate| CandidateEntity {
                    id: candidate.id.clone(),
                    display_name: candidate.display_name.clone(),
                    entity_type: candidate.entity_type.clone(),
                    summary: candidate.summary.clone(),
                })
                .collect(),
        }
    }
}

// ---------------------------------------------------------------------------
// Suite loading and validation
// ---------------------------------------------------------------------------

/// Loads and validates the embedded TM adjudication suite.
///
/// # Errors
///
/// Returns an error when the JSON fails to parse or a case is internally
/// inconsistent (duplicate ids, expectations referencing unknown claim ids,
/// malformed partitions, unknown labels).
pub fn load_tm_cases() -> Result<Vec<TmCase>> {
    let cases: Vec<TmCase> = serde_json::from_str(TM_ADJUDICATION_CASES)
        .context("parsing cases/tm_adjudication.json")?;
    validate_tm_suite(&cases)?;
    Ok(cases)
}

/// Loads and validates the embedded entity disambiguation suite.
///
/// # Errors
///
/// Returns an error when the JSON fails to parse or a case is internally
/// inconsistent (duplicate ids, unknown actions, reuse expectations pointing
/// at ids that are not among the candidates).
pub fn load_disambiguation_cases() -> Result<Vec<DisambiguationCase>> {
    let cases: Vec<DisambiguationCase> = serde_json::from_str(ENTITY_DISAMBIGUATION_CASES)
        .context("parsing cases/entity_disambiguation.json")?;
    validate_disambiguation_suite(&cases)?;
    Ok(cases)
}

/// Validates internal consistency of a TM case suite.
///
/// # Errors
///
/// Returns an error describing the first inconsistency found.
pub fn validate_tm_suite(cases: &[TmCase]) -> Result<()> {
    let mut ids = BTreeSet::new();
    for case in cases {
        if !ids.insert(case.id.as_str()) {
            bail!("duplicate case id '{}'", case.id);
        }
        if case.input.claims.is_empty() {
            bail!("case '{}' has no claims", case.id);
        }
        let mut claim_ids = BTreeSet::new();
        for claim in &case.input.claims {
            if !claim_ids.insert(claim.claim_id.as_str()) {
                bail!("case '{}' repeats claim id '{}'", case.id, claim.claim_id);
            }
        }
        for expectation in std::iter::once(&case.expected).chain(&case.acceptable_alternatives) {
            validate_tm_expectation(&case.id, expectation, &claim_ids)?;
        }
    }
    Ok(())
}

fn validate_tm_expectation(
    case_id: &str,
    expectation: &TmExpectation,
    claim_ids: &BTreeSet<&str>,
) -> Result<()> {
    if parse_decision_label(&expectation.decision).is_none() {
        bail!(
            "case '{case_id}' expects unknown decision '{}'",
            expectation.decision
        );
    }
    if let Some(winner) = &expectation.winner_id {
        if !claim_ids.contains(winner.as_str()) {
            bail!("case '{case_id}' expects unknown winner_id '{winner}'");
        }
    }
    for id in expectation.superseded_ids.iter().flatten() {
        if !claim_ids.contains(id.as_str()) {
            bail!("case '{case_id}' expects unknown superseded id '{id}'");
        }
    }
    if let Some(partition) = &expectation.equivalence_partition {
        let mut seen = BTreeSet::new();
        for group in partition {
            for id in group {
                if !claim_ids.contains(id.as_str()) {
                    bail!("case '{case_id}' partition references unknown claim id '{id}'");
                }
                if !seen.insert(id.as_str()) {
                    bail!("case '{case_id}' partition repeats claim id '{id}'");
                }
            }
        }
    }
    if let Some(cardinality) = &expectation.proposed_cardinality {
        if cardinality != "one" && cardinality != "many" {
            bail!("case '{case_id}' expects unknown cardinality '{cardinality}'");
        }
    }
    for id in expectation
        .superseded_year_by_claim
        .iter()
        .flat_map(BTreeMap::keys)
    {
        if !claim_ids.contains(id.as_str()) {
            bail!("case '{case_id}' superseded_year_by_claim references unknown claim id '{id}'");
        }
    }
    Ok(())
}

/// Validates internal consistency of a disambiguation case suite.
///
/// # Errors
///
/// Returns an error describing the first inconsistency found.
pub fn validate_disambiguation_suite(cases: &[DisambiguationCase]) -> Result<()> {
    let mut ids = BTreeSet::new();
    for case in cases {
        if !ids.insert(case.id.as_str()) {
            bail!("duplicate case id '{}'", case.id);
        }
        if case.input.candidates.is_empty() {
            bail!("case '{}' has no candidates", case.id);
        }
        let mut candidate_ids = BTreeSet::new();
        for candidate in &case.input.candidates {
            if !candidate_ids.insert(candidate.id.as_str()) {
                bail!("case '{}' repeats candidate id '{}'", case.id, candidate.id);
            }
        }
        for expectation in std::iter::once(&case.expected).chain(&case.acceptable_alternatives) {
            match expectation.action.as_str() {
                "create_new" => {}
                "reuse" => {
                    let Some(entity_id) = &expectation.entity_id else {
                        bail!("case '{}' expects reuse without an entity_id", case.id);
                    };
                    if !candidate_ids.contains(entity_id.as_str()) {
                        bail!(
                            "case '{}' expects reuse of unknown candidate '{entity_id}'",
                            case.id
                        );
                    }
                }
                other => bail!("case '{}' expects unknown action '{other}'", case.id),
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Scoring
// ---------------------------------------------------------------------------

/// Maps a case-file decision label to the production decision enum.
#[must_use]
pub fn parse_decision_label(label: &str) -> Option<AdjudicationDecision> {
    match label {
        "corroborate" => Some(AdjudicationDecision::Corroborate),
        "single_winner" => Some(AdjudicationDecision::SingleWinner),
        "coexist" => Some(AdjudicationDecision::Coexist),
        "supersede" => Some(AdjudicationDecision::Supersede),
        "needs_review" => Some(AdjudicationDecision::NeedsReview),
        _ => None,
    }
}

/// Stable label for a decision, matching the case-file vocabulary.
#[must_use]
pub const fn decision_label(decision: AdjudicationDecision) -> &'static str {
    match decision {
        AdjudicationDecision::Corroborate => "corroborate",
        AdjudicationDecision::SingleWinner => "single_winner",
        AdjudicationDecision::Coexist => "coexist",
        AdjudicationDecision::Supersede => "supersede",
        AdjudicationDecision::NeedsReview => "needs_review",
    }
}

/// Soft-field scores; `None` means the case did not specify the field.
#[derive(Debug, Clone, Serialize)]
pub struct SoftScores {
    /// Equivalence partition matched (set-of-sets equality).
    pub equivalence_partition: Option<bool>,
    /// Proposed cardinality matched.
    pub proposed_cardinality: Option<bool>,
    /// All expected superseded `valid_until` years matched.
    pub superseded_years: Option<bool>,
}

/// Outcome of scoring one TM case against a verdict.
#[derive(Debug, Clone, Serialize)]
pub struct TmCaseScore {
    /// Whether any expectation block matched on all hard fields.
    pub hard_pass: bool,
    /// Index of the matching expectation (0 = primary, 1.. = alternatives).
    pub matched_expectation: Option<usize>,
    /// Hard-field misses against the primary expectation when nothing matched.
    pub missed_hard: Vec<String>,
    /// Soft scores against the matched expectation (primary on hard fail).
    pub soft: SoftScores,
}

fn hard_misses(expectation: &TmExpectation, verdict: &AdjudicationVerdict) -> Vec<String> {
    let mut misses = Vec::new();
    match parse_decision_label(&expectation.decision) {
        Some(decision) if decision == verdict.decision => {}
        Some(_) | None => misses.push(format!(
            "decision: expected {}, got {}",
            expectation.decision,
            decision_label(verdict.decision)
        )),
    }
    if let Some(winner) = &expectation.winner_id {
        if verdict.winner_id.as_deref() != Some(winner.as_str()) {
            misses.push(format!(
                "winner_id: expected {winner}, got {}",
                verdict.winner_id.as_deref().unwrap_or("(none)")
            ));
        }
    }
    if let Some(ids) = &expectation.superseded_ids {
        let expected: BTreeSet<&str> = ids.iter().map(String::as_str).collect();
        let got: BTreeSet<&str> = verdict
            .superseded
            .iter()
            .map(|entry| entry.claim_id.as_str())
            .collect();
        if expected != got {
            misses.push(format!(
                "superseded_ids: expected {expected:?}, got {got:?}"
            ));
        }
    }
    misses
}

fn as_partition(groups: &[Vec<String>]) -> BTreeSet<BTreeSet<&str>> {
    groups
        .iter()
        .map(|group| group.iter().map(String::as_str).collect())
        .collect()
}

fn soft_scores(expectation: &TmExpectation, verdict: &AdjudicationVerdict) -> SoftScores {
    let equivalence_partition = expectation
        .equivalence_partition
        .as_ref()
        .map(|partition| as_partition(partition) == as_partition(&verdict.equivalent_groups));
    let proposed_cardinality = expectation
        .proposed_cardinality
        .as_ref()
        .map(|cardinality| {
            verdict
                .proposed_semantics
                .is_some_and(|semantics| semantics.cardinality.to_string() == *cardinality)
        });
    let superseded_years = expectation.superseded_year_by_claim.as_ref().map(|years| {
        years.iter().all(|(claim_id, year)| {
            verdict.superseded.iter().any(|entry| {
                entry.claim_id == *claim_id
                    && entry.valid_until.is_some_and(|date| date.year() == *year)
            })
        })
    });
    SoftScores {
        equivalence_partition,
        proposed_cardinality,
        superseded_years,
    }
}

/// Scores a verdict against a TM case: hard pass when the primary expectation
/// or any acceptable alternative matches on every specified hard field.
#[must_use]
pub fn score_tm_case(case: &TmCase, verdict: &AdjudicationVerdict) -> TmCaseScore {
    let mut primary_misses = Vec::new();
    for (index, expectation) in std::iter::once(&case.expected)
        .chain(&case.acceptable_alternatives)
        .enumerate()
    {
        let misses = hard_misses(expectation, verdict);
        if misses.is_empty() {
            return TmCaseScore {
                hard_pass: true,
                matched_expectation: Some(index),
                missed_hard: Vec::new(),
                soft: soft_scores(expectation, verdict),
            };
        }
        if index == 0 {
            primary_misses = misses;
        }
    }
    TmCaseScore {
        hard_pass: false,
        matched_expectation: None,
        missed_hard: primary_misses,
        soft: soft_scores(&case.expected, verdict),
    }
}

/// Outcome of scoring one disambiguation case.
#[derive(Debug, Clone, Serialize)]
pub struct DisambiguationScore {
    /// Whether any expectation matched.
    pub hard_pass: bool,
    /// Index of the matching expectation (0 = primary, 1.. = alternatives).
    pub matched_expectation: Option<usize>,
    /// Misses against the primary expectation when nothing matched.
    pub missed_hard: Vec<String>,
}

fn disambiguation_misses(
    expectation: &DisambiguationExpectation,
    decision: &PromotionDisambiguationDecision,
) -> Vec<String> {
    let (action, entity_id) = match decision {
        PromotionDisambiguationDecision::Reuse { entity_id } => ("reuse", Some(entity_id.as_str())),
        PromotionDisambiguationDecision::CreateNew => ("create_new", None),
    };
    let mut misses = Vec::new();
    if expectation.action != action {
        misses.push(format!(
            "action: expected {}, got {action}",
            expectation.action
        ));
    }
    if let Some(expected_id) = &expectation.entity_id {
        if entity_id != Some(expected_id.as_str()) {
            misses.push(format!(
                "entity_id: expected {expected_id}, got {}",
                entity_id.unwrap_or("(none)")
            ));
        }
    }
    misses
}

/// Scores a disambiguation decision (or its absence) against a case.
#[must_use]
pub fn score_disambiguation_case(
    case: &DisambiguationCase,
    decision: Option<&PromotionDisambiguationDecision>,
) -> DisambiguationScore {
    let Some(decision) = decision else {
        return DisambiguationScore {
            hard_pass: false,
            matched_expectation: None,
            missed_hard: vec!["no decision returned".to_string()],
        };
    };
    let mut primary_misses = Vec::new();
    for (index, expectation) in std::iter::once(&case.expected)
        .chain(&case.acceptable_alternatives)
        .enumerate()
    {
        let misses = disambiguation_misses(expectation, decision);
        if misses.is_empty() {
            return DisambiguationScore {
                hard_pass: true,
                matched_expectation: Some(index),
                missed_hard: Vec::new(),
            };
        }
        if index == 0 {
            primary_misses = misses;
        }
    }
    DisambiguationScore {
        hard_pass: false,
        matched_expectation: None,
        missed_hard: primary_misses,
    }
}

// ---------------------------------------------------------------------------
// Results output
// ---------------------------------------------------------------------------

/// Per-case entry in the results file.
#[derive(Debug, Serialize)]
pub struct CaseResult {
    /// Case id.
    pub id: String,
    /// Whether the case hard-passed.
    pub hard_pass: bool,
    /// Index of the matched expectation, when one matched.
    pub matched_expectation: Option<usize>,
    /// Hard-field misses (against the primary expectation on failure).
    pub missed_hard: Vec<String>,
    /// Soft scores (TM suite only).
    pub soft: Option<SoftScores>,
    /// The verdict/decision as returned, serialized for diffing.
    pub verdict: serde_json::Value,
    /// Model rationale, when the entrypoint returns one.
    pub rationale: String,
}

/// Suite-level results written to `<target>/calibration/<suite>/results.json`.
#[derive(Debug, Serialize)]
pub struct SuiteResults {
    /// Suite name (`tm_adjudication` or `entity_disambiguation`).
    pub suite: String,
    /// Model that produced the verdicts.
    pub model: String,
    /// SHA-256 of the prompt template file bytes.
    pub prompt_sha256: String,
    /// What was hashed (always `template_file_bytes` today).
    pub prompt_hash_source: String,
    /// When the run finished.
    pub timestamp: DateTime<Utc>,
    /// Fraction of cases that hard-passed.
    pub hard_pass_rate: f64,
    /// Per-case results.
    pub cases: Vec<CaseResult>,
}

/// Hex-encoded SHA-256 of `bytes`.
#[must_use]
pub fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// Serializes a production verdict into a diff-friendly JSON value.
#[must_use]
pub fn verdict_to_json(verdict: &AdjudicationVerdict) -> serde_json::Value {
    serde_json::json!({
        "decision": decision_label(verdict.decision),
        "winner_id": &verdict.winner_id,
        "equivalent_groups": &verdict.equivalent_groups,
        "superseded": verdict
            .superseded
            .iter()
            .map(|entry| serde_json::json!({
                "claim_id": &entry.claim_id,
                "valid_until": entry.valid_until.map(|date| date.to_rfc3339()),
            }))
            .collect::<Vec<_>>(),
        "winner_valid_from": verdict.winner_valid_from.map(|date| date.to_rfc3339()),
        "proposed_semantics": verdict.proposed_semantics.map(|semantics| serde_json::json!({
            "cardinality": semantics.cardinality.to_string(),
            "temporal_model": semantics.temporal_model.to_string(),
            "conflict_policy": semantics.conflict_policy.to_string(),
        })),
        "rationale": &verdict.rationale,
    })
}

/// Serializes a disambiguation decision into a diff-friendly JSON value.
#[must_use]
pub fn disambiguation_decision_to_json(
    decision: Option<&PromotionDisambiguationDecision>,
) -> serde_json::Value {
    match decision {
        Some(PromotionDisambiguationDecision::Reuse { entity_id }) => {
            serde_json::json!({ "action": "reuse", "entity_id": entity_id })
        }
        Some(PromotionDisambiguationDecision::CreateNew) => {
            serde_json::json!({ "action": "create_new" })
        }
        None => serde_json::json!({ "action": null }),
    }
}

/// Resolves the results file path for a suite: `$CARGO_TARGET_DIR` when set
/// (the `repo-cargo` wrapper always sets it), else the workspace `target/`.
#[must_use]
pub fn results_path(suite: &str) -> PathBuf {
    let target = std::env::var_os("CARGO_TARGET_DIR").map_or_else(
        || PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target"),
        PathBuf::from,
    );
    target.join("calibration").join(suite).join("results.json")
}

/// Writes suite results as pretty JSON and returns the path written.
///
/// # Errors
///
/// Returns an error when the directory cannot be created or the file cannot
/// be written.
pub fn write_results(results: &SuiteResults) -> Result<PathBuf> {
    let path = results_path(&results.suite);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    let json = serde_json::to_string_pretty(results).context("serializing suite results")?;
    std::fs::write(&path, json).with_context(|| format!("writing {}", path.display()))?;
    Ok(path)
}

/// Model override from `CALIBRATION_MODEL`, when set and non-empty.
#[must_use]
pub fn calibration_model() -> Option<String> {
    std::env::var("CALIBRATION_MODEL")
        .ok()
        .filter(|model| !model.trim().is_empty())
}

/// Single-line rationale excerpt for table output.
#[must_use]
pub fn rationale_excerpt(rationale: &str, max_chars: usize) -> String {
    let flat = rationale.split_whitespace().collect::<Vec<_>>().join(" ");
    if flat.chars().count() <= max_chars {
        flat
    } else {
        let truncated: String = flat.chars().take(max_chars).collect();
        format!("{truncated}…")
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use chrono::TimeZone;
    use elephant_pipeline::truth_maintenance::adjudicator::{ProposedSemantics, SupersededClaim};
    use elephant_types::records::predicate::Cardinality;
    use elephant_types::records::temporal::TemporalPersonality;
    use elephant_types::records::truth::ConflictPolicy;

    use super::*;

    fn verdict(decision: AdjudicationDecision) -> AdjudicationVerdict {
        AdjudicationVerdict {
            decision,
            winner_id: None,
            equivalent_groups: Vec::new(),
            superseded: Vec::new(),
            winner_valid_from: None,
            proposed_semantics: None,
            rationale: "synthetic verdict".to_string(),
        }
    }

    #[test]
    fn tm_case_suite_parses_and_validates() {
        let cases = load_tm_cases().expect("tm_adjudication.json must be valid");
        assert_eq!(cases.len(), 22);

        // Every case converts into a production input with intact claims.
        for case in &cases {
            let input = case.input.to_adjudication_input(&case.id);
            assert_eq!(input.claims.len(), case.input.claims.len());
            assert_eq!(input.slot_key, format!("calibration::{}", case.id));
        }

        // Exactly the two configured-semantics cases carry semantics.
        let configured: Vec<&str> = cases
            .iter()
            .filter(|case| case.input.semantics.is_some())
            .map(|case| case.id.as_str())
            .collect();
        assert_eq!(configured, vec!["configured_one_age", "configured_one_ceo"]);
    }

    #[test]
    fn disambiguation_suite_parses_and_validates() {
        let cases = load_disambiguation_cases().expect("entity_disambiguation.json must be valid");
        assert_eq!(cases.len(), 10);
        for case in &cases {
            let request = case.input.to_request();
            assert_eq!(request.candidates.len(), case.input.candidates.len());
        }
    }

    #[test]
    fn suite_validation_rejects_inconsistent_cases() {
        let mut cases = load_tm_cases().unwrap();
        cases[0].expected.winner_id = Some("ghost".to_string());
        assert!(validate_tm_suite(&cases)
            .unwrap_err()
            .to_string()
            .contains("unknown winner_id"));

        let mut cases = load_disambiguation_cases().unwrap();
        cases[0].expected.entity_id = Some("entity:ghost".to_string());
        cases[0].expected.action = "reuse".to_string();
        assert!(validate_disambiguation_suite(&cases)
            .unwrap_err()
            .to_string()
            .contains("unknown candidate"));
    }

    #[test]
    fn scoring_matches_synthetic_verdicts() {
        let cases = load_tm_cases().unwrap();
        let supersede_case = cases
            .iter()
            .find(|case| case.id == "supersede_employer_2024")
            .unwrap();

        let mut good = verdict(AdjudicationDecision::Supersede);
        good.winner_id = Some("c2".to_string());
        good.superseded = vec![SupersededClaim {
            claim_id: "c1".to_string(),
            valid_until: Some(Utc.with_ymd_and_hms(2024, 2, 1, 0, 0, 0).unwrap()),
        }];
        good.equivalent_groups = vec![vec!["c1".to_string()], vec!["c2".to_string()]];
        good.proposed_semantics = Some(ProposedSemantics {
            cardinality: Cardinality::One,
            temporal_model: TemporalPersonality::StateOverTime,
            conflict_policy: ConflictPolicy::SupersedeByRecency,
        });

        let score = score_tm_case(supersede_case, &good);
        assert!(score.hard_pass);
        assert_eq!(score.matched_expectation, Some(0));
        assert_eq!(score.soft.equivalence_partition, Some(true));
        assert_eq!(score.soft.proposed_cardinality, Some(true));
        assert_eq!(score.soft.superseded_years, Some(true));

        // Wrong winner fails hard and reports the field.
        let mut wrong_winner = good;
        wrong_winner.winner_id = Some("c1".to_string());
        wrong_winner.superseded[0].claim_id = "c2".to_string();
        let score = score_tm_case(supersede_case, &wrong_winner);
        assert!(!score.hard_pass);
        assert!(score.missed_hard.iter().any(|m| m.contains("winner_id")));
        assert!(score
            .missed_hard
            .iter()
            .any(|m| m.contains("superseded_ids")));

        // A case with alternatives passes via the alternative block.
        let ambiguous_case = cases
            .iter()
            .find(|case| case.id == "ambiguous_birthplace")
            .unwrap();
        let mut alt = verdict(AdjudicationDecision::SingleWinner);
        alt.winner_id = Some("c1".to_string());
        let score = score_tm_case(ambiguous_case, &alt);
        assert!(score.hard_pass);
        assert_eq!(score.matched_expectation, Some(1));

        // Disambiguation: right id passes, wrong id and no decision fail.
        let dis_cases = load_disambiguation_cases().unwrap();
        let mercury = &dis_cases[0];
        let reuse_planet = PromotionDisambiguationDecision::Reuse {
            entity_id: "entity:mercury-planet".to_string(),
        };
        assert!(score_disambiguation_case(mercury, Some(&reuse_planet)).hard_pass);
        let reuse_element = PromotionDisambiguationDecision::Reuse {
            entity_id: "entity:mercury-element".to_string(),
        };
        let score = score_disambiguation_case(mercury, Some(&reuse_element));
        assert!(!score.hard_pass);
        assert!(score.missed_hard.iter().any(|m| m.contains("entity_id")));
        assert!(!score_disambiguation_case(mercury, None).hard_pass);
    }

    #[test]
    fn results_json_serialization_is_stable() {
        let mut winner = verdict(AdjudicationDecision::Supersede);
        winner.winner_id = Some("c2".to_string());
        winner.superseded = vec![SupersededClaim {
            claim_id: "c1".to_string(),
            valid_until: Some(Utc.with_ymd_and_hms(2024, 2, 1, 0, 0, 0).unwrap()),
        }];

        let results = SuiteResults {
            suite: "tm_adjudication".to_string(),
            model: "claude-sonnet-4-5".to_string(),
            prompt_sha256: sha256_hex(TM_PROMPT_TEMPLATE.as_bytes()),
            prompt_hash_source: "template_file_bytes".to_string(),
            timestamp: Utc.with_ymd_and_hms(2026, 7, 1, 0, 0, 0).unwrap(),
            hard_pass_rate: 0.95,
            cases: vec![CaseResult {
                id: "supersede_employer_2024".to_string(),
                hard_pass: true,
                matched_expectation: Some(0),
                missed_hard: Vec::new(),
                soft: Some(SoftScores {
                    equivalence_partition: Some(true),
                    proposed_cardinality: None,
                    superseded_years: Some(true),
                }),
                verdict: verdict_to_json(&winner),
                rationale: "Alice moved to Blizzard in 2024.".to_string(),
            }],
        };

        let json: serde_json::Value =
            serde_json::from_str(&serde_json::to_string_pretty(&results).unwrap()).unwrap();
        assert_eq!(json["suite"], "tm_adjudication");
        assert_eq!(json["prompt_hash_source"], "template_file_bytes");
        assert_eq!(json["prompt_sha256"].as_str().unwrap().len(), 64);
        assert_eq!(json["cases"][0]["verdict"]["decision"], "supersede");
        assert_eq!(
            json["cases"][0]["verdict"]["superseded"][0]["valid_until"],
            "2024-02-01T00:00:00+00:00"
        );

        // Known-answer check keeps the hash implementation honest.
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );

        let decision = PromotionDisambiguationDecision::CreateNew;
        assert_eq!(
            disambiguation_decision_to_json(Some(&decision)),
            serde_json::json!({ "action": "create_new" })
        );

        assert_eq!(rationale_excerpt("a\n b   c", 10), "a b c");
        assert_eq!(rationale_excerpt("abcdef", 3), "abc…");
    }
}
