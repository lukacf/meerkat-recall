//! Bounded revision-aware search shared by scoped HTTP and MCP adapters.
use crate::{current_read, handlers::ToolContext};
use elephant_embeddings::{discover_accessible_partition_keys, PartitionKey, PartitionManager};
use elephant_storage::read_eligibility::ReadEligibility;
use elephant_storage::source_publication::SourcePublicationRepository;
use elephant_storage::source_publication_manifest::SourcePublicationManifestRepository;
use elephant_storage::{
    AssertionRepository, DocNodeRepository, DocRepository, DocRevisionRepository, EntityRepository,
    NodeEmbeddingRepository, Repository, SearchCriteria, SearchRepository, SourceRepository,
    SpaceConfigRepository,
};
use elephant_types::public_error::{ErrorCode, PublicError};
use elephant_types::publication::{
    OperationState, SourceAuthorizationSnapshot, SourceHead, SourceVisibility,
};
use elephant_types::records::assertion::EvidenceSpan;
use elephant_types::records::source::SourceType;
use elephant_types::records::{Assertion, Entity, Source};
use elephant_types::{Link, RecordId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};

#[path = "scoped_query_vectors.rs"]
mod query_vectors;

/// Maximum encoded public search response.
pub const MAX_SEARCH_RESPONSE_BYTES: usize = 262_144;
const MAX_CANDIDATES: usize = 400;
const MAX_SEMANTIC_PARTITIONS: usize = 64;
const MAX_SEMANTIC_CANDIDATES: usize = 4096;
const MAX_EVIDENCE: usize = 3;
const MAX_RESULT_TEXT_BYTES: usize = 1024;

fn valid_identifier(value: &str, max_bytes: usize) -> bool {
    !value.trim().is_empty() && value.len() <= max_bytes && !value.chars().any(char::is_control)
}

/// Single- or multiple-valued source labels.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum LabelValue {
    /// A single value; filter matches membership.
    One(String),
    /// Any listed value may match.
    Many(Vec<String>),
}
impl LabelValue {
    fn values(&self) -> Vec<&str> {
        match self {
            Self::One(value) => vec![value.as_str()],
            Self::Many(values) => values.iter().map(String::as_str).collect(),
        }
    }
    fn valid(&self) -> bool {
        let values = self.values();
        !values.is_empty()
            && values.len() <= 64
            && values.iter().all(|value| valid_identifier(value, 1024))
    }
}
/// Supported scoped filters; unrecognized filters are rejected.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchFilters {
    /// All keys must match; each key's values use any semantics.
    #[serde(default)]
    pub labels: BTreeMap<String, LabelValue>,
}

/// Candidate discovery and ranking used for the original query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RankingMode {
    /// Provider-free text retrieval.
    Lexical,
    /// Actual indexed node similarity, without lexical candidates or rescue.
    Semantic,
    /// Retained lexical and vector reciprocal-rank fusion.
    Hybrid,
}

fn present_value<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    // Missing fields use default None; explicitly null is not a valid option.
    T::deserialize(deserializer).map(Some)
}
/// Public scoped search input.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchRequest {
    /// Original query.
    pub query: String,
    /// Number of ranked results, 1 through 50.
    #[serde(default = "default_limit")]
    pub limit: usize,
    /// Exact metadata filters.
    #[serde(default)]
    pub filters: SearchFilters,
    /// External source IDs mapped to required immutable source revisions.
    #[serde(default)]
    pub source_revision_constraints: BTreeMap<String, String>,
    /// Explicit ranking. Mutually exclusive with the legacy `use_vector` field.
    #[serde(
        default,
        deserialize_with = "present_value",
        skip_serializing_if = "Option::is_none"
    )]
    pub ranking_mode: Option<RankingMode>,
    /// Legacy selector: false means lexical; true means hybrid.
    #[serde(
        default,
        deserialize_with = "present_value",
        skip_serializing_if = "Option::is_none"
    )]
    pub use_vector: Option<bool>,
}
const fn default_limit() -> usize {
    10
}
impl SearchRequest {
    /// Preserve legacy defaults while refusing ambiguous explicit selectors.
    ///
    /// # Errors
    /// Returns invalid input when both ranking selectors are supplied.
    pub fn effective_ranking_mode(&self) -> Result<RankingMode, PublicError> {
        match (self.ranking_mode, self.use_vector) {
            (Some(_), Some(_)) => Err(invalid(
                "ranking_mode and use_vector are mutually exclusive",
            )),
            (Some(mode), None) => Ok(mode),
            (None, Some(false)) => Ok(RankingMode::Lexical),
            (None, Some(true) | None) => Ok(RankingMode::Hybrid),
        }
    }

    /// Reject malformed bounds before storage or provider work.
    ///
    /// # Errors
    /// Returns invalid input for conflicting ranking selectors, invalid query or
    /// result bounds, or malformed source revision constraints or labels.
    pub fn validate(&self) -> Result<(), PublicError> {
        self.effective_ranking_mode()?;
        if self.query.trim().is_empty()
            || self.query.len() > 4096
            || !(1..=50).contains(&self.limit)
        {
            return Err(invalid(
                "query must contain 1..4096 UTF-8 bytes and limit must be 1..50",
            ));
        }
        if self.source_revision_constraints.len() > 100
            || self
                .source_revision_constraints
                .iter()
                .any(|(id, revision)| {
                    !valid_identifier(id, 1024) || !valid_identifier(revision, 256)
                })
            || self.filters.labels.len() > 64
            || self
                .filters
                .labels
                .iter()
                .any(|(key, value)| !valid_identifier(key, 128) || !value.valid())
        {
            return Err(invalid("Invalid source revision constraints or labels"));
        }
        Ok(())
    }
}
/// Exact half-open UTF-8 byte interval in the identified source or rendered text.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchSpan {
    /// Inclusive start.
    pub start: u32,
    /// Exclusive end.
    pub end: u32,
}
/// One authorized, current immutable source passage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchEvidence {
    /// External identity, never an implicit internal record ID.
    pub source_id: String,
    /// Exact consumer revision.
    pub revision: String,
    /// Exact original byte offsets.
    pub span: SearchSpan,
    /// Bytes at those offsets.
    pub quote: String,
    /// Exact containing document ID: `doc:` plus a lowercase hyphenated UUID
    /// (40 ASCII bytes), used by the immutable revision read route.
    pub doc_id: String,
    /// Explicit internal provenance identifier.
    pub internal_doc_revision_id: String,
    /// Exact support interval in that immutable rendered revision.
    pub rendered_span: SearchSpan,
    /// SHA-256 of the exact rendered support bytes, as 64 lowercase hex digits.
    pub rendered_quote_sha256: String,
    /// SHA-256 of the entire immutable rendered text, as 64 lowercase hex digits.
    pub rendered_text_sha256: String,
}
/// Search result family.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchKind {
    /// Source-backed claim.
    Claim,
    /// Canonical entity.
    Entity,
    /// Exact source passage.
    Chunk,
}
/// Bounded result, without vectors.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    /// Record family.
    pub kind: SearchKind,
    /// Complete claim/passage text within the encoded response budget;
    /// entity display names alone may be shortened at a UTF-8 boundary.
    pub text: String,
    /// Current exact support.
    pub evidence: Vec<SearchEvidence>,
    /// Cosine similarity in semantic mode; reciprocal-rank score otherwise.
    /// Neither score is a probability.
    pub score: f64,
}
/// Coverage is independent from the number of matches.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceCoverage {
    /// Requested revision, if constrained.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requested_revision: Option<String>,
    /// Last completely published revision.
    pub published_revision: Option<String>,
    /// Current accepted revision.
    pub desired_revision: Option<String>,
    /// Public operation/eligibility state.
    pub state: String,
}
/// Actual query response with explicit source coverage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResponse {
    /// Ranked results for the original query.
    pub results: Vec<SearchResult>,
    /// Coverage keyed by external source identity.
    pub coverage: BTreeMap<String, SourceCoverage>,
    /// A required or returned source has incomplete coverage.
    pub partial: bool,
}
fn invalid(message: &str) -> PublicError {
    PublicError::new(ErrorCode::InvalidRequest, message)
}
fn unavailable(_: elephant_storage::StorageError) -> PublicError {
    PublicError::new(ErrorCode::Unavailable, "Search storage is unavailable")
}
fn binding_error(
    error: elephant_storage::space_config_repository::SpaceResolutionError,
) -> PublicError {
    match error {
        elephant_storage::space_config_repository::SpaceResolutionError::Storage(error) => {
            unavailable(error)
        }
        elephant_storage::space_config_repository::SpaceResolutionError::Authorization(_) => {
            PublicError::new(
                ErrorCode::ProviderUnapproved,
                "Query embedding approval is unavailable",
            )
        }
    }
}
fn embedding_resolution_error(error: elephant_pipeline::PipelineError) -> PublicError {
    match error {
        elephant_pipeline::PipelineError::StorageError(error) => unavailable(error),
        elephant_pipeline::PipelineError::ConfigError(_) => PublicError::new(
            ErrorCode::ProviderUnapproved,
            "No approved query embedding binding",
        ),
        _ => PublicError::new(
            ErrorCode::Unavailable,
            "Query embedding resolution is unavailable",
        ),
    }
}
fn semantic_budget(partitions: usize, candidates: usize) -> Result<(), PublicError> {
    if partitions > MAX_SEMANTIC_PARTITIONS || candidates > MAX_SEMANTIC_CANDIDATES {
        return Err(PublicError::new(
            ErrorCode::RateLimited,
            "Semantic search exceeds the bounded index candidate budget",
        ));
    }
    Ok(())
}
fn response_budget(response: &SearchResponse) -> Result<(), PublicError> {
    struct BudgetWriter {
        remaining: usize,
        exceeded: bool,
    }
    impl std::io::Write for BudgetWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.remaining {
                self.exceeded = true;
                return Err(std::io::Error::other("search response byte budget"));
            }
            self.remaining -= bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    // Count actual JSON bytes (including escaping) without allocating a second
    // potentially oversized serialized body. Only selected results pay this cap.
    let mut writer = BudgetWriter {
        remaining: MAX_SEARCH_RESPONSE_BYTES,
        exceeded: false,
    };
    serde_json::to_writer(&mut writer, response).map_err(|_| {
        if writer.exceeded {
            PublicError::new(
                ErrorCode::PayloadTooLarge,
                "Complete selected evidence exceeds the search response byte budget; lower limit",
            )
        } else {
            PublicError::new(ErrorCode::Unavailable, "Search serialization failed")
        }
    })
}

fn boundary_prefix(text: &str, bytes: usize) -> &str {
    let mut end = bytes.min(text.len());
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}
/// Scalar label filters match membership; list filters match any value.
#[must_use]
pub fn labels_match(source: &Source, filters: &BTreeMap<String, LabelValue>) -> bool {
    filters.iter().all(|(key, wanted)| {
        let Some(stored) = source
            .metadata
            .get("labels")
            .and_then(|labels| labels.get(key))
        else {
            return false;
        };
        let values: Vec<&str> = match stored {
            serde_json::Value::String(value) => vec![value.as_str()],
            serde_json::Value::Array(values) => values
                .iter()
                .filter_map(serde_json::Value::as_str)
                .collect(),
            _ => return false,
        };
        wanted.values().iter().any(|wanted| values.contains(wanted))
    })
}
/// Compare opaque revision identities without ordering them.
///
/// A matching current desired revision also requires its current acceptance
/// operation to complete;
/// an older publication can remain searchable during a fresh A-B-A acceptance.
#[must_use]
pub fn coverage(
    head: Option<&SourceHead>,
    requested: Option<&str>,
    operation: Option<OperationState>,
) -> SourceCoverage {
    let Some(head) = head else {
        return SourceCoverage {
            requested_revision: requested.map(str::to_owned),
            published_revision: None,
            desired_revision: None,
            state: "missing".into(),
        };
    };
    let state = match head.lifecycle {
        SourceVisibility::Erased | SourceVisibility::Erasing => "erased",
        SourceVisibility::Retracted => "retracted",
        SourceVisibility::Active => {
            let expected = requested.or(head.desired_revision.as_deref());
            let published = expected.is_some() && expected == head.published_revision.as_deref();
            let current_acceptance = expected == head.desired_revision.as_deref();
            if published && (!current_acceptance || operation == Some(OperationState::Completed)) {
                "complete"
            } else if requested.is_some() && requested != head.desired_revision.as_deref() {
                "stale"
            } else {
                match operation {
                    Some(OperationState::Processing) => "running",
                    Some(OperationState::FailedRetryable) => "failed_retryable",
                    Some(OperationState::FailedTerminal) => "failed_final",
                    _ => "pending",
                }
            }
        }
    };
    SourceCoverage {
        requested_revision: requested.map(str::to_owned),
        published_revision: head.published_revision.clone(),
        desired_revision: head.desired_revision.clone(),
        state: state.into(),
    }
}

struct Assembly<'a> {
    ctx: &'a ToolContext,
    request: &'a SearchRequest,
    coverage: BTreeMap<String, SourceCoverage>,
    heads: HashMap<RecordId, SourceHead>,
    authorizations: HashMap<RecordId, SourceAuthorizationSnapshot>,
    external_sources: HashMap<String, RecordId>,
}
impl Assembly<'_> {
    #[expect(
        clippy::significant_drop_tightening,
        clippy::too_many_lines,
        reason = "Keep the complete authorization and current-evidence operation with its original ownership"
    )]
    async fn observe_source(
        &mut self,
        source: &Source,
    ) -> Result<Option<(String, String)>, PublicError> {
        let Some(external) = source.external_id.as_ref().filter(|id| !id.is_empty()) else {
            return Ok(None);
        };
        if !self.request.source_revision_constraints.is_empty()
            && !self
                .request
                .source_revision_constraints
                .contains_key(external)
        {
            return Ok(None);
        }
        if self
            .external_sources
            .get(external)
            .is_some_and(|id| id != &source.base.id)
        {
            return Err(PublicError::new(
                ErrorCode::Unavailable,
                "External source identity is ambiguous",
            ));
        }
        self.external_sources
            .insert(external.clone(), source.base.id.clone());
        let snapshot = SourceAuthorizationSnapshot::from(source);
        if self
            .authorizations
            .get(&source.base.id)
            .is_some_and(|old| old != &snapshot)
        {
            return Err(PublicError::new(
                ErrorCode::NotReady,
                "Source authorization changed during search; retry",
            ));
        }
        self.authorizations.insert(source.base.id.clone(), snapshot);
        let repo = SourcePublicationRepository::new(self.ctx.storage.clone());
        let head = repo
            .head(&self.ctx.principal.space_id, &source.base.id)
            .await
            .map_err(unavailable)?;
        let Some(head) = head else {
            // Never fabricate a consumer revision for retained pre-preview data.
            self.coverage.entry(external.clone()).or_insert_with(|| {
                coverage(
                    None,
                    self.request
                        .source_revision_constraints
                        .get(external)
                        .map(String::as_str),
                    None,
                )
            });
            return Ok(None);
        };
        if self
            .heads
            .get(&source.base.id)
            .is_some_and(|old| old != &head)
        {
            return Err(PublicError::new(
                ErrorCode::NotReady,
                "Source publication changed during search; retry",
            ));
        }
        let operation = repo
            .operation(&head.space_id, &head.operation_id)
            .await
            .map_err(unavailable)?;
        self.coverage.insert(
            external.clone(),
            coverage(
                Some(&head),
                self.request
                    .source_revision_constraints
                    .get(external)
                    .map(String::as_str),
                operation.as_ref().map(|operation| operation.state),
            ),
        );
        self.heads.insert(source.base.id.clone(), head.clone());
        if head.lifecycle != SourceVisibility::Active {
            return Ok(None);
        }
        let Some(revision) = head.published_revision else {
            return Ok(None);
        };
        if self
            .request
            .source_revision_constraints
            .get(external)
            .is_some_and(|required| required != &revision)
        {
            return Ok(None);
        }
        let Some(published) = repo
            .revision(&head.space_id, &source.base.id, &revision)
            .await
            .map_err(unavailable)?
        else {
            return Err(PublicError::new(
                ErrorCode::Unavailable,
                "Published source snapshot is unavailable",
            ));
        };
        if !current_read::authorized(self.ctx, &published.source.base, "read:doc:raw")
            || !labels_match(&published.source, &self.request.filters.labels)
        {
            return Ok(None);
        }
        Ok(Some((external.clone(), revision)))
    }
    #[allow(clippy::too_many_lines)] // Keep original observations and their final checks together.
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Preserve the original repository lifetimes and future ownership"
    )]
    async fn evidence(
        &mut self,
        span: &EvidenceSpan,
    ) -> Result<Option<SearchEvidence>, PublicError> {
        fn current_read_error(error: elephant_storage::StorageError) -> PublicError {
            if matches!(error, elephant_storage::StorageError::Conflict(_)) {
                PublicError::new(
                    ErrorCode::NotReady,
                    "Evidence input changed during search; retry",
                )
            } else {
                unavailable(error)
            }
        }
        let reader = ReadEligibility::new(self.ctx.storage.clone());
        let Some(resolved) = reader
            .evidence(&self.ctx.principal.space_id, span, &|base| {
                current_read::authorized(self.ctx, base, "read:doc:raw")
            })
            .await
            .map_err(current_read_error)?
        else {
            return Ok(None);
        };
        let Some(source) = resolved.source.as_ref() else {
            return Ok(None);
        };
        let Some((source_id, revision)) = self.observe_source(source).await? else {
            return Ok(None);
        };
        let mapping = SourcePublicationManifestRepository::new(self.ctx.storage.clone())
            .source_revision_for_document_revision(
                &self.ctx.principal.space_id,
                &resolved.revision.base.id,
            )
            .await
            .map_err(unavailable)?;
        let Some(mapping) = mapping.filter(|mapping| {
            mapping.publication.source_id == source.base.id
                && mapping.publication.revision_id == revision
                && mapping.doc_id == resolved.doc.base.id
        }) else {
            return Ok(None);
        };
        let accepted = SourcePublicationRepository::new(self.ctx.storage.clone())
            .revision(&self.ctx.principal.space_id, &source.base.id, &revision)
            .await
            .map_err(unavailable)?
            .ok_or_else(|| {
                PublicError::new(
                    ErrorCode::Unavailable,
                    "Published source input is unavailable",
                )
            })?;
        let original_text = SourcePublicationManifestRepository::new(self.ctx.storage.clone())
            .verified_original_document_text(&mapping, &resolved.doc, &resolved.revision, &accepted)
            .await
            .map_err(current_read_error)?
            .ok_or_else(|| {
                PublicError::new(
                    ErrorCode::Unavailable,
                    "Exact published source input is unavailable",
                )
            })?;
        // Source hashes identify the producer's complete deduplication input,
        // which may include title, metadata, sidecar and media. They are not a
        // digest of text alone. Verify this exact accepted snapshot against the
        // original canonical completion before mapping its raw coordinates;
        // matching deduplication fields alone never certify supplied bytes.
        let rendered_text_sha256 = hex::encode(Sha256::digest(resolved.revision.text.as_bytes()));
        let matches_hash = |declared: &str, actual: &str| {
            declared.strip_prefix("sha256:").unwrap_or(declared) == actual
        };
        if !matches_hash(&resolved.revision.text_hash, &rendered_text_sha256) {
            return Err(PublicError::new(
                ErrorCode::Unavailable,
                "Published evidence input proof is invalid",
            ));
        }
        let original_span = elephant_types::rendered_text::original_text_span(
            &original_text,
            &resolved.revision.text,
            &resolved.revision.renderer,
            span.start as usize..span.end as usize,
        )
        .map_err(|_| {
            PublicError::new(
                ErrorCode::Unavailable,
                "Published evidence has no verified original source coordinates",
            )
        })?;
        let quote = original_text
            .get(original_span.clone())
            .ok_or_else(|| {
                PublicError::new(ErrorCode::Unavailable, "Original evidence span is invalid")
            })?
            .to_owned();
        let start = u32::try_from(original_span.start).map_err(|_| {
            PublicError::new(
                ErrorCode::Unavailable,
                "Original evidence span is too large",
            )
        })?;
        let end = u32::try_from(original_span.end).map_err(|_| {
            PublicError::new(
                ErrorCode::Unavailable,
                "Original evidence span is too large",
            )
        })?;
        let rendered_quote_sha256 = hex::encode(Sha256::digest(resolved.quote.as_bytes()));

        // Keep the original immutable inputs, mapping and completion. A later
        // successful read cannot silently supply a different proof or revision.
        let accepted_fingerprint =
            elephant_storage::canonical_commit::source_support_fingerprint(&accepted)
                .map_err(unavailable)?;
        let changed = || {
            PublicError::new(
                ErrorCode::NotReady,
                "Evidence input changed during search; retry",
            )
        };
        let current_accepted = SourcePublicationRepository::new(self.ctx.storage.clone())
            .revision(&self.ctx.principal.space_id, &source.base.id, &revision)
            .await
            .map_err(unavailable)?
            .ok_or_else(changed)?;
        if elephant_storage::canonical_commit::source_support_fingerprint(&current_accepted)
            .map_err(unavailable)?
            != accepted_fingerprint
        {
            return Err(changed());
        }
        let current_mapping = SourcePublicationManifestRepository::new(self.ctx.storage.clone())
            .source_revision_for_document_revision(
                &self.ctx.principal.space_id,
                &resolved.revision.base.id,
            )
            .await
            .map_err(unavailable)?;
        if current_mapping.as_ref() != Some(&mapping) {
            return Err(changed());
        }
        let current = reader
            .evidence(&self.ctx.principal.space_id, span, &|base| {
                current_read::authorized(self.ctx, base, "read:doc:raw")
            })
            .await
            .map_err(current_read_error)?
            .ok_or_else(changed)?;
        if current.revision != resolved.revision
            || current.doc != resolved.doc
            || current.source != resolved.source
            || current.node != resolved.node
            || current.quote != resolved.quote
        {
            return Err(changed());
        }
        reader.verify_current().await.map_err(current_read_error)?;
        // This is complete support, not a prefix excerpt. Interior removed
        // whitespace remains in the honest raw cover; rendered bytes have
        // their own exact coordinates and digest on the same immutable DTO.
        Ok(Some(SearchEvidence {
            source_id,
            revision,
            span: SearchSpan { start, end },
            quote,
            doc_id: resolved.doc.base.id.to_string(),
            internal_doc_revision_id: resolved.revision.base.id.to_string(),
            rendered_span: SearchSpan {
                start: span.start,
                end: span.end,
            },
            rendered_quote_sha256,
            rendered_text_sha256,
        }))
    }

    async fn vector_passage(
        &mut self,
        hit: elephant_storage::chunk_embedding_repository::NodeEmbeddingVectorHit,
    ) -> Result<Option<SearchResult>, PublicError> {
        let space = &self.ctx.principal.space_id;
        let Some(node) = DocNodeRepository::new(self.ctx.storage.clone())
            .get(space, &hit.node_id)
            .await
            .map_err(unavailable)?
        else {
            return Ok(None);
        };
        let Some(revision) = DocRevisionRepository::new(self.ctx.storage.clone())
            .get(space, node.revision.id())
            .await
            .map_err(unavailable)?
        else {
            return Ok(None);
        };
        if revision.doc.id() != &hit.doc_id {
            return Ok(None);
        }
        let Some(quote) = revision
            .text
            .get(node.span.start as usize..node.span.end as usize)
        else {
            return Ok(None);
        };
        let span = EvidenceSpan {
            revision: Link::new(revision.base.id),
            node: Some(Link::new(node.base.id)),
            start: node.span.start,
            end: node.span.end,
            quote_hash: format!("sha256:{}", hex::encode(Sha256::digest(quote.as_bytes()))),
        };
        // The exact matched node enters the same current source, immutable
        // revision, policy and quote checks as every other public evidence path.
        Ok(self.evidence(&span).await?.map(|evidence| SearchResult {
            kind: SearchKind::Chunk,
            text: evidence.quote.clone(),
            evidence: vec![evidence],
            score: hit.score,
        }))
    }
    async fn assertion_evidence(
        &mut self,
        assertion: &Assertion,
    ) -> Result<Option<SearchEvidence>, PublicError> {
        if ReadEligibility::new(self.ctx.storage.clone())
            .project(assertion.clone(), &|base| {
                current_read::authorized(self.ctx, base, "read:assertion")
            })
            .await
            .map_err(unavailable)?
            .is_none()
        {
            return Ok(None);
        }
        let Some(evidence) = self.evidence(&assertion.evidence).await? else {
            return Ok(None);
        };
        let Some(source_id) = self.external_sources.get(&evidence.source_id) else {
            return Ok(None);
        };
        // A handwritten/stale assertion cannot borrow a published document's
        // authority. The whole-source canonical commit must own this exact row.
        let support = SourcePublicationRepository::new(self.ctx.storage.clone())
            .support_for_record(&self.ctx.principal.space_id, &assertion.base.id)
            .await
            .map_err(unavailable)?;
        let fingerprint = elephant_storage::canonical_commit::source_support_fingerprint(assertion)
            .map_err(unavailable)?;
        if !support.iter().any(|entry| {
            entry.space_id == self.ctx.principal.space_id
                && entry.record_id == assertion.base.id
                && &entry.source_id == source_id
                && entry.source_revision == evidence.revision
                && entry.record_fingerprint == fingerprint
        }) {
            return Ok(None);
        }
        Ok(Some(evidence))
    }
    async fn claim(
        &mut self,
        assertion: Assertion,
        score: f64,
    ) -> Result<Option<SearchResult>, PublicError> {
        let Some(evidence) = self.assertion_evidence(&assertion).await? else {
            return Ok(None);
        };
        let text = format!(
            "{} {} {}",
            assertion.subject_text,
            assertion.predicate_raw,
            assertion
                .object_text
                .clone()
                .or_else(|| assertion.object_literal.as_ref().map(ToString::to_string))
                .unwrap_or_default()
        );
        Ok(Some(SearchResult {
            kind: SearchKind::Claim,
            text,
            evidence: vec![evidence],
            score,
        }))
    }
    async fn entity(
        &mut self,
        entity: Entity,
        score: f64,
    ) -> Result<Option<SearchResult>, PublicError> {
        let eligible = ReadEligibility::new(self.ctx.storage.clone())
            .project(entity, &|base| {
                current_read::authorized(self.ctx, base, "read:entity")
            })
            .await
            .map_err(unavailable)?;
        let Some(entity) = eligible else {
            return Ok(None);
        };
        let mut evidence = Vec::new();
        for assertion in AssertionRepository::new(self.ctx.storage.clone())
            .find_by_resolved_entity(&self.ctx.principal.space_id, &entity.base.id, 32)
            .await
            .map_err(unavailable)?
        {
            if let Some(item) = self.assertion_evidence(&assertion).await? {
                evidence.push(item);
                if evidence.len() == MAX_EVIDENCE {
                    break;
                }
            }
        }
        if evidence.is_empty()
            && (!self.request.filters.labels.is_empty()
                || !self.request.source_revision_constraints.is_empty()
                || !entity.base.provenance.sources.is_empty())
        {
            return Ok(None);
        }
        Ok(Some(SearchResult {
            kind: SearchKind::Entity,
            text: boundary_prefix(&entity.display_name, MAX_RESULT_TEXT_BYTES).into(),
            evidence,
            score,
        }))
    }
}

fn push_ranked(ranked: &mut Vec<SearchResult>, result: SearchResult, limit: usize) {
    ranked.push(result);
    rank_unique_results(ranked);
    ranked.truncate(limit);
}

fn rank_unique_results(ranked: &mut Vec<SearchResult>) {
    ranked.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then(left.text.cmp(&right.text))
            .then_with(|| evidence_order(left).cmp(&evidence_order(right)))
    });
    let mut seen = HashSet::new();
    ranked.retain(|result| {
        seen.insert((
            format!("{:?}", result.kind),
            result.text.clone(),
            result
                .evidence
                .first()
                .map(|e| (e.source_id.clone(), e.revision.clone(), e.span.start)),
        ))
    });
}

fn evidence_order(result: &SearchResult) -> Option<(&str, &str, u32, u32)> {
    result.evidence.first().map(|e| {
        (
            e.source_id.as_str(),
            e.revision.as_str(),
            e.span.start,
            e.span.end,
        )
    })
}

struct SemanticPartition {
    key: PartitionKey,
    limit: usize,
    window: elephant_storage::PartitionWindow,
    scan: bool,
}

fn semantic_settled(ranked: &[SearchResult], needed: usize, windows: &[SemanticPartition]) -> bool {
    let nth = needed
        .checked_sub(1)
        .and_then(|index| ranked.get(index))
        .map(|result| result.score);
    semantic_scores_settled(ranked.len(), needed, nth, windows)
}

fn semantic_scores_settled(
    count: usize,
    needed: usize,
    nth: Option<f64>,
    windows: &[SemanticPartition],
) -> bool {
    if windows.iter().all(|partition| !partition.window.full) {
        return true;
    }
    // A full raw window with no finite floor cannot certify exhaustion.
    if windows
        .iter()
        .any(|partition| partition.window.full && partition.window.floor.is_none())
    {
        return false;
    }
    let floor = windows
        .iter()
        .fold(None, |floor, partition| partition.window.raise_floor(floor));
    elephant_storage::semantic_window_settled(count, needed, nth, floor)
        // The storage helper allows score ties. Our text/evidence tie-break can
        // still be displaced by an unseen equal-scoring node, so widen ties.
        && match (nth, floor) { (Some(nth), Some(floor)) => nth > floor, (_, None) => true, _ => false }
}

fn widen_semantic_window(
    windows: &mut [SemanticPartition],
    nth: Option<f64>,
) -> Result<(), PublicError> {
    let reserved = windows
        .iter()
        .map(|partition| partition.limit)
        .sum::<usize>();
    semantic_budget(windows.len(), reserved)?;
    // Spend only on a full partition that can still affect the visible rank.
    let selected = windows
        .iter()
        .enumerate()
        .filter(|(_, partition)| {
            partition.window.full
                && match (nth, partition.window.floor) {
                    (Some(nth), Some(floor)) => nth <= floor,
                    _ => true,
                }
        })
        .max_by(|(_, left), (_, right)| {
            left.window
                .floor
                .unwrap_or(f64::INFINITY)
                .total_cmp(&right.window.floor.unwrap_or(f64::INFINITY))
        })
        .map(|(index, _)| index);
    let remaining = MAX_SEMANTIC_CANDIDATES.saturating_sub(reserved);
    let Some(index) = selected.filter(|_| remaining > 0) else {
        return Err(PublicError::new(
            ErrorCode::RateLimited,
            "Semantic ranking cannot be established within the index candidate budget",
        ));
    };
    let partition = &mut windows[index];
    partition.limit += partition.limit.min(remaining);
    partition.scan = true;
    Ok(())
}

fn improve_semantic_score(
    scores: &mut HashMap<RecordId, f64>,
    node: &RecordId,
    score: f64,
) -> bool {
    if !score.is_finite() || scores.get(node).is_some_and(|previous| score <= *previous) {
        return false;
    }
    scores.insert(node.clone(), score);
    true
}

async fn collect_semantic(
    assembly: &mut Assembly<'_>,
    embeddings: &NodeEmbeddingRepository,
    keys: Vec<PartitionKey>,
    vector: &[f32],
    ranked: &mut Vec<SearchResult>,
) -> Result<(), PublicError> {
    semantic_budget(keys.len(), 0)?;
    if keys.is_empty() {
        return Ok(());
    }
    // Reserve RAW slots, including unresolved/orphan hits. Initial windows share
    // one budget; widening never silently trims a partition or skips a policy.
    let initial = MAX_CANDIDATES.min(MAX_SEMANTIC_CANDIDATES / keys.len());
    let mut partitions = keys
        .into_iter()
        .map(|key| SemanticPartition {
            key,
            limit: initial,
            window: elephant_storage::PartitionWindow::default(),
            scan: true,
        })
        .collect::<Vec<_>>();
    let partition_count = partitions.len();
    let mut best_scores = HashMap::new();
    loop {
        for partition in partitions.iter_mut().filter(|partition| partition.scan) {
            partition.window = embeddings
                .search_partition_window(
                    &partition.key,
                    &assembly.ctx.principal.space_id,
                    vector,
                    partition.limit,
                    None,
                    false,
                )
                .await
                .map_err(unavailable)?;
            partition.scan = false;
            for hit in std::mem::take(&mut partition.window.hits) {
                if improve_semantic_score(&mut best_scores, &hit.node_id, hit.score) {
                    // Retained/reclassified rows can contain one node in
                    // several partitions. Keep its highest similarity, never
                    // whichever partition was visited first. Bound unique
                    // nodes across repeated windows as well as raw slots.
                    semantic_budget(partition_count, best_scores.len())?;
                    if let Some(result) = assembly.vector_passage(hit).await? {
                        push_ranked(ranked, result, assembly.request.limit);
                    }
                }
            }
        }
        rank_unique_results(ranked);
        let needed = assembly.request.limit;
        if semantic_settled(ranked, needed, &partitions) {
            return Ok(());
        }
        let nth = ranked.get(needed - 1).map(|result| result.score);
        widen_semantic_window(&mut partitions, nth)?;
    }
}

fn rank_documents(scores: &HashMap<RecordId, f64>) -> Vec<(RecordId, f64)> {
    let mut ranked = scores
        .iter()
        .map(|(id, score)| (id.clone(), *score))
        .collect::<Vec<_>>();
    ranked.sort_by(|left, right| right.1.total_cmp(&left.1).then(left.0.cmp(&right.0)));
    ranked
}

/// A semantic modality votes once per document. Node count, partition count and
/// discovery order cannot multiply that vote. Only current authorized evidence
/// may contribute, including when an obsolete node shares a still-current doc.
#[expect(
    clippy::float_cmp,
    clippy::suspicious_operation_groupings,
    reason = "Break exactly equal scores by the text and evidence order of each passage"
)]
async fn collect_hybrid_documents(
    assembly: &mut Assembly<'_>,
    embeddings: &NodeEmbeddingRepository,
    keys: Vec<PartitionKey>,
    vector: &[f32],
    doc_scores: &mut HashMap<RecordId, f64>,
) -> Result<HashMap<RecordId, SearchResult>, PublicError> {
    semantic_budget(keys.len(), 0)?;
    if keys.is_empty() {
        return Ok(HashMap::new());
    }
    let initial = MAX_CANDIDATES.min(MAX_SEMANTIC_CANDIDATES / keys.len());
    let mut partitions = keys
        .into_iter()
        .map(|key| SemanticPartition {
            key,
            limit: initial,
            window: elephant_storage::PartitionWindow::default(),
            scan: true,
        })
        .collect::<Vec<_>>();
    let partition_count = partitions.len();
    let mut seen_nodes = HashSet::new();
    let mut node_scores = HashMap::new();
    let mut best_documents: HashMap<RecordId, SearchResult> = HashMap::new();
    loop {
        for partition in partitions.iter_mut().filter(|partition| partition.scan) {
            partition.window = embeddings
                .search_partition_window(
                    &partition.key,
                    &assembly.ctx.principal.space_id,
                    vector,
                    partition.limit,
                    None,
                    false,
                )
                .await
                .map_err(unavailable)?;
            partition.scan = false;
            for hit in std::mem::take(&mut partition.window.hits) {
                seen_nodes.insert(hit.node_id.clone());
                semantic_budget(partition_count, seen_nodes.len())?;
                if !hit.score.is_finite()
                    || node_scores
                        .get(&hit.node_id)
                        .is_some_and(|score| hit.score <= *score)
                {
                    continue;
                }
                let doc = hit.doc_id.clone();
                let node = hit.node_id.clone();
                if let Some(passage) = assembly.vector_passage(hit).await? {
                    // Rejected raw hits cannot suppress a later eligible copy.
                    node_scores.insert(node, passage.score);
                    let replace = best_documents.get(&doc).is_none_or(|previous| {
                        passage.score > previous.score
                            || (passage.score == previous.score
                                && (passage.text.as_str(), evidence_order(&passage))
                                    < (previous.text.as_str(), evidence_order(previous)))
                    });
                    if replace {
                        best_documents.insert(doc, passage);
                    }
                }
            }
        }
        // Similarities are globally comparable under the one governed query
        // profile. Fuse their document ranks, never partition-local node ranks.
        let ranked = rank_documents(
            &best_documents
                .iter()
                .map(|(doc, passage)| (doc.clone(), passage.score))
                .collect(),
        );
        let nth = ranked.get(MAX_CANDIDATES - 1).map(|(_, score)| *score);
        if semantic_scores_settled(ranked.len(), MAX_CANDIDATES, nth, &partitions) {
            let mut passages = HashMap::new();
            for (rank, (doc, _)) in ranked.into_iter().take(MAX_CANDIDATES).enumerate() {
                *doc_scores.entry(doc.clone()).or_default() += reciprocal_rank(rank);
                if let Some(passage) = best_documents.remove(&doc) {
                    passages.insert(doc, passage);
                }
            }
            return Ok(passages);
        }
        // Repeated or ineligible nodes consume raw slots, not document votes.
        // Widen within the existing budget or refuse an uncertifiable ranking.
        widen_semantic_window(&mut partitions, nth)?;
    }
}

// Failed candidates and coverage are discarded. These observations are only
// negative fences: an internal retry cannot adopt a changed source authority.
struct SearchSourceFence {
    heads: HashMap<RecordId, SourceHead>,
    authorizations: HashMap<RecordId, SourceAuthorizationSnapshot>,
}
impl SearchSourceFence {
    async fn verify(&self, ctx: &ToolContext, request: &SearchRequest) -> Result<(), PublicError> {
        query_vectors::ensure_ready(ctx)?;
        let publication = SourcePublicationRepository::new(ctx.storage.clone());
        let space = &ctx.principal.space_id;
        for (id, observed) in &self.heads {
            if publication
                .head(space, id)
                .await
                .map_err(unavailable)?
                .as_ref()
                != Some(observed)
            {
                return Err(PublicError::new(
                    ErrorCode::NotReady,
                    "Source publication changed during search; retry",
                ));
            }
        }
        verify_external_heads(&publication, space, request, &self.heads).await?;
        verify_authorizations(&publication, space, &self.authorizations).await?;
        drop(publication);
        ReadEligibility::new(ctx.storage.clone())
            .verify_current()
            .await
            .map_err(unavailable)?;
        query_vectors::ensure_ready(ctx)
    }
}
enum SearchPreparation {
    Complete(SearchResponse),
    EpochChanged(SearchSourceFence),
}

/// Search actual indexed candidates with source-owner-resolved external IDs.
///
/// # Errors
/// Returns typed failures for invalid input, denial, unavailable storage,
/// revoked providers, changing publication, and oversized output.
pub async fn search(
    ctx: &ToolContext,
    request: &SearchRequest,
) -> Result<SearchResponse, PublicError> {
    // Also covers direct/in-process callers. Nested activity reuses the existing
    // admission lease; restore and physical erasure cannot complete between the
    // two preparations. Never start a new timeout or detach the original future.
    Box::pin(ctx.storage.with_activity(async {
        Box::pin(elephant_storage::read_eligibility::with_read_fence(
            ctx.storage.clone(),
            Box::pin(search_with_reprepare(ctx, request)),
        ))
        .await
        .map_err(unavailable)?
    }))
    .await
    .map_err(unavailable)?
}

#[expect(
    clippy::large_futures,
    reason = "Retain the complete original search future"
)]
async fn search_with_reprepare(
    ctx: &ToolContext,
    request: &SearchRequest,
) -> Result<SearchResponse, PublicError> {
    let mut authority = None;
    match prepare_search(ctx, request, &mut authority).await? {
        SearchPreparation::Complete(response) => Ok(response),
        SearchPreparation::EpochChanged(original_sources) => {
            original_sources.verify(ctx, request).await?;
            let original = authority.as_ref().ok_or_else(|| {
                PublicError::new(
                    ErrorCode::Unavailable,
                    "Query embedding authority is unavailable",
                )
            })?;
            query_vectors::verify_authority(ctx, &ctx.principal.space_id, original).await?;
            // Same request, mode, limits, filters and constraints; all candidate
            // vectors/ranks/coverage and Assembly state are rebuilt from scratch.
            match prepare_search(ctx, request, &mut authority).await? {
                SearchPreparation::Complete(response) => {
                    original_sources.verify(ctx, request).await?;
                    Ok(response)
                }
                SearchPreparation::EpochChanged(_) => {
                    Err(query_vectors::QueryError::EpochChanged.into_public())
                }
            }
        }
    }
}

#[expect(
    clippy::large_futures,
    clippy::significant_drop_tightening,
    clippy::too_many_lines,
    reason = "Keep the complete authorization and current-evidence operation with its original ownership"
)]
async fn prepare_search(
    ctx: &ToolContext,
    request: &SearchRequest,
    authority: &mut Option<query_vectors::Authority>,
) -> Result<SearchPreparation, PublicError> {
    query_vectors::ensure_ready(ctx)?;
    #[cfg(test)]
    reprepare_tests::preparing();
    request.validate()?;
    let ranking_mode = request.effective_ranking_mode()?;
    crate::authz::authorize_preflight_scope(&ctx.authz, &ctx.principal, "read:doc:raw").map_err(
        |_| {
            PublicError::new(
                ErrorCode::ForbiddenSpace,
                "Raw document search is not permitted",
            )
        },
    )?;
    let space = &ctx.principal.space_id;
    let publication = SourcePublicationRepository::new(ctx.storage.clone());
    let mut assembly = Assembly {
        ctx,
        request,
        coverage: BTreeMap::new(),
        heads: HashMap::new(),
        authorizations: HashMap::new(),
        external_sources: HashMap::new(),
    };
    for (external, requested) in &request.source_revision_constraints {
        let head = publication
            .external_head(space, SourceType::Manual, external)
            .await
            .map_err(unavailable)?;
        let operation = if let Some(head) = &head {
            let snapshot = publication
                .authorization_snapshot(space, &head.source_id)
                .await
                .map_err(unavailable)?
                .ok_or_else(|| {
                    PublicError::new(
                        ErrorCode::Unavailable,
                        "Source authorization envelope is unavailable",
                    )
                })?;
            let envelope = &snapshot.envelope;
            if envelope.space_id != *space || envelope.source_id != head.source_id {
                return Err(PublicError::new(
                    ErrorCode::Unavailable,
                    "Source authorization envelope is inconsistent",
                ));
            }
            ctx.authz
                .evaluate(
                    &ctx.principal,
                    &elephant_policy::RecordAttributes {
                        space_id: envelope.space_id.clone(),
                        security: envelope.security.clone(),
                        subjects: envelope.subjects.clone(),
                        subjects_state: envelope.subjects_state,
                    },
                    "read:doc:raw",
                )
                .map_err(|_| {
                    PublicError::new(ErrorCode::ForbiddenSpace, "Source search is not permitted")
                })?;
            if let Some(source) = SourceRepository::new(ctx.storage.clone())
                .get(space, &head.source_id)
                .await
                .map_err(unavailable)?
            {
                if !current_read::authorized(ctx, &source.base, "read:doc:raw") {
                    return Err(PublicError::new(
                        ErrorCode::ForbiddenSpace,
                        "Source search is not permitted",
                    ));
                }
            } else if head.lifecycle == SourceVisibility::Active {
                return Err(PublicError::new(
                    ErrorCode::Unavailable,
                    "Active source is unavailable",
                ));
            }
            assembly
                .authorizations
                .insert(head.source_id.clone(), snapshot);
            publication
                .operation(space, &head.operation_id)
                .await
                .map_err(unavailable)?
        } else {
            None
        };
        assembly.coverage.insert(
            external.clone(),
            coverage(
                head.as_ref(),
                Some(requested),
                operation.as_ref().map(|operation| operation.state),
            ),
        );
        if let Some(head) = head {
            assembly
                .external_sources
                .insert(external.clone(), head.source_id.clone());
            assembly.heads.insert(head.source_id.clone(), head);
        }
    }
    let requested_publication_available =
        request
            .source_revision_constraints
            .iter()
            .any(|(external, requested)| {
                assembly
                    .external_sources
                    .get(external)
                    .and_then(|source| assembly.heads.get(source))
                    .is_some_and(|head| {
                        head.lifecycle == SourceVisibility::Active
                            && head.published_revision.as_deref() == Some(requested.as_str())
                    })
            });
    if !request.source_revision_constraints.is_empty() && !requested_publication_available {
        // No requested immutable revision is currently published. A fresh
        // acceptance of an already-published A remains pending, but its old A
        // evidence is still available and must proceed through normal read
        // eligibility rather than being suppressed by operation state alone.
        for (id, observed) in &assembly.heads {
            if publication
                .head(space, id)
                .await
                .map_err(unavailable)?
                .as_ref()
                != Some(observed)
            {
                return Err(PublicError::new(
                    ErrorCode::NotReady,
                    "Source publication changed during search; retry",
                ));
            }
        }
        verify_external_heads(&publication, space, request, &assembly.heads).await?;
        verify_authorizations(&publication, space, &assembly.authorizations).await?;
        let response = SearchResponse {
            results: vec![],
            coverage: assembly.coverage,
            partial: true,
        };
        response_budget(&response)?;
        query_vectors::ensure_ready(ctx)?;
        return Ok(SearchPreparation::Complete(response));
    }
    let mut ranked = Vec::new();
    let mut doc_scores: HashMap<RecordId, f64> = HashMap::new();
    let mut semantic_passages = HashMap::new();
    if ranking_mode != RankingMode::Semantic {
        let criteria = SearchCriteria::new()
            .with_space_id(space.clone())
            .with_query(&request.query)
            .with_limit(MAX_CANDIDATES);
        let docs = DocRepository::new(ctx.storage.clone())
            .search(&criteria)
            .await
            .map_err(unavailable)?;
        let claims = AssertionRepository::new(ctx.storage.clone())
            .search(&criteria)
            .await
            .map_err(unavailable)?;
        let entities = EntityRepository::new(ctx.storage.clone())
            .search(&criteria)
            .await
            .map_err(unavailable)?;
        doc_scores.extend(
            docs.results
                .into_iter()
                .enumerate()
                .map(|(rank, doc)| (doc.base.id, reciprocal_rank(rank))),
        );
        for (rank, assertion) in claims.results.into_iter().enumerate() {
            if let Some(result) = assembly.claim(assertion, reciprocal_rank(rank)).await? {
                push_ranked(&mut ranked, result, request.limit);
            }
        }
        for (rank, entity) in entities.results.into_iter().enumerate() {
            if let Some(result) = assembly.entity(entity, reciprocal_rank(rank)).await? {
                push_ranked(&mut ranked, result, request.limit);
            }
        }
    }
    if ranking_mode != RankingMode::Lexical {
        let (policy, vector) =
            match query_vectors::embed_for_search(ctx, space, &request.query, authority).await {
                Ok(value) => value,
                Err(query_vectors::QueryError::Terminal(error)) => return Err(error),
                Err(query_vectors::QueryError::EpochChanged) => {
                    let fence = SearchSourceFence {
                        heads: assembly.heads,
                        authorizations: assembly.authorizations,
                    };
                    fence.verify(ctx, request).await?;
                    return Ok(SearchPreparation::EpochChanged(fence));
                }
            };
        let tables = ctx.storage.list_tables().await.map_err(unavailable)?;
        let clearances = ctx
            .principal
            .clearances
            .iter()
            .map(|c| (c.policy_id.clone(), c.max_level))
            .collect::<Vec<_>>();
        let keys = discover_accessible_partition_keys(&tables, &policy, &clearances, "default", 0);
        let embeddings = NodeEmbeddingRepository::new(ctx.storage.clone(), PartitionManager::new());
        if ranking_mode == RankingMode::Semantic {
            collect_semantic(&mut assembly, &embeddings, keys, &vector, &mut ranked).await?;
        } else {
            semantic_passages = collect_hybrid_documents(
                &mut assembly,
                &embeddings,
                keys,
                &vector,
                &mut doc_scores,
            )
            .await?;
        }
    }
    let mut ordered_docs = doc_scores.into_iter().collect::<Vec<_>>();
    ordered_docs.sort_by(|left, right| {
        right
            .1
            .total_cmp(&left.1)
            .then(left.0.to_string().cmp(&right.0.to_string()))
    });
    for (doc_id, score) in ordered_docs.into_iter().take(MAX_CANDIDATES) {
        if let Some(mut passage) = semantic_passages.remove(&doc_id) {
            // The semantic vote and returned quote share the exact eligible
            // node. A lexical sibling cannot stand in for its vector support.
            passage.score = score;
            push_ranked(&mut ranked, passage, request.limit);
            continue;
        }
        for revision in DocRevisionRepository::new(ctx.storage.clone())
            .find_by_doc(&doc_id)
            .await
            .map_err(unavailable)?
        {
            if revision.base.space_id != *space {
                continue;
            }
            let nodes = DocNodeRepository::new(ctx.storage.clone())
                .find_chunks_by_revision(&revision.base.id)
                .await
                .map_err(unavailable)?;
            let mut passages = nodes
                .into_iter()
                .filter_map(|node| {
                    let quote = revision
                        .text
                        .get(node.span.start as usize..node.span.end as usize)?;
                    Some((
                        lexical_overlap(quote, &request.query),
                        node,
                        quote.to_owned(),
                    ))
                })
                .collect::<Vec<_>>();
            passages.sort_by(|left, right| {
                right
                    .0
                    .cmp(&left.0)
                    .then(left.1.span.start.cmp(&right.1.span.start))
            });
            for (_, node, quote) in passages.into_iter().take(3) {
                let span = EvidenceSpan {
                    revision: Link::new(revision.base.id.clone()),
                    node: Some(Link::new(node.base.id)),
                    start: node.span.start,
                    end: node.span.end,
                    quote_hash: format!("sha256:{}", hex::encode(Sha256::digest(quote.as_bytes()))),
                };
                if let Some(evidence) = assembly.evidence(&span).await? {
                    push_ranked(
                        &mut ranked,
                        SearchResult {
                            kind: SearchKind::Chunk,
                            text: evidence.quote.clone(),
                            evidence: vec![evidence],
                            score,
                        },
                        request.limit,
                    );
                    break;
                }
            }
        }
    }
    rank_unique_results(&mut ranked);
    ranked.truncate(request.limit);
    if let Some(original) = authority.as_ref() {
        query_vectors::verify_authority(ctx, space, original).await?;
    }
    for (id, observed) in &assembly.heads {
        if publication
            .head(space, id)
            .await
            .map_err(unavailable)?
            .as_ref()
            != Some(observed)
        {
            return Err(PublicError::new(
                ErrorCode::NotReady,
                "Source publication changed during search; retry",
            ));
        }
    }
    verify_external_heads(&publication, space, request, &assembly.heads).await?;
    verify_authorizations(&publication, space, &assembly.authorizations).await?;
    let response = SearchResponse {
        results: ranked,
        partial: assembly
            .coverage
            .values()
            .any(|coverage| coverage.state != "complete"),
        coverage: assembly.coverage,
    };
    response_budget(&response)?;
    query_vectors::ensure_ready(ctx)?;
    Ok(SearchPreparation::Complete(response))
}
async fn verify_authorizations(
    publication: &SourcePublicationRepository,
    space: &str,
    observed: &HashMap<RecordId, SourceAuthorizationSnapshot>,
) -> Result<(), PublicError> {
    for (source, snapshot) in observed {
        if publication
            .authorization_snapshot(space, source)
            .await
            .map_err(unavailable)?
            .as_ref()
            != Some(snapshot)
        {
            return Err(PublicError::new(
                ErrorCode::NotReady,
                "Source authorization changed during search; retry",
            ));
        }
    }
    Ok(())
}
async fn verify_external_heads(
    publication: &SourcePublicationRepository,
    space: &str,
    request: &SearchRequest,
    heads: &HashMap<RecordId, SourceHead>,
) -> Result<(), PublicError> {
    for external in request.source_revision_constraints.keys() {
        if let Some(current) = publication
            .external_head(space, SourceType::Manual, external)
            .await
            .map_err(unavailable)?
        {
            if heads.get(&current.source_id) != Some(&current) {
                return Err(PublicError::new(
                    ErrorCode::NotReady,
                    "Source incarnation changed during search; retry",
                ));
            }
        }
    }
    Ok(())
}

/// Harmonic rank fusion gives the two modalities equal weight without flattening
/// their leaders into the tail. With a fixed offset of 60, two weak matches in a
/// small corpus almost always beat a first-ranked paraphrase absent from the
/// lexical list. One vote per eligible document per modality is still required;
/// this score does not assert semantic relevance or calibrate cosine values.
fn reciprocal_rank(rank: usize) -> f64 {
    1.0 / f64::from(u32::try_from(rank).unwrap_or(u32::MAX).saturating_add(1))
}
fn lexical_overlap(text: &str, query: &str) -> usize {
    let text = text.to_lowercase();
    query
        .split_whitespace()
        .filter(|term| text.contains(&term.to_lowercase()))
        .count()
}

/// MCP counterpart of the scoped HTTP route, using the verified context space.
pub struct ScopedSearchHandler;

#[async_trait::async_trait]
impl crate::ToolHandler for ScopedSearchHandler {
    fn name(&self) -> &'static str {
        "search_scoped"
    }
    fn description(&self) -> &'static str {
        concat!(
            "Search the selected named space with exact source revision constraints and explicit coverage. ",
            "Returns bounded claims, entities and source passages; partial coverage is distinct from zero matches.\n\n",
            "Example:\n",
            r#"{ "query": "When is the boiler service?", "limit": 3, "source_revision_constraints": { "home/heat": "heat-v2" } }"#,
        )
    }
    fn input_schema(&self) -> serde_json::Value {
        serde_json::json!({
            "type":"object", "additionalProperties":false, "required":["query"],
            "properties": {
                "query":{"type":"string","minLength":1,"maxLength":4096},
                "limit":{"type":"integer","minimum":1,"maximum":50,"default":10},
                "ranking_mode":{"type":"string","enum":["lexical","semantic","hybrid"],"description":"Explicit ranking mode; mutually exclusive with use_vector"},
                "use_vector":{"type":"boolean","description":"Legacy selector: false=lexical, true=hybrid; omitted selectors default to hybrid"},
                "source_revision_constraints":{"type":"object","required":[],"maxProperties":100,"propertyNames":{"minLength":1,"maxLength":1024},"additionalProperties":{"type":"string","minLength":1,"maxLength":256,"description":"Nonblank revision identifier, at most 256 UTF-8 bytes, no control characters"}},
                "filters":{"type":"object","required":[],"additionalProperties":false,"properties":{
                    "labels":{"type":"object","required":[],"maxProperties":64,"additionalProperties":{"oneOf":[
                        {"type":"string"},{"type":"array","minItems":1,"maxItems":64,"items":{"type":"string"}}
                    ]}}
                }}
            },
            "not":{"required":["ranking_mode","use_vector"]}
        })
    }
    fn profile(&self) -> crate::ToolProfile {
        crate::ToolProfile::Agent
    }
    async fn execute(
        &self,
        ctx: &ToolContext,
        params: serde_json::Value,
    ) -> crate::McpResult<serde_json::Value> {
        let request: SearchRequest = serde_json::from_value(params)
            .map_err(|_| crate::McpError::Public(invalid("Invalid scoped search request")))?;
        request.validate().map_err(crate::McpError::Public)?;
        crate::authz::authorize_preflight_scope(&ctx.authz, &ctx.principal, "read:doc:raw")
            .map_err(|_| {
                crate::McpError::Public(PublicError::new(
                    ErrorCode::ForbiddenSpace,
                    "Raw document search is not permitted",
                ))
            })?;
        let config = Box::pin(
            SpaceConfigRepository::new(ctx.storage.clone())
                .get_by_space_id(&ctx.principal.space_id),
        )
        .await
        .map_err(|error| crate::McpError::Public(unavailable(error)))?
        .ok_or_else(|| {
            crate::McpError::Public(PublicError::new(
                ErrorCode::UnknownSpace,
                "Selected space does not exist",
            ))
        })?;
        if !current_read::authorized(ctx, &config.base, "read:doc:raw") {
            return Err(crate::McpError::Public(PublicError::new(
                ErrorCode::ForbiddenSpace,
                "Selected space is not permitted",
            )));
        }
        let result = Box::pin(search(ctx, &request))
            .await
            .map_err(crate::McpError::Public)?;
        serde_json::to_value(crate::ToolResponse::success(result)).map_err(|_| {
            crate::McpError::Public(PublicError::new(
                ErrorCode::Unavailable,
                "Search serialization failed",
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::ToolHandler;
    use chrono::Utc;
    use elephant_storage::source_publication::AcceptSourceRevision;
    use elephant_types::publication::SourcePrecondition;
    use elephant_types::records::base::SubjectsState;
    use elephant_types::records::{BaseFields, SpaceConfig};
    use elephant_types::TableName;
    use serde_json::json;

    #[test]
    fn storage_resolution_failures_remain_retryable_and_budget_overflow_is_explicit() {
        use elephant_storage::space_config_repository::SpaceResolutionError;
        use elephant_types::provider_binding::ProviderAuthorizationError;
        let storage = binding_error(SpaceResolutionError::Storage(
            elephant_storage::StorageError::QueryError("private storage diagnostic".into()),
        ));
        assert_eq!(storage.error_code, ErrorCode::Unavailable);
        assert!(storage.retryable && !storage.message.contains("private"));
        let initial = embedding_resolution_error(elephant_pipeline::PipelineError::StorageError(
            elephant_storage::StorageError::QueryError("private storage diagnostic".into()),
        ));
        assert_eq!(initial.error_code, ErrorCode::Unavailable);
        assert!(initial.retryable && !initial.message.contains("private"));
        assert_eq!(
            embedding_resolution_error(elephant_pipeline::PipelineError::ConfigError(
                "provider_authorization: missing binding".into()
            ))
            .error_code,
            ErrorCode::ProviderUnapproved
        );
        let denied = binding_error(SpaceResolutionError::Authorization(
            ProviderAuthorizationError::MissingSpace,
        ));
        assert_eq!(denied.error_code, ErrorCode::ProviderUnapproved);
        assert!(!denied.retryable);
        semantic_budget(MAX_SEMANTIC_PARTITIONS, MAX_SEMANTIC_CANDIDATES).unwrap();
        for (partitions, candidates) in [
            (MAX_SEMANTIC_PARTITIONS + 1, 0),
            (1, MAX_SEMANTIC_CANDIDATES + 1),
        ] {
            assert_eq!(
                semantic_budget(partitions, candidates)
                    .unwrap_err()
                    .error_code,
                ErrorCode::RateLimited
            );
        }
    }

    #[test]
    fn semantic_floor_proof_counts_authorized_unique_results_and_ties() {
        fn partition(limit: usize, full: bool, floor: Option<f64>) -> SemanticPartition {
            SemanticPartition {
                key: PartitionKey::new("policy".into(), "L0".into(), "profile".into(), 8).unwrap(),
                limit,
                window: elephant_storage::PartitionWindow {
                    hits: vec![],
                    full,
                    floor,
                },
                scan: false,
            }
        }
        let mut windows = vec![partition(400, true, Some(1.0))];
        assert!(
            !semantic_settled(&[], 1, &windows),
            "400 rejected hits do not prove no authorized matches"
        );
        widen_semantic_window(&mut windows, None).unwrap();
        assert_eq!(windows[0].limit, 800);
        assert!(windows[0].scan);
        let ranked = vec![SearchResult {
            kind: SearchKind::Chunk,
            text: "eligible".into(),
            evidence: vec![],
            score: 0.8,
        }];
        assert!(
            !semantic_settled(&ranked, 1, &[partition(400, true, Some(0.8))]),
            "unseen equal-score result can precede the visible tie"
        );
        assert!(semantic_settled(
            &ranked,
            1,
            &[partition(400, true, Some(0.7))]
        ));
        assert!(
            !semantic_settled(&ranked, 1, &[partition(400, true, None)]),
            "raw full window without a finite floor cannot certify exhaustion"
        );
        assert!(semantic_settled(&[], 1, &[partition(400, false, None)]));
        let error = widen_semantic_window(
            &mut [partition(MAX_SEMANTIC_CANDIDATES, true, Some(1.0))],
            None,
        )
        .unwrap_err();
        assert_eq!(error.error_code, ErrorCode::RateLimited);
    }

    #[test]
    #[expect(
        clippy::redundant_clone,
        reason = "Keep the complete control and its original fixture ownership, futures and drop lifetimes"
    )]
    fn duplicate_node_scores_are_independent_of_partition_order() {
        let x = RecordId::new_v4(TableName::DocNode);
        let y = RecordId::new_v4(TableName::DocNode);
        let partitions = [
            vec![(x.clone(), 0.4)],
            vec![(x.clone(), 0.9), (y.clone(), 0.8)],
        ];
        for order in [[0, 1], [1, 0]] {
            let mut scores = HashMap::new();
            let mut ranked = Vec::new();
            for partition in order {
                for (node, score) in &partitions[partition] {
                    if improve_semantic_score(&mut scores, node, *score) {
                        push_ranked(
                            &mut ranked,
                            SearchResult {
                                kind: SearchKind::Chunk,
                                text: node.to_string(),
                                evidence: vec![],
                                score: *score,
                            },
                            1,
                        );
                    }
                }
            }
            assert_eq!(
                scores.len(),
                2,
                "physical copies do not consume extra unique-node budget"
            );
            assert_eq!(ranked.len(), 1);
            assert_eq!(ranked[0].text, x.to_string());
            assert!((ranked[0].score - 0.9).abs() < f64::EPSILON);
            assert!(!improve_semantic_score(&mut scores, &x, f64::NAN));
            assert!(!improve_semantic_score(&mut scores, &x, 0.1));
        }
    }

    #[test]
    #[expect(
        clippy::redundant_clone,
        reason = "Keep the complete control and its original fixture ownership, futures and drop lifetimes"
    )]
    fn hybrid_document_ranks_are_global_and_order_independent() {
        let first: RecordId = "doc:00000000-0000-4000-8000-000000000001".parse().unwrap();
        let second: RecordId = "doc:00000000-0000-4000-8000-000000000002".parse().unwrap();
        let third: RecordId = "doc:00000000-0000-4000-8000-000000000003".parse().unwrap();
        let expected = vec![
            (first.clone(), 0.9),
            (second.clone(), 0.9),
            (third.clone(), 0.8),
        ];
        for order in [[0, 1, 2], [2, 1, 0], [1, 2, 0]] {
            let scores = order
                .into_iter()
                .map(|index| expected[index].clone())
                .collect();
            assert_eq!(rank_documents(&scores), expected);
        }
    }

    #[test]
    fn harmonic_fusion_preserves_each_modality_leader_without_shared_terms() {
        // Neither source IDs nor text decide the vote. Test both orientations:
        // an exact lexical leader and a semantic paraphrase receive equal weight.
        let ids = (1..=8)
            .map(|id| {
                format!("doc:00000000-0000-4000-8000-{id:012}")
                    .parse::<RecordId>()
                    .unwrap()
            })
            .collect::<Vec<_>>();
        for swapped in [false, true] {
            let first = vec![
                ids[0].clone(),
                ids[1].clone(),
                ids[2].clone(),
                ids[3].clone(),
            ];
            let second = vec![
                ids[4].clone(),
                ids[1].clone(),
                ids[2].clone(),
                ids[3].clone(),
            ];
            let rankings = if swapped {
                [second, first]
            } else {
                [first, second]
            };
            let mut scores = HashMap::new();
            for ranking in rankings {
                for (rank, id) in ranking.into_iter().enumerate() {
                    *scores.entry(id).or_default() += reciprocal_rank(rank);
                }
            }
            let ranked = rank_documents(&scores);
            let top = ranked
                .iter()
                .take(3)
                .map(|(id, _)| id)
                .collect::<HashSet<_>>();
            assert!(top.contains(&ids[0]) && top.contains(&ids[4]));
            assert!(scores[&ids[0]] > scores[&ids[2]]);
        }
    }

    #[test]
    fn harmonic_fusion_rewards_agreement_and_keeps_tail_order() {
        assert!(reciprocal_rank(1) + reciprocal_rank(0) > reciprocal_rank(0));
        assert!(reciprocal_rank(0) > reciprocal_rank(2) + reciprocal_rank(2));
        assert!(reciprocal_rank(2) > reciprocal_rank(3));
        assert!(reciprocal_rank(usize::MAX).is_finite());
    }

    #[test]
    fn selected_evidence_budget_preserves_support_and_counts_json_escaping() {
        let support = format!(
            "{} The Oriole office opened on 12 March 2025.",
            "Neutral context. ".repeat(80)
        );
        assert!(support.len() > 1024);
        let candidate = |text: String, score| SearchResult {
            kind: SearchKind::Chunk,
            evidence: vec![SearchEvidence {
                source_id: "source".into(),
                revision: "r1".into(),
                span: SearchSpan {
                    start: 0,
                    end: u32::try_from(text.len()).unwrap(),
                },
                quote: text.clone(),
                doc_id: "doc:fixed".into(),
                internal_doc_revision_id: "doc_revision:fixed".into(),
                rendered_span: SearchSpan {
                    start: 0,
                    end: u32::try_from(text.len()).unwrap(),
                },
                rendered_quote_sha256: hex::encode(Sha256::digest(text.as_bytes())),
                rendered_text_sha256: hex::encode(Sha256::digest(text.as_bytes())),
            }],
            text,
            score,
        };
        let mut ranked = Vec::new();
        push_ranked(
            &mut ranked,
            candidate("x".repeat(MAX_SEARCH_RESPONSE_BYTES + 1), 0.1),
            1,
        );
        push_ranked(&mut ranked, candidate(support.clone(), 0.9), 1);
        let mut response = SearchResponse {
            results: ranked,
            coverage: BTreeMap::new(),
            partial: false,
        };
        response_budget(&response).unwrap();
        assert_eq!(response.results[0].evidence[0].quote, support);
        // A lower-ranked oversized support does not break a provably selected
        // smaller answer. Selecting it must fail rather than clip its citation.
        response.results = vec![candidate("x".repeat(MAX_SEARCH_RESPONSE_BYTES + 1), 1.0)];
        assert_eq!(
            response_budget(&response).unwrap_err().error_code,
            ErrorCode::PayloadTooLarge
        );
        response.results = vec![candidate("\"".repeat(MAX_SEARCH_RESPONSE_BYTES / 3), 1.0)];
        assert_eq!(
            response_budget(&response).unwrap_err().error_code,
            ErrorCode::PayloadTooLarge,
            "encoded quotes cost more bytes than unescaped text"
        );
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the complete control and its original fixture ownership, futures and drop lifetimes"
    )]
    async fn authorization_fence_rejects_reclassification_without_a_source_head_change() {
        let storage = elephant_storage::StorageClient::new_embedded()
            .await
            .unwrap();
        // Acceptance now requires a current named space even when this test
        // exercises source-label fencing without model dispatch.
        SpaceConfigRepository::new(storage.clone())
            .create(SpaceConfig {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::SpaceConfig),
                    "alpha".into(),
                    "test".into(),
                ),
                embedding_provider: "unapproved".into(),
                embedding_model: "unused".into(),
                embedding_dims: 3,
                purpose: None,
                skill: None,
                processing: None,
            })
            .await
            .unwrap();
        let publications = SourcePublicationRepository::new(storage.clone());
        let sources = SourceRepository::new(storage);
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::Source),
            "alpha".into(),
            "test".into(),
        );
        base.subjects_state = SubjectsState::Complete;
        let source = Source::new(base, SourceType::Manual, "original digest".into());
        let id = source.base.id.clone();
        publications
            .accept(AcceptSourceRevision {
                source,
                revision_id: "r1".into(),
                expected: SourcePrecondition::Absent,
                idempotency_key: "authorization-fence".into(),
                request_fingerprint: "authorization-fence".into(),
                reactivate: false,
                recreate_after_erasure: false,
                authorized: None,
                authorized_document: None,
            })
            .await
            .unwrap();
        let head = publications.head("alpha", &id).await.unwrap();
        let snapshot = publications
            .authorization_snapshot("alpha", &id)
            .await
            .unwrap()
            .unwrap();
        let observed = HashMap::from([(id.clone(), snapshot)]);
        verify_authorizations(&publications, "alpha", &observed)
            .await
            .unwrap();
        let mut reclassified = sources.get("alpha", &id).await.unwrap().unwrap();
        // Deliberately retain updated_at: the envelope itself must be compared,
        // not only a head token or a timestamp supplied by a writer.
        reclassified.base.security.level += 1;
        sources.update("alpha", &id, reclassified).await.unwrap();
        assert_eq!(publications.head("alpha", &id).await.unwrap(), head);
        assert_eq!(
            verify_authorizations(&publications, "alpha", &observed)
                .await
                .unwrap_err()
                .error_code,
            ErrorCode::NotReady
        );
    }

    #[test]
    fn ranking_wire_defaults_and_legacy_requests_preserve_their_meaning() {
        for (selector, expected) in [
            (json!({}), RankingMode::Hybrid),
            (json!({"use_vector": true}), RankingMode::Hybrid),
            (json!({"use_vector": false}), RankingMode::Lexical),
            (json!({"ranking_mode": "lexical"}), RankingMode::Lexical),
            (json!({"ranking_mode": "semantic"}), RankingMode::Semantic),
            (json!({"ranking_mode": "hybrid"}), RankingMode::Hybrid),
        ] {
            let mut value = selector;
            value["query"] = json!("unchanged original question");
            let request: SearchRequest = serde_json::from_value(value).unwrap();
            request.validate().unwrap();
            assert_eq!(request.effective_ranking_mode().unwrap(), expected);
            assert_eq!(request.query, "unchanged original question");
            let roundtrip: SearchRequest =
                serde_json::from_value(serde_json::to_value(request).unwrap()).unwrap();
            assert_eq!(roundtrip.effective_ranking_mode().unwrap(), expected);
        }
    }

    #[test]
    fn ranking_wire_refuses_null_unknown_and_ambiguous_selectors() {
        for selector in [
            json!({"ranking_mode": null}),
            json!({"use_vector": null}),
            json!({"ranking_mode": "automatic"}),
            json!({"use_vector": "true"}),
            json!({"ranking_mode": "semantic", "use_vector": null}),
        ] {
            let mut value = selector;
            value["query"] = json!("original question");
            assert!(serde_json::from_value::<SearchRequest>(value).is_err());
        }
        for mode in ["lexical", "semantic", "hybrid"] {
            for legacy in [true, false] {
                let request: SearchRequest = serde_json::from_value(json!({
                    "query":"original question", "ranking_mode":mode, "use_vector":legacy
                }))
                .unwrap();
                assert!(request.validate().is_err());
            }
        }
    }

    #[test]
    fn scoped_identifiers_match_source_intake_byte_bounds_without_normalization() {
        let mut value = json!({"query":"original question","use_vector":false,
            "source_revision_constraints":{ "original/id": "é".repeat(128) },
            "filters":{"labels":{"original-label":["original value"]}}});
        let request: SearchRequest = serde_json::from_value(value.clone()).unwrap();
        request.validate().unwrap();
        assert_eq!(
            request.source_revision_constraints["original/id"],
            "é".repeat(128)
        );
        for revision in ["é".repeat(129), "x".repeat(257), "  ".into(), "r\n1".into()] {
            value["source_revision_constraints"]["original/id"] = json!(revision);
            let request: SearchRequest = serde_json::from_value(value.clone()).unwrap();
            assert!(request.validate().is_err());
        }
        for external in [
            "x".repeat(1025),
            "\t".into(),
            "  ".into(),
            "source\0id".into(),
        ] {
            let request: SearchRequest = serde_json::from_value(json!({"query":"original",
                "source_revision_constraints":{ external:"revision" }}))
            .unwrap();
            assert!(request.validate().is_err());
        }
        for labels in [
            json!({" ":"value"}),
            json!({"label":"\n"}),
            json!({"label":["valid", " "]}),
            json!({"label":"x".repeat(1025)}),
            json!({ "x".repeat(129):"value" }),
        ] {
            let request: SearchRequest = serde_json::from_value(json!({"query":"original",
                "filters":{"labels":labels}}))
            .unwrap();
            assert!(request.validate().is_err());
        }
    }

    fn head() -> SourceHead {
        SourceHead {
            space_id: "alpha".into(),
            source_id: RecordId::new_v4(TableName::Source),
            generation: "opaque-generation".into(),
            desired_revision: Some("revision-new".into()),
            published_revision: Some("revision-old".into()),
            operation_id: "operation".into(),
            lifecycle: SourceVisibility::Active,
            updated_at: Utc::now(),
        }
    }
    #[test]
    fn same_revision_reacceptance_requires_current_operation_completion() {
        let mut current = head();
        current.generation = "fresh-a-generation-after-b".into();
        current.operation_id = "fresh-a-operation".into();
        current.desired_revision = Some("revision-old".into());
        // The earlier A publication remains available. Its equal revision
        // string does not prove that this later acceptance has completed.
        for requested in [None, Some("revision-old")] {
            for (operation, expected) in [
                (None, "pending"),
                (Some(OperationState::Accepted), "pending"),
                (Some(OperationState::Processing), "running"),
                (Some(OperationState::FailedRetryable), "failed_retryable"),
                (Some(OperationState::FailedTerminal), "failed_final"),
                (Some(OperationState::Completed), "complete"),
            ] {
                let result = coverage(Some(&current), requested, operation);
                assert_eq!(result.state, expected);
                assert_eq!(result.published_revision.as_deref(), Some("revision-old"));
            }
        }
    }

    #[test]
    fn coverage_never_substitutes_a_different_revision_for_an_exact_request() {
        let mut current = head();
        assert_eq!(
            coverage(
                Some(&current),
                Some("revision-old"),
                Some(OperationState::Processing)
            )
            .state,
            "complete"
        );
        assert_eq!(
            coverage(
                Some(&current),
                Some("revision-new"),
                Some(OperationState::Processing)
            )
            .state,
            "running"
        );
        assert_eq!(
            coverage(
                Some(&current),
                Some("unrelated"),
                Some(OperationState::Completed)
            )
            .state,
            "stale"
        );
        assert_eq!(
            coverage(Some(&current), None, Some(OperationState::FailedTerminal)).state,
            "failed_final"
        );
        for lifecycle in [
            SourceVisibility::Retracted,
            SourceVisibility::Erasing,
            SourceVisibility::Erased,
        ] {
            current.lifecycle = lifecycle;
            let result = coverage(
                Some(&current),
                Some("revision-old"),
                Some(OperationState::Completed),
            );
            assert_ne!(result.state, "complete");
        }
        assert_eq!(coverage(None, Some("requested"), None).state, "missing");
    }

    #[test]
    fn labels_use_membership_and_any_value_without_accepting_empty_filters() {
        let mut source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "alpha".into(),
                "test".into(),
            ),
            SourceType::Manual,
            "digest".into(),
        );
        source.metadata = json!({"labels":{"initiative":["one","two"],"type":"note"}});
        assert!(labels_match(
            &source,
            &BTreeMap::from([("initiative".into(), LabelValue::One("two".into()))])
        ));
        assert!(labels_match(
            &source,
            &BTreeMap::from([(
                "initiative".into(),
                LabelValue::Many(vec!["other".into(), "one".into()])
            )])
        ));
        assert!(!labels_match(
            &source,
            &BTreeMap::from([("initiative".into(), LabelValue::One("missing".into()))])
        ));
        let request: SearchRequest =
            serde_json::from_value(json!({"query":"text","filters":{"labels":{"initiative":[]}}}))
                .unwrap();
        assert!(request.validate().is_err());
        assert!(serde_json::from_value::<SearchRequest>(
            json!({"query":"text","source_revision_constraints":["source"]})
        )
        .is_err());
    }

    #[test]
    fn public_shape_has_object_coverage_and_exact_utf8_evidence() {
        let quote = boundary_prefix("évidence", 3);
        assert_eq!(quote, "év");
        let result = SearchResponse {
            results: vec![SearchResult {
                kind: SearchKind::Chunk,
                text: quote.into(),
                score: 0.5,
                evidence: vec![SearchEvidence {
                    source_id: "external/source".into(),
                    revision: "hash".into(),
                    span: SearchSpan { start: 0, end: 3 },
                    quote: quote.into(),
                    doc_id: "doc:fixture".into(),
                    internal_doc_revision_id: "doc_revision:fixture".into(),
                    rendered_span: SearchSpan { start: 0, end: 3 },
                    rendered_quote_sha256: hex::encode(Sha256::digest(quote.as_bytes())),
                    rendered_text_sha256: hex::encode(Sha256::digest(quote.as_bytes())),
                }],
            }],
            coverage: BTreeMap::from([(
                "external/source".into(),
                coverage(None, Some("hash"), None),
            )]),
            partial: true,
        };
        let value = serde_json::to_value(result).unwrap();
        assert!(value["coverage"].is_object());
        assert_eq!(
            value["results"][0]["evidence"][0]["source_id"],
            "external/source"
        );
        assert_eq!(value["results"][0]["evidence"][0]["span"]["end"], 3);
        assert!(value.get("partial").is_some());
    }

    #[test]
    fn citation_wire_preserves_both_coordinate_systems_without_duplicate_quote() {
        // D's literal repeated-phrase case: a substring search would select
        // the first occurrence, while these exact original coordinates do not.
        let evidence = SearchEvidence {
            source_id: "initiative-42".into(),
            revision: "A".into(),
            span: SearchSpan { start: 28, end: 50 },
            quote: "Mira maintains Helios.".into(),
            doc_id: "doc:11111111-1111-4111-8111-111111111111".into(),
            internal_doc_revision_id: "doc_revision:fixture".into(),
            rendered_span: SearchSpan { start: 24, end: 46 },
            rendered_quote_sha256:
                "837d5285260451e0480c01b1c4ec8e1120acdd6821a8342f4d71fe8ea1392c31".into(),
            rendered_text_sha256:
                "d46236961ccf38554c1321df0caccf6dbb4bdf92afda489d58886bfa4eca4bbc".into(),
        };
        let value = serde_json::to_value(&evidence).unwrap();
        assert_eq!(value["span"], json!({"start":28,"end":50}));
        assert_eq!(value["rendered_span"], json!({"start":24,"end":46}));
        assert_eq!(value["quote"], "Mira maintains Helios.");
        assert_eq!(value["doc_id"], evidence.doc_id);
        assert_eq!(evidence.doc_id.len(), 40);
        assert_eq!(
            value["rendered_quote_sha256"],
            evidence.rendered_quote_sha256
        );
        assert_eq!(value["rendered_text_sha256"], evidence.rendered_text_sha256);
        assert!(value.get("rendered_quote").is_none());
        assert!(value.get("evidence_id").is_none());
        assert_eq!(value.as_object().unwrap().len(), 9);
        for required in [
            "doc_id",
            "rendered_span",
            "rendered_quote_sha256",
            "rendered_text_sha256",
        ] {
            let mut missing = value.clone();
            missing.as_object_mut().unwrap().remove(required);
            assert!(serde_json::from_value::<SearchEvidence>(missing).is_err());
        }
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the complete control and its original fixture ownership, futures and drop lifetimes"
    )]
    async fn actual_mcp_handler_reports_pending_exact_revision_without_dispatching_a_provider() {
        let storage = elephant_storage::StorageClient::new_embedded()
            .await
            .unwrap();
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::SpaceConfig),
            "alpha".into(),
            "test".into(),
        );
        base.subjects_state = SubjectsState::Complete;
        SpaceConfigRepository::new(storage.clone())
            .create(SpaceConfig {
                base,
                embedding_provider: "unapproved".into(),
                embedding_model: "unused".into(),
                embedding_dims: 3,
                purpose: None,
                skill: None,
                processing: None,
            })
            .await
            .unwrap();
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::Source),
            "alpha".into(),
            "test".into(),
        );
        base.subjects_state = SubjectsState::Complete;
        let mut source = Source::new(base, SourceType::Manual, "digest".into());
        source.external_id = Some("consumer/source".into());
        SourcePublicationRepository::new(storage.clone())
            .accept(AcceptSourceRevision {
                source,
                revision_id: "required".into(),
                expected: SourcePrecondition::Absent,
                idempotency_key: "accept-fixture".into(),
                request_fingerprint: "fixture".into(),
                reactivate: false,
                recreate_after_erasure: false,
                authorized: None,
                authorized_document: None,
            })
            .await
            .unwrap();
        let ctx = ToolContext::with_authz(
            storage,
            std::sync::Arc::new(crate::authz::AuthzService::new_with_skip_auth(true)),
            elephant_policy::Principal {
                space_id: "alpha".into(),
                scopes: std::collections::HashSet::new(),
                clearances: vec![],
                subject_allowlist: None,
                purpose: std::collections::HashSet::new(),
            },
        );
        // use_vector defaults true. No eligible generation exists, so the
        // actual handler must report readiness without touching a model.
        let result=ScopedSearchHandler.execute(&ctx,json!({"query":"original question","source_revision_constraints":{"consumer/source":"required"}})).await.unwrap();
        assert_eq!(result["data"]["results"], json!([]));
        assert_eq!(
            result["data"]["coverage"]["consumer/source"]["state"],
            "pending"
        );
        assert_eq!(
            result["data"]["coverage"]["consumer/source"]["desired_revision"],
            "required"
        );
        assert!(result["data"]["coverage"]["consumer/source"]["published_revision"].is_null());
        assert_eq!(result["data"]["partial"], true);
    }
}

#[cfg(test)]
#[path = "scoped_search_reprepare_tests.rs"]
mod reprepare_tests;
