//! MCP tools for derived knowledge projections and curated article definitions.

use async_trait::async_trait;
use elephant_knowledge_views::{
    CompiledEntityView, CompiledGraphView, CompiledSourceView, EntityViewCompiler,
    EntityViewLimits, GraphViewCompiler, SourceViewCompiler, SourceViewLimits,
};
#[cfg(feature = "meerkat-extraction")]
use elephant_storage::WorkItemRepository;
use elephant_storage::{
    parse_id, EntityRepository, KnowledgeViewDefinitionRepository, KnowledgeViewRepository,
    Repository, SourceRepository, StorageClient,
};
use elephant_types::records::base::{BaseFields, Security, SubjectsState};
use elephant_types::records::{
    Entity, KnowledgeView, KnowledgeViewDefinition, KnowledgeViewPublicationStatus,
    KnowledgeViewRevision, KnowledgeViewSelector,
};
use elephant_types::{Link, RecordId, TableName};
use futures::{stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::coercion::coerce_params;
use crate::error::{McpError, McpResult};
use crate::profile::ToolProfile;
use crate::tools::ToolResponse;

use super::{ToolContext, ToolHandler};

/// Authorizes delegation to the space-wide wiki service. This scope explicitly
/// permits compilation across all security envelopes and durable publication.
pub(super) fn authorize_wiki_management(ctx: &ToolContext) -> Result<(), String> {
    if ctx.principal.space_id.trim().is_empty() {
        return Err("Access denied: missing space_id".to_string());
    }
    if ctx.authz.is_skip_auth() {
        return Ok(());
    }
    if ctx.principal.subject_allowlist.is_some() {
        return Err(
            "Access denied: wiki management requires an unrestricted subject scope".to_string(),
        );
    }
    if !ctx.principal.scopes.contains("manage:wiki") {
        return Err("Access denied: space-wide wiki management requires manage:wiki".to_string());
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum FreshnessMode {
    AllowStale,
    PreferFresh,
    Required,
}

const fn default_freshness_mode() -> FreshnessMode {
    FreshnessMode::PreferFresh
}

const fn default_limit() -> usize {
    25
}

const fn default_materialization_limit() -> usize {
    10
}

const fn default_materialization_concurrency() -> usize {
    4
}

const fn default_wiki_article_limit() -> usize {
    0
}

const fn default_wiki_article_hops() -> u8 {
    1
}

const fn default_wiki_article_members() -> u16 {
    250
}

const fn default_wiki_refresh_interval_minutes() -> u32 {
    360
}

const fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize)]
struct GetKnowledgeViewInput {
    #[serde(default)]
    entity_id: Option<String>,
    #[serde(default)]
    source_id: Option<String>,
    #[serde(default)]
    definition_id: Option<String>,
    #[serde(default = "default_freshness_mode")]
    freshness: FreshnessMode,
    #[serde(default = "default_limit")]
    limit: usize,
}

#[derive(Debug, Deserialize)]
struct RebuildKnowledgeViewInput {
    #[serde(default)]
    entity_id: Option<String>,
    #[serde(default)]
    source_id: Option<String>,
    #[serde(default)]
    definition_id: Option<String>,
    #[serde(default = "default_limit")]
    limit: usize,
}

#[derive(Debug, Deserialize)]
struct GetKnowledgeViewDefinitionInput {
    definition_id: String,
}

#[derive(Debug, Deserialize)]
struct UpsertKnowledgeViewDefinitionInput {
    #[serde(default)]
    definition_id: Option<String>,
    title: String,
    #[serde(default)]
    publication_status: Option<KnowledgeViewPublicationStatus>,
    #[serde(default)]
    related_articles: Option<Vec<String>>,
    selector: KnowledgeViewSelector,
    #[serde(default)]
    security: Option<Security>,
}

#[derive(Debug, Deserialize)]
struct MaterializeKnowledgeViewsInput {
    #[serde(default = "default_true")]
    include_entities: bool,
    #[serde(default = "default_true")]
    include_sources: bool,
    #[serde(default = "default_true")]
    include_definitions: bool,
    #[serde(default)]
    entity_cursor: Option<String>,
    #[serde(default)]
    source_cursor: Option<String>,
    #[serde(default)]
    definition_cursor: Option<String>,
    #[serde(default = "default_materialization_limit")]
    limit_per_kind: usize,
    #[serde(default = "default_limit")]
    view_limit: usize,
    #[serde(default = "default_materialization_concurrency")]
    concurrency: usize,
}

#[derive(Debug, Deserialize)]
struct CompileWikiArticleInput {
    definition_id: String,
    #[serde(default)]
    publish: bool,
    #[serde(default)]
    writer_model: Option<String>,
    #[serde(default)]
    verifier_model: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CreateWikiFromGraphInput {
    #[serde(default = "default_wiki_article_limit")]
    max_articles: usize,
    #[serde(default = "default_wiki_article_hops")]
    article_max_hops: u8,
    #[serde(default = "default_wiki_article_members")]
    article_max_members: u16,
    #[serde(default = "default_true")]
    publish: bool,
    #[serde(default = "default_true")]
    manage: bool,
    #[serde(default = "default_wiki_refresh_interval_minutes")]
    refresh_interval_minutes: u32,
    #[serde(default)]
    writer_model: Option<String>,
    #[serde(default)]
    verifier_model: Option<String>,
}

#[cfg(feature = "meerkat-extraction")]
struct AuthorizedGraphScan {
    candidates: Vec<Entity>,
    entities_scanned: usize,
    entities_skipped_unauthorized: usize,
}

#[cfg(feature = "meerkat-extraction")]
#[derive(Serialize)]
struct FullGraphWikiResult {
    full_graph_scan: bool,
    entities_scanned: usize,
    entities_authorized: usize,
    entities_skipped_unauthorized: usize,
    managed: bool,
    next_reconciliation_work_item: Option<String>,
    next_reconciliation_at: Option<chrono::DateTime<chrono::Utc>>,
    reconciliation: elephant_pipeline::wiki_editorial::ReconciledWiki,
}

#[derive(Debug, Default, Serialize)]
struct MaterializationKindReport {
    scanned: usize,
    compiled: usize,
    changed: usize,
    reused: usize,
    skipped_unauthorized: usize,
    failed: usize,
    next_cursor: Option<String>,
}

#[derive(Debug, Serialize)]
struct MaterializationFailure {
    kind: &'static str,
    record_id: String,
    error: String,
}

#[derive(Debug, Default, Serialize)]
struct MaterializeKnowledgeViewsReport {
    entities: MaterializationKindReport,
    sources: MaterializationKindReport,
    definitions: MaterializationKindReport,
    failures: Vec<MaterializationFailure>,
}

/// Response containing both the stable definition and immutable receipt.
#[derive(Debug, Clone, Serialize)]
pub struct KnowledgeViewResponse {
    /// Stable view record.
    pub view: KnowledgeView,
    /// Current immutable view revision.
    pub revision: KnowledgeViewRevision,
    /// Whether the request published a new revision.
    pub changed: bool,
    /// Whether a stale revision was returned after a preferred refresh failed.
    pub stale_fallback: bool,
    /// Optional non-fatal refresh warning.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warning: Option<String>,
}

/// Reads one governed graph-view definition.
pub struct GetKnowledgeViewDefinitionHandler {
    definitions: KnowledgeViewDefinitionRepository,
}

impl GetKnowledgeViewDefinitionHandler {
    /// Creates the handler.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            definitions: KnowledgeViewDefinitionRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for GetKnowledgeViewDefinitionHandler {
    fn name(&self) -> &'static str {
        "get_knowledge_view_definition"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Get the governed typed selector behind a topic, community, or saved-query article candidate.\n\n",
            "Example:\n",
            r#"{ "definition_id": "knowledge_view_definition:550e8400-e29b-41d4-a716-446655440000" }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["definition_id"],
            "properties": {
                "definition_id": {
                    "type": "string",
                    "description": "knowledge_view_definition ID"
                }
            }
        })
    }

    fn profile(&self) -> ToolProfile {
        ToolProfile::Agent
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: GetKnowledgeViewDefinitionInput = serde_json::from_value(params)
            .map_err(|error| McpError::InvalidParameters(error.to_string()))?;
        let id = parse_id(&input.definition_id, TableName::KnowledgeViewDefinition)
            .map_err(|error| McpError::InvalidParameters(error.to_string()))?;
        let Some(definition) = self.definitions.get(&ctx.principal.space_id, &id).await? else {
            return response_error(format!("Knowledge-view definition not found: {id}"));
        };
        if let Err(reason) = authorize_definition(ctx, &definition) {
            return response_error(format!("Access denied: {reason}"));
        }
        serde_json::to_value(ToolResponse::success(definition))
            .map_err(|error| McpError::ExecutionError(error.to_string()))
    }
}

fn authorize_definition(
    ctx: &ToolContext,
    definition: &KnowledgeViewDefinition,
) -> Result<(), String> {
    if ctx.principal.subject_allowlist.is_some()
        && !elephant_knowledge_views::has_complete_definition_subject_contract(definition)
    {
        return Err("definition requires replanning to establish its subject scope".to_string());
    }
    ctx.authz
        .authorize_record_read(&ctx.principal, definition, "read:entity")
}

/// Creates or updates a governed graph-view definition.
pub struct UpsertKnowledgeViewDefinitionHandler {
    storage: StorageClient,
    definitions: KnowledgeViewDefinitionRepository,
    views: KnowledgeViewRepository,
}

impl UpsertKnowledgeViewDefinitionHandler {
    /// Creates the handler.
    #[must_use]
    pub fn new(storage: StorageClient) -> Self {
        Self {
            definitions: KnowledgeViewDefinitionRepository::new(storage.clone()),
            views: KnowledgeViewRepository::new(storage.clone()),
            storage,
        }
    }
}

#[async_trait]
impl ToolHandler for UpsertKnowledgeViewDefinitionHandler {
    fn name(&self) -> &'static str {
        "upsert_knowledge_view_definition"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Create or update a governed, bounded topic, community, or saved-query context definition for a future editorial article compiler. ",
            "Selectors are typed data, never raw database queries, and compiling one does not create or publish a wiki article.\n\n",
            "Example:\n",
            r#"{ "title": "Rust concepts", "selector": { "type": "topic", "topic": "entity:550e8400-e29b-41d4-a716-446655440000", "relation_types": [], "max_hops": 1, "max_members": 50 } }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["title", "selector"],
            "properties": {
                "definition_id": {
                    "type": "string",
                    "description": "Existing knowledge_view_definition ID; omit to create"
                },
                "title": {"type": "string", "minLength": 1, "maxLength": 200},
                "publication_status": {
                    "type": "string",
                    "enum": ["draft", "published", "hidden"],
                    "description": "Context-plan state. Published means eligible for an LLM editorial compiler; it never publishes the deterministic graph projection as an article."
                },
                "related_articles": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Canonical entity IDs selected as related wiki articles; these are navigation targets, not security subjects"
                },
                "selector": {
                    "type": "object",
                    "required": [],
                    "description": "Tagged topic, community, or query KnowledgeViewSelector"
                },
                "security": {
                    "type": "object",
                    "required": [],
                    "description": "Optional exact security envelope; existing envelope is preserved on update by default"
                }
            }
        })
    }

    fn profile(&self) -> ToolProfile {
        ToolProfile::Maintenance
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: UpsertKnowledgeViewDefinitionInput = serde_json::from_value(params)
            .map_err(|error| McpError::InvalidParameters(error.to_string()))?;
        let id = input.definition_id.as_deref().map_or_else(
            || Ok(RecordId::new_v7(TableName::KnowledgeViewDefinition)),
            |value| {
                parse_id(value, TableName::KnowledgeViewDefinition)
                    .map_err(|error| McpError::InvalidParameters(error.to_string()))
            },
        )?;
        let existing = self.definitions.get(&ctx.principal.space_id, &id).await?;
        let related_articles = input
            .related_articles
            .as_deref()
            .map(parse_entity_links)
            .transpose()?;
        let now = chrono::Utc::now();
        let (mut subjects, mut subjects_state) = selector_subjects(&input.selector);
        let mut definition = if let Some(existing) = existing {
            if let Err(reason) =
                ctx.authz
                    .authorize_record_write(&ctx.principal, &existing, "write:entity")
            {
                return response_error(format!("Access denied: {reason}"));
            }
            if existing.navigation.is_some() {
                if input
                    .security
                    .as_ref()
                    .is_some_and(|security| security != &existing.base.security)
                {
                    return response_error("Changing the security of retained organizer navigation requires replanning");
                }
                subjects.extend(existing.base.subjects.iter().cloned());
                subjects.sort_by(|left, right| left.id().cmp(right.id()));
                subjects.dedup_by(|left, right| left.id() == right.id());
                if existing.base.subjects_state != SubjectsState::Complete
                    || existing.base.subjects.is_empty()
                {
                    subjects_state = SubjectsState::Unknown;
                }
            }
            let existing_publication_status = existing.publication_status;
            KnowledgeViewDefinition {
                base: BaseFields {
                    updated_at: now,
                    security: input.security.unwrap_or(existing.base.security),
                    subjects,
                    subjects_state,
                    ..existing.base
                },
                title: input.title,
                publication_status: input
                    .publication_status
                    .unwrap_or(existing_publication_status),
                related_articles: related_articles.unwrap_or(existing.related_articles),
                navigation: existing.navigation,
                selector: input.selector,
            }
        } else {
            let mut base =
                BaseFields::new(id, ctx.principal.space_id.clone(), "mcp_tool".to_string());
            base.security = input.security.unwrap_or(base.security);
            base.subjects = subjects;
            base.subjects_state = subjects_state;
            KnowledgeViewDefinition {
                base,
                title: input.title,
                publication_status: input.publication_status.unwrap_or_default(),
                related_articles: related_articles.unwrap_or_default(),
                navigation: None,
                selector: input.selector,
            }
        };
        definition.base.updated_at = now;
        if let Err(reason) =
            ctx.authz
                .authorize_record_write(&ctx.principal, &definition, "write:entity")
        {
            return response_error(format!("Access denied: {reason}"));
        }
        validate_definition_entities(&self.storage, &ctx.principal.space_id, &definition).await?;
        let definition = self.definitions.upsert(definition).await?;
        let _ = self
            .views
            .mark_dirty_for_dependencies(
                &ctx.principal.space_id,
                &[definition.base.id.to_string()],
                "knowledge-view definition updated",
            )
            .await?;
        serde_json::to_value(ToolResponse::success(definition))
            .map_err(|error| McpError::ExecutionError(error.to_string()))
    }
}

fn selector_subjects(selector: &KnowledgeViewSelector) -> (Vec<Link<Entity>>, SubjectsState) {
    match selector {
        KnowledgeViewSelector::Topic { topic, .. } => {
            (vec![topic.clone()], SubjectsState::Complete)
        }
        KnowledgeViewSelector::Community { seeds, .. } => (seeds.clone(), SubjectsState::Complete),
        KnowledgeViewSelector::Query { .. } => (Vec::new(), SubjectsState::Complete),
    }
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the entity repository through the complete definition-entity validation pass."
)]
async fn validate_definition_entities(
    storage: &StorageClient,
    space_id: &str,
    definition: &KnowledgeViewDefinition,
) -> McpResult<()> {
    let entities = EntityRepository::new(storage.clone());
    for subject in &definition.base.subjects {
        let Some(entity) = entities.get(space_id, subject.id()).await? else {
            return Err(McpError::InvalidParameters(format!(
                "Selector entity not found: {}",
                subject.id()
            )));
        };
        if entity.base.security != definition.base.security {
            return Err(McpError::InvalidParameters(format!(
                "Selector entity {} does not share the definition security envelope",
                subject.id()
            )));
        }
    }
    Ok(())
}

/// Reads or lazily refreshes a security-scoped knowledge projection.
pub struct GetKnowledgeViewHandler {
    storage: StorageClient,
    entity_compiler: EntityViewCompiler,
    source_compiler: SourceViewCompiler,
    graph_compiler: GraphViewCompiler,
}

impl GetKnowledgeViewHandler {
    /// Creates the handler.
    #[must_use]
    pub fn new(storage: StorageClient) -> Self {
        Self {
            entity_compiler: EntityViewCompiler::new(storage.clone()),
            source_compiler: SourceViewCompiler::new(storage.clone()),
            graph_compiler: GraphViewCompiler::new(storage.clone()),
            storage,
        }
    }

    async fn existing_response(
        &self,
        ctx: &ToolContext,
        entity: &elephant_types::records::Entity,
    ) -> McpResult<Option<KnowledgeViewResponse>> {
        let Some(view) = self
            .entity_compiler
            .find_existing(&ctx.principal.space_id, entity)
            .await?
        else {
            return Ok(None);
        };
        let Some(revision) = self
            .entity_compiler
            .repository()
            .get_current_revision(&view)
            .await?
        else {
            return Ok(None);
        };
        authorize_projection(ctx, &view, &revision, "read:entity").await?;
        Ok(Some(KnowledgeViewResponse {
            view,
            revision,
            changed: false,
            stale_fallback: false,
            warning: None,
        }))
    }

    async fn existing_source_response(
        &self,
        ctx: &ToolContext,
        source: &elephant_types::records::Source,
    ) -> McpResult<Option<KnowledgeViewResponse>> {
        let Some(view) = self
            .source_compiler
            .find_existing(&ctx.principal.space_id, source)
            .await?
        else {
            return Ok(None);
        };
        let Some(revision) = self
            .source_compiler
            .repository()
            .get_current_revision(&view)
            .await?
        else {
            return Ok(None);
        };
        authorize_projection(ctx, &view, &revision, "read:source").await?;
        Ok(Some(KnowledgeViewResponse {
            view,
            revision,
            changed: false,
            stale_fallback: false,
            warning: None,
        }))
    }

    async fn existing_graph_response(
        &self,
        ctx: &ToolContext,
        definition: &KnowledgeViewDefinition,
    ) -> McpResult<Option<KnowledgeViewResponse>> {
        let Some(view) = self
            .graph_compiler
            .find_existing(&ctx.principal.space_id, definition)
            .await?
        else {
            return Ok(None);
        };
        let Some(revision) = self
            .graph_compiler
            .repository()
            .get_current_revision(&view)
            .await?
        else {
            return Ok(None);
        };
        authorize_projection(ctx, &view, &revision, "read:entity").await?;
        Ok(Some(KnowledgeViewResponse {
            view,
            revision,
            changed: false,
            stale_fallback: false,
            warning: None,
        }))
    }
}

#[async_trait]
impl ToolHandler for GetKnowledgeViewHandler {
    fn name(&self) -> &'static str {
        "get_knowledge_view"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Get a graph-native cited projection for an entity, source, or governed topic/community/query definition. Entity and graph-selection projections are inspector inputs; source projections are evidence. None is an LLM-authored wiki article. Every projection is derived and never a source of truth.\n\n",
            "Example:\n",
            r#"{ "entity_id": "entity:550e8400-e29b-41d4-a716-446655440000", "freshness": "prefer_fresh", "limit": 25 }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": [],
            "oneOf": [
                {"required": ["entity_id"]},
                {"required": ["source_id"]},
                {"required": ["definition_id"]}
            ],
            "properties": {
                "entity_id": {
                    "type": "string",
                    "description": "Canonical entity ID"
                },
                "source_id": {
                    "type": "string",
                    "description": "Canonical source ID"
                },
                "definition_id": {
                    "type": "string",
                    "description": "Governed knowledge_view_definition ID"
                },
                "freshness": {
                    "type": "string",
                    "enum": ["allow_stale", "prefer_fresh", "required"],
                    "default": "prefer_fresh",
                    "description": "Whether to reuse, prefer, or require a revision compiled from current dependencies"
                },
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 100,
                    "default": 25,
                    "description": "Maximum records per graph section"
                }
            }
        })
    }

    fn profile(&self) -> ToolProfile {
        ToolProfile::Agent
    }

    #[expect(
        clippy::large_futures,
        reason = "Keep the original unboxed source projection future and root-dispatch await order."
    )]
    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: GetKnowledgeViewInput = serde_json::from_value(params)
            .map_err(|error| McpError::InvalidParameters(error.to_string()))?;
        if ctx.principal.space_id.is_empty() {
            return response_error("Access denied: missing space_id");
        }
        if !(1..=100).contains(&input.limit) {
            return Err(McpError::InvalidParameters(
                "limit must be between 1 and 100".to_string(),
            ));
        }
        let root = parse_root(
            input.entity_id.as_deref(),
            input.source_id.as_deref(),
            input.definition_id.as_deref(),
        )?;
        match root {
            KnowledgeViewRoot::Entity(entity_id) => {
                self.execute_entity(ctx, input, entity_id).await
            }
            KnowledgeViewRoot::Source(source_id) => {
                self.execute_source(ctx, input, source_id).await
            }
            KnowledgeViewRoot::Definition(definition_id) => {
                self.execute_definition(ctx, input, definition_id).await
            }
        }
    }
}

impl GetKnowledgeViewHandler {
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the repository scope through authorization, compilation and stale-fallback handling."
    )]
    async fn execute_entity(
        &self,
        ctx: &ToolContext,
        input: GetKnowledgeViewInput,
        entity_id: elephant_types::RecordId,
    ) -> McpResult<Value> {
        let entities = EntityRepository::new(self.storage.clone());
        let Some(entity) = entities.get(&ctx.principal.space_id, &entity_id).await? else {
            return response_error(format!("Entity not found: {entity_id}"));
        };
        if let Err(reason) = ctx
            .authz
            .authorize_record_read(&ctx.principal, &entity, "read:entity")
        {
            return response_error(format!("Access denied: {reason}"));
        }

        if input.freshness == FreshnessMode::AllowStale {
            if let Some(response) = self.existing_response(ctx, &entity).await? {
                return response_success(response);
            }
        }

        let limits = EntityViewLimits {
            outgoing_relations: input.limit,
            incoming_relations: input.limit,
            attributes: input.limit,
            events: input.limit,
        };
        match self
            .entity_compiler
            .compile(&ctx.principal.space_id, &entity_id, limits, false)
            .await
        {
            Ok(compiled) => {
                let response = compiled_entity_response(ctx, compiled).await?;
                response_success(response)
            }
            Err(error @ elephant_storage::StorageError::ErasureInProgress) => Err(error.into()),
            Err(error) if input.freshness == FreshnessMode::PreferFresh => {
                let Some(mut response) = self.existing_response(ctx, &entity).await? else {
                    return Err(error.into());
                };
                response.stale_fallback = true;
                response.warning = Some(format!("Refresh failed: {error}"));
                response_success(response)
            }
            Err(error) => Err(error.into()),
        }
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the repository scope through authorization, compilation and stale-fallback handling."
    )]
    async fn execute_source(
        &self,
        ctx: &ToolContext,
        input: GetKnowledgeViewInput,
        source_id: elephant_types::RecordId,
    ) -> McpResult<Value> {
        let sources = SourceRepository::new(self.storage.clone());
        let Some(source) = sources.get(&ctx.principal.space_id, &source_id).await? else {
            return response_error(format!("Source not found: {source_id}"));
        };
        if let Err(reason) = ctx
            .authz
            .authorize_record_read(&ctx.principal, &source, "read:source")
        {
            return response_error(format!("Access denied: {reason}"));
        }

        if input.freshness == FreshnessMode::AllowStale {
            if let Some(response) = self.existing_source_response(ctx, &source).await? {
                return response_success(response);
            }
        }

        let limits = SourceViewLimits {
            documents: input.limit,
            revisions_per_document: input.limit,
        };
        match self
            .source_compiler
            .compile(&ctx.principal.space_id, &source_id, limits, false)
            .await
        {
            Ok(compiled) => {
                let response = compiled_source_response(ctx, compiled).await?;
                response_success(response)
            }
            Err(error @ elephant_storage::StorageError::ErasureInProgress) => Err(error.into()),
            Err(error) if input.freshness == FreshnessMode::PreferFresh => {
                let Some(mut response) = self.existing_source_response(ctx, &source).await? else {
                    return Err(error.into());
                };
                response.stale_fallback = true;
                response.warning = Some(format!("Refresh failed: {error}"));
                response_success(response)
            }
            Err(error) => Err(error.into()),
        }
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the repository scope through definition authorization, compilation and fallback handling."
    )]
    async fn execute_definition(
        &self,
        ctx: &ToolContext,
        input: GetKnowledgeViewInput,
        definition_id: RecordId,
    ) -> McpResult<Value> {
        let definitions = KnowledgeViewDefinitionRepository::new(self.storage.clone());
        let Some(definition) = definitions
            .get(&ctx.principal.space_id, &definition_id)
            .await?
        else {
            return response_error(format!(
                "Knowledge-view definition not found: {definition_id}"
            ));
        };
        if let Err(reason) = authorize_definition(ctx, &definition) {
            return response_error(format!("Access denied: {reason}"));
        }
        if input.freshness == FreshnessMode::AllowStale {
            if let Some(response) = self.existing_graph_response(ctx, &definition).await? {
                return response_success(response);
            }
        }
        match self
            .graph_compiler
            .compile(&ctx.principal.space_id, &definition_id, false)
            .await
        {
            Ok(compiled) => response_success(compiled_graph_response(ctx, compiled).await?),
            Err(error @ elephant_storage::StorageError::ErasureInProgress) => Err(error.into()),
            Err(error) if input.freshness == FreshnessMode::PreferFresh => {
                let Some(mut response) = self.existing_graph_response(ctx, &definition).await?
                else {
                    return Err(error.into());
                };
                response.stale_fallback = true;
                response.warning = Some(format!("Refresh failed: {error}"));
                response_success(response)
            }
            Err(error) => Err(error.into()),
        }
    }
}

/// Forces publication of a new entity-view revision.
pub struct RebuildKnowledgeViewHandler {
    storage: StorageClient,
    entity_compiler: EntityViewCompiler,
    source_compiler: SourceViewCompiler,
    graph_compiler: GraphViewCompiler,
}

impl RebuildKnowledgeViewHandler {
    /// Creates the handler.
    #[must_use]
    pub fn new(storage: StorageClient) -> Self {
        Self {
            entity_compiler: EntityViewCompiler::new(storage.clone()),
            source_compiler: SourceViewCompiler::new(storage.clone()),
            graph_compiler: GraphViewCompiler::new(storage.clone()),
            storage,
        }
    }

    // Keep each selected branch and its large child futures out of the shared
    // dispatch poll frame. Authorization and publication order stay local to
    // the original branch; no task, thread, or execution context is introduced.
    #[inline(never)]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the repository scope through entity authorization and rebuild completion."
    )]
    async fn rebuild_entity(
        &self,
        ctx: &ToolContext,
        entity_id: RecordId,
        limit: usize,
    ) -> McpResult<Value> {
        let entities = EntityRepository::new(self.storage.clone());
        let Some(entity) = Box::pin(entities.get(&ctx.principal.space_id, &entity_id)).await?
        else {
            return response_error(format!("Entity not found: {entity_id}"));
        };
        if let Err(reason) =
            ctx.authz
                .authorize_record_write(&ctx.principal, &entity, "write:entity")
        {
            return response_error(format!("Access denied: {reason}"));
        }
        let limits = EntityViewLimits {
            outgoing_relations: limit,
            incoming_relations: limit,
            attributes: limit,
            events: limit,
        };
        let compiled = Box::pin(self.entity_compiler.compile(
            &ctx.principal.space_id,
            &entity_id,
            limits,
            true,
        ))
        .await?;
        response_success(Box::pin(compiled_entity_response(ctx, compiled)).await?)
    }

    #[inline(never)]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the repository scope through source authorization and rebuild completion."
    )]
    async fn rebuild_source(
        &self,
        ctx: &ToolContext,
        source_id: RecordId,
        limit: usize,
    ) -> McpResult<Value> {
        let sources = SourceRepository::new(self.storage.clone());
        let Some(source) = Box::pin(sources.get(&ctx.principal.space_id, &source_id)).await? else {
            return response_error(format!("Source not found: {source_id}"));
        };
        if let Err(reason) =
            ctx.authz
                .authorize_record_write(&ctx.principal, &source, "write:source")
        {
            return response_error(format!("Access denied: {reason}"));
        }
        let limits = SourceViewLimits {
            documents: limit,
            revisions_per_document: limit,
        };
        let compiled = Box::pin(self.source_compiler.compile(
            &ctx.principal.space_id,
            &source_id,
            limits,
            true,
        ))
        .await?;
        response_success(Box::pin(compiled_source_response(ctx, compiled)).await?)
    }

    #[inline(never)]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the repository scope through definition authorization and rebuild completion."
    )]
    async fn rebuild_definition(
        &self,
        ctx: &ToolContext,
        definition_id: RecordId,
    ) -> McpResult<Value> {
        let definitions = KnowledgeViewDefinitionRepository::new(self.storage.clone());
        let Some(definition) =
            Box::pin(definitions.get(&ctx.principal.space_id, &definition_id)).await?
        else {
            return response_error(format!(
                "Knowledge-view definition not found: {definition_id}"
            ));
        };
        if let Err(reason) =
            ctx.authz
                .authorize_record_write(&ctx.principal, &definition, "write:entity")
        {
            return response_error(format!("Access denied: {reason}"));
        }
        let compiled = Box::pin(self.graph_compiler.compile(
            &ctx.principal.space_id,
            &definition_id,
            true,
        ))
        .await?;
        response_success(Box::pin(compiled_graph_response(ctx, compiled)).await?)
    }
}

#[async_trait]
impl ToolHandler for RebuildKnowledgeViewHandler {
    fn name(&self) -> &'static str {
        "rebuild_knowledge_view"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Force a new immutable projection revision for a canonical entity, source, or governed graph-view definition. Entity and source revisions remain inspector/evidence records.\n\n",
            "Example:\n",
            r#"{ "entity_id": "entity:550e8400-e29b-41d4-a716-446655440000", "limit": 25 }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": [],
            "oneOf": [
                {"required": ["entity_id"]},
                {"required": ["source_id"]},
                {"required": ["definition_id"]}
            ],
            "properties": {
                "entity_id": {
                    "type": "string",
                    "description": "Canonical entity ID"
                },
                "source_id": {
                    "type": "string",
                    "description": "Canonical source ID"
                },
                "definition_id": {
                    "type": "string",
                    "description": "Governed knowledge_view_definition ID"
                },
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 100,
                    "default": 25,
                    "description": "Maximum records per graph section"
                }
            }
        })
    }

    fn profile(&self) -> ToolProfile {
        ToolProfile::Maintenance
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: RebuildKnowledgeViewInput = serde_json::from_value(params)
            .map_err(|error| McpError::InvalidParameters(error.to_string()))?;
        if ctx.principal.space_id.is_empty() {
            return response_error("Access denied: missing space_id");
        }
        if !(1..=100).contains(&input.limit) {
            return Err(McpError::InvalidParameters(
                "limit must be between 1 and 100".to_string(),
            ));
        }
        let root = parse_root(
            input.entity_id.as_deref(),
            input.source_id.as_deref(),
            input.definition_id.as_deref(),
        )?;
        match root {
            KnowledgeViewRoot::Entity(entity_id) => {
                Box::pin(self.rebuild_entity(ctx, entity_id, input.limit)).await
            }
            KnowledgeViewRoot::Source(source_id) => {
                Box::pin(self.rebuild_source(ctx, source_id, input.limit)).await
            }
            KnowledgeViewRoot::Definition(definition_id) => {
                Box::pin(self.rebuild_definition(ctx, definition_id)).await
            }
        }
    }
}

#[cfg(test)]
#[path = "knowledge_view_rebuild_frame_tests.rs"]
mod rebuild_frame_tests;

/// Materializes bounded pages of existing Elephant data into derived views.
pub struct MaterializeKnowledgeViewsHandler {
    storage: StorageClient,
    entity_compiler: EntityViewCompiler,
    source_compiler: SourceViewCompiler,
    graph_compiler: GraphViewCompiler,
}

impl MaterializeKnowledgeViewsHandler {
    /// Creates the handler.
    #[must_use]
    pub fn new(storage: StorageClient) -> Self {
        Self {
            entity_compiler: EntityViewCompiler::new(storage.clone()),
            source_compiler: SourceViewCompiler::new(storage.clone()),
            graph_compiler: GraphViewCompiler::new(storage.clone()),
            storage,
        }
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the repository scope through the complete authorized entity materialization pass."
    )]
    async fn materialize_entities(
        &self,
        ctx: &ToolContext,
        input: &MaterializeKnowledgeViewsInput,
        cursor: Option<&RecordId>,
        report: &mut MaterializeKnowledgeViewsReport,
    ) -> McpResult<()> {
        let repository = EntityRepository::new(self.storage.clone());
        let mut records = repository
            .list(&ctx.principal.space_id, input.limit_per_kind + 1, cursor)
            .await?;
        let has_more = records.len() > input.limit_per_kind;
        records.truncate(input.limit_per_kind);
        if has_more {
            report.entities.next_cursor = records.last().map(|record| record.base.id.to_string());
        }
        let limits = EntityViewLimits {
            outgoing_relations: input.view_limit,
            incoming_relations: input.view_limit,
            attributes: input.view_limit,
            events: input.view_limit,
        };
        let mut authorized = Vec::with_capacity(records.len());
        for record in records {
            report.entities.scanned += 1;
            if ctx
                .authz
                .authorize_record_read(&ctx.principal, &record, "read:entity")
                .is_err()
            {
                report.entities.skipped_unauthorized += 1;
                continue;
            }
            authorized.push(record);
        }
        let mut results = stream::iter(authorized)
            .map(|record| {
                let compiler = self.entity_compiler.clone();
                let space_id = ctx.principal.space_id.clone();
                async move {
                    let record_id = record.base.id;
                    let result = compiler.compile(&space_id, &record_id, limits, false).await;
                    (record_id, result)
                }
            })
            .buffer_unordered(input.concurrency)
            .collect::<Vec<_>>()
            .await;
        results.sort_by(|(left, _), (right, _)| left.to_string().cmp(&right.to_string()));
        for (record_id, result) in results {
            match result {
                Ok(compiled) => {
                    if let Err(error) =
                        authorize_projection(ctx, &compiled.view, &compiled.revision, "read:entity")
                            .await
                    {
                        push_materialization_failure(
                            &mut report.entities,
                            &mut report.failures,
                            "entity",
                            &record_id,
                            error,
                        );
                    } else {
                        record_materialization_success(&mut report.entities, compiled.changed);
                    }
                }
                Err(error @ elephant_storage::StorageError::ErasureInProgress) => {
                    return Err(error.into());
                }
                Err(error) => push_materialization_failure(
                    &mut report.entities,
                    &mut report.failures,
                    "entity",
                    &record_id,
                    error,
                ),
            }
        }
        Ok(())
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the repository scope through the complete authorized source materialization pass."
    )]
    async fn materialize_sources(
        &self,
        ctx: &ToolContext,
        input: &MaterializeKnowledgeViewsInput,
        cursor: Option<&RecordId>,
        report: &mut MaterializeKnowledgeViewsReport,
    ) -> McpResult<()> {
        let repository = SourceRepository::new(self.storage.clone());
        let mut records = repository
            .list(&ctx.principal.space_id, input.limit_per_kind + 1, cursor)
            .await?;
        let has_more = records.len() > input.limit_per_kind;
        records.truncate(input.limit_per_kind);
        if has_more {
            report.sources.next_cursor = records.last().map(|record| record.base.id.to_string());
        }
        let limits = SourceViewLimits {
            documents: input.view_limit,
            revisions_per_document: input.view_limit,
        };
        let mut authorized = Vec::with_capacity(records.len());
        for record in records {
            report.sources.scanned += 1;
            if ctx
                .authz
                .authorize_record_read(&ctx.principal, &record, "read:source")
                .is_err()
            {
                report.sources.skipped_unauthorized += 1;
                continue;
            }
            authorized.push(record);
        }
        let mut results = stream::iter(authorized)
            .map(|record| {
                let compiler = self.source_compiler.clone();
                let space_id = ctx.principal.space_id.clone();
                async move {
                    let record_id = record.base.id;
                    let result = compiler.compile(&space_id, &record_id, limits, false).await;
                    (record_id, result)
                }
            })
            .buffer_unordered(input.concurrency)
            .collect::<Vec<_>>()
            .await;
        results.sort_by(|(left, _), (right, _)| left.to_string().cmp(&right.to_string()));
        for (record_id, result) in results {
            match result {
                Ok(compiled) => {
                    if let Err(error) =
                        authorize_projection(ctx, &compiled.view, &compiled.revision, "read:source")
                            .await
                    {
                        push_materialization_failure(
                            &mut report.sources,
                            &mut report.failures,
                            "source",
                            &record_id,
                            error,
                        );
                    } else {
                        record_materialization_success(&mut report.sources, compiled.changed);
                    }
                }
                Err(error @ elephant_storage::StorageError::ErasureInProgress) => {
                    return Err(error.into());
                }
                Err(error) => push_materialization_failure(
                    &mut report.sources,
                    &mut report.failures,
                    "source",
                    &record_id,
                    error,
                ),
            }
        }
        Ok(())
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the repository scope through the complete authorized definition materialization pass."
    )]
    async fn materialize_definitions(
        &self,
        ctx: &ToolContext,
        input: &MaterializeKnowledgeViewsInput,
        cursor: Option<&RecordId>,
        report: &mut MaterializeKnowledgeViewsReport,
    ) -> McpResult<()> {
        let repository = KnowledgeViewDefinitionRepository::new(self.storage.clone());
        let mut records = repository
            .list_page(&ctx.principal.space_id, input.limit_per_kind + 1, cursor)
            .await?;
        let has_more = records.len() > input.limit_per_kind;
        records.truncate(input.limit_per_kind);
        if has_more {
            report.definitions.next_cursor =
                records.last().map(|record| record.base.id.to_string());
        }
        let mut authorized = Vec::with_capacity(records.len());
        for record in records {
            report.definitions.scanned += 1;
            if ctx
                .authz
                .authorize_record_read(&ctx.principal, &record, "read:entity")
                .is_err()
            {
                report.definitions.skipped_unauthorized += 1;
                continue;
            }
            authorized.push(record);
        }
        let mut results = stream::iter(authorized)
            .map(|record| {
                let compiler = self.graph_compiler.clone();
                let space_id = ctx.principal.space_id.clone();
                async move {
                    let record_id = record.base.id;
                    let result = compiler.compile(&space_id, &record_id, false).await;
                    (record_id, result)
                }
            })
            .buffer_unordered(input.concurrency)
            .collect::<Vec<_>>()
            .await;
        results.sort_by(|(left, _), (right, _)| left.to_string().cmp(&right.to_string()));
        for (record_id, result) in results {
            match result {
                Ok(compiled) => {
                    if let Err(error) =
                        authorize_projection(ctx, &compiled.view, &compiled.revision, "read:entity")
                            .await
                    {
                        push_materialization_failure(
                            &mut report.definitions,
                            &mut report.failures,
                            "definition",
                            &record_id,
                            error,
                        );
                    } else {
                        record_materialization_success(&mut report.definitions, compiled.changed);
                    }
                }
                Err(error @ elephant_storage::StorageError::ErasureInProgress) => {
                    return Err(error.into());
                }
                Err(error) => push_materialization_failure(
                    &mut report.definitions,
                    &mut report.failures,
                    "definition",
                    &record_id,
                    error,
                ),
            }
        }
        Ok(())
    }
}

#[async_trait]
impl ToolHandler for MaterializeKnowledgeViewsHandler {
    fn name(&self) -> &'static str {
        "materialize_knowledge_views"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Compile bounded, resumable pages of existing Elephant entities, sources, and governed graph definitions into cited derived projections. This does not turn raw entities or sources into wiki articles. The operation reads canonical data and writes only derived views; failures are isolated and reported per root.\n\n",
            "Example:\n",
            r#"{ "include_entities": true, "include_sources": true, "include_definitions": true, "limit_per_kind": 10, "view_limit": 25, "concurrency": 4 }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": [],
            "properties": {
                "include_entities": {"type": "boolean", "default": true},
                "include_sources": {"type": "boolean", "default": true},
                "include_definitions": {"type": "boolean", "default": true},
                "entity_cursor": {"type": "string", "description": "Entity cursor returned by the previous call"},
                "source_cursor": {"type": "string", "description": "Source cursor returned by the previous call"},
                "definition_cursor": {"type": "string", "description": "Definition cursor returned by the previous call"},
                "limit_per_kind": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 100,
                    "default": 10,
                    "description": "Maximum roots of each included kind compiled in this call"
                },
                "view_limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 100,
                    "default": 25,
                    "description": "Maximum related records included in each compiled page section"
                },
                "concurrency": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 8,
                    "default": 4,
                    "description": "Maximum roots compiled concurrently within one record kind"
                }
            }
        })
    }

    fn profile(&self) -> ToolProfile {
        ToolProfile::Maintenance
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: MaterializeKnowledgeViewsInput = serde_json::from_value(params)
            .map_err(|error| McpError::InvalidParameters(error.to_string()))?;
        if ctx.principal.space_id.is_empty() {
            return response_error("Access denied: missing space_id");
        }
        if !input.include_entities && !input.include_sources && !input.include_definitions {
            return Err(McpError::InvalidParameters(
                "at least one record kind must be included".to_string(),
            ));
        }
        if !(1..=100).contains(&input.limit_per_kind) {
            return Err(McpError::InvalidParameters(
                "limit_per_kind must be between 1 and 100".to_string(),
            ));
        }
        if !(1..=100).contains(&input.view_limit) {
            return Err(McpError::InvalidParameters(
                "view_limit must be between 1 and 100".to_string(),
            ));
        }
        if !(1..=8).contains(&input.concurrency) {
            return Err(McpError::InvalidParameters(
                "concurrency must be between 1 and 8".to_string(),
            ));
        }
        let entity_cursor =
            parse_optional_cursor(input.entity_cursor.as_deref(), TableName::Entity)?;
        let source_cursor =
            parse_optional_cursor(input.source_cursor.as_deref(), TableName::Source)?;
        let definition_cursor = parse_optional_cursor(
            input.definition_cursor.as_deref(),
            TableName::KnowledgeViewDefinition,
        )?;
        let mut report = MaterializeKnowledgeViewsReport::default();
        if input.include_entities {
            self.materialize_entities(ctx, &input, entity_cursor.as_ref(), &mut report)
                .await?;
        }
        if input.include_sources {
            self.materialize_sources(ctx, &input, source_cursor.as_ref(), &mut report)
                .await?;
        }
        if input.include_definitions {
            self.materialize_definitions(ctx, &input, definition_cursor.as_ref(), &mut report)
                .await?;
        }
        serde_json::to_value(ToolResponse::success(report))
            .map_err(|error| McpError::ExecutionError(error.to_string()))
    }
}

/// Runs Elephant's LLM writer and verifier for one governed article context.
pub struct CompileWikiArticleHandler {
    #[cfg(feature = "meerkat-extraction")]
    storage: StorageClient,
    definitions: KnowledgeViewDefinitionRepository,
}

impl CompileWikiArticleHandler {
    /// Creates the handler.
    #[must_use]
    #[cfg(feature = "meerkat-extraction")]
    pub fn new(storage: StorageClient) -> Self {
        Self {
            definitions: KnowledgeViewDefinitionRepository::new(storage.clone()),
            storage,
        }
    }

    /// Creates the handler when LLM extraction is unavailable.
    #[must_use]
    #[cfg(not(feature = "meerkat-extraction"))]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            definitions: KnowledgeViewDefinitionRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for CompileWikiArticleHandler {
    fn name(&self) -> &'static str {
        "compile_wiki_article"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Requires an explicit manage:wiki scope (wildcard is insufficient), an explicit space-wide wiki service grant with no subject restriction. Use Elephant's Meerkat-backed LLM editor to write a real semantic wiki article from a governed graph context, then run a separate structured entailment-verification call for every factual statement. The article is stored as draft unless publish=true; publication also requires an explicitly approved context plan and a clean freshness/quality receipt.\n\n",
            "Example:\n",
            r#"{ "definition_id": "knowledge_view_definition:550e8400-e29b-41d4-a716-446655440000", "publish": true }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["definition_id"],
            "properties": {
                "definition_id": {
                    "type": "string",
                    "description": "Governed knowledge_view_definition ID used to build bounded editorial context"
                },
                "publish": {
                    "type": "boolean",
                    "default": false,
                    "description": "Expose the verified Article under /wiki. Requires a published context plan and clean context receipt."
                },
                "writer_model": {
                    "type": "string",
                    "description": "Optional Meerkat model override; defaults to ELEPHANT_WIKI_WRITER_MODEL, PIPELINE_AGENTIC_MODEL, then gpt-6.1-sol"
                },
                "verifier_model": {
                    "type": "string",
                    "description": "Optional independent verifier model override; defaults to ELEPHANT_WIKI_VERIFIER_MODEL or the writer model"
                }
            }
        })
    }

    fn profile(&self) -> ToolProfile {
        ToolProfile::Maintenance
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the editorial compiler scope through the existing wiki compilation and publication handling."
    )]
    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        if let Err(reason) = authorize_wiki_management(ctx) {
            return response_error(reason);
        }
        let params = coerce_params(params, &self.input_schema());
        let input: CompileWikiArticleInput = serde_json::from_value(params)
            .map_err(|error| McpError::InvalidParameters(error.to_string()))?;
        let definition_id = parse_id(&input.definition_id, TableName::KnowledgeViewDefinition)
            .map_err(|error| McpError::InvalidParameters(error.to_string()))?;
        let Some(definition) = self
            .definitions
            .get(&ctx.principal.space_id, &definition_id)
            .await?
        else {
            return response_error(format!("Definition not found: {}", input.definition_id));
        };
        if let Err(reason) =
            ctx.authz
                .authorize_record_write(&ctx.principal, &definition, "write:entity")
        {
            return response_error(format!("Access denied: {reason}"));
        }

        #[cfg(feature = "meerkat-extraction")]
        {
            let mut config = elephant_pipeline::wiki_editorial::WikiEditorialConfig::default();
            if let Some(model) = input.writer_model.filter(|model| !model.trim().is_empty()) {
                config.writer_model = model;
            }
            if let Some(model) = input
                .verifier_model
                .filter(|model| !model.trim().is_empty())
            {
                config.verifier_model = model;
            } else if std::env::var("ELEPHANT_WIKI_VERIFIER_MODEL").is_err() {
                config.verifier_model.clone_from(&config.writer_model);
            }
            let compiler = elephant_pipeline::wiki_editorial::WikiEditorialCompiler::with_meerkat(
                self.storage.clone(),
                &ctx.principal.space_id,
                config,
            )
            .await?;
            return match compiler
                .compile(&ctx.principal.space_id, &definition_id, input.publish)
                .await
            {
                Ok(article) => serde_json::to_value(ToolResponse::success(article))
                    .map_err(|error| McpError::ExecutionError(error.to_string())),
                Err(error) => pipeline_response_error(error),
            };
        }

        #[cfg(not(feature = "meerkat-extraction"))]
        {
            let CompileWikiArticleInput {
                publish,
                writer_model,
                verifier_model,
                ..
            } = input;
            let _ = (publish, writer_model, verifier_model);
            response_error(
                "compile_wiki_article requires Elephant's meerkat-extraction feature".to_string(),
            )
        }
    }
}

/// Curates and writes a private wiki from existing Elephant graph entities.
///
/// Planning, writing, and verification are all LLM calls. Deterministic code
/// only bounds inputs, validates IDs, and enforces policy.
pub struct CreateWikiFromGraphHandler {
    #[cfg(feature = "meerkat-extraction")]
    storage: StorageClient,
    #[cfg(feature = "meerkat-extraction")]
    entities: EntityRepository,
    #[cfg(not(feature = "meerkat-extraction"))]
    _storage: StorageClient,
}

impl CreateWikiFromGraphHandler {
    /// Creates the handler.
    #[must_use]
    #[cfg(feature = "meerkat-extraction")]
    pub fn new(storage: StorageClient) -> Self {
        Self {
            entities: EntityRepository::new(storage.clone()),
            storage,
        }
    }

    /// Creates the handler when LLM extraction is unavailable.
    #[must_use]
    #[cfg(not(feature = "meerkat-extraction"))]
    pub const fn new(storage: StorageClient) -> Self {
        Self { _storage: storage }
    }

    #[cfg(feature = "meerkat-extraction")]
    async fn authorized_candidates(&self, ctx: &ToolContext) -> McpResult<AuthorizedGraphScan> {
        let mut cursor = None;
        let mut candidates = Vec::new();
        let mut entities_scanned = 0_usize;
        let entities_skipped_unauthorized = 0_usize;
        loop {
            let page_limit = 500;
            let page = self
                .entities
                .list(&ctx.principal.space_id, page_limit, cursor.as_ref())
                .await?;
            if page.is_empty() {
                break;
            }
            entities_scanned = entities_scanned.saturating_add(page.len());
            cursor = page.last().map(|entity| entity.base.id.clone());
            let last_page = page.len() < page_limit;
            for entity in page {
                candidates.push(entity);
            }
            if last_page {
                break;
            }
        }
        Ok(AuthorizedGraphScan {
            candidates,
            entities_scanned,
            entities_skipped_unauthorized,
        })
    }

    #[cfg(feature = "meerkat-extraction")]
    async fn reusable_graph_fingerprint(
        &self,
        space_id: &str,
        input: &CreateWikiFromGraphInput,
        editorial_config: &elephant_pipeline::wiki_editorial::WikiEditorialConfig,
    ) -> McpResult<Option<String>> {
        if !input.publish {
            return Ok(None);
        }
        let payload = WorkItemRepository::new(self.storage.clone())
            .find_latest_managed_wiki_work(space_id)
            .await?
            .and_then(|item| {
                serde_json::from_value::<elephant_pipeline::wiki_editorial::ManagedWikiPayload>(
                    item.payload,
                )
                .ok()
            });
        Ok(payload
            .filter(|payload| {
                payload.max_articles == input.max_articles
                    && payload.article_max_hops == input.article_max_hops
                    && payload.article_max_members == input.article_max_members
                    && payload.writer_model == editorial_config.writer_model
                    && payload.verifier_model == editorial_config.verifier_model
            })
            .and_then(|payload| payload.last_graph_fingerprint))
    }

    #[cfg(feature = "meerkat-extraction")]
    fn editorial_config(
        input: &CreateWikiFromGraphInput,
    ) -> elephant_pipeline::wiki_editorial::WikiEditorialConfig {
        let mut editorial_config =
            elephant_pipeline::wiki_editorial::WikiEditorialConfig::default();
        if let Some(model) = input
            .writer_model
            .as_deref()
            .filter(|model| !model.trim().is_empty())
        {
            editorial_config.writer_model = model.to_string();
        }
        if let Some(model) = input
            .verifier_model
            .as_deref()
            .filter(|model| !model.trim().is_empty())
        {
            editorial_config.verifier_model = model.to_string();
        } else if std::env::var("ELEPHANT_WIKI_VERIFIER_MODEL").is_err() {
            editorial_config
                .verifier_model
                .clone_from(&editorial_config.writer_model);
        }
        editorial_config
    }

    #[cfg(feature = "meerkat-extraction")]
    async fn schedule_reconciliation(
        &self,
        space_id: &str,
        grant_id: RecordId,
        input: &CreateWikiFromGraphInput,
        editorial_config: elephant_pipeline::wiki_editorial::WikiEditorialConfig,
        last_graph_fingerprint: Option<String>,
    ) -> McpResult<(Option<String>, Option<chrono::DateTime<chrono::Utc>>)> {
        let run_after = chrono::Utc::now()
            + chrono::Duration::minutes(i64::from(input.refresh_interval_minutes));
        let payload = elephant_pipeline::wiki_editorial::ManagedWikiPayload {
            authority: elephant_pipeline::wiki_editorial::ManagedWikiAuthority::Space {
                space_id: space_id.to_string(),
                grant_id,
            },
            max_articles: input.max_articles,
            article_max_hops: input.article_max_hops,
            article_max_members: input.article_max_members,
            refresh_interval_minutes: input.refresh_interval_minutes,
            writer_model: editorial_config.writer_model,
            verifier_model: editorial_config.verifier_model,
            last_graph_fingerprint,
        };
        let work_item = elephant_pipeline::wiki_editorial::enqueue_managed_wiki_reconciliation(
            &self.storage,
            space_id,
            "mcp.create_wiki_from_graph",
            &payload,
            run_after,
        )
        .await;
        Ok(match work_item {
            Ok(item) => (Some(item.base.id.to_string()), item.run_after),
            Err(error) => {
                let error = McpError::from(error);
                if matches!(
                    error,
                    McpError::StorageError(elephant_storage::StorageError::ErasureInProgress)
                ) {
                    return Err(error);
                }
                if payload.authority.is_active(&self.storage, space_id).await? {
                    return Err(error);
                }
                (None, None)
            }
        })
    }

    #[cfg(feature = "meerkat-extraction")]
    #[expect(
        clippy::large_futures,
        clippy::significant_drop_tightening,
        reason = "Keep the planner scope and original unboxed reconciliation future through managed-wiki handling."
    )]
    async fn execute_full_graph_reconciliation(
        &self,
        ctx: &ToolContext,
        input: CreateWikiFromGraphInput,
    ) -> McpResult<Value> {
        let editorial_config = Self::editorial_config(&input);
        let planner = elephant_pipeline::wiki_editorial::WikiGraphPlanner::with_meerkat(
            self.storage.clone(),
            &ctx.principal.space_id,
            editorial_config.clone(),
        )
        .await?;
        let previous_graph_fingerprint = self
            .reusable_graph_fingerprint(&ctx.principal.space_id, &input, &editorial_config)
            .await?;
        let grant_id = if input.manage {
            Some(
                WorkItemRepository::new(self.storage.clone())
                    .create_managed_wiki_grant(
                        &ctx.principal.space_id,
                        "mcp.create_wiki_from_graph",
                    )
                    .await?,
            )
        } else {
            None
        };
        let scan = self.authorized_candidates(ctx).await?;
        if scan.candidates.is_empty() {
            return response_error(
                "No caller-authorized semantic entities are available for wiki planning",
            );
        }
        let entities_authorized = scan.candidates.len();
        let reconciliation = match planner
            .reconcile_full_graph_if_changed(
                &ctx.principal.space_id,
                scan.candidates,
                elephant_pipeline::wiki_editorial::WikiCreationConfig {
                    max_articles: input.max_articles,
                    article_max_hops: input.article_max_hops,
                    article_max_members: input.article_max_members,
                    publish: input.publish,
                    ..elephant_pipeline::wiki_editorial::WikiCreationConfig::default()
                },
                previous_graph_fingerprint.as_deref(),
            )
            .await
        {
            Ok(result) => result,
            Err(error) => return pipeline_response_error(error),
        };
        let (next_reconciliation_work_item, next_reconciliation_at) =
            if let Some(grant_id) = grant_id {
                self.schedule_reconciliation(
                    &ctx.principal.space_id,
                    grant_id,
                    &input,
                    editorial_config,
                    (reconciliation.failed_articles == 0)
                        .then(|| reconciliation.graph_fingerprint.clone()),
                )
                .await?
            } else {
                (None, None)
            };
        let result = FullGraphWikiResult {
            full_graph_scan: true,
            entities_scanned: scan.entities_scanned,
            entities_authorized,
            entities_skipped_unauthorized: scan.entities_skipped_unauthorized,
            managed: next_reconciliation_work_item.is_some(),
            next_reconciliation_work_item,
            next_reconciliation_at,
            reconciliation,
        };
        serde_json::to_value(ToolResponse::success(result))
            .map_err(|error| McpError::ExecutionError(error.to_string()))
    }
}

#[async_trait]
impl ToolHandler for CreateWikiFromGraphHandler {
    fn name(&self) -> &'static str {
        "create_wiki_from_graph"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Create and manage a real private wiki from the complete Elephant graph under an explicit space-wide manage:wiki service grant. The operation scans entities, relationships, assertions, attributes, events, and truth slots to exhaustion, partitions every eligible entity into bounded graph-neighborhood planner prompts, and asks an LLM to curate semantic article subjects and graph-backed links. A separate organizer LLM pass creates the reader navigation. A writer LLM then writes each English article from bounded canonical facts and source quotations, and a separate verifier must accept every cited statement before publication. Raw entities, source records, and ingest metadata are never copied into the reader wiki. Graph-quality, freshness, contradiction, citation, and ABAC gates fail closed per article. Published creation schedules cached periodic reconciliation plus debounced graph-change reconciliation by default.\n\n",
            "Example:\n",
            r#"{ "publish": true }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "additionalProperties": false,
            "required": [],
            "properties": {
                "max_articles": {
                    "type": "integer",
                    "minimum": 0,
                    "maximum": 2000,
                    "default": 0,
                    "description": "Maximum articles across exhaustive full-graph curation; zero gives every eligible semantic subject an article unless the LLM explicitly consolidates possible identity duplicates"
                },
                "article_max_hops": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 4,
                    "default": 1,
                    "description": "Maximum graph hops included in each article context"
                },
                "article_max_members": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 250,
                    "default": 250,
                    "description": "Maximum governed graph members included in each article context"
                },
                "publish": {
                    "type": "boolean",
                    "default": true,
                    "description": "Publish only articles that pass all evidence and freshness gates"
                },
                "manage": {
                    "type": "boolean",
                    "default": true,
                    "description": "Delegate durable space-wide wiki service authority to recurring reconciliation after initial publication"
                },
                "refresh_interval_minutes": {
                    "type": "integer",
                    "minimum": 5,
                    "maximum": 10080,
                    "default": 360,
                    "description": "Interval between complete managed curation and publication reconciliations"
                },
                "writer_model": {
                    "type": "string",
                    "description": "Optional Meerkat model override for wiki planning and article writing"
                },
                "verifier_model": {
                    "type": "string",
                    "description": "Optional independent verifier model override"
                }
            }
        })
    }

    fn profile(&self) -> ToolProfile {
        ToolProfile::Maintenance
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        if let Err(reason) = authorize_wiki_management(ctx) {
            return response_error(reason);
        }
        let params = coerce_params(params, &self.input_schema());
        let input: CreateWikiFromGraphInput = serde_json::from_value(params)
            .map_err(|error| McpError::InvalidParameters(error.to_string()))?;
        if input.max_articles > 2_000
            || !(1..=4).contains(&input.article_max_hops)
            || !(1..=250).contains(&input.article_max_members)
            || !(5..=10_080).contains(&input.refresh_interval_minutes)
        {
            return Err(McpError::InvalidParameters(
                "max_articles must be 0-2000 where zero is exhaustive automatic curation, article_max_hops 1-4, article_max_members 1-250, and refresh_interval_minutes 5-10080"
                    .to_string(),
            ));
        }
        if input.manage && !input.publish {
            return Err(McpError::InvalidParameters(
                "managed wiki reconciliation requires publish=true".to_string(),
            ));
        }
        if ctx.principal.space_id.is_empty() {
            return response_error("Access denied: missing space_id");
        }

        #[cfg(feature = "meerkat-extraction")]
        return Box::pin(self.execute_full_graph_reconciliation(ctx, input)).await;

        #[cfg(not(feature = "meerkat-extraction"))]
        {
            let CreateWikiFromGraphInput {
                writer_model,
                verifier_model,
                ..
            } = input;
            let _ = (writer_model, verifier_model);
            response_error("create_wiki_from_graph requires Elephant's meerkat-extraction feature")
        }
    }
}

fn parse_optional_cursor(cursor: Option<&str>, table: TableName) -> McpResult<Option<RecordId>> {
    cursor
        .map(|value| {
            parse_id(value, table).map_err(|error| McpError::InvalidParameters(error.to_string()))
        })
        .transpose()
}

fn parse_entity_links(values: &[String]) -> McpResult<Vec<Link<Entity>>> {
    values
        .iter()
        .map(|value| {
            parse_id(value, TableName::Entity)
                .map(Link::new)
                .map_err(|error| McpError::InvalidParameters(error.to_string()))
        })
        .collect()
}

const fn record_materialization_success(report: &mut MaterializationKindReport, changed: bool) {
    report.compiled += 1;
    if changed {
        report.changed += 1;
    } else {
        report.reused += 1;
    }
}

fn push_materialization_failure(
    report: &mut MaterializationKindReport,
    failures: &mut Vec<MaterializationFailure>,
    kind: &'static str,
    record_id: &RecordId,
    error: impl std::fmt::Display,
) {
    report.failed += 1;
    failures.push(MaterializationFailure {
        kind,
        record_id: record_id.to_string(),
        error: error.to_string(),
    });
}

async fn compiled_entity_response(
    ctx: &ToolContext,
    compiled: CompiledEntityView,
) -> McpResult<KnowledgeViewResponse> {
    authorize_projection(ctx, &compiled.view, &compiled.revision, "read:entity").await?;
    Ok(KnowledgeViewResponse {
        view: compiled.view,
        revision: compiled.revision,
        changed: compiled.changed,
        stale_fallback: false,
        warning: None,
    })
}

async fn compiled_source_response(
    ctx: &ToolContext,
    compiled: CompiledSourceView,
) -> McpResult<KnowledgeViewResponse> {
    authorize_projection(ctx, &compiled.view, &compiled.revision, "read:source").await?;
    Ok(KnowledgeViewResponse {
        view: compiled.view,
        revision: compiled.revision,
        changed: compiled.changed,
        stale_fallback: false,
        warning: None,
    })
}

async fn compiled_graph_response(
    ctx: &ToolContext,
    compiled: CompiledGraphView,
) -> McpResult<KnowledgeViewResponse> {
    authorize_projection(ctx, &compiled.view, &compiled.revision, "read:entity").await?;
    Ok(KnowledgeViewResponse {
        view: compiled.view,
        revision: compiled.revision,
        changed: compiled.changed,
        stale_fallback: false,
        warning: None,
    })
}

async fn authorize_projection(
    ctx: &ToolContext,
    view: &KnowledgeView,
    revision: &KnowledgeViewRevision,
    scope: &str,
) -> McpResult<()> {
    if !elephant_knowledge_views::has_complete_subject_contract(&view.base)
        || !elephant_knowledge_views::has_complete_subject_contract(&revision.base)
    {
        return Err(McpError::ExecutionError(
            "Access denied: projection requires recompilation to establish its subject scope"
                .to_string(),
        ));
    }
    if view.kind == elephant_types::records::KnowledgeViewKind::Source {
        ctx.authz
            .authorize_record_read(&ctx.principal, revision, "read:doc:raw")
            .map_err(|reason| McpError::ExecutionError(format!("Access denied: {reason}")))?;
    }
    ctx.authz
        .authorize_record_read(&ctx.principal, view, scope)
        .map_err(|reason| McpError::ExecutionError(format!("Access denied: {reason}")))?;
    ctx.authz
        .authorize_record_read(&ctx.principal, revision, scope)
        .map_err(|reason| McpError::ExecutionError(format!("Access denied: {reason}")))?;
    crate::current_read::require(ctx, revision.clone(), scope).await?;
    Ok(())
}

enum KnowledgeViewRoot {
    Entity(elephant_types::RecordId),
    Source(elephant_types::RecordId),
    Definition(elephant_types::RecordId),
}

fn parse_root(
    entity_id: Option<&str>,
    source_id: Option<&str>,
    definition_id: Option<&str>,
) -> McpResult<KnowledgeViewRoot> {
    match (entity_id, source_id, definition_id) {
        (Some(entity_id), None, None) => parse_id(entity_id, TableName::Entity)
            .map(KnowledgeViewRoot::Entity)
            .map_err(|error| McpError::InvalidParameters(error.to_string())),
        (None, Some(source_id), None) => parse_id(source_id, TableName::Source)
            .map(KnowledgeViewRoot::Source)
            .map_err(|error| McpError::InvalidParameters(error.to_string())),
        (None, None, Some(definition_id)) => {
            parse_id(definition_id, TableName::KnowledgeViewDefinition)
                .map(KnowledgeViewRoot::Definition)
                .map_err(|error| McpError::InvalidParameters(error.to_string()))
        }
        (None, None, None) => Err(McpError::InvalidParameters(
            "exactly one of entity_id, source_id, or definition_id is required".to_string(),
        )),
        _ => Err(McpError::InvalidParameters(
            "entity_id, source_id, and definition_id are mutually exclusive".to_string(),
        )),
    }
}

fn response_success(response: KnowledgeViewResponse) -> McpResult<Value> {
    serde_json::to_value(ToolResponse::success(response))
        .map_err(|error| McpError::ExecutionError(error.to_string()))
}

#[cfg(feature = "meerkat-extraction")]
fn pipeline_response_error(error: elephant_pipeline::PipelineError) -> McpResult<Value> {
    let message = error.to_string();
    match McpError::from(error) {
        error @ McpError::StorageError(elephant_storage::StorageError::ErasureInProgress) => {
            Err(error)
        }
        _ => response_error(message),
    }
}

fn response_error(message: impl Into<String>) -> McpResult<Value> {
    let response: ToolResponse<()> = ToolResponse::error(message);
    serde_json::to_value(response).map_err(|error| McpError::ExecutionError(error.to_string()))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::collections::HashSet;
    use std::sync::Arc;

    use elephant_policy::Principal;
    use elephant_storage::Repository;
    use elephant_types::records::base::BaseFields;
    use elephant_types::records::{Entity, EntityType, Source, SourceType};
    use elephant_types::{RecordId, TableName};

    use crate::authz::AuthzService;

    use super::*;

    fn context(storage: StorageClient) -> ToolContext {
        ToolContext::with_authz(
            storage,
            Arc::new(AuthzService::new_with_skip_auth(true)),
            Principal {
                space_id: "wiki-space".to_string(),
                scopes: HashSet::new(),
                clearances: Vec::new(),
                subject_allowlist: None,
                purpose: HashSet::new(),
            },
        )
    }

    async fn create_test_space(storage: &StorageClient) {
        let config: elephant_types::records::SpaceConfig = serde_json::from_value(json!({
            "id": RecordId::new_v4(TableName::SpaceConfig),
            "space_id": "wiki-space",
            "created_at": chrono::Utc::now(), "updated_at": chrono::Utc::now(),
            "created_by": "test", "provenance": {},
            "security": {"policy_id": "default", "level": 0, "labels": [], "handling": []},
            "subjects": [], "subjects_state": "complete",
            "embedding_provider": "fake", "embedding_model": "test", "embedding_dims": 8
        }))
        .expect("space configuration");
        elephant_storage::SpaceConfigRepository::new(storage.clone())
            .create(config)
            .await
            .expect("create space");
    }

    async fn embedded_storage() -> StorageClient {
        let storage = StorageClient::new_embedded().await.expect("storage");
        create_test_space(&storage).await;
        storage
    }

    async fn create_entity(storage: &StorageClient, entity_type: &str, name: &str) -> RecordId {
        let entity = Entity::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Entity),
                "wiki-space".to_string(),
                "test".to_string(),
            ),
            EntityType::new(entity_type),
            name.to_string(),
        );
        let id = entity.base.id.clone();
        EntityRepository::new(storage.clone())
            .create(entity)
            .await
            .expect("entity");
        id
    }

    fn enforcing_context(storage: StorageClient) -> ToolContext {
        let mut ctx = context(storage);
        ctx.authz = Arc::new(AuthzService::new());
        ctx.principal.scopes =
            HashSet::from(["read:entity".to_string(), "write:entity".to_string()]);
        ctx.principal.clearances = vec![elephant_policy::Clearance {
            policy_id: "default".to_string(),
            max_level: 0,
            labels: HashSet::new(),
            handling_allow: HashSet::new(),
        }];
        ctx
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        clippy::too_many_lines,
        reason = "Keep the complete native reservation-error control and fixture lifetimes through publication assertions."
    )]
    async fn native_wiki_writers_preserve_reservation_errors_without_publication() {
        let storage = StorageClient::new_memory().await.expect("native storage");
        storage
            .query_checked(concat!(
                include_str!("../../../migrations/sql/031_source_publication.surql"),
                "\n",
                include_str!("../../../migrations/sql/046_source_support_index_fence.surql")
            ))
            .await
            .expect("publication schema");
        storage
            .query_checked(include_str!(
                "../../../migrations/sql/039_private_reference_fence.surql"
            ))
            .await
            .expect("native reference guards");
        create_test_space(&storage).await;
        let entity_id = create_entity(&storage, "topic", "Reserved wiki topic").await;
        let source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "wiki-space".into(),
                "test".into(),
            ),
            SourceType::Manual,
            "wiki-reservation-source".into(),
        );
        let source_id = source.base.id.clone();
        SourceRepository::new(storage.clone())
            .create(source)
            .await
            .expect("source");
        let ctx = context(storage.clone());
        let upsert = UpsertKnowledgeViewDefinitionHandler::new(storage.clone());
        let selector = json!({
            "type": "query", "entity_types": ["topic"], "name_contains": null,
            "attributes": [], "scan_limit": 100, "max_members": 10
        });
        let created = upsert
            .execute(
                &ctx,
                json!({"title": "Existing definition", "selector": selector}),
            )
            .await
            .expect("unreserved definition write");
        assert_eq!(created["ok"], true);
        let definition_id = created["data"]["id"].as_str().expect("definition id");
        let materialize = MaterializeKnowledgeViewsHandler::new(storage.clone());
        let first = materialize
            .execute(&ctx, json!({"concurrency": 1}))
            .await
            .expect("unreserved materialization");
        assert_eq!(first["data"]["failures"], json!([]));
        for kind in ["entities", "sources", "definitions"] {
            assert_eq!(first["data"][kind]["compiled"], 1, "{kind}");
        }
        KnowledgeViewRepository::new(storage.clone())
            .mark_dirty_for_dependencies(
                "wiki-space",
                &[
                    entity_id.to_string(),
                    source_id.to_string(),
                    definition_id.to_owned(),
                ],
                "reservation regression requires actual publication",
            )
            .await
            .expect("invalidate cached projections");

        // Hold the real database admission boundary. The handlers retain their
        // ordinary query context, without the private original erasure permit.
        storage
            .query_checked(
                "UPDATE type::thing('source_erasure_guard', 'wiki-space') SET erasure_reservation='wiki-adapter-reservation';",
            )
            .await
            .expect("reserve existing native guard");
        let snapshot_sql = r"
RETURN {
    definitions: (SELECT * FROM knowledge_view_definition WHERE space_id='wiki-space' ORDER BY id),
    views: (SELECT * FROM knowledge_view WHERE space_id='wiki-space' ORDER BY id),
    revisions: (SELECT * FROM knowledge_view_revision WHERE space_id='wiki-space' ORDER BY id),
    dependencies: (SELECT * FROM knowledge_view_dependency WHERE space_id='wiki-space' ORDER BY id),
    events: (SELECT * FROM system_event WHERE space_id='wiki-space' ORDER BY id),
    epoch: (SELECT * FROM ONLY type::thing('canonical_epoch', 'wiki-space'))
};";
        let before: surrealdb::Value = storage
            .query_checked(snapshot_sql)
            .await
            .expect("before snapshot")
            .take(0)
            .expect("before snapshot value");
        let update = upsert
            .execute(
                &ctx,
                json!({"definition_id": definition_id, "title": "Must not publish", "selector": selector}),
            )
            .await;
        assert!(matches!(
            update,
            Err(McpError::StorageError(
                elephant_storage::StorageError::ErasureInProgress
            ))
        ));
        let rebuild = RebuildKnowledgeViewHandler::new(storage.clone());
        for params in [
            json!({"entity_id": entity_id.to_string()}),
            json!({"source_id": source_id.to_string()}),
            json!({"definition_id": definition_id}),
        ] {
            assert!(matches!(
                rebuild.execute(&ctx, params).await,
                Err(McpError::StorageError(
                    elephant_storage::StorageError::ErasureInProgress
                ))
            ));
        }
        let get = GetKnowledgeViewHandler::new(storage.clone());
        for params in [
            json!({"entity_id": entity_id.to_string(), "freshness": "prefer_fresh"}),
            json!({"source_id": source_id.to_string(), "freshness": "prefer_fresh"}),
            json!({"definition_id": definition_id, "freshness": "prefer_fresh"}),
        ] {
            assert!(
                matches!(
                    get.execute(&ctx, params).await,
                    Err(McpError::StorageError(
                        elephant_storage::StorageError::ErasureInProgress
                    ))
                ),
                "refresh reservations must not become successful stale responses"
            );
        }
        for kind in ["entities", "sources", "definitions"] {
            let mut params = json!({
                "include_entities": false, "include_sources": false,
                "include_definitions": false, "concurrency": 1
            });
            params[format!("include_{kind}")] = json!(true);
            assert!(
                matches!(
                    materialize.execute(&ctx, params).await,
                    Err(McpError::StorageError(
                        elephant_storage::StorageError::ErasureInProgress
                    ))
                ),
                "{kind} must preserve the typed reservation"
            );
        }
        let after: surrealdb::Value = storage
            .query_checked(snapshot_sql)
            .await
            .expect("after snapshot")
            .take(0)
            .expect("after snapshot value");
        assert_eq!(
            after, before,
            "refused writers must leave publication unchanged"
        );
    }

    #[cfg(feature = "meerkat-extraction")]
    #[test]
    fn wiki_pipeline_response_preserves_typed_reservation_and_validation_text() {
        use elephant_pipeline::PipelineError;
        use elephant_storage::StorageError;
        for error in [
            PipelineError::StorageError(StorageError::ErasureInProgress),
            PipelineError::OutboxError(StorageError::ErasureInProgress.into()),
        ] {
            assert!(matches!(
                pipeline_response_error(error),
                Err(McpError::StorageError(StorageError::ErasureInProgress))
            ));
        }
        let validation = PipelineError::InvalidInput("keep the exact validation reason".into());
        let expected = validation.to_string();
        let response = pipeline_response_error(validation).expect("ordinary response");
        assert_eq!(response["ok"], false);
        assert_eq!(response["error"], expected);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn manual_definition_edit_preserves_organizer_subject_scope() {
        let storage = embedded_storage().await;
        let root = create_entity(&storage, "person", "Allowed root").await;
        let other = create_entity(&storage, "person", "Secret project").await;
        let mut ctx = enforcing_context(storage.clone());
        let upsert = UpsertKnowledgeViewDefinitionHandler::new(storage.clone());
        let selector = json!({"type": "topic", "topic": root.to_string(), "relation_types": [], "max_hops": 1, "max_members": 10});
        let created = upsert
            .execute(&ctx, json!({"title": "Allowed root", "selector": selector}))
            .await
            .expect("create");
        assert_eq!(created["ok"], true);
        let mut definition: KnowledgeViewDefinition =
            serde_json::from_value(created["data"].clone()).expect("definition");
        let id = definition.base.id.clone();
        definition.base.created_by = "elephant.wiki_planner".to_string();
        definition.base.provenance.extractor = Some(elephant_types::records::base::Extractor {
            name: Some("elephant.wiki_planner".to_string()),
            version: Some("10".to_string()),
            model: None,
        });
        definition.base.subjects.push(Link::new(other));
        definition.navigation = Some(serde_json::from_value(json!({"wiki_title": "Secret project", "section_id": "private", "section_title": "Private project", "section_order": 0, "article_order": 0})).expect("navigation"));
        KnowledgeViewDefinitionRepository::new(storage.clone())
            .upsert(definition)
            .await
            .expect("save organizer");
        let updated = upsert.execute(&ctx, json!({"definition_id": id.to_string(), "title": "Renamed root", "selector": selector})).await.expect("update");
        assert_eq!(updated["ok"], true);
        assert_eq!(
            updated["data"]["subjects"]
                .as_array()
                .expect("subjects")
                .len(),
            2
        );
        ctx.principal.subject_allowlist = Some(HashSet::from([root]));
        let read = GetKnowledgeViewDefinitionHandler::new(storage)
            .execute(&ctx, json!({"definition_id": id.to_string()}))
            .await
            .expect("read");
        assert_eq!(read["ok"], false);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn source_projection_requires_raw_document_scope_for_fresh_and_cached_reads() {
        use elephant_storage::{DocRepository, DocRevisionRepository};
        use elephant_types::records::doc_revision::RendererMetadata;
        use elephant_types::records::{Doc, DocRevision, RevisionStatus};
        let storage = embedded_storage().await;
        let source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "wiki-space".to_string(),
                "test".to_string(),
            ),
            SourceType::Manual,
            "sha256:test".to_string(),
        );
        let source_id = source.base.id.clone();
        SourceRepository::new(storage.clone())
            .create(source)
            .await
            .expect("source");
        let doc = Doc {
            base: BaseFields::new(
                RecordId::new_v4(TableName::Doc),
                "wiki-space".to_string(),
                "test".to_string(),
            ),
            title: "Secret".to_string(),
            text: "Private document text".to_string(),
            sidecar_text: None,
            source: Some(Link::new(source_id.clone())),
            media_attachments: Vec::new(),
            rendition_key: "test".to_string(),
            metadata: json!({}),
        };
        let doc_id = doc.base.id.clone();
        DocRepository::new(storage.clone())
            .create(doc)
            .await
            .expect("doc");
        DocRevisionRepository::new(storage.clone())
            .create(DocRevision {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::DocRevision),
                    "wiki-space".to_string(),
                    "test".to_string(),
                ),
                doc: Link::new(doc_id),
                text: "Private document text".to_string(),
                text_hash:
                    "sha256:2181faf3c1fa0d09a3fafce572dfbcd5c4387677c880ad4cfe1d5c79140b9cbf"
                        .to_string(),
                renderer: RendererMetadata {
                    name: "test".to_string(),
                    version: "1".to_string(),
                    config_hash: None,
                },
                manifest: None,
                supersedes: None,
                status: RevisionStatus::Active,
                rendered_at: chrono::Utc::now(),
            })
            .await
            .expect("revision");
        let mut ctx = enforcing_context(storage.clone());
        ctx.principal.scopes = HashSet::from(["read:source".to_string()]);
        let handler = GetKnowledgeViewHandler::new(storage);
        for freshness in ["required", "allow_stale", "prefer_fresh"] {
            let response = handler
                .execute(
                    &ctx,
                    json!({"source_id": source_id.to_string(), "freshness": freshness}),
                )
                .await;
            assert!(
                response.is_err(),
                "raw scope must be required for {freshness}"
            );
        }
        ctx.principal.scopes.insert("read:doc:raw".to_string());
        let response = handler
            .execute(
                &ctx,
                json!({"source_id": source_id.to_string(), "freshness": "allow_stale"}),
            )
            .await
            .expect("authorized");
        assert_eq!(response["ok"], true);
        assert!(response.to_string().contains("Private document text"));
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn entity_projection_enforces_all_contributing_subjects_on_fresh_and_cached_reads() {
        use elephant_storage::RelRepository;
        use elephant_types::records::{ClaimStatus, Rel};
        let storage = embedded_storage().await;
        let root = create_entity(&storage, "person", "Allowed root").await;
        let counterpart = create_entity(&storage, "person", "Restricted counterpart").await;
        let relation = Rel {
            base: BaseFields::new(
                RecordId::new_v4(TableName::Rel),
                "wiki-space".to_string(),
                "test".to_string(),
            ),
            subject: Link::new(root.clone()),
            object: Link::new(counterpart.clone()),
            rel_type: "knows".to_string(),
            status: ClaimStatus::Approved,
            confidence: 1.0,
            validity_window: None,
            evidence: vec![],
            temporal_audit: None,
        };
        RelRepository::new(storage.clone())
            .create(relation.clone())
            .await
            .expect("relation");
        let mut ctx = enforcing_context(storage.clone());
        ctx.principal.subject_allowlist = Some(HashSet::from([root.clone()]));
        assert!(ctx
            .authz
            .authorize_record_read(&ctx.principal, &relation, "read:entity")
            .is_err());
        let handler = GetKnowledgeViewHandler::new(storage);
        for freshness in ["required", "allow_stale", "prefer_fresh"] {
            let result = handler
                .execute(
                    &ctx,
                    json!({"entity_id": root.to_string(), "freshness": freshness}),
                )
                .await;
            assert!(
                matches!(result, Err(McpError::ExecutionError(ref reason)) if reason.contains("not in allowlist"))
            );
        }
        ctx.principal.subject_allowlist = Some(HashSet::from([root.clone(), counterpart]));
        let result = handler
            .execute(&ctx, json!({"entity_id": root.to_string()}))
            .await
            .expect("authorized");
        assert_eq!(result["ok"], true);
        assert!(result["data"]["revision"]["rendered_markdown"]
            .as_str()
            .expect("markdown")
            .contains("Restricted counterpart"));
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn wiki_service_requires_explicit_unrestricted_management_grant() {
        let storage = embedded_storage().await;
        let mut ctx = enforcing_context(storage.clone());
        let handler = CreateWikiFromGraphHandler::new(storage);
        let denied = handler.execute(&ctx, json!({})).await.expect("denial");
        assert_eq!(denied["ok"], false);
        assert!(denied.to_string().contains("manage:wiki"));
        ctx.principal.scopes.insert("*".to_string());
        assert!(authorize_wiki_management(&ctx).is_err());
        ctx.principal.scopes.insert("manage:wiki".to_string());
        assert!(authorize_wiki_management(&ctx).is_ok());
        ctx.principal.subject_allowlist =
            Some(HashSet::from([RecordId::new_v4(TableName::Entity)]));
        let denied = handler.execute(&ctx, json!({})).await.expect("denial");
        assert_eq!(denied["ok"], false);
        assert!(denied.to_string().contains("unrestricted subject scope"));
        ctx.principal.scopes.insert("*".to_string());
        assert!(authorize_wiki_management(&ctx).is_err());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn create_wiki_from_graph_exposes_llm_planning_contract() {
        let storage = embedded_storage().await;
        let handler = CreateWikiFromGraphHandler::new(storage);
        assert_eq!(handler.name(), "create_wiki_from_graph");
        let schema = handler.input_schema();
        assert!(schema["properties"].get("scan_limit").is_none());
        assert_eq!(schema["properties"]["max_articles"]["maximum"], 2000);
        assert_eq!(schema["properties"]["max_articles"]["default"], 0);
        assert_eq!(schema["properties"]["article_max_hops"]["default"], 1);
        assert_eq!(schema["properties"]["article_max_members"]["default"], 250);
        assert_eq!(schema["properties"]["publish"]["default"], true);
        assert_eq!(schema["properties"]["manage"]["default"], true);
        assert_eq!(
            schema["properties"]["refresh_interval_minutes"]["default"],
            360
        );
        assert!(handler.description().contains("writer LLM"));
        assert!(handler.description().contains("separate verifier"));
        assert!(handler.description().contains("Raw entities"));
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn get_knowledge_view_routes_entity_reuse_and_source_projection() {
        let storage = embedded_storage().await;
        let entity = Entity::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Entity),
                "wiki-space".to_string(),
                "test".to_string(),
            ),
            EntityType::new("person"),
            "Ada Lovelace".to_string(),
        );
        let entity_id = entity.base.id.clone();
        EntityRepository::new(storage.clone())
            .create(entity)
            .await
            .expect("entity");
        let handler = GetKnowledgeViewHandler::new(storage.clone());
        let ctx = context(storage);

        let first = handler
            .execute(
                &ctx,
                json!({"entity_id": entity_id.to_string(), "freshness": "required"}),
            )
            .await
            .expect("first response");
        assert_eq!(first["ok"], true);
        assert_eq!(first["data"]["changed"], true);
        assert_eq!(
            first["data"]["revision"]["validation"]["statements_supported"],
            true
        );
        assert_eq!(
            first["data"]["revision"]["validation"]["prose_is_structured"],
            true
        );

        let reused = handler
            .execute(
                &ctx,
                json!({"entity_id": entity_id.to_string(), "freshness": "allow_stale"}),
            )
            .await
            .expect("reused response");
        assert_eq!(reused["ok"], true);
        assert_eq!(reused["data"]["changed"], false);

        let source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "wiki-space".to_string(),
                "test".to_string(),
            ),
            SourceType::Web,
            "sha256:source".to_string(),
        );
        let source_id = source.base.id.clone();
        SourceRepository::new(ctx.storage.clone())
            .create(source)
            .await
            .expect("source");
        let source_view = handler
            .execute(
                &ctx,
                json!({"source_id": source_id.to_string(), "freshness": "required"}),
            )
            .await
            .expect("source response");
        assert_eq!(source_view["ok"], true);
        assert_eq!(source_view["data"]["view"]["kind"], "source");
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn governed_topic_definition_round_trips_and_compiles() {
        let storage = embedded_storage().await;
        let topic_id = create_entity(&storage, "topic", "Knowledge graphs").await;
        let related_id = create_entity(&storage, "concept", "Graph databases").await;
        let ctx = context(storage.clone());
        let upsert = UpsertKnowledgeViewDefinitionHandler::new(storage.clone());
        let created = upsert
            .execute(
                &ctx,
                json!({
                    "title": "Knowledge graphs",
                    "related_articles": [related_id.to_string()],
                    "selector": {
                        "type": "topic",
                        "topic": topic_id.to_string(),
                        "relation_types": [],
                        "max_hops": 1,
                        "max_members": 10
                    }
                }),
            )
            .await
            .expect("create definition");
        assert_eq!(created["ok"], true);
        let definition_id = created["data"]["id"]
            .as_str()
            .expect("definition id")
            .to_string();

        let updated = upsert
            .execute(
                &ctx,
                json!({
                    "definition_id": definition_id,
                    "title": "Knowledge graphs",
                    "selector": {
                        "type": "topic",
                        "topic": topic_id.to_string(),
                        "relation_types": [],
                        "max_hops": 1,
                        "max_members": 10
                    }
                }),
            )
            .await
            .expect("update definition");
        assert_eq!(updated["ok"], true);
        let stored_id = parse_id(
            updated["data"]["id"].as_str().expect("updated id"),
            TableName::KnowledgeViewDefinition,
        )
        .expect("stored definition id");
        let stored = KnowledgeViewDefinitionRepository::new(storage.clone())
            .get("wiki-space", &stored_id)
            .await
            .expect("read definition")
            .expect("stored definition");
        assert_eq!(stored.related_articles.len(), 1);
        assert_eq!(stored.related_articles[0].id(), &related_id);

        let definition = GetKnowledgeViewDefinitionHandler::new(storage.clone())
            .execute(&ctx, json!({"definition_id": definition_id}))
            .await
            .expect("get definition");
        assert_eq!(definition["data"]["selector"]["type"], "topic");

        let compiled = GetKnowledgeViewHandler::new(storage)
            .execute(
                &ctx,
                json!({
                    "definition_id": definition["data"]["id"],
                    "freshness": "required"
                }),
            )
            .await
            .expect("compile definition");
        assert_eq!(compiled["ok"], true);
        assert_eq!(compiled["data"]["view"]["kind"], "topic");
        assert_eq!(
            compiled["data"]["revision"]["validation"]["prose_is_structured"],
            true
        );
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn materialize_existing_data_compiles_and_then_reuses_all_root_kinds() {
        let storage = embedded_storage().await;
        let entity = Entity::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Entity),
                "wiki-space".to_string(),
                "test".to_string(),
            ),
            EntityType::new("topic"),
            "Existing graph topic".to_string(),
        );
        EntityRepository::new(storage.clone())
            .create(entity)
            .await
            .expect("entity");
        let source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "wiki-space".to_string(),
                "test".to_string(),
            ),
            SourceType::Manual,
            "sha256:existing-source".to_string(),
        );
        SourceRepository::new(storage.clone())
            .create(source)
            .await
            .expect("source");
        let definition = KnowledgeViewDefinition {
            base: BaseFields::new(
                RecordId::new_v4(TableName::KnowledgeViewDefinition),
                "wiki-space".to_string(),
                "test".to_string(),
            ),
            title: "All existing topics".to_string(),
            publication_status: KnowledgeViewPublicationStatus::Draft,
            related_articles: Vec::new(),
            navigation: None,
            selector: KnowledgeViewSelector::Query {
                entity_types: vec!["topic".to_string()],
                name_contains: None,
                attributes: Vec::new(),
                scan_limit: 100,
                max_members: 10,
            },
        };
        KnowledgeViewDefinitionRepository::new(storage.clone())
            .upsert(definition)
            .await
            .expect("definition");
        let ctx = context(storage.clone());
        let handler = MaterializeKnowledgeViewsHandler::new(storage.clone());

        let first = handler.execute(&ctx, json!({})).await.expect("first run");
        assert_eq!(first["ok"], true);
        assert_eq!(first["data"]["entities"]["compiled"], 1);
        assert_eq!(first["data"]["sources"]["compiled"], 1);
        assert_eq!(first["data"]["definitions"]["compiled"], 1);
        assert_eq!(first["data"]["failures"], json!([]));
        assert_eq!(
            KnowledgeViewRepository::new(storage.clone())
                .list_views("wiki-space", 10)
                .await
                .expect("views")
                .len(),
            3
        );

        let second = handler.execute(&ctx, json!({})).await.expect("second run");
        assert_eq!(second["data"]["entities"]["reused"], 1);
        assert_eq!(second["data"]["sources"]["reused"], 1);
        assert_eq!(second["data"]["definitions"]["reused"], 1);
    }
}
