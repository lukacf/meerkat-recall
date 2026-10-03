//! MCP tool handlers.
//!
//! This module implements the core MCP tools as defined in the spec.

pub mod assertions;
pub mod docs;
pub mod entities;
pub mod events;
pub mod identity;
pub mod knowledge_views;
pub mod predicates;
pub mod relationships;
pub mod reprocess;
pub mod search;
pub mod source_watches;
pub mod sources;
pub mod spaces;
pub mod system;
pub mod truth_slots;
pub mod work_items;

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Map, Value};

use crate::authz::AuthzService;
use crate::error::{McpError, McpResult};
use crate::profile::{is_agent_tool, is_maintenance_tool, ToolProfile};

use elephant_policy::Principal as AbacPrincipal;
use elephant_types::ids::{RecordId, TableName};
use elephant_types::records::BaseFields;

pub use assertions::*;
pub use docs::*;
pub use entities::*;
pub use events::*;
pub use identity::*;
pub use knowledge_views::*;
pub use predicates::*;
pub use relationships::*;
pub use reprocess::*;
pub use search::*;
pub use source_watches::*;
pub use sources::*;
pub use spaces::*;
pub use system::*;
pub use truth_slots::*;
pub use work_items::*;

/// Trait for MCP tool handlers.
#[async_trait]
pub trait ToolHandler: Send + Sync {
    /// Tool name.
    fn name(&self) -> &'static str;

    /// Tool description.
    fn description(&self) -> &'static str;

    /// Input schema as JSON Schema.
    fn input_schema(&self) -> Value;

    /// Execute the tool with the given parameters.
    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value>;

    /// Returns the tool profile for access control.
    ///
    /// Per spec section 8.3, extraction agents may only use ABAC-filtered read tools.
    /// Override this method to specify the correct profile for each handler.
    ///
    /// Default implementation returns `Full` for backwards compatibility, but new
    /// handlers should explicitly specify their profile.
    fn profile(&self) -> ToolProfile {
        // Default based on tool name for backwards compatibility
        if is_agent_tool(self.name()) {
            ToolProfile::Agent
        } else if is_maintenance_tool(self.name()) {
            ToolProfile::Maintenance
        } else {
            ToolProfile::Full
        }
    }
}

/// Context for tool execution containing shared dependencies.
#[derive(Clone)]
pub struct ToolContext {
    /// Storage client for database access.
    pub storage: elephant_storage::StorageClient,
    /// ABAC authorization service.
    pub authz: Arc<AuthzService>,
    /// ABAC principal for the current request.
    pub principal: AbacPrincipal,
}

impl ToolContext {
    /// Creates a new tool context.
    #[must_use]
    pub fn new(storage: elephant_storage::StorageClient, principal: AbacPrincipal) -> Self {
        Self {
            storage,
            authz: Arc::new(AuthzService::new()),
            principal,
        }
    }

    /// Creates a new tool context with a custom authorization service.
    #[must_use]
    pub const fn with_authz(
        storage: elephant_storage::StorageClient,
        authz: Arc<AuthzService>,
        principal: AbacPrincipal,
    ) -> Self {
        Self {
            storage,
            authz,
            principal,
        }
    }
}

pub(crate) fn merge_base_fields(
    obj: &mut Map<String, Value>,
    table: TableName,
    ctx: &ToolContext,
) -> McpResult<()> {
    if let Some(Value::Object(base)) = obj.remove("base") {
        for (key, value) in base {
            obj.entry(key).or_insert(value);
        }
    }

    let mut base = BaseFields::new(
        RecordId::new_v4(table),
        ctx.principal.space_id.clone(),
        "mcp_tool".to_string(),
    );
    if base.security.policy_id.is_empty() {
        base.security.policy_id = "default".to_string();
    }

    let base_value =
        serde_json::to_value(base).map_err(|e| McpError::ExecutionError(e.to_string()))?;
    if let Some(base_obj) = base_value.as_object() {
        for (key, value) in base_obj {
            obj.entry(key.clone()).or_insert(value.clone());
        }
    }

    Ok(())
}

/// Dispatcher for MCP tool execution.
pub struct ToolDispatcher {
    handlers: std::collections::HashMap<String, Arc<dyn ToolHandler>>,
    context: ToolContext,
}

impl ToolDispatcher {
    /// Creates a new dispatcher with the given context.
    #[must_use]
    pub fn new(context: ToolContext) -> Self {
        Self {
            handlers: std::collections::HashMap::new(),
            context,
        }
    }

    /// Registers a tool handler.
    pub fn register<H: ToolHandler + 'static>(&mut self, handler: H) {
        self.handlers
            .insert(handler.name().to_string(), Arc::new(handler));
    }

    /// Executes a tool by name.
    ///
    /// # Errors
    ///
    /// Returns an error if the tool is not found or if execution fails.
    pub async fn execute(&self, name: &str, params: Value) -> McpResult<Value> {
        let handler = self
            .handlers
            .get(name)
            .ok_or_else(|| McpError::ToolNotFound(name.to_string()))?;
        let mut result = handler.execute(&self.context, params).await?;
        strip_embedding_vectors(&mut result);
        Ok(result)
    }

    /// Lists all registered tools.
    #[must_use]
    pub fn list_tools(&self) -> Vec<&str> {
        self.handlers
            .keys()
            .map(std::string::String::as_str)
            .collect()
    }

    /// Gets tool metadata by name.
    #[must_use]
    pub fn get_tool_meta(&self, name: &str) -> Option<crate::registry::ToolMeta> {
        self.handlers.get(name).map(|h| crate::registry::ToolMeta {
            name: h.name().to_string(),
            description: h.description().to_string(),
            owner: "core".to_string(),
            input_schema: h.input_schema(),
        })
    }

    /// Gets all tool metadata.
    #[must_use]
    pub fn all_tool_meta(&self) -> Vec<crate::registry::ToolMeta> {
        self.handlers
            .values()
            .map(|h| crate::registry::ToolMeta {
                name: h.name().to_string(),
                description: h.description().to_string(),
                owner: "core".to_string(),
                input_schema: h.input_schema(),
            })
            .collect()
    }

    /// Returns a reference to the tool context.
    #[must_use]
    pub const fn context(&self) -> &ToolContext {
        &self.context
    }

    /// Returns a list of tool names available to extraction agents.
    ///
    /// Per spec section 8.3, extraction agents may only use ABAC-filtered read tools.
    /// This method returns only tools with `ToolProfile::Agent`.
    #[must_use]
    pub fn agent_tools(&self) -> Vec<&str> {
        self.handlers
            .iter()
            .filter(|(_, h)| h.profile() == ToolProfile::Agent)
            .map(|(name, _)| name.as_str())
            .collect()
    }

    /// Returns a list of tool names for pipeline workers.
    ///
    /// This method returns only tools with `ToolProfile::Pipeline`.
    #[must_use]
    pub fn pipeline_tools(&self) -> Vec<&str> {
        self.handlers
            .iter()
            .filter(|(_, h)| h.profile() == ToolProfile::Pipeline)
            .map(|(name, _)| name.as_str())
            .collect()
    }
}

fn register_capabilities_handler(
    dispatcher: &mut ToolDispatcher,
    storage: elephant_storage::StorageClient,
    advertise_snapshot_tools: bool,
) {
    let mut registered_tools: Vec<String> = dispatcher
        .list_tools()
        .into_iter()
        .map(std::string::ToString::to_string)
        .collect();
    registered_tools.push("get_capabilities".to_string());

    dispatcher.register(system::GetCapabilitiesHandler::new(
        storage,
        advertise_snapshot_tools,
        registered_tools,
    ));
}

/// Creates a dispatcher with all core tools registered.
#[must_use]
pub fn create_dispatcher(
    storage: elephant_storage::StorageClient,
    principal: AbacPrincipal,
) -> ToolDispatcher {
    create_dispatcher_with_context(ToolContext::new(storage, principal))
}

/// Creates a dispatcher with all core tools registered using the provided context.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn create_dispatcher_with_context(context: ToolContext) -> ToolDispatcher {
    let storage = context.storage.clone();
    let snapshots_supported = storage.is_embedded();
    let mut dispatcher = ToolDispatcher::new(context);

    // Register entity tools
    dispatcher.register(entities::SearchEntitiesHandler::new(storage.clone()));
    dispatcher.register(entities::GetEntityHandler::new(storage.clone()));
    dispatcher.register(entities::GetEntityWithRelationsHandler::new(
        storage.clone(),
    ));
    dispatcher.register(entities::UpdateEntityHandler::new(storage.clone()));
    dispatcher.register(entities::DeleteEntityHandler::new(storage.clone()));
    dispatcher.register(entities::RestoreEntityHandler::new(storage.clone()));
    dispatcher.register(entities::BatchEntitiesHandler::new(storage.clone()));
    dispatcher.register(entities::CreateEntitySimpleHandler::new(storage.clone()));

    // Register relationship tools
    dispatcher.register(relationships::RelateEntitiesHandler::new(storage.clone()));
    dispatcher.register(relationships::UpsertRelationshipHandler::new(
        storage.clone(),
    ));
    dispatcher.register(relationships::UpdateRelationshipHandler::new(
        storage.clone(),
    ));
    dispatcher.register(relationships::DeleteRelationshipHandler::new(
        storage.clone(),
    ));
    dispatcher.register(relationships::RestoreRelationshipHandler::new(
        storage.clone(),
    ));
    dispatcher.register(relationships::BatchRelationshipsHandler::new(
        storage.clone(),
    ));

    // Register document tools
    dispatcher.register(docs::SearchDocsHandler::new(storage.clone()));
    dispatcher.register(docs::IngestDocHandler::new(storage.clone()));
    dispatcher.register(docs::IngestDocSimpleHandler::new(storage.clone()));
    dispatcher.register(docs::IngestFileHandler::new(storage.clone()));
    dispatcher.register(docs::IngestDirectoryHandler);
    dispatcher.register(docs::IngestUrlHandler::new(storage.clone()));
    dispatcher.register(docs::ExtractDocHandler::new(storage.clone()));
    dispatcher.register(docs::GetDocPipelineStatusHandler::new(storage.clone()));
    dispatcher.register(docs::ListDocCollectionsHandler::new(storage.clone()));
    dispatcher.register(docs::GetDocHandler::new(storage.clone()));
    dispatcher.register(docs::ListDocChunksHandler::new(storage.clone()));
    dispatcher.register(docs::GetDocChunkHandler::new(storage.clone()));
    dispatcher.register(docs::DeleteDocHandler::new(storage.clone()));
    dispatcher.register(docs::RestoreDocHandler::new(storage.clone()));
    dispatcher.register(docs::BatchIngestDocsHandler::new(storage.clone()));
    dispatcher.register(docs::RestampAttributionHandler::new(storage.clone()));

    // Register identity tools
    dispatcher.register(identity::GetEntityContextBundleHandler::new(
        storage.clone(),
    ));
    dispatcher.register(knowledge_views::GetKnowledgeViewHandler::new(
        storage.clone(),
    ));
    dispatcher.register(knowledge_views::GetKnowledgeViewDefinitionHandler::new(
        storage.clone(),
    ));
    dispatcher.register(knowledge_views::UpsertKnowledgeViewDefinitionHandler::new(
        storage.clone(),
    ));
    dispatcher.register(knowledge_views::RebuildKnowledgeViewHandler::new(
        storage.clone(),
    ));
    dispatcher.register(knowledge_views::MaterializeKnowledgeViewsHandler::new(
        storage.clone(),
    ));
    dispatcher.register(knowledge_views::CompileWikiArticleHandler::new(
        storage.clone(),
    ));
    dispatcher.register(knowledge_views::CreateWikiFromGraphHandler::new(
        storage.clone(),
    ));
    dispatcher.register(identity::ProposeIdentityCandidateHandler::new(
        storage.clone(),
    ));
    dispatcher.register(identity::ListIdentityCandidatesHandler::new(
        storage.clone(),
    ));
    dispatcher.register(identity::MergeEntitiesHandler::new(storage.clone()));

    // Register source tools
    dispatcher.register(sources::SearchSourcesHandler::new(storage.clone()));
    dispatcher.register(sources::GetSourceHandler::new(storage.clone()));
    dispatcher.register(sources::CreateSourceHandler::new(storage.clone()));
    dispatcher.register(sources::CreateSourceSimpleHandler::new(storage.clone()));
    dispatcher.register(sources::IngestSourceHandler::new(storage.clone()));
    dispatcher.register(sources::IngestSourceSimpleHandler::new(storage.clone()));
    dispatcher.register(sources::BatchIngestSourcesHandler::new(storage.clone()));
    dispatcher.register(sources::UpdateSourceHandler::new(storage.clone()));
    dispatcher.register(sources::DeleteSourceHandler::new(storage.clone()));
    dispatcher.register(sources::RestoreSourceHandler::new(storage.clone()));
    dispatcher.register(source_watches::GetSourceWatchHandler::new(storage.clone()));
    dispatcher.register(source_watches::ConfigureSourceWatchHandler::new(
        storage.clone(),
    ));
    dispatcher.register(source_watches::CheckSourceWatchHandler::new(
        storage.clone(),
    ));

    // Register domain event tools (timeline)
    dispatcher.register(events::AddTimelineEventHandler::new(storage.clone()));
    dispatcher.register(events::SearchEventsHandler::new(storage.clone()));
    dispatcher.register(events::GetEventHandler::new(storage.clone()));
    dispatcher.register(events::ListRecentEventsHandler::new(storage.clone()));
    dispatcher.register(events::ListUpcomingEventsHandler::new(storage.clone()));

    // Register unified search tool
    dispatcher.register(search::UnifiedSearchHandler::new(storage.clone()));

    // Register system tools
    dispatcher.register(system::RevertAuditHandler::new(storage.clone()));
    dispatcher.register(system::StreamEventsHandler::new(storage.clone()));
    dispatcher.register(system::GetToolSchemaHandler::new());
    dispatcher.register(system::GetJobStatusHandler::new(storage.clone()));
    dispatcher.register(system::GetHealthHandler::new(storage.clone()));
    if snapshots_supported {
        dispatcher.register(system::ExportSnapshotHandler::new(storage.clone()));
        dispatcher.register(system::ImportSnapshotHandler::new(storage.clone()));
    }

    // Register space management tools
    dispatcher.register(spaces::CreateSpaceHandler::new(storage.clone()));
    dispatcher.register(spaces::ListSpacesHandler::new(storage.clone()));
    dispatcher.register(spaces::GetSpaceHandler::new(storage.clone()));
    dispatcher.register(spaces::UpdateSpaceHandler::new(storage.clone()));

    // Register assertion tools
    dispatcher.register(assertions::ListAssertionsHandler::new(storage.clone()));
    dispatcher.register(assertions::GetAssertionHandler::new(storage.clone()));
    dispatcher.register(assertions::SearchAssertionsHandler::new(storage.clone()));

    // Register predicate tools
    dispatcher.register(predicates::ListPredicatesHandler::new(storage.clone()));
    dispatcher.register(predicates::GetPredicateHandler::new(storage.clone()));
    dispatcher.register(predicates::RegisterPredicateHandler::new(storage.clone()));
    dispatcher.register(predicates::ProposePredAliasHandler::new(storage.clone()));
    dispatcher.register(predicates::ValidatePredicateProposalHandler::new(
        storage.clone(),
    ));
    dispatcher.register(predicates::ApprovePredicateProposalHandler::new(
        storage.clone(),
    ));
    dispatcher.register(reprocess::CreateReprocessJobHandler::new(storage.clone()));
    dispatcher.register(reprocess::GetReprocessJobProgressHandler::new(
        storage.clone(),
    ));

    // Register truth slot tools
    dispatcher.register(truth_slots::ListTruthSlotsHandler::new(storage.clone()));
    dispatcher.register(truth_slots::GetTruthSlotHistoryHandler::new(
        storage.clone(),
    ));

    // Register work item tools
    dispatcher.register(work_items::ListWorkItemsHandler::new(storage.clone()));
    dispatcher.register(work_items::ListPendingWorkItemsHandler::new(
        storage.clone(),
    ));
    dispatcher.register(work_items::ClaimWorkItemHandler::new(storage.clone()));
    dispatcher.register(work_items::CompleteWorkItemHandler::new(storage.clone()));
    dispatcher.register(work_items::RetryWorkItemHandler::new(storage.clone()));
    dispatcher.register(work_items::StopManagedWikiHandler::new(storage.clone()));

    register_capabilities_handler(&mut dispatcher, storage, true);

    dispatcher
}

/// Creates a dispatcher with end-user tools (`DEFAULT_TOOLS` profile).
///
/// For external LLMs that need to ingest, search, navigate, and inspect provenance.
#[must_use]
pub fn create_default_dispatcher(
    storage: elephant_storage::StorageClient,
    principal: AbacPrincipal,
) -> ToolDispatcher {
    create_default_dispatcher_with_context(ToolContext::new(storage, principal))
}

/// Creates a default dispatcher with the provided context.
#[must_use]
pub fn create_default_dispatcher_with_context(context: ToolContext) -> ToolDispatcher {
    let storage = context.storage.clone();
    let mut dispatcher = ToolDispatcher::new(context);

    // Ingest tools (file ingestion handled by elephant-local-helper via stdio)
    dispatcher.register(docs::IngestDocHandler::new(storage.clone()));
    dispatcher.register(docs::IngestDocSimpleHandler::new(storage.clone()));
    dispatcher.register(sources::IngestSourceHandler::new(storage.clone()));
    dispatcher.register(sources::IngestSourceSimpleHandler::new(storage.clone()));
    dispatcher.register(sources::BatchIngestSourcesHandler::new(storage.clone()));

    // Discover tools
    dispatcher.register(search::UnifiedSearchHandler::new(storage.clone()));

    // Navigate tools
    dispatcher.register(entities::GetEntityHandler::new(storage.clone()));
    dispatcher.register(identity::GetEntityContextBundleHandler::new(
        storage.clone(),
    ));
    dispatcher.register(knowledge_views::GetKnowledgeViewHandler::new(
        storage.clone(),
    ));
    dispatcher.register(knowledge_views::GetKnowledgeViewDefinitionHandler::new(
        storage.clone(),
    ));
    dispatcher.register(source_watches::GetSourceWatchHandler::new(storage.clone()));
    dispatcher.register(entities::GetEntityWithRelationsHandler::new(
        storage.clone(),
    ));
    dispatcher.register(entities::SearchEntitiesHandler::new(storage.clone()));
    dispatcher.register(events::AddTimelineEventHandler::new(storage.clone()));
    dispatcher.register(events::SearchEventsHandler::new(storage.clone()));
    dispatcher.register(events::GetEventHandler::new(storage.clone()));
    dispatcher.register(events::ListRecentEventsHandler::new(storage.clone()));
    dispatcher.register(events::ListUpcomingEventsHandler::new(storage.clone()));

    // Inspect tools
    dispatcher.register(system::GetProvenanceHandler::new(storage.clone()));

    // Browse tools
    dispatcher.register(spaces::ListSpacesHandler::new(storage.clone()));
    dispatcher.register(spaces::GetSpaceHandler::new(storage.clone()));
    dispatcher.register(predicates::ListPredicatesHandler::new(storage.clone()));

    // System tools
    dispatcher.register(system::GetHealthHandler::new(storage));

    dispatcher
}

/// Creates a dispatcher with only Agent-profile tools for extraction agents.
///
/// Per spec section 8.3, extraction agents may only use ABAC-filtered read tools.
/// This dispatcher contains approximately 15-20 read-only tools.
///
/// Tools included:
/// - `search_entities`, `get_entity`, `get_entity_context_bundle`
/// - `list_predicates`, `get_predicate`
/// - `search_docs`, `get_doc`, `list_doc_chunks`, `get_doc_chunk`
/// - `get_source`
/// - `list_assertions`, `get_assertion`, `search_assertions`
/// - `list_truth_slots`
/// - `search_events`, `get_event`, `list_recent_events`, `list_upcoming_events`
/// - `search_all`
/// - `get_health`, `get_capabilities`
#[must_use]
pub fn create_agent_dispatcher(
    storage: elephant_storage::StorageClient,
    principal: AbacPrincipal,
) -> ToolDispatcher {
    create_agent_dispatcher_with_context(ToolContext::new(storage, principal))
}

/// Creates an agent dispatcher with the provided context.
#[must_use]
pub fn create_agent_dispatcher_with_context(context: ToolContext) -> ToolDispatcher {
    let storage = context.storage.clone();
    let mut dispatcher = ToolDispatcher::new(context);

    // Register Agent-profile entity tools (read-only)
    dispatcher.register(entities::SearchEntitiesHandler::new(storage.clone()));
    dispatcher.register(entities::GetEntityHandler::new(storage.clone()));

    // Register get_entity_context_bundle from identity
    dispatcher.register(identity::GetEntityContextBundleHandler::new(
        storage.clone(),
    ));
    dispatcher.register(knowledge_views::GetKnowledgeViewHandler::new(
        storage.clone(),
    ));
    dispatcher.register(knowledge_views::GetKnowledgeViewDefinitionHandler::new(
        storage.clone(),
    ));
    dispatcher.register(source_watches::GetSourceWatchHandler::new(storage.clone()));

    // Register Agent-profile predicate tools (read-only)
    dispatcher.register(predicates::ListPredicatesHandler::new(storage.clone()));
    dispatcher.register(predicates::GetPredicateHandler::new(storage.clone()));

    // Register Agent-profile document tools (read-only)
    dispatcher.register(docs::SearchDocsHandler::new(storage.clone()));
    dispatcher.register(docs::GetDocHandler::new(storage.clone()));
    dispatcher.register(docs::ListDocChunksHandler::new(storage.clone()));
    dispatcher.register(docs::GetDocChunkHandler::new(storage.clone()));

    // Register Agent-profile source tools (read-only)
    dispatcher.register(sources::GetSourceHandler::new(storage.clone()));

    // Register Agent-profile assertion tools (read-only)
    dispatcher.register(assertions::ListAssertionsHandler::new(storage.clone()));
    dispatcher.register(assertions::GetAssertionHandler::new(storage.clone()));
    dispatcher.register(assertions::SearchAssertionsHandler::new(storage.clone()));

    // Register Agent-profile truth slot tools (read-only)
    dispatcher.register(truth_slots::ListTruthSlotsHandler::new(storage.clone()));

    // Register Agent-profile event tools (read-only)
    dispatcher.register(events::SearchEventsHandler::new(storage.clone()));
    dispatcher.register(events::GetEventHandler::new(storage.clone()));
    dispatcher.register(events::ListRecentEventsHandler::new(storage.clone()));
    dispatcher.register(events::ListUpcomingEventsHandler::new(storage.clone()));

    // Register Agent-profile unified search tool
    dispatcher.register(search::UnifiedSearchHandler::new(storage.clone()));

    // Register Agent-profile system tools (read-only)
    dispatcher.register(system::GetHealthHandler::new(storage.clone()));
    register_capabilities_handler(&mut dispatcher, storage, false);

    dispatcher
}

/// Creates a dispatcher with Maintenance-profile tools.
///
/// Includes all Agent read tools plus identity resolution, predicate management,
/// entity update tools, and explicit manual maintenance helpers.
///
/// Tools included (in addition to all Agent tools):
/// - `propose_identity_candidate`, `list_identity_candidates`, `merge_entities`
/// - `register_predicate`, `propose_predicate_alias`, `approve_predicate_proposal`
/// - `update_entity`
/// - `extract_doc`
#[must_use]
pub fn create_maintenance_dispatcher(
    storage: elephant_storage::StorageClient,
    principal: AbacPrincipal,
) -> ToolDispatcher {
    create_maintenance_dispatcher_with_context(ToolContext::new(storage, principal))
}

/// Creates a maintenance dispatcher with the provided context.
#[must_use]
pub fn create_maintenance_dispatcher_with_context(context: ToolContext) -> ToolDispatcher {
    let storage = context.storage.clone();
    let snapshots_supported = storage.is_embedded();
    let mut dispatcher = ToolDispatcher::new(context);

    // --- Agent-profile tools (read-only) ---

    // Entity tools (read-only)
    dispatcher.register(entities::SearchEntitiesHandler::new(storage.clone()));
    dispatcher.register(entities::GetEntityHandler::new(storage.clone()));

    // Identity context bundle (read-only)
    dispatcher.register(identity::GetEntityContextBundleHandler::new(
        storage.clone(),
    ));
    dispatcher.register(knowledge_views::GetKnowledgeViewHandler::new(
        storage.clone(),
    ));
    dispatcher.register(knowledge_views::GetKnowledgeViewDefinitionHandler::new(
        storage.clone(),
    ));
    dispatcher.register(source_watches::GetSourceWatchHandler::new(storage.clone()));

    // Predicate tools (read-only)
    dispatcher.register(predicates::ListPredicatesHandler::new(storage.clone()));
    dispatcher.register(predicates::GetPredicateHandler::new(storage.clone()));

    // Document tools (read-only)
    dispatcher.register(docs::SearchDocsHandler::new(storage.clone()));
    dispatcher.register(docs::GetDocHandler::new(storage.clone()));
    dispatcher.register(docs::ListDocChunksHandler::new(storage.clone()));
    dispatcher.register(docs::GetDocChunkHandler::new(storage.clone()));

    // Source tools (read-only)
    dispatcher.register(sources::GetSourceHandler::new(storage.clone()));

    // Assertion tools (read-only)
    dispatcher.register(assertions::ListAssertionsHandler::new(storage.clone()));
    dispatcher.register(assertions::GetAssertionHandler::new(storage.clone()));
    dispatcher.register(assertions::SearchAssertionsHandler::new(storage.clone()));

    // Truth slot tools (read-only)
    dispatcher.register(truth_slots::ListTruthSlotsHandler::new(storage.clone()));

    // Event tools (read-only)
    dispatcher.register(events::SearchEventsHandler::new(storage.clone()));
    dispatcher.register(events::GetEventHandler::new(storage.clone()));
    dispatcher.register(events::ListRecentEventsHandler::new(storage.clone()));
    dispatcher.register(events::ListUpcomingEventsHandler::new(storage.clone()));

    // Event write tools
    dispatcher.register(events::AddTimelineEventHandler::new(storage.clone()));

    // Unified search
    dispatcher.register(search::UnifiedSearchHandler::new(storage.clone()));

    // System tools (read-only)
    dispatcher.register(system::GetHealthHandler::new(storage.clone()));

    // --- Maintenance-only tools (write) ---

    if snapshots_supported {
        dispatcher.register(system::ExportSnapshotHandler::new(storage.clone()));
        dispatcher.register(system::ImportSnapshotHandler::new(storage.clone()));
    }

    // Identity resolution tools
    dispatcher.register(identity::ProposeIdentityCandidateHandler::new(
        storage.clone(),
    ));
    dispatcher.register(identity::ListIdentityCandidatesHandler::new(
        storage.clone(),
    ));
    dispatcher.register(identity::MergeEntitiesHandler::new(storage.clone()));

    // Predicate management tools
    dispatcher.register(predicates::RegisterPredicateHandler::new(storage.clone()));
    dispatcher.register(predicates::ProposePredAliasHandler::new(storage.clone()));
    dispatcher.register(predicates::ApprovePredicateProposalHandler::new(
        storage.clone(),
    ));

    // Entity write tools
    dispatcher.register(entities::UpdateEntityHandler::new(storage.clone()));

    // Manual maintenance helpers
    dispatcher.register(work_items::StopManagedWikiHandler::new(storage.clone()));
    dispatcher.register(docs::ExtractDocHandler::new(storage.clone()));
    dispatcher.register(knowledge_views::RebuildKnowledgeViewHandler::new(
        storage.clone(),
    ));
    dispatcher.register(knowledge_views::MaterializeKnowledgeViewsHandler::new(
        storage.clone(),
    ));
    dispatcher.register(knowledge_views::UpsertKnowledgeViewDefinitionHandler::new(
        storage.clone(),
    ));
    dispatcher.register(knowledge_views::CompileWikiArticleHandler::new(
        storage.clone(),
    ));
    dispatcher.register(knowledge_views::CreateWikiFromGraphHandler::new(
        storage.clone(),
    ));
    dispatcher.register(source_watches::ConfigureSourceWatchHandler::new(
        storage.clone(),
    ));
    dispatcher.register(source_watches::CheckSourceWatchHandler::new(
        storage.clone(),
    ));

    register_capabilities_handler(&mut dispatcher, storage, true);

    dispatcher
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::mixed_attributes_style,
        clippy::similar_names,
        clippy::type_complexity,
        clippy::float_cmp,
        clippy::const_is_empty,
        clippy::expect_used,
        clippy::panic,
        unused_imports,
        dead_code,
        clippy::too_many_lines,
        clippy::cognitive_complexity
    )]
    use super::*;
    use crate::profile::AGENT_TOOLS;
    use crate::tools::ToolResponse;
    use elephant_policy::{Clearance, Principal};
    use std::collections::HashSet;

    #[test]
    fn test_tool_context_creation() {
        // Context creation test would require a storage client
    }

    #[test]
    fn test_tool_profile_default_derives_from_name() {
        // Test that the default profile() implementation uses is_agent_tool
        // This is a logical test - actual handler tests would require storage
        for tool_name in AGENT_TOOLS {
            assert!(
                is_agent_tool(tool_name),
                "Tool {tool_name} should be an agent tool"
            );
        }
    }

    #[test]
    fn test_agent_tools_excluded() {
        // Verify work_item tools are not in AGENT_TOOLS
        assert!(!is_agent_tool("claim_work_item"));
        assert!(!is_agent_tool("complete_work_item"));
        assert!(!is_agent_tool("list_work_items"));
        assert!(!is_agent_tool("list_pending_work_items"));
        assert!(!is_agent_tool("retry_work_item"));

        // Verify write tools are not in AGENT_TOOLS
        assert!(!is_agent_tool("update_entity"));
        assert!(!is_agent_tool("delete_entity"));
        assert!(!is_agent_tool("ingest_doc"));
        assert!(!is_agent_tool("create_source"));
    }

    #[tokio::test]
    async fn test_dispatcher_registers_extract_doc() {
        let storage = elephant_storage::StorageClient::new_memory()
            .await
            .expect("storage");
        let principal = Principal {
            space_id: "default".to_string(),
            scopes: HashSet::new(),
            clearances: Vec::new(),
            subject_allowlist: None,
            purpose: HashSet::new(),
        };
        let dispatcher = create_dispatcher(storage, principal);
        assert!(dispatcher.list_tools().contains(&"extract_doc"));
        assert!(dispatcher.list_tools().contains(&"get_doc_pipeline_status"));
        assert!(dispatcher.list_tools().contains(&"ingest_file"));
        assert!(dispatcher.list_tools().contains(&"ingest_directory"));
        assert!(dispatcher.list_tools().contains(&"ingest_url"));
    }

    #[tokio::test]
    async fn test_snapshot_tools_hidden_on_surreal_backend() {
        let storage = elephant_storage::StorageClient::new_memory()
            .await
            .expect("storage");
        let principal = Principal {
            space_id: "default".to_string(),
            scopes: HashSet::new(),
            clearances: Vec::new(),
            subject_allowlist: None,
            purpose: HashSet::new(),
        };

        let full = create_dispatcher(storage.clone(), principal.clone());
        let maintenance = create_maintenance_dispatcher(storage, principal);

        assert!(!full.list_tools().contains(&"export_snapshot"));
        assert!(!full.list_tools().contains(&"import_snapshot"));
        assert!(!maintenance.list_tools().contains(&"export_snapshot"));
        assert!(!maintenance.list_tools().contains(&"import_snapshot"));
    }

    #[tokio::test]
    async fn test_snapshot_tools_exposed_on_embedded_backend() {
        let storage = elephant_storage::StorageClient::new_embedded()
            .await
            .expect("storage");
        let principal = Principal {
            space_id: "default".to_string(),
            scopes: HashSet::new(),
            clearances: Vec::new(),
            subject_allowlist: None,
            purpose: HashSet::new(),
        };

        let full = create_dispatcher(storage.clone(), principal.clone());
        let maintenance = create_maintenance_dispatcher(storage, principal);

        assert!(full.list_tools().contains(&"export_snapshot"));
        assert!(full.list_tools().contains(&"import_snapshot"));
        assert!(maintenance.list_tools().contains(&"export_snapshot"));
        assert!(maintenance.list_tools().contains(&"import_snapshot"));
    }

    #[tokio::test]
    async fn test_agent_dispatcher_capabilities_only_advertise_agent_tools() {
        let storage = elephant_storage::StorageClient::new_memory()
            .await
            .expect("storage");
        let principal = Principal {
            space_id: "default".to_string(),
            scopes: HashSet::from(["*".to_string()]),
            clearances: vec![Clearance {
                policy_id: "default".to_string(),
                max_level: 0,
                labels: HashSet::new(),
                handling_allow: HashSet::new(),
            }],
            subject_allowlist: None,
            purpose: HashSet::new(),
        };

        let dispatcher = create_agent_dispatcher(storage, principal);
        let response = dispatcher
            .execute("get_capabilities", serde_json::json!({}))
            .await
            .expect("capabilities");
        let decoded: ToolResponse<CapabilitiesOutput> =
            serde_json::from_value(response).expect("decode capabilities");
        assert!(decoded.ok, "unexpected capabilities response: {decoded:?}");

        let output = decoded.data.expect("capabilities output");
        assert!(output
            .tools
            .iter()
            .all(|tool| AGENT_TOOLS.contains(&tool.as_str())));
        assert!(!output.tools.iter().any(|tool| tool == "update_entity"));
        assert!(!output.tools.iter().any(|tool| tool == "export_snapshot"));
        assert!(!output.tools.iter().any(|tool| tool == "import_snapshot"));
    }

    fn validate_object_required(schema: &serde_json::Value, path: &str, errors: &mut Vec<String>) {
        use serde_json::Value;

        let Value::Object(map) = schema else {
            return;
        };

        if map.get("type").and_then(Value::as_str) == Some("object") {
            match map.get("required") {
                Some(Value::Array(_)) => {}
                Some(other) => errors.push(format!(
                    "{path}: schema.type=object but schema.required is not an array (got {other:?})"
                )),
                None => errors.push(format!(
                    "{path}: schema.type=object but schema.required is missing (expected [] or [..])"
                )),
            }
        }

        if let Some(Value::Object(props)) = map.get("properties") {
            for (key, value) in props {
                validate_object_required(value, &format!("{path}.properties.{key}"), errors);
            }
        }

        if let Some(items) = map.get("items") {
            validate_object_required(items, &format!("{path}.items"), errors);
        }

        if let Some(Value::Array(variants)) = map.get("oneOf") {
            for (idx, variant) in variants.iter().enumerate() {
                validate_object_required(variant, &format!("{path}.oneOf[{idx}]"), errors);
            }
        }
        if let Some(Value::Array(variants)) = map.get("anyOf") {
            for (idx, variant) in variants.iter().enumerate() {
                validate_object_required(variant, &format!("{path}.anyOf[{idx}]"), errors);
            }
        }
        if let Some(Value::Array(variants)) = map.get("allOf") {
            for (idx, variant) in variants.iter().enumerate() {
                validate_object_required(variant, &format!("{path}.allOf[{idx}]"), errors);
            }
        }

        if let Some(additional) = map.get("additionalProperties") {
            validate_object_required(additional, &format!("{path}.additionalProperties"), errors);
        }
    }

    #[tokio::test]
    async fn test_schemas_have_required_fields() {
        let storage = elephant_storage::StorageClient::new_memory()
            .await
            .expect("storage");
        let principal = Principal {
            space_id: "default".to_string(),
            scopes: HashSet::new(),
            clearances: Vec::new(),
            subject_allowlist: None,
            purpose: HashSet::new(),
        };
        let dispatcher = create_dispatcher(storage, principal);

        let mut errors = Vec::new();
        for meta in dispatcher.all_tool_meta() {
            validate_object_required(
                &meta.input_schema,
                &format!("tool.{}", meta.name),
                &mut errors,
            );
        }

        assert!(
            errors.is_empty(),
            "Found object schemas missing explicit required arrays:\n{}",
            errors.join("\n")
        );
    }

    #[tokio::test]
    async fn test_tool_descriptions_have_no_uuid_placeholders() {
        let storage = elephant_storage::StorageClient::new_memory()
            .await
            .expect("storage");
        let principal = Principal {
            space_id: "default".to_string(),
            scopes: HashSet::new(),
            clearances: Vec::new(),
            subject_allowlist: None,
            purpose: HashSet::new(),
        };
        let dispatcher = create_dispatcher(storage, principal);

        let mut offenders = Vec::new();
        for meta in dispatcher.all_tool_meta() {
            let description_lower = meta.description.to_ascii_lowercase();
            if description_lower.contains("<uuid>") {
                offenders.push(meta.name.clone());
            }
        }

        assert!(
            offenders.is_empty(),
            "Tool descriptions must not include placeholder UUIDs like <uuid>. Offenders:\n{}",
            offenders.join("\n")
        );
    }

    #[tokio::test]
    async fn test_tool_descriptions_include_examples() {
        let storage = elephant_storage::StorageClient::new_memory()
            .await
            .expect("storage");
        let principal = Principal {
            space_id: "default".to_string(),
            scopes: HashSet::new(),
            clearances: Vec::new(),
            subject_allowlist: None,
            purpose: HashSet::new(),
        };
        let dispatcher = create_dispatcher(storage, principal);

        let mut offenders = Vec::new();
        for meta in dispatcher.all_tool_meta() {
            let example_start = meta.description.find("Example:\n");
            let Some(example_start) = example_start else {
                offenders.push(meta.name.clone());
                continue;
            };
            if !meta.description[example_start..].contains('{') {
                offenders.push(meta.name.clone());
            }
        }

        assert!(
            offenders.is_empty(),
            "Tool descriptions must include an Example section with a JSON payload. Offenders:\n{}",
            offenders.join("\n")
        );
    }

    #[tokio::test]
    async fn test_key_payload_schemas_require_core_fields() {
        let storage = elephant_storage::StorageClient::new_memory()
            .await
            .expect("storage");
        let principal = Principal {
            space_id: "default".to_string(),
            scopes: HashSet::new(),
            clearances: Vec::new(),
            subject_allowlist: None,
            purpose: HashSet::new(),
        };
        let dispatcher = create_dispatcher(storage, principal);

        let meta_by_name = dispatcher
            .all_tool_meta()
            .into_iter()
            .map(|meta| (meta.name.clone(), meta))
            .collect::<std::collections::HashMap<_, _>>();

        let ingest_doc = meta_by_name
            .get("ingest_doc")
            .expect("ingest_doc tool meta");
        let doc_required = ingest_doc
            .input_schema
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|props| props.get("doc"))
            .and_then(|doc| doc.get("required"))
            .and_then(Value::as_array)
            .expect("ingest_doc.properties.doc.required");
        let doc_required = doc_required
            .iter()
            .filter_map(Value::as_str)
            .collect::<std::collections::HashSet<_>>();
        let doc_any_of = ingest_doc
            .input_schema
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|props| props.get("doc"))
            .and_then(|doc| doc.get("anyOf"))
            .and_then(Value::as_array)
            .expect("ingest_doc.properties.doc.anyOf");
        let doc_supports_text_or_media = doc_any_of.iter().any(|variant| {
            variant
                .get("required")
                .and_then(Value::as_array)
                .is_some_and(|required| required.iter().any(|field| field == "text"))
        }) && doc_any_of.iter().any(|variant| {
            variant
                .get("required")
                .and_then(Value::as_array)
                .is_some_and(|required| required.iter().any(|field| field == "media_attachments"))
        });
        assert!(
            doc_required.contains("title") && doc_supports_text_or_media,
            "ingest_doc doc schema must require title and allow text or media_attachments"
        );

        let batch_ingest_docs = meta_by_name
            .get("batch_ingest_docs")
            .expect("batch_ingest_docs tool meta");
        let item_required = batch_ingest_docs
            .input_schema
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|props| props.get("docs"))
            .and_then(|docs| docs.get("items"))
            .and_then(|item| item.get("required"))
            .and_then(Value::as_array)
            .expect("batch_ingest_docs.properties.docs.items.required");
        let item_required = item_required
            .iter()
            .filter_map(Value::as_str)
            .collect::<std::collections::HashSet<_>>();
        let item_any_of = batch_ingest_docs
            .input_schema
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|props| props.get("docs"))
            .and_then(|docs| docs.get("items"))
            .and_then(|item| item.get("anyOf"))
            .and_then(Value::as_array)
            .expect("batch_ingest_docs.properties.docs.items.anyOf");
        let item_supports_text_or_media = item_any_of.iter().any(|variant| {
            variant
                .get("required")
                .and_then(Value::as_array)
                .is_some_and(|required| required.iter().any(|field| field == "text"))
        }) && item_any_of.iter().any(|variant| {
            variant
                .get("required")
                .and_then(Value::as_array)
                .is_some_and(|required| required.iter().any(|field| field == "media_attachments"))
        });
        assert!(
            item_required.contains("title") && item_supports_text_or_media,
            "batch_ingest_docs docs.items schema must require title and allow text or media_attachments"
        );

        let batch_entities = meta_by_name
            .get("batch_entities")
            .expect("batch_entities tool meta");
        let entity_required = batch_entities
            .input_schema
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|props| props.get("items"))
            .and_then(|items| items.get("items"))
            .and_then(|item| item.get("required"))
            .and_then(Value::as_array)
            .expect("batch_entities.properties.items.items.required");
        let entity_required = entity_required
            .iter()
            .filter_map(Value::as_str)
            .collect::<std::collections::HashSet<_>>();
        assert!(
            entity_required.contains("type") && entity_required.contains("display_name"),
            "batch_entities items.items.required must include type and display_name"
        );
    }
}

/// Drops raw embedding vectors from a tool result.
///
/// Entities carry their name embedding (3072 floats, about 62 KB as JSON)
/// in `metadata`, so five entities made a `search_all` answer of roughly
/// 280 KB that agents had to read through. Vectors are never meaningful to a
/// tool caller; the model name and text hash beside them are kept.
fn strip_embedding_vectors(value: &mut Value) {
    match value {
        Value::Object(map) => {
            map.retain(|key, item| !(key.ends_with("embedding") && is_vector(item)));
            for item in map.values_mut() {
                strip_embedding_vectors(item);
            }
        }
        Value::Array(items) => {
            for item in items {
                strip_embedding_vectors(item);
            }
        }
        _ => {}
    }
}

fn is_vector(value: &Value) -> bool {
    matches!(value, Value::Array(items) if items.len() > 16 && items.iter().all(Value::is_number))
}

#[cfg(test)]
mod strip_tests {
    use serde_json::json;

    use super::strip_embedding_vectors;

    #[test]
    fn embedding_vectors_never_reach_tool_callers() {
        let vector: Vec<f64> = (0..3072).map(|i| f64::from(i) * 0.001).collect();
        let mut result = json!({
            "ok": true,
            "data": {"entities": {"items": [{
                "display_name": "Ada",
                "metadata": {
                    "name_embedding": vector,
                    "name_embedding_model": "text-embedding-3-large",
                    "tags": [1, 2, 3]
                }
            }]}}
        });
        strip_embedding_vectors(&mut result);
        let metadata = &result["data"]["entities"]["items"][0]["metadata"];
        assert!(metadata.get("name_embedding").is_none());
        assert_eq!(metadata["name_embedding_model"], "text-embedding-3-large");
        assert_eq!(metadata["tags"], json!([1, 2, 3]));
    }
}
