//! MCP tool profiles for access control.
//!
//! Profiles restrict which MCP tools are available to different types of callers:
//! - `Full` - All tools (operators, admin)
//! - `Agent` - Read-only tools for extraction agents (~17 tools)
//! - `Maintenance` - Agent read tools plus identity/predicate/entity write tools (~24 tools)
//! - `Pipeline` - Write tools for internal pipeline workers

/// Tool profile for MCP access control.
///
/// Per spec section 8.3, extractors (agentic LLM workers) may only use ABAC-filtered
/// read tools. This enum categorizes tools to enforce that restriction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ToolProfile {
    /// All tools available (operators, admin).
    #[default]
    Full,
    /// Read-only tools for extraction agents.
    ///
    /// Includes: `search_entities`, `get_entity`, `get_entity_context_bundle`,
    /// `list_predicates`, `get_predicate`, `search_docs`, `search_all`,
    /// `get_doc`, `list_doc_chunks`, `get_doc_chunk`, `get_source`, `list_assertions`,
    /// `get_assertion`, `search_assertions`, `list_truth_slots`,
    /// `search_events`, `get_event`, `list_recent_events`, `list_upcoming_events`,
    /// `get_health`, `get_capabilities`
    Agent,
    /// Agent read tools plus identity resolution, predicate management, and entity updates.
    ///
    /// Includes all `Agent` tools plus: `propose_identity_candidate`,
    /// `list_identity_candidates`, `merge_entities`, `register_predicate`,
    /// `propose_predicate_alias`, `approve_predicate_proposal`, `update_entity`
    Maintenance,
    /// Write tools for internal pipeline workers.
    ///
    /// Includes: `claim_work_item`, `complete_work_item`, and other work queue tools
    Pipeline,
}

impl ToolProfile {
    /// Returns true if this profile includes read-only Agent tools.
    #[must_use]
    pub const fn includes_agent_tools(&self) -> bool {
        matches!(self, Self::Full | Self::Agent | Self::Maintenance)
    }

    /// Returns true if this profile includes maintenance tools (agent read + identity/predicate/entity writes).
    #[must_use]
    pub const fn includes_maintenance_tools(&self) -> bool {
        matches!(self, Self::Full | Self::Maintenance)
    }

    /// Returns true if this profile includes pipeline worker tools.
    #[must_use]
    pub const fn includes_pipeline_tools(&self) -> bool {
        matches!(self, Self::Full | Self::Pipeline)
    }

    /// Returns true if this is the full unrestricted profile.
    #[must_use]
    pub const fn is_full(&self) -> bool {
        matches!(self, Self::Full)
    }
}

/// List of tool names available to extraction agents.
/// Per spec section 8.3, these are ABAC-filtered read-only tools.
pub const AGENT_TOOLS: &[&str] = &[
    // Entity tools (read-only)
    "search_entities",
    "get_entity",
    "get_entity_context_bundle",
    "get_knowledge_view",
    "get_knowledge_view_definition",
    "get_source_watch",
    // Predicate tools (read-only)
    "list_predicates",
    "get_predicate",
    // Document tools (read-only)
    "search_docs",
    "get_doc",
    "list_doc_chunks",
    "get_doc_chunk",
    // Source tools (read-only)
    "get_source",
    // Assertion tools (read-only)
    "list_assertions",
    "get_assertion",
    "search_assertions",
    // Truth slot tools (read-only)
    "list_truth_slots",
    // Event tools (read-only)
    "search_events",
    "get_event",
    "list_recent_events",
    "list_upcoming_events",
    // Unified search
    "search_all",
    // System tools (read-only)
    "get_health",
    "get_capabilities",
];

/// List of tool names for pipeline workers.
/// These are work queue management tools.
pub const PIPELINE_TOOLS: &[&str] = &[
    "claim_work_item",
    "complete_work_item",
    "list_work_items",
    "list_pending_work_items",
    "retry_work_item",
];

/// List of tool names available to the maintenance profile.
///
/// Includes all Agent read tools plus identity resolution, predicate management,
/// entity update tools, and explicit manual maintenance helpers.
pub const MAINTENANCE_TOOLS: &[&str] = &[
    // All Agent tools (read-only)
    "search_entities",
    "get_entity",
    "get_entity_context_bundle",
    "get_knowledge_view",
    "get_knowledge_view_definition",
    "get_source_watch",
    "list_predicates",
    "get_predicate",
    "search_docs",
    "get_doc",
    "list_doc_chunks",
    "get_doc_chunk",
    "get_source",
    "list_assertions",
    "get_assertion",
    "search_assertions",
    "list_truth_slots",
    "add_timeline_event",
    "search_events",
    "get_event",
    "list_recent_events",
    "list_upcoming_events",
    "search_all",
    "get_health",
    "get_capabilities",
    "extract_doc",
    "rebuild_knowledge_view",
    "materialize_knowledge_views",
    "upsert_knowledge_view_definition",
    "compile_wiki_article",
    "create_wiki_from_graph",
    "stop_managed_wiki",
    "configure_source_watch",
    "check_source_watch",
    "export_snapshot",
    "import_snapshot",
    // Identity resolution tools
    "propose_identity_candidate",
    "list_identity_candidates",
    "merge_entities",
    // Predicate management tools
    "register_predicate",
    "propose_predicate_alias",
    "approve_predicate_proposal",
    // Entity write tools
    "update_entity",
];

/// End-user tools for external LLMs.
/// Designed for an external LLM that needs to ingest, search, navigate, and inspect.
pub const DEFAULT_TOOLS: &[&str] = &[
    // Ingest (file ingestion handled by elephant-local-helper via stdio)
    "ingest_doc",
    "ingest_doc_simple",
    "ingest_source",
    "ingest_source_simple",
    "batch_ingest_sources",
    // Discover
    "search_all",
    // Navigate
    "get_entity",
    "get_entity_context_bundle",
    "get_knowledge_view",
    "get_knowledge_view_definition",
    "get_source_watch",
    "get_entity_with_relations",
    "search_entities",
    "add_timeline_event",
    "search_events",
    // Inspect (provenance)
    "get_provenance",
    "get_event",
    "list_recent_events",
    "list_upcoming_events",
    // Browse
    "list_spaces",
    "get_space",
    "list_predicates",
    // System
    "get_health",
];

/// Returns true if the given tool name is a default end-user tool.
#[must_use]
pub fn is_default_tool(name: &str) -> bool {
    DEFAULT_TOOLS.contains(&name)
}

/// Returns true if the given tool name is available to extraction agents.
#[must_use]
pub fn is_agent_tool(name: &str) -> bool {
    AGENT_TOOLS.contains(&name)
}

/// Returns true if the given tool name is a pipeline worker tool.
#[must_use]
pub fn is_pipeline_tool(name: &str) -> bool {
    PIPELINE_TOOLS.contains(&name)
}

/// Returns true if the given tool name is available to the maintenance profile.
#[must_use]
pub fn is_maintenance_tool(name: &str) -> bool {
    MAINTENANCE_TOOLS.contains(&name)
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::cognitive_complexity
    )]
    use super::*;

    #[test]
    fn test_agent_profile_excludes_work_item_tools() {
        assert!(!is_agent_tool("claim_work_item"));
        assert!(!is_agent_tool("complete_work_item"));
        assert!(!is_agent_tool("list_work_items"));
        assert!(!is_agent_tool("list_pending_work_items"));
        assert!(!is_agent_tool("retry_work_item"));
    }

    #[test]
    fn test_agent_profile_includes_search_entities() {
        assert!(is_agent_tool("search_entities"));
    }

    #[test]
    fn test_agent_profile_includes_read_tools() {
        // Entity read tools
        assert!(is_agent_tool("search_entities"));
        assert!(is_agent_tool("get_entity"));
        assert!(is_agent_tool("get_entity_context_bundle"));
        assert!(is_agent_tool("get_knowledge_view"));
        assert!(is_agent_tool("get_knowledge_view_definition"));

        // Predicate tools
        assert!(is_agent_tool("list_predicates"));
        assert!(is_agent_tool("get_predicate"));

        // Doc tools
        assert!(is_agent_tool("search_docs"));
        assert!(is_agent_tool("get_doc"));
        assert!(is_agent_tool("list_doc_chunks"));
        assert!(is_agent_tool("get_doc_chunk"));

        // Source tools
        assert!(is_agent_tool("get_source"));

        // Assertion tools
        assert!(is_agent_tool("list_assertions"));
        assert!(is_agent_tool("get_assertion"));
        assert!(is_agent_tool("search_assertions"));

        // Truth slot tools
        assert!(is_agent_tool("list_truth_slots"));

        // Timeline tools
        assert!(is_agent_tool("search_events"));
        assert!(is_agent_tool("get_event"));
        assert!(is_agent_tool("list_recent_events"));
        assert!(is_agent_tool("list_upcoming_events"));

        // Unified search
        assert!(is_agent_tool("search_all"));

        // System tools
        assert!(is_agent_tool("get_health"));
    }

    #[test]
    fn test_agent_profile_excludes_write_tools() {
        // Entity write tools
        assert!(!is_agent_tool("update_entity"));
        assert!(!is_agent_tool("delete_entity"));
        assert!(!is_agent_tool("restore_entity"));
        assert!(!is_agent_tool("batch_entities"));

        // Relationship tools
        assert!(!is_agent_tool("relate_entities"));
        assert!(!is_agent_tool("upsert_relationship"));

        // Doc write tools
        assert!(!is_agent_tool("ingest_doc"));
        assert!(!is_agent_tool("delete_doc"));

        // Source write tools
        assert!(!is_agent_tool("create_source"));
        assert!(!is_agent_tool("ingest_source"));
        assert!(!is_agent_tool("update_source"));
        assert!(!is_agent_tool("delete_source"));
    }

    #[test]
    fn test_agent_profile_tool_count() {
        // Per spec, agent profile should have ~15-20 tools
        assert!(
            AGENT_TOOLS.len() >= 15,
            "Agent profile should have at least 15 tools, found {}",
            AGENT_TOOLS.len()
        );
        assert!(
            AGENT_TOOLS.len() <= 25,
            "Agent profile should have at most 25 tools, found {}",
            AGENT_TOOLS.len()
        );
    }

    #[test]
    fn test_pipeline_tools() {
        assert!(is_pipeline_tool("claim_work_item"));
        assert!(is_pipeline_tool("complete_work_item"));
        assert!(is_pipeline_tool("list_work_items"));
        assert!(is_pipeline_tool("list_pending_work_items"));
        assert!(is_pipeline_tool("retry_work_item"));
    }

    #[test]
    fn test_profile_includes_methods() {
        let full = ToolProfile::Full;
        let agent = ToolProfile::Agent;
        let maintenance = ToolProfile::Maintenance;
        let pipeline = ToolProfile::Pipeline;

        assert!(full.includes_agent_tools());
        assert!(full.includes_maintenance_tools());
        assert!(full.includes_pipeline_tools());
        assert!(full.is_full());

        assert!(agent.includes_agent_tools());
        assert!(!agent.includes_maintenance_tools());
        assert!(!agent.includes_pipeline_tools());
        assert!(!agent.is_full());

        assert!(maintenance.includes_agent_tools());
        assert!(maintenance.includes_maintenance_tools());
        assert!(!maintenance.includes_pipeline_tools());
        assert!(!maintenance.is_full());

        assert!(!pipeline.includes_agent_tools());
        assert!(!pipeline.includes_maintenance_tools());
        assert!(pipeline.includes_pipeline_tools());
        assert!(!pipeline.is_full());
    }

    #[test]
    fn test_default_profile_is_full() {
        let profile = ToolProfile::default();
        assert_eq!(profile, ToolProfile::Full);
    }

    #[test]
    fn test_default_tools_count() {
        assert_eq!(
            DEFAULT_TOOLS.len(),
            23,
            "Default profile should have exactly 23 tools, found {}",
            DEFAULT_TOOLS.len()
        );
    }

    #[test]
    fn test_default_tools_include_expected() {
        assert!(is_default_tool("ingest_doc"));
        assert!(is_default_tool("ingest_doc_simple"));
        assert!(is_default_tool("ingest_source"));
        assert!(is_default_tool("ingest_source_simple"));
        assert!(is_default_tool("batch_ingest_sources"));
        assert!(
            !is_default_tool("ingest_file"),
            "ingest_file is handled by elephant-local-helper"
        );
        assert!(is_default_tool("search_all"));
        assert!(is_default_tool("get_entity"));
        assert!(is_default_tool("get_entity_context_bundle"));
        assert!(is_default_tool("get_knowledge_view"));
        assert!(is_default_tool("get_knowledge_view_definition"));
        assert!(is_default_tool("get_source_watch"));
        assert!(is_default_tool("get_entity_with_relations"));
        assert!(is_default_tool("search_entities"));
        assert!(is_default_tool("add_timeline_event"));
        assert!(is_default_tool("search_events"));
        assert!(is_default_tool("get_event"));
        assert!(is_default_tool("list_recent_events"));
        assert!(is_default_tool("list_upcoming_events"));
        assert!(is_default_tool("get_provenance"));
        assert!(is_default_tool("list_spaces"));
        assert!(is_default_tool("get_space"));
        assert!(is_default_tool("list_predicates"));
        assert!(is_default_tool("get_health"));
    }

    #[test]
    fn test_default_tools_exclude_admin() {
        assert!(!is_default_tool("batch_entities"));
        assert!(!is_default_tool("delete_entity"));
        assert!(!is_default_tool("update_entity"));
        assert!(!is_default_tool("claim_work_item"));
        assert!(!is_default_tool("relate_entities"));
        assert!(!is_default_tool("create_source"));
    }

    #[test]
    fn test_maintenance_tools_count() {
        // Maintenance has all agent reads plus admin/write/helper tools.
        assert_eq!(
            MAINTENANCE_TOOLS.len(),
            43,
            "Maintenance profile should have exactly 43 tools, found {}",
            MAINTENANCE_TOOLS.len()
        );
    }

    #[test]
    fn test_maintenance_tools_include_all_agent_tools() {
        for tool in AGENT_TOOLS {
            assert!(
                is_maintenance_tool(tool),
                "Maintenance profile should include agent tool '{tool}'"
            );
        }
    }

    #[test]
    fn test_maintenance_tools_include_write_tools() {
        // Identity resolution tools
        assert!(is_maintenance_tool("propose_identity_candidate"));
        assert!(is_maintenance_tool("list_identity_candidates"));
        assert!(is_maintenance_tool("merge_entities"));

        // Predicate management tools
        assert!(is_maintenance_tool("register_predicate"));
        assert!(is_maintenance_tool("propose_predicate_alias"));
        assert!(is_maintenance_tool("approve_predicate_proposal"));

        // Snapshot admin tools
        assert!(is_maintenance_tool("export_snapshot"));
        assert!(is_maintenance_tool("import_snapshot"));

        // Manual maintenance helpers
        assert!(is_maintenance_tool("extract_doc"));
        assert!(is_maintenance_tool("rebuild_knowledge_view"));
        assert!(is_maintenance_tool("materialize_knowledge_views"));
        assert!(is_maintenance_tool("upsert_knowledge_view_definition"));
        assert!(is_maintenance_tool("compile_wiki_article"));
        assert!(is_maintenance_tool("stop_managed_wiki"));
        assert!(!is_agent_tool("stop_managed_wiki"));
        assert!(!is_default_tool("stop_managed_wiki"));
        assert!(is_maintenance_tool("configure_source_watch"));
        assert!(is_maintenance_tool("check_source_watch"));

        // Event write tools
        assert!(is_maintenance_tool("add_timeline_event"));

        // Entity write tools
        assert!(is_maintenance_tool("update_entity"));
    }

    #[test]
    fn test_maintenance_tools_exclude_destructive_writes() {
        assert!(!is_maintenance_tool("delete_entity"));
        assert!(!is_maintenance_tool("batch_entities"));
        assert!(!is_maintenance_tool("ingest_doc"));
        assert!(!is_maintenance_tool("create_source"));
        assert!(!is_maintenance_tool("claim_work_item"));
        assert!(!is_maintenance_tool("relate_entities"));
    }
}
