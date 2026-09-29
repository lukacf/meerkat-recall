//! MCP tool definitions and dispatcher for Elephant.
//!
//! This crate provides:
//! - MCP tool definitions
//! - Tool dispatcher
//! - Extension tool registry
//! - 39 core tool handlers

pub mod authz;
pub mod coercion;
pub mod error;
pub mod handlers;
pub mod profile;
pub mod redaction;
pub mod registry;
pub mod tools;

pub use error::{McpError, McpResult};
pub use handlers::{
    create_agent_dispatcher, create_agent_dispatcher_with_context, create_default_dispatcher,
    create_default_dispatcher_with_context, create_dispatcher, create_dispatcher_with_context,
    create_maintenance_dispatcher, create_maintenance_dispatcher_with_context, ToolContext,
    ToolDispatcher, ToolHandler,
};
pub use profile::{
    is_agent_tool, is_default_tool, is_maintenance_tool, is_pipeline_tool, ToolProfile,
    AGENT_TOOLS, DEFAULT_TOOLS, MAINTENANCE_TOOLS, PIPELINE_TOOLS,
};
pub use registry::{ToolMeta, ToolRegistry};
pub use tools::{IdResponse, ToolResponse};

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

    #[test]
    fn test_mcp_error_display() {
        let err = McpError::ToolNotFound("test".to_string());
        assert!(err.to_string().contains("test"));
    }
}
