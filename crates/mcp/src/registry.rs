//! Tool registry for MCP.

use std::collections::HashMap;
use std::sync::RwLock;

use crate::error::{McpError, McpResult};

/// Metadata about a tool.
#[derive(Debug, Clone)]
pub struct ToolMeta {
    /// Tool name.
    pub name: String,
    /// Tool description.
    pub description: String,
    /// Owner (core or extension name).
    pub owner: String,
    /// Input schema (JSON Schema).
    pub input_schema: serde_json::Value,
}

/// Registry for MCP tools.
pub struct ToolRegistry {
    tools: RwLock<HashMap<String, ToolMeta>>,
}

impl ToolRegistry {
    /// Creates a new tool registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            tools: RwLock::new(HashMap::new()),
        }
    }

    /// Registers a tool.
    ///
    /// # Errors
    ///
    /// Returns an error if a tool with the same name already exists.
    pub fn register(&self, tool: ToolMeta) -> McpResult<()> {
        let mut tools = self
            .tools
            .write()
            .map_err(|_| McpError::ExecutionError("lock poisoned".to_string()))?;

        if tools.contains_key(&tool.name) {
            return Err(McpError::DuplicateTool(tool.name));
        }

        tools.insert(tool.name.clone(), tool);
        drop(tools);
        Ok(())
    }

    /// Gets tool metadata by name.
    ///
    /// # Errors
    ///
    /// Returns an error if the tool is not found.
    pub fn get(&self, name: &str) -> McpResult<ToolMeta> {
        let tools = self
            .tools
            .read()
            .map_err(|_| McpError::ExecutionError("lock poisoned".to_string()))?;

        tools
            .get(name)
            .cloned()
            .ok_or_else(|| McpError::ToolNotFound(name.to_string()))
    }

    /// Lists all registered tools.
    ///
    /// # Errors
    ///
    /// Returns an error if the lock is poisoned.
    pub fn list(&self) -> McpResult<Vec<ToolMeta>> {
        let tools = self
            .tools
            .read()
            .map_err(|_| McpError::ExecutionError("lock poisoned".to_string()))?;

        Ok(tools.values().cloned().collect())
    }

    /// Unregisters a tool.
    ///
    /// # Errors
    ///
    /// Returns an error if the lock is poisoned.
    pub fn unregister(&self, name: &str) -> McpResult<()> {
        self.tools
            .write()
            .map_err(|_| McpError::ExecutionError("lock poisoned".to_string()))?
            .remove(name);
        Ok(())
    }
}

impl Default for ToolRegistry {
    fn default() -> Self {
        Self::new()
    }
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

    #[test]
    fn test_tool_registry() {
        let registry = ToolRegistry::new();

        let tool = ToolMeta {
            name: "search_entities".to_string(),
            description: "Search for entities".to_string(),
            owner: "core".to_string(),
            input_schema: serde_json::json!({"type": "object"}),
        };

        registry.register(tool).expect("register");

        let retrieved = registry.get("search_entities").expect("get");
        assert_eq!(retrieved.name, "search_entities");

        // Duplicate should fail
        let duplicate = ToolMeta {
            name: "search_entities".to_string(),
            description: "Duplicate".to_string(),
            owner: "other".to_string(),
            input_schema: serde_json::json!({}),
        };
        assert!(registry.register(duplicate).is_err());
    }
}
