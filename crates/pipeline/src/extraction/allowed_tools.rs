//! Tool access restrictions for extractors.
//!
//! Defines which tools extractors are allowed to call during extraction.

/// Tool access configuration for extractors.
///
/// Per spec: extractors have limited tool access to prevent
/// unbounded resource consumption and maintain security boundaries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractorToolAccess {
    /// List of tool names the extractor is allowed to call.
    pub allowed_tools: Vec<String>,
}

impl Default for ExtractorToolAccess {
    fn default() -> Self {
        Self {
            allowed_tools: vec![
                "fetch_node_text".to_string(),
                "list_revision_nodes".to_string(),
                "search_entities".to_string(),
                "fetch_entity_context".to_string(),
                "fetch_predicate_registry".to_string(),
            ],
        }
    }
}

impl ExtractorToolAccess {
    /// Creates a new tool access configuration with the given tools.
    #[must_use]
    pub fn new(allowed_tools: Vec<String>) -> Self {
        Self { allowed_tools }
    }

    /// Creates an empty tool access configuration (no tools allowed).
    #[must_use]
    pub fn none() -> Self {
        Self {
            allowed_tools: Vec::new(),
        }
    }

    /// Checks if a tool is allowed.
    #[must_use]
    pub fn is_allowed(&self, tool_name: &str) -> bool {
        self.allowed_tools.iter().any(|t| t == tool_name)
    }

    /// Adds a tool to the allowed list.
    pub fn allow(&mut self, tool_name: String) {
        if !self.is_allowed(&tool_name) {
            self.allowed_tools.push(tool_name);
        }
    }

    /// Removes a tool from the allowed list.
    pub fn deny(&mut self, tool_name: &str) {
        self.allowed_tools.retain(|t| t != tool_name);
    }

    /// Returns the number of allowed tools.
    #[must_use]
    pub fn len(&self) -> usize {
        self.allowed_tools.len()
    }

    /// Returns true if no tools are allowed.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.allowed_tools.is_empty()
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        unused_imports,
        dead_code
    )]

    use super::*;

    #[test]
    fn test_default_tool_access() {
        let access = ExtractorToolAccess::default();
        assert_eq!(access.len(), 5);
        assert!(access.is_allowed("fetch_node_text"));
        assert!(access.is_allowed("list_revision_nodes"));
        assert!(access.is_allowed("search_entities"));
        assert!(access.is_allowed("fetch_entity_context"));
        assert!(access.is_allowed("fetch_predicate_registry"));
        assert!(!access.is_allowed("delete_everything"));
    }

    #[test]
    fn test_empty_tool_access() {
        let access = ExtractorToolAccess::none();
        assert!(access.is_empty());
        assert!(!access.is_allowed("fetch_node_text"));
    }

    #[test]
    fn test_allow_tool() {
        let mut access = ExtractorToolAccess::none();
        access.allow("my_tool".to_string());
        assert!(access.is_allowed("my_tool"));
        assert_eq!(access.len(), 1);

        // Adding same tool again should not duplicate
        access.allow("my_tool".to_string());
        assert_eq!(access.len(), 1);
    }

    #[test]
    fn test_deny_tool() {
        let mut access = ExtractorToolAccess::default();
        assert!(access.is_allowed("fetch_node_text"));

        access.deny("fetch_node_text");
        assert!(!access.is_allowed("fetch_node_text"));
    }

    #[test]
    fn test_custom_tool_access() {
        let access = ExtractorToolAccess::new(vec!["tool_a".to_string(), "tool_b".to_string()]);
        assert_eq!(access.len(), 2);
        assert!(access.is_allowed("tool_a"));
        assert!(access.is_allowed("tool_b"));
        assert!(!access.is_allowed("tool_c"));
    }
}
