//! Plugin configuration and discovery.
//!
//! Per spec §2.4a: Plugins register via configuration, bound to source_type patterns
//! or work_kind handlers. Config is versioned for revalidation.

use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::PipelineError;

/// Plugin configuration file format.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PluginConfigFile {
    /// Configuration file version.
    #[serde(default = "default_version")]
    pub version: u32,
    /// Plugin configurations.
    #[serde(default)]
    pub plugins: Vec<PluginConfig>,
}

fn default_version() -> u32 {
    1
}

/// Individual plugin configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginConfig {
    /// Plugin identifier.
    pub plugin_id: String,
    /// Whether the plugin is enabled.
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Priority (lower = higher priority).
    #[serde(default = "default_priority")]
    pub priority: u32,
    /// Source types this plugin handles (glob patterns).
    #[serde(default)]
    pub source_types: Vec<String>,
    /// Work kinds this plugin handles.
    #[serde(default)]
    pub work_kinds: Vec<String>,
    /// Plugin-specific configuration.
    #[serde(default)]
    pub config: serde_json::Value,
    /// Configuration version for change tracking.
    #[serde(default = "default_config_version")]
    pub config_version: u64,
}

fn default_enabled() -> bool {
    true
}

fn default_priority() -> u32 {
    100
}

fn default_config_version() -> u64 {
    1
}

impl PluginConfig {
    /// Creates a new plugin configuration.
    #[must_use]
    pub fn new(plugin_id: String) -> Self {
        Self {
            plugin_id,
            enabled: true,
            priority: 100,
            source_types: vec![],
            work_kinds: vec![],
            config: serde_json::Value::Null,
            config_version: 1,
        }
    }

    /// Sets the source types this plugin handles.
    #[must_use]
    pub fn with_source_types(mut self, types: Vec<String>) -> Self {
        self.source_types = types;
        self
    }

    /// Sets the work kinds this plugin handles.
    #[must_use]
    pub fn with_work_kinds(mut self, kinds: Vec<String>) -> Self {
        self.work_kinds = kinds;
        self
    }

    /// Sets the priority.
    #[must_use]
    pub fn with_priority(mut self, priority: u32) -> Self {
        self.priority = priority;
        self
    }

    /// Sets the plugin-specific config.
    #[must_use]
    pub fn with_config(mut self, config: serde_json::Value) -> Self {
        self.config = config;
        self
    }

    /// Increments the config version.
    pub fn bump_version(&mut self) {
        self.config_version = self.config_version.saturating_add(1);
    }
}

/// Plugin configuration loader and registry.
#[derive(Debug, Clone, Default)]
pub struct PluginConfigRegistry {
    /// All plugin configurations.
    configs: Vec<PluginConfig>,
    /// Index: source_type pattern -> plugin_ids (sorted by priority).
    source_type_index: HashMap<String, Vec<String>>,
    /// Index: work_kind -> plugin_ids (sorted by priority).
    work_kind_index: HashMap<String, Vec<String>>,
    /// Tracked config versions for change detection.
    config_versions: HashMap<String, u64>,
}

impl PluginConfigRegistry {
    /// Creates a new empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Loads plugin configuration from a TOML file.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read or parsed.
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, PipelineError> {
        let content = std::fs::read_to_string(path.as_ref())
            .map_err(|e| PipelineError::ConfigError(format!("Failed to read config: {e}")))?;

        Self::load_from_toml(&content)
    }

    /// Loads plugin configuration from TOML string.
    ///
    /// # Errors
    ///
    /// Returns an error if the TOML cannot be parsed.
    pub fn load_from_toml(toml_content: &str) -> Result<Self, PipelineError> {
        let config_file: PluginConfigFile = toml::from_str(toml_content)
            .map_err(|e| PipelineError::ConfigError(format!("Failed to parse config: {e}")))?;

        let mut registry = Self::new();
        for config in config_file.plugins {
            if config.enabled {
                registry.register(config);
            }
        }

        Ok(registry)
    }

    /// Registers a plugin configuration.
    pub fn register(&mut self, config: PluginConfig) {
        let plugin_id = config.plugin_id.clone();

        // Track config version
        self.config_versions
            .insert(plugin_id.clone(), config.config_version);

        // Index by source types
        for pattern in &config.source_types {
            self.source_type_index
                .entry(pattern.clone())
                .or_default()
                .push(plugin_id.clone());
        }

        // Index by work kinds
        for kind in &config.work_kinds {
            self.work_kind_index
                .entry(kind.clone())
                .or_default()
                .push(plugin_id.clone());
        }

        self.configs.push(config);

        // Re-sort indices by priority
        self.sort_indices();
    }

    /// Re-sorts all indices by plugin priority.
    fn sort_indices(&mut self) {
        let priority_map: HashMap<String, u32> = self
            .configs
            .iter()
            .map(|c| (c.plugin_id.clone(), c.priority))
            .collect();

        for plugins in self.source_type_index.values_mut() {
            plugins.sort_by_key(|id| priority_map.get(id).copied().unwrap_or(u32::MAX));
        }

        for plugins in self.work_kind_index.values_mut() {
            plugins.sort_by_key(|id| priority_map.get(id).copied().unwrap_or(u32::MAX));
        }
    }

    /// Gets plugins that handle a source type, ordered by priority.
    ///
    /// Supports glob pattern matching (e.g., "text/*" matches "text/plain").
    #[must_use]
    pub fn get_plugins_for_source_type(&self, source_type: &str) -> Vec<&PluginConfig> {
        let mut result = Vec::new();

        for (pattern, plugin_ids) in &self.source_type_index {
            if matches_pattern(pattern, source_type) {
                for plugin_id in plugin_ids {
                    if let Some(config) = self.get_config(plugin_id) {
                        if !result
                            .iter()
                            .any(|c: &&PluginConfig| c.plugin_id == config.plugin_id)
                        {
                            result.push(config);
                        }
                    }
                }
            }
        }

        // Sort by priority
        result.sort_by_key(|c| c.priority);
        result
    }

    /// Gets plugins that handle a work kind, ordered by priority.
    #[must_use]
    pub fn get_plugins_for_work_kind(&self, work_kind: &str) -> Vec<&PluginConfig> {
        self.work_kind_index
            .get(work_kind)
            .map(|ids| ids.iter().filter_map(|id| self.get_config(id)).collect())
            .unwrap_or_default()
    }

    /// Gets the highest-priority plugin for a source type.
    #[must_use]
    pub fn get_primary_plugin_for_source_type(&self, source_type: &str) -> Option<&PluginConfig> {
        self.get_plugins_for_source_type(source_type)
            .first()
            .copied()
    }

    /// Gets the highest-priority plugin for a work kind.
    #[must_use]
    pub fn get_primary_plugin_for_work_kind(&self, work_kind: &str) -> Option<&PluginConfig> {
        self.get_plugins_for_work_kind(work_kind).first().copied()
    }

    /// Gets a plugin configuration by ID.
    #[must_use]
    pub fn get_config(&self, plugin_id: &str) -> Option<&PluginConfig> {
        self.configs.iter().find(|c| c.plugin_id == plugin_id)
    }

    /// Gets a mutable plugin configuration by ID.
    pub fn get_config_mut(&mut self, plugin_id: &str) -> Option<&mut PluginConfig> {
        self.configs.iter_mut().find(|c| c.plugin_id == plugin_id)
    }

    /// Checks if a plugin's config has changed since last check.
    #[must_use]
    pub fn has_config_changed(&self, plugin_id: &str, known_version: u64) -> bool {
        self.config_versions
            .get(plugin_id)
            .map_or(true, |&v| v != known_version)
    }

    /// Gets the current config version for a plugin.
    #[must_use]
    pub fn get_config_version(&self, plugin_id: &str) -> Option<u64> {
        self.config_versions.get(plugin_id).copied()
    }

    /// Updates a plugin's configuration and bumps version.
    ///
    /// Returns the new version number.
    pub fn update_config(&mut self, plugin_id: &str, new_config: serde_json::Value) -> Option<u64> {
        if let Some(config) = self.get_config_mut(plugin_id) {
            config.config = new_config;
            config.bump_version();
            let version = config.config_version;
            self.config_versions.insert(plugin_id.to_string(), version);
            Some(version)
        } else {
            None
        }
    }

    /// Lists all registered plugins.
    #[must_use]
    pub fn list_plugins(&self) -> &[PluginConfig] {
        &self.configs
    }

    /// Returns the number of registered plugins.
    #[must_use]
    pub fn len(&self) -> usize {
        self.configs.len()
    }

    /// Returns true if no plugins are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.configs.is_empty()
    }
}

/// Simple glob-like pattern matching.
///
/// Supports:
/// - `*` matches any single segment
/// - `**` matches any number of segments
/// - Exact matches
fn matches_pattern(pattern: &str, value: &str) -> bool {
    if pattern == "*" || pattern == "**" {
        return true;
    }

    if pattern == value {
        return true;
    }

    // Handle trailing wildcard (e.g., "text/*")
    if let Some(prefix) = pattern.strip_suffix("/*") {
        if let Some(value_prefix) = value.rsplit_once('/') {
            return value_prefix.0 == prefix;
        }
    }

    // Handle prefix wildcard (e.g., "*/json")
    if let Some(suffix) = pattern.strip_prefix("*/") {
        if let Some(value_suffix) = value.rsplit_once('/') {
            return value_suffix.1 == suffix;
        }
    }

    false
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    #[test]
    fn test_pattern_matching_exact() {
        assert!(matches_pattern("text/plain", "text/plain"));
        assert!(!matches_pattern("text/plain", "text/html"));
    }

    #[test]
    fn test_pattern_matching_wildcard() {
        assert!(matches_pattern("*", "anything"));
        assert!(matches_pattern("**", "anything/at/all"));
    }

    #[test]
    fn test_pattern_matching_trailing_wildcard() {
        assert!(matches_pattern("text/*", "text/plain"));
        assert!(matches_pattern("text/*", "text/html"));
        assert!(!matches_pattern("text/*", "image/png"));
    }

    #[test]
    fn test_pattern_matching_prefix_wildcard() {
        assert!(matches_pattern("*/json", "application/json"));
        assert!(matches_pattern("*/json", "text/json"));
        assert!(!matches_pattern("*/json", "text/plain"));
    }

    #[test]
    fn test_plugin_config_builder() {
        let config = PluginConfig::new("test-plugin".to_string())
            .with_source_types(vec!["text/*".to_string()])
            .with_work_kinds(vec!["extract".to_string()])
            .with_priority(50)
            .with_config(serde_json::json!({"key": "value"}));

        assert_eq!(config.plugin_id, "test-plugin");
        assert_eq!(config.priority, 50);
        assert_eq!(config.source_types, vec!["text/*"]);
        assert_eq!(config.work_kinds, vec!["extract"]);
    }

    #[test]
    fn test_load_from_toml() {
        let toml = r#"
version = 1

[[plugins]]
plugin_id = "text-extractor"
priority = 10
source_types = ["text/*"]
work_kinds = ["extract_text"]

[[plugins]]
plugin_id = "pdf-extractor"
priority = 20
source_types = ["application/pdf"]
work_kinds = ["extract_text"]

[[plugins]]
plugin_id = "disabled-plugin"
enabled = false
source_types = ["*"]
"#;

        let registry = PluginConfigRegistry::load_from_toml(toml).expect("parse");

        assert_eq!(registry.len(), 2); // disabled not included

        let text_plugins = registry.get_plugins_for_source_type("text/plain");
        assert_eq!(text_plugins.len(), 1);
        assert_eq!(text_plugins[0].plugin_id, "text-extractor");

        let pdf_plugins = registry.get_plugins_for_source_type("application/pdf");
        assert_eq!(pdf_plugins.len(), 1);
        assert_eq!(pdf_plugins[0].plugin_id, "pdf-extractor");
    }

    #[test]
    fn test_priority_ordering() {
        let mut registry = PluginConfigRegistry::new();

        registry.register(
            PluginConfig::new("low-priority".to_string())
                .with_source_types(vec!["text/*".to_string()])
                .with_priority(100),
        );

        registry.register(
            PluginConfig::new("high-priority".to_string())
                .with_source_types(vec!["text/*".to_string()])
                .with_priority(10),
        );

        let plugins = registry.get_plugins_for_source_type("text/plain");
        assert_eq!(plugins.len(), 2);
        assert_eq!(plugins[0].plugin_id, "high-priority");
        assert_eq!(plugins[1].plugin_id, "low-priority");
    }

    #[test]
    fn test_primary_plugin() {
        let mut registry = PluginConfigRegistry::new();

        registry.register(
            PluginConfig::new("plugin-a".to_string())
                .with_work_kinds(vec!["extract".to_string()])
                .with_priority(50),
        );

        registry.register(
            PluginConfig::new("plugin-b".to_string())
                .with_work_kinds(vec!["extract".to_string()])
                .with_priority(10),
        );

        let primary = registry.get_primary_plugin_for_work_kind("extract");
        assert!(primary.is_some());
        assert_eq!(primary.unwrap().plugin_id, "plugin-b");
    }

    #[test]
    fn test_config_version_tracking() {
        let mut registry = PluginConfigRegistry::new();

        registry.register(PluginConfig::new("test-plugin".to_string()));

        assert_eq!(registry.get_config_version("test-plugin"), Some(1));
        assert!(!registry.has_config_changed("test-plugin", 1));
        assert!(registry.has_config_changed("test-plugin", 0));

        let new_version = registry.update_config("test-plugin", serde_json::json!({"new": true}));
        assert_eq!(new_version, Some(2));
        assert!(registry.has_config_changed("test-plugin", 1));
        assert!(!registry.has_config_changed("test-plugin", 2));
    }

    #[test]
    fn test_bump_version() {
        let mut config = PluginConfig::new("test".to_string());
        assert_eq!(config.config_version, 1);

        config.bump_version();
        assert_eq!(config.config_version, 2);

        config.bump_version();
        assert_eq!(config.config_version, 3);
    }
}
