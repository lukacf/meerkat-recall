//! Plugin security enforcement.
//!
//! Per spec §2.4a: Plugins receive ABAC-filtered views, have tool access enforced,
//! and outputs are validated to prevent privilege escalation.

use elephant_types::records::base::{BaseFields, Security};
use elephant_types::records::work::{PluginManifest, WorkArtifact};

use crate::error::PipelineError;

/// Security context for plugin execution.
#[derive(Debug, Clone)]
pub struct PluginSecurityContext {
    /// Manifest declaring the plugin's capabilities.
    pub manifest: PluginManifest,
    /// Space ID the plugin is operating in.
    pub space_id: String,
    /// The most restrictive security from inputs.
    pub input_security: Security,
}

impl PluginSecurityContext {
    /// Creates a new plugin security context.
    #[must_use]
    pub fn new(manifest: PluginManifest, space_id: String) -> Self {
        Self {
            manifest,
            space_id,
            input_security: Security::default(),
        }
    }

    /// Creates context with input security.
    #[must_use]
    pub fn with_input_security(mut self, security: Security) -> Self {
        self.input_security = security;
        self
    }
}

/// ABAC-based record filtering for plugins.
///
/// Ensures plugins only see records their `required_scopes` permit.
#[derive(Debug, Clone)]
pub struct AbacFilter {
    /// Scopes the plugin has.
    allowed_scopes: Vec<String>,
    /// Space ID boundary.
    space_id: String,
}

impl AbacFilter {
    /// Creates a new ABAC filter from a plugin manifest.
    #[must_use]
    pub fn from_manifest(manifest: &PluginManifest, space_id: &str) -> Self {
        Self {
            allowed_scopes: manifest.required_scopes.clone(),
            space_id: space_id.to_string(),
        }
    }

    /// Checks if a record is visible to the plugin.
    ///
    /// A record is visible if:
    /// - It belongs to the same space
    /// - The plugin has all required scopes for the record's security level
    #[must_use]
    pub fn is_visible(&self, base: &BaseFields) -> bool {
        // Space boundary check
        if base.space_id != self.space_id {
            return false;
        }

        // Check security level against scopes
        // Higher levels require corresponding scope
        match base.security.level {
            0 => true, // Public - always visible
            1 => self
                .allowed_scopes
                .iter()
                .any(|s| s == "read:internal" || s == "read:*"),
            2 => self
                .allowed_scopes
                .iter()
                .any(|s| s == "read:confidential" || s == "read:*"),
            3 => self
                .allowed_scopes
                .iter()
                .any(|s| s == "read:restricted" || s == "read:*"),
            _ => self.allowed_scopes.contains(&"read:*".to_string()),
        }
    }

    /// Filters a collection of records to only those visible to the plugin.
    pub fn filter_visible<T, F>(&self, records: Vec<T>, get_base: F) -> Vec<T>
    where
        F: Fn(&T) -> &BaseFields,
    {
        records
            .into_iter()
            .filter(|r| self.is_visible(get_base(r)))
            .collect()
    }
}

/// Tool access enforcer for plugins.
///
/// Validates that plugins only call tools they declared in their manifest.
#[derive(Debug, Clone)]
pub struct ToolAccessEnforcer {
    /// Tools the plugin is allowed to use.
    allowed_tools: Vec<String>,
    /// Plugin ID for error reporting.
    plugin_id: String,
}

/// Error when a plugin attempts unauthorized tool access.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolAccessDenied {
    /// The plugin that attempted access.
    pub plugin_id: String,
    /// The tool that was denied.
    pub tool_id: String,
    /// Reason for denial.
    pub reason: String,
}

impl std::fmt::Display for ToolAccessDenied {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Plugin '{}' denied access to tool '{}': {}",
            self.plugin_id, self.tool_id, self.reason
        )
    }
}

impl std::error::Error for ToolAccessDenied {}

impl ToolAccessEnforcer {
    /// Creates a new tool access enforcer from a plugin manifest.
    #[must_use]
    pub fn from_manifest(manifest: &PluginManifest) -> Self {
        Self {
            allowed_tools: manifest.tool_access.clone(),
            plugin_id: manifest.plugin_id.clone(),
        }
    }

    /// Checks if a tool call is allowed.
    ///
    /// # Errors
    ///
    /// Returns `ToolAccessDenied` if the tool is not in the allowed list.
    pub fn check_access(&self, tool_id: &str) -> Result<(), ToolAccessDenied> {
        if self.allowed_tools.contains(&tool_id.to_string()) {
            Ok(())
        } else {
            Err(ToolAccessDenied {
                plugin_id: self.plugin_id.clone(),
                tool_id: tool_id.to_string(),
                reason: "Tool not declared in manifest tool_access".to_string(),
            })
        }
    }

    /// Returns the list of allowed tools.
    #[must_use]
    pub fn allowed_tools(&self) -> &[String] {
        &self.allowed_tools
    }
}

/// Security validator for plugin outputs.
///
/// Per spec §3.6: Derived artifacts inherit the most restrictive security
/// of their inputs. Plugins cannot escalate privileges.
#[derive(Debug, Clone)]
pub struct OutputSecurityValidator {
    /// The input security (ceiling for outputs).
    input_security: Security,
    /// Plugin ID for error reporting.
    plugin_id: String,
}

/// Error when a plugin attempts privilege escalation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecurityEscalation {
    /// The plugin that attempted escalation.
    pub plugin_id: String,
    /// Description of the escalation attempt.
    pub description: String,
    /// The input security level.
    pub input_level: i32,
    /// The attempted output security level.
    pub output_level: i32,
}

impl std::fmt::Display for SecurityEscalation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Plugin '{}' attempted security escalation: {} (input level: {}, output level: {})",
            self.plugin_id, self.description, self.input_level, self.output_level
        )
    }
}

impl std::error::Error for SecurityEscalation {}

impl OutputSecurityValidator {
    /// Creates a new output security validator.
    #[must_use]
    pub fn new(input_security: Security, plugin_id: String) -> Self {
        Self {
            input_security,
            plugin_id,
        }
    }

    /// Validates that an artifact's security doesn't exceed the input.
    ///
    /// # Errors
    ///
    /// Returns `SecurityEscalation` if the output attempts to reduce security
    /// (i.e., make data more accessible than inputs allowed).
    pub fn validate_artifact(&self, artifact: &WorkArtifact) -> Result<(), SecurityEscalation> {
        let output_security = &artifact.base.security;

        // Lower level number = higher sensitivity
        // Output level must be >= input level (can't make data less sensitive)
        if output_security.level < self.input_security.level {
            return Err(SecurityEscalation {
                plugin_id: self.plugin_id.clone(),
                description:
                    "Output security level is lower than input (data would be more accessible)"
                        .to_string(),
                input_level: self.input_security.level,
                output_level: output_security.level,
            });
        }

        Ok(())
    }

    /// Validates multiple artifacts.
    ///
    /// # Errors
    ///
    /// Returns the first escalation error found.
    pub fn validate_artifacts(&self, artifacts: &[WorkArtifact]) -> Result<(), SecurityEscalation> {
        for artifact in artifacts {
            self.validate_artifact(artifact)?;
        }
        Ok(())
    }

    /// Returns the input security.
    #[must_use]
    pub fn input_security(&self) -> &Security {
        &self.input_security
    }

    /// Applies input security to an artifact (sets minimum security level).
    pub fn apply_minimum_security(&self, artifact: &mut WorkArtifact) {
        // Ensure output is at least as restrictive as input
        if artifact.base.security.level < self.input_security.level {
            artifact.base.security.level = self.input_security.level;
        }

        // Inherit policy_id if output doesn't have an explicit one
        if artifact.base.security.policy_id.is_empty()
            || artifact.base.security.policy_id == "default"
        {
            artifact.base.security.policy_id = self.input_security.policy_id.clone();
        }
    }
}

/// Combined plugin security enforcer.
///
/// Integrates ABAC filtering, tool access, and output validation.
pub struct PluginSecurityEnforcer {
    /// ABAC filter for record visibility.
    pub abac_filter: AbacFilter,
    /// Tool access enforcer.
    pub tool_enforcer: ToolAccessEnforcer,
    /// Output security validator.
    pub output_validator: OutputSecurityValidator,
}

impl PluginSecurityEnforcer {
    /// Creates a new enforcer from security context.
    #[must_use]
    pub fn new(ctx: &PluginSecurityContext) -> Self {
        Self {
            abac_filter: AbacFilter::from_manifest(&ctx.manifest, &ctx.space_id),
            tool_enforcer: ToolAccessEnforcer::from_manifest(&ctx.manifest),
            output_validator: OutputSecurityValidator::new(
                ctx.input_security.clone(),
                ctx.manifest.plugin_id.clone(),
            ),
        }
    }

    /// Executes a tool call with access enforcement.
    ///
    /// # Errors
    ///
    /// Returns an error if tool access is denied.
    pub fn execute_tool_call<F, T, E>(&self, tool_id: &str, call: F) -> Result<T, PipelineError>
    where
        F: FnOnce() -> Result<T, E>,
        E: Into<PipelineError>,
    {
        self.tool_enforcer
            .check_access(tool_id)
            .map_err(|e| PipelineError::SecurityViolation(e.to_string()))?;

        call().map_err(Into::into)
    }

    /// Validates and potentially modifies output artifacts.
    ///
    /// # Errors
    ///
    /// Returns an error if any artifact attempts security escalation.
    pub fn validate_outputs(&self, artifacts: &[WorkArtifact]) -> Result<(), PipelineError> {
        self.output_validator
            .validate_artifacts(artifacts)
            .map_err(|e| PipelineError::SecurityViolation(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use elephant_types::ids::TableName;
    use elephant_types::records::work::{ArtifactStatus, Determinism, RetryPolicy};
    use elephant_types::{Link, RecordId};

    fn make_manifest(plugin_id: &str, tools: Vec<String>, scopes: Vec<String>) -> PluginManifest {
        PluginManifest {
            plugin_id: plugin_id.to_string(),
            version: "1.0.0".to_string(),
            plugin_type: "test".to_string(),
            required_scopes: scopes,
            tool_access: tools,
            work_kinds: vec!["test_work".to_string()],
            determinism: Determinism::Deterministic,
            max_tokens: None,
            max_wall_time_secs: None,
            input_types: vec![],
            output_types: vec![],
            validator_profile: None,
            config_schema: None,
            health_check: None,
            retry_policy: RetryPolicy::default(),
        }
    }

    fn make_base_fields(space_id: &str, level: i32) -> BaseFields {
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::WorkArtifact),
            space_id.to_string(),
            "test".to_string(),
        );
        base.security.level = level;
        base
    }

    fn make_artifact(space_id: &str, level: i32) -> WorkArtifact {
        WorkArtifact {
            base: make_base_fields(space_id, level),
            work_run: Link::new(RecordId::new_v4(TableName::WorkRun)),
            artifact_type: "test".to_string(),
            data: serde_json::json!({}),
            status: ArtifactStatus::Draft,
            validation_errors: None,
            validated_at: None,
            committed_at: None,
        }
    }

    // ABAC Filter Tests

    #[test]
    fn test_abac_filter_same_space_public() {
        let manifest = make_manifest("test", vec![], vec![]);
        let filter = AbacFilter::from_manifest(&manifest, "default");

        let base = make_base_fields("default", 0);
        assert!(filter.is_visible(&base));
    }

    #[test]
    fn test_abac_filter_different_space() {
        let manifest = make_manifest("test", vec![], vec!["read:*".to_string()]);
        let filter = AbacFilter::from_manifest(&manifest, "default");

        let base = make_base_fields("other_space", 0);
        assert!(!filter.is_visible(&base));
    }

    #[test]
    fn test_abac_filter_internal_requires_scope() {
        let manifest = make_manifest("test", vec![], vec![]);
        let filter = AbacFilter::from_manifest(&manifest, "default");

        let base = make_base_fields("default", 1);
        assert!(!filter.is_visible(&base));
    }

    #[test]
    fn test_abac_filter_internal_with_scope() {
        let manifest = make_manifest("test", vec![], vec!["read:internal".to_string()]);
        let filter = AbacFilter::from_manifest(&manifest, "default");

        let base = make_base_fields("default", 1);
        assert!(filter.is_visible(&base));
    }

    #[test]
    fn test_abac_filter_wildcard_scope() {
        let manifest = make_manifest("test", vec![], vec!["read:*".to_string()]);
        let filter = AbacFilter::from_manifest(&manifest, "default");

        for level in 0..=3 {
            let base = make_base_fields("default", level);
            assert!(filter.is_visible(&base));
        }
    }

    // Tool Access Tests

    #[test]
    fn test_tool_access_allowed() {
        let manifest = make_manifest("test", vec!["search".to_string()], vec![]);
        let enforcer = ToolAccessEnforcer::from_manifest(&manifest);

        assert!(enforcer.check_access("search").is_ok());
    }

    #[test]
    fn test_tool_access_denied() {
        let manifest = make_manifest("test", vec!["search".to_string()], vec![]);
        let enforcer = ToolAccessEnforcer::from_manifest(&manifest);

        let result = enforcer.check_access("delete");
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(err.plugin_id, "test");
        assert_eq!(err.tool_id, "delete");
    }

    #[test]
    fn test_tool_access_empty_manifest() {
        let manifest = make_manifest("test", vec![], vec![]);
        let enforcer = ToolAccessEnforcer::from_manifest(&manifest);

        assert!(enforcer.check_access("any_tool").is_err());
    }

    // Output Security Tests

    #[test]
    fn test_output_security_same_level() {
        let input = Security {
            level: 2,
            ..Default::default()
        };
        let validator = OutputSecurityValidator::new(input, "test".to_string());

        let artifact = make_artifact("default", 2);
        assert!(validator.validate_artifact(&artifact).is_ok());
    }

    #[test]
    fn test_output_security_more_restrictive() {
        let input = Security {
            level: 2,
            ..Default::default()
        };
        let validator = OutputSecurityValidator::new(input, "test".to_string());

        let artifact = make_artifact("default", 3); // More restrictive
        assert!(validator.validate_artifact(&artifact).is_ok());
    }

    #[test]
    fn test_output_security_escalation_rejected() {
        let input = Security {
            level: 2,
            ..Default::default()
        };
        let validator = OutputSecurityValidator::new(input, "test".to_string());

        let artifact = make_artifact("default", 1); // Less restrictive = escalation
        let result = validator.validate_artifact(&artifact);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.input_level, 2);
        assert_eq!(err.output_level, 1);
    }

    #[test]
    fn test_apply_minimum_security() {
        let input = Security {
            level: 2,
            policy_id: "strict".to_string(),
            ..Default::default()
        };
        let validator = OutputSecurityValidator::new(input, "test".to_string());

        let mut artifact = make_artifact("default", 0);
        validator.apply_minimum_security(&mut artifact);

        assert_eq!(artifact.base.security.level, 2);
        assert_eq!(artifact.base.security.policy_id, "strict");
    }

    // Integrated Enforcer Tests

    #[test]
    fn test_plugin_security_enforcer_tool_call() {
        let manifest = make_manifest("test", vec!["search".to_string()], vec![]);
        let ctx = PluginSecurityContext::new(manifest, "default".to_string());
        let enforcer = PluginSecurityEnforcer::new(&ctx);

        let result: Result<i32, PipelineError> =
            enforcer.execute_tool_call("search", || Ok::<i32, PipelineError>(42));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn test_plugin_security_enforcer_tool_denied() {
        let manifest = make_manifest("test", vec![], vec![]);
        let ctx = PluginSecurityContext::new(manifest, "default".to_string());
        let enforcer = PluginSecurityEnforcer::new(&ctx);

        let result: Result<i32, PipelineError> =
            enforcer.execute_tool_call("search", || Ok::<i32, PipelineError>(42));
        assert!(result.is_err());
    }
}
