//! Worker trait and registry for plugin management.
//!
//! Per spec §2.4: Workers declare manifests and execute work items.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use elephant_types::records::work::{PluginManifest, WorkArtifact, WorkItem, WorkRun};

use crate::error::PipelineError;

/// Result of worker execution.
#[derive(Debug, Clone)]
pub struct WorkerResult {
    /// Whether the execution succeeded.
    pub success: bool,
    /// Error message if failed.
    pub error: Option<String>,
    /// Artifacts produced by the worker.
    pub artifacts: Vec<WorkArtifact>,
    /// Metrics collected during execution.
    pub metrics: WorkRunMetrics,
}

/// Metrics from worker execution.
#[derive(Debug, Clone, Default)]
pub struct WorkRunMetrics {
    /// Tokens used (for LLM workers).
    pub tokens_used: u32,
    /// Steps taken (for agentic loops).
    pub steps_taken: u32,
    /// Wall time in milliseconds.
    pub wall_time_ms: u64,
    /// Estimated cost in euros.
    pub estimated_cost_euros: f64,
}

/// Context provided to workers during execution.
#[derive(Debug, Clone)]
pub struct WorkerContext {
    /// The work item being executed.
    pub work_item: WorkItem,
    /// The work run record.
    pub work_run: WorkRun,
    /// Space ID for the execution.
    pub space_id: String,
    /// Allowed tool access from manifest.
    pub allowed_tools: Vec<String>,
}

/// Trait for work executors.
#[async_trait]
pub trait Worker: Send + Sync {
    /// Returns the worker's manifest.
    fn manifest(&self) -> &PluginManifest;

    /// Returns the work kinds this worker handles.
    fn work_kinds(&self) -> &[String] {
        &self.manifest().work_kinds
    }

    /// Executes work and returns the result.
    ///
    /// # Errors
    ///
    /// Returns an error if execution fails.
    async fn execute(&self, ctx: WorkerContext) -> Result<WorkerResult, PipelineError>;

    /// Optional health check.
    async fn health_check(&self) -> Result<bool, PipelineError> {
        Ok(true)
    }
}

/// Validation errors for manifests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestValidationError {
    /// Missing required field.
    MissingField(String),
    /// Invalid field value.
    InvalidValue {
        /// The field that has an invalid value.
        field: String,
        /// The reason the value is invalid.
        reason: String,
    },
    /// Disallowed tool access.
    DisallowedTool(String),
}

impl std::fmt::Display for ManifestValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingField(field) => write!(f, "Missing required field: {field}"),
            Self::InvalidValue { field, reason } => {
                write!(f, "Invalid value for {field}: {reason}")
            }
            Self::DisallowedTool(tool) => write!(f, "Disallowed tool access: {tool}"),
        }
    }
}

impl std::error::Error for ManifestValidationError {}

/// Registry of workers.
#[derive(Default)]
pub struct WorkerRegistry {
    workers: HashMap<String, Arc<dyn Worker>>,
    allowed_tools: Vec<String>,
}

impl WorkerRegistry {
    /// Creates a new empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a registry with allowed tools whitelist.
    #[must_use]
    pub fn with_allowed_tools(allowed_tools: Vec<String>) -> Self {
        Self {
            workers: HashMap::new(),
            allowed_tools,
        }
    }

    /// Validates a plugin manifest.
    ///
    /// # Errors
    ///
    /// Returns validation errors if the manifest is invalid.
    pub fn validate_manifest(
        &self,
        manifest: &PluginManifest,
    ) -> Result<(), Vec<ManifestValidationError>> {
        let mut errors = Vec::new();

        // Required fields
        if manifest.plugin_id.is_empty() {
            errors.push(ManifestValidationError::MissingField(
                "plugin_id".to_string(),
            ));
        }
        if manifest.version.is_empty() {
            errors.push(ManifestValidationError::MissingField("version".to_string()));
        }
        if manifest.work_kinds.is_empty() {
            errors.push(ManifestValidationError::MissingField(
                "work_kinds".to_string(),
            ));
        }

        // Tool access validation
        if !self.allowed_tools.is_empty() {
            for tool in &manifest.tool_access {
                if !self.allowed_tools.contains(tool) {
                    errors.push(ManifestValidationError::DisallowedTool(tool.clone()));
                }
            }
        }

        // Validate determinism bounds
        match manifest.determinism {
            elephant_types::records::work::Determinism::LlmSingleShot => {
                if manifest.max_tokens.is_none() {
                    errors.push(ManifestValidationError::InvalidValue {
                        field: "max_tokens".to_string(),
                        reason: "Required for LLM single-shot workers".to_string(),
                    });
                }
            }
            elephant_types::records::work::Determinism::AgenticLoop => {
                if manifest.max_wall_time_secs.is_none() {
                    errors.push(ManifestValidationError::InvalidValue {
                        field: "max_wall_time_secs".to_string(),
                        reason: "Required for agentic loop workers".to_string(),
                    });
                }
            }
            elephant_types::records::work::Determinism::Deterministic => {}
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Registers a worker.
    ///
    /// # Errors
    ///
    /// Returns an error if the manifest is invalid.
    pub fn register(
        &mut self,
        worker: Arc<dyn Worker>,
    ) -> Result<(), Vec<ManifestValidationError>> {
        let manifest = worker.manifest();
        self.validate_manifest(manifest)?;

        for kind in &manifest.work_kinds {
            self.workers.insert(kind.clone(), worker.clone());
        }

        Ok(())
    }

    /// Gets a worker for a work kind.
    #[must_use]
    pub fn get(&self, work_kind: &str) -> Option<Arc<dyn Worker>> {
        self.workers.get(work_kind).cloned()
    }

    /// Lists all registered work kinds.
    #[must_use]
    pub fn work_kinds(&self) -> Vec<String> {
        self.workers.keys().cloned().collect()
    }

    /// Returns the number of registered workers.
    #[must_use]
    pub fn len(&self) -> usize {
        self.workers.len()
    }

    /// Returns true if no workers are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.workers.is_empty()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use elephant_types::records::work::{Determinism, RetryPolicy};

    fn make_manifest(plugin_id: &str, work_kinds: Vec<String>) -> PluginManifest {
        PluginManifest {
            plugin_id: plugin_id.to_string(),
            version: "1.0.0".to_string(),
            plugin_type: "test".to_string(),
            required_scopes: vec![],
            tool_access: vec![],
            work_kinds,
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

    struct TestWorker {
        manifest: PluginManifest,
    }

    #[async_trait]
    impl Worker for TestWorker {
        fn manifest(&self) -> &PluginManifest {
            &self.manifest
        }

        async fn execute(&self, _ctx: WorkerContext) -> Result<WorkerResult, PipelineError> {
            Ok(WorkerResult {
                success: true,
                error: None,
                artifacts: vec![],
                metrics: WorkRunMetrics::default(),
            })
        }
    }

    #[test]
    fn test_validate_valid_manifest() {
        let registry = WorkerRegistry::new();
        let manifest = make_manifest("test-plugin", vec!["test_work".to_string()]);

        let result = registry.validate_manifest(&manifest);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_missing_required_fields() {
        let registry = WorkerRegistry::new();
        let manifest = PluginManifest {
            plugin_id: String::new(),
            version: String::new(),
            plugin_type: "test".to_string(),
            required_scopes: vec![],
            tool_access: vec![],
            work_kinds: vec![],
            determinism: Determinism::Deterministic,
            max_tokens: None,
            max_wall_time_secs: None,
            input_types: vec![],
            output_types: vec![],
            validator_profile: None,
            config_schema: None,
            health_check: None,
            retry_policy: RetryPolicy::default(),
        };

        let result = registry.validate_manifest(&manifest);
        assert!(result.is_err());

        let errors = result.unwrap_err();
        assert!(errors
            .iter()
            .any(|e| matches!(e, ManifestValidationError::MissingField(f) if f == "plugin_id")));
        assert!(errors
            .iter()
            .any(|e| matches!(e, ManifestValidationError::MissingField(f) if f == "version")));
        assert!(errors
            .iter()
            .any(|e| matches!(e, ManifestValidationError::MissingField(f) if f == "work_kinds")));
    }

    #[test]
    fn test_validate_disallowed_tool() {
        let registry = WorkerRegistry::with_allowed_tools(vec!["safe_tool".to_string()]);
        let mut manifest = make_manifest("test-plugin", vec!["test_work".to_string()]);
        manifest.tool_access = vec!["unsafe_tool".to_string()];

        let result = registry.validate_manifest(&manifest);
        assert!(result.is_err());

        let errors = result.unwrap_err();
        assert!(errors.iter().any(
            |e| matches!(e, ManifestValidationError::DisallowedTool(t) if t == "unsafe_tool")
        ));
    }

    #[test]
    fn test_llm_worker_requires_max_tokens() {
        let registry = WorkerRegistry::new();
        let mut manifest = make_manifest("test-plugin", vec!["test_work".to_string()]);
        manifest.determinism = Determinism::LlmSingleShot;

        let result = registry.validate_manifest(&manifest);
        assert!(result.is_err());

        let errors = result.unwrap_err();
        assert!(errors.iter().any(
            |e| matches!(e, ManifestValidationError::InvalidValue { field, .. } if field == "max_tokens")
        ));
    }

    #[test]
    fn test_register_and_get_worker() {
        let mut registry = WorkerRegistry::new();
        let manifest = make_manifest("test-plugin", vec!["test_work".to_string()]);
        let worker = Arc::new(TestWorker { manifest });

        registry.register(worker).expect("register");

        let retrieved = registry.get("test_work");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().manifest().plugin_id, "test-plugin");
    }

    #[test]
    fn test_register_invalid_manifest_fails() {
        let mut registry = WorkerRegistry::new();
        let manifest = PluginManifest {
            plugin_id: String::new(),
            version: "1.0.0".to_string(),
            plugin_type: "test".to_string(),
            required_scopes: vec![],
            tool_access: vec![],
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
        };
        let worker = Arc::new(TestWorker { manifest });

        let result = registry.register(worker);
        assert!(result.is_err());
    }
}
