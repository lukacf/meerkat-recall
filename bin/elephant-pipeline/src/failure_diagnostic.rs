//! Bounded diagnostics from typed causes retained beneath anyhow context.
//!
//! These codes explain a failed stage. They do not classify retries, change
//! authority, or copy nested provider messages, source text, URLs, or secrets.

use elephant_pipeline::PipelineError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct FailureDiagnostic {
    pub(super) kind: &'static str,
    pub(super) stage: &'static str,
    pub(super) code: &'static str,
}

impl FailureDiagnostic {
    pub(super) fn from_error(error: &anyhow::Error) -> Self {
        error
            .chain()
            .take(16)
            .find_map(|cause| {
                cause
                    .downcast_ref::<PipelineError>()
                    .map(Self::from_pipeline)
            })
            .unwrap_or(Self::new("other", "unknown", "unclassified"))
    }

    const fn new(kind: &'static str, stage: &'static str, code: &'static str) -> Self {
        Self { kind, stage, code }
    }

    fn from_pipeline(error: &PipelineError) -> Self {
        match error {
            PipelineError::ExtractionError(message) => Self::extraction(message),
            PipelineError::ConfigError(message) => {
                if message.starts_with("provider_authorization: ") {
                    Self::new(
                        "configuration",
                        "provider_authorization",
                        "provider_authorization",
                    )
                } else {
                    Self::new("configuration", "configuration", "configuration_error")
                }
            }
            PipelineError::ValidationError(message) => match message.as_str() {
                "Grounding revision is missing or deleted"
                | "Grounding node is missing or deleted"
                | "Grounding document is missing or deleted"
                | "Grounding document has no source"
                | "Grounding source is missing or deleted" => {
                    Self::new("validation", "lineage_lookup", "grounding_record_missing")
                }
                "Grounding source authorization lineage changed or was deleted"
                | "Grounding node is deleted or its revision/authorization lineage changed" => {
                    Self::new(
                        "validation",
                        "lineage_validation",
                        "grounding_authorization_changed",
                    )
                }
                "Grounding lineage does not identify one active source revision"
                | "Extraction node belongs to another revision" => Self::new(
                    "validation",
                    "lineage_validation",
                    "grounding_revision_mismatch",
                ),
                "Grounding revision content changed or its hash is invalid"
                | "Grounding revision lacks an exact current source-content binding"
                | "Grounding node has an invalid UTF-8 evidence range" => Self::new(
                    "validation",
                    "lineage_validation",
                    "grounding_content_invalid",
                ),
                _ => Self::new("validation", "validation", "validation_error"),
            },
            PipelineError::StorageError(_) => Self::new("storage", "storage", "storage_error"),
            PipelineError::OutboxError(_) => Self::new("outbox", "outbox", "outbox_error"),
            PipelineError::EmbeddingError(_) => {
                Self::new("embedding", "embedding", "embedding_error")
            }
            PipelineError::IngestionError(_) => {
                Self::new("ingestion", "ingestion", "ingestion_error")
            }
            PipelineError::PromotionError(_) => {
                Self::new("promotion", "promotion", "promotion_error")
            }
            PipelineError::NotFound(_) => Self::new("not_found", "input_lookup", "input_not_found"),
            PipelineError::Duplicate(_) => Self::new("duplicate", "unknown", "duplicate"),
            PipelineError::InvalidStateTransition { .. } => {
                Self::new("state", "state", "invalid_transition")
            }
            PipelineError::CycleDetected => {
                Self::new("dependency", "dependencies", "cycle_detected")
            }
            PipelineError::LeaseConflict(_) => Self::new("lease", "lease", "lease_conflict"),
            PipelineError::SecurityViolation(_) => {
                Self::new("security", "authorization", "security_violation")
            }
            PipelineError::BudgetExceeded(_)
            | PipelineError::BudgetBreach { .. }
            | PipelineError::PerItemCapExceeded { .. } => {
                Self::new("budget", "budget", "budget_exceeded")
            }
            PipelineError::GroundingRepairRequired => Self::new(
                "grounding",
                "producer_validation",
                "grounding_repair_required",
            ),
            PipelineError::GroundingRepairExhausted => Self::new(
                "grounding",
                "repair_admission",
                "grounding_repair_exhausted",
            ),
            PipelineError::GroundingRepairEmpty => {
                Self::new("grounding", "producer_validation", "grounding_repair_empty")
            }
            PipelineError::InvalidInput(_) => {
                Self::new("input", "input_validation", "invalid_input")
            }
            PipelineError::InvalidState(_) => Self::new("state", "state", "invalid_state"),
        }
    }

    fn extraction(message: &str) -> Self {
        // Prefixes are fixed producer context from AgenticExtractor. Only the
        // corresponding static code is retained; the suffix is never copied.
        for (prefix, stage) in [
            ("MCP connection failed: ", "mcp_connect"),
            ("Invalid output schema: ", "output_schema"),
            ("Agent build failed: ", "agent_build"),
            ("Agent run failed: ", "agent_run"),
            (
                "Direct extraction agent build failed: ",
                "direct_agent_build",
            ),
            ("Direct extraction agent run failed: ", "direct_agent_run"),
            ("Support verifier build failed: ", "support_agent_build"),
            ("Support verifier failed: ", "support_agent_run"),
            ("Extraction agent terminated: ", "agent_terminal"),
            ("Structured extraction failed after ", "structured_output"),
        ] {
            if message.starts_with(prefix) {
                return Self::new("extraction", stage, stage);
            }
        }
        Self::new("extraction", "extractor", "extraction_error")
    }

    pub(super) fn as_json(self) -> serde_json::Value {
        serde_json::json!({ "version": 1, "kind": self.kind, "stage": self.stage, "code": self.code })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nested_extractor_stage_survives_outer_context_without_nested_text() {
        for (prefix, stage) in [
            ("MCP connection failed: ", "mcp_connect"),
            ("Agent build failed: ", "agent_build"),
            ("Agent run failed: ", "agent_run"),
            ("Support verifier failed: ", "support_agent_run"),
        ] {
            let error = anyhow::Error::new(PipelineError::ExtractionError(format!(
                "{prefix}SYNTHETIC_SECRET https://private.invalid/source original source text"
            )))
            .context("extract_assertions failed")
            .context("outer work context");
            let classification = super::super::classify_failure(&error);
            let diagnostic = FailureDiagnostic::from_error(&error);
            assert_eq!(diagnostic.kind, "extraction");
            assert_eq!(diagnostic.stage, stage);
            let serialized = diagnostic.as_json().to_string();
            assert!(!serialized.contains("SYNTHETIC_SECRET"));
            assert!(!serialized.contains("private.invalid"));
            assert!(!serialized.contains("original source text"));
            assert_eq!(super::super::classify_failure(&error), classification);
            assert_eq!(error.to_string(), "outer work context");
        }
    }

    #[test]
    fn typed_configuration_and_lineage_remain_distinct() {
        let configuration = anyhow::Error::new(PipelineError::ConfigError(
            "provider_authorization: SYNTHETIC_SECRET".into(),
        ))
        .context("extract_assertions failed");
        let lineage = anyhow::Error::new(PipelineError::ValidationError(
            "Grounding revision lacks an exact current source-content binding".into(),
        ))
        .context("extract_assertions failed");
        assert_eq!(
            FailureDiagnostic::from_error(&configuration).stage,
            "provider_authorization"
        );
        assert_eq!(
            FailureDiagnostic::from_error(&lineage).code,
            "grounding_content_invalid"
        );
        assert!(!FailureDiagnostic::from_error(&configuration)
            .as_json()
            .to_string()
            .contains("SYNTHETIC_SECRET"));
    }

    #[test]
    fn unknown_and_overdeep_causes_never_copy_error_messages() {
        let unknown = anyhow::anyhow!("SYNTHETIC_SECRET unknown source text");
        assert_eq!(FailureDiagnostic::from_error(&unknown).code, "unclassified");
        let validation =
            anyhow::Error::new(PipelineError::ValidationError("SYNTHETIC_SECRET".into()));
        assert_eq!(
            FailureDiagnostic::from_error(&validation).code,
            "validation_error"
        );
        let mut deep = anyhow::Error::new(PipelineError::ExtractionError(
            "Agent build failed: SYNTHETIC_SECRET".into(),
        ));
        for _ in 0..17 {
            deep = deep.context("bounded outer context");
        }
        assert_eq!(FailureDiagnostic::from_error(&deep).code, "unclassified");
        assert!(!FailureDiagnostic::from_error(&deep)
            .as_json()
            .to_string()
            .contains("SYNTHETIC_SECRET"));
    }
}
