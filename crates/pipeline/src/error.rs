//! Error types for the pipeline crate.

use thiserror::Error;

/// Errors in pipeline operations.
#[derive(Debug, Error)]
pub enum PipelineError {
    /// Ingestion error.
    #[error("Ingestion error: {0}")]
    IngestionError(String),

    /// Extraction error.
    #[error("Extraction error: {0}")]
    ExtractionError(String),

    /// SDK extraction failure with safe, fixed diagnostic codes. The original
    /// detail is retained unchanged for existing failure classification; it is
    /// not a safe log field or a public diagnostic.
    #[error("Extraction error: {detail}")]
    ExtractionFailure {
        /// Fixed producer stage, never provider-supplied text.
        stage: &'static str,
        /// Fixed cause selected from the SDK's typed error, not its message.
        code: &'static str,
        /// Original diagnostic context retained for failure classification.
        detail: String,
    },

    /// Promotion error.
    #[error("Promotion error: {0}")]
    PromotionError(String),

    /// Storage error.
    #[error("Storage error: {0}")]
    StorageError(#[from] elephant_storage::StorageError),

    /// Outbox error.
    #[error("Outbox error: {0}")]
    OutboxError(#[from] elephant_outbox::OutboxError),

    /// Embedding error.
    #[error("Embedding error: {0}")]
    EmbeddingError(#[from] elephant_embeddings::EmbeddingError),

    /// Duplicate detected.
    #[error("Duplicate: {0}")]
    Duplicate(String),

    /// Resource not found.
    #[error("Not found: {0}")]
    NotFound(String),

    /// Invalid state transition.
    #[error("Invalid state transition from {from} to {to}")]
    InvalidStateTransition {
        /// The current state.
        from: String,
        /// The attempted target state.
        to: String,
    },

    /// Cycle detected in DAG.
    #[error("Cycle detected in work item dependencies")]
    CycleDetected,

    /// Lease conflict - another worker acquired the lease.
    #[error("Lease conflict for item: {0}")]
    LeaseConflict(String),

    /// Security violation (ABAC, tool access, escalation).
    #[error("Security violation: {0}")]
    SecurityViolation(String),

    /// Configuration error.
    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// Budget exceeded error.
    #[error("Budget exceeded: {0}")]
    BudgetExceeded(String),

    /// Budget breach event (for alerting).
    #[error("Budget breach: {mode} budget breached by {amount} euros")]
    BudgetBreach {
        /// The mode that breached.
        mode: String,
        /// The amount of overrun.
        amount: f64,
    },

    /// Per-item cap exceeded.
    #[error("Per-item cap exceeded: {cap_type} ({actual} > {limit})")]
    PerItemCapExceeded {
        /// Type of cap (tokens, wall_time).
        cap_type: String,
        /// Actual value.
        actual: u32,
        /// Limit value.
        limit: u32,
    },

    /// Invalid input.
    #[error("Invalid input: {0}")]
    InvalidInput(String),

    /// Validation error.
    #[error("Validation error: {0}")]
    ValidationError(String),

    /// A complete structurally grounded proposal needs one bounded fresh extraction.
    #[error("grounding_repair_required")]
    GroundingRepairRequired,

    /// Automatic repair cannot remain within its original authority or hard limits.
    #[error("grounding_repair_exhausted")]
    GroundingRepairExhausted,

    /// A nonempty rejected proposal cannot be replaced by an empty repair.
    #[error("grounding_repair_empty")]
    GroundingRepairEmpty,

    /// Invalid state (for state machine violations).
    #[error("Invalid state: {0}")]
    InvalidState(String),
}

/// Result type for pipeline operations.
pub type PipelineResult<T> = Result<T, PipelineError>;
