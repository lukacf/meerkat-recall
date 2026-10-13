//! Secret-free observation of the exact managed-worker construction values.

use elephant_pipeline::scheduler::evidence_work_kinds::DocTreeConfig;
use elephant_pipeline_bin::config::PipelineConfig;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub(super) struct ManagedWorkerRecipe {
    format: &'static str,
    instance_id: String,
    backend: String,
    work_queue: QueueRecipe,
    /// Global worker construction template. Per-space governed provider policy
    /// remains independently resolved and must not be inferred from this value.
    embed_worker: EmbedRecipe,
}

#[derive(Debug, Clone, Serialize)]
struct QueueRecipe {
    enabled: bool,
    max_concurrent: usize,
    max_extract_concurrent: usize,
    work_item_timeout_secs: u64,
    transient_retry_max_attempts: u32,
    transient_retry_base_delay_secs: i64,
    transient_retry_max_delay_secs: i64,
    allowed_kinds: Vec<String>,
    disabled_kinds: Vec<String>,
    doc_tree: DocTreeConfig,
}

#[derive(Debug, Clone, Serialize)]
struct EmbedRecipe {
    enabled: bool,
    batch_size: usize,
    provider: String,
    model: String,
    dimensions: usize,
}

impl ManagedWorkerRecipe {
    /// Borrow the same config that is moved into `run_pipeline_with_shutdown`.
    /// This never reparses environment variables or resolves another default.
    pub(super) fn capture(config: &PipelineConfig) -> Self {
        let queue = &config.work_queue;
        Self {
            format: "recall-managed-worker-recipe/v1",
            instance_id: uuid::Uuid::new_v4().to_string(),
            backend: config.db_backend.as_str().into(),
            work_queue: QueueRecipe {
                enabled: queue.enabled,
                max_concurrent: queue.max_concurrent,
                max_extract_concurrent: queue.max_extract_concurrent,
                work_item_timeout_secs: queue.work_item_timeout_secs,
                transient_retry_max_attempts: queue.transient_retry_max_attempts,
                transient_retry_base_delay_secs: queue.transient_retry_base_delay_secs,
                transient_retry_max_delay_secs: queue.transient_retry_max_delay_secs,
                allowed_kinds: queue.allowed_kinds.clone(),
                disabled_kinds: queue.disabled_kinds.clone(),
                doc_tree: queue.doc_tree.clone(),
            },
            embed_worker: EmbedRecipe {
                enabled: config.embed.enabled,
                batch_size: config.embed.batch_size,
                provider: config.embed.provider.clone(),
                model: config.embed.model.clone(),
                dimensions: config.embed.dims,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    #[expect(
        clippy::field_reassign_with_default,
        reason = "Preserve the original default-first private configuration canary fixture."
    )]
    fn construction_snapshot_retains_original_nondefault_values_without_credentials() {
        let mut config = PipelineConfig::default();
        config.db_pass = "PRIVATE DATABASE PASSWORD CANARY".into();
        config.db_user = "PRIVATE DATABASE USER CANARY".into();
        config.db_url = "PRIVATE DATABASE ADDRESS CANARY".into();
        config.work_queue.transient_retry_max_attempts = 7;
        config.work_queue.max_concurrent = 5;
        config.work_queue.max_extract_concurrent = 2;
        config.work_queue.doc_tree.create_summaries = true;
        config.work_queue.doc_tree.max_chunk_size = 791;
        config.work_queue.doc_tree.summarizer_model = "actual-original-summary-model".into();
        config.embed.batch_size = 9;
        let snapshot = ManagedWorkerRecipe::capture(&config);
        config.work_queue.transient_retry_max_attempts = 1;
        config.work_queue.doc_tree.create_summaries = false;
        let value = serde_json::to_value(snapshot).unwrap();
        assert_eq!(value["work_queue"]["transient_retry_max_attempts"], 7);
        assert_eq!(value["work_queue"]["max_concurrent"], 5);
        assert_eq!(value["work_queue"]["max_extract_concurrent"], 2);
        assert_eq!(value["work_queue"]["doc_tree"]["create_summaries"], true);
        assert_eq!(value["work_queue"]["doc_tree"]["max_chunk_size"], 791);
        assert_eq!(
            value["work_queue"]["doc_tree"]["summarizer_model"],
            "actual-original-summary-model"
        );
        assert_eq!(value["embed_worker"]["batch_size"], 9);
        assert!(!value.to_string().contains("CANARY"));
        assert_eq!(value.as_object().unwrap().len(), 5);
    }

    #[test]
    fn distinct_worker_constructions_have_distinct_inspection_identity() {
        let config = PipelineConfig::default();
        let first = ManagedWorkerRecipe::capture(&config);
        let second = ManagedWorkerRecipe::capture(&config);
        assert_ne!(first.instance_id, second.instance_id);
        let first = serde_json::to_value(first).unwrap();
        let second = serde_json::to_value(second).unwrap();
        assert_eq!(first["work_queue"], second["work_queue"]);
        assert_eq!(first["embed_worker"], second["embed_worker"]);
    }
}
