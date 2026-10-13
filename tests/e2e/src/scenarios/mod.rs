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
pub mod m10_work_queue;
pub mod m1_tenant_isolation;
pub mod m2_startup_ordering;
pub mod m4_cursor_stability;
pub mod m6_outbox_monotonicity;
pub mod m7_space_embedding_policy;
pub mod m8_assertion_pipeline;
pub mod m9_truth_maintenance;
pub mod s1_1_entity_lifecycle;
pub mod s1_2_rel_traversal;
pub mod s1_4_identity_key_sot;
pub mod s1_8_audit_inheritance;
pub mod s2_1_zombie_key;
pub mod s2_2_amnesia_merge;
pub mod s2_4_partial_success;
pub mod s2_5_masked_truth;
pub mod s3_1_manifest_repointing;
pub mod s4_10_reprocessing;
pub mod s4_11_massive_doc;
pub mod s4_12_implicit_now;
pub mod s4_13_outbox_triggered;
pub mod s4_14_abac_inheritance;
pub mod s4_1_progressive_triage;
pub mod s4_2_assertion_lifecycle;
pub mod s4_3_predicate_governance;
pub mod s4_4_truth_maintenance;
pub mod s4_5_budget_enforcement;
pub mod s4_6_temporal_supersession;
pub mod s4_7_abstention_retry;
pub mod s4_8_staged_commit;
pub mod s4_9_freshness_sweep;
pub mod s5_10_relationship_evidence;
pub mod s5_11_space_management;
pub mod s5_12_batch_progress;
pub mod s5_13_alice_in_wonderland;
pub mod s5_14_events_timeline;
pub mod s5_15_wizard_of_oz;
pub mod s5_16_elephant_session_log;
pub mod s5_17_source_first_ingest;
pub mod s5_18_multimodal_real_media;
pub mod s5_19_truth_adjudication;
pub mod s5_1_ingest_doc_simple;
pub mod s5_2_create_entity_simple;
pub mod s5_3_type_coercion;
pub mod s5_4_extraction_pipeline;
pub mod s5_5_manual_extraction;
pub mod s5_6_pipeline_status;
pub mod s5_7_search_all;
pub mod s5_8_file_ingestion;
pub mod s5_9_tool_introspection;

/// Explicit named-space fixture for scenarios that exercise public admission.
/// Production admission must continue to refuse unregistered spaces.
#[cfg(test)]
pub(crate) async fn register_scenario_space(
    storage: &elephant_storage::StorageClient,
    space: &str,
) -> anyhow::Result<()> {
    use elephant_storage::Repository;
    use elephant_types::records::{base::BaseFields, SpaceConfig};
    use elephant_types::{RecordId, TableName};
    elephant_storage::SpaceConfigRepository::new(storage.clone())
        .create(SpaceConfig {
            base: BaseFields::new(
                RecordId::new_v4(TableName::SpaceConfig),
                space.into(),
                "scenario-fixture".into(),
            ),
            embedding_provider: "fake".into(),
            embedding_model: "fixture".into(),
            embedding_dims: 8,
            processing: None,
            purpose: None,
            skill: None,
        })
        .await?;
    Ok(())
}
