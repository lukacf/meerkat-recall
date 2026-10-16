//! Actual offline import and durable empty successor through the CLI certification seam.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::*;
use crate::data_migrate::historical_delta::integration_tests::{
    deleted_population_fixture, initial_population_fixture,
};
use elephant_storage::{DocNodeRepository, EntityRepository, Repository};
use elephant_types::provider_binding::{ModelOperation, OperationBinding};
use elephant_types::records::{NodeEmbedding, SpaceConfig};
use elephant_types::Link;

async fn stage(client: &StorageClient) -> SpaceConfig {
    let repo = SpaceConfigRepository::new(client.clone());
    let original = repo.get_by_space_id("household").await.unwrap().unwrap();
    let mut approved = original.clone();
    let processing = approved.processing.as_mut().unwrap();
    processing.generation += 1;
    processing.bindings.insert(
        ModelOperation::ReindexEmbedding,
        OperationBinding {
            id: "explicit-fixture-reindex-grant".into(),
            profile_id: "embedding".into(),
            revoked: false,
        },
    );
    let approved = repo.replace_authorized(&original, approved).await.unwrap();
    repo.rebuild_active_embedding_profile(&approved, "embedding")
        .await
        .unwrap()
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original configuration reader through staged vectors, exact progress CAS and activation before returning fixture identities."
)]
async fn activate(
    client: &StorageClient,
    root: &Path,
    include_vectors: bool,
    empty: bool,
) -> (String, String) {
    let staged = stage(client).await;
    let repo = SpaceConfigRepository::new(client.clone());
    let id = staged
        .processing
        .as_ref()
        .unwrap()
        .pending_reindex
        .as_ref()
        .unwrap()
        .id
        .clone();
    let mut node_count = 0;
    let mut entity_count = 0;
    if !empty {
        let originals: Value =
            serde_json::from_slice(&std::fs::read(root.join("operational.json")).unwrap()).unwrap();
        let binding = repo
            .authorize_model_operation("household", ModelOperation::ReindexEmbedding, None)
            .await
            .unwrap();
        let policy = elephant_embeddings::governed::policy_for_binding(&binding).unwrap();
        let mut vectors = Vec::new();
        for original in originals["doc_node"].as_array().unwrap() {
            let node_id: RecordId = original["id"].as_str().unwrap().parse().unwrap();
            let node = DocNodeRepository::new(client.clone())
                .get("household", &node_id)
                .await
                .unwrap()
                .unwrap();
            vectors.push(NodeEmbedding {
                base: node.base.clone(),
                node: Link::new(node.base.id),
                model: policy.model.clone(),
                dims: 3,
                vector: vec![1.0, 0.0, 0.0],
            });
        }
        node_count = vectors.len() as u64;
        if include_vectors {
            elephant_storage::NodeEmbeddingRepository::new(
                client.clone(),
                elephant_embeddings::PartitionManager::new(),
            )
            .store_reindex_embeddings(&staged, vectors)
            .await
            .unwrap();
        }
        for original in originals["entity"].as_array().unwrap() {
            let entity_id: RecordId = original["id"].as_str().unwrap().parse().unwrap();
            let entity = EntityRepository::new(client.clone())
                .get("household", &entity_id)
                .await
                .unwrap()
                .unwrap();
            entity_count += 1;
            if include_vectors {
                elephant_storage::entity_profile_embedding_repository::EntityProfileEmbeddingRepository::new(client.clone())
                    .store(&staged, &entity, &policy.model, &elephant_storage::entity_profile_embedding_repository::entity_input_hash(&entity), vec![1.0, 0.0, 0.0]).await.unwrap();
            }
        }
    }
    // The missing-vector negative deliberately advances counters without vectors:
    // certification must independently inspect physical current partitions.
    let nodes = repo
        .record_embedding_reindex_page(&staged, None, node_count, true)
        .await
        .unwrap();
    let entities = repo
        .record_embedding_reindex_page(&nodes, None, entity_count, true)
        .await
        .unwrap();
    let active = repo
        .activate_embedding_reindex(&entities, &id)
        .await
        .unwrap();
    (active.base.id.to_string(), id)
}

fn args(
    root: &Path,
    empty: bool,
    config: String,
    reindex: String,
    output: &str,
) -> CertifyHistoricalArgs {
    let report = root.join(if empty {
        "delete-last.json"
    } else {
        "initial.json"
    });
    CertifyHistoricalArgs {
        target: crate::data_migrate::TransferTarget {
            url: Some(format!("rocksdb://{}", root.join("target").display())),
            namespace: None,
            database: None,
        },
        space: "household".into(),
        baseline_report_sha256: hash(&std::fs::read(&report).unwrap()),
        bindings: report.with_extension("catchup-bindings.jsonl"),
        baseline_report: report,
        configuration_id: config,
        reindex_id: reindex,
        output: root.join(output),
        max_elapsed_seconds: 60,
    }
}

#[tokio::test]
async fn actual_import_vectors_activation_certification_and_exact_resume() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let client = initial_population_fixture(root).await;
    let (config, reindex) = activate(&client, root, true, false).await;
    let args = args(root, false, config, reindex, "certification");
    run_inner(args.clone(), Some(client.clone())).await.unwrap();
    let complete = std::fs::read(args.output.join("complete.json")).unwrap();
    let parsed: Value = serde_json::from_slice(&complete).unwrap();
    assert_eq!(
        parsed["state"],
        "all_bound_current_imported_publications_certified"
    );
    assert_eq!(parsed["completed"], 3); // two document groups and one zero-document snapshot group
    assert_eq!(parsed["empty_population"], false);
    run_inner(args.clone(), Some(client.clone())).await.unwrap();
    assert_eq!(
        std::fs::read(args.output.join("complete.json")).unwrap(),
        complete
    );
    let mut wrong = args.clone();
    wrong.configuration_id = RecordId::new_v4(TableName::SpaceConfig).to_string();
    wrong.output = root.join("wrong-configuration");
    assert!(run_inner(wrong.clone(), Some(client.clone()))
        .await
        .is_err());
    assert!(!wrong.output.join("complete.json").exists());
    let mut wrong = args.clone();
    wrong.baseline_report_sha256 = "f".repeat(64);
    wrong.output = root.join("wrong-report");
    assert!(run_inner(wrong.clone(), Some(client.clone()))
        .await
        .is_err());
    assert!(!wrong.output.join("complete.json").exists());
    let mut wrong = args;
    wrong.output = root.join("wrong-bindings");
    wrong.bindings = root.join("forged-bindings.jsonl");
    std::fs::write(&wrong.bindings, b"").unwrap();
    assert!(run_inner(wrong.clone(), Some(client)).await.is_err());
    assert!(!wrong.output.join("complete.json").exists());
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original client through refusal, absent completion report and final ready-state assertions."
)]
async fn certification_refuses_activated_counters_without_actual_current_vectors() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let client = initial_population_fixture(root).await;
    let (config, reindex) = activate(&client, root, false, false).await;
    let args = args(root, false, config, reindex, "missing-vectors");
    assert!(run_inner(args.clone(), Some(client.clone())).await.is_err());
    assert!(!args.output.join("complete.json").exists());
    assert!(client.recovery_status().ready);
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep original client, manifest and configuration readers through partial failure, exact native repair, successful replay and unchanged receipt/configuration assertions."
)]
async fn partial_certification_repairs_one_exact_vector_and_replays_prior_receipt() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let client = initial_population_fixture(root).await;
    let (config, reindex) = activate(&client, root, true, false).await;
    let args = args(root, false, config, reindex, "partial-certification");
    let (_, sources) = input(&args).unwrap();
    let manifests = SourcePublicationManifestRepository::new(client.clone());
    // Omit a node from the last document group in the caller's actual source-ID
    // order, ensuring at least one independently complete source precedes it.
    let mut selected = None;
    for (position, source) in sources.values().enumerate() {
        let fence = PublicationFence {
            source_id: source.source_id.clone(),
            generation: source.head.generation.clone(),
            revision_id: source.head.published_revision.clone().unwrap(),
            operation_id: source.head.operation_id.clone(),
        };
        let manifest = manifests.get("household", &fence).await.unwrap().unwrap();
        if let Some(document) = manifest.documents.first() {
            selected = Some((position, document.doc_revision_id.clone()));
        }
    }
    let (position, revision) = selected.expect("document group");
    assert!(position > 0);
    let originals: Value =
        serde_json::from_slice(&std::fs::read(root.join("operational.json")).unwrap()).unwrap();
    let node_id: RecordId = originals["doc_node"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["revision"] == revision.to_string())
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let node = DocNodeRepository::new(client.clone())
        .get("household", &node_id)
        .await
        .unwrap()
        .unwrap();
    let configs = SpaceConfigRepository::new(client.clone());
    let active = configs.get_by_space_id("household").await.unwrap().unwrap();
    let binding = configs
        .authorize_model_operation("household", ModelOperation::QueryEmbedding, None)
        .await
        .unwrap();
    let policy = elephant_embeddings::governed::policy_for_binding(&binding).unwrap();
    let table = elephant_embeddings::PartitionKey::new(
        node.base.security.policy_id.clone(),
        elephant_embeddings::level_bucket(node.base.security.level),
        policy.model,
        3,
    )
    .unwrap()
    .table_name();
    let query = format!("SELECT VALUE type::string($this) FROM {table} WHERE node = $node;");
    let mut response = client
        .query_bind_checked(
            &query,
            (
                "node",
                elephant_storage::surreal::thing_from_record_id(&node_id),
            ),
        )
        .await
        .unwrap();
    let original_rows: Vec<String> = response.take(0).unwrap();
    assert_eq!(original_rows.len(), 1);
    client
        .query_bind_checked(
            &format!("DELETE FROM {table} WHERE node = $node;"),
            (
                "node",
                elephant_storage::surreal::thing_from_record_id(&node_id),
            ),
        )
        .await
        .unwrap();
    assert!(run_inner(args.clone(), Some(client.clone())).await.is_err());
    assert!(!args.output.join("complete.json").exists());
    let mut retained = Vec::new();
    let mut partial = None;
    for entry in std::fs::read_dir(&args.output).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap().to_str().unwrap();
        if name.starts_with("source-") {
            retained.push((path.clone(), std::fs::read(&path).unwrap()));
        } else if name.starts_with("attempt-") {
            let report: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
            if report["state"] == "partial_failure" {
                partial = Some(report);
            }
        }
    }
    assert_eq!(retained.len(), position);
    assert_eq!(
        partial.expect("actual partial attempt")["completed"],
        position
    );
    // Fault repair restores exactly the saved native row, including timestamps
    // and native types. This is fixture-only repair, not a new product bypass.
    client
        .query_checked(&format!("INSERT INTO {table} {};", original_rows[0]))
        .await
        .unwrap();
    let mut response = client
        .query_bind_checked(
            &query,
            (
                "node",
                elephant_storage::surreal::thing_from_record_id(&node_id),
            ),
        )
        .await
        .unwrap();
    let repaired: Vec<String> = response.take(0).unwrap();
    assert_eq!(repaired, original_rows);
    assert_eq!(
        configs.get_by_space_id("household").await.unwrap().unwrap(),
        active
    );
    run_inner(args.clone(), Some(client.clone())).await.unwrap();
    assert!(args.output.join("complete.json").exists());
    for (path, bytes) in retained {
        assert_eq!(std::fs::read(path).unwrap(), bytes);
    }
    assert_eq!(
        configs.get_by_space_id("household").await.unwrap().unwrap(),
        active
    );
}

#[tokio::test]
async fn delete_all_completed_certificate_survives_activation_but_forged_empty_report_refuses() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let client = Box::pin(deleted_population_fixture(root)).await;
    let original: Value =
        serde_json::from_slice(&std::fs::read(root.join("delete-last.json")).unwrap()).unwrap();
    let (config, reindex) = activate(&client, root, false, true).await;
    let current_epoch =
        elephant_storage::canonical_commit::CanonicalRepository::new(client.clone())
            .observe_epoch("household")
            .await
            .unwrap()
            .unwrap();
    assert_ne!(
        original["catchup_baseline"]["expected_epoch"],
        current_epoch
    );
    let args = args(root, true, config, reindex, "certify-empty");
    let mut forged = original;
    forged["catchup_baseline"]["expected_epoch"] = current_epoch.into();
    let mut bad = args.clone();
    bad.baseline_report = root.join("forged-empty.json");
    let bytes = serde_json::to_vec_pretty(&forged).unwrap();
    std::fs::write(&bad.baseline_report, &bytes).unwrap();
    bad.baseline_report_sha256 = hash(&bytes);
    bad.output = root.join("forged-empty-output");
    assert!(run_inner(bad.clone(), Some(client.clone())).await.is_err());
    assert!(!bad.output.join("complete.json").exists());
    run_inner(args.clone(), Some(client.clone())).await.unwrap();
    let complete: Value =
        serde_json::from_slice(&std::fs::read(args.output.join("complete.json")).unwrap()).unwrap();
    assert_eq!(complete["empty_population"], true);
    assert_eq!(complete["completed"], 0);
    assert_eq!(
        complete["state"],
        "all_bound_current_imported_publications_certified"
    );
    run_inner(args, Some(client)).await.unwrap();
}
