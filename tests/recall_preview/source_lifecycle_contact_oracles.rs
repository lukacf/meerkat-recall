//! Independent D storage-composition oracle, included by the manifest tests.
//!
//! Source publication is real acceptance plus claimed canonical finalization.
//! This is not a full pipeline/HTTP/model-quality test; F and D own those separately.
use super::*;
use crate::canonical_commit::{
    CanonicalCommit, CanonicalCommitReceipt, CanonicalPublicationContext, CanonicalRecord,
    CanonicalRepository,
};
use crate::source_contact_intake::PreparedSourceContact;
use crate::source_publication::{AcceptSourceRevision, SourceMutation};
use elephant_types::publication::{
    OperationReceipt, OperationState, SourcePrecondition, SourceVisibility,
};
use elephant_types::records::base::SubjectsState;

const SPACE: &str = "independent-contact-erasure-transaction";

#[expect(
    clippy::format_collect,
    reason = "Keep this independent oracle's direct length-prefixed reference encoding."
)]
fn private_key(parts: &[&str]) -> String {
    let bytes = parts
        .iter()
        .map(|part| format!("{}:{part}", part.len()))
        .collect::<String>();
    Uuid::new_v5(&Uuid::NAMESPACE_OID, bytes.as_bytes()).to_string()
}

// The kernel returns fully qualified keys and caps each page at 101 rows.
// Traverse the original complete table within the independent 256-row bound.
async fn embedded_controls(
    storage: &StorageClient,
    table: &str,
) -> Vec<(String, serde_json::Value)> {
    let store = storage.embedded_store().unwrap();
    let mut rows = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..3 {
        let page = store
            .extension_rows_page(table, SPACE, cursor.as_deref(), 101)
            .await
            .unwrap();
        assert!(page.len() <= 101);
        assert!(page.windows(2).all(|pair| pair[0].0 < pair[1].0));
        assert!(page
            .first()
            .is_none_or(|(key, _)| cursor.as_ref().is_none_or(|prior| key > prior)));
        let complete = page.len() < 101;
        cursor = page.last().map(|(key, _)| key.clone());
        rows.extend(page);
        assert!(
            rows.len() <= 256,
            "independent private-control fixture bound"
        );
        if complete {
            return rows;
        }
    }
    panic!("bounded complete private-control inventory was not obtained");
}

async fn payload(storage: &StorageClient, table: &str, key: &str) -> String {
    assert!(matches!(
        table,
        "source_effect_support" | "source_commit_receipt"
    ));
    if storage.embedded_store().is_some() {
        let full_key = format!("{table}:{key}");
        let row = embedded_controls(storage, table)
            .await
            .into_iter()
            .find(|(id, _)| id == &full_key)
            .unwrap()
            .1;
        return row["payload"].as_str().unwrap().to_owned();
    }
    let mut response = storage
        .query_bind_checked(
            "SELECT VALUE payload FROM type::thing($table,$key)",
            BTreeMap::from([("table", table.to_owned()), ("key", key.to_owned())]),
        )
        .await
        .unwrap();
    let rows: Vec<String> = response.take(0).unwrap();
    assert_eq!(rows.len(), 1);
    rows[0].clone()
}

async fn replace_owned_fault(storage: &StorageClient, key: &str, before: &str, after: &str) {
    assert_eq!(payload(storage, "source_effect_support", key).await, before);
    if let Some(store) = storage.embedded_store() {
        let full_key = format!("source_effect_support:{key}");
        let mut row = embedded_controls(storage, "source_effect_support")
            .await
            .into_iter()
            .find(|(id, _)| id == &full_key)
            .unwrap()
            .1;
        row["payload"] = serde_json::json!(after);
        store
            .mutate(|tx| Ok(tx.upsert_extension_row("source_effect_support", row.clone())?))
            .await
            .unwrap();
    } else {
        storage.query_bind_checked(
            "UPDATE type::thing('source_effect_support',$key) SET payload=$after WHERE payload=$before",
            BTreeMap::from([("key",key.to_owned()),("before",before.to_owned()),("after",after.to_owned())]),
        ).await.unwrap();
    }
    assert_eq!(payload(storage, "source_effect_support", key).await, after);
}

// Raw physical inventory includes tombstones; no public eligibility projection.
async fn physical_graph(storage: &StorageClient) -> Vec<String> {
    let mut result = Vec::new();
    for table in [
        TableName::Entity,
        TableName::EntityIdentityKey,
        TableName::Rel,
        TableName::Source,
    ] {
        if let Some(store) = storage.embedded_store() {
            let rows = store
                .mutate(|tx| Ok(tx.scan_raw_rows(table)?))
                .await
                .unwrap();
            for (_, mut row) in rows {
                if row["space_id"] == SPACE {
                    row.sort_all_objects();
                    result.push(format!("{}:{row}", table.as_str()));
                }
            }
        } else {
            let mut response = storage
                .query_bind_checked(
                    &format!("SELECT * FROM {} WHERE space_id=$space", table.as_str()),
                    BTreeMap::from([("space", SPACE.to_owned())]),
                )
                .await
                .unwrap();
            let rows: surrealdb::Value = response.take(0).unwrap();
            let surrealdb::sql::Value::Array(rows) = rows.into_inner() else {
                panic!("physical table inventory is not an array")
            };
            result.extend(
                rows.0
                    .into_iter()
                    .map(|row| format!("{}:{row}", table.as_str())),
            );
        }
    }
    assert!(result.len() < 100, "independent fixture inventory bound");
    result.sort();
    result
}

async fn accepted_contact(
    storage: &StorageClient,
    external: &str,
    foreign: bool,
) -> OperationReceipt {
    let mut source = Source::new(
        base(TableName::Source, SPACE),
        SourceType::Manual,
        format!("contact-content:{external}"),
    );
    source.external_id = Some(external.into());
    source.base.subjects_state = SubjectsState::Complete;
    source.set_requested_ingest_mode(IngestMode::Metadata);
    source.metadata = if foreign {
        serde_json::json!({"contact":{"name":"Foreign Fern","email":"fern@example.invalid","company":"Foreign Company"}})
    } else {
        serde_json::json!({"contact":{"name":"Shared Casey","email":"casey@example.invalid","company":"Shared Company","job_title":"Engineer"}})
    };
    let contact = Box::pin(PreparedSourceContact::prepare(storage, &source))
        .await
        .unwrap()
        .unwrap();
    Box::pin(
        SourcePublicationRepository::new(storage.clone()).accept_with_prepared_contact(
            AcceptSourceRevision {
                source,
                revision_id: format!("{external}-r1"),
                expected: SourcePrecondition::Absent,
                idempotency_key: format!("accept:{external}"),
                request_fingerprint: format!("fixed-contact:{external}"),
                reactivate: false,
                recreate_after_erasure: false,
                authorized: None,
                authorized_document: None,
            },
            &contact,
        ),
    )
    .await
    .unwrap()
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete fault and recovery sequence together with its original assertions."
)]
async fn publish_contact(
    storage: &StorageClient,
    accepted: &OperationReceipt,
) -> CanonicalCommitReceipt {
    let mut manifest = SourcePublicationManifest {
        space_id: SPACE.into(),
        publication: PublicationFence {
            source_id: accepted.source_id.clone(),
            generation: accepted.source_generation.clone(),
            revision_id: accepted.desired_revision.clone().unwrap(),
            operation_id: accepted.operation_id.clone(),
        },
        mode: IngestMode::Metadata,
        configuration: crate::SpaceConfigRepository::new(storage.clone())
            .resolve_space(SPACE)
            .await
            .unwrap()
            .token,
        embedding_profile: None,
        documents: vec![],
        metadata_artifact_ids: vec![],
        planning_artifact_ids: vec![],
        bundles: vec![],
        embeddings: vec![],
        embedding_work_item_ids: vec![],
    };
    // Perform the fixed metadata stage under an actual claim and original run.
    // Do not use the parent's create_successful_origin synthetic Done helper.
    let producer = claim_plan_work(
        storage,
        &manifest,
        "independent_fixed_metadata",
        plan_payload(&manifest),
    )
    .await;
    let attempt = crate::work_attempt_repository::fence_from_claim(&producer).unwrap();
    let mut metrics = elephant_types::records::work::WorkRunMetrics::default();
    metrics.custom.insert(
        "attempt_fence".into(),
        serde_json::to_value(&attempt).unwrap(),
    );
    let runs = crate::WorkRunRepository::new(storage.clone());
    let mut run = runs
        .create(WorkRun {
            base: base(TableName::WorkRun, SPACE),
            work_item: Link::new(producer.base.id.clone()),
            attempt: producer.attempts,
            started_at: Utc::now(),
            completed_at: None,
            success: false,
            error: None,
            metrics,
            worker_id: "replan-fixture".into(),
            worker_version: "independent-oracle".into(),
            model_version: None,
            prompt_id: None,
            config_hash: None,
        })
        .await
        .unwrap();
    let metadata = serde_json::json!({"source_id":manifest.publication.source_id.to_string()});
    let mut candidate = artifact(SPACE, "source_metadata", metadata.clone());
    candidate.work_run = Link::new(run.base.id.clone());
    candidate
        .data
        .as_object_mut()
        .unwrap()
        .extend(plan_payload(&manifest).as_object().unwrap().clone());
    let mut bundle = artifact(
        SPACE,
        "commit_bundle",
        serde_json::json!({
            "publication":manifest.publication,"source_artifact_id":candidate.base.id.to_string(),
            "source_artifact_type":"source_metadata","bundle_kind":"source_metadata",
            "payload":{"kind":"source_metadata","data":metadata},
        }),
    );
    bundle.work_run = Link::new(run.base.id.clone());
    let artifacts = crate::WorkArtifactRepository::new(storage.clone());
    artifacts.create(candidate.clone()).await.unwrap();
    artifacts.create(bundle.clone()).await.unwrap();
    run.success = true;
    run.completed_at = Some(Utc::now());
    runs.update(SPACE, &run.base.id.clone(), run).await.unwrap();
    complete_claim(storage, &producer).await;
    manifest.metadata_artifact_ids = vec![bundle.base.id.clone()];
    let mut register_payload = plan_payload(&manifest);
    register_payload["work_run_id"] = serde_json::json!(bundle.work_run.id().to_string());
    let registration =
        claim_plan_work(storage, &manifest, "commit_artifacts", register_payload).await;
    let manifests = SourcePublicationManifestRepository::new(storage.clone());
    manifests
        .register_bundle(
            SPACE,
            &manifest.publication,
            &bundle.base.id,
            &crate::work_attempt_repository::fence_from_claim(&registration).unwrap(),
        )
        .await
        .unwrap();
    complete_claim(storage, &registration).await;
    manifests
        .set_operation_plan(manifest.clone())
        .await
        .unwrap();
    let finalizer = claim_plan_work(
        storage,
        &manifest,
        "finalize_source_revision",
        plan_payload(&manifest),
    )
    .await;
    let attempt = crate::work_attempt_repository::fence_from_claim(&finalizer).unwrap();
    let requirements =
        Box::pin(manifests.requirements_for_attempt(SPACE, &manifest.publication, &attempt))
            .await
            .unwrap();
    let canonical = CanonicalRepository::new(storage.clone());
    let mut plan = Box::pin(canonical.prepare_source_replacement(SPACE, &manifest.publication))
        .await
        .unwrap();
    Box::pin(canonical.append_accepted_source_effects(&mut plan, SPACE, &manifest.publication))
        .await
        .unwrap();
    for id in [&bundle.base.id, &candidate.base.id] {
        let before = artifacts.get(SPACE, id).await.unwrap().unwrap();
        let mut after = before.clone();
        after.status = ArtifactStatus::Committed;
        after.committed_at = Some(Utc::now());
        after.base.updated_at = Utc::now();
        plan.push(
            Some(CanonicalRecord::WorkArtifact(Box::new(before))),
            Some(CanonicalRecord::WorkArtifact(Box::new(after))),
        )
        .unwrap();
    }
    let before = crate::SourceRepository::new(storage.clone())
        .get(SPACE, &manifest.publication.source_id)
        .await
        .unwrap()
        .unwrap();
    let mut after = before.clone();
    after.current_ingest_mode = Some(IngestMode::Metadata);
    after.lifecycle_status = elephant_types::records::source::SourceLifecycleStatus::MetadataReady;
    after.base.updated_at = Utc::now();
    plan.push(
        Some(CanonicalRecord::Source(Box::new(before))),
        Some(CanonicalRecord::Source(Box::new(after))),
    )
    .unwrap();
    let committed = Box::pin(canonical.commit(CanonicalCommit {
        space_id: SPACE.into(),
        commit_id: requirements.commit_id(),
        input_fingerprint: manifest.digest().unwrap(),
        replay_output: None,
        publication: Some(CanonicalPublicationContext {
            source: manifest.publication.clone(),
            attempt,
        }),
        plan,
        finalization: Some(requirements),
        maintenance: None,
    }))
    .await
    .unwrap();
    complete_claim(storage, &finalizer).await;
    assert_eq!(
        SourcePublicationRepository::new(storage.clone())
            .operation(SPACE, &accepted.operation_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        OperationState::Completed
    );
    assert_eq!(
        manifests
            .completion(SPACE, &manifest.publication)
            .await
            .unwrap()
            .unwrap()
            .commit_id,
        committed.commit_id
    );
    committed
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete fault and recovery sequence together with its original assertions."
)]
async fn corrupt_survivor_rolls_back_physical_purge_and_recovers_original_intent(embedded: bool) {
    let storage = client(embedded).await;
    if !embedded {
        storage
            .query_checked(include_str!(
                "../../crates/migrations/sql/040_source_contact_intent.surql"
            ))
            .await
            .unwrap();
    }
    let configuration_base = base(TableName::SpaceConfig, SPACE);
    let policy_id = configuration_base.security.policy_id.clone();
    crate::SpaceConfigRepository::new(storage.clone())
        .create(SpaceConfig {
            base: configuration_base,
            embedding_provider: "fake".into(),
            embedding_model: "unused-metadata-only".into(),
            embedding_dims: 8,
            processing: Some(SpaceProcessingConfig {
                generation: 1,
                index_generation: 0,
                policy_id,
                connections: BTreeMap::new(),
                profiles: BTreeMap::new(),
                bindings: BTreeMap::new(),
                active_embedding_profile_id: None,
                pending_reindex: None,
            }),
            purpose: None,
            skill: None,
        })
        .await
        .unwrap();
    let first = Box::pin(accepted_contact(&storage, "a", false)).await;
    let first_receipt = Box::pin(publish_contact(&storage, &first)).await;
    let second = Box::pin(accepted_contact(&storage, "b", false)).await;
    let second_receipt = Box::pin(publish_contact(&storage, &second)).await;
    let foreign = Box::pin(accepted_contact(&storage, "foreign", true)).await;
    let foreign_receipt = Box::pin(publish_contact(&storage, &foreign)).await;
    let publications = SourcePublicationRepository::new(storage.clone());
    let relations = crate::RelRepository::new(storage.clone())
        .list(SPACE, 100, None)
        .await
        .unwrap();
    let shared = relations
        .into_iter()
        .find(|row| {
            row.base.provenance.sources.contains(&first.source_id)
                && row.base.provenance.sources.contains(&second.source_id)
        })
        .unwrap();
    assert!(shared.evidence.is_empty());
    assert!(!foreign_receipt.effect_ids.contains(&shared.base.id));
    for source in [&first.source_id, &second.source_id] {
        assert!(Box::pin(
            CanonicalRepository::new(storage.clone())
                .accepted_contact_rel_support(SPACE, &shared, source)
        )
        .await
        .unwrap());
    }
    let support = publications
        .support_for_record(SPACE, &shared.base.id)
        .await
        .unwrap()
        .into_iter()
        .find(|row| row.source_id == second.source_id)
        .unwrap();
    let key = private_key(&[
        SPACE,
        &support.source_id.to_string(),
        &support.source_revision,
        &support.record_id.to_string(),
    ]);
    let original = payload(&storage, "source_effect_support", &key).await;
    let mut corrupted: serde_json::Value = serde_json::from_str(&original).unwrap();
    corrupted["last_commit_id"] = serde_json::json!(foreign_receipt.commit_id);
    let corrupted = corrupted.to_string();
    let graph = physical_graph(&storage).await;
    let first_head = publications
        .head(SPACE, &first.source_id)
        .await
        .unwrap()
        .unwrap();
    let second_head = publications.head(SPACE, &second.source_id).await.unwrap();
    let mut original_receipts = Vec::new();
    for receipt in [&first_receipt, &second_receipt, &foreign_receipt] {
        let key = private_key(&[SPACE, &receipt.commit_id]);
        original_receipts.push((
            key.clone(),
            payload(&storage, "source_commit_receipt", &key).await,
        ));
    }
    assert!(storage
        .erasure_journal()
        .unwrap()
        .intents()
        .unwrap()
        .is_empty());
    let request = SourceMutation {
        space_id: SPACE.into(),
        source_id: first.source_id.clone(),
        expected: SourcePrecondition::Generation(first_head.generation.clone()),
        idempotency_key: "independent-corrupt-survivor-erase".into(),
        request_fingerprint: "independent-corrupt-survivor-erase".into(),
        actor: "independent-D".into(),
        authorized: publications
            .authorization_snapshot(SPACE, &first.source_id)
            .await
            .unwrap()
            .unwrap(),
        authorized_document: None,
    };
    replace_owned_fault(&storage, &key, &original, &corrupted).await;
    let refused = Box::pin(publications.erase_live(request))
        .await
        .unwrap_err();
    assert!(
        matches!(refused,crate::StorageError::Conflict(ref reason) if reason == "contact_survival_support_writer_changed"),
        "wrong destructive proof refusal: {refused}"
    );
    assert_eq!(
        storage.recovery_status().phase,
        crate::recovery_barrier::RecoveryPhase::Failed
    );
    assert!(!storage.recovery_status().ready);
    assert!(crate::EntityRepository::new(storage.clone())
        .list(SPACE, 100, None)
        .await
        .is_err());
    let intents = storage.erasure_journal().unwrap().intents().unwrap();
    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0].source_id, first.source_id);
    assert_eq!(intents[0].space_id, SPACE);
    let original_intents = intents.clone();
    let original_source = first.source_id.clone();
    let surviving_source = second.source_id.clone();
    let rel_id = shared.base.id.clone();
    // The same existing private exclusive scope inspects, repairs only the
    // injected fault and actually finishes original recovery before returning Ok.
    // An inspection-only success here would incorrectly reset readiness.
    Box::pin(storage.with_erasure_barrier(move |exclusive| async move {
        assert_eq!(physical_graph(&exclusive).await, graph);
        let repo = SourcePublicationRepository::new(exclusive.clone());
        assert_eq!(
            repo.head(SPACE, &surviving_source).await.unwrap(),
            second_head
        );
        let reserved = repo.head(SPACE, &original_source).await.unwrap().unwrap();
        assert_eq!(reserved.lifecycle, SourceVisibility::Erasing);
        assert_ne!(reserved.generation, first_head.generation);
        assert_ne!(reserved.operation_id, first_head.operation_id);
        assert_eq!(reserved.published_revision, None);
        let pending = repo
            .operation(SPACE, &reserved.operation_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(pending.state, OperationState::Erasing);
        assert!(pending.completed_at.is_none());
        for (receipt_key, before) in &original_receipts {
            assert_eq!(
                payload(&exclusive, "source_commit_receipt", receipt_key).await,
                *before
            );
        }
        assert_eq!(
            payload(&exclusive, "source_effect_support", &key).await,
            corrupted
        );
        assert_eq!(exclusive.erasure_journal()?.intents()?, original_intents);
        // Planning refused before any erased reference membership was persisted.
        // Restore our injected fault under the exact existing reservation owner;
        // the maintenance lease alone deliberately does not authorize this write.
        assert!(exclusive
            .erasure_journal()?
            .reference_membership(&original_intents[0])?
            .is_empty());
        exclusive
            .with_erasure_reservation(&original_intents[0], async {
                replace_owned_fault(&exclusive, &key, &corrupted, &original).await;
                Ok(())
            })
            .await?;
        Box::pin(repo.recover_pending_erasures()).await?;
        assert_eq!(
            repo.head(SPACE, &original_source).await?.unwrap().lifecycle,
            SourceVisibility::Erased
        );
        assert_eq!(
            repo.operation(SPACE, &reserved.operation_id)
                .await?
                .unwrap()
                .state,
            OperationState::Erased
        );
        assert_eq!(repo.head(SPACE, &surviving_source).await?, second_head);
        assert_eq!(exclusive.erasure_journal()?.intents()?, original_intents);
        let retained = crate::RelRepository::new(exclusive.clone())
            .get(SPACE, &rel_id)
            .await?
            .unwrap();
        assert!(!retained.base.provenance.sources.contains(&original_source));
        assert!(retained.base.provenance.sources.contains(&surviving_source));
        assert!(
            Box::pin(
                CanonicalRepository::new(exclusive.clone()).accepted_contact_rel_support(
                    SPACE,
                    &retained,
                    &surviving_source
                )
            )
            .await?
        );
        Ok(())
    }))
    .await
    .unwrap();
    assert!(storage.recovery_status().ready);
    assert_eq!(
        storage.erasure_journal().unwrap().intents().unwrap(),
        intents
    );
    assert_eq!(
        publications
            .head(SPACE, &first.source_id)
            .await
            .unwrap()
            .unwrap()
            .lifecycle,
        SourceVisibility::Erased
    );
}

#[tokio::test]
async fn embedded_corrupt_survivor_refuses_physical_purge_then_recovers_original_intent() {
    Box::pin(corrupt_survivor_rolls_back_physical_purge_and_recovers_original_intent(true)).await;
}
#[tokio::test]
async fn surreal_corrupt_survivor_refuses_physical_purge_then_recovers_original_intent() {
    Box::pin(corrupt_survivor_rolls_back_physical_purge_and_recovers_original_intent(false)).await;
}

#[test]
fn independent_storage_erasure_future_fits_the_fixture_stack_budget() {
    let future = corrupt_survivor_rolls_back_physical_purge_and_recovers_original_intent(true);
    let bytes = std::mem::size_of_val(&future);
    assert!(
        bytes <= 64 * 1024,
        "independent storage erasure future is {bytes} bytes"
    );
}
