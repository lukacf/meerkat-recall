//! Fault/recovery checks for the original producer at manifest write boundaries.
//!
//! Real standing registration/revocation races also belong to B/D's composed
//! standing-watch tests; these cases do not manufacture a positive delegation.

use super::*;

async fn replace_operation_control(
    storage: &StorageClient,
    manifest: &SourcePublicationManifest,
    value: &serde_json::Value,
) {
    let space = &manifest.space_id;
    let source = &manifest.publication.source_id;
    let key = &manifest.publication.operation_id;
    if let Some(store) = storage.embedded_store() {
        store
            .mutate(|tx| {
                write_control_tx(
                    tx,
                    crate::source_publication::OPERATION_TABLE,
                    key,
                    space,
                    source,
                    value,
                )
            })
            .await
            .unwrap();
    } else {
        storage
            .query_bind_checked(
                "UPSERT $original_operation CONTENT $original_value",
                crate::bindings! {
                    "original_operation" => control_thing(crate::source_publication::OPERATION_TABLE, key),
                    "original_value" => control_value(space, source, value).unwrap(),
                },
            )
            .await
            .unwrap();
    }
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn missing_producer_refuses_manifest_and_embedding_preparation(embedded: bool) {
    let space = "standing-original-manifest";
    // The fixture performs actual direct acceptance, prior publication, closed
    // retrieval planning and an actual current embedding claim. It has no Watch
    // delegation, and a missing original must not infer that direct admission.
    let (fixture, claim, _) = Box::pin(embedding_write_fixture(embedded, space)).await;
    let repository = SourcePublicationManifestRepository::new(fixture.storage.clone());
    let publication = &fixture.manifest.publication;
    let sources = SourcePublicationRepository::new(fixture.storage.clone());
    let original: serde_json::Value = sources
        .read_control(
            crate::source_publication::OPERATION_TABLE,
            &publication.operation_id,
            space,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(original["producer_origin"]["kind"], "direct");
    let before_head = sources.head(space, &publication.source_id).await.unwrap();
    let before_plan = repository.get(space, publication).await.unwrap();
    let before_claim = crate::WorkItemRepository::new(fixture.storage.clone())
        .get(space, &claim.base.id)
        .await
        .unwrap();
    let mut missing = original.clone();
    missing.as_object_mut().unwrap().remove("producer_origin");
    replace_operation_control(&fixture.storage, &fixture.manifest, &missing).await;

    let error = Box::pin(repository.set_operation_plan(fixture.manifest.clone()))
        .await
        .unwrap_err();
    assert!(matches!(error, StorageError::Conflict(ref reason)
        if reason == "standing_watch_authority_changed_or_missing"));
    let attempt = crate::work_attempt_repository::fence_from_claim(&claim).unwrap();
    let revision = &fixture.manifest.documents[0].doc_revision_id;
    let error = Box::pin(repository.prepare_embedding_write(
        space,
        publication,
        &fixture.manifest.configuration,
        &attempt,
        revision,
    ))
    .await
    .unwrap_err();
    assert!(matches!(error, StorageError::Conflict(ref reason)
        if reason == "standing_watch_authority_changed_or_missing"));
    assert_eq!(
        sources.head(space, &publication.source_id).await.unwrap(),
        before_head
    );
    assert_eq!(
        repository.get(space, publication).await.unwrap(),
        before_plan
    );
    assert_eq!(
        crate::WorkItemRepository::new(fixture.storage.clone())
            .get(space, &claim.base.id)
            .await
            .unwrap(),
        before_claim
    );
    assert!(repository
        .completion(space, publication)
        .await
        .unwrap()
        .is_none());

    // Restore only the deliberately removed original field, then retry the
    // original operation and claim. No replacement actor, generation or grant.
    replace_operation_control(&fixture.storage, &fixture.manifest, &original).await;
    assert_eq!(
        Box::pin(repository.set_operation_plan(fixture.manifest.clone()))
            .await
            .unwrap(),
        fixture.manifest
    );
    let authority = Box::pin(repository.prepare_embedding_write(
        space,
        publication,
        &fixture.manifest.configuration,
        &attempt,
        revision,
    ))
    .await
    .unwrap();
    assert_eq!(authority.space_id(), space);
    assert_eq!(
        sources.head(space, &publication.source_id).await.unwrap(),
        before_head
    );
    assert!(repository
        .completion(space, publication)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn embedded_missing_producer_blocks_manifest_and_embedding_without_adopting_authority() {
    Box::pin(missing_producer_refuses_manifest_and_embedding_preparation(
        true,
    ))
    .await;
}

#[tokio::test]
async fn surreal_missing_producer_blocks_manifest_and_embedding_without_adopting_authority() {
    Box::pin(missing_producer_refuses_manifest_and_embedding_preparation(
        false,
    ))
    .await;
}

async fn sibling_pending_progress(fixture: &Fixture) {
    let claim = claim_sibling_progress(fixture).await;
    record_sibling_progress(fixture, &claim, "pending_dependencies").await;
}

async fn claim_sibling_progress(fixture: &Fixture) -> WorkItem {
    let space = &fixture.manifest.space_id;
    let item = work_item(
        space,
        "join_source_publication",
        plan_payload(&fixture.manifest),
    );
    let items = crate::WorkItemRepository::new(fixture.storage.clone());
    items.create(item.clone()).await.unwrap();
    items
        .claim_with_lease(
            space,
            &item.base.id,
            "original-publication-join",
            Utc::now() + chrono::Duration::minutes(5),
        )
        .await
        .unwrap()
        .unwrap()
}

async fn record_sibling_progress(fixture: &Fixture, claim: &WorkItem, code: &str) {
    SourcePublicationRepository::new(fixture.storage.clone())
        .record_execution_state(
            &fixture.manifest.space_id,
            &fixture.manifest.publication,
            &crate::work_attempt_repository::fence_from_claim(claim).unwrap(),
            elephant_types::publication::OperationState::Processing,
            Some(code),
        )
        .await
        .unwrap();
}

async fn write_original_vector(
    fixture: &Fixture,
    authority: &SourceEmbeddingWriteAuthority,
) -> StorageResult<()> {
    crate::NodeEmbeddingRepository::new(fixture.storage.clone(), PartitionManager::new())
        .store_source_embeddings_and_mark_nodes(
            &fixture.manifest.space_id,
            &[fixture.nodes[0].base.id.clone()],
            &embedding_model(&fixture.manifest).unwrap(),
            Utc::now(),
            vec![source_vector(fixture, vec![1.0, 0.0, 0.0])],
            authority,
        )
        .await
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn original_vector_survives_live_sibling_progress(embedded: bool) {
    let (fixture, claim, _) = Box::pin(embedding_write_fixture(
        embedded,
        "embedding-sibling-progress",
    ))
    .await;
    let sources = SourcePublicationRepository::new(fixture.storage.clone());
    let space = &fixture.manifest.space_id;
    let fence = &fixture.manifest.publication;
    let attempt = crate::work_attempt_repository::fence_from_claim(&claim).unwrap();
    sources
        .record_execution_state(
            space,
            fence,
            &attempt,
            elephant_types::publication::OperationState::Processing,
            None,
        )
        .await
        .unwrap();
    let authority = Box::pin(
        SourcePublicationManifestRepository::new(fixture.storage.clone()).prepare_embedding_write(
            space,
            fence,
            &fixture.manifest.configuration,
            &attempt,
            &fixture.manifest.documents[0].doc_revision_id,
        ),
    )
    .await
    .unwrap();
    let before_head = sources.head(space, &fence.source_id).await.unwrap();
    sibling_pending_progress(&fixture).await;
    let receipt = sources
        .operation(space, &fence.operation_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        receipt.state,
        elephant_types::publication::OperationState::Processing
    );
    assert_eq!(receipt.error_code.as_deref(), Some("pending_dependencies"));
    // Same authority captured BEFORE the sibling update, no inference or recapture.
    write_original_vector(&fixture, &authority).await.unwrap();
    assert_eq!(
        stored_source_vector(&fixture).await,
        Some(vec![1.0, 0.0, 0.0])
    );
    let node = crate::DocNodeRepository::new(fixture.storage.clone())
        .get(space, &fixture.nodes[0].base.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        node.embedding_model,
        Some(embedding_model(&fixture.manifest).unwrap())
    );
    assert_eq!(
        sources.operation(space, &fence.operation_id).await.unwrap(),
        Some(receipt)
    );
    assert_eq!(
        sources.head(space, &fence.source_id).await.unwrap(),
        before_head
    );
    assert!(
        SourcePublicationManifestRepository::new(fixture.storage.clone())
            .completion(space, fence)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn embedded_original_embedding_accepts_sibling_pending_progress() {
    Box::pin(original_vector_survives_live_sibling_progress(true)).await;
}

#[tokio::test]
async fn surreal_original_embedding_accepts_sibling_pending_progress() {
    Box::pin(original_vector_survives_live_sibling_progress(false)).await;
}

async fn remove_original_operation(fixture: &Fixture) {
    let table = crate::source_publication::OPERATION_TABLE;
    let key = &fixture.manifest.publication.operation_id;
    let space = &fixture.manifest.space_id;
    if let Some(store) = fixture.storage.embedded_store() {
        store
            .mutate(|tx| {
                tx.hard_delete_extension_row(table, &format!("{table}:{key}"), space)?;
                Ok(())
            })
            .await
            .unwrap();
    } else {
        fixture
            .storage
            .query_bind_checked(
                "DELETE $id;",
                crate::bindings! {"id" => control_thing(table, key)},
            )
            .await
            .unwrap();
    }
}

async fn replace_only_operation_payload(fixture: &Fixture, value: &serde_json::Value) {
    let table = crate::source_publication::OPERATION_TABLE;
    let key = &fixture.manifest.publication.operation_id;
    let payload = serde_json::to_string(value).unwrap();
    if let Some(store) = fixture.storage.embedded_store() {
        store
            .mutate(|tx| {
                let mut row = tx
                    .scan_extension_rows(table)?
                    .into_iter()
                    .find(|(id, _)| id == &format!("{table}:{key}"))
                    .unwrap()
                    .1;
                row["payload"] = serde_json::Value::String(payload.clone());
                tx.upsert_extension_row(table, row)?;
                Ok(())
            })
            .await
            .unwrap();
    } else {
        fixture
            .storage
            .query_bind_checked(
                "UPDATE $id SET payload = $payload;",
                crate::bindings! {"id" => control_thing(table, key), "payload" => payload},
            )
            .await
            .unwrap();
    }
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn progress_cannot_adopt_changed_original_authority(embedded: bool, preparation: bool) {
    for fault in 0..13 {
        let (fixture, _, authority) = Box::pin(embedding_write_fixture(
            embedded,
            "embedding-original-authority",
        ))
        .await;
        sibling_pending_progress(&fixture).await;
        let space = &fixture.manifest.space_id;
        let fence = &fixture.manifest.publication;
        let sources = SourcePublicationRepository::new(fixture.storage.clone());
        let mut original: serde_json::Value = sources
            .read_control(
                crate::source_publication::OPERATION_TABLE,
                &fence.operation_id,
                space,
            )
            .await
            .unwrap()
            .unwrap();
        match fault {
            0 => original["producer_origin"] = serde_json::json!({"kind":"unknown_original"}),
            1 => {
                original["authorization"]["security"]["policy_id"] =
                    serde_json::json!("changed-policy");
            }
            2 => original["receipt"]["desired_revision"] = serde_json::json!("changed-revision"),
            3 => original["receipt"]["source_generation"] = serde_json::json!("changed-generation"),
            4 => original["receipt"]["state"] = serde_json::json!("failed_terminal"),
            5 => original["request_fingerprint"] = serde_json::json!("changed-request"),
            6 => remove_original_operation(&fixture).await,
            7 => {
                crate::SourceRepository::new(fixture.storage.clone())
                    .delete(space, &fence.source_id)
                    .await
                    .unwrap();
            }
            8 => {
                original["producer_origin"] = serde_json::json!({"kind":"unknown_original"});
                // The mirrored native state remains unchanged; payload is authoritative.
                replace_only_operation_payload(&fixture, &original).await;
            }
            9 => original["receipt"]["completed_at"] = serde_json::json!(Utc::now()),
            10 => {
                original["receipt"]["published_revision"] = serde_json::json!("changed-published");
            }
            11 => original["unexpected_authority"] = serde_json::json!({"delegation":"changed"}),
            12 => original["input_fingerprint"] = serde_json::json!("changed-input"),
            _ => unreachable!(),
        }
        if !(6..=8).contains(&fault) {
            replace_operation_control(&fixture.storage, &fixture.manifest, &original).await;
        }
        let result = if preparation {
            authority
                .validate_preparation(
                    &fixture.storage,
                    &embedding_model(&fixture.manifest).unwrap(),
                )
                .await
                .map(|_| ())
        } else {
            write_original_vector(&fixture, &authority).await
        };
        assert!(result.is_err(), "fault {fault}");
        assert_eq!(stored_source_vector(&fixture).await, None, "fault {fault}");
        let node = crate::DocNodeRepository::new(fixture.storage.clone())
            .get(space, &fixture.nodes[0].base.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(node.embedding_model, None, "fault {fault}");
        assert!(
            SourcePublicationManifestRepository::new(fixture.storage.clone())
                .completion(space, fence)
                .await
                .unwrap()
                .is_none()
        );
    }
}

#[tokio::test]
async fn embedded_embedding_progress_preserves_original_authority() {
    Box::pin(progress_cannot_adopt_changed_original_authority(
        true, false,
    ))
    .await;
}

#[tokio::test]
async fn surreal_embedding_progress_preserves_original_authority() {
    Box::pin(progress_cannot_adopt_changed_original_authority(
        false, false,
    ))
    .await;
}

async fn check_reconciled_embedding_guard(
    fixture: &Fixture,
    authority: &SourceEmbeddingWriteAuthority,
) -> StorageResult<()> {
    let model = embedding_model(&fixture.manifest).unwrap();
    if let Some(store) = fixture.storage.embedded_store() {
        store
            .mutate(|tx| check_embedding_write_tx(tx, authority, &[], &model, &[]))
            .await
    } else {
        let mut sql = String::from("BEGIN TRANSACTION;\n");
        let mut bindings = BTreeMap::new();
        append_embedding_write_sql(&mut sql, &mut bindings, authority, &[], &model, &[])?;
        sql.push_str("COMMIT TRANSACTION;");
        fixture
            .storage
            .query_bind_checked(&sql, bindings)
            .await
            .map(|_| ())
    }
}

async fn reconciled_progress_keeps_transaction_payload_fence(embedded: bool) {
    let (fixture, _, original) = Box::pin(embedding_write_fixture(
        embedded,
        "embedding-progress-write-fence",
    ))
    .await;
    sibling_pending_progress(&fixture).await;
    let reconciled = original
        .reconcile_source_progress(&fixture.storage)
        .await
        .unwrap();
    // Prove this exact reconciled guard is valid before the injected mutation.
    check_reconciled_embedding_guard(&fixture, &reconciled)
        .await
        .unwrap();
    // Simulate a payload-only producer change after the read and before mutation.
    let mut changed: serde_json::Value = SourcePublicationRepository::new(fixture.storage.clone())
        .read_control(
            crate::source_publication::OPERATION_TABLE,
            &fixture.manifest.publication.operation_id,
            &fixture.manifest.space_id,
        )
        .await
        .unwrap()
        .unwrap();
    changed["producer_origin"] = serde_json::json!({"kind":"unknown_original"});
    replace_only_operation_payload(&fixture, &changed).await;
    assert!(check_reconciled_embedding_guard(&fixture, &reconciled)
        .await
        .is_err());
    assert_eq!(stored_source_vector(&fixture).await, None);
}

#[tokio::test]
async fn embedded_reconciled_progress_keeps_atomic_operation_fence() {
    Box::pin(reconciled_progress_keeps_transaction_payload_fence(true)).await;
}

#[tokio::test]
async fn surreal_reconciled_progress_keeps_atomic_operation_fence() {
    Box::pin(reconciled_progress_keeps_transaction_payload_fence(false)).await;
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn preparation_survives_live_sibling_progress_without_effects(embedded: bool) {
    let (fixture, _, original) = Box::pin(embedding_write_fixture(
        embedded,
        "embedding-preparation-progress",
    ))
    .await;
    let space = &fixture.manifest.space_id;
    let fence = &fixture.manifest.publication;
    let sources = SourcePublicationRepository::new(fixture.storage.clone());
    let before_head = sources.head(space, &fence.source_id).await.unwrap();
    check_reconciled_embedding_guard(&fixture, &original)
        .await
        .unwrap();
    sibling_pending_progress(&fixture).await;
    let before_receipt = sources.operation(space, &fence.operation_id).await.unwrap();

    // Reproduce a sibling update between ancestry/input capture and the initial
    // preparation transaction. The unchanged raw guard rejects that stale copy.
    assert!(check_reconciled_embedding_guard(&fixture, &original)
        .await
        .is_err());
    // This is the same final preparation step used by prepare_embedding_write,
    // before source_runtime can bind or call the embedding provider.
    let prepared = original
        .validate_preparation(
            &fixture.storage,
            &embedding_model(&fixture.manifest).unwrap(),
        )
        .await
        .unwrap();
    check_reconciled_embedding_guard(&fixture, &prepared)
        .await
        .unwrap();
    assert_eq!(stored_source_vector(&fixture).await, None);
    assert_eq!(
        crate::DocNodeRepository::new(fixture.storage.clone())
            .get(space, &fixture.nodes[0].base.id)
            .await
            .unwrap()
            .unwrap()
            .embedding_model,
        None
    );
    assert_eq!(
        sources.operation(space, &fence.operation_id).await.unwrap(),
        before_receipt
    );
    assert_eq!(
        sources.head(space, &fence.source_id).await.unwrap(),
        before_head
    );
    assert!(
        SourcePublicationManifestRepository::new(fixture.storage.clone())
            .completion(space, fence)
            .await
            .unwrap()
            .is_none()
    );

    // A producer change after preparation still fails the existing atomic fence.
    let mut changed: serde_json::Value = sources
        .read_control(
            crate::source_publication::OPERATION_TABLE,
            &fence.operation_id,
            space,
        )
        .await
        .unwrap()
        .unwrap();
    changed["producer_origin"] = serde_json::json!({"kind":"unknown_original"});
    replace_only_operation_payload(&fixture, &changed).await;
    assert!(check_reconciled_embedding_guard(&fixture, &prepared)
        .await
        .is_err());
    assert_eq!(stored_source_vector(&fixture).await, None);
}

#[tokio::test]
async fn embedded_preparation_accepts_sibling_progress_without_effects() {
    Box::pin(preparation_survives_live_sibling_progress_without_effects(
        true,
    ))
    .await;
}

#[tokio::test]
async fn surreal_preparation_accepts_sibling_progress_without_effects() {
    Box::pin(preparation_survives_live_sibling_progress_without_effects(
        false,
    ))
    .await;
}

#[tokio::test]
async fn embedded_preparation_progress_preserves_original_authority() {
    Box::pin(progress_cannot_adopt_changed_original_authority(true, true)).await;
}

#[tokio::test]
async fn surreal_preparation_progress_preserves_original_authority() {
    Box::pin(progress_cannot_adopt_changed_original_authority(
        false, true,
    ))
    .await;
}

// Inject changes after the production retry runner has reconciled, then use
// the actual atomic preparation/vector transaction. The fixed vectors below
// are storage fixtures, not provider output or semantic relevance evidence.
async fn commit_progress_attempt(
    fixture: &Fixture,
    authority: &SourceEmbeddingWriteAuthority,
    preparation: bool,
    vectors: &[NodeEmbedding],
) -> StorageResult<()> {
    if preparation {
        return check_reconciled_embedding_guard(fixture, authority).await;
    }
    crate::NodeEmbeddingRepository::new(fixture.storage.clone(), PartitionManager::new())
        .store_source_embedding_transaction_for_test(
            &fixture.manifest.space_id,
            &[fixture.nodes[0].base.id.clone()],
            &embedding_model(&fixture.manifest).unwrap(),
            Utc::now(),
            vectors.to_vec(),
            authority,
        )
        .await
}

async fn assert_no_progress_write(fixture: &Fixture) {
    assert_eq!(stored_source_vector(fixture).await, None);
    assert_eq!(
        crate::DocNodeRepository::new(fixture.storage.clone())
            .get(&fixture.manifest.space_id, &fixture.nodes[0].base.id)
            .await
            .unwrap()
            .unwrap()
            .embedding_model,
        None
    );
    assert!(
        SourcePublicationManifestRepository::new(fixture.storage.clone())
            .completion(&fixture.manifest.space_id, &fixture.manifest.publication)
            .await
            .unwrap()
            .is_none()
    );
}

async fn progress_between_reconciliation_and_commit(embedded: bool) {
    use std::sync::atomic::{AtomicUsize, Ordering};
    for preparation in [true, false] {
        let (fixture, claim, original) =
            Box::pin(embedding_write_fixture(embedded, "progress-commit-race")).await;
        let sibling = claim_sibling_progress(&fixture).await;
        let visits = AtomicUsize::new(0);
        let vectors = vec![source_vector(&fixture, vec![0.25, 0.5, 0.75])];
        let before_head = SourcePublicationRepository::new(fixture.storage.clone())
            .head(
                &fixture.manifest.space_id,
                &fixture.manifest.publication.source_id,
            )
            .await
            .unwrap();
        let fixture_ref = &fixture;
        let vectors_ref = &vectors;
        let sibling_ref = &sibling;
        let result = original
            .commit_with_source_progress(&fixture.storage, |authority| {
                let visit = visits.fetch_add(1, Ordering::Relaxed);
                async move {
                    if visit == 0 {
                        record_sibling_progress(fixture_ref, sibling_ref, "pending_dependencies")
                            .await;
                    }
                    let result =
                        commit_progress_attempt(fixture_ref, &authority, preparation, vectors_ref)
                            .await;
                    if visit == 0 {
                        assert!(
                            result.is_err(),
                            "stale whole-payload guard must refuse first"
                        );
                        assert_no_progress_write(fixture_ref).await;
                    }
                    result
                }
            })
            .await;
        assert!(result.is_ok());
        assert_eq!(visits.load(Ordering::Relaxed), 2);
        if preparation {
            assert_no_progress_write(&fixture).await;
        } else {
            assert_eq!(
                stored_source_vector(&fixture).await,
                Some(vec![0.25, 0.5, 0.75])
            );
        }
        assert_eq!(
            crate::WorkItemRepository::new(fixture.storage.clone())
                .get(&fixture.manifest.space_id, &claim.base.id)
                .await
                .unwrap(),
            Some(claim),
            "local commit retry must not renew or refund the queue claim"
        );
        assert_eq!(
            SourcePublicationRepository::new(fixture.storage.clone())
                .head(
                    &fixture.manifest.space_id,
                    &fixture.manifest.publication.source_id
                )
                .await
                .unwrap(),
            before_head,
            "vector success alone must not publish the source"
        );
    }
}

#[tokio::test]
async fn embedded_source_progress_race_retries_atomic_commit_only() {
    Box::pin(progress_between_reconciliation_and_commit(true)).await;
}

#[tokio::test]
async fn surreal_source_progress_race_retries_atomic_commit_only() {
    Box::pin(progress_between_reconciliation_and_commit(false)).await;
}

async fn continuing_progress_is_bounded(embedded: bool) {
    use std::sync::atomic::{AtomicUsize, Ordering};
    for preparation in [true, false] {
        let (fixture, _, original) = Box::pin(embedding_write_fixture(
            embedded,
            "progress-commit-exhaustion",
        ))
        .await;
        let sibling = claim_sibling_progress(&fixture).await;
        let visits = AtomicUsize::new(0);
        let vectors = vec![source_vector(&fixture, vec![0.25, 0.5, 0.75])];
        let fixture_ref = &fixture;
        let vectors_ref = &vectors;
        let sibling_ref = &sibling;
        let result = original
            .commit_with_source_progress(&fixture.storage, |authority| {
                let visit = visits.fetch_add(1, Ordering::Relaxed);
                async move {
                    record_sibling_progress(fixture_ref, sibling_ref, &format!("progress_{visit}"))
                        .await;
                    commit_progress_attempt(fixture_ref, &authority, preparation, vectors_ref).await
                }
            })
            .await;
        assert!(result.is_err());
        assert_eq!(visits.load(Ordering::Relaxed), 4, "finite local bound");
        assert_no_progress_write(&fixture).await;
    }
}

#[tokio::test]
async fn embedded_source_progress_race_exhaustion_writes_nothing() {
    Box::pin(continuing_progress_is_bounded(true)).await;
}

#[tokio::test]
async fn surreal_source_progress_race_exhaustion_writes_nothing() {
    Box::pin(continuing_progress_is_bounded(false)).await;
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep each fault-injection repository in its branch scope until the original authority mutation completes."
)]
async fn change_original_between_commits(fixture: &Fixture, claim: &WorkItem, fault: usize) {
    let space = &fixture.manifest.space_id;
    if fault < 2 {
        let mut changed: serde_json::Value =
            SourcePublicationRepository::new(fixture.storage.clone())
                .read_control(
                    crate::source_publication::OPERATION_TABLE,
                    &fixture.manifest.publication.operation_id,
                    space,
                )
                .await
                .unwrap()
                .unwrap();
        if fault == 0 {
            changed["producer_origin"] = serde_json::json!({"kind":"unknown_original"});
        } else {
            changed["receipt"]["state"] = serde_json::json!("failed_terminal");
        }
        // A native state mirror still containing the old authority cannot save
        // a changed original serialized operation.
        replace_only_operation_payload(fixture, &changed).await;
    } else if fault == 2 {
        next_configuration(fixture).await;
    } else if fault == 3 {
        let items = crate::WorkItemRepository::new(fixture.storage.clone());
        items
            .requeue_with_delay(
                space,
                &claim.base.id,
                claim.leased_by.as_deref().unwrap(),
                0,
                None,
            )
            .await
            .unwrap()
            .unwrap();
        items
            .claim_with_lease(
                space,
                &claim.base.id,
                "successor",
                Utc::now() + chrono::Duration::minutes(5),
            )
            .await
            .unwrap()
            .unwrap();
    } else if fault == 4 {
        let mut node = fixture.nodes[0].clone();
        node.span.end -= 1;
        crate::DocNodeRepository::new(fixture.storage.clone())
            .update(space, &node.base.id.clone(), node)
            .await
            .unwrap();
    } else {
        let configs = crate::SpaceConfigRepository::new(fixture.storage.clone());
        let config_id = RecordId::from_str(&fixture.manifest.configuration.config_id).unwrap();
        let mut config = configs.get(space, &config_id).await.unwrap().unwrap();
        let processing = config.processing.as_mut().unwrap();
        processing.generation += 1;
        if fault == 5 {
            processing
                .bindings
                .get_mut(&ModelOperation::DocumentEmbedding)
                .unwrap()
                .revoked = true;
        } else {
            processing
                .connections
                .get_mut("connection")
                .unwrap()
                .revoked = true;
        }
        configs.update(space, &config_id, config).await.unwrap();
    }
}

async fn progress_retry_cannot_refresh_original_authority(embedded: bool) {
    use std::sync::atomic::{AtomicUsize, Ordering};
    for preparation in [true, false] {
        for fault in 0..7 {
            let (fixture, claim, original) = Box::pin(embedding_write_fixture(
                embedded,
                "progress-retry-original-fence",
            ))
            .await;
            let sibling = claim_sibling_progress(&fixture).await;
            let visits = AtomicUsize::new(0);
            let vectors = vec![source_vector(&fixture, vec![0.25, 0.5, 0.75])];
            let fixture_ref = &fixture;
            let vectors_ref = &vectors;
            let sibling_ref = &sibling;
            let claim_ref = &claim;
            let result = original
                .commit_with_source_progress(&fixture.storage, |authority| {
                    let visit = visits.fetch_add(1, Ordering::Relaxed);
                    async move {
                        if visit == 0 {
                            record_sibling_progress(
                                fixture_ref,
                                sibling_ref,
                                "pending_dependencies",
                            )
                            .await;
                            // Real guard refusal from benign progress first, then
                            // adversarial change before the retry's reconciliation.
                            let refused = commit_progress_attempt(
                                fixture_ref,
                                &authority,
                                preparation,
                                vectors_ref,
                            )
                            .await;
                            assert!(refused.is_err());
                            change_original_between_commits(fixture_ref, claim_ref, fault).await;
                            return refused;
                        }
                        commit_progress_attempt(fixture_ref, &authority, preparation, vectors_ref)
                            .await
                    }
                })
                .await;
            assert!(result.is_err(), "fault {fault}");
            assert_eq!(
                visits.load(Ordering::Relaxed),
                if fault < 2 { 1 } else { 2 }
            );
            assert_no_progress_write(&fixture).await;
        }
    }
}

#[tokio::test]
async fn embedded_source_progress_retry_preserves_original_fences() {
    Box::pin(progress_retry_cannot_refresh_original_authority(true)).await;
}

#[tokio::test]
async fn surreal_source_progress_retry_preserves_original_fences() {
    Box::pin(progress_retry_cannot_refresh_original_authority(false)).await;
}

async fn refusal_without_proven_progress_is_not_retried(embedded: bool) {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let (fixture, _, original) = Box::pin(embedding_write_fixture(
        embedded,
        "progress-retry-error-scope",
    ))
    .await;
    let sibling = claim_sibling_progress(&fixture).await;
    // Exact sentinel with unchanged receipt; other typed/incidental errors even
    // when live progress did change. Neither fact alone warrants a local retry.
    for fault in 0..5 {
        let visits = AtomicUsize::new(0);
        let fixture_ref = &fixture;
        let sibling_ref = &sibling;
        let result = original
            .commit_with_source_progress(&fixture.storage, |_| {
                visits.fetch_add(1, Ordering::Relaxed);
                async move {
                    if fault != 0 {
                        record_sibling_progress(fixture_ref, sibling_ref, &format!("progress_{fault}"))
                            .await;
                    }
                    Err(match fault {
                        0 => StorageError::Conflict("standing_watch_authority_changed_or_missing".into()),
                        1 => StorageError::Conflict("canonical_inputs_changed".into()),
                        2 => StorageError::DataIntegrityError("standing_watch_authority_changed_or_missing".into()),
                        3 => StorageError::QueryError("provider says: standing_watch_authority_changed_or_missing".into()),
                        _ => StorageError::QueryError("DocNode embedding transaction failed: An error occurred: standing_watch_authority_changed_or_missing extra".into()),
                    })
                }
            })
            .await;
        assert!(result.is_err());
        assert_eq!(visits.load(Ordering::Relaxed), 1, "fault {fault}");
        assert_no_progress_write(&fixture).await;
    }
}

#[tokio::test]
async fn embedded_source_progress_retry_requires_exact_refusal_and_changed_progress() {
    Box::pin(refusal_without_proven_progress_is_not_retried(true)).await;
}

#[tokio::test]
async fn surreal_source_progress_retry_requires_exact_refusal_and_changed_progress() {
    Box::pin(refusal_without_proven_progress_is_not_retried(false)).await;
}
