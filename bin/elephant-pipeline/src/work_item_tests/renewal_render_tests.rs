//! Exercise the worker's renewal handoff against actual render input custody.
//! No model, provider transport, retrieval score or publication acceptance here.
use super::*;
use elephant_pipeline::renderer::{default::DefaultTextRenderer, RendererRegistry};
use elephant_storage::standing_watch_repository::PreparedStandingSourceAuthority;

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep original repository and fixture ownership through the complete control assertions"
)]
async fn fixture(embedded: bool) -> (StorageClient, Source, Doc, WorkItem, WorkQueue) {
    let storage = if embedded {
        StorageClient::new_embedded()
            .await
            .expect("embedded storage")
    } else {
        StorageClient::new_memory().await.expect("native storage")
    };
    initialize_fixture_schema(&storage).await;
    let space = "renewal-render";
    let source = create_manual_source(
        &storage,
        space,
        "renewal-regression",
        "Plain text",
        "Original plain text.\r\nSecond line.",
        IngestMode::Retrieval,
    )
    .await;
    // A retained direct source isolates the guard used before every renderer.
    // Accepted-source completion is covered by the existing publication tests.
    let doc = Doc {
        base: BaseFields::new(
            RecordId::new_v4(TableName::Doc),
            space.into(),
            "renewal-regression".into(),
        ),
        title: "Plain text".into(),
        text: "Original plain text.\r\nSecond line.".into(),
        sidecar_text: None,
        source: Some(Link::new(source.base.id.clone())),
        media_attachments: vec![],
        rendition_key: "renewal-render-v1".into(),
        metadata: serde_json::json!({}),
    };
    let doc = DocRepository::new(storage.clone())
        .create(doc)
        .await
        .expect("original document");
    let items = WorkItemRepository::new(storage.clone());
    let item = items
        .create(WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                space.into(),
                "renewal-regression".into(),
            ),
            kind: RENDER_DOC_KIND.into(),
            payload: serde_json::json!({
                "doc_id": doc.base.id.to_string(),
                "source_id": source.base.id.to_string(),
                "ingest_mode": "retrieval",
            }),
            status: WorkItemStatus::Runnable,
            priority: 1,
            idempotency_key: "renewal-render-v1".into(),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: Some(Utc::now() - chrono::Duration::seconds(1)),
            last_error: Some("prior transient fixture failure".into()),
        })
        .await
        .expect("original admitted work");
    let original = items
        .claim_with_lease(
            space,
            &item.base.id,
            "renewal-regression",
            Utc::now() + chrono::Duration::seconds(60),
        )
        .await
        .expect("claim")
        .expect("runnable work claimed");
    let queue = WorkQueue::with_defaults(storage.clone())
        .for_attempt(&original)
        .expect("queue owns original attempt");
    (storage, source, doc, original, queue)
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep original repository and fixture ownership through the complete control assertions"
)]
async fn renewal_render(embedded: bool) {
    let (storage, source, doc, original, queue) = fixture(embedded).await;
    let space = original.base.space_id.as_str();
    let items = WorkItemRepository::new(storage.clone());
    let original_fence = elephant_storage::work_attempt_repository::fence_from_claim(&original)
        .expect("original attempt");
    // This is the same helper whose returned row enters the live worker task.
    let renewed = renew_execution_work(&queue, &original, 300)
        .await
        .expect("renewed execution snapshot");
    assert!(renewed.lease_expires_at > original.lease_expires_at);
    assert_eq!(renewed.attempts, original.attempts);
    assert_eq!(renewed.payload, original.payload);
    assert!(renewed.run_after.is_none() && renewed.last_error.is_none());
    assert_eq!(
        elephant_storage::work_attempt_repository::fence_from_claim(&renewed)
            .expect("renewed attempt"),
        original_fence
    );
    assert_eq!(
        items
            .get(space, &original.base.id)
            .await
            .expect("current row"),
        Some(renewed.clone())
    );
    let revisions = DocRevisionRepository::new(storage.clone());
    let mut registry = RendererRegistry::new();
    registry.register(Box::new(DefaultTextRenderer::new()));
    let stale = elephant_pipeline::scheduler::evidence_work_kinds::execute_render_doc_for_work(
        &storage,
        &registry,
        &doc,
        Some("text/plain"),
        &original,
    )
    .await
    .expect_err("pre-renewal snapshot must still be refused");
    assert!(stale
        .to_string()
        .contains("standing_watch_authority_changed_or_missing"));
    assert!(revisions
        .find_by_doc(&doc.base.id)
        .await
        .expect("no render")
        .is_empty());
    let result = elephant_pipeline::scheduler::evidence_work_kinds::execute_render_doc_for_work(
        &storage,
        &registry,
        &doc,
        Some("text/plain"),
        &renewed,
    )
    .await
    .expect("actual deterministic renderer accepts renewed original work");
    assert!(result.success && result.error.is_none());
    let rendered = revisions
        .get(space, &result.revision_id)
        .await
        .expect("rendered revision")
        .expect("actual revision persisted");
    assert_eq!(rendered.text, "Original plain text.\nSecond line.\n");
    assert_eq!(rendered.doc.id(), &doc.base.id);
    assert_eq!(rendered.base.provenance.sources, vec![source.base.id]);

    refuse_changed_input(&storage, &doc, &original, &queue, &renewed).await;
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep original repository and fixture ownership through the complete control assertions"
)]
async fn refuse_changed_input(
    storage: &StorageClient,
    doc: &Doc,
    original: &WorkItem,
    queue: &WorkQueue,
    renewed: &WorkItem,
) {
    let space = original.base.space_id.as_str();
    let items = WorkItemRepository::new(storage.clone());
    let revisions = DocRevisionRepository::new(storage.clone());
    // Ordinary repository writes cannot alter a Running item. Preserve that
    // guard, then model a faulty owned producer through the actual transaction
    // fence: renewal must not adopt changed input even under the same attempt.
    let mut changed = renewed.clone();
    changed.payload["doc_id"] = serde_json::json!(RecordId::new_v4(TableName::Doc).to_string());
    let unowned = items
        .update(space, &original.base.id, changed.clone())
        .await
        .expect_err("generic update must refuse a Running item");
    assert!(matches!(unowned, StorageError::Conflict(reason)
        if reason == "work_attempt_required: claim or renew through the attempt repository"));
    assert_eq!(
        items
            .get(space, &original.base.id)
            .await
            .expect("unchanged original"),
        Some(renewed.clone())
    );
    change_input_under_original_fence(storage, renewed, &changed).await;
    let stored = items
        .get(space, &original.base.id)
        .await
        .expect("changed input readback")
        .expect("original item remains");
    assert_eq!(stored.payload, changed.payload);
    assert_eq!(
        elephant_storage::work_attempt_repository::fence_from_claim(&stored)
            .expect("same owned attempt after input mutation"),
        elephant_storage::work_attempt_repository::fence_from_claim(renewed)
            .expect("original captured attempt")
    );
    let denied = renew_execution_work(queue, renewed, 300)
        .await
        .expect_err("renewal cannot adopt replacement input");
    assert!(matches!(denied, PipelineError::LeaseConflict(_)));
    assert!(
        PreparedStandingSourceAuthority::capture_for_work(storage, renewed)
            .await
            .is_err()
    );
    assert_eq!(
        revisions
            .find_by_doc(&doc.base.id)
            .await
            .expect("one render")
            .len(),
        1
    );
    queue
        .release(space, &original.base.id)
        .await
        .expect("release original attempt");
    assert!(matches!(
        renew_execution_work(queue, renewed, 300).await,
        Err(PipelineError::LeaseConflict(_))
    ));
}

// Deliberate faulty-producer fixture, not a new public mutation capability.
// Both backends check/touch the original durable attempt in the same transaction
// as the changed input; no fabricated fence, lease, replacement claim or bypass.
async fn change_input_under_original_fence(
    storage: &StorageClient,
    original: &WorkItem,
    changed: &WorkItem,
) {
    let fence = elephant_storage::work_attempt_repository::fence_from_claim(original)
        .expect("original owned mutation fence");
    if let Some(store) = storage.embedded_store() {
        store
            .mutate(|tx| {
                elephant_storage::work_attempt_repository::check_attempt_fence(
                    tx,
                    &fence,
                    Utc::now(),
                )?;
                assert_eq!(
                    tx.get::<WorkItem>(&original.base.space_id, &original.base.id, false)?,
                    Some(original.clone())
                );
                Ok(tx.replace(changed.clone())?)
            })
            .await
            .expect("fenced embedded input mutation");
    } else {
        let mut sql = String::from("BEGIN TRANSACTION;\n");
        let mut bindings = std::collections::BTreeMap::new();
        elephant_storage::work_attempt_repository::append_attempt_fence(
            &mut sql,
            &mut bindings,
            &fence,
        )
        .expect("append original native attempt fence");
        bindings.insert(
            "expected_payload".into(),
            elephant_storage::surreal::json_to_surreal(original.payload.clone())
                .expect("original payload binding"),
        );
        bindings.insert(
            "changed_doc".into(),
            elephant_storage::surreal::json_to_surreal(changed.payload["doc_id"].clone())
                .expect("changed document binding"),
        );
        sql.push_str(
            "IF $fenced_work.payload != $expected_payload { THROW 'fixture_input_changed'; }; \
             UPDATE $attempt_work_item SET payload.doc_id = $changed_doc; COMMIT TRANSACTION;",
        );
        storage
            .query_bind_checked(&sql, bindings)
            .await
            .expect("fenced native input mutation");
    }
}

#[tokio::test]
async fn embedded_renewed_execution_snapshot_renders_and_refuses_changed_input() {
    Box::pin(renewal_render(true)).await;
}

#[tokio::test]
async fn native_renewed_execution_snapshot_renders_and_refuses_changed_input() {
    Box::pin(renewal_render(false)).await;
}

#[test]
fn failure_context_reports_only_exact_static_authority_code() {
    let original = anyhow::Error::new(PipelineError::StorageError(StorageError::Conflict(
        "standing_watch_authority_changed_or_missing".into(),
    )))
    .context("render_doc failed");
    assert_eq!(
        failure_context_code(&original),
        Some("original_work_authority_changed")
    );
    for private in [
        "standing_watch_authority_changed_or_missing: private-source-canary",
        "provider credential private-secret-canary",
    ] {
        let unrecognized = anyhow::Error::new(PipelineError::StorageError(StorageError::Conflict(
            private.into(),
        )))
        .context("render_doc failed");
        assert_eq!(failure_context_code(&unrecognized), None);
    }
}
