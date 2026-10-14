//! Barrier-synchronized admission through the repository used by public PUT.
//! These checks cover atomic storage semantics, not HTTP status/authentication.
//! No caller retries, queue dispatch, providers or publication shortcuts.

use super::*;
use elephant_storage::canonical_commit::CanonicalRepository;
use elephant_storage::source_publication::{AcceptSourceRevision, SourcePublicationRepository};
use elephant_storage::{StorageError, StorageResult};
use elephant_types::publication::{OperationReceipt, OperationState, SourcePrecondition};
use futures::future::join_all;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

const SPACE: &str = "concurrent-source-admission";
const ACTOR: &str = "source-admission-oracle";
const COUNT: usize = 14;
const OBSERVED: &str = "2026-10-06T20:11:00Z";

fn request(index: usize) -> AcceptSourceRevision {
    let text = format!("Exact immutable UTF-8 source {index}: Aria works at Cedar. é");
    let mut source = Source::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Source),
            SPACE.into(),
            ACTOR.into(),
        ),
        SourceType::Manual,
        elephant_pipeline::extraction::AssertionValidator::hash_quote(&text),
    );
    source.external_id = Some(format!("simultaneous-source-{index}"));
    source.base.security.labels = vec![format!("source-{index}")];
    source.metadata = serde_json::json!({"observed_at": OBSERVED});
    source
        .set_manual_doc_template(&ManualDocTemplate {
            doc_id: RecordId::new_v4(TableName::Doc).to_string(),
            title: format!("Source {index}"),
            text,
            sidecar_text: None,
            media_attachments: vec![],
            rendition_key: "r1".into(),
            metadata: serde_json::json!({"observed_at": OBSERVED}),
        })
        .unwrap();
    source.set_requested_ingest_mode(IngestMode::Graph);
    AcceptSourceRevision {
        source,
        revision_id: "r1".into(),
        expected: SourcePrecondition::Absent,
        idempotency_key: format!("immutable-intent-{index}"),
        request_fingerprint: format!("immutable-request-{index}"),
        reactivate: false,
        recreate_after_erasure: false,
        authorized: None,
        authorized_document: None,
    }
}

async fn concurrent_accept(
    repository: &SourcePublicationRepository,
    requests: Vec<AcceptSourceRevision>,
) -> Vec<StorageResult<OperationReceipt>> {
    let barrier = Arc::new(tokio::sync::Barrier::new(requests.len()));
    let pending = requests.into_iter().map(|request| {
        let barrier = barrier.clone();
        async move {
            barrier.wait().await;
            Box::pin(repository.accept(request)).await
        }
    });
    tokio::time::timeout(Duration::from_secs(15), join_all(pending))
        .await
        .expect("original concurrent admission bound; no caller retry")
}

async fn assert_immutable_acceptance(
    repository: &SourcePublicationRepository,
    request: &AcceptSourceRevision,
    receipt: &OperationReceipt,
) {
    assert_eq!(receipt.state, OperationState::Accepted);
    assert_eq!(receipt.source_id, request.source.base.id);
    assert_eq!(receipt.desired_revision.as_deref(), Some("r1"));
    assert!(receipt.published_revision.is_none());
    assert!(receipt.completed_at.is_none());
    let head = repository
        .head(SPACE, &receipt.source_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(head.operation_id, receipt.operation_id);
    assert_eq!(head.generation, receipt.source_generation);
    assert_eq!(head.desired_revision, receipt.desired_revision);
    assert!(head.published_revision.is_none());
    assert_eq!(
        repository
            .operation(SPACE, &receipt.operation_id)
            .await
            .unwrap(),
        Some(receipt.clone())
    );
    let revision = repository
        .revision(SPACE, &receipt.source_id, "r1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(revision.accepted_at, receipt.accepted_at);
    assert_eq!(revision.content_hash, request.source.hash);
    assert_eq!(revision.source.metadata, request.source.metadata);
    assert_eq!(revision.source.base.security, request.source.base.security);
    assert_eq!(revision.source.base.subjects, request.source.base.subjects);
    assert_eq!(
        revision.source.base.subjects_state,
        request.source.base.subjects_state
    );
    let envelope = repository
        .operation_authorization(SPACE, &receipt.operation_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(envelope.security, request.source.base.security);
    assert_eq!(envelope.subjects, request.source.base.subjects);
}

async fn competing_replacements(
    repository: &SourcePublicationRepository,
    original: &AcceptSourceRevision,
    receipt: &OperationReceipt,
) {
    let authorization = repository
        .authorization_snapshot(SPACE, &receipt.source_id)
        .await
        .unwrap();
    let requests: Vec<_> = (0..COUNT)
        .map(|index| {
            let mut next = original.clone();
            next.revision_id = format!("replacement-{index}");
            next.idempotency_key = format!("replacement-intent-{index}");
            next.request_fingerprint = format!("replacement-request-{index}");
            next.source.hash = format!("changed-content-{index}");
            next.source.metadata["_manual_doc"]["text"] =
                serde_json::json!(format!("Distinct candidate {index}"));
            next.expected = SourcePrecondition::Generation(receipt.source_generation.clone());
            next.authorized = authorization.clone();
            next
        })
        .collect();
    let outcomes = Box::pin(concurrent_accept(repository, requests.clone())).await;
    assert_eq!(
        outcomes.iter().filter(|outcome| outcome.is_ok()).count(),
        1,
        "exactly one original generation may change"
    );
    for (request, outcome) in requests.iter().zip(outcomes) {
        if let Ok(winner) = outcome {
            let head = repository
                .head(SPACE, &receipt.source_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(head.operation_id, winner.operation_id);
            assert_eq!(
                head.desired_revision.as_deref(),
                Some(request.revision_id.as_str())
            );
            assert_ne!(head.generation, receipt.source_generation);
        } else {
            assert!(
                matches!(&outcome, Err(StorageError::Conflict(_))),
                "stale generation must be refused as a conflict, not admission exhaustion: {outcome:?}"
            );
            assert!(
                repository
                    .operation_by_idempotency_key(SPACE, &request.idempotency_key)
                    .await
                    .unwrap()
                    .is_none(),
                "refused replacement left an operation"
            );
            assert!(
                repository
                    .revision(SPACE, &receipt.source_id, &request.revision_id)
                    .await
                    .unwrap()
                    .is_none(),
                "refused replacement left immutable input"
            );
        }
    }
    // The old accepted input remains exact even after a competing winner.
    assert_eq!(
        repository
            .revision(SPACE, &receipt.source_id, "r1")
            .await
            .unwrap()
            .unwrap()
            .source
            .metadata,
        original.source.metadata
    );
}

#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete concurrent admission, idempotency, rollback and epoch contract in one backend fixture"
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture storage and repository owners alive through the complete fault sequence and final assertions"
)]
async fn concurrent_admission_contract(embedded: bool) {
    let storage = if embedded {
        StorageClient::new_embedded().await.unwrap()
    } else {
        let storage = StorageClient::new_memory().await.unwrap();
        Box::pin(initialize_fixture_schema(&storage)).await;
        storage
    };
    ensure_fixture_space(&storage, SPACE, ACTOR).await;
    let repository = SourcePublicationRepository::new(storage.clone());
    let canonical = CanonicalRepository::new(storage.clone());
    let original_epoch = canonical.epoch(SPACE).await.unwrap();
    let requests: Vec<_> = (0..COUNT).map(request).collect();
    let outcomes = Box::pin(concurrent_accept(&repository, requests.clone())).await;
    let receipts: Vec<_> = outcomes
        .into_iter()
        .map(|outcome| {
            outcome.expect(
                "each independent same-space intent must be accepted without a caller workaround",
            )
        })
        .collect();
    assert_eq!(
        receipts
            .iter()
            .map(|receipt| &receipt.operation_id)
            .collect::<BTreeSet<_>>()
            .len(),
        COUNT
    );
    for (request, receipt) in requests.iter().zip(&receipts) {
        Box::pin(assert_immutable_acceptance(&repository, request, receipt)).await;
    }
    let accepted_epoch = canonical.current_epoch(SPACE).await.unwrap();
    assert_ne!(
        accepted_epoch, original_epoch,
        "new source membership must invalidate old semantic input"
    );

    let mut replays = requests.clone();
    for replay in &mut replays {
        replay.source.metadata["observed_at"] = serde_json::json!("2026-10-12T20:11:00Z");
        replay.source.metadata["_manual_doc"]["metadata"]["observed_at"] =
            serde_json::json!("2026-10-12T20:11:00Z");
    }
    let replayed = Box::pin(concurrent_accept(&repository, replays)).await;
    for ((request, original), replay) in requests.iter().zip(&receipts).zip(replayed) {
        assert_eq!(
            &replay.unwrap(),
            original,
            "replay retains generation, operation and acceptance time"
        );
        Box::pin(assert_immutable_acceptance(&repository, request, original)).await;
    }
    assert_eq!(
        canonical.current_epoch(SPACE).await.unwrap(),
        accepted_epoch,
        "observation-only replay does not mutate semantic membership"
    );

    for changed_fingerprint in [false, true] {
        let mut conflicting = requests[1].clone();
        if changed_fingerprint {
            conflicting.request_fingerprint = "changed-request-intent".into();
        } else {
            conflicting.source.hash = "forged-content".into();
            conflicting.source.metadata["_manual_doc"]["text"] =
                serde_json::json!("Unsupported replacement under the original key");
        }
        assert!(
            matches!(
                repository.accept(conflicting).await,
                Err(StorageError::Conflict(_))
            ),
            "same-key changed content/intent must be refused"
        );
    }
    Box::pin(assert_immutable_acceptance(
        &repository,
        &requests[1],
        &receipts[1],
    ))
    .await;
    assert_eq!(
        canonical.current_epoch(SPACE).await.unwrap(),
        accepted_epoch
    );

    // Fourteen callers sharing one new intent must converge to one original receipt.
    let duplicate = request(COUNT);
    let duplicate_results = Box::pin(concurrent_accept(
        &repository,
        vec![duplicate.clone(); COUNT],
    ))
    .await;
    let duplicate_receipts: Vec<_> = duplicate_results.into_iter().map(Result::unwrap).collect();
    assert!(duplicate_receipts
        .iter()
        .all(|receipt| receipt == &duplicate_receipts[0]));
    Box::pin(assert_immutable_acceptance(
        &repository,
        &duplicate,
        &duplicate_receipts[0],
    ))
    .await;
    let before_replacement = canonical.current_epoch(SPACE).await.unwrap();
    Box::pin(competing_replacements(
        &repository,
        &requests[0],
        &receipts[0],
    ))
    .await;
    assert_ne!(
        canonical.current_epoch(SPACE).await.unwrap(),
        before_replacement
    );

    if !embedded {
        let before_nonce = canonical.current_epoch(SPACE).await.unwrap();
        storage.query_bind_checked(
            "UPDATE source_head SET fence_nonce = <string>rand::uuid() WHERE space_id = $space;",
            std::collections::BTreeMap::from([("space", SPACE.to_owned())]),
        ).await.unwrap();
        assert_eq!(
            canonical.current_epoch(SPACE).await.unwrap(),
            before_nonce,
            "physical source nonce fences must not alter the semantic epoch"
        );
    }
    assert_eq!(
        elephant_storage::SourceRepository::new(storage.clone())
            .list(SPACE, 100, None)
            .await
            .unwrap()
            .len(),
        COUNT + 1
    );
    assert!(
        WorkItemRepository::new(storage)
            .list(SPACE, 100, None)
            .await
            .unwrap()
            .is_empty(),
        "admission controls must not invoke a queue or provider"
    );
}

#[tokio::test]
async fn embedded_fourteen_same_space_source_admissions_preserve_atomic_identity() {
    Box::pin(concurrent_admission_contract(true)).await;
}

#[tokio::test]
async fn surreal_fourteen_same_space_source_admissions_preserve_atomic_identity() {
    Box::pin(concurrent_admission_contract(false)).await;
}
