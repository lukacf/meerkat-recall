//! Actual dispatcher races, without changing the source head while fetching.

use super::*;
use elephant_pipeline::remote_source::RemoteFetchPolicy;
use elephant_storage::source_intake::{IntakeControl, PreparedSourceIntake};
use elephant_storage::source_publication::SourcePublicationRepository;
use elephant_types::records::{SourceWatch, SourceWatchOutcome, SourceWatchStatus};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::oneshot;

const SPACE: &str = "quiet-watch-attempt-space";
const ACTOR: &str = "watch-attempt-oracle";
const OLD: &str = "The delivery gate closes at six.";
const NEW: &str = "The delivery gate now closes at seven.";

#[derive(Clone, Copy)]
enum LeaseChange {
    None,
    Reclaim,
    Cancel,
}

#[derive(Clone, Copy)]
enum Response {
    Changed,
    NotModified,
    Unavailable,
}

fn hash(text: &str) -> String {
    hex::encode(Sha256::digest(text.as_bytes()))
}

fn json(value: &impl serde::Serialize) -> serde_json::Value {
    serde_json::to_value(value).expect("fixture JSON")
}

#[test]
fn erased_original_contact_plan_is_terminal_for_the_original_watch_intent() {
    let error = anyhow::Error::new(PipelineError::InvalidState(
        "accepted_contact_plan_unavailable".into(),
    ));
    assert_eq!(classify_failure(&error), FailureClass::Terminal);
}

#[expect(
    clippy::if_not_else,
    clippy::significant_drop_tightening,
    reason = "Keep the obsolete-attempt branch first and preserve original repository lifetimes"
)]
async fn contract(embedded: bool, lease_change: LeaseChange, response: Response) {
    let storage = if embedded {
        Box::pin(StorageClient::new_embedded())
            .await
            .expect("embedded")
    } else {
        Box::pin(StorageClient::new_memory()).await.expect("native")
    };
    Box::pin(initialize_fixture_schema(&storage)).await;
    Box::pin(ensure_fixture_space(&storage, SPACE, ACTOR)).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let uri = format!("http://{}/source", listener.local_addr().expect("address"));
    let (arrived_tx, arrived_rx) = oneshot::channel();
    let (resume_tx, resume_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept");
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            assert!(request.len() < 8192, "bounded request header");
            request.push(socket.read_u8().await.expect("request byte"));
        }
        arrived_tx.send(()).expect("announce fetch");
        resume_rx.await.expect("release fetch");
        let wire = match response {
            Response::Changed => format!("HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{NEW}", NEW.len()),
            Response::NotModified => "HTTP/1.1 304 Not Modified\r\nConnection: close\r\n\r\n".into(),
            Response::Unavailable => "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into(),
        };
        socket.write_all(wire.as_bytes()).await.expect("response");
    });

    let mut source = Source::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Source),
            SPACE.into(),
            ACTOR.into(),
        ),
        SourceType::Web,
        hash(OLD),
    );
    source.uri = Some(uri.clone());
    source.external_id = Some("original-watched-source".into());
    source
        .set_manual_doc_template(&ManualDocTemplate {
            doc_id: RecordId::new_v4(TableName::Doc).to_string(),
            title: "Gate hours".into(),
            text: OLD.into(),
            sidecar_text: None,
            media_attachments: vec![],
            rendition_key: "original-watch".into(),
            metadata: serde_json::json!({"content_type":"text/plain"}),
        })
        .expect("template");
    let prepared = Box::pin(PreparedSourceIntake::prepare(
        &storage,
        source,
        IntakeControl::default(),
    ))
    .await
    .expect("prepare original");
    let accepted = Box::pin(prepared.accept(&storage))
        .await
        .expect("accept original");
    let source_id = accepted.source.base.id.clone();
    let mut base = accepted.source.base.clone();
    base.id = RecordId::new_v4(TableName::SourceWatch);
    let watch = SourceWatch {
        base,
        source: Link::new(source_id.clone()),
        requested_uri: uri,
        status: SourceWatchStatus::Enabled,
        interval_seconds: 3600,
        next_check_at: Utc::now(),
        last_checked_at: None,
        last_changed_at: None,
        etag: None,
        last_modified: None,
        observed_hash: Some(hash(OLD)),
        consecutive_failures: 0,
        last_error: None,
        latest_check: None,
    };
    let watches = SourceWatchRepository::new(storage.clone());
    let watch = watches.create(watch).await.expect("actual watch");
    Box::pin(super::source_watch_test_authority::register_local(
        &storage, &watch,
    ))
    .await;
    let work_id = Box::pin(enqueue_source_watch_refresh(&storage, &watch, ACTOR, 5))
        .await
        .expect("actual watch queue intent");
    let items = WorkItemRepository::new(storage.clone());
    let claim = items
        .claim_with_lease(
            SPACE,
            &work_id,
            ACTOR,
            Utc::now() + chrono::Duration::minutes(5),
        )
        .await
        .expect("claim")
        .expect("original claim");
    let original = elephant_storage::work_attempt_repository::fence_from_claim(&claim)
        .expect("original fence");
    let run_id = Box::pin(create_work_run(&storage, SPACE, ACTOR, &work_id)).await;
    let runs = WorkRunRepository::new(storage.clone());
    let mut run = runs
        .get(SPACE, &run_id)
        .await
        .expect("run")
        .expect("present");
    run.attempt = claim.attempts;
    run.metrics
        .custom
        .insert("attempt_fence".into(), json(&original));
    runs.update(SPACE, &run_id, run)
        .await
        .expect("original run authority");

    let publication = SourcePublicationRepository::new(storage.clone());
    let before_head = publication
        .head(SPACE, &source_id)
        .await
        .expect("head")
        .expect("present");
    let before_source = SourceRepository::new(storage.clone())
        .get(SPACE, &source_id)
        .await
        .expect("source");
    let before_docs = DocRepository::new(storage.clone())
        .find_by_source(SPACE, &source_id, 100)
        .await
        .expect("docs");
    let before_operations = publication
        .recoverable_operations(100)
        .await
        .expect("operations");
    let dispatch_storage = storage.clone();
    let dispatch = tokio::spawn(WATCH_FETCH_POLICY.scope(
        RemoteFetchPolicy {
            allow_private_networks: true,
            timeout: std::time::Duration::from_secs(20),
            ..RemoteFetchPolicy::web_watch()
        },
        async move {
            Box::pin(execute_work_item_with_extractor(
                &claim,
                &run_id,
                dispatch_storage,
                &WorkQueueWorkerConfig::default(),
                &TmContext::none(),
                None,
            ))
            .await
        },
    ));
    tokio::time::timeout(std::time::Duration::from_secs(10), arrived_rx)
        .await
        .expect("dispatcher reaches real fetch")
        .expect("fetch announced");

    match lease_change {
        LeaseChange::None => {}
        LeaseChange::Cancel => {
            items
                .update_running_for_worker(
                    SPACE,
                    &work_id,
                    &original.lease_token,
                    WorkItemStatus::Cancelled,
                    Some("authorized fixture cancellation".into()),
                )
                .await
                .expect("cancel")
                .expect("original token cancelled");
        }
        LeaseChange::Reclaim => {
            // Expire through the real ownership transition, then use the actual
            // all-space recovery sweep. The source head stays untouched.
            items
                .renew_lease(
                    SPACE,
                    &work_id,
                    &original.lease_token,
                    Utc::now() + chrono::Duration::seconds(1),
                )
                .await
                .expect("short original lease")
                .expect("original still owned");
            tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
            assert_eq!(
                items
                    .recover_expired_leases_all_spaces()
                    .await
                    .expect("quiet space recovery"),
                1
            );
            let successor = items
                .claim_with_lease(
                    SPACE,
                    &work_id,
                    "successor",
                    Utc::now() + chrono::Duration::minutes(5),
                )
                .await
                .expect("reclaim")
                .expect("successor");
            let successor_fence =
                elephant_storage::work_attempt_repository::fence_from_claim(&successor)
                    .expect("successor fence");
            assert!(successor_fence.generation > original.generation);
            assert_ne!(successor_fence.lease_token, original.lease_token);
        }
    }
    assert_eq!(
        json(&publication.head(SPACE, &source_id).await.expect("head")),
        json(&Some(before_head.clone()))
    );
    let expected_work = items
        .list(SPACE, 100, None)
        .await
        .expect("post-transition queue");
    resume_tx.send(()).expect("release response");
    let result = tokio::time::timeout(std::time::Duration::from_secs(15), dispatch)
        .await
        .expect("bounded dispatcher")
        .expect("dispatcher task");
    server.await.expect("server task");

    if !matches!(lease_change, LeaseChange::None) {
        let error = result.expect_err("obsolete attempt must not become successful check work");
        assert_eq!(classify_failure(&error), FailureClass::LostLease);
        assert_eq!(
            json(&watches.get(SPACE, &watch.base.id).await.expect("watch")),
            json(&Some(watch.clone()))
        );
        assert!(watches
            .list_checks(SPACE, &watch.base.id, 100)
            .await
            .expect("checks")
            .is_empty());
        assert_eq!(
            json(&publication.head(SPACE, &source_id).await.expect("head")),
            json(&Some(before_head.clone()))
        );
        assert_eq!(
            json(
                &SourceRepository::new(storage.clone())
                    .get(SPACE, &source_id)
                    .await
                    .expect("source")
            ),
            json(&before_source)
        );
        assert_eq!(
            json(
                &DocRepository::new(storage.clone())
                    .find_by_source(SPACE, &source_id, 100)
                    .await
                    .expect("docs")
            ),
            json(&before_docs)
        );
        assert_eq!(
            json(
                &publication
                    .recoverable_operations(100)
                    .await
                    .expect("operations")
            ),
            json(&before_operations)
        );
        assert_eq!(
            json(&items.list(SPACE, 100, None).await.expect("queue")),
            json(&expected_work)
        );
        let revision = format!("watch:{}:{}", before_head.generation, hash(NEW));
        assert!(publication
            .revision(SPACE, &source_id, &revision)
            .await
            .expect("no orphan revision")
            .is_none());
    } else {
        result.expect("valid original attempt completes check");
        let checks = watches
            .list_checks(SPACE, &watch.base.id, 100)
            .await
            .expect("checks");
        assert_eq!(checks.len(), 1);
        match response {
            Response::Changed => {
                assert_eq!(checks[0].outcome, SourceWatchOutcome::Changed);
                let head = publication
                    .head(SPACE, &source_id)
                    .await
                    .expect("head")
                    .expect("present");
                assert_ne!(head.generation, before_head.generation);
                let queued = items.list(SPACE, 100, None).await.expect("queue");
                assert_eq!(queued.len(), expected_work.len() + 1);
                assert!(queued
                    .iter()
                    .any(|item| item.payload["_publication"]["generation"] == head.generation));
            }
            Response::NotModified => assert_eq!(checks[0].outcome, SourceWatchOutcome::NotModified),
            Response::Unavailable => assert_eq!(checks[0].outcome, SourceWatchOutcome::Failed),
        }
    }
}

macro_rules! race {
    ($name:ident, $embedded:expr, $lease:ident, $response:ident) => {
        #[tokio::test]
        async fn $name() {
            Box::pin(contract(
                $embedded,
                LeaseChange::$lease,
                Response::$response,
            ))
            .await;
        }
    };
}

race!(embedded_original_changed, true, None, Changed);
race!(native_original_changed, false, None, Changed);
race!(embedded_reclaimed_changed, true, Reclaim, Changed);
race!(native_reclaimed_changed, false, Reclaim, Changed);
race!(embedded_cancelled_changed, true, Cancel, Changed);
race!(native_cancelled_changed, false, Cancel, Changed);
race!(embedded_reclaimed_not_modified, true, Reclaim, NotModified);
race!(native_reclaimed_not_modified, false, Reclaim, NotModified);
race!(embedded_cancelled_fetch_failure, true, Cancel, Unavailable);
race!(native_cancelled_fetch_failure, false, Cancel, Unavailable);
race!(embedded_original_not_modified, true, None, NotModified);
race!(native_original_not_modified, false, None, NotModified);
