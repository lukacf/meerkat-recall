//! Original admitted ancestry is authoritative; raw traversal hints are not.
#![allow(clippy::expect_used, clippy::too_many_lines)]

use super::*;
use crate::work_producer_repository::{
    append_reprocess_intent_with_parent_sql, PreparedReprocessParent, TABLE,
};
use crate::{ReprocessJobRepository, StorageClient, WorkItemRepository, WorkRunRepository};
use elephant_types::records::reprocess::{
    ReprocessJob, ReprocessProgress, ReprocessStatus, ReprocessTargetStage, SelectionCriteria,
};
use elephant_types::records::{BaseFields, WorkRunMetrics};
use elephant_types::Link;

const SPACE: &str = "original-chain-observation";

struct Claimed {
    work: WorkItem,
    run: WorkRun,
    origin: OriginEnvelope,
}

async fn storage() -> StorageClient {
    let client = Box::pin(StorageClient::new_memory()).await.expect("native");
    for schema in [
        include_str!("../../migrations/sql/028_work_queue_indexes.surql"),
        include_str!("../../migrations/sql/029_work_history_archives.surql"),
        include_str!("../../migrations/sql/031_source_publication.surql"),
        include_str!("../../migrations/sql/032_work_recovery.surql"),
        include_str!("../../migrations/sql/041_work_producer_origin.surql"),
    ] {
        client.query_checked(schema).await.expect("original schema");
    }
    crate::reference_fence::install_native_reference_guards(&client, &[])
        .await
        .expect("actual reference guards");
    client
}

fn work(base: BaseFields) -> WorkItem {
    WorkItem {
        base,
        kind: "tm_recompute".into(),
        payload: serde_json::json!({"selection":"original"}),
        status: WorkItemStatus::Runnable,
        priority: 1,
        idempotency_key: uuid::Uuid::new_v4().to_string(),
        attempts: 0,
        lease_expires_at: None,
        leased_by: None,
        run_after: None,
        last_error: None,
    }
}

fn successor(parent: &Claimed) -> WorkItem {
    let mut base = parent.work.base.clone();
    base.id = RecordId::new_v4(TableName::WorkItem);
    work(base)
}

async fn claim(client: &StorageClient, original: &WorkItem) -> Claimed {
    let work = WorkItemRepository::new(client.clone())
        .claim_with_lease(
            SPACE,
            &original.base.id,
            "actual-original-worker",
            chrono::Utc::now() + chrono::Duration::minutes(5),
        )
        .await
        .expect("claim")
        .expect("owned original attempt");
    let fence = fence_from_claim(&work).expect("original live claim");
    let mut metrics = WorkRunMetrics::default();
    metrics.custom.insert(
        "attempt_fence".into(),
        serde_json::to_value(&fence).expect("original fence"),
    );
    let mut base = work.base.clone();
    base.id = RecordId::new_v4(TableName::WorkRun);
    let run = WorkRunRepository::new(client.clone())
        .create(WorkRun {
            base,
            work_item: Link::new(work.base.id.clone()),
            attempt: work.attempts,
            started_at: chrono::Utc::now(),
            completed_at: None,
            success: false,
            error: None,
            metrics,
            worker_id: fence.lease_token,
            worker_version: "original".into(),
            model_version: None,
            prompt_id: None,
            config_hash: None,
        })
        .await
        .expect("actual original execution receipt");
    let origin = WorkProducerRepository::new(client.clone())
        .read_envelope(&work)
        .await
        .expect("read original origin")
        .expect("admitted private evidence");
    Claimed { work, run, origin }
}

async fn direct(client: &StorageClient) -> Claimed {
    let root = WorkItemRepository::new(client.clone())
        .create(work(BaseFields::new(
            RecordId::new_v4(TableName::WorkItem),
            SPACE.into(),
            "original-owner".into(),
        )))
        .await
        .expect("actual independent admission");
    let root = Box::pin(claim(client, &root)).await;
    assert!(matches!(&root.origin.origin, WorkProducerOrigin::Direct));
    root
}

async fn child(client: &StorageClient, parent: &Claimed) -> Claimed {
    let original = Box::pin(
        WorkProducerRepository::new(client.clone()).enqueue_successor(
            &parent.work,
            &parent.run,
            successor(parent),
            false,
        ),
    )
    .await
    .expect("actual same-attempt child admission");
    let child = Box::pin(claim(client, &original)).await;
    verify_parent_origin(&child.origin.origin, &parent.work, &parent.run)
        .expect("actual stored parent edge");
    child
}

async fn chain(client: &StorageClient) -> [Claimed; 3] {
    let root = Box::pin(direct(client)).await;
    let middle = Box::pin(child(client, &root)).await;
    let leaf = Box::pin(child(client, &middle)).await;
    [root, middle, leaf]
}

fn row(work: &Claimed) -> CanonicalRecord {
    CanonicalRecord::WorkItem(Box::new(work.work.clone()))
}

fn run(work: &Claimed) -> CanonicalRecord {
    CanonicalRecord::WorkRun(Box::new(work.run.clone()))
}

async fn exact_observation(
    client: &StorageClient,
    current: &Claimed,
    expected_rows: &[CanonicalRecord],
    expected_origins: &[(WorkItem, OriginEnvelope)],
) {
    let (rows, origins) =
        Box::pin(WorkProducerRepository::new(client.clone()).lineage_dependencies(&current.work))
            .await
            .expect("complete original ancestry observation");
    assert_eq!(
        serde_json::to_value(&rows).expect("actual typed rows"),
        serde_json::to_value(expected_rows).expect("independent original ordered rows"),
        "retain every complete original row and its traversal position"
    );
    assert_eq!(
        origins, expected_origins,
        "retain exact original payloads in order"
    );
}

async fn exact_chain(client: &StorageClient, chain: &[Claimed; 3]) {
    let [root, middle, leaf] = chain;
    exact_observation(
        client,
        leaf,
        &[row(leaf), run(middle), row(middle), run(root), row(root)],
        &[
            (leaf.work.clone(), leaf.origin.clone()),
            (middle.work.clone(), middle.origin.clone()),
            (root.work.clone(), root.origin.clone()),
        ],
    )
    .await;
}

fn original_hint(origin: &OriginEnvelope, field: &str) -> Value {
    let fields = match origin.native().expect("original native private control") {
        Value::Object(fields) => Some(fields),
        _ => None,
    }
    .expect("private control must be an object");
    fields
        .get(field)
        .expect("known original hint field")
        .clone()
}

async fn replace_hint(client: &StorageClient, origin: &OriginEnvelope, field: &str, value: Value) {
    assert!(matches!(
        field,
        "space_id" | "work_item_id" | "parent_work_item_id" | "parent_work_run_id" | "payload"
    ));
    client
        .query_bind_checked(
            &format!("UPDATE $id SET {field}=$value;"),
            crate::bindings! {"id" => origin_id(&origin.work_item_id), "value" => value},
        )
        .await
        .expect("controlled invalid hint after actual admission");
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep repository in the original fixture scope through assertions and cleanup."
)]
async fn refusal_without_child(client: &StorageClient, parent: &Claimed) {
    let repository = WorkProducerRepository::new(client.clone());
    assert!(Box::pin(repository.lineage_dependencies(&parent.work))
        .await
        .is_err());
    let candidate = successor(parent);
    // The actual admission derives its physical ID from the original parent
    // receipt and semantic key. Checking the caller's random ID proves nothing.
    let key = source_support_fingerprint(&serde_json::json!({
        "format": "work-producer-child-key/v1",
        "space_id": parent.work.base.space_id,
        "parent_work_item_id": parent.work.base.id,
        "original_parent_work_run_id": parent.run.base.id,
        "kind": candidate.kind,
        "semantic_key": candidate.idempotency_key,
    }))
    .expect("actual original child key");
    let mut attempted = candidate.clone();
    attempted.base.id = RecordId::new(
        TableName::WorkItem,
        uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, key.as_bytes()),
    );
    let result =
        Box::pin(repository.enqueue_successor(&parent.work, &parent.run, candidate.clone(), false))
            .await;
    assert!(
        result.is_err(),
        "invalid original hints admitted a child: {result:?}"
    );
    assert!(WorkItemRepository::new(client.clone())
        .get(SPACE, &attempted.base.id)
        .await
        .expect("check no child")
        .is_none());
    assert!(repository
        .read_envelope(&attempted)
        .await
        .expect("check no child origin")
        .is_none());
    assert_eq!(
        WorkItemRepository::new(client.clone())
            .get(SPACE, &parent.work.base.id)
            .await
            .expect("original work"),
        Some(parent.work.clone())
    );
    assert_eq!(
        WorkRunRepository::new(client.clone())
            .get(SPACE, &parent.run.base.id)
            .await
            .expect("original run"),
        Some(parent.run.clone())
    );
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep client in the original fixture scope through assertions and cleanup."
)]
async fn native_observation_preserves_actual_admitted_chain_rows_runs_and_origins() {
    let client = Box::pin(storage()).await;
    let chain = Box::pin(chain(&client)).await;
    exact_chain(&client, &chain).await;
    let last = Box::pin(child(&client, &chain[2])).await;
    exact_observation(
        &client,
        &last,
        &[
            row(&last),
            run(&chain[2]),
            row(&chain[2]),
            run(&chain[1]),
            row(&chain[1]),
            run(&chain[0]),
            row(&chain[0]),
        ],
        &[
            (last.work.clone(), last.origin.clone()),
            (chain[2].work.clone(), chain[2].origin.clone()),
            (chain[1].work.clone(), chain[1].origin.clone()),
            (chain[0].work.clone(), chain[0].origin.clone()),
        ],
    )
    .await;
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep client in the original fixture scope through assertions and cleanup."
)]
async fn native_observation_rejects_untrusted_hint_fields_before_child_creation() {
    let client = Box::pin(storage()).await;
    let chain = Box::pin(chain(&client)).await;
    let [root, middle, leaf] = &chain;
    exact_chain(&client, &chain).await;
    let wrong_native_kind = Thing::from((
        "work_item",
        surrealdb::sql::Id::String(middle.work.base.id.id().to_string()),
    ));
    for (field, value) in [
        ("parent_work_item_id", Value::None),
        ("parent_work_run_id", Value::None),
        (
            "parent_work_item_id",
            crate::surreal::thing_from_record_id(&root.work.base.id).into(),
        ),
        (
            "parent_work_run_id",
            crate::surreal::thing_from_record_id(&root.run.base.id).into(),
        ),
        (
            "parent_work_item_id",
            crate::surreal::thing_from_record_id(&middle.run.base.id).into(),
        ),
        (
            "parent_work_run_id",
            crate::surreal::thing_from_record_id(&middle.work.base.id).into(),
        ),
        (
            "parent_work_item_id",
            middle.work.base.id.to_string().into(),
        ),
        ("parent_work_item_id", wrong_native_kind.into()),
        (
            "work_item_id",
            crate::surreal::thing_from_record_id(&RecordId::new_v4(TableName::WorkItem)).into(),
        ),
        ("space_id", Value::from("foreign-space")),
    ] {
        let original = original_hint(&leaf.origin, field);
        replace_hint(&client, &leaf.origin, field, value).await;
        refusal_without_child(&client, leaf).await;
        replace_hint(&client, &leaf.origin, field, original).await;
        exact_chain(&client, &chain).await;
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep client in the original fixture scope through assertions and cleanup."
)]
async fn native_observation_rejects_false_terminal_extra_terminal_hint_and_cycle() {
    let client = Box::pin(storage()).await;
    let chain = Box::pin(chain(&client)).await;
    let [root, middle, leaf] = &chain;
    exact_chain(&client, &chain).await;
    // A child with missing traversal hints cannot become an independent root.
    replace_hint(&client, &leaf.origin, "parent_work_item_id", Value::None).await;
    replace_hint(&client, &leaf.origin, "parent_work_run_id", Value::None).await;
    refusal_without_child(&client, leaf).await;
    for field in ["parent_work_item_id", "parent_work_run_id"] {
        replace_hint(
            &client,
            &leaf.origin,
            field,
            original_hint(&leaf.origin, field),
        )
        .await;
    }
    // A cycle in untrusted projected links cannot replace the real payload edge.
    replace_hint(
        &client,
        &middle.origin,
        "parent_work_item_id",
        crate::surreal::thing_from_record_id(&leaf.work.base.id).into(),
    )
    .await;
    replace_hint(
        &client,
        &middle.origin,
        "parent_work_run_id",
        crate::surreal::thing_from_record_id(&leaf.run.base.id).into(),
    )
    .await;
    refusal_without_child(&client, leaf).await;
    for field in ["parent_work_item_id", "parent_work_run_id"] {
        replace_hint(
            &client,
            &middle.origin,
            field,
            original_hint(&middle.origin, field),
        )
        .await;
    }
    let extra = Box::pin(direct(&client)).await;
    replace_hint(
        &client,
        &root.origin,
        "parent_work_item_id",
        crate::surreal::thing_from_record_id(&extra.work.base.id).into(),
    )
    .await;
    replace_hint(
        &client,
        &root.origin,
        "parent_work_run_id",
        crate::surreal::thing_from_record_id(&extra.run.base.id).into(),
    )
    .await;
    refusal_without_child(&client, leaf).await;
    for field in ["parent_work_item_id", "parent_work_run_id"] {
        replace_hint(
            &client,
            &root.origin,
            field,
            original_hint(&root.origin, field),
        )
        .await;
    }
    exact_chain(&client, &chain).await;
    let mut unknown = root.origin.clone();
    unknown.origin = WorkProducerOrigin::Unknown;
    replace_hint(
        &client,
        &root.origin,
        "payload",
        unknown.payload().expect("negative Unknown payload").into(),
    )
    .await;
    refusal_without_child(&client, leaf).await;
    replace_hint(
        &client,
        &root.origin,
        "payload",
        root.origin
            .payload()
            .expect("original Direct payload")
            .into(),
    )
    .await;
    exact_chain(&client, &chain).await;
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep client in the original fixture scope through assertions and cleanup."
)]
async fn native_observation_requires_original_payload_and_private_string_key() {
    let client = Box::pin(storage()).await;
    let chain = Box::pin(chain(&client)).await;
    let [_, middle, leaf] = &chain;
    exact_chain(&client, &chain).await;
    replace_hint(
        &client,
        &middle.origin,
        "payload",
        "invalid original payload".into(),
    )
    .await;
    refusal_without_child(&client, leaf).await;
    replace_hint(
        &client,
        &middle.origin,
        "payload",
        middle
            .origin
            .payload()
            .expect("actual original bytes")
            .into(),
    )
    .await;
    exact_chain(&client, &chain).await;
    let actual = origin_id(&middle.work.base.id);
    assert!(matches!(&actual.id, surrealdb::sql::Id::String(_)));
    let mut wrong = crate::surreal::thing_from_record_id(&middle.work.base.id);
    wrong.tb = TABLE.into();
    assert!(matches!(&wrong.id, surrealdb::sql::Id::Uuid(_)));
    assert_ne!(
        wrong, actual,
        "native UUID and private string keys are different physical records"
    );
    client.query_bind_checked(
        "BEGIN TRANSACTION; DELETE $actual; CREATE $wrong CONTENT $record; COMMIT TRANSACTION;",
        crate::bindings! {"actual" => actual, "wrong" => wrong.clone(), "record" => middle.origin.native().expect("negative alias only")},
    ).await.expect("replace original with invalid physical-key alias as a negative input");
    refusal_without_child(&client, leaf).await;
    client
        .query_bind_checked("DELETE $wrong;", crate::bindings! {"wrong" => wrong})
        .await
        .expect("remove invalid alias");
    refusal_without_child(&client, leaf).await;
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep client in the original fixture scope through assertions and cleanup."
)]
async fn native_observation_requires_original_parent_work_and_run_rows() {
    let client = Box::pin(storage()).await;
    let chain = Box::pin(chain(&client)).await;
    let [_, middle, leaf] = &chain;
    exact_chain(&client, &chain).await;
    for id in [&middle.work.base.id, &middle.run.base.id] {
        let target = crate::surreal::thing_from_record_id(id);
        client
            .query_bind_checked(
                "UPDATE $id SET space_id='foreign-space';",
                crate::bindings! {"id" => target.clone()},
            )
            .await
            .expect("negative foreign original row");
        refusal_without_child(&client, leaf).await;
        client
            .query_bind_checked(
                "UPDATE $id SET space_id=$space;",
                crate::bindings! {"id" => target.clone(), "space" => SPACE},
            )
            .await
            .expect("restore original row space");
        exact_chain(&client, &chain).await;
        client
            .query_bind_checked(
                "UPDATE $id SET deleted_at=time::now();",
                crate::bindings! {"id" => target.clone()},
            )
            .await
            .expect("negative deleted original row");
        refusal_without_child(&client, leaf).await;
        client
            .query_bind_checked(
                "UPDATE $id SET deleted_at=NONE;",
                crate::bindings! {"id" => target},
            )
            .await
            .expect("restore original live row");
        exact_chain(&client, &chain).await;
    }
    // Remove an actual original receipt, then restore its exact typed bytes;
    // this never creates new successful authority or invents an attempt.
    let original_run = crate::surreal::thing_from_record_id(&middle.run.base.id);
    client
        .query_bind_checked(
            "DELETE $id;",
            crate::bindings! {"id" => original_run.clone()},
        )
        .await
        .expect("negative missing original run");
    refusal_without_child(&client, leaf).await;
    client.query_bind_checked("CREATE $id CONTENT $record;", crate::bindings! {
        "id" => original_run,
        "record" => WorkRunRepository::work_run_to_value(&middle.run).expect("exact admitted original receipt"),
    }).await.expect("restore exact original receipt");
    exact_chain(&client, &chain).await;
    client
        .query_bind_checked(
            "DELETE $id;",
            crate::bindings! {"id" => crate::surreal::thing_from_record_id(&middle.work.base.id)},
        )
        .await
        .expect("negative missing original Work");
    refusal_without_child(&client, leaf).await;
}

async fn archive_original(client: &StorageClient, original: &mut Claimed) {
    let fence = fence_from_claim(&original.work).expect("original claim");
    original.work = WorkItemRepository::new(client.clone())
        .finish_authorized(
            &original.work,
            &fence.lease_token,
            WorkItemStatus::Done,
            None,
        )
        .await
        .expect("finish original admitted attempt")
        .expect("actual owner");
    original.run.success = true;
    original.run.completed_at = Some(chrono::Utc::now());
    original.run = WorkRunRepository::new(client.clone())
        .update(SPACE, &original.run.base.id, original.run.clone())
        .await
        .expect("complete original admitted receipt");
    crate::work_retention::archive_terminal_work(
        client,
        chrono::Utc::now() + chrono::Duration::seconds(1),
        16,
    )
    .await
    .expect("actual original history archival");
    for id in [&original.work.base.id, &original.run.base.id] {
        let mut response = client.query_bind_checked(
            "RETURN array::len((SELECT VALUE id FROM $hot)); RETURN array::len((SELECT VALUE id FROM $cold));",
            crate::bindings! {
                "hot" => crate::surreal::thing_from_record_id(id),
                "cold" => crate::surreal::thing_from_record_id(&crate::work_retention::archive_id(id)),
            },
        ).await.expect("original hot and archive physical presence");
        let hot: Option<u64> = response.take(0).expect("actual hot count");
        let cold: Option<u64> = response.take(1).expect("actual archive count");
        assert_eq!(hot, Some(0), "the original hot record was actually retired");
        assert_eq!(
            cold,
            Some(1),
            "the actual archive contains original history"
        );
    }
}

// A stale archive must not replace the eligible live original, or acquire a
// conflict nonce while the actual admission still guards the live row.
async fn ignored_archive_beside_live_original(client: &StorageClient, chain: &[Claimed; 3]) {
    let root = &chain[0];
    let ids = [&root.work.base.id, &root.run.base.id];
    for id in ids {
        client
            .query_bind_checked(
                "CREATE $cold CONTENT {space_id:$space,original_id:$hot,record:{},marker:'unused-cold'};",
                crate::bindings! {
                    "cold" => crate::surreal::thing_from_record_id(&crate::work_retention::archive_id(id)),
                    "hot" => crate::surreal::thing_from_record_id(id),
                    "space" => SPACE,
                },
            )
            .await
            .expect("negative malformed archive beside genuine live original");
    }
    assert_eq!(
        WorkItemRepository::new(client.clone())
            .get(SPACE, &root.work.base.id)
            .await
            .expect("live work wins")
            .as_ref(),
        Some(&root.work)
    );
    assert_eq!(
        WorkRunRepository::new(client.clone())
            .get(SPACE, &root.run.base.id)
            .await
            .expect("live run wins")
            .as_ref(),
        Some(&root.run)
    );
    exact_chain(client, chain).await;
    let _next = Box::pin(child(client, &chain[2])).await;
    for id in ids {
        let mut response = client
            .query_bind_checked(
                "RETURN (SELECT VALUE canonical_read_nonce FROM ONLY $hot) != NONE; RETURN (SELECT VALUE canonical_read_nonce FROM ONLY $cold) = NONE; DELETE $cold;",
                crate::bindings! {
                    "hot" => crate::surreal::thing_from_record_id(id),
                    "cold" => crate::surreal::thing_from_record_id(&crate::work_retention::archive_id(id)),
                },
            )
            .await
            .expect("inspect genuine conflict write and remove negative archive");
        assert_eq!(
            response.take::<Option<bool>>(0).expect("live nonce"),
            Some(true)
        );
        assert_eq!(
            response.take::<Option<bool>>(1).expect("cold nonce"),
            Some(true)
        );
    }
    assert!(WorkItemRepository::new(client.clone())
        .get(SPACE, &root.run.base.id)
        .await
        .expect("wrong target table remains absent")
        .is_none());
    assert!(WorkRunRepository::new(client.clone())
        .get(SPACE, &root.work.base.id)
        .await
        .expect("wrong target table remains absent")
        .is_none());
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep client in the original fixture scope through assertions and cleanup."
)]
async fn native_observation_preserves_actual_archived_ancestor_authority() {
    let client = Box::pin(storage()).await;
    let mut chain = Box::pin(chain(&client)).await;
    Box::pin(ignored_archive_beside_live_original(&client, &chain)).await;
    exact_chain(&client, &chain).await;
    Box::pin(archive_original(&client, &mut chain[0])).await;
    exact_chain(&client, &chain).await;
    Box::pin(archive_original(&client, &mut chain[1])).await;
    exact_chain(&client, &chain).await;
    for original in &chain[..2] {
        assert_eq!(
            WorkItemRepository::new(client.clone())
                .get(SPACE, &original.work.base.id)
                .await
                .expect("actual archived work remains readable")
                .as_ref(),
            Some(&original.work)
        );
        assert_eq!(
            WorkRunRepository::new(client.clone())
                .get(SPACE, &original.run.base.id)
                .await
                .expect("actual archived run remains readable")
                .as_ref(),
            Some(&original.run)
        );
    }
    // Also exercise actual final same-transaction guards against the observed
    // cold ancestors; a read-only successful observation is insufficient.
    let _next = Box::pin(child(&client, &chain[2])).await;
    let target = crate::surreal::thing_from_record_id(&crate::work_retention::archive_id(
        &chain[1].run.base.id,
    ));
    client
        .query_bind_checked("DELETE $id;", crate::bindings! {"id" => target})
        .await
        .expect("negative missing actual original archive");
    refusal_without_child(&client, &chain[2]).await;
}

fn job() -> ReprocessJob {
    ReprocessJob {
        base: BaseFields::new(
            RecordId::new_v4(TableName::ReprocessJob),
            SPACE.into(),
            "original-owner".into(),
        ),
        name: "original job".into(),
        selection_criteria: SelectionCriteria::default(),
        target_stages: vec![ReprocessTargetStage::ReMaintain],
        status: ReprocessStatus::Pending,
        progress: ReprocessProgress::default(),
        priority: 1,
        budget_limit_euros: Some(2.0),
        max_sources_per_day: None,
        started_at: None,
        completed_at: None,
        error: None,
        reason: None,
    }
}

async fn job_claim(client: &StorageClient, job: &ReprocessJob) -> Claimed {
    let id = ReprocessJobRepository::new(client.clone())
        .ensure_work(job)
        .await
        .expect("actual durable handoff");
    let work = WorkItemRepository::new(client.clone())
        .get(SPACE, &id)
        .await
        .expect("read handoff")
        .expect("actual work");
    Box::pin(claim(client, &work)).await
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep client in the original fixture scope through assertions and cleanup."
)]
async fn native_observation_retains_independent_and_inherited_original_job_authority() {
    let client = Box::pin(storage()).await;
    let independent = Box::pin(ReprocessJobRepository::new(client.clone()).create_with_work(job()))
        .await
        .expect("actual independent job admission");
    let root = Box::pin(job_claim(&client, &independent)).await;
    assert!(matches!(
        &root.origin.origin,
        WorkProducerOrigin::ReprocessJob {
            execution_origin: ReprocessExecutionOrigin::Independent,
            ..
        }
    ));
    exact_observation(
        &client,
        &root,
        &[
            row(&root),
            CanonicalRecord::ReprocessJob(Box::new(independent.clone())),
        ],
        &[(root.work.clone(), root.origin.clone())],
    )
    .await;
    let fence = fence_from_claim(&root.work).expect("actual original parent fence");
    let parent = Box::pin(PreparedReprocessParent::capture(&client, &fence))
        .await
        .expect("actual sealed original execution");
    let inherited = job();
    let mut sql = "BEGIN TRANSACTION; CREATE $job_id CONTENT $job;\n".to_owned();
    let mut bindings = crate::bindings! {"job_id" => crate::surreal::thing_from_record_id(&inherited.base.id), "job" => ReprocessJobRepository::reprocess_job_to_value(&inherited).expect("typed job")};
    append_reprocess_intent_with_parent_sql(
        &mut sql,
        &mut bindings,
        "original_observation_parent",
        &inherited,
        Some(&parent),
    )
    .expect("existing atomic original-parent factory");
    sql.push_str("COMMIT TRANSACTION;");
    client
        .query_bind_checked(&sql, bindings)
        .await
        .expect("commit actual inherited intent and private origin");
    let inherited_work = Box::pin(job_claim(&client, &inherited)).await;
    assert!(matches!(
        &inherited_work.origin.origin,
        WorkProducerOrigin::ReprocessJob {
            execution_origin: ReprocessExecutionOrigin::Inherited { .. },
            ..
        }
    ));
    let expected = [
        row(&inherited_work),
        CanonicalRecord::ReprocessJob(Box::new(inherited.clone())),
        run(&root),
        row(&root),
        CanonicalRecord::ReprocessJob(Box::new(independent)),
    ];
    let expected_origins = [
        (inherited_work.work.clone(), inherited_work.origin.clone()),
        (root.work.clone(), root.origin.clone()),
    ];
    exact_observation(&client, &inherited_work, &expected, &expected_origins).await;
    replace_hint(
        &client,
        &inherited_work.origin,
        "parent_work_item_id",
        Value::None,
    )
    .await;
    replace_hint(
        &client,
        &inherited_work.origin,
        "parent_work_run_id",
        Value::None,
    )
    .await;
    refusal_without_child(&client, &inherited_work).await;
    for field in ["parent_work_item_id", "parent_work_run_id"] {
        replace_hint(
            &client,
            &inherited_work.origin,
            field,
            original_hint(&inherited_work.origin, field),
        )
        .await;
    }
    exact_observation(&client, &inherited_work, &expected, &expected_origins).await;
}
