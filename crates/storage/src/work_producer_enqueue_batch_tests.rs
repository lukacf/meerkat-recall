//! Independent native regressions for the retained original guard templates.
#![allow(clippy::expect_used, clippy::too_many_lines)]

use super::*;
use crate::{StorageClient, WorkItemRepository, WorkRunRepository};
use elephant_types::records::{BaseFields, WorkRunMetrics};
use elephant_types::Link;

const SPACE: &str = "batch-originals";

struct Original {
    work: WorkItem,
    run: WorkRun,
    origin: OriginEnvelope,
}

async fn native() -> StorageClient {
    let client = Box::pin(StorageClient::new_memory())
        .await
        .expect("native storage");
    for schema in [
        include_str!("../../migrations/sql/028_work_queue_indexes.surql"),
        include_str!("../../migrations/sql/029_work_history_archives.surql"),
        concat!(
            include_str!("../../migrations/sql/031_source_publication.surql"),
            "\n",
            include_str!("../../migrations/sql/046_source_support_index_fence.surql")
        ),
        include_str!("../../migrations/sql/032_work_recovery.surql"),
        include_str!("../../migrations/sql/041_work_producer_origin.surql"),
    ] {
        client.query_checked(schema).await.expect("original schema");
    }
    crate::reference_fence::install_native_reference_guards(&client, &[])
        .await
        .expect("real native mutation guards");
    client
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep items in the original fixture scope through assertions and cleanup."
)]
async fn original(client: &StorageClient) -> Original {
    let items = WorkItemRepository::new(client.clone());
    let work = items
        .create(WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                SPACE.into(),
                "original-actor".into(),
            ),
            kind: "triage_source".into(),
            payload: serde_json::json!({"original": "unchanged"}),
            status: WorkItemStatus::Runnable,
            priority: 3,
            idempotency_key: uuid::Uuid::new_v4().to_string(),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        })
        .await
        .expect("admit actual direct work and private origin");
    let work = items
        .claim_with_lease(
            SPACE,
            &work.base.id,
            "original-worker",
            chrono::Utc::now() + chrono::Duration::minutes(5),
        )
        .await
        .expect("claim")
        .expect("owned original attempt");
    let fence = fence_from_claim(&work).expect("actual claimed fence");
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
            worker_version: "original-worker-version".into(),
            model_version: None,
            prompt_id: None,
            config_hash: None,
        })
        .await
        .expect("persist real run for original claim");
    let origin = WorkProducerRepository::new(client.clone())
        .read_envelope(&work)
        .await
        .expect("read admitted origin")
        .expect("original private evidence");
    assert!(matches!(&origin.origin, WorkProducerOrigin::Direct));
    Original { work, run, origin }
}

fn rows(original: &Original) -> Vec<CanonicalRecord> {
    vec![
        CanonicalRecord::WorkItem(Box::new(original.work.clone())),
        CanonicalRecord::WorkRun(Box::new(original.run.clone())),
    ]
}

fn origins(original: &Original) -> Vec<(WorkItem, OriginEnvelope)> {
    vec![(original.work.clone(), original.origin.clone())]
}

fn sentinel() -> Thing {
    crate::source_publication::control_thing(
        "producer_batch_sentinel",
        &uuid::Uuid::new_v4().to_string(),
    )
}

async fn commit_guards(
    client: &StorageClient,
    rows: &[CanonicalRecord],
    origins: &[(WorkItem, OriginEnvelope)],
    sentinel: &Thing,
) -> StorageResult<()> {
    let mut sql = "BEGIN TRANSACTION;\n".to_owned();
    let mut bindings = crate::bindings! {"space" => SPACE, "sentinel" => sentinel.clone()};
    append_lineage_row_guards(&mut sql, &mut bindings, rows)?;
    append_origin_guards(&mut sql, &mut bindings, origins)?;
    sql.push_str(
        "CREATE $sentinel CONTENT {space_id:$space,marker:'after-all-original-guards'};\nCOMMIT TRANSACTION;",
    );
    client.query_bind_checked(&sql, bindings).await?;
    Ok(())
}

async fn exists(client: &StorageClient, id: &Thing) -> bool {
    let mut response = client
        .query_bind_checked(
            "RETURN array::len((SELECT VALUE id FROM $id));",
            crate::bindings! {"id" => id.clone()},
        )
        .await
        .expect("physical row count");
    let count: Option<u64> = response.take(0).expect("native count");
    count.expect("count query must return its actual value, including zero") != 0
}

async fn nonce(client: &StorageClient, id: Thing, field: &str) -> Option<String> {
    assert!(matches!(field, "canonical_read_nonce" | "read_nonce"));
    let mut response = client
        .query_bind_checked(
            &format!("SELECT VALUE {field} FROM ONLY $id;"),
            crate::bindings! {"id" => id},
        )
        .await
        .expect("read actual conflict nonce");
    response.take(0).expect("native optional nonce")
}

async fn nonces(client: &StorageClient, original: &Original, cold: bool) -> Vec<Option<String>> {
    let work = if cold {
        crate::work_retention::archive_id(&original.work.base.id)
    } else {
        original.work.base.id.clone()
    };
    let run = if cold {
        crate::work_retention::archive_id(&original.run.base.id)
    } else {
        original.run.base.id.clone()
    };
    vec![
        nonce(
            client,
            crate::surreal::thing_from_record_id(&work),
            "canonical_read_nonce",
        )
        .await,
        nonce(
            client,
            crate::surreal::thing_from_record_id(&run),
            "canonical_read_nonce",
        )
        .await,
        nonce(client, origin_id(&original.work.base.id), "read_nonce").await,
    ]
}

async fn erasure_revision(client: &StorageClient) -> u64 {
    let mut response = client
        .query_bind_checked(
            "SELECT VALUE revision FROM ONLY $guard;",
            crate::bindings! {"guard" => Thing::from(("source_erasure_guard", SPACE))},
        )
        .await
        .expect("read the actual shared erasure revision");
    response
        .take::<Option<u64>>(0)
        .expect("native revision type")
        .expect("original admission created the erasure guard")
}

async fn changed_field(client: &StorageClient, id: Thing, field: &str, value: Value) {
    assert!(matches!(
        field,
        "last_error"
            | "priority"
            | "completed_at"
            | "payload"
            | "space_id"
            | "deleted_at"
            | "record.last_error"
    ));
    client
        .query_bind_checked(
            &format!("UPDATE $id SET {field}=$value;"),
            crate::bindings! {"id" => id, "value" => value},
        )
        .await
        .expect("controlled negative mutation after original observation");
}

async fn assert_refusal(
    client: &StorageClient,
    original: &Original,
    rows: &[CanonicalRecord],
    origins: &[(WorkItem, OriginEnvelope)],
    cold: bool,
) {
    let before = nonces(client, original, cold).await;
    let revision_before = erasure_revision(client).await;
    let marker = sentinel();
    let result = commit_guards(client, rows, origins, &marker).await;
    let error = result
        .expect_err("changed original must refuse")
        .to_string();
    assert!(
        error.contains("publication:canonical_row_changed")
            || error.contains("work_producer_origin_changed_or_unknown"),
        "refusal must come from the original guard predicate: {error}"
    );
    assert!(
        !exists(client, &marker).await,
        "later mutation must not commit"
    );
    assert_eq!(
        nonces(client, original, cold).await,
        before,
        "earlier conflict writes must roll back"
    );
    assert_eq!(
        erasure_revision(client).await,
        revision_before,
        "earlier nonce events must roll back their shared erasure writes"
    );
}

async fn archive(client: &StorageClient, original: &mut Original) {
    let fence = fence_from_claim(&original.work).expect("original claim");
    original.work = WorkItemRepository::new(client.clone())
        .finish_authorized(
            &original.work,
            &fence.lease_token,
            WorkItemStatus::Done,
            None,
        )
        .await
        .expect("finish actual original attempt")
        .expect("same owner");
    original.run.success = true;
    original.run.completed_at = Some(chrono::Utc::now());
    original.run = WorkRunRepository::new(client.clone())
        .update(SPACE, &original.run.base.id, original.run.clone())
        .await
        .expect("complete actual original run");
    crate::work_retention::archive_terminal_work(
        client,
        chrono::Utc::now() + chrono::Duration::seconds(1),
        100,
    )
    .await
    .expect("actual retention path");
    assert!(
        !exists(
            client,
            &crate::surreal::thing_from_record_id(&original.work.base.id)
        )
        .await
    );
    assert!(
        !exists(
            client,
            &crate::surreal::thing_from_record_id(&original.run.base.id)
        )
        .await
    );
    assert!(
        exists(
            client,
            &crate::surreal::thing_from_record_id(&crate::work_retention::archive_id(
                &original.work.base.id
            ))
        )
        .await
    );
    assert!(
        exists(
            client,
            &crate::surreal::thing_from_record_id(&crate::work_retention::archive_id(
                &original.run.base.id
            ))
        )
        .await
    );
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep client, repository in the original fixture scope through assertions and cleanup."
)]
async fn native_grouped_original_guards_preserve_hot_cold_and_duplicate_observations() {
    let client = Box::pin(native()).await;
    let mut original = Box::pin(original(&client)).await;
    let repository = WorkProducerRepository::new(client.clone());
    for cold in [false, true] {
        if cold {
            Box::pin(archive(&client, &mut original)).await;
        }
        let (work, run, origin) = repository
            .lineage_parent(SPACE, &original.work.base.id, &original.run.base.id)
            .await
            .expect("actual batched original observation");
        assert_eq!(work, original.work);
        assert_eq!(run, original.run);
        assert_eq!(origin, original.origin);
        let mut repeated = rows(&original);
        repeated.extend(rows(&original));
        let mut repeated_origins = origins(&original);
        repeated_origins.extend(origins(&original));
        let marker = sentinel();
        let revision_before = erasure_revision(&client).await;
        commit_guards(&client, &repeated, &repeated_origins, &marker)
            .await
            .expect("same original rows and duplicate descriptors remain admissible");
        assert!(exists(&client, &marker).await);
        assert!(nonces(&client, &original, cold)
            .await
            .iter()
            .all(Option::is_some));
        assert_eq!(
            erasure_revision(&client).await,
            revision_before + 6,
            "each Work, Run and private origin nonce must run its event on both observations"
        );
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep client in the original fixture scope through assertions and cleanup."
)]
async fn native_grouped_none_and_native_types_refuse_work_and_run_changes_atomically() {
    let client = Box::pin(native()).await;
    let original = Box::pin(original(&client)).await;
    assert!(original.work.last_error.is_none());
    assert!(original.run.completed_at.is_none());
    commit_guards(&client, &rows(&original), &origins(&original), &sentinel())
        .await
        .expect("original positive control before negative mutations");
    changed_field(
        &client,
        crate::surreal::thing_from_record_id(&original.work.base.id),
        "last_error",
        "changed after observation".into(),
    )
    .await;
    let mut run_before_work = rows(&original);
    run_before_work.reverse();
    assert_refusal(
        &client,
        &original,
        &run_before_work,
        &origins(&original),
        false,
    )
    .await;
    changed_field(
        &client,
        crate::surreal::thing_from_record_id(&original.work.base.id),
        "last_error",
        Value::None,
    )
    .await;
    changed_field(
        &client,
        crate::surreal::thing_from_record_id(&original.work.base.id),
        "priority",
        original.work.priority.to_string().into(),
    )
    .await;
    assert_refusal(
        &client,
        &original,
        &run_before_work,
        &origins(&original),
        false,
    )
    .await;
    changed_field(
        &client,
        crate::surreal::thing_from_record_id(&original.work.base.id),
        "priority",
        i64::from(original.work.priority).into(),
    )
    .await;
    changed_field(
        &client,
        crate::surreal::thing_from_record_id(&original.run.base.id),
        "completed_at",
        crate::surreal::datetime_from_chrono(chrono::Utc::now()).into(),
    )
    .await;
    assert_refusal(
        &client,
        &original,
        &rows(&original),
        &origins(&original),
        false,
    )
    .await;
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep client in the original fixture scope through assertions and cleanup."
)]
async fn native_grouped_private_origin_refusal_rolls_back_prior_row_and_origin_nonces() {
    let client = Box::pin(native()).await;
    let original = Box::pin(original(&client)).await;
    let later = Box::pin(self::original(&client)).await;
    let mut controls = origins(&original);
    controls.extend(origins(&later));
    commit_guards(&client, &rows(&original), &controls, &sentinel())
        .await
        .expect("both original private controls admitted");
    for (field, changed, restored) in [
        (
            "payload",
            Value::from("changed original payload"),
            Value::from(later.origin.payload().expect("original payload")),
        ),
        ("space_id", Value::from("foreign-space"), Value::from(SPACE)),
    ] {
        changed_field(&client, origin_id(&later.work.base.id), field, changed).await;
        let later_before = nonces(&client, &later, false).await;
        assert_refusal(&client, &original, &rows(&original), &controls, false).await;
        assert_eq!(nonces(&client, &later, false).await, later_before);
        changed_field(&client, origin_id(&later.work.base.id), field, restored).await;
    }
    client
        .query_bind_checked(
            "DELETE $id;",
            crate::bindings! {"id" => origin_id(&later.work.base.id)},
        )
        .await
        .expect("remove later original evidence");
    assert_refusal(&client, &original, &rows(&original), &controls, false).await;
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep client in the original fixture scope through assertions and cleanup."
)]
async fn native_grouped_archive_envelope_and_nested_before_image_changes_refuse_atomically() {
    let client = Box::pin(native()).await;
    let mut original = Box::pin(original(&client)).await;
    Box::pin(archive(&client, &mut original)).await;
    commit_guards(&client, &rows(&original), &origins(&original), &sentinel())
        .await
        .expect("original cold records");
    let archive_id = crate::surreal::thing_from_record_id(&crate::work_retention::archive_id(
        &original.work.base.id,
    ));
    let mut run_before_work = rows(&original);
    run_before_work.reverse();
    for (field, changed, restored) in [
        ("space_id", Value::from("foreign-space"), Value::from(SPACE)),
        (
            "deleted_at",
            Value::from(crate::surreal::datetime_from_chrono(chrono::Utc::now())),
            Value::None,
        ),
        (
            "record.last_error",
            Value::from("changed archived original"),
            Value::None,
        ),
    ] {
        changed_field(&client, archive_id.clone(), field, changed).await;
        assert_refusal(
            &client,
            &original,
            &run_before_work,
            &origins(&original),
            true,
        )
        .await;
        changed_field(&client, archive_id.clone(), field, restored).await;
    }
    // A physically present invalid hot row must never be hidden by a valid
    // archive. These rows are negative inputs, not admitted producer evidence.
    let hot_id = crate::surreal::thing_from_record_id(&original.work.base.id);
    for (field, value) in [
        ("space_id", Value::from("foreign-space")),
        (
            "deleted_at",
            Value::from(crate::surreal::datetime_from_chrono(chrono::Utc::now())),
        ),
    ] {
        client.query_bind_checked(
            "CREATE $id CONTENT $record;",
            crate::bindings! {
                "id" => hot_id.clone(),
                "record" => CanonicalRecord::WorkItem(Box::new(original.work.clone())).native().expect("original typed row"),
            },
        ).await.expect("negative hot presence beside real archive");
        changed_field(&client, hot_id.clone(), field, value).await;
        assert_refusal(
            &client,
            &original,
            &run_before_work,
            &origins(&original),
            true,
        )
        .await;
        assert!(nonce(&client, hot_id.clone(), "canonical_read_nonce")
            .await
            .is_none());
        client
            .query_bind_checked("DELETE $id;", crate::bindings! {"id" => hot_id.clone()})
            .await
            .expect("remove negative hot row");
    }
    client
        .query_bind_checked("DELETE $id;", crate::bindings! {"id" => archive_id})
        .await
        .expect("remove original archive");
    assert_refusal(
        &client,
        &original,
        &run_before_work,
        &origins(&original),
        true,
    )
    .await;
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep client, repository in the original fixture scope through assertions and cleanup."
)]
async fn native_lineage_tuple_refuses_missing_foreign_or_invalid_original_evidence() {
    let client = Box::pin(native()).await;
    let original = Box::pin(original(&client)).await;
    let repository = WorkProducerRepository::new(client.clone());
    assert!(repository
        .lineage_parent(SPACE, &original.run.base.id, &original.run.base.id)
        .await
        .is_err());
    assert!(repository
        .lineage_parent(SPACE, &original.work.base.id, &original.work.base.id)
        .await
        .is_err());
    assert!(repository
        .lineage_parent(
            "foreign-space",
            &original.work.base.id,
            &original.run.base.id
        )
        .await
        .is_err());
    assert!(repository
        .lineage_parent(
            SPACE,
            &RecordId::new_v4(TableName::WorkItem),
            &original.run.base.id
        )
        .await
        .is_err());
    assert!(repository
        .lineage_parent(
            SPACE,
            &original.work.base.id,
            &RecordId::new_v4(TableName::WorkRun)
        )
        .await
        .is_err());
    changed_field(
        &client,
        origin_id(&original.work.base.id),
        "payload",
        "invalid captured evidence".into(),
    )
    .await;
    assert!(repository
        .lineage_parent(SPACE, &original.work.base.id, &original.run.base.id)
        .await
        .is_err());
    client
        .query_bind_checked(
            "DELETE $id;",
            crate::bindings! {"id" => origin_id(&original.work.base.id)},
        )
        .await
        .expect("remove original private control");
    assert!(repository
        .lineage_parent(SPACE, &original.work.base.id, &original.run.base.id)
        .await
        .is_err());
}

fn comparison_object(fields: impl IntoIterator<Item = (&'static str, Value)>) -> Value {
    Value::Object(surrealdb::sql::Object::from(
        fields
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value))
            .collect::<BTreeMap<_, _>>(),
    ))
}

async fn compare_with_original_field_loop(
    client: &StorageClient,
    label: &str,
    current: Value,
    expected: Value,
    matches: bool,
) {
    let mut guarded_current = current.clone();
    let guarded_fields = match &mut guarded_current {
        Value::Object(fields) => Some(fields),
        _ => None,
    }
    .expect("existing field oracle uses actual native objects");
    guarded_fields
        .entry("space_id".into())
        .or_insert_with(|| SPACE.into());
    let guarded_expected = expected.clone();
    let bindings = BTreeMap::from([
        ("current".to_owned(), current),
        ("before".to_owned(), expected),
    ]);
    let mut response = client
        .query_bind_checked(
            "RETURN recall::original_fields_match($current, $before);",
            bindings.clone(),
        )
        .await
        .expect("actual native comparison builtin");
    let actual: Option<bool> = response.take(0).expect("required native boolean");
    assert_eq!(actual, Some(matches), "builtin case {label}");

    // Retain the exact former SQL predicate as an independent executable oracle.
    // It also evaluates native dynamic-key access, rather than mirroring Rust.
    let original = client
        .query_bind_checked(
            "FOR $entry IN object::entries($before) { IF $current[$entry[0]] != $entry[1] { THROW 'original_field_loop_mismatch'; }; }; RETURN true;",
            bindings,
        )
        .await;
    if matches {
        let mut response = original.expect("original predicate must accept");
        let last = response.num_statements().checked_sub(1).expect("result");
        let original: Option<bool> = response.take(last).expect("original result");
        assert_eq!(original, Some(true), "old predicate case {label}");
    } else {
        let error = original.expect_err("original predicate must refuse");
        assert!(
            error.to_string().contains("original_field_loop_mismatch"),
            "old predicate case {label}: {error}"
        );
    }
    Box::pin(compare_with_original_row_predicate(
        client,
        label,
        guarded_current.clone(),
        guarded_expected.clone(),
        SPACE,
        matches,
    ))
    .await;
    Box::pin(compare_with_original_row_predicate(
        client,
        label,
        guarded_current.clone(),
        guarded_expected.clone(),
        "foreign-space",
        false,
    ))
    .await;
    if let Value::Object(fields) = &mut guarded_current {
        fields.remove("space_id");
    }
    Box::pin(compare_with_original_row_predicate(
        client,
        label,
        guarded_current,
        guarded_expected,
        SPACE,
        false,
    ))
    .await;
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep client in the original fixture scope through assertions and cleanup."
)]
async fn native_original_field_comparison_matches_retained_native_predicate() {
    let client = Box::pin(native()).await;
    let one = |value| comparison_object([("observed", value)]);
    let empty = || comparison_object([]);
    let uuid = uuid::Uuid::new_v4();
    let uuid_thing = Value::Thing(Thing::from((
        "work_item",
        surrealdb::sql::Id::Uuid(uuid.into()),
    )));
    let string_thing = Value::Thing(Thing::from((
        "work_item",
        surrealdb::sql::Id::String(uuid.to_string()),
    )));
    let now = Value::Datetime(surrealdb::sql::Datetime::from(chrono::Utc::now()));
    let bytes = Value::Bytes(vec![0_u8, 1, 255].into());
    let regex = Value::Regex("^(alpha|beta)$".parse().expect("native regex"));
    let nested = comparison_object([(
        "value",
        Value::Array(vec![Value::from(1_i64), Value::Null].into()),
    )]);
    let mut response = client
        .query_checked("RETURN <decimal>7;")
        .await
        .expect("native decimal input");
    let decimal: surrealdb::Value = response.take(0).expect("native decimal response");
    let decimal = decimal.into_inner();
    assert!(matches!(
        decimal,
        Value::Number(surrealdb::sql::Number::Decimal(_))
    ));
    let cases = vec![
        ("empty", empty(), empty(), true),
        (
            "literal dotted key",
            comparison_object([("a.b", 7_i64.into())]),
            comparison_object([("a.b", 7_i64.into())]),
            true,
        ),
        (
            "dotted key is not traversal",
            comparison_object([("a", comparison_object([("b", 7_i64.into())]))]),
            comparison_object([("a.b", 7_i64.into())]),
            false,
        ),
        (
            "decimal and integer",
            one(decimal.clone()),
            one(7_i64.into()),
            true,
        ),
        (
            "decimal and float",
            one(decimal.clone()),
            one(7_f64.into()),
            true,
        ),
        ("decimal and text", one(decimal), one("7".into()), false),
        (
            "native UUID",
            one(Value::Uuid(uuid.into())),
            one(Value::Uuid(uuid.into())),
            true,
        ),
        (
            "UUID and text",
            one(Value::Uuid(uuid.into())),
            one(uuid.to_string().into()),
            false,
        ),
        ("missing NONE", empty(), one(Value::None), true),
        ("explicit NONE", one(Value::None), one(Value::None), true),
        (
            "NULL is not NONE",
            one(Value::Null),
            one(Value::None),
            false,
        ),
        ("missing is not NULL", empty(), one(Value::Null), false),
        ("NULL", one(Value::Null), one(Value::Null), true),
        (
            "ignored extra",
            comparison_object([("observed", 7_i64.into()), ("extra", "retained".into())]),
            one(7_i64.into()),
            true,
        ),
        ("changed", one(8_i64.into()), one(7_i64.into()), false),
        (
            "integer versus string",
            one("7".into()),
            one(7_i64.into()),
            false,
        ),
        (
            "native numeric equality",
            one(7_f64.into()),
            one(7_i64.into()),
            true,
        ),
        (
            "native UUID Thing",
            one(uuid_thing.clone()),
            one(uuid_thing.clone()),
            true,
        ),
        (
            "Thing identity type",
            one(string_thing),
            one(uuid_thing),
            false,
        ),
        ("native datetime", one(now.clone()), one(now.clone()), true),
        (
            "datetime versus text",
            one(now.to_string().into()),
            one(now),
            false,
        ),
        ("native bytes", one(bytes.clone()), one(bytes.clone()), true),
        ("bytes versus text", one("0001ff".into()), one(bytes), false),
        (
            "nested object and array",
            one(nested.clone()),
            one(nested),
            true,
        ),
        (
            "nested missing differs from explicit NONE",
            one(empty()),
            one(comparison_object([("nested", Value::None)])),
            false,
        ),
        (
            "regex on current side",
            one(regex.clone()),
            one("alpha".into()),
            true,
        ),
        (
            "regex on expected side",
            one("beta".into()),
            one(regex.clone()),
            true,
        ),
        ("regex mismatch", one(regex), one("gamma".into()), false),
    ];
    for (label, current, expected, matches) in cases {
        Box::pin(compare_with_original_field_loop(
            &client, label, current, expected, matches,
        ))
        .await;
    }

    // Incorrect shape/arity fail with fixed diagnostics, never caller payloads.
    for sql in [
        "RETURN recall::original_fields_match('private-original-marker', {});",
        "RETURN recall::original_fields_match({}, 'private-original-marker');",
        "RETURN recall::original_fields_match({});",
        "RETURN recall::original_fields_match({}, {}, 'private-original-marker');",
    ] {
        let error = client
            .query_checked(sql)
            .await
            .expect_err("invalid argument");
        let message = error.to_string();
        assert!(message.contains("expected current and original field objects"));
        assert!(!message.contains("private-original-marker"));
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep client in the original fixture scope through assertions and cleanup."
)]
async fn native_original_field_comparison_checks_every_actual_work_and_run_field() {
    let client = Box::pin(native()).await;
    let original = Box::pin(original(&client)).await;
    for row in rows(&original) {
        let expected =
            crate::canonical_commit::native_expected_fields(row.native().expect("native original"));
        let mut response = client
            .query_bind_checked(
                "SELECT * FROM ONLY $id;",
                crate::bindings! {"id" => crate::surreal::thing_from_record_id(&row.base().id)},
            )
            .await
            .expect("actual persisted original row");
        let current: surrealdb::Value = response.take(0).expect("native beforeimage response");
        let current = current.into_inner();
        assert!(matches!(current, Value::Object(_)), "original row exists");
        compare_with_original_field_loop(
            &client,
            "real original row",
            current.clone(),
            expected.clone(),
            true,
        )
        .await;
        let expected_fields = match &expected {
            Value::Object(fields) => Some(fields),
            _ => None,
        }
        .expect("native row must be an object");
        for (key, value) in expected_fields.iter() {
            let mut changed = current.clone();
            let fields = match &mut changed {
                Value::Object(fields) => Some(fields),
                _ => None,
            }
            .expect("persisted original row must be an object");
            fields.insert(
                key.clone(),
                if value.is_none() {
                    Value::Null
                } else {
                    Value::None
                },
            );
            compare_with_original_field_loop(&client, key, changed, expected.clone(), false).await;
        }
    }
}

async fn compare_with_original_row_predicate(
    client: &StorageClient,
    label: &str,
    current: Value,
    expected: Value,
    space: &str,
    matches: bool,
) {
    let bindings = crate::bindings! {
        "current" => current, "before" => expected, "space" => space,
    };
    let mut response = client
        .query_bind_checked(
            "RETURN recall::original_row_matches($current, $space, $before);",
            bindings.clone(),
        )
        .await
        .expect("actual fused native predicate");
    let actual: Option<bool> = response.take(0).expect("required fused boolean");
    assert_eq!(actual, Some(matches), "fused case {label}");
    // Actual original presence/namespace IF and original native field loop.
    // This remains independent of both pure Rust builtin implementations.
    let original = client.query_bind_checked(
        "IF $current = NONE OR $current.space_id != $space { THROW 'original_row_predicate_mismatch'; }; FOR $entry IN object::entries($before) { IF $current[$entry[0]] != $entry[1] { THROW 'original_row_predicate_mismatch'; }; }; RETURN true;",
        bindings,
    ).await;
    if matches {
        let mut response = original.expect("original complete row predicate accepts");
        let last = response
            .num_statements()
            .checked_sub(1)
            .expect("actual result");
        let actual: Option<bool> = response.take(last).expect("original row result");
        assert_eq!(actual, Some(true), "original row case {label}");
    } else {
        let error = original.expect_err("original complete row predicate refuses");
        assert!(
            error
                .to_string()
                .contains("original_row_predicate_mismatch"),
            "original row case {label}: {error}"
        );
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep client in the original fixture scope through assertions and cleanup."
)]
async fn native_original_row_comparison_matches_presence_and_private_origin_guards() {
    let client = Box::pin(native()).await;
    let original = Box::pin(original(&client)).await;
    let payload = original
        .origin
        .payload()
        .expect("actual original private payload");
    let mut response = client
        .query_bind_checked(
            "SELECT * FROM ONLY $id;",
            crate::bindings! {"id" => origin_id(&original.work.base.id)},
        )
        .await
        .expect("actual persisted private origin");
    let raw: surrealdb::Value = response.take(0).expect("raw native origin response");
    let current = raw.into_inner();
    assert!(
        matches!(current, Value::Object(_)),
        "physical private origin exists"
    );
    let expected = comparison_object([("payload", payload.clone().into())]);
    let cases = vec![
        ("actual private origin", current, true),
        ("absent", Value::None, false),
        ("null", Value::Null, false),
        ("malformed scalar", "private-current-marker".into(), false),
        (
            "malformed array",
            Value::Array(vec![Value::from(7_i64)].into()),
            false,
        ),
        ("empty object", comparison_object([]), false),
        (
            "missing namespace",
            comparison_object([("payload", payload.clone().into())]),
            false,
        ),
        (
            "null namespace",
            comparison_object([
                ("space_id", Value::Null),
                ("payload", payload.clone().into()),
            ]),
            false,
        ),
        (
            "foreign namespace",
            comparison_object([
                ("space_id", "foreign-space".into()),
                ("payload", payload.clone().into()),
            ]),
            false,
        ),
        (
            "wrong namespace type",
            comparison_object([
                ("space_id", 7_i64.into()),
                ("payload", payload.clone().into()),
            ]),
            false,
        ),
        (
            "missing payload",
            comparison_object([("space_id", SPACE.into())]),
            false,
        ),
        (
            "null payload",
            comparison_object([("space_id", SPACE.into()), ("payload", Value::Null)]),
            false,
        ),
        (
            "changed payload",
            comparison_object([
                ("space_id", SPACE.into()),
                ("payload", "changed-original".into()),
            ]),
            false,
        ),
        (
            "unobserved extra",
            comparison_object([
                ("space_id", SPACE.into()),
                ("payload", payload.clone().into()),
                ("unobserved", 7_i64.into()),
            ]),
            true,
        ),
    ];
    for (label, current, matches) in cases {
        Box::pin(compare_with_original_row_predicate(
            &client,
            label,
            current.clone(),
            expected.clone(),
            SPACE,
            matches,
        ))
        .await;
        // Also execute the exact original private-origin IF, without a field
        // loop or synthetic expected row substituting for the physical origin.
        let old = client.query_bind_checked(
            "IF $current = NONE OR $current.space_id != $space OR $current.payload != $payload { THROW 'original_origin_predicate_mismatch'; }; RETURN true;",
            crate::bindings! {"current" => current, "space" => SPACE, "payload" => payload.clone()},
        ).await;
        if matches {
            let mut response = old.expect("original private predicate accepts");
            let last = response
                .num_statements()
                .checked_sub(1)
                .expect("actual result");
            assert_eq!(
                response
                    .take::<Option<bool>>(last)
                    .expect("original boolean"),
                Some(true),
                "private origin case {label}"
            );
        } else {
            let error = old.expect_err("original private predicate refuses");
            assert!(
                error
                    .to_string()
                    .contains("original_origin_predicate_mismatch"),
                "private origin case {label}: {error}"
            );
        }
    }
    // Invalid guard inputs fail closed even when current is absent. The fixed
    // error intentionally omits all supplied values; no default guard exists.
    for sql in [
        "RETURN recall::original_row_matches({}, NONE, {});",
        "RETURN recall::original_row_matches({}, 7, {});",
        "RETURN recall::original_row_matches({}, 'probe', 'private-original-marker');",
        "RETURN recall::original_row_matches(NONE, 'probe', 'private-original-marker');",
        "RETURN recall::original_row_matches({}, 'private-original-marker');",
        "RETURN recall::original_row_matches({}, 'probe', {}, 'private-original-marker');",
    ] {
        let error = client
            .query_checked(sql)
            .await
            .expect_err("invalid row guard arguments");
        let message = error.to_string();
        assert!(message.contains("expected current value, space string and original field object"));
        assert!(!message.contains("private-original-marker"));
    }
}
