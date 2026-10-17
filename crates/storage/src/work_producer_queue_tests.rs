//! Native producer lookups must remain bounded without inventing queue authority.

use super::{
    origin_id, schedulable_ids, OriginEnvelope, QueueOrigin, WorkProducerOrigin,
    QUEUE_ORIGINS_QUERY,
};
use crate::{StorageClient, StorageResult};
use elephant_types::records::{BaseFields, WorkItem, WorkItemStatus};
use elephant_types::{RecordId, TableName};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use surrealdb::sql::{Thing, Value};

const ORIGINAL_QUEUE_ORIGINS_QUERY: &str =
    "SELECT work_item_id, space_id, payload FROM work_producer_origin WHERE id IN $origins";

fn work(sequence: u128, space: &str) -> WorkItem {
    WorkItem {
        base: BaseFields::new(
            RecordId::new(TableName::WorkItem, uuid::Uuid::from_u128(sequence)),
            space.into(),
            "queue-origin-test".into(),
        ),
        kind: "extract_doc".into(),
        payload: serde_json::json!({}),
        status: WorkItemStatus::Runnable,
        priority: 1,
        idempotency_key: format!("queue-origin-{sequence}"),
        attempts: 0,
        lease_expires_at: None,
        leased_by: None,
        run_after: None,
        last_error: None,
    }
}

fn origin_bindings(work: &[WorkItem]) -> BTreeMap<String, Value> {
    let ids: Vec<Value> = work
        .iter()
        .map(|item| Value::Thing(origin_id(&item.base.id)))
        .collect();
    crate::bindings! { "origins" => Value::Array(ids.into()) }
}

async fn create_origin(
    client: &StorageClient,
    work: &WorkItem,
    origin: WorkProducerOrigin,
) -> StorageResult<OriginEnvelope> {
    let envelope = OriginEnvelope::new(work, origin, &[])?;
    client
        .query_bind_checked(
            "CREATE $id CONTENT $row",
            crate::bindings! { "id" => origin_id(&work.base.id), "row" => envelope.native()? },
        )
        .await?;
    Ok(envelope)
}

#[derive(Debug, Deserialize)]
struct PlanStep {
    operation: String,
    detail: PlanDetail,
}

#[derive(Debug, Deserialize)]
struct PlanDetail {
    thing: Option<Thing>,
}

async fn explain(
    client: &StorageClient,
    query: &str,
    work: &[WorkItem],
) -> StorageResult<Vec<PlanStep>> {
    let mut response = client
        .query_bind_checked(&format!("{query} EXPLAIN"), origin_bindings(work))
        .await?;
    Ok(response.take(0)?)
}

#[tokio::test]
async fn native_queue_origins_use_only_requested_record_iterators() -> StorageResult<()> {
    let client = StorageClient::new_memory().await?;
    let requested = [work(1, "first"), work(2, "second"), work(3, "missing")];
    create_origin(&client, &requested[0], WorkProducerOrigin::Direct).await?;
    create_origin(&client, &requested[1], WorkProducerOrigin::Unknown).await?;
    create_origin(&client, &work(4, "unrelated"), WorkProducerOrigin::Direct).await?;
    // The previous production query's real plan is the negative control.
    let original = explain(&client, ORIGINAL_QUEUE_ORIGINS_QUERY, &requested).await?;
    assert!(original
        .iter()
        .any(|step| step.operation == "Iterate Table"));
    let plan = explain(&client, QUEUE_ORIGINS_QUERY, &requested).await?;
    println!("producer origin original plan: {original:?}; production plan: {plan:?}");
    let iterators: Vec<_> = plan
        .iter()
        .filter(|step| step.operation.starts_with("Iterate "))
        .collect();
    assert_eq!(iterators.len(), requested.len());
    for (step, item) in iterators.into_iter().zip(&requested) {
        assert_eq!(step.operation, "Iterate Thing");
        assert_eq!(step.detail.thing, Some(origin_id(&item.base.id)));
    }
    client.close_owned_command_connection(30_000).await
}

async fn projected_origins(
    client: &StorageClient,
    query: &str,
    work: &[WorkItem],
) -> StorageResult<BTreeMap<RecordId, (String, String)>> {
    let mut response = client
        .query_bind_checked(query, origin_bindings(work))
        .await?;
    let rows: Vec<QueueOrigin> = response.take(0)?;
    rows.into_iter()
        .map(|row| {
            Ok((
                crate::surreal::record_id_from_thing(&row.work_item_id)?,
                (row.space_id, row.payload),
            ))
        })
        .collect()
}

#[tokio::test]
async fn native_queue_origin_batch_preserves_known_unknown_missing_and_space_identity(
) -> StorageResult<()> {
    let client = StorageClient::new_memory().await?;
    let requested = [
        work(1, "first"),
        work(2, "second"),
        work(3, "held"),
        work(4, "missing"),
    ];
    for item in &requested[..2] {
        create_origin(&client, item, WorkProducerOrigin::Direct).await?;
    }
    create_origin(&client, &requested[2], WorkProducerOrigin::Unknown).await?;
    create_origin(&client, &work(5, "unrelated"), WorkProducerOrigin::Direct).await?;
    let original = projected_origins(&client, ORIGINAL_QUEUE_ORIGINS_QUERY, &requested).await?;
    assert_eq!(original.len(), 3);
    assert_eq!(
        projected_origins(&client, QUEUE_ORIGINS_QUERY, &requested).await?,
        original
    );
    assert_eq!(
        schedulable_ids(&client, &requested).await?,
        BTreeSet::from([requested[0].base.id.clone(), requested[1].base.id.clone()])
    );
    assert!(schedulable_ids(&client, &[]).await?.is_empty());
    assert!(schedulable_ids(&client, &requested[3..]).await?.is_empty());
    client.close_owned_command_connection(30_000).await
}

#[tokio::test]
async fn native_queue_origin_batch_refuses_corrupted_identity_and_payload() -> StorageResult<()> {
    let client = StorageClient::new_memory().await?;
    let requested = work(1, "first");
    let envelope = create_origin(&client, &requested, WorkProducerOrigin::Direct).await?;
    let mut wrong_version = envelope.clone();
    wrong_version.version = 2;
    let mut wrong_space = envelope.clone();
    wrong_space.space_id = "wrong-space".into();
    let mut wrong_id = envelope.clone();
    wrong_id.work_item_id = work(2, "first").base.id;
    for payload in [
        "not-json".into(),
        wrong_version.payload()?,
        wrong_space.payload()?,
        wrong_id.payload()?,
    ] {
        client
            .query_bind_checked(
                "UPDATE $id SET payload = $payload",
                crate::bindings! { "id" => origin_id(&requested.base.id), "payload" => payload },
            )
            .await?;
        assert!(schedulable_ids(&client, std::slice::from_ref(&requested))
            .await
            .is_err());
    }
    client
        .query_bind_checked(
            "UPDATE $id SET payload = $payload, space_id = 'wrong-wrapper-space'",
            crate::bindings! { "id" => origin_id(&requested.base.id), "payload" => envelope.payload()? },
        )
        .await?;
    assert!(schedulable_ids(&client, std::slice::from_ref(&requested))
        .await
        .is_err());
    client
        .query_bind_checked(
            "UPDATE $id SET space_id = $space, work_item_id = $other",
            crate::bindings! {
                "id" => origin_id(&requested.base.id), "space" => requested.base.space_id.clone(),
                "other" => crate::surreal::thing_from_record_id(&work(2, "first").base.id),
            },
        )
        .await?;
    assert!(schedulable_ids(&client, std::slice::from_ref(&requested))
        .await
        .is_err());
    client.close_owned_command_connection(30_000).await
}
