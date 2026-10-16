//! Exact operational controls for S1 work holds and historical event baselines.
use crate::historical_adoption::catchup::HistoricalControlImage;
use crate::historical_delta::{runtime_row_fingerprint, HistoricalDeltaRow, PreparedDeltaBatch};
use crate::transfer_batch::TransferRecord;
use crate::{EmbeddedMutation, StorageClient, StorageError, StorageResult};
use elephant_types::{records::WorkItem, RecordId, TableName};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use surrealdb::sql::Value;

#[derive(Debug)]
pub struct OperationalEffect {
    pub table: &'static str,
    pub key: String,
    pub before: Option<HistoricalControlImage>,
    pub after: Option<HistoricalControlImage>,
}
/// Full operational before/after images retained by the atomic batch receipt.
/// Native query text preserves native IDs, NONE and timestamp semantics; these
/// are evidence, never executable SQL or a recovery permission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "representation", content = "value", rename_all = "snake_case")]
pub enum RetainedOperationalImage {
    Embedded(serde_json::Value),
    Native(String),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationalReceiptEffect {
    table: String,
    key: String,
    before: Option<RetainedOperationalImage>,
    after: Option<RetainedOperationalImage>,
}
/// Derive control identities from original typed rows; receipt contents cannot
/// introduce a work producer or event checkpoint outside this exact set.
pub fn planned_identities(batch: &PreparedDeltaBatch) -> Vec<(String, surrealdb::sql::Thing)> {
    let mut ids = Vec::new();
    let mut events = false;
    for row in &batch.input.rows {
        for record in row.before.iter().chain(row.after.iter()) {
            if let Some(item) = work(record) {
                ids.push(crate::source_publication::control_thing(
                    "work_producer_origin",
                    &item.base.id.id().to_string(),
                ));
            }
            events |= matches!(record, TransferRecord::SystemEvent(_));
        }
    }
    if events {
        for consumer in [
            "knowledge_views",
            "truth_maintenance",
            crate::event_sequence::RESERVED_CONSUMER,
        ] {
            ids.push(crate::source_publication::control_thing(
                "outbox_checkpoint",
                &crate::outbox_checkpoint_repository::key(&batch.input.space_id, consumer),
            ));
        }
    }
    ids.sort();
    ids.dedup_by(|a, b| surrealdb::sql::Thing::cmp(a, b).is_eq());
    ids.into_iter().map(|id| (id.tb.clone(), id)).collect()
}
pub fn verify_receipt_identities(
    batch: &PreparedDeltaBatch,
    rows: &[OperationalReceiptEffect],
) -> StorageResult<()> {
    let expected = planned_identities(batch)
        .into_iter()
        .map(|(_, id)| id)
        .collect::<Vec<_>>();
    let mut actual = rows
        .iter()
        .map(|row| crate::source_publication::control_thing(&row.table, &row.key))
        .collect::<Vec<_>>();
    actual.sort();
    actual.dedup_by(|a, b| surrealdb::sql::Thing::cmp(a, b).is_eq());
    if actual != expected || actual.len() != rows.len() {
        return Err(refused());
    }
    Ok(())
}

pub fn receipt_effects(controls: &[OperationalEffect]) -> Vec<OperationalReceiptEffect> {
    fn retain(image: &HistoricalControlImage) -> RetainedOperationalImage {
        match image {
            HistoricalControlImage::Embedded(value) => {
                RetainedOperationalImage::Embedded(value.clone())
            }
            HistoricalControlImage::Native(value) => {
                RetainedOperationalImage::Native(value.to_string())
            }
        }
    }
    controls
        .iter()
        .map(|control| OperationalReceiptEffect {
            table: control.table.into(),
            key: control.key.clone(),
            before: control.before.as_ref().map(retain),
            after: control.after.as_ref().map(retain),
        })
        .collect()
}

/// Guard the unmentioned half of every hot/archive identity as absent. A move
/// supplies both rows in one batch and their normal full-image guards apply.
pub fn absent_work_counterparts(rows: &[HistoricalDeltaRow]) -> StorageResult<BTreeSet<RecordId>> {
    let mentioned: BTreeSet<_> = rows.iter().map(|row| row.id.clone()).collect();
    let mut absent = BTreeSet::new();
    for row in rows {
        for record in row.before.iter().chain(row.after.iter()) {
            let counterpart = match record {
                TransferRecord::WorkItem(item) => crate::work_retention::archive_id(&item.base.id),
                TransferRecord::WorkItemArchive(archive) => {
                    if archive.original_id != archive.record.base.id
                        || archive.original_id.table() != TableName::WorkItem
                        || archive.base.id
                            != crate::work_retention::archive_id(&archive.original_id)
                        || archive.base.space_id != archive.record.base.space_id
                    {
                        return Err(refused());
                    }
                    archive.original_id.clone()
                }
                _ => continue,
            };
            if !mentioned.contains(&counterpart) {
                absent.insert(counterpart);
            }
        }
    }
    Ok(absent)
}

fn refused() -> StorageError {
    StorageError::Conflict("historical_delta:operational_before_image_or_custody_changed".into())
}

pub async fn full_optional(
    client: &StorageClient,
    table: &'static str,
    key: &str,
    space: &str,
) -> StorageResult<Option<HistoricalControlImage>> {
    if let Some(store) = client.embedded_store() {
        // Retain the exact addressed row even if its scope was changed. It must
        // fail the subsequent identity check, not masquerade as absence.
        return store
            .extension_row_by_key(table, &format!("{table}:{key}"))
            .await
            .map(|row| row.map(HistoricalControlImage::Embedded));
    }
    let mut result=client.query_bind_checked("SELECT * FROM ONLY $id;",crate::bindings!{"id"=>crate::source_publication::control_thing(table,key),"space"=>space}).await?;
    let row: surrealdb::Value = result.take(0).map_err(|_| refused())?;
    let row = row.into_inner();
    Ok(if matches!(row, Value::None | Value::Null) {
        None
    } else {
        Some(HistoricalControlImage::Native(row))
    })
}
fn field(image: &HistoricalControlImage, key: &str) -> StorageResult<serde_json::Value> {
    match image {
        HistoricalControlImage::Embedded(value) => {
            Ok(value.get(key).cloned().unwrap_or(serde_json::Value::Null))
        }
        HistoricalControlImage::Native(Value::Object(value)) => match value.get(key) {
            None | Some(Value::None) => Ok(serde_json::Value::Null),
            Some(Value::Strand(s)) => Ok(s.as_str().into()),
            Some(value) => surrealdb::sql::from_value(value.clone()).map_err(|_| refused()),
        },
        HistoricalControlImage::Native(_) => Err(refused()),
    }
}
fn replace_fields(
    image: &HistoricalControlImage,
    updates: impl IntoIterator<Item = (&'static str, serde_json::Value)>,
) -> StorageResult<HistoricalControlImage> {
    let mut image = image.clone();
    for (key, value) in updates {
        match &mut image {
            HistoricalControlImage::Embedded(row) => {
                row[key] = value;
            }
            HistoricalControlImage::Native(Value::Object(row)) => {
                row.insert(key.into(), crate::surreal::json_to_surreal(value)?);
            }
            HistoricalControlImage::Native(_) => return Err(refused()),
        }
    }
    Ok(image)
}
const fn work(row: &TransferRecord) -> Option<&WorkItem> {
    match row {
        TransferRecord::WorkItem(work) => Some(work),
        TransferRecord::WorkItemArchive(archive) => Some(&archive.record),
        _ => None,
    }
}

/// Original rows bind S1 proof hashes before namespace projection. Both hot/cold
/// sides of a move must be in the same batch; no intermediate runnable history.
pub async fn prepare(
    authority: &crate::client::PreparedHistoricalCatchupAuthority,
    batch: &PreparedDeltaBatch,
    projected: &[HistoricalDeltaRow],
) -> StorageResult<Vec<OperationalEffect>> {
    authority.bind_batch(batch)?;
    if projected.len() != batch.input.rows.len() {
        return Err(refused());
    }
    let controls =
        prepare_controls(authority, authority.client()?, &batch.input, projected).await?;
    authority.check_custody()?;
    Ok(controls)
}

async fn prepare_controls(
    authority: &crate::client::PreparedHistoricalCatchupAuthority,
    client: &StorageClient,
    input: &crate::historical_delta::HistoricalDeltaBatch,
    projected: &[HistoricalDeltaRow],
) -> StorageResult<Vec<OperationalEffect>> {
    let groups = collect_work_groups(input, projected)?;
    let mut controls = Vec::new();
    prepare_work_controls(authority, client, input, groups, &mut controls).await?;
    prepare_event_controls(authority, client, input, projected, &mut controls).await?;
    Ok(controls)
}

type WorkTransition = (Option<WorkItem>, Option<WorkItem>, Option<String>);
fn collect_work_groups(
    input: &crate::historical_delta::HistoricalDeltaBatch,
    projected: &[HistoricalDeltaRow],
) -> StorageResult<BTreeMap<RecordId, WorkTransition>> {
    let mut groups = BTreeMap::<RecordId, WorkTransition>::new();
    for (raw, row) in input.rows.iter().zip(projected) {
        if raw.id != row.id {
            return Err(refused());
        }
        for (before, image) in [(true, &row.before), (false, &row.after)] {
            if let Some(item) = image.as_ref().and_then(work) {
                let entry = groups.entry(item.base.id.clone()).or_default();
                let slot = if before { &mut entry.0 } else { &mut entry.1 };
                if slot.replace(item.clone()).is_some() {
                    return Err(refused());
                }
                if !before {
                    entry.2 = Some(runtime_row_fingerprint(
                        raw.after.as_ref().ok_or_else(refused)?,
                    )?);
                }
            }
        }
    }
    Ok(groups)
}

async fn prepare_work_controls(
    authority: &crate::client::PreparedHistoricalCatchupAuthority,
    client: &StorageClient,
    input: &crate::historical_delta::HistoricalDeltaBatch,
    groups: BTreeMap<RecordId, WorkTransition>,
    controls: &mut Vec<OperationalEffect>,
) -> StorageResult<()> {
    for (id, (before, after, after_hash)) in groups {
        let key = id.id().to_string();
        let prior = full_optional(client, "work_producer_origin", &key, &input.space_id).await?;
        if let Some(prior) = &prior {
            if field(prior, "space_id")?.as_str() != Some(input.space_id.as_str()) {
                return Err(refused());
            }
        }
        let updated = prepare_work_envelope(
            authority,
            input,
            &key,
            prior.as_ref(),
            (before.as_ref(), after.as_ref(), after_hash.as_deref()),
        )
        .await?;
        let after = updated_work_image(client, input, &id, &key, prior.as_ref(), updated)?;
        controls.push(OperationalEffect {
            table: "work_producer_origin",
            key,
            before: prior,
            after,
        });
    }
    Ok(())
}
async fn prepare_work_envelope(
    authority: &crate::client::PreparedHistoricalCatchupAuthority,
    input: &crate::historical_delta::HistoricalDeltaBatch,
    key: &str,
    prior: Option<&HistoricalControlImage>,
    transition: (Option<&WorkItem>, Option<&WorkItem>, Option<&str>),
) -> StorageResult<Option<(String, Vec<RecordId>)>> {
    let (before, after, after_hash) = transition;
    let payload = prior.map(|v| field(v, "payload")).transpose()?;
    let inherited = match (prior, payload.as_ref().and_then(|v| v.as_str())) {
        (Some(prior), Some(payload)) => {
            let envelope: serde_json::Value =
                serde_json::from_str(payload).map_err(|_| refused())?;
            let snapshot = envelope["historical_transfer"]["snapshot_sha256"]
                .as_str()
                .ok_or_else(refused)?;
            if snapshot == input.before_snapshot_sha256 {
                None
            } else {
                Some(
                    authority
                        .verify_inherited_operational_control("work_producer_origin", key, prior)
                        .await?,
                )
            }
        }
        _ => None,
    };
    crate::work_producer_repository::historical_catchup_envelope_with_lineage(
        before,
        after,
        payload.as_ref().and_then(|v| v.as_str()),
        (&input.source_space_id, &input.space_id),
        (&input.before_snapshot_sha256, &input.after_snapshot_sha256),
        after_hash,
        inherited.as_ref(),
    )
}
fn updated_work_image(
    client: &StorageClient,
    input: &crate::historical_delta::HistoricalDeltaBatch,
    id: &RecordId,
    key: &str,
    prior: Option<&HistoricalControlImage>,
    updated: Option<(String, Vec<RecordId>)>,
) -> StorageResult<Option<HistoricalControlImage>> {
    Ok(if let Some((payload, sources)) = updated {
        if let Some(prior) = prior {
            Some(match prior {
                HistoricalControlImage::Embedded(_) => replace_fields(
                    prior,
                    [
                        ("payload", payload.into()),
                        (
                            "source_ids",
                            serde_json::to_value(&sources).map_err(|_| refused())?,
                        ),
                    ],
                )?,
                HistoricalControlImage::Native(Value::Object(fields)) => {
                    let mut fields = fields.clone();
                    fields.insert("payload".into(), payload.into());
                    fields.insert(
                        "source_ids".into(),
                        Value::Array(crate::surreal::things_from_record_ids(
                            sources.iter().cloned(),
                        )),
                    );
                    HistoricalControlImage::Native(Value::Object(fields))
                }
                HistoricalControlImage::Native(_) => return Err(refused()),
            })
        } else {
            let embedded = serde_json::json!({"id":format!("work_producer_origin:{key}"),"space_id":input.space_id,"work_item_id":id,"source_ids":sources,"job_id":null,"parent_work_item_id":null,"parent_work_run_id":null,"payload":payload,"created_at":chrono::Utc::now()});
            Some(if client.embedded_store().is_some() {
                HistoricalControlImage::Embedded(embedded)
            } else {
                let mut fields = BTreeMap::<String, Value>::new();
                fields.insert(
                    "id".into(),
                    crate::source_publication::control_thing("work_producer_origin", key).into(),
                );
                fields.insert("space_id".into(), input.space_id.clone().into());
                fields.insert(
                    "work_item_id".into(),
                    crate::surreal::thing_from_record_id(id).into(),
                );
                fields.insert(
                    "source_ids".into(),
                    Value::Array(crate::surreal::things_from_record_ids(sources)),
                );
                fields.insert("payload".into(), payload.into());
                fields.insert(
                    "created_at".into(),
                    crate::surreal::datetime_from_chrono(chrono::Utc::now()).into(),
                );
                HistoricalControlImage::Native(Value::Object(surrealdb::sql::Object::from(fields)))
            })
        }
    } else {
        None
    })
}
async fn prepare_event_controls(
    authority: &crate::client::PreparedHistoricalCatchupAuthority,
    client: &StorageClient,
    input: &crate::historical_delta::HistoricalDeltaBatch,
    projected: &[HistoricalDeltaRow],
    controls: &mut Vec<OperationalEffect>,
) -> StorageResult<()> {
    let mut new_tail = None;
    let mut touches_events = false;
    for row in projected {
        for record in row.before.iter().chain(row.after.iter()) {
            if let TransferRecord::SystemEvent(event) = record {
                if event.seq < 0 {
                    return Err(refused());
                }
                touches_events = true;
                // Retain monotonic floor on deletion and sequence lowering too.
                new_tail = Some(new_tail.map_or(event.seq, |value: i64| value.max(event.seq)));
            }
        }
    }
    if touches_events {
        let tail = new_tail.ok_or_else(refused)?;
        let mut consumers = BTreeSet::from(["knowledge_views", "truth_maintenance"]);
        consumers.insert(crate::event_sequence::RESERVED_CONSUMER);
        let floor_key = crate::outbox_checkpoint_repository::key(
            &input.space_id,
            crate::event_sequence::RESERVED_CONSUMER,
        );
        let floor = full_optional(client, "outbox_checkpoint", &floor_key, &input.space_id)
            .await?
            .ok_or_else(refused)?;
        let old_floor = field(&floor, "seq")?.as_i64().ok_or_else(refused)?;
        for consumer in consumers {
            let key = crate::outbox_checkpoint_repository::key(&input.space_id, consumer);
            let before = full_optional(client, "outbox_checkpoint", &key, &input.space_id)
                .await?
                .ok_or_else(refused)?;
            if field(&before, "space_id")?.as_str() != Some(input.space_id.as_str())
                || field(&before, "consumer")?.as_str() != Some(consumer)
            {
                return Err(refused());
            }
            let seq = field(&before, "seq")?
                .as_i64()
                .filter(|v| *v >= 0)
                .ok_or_else(refused)?;
            let mut updates = vec![("seq", seq.max(tail).into())];
            if consumer != crate::event_sequence::RESERVED_CONSUMER {
                let mut baseline: crate::outbox_checkpoint_repository::HistoricalEventBaseline =
                    serde_json::from_value(field(&before, "historical_baseline")?)
                        .map_err(|_| refused())?;
                baseline.validate(client.erasure_journal()?.identity(), seq)?;
                if baseline.snapshot_sha256 != input.before_snapshot_sha256
                    && baseline.snapshot_sha256 != input.after_snapshot_sha256
                {
                    // Preserve the old snapshot as provenance until this actual
                    // event transition. Only the exact completed predecessor row
                    // can bridge an untouched earlier cut.
                    authority
                        .verify_inherited_operational_control("outbox_checkpoint", &key, &before)
                        .await?;
                }
                // Advancing across undelivered target-owned events would forge
                // acknowledgments. Exact initial historical baseline is required.
                if tail > seq && (seq != baseline.through_seq || old_floor > seq) {
                    return Err(refused());
                }
                baseline.snapshot_sha256 = input.after_snapshot_sha256.clone();
                baseline.through_seq = baseline.through_seq.max(tail);
                updates.push((
                    "historical_baseline",
                    serde_json::to_value(baseline).map_err(|_| refused())?,
                ));
            }
            let after = replace_fields(&before, updates)?;
            controls.push(OperationalEffect {
                table: "outbox_checkpoint",
                key,
                before: Some(before),
                after: Some(after),
            });
        }
    }
    Ok(())
}

pub fn check_tx(tx: &EmbeddedMutation<'_>, controls: &[OperationalEffect]) -> StorageResult<()> {
    for control in controls {
        let actual = tx
            .extension_row_by_key(control.table, &format!("{}:{}", control.table, control.key))?
            .map(HistoricalControlImage::Embedded);
        if actual != control.before {
            return Err(refused());
        }
    }
    Ok(())
}
pub fn apply_tx(
    tx: &mut EmbeddedMutation<'_>,
    controls: &[OperationalEffect],
    space: &str,
) -> StorageResult<()> {
    for control in controls {
        let key = format!("{}:{}", control.table, control.key);
        match &control.after {
            Some(HistoricalControlImage::Embedded(row)) => {
                if control.before.is_some() {
                    tx.replace_extension_row(control.table, &key, row.clone())?;
                } else {
                    tx.upsert_extension_row(control.table, row.clone())?;
                }
            }
            None => tx.hard_delete_extension_row(control.table, &key, space)?,
            _ => return Err(refused()),
        }
        let actual = tx
            .extension_row_by_key(control.table, &key)?
            .map(HistoricalControlImage::Embedded);
        if actual != control.after {
            return Err(refused());
        }
    }
    Ok(())
}
pub fn append_guards(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    controls: &[OperationalEffect],
) -> StorageResult<()> {
    for (i, control) in controls.iter().enumerate() {
        bindings.insert(
            format!("op_{i}_id"),
            crate::source_publication::control_thing(control.table, &control.key).into(),
        );
        let before = match &control.before {
            None => Value::None,
            Some(HistoricalControlImage::Native(value)) => value.clone(),
            _ => return Err(refused()),
        };
        bindings.insert(format!("op_{i}_before"), before);
        writeln!(sql,"IF (SELECT * FROM ONLY $op_{i}_id) != $op_{i}_before {{ THROW 'historical_operational_control_changed'; }};").map_err(|_|refused())?;
    }
    Ok(())
}
pub fn append_writes(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    controls: &[OperationalEffect],
) -> StorageResult<()> {
    for (i, control) in controls.iter().enumerate() {
        let after = match &control.after {
            None => Value::None,
            Some(HistoricalControlImage::Native(value)) => {
                crate::surreal::stored_native_value(value.clone())
            }
            _ => return Err(refused()),
        };
        bindings.insert(format!("op_{i}_after"), after);
        if control.after.is_none() {
            writeln!(sql, "DELETE $op_{i}_id;").map_err(|_| refused())?;
        } else {
            let verb = if control.before.is_some() {
                "UPDATE"
            } else {
                "CREATE"
            };
            writeln!(sql, "{verb} $op_{i}_id CONTENT $op_{i}_after;").map_err(|_| refused())?;
        }
        writeln!(sql,"IF (SELECT * FROM ONLY $op_{i}_id) != $op_{i}_after {{ THROW 'historical_operational_after_changed'; }};").map_err(|_|refused())?;
    }
    Ok(())
}
