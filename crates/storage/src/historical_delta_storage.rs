//! Full-image historical delta transactions. No provider, queue, or publication admission.
use crate::canonical_commit::{epoch_tx, CanonicalCommitReceipt};
use crate::client::PreparedHistoricalCatchupAuthority;
use crate::historical_adoption::catchup::{HistoricalControlImage, PreparedHistoricalEffects};
use crate::historical_delta::{runtime_row_fingerprint, HistoricalDeltaRow};
use crate::source_publication::{control_thing, control_value, stable_key, write_control_tx};
use crate::transfer_batch::TransferRecord;
use crate::{EmbeddedMutation, StorageError, StorageResult};
use std::fmt::Write as _;
use surrealdb::sql::Value;
#[path = "historical_delta_inventory.rs"]
pub mod inventory;

fn changed() -> StorageError {
    StorageError::Conflict("historical_delta:full_before_image_changed".into())
}

pub fn full_native(row: &TransferRecord) -> StorageResult<Value> {
    let Value::Object(mut fields) = row.native()? else {
        return Err(changed());
    };
    if let TransferRecord::Entity(entity) = row {
        crate::historical_delta_vectors::attach_entity_projection(entity, &mut fields)?;
    }
    fields.insert(
        "id".into(),
        crate::surreal::thing_from_record_id(row.id()).into(),
    );
    Ok(crate::surreal::stored_native_value(Value::Object(fields)))
}

fn check_row_tx(tx: &EmbeddedMutation<'_>, row: &HistoricalDeltaRow) -> StorageResult<()> {
    let actual = tx.raw_row_by_id(row.id.table(), &row.id);
    let expected = row.before.as_ref().map(TransferRecord::json).transpose()?;
    if actual != expected {
        return Err(changed());
    }
    Ok(())
}

fn write_row_tx<T: elephant_db::ElephantDbRecord>(
    tx: &mut EmbeddedMutation<'_>,
    record: &T,
    replace: bool,
) -> StorageResult<()> {
    if replace {
        tx.replace(record.clone())?;
    } else {
        tx.create(record.clone())?;
    }
    Ok(())
}

fn apply_row_tx(
    tx: &mut EmbeddedMutation<'_>,
    row: &HistoricalDeltaRow,
    space: &str,
) -> StorageResult<()> {
    if let Some(after) = &row.after {
        match after {
            TransferRecord::AuditLog(record) => write_row_tx(tx, record, row.before.is_some())?,
            TransferRecord::CostLedgerDaily(record) => {
                write_row_tx(tx, record, row.before.is_some())?;
            }
            TransferRecord::Doc(record) => write_row_tx(tx, record, row.before.is_some())?,
            TransferRecord::DocRevision(record) => write_row_tx(tx, record, row.before.is_some())?,
            TransferRecord::DocNode(record) => write_row_tx(tx, record, row.before.is_some())?,
            TransferRecord::MediaAsset(record) => write_row_tx(tx, record, row.before.is_some())?,
            TransferRecord::Entity(record) => write_row_tx(tx, record, row.before.is_some())?,
            TransferRecord::Source(record) => write_row_tx(tx, record, row.before.is_some())?,
            TransferRecord::EntityIdentityKey(record) => {
                write_row_tx(tx, record, row.before.is_some())?;
            }
            TransferRecord::IdentityCandidate(record) => {
                write_row_tx(tx, record, row.before.is_some())?;
            }
            TransferRecord::EntityMerge(record) => write_row_tx(tx, record, row.before.is_some())?,
            TransferRecord::PredicateRegistry(record) => {
                write_row_tx(tx, record, row.before.is_some())?;
            }
            TransferRecord::PredicateAlias(record) => {
                write_row_tx(tx, record, row.before.is_some())?;
            }
            TransferRecord::Assertion(record) => write_row_tx(tx, record, row.before.is_some())?,
            TransferRecord::Attribute(record) => write_row_tx(tx, record, row.before.is_some())?,
            TransferRecord::Rel(record) => write_row_tx(tx, record, row.before.is_some())?,
            TransferRecord::ConflictGroup(record) => {
                write_row_tx(tx, record, row.before.is_some())?;
            }
            TransferRecord::TruthSlot(record) => write_row_tx(tx, record, row.before.is_some())?,
            TransferRecord::Event(record) => write_row_tx(tx, record, row.before.is_some())?,
            TransferRecord::Freshness(record) => write_row_tx(tx, record, row.before.is_some())?,
            TransferRecord::ReprocessJob(record) => write_row_tx(tx, record, row.before.is_some())?,
            TransferRecord::WorkItem(record) => write_row_tx(tx, record, row.before.is_some())?,
            TransferRecord::WorkEdge(record) => write_row_tx(tx, record, row.before.is_some())?,
            TransferRecord::WorkRun(record) => write_row_tx(tx, record, row.before.is_some())?,
            TransferRecord::WorkArtifact(record) => write_row_tx(tx, record, row.before.is_some())?,
            TransferRecord::WorkDeadletter(record) => {
                write_row_tx(tx, record, row.before.is_some())?;
            }
            TransferRecord::SystemEvent(record) => write_row_tx(tx, record, row.before.is_some())?,
            TransferRecord::IngestDecision(record) => {
                write_row_tx(tx, record, row.before.is_some())?;
            }
            TransferRecord::WorkItemArchive(record) => {
                write_row_tx(tx, record, row.before.is_some())?;
            }
            TransferRecord::WorkRunArchive(record) => {
                write_row_tx(tx, record, row.before.is_some())?;
            }
        }
    } else {
        tx.hard_delete_raw_row(row.id.table(), space, &row.id)?;
    }
    let actual = tx.raw_row_by_id(row.id.table(), &row.id);
    let expected = row.after.as_ref().map(TransferRecord::json).transpose()?;
    if actual != expected {
        return Err(changed());
    }
    Ok(())
}

fn check_control_tx(
    tx: &EmbeddedMutation<'_>,
    table: &str,
    key: &str,
    expected: &HistoricalControlImage,
) -> StorageResult<()> {
    let HistoricalControlImage::Embedded(expected) = expected else {
        return Err(changed());
    };
    let actual = tx.extension_row_by_key(table, &format!("{table}:{key}"))?;
    if actual.as_ref() != Some(expected) {
        return Err(changed());
    }
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum BatchFault {
    None,
    #[cfg(test)]
    BeforeReceipt,
}

pub async fn commit_batch(
    authority: &PreparedHistoricalCatchupAuthority,
    effects: &PreparedHistoricalEffects,
    operational: &[crate::historical_delta_operations::OperationalEffect],
    receipt: &CanonicalCommitReceipt,
    after_epoch: &str,
) -> StorageResult<Option<crate::historical_native_effects::VerifiedHistoricalNativeEffects>> {
    commit_batch_inner(
        authority,
        effects,
        operational,
        receipt,
        after_epoch,
        BatchFault::None,
    )
    .await
}

fn append_native_preimage_guards(
    sql: &mut String,
    bindings: &mut std::collections::BTreeMap<String, Value>,
    effects: &PreparedHistoricalEffects,
    absent_work: &std::collections::BTreeSet<elephant_types::RecordId>,
) -> StorageResult<()> {
    for (index, id) in absent_work.iter().enumerate() {
        bindings.insert(
            format!("absent_work_{index}"),
            crate::surreal::thing_from_record_id(id).into(),
        );
        writeln!(sql,"IF (SELECT * FROM ONLY $absent_work_{index}) != NONE {{ THROW 'historical_work_counterpart_changed'; }};").map_err(|_| changed())?;
    }
    for (index, original) in effects.original_receipts.iter().enumerate() {
        let HistoricalControlImage::Native(expected) = &original.before else {
            return Err(changed());
        };
        bindings.insert(
            format!("original_{index}_id"),
            control_thing(original.table, &original.key).into(),
        );
        bindings.insert(format!("original_{index}"), expected.clone());
        writeln!(sql,"IF (SELECT * FROM ONLY $original_{index}_id) != $original_{index} {{ THROW 'historical_original_receipt_changed'; }};").map_err(|_|changed())?;
    }
    for (index, row) in effects.projected_rows.iter().enumerate() {
        bindings.insert(
            format!("row_{index}_id"),
            crate::surreal::thing_from_record_id(&row.id).into(),
        );
        bindings.insert(
            format!("row_{index}_before"),
            row.before
                .as_ref()
                .map(full_native)
                .transpose()?
                .unwrap_or(Value::None),
        );
        writeln!(sql,"IF (SELECT * FROM ONLY $row_{index}_id) != $row_{index}_before {{ THROW 'historical_delta_row_changed'; }};").map_err(|_|changed())?;
    }
    for (index, control) in effects.controls.iter().enumerate() {
        let HistoricalControlImage::Native(expected) = &control.before else {
            return Err(changed());
        };
        bindings.insert(
            format!("control_{index}_id"),
            control_thing(control.table, &control.key).into(),
        );
        bindings.insert(format!("control_{index}_before"), expected.clone());
        writeln!(sql,"IF (SELECT * FROM ONLY $control_{index}_id) != $control_{index}_before {{ THROW 'historical_delta_control_changed'; }};").map_err(|_|changed())?;
    }
    Ok(())
}

fn append_native_row_writes(
    sql: &mut String,
    bindings: &mut std::collections::BTreeMap<String, Value>,
    rows: &[HistoricalDeltaRow],
) -> StorageResult<()> {
    for (index, row) in rows.iter().enumerate() {
        if let Some(after) = &row.after {
            bindings.insert(format!("row_{index}_after"), full_native(after)?);
            if row.before.is_none() {
                if let TransferRecord::Rel(rel) = after {
                    bindings.insert(
                        format!("row_{index}_subject"),
                        crate::surreal::thing_from_record_id(rel.subject.id()).into(),
                    );
                    bindings.insert(
                        format!("row_{index}_object"),
                        crate::surreal::thing_from_record_id(rel.object.id()).into(),
                    );
                    writeln!(sql,"RELATE $row_{index}_subject->$row_{index}_id->$row_{index}_object CONTENT $row_{index}_after;").map_err(|_|changed())?;
                } else {
                    writeln!(sql, "CREATE $row_{index}_id CONTENT $row_{index}_after;")
                        .map_err(|_| changed())?;
                }
            } else {
                writeln!(sql, "UPDATE $row_{index}_id CONTENT $row_{index}_after;")
                    .map_err(|_| changed())?;
            }
        } else {
            writeln!(sql, "DELETE $row_{index}_id;").map_err(|_| changed())?;
        }
        bindings.insert(
            format!("row_{index}_expected"),
            row.after
                .as_ref()
                .map(full_native)
                .transpose()?
                .unwrap_or(Value::None),
        );
        writeln!(sql,"IF (SELECT * FROM ONLY $row_{index}_id) != $row_{index}_expected {{ THROW 'historical_delta_after_image_changed'; }};").map_err(|_|changed())?;
    }
    Ok(())
}

fn append_native_control_writes(
    sql: &mut String,
    bindings: &mut std::collections::BTreeMap<String, Value>,
    effects: &PreparedHistoricalEffects,
) -> StorageResult<()> {
    for (index, control) in effects.controls.iter().enumerate() {
        match &control.after {
            Some(HistoricalControlImage::Native(value)) => {
                bindings.insert(format!("control_{index}_after"), value.clone());
                writeln!(
                    sql,
                    "UPDATE $control_{index}_id CONTENT $control_{index}_after;"
                )
                .map_err(|_| changed())?;
            }
            None => {
                writeln!(sql, "DELETE $control_{index}_id;").map_err(|_| changed())?;
            }
            Some(HistoricalControlImage::Embedded(_)) => return Err(changed()),
        }
        bindings.insert(
            format!("control_{index}_expected"),
            match &control.after {
                Some(HistoricalControlImage::Native(value)) => {
                    crate::surreal::stored_native_value(value.clone())
                }
                None => Value::None,
                Some(HistoricalControlImage::Embedded(_)) => return Err(changed()),
            },
        );
        writeln!(sql,"IF (SELECT * FROM ONLY $control_{index}_id) != $control_{index}_expected {{ THROW 'historical_delta_control_after_changed'; }};").map_err(|_|changed())?;
    }
    Ok(())
}

fn validate_batch_effects(
    effects: &PreparedHistoricalEffects,
    receipt: &CanonicalCommitReceipt,
    space: &str,
) -> StorageResult<()> {
    // The source-owned constructor chooses projection and affected controls.
    if effects.batch_digest != receipt.plan_fingerprint || effects.expected_epoch.is_empty() {
        return Err(changed());
    }
    for row in &effects.projected_rows {
        for (image, hash) in [
            (&row.before, &row.before_fingerprint),
            (&row.after, &row.after_fingerprint),
        ] {
            match (image, hash) {
                (None, None) => (),
                (Some(image), Some(hash))
                    if image.id() == &row.id
                        && image.base().space_id == space
                        && runtime_row_fingerprint(image)? == *hash => {}
                _ => return Err(changed()),
            }
        }
    }
    if effects.original_receipts.iter().any(|original| {
        original.table != "source_commit_receipt"
            || original.after.as_ref() != Some(&original.before)
    }) {
        return Err(changed());
    }
    if effects.controls.iter().any(|control| {
        !matches!(
            control.table,
            "source_head" | "source_operation" | "source_publication_completion"
        )
    }) {
        return Err(changed());
    }
    Ok(())
}

pub async fn commit_batch_inner(
    authority: &PreparedHistoricalCatchupAuthority,
    effects: &PreparedHistoricalEffects,
    operational: &[crate::historical_delta_operations::OperationalEffect],
    receipt: &CanonicalCommitReceipt,
    after_epoch: &str,
    fault: BatchFault,
) -> StorageResult<Option<crate::historical_native_effects::VerifiedHistoricalNativeEffects>> {
    let _ = fault;
    commit_batch_with_client(
        authority,
        authority.client()?,
        effects,
        operational,
        receipt,
        after_epoch,
        fault,
    )
    .await
}

async fn commit_batch_with_client(
    authority: &PreparedHistoricalCatchupAuthority,
    client: &crate::StorageClient,
    effects: &PreparedHistoricalEffects,
    operational: &[crate::historical_delta_operations::OperationalEffect],
    receipt: &CanonicalCommitReceipt,
    after_epoch: &str,
    fault: BatchFault,
) -> StorageResult<Option<crate::historical_native_effects::VerifiedHistoricalNativeEffects>> {
    let _ = fault;
    let space = &receipt.space_id;
    let receipt_key = stable_key(&[space, &receipt.commit_id]);
    let receipt_source = receipt.source_ids.first().ok_or_else(changed)?;
    validate_batch_effects(effects, receipt, space)?;
    let absent_work =
        crate::historical_delta_operations::absent_work_counterparts(&effects.projected_rows)?;
    crate::historical_delta_vectors::prepare_partitions(authority, &effects.projected_embeddings)
        .await?;
    if let Some(store) = client.embedded_store() {
        return store.mutate(|tx| {
            authority.check_custody()?;
            if epoch_tx(tx,space)?.as_deref()!=Some(effects.expected_epoch.as_str()) { return Err(changed()); }
            if tx.extension_row_by_key("source_commit_receipt",&format!("source_commit_receipt:{receipt_key}"))?.is_some() { return Err(changed()); }
            for original in &effects.original_receipts { check_control_tx(tx,original.table,&original.key,&original.before)?; }
            for id in &absent_work { if tx.raw_row_by_id(id.table(),id).is_some() { return Err(changed()); } }
            for row in &effects.projected_rows { check_row_tx(tx,row)?; }
            crate::historical_delta_vectors::check_tx(tx,&effects.projected_embeddings,space)?;
            crate::historical_delta_operations::check_tx(tx,operational)?;
            for control in &effects.controls { check_control_tx(tx,control.table,&control.key,&control.before)?; }
            for row in &effects.projected_rows { apply_row_tx(tx,row,space)?; }
            crate::historical_delta_vectors::apply_tx(tx,&effects.projected_embeddings,space)?;
            crate::historical_delta_operations::apply_tx(tx,operational,space)?;
            for control in &effects.controls {
                let key=format!("{}:{}",control.table,control.key);
                match &control.after {
                    Some(HistoricalControlImage::Embedded(value)) => tx.replace_extension_row(control.table,&key,value.clone())?,
                    None => tx.hard_delete_extension_row(control.table,&key,space)?,
                    Some(HistoricalControlImage::Native(_)) => return Err(changed()),
                }
            }
            #[cfg(test)]
            if fault == BatchFault::BeforeReceipt { return Err(StorageError::QueryError("injected historical batch failure".into())); }
            tx.upsert_extension_row("canonical_epoch",serde_json::json!({"id":format!("canonical_epoch:{space}"),"space_id":space,"epoch":after_epoch}))?;
            write_control_tx(tx,"source_commit_receipt",&receipt_key,space,receipt_source,receipt)?;
            if epoch_tx(tx,space)?.as_deref()!=Some(after_epoch) { return Err(changed()); }
            authority.check_custody()?;
            Ok(())
        }).await.map(|()| None);
    }
    // Make the explicitly planned checkpoint guard deterministic across old
    // restored schemas; installation never changes checkpoint row payloads.
    if operational
        .iter()
        .any(|row| row.table == "outbox_checkpoint")
    {
        authority.check_custody()?;
        client
            .query_checked(&crate::reference_fence::native_reference_event(
                "outbox_checkpoint",
            ))
            .await?;
    }
    let mut sql=String::from("BEGIN TRANSACTION; IF (SELECT VALUE epoch FROM ONLY $epoch_id) != $expected_epoch OR (SELECT VALUE id FROM ONLY $receipt_id) != NONE { THROW 'historical_delta_fence_changed'; };\n");
    let mut bindings = crate::bindings! {
        "epoch_id"=>control_thing("canonical_epoch",space),"expected_epoch"=>effects.expected_epoch.clone(),
        "space"=>space.clone(),"after_epoch"=>after_epoch,"receipt_id"=>control_thing("source_commit_receipt",&receipt_key),
        "receipt"=>control_value(space,receipt_source,receipt)?,
    };
    append_native_preimage_guards(&mut sql, &mut bindings, effects, &absent_work)?;
    crate::historical_delta_vectors::append_guards(
        &mut sql,
        &mut bindings,
        &effects.projected_embeddings,
        space,
    )?;
    crate::historical_delta_operations::append_guards(&mut sql, &mut bindings, operational)?;
    let inventory = inventory::begin(
        authority,
        effects,
        operational,
        receipt,
        receipt_source,
        &mut sql,
        &mut bindings,
    )
    .await?;
    append_native_row_writes(&mut sql, &mut bindings, &effects.projected_rows)?;
    crate::historical_delta_vectors::append_writes(&mut sql, &effects.projected_embeddings)?;
    crate::historical_delta_operations::append_writes(&mut sql, &mut bindings, operational)?;
    append_native_control_writes(&mut sql, &mut bindings, effects)?;
    #[cfg(test)]
    if fault == BatchFault::BeforeReceipt {
        sql.push_str("THROW 'injected_historical_batch_failure';");
    }
    sql.push_str("UPSERT $epoch_id SET space_id=$space,epoch=$after_epoch; CREATE $receipt_id CONTENT $receipt; IF (SELECT VALUE epoch FROM ONLY $epoch_id) != $after_epoch { THROW 'historical_delta_receipt_epoch_changed'; };");
    inventory::finish(&mut sql, &inventory)?;
    sql.push_str("COMMIT TRANSACTION;");
    authority.check_custody()?;
    client.query_bind_checked(&sql, bindings).await?;
    let verified = crate::historical_native_effects::recover_verified(
        client,
        receipt_source,
        &inventory.binding,
        receipt,
        &inventory.identities,
    )
    .await?;
    Ok(Some(verified))
}
