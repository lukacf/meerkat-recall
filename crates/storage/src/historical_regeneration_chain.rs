//! Recovery of Source-owned atomic regeneration receipts. This validates chain
//! custody only; Source must additionally validate every step against its entire
//! immutable ordered plan and the associated adoption or retirement receipt.
use super::{
    archive_catchup, journal, CanonicalCommitReceipt, CatchupMarker, ChainPosition, Deserialize,
    Digest, Sha256, StorageClient, StorageError, StorageResult,
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Progress {
    version: u32,
    authority_binding_sha256: String,
    original_last_batch_receipt_sha256: String,
    s1_plan_sha256: String,
    groups: u64,
    ordinal: u64,
    previous_regeneration_receipt_sha256: Option<String>,
    group_id: elephant_types::RecordId,
    adoption_commit_id: String,
    before_epoch: String,
    after_epoch: String,
}

fn commit_id(marker: &CatchupMarker, ordinal: u64) -> String {
    crate::source_publication::stable_key(&[
        "historical-catchup-regeneration-v1",
        &marker.authority_binding_sha256,
        &ordinal.to_string(),
    ])
}

/// A zero-batch regeneration has no batch receipt. Bind it to the original
/// authority/epoch in its own domain rather than inventing a receipt or using
/// an empty string. Nonempty batch chains retain their actual last receipt.
pub(super) fn anchor(
    marker: &CatchupMarker,
    original_epoch: &str,
    last_receipt: Option<&str>,
) -> StorageResult<String> {
    if marker.batches == 0 {
        if last_receipt.is_some() || original_epoch.is_empty() {
            return Err(StorageError::Conflict(
                "zero-batch regeneration anchor differs".into(),
            ));
        }
        let bytes = serde_json::to_vec(&(
            "historical-catchup-zero-batch-anchor-v1",
            &marker.authority_binding_sha256,
            &marker.journal_identity,
            &marker.before_snapshot_sha256,
            &marker.after_snapshot_sha256,
            &marker.regeneration_plan_sha256,
            original_epoch,
        ))
        .map_err(crate::durability_io::io_error)?;
        Ok(hex::encode(Sha256::digest(bytes)))
    } else {
        last_receipt
            .filter(|value| journal::valid_digest(value))
            .map(str::to_owned)
            .ok_or_else(|| StorageError::Conflict("regeneration batch anchor absent".into()))
    }
}

fn extend(
    marker: &CatchupMarker,
    chain: &mut ChainPosition,
    batch_anchor: &str,
    ordinal: u64,
    previous: Option<&str>,
    receipt: &CanonicalCommitReceipt,
) -> StorageResult<String> {
    let state: Progress = serde_json::from_value(
        receipt
            .replay_output
            .clone()
            .ok_or_else(|| StorageError::Conflict("regeneration receipt state absent".into()))?,
    )
    .map_err(crate::durability_io::io_error)?;
    if chain.next_batch != marker.batches
        || state.original_last_batch_receipt_sha256 != batch_anchor
        || ordinal >= marker.regeneration_steps
        || state.version != 1
        || state.authority_binding_sha256 != marker.authority_binding_sha256
        || state.s1_plan_sha256 != marker.regeneration_plan_sha256
        || state.groups != marker.regeneration_steps
        || state.ordinal != ordinal
        || state.previous_regeneration_receipt_sha256.as_deref() != previous
        || state.before_epoch != chain.epoch
        || state.after_epoch == state.before_epoch
        || uuid::Uuid::parse_str(&state.after_epoch).is_err()
        || state.group_id.table() != elephant_types::TableName::Source
        || state.adoption_commit_id.is_empty()
        || state.adoption_commit_id.len() > 256
        || state.adoption_commit_id.chars().any(char::is_control)
        || receipt.space_id != marker.space_id
        || receipt.commit_id != commit_id(marker, ordinal)
        || receipt.input_fingerprint != marker.authority_binding_sha256
        || receipt.plan_fingerprint != marker.regeneration_plan_sha256
        || receipt.historical_import.is_some()
        || receipt.publication_authority.is_some()
        || !receipt.maintenance_authority.is_empty()
        || receipt.source_ids != [state.group_id.clone()]
        || receipt.source_id.as_ref() != Some(&state.group_id)
        || receipt.source_revision.is_some()
        || !receipt.effect_ids.is_empty()
        || !receipt.extension_ids.is_empty()
        || !receipt.outbox_ids.is_empty()
    {
        return Err(StorageError::Conflict(
            "regeneration receipt chain differs".into(),
        ));
    }
    let bytes = serde_json::to_vec(receipt).map_err(crate::durability_io::io_error)?;
    if bytes.len() > 16 * 1024 {
        return Err(StorageError::Conflict(
            "regeneration receipt exceeds bound".into(),
        ));
    }
    chain.epoch = state.after_epoch;
    Ok(hex::encode(Sha256::digest(bytes)))
}

async fn read_native_payloads(
    client: &StorageClient,
    space: &str,
    keys: &[String],
    count: usize,
) -> StorageResult<std::collections::BTreeMap<String, String>> {
    use surrealdb::sql::Value;
    let mut payloads = std::collections::BTreeMap::new();
    let records = keys
        .iter()
        .map(|key| {
            Value::Thing(crate::source_publication::control_thing(
                "source_commit_receipt",
                key,
            ))
        })
        .collect::<Vec<_>>();
    let mut response = client
        .query_bind_checked(
            "SELECT id,space_id,payload FROM $ids",
            crate::bindings! {"ids"=>Value::Array(records.into())},
        )
        .await?;
    let values: surrealdb::Value = response.take(0).map_err(crate::durability_io::io_error)?;
    let Value::Array(values) = values.into_inner() else {
        return Err(StorageError::Conflict("regeneration page shape".into()));
    };
    if values.len() > count {
        return Err(StorageError::Conflict("regeneration page count".into()));
    }
    for row in values.0 {
        let Value::Object(mut row) = row else {
            return Err(StorageError::Conflict("regeneration row shape".into()));
        };
        let Some(Value::Thing(id)) = row.remove("id") else {
            return Err(StorageError::Conflict("regeneration row identity".into()));
        };
        let Some(Value::Strand(actual_space)) = row.remove("space_id") else {
            return Err(StorageError::Conflict("regeneration row scope".into()));
        };
        let Some(Value::Strand(payload)) = row.remove("payload") else {
            return Err(StorageError::Conflict("regeneration row payload".into()));
        };
        if actual_space.0 != space
            || !row.is_empty()
            || payloads.insert(id.to_string(), payload.0).is_some()
        {
            return Err(StorageError::Conflict(
                "regeneration row scope or duplicate".into(),
            ));
        }
    }
    Ok(payloads)
}

/// One native bounded point-read round trip; exact physical keys prevent a
/// foreign-space row from masquerading as an absent receipt.
async fn read_page(
    client: &StorageClient,
    space: &str,
    ids: &[String],
) -> StorageResult<Vec<Option<CanonicalCommitReceipt>>> {
    if ids.len() > 256 {
        return Err(StorageError::Conflict("regeneration page bound".into()));
    }
    let keys = ids
        .iter()
        .map(|id| crate::source_publication::stable_key(&[space, id]))
        .collect::<Vec<_>>();
    let mut payloads = std::collections::BTreeMap::new();
    if let Some(store) = client.embedded_store() {
        for key in &keys {
            let id = format!("source_commit_receipt:{key}");
            if let Some(row) = store
                .extension_row_by_key("source_commit_receipt", &id)
                .await?
            {
                if row.get("id").and_then(serde_json::Value::as_str) != Some(id.as_str())
                    || row.get("space_id").and_then(serde_json::Value::as_str) != Some(space)
                {
                    return Err(StorageError::Conflict(
                        "regeneration physical receipt scope changed".into(),
                    ));
                }
                let payload = row
                    .get("payload")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| {
                        StorageError::Conflict("regeneration receipt payload absent".into())
                    })?;
                payloads.insert(id, payload.to_owned());
            }
        }
    } else {
        payloads = read_native_payloads(client, space, &keys, ids.len()).await?;
    }
    let mut result = Vec::with_capacity(ids.len());
    for (key, expected_id) in keys.iter().zip(ids) {
        let full =
            crate::source_publication::control_thing("source_commit_receipt", key).to_string();
        let receipt = payloads
            .remove(&full)
            .map(|payload| {
                if payload.len() > 16 * 1024 {
                    return Err(StorageError::Conflict("regeneration payload bound".into()));
                }
                let receipt: CanonicalCommitReceipt =
                    serde_json::from_str(&payload).map_err(crate::durability_io::io_error)?;
                if receipt.space_id != space || receipt.commit_id != *expected_id {
                    return Err(StorageError::Conflict(
                        "regeneration payload identity".into(),
                    ));
                }
                Ok(receipt)
            })
            .transpose()?;
        result.push(receipt);
    }
    if !payloads.is_empty() {
        return Err(StorageError::Conflict(
            "regeneration unexpected physical receipt".into(),
        ));
    }
    Ok(result)
}

pub(super) async fn recover(
    client: &StorageClient,
    marker: &CatchupMarker,
    chain: &mut ChainPosition,
) -> StorageResult<()> {
    // Partial row-batch recovery may legitimately have no completed chain yet.
    // A regeneration receipt in that state still refuses below.
    let batch_anchor = if chain.next_batch == marker.batches {
        Some(archive_catchup::regeneration_anchor(
            marker,
            &chain.epoch,
            chain.last_receipt.as_deref(),
            chain.archive_receipt.as_ref(),
        )?)
    } else {
        None
    };
    let mut previous = None;
    let mut gap = false;
    let mut total_bytes = 0_usize;
    // The complete count and plan digest are bound before maintenance begins.
    // Read all declared slots, including after a gap; absence at 0 is not a
    // license to ignore a later committed regeneration transaction.
    for start in (0..=marker.regeneration_steps).step_by(256) {
        let end = start.saturating_add(256).min(marker.regeneration_steps + 1);
        let ids = (start..end)
            .map(|ordinal| commit_id(marker, ordinal))
            .collect::<Vec<_>>();
        for (offset, receipt) in read_page(client, &marker.space_id, &ids)
            .await?
            .into_iter()
            .enumerate()
        {
            let ordinal = start + offset as u64;
            match receipt {
                None => gap = true,
                Some(receipt) if !gap && ordinal < marker.regeneration_steps => {
                    let bound = batch_anchor.as_deref().ok_or_else(|| {
                        StorageError::Conflict("regeneration precedes complete batch chain".into())
                    })?;
                    let hash =
                        extend(marker, chain, bound, ordinal, previous.as_deref(), &receipt)?;
                    total_bytes = total_bytes
                        .checked_add(
                            serde_json::to_vec(&receipt)
                                .map_err(crate::durability_io::io_error)?
                                .len(),
                        )
                        .ok_or_else(|| {
                            StorageError::Conflict("regeneration receipt bound".into())
                        })?;
                    if total_bytes > 64 * 1024 * 1024 {
                        return Err(StorageError::Conflict(
                            "regeneration receipt byte bound".into(),
                        ));
                    }
                    chain.recovered_regeneration.push(receipt);
                    previous = Some(hash);
                }
                Some(_) => {
                    return Err(StorageError::Conflict(
                        "regeneration receipt gap or excess".into(),
                    ))
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
    use super::*;

    fn marker() -> CatchupMarker {
        CatchupMarker {
            version: 1,
            journal_identity: "journal".into(),
            migration_id: "a".repeat(64),
            source_space_id: "default".into(),
            space_id: "household".into(),
            before_snapshot_sha256: "b".repeat(64),
            after_snapshot_sha256: "c".repeat(64),
            delta_sha256: "d".repeat(64),
            original_report_sha256: "a".repeat(64),
            baseline_ledger_sha256: "b".repeat(64),
            baseline_native_rows_sha256: "c".repeat(64),
            accept_deletions: true,
            authority_binding_sha256: "e".repeat(64),
            batches: 3,
            regeneration_plan_sha256: "f".repeat(64),
            regeneration_steps: 2,
        }
    }
    fn chain() -> ChainPosition {
        ChainPosition {
            next_batch: 3,
            last_receipt: Some("1".repeat(64)),
            epoch: "batch-epoch".into(),
            recovered_regeneration: Vec::new(),
            archive_receipt: None,
        }
    }
    fn receipt(marker: &CatchupMarker) -> CanonicalCommitReceipt {
        let group = elephant_types::RecordId::new_v4(elephant_types::TableName::Source);
        CanonicalCommitReceipt {
            historical_import: None,
            space_id: marker.space_id.clone(),
            commit_id: commit_id(marker, 0),
            input_fingerprint: marker.authority_binding_sha256.clone(),
            plan_fingerprint: marker.regeneration_plan_sha256.clone(),
            replay_output: Some(
                serde_json::json!({"version":1,"authority_binding_sha256":marker.authority_binding_sha256,
                "original_last_batch_receipt_sha256":"1".repeat(64),"s1_plan_sha256":marker.regeneration_plan_sha256,
                "groups":2,"ordinal":0,"previous_regeneration_receipt_sha256":null,"group_id":group,
                "adoption_commit_id":format!("historical-adoption:{}", uuid::Uuid::new_v4()),"before_epoch":"batch-epoch","after_epoch":uuid::Uuid::new_v4().to_string()}),
            ),
            source_ids: vec![group.clone()],
            publication_authority: None,
            maintenance_authority: Vec::new(),
            source_id: Some(group),
            source_revision: None,
            effect_ids: Vec::new(),
            extension_ids: Vec::new(),
            outbox_ids: Vec::new(),
            committed_at: chrono::Utc::now(),
        }
    }
    #[test]
    fn regeneration_chain_binds_complete_plan_anchor_and_wrapper() {
        let marker = marker();
        let receipt = receipt(&marker);
        let mut current = chain();
        let hash = extend(&marker, &mut current, &"1".repeat(64), 0, None, &receipt)
            .expect("valid first step");
        assert!(journal::valid_digest(&hash));
        assert_ne!(current.epoch, "batch-epoch");
        for (field, value) in [
            ("groups", serde_json::json!(1)),
            ("ordinal", serde_json::json!(1)),
            ("s1_plan_sha256", serde_json::json!("0".repeat(64))),
            (
                "original_last_batch_receipt_sha256",
                serde_json::json!("0".repeat(64)),
            ),
            (
                "previous_regeneration_receipt_sha256",
                serde_json::json!("0".repeat(64)),
            ),
            ("before_epoch", serde_json::json!("foreign-epoch")),
            ("after_epoch", serde_json::json!("not-a-nonce")),
        ] {
            let mut altered = receipt.clone();
            altered.replay_output.as_mut().expect("state")[field] = value;
            let mut current = chain();
            assert!(
                extend(&marker, &mut current, &"1".repeat(64), 0, None, &altered).is_err(),
                "accepted changed {field}"
            );
            assert_eq!(current.epoch, "batch-epoch");
        }
        let mut altered = receipt.clone();
        altered.source_ids.clear();
        assert!(extend(&marker, &mut chain(), &"1".repeat(64), 0, None, &altered).is_err());
        altered = receipt;
        altered.outbox_ids.push(elephant_types::RecordId::new_v4(
            elephant_types::TableName::SystemEvent,
        ));
        assert!(extend(&marker, &mut chain(), &"1".repeat(64), 0, None, &altered).is_err());
        let mut incomplete = chain();
        incomplete.next_batch = 2;
        assert!(extend(&marker, &mut incomplete, &"1".repeat(64), 0, None, &altered).is_err());
    }
    #[test]
    fn zero_batch_anchor_binds_original_epoch_authority_and_no_fabricated_receipt() {
        let mut marker = marker();
        marker.batches = 0;
        let original_epoch = "original-zero-row-epoch";
        let bound = anchor(&marker, original_epoch, None).expect("zero-batch anchor");
        assert!(journal::valid_digest(&bound));
        assert_ne!(
            bound,
            anchor(&marker, "other-epoch", None).expect("changed epoch")
        );
        let mut foreign = marker.clone();
        foreign.authority_binding_sha256 = "9".repeat(64);
        assert_ne!(
            bound,
            anchor(&foreign, original_epoch, None).expect("changed authority")
        );
        assert!(anchor(&marker, original_epoch, Some(&"1".repeat(64))).is_err());
        let mut step = receipt(&marker);
        let state = step.replay_output.as_mut().expect("state");
        state["before_epoch"] = original_epoch.into();
        state["original_last_batch_receipt_sha256"] = bound.clone().into();
        let mut current = ChainPosition {
            next_batch: 0,
            last_receipt: None,
            epoch: original_epoch.into(),
            recovered_regeneration: Vec::new(),
            archive_receipt: None,
        };
        let mut wrong = ChainPosition {
            next_batch: 0,
            last_receipt: None,
            epoch: original_epoch.into(),
            recovered_regeneration: Vec::new(),
            archive_receipt: None,
        };
        assert!(extend(&marker, &mut wrong, &"1".repeat(64), 0, None, &step).is_err());
        extend(&marker, &mut current, &bound, 0, None, &step).expect("zero-row source receipt");
        assert!(
            current.last_receipt.is_none(),
            "no batch receipt is synthesized"
        );
        assert_eq!(current.next_batch, 0);
        assert_ne!(current.epoch, original_epoch);
    }
}
