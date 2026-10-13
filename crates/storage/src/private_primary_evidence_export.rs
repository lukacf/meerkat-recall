//! Read-only adapters for the three fixed private evidence selections.
use super::*;
use crate::canonical_commit::{
    source_support_fingerprint, CanonicalRecord, CanonicalRepository, SourceEffectSupport,
};
use crate::provider_usage_repository::ProviderCallReceipt;
use crate::source_decision::SourceDecisionReceipt;
use crate::source_publication::{encode_payload, stable_key, SourcePublicationRepository};
use crate::source_publication_manifest::{
    SourcePublicationCompletion, SourcePublicationManifestRepository,
};
use crate::{Repository, SourceRepository};
use elephant_types::records::reprocess::ReprocessStatus;
use elephant_types::records::Source;
use elephant_types::{
    publication::{OperationState, SourceHead, SourceVisibility},
    TableName,
};
use std::str::FromStr;

const CANONICAL_TABLES: [TableName; 13] = [
    TableName::Assertion,
    TableName::Rel,
    TableName::Attribute,
    TableName::Event,
    TableName::Entity,
    TableName::EntityIdentityKey,
    TableName::EntityMerge,
    TableName::IdMap,
    TableName::IdentityCandidate,
    TableName::TruthSlot,
    TableName::ConflictGroup,
    TableName::PredicateRegistry,
    TableName::PredicateAlias,
];

pub(super) fn is_canonical_table(table: &str) -> bool {
    CANONICAL_TABLES.iter().any(|name| name.as_str() == table)
        || matches!(
            table,
            "source_effect_support" | "source_decision_receipt" | "source_commit_receipt"
        )
}

fn value<T: Serialize>(row: &T) -> Result<Value> {
    serde_json::to_value(row).map_err(|_| refusal("row_serialization_failed"))
}
fn fingerprint<T: Serialize>(row: &T) -> Result<Value> {
    let raw = encode(row)?;
    Ok(json!({"sha256":digest(&raw),"bytes":raw.len(),"encoding":"canonical_json_utf8"}))
}
fn row_value(row: &CanonicalRecord) -> Result<Value> {
    value(row)?
        .get("record")
        .cloned()
        .ok_or_else(|| refusal("canonical_row_invalid"))
}
async fn epoch(client: &StorageClient, space: &str) -> Result<String> {
    CanonicalRepository::new(client.clone())
        .current_epoch(space)
        .await
        .map_err(storage_error)
}
async fn current_source(
    client: &StorageClient,
    space: &str,
    selected: &PublicationSelection,
) -> Result<(Source, SourceHead)> {
    let fence = &selected.publication;
    if fence.source_id.table() != TableName::Source
        || selected.doc_id.table() != TableName::Doc
        || selected.doc_revision_id.table() != TableName::DocRevision
    {
        return Err(refusal("publication_record_type_invalid"));
    }
    let repository = SourcePublicationRepository::new(client.clone());
    let source = SourceRepository::new(client.clone())
        .get(space, &fence.source_id)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| refusal("current_source_missing"))?;
    let head = repository
        .head(space, &fence.source_id)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| refusal("current_source_head_missing"))?;
    if source.base.space_id != space
        || source.base.id != fence.source_id
        || source.base.deleted_at.is_some()
        || source.external_id.as_deref() != Some(&selected.source_key)
        || head.space_id != space
        || head.source_id != fence.source_id
        || head.generation != fence.generation
        || head.operation_id != fence.operation_id
        || head.lifecycle != SourceVisibility::Active
        || head.published_revision.as_deref() != Some(&fence.revision_id)
        || head.desired_revision.as_deref() != Some(&fence.revision_id)
    {
        return Err(refusal("source_not_current"));
    }
    drop(repository);
    Ok((source, head))
}
async fn heads(
    client: &StorageClient,
    space: &str,
    selected: &[PublicationSelection],
) -> Result<Value> {
    let mut result = Vec::new();
    for selection in selected {
        let (source, head) = current_source(client, space, selection).await?;
        result.push(json!({"source_key":selection.source_key,"head":head,"source_fingerprint":fingerprint(&source)?}));
    }
    // Reject unexpected physical heads, including an erased extra incarnation.
    // A scoped inventory with an undisclosed source cannot prove completeness.
    let repository = SourcePublicationRepository::new(client.clone());
    let mut after = None;
    let mut actual = BTreeSet::new();
    loop {
        let page = repository
            .list_heads_page(space, after.as_deref(), 100)
            .await
            .map_err(storage_error)?;
        for head in page.heads {
            if actual.len() >= selected.len() || !actual.insert(head.source_id) {
                return Err(refusal("unexpected_source_inventory"));
            }
        }
        after = page.next_key;
        if after.is_none() {
            break;
        }
    }
    drop(repository);
    if actual
        != selected
            .iter()
            .map(|row| row.publication.source_id.clone())
            .collect()
    {
        return Err(refusal("source_inventory_incomplete"));
    }
    Ok(Value::Array(result))
}

#[allow(
    clippy::too_many_lines,
    reason = "One bounded evidence capture binds original publication rows and verifies the same source and epoch after all reads."
)]
pub(super) async fn publication(
    client: &StorageClient,
    space: &str,
    selected: &PublicationSelection,
    expected: &SelectedRevision,
) -> Result<Packet> {
    let before = epoch(client, space).await?;
    let (source, head_before) = current_source(client, space, selected).await?;
    let publication = SourcePublicationRepository::new(client.clone());
    let manifest_repository = SourcePublicationManifestRepository::new(client.clone());
    let operation = publication
        .operation(space, &selected.publication.operation_id)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| refusal("original_operation_missing"))?;
    if operation.state != OperationState::Completed
        || operation.space_id != space
        || operation.source_id != selected.publication.source_id
        || operation.source_generation != selected.publication.generation
        || operation.operation_id != selected.publication.operation_id
        || operation.desired_revision.as_deref() != Some(&selected.publication.revision_id)
        || operation.published_revision.as_deref() != Some(&selected.publication.revision_id)
    {
        return Err(refusal("original_publication_not_complete"));
    }
    let revision = publication
        .revision(
            space,
            &selected.publication.source_id,
            &selected.publication.revision_id,
        )
        .await
        .map_err(storage_error)?
        .ok_or_else(|| refusal("original_revision_missing"))?;
    drop(publication);
    let template = revision
        .source
        .manual_doc_template()
        .map_err(|_| refusal("original_input_invalid"))?
        .ok_or_else(|| refusal("original_input_missing"))?;
    if revision.space_id != space
        || revision.source_id != selected.publication.source_id
        || revision.revision_id != selected.publication.revision_id
        || revision.source.base.space_id != space
        || revision.source.base.id != selected.publication.source_id
        || revision.source.base.deleted_at.is_some()
        || revision.content_hash != revision.source.hash
        || digest(template.text.as_bytes()) != expected.input_sha256
    {
        return Err(refusal("original_input_identity_mismatch"));
    }
    let manifest = manifest_repository
        .get(space, &selected.publication)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| refusal("original_manifest_missing"))?;
    let completion = manifest_repository
        .completion(space, &selected.publication)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| refusal("original_completion_missing"))?;
    drop(manifest_repository);
    verify_selected_render_membership(&completion, selected)?;
    if manifest.space_id != space
        || manifest.publication != selected.publication
        || completion.space_id != space
        || completion.publication != selected.publication
        || completion.manifest_digest
            != stable_key(&[&encode_payload(&manifest).map_err(storage_error)?])
        || completion.accepted_source_fingerprint.as_deref()
            != Some(&source_support_fingerprint(&revision).map_err(storage_error)?)
        || !manifest.documents.iter().any(|row| {
            row.space_id == space
                && row.publication == selected.publication
                && row.doc_id == selected.doc_id
                && row.doc_revision_id == selected.doc_revision_id
        })
    {
        return Err(refusal("original_finalizer_identity_mismatch"));
    }
    for (id, expected) in &completion.rendered_revision_fingerprints {
        let row = CanonicalRecord::load(client, space, id)
            .await
            .map_err(storage_error)?
            .ok_or_else(|| refusal("certified_revision_missing"))?;
        let CanonicalRecord::DocRevision(row) = row else {
            return Err(refusal("certified_revision_type_invalid"));
        };
        if source_support_fingerprint(&row).map_err(storage_error)? != *expected {
            return Err(refusal("certified_revision_changed"));
        }
    }
    for (id, expected) in &completion.document_fingerprints {
        let row = CanonicalRecord::load(client, space, id)
            .await
            .map_err(storage_error)?
            .ok_or_else(|| refusal("certified_document_missing"))?;
        let CanonicalRecord::Doc(row) = row else {
            return Err(refusal("certified_document_type_invalid"));
        };
        if source_support_fingerprint(&row).map_err(storage_error)? != *expected {
            return Err(refusal("certified_document_changed"));
        }
    }
    let receipt = CanonicalRepository::new(client.clone())
        .receipt_read_only(space, &completion.commit_id)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| refusal("original_receipt_missing"))?;
    if receipt.publication_authority.as_ref() != Some(&selected.publication) {
        return Err(refusal("original_receipt_authority_mismatch"));
    }
    let receipt_evidence = receipt_projection(&receipt)?;
    let (after_source, head_after) = current_source(client, space, selected).await?;
    let after = epoch(client, space).await?;
    if before != after || head_before != head_after || source != after_source {
        return Err(refusal("capture_fence_changed"));
    }
    Ok(Packet {
        evidence: json!({"complete":true,"selection":selected,"input_sha256":expected.input_sha256,
        "epoch_before":before,"epoch_after":after,"source_head_before":head_before,"source_head_after":head_after,
        "operation":operation,"manifest":manifest,"completion":completion,"receipt":receipt_evidence}),
        tables: BTreeMap::new(),
    })
}
fn verify_selected_render_membership(
    completion: &SourcePublicationCompletion,
    selected: &PublicationSelection,
) -> Result<()> {
    if !completion
        .document_fingerprints
        .contains_key(&selected.doc_id)
        || !completion
            .rendered_revision_fingerprints
            .contains_key(&selected.doc_revision_id)
    {
        return Err(refusal("selected_render_certificate_missing"));
    }
    Ok(())
}
fn receipt_projection(receipt: &crate::canonical_commit::CanonicalCommitReceipt) -> Result<Value> {
    let mut projection = value(receipt)?;
    projection
        .as_object_mut()
        .ok_or_else(|| refusal("receipt_shape_invalid"))?
        .remove("replay_output");
    let omission = match &receipt.replay_output {
        Some(output) => json!({"present":true,"omitted":true,"fingerprint":fingerprint(output)?}),
        None => json!({"present":false,"omitted":true,"fingerprint":null}),
    };
    Ok(
        json!({"canonical_receipt":projection,"original_typed_receipt_fingerprint":fingerprint(receipt)?,
        "replay_output_omission":omission}),
    )
}

async fn typed_ids(
    client: &StorageClient,
    table: TableName,
    space: &str,
    after: Option<&RecordId>,
) -> Result<Vec<RecordId>> {
    #[derive(Deserialize)]
    struct IdDb {
        id: surrealdb::sql::Thing,
    }

    if let Some(store) = client.embedded_store() {
        return store
            .physical_record_ids_page(table, space, after, 100)
            .await
            .map_err(storage_error);
    }
    let cursor = after.map_or_else(
        || surrealdb::sql::Thing::from((table.as_str(), "")),
        crate::surreal::thing_from_record_id,
    );
    // table comes only from compiled TableName constants, never the request.
    let mut response = client.query_bind_checked(&format!("SELECT id FROM {table} WHERE space_id = $space AND ($first OR id > $after) ORDER BY id LIMIT 100;"),
        crate::bindings! {"space"=>space,"first"=>after.is_none(),"after"=>cursor}).await.map_err(storage_error)?;
    let rows: Vec<IdDb> = response
        .take(0)
        .map_err(|_| refusal("inventory_id_decode_failed"))?;
    let ids = rows
        .into_iter()
        .map(|row| {
            crate::surreal::record_id_from_thing(&row.id)
                .map_err(|_| refusal("inventory_id_invalid"))
        })
        .collect::<Result<Vec<_>>>()?;
    if ids.len() > 100
        || ids
            .iter()
            .any(|id| id.table() != table || after.is_some_and(|after| id <= after))
        || ids.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(refusal("inventory_page_invalid"));
    }
    Ok(ids)
}
async fn typed_rows(
    client: &StorageClient,
    table: TableName,
    space: &str,
) -> Result<Vec<CanonicalRecord>> {
    let mut rows = Vec::new();
    let mut bytes = 0usize;
    let mut after = None;
    loop {
        let ids = typed_ids(client, table, space, after.as_ref()).await?;
        if ids.is_empty() {
            break;
        }
        for physical_id in &ids {
            if rows.len() >= MAX_ROWS {
                return Err(refusal("inventory_row_limit"));
            }
            let id = match table {
                TableName::WorkItemArchive => RecordId::new(TableName::WorkItem, physical_id.id()),
                TableName::WorkRunArchive => RecordId::new(TableName::WorkRun, physical_id.id()),
                _ => physical_id.clone(),
            };
            let row = CanonicalRecord::load(client, space, &id)
                .await
                .map_err(storage_error)?
                .ok_or_else(|| refusal("inventory_row_disappeared"))?;
            if row.base().space_id != space || row.base().id != id {
                return Err(refusal("inventory_row_identity_mismatch"));
            }
            let row_bytes = encode(&row)?.len();
            bytes += row_bytes;
            if row_bytes > MAX_PAGE_BYTES || bytes > MAX_TOTAL_BYTES {
                return Err(refusal("inventory_byte_limit"));
            }
            rows.push(row);
        }
        after = ids.last().cloned();
    }
    Ok(rows)
}
async fn controls<T: serde::de::DeserializeOwned>(
    client: &StorageClient,
    table: &str,
    space: &str,
    maximum: usize,
) -> Result<Vec<(String, T)>> {
    let mut output = Vec::new();
    let mut after: Option<String> = None;
    let mut bytes = 0usize;
    loop {
        let rows: Vec<(String, String)> = if let Some(store) = client.embedded_store() {
            store
                .extension_rows_page(table, space, after.as_deref(), 100)
                .await
                .map_err(storage_error)?
                .into_iter()
                .map(|(id, row)| {
                    Ok((
                        id,
                        row.get("payload")
                            .and_then(Value::as_str)
                            .ok_or_else(|| refusal("control_payload_missing"))?
                            .to_owned(),
                    ))
                })
                .collect::<Result<_>>()?
        } else {
            #[derive(Deserialize)]
            struct ControlDb {
                id: surrealdb::sql::Thing,
                space_id: String,
                payload: String,
            }
            let cursor = after
                .as_ref()
                .and_then(|id| id.split_once(':').map(|(_, key)| key))
                .unwrap_or("");
            let mut response = client.query_bind_checked(&format!("SELECT id,space_id,payload FROM {table} WHERE space_id = $space AND ($first OR id > $after) ORDER BY id LIMIT 100;"),
                crate::bindings! {"space"=>space,"first"=>after.is_none(),"after"=>surrealdb::sql::Thing::from((table,cursor))}).await.map_err(storage_error)?;
            let rows: Vec<ControlDb> = response
                .take(0)
                .map_err(|_| refusal("control_decode_failed"))?;
            rows.into_iter()
                .map(|row| {
                    let surrealdb::sql::Id::String(key) = row.id.id else {
                        return Err(refusal("control_key_invalid"));
                    };
                    if row.id.tb != table || row.space_id != space {
                        return Err(refusal("control_scope_invalid"));
                    }
                    Ok((format!("{table}:{key}"), row.payload))
                })
                .collect::<Result<_>>()?
        };
        if rows.is_empty() {
            break;
        }
        if rows.len() > 100 {
            return Err(refusal("control_page_limit"));
        }
        for (id, payload) in rows {
            bytes += payload.len();
            if output.len() == maximum
                || bytes > MAX_TOTAL_BYTES
                || after.as_ref().is_some_and(|cursor| id <= *cursor)
            {
                return Err(refusal("control_inventory_limit_or_order"));
            }
            let decoded =
                serde_json::from_str(&payload).map_err(|_| refusal("control_payload_invalid"))?;
            after = Some(id.clone());
            output.push((id, decoded));
        }
    }
    Ok(output)
}
fn insert_table(packet: &mut Packet, table: &str, mut rows: Vec<Value>) -> Result<()> {
    let existing = packet.tables.values().map(Vec::len).sum::<usize>();
    let existing_bytes = packet
        .tables
        .values()
        .try_fold(0usize, |bytes, rows| -> Result<usize> {
            Ok(bytes + encode(rows)?.len())
        })?;
    if existing + rows.len() > MAX_ROWS + MAX_PROVIDER_RECEIPTS
        || existing_bytes + encode(&rows)?.len() > MAX_TOTAL_BYTES
    {
        return Err(refusal("inventory_allocation_exceeded"));
    }
    rows.sort_by(|left, right| inventory_identity(left).cmp(&inventory_identity(right)));
    packet.tables.insert(table.to_owned(), rows);
    Ok(())
}
fn inventory_identity(row: &Value) -> Option<&str> {
    row.get("id")
        .or_else(|| row.get("call_id"))
        .or_else(|| row.get("commit_id"))
        .and_then(Value::as_str)
}
fn inventory(packet: &Packet) -> Result<Value> {
    let mut tables = BTreeMap::new();
    for (name, rows) in &packet.tables {
        let identities: Vec<Value> = rows
            .iter()
            .map(|row| {
                let id = row
                    .get("id")
                    .or_else(|| row.get("call_id"))
                    .or_else(|| row.get("commit_id"))
                    .and_then(Value::as_str)
                    .ok_or_else(|| refusal("inventory_identity_missing"))?;
                Ok(json!({"id":id,"sha256":digest(&encode(row)?)}))
            })
            .collect::<Result<_>>()?;
        let ids = identities
            .iter()
            .map(|row| row["id"].as_str().unwrap_or_default())
            .collect::<BTreeSet<_>>();
        if ids.len() != identities.len() {
            return Err(refusal("duplicate_inventory_identity"));
        }
        // The original rows already travel in hash-bound table pages. Repeating
        // every identity/hash pair in the reply can exhaust its 128 KiB budget
        // long before the unchanged 4096-row capture limit. Commit to that full
        // ordered manifest instead: consumers reconstruct each row's identity
        // and hash from the original canonical row bytes, without reserializing
        // numbers or omitting deleted, superseded or unattributed records.
        let row_manifest = json!({
            "format":"recall-private-primary-row-manifest/v1",
            "source":"original_table_pages",
            "encoding":"canonical_json_utf8",
            "sha256":digest(&encode(&identities)?),
        });
        tables.insert(name, json!({"count":rows.len(),"rows":row_manifest,"table_sha256":digest(&encode(rows)?),"exhausted":true,"selection_scope":if matches!(name.as_str(),"work_item"|"work_run"|"work_artifact"|"provider_call_receipt"){"original_job_work_lineage"}else{"complete_named_space_table"}}));
    }
    Ok(json!(tables))
}
async fn canonical_tables(client: &StorageClient, space: &str, packet: &mut Packet) -> Result<()> {
    for table in CANONICAL_TABLES {
        let rows = typed_rows(client, table, space).await?;
        insert_table(
            packet,
            table.as_str(),
            rows.iter().map(row_value).collect::<Result<_>>()?,
        )?;
    }
    let mut supports = Vec::new();
    for (key, support) in
        controls::<SourceEffectSupport>(client, "source_effect_support", space, MAX_ROWS).await?
    {
        if support.space_id != space
            || key
                != format!(
                    "source_effect_support:{}",
                    crate::canonical_commit::support_key(
                        space,
                        &support.source_id,
                        &support.source_revision,
                        &support.record_id
                    )
                )
        {
            return Err(refusal("support_identity_mismatch"));
        }
        let mut row = value(&support)?;
        row.as_object_mut()
            .ok_or_else(|| refusal("support_shape_invalid"))?
            .insert("id".into(), json!(key));
        supports.push(row);
    }
    insert_table(packet, "source_effect_support", supports)?;
    decision_tables(client, space, packet).await
}
async fn decision_tables(client: &StorageClient, space: &str, packet: &mut Packet) -> Result<()> {
    let canonical = CanonicalRepository::new(client.clone());
    let mut authorized = BTreeSet::new();
    let mut present = BTreeSet::new();
    let mut receipts = Vec::new();
    for (key, original) in controls::<crate::canonical_commit::CanonicalCommitReceipt>(
        client,
        "source_commit_receipt",
        space,
        MAX_ROWS,
    )
    .await?
    {
        if original.space_id != space
            || key
                != format!(
                    "source_commit_receipt:{}",
                    stable_key(&[space, &original.commit_id])
                )
            || !present.insert(original.commit_id.clone())
        {
            return Err(refusal("receipt_identity_mismatch"));
        }
        match canonical
            .receipt_read_only(space, &original.commit_id)
            .await
        {
            Ok(Some(receipt)) => {
                if receipt != original {
                    return Err(refusal("receipt_changed_during_capture"));
                }
                authorized.insert(receipt.commit_id.clone());
                let mut projection = receipt_projection(&receipt)?;
                projection
                    .as_object_mut()
                    .ok_or_else(|| refusal("receipt_shape_invalid"))?
                    .insert("commit_id".into(), json!(receipt.commit_id));
                receipts.push(projection);
            }
            Ok(None) => return Err(refusal("original_receipt_disappeared")),
            Err(StorageError::Conflict(code)) if code == "receipt_source_not_current" => {
                receipts.push(json!({"commit_id":original.commit_id,"space_id":space,
                    "payload_omitted":true,"reason":"source_not_current","fingerprint":fingerprint(&original)?}));
            }
            Err(error) => return Err(storage_error(error)),
        }
    }
    let mut decisions = Vec::new();
    for (key, decision) in
        controls::<SourceDecisionReceipt>(client, "source_decision_receipt", space, MAX_ROWS)
            .await?
    {
        if decision.space_id != space
            || key
                != format!(
                    "source_decision_receipt:{}",
                    stable_key(&[space, &decision.commit_id])
                )
            || !present.contains(&decision.commit_id)
        {
            return Err(refusal("decision_receipt_identity_mismatch"));
        }
        if authorized.contains(&decision.commit_id) {
            decisions.push(value(&decision)?);
        } else {
            // Complete physical membership remains observable, but an old fence
            // grants no authority to the old inverse images or replay output.
            decisions.push(json!({"commit_id":decision.commit_id,"space_id":space,
                "payload_omitted":true,"reason":"source_not_current","fingerprint":fingerprint(&decision)?}));
        }
    }
    insert_table(packet, "source_decision_receipt", decisions)?;
    insert_table(packet, "source_commit_receipt", receipts)
}
pub(super) async fn canonical(
    client: &StorageClient,
    space: &str,
    selected: &[PublicationSelection],
) -> Result<Packet> {
    let before = epoch(client, space).await?;
    let heads_before = heads(client, space, selected).await?;
    let mut packet = Packet::default();
    canonical_tables(client, space, &mut packet).await?;
    let heads_after = heads(client, space, selected).await?;
    let after = epoch(client, space).await?;
    if before != after || heads_before != heads_after {
        return Err(refusal("capture_fence_changed"));
    }
    packet.evidence = json!({"complete":true,"epoch_before":before,"epoch_after":after,
        "source_heads_before":heads_before,"source_heads_after":heads_after,"inventory":inventory(&packet)?,
        "claim_verdict":"independent_review_required","includes_deleted_and_superseded_markers":true});
    Ok(packet)
}
#[allow(
    clippy::too_many_lines,
    reason = "One bounded maintenance capture binds original job, receipt and affected rows between the existing epoch observations."
)]
pub(super) async fn maintenance(
    client: &StorageClient,
    space: &str,
    job_id: &RecordId,
    selected: &[PublicationSelection],
) -> Result<Packet> {
    if job_id.table() != TableName::ReprocessJob {
        return Err(refusal("maintenance_job_type_invalid"));
    }
    let before = epoch(client, space).await?;
    let heads_before = heads(client, space, selected).await?;
    let job = CanonicalRecord::load(client, space, job_id)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| refusal("maintenance_job_missing"))?;
    let CanonicalRecord::ReprocessJob(ref original_job) = job else {
        return Err(refusal("maintenance_job_type_invalid"));
    };
    if original_job.status != ReprocessStatus::Completed
        || original_job.completed_at.is_none()
        || original_job.progress.failed_sources != 0
    {
        return Err(refusal("maintenance_job_incomplete"));
    }
    let mut items = BTreeMap::new();
    for table in [TableName::WorkItem, TableName::WorkItemArchive] {
        for row in typed_rows(client, table, space).await? {
            let CanonicalRecord::WorkItem(item) = &row else {
                return Err(refusal("maintenance_work_type_invalid"));
            };
            if item.kind == "execute_reprocess_job"
                && item.payload.get("job_id") == Some(&json!(job_id))
            {
                items.insert(item.base.id.clone(), row);
            }
        }
    }
    if items.len() != 1 {
        return Err(refusal("maintenance_original_work_ambiguous"));
    }
    let mut runs = BTreeMap::new();
    for table in [TableName::WorkRun, TableName::WorkRunArchive] {
        for row in typed_rows(client, table, space).await? {
            let CanonicalRecord::WorkRun(run) = &row else {
                return Err(refusal("maintenance_run_type_invalid"));
            };
            if items.contains_key(run.work_item.id()) {
                runs.insert(run.base.id.clone(), row);
            }
        }
    }
    if runs.is_empty() {
        return Err(refusal("maintenance_original_runs_missing"));
    }
    let mut artifacts = Vec::new();
    for row in typed_rows(client, TableName::WorkArtifact, space).await? {
        let CanonicalRecord::WorkArtifact(artifact) = &row else {
            return Err(refusal("maintenance_artifact_type_invalid"));
        };
        if runs.contains_key(artifact.work_run.id()) {
            artifacts.push(row_value(&row)?);
        }
    }
    let seed = RecordId::new(
        TableName::WorkArtifact,
        uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_OID,
            format!("reprocess-maintenance:{job_id}").as_bytes(),
        ),
    );
    if !artifacts.iter().any(|row| {
        row.get("id") == Some(&json!(seed))
            && row.get("artifact_type") == Some(&json!("reprocess_maintenance_seed/v1"))
    }) {
        return Err(refusal("maintenance_original_seed_missing"));
    }
    let mut calls = Vec::new();
    for (key, call) in controls::<ProviderCallReceipt>(
        client,
        "provider_call_receipt",
        space,
        MAX_PROVIDER_RECEIPTS,
    )
    .await?
    {
        if key != format!("provider_call_receipt:{}", call.call_id)
            || call.space_id != space
            || call.provenance.config.space_id != space
        {
            return Err(refusal("provider_call_identity_invalid"));
        }
        if let Ok(scope) = RecordId::from_str(&call.scope_id) {
            if runs.contains_key(&scope) {
                let attempt = call
                    .attempt
                    .as_ref()
                    .ok_or_else(|| refusal("provider_attempt_missing"))?;
                let Some(CanonicalRecord::WorkRun(run)) = runs.get(&scope) else {
                    return Err(refusal("provider_run_identity_missing"));
                };
                if attempt.space_id != space
                    || run.work_item.id() != &attempt.work_item_id
                    || !items.contains_key(&attempt.work_item_id)
                {
                    return Err(refusal("provider_attempt_identity_mismatch"));
                }
                calls.push(value(&call)?);
            }
        }
    }
    let mut packet = Packet::default();
    insert_table(
        &mut packet,
        "work_item",
        items.values().map(row_value).collect::<Result<_>>()?,
    )?;
    insert_table(
        &mut packet,
        "work_run",
        runs.values().map(row_value).collect::<Result<_>>()?,
    )?;
    insert_table(&mut packet, "work_artifact", artifacts)?;
    let observed_calls = calls.len();
    insert_table(&mut packet, "provider_call_receipt", calls)?;
    canonical_tables(client, space, &mut packet).await?;
    let heads_after = heads(client, space, selected).await?;
    let after = epoch(client, space).await?;
    let after_job = CanonicalRecord::load(client, space, job_id)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| refusal("maintenance_job_disappeared"))?;
    if before != after || heads_before != heads_after || encode(&job)? != encode(&after_job)? {
        return Err(refusal("capture_fence_changed"));
    }
    packet.evidence = json!({"complete":true,"job":row_value(&job)?,"seed_artifact_id":seed,
        "epoch_before":before,"epoch_after":after,"source_heads_before":heads_before,"source_heads_after":heads_after,
        "inventory":inventory(&packet)?,"original_provider_receipt_count":observed_calls,
        "adjudicator_disposition":if observed_calls==0{"no_recorded_dispatch_review_original_inputs"}else{"review_original_per_call_provenance"},
        "missing_usage_is_zero":false,"adjudicator_conformance":"independent_review_required"});
    Ok(packet)
}

#[cfg(test)]
#[path = "private_primary_evidence_authority_tests.rs"]
mod tests;
