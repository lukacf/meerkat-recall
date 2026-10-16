//! Historical vector preservation. No embedding request or model approval is implied.
use super::historical_delta::HistoricalChangeKind;
use crate::{EmbeddedMutation, StorageError, StorageResult};
use elephant_embeddings::{level_bucket, PartitionKey, PartitionManager};
use elephant_types::{records::NodeEmbedding, RecordId, TableName};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt::Write as _;
use surrealdb::sql::{Thing, Value};

/// Complete original and successor legacy vector images for guarded preservation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoricalEmbeddingDelta {
    /// Original chunk embedding record identity.
    pub id: RecordId,
    /// Requested vector row transition.
    pub kind: HistoricalChangeKind,
    /// Complete original vector row, absent only for creation.
    pub before: Option<NodeEmbedding>,
    /// Complete successor vector row, absent only for removal.
    pub after: Option<NodeEmbedding>,
    /// Runtime-domain fingerprint of the before-image, or absent for creation.
    pub before_fingerprint: Option<String>,
    /// Runtime-domain fingerprint of the after-image, or absent for removal.
    pub after_fingerprint: Option<String>,
    /// Explicit caller intent for removal or soft deletion; never erasure authority.
    pub destructive_intent: bool,
}
fn refused() -> StorageError {
    StorageError::Conflict("historical_delta:embedding_binding_or_before_image_changed".into())
}
/// Exact source identity, timestamps, payload and f32 vector values in a distinct domain.
pub fn runtime_embedding_fingerprint(row: &NodeEmbedding) -> StorageResult<String> {
    if row.base.id.table() != TableName::ChunkEmbedding
        || row.node.id().table() != TableName::DocNode
        || row.model.trim().is_empty()
        || row.dims <= 0
        || usize::try_from(row.dims).ok() != Some(row.vector.len())
        || !row.vector.iter().all(|v| v.is_finite())
    {
        return Err(refused());
    }
    let bytes = serde_json::to_vec(row).map_err(|_| refused())?;
    if bytes.len() > 64 * 1024 * 1024 {
        return Err(refused());
    }
    let mut hash = Sha256::new();
    hash.update(b"recall-historical-runtime-embedding/v1\0");
    hash.update(bytes);
    Ok(hex::encode(hash.finalize()))
}
impl HistoricalEmbeddingDelta {
    pub(crate) fn validate(&self, space: &str) -> StorageResult<()> {
        for (row, fingerprint) in [
            (&self.before, &self.before_fingerprint),
            (&self.after, &self.after_fingerprint),
        ] {
            match (row, fingerprint) {
                (None, None) => (),
                (Some(row), Some(hash))
                    if row.base.id == self.id
                        && row.base.space_id == space
                        && runtime_embedding_fingerprint(row)? == *hash => {}
                _ => return Err(refused()),
            }
        }
        let kind = match (&self.before, &self.after) {
            (None, Some(_)) => HistoricalChangeKind::Create,
            (Some(_), None) => HistoricalChangeKind::Remove,
            (Some(before), Some(after)) if self.before_fingerprint != self.after_fingerprint => {
                match (
                    before.base.deleted_at.is_some(),
                    after.base.deleted_at.is_some(),
                ) {
                    (false, true) => HistoricalChangeKind::SoftDelete,
                    (true, false) => HistoricalChangeKind::Restore,
                    _ => HistoricalChangeKind::Update,
                }
            }
            _ => return Err(refused()),
        };
        if self.kind != kind
            || (matches!(
                kind,
                HistoricalChangeKind::Remove | HistoricalChangeKind::SoftDelete
            ) && !self.destructive_intent)
        {
            return Err(refused());
        }
        Ok(())
    }
}

pub fn check_tx(
    tx: &EmbeddedMutation<'_>,
    rows: &[HistoricalEmbeddingDelta],
    space: &str,
) -> StorageResult<()> {
    for row in rows {
        row.validate(space)?;
        let expected = row
            .before
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(|_| refused())?;
        if tx.raw_row_by_id(TableName::ChunkEmbedding, &row.id) != expected {
            return Err(refused());
        }
    }
    Ok(())
}
pub fn apply_tx(
    tx: &mut EmbeddedMutation<'_>,
    rows: &[HistoricalEmbeddingDelta],
    space: &str,
) -> StorageResult<()> {
    for row in rows {
        if let Some(after) = &row.after {
            if row.before.is_some() {
                tx.replace(after.clone())?;
            } else {
                tx.create(after.clone())?;
            }
        } else {
            tx.hard_delete_raw_row(TableName::ChunkEmbedding, space, &row.id)?;
        }
        let expected = row
            .after
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(|_| refused())?;
        if tx.raw_row_by_id(TableName::ChunkEmbedding, &row.id) != expected {
            return Err(refused());
        }
    }
    Ok(())
}

/// The initial transfer explicitly maps original embedding ID to partition+node ID.
/// Repeat precisely that representation; do not infer model/profile compatibility.
pub fn native(row: &NodeEmbedding) -> StorageResult<(PartitionKey, Thing, Value)> {
    let _ = runtime_embedding_fingerprint(row)?;
    let norm = row.vector.iter().map(|x| x * x).sum::<f32>().sqrt();
    if !norm.is_finite() {
        return Err(refused());
    }
    let key = PartitionKey::new(
        row.base.security.policy_id.clone(),
        level_bucket(row.base.security.level),
        row.model.clone(),
        row.vector.len(),
    )
    .map_err(|_| refused())?;
    let mut id = crate::surreal::thing_from_record_id(row.node.id());
    id.tb = key.table_name();
    let mut fields = crate::surreal::base_fields_to_object(&row.base)?;
    fields.insert("id".into(), id.clone().into());
    fields.insert(
        "node".into(),
        crate::surreal::thing_from_record_id(row.node.id()).into(),
    );
    fields.insert("model".into(), row.model.clone().into());
    fields.insert("dims".into(), row.dims.into());
    fields.insert("vector_norm".into(), f64::from(norm).into());
    fields.insert(
        "vector".into(),
        Value::Array(surrealdb::sql::Array::from(row.vector.clone())),
    );
    Ok((
        key,
        id,
        crate::surreal::stored_native_value(Value::Object(fields)),
    ))
}

// Preserve BTreeSet insertion semantics using native Thing::Ord directly.
// Equal identities refuse at the same row before any partition DDL executes.
fn insert_native_identity(ids: &mut Vec<Thing>, id: Thing) -> bool {
    match ids.binary_search(&id) {
        Ok(_) => false,
        Err(index) => {
            ids.insert(index, id);
            true
        }
    }
}

/// DDL is bounded and performed only under already-held catch-up custody. Row,
/// source-head, erasure checks and receipts still share one later transaction.
pub async fn prepare_partitions(
    authority: &crate::client::PreparedHistoricalCatchupAuthority,
    rows: &[HistoricalEmbeddingDelta],
) -> StorageResult<()> {
    prepare_partitions_with_client(authority, authority.client()?, rows).await
}

async fn prepare_partitions_with_client(
    authority: &crate::client::PreparedHistoricalCatchupAuthority,
    client: &crate::StorageClient,
    rows: &[HistoricalEmbeddingDelta],
) -> StorageResult<()> {
    if client.embedded_store().is_some() {
        return Ok(());
    }
    let mut keys = BTreeMap::new();
    let mut before_ids = Vec::new();
    let mut after_ids = Vec::new();
    for row in rows {
        if let Some(before) = &row.before {
            let (key, id, _) = native(before)?;
            keys.insert(key.table_name(), key);
            if !insert_native_identity(&mut before_ids, id) {
                return Err(refused());
            }
        }
        if let Some(after) = &row.after {
            let (key, id, _) = native(after)?;
            if !insert_native_identity(&mut after_ids, id) {
                return Err(refused());
            }
            keys.insert(key.table_name(), key);
        }
    }
    for key in keys.values() {
        authority.check_custody()?;
        client
            .query_checked(&PartitionManager::create_partition_ddl(key))
            .await?;
        client
            .query_checked(&crate::reference_fence::native_reference_event(
                &key.table_name(),
            ))
            .await?;
    }
    authority.check_custody()
}

/// Separate guards from writes so a later stale before-image cannot see a prior
/// mutation. A move across physical partitions guards absence at its new ID.
pub fn append_guards(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    rows: &[HistoricalEmbeddingDelta],
    space: &str,
) -> StorageResult<()> {
    for (i, row) in rows.iter().enumerate() {
        row.validate(space)?;
        let before = row.before.as_ref().map(native).transpose()?;
        let after = row.after.as_ref().map(native).transpose()?;
        if let Some((_, id, value)) = &before {
            bindings.insert(format!("vector_{i}_before_id"), id.clone().into());
            bindings.insert(format!("vector_{i}_before"), value.clone());
            writeln!(sql,"IF (SELECT * FROM ONLY $vector_{i}_before_id) != $vector_{i}_before {{ THROW 'historical_vector_changed'; }};").map_err(|_|refused())?;
        }
        if let Some((_, id, value)) = &after {
            bindings.insert(format!("vector_{i}_after_id"), id.clone().into());
            bindings.insert(format!("vector_{i}_after"), value.clone());
            if before.as_ref().map(|(_, old, _)| old) != Some(id) {
                writeln!(sql,"IF (SELECT * FROM ONLY $vector_{i}_after_id) != NONE {{ THROW 'historical_vector_destination_exists'; }};").map_err(|_|refused())?;
            }
        }
    }
    Ok(())
}
pub fn append_writes(sql: &mut String, rows: &[HistoricalEmbeddingDelta]) -> StorageResult<()> {
    for (i, row) in rows.iter().enumerate() {
        let before = row.before.as_ref().map(native).transpose()?;
        let after = row.after.as_ref().map(native).transpose()?;
        let moved = before.as_ref().map(|(_, id, _)| id) != after.as_ref().map(|(_, id, _)| id);
        if before.is_some() && (after.is_none() || moved) {
            writeln!(sql,"DELETE $vector_{i}_before_id; IF (SELECT * FROM ONLY $vector_{i}_before_id) != NONE {{ THROW 'historical_vector_delete_changed'; }};").map_err(|_|refused())?;
        }
        if after.is_some() {
            let verb = if before.is_none() || moved {
                "CREATE"
            } else {
                "UPDATE"
            };
            writeln!(sql,"{verb} $vector_{i}_after_id CONTENT $vector_{i}_after; IF (SELECT * FROM ONLY $vector_{i}_after_id) != $vector_{i}_after {{ THROW 'historical_vector_after_changed'; }};").map_err(|_|refused())?;
        }
    }
    Ok(())
}

/// Match the initial importer entity projection, including its exact f32 check.
pub fn attach_entity_projection(
    entity: &elephant_types::records::Entity,
    fields: &mut surrealdb::sql::Object,
) -> StorageResult<()> {
    let names = [
        "name_embedding",
        "name_embedding_model",
        "name_embedding_text_hash",
    ];
    if names.iter().all(|name| entity.metadata.get(name).is_none()) {
        if entity.metadata.get("name_embedding_dims").is_some() {
            return Err(refused());
        }
        return Ok(());
    }
    let array = entity.metadata[names[0]]
        .as_array()
        .filter(|v| !v.is_empty())
        .ok_or_else(refused)?;
    let model = entity.metadata[names[1]]
        .as_str()
        .filter(|v| !v.is_empty())
        .ok_or_else(refused)?;
    let hash = entity.metadata[names[2]]
        .as_str()
        .filter(|v| !v.is_empty())
        .ok_or_else(refused)?;
    let mut vector = Vec::with_capacity(array.len());
    for value in array {
        let number = value.as_f64().ok_or_else(refused)?;
        #[allow(clippy::cast_possible_truncation)]
        let converted = number as f32;
        #[allow(clippy::cast_possible_truncation)]
        let exact = value.as_i64().map_or_else(
            || {
                value.as_u64().map_or_else(
                    || f64::from(converted).to_bits() == number.to_bits(),
                    |integer| converted as i128 == i128::from(integer),
                )
            },
            |integer| converted as i128 == i128::from(integer),
        );
        if !number.is_finite() || !converted.is_finite() || !exact {
            return Err(refused());
        }
        vector.push(converted);
    }
    if entity
        .metadata
        .get("name_embedding_dims")
        .is_some_and(|value| value.as_u64() != u64::try_from(vector.len()).ok())
    {
        return Err(refused());
    }
    fields.insert(
        names[0].into(),
        Value::Array(surrealdb::sql::Array::from(vector)),
    );
    fields.insert(names[1].into(), model.into());
    fields.insert(names[2].into(), hash.into());
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
    use super::*;
    use elephant_types::{records::BaseFields, Link};
    fn row() -> NodeEmbedding {
        NodeEmbedding {
            base: BaseFields::new(
                RecordId::new_v4(TableName::ChunkEmbedding),
                "original".into(),
                "original-actor".into(),
            ),
            node: Link::new(RecordId::new_v4(TableName::DocNode)),
            model: "original-3072-profile".into(),
            dims: 3,
            vector: vec![0.5, -0.25, -0.0],
        }
    }
    #[test]
    fn vector_fingerprint_binds_exact_values_timestamps_and_native_partition() {
        let before = row();
        let hash = runtime_embedding_fingerprint(&before).expect("hash");
        let mut after = before.clone();
        after.vector[2] = 0.0;
        assert_ne!(
            hash,
            runtime_embedding_fingerprint(&after).expect("signed zero")
        );
        after = before.clone();
        after.base.updated_at += chrono::Duration::seconds(1);
        assert_ne!(
            hash,
            runtime_embedding_fingerprint(&after).expect("timestamp")
        );
        let (partition, id, _) = native(&before).expect("original native partition");
        assert_eq!(id.tb, partition.table_name());
        assert_eq!(
            id.id,
            crate::surreal::thing_from_record_id(before.node.id()).id
        );
        after = before.clone();
        after.model = "different-model".into();
        assert_ne!(id, native(&after).expect("different physical partition").1);
        after = before.clone();
        after.dims = 4;
        assert!(runtime_embedding_fingerprint(&after).is_err());
        after = before;
        after.vector[0] = f32::INFINITY;
        assert!(runtime_embedding_fingerprint(&after).is_err());
    }
    #[test]
    fn vector_delete_and_changed_values_require_original_hash_and_explicit_intent() {
        let before = row();
        let mut after = before.clone();
        after.vector[0] = 0.25;
        let delta = HistoricalEmbeddingDelta {
            id: before.base.id.clone(),
            kind: HistoricalChangeKind::Update,
            before_fingerprint: Some(runtime_embedding_fingerprint(&before).expect("hash")),
            after_fingerprint: Some(runtime_embedding_fingerprint(&after).expect("hash")),
            before: Some(before),
            after: Some(after),
            destructive_intent: false,
        };
        delta.validate("original").expect("exact update");
        let mut forged = delta.clone();
        forged.after.as_mut().expect("after").vector[0] = 0.75;
        assert!(forged.validate("original").is_err());
        assert!(delta.validate("foreign").is_err());
        let mut removal = delta;
        removal.after = None;
        removal.after_fingerprint = None;
        removal.kind = HistoricalChangeKind::Remove;
        assert!(removal.validate("original").is_err());
        removal.destructive_intent = true;
        removal.validate("original").expect("explicit removal");
    }
    #[test]
    fn entity_projection_retains_exact_vector_metadata_and_refuses_rounding() {
        let mut entity = elephant_types::records::Entity::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Entity),
                "original".into(),
                "actor".into(),
            ),
            "person".into(),
            "name".into(),
        );
        entity.metadata = serde_json::json!({"name_embedding":[0.5,-0.25,-0.0],"name_embedding_dims":3,"name_embedding_model":"actual-model","name_embedding_text_hash":"actual-hash"});
        let mut fields = surrealdb::sql::Object::default();
        attach_entity_projection(&entity, &mut fields).expect("exact values");
        assert_eq!(
            fields.get("name_embedding_model"),
            Some(&Value::from("actual-model"))
        );
        entity.metadata["name_embedding"][0] = serde_json::json!(0.1);
        assert!(attach_entity_projection(&entity, &mut fields).is_err());
        entity.metadata = serde_json::json!({"name_embedding_dims":3});
        assert!(attach_entity_projection(&entity, &mut fields).is_err());
    }
}
