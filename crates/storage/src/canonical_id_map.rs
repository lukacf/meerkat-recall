//! Explicit native storage projection for identity mappings in canonical merges.

use chrono::{DateTime, Utc};
use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
use elephant_types::records::IdMap;
use elephant_types::{Link, RecordId};
use serde::Deserialize;
use surrealdb::sql::{Thing, Value};

use crate::source_publication::integrity;
use crate::{StorageClient, StorageError, StorageResult};

pub fn native(record: &IdMap) -> StorageResult<Value> {
    let mut row = crate::surreal::base_fields_to_object(&record.base)?;
    row.insert("legacy_id".into(), record.legacy_id.clone().into());
    row.insert(
        "record".into(),
        crate::surreal::thing_from_record_id(&record.record).into(),
    );
    row.insert("source_system".into(), record.source_system.clone().into());
    Ok(Value::Object(row))
}

#[derive(Deserialize)]
struct IdMapDb {
    id: Thing,
    space_id: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    created_by: String,
    deleted_at: Option<DateTime<Utc>>,
    security: Security,
    provenance: Provenance,
    subjects: Vec<Thing>,
    subjects_state: SubjectsState,
    legacy_id: String,
    record: Thing,
    source_system: String,
}

impl TryFrom<IdMapDb> for IdMap {
    type Error = StorageError;
    fn try_from(row: IdMapDb) -> StorageResult<Self> {
        let convert = |id: &Thing| {
            crate::surreal::record_id_from_thing(id).map_err(|error| integrity(&error.to_string()))
        };
        Ok(Self {
            base: BaseFields {
                id: convert(&row.id)?,
                space_id: row.space_id,
                created_at: row.created_at,
                updated_at: row.updated_at,
                created_by: row.created_by,
                deleted_at: row.deleted_at,
                security: row.security,
                provenance: row.provenance,
                subjects: row
                    .subjects
                    .iter()
                    .map(|id| convert(id).map(Link::new))
                    .collect::<StorageResult<_>>()?,
                subjects_state: row.subjects_state,
            },
            legacy_id: row.legacy_id,
            record: convert(&row.record)?,
            source_system: row.source_system,
        })
    }
}

pub async fn load(
    client: &StorageClient,
    space: &str,
    id: &RecordId,
) -> StorageResult<Option<IdMap>> {
    if let Some(store) = client.embedded_store() {
        return store.get::<IdMap>(space, id, true).await;
    }
    // Native fields plus an explicit provenance projection; no flattened domain
    // deserialization, JSON intermediate, formatted times or missing defaults.
    let mut response=client.query_bind_checked(r"SELECT id,space_id,created_at,updated_at,created_by,deleted_at,security,subjects,subjects_state,legacy_id,record,source_system,
        { sources: array::map(provenance.sources, |$s| string::concat(meta::tb($s), ':', meta::id($s))), extractor:provenance.extractor,run_id:provenance.run_id,work_run_id:provenance.work_run_id,prompt_id:provenance.prompt_id,config_id:provenance.config_id,chain:provenance.chain } AS provenance
        FROM id_map WHERE id=$id AND space_id=$space",crate::bindings!{"id"=>crate::surreal::thing_from_record_id(id),"space"=>space}).await?;
    let rows: Vec<IdMapDb> = response
        .take(0)
        .map_err(|error| integrity(&error.to_string()))?;
    rows.into_iter().next().map(IdMap::try_from).transpose()
}
