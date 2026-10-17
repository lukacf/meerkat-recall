//! Bounded original graph partition over the already verified immutable spool.
//! Snapshot evidence records the import, never invents document/agent authorship.
use super::{original, MAX_GROUP_BYTES};
use anyhow::{ensure, Context};
use elephant_storage::canonical_commit::CanonicalRecord;
use elephant_storage::historical_adoption::{
    HistoricalSnapshotGraphGroup, HistoricalSnapshotGraphRow,
};
use elephant_storage::transfer_batch::MAX_ROW_BYTES;
use elephant_types::RecordId;
use rusqlite::{params, Connection};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

fn canonical(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(fields) => serde_json::Value::Object(
            fields
                .into_iter()
                .map(|(k, v)| (k, canonical(v)))
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .collect(),
        ),
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonical).collect())
        }
        scalar => scalar,
    }
}
fn canonical_hash(value: &serde_json::Value) -> anyhow::Result<String> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(
        &canonical(value.clone()),
    )?)))
}
fn envelope(record: &CanonicalRecord) -> anyhow::Result<String> {
    let base = record.base();
    let bytes = serde_json::to_vec(&canonical(serde_json::json!([
        base.security,
        base.subjects,
        base.subjects_state
    ])))?;
    ensure!(
        bytes.len() <= MAX_ROW_BYTES,
        "snapshot policy envelope bound"
    );
    Ok(String::from_utf8(bytes)?)
}
fn projected(
    raw: &serde_json::Value,
    table: &str,
    source: &str,
    target: &str,
) -> anyhow::Result<CanonicalRecord> {
    let projected = elephant_storage::transfer_batch::historical_record_projection(table, raw)?;
    let record: CanonicalRecord =
        serde_json::from_value(serde_json::json!({"table": table, "record": projected}))?;
    ensure!(
        record.base().space_id == source
            && record.base().deleted_at.is_none()
            && record.base().provenance.sources.is_empty(),
        "unassigned graph has unsupported source provenance, namespace or lifecycle"
    );
    // Match the document-backed graph adapter: relocate only the declared
    // top-level namespace before typed decoding. Keep the complete original JSON
    // and its hash untouched; do not round-trip it through a typed record.
    let mut relocated = projected;
    relocated
        .as_object_mut()
        .context("snapshot original graph row must be an object")?
        .insert("space_id".into(), target.into());
    Ok(serde_json::from_value(serde_json::json!({
        "table": table,
        "record": relocated,
    }))?)
}

pub(super) fn partition(
    original_db: &Connection,
    index: &Connection,
    source: &str,
    target: &str,
) -> anyhow::Result<Vec<HistoricalSnapshotGraphGroup>> {
    index.execute_batch("BEGIN IMMEDIATE; CREATE TABLE snapshot_candidates(e TEXT,t TEXT,k TEXT,h TEXT,n INTEGER,PRIMARY KEY(t,k)); CREATE INDEX snapshot_order ON snapshot_candidates(e,t,k); CREATE TABLE snapshot_memberships(t TEXT,k TEXT,external TEXT,PRIMARY KEY(t,k));")?;
    let mut query = index.prepare("SELECT g.t,g.k FROM graph g WHERE NOT EXISTS(SELECT 1 FROM memberships m WHERE m.t=g.t AND m.k=g.k) ORDER BY g.t,g.k")?;
    for row in query.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))? {
        let (table, key) = row?;
        let raw = original(original_db, &table, &key)?;
        let record = projected(&raw, &table, source, target)?;
        ensure!(
            record.base().id.to_string() == key,
            "snapshot original key differs"
        );
        let envelope = envelope(&record)?;
        let envelope_hash = hex::encode(Sha256::digest(envelope.as_bytes()));
        let n = serde_json::to_vec(&raw)?.len();
        ensure!(
            n <= MAX_GROUP_BYTES,
            "snapshot original exceeds group bound"
        );
        index.execute(
            "INSERT INTO snapshot_candidates VALUES (?,?,?,?,?)",
            params![envelope_hash, table, key, canonical_hash(&raw)?, n],
        )?;
    }
    let mut groups = Vec::new();
    let mut declaration_bytes = 0usize;
    let mut current_envelope = None;
    let mut members = BTreeMap::new();
    let mut bytes = 0usize;
    let mut query = index.prepare("SELECT e,t,k,h,n FROM snapshot_candidates ORDER BY e,t,k")?;
    for row in query.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, String>(2)?,
            r.get::<_, String>(3)?,
            r.get::<_, usize>(4)?,
        ))
    })? {
        let (policy, table, key, hash, size) = row?;
        if !members.is_empty()
            && (current_envelope.as_ref() != Some(&policy)
                || members.len() == 256
                || bytes.checked_add(size).is_none_or(|n| n > MAX_GROUP_BYTES))
        {
            finish(
                index,
                &mut groups,
                &mut declaration_bytes,
                std::mem::take(&mut members),
            )?;
            bytes = 0;
        }
        let id: RecordId = key.parse()?;
        ensure!(id.table().as_str() == table, "snapshot table/key differs");
        ensure!(
            members.insert(id, hash).is_none(),
            "snapshot row duplicated"
        );
        bytes += size;
        current_envelope = Some(policy);
    }
    if !members.is_empty() {
        finish(index, &mut groups, &mut declaration_bytes, members)?;
    }
    // Rehydrate each bounded group before any destination write; exact envelope
    // equality also prevents treating a hash collision as an authorization join.
    for group in &groups {
        rows(original_db, group, source, target)?;
    }
    index.execute_batch("COMMIT;")?;
    Ok(groups)
}
fn finish(
    index: &Connection,
    groups: &mut Vec<HistoricalSnapshotGraphGroup>,
    declaration_bytes: &mut usize,
    members: BTreeMap<RecordId, String>,
) -> anyhow::Result<()> {
    ensure!(groups.len() < 100_000, "snapshot group population bound");
    let group = HistoricalSnapshotGraphGroup::from_rows(members)?;
    *declaration_bytes = declaration_bytes
        .checked_add(serde_json::to_vec(&group)?.len())
        .context("snapshot declaration size overflow")?;
    ensure!(
        *declaration_bytes <= 64 * 1024 * 1024,
        "complete snapshot declaration byte bound"
    );
    for id in group.original_rows_sha256.keys() {
        index.execute(
            "INSERT INTO snapshot_memberships VALUES (?,?,?)",
            params![
                id.table().as_str(),
                id.to_string(),
                group.external_source_id
            ],
        )?;
    }
    groups.push(group);
    Ok(())
}
pub(super) fn rows(
    db: &Connection,
    group: &HistoricalSnapshotGraphGroup,
    source: &str,
    target: &str,
) -> anyhow::Result<Vec<HistoricalSnapshotGraphRow>> {
    ensure!(
        !group.original_rows_sha256.is_empty() && group.original_rows_sha256.len() <= 256,
        "snapshot group row bound"
    );
    let mut rows = Vec::new();
    let mut first_envelope = None;
    let mut bytes = 0usize;
    for (id, expected) in &group.original_rows_sha256 {
        let original_row = original(db, id.table().as_str(), &id.to_string())?;
        bytes = bytes
            .checked_add(serde_json::to_vec(&original_row)?.len())
            .context("snapshot bytes overflow")?;
        ensure!(
            bytes <= MAX_GROUP_BYTES && canonical_hash(&original_row)? == *expected,
            "snapshot original hash/bytes differs"
        );
        let record = projected(&original_row, id.table().as_str(), source, target)?;
        ensure!(record.base().id == *id, "snapshot record identity differs");
        let policy = envelope(&record)?;
        if let Some(first) = &first_envelope {
            ensure!(*first == policy, "snapshot policy differs");
        } else {
            first_envelope = Some(policy);
        }
        rows.push(HistoricalSnapshotGraphRow {
            record,
            original_row,
            original_row_sha256: expected.clone(),
        });
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
    use super::*;
    use elephant_storage::transfer_batch::strict_json;
    use elephant_types::records::base::{BaseFields, Security};
    use elephant_types::records::{entity::EntityType, Entity};
    use elephant_types::TableName;

    fn fixture(rows: usize) -> (Connection, Connection, Vec<serde_json::Value>) {
        let original = Connection::open_in_memory().expect("spool");
        original.execute_batch("CREATE TABLE raw_rows(t TEXT,k TEXT,lexical BLOB,lexical_sha256 TEXT,PRIMARY KEY(t,k));").expect("schema");
        let index = Connection::open_in_memory().expect("index");
        index.execute_batch("CREATE TABLE graph(t TEXT,k TEXT,PRIMARY KEY(t,k)); CREATE TABLE memberships(t TEXT,k TEXT,external TEXT,PRIMARY KEY(t,k,external));").expect("schema");
        let mut values = Vec::new();
        for i in 0..rows {
            let mut base = BaseFields::new(
                RecordId::new(
                    TableName::Entity,
                    uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, i.to_string().as_bytes()),
                ),
                "default".into(),
                "synthetic-agent-original".into(),
            );
            base.security = Security {
                policy_id: "fixture".into(),
                level: 0,
                ..Security::default()
            };
            let entity = Entity::new(base, EntityType::new("person"), format!("original-{i}"));
            let value = serde_json::to_value(entity).expect("entity");
            let raw = serde_json::to_vec(&value).expect("bytes");
            original
                .execute(
                    "INSERT INTO raw_rows VALUES ('entity',?,?,?)",
                    params![value["id"].as_str(), raw, hex::encode(Sha256::digest(&raw))],
                )
                .expect("row");
            index
                .execute(
                    "INSERT INTO graph VALUES ('entity',?)",
                    [value["id"].as_str()],
                )
                .expect("identity");
            values.push(value);
        }
        (original, index, values)
    }
    fn replace(db: &Connection, value: &serde_json::Value) {
        let bytes = serde_json::to_vec(value).expect("bytes");
        db.execute(
            "UPDATE raw_rows SET lexical=?,lexical_sha256=? WHERE k=?",
            params![
                bytes,
                hex::encode(Sha256::digest(&bytes)),
                value["id"].as_str()
            ],
        )
        .expect("replace");
    }
    #[test]
    fn bounded_snapshot_partition_preserves_original_policy_and_excludes_document_membership() {
        let (db, index, values) = fixture(259);
        // 257 common-envelope rows, one distinct envelope, one document-backed.
        let mut distinct = values[257].clone();
        distinct["security"]["level"] = 2.into();
        replace(&db, &distinct);
        index
            .execute(
                "INSERT INTO memberships VALUES ('entity',?,'document-group')",
                [values[258]["id"].as_str()],
            )
            .expect("membership");
        let groups = partition(&db, &index, "default", "household").expect("partition");
        assert_eq!(groups.len(), 3);
        assert_eq!(
            groups
                .iter()
                .map(|g| g.original_rows_sha256.len())
                .sum::<usize>(),
            258
        );
        assert!(groups.iter().all(|g| g.original_rows_sha256.len() <= 256));
        for group in &groups {
            let decoded = rows(&db, group, "default", "household").expect("rows");
            assert!(decoded
                .iter()
                .all(|r| r.record.base().space_id == "household"
                    && r.original_row["space_id"] == "default"
                    && r.record.base().provenance.sources.is_empty()));
            let first = envelope(&decoded[0].record).expect("envelope");
            assert!(decoded
                .iter()
                .all(|r| envelope(&r.record).expect("envelope") == first));
            assert_eq!(
                HistoricalSnapshotGraphGroup::from_rows(group.original_rows_sha256.clone())
                    .expect("identity"),
                *group
            );
        }
        let excluded: RecordId = values[258]["id"].as_str().expect("id").parse().expect("id");
        assert!(!groups
            .iter()
            .any(|g| g.original_rows_sha256.contains_key(&excluded)));
        let mut changed = values[0].clone();
        changed["name"] = "changed original".into();
        replace(&db, &changed);
        assert!(groups
            .iter()
            .any(|g| rows(&db, g, "default", "household").is_err()));
    }
    #[test]
    fn snapshot_namespace_projection_preserves_original_payload_and_authority() {
        let (_, _, values) = fixture(1);
        for metadata in [
            serde_json::Value::Null,
            serde_json::json!("original metadata"),
            serde_json::json!([1, -0.0_f64, {"embedded_space_id":"default"}]),
            serde_json::json!({"name_embedding":[f64::from(0.1_f32), -0.0_f64, f64::from(f32::from_bits(1))], "space_id":"default"}),
        ] {
            let mut original = values[0].clone();
            original["metadata"] = metadata;
            let bytes = serde_json::to_vec(&original).expect("original bytes");
            let hash = canonical_hash(&original).expect("original hash");
            let mut expected = original.clone();
            expected["space_id"] = "household".into();
            let record = projected(&original, "entity", "default", "household")
                .expect("explicit namespace projection");
            assert_eq!(
                serde_json::to_value(record).expect("typed record")["record"],
                expected
            );
            assert_eq!(
                serde_json::to_vec(&original).expect("unchanged bytes"),
                bytes
            );
            assert_eq!(canonical_hash(&original).expect("unchanged hash"), hash);
            assert!(projected(&original, "entity", "foreign", "household").is_err());
        }
    }

    #[test]
    fn unassigned_source_provenance_refuses_before_destination_mutation() {
        let (db, index, mut values) = fixture(1);
        values[0]["provenance"]["sources"] =
            serde_json::json!([RecordId::new_v4(TableName::Source).to_string()]);
        replace(&db, &values[0]);
        assert!(partition(&db, &index, "default", "household").is_err());
    }
    #[test]
    fn snapshot_hash_domain_sorts_nested_keys_but_retains_exact_original_numbers() {
        let first = strict_json(r#"{"z":{"b":1.0,"a":-0.0},"a":[2,1]}"#).expect("json");
        let second = strict_json(r#"{"a":[2,1],"z":{"a":-0.0,"b":1.0}}"#).expect("json");
        assert_eq!(
            canonical_hash(&first).expect("hash"),
            canonical_hash(&second).expect("hash")
        );
        assert_ne!(
            canonical_hash(&first).expect("hash"),
            canonical_hash(&serde_json::json!({"z":{"b":1,"a":0},"a":[2,1]})).expect("hash")
        );
    }
}
