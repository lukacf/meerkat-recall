//! Private, journal-bound reference membership shared by both storage engines.
//!
//! Native normalization and HMAC come from the pinned engine implementation.
//! Keys live only in runtime adapters; persisted projections contain opaque tokens.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::sync::Arc;

use surrealdb::sql::{Array, Thing, Value};

use crate::embedded::EmbeddedMutation;
use crate::source_publication::{control_thing, stable_key};
use crate::{StorageClient, StorageError, StorageResult};

pub const GUARD_TABLE: &str = "source_erasure_guard";
pub const MEMBERSHIP_TABLE: &str = "source_erasure_membership";
const CUSTODY_SPACE: &str = "__recall_custody__";

// Keep this explicit: an extension UUID-looking string remains a native String.
pub const TYPED_UUID_TABLES: &[&str] = &[
    "entity",
    "rel",
    "doc",
    "media_asset",
    "doc_chunk",
    "chunk_embedding",
    "entity_identity_key",
    "system_event",
    "event",
    "source",
    "audit_log",
    "id_map",
    "identity_candidate",
    "rel_evidence",
    "entity_merge",
    "space_config",
    "doc_revision",
    "doc_node",
    "assertion",
    "attribute",
    "work_item",
    "work_item_archive",
    "work_edge",
    "work_run",
    "work_run_archive",
    "work_artifact",
    "work_deadletter",
    "predicate_registry",
    "predicate_alias",
    "conflict_group",
    "truth_slot",
    "ingest_decision",
    "cost_ledger_daily",
    "freshness",
    "reprocess_job",
    "knowledge_view",
    "knowledge_view_revision",
    "knowledge_view_definition",
    "knowledge_view_dependency",
    "source_watch",
    "source_watch_check",
];

fn integrity(message: &str) -> StorageError {
    StorageError::DataIntegrityError(message.into())
}

pub fn reference_tokens(
    journal_id: &str,
    space: &str,
    key: &[u8; 32],
    value: &Value,
) -> StorageResult<BTreeSet<String>> {
    let tables = TYPED_UUID_TABLES
        .iter()
        .map(|table| (*table).to_owned())
        .collect::<Vec<_>>();
    surrealdb::recall_reference_tokens(journal_id, space, key, &tables, value)
        .map(|tokens| tokens.into_iter().collect())
        .map_err(|_| integrity("private reference normalization failed"))
}

/// Tokenize only the deleted physical identity, never independent identities
/// referenced inside a composite key.
pub fn reference_identity_token(
    journal_id: &str,
    space: &str,
    key: &[u8; 32],
    identity: &Thing,
) -> StorageResult<String> {
    let tables = TYPED_UUID_TABLES
        .iter()
        .map(|table| (*table).to_owned())
        .collect::<Vec<_>>();
    surrealdb::recall_reference_identity_token(journal_id, space, key, &tables, identity)
        .map_err(|_| integrity("private reference identity normalization failed"))
}

pub fn reference_key_fingerprint(journal_id: &str, key: &[u8; 32]) -> StorageResult<String> {
    let value = Value::Thing(Thing::from(("recall_reference_key", journal_id)));
    let tokens = surrealdb::recall_reference_tokens(journal_id, CUSTODY_SPACE, key, &[], &value)
        .map_err(|_| integrity("private reference key invalid"))?;
    match tokens.as_slice() {
        [fingerprint] => Ok(fingerprint.clone()),
        _ => Err(integrity("private reference key commitment invalid")),
    }
}

struct KernelReferenceNormalizer {
    journal_id: String,
    key: [u8; 32],
    key_fingerprint: String,
}

impl elephant_db::ReferenceTokenNormalizer for KernelReferenceNormalizer {
    fn journal_identity(&self) -> &str {
        &self.journal_id
    }
    fn key_fingerprint(&self) -> &str {
        &self.key_fingerprint
    }
    fn erasure_reservation(&self) -> Option<String> {
        crate::client::current_erasure_reservation(&self.journal_id)
    }
    fn reference_tokens(
        &self,
        space: &str,
        table: &str,
        row: &serde_json::Value,
    ) -> elephant_db::Result<BTreeSet<String>> {
        // The physical identity is authoritative, even when a typed embedding
        // stores a DocNode domain ID under a different physical table.
        let identity =
            crate::canonical_extension::embedded_physical_identity(table, row).map_err(|_| {
                elephant_db::ElephantDbError::DataIntegrity(
                    "invalid private reference row identity".into(),
                )
            })?;
        let postimage = crate::surreal::json_to_surreal(row.clone()).map_err(|_| {
            elephant_db::ElephantDbError::DataIntegrity(
                "invalid private reference postimage".into(),
            )
        })?;
        reference_tokens(
            &self.journal_id,
            space,
            &self.key,
            &Value::Array(Array::from(vec![Value::Thing(identity), postimage])),
        )
        .map_err(|_| {
            elephant_db::ElephantDbError::DataIntegrity(
                "private reference normalization failed".into(),
            )
        })
    }
}

pub fn install_kernel_reference_guard(
    db: &elephant_db::ElephantDb,
    journal_id: String,
    key: [u8; 32],
) -> StorageResult<()> {
    let key_fingerprint = reference_key_fingerprint(&journal_id, &key)?;
    db.install_reference_token_normalizer(Arc::new(KernelReferenceNormalizer {
        journal_id,
        key,
        key_fingerprint,
    }))?;
    Ok(())
}

fn validate_tokens(tokens: &BTreeSet<String>) -> StorageResult<()> {
    if tokens.iter().any(|token| {
        token.len() != 64
            || !token
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    }) {
        return Err(integrity("invalid private erasure membership"));
    }
    Ok(())
}

fn validate_anchor(
    anchor: &serde_json::Value,
    journal_id: &str,
    key_fingerprint: &str,
) -> StorageResult<()> {
    if anchor.get("journal_id").and_then(serde_json::Value::as_str) != Some(journal_id)
        || anchor
            .get("key_fingerprint")
            .and_then(serde_json::Value::as_str)
            != Some(key_fingerprint)
        || anchor.get("version").and_then(serde_json::Value::as_u64) != Some(1)
    {
        return Err(integrity("private erasure authority mismatch"));
    }
    Ok(())
}

/// Called after cleanup in the same purge transaction, only after journal fsync.
pub fn project_reference_membership_tx(
    tx: &mut EmbeddedMutation<'_>,
    space: &str,
    journal_id: &str,
    key_fingerprint: &str,
    tokens: &BTreeSet<String>,
) -> StorageResult<()> {
    validate_tokens(tokens)?;
    let mut next_revision = 1_u64;
    let mut reservation = None;
    for (_, row) in tx.scan_extension_rows(GUARD_TABLE)? {
        if row.get("space_id").and_then(serde_json::Value::as_str) == Some(space) {
            validate_anchor(&row, journal_id, key_fingerprint)?;
            reservation = row.get("erasure_reservation").cloned();
            next_revision = checked_revision(&row)?
                .checked_add(1)
                .filter(|revision| *revision <= i64::MAX.unsigned_abs())
                .ok_or_else(|| integrity("private erasure revision exhausted"))?;
        }
    }
    let mut union = tokens.clone();
    for (_, row) in tx.scan_extension_rows(MEMBERSHIP_TABLE)? {
        if row.get("space_id").and_then(serde_json::Value::as_str) == Some(space) {
            union.insert(
                row.get("token")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| integrity("invalid private erasure membership"))?
                    .to_owned(),
            );
        }
    }
    validate_tokens(&union)?;
    for token in tokens {
        tx.upsert_extension_row(
            MEMBERSHIP_TABLE,
            serde_json::json!({
                "id":format!("{MEMBERSHIP_TABLE}:{}", stable_key(&[space,token])),
                "space_id":space,"token":token,
            }),
        )?;
    }
    let mut anchor = serde_json::json!({
        "id":{"tb":GUARD_TABLE,"id":space},"space_id":space,"journal_id":journal_id,
        "key_fingerprint":key_fingerprint,"version":1,"membership_count":union.len(),"revision":next_revision,
    });
    if let Some(owner) = reservation {
        anchor["erasure_reservation"] = owner;
    }
    tx.upsert_extension_row(GUARD_TABLE, anchor)?;
    Ok(())
}

/// Appends membership publication inside the caller's native purge transaction.
pub fn append_reference_membership_sql(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    space: &str,
    journal_id: &str,
    key_fingerprint: &str,
    tokens: &BTreeSet<String>,
) -> StorageResult<()> {
    validate_tokens(tokens)?;
    bindings.extend(crate::bindings! {
        "reference_guard_id" => control_thing(GUARD_TABLE,space),
        "reference_guard_space" => space,
        "reference_guard_journal" => journal_id,
        "reference_guard_fingerprint" => key_fingerprint,
    });
    sql.push_str("LET $reference_guard_before = SELECT * FROM ONLY $reference_guard_id;\nIF $reference_guard_before != NONE AND ($reference_guard_before.journal_id != $reference_guard_journal OR $reference_guard_before.key_fingerprint != $reference_guard_fingerprint OR $reference_guard_before.version != 1) { THROW 'publication:private_erasure_authority_mismatch'; };\n");
    for (index, token) in tokens.iter().enumerate() {
        bindings.insert(
            format!("reference_membership_id_{index}"),
            control_thing(MEMBERSHIP_TABLE, &stable_key(&[space, token])).into(),
        );
        bindings.insert(
            format!("reference_membership_token_{index}"),
            token.clone().into(),
        );
        writeln!(sql, "UPSERT $reference_membership_id_{index} SET space_id=$reference_guard_space, token=$reference_membership_token_{index};").map_err(|error| crate::StorageError::DataIntegrityError(error.to_string()))?;
    }
    sql.push_str("IF $reference_guard_before != NONE AND (!type::is::int($reference_guard_before.revision) OR $reference_guard_before.revision < 0 OR $reference_guard_before.revision >= 9223372036854775807) { THROW 'publication:private_erasure_revision_exhausted'; };\nLET $reference_next_revision = IF $reference_guard_before = NONE THEN 1 ELSE $reference_guard_before.revision + 1 END;\nLET $reference_membership_count = array::len(SELECT id FROM source_erasure_membership WHERE space_id=$reference_guard_space);\nUPSERT $reference_guard_id SET space_id=$reference_guard_space,journal_id=$reference_guard_journal,key_fingerprint=$reference_guard_fingerprint,version=1,membership_count=$reference_membership_count,revision=$reference_next_revision;\n");
    Ok(())
}

/// This bounded projection returns only booleans, never unknown restored payload.
/// Keep the predicate identical to `recall_physical_read_fence_valid` in 043.
async fn validate_native_read_fence_rows(client: &StorageClient) -> StorageResult<()> {
    let mut response = client
        .query_checked(
            "SELECT VALUE (type::is::object($this) AND object::len($this)=3 \
             AND type::is::record($this.id,'native_read_fence') \
             AND type::is::string(meta::id($this.id)) \
             AND string::matches(meta::id($this.id),/^(doc|source_head|source_publication_manifest):[0-9a-f]{3}$/) \
             AND type::is::int($this.version) AND $this.version=1 \
             AND type::is::string($this.nonce) AND string::len($this.nonce)=36 \
             AND string::is::uuid($this.nonce) \
             AND string::matches($this.nonce,/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/)) \
             FROM native_read_fence LIMIT 12289;",
        )
        .await?;
    let rows: Vec<bool> = response.take(0)?;
    if rows.len() > 12288 || rows.iter().any(|valid| !valid) {
        return Err(integrity("invalid payload-free native read fence"));
    }
    Ok(())
}

/// Refreshes historical extension guards after restore, before ordinary readiness.
pub async fn install_native_reference_guards(
    client: &StorageClient,
    tables: &[String],
) -> StorageResult<()> {
    if client.embedded_store().is_some() {
        return Ok(());
    }
    // Refuse ambiguous restored private rows before changing any saved guards.
    // Installing current events must never silently clean or preserve payload
    // hidden in a table that is exempt from source-reference classification.
    validate_native_read_fence_rows(client).await?;
    // Old native backups also carry old event definitions. Reinstall the core
    // function and all known payload guards before considering historical tables.
    client
        .query_checked(include_str!(
            "../../migrations/sql/039_private_reference_fence.surql"
        ))
        .await?;
    // Old exports retain old/missing events. Reinstall bounded physical-key
    // coordination and the metadata-only head event before opening admission.
    client
        .query_checked(include_str!(
            "../../migrations/sql/043_native_read_fence.surql"
        ))
        .await?;
    let mut sql = String::new();
    let mut unique_tables = BTreeSet::new();
    unique_tables.extend(tables.iter());
    for table in unique_tables {
        if reference_guard_exempt(table) {
            continue;
        }
        if table.is_empty()
            || !table
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        {
            return Err(integrity("invalid private reference table"));
        }
        sql.push_str(&native_reference_event(table));
    }
    if !sql.is_empty() {
        client.query_checked(&sql).await?;
    }
    Ok(())
}

fn reference_guard_exempt(table: &str) -> bool {
    matches!(
        table,
        "canonical_epoch"
            | "native_read_fence"
            | "source_identity"
            | "source_authorization"
            | "source_operation"
            | "source_tombstone_ref"
            | "source_erasure_files"
            | "source_erasure_guard"
            | "source_erasure_membership"
            | "source_decision_sequence"
            | "recall_durability"
            | "publication_schema"
            | "migration_history"
            | "restore_barrier"
    )
}

pub fn native_reference_event(table: &str) -> String {
    if table == "source_head" {
        // A retained erased head is metadata, not a late live source reference.
        // Preserve reservation admission and the actual shared writer revision.
        return "REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE source_head;\nDEFINE EVENT OVERWRITE erased_reference_fence ON TABLE source_head\nWHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {\n    IF $event='DELETE' { fn::recall_reference_guard($before,false); } ELSE {\n        fn::recall_reference_guard($after,false);\n        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };\n    };\n};\n".into();
    }
    // Override both historical event names: leaving an old substring guard would
    // wrongly classify harmless prose as reference ownership.
    format!("REMOVE EVENT IF EXISTS recall_erased_reference ON TABLE {table};\nDEFINE EVENT OVERWRITE erased_reference_fence ON TABLE {table}\nWHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {{\n    IF $event = 'DELETE' {{ fn::recall_reference_guard($before, false); }} ELSE {{\n        fn::recall_reference_guard($after, !('{table}'='system_event' AND $after.event_type='source.erased' AND $__recall_erasure_reservation != NONE));\n        IF $event='UPDATE' AND $before.space_id!=$after.space_id {{ fn::recall_reference_guard($before,false); }};\n    }};\n}};\n")
}

/// Sealed inventory observation; callers must capture before their first read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceGuardRevision {
    space: String,
    revision: Option<u64>,
}

fn checked_revision(row: &serde_json::Value) -> StorageResult<u64> {
    row.get("revision")
        .and_then(serde_json::Value::as_u64)
        .filter(|revision| *revision <= i64::MAX.unsigned_abs())
        .ok_or_else(|| integrity("private erasure revision invalid"))
}

fn reference_revision_tx(tx: &EmbeddedMutation<'_>, space: &str) -> StorageResult<Option<u64>> {
    let rows = tx.scan_extension_rows(GUARD_TABLE)?;
    let mut matching = rows
        .iter()
        .filter(|(_, row)| row.get("space_id").and_then(serde_json::Value::as_str) == Some(space));
    let revision = matching
        .next()
        .map(|(_, row)| checked_revision(row))
        .transpose()?;
    if matching.next().is_some() {
        return Err(integrity("duplicate private erasure anchor"));
    }
    Ok(revision)
}

pub async fn reference_guard_revision(
    client: &StorageClient,
    space: &str,
) -> StorageResult<ReferenceGuardRevision> {
    let revision = if let Some(store) = client.embedded_store() {
        store.mutate(|tx| reference_revision_tx(tx, space)).await?
    } else {
        #[derive(serde::Deserialize)]
        struct RevisionDb {
            space_id: String,
            revision: u64,
        }
        let mut response = client
            .query_bind_checked(
                "SELECT space_id,revision FROM $reference_guard_id",
                crate::bindings! { "reference_guard_id"=>control_thing(GUARD_TABLE,space) },
            )
            .await?;
        let rows: Vec<RevisionDb> = response
            .take(0)
            .map_err(|_| integrity("invalid private erasure revision projection"))?;
        match rows.as_slice() {
            [] => None,
            [row] if row.space_id == space && row.revision <= i64::MAX.unsigned_abs() => {
                Some(row.revision)
            }
            _ => return Err(integrity("invalid private erasure revision projection")),
        }
    };
    Ok(ReferenceGuardRevision {
        space: space.into(),
        revision,
    })
}

/// Compare and advance before any purge effects, under the kernel write lock.
pub fn check_reference_guard_revision_tx(
    tx: &mut EmbeddedMutation<'_>,
    observed: &ReferenceGuardRevision,
) -> StorageResult<()> {
    if reference_revision_tx(tx, &observed.space)? != observed.revision {
        return Err(StorageError::Conflict(
            "publication:stale_erasure_inventory".into(),
        ));
    }
    tx.advance_reference_guard_revision(&observed.space)?;
    if reference_revision_tx(tx, &observed.space)?.is_none() {
        return Err(integrity("private erasure key unavailable"));
    }
    Ok(())
}

/// Compare and advance before any purge effects inside the existing transaction.
#[allow(
    clippy::unnecessary_wraps,
    reason = "Preserve the shared fallible transaction-builder contract; the appended native guard can refuse at commit."
)]
pub fn append_reference_guard_revision_sql(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    observed: &ReferenceGuardRevision,
) -> StorageResult<()> {
    bindings.extend(crate::bindings! {
        "inventory_guard_id" => control_thing(GUARD_TABLE,&observed.space),
        "inventory_guard_space" => observed.space.clone(),
        "inventory_guard_revision" => observed.revision.map_or(Value::None, Value::from),
    });
    sql.push_str("LET $inventory_guard = SELECT * FROM ONLY $inventory_guard_id;\nIF $inventory_guard_revision = NONE { IF $inventory_guard != NONE { THROW 'publication:stale_erasure_inventory'; }; } ELSE { IF $inventory_guard = NONE OR $inventory_guard.space_id != $inventory_guard_space OR $inventory_guard.revision != $inventory_guard_revision { THROW 'publication:stale_erasure_inventory'; }; };\nfn::recall_reference_guard({id:$inventory_guard_id,space_id:$inventory_guard_space}, false);\n");
    Ok(())
}

fn require_reservation_scope(journal_id: &str, intent_id: &str) -> StorageResult<()> {
    if crate::client::current_erasure_reservation(journal_id).as_deref() != Some(intent_id) {
        return Err(integrity("trusted erasure reservation scope required"));
    }
    Ok(())
}

pub async fn reserve_reference_guard(
    client: &StorageClient,
    space: &str,
    intent: &crate::erasure_journal::ErasureIntent,
) -> StorageResult<()> {
    require_reservation_scope(client.erasure_journal()?.identity(), &intent.intent_id)?;
    if intent.space_id != space {
        return Err(integrity("foreign erasure reservation space"));
    }
    if let Some(store) = client.embedded_store() {
        return store
            .mutate(|tx| {
                tx.advance_reference_guard_revision(space)?;
                let rows = tx.scan_extension_rows(GUARD_TABLE)?;
                let mut anchor = rows
                    .into_iter()
                    .find(|(_, row)| {
                        row.get("space_id").and_then(serde_json::Value::as_str) == Some(space)
                    })
                    .map(|(_, row)| row)
                    .ok_or_else(|| integrity("private erasure key unavailable"))?;
                if anchor
                    .get("erasure_reservation")
                    .filter(|owner| !owner.is_null())
                    .is_some_and(|owner| owner.as_str() != Some(intent.intent_id.as_str()))
                {
                    return Err(StorageError::ErasureInProgress);
                }
                anchor["erasure_reservation"] = intent.intent_id.clone().into();
                tx.upsert_extension_row(GUARD_TABLE, anchor)?;
                Ok(())
            })
            .await;
    }
    client.query_bind_checked("BEGIN TRANSACTION; fn::recall_reference_guard({id:$reservation_guard_id,space_id:$reservation_space},false); LET $reservation_guard=SELECT * FROM ONLY $reservation_guard_id; IF $reservation_guard.erasure_reservation != NONE AND $reservation_guard.erasure_reservation != $reservation_owner { THROW 'publication:erasure_space_reserved'; }; UPDATE $reservation_guard_id SET erasure_reservation=$reservation_owner; COMMIT TRANSACTION;",crate::bindings! {"reservation_guard_id"=>control_thing(GUARD_TABLE,space),"reservation_space"=>space,"reservation_owner"=>intent.intent_id.clone()}).await?;
    Ok(())
}

pub fn release_reference_guard_tx(
    tx: &mut EmbeddedMutation<'_>,
    space: &str,
    intent_id: &str,
) -> StorageResult<()> {
    tx.advance_reference_guard_revision(space)?;
    let rows = tx.scan_extension_rows(GUARD_TABLE)?;
    let mut anchor = rows
        .into_iter()
        .find(|(_, row)| row.get("space_id").and_then(serde_json::Value::as_str) == Some(space))
        .map(|(_, row)| row)
        .ok_or_else(|| integrity("erasure reservation missing"))?;
    if anchor
        .get("erasure_reservation")
        .and_then(serde_json::Value::as_str)
        != Some(intent_id)
    {
        return Err(integrity("erasure reservation owner mismatch"));
    }
    anchor
        .as_object_mut()
        .ok_or_else(|| integrity("invalid erasure reservation"))?
        .remove("erasure_reservation");
    tx.upsert_extension_row(GUARD_TABLE, anchor)?;
    Ok(())
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "Preserve the shared fallible transaction-builder contract; the appended native guard can refuse at commit."
)]
pub fn append_release_reference_guard_sql(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    space: &str,
    intent_id: &str,
) -> StorageResult<()> {
    bindings.extend(crate::bindings! {"release_guard_id"=>control_thing(GUARD_TABLE,space),"release_guard_space"=>space,"release_guard_owner"=>intent_id});
    sql.push_str("LET $release_guard=SELECT * FROM ONLY $release_guard_id;\nIF $release_guard=NONE OR $release_guard.erasure_reservation!=$release_guard_owner { THROW 'publication:erasure_reservation_owner_mismatch'; };\nfn::recall_reference_guard({id:$release_guard_id,space_id:$release_guard_space},false);\nUPDATE $release_guard_id UNSET erasure_reservation;\n");
    Ok(())
}

/// Recovery must resume reserved owners first and refuse missing journal intents.
pub async fn reference_reservations(
    client: &StorageClient,
) -> StorageResult<Vec<(String, String)>> {
    let rows = if let Some(store) = client.embedded_store() {
        store
            .mutate(|tx| {
                tx.scan_extension_rows(GUARD_TABLE)?
                    .into_iter()
                    .filter_map(|(_, row)| {
                        row.get("erasure_reservation")
                            .filter(|owner| !owner.is_null())
                            .map(|owner| {
                                let space = row
                                    .get("space_id")
                                    .and_then(serde_json::Value::as_str)
                                    .ok_or_else(|| {
                                        integrity("invalid erasure reservation space")
                                    })?;
                                let owner = owner.as_str().ok_or_else(|| {
                                    integrity("invalid erasure reservation owner")
                                })?;
                                Ok((space.to_owned(), owner.to_owned()))
                            })
                    })
                    .collect::<StorageResult<Vec<_>>>()
            })
            .await?
    } else {
        #[derive(serde::Deserialize)]
        struct ReservationDb {
            space_id: String,
            erasure_reservation: String,
        }
        let mut response=client.query_checked("SELECT space_id,erasure_reservation FROM source_erasure_guard WHERE erasure_reservation != NONE").await?;
        let rows: Vec<ReservationDb> = response
            .take(0)
            .map_err(|_| integrity("invalid erasure reservation projection"))?;
        rows.into_iter()
            .map(|row| (row.space_id, row.erasure_reservation))
            .collect()
    };
    for (space, owner) in &rows {
        if space.is_empty() || uuid::Uuid::parse_str(owner).is_err() {
            return Err(integrity("invalid erasure reservation identity"));
        }
    }
    Ok(rows)
}

#[cfg(test)]
#[path = "reference_fence_tests.rs"]
mod tests;
