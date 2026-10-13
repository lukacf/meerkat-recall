//! Exact retained native control values, revalidated for each inspected stream.
//!
//! Lifecycle controls are current-only. Historical retention is limited to the
//! full writer shapes of `publication_schema`, `migration_history`,
//! `recall_durability:journal`, and `native_read_fence`. No table name by itself
//! grants retention. These per-version commitments must never be journaled.

use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;

use elephant_types::publication::{
    OperationState, SourceAuthorizationEnvelope, SourceHead, SourceVisibility,
};
use elephant_types::{RecordId, TableName};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest, Sha256};
use surrealdb::kvs::recall_history::{
    NativeHistoryKey, RecallHistoryAuthority, RecallHistoryRecord, RecallHistorySnapshot,
};
use surrealdb::sql::{Id, Number, Object, Thing, Value};

use crate::erasure_journal::{ErasureIntent, ErasureJournal};
use crate::reference_fence::{GUARD_TABLE, MEMBERSHIP_TABLE, TYPED_UUID_TABLES};
use crate::source_publication::{
    control_thing, encode_payload, head_key, stable_key, StoredOperation,
};
use crate::{StorageError, StorageResult};

fn invalid() -> StorageError {
    // Do not include source values, raw native keys, or journal secrets.
    StorageError::DataIntegrityError(
        "retained native history control differs from its authority".into(),
    )
}

fn require(condition: bool) -> StorageResult<()> {
    if condition {
        Ok(())
    } else {
        Err(invalid())
    }
}

fn text<'a>(fields: &'a Object, name: &str) -> StorageResult<&'a str> {
    match fields.get(name) {
        Some(Value::Strand(value)) => Ok(value.as_str()),
        _ => Err(invalid()),
    }
}

fn integer(fields: &Object, name: &str) -> StorageResult<i64> {
    match fields.get(name) {
        Some(Value::Number(Number::Int(value))) => Ok(*value),
        _ => Err(invalid()),
    }
}

fn fields_exact(fields: &Object, names: &[&str]) -> StorageResult<()> {
    require(fields.len() == names.len() && names.iter().all(|name| fields.contains_key(*name)))
}

fn string_id(identity: &Thing) -> StorageResult<&str> {
    match &identity.id {
        Id::String(value) => Ok(value),
        _ => Err(invalid()),
    }
}

fn canonical_uuid(value: &str) -> bool {
    uuid::Uuid::parse_str(value).is_ok_and(|id| id.to_string() == value)
}

/// Deserialize only the payload string emitted by the original control writer.
///
/// Re-encoding the *complete* typed value rejects unknown nested members,
/// duplicate JSON fields, noncanonical primitive forms, and ignored enum data.
fn payload<T: DeserializeOwned + Serialize>(fields: &Object) -> StorageResult<T> {
    let encoded = text(fields, "payload")?;
    let value: T = serde_json::from_str(encoded).map_err(|_| invalid())?;
    require(encode_payload(&value).map_err(|_| invalid())? == encoded)?;
    Ok(value)
}

fn control<T: DeserializeOwned + Serialize>(fields: &Object) -> StorageResult<T> {
    let mut names = vec!["id", "space_id", "source_id", "payload", "state"];
    if fields.contains_key("canonical_read_nonce") {
        // Existing standing/watch/contact guards update only these two
        // retained control tables. Preserve the exact nonce; never strip it.
        require(
            matches!(fields.get("id"), Some(Value::Thing(id)) if matches!(id.tb.as_str(), "source_head" | "source_operation"))
                && canonical_uuid(text(fields, "canonical_read_nonce")?),
        )?;
        names.push("canonical_read_nonce");
    }
    if fields.contains_key("fence_nonce") {
        // Canonical publication/maintenance and source execution fence the
        // original head without replacing its complete payload/state value.
        require(
            matches!(fields.get("id"), Some(Value::Thing(id)) if id.tb == "source_head")
                && canonical_uuid(text(fields, "fence_nonce")?),
        )?;
        names.push("fence_nonce");
    }
    fields_exact(fields, &names)?;
    let value = payload::<T>(fields)?;
    let expected = crate::surreal::stored_native_value(crate::surreal::json_to_surreal(
        serde_json::to_value(&value).map_err(|_| invalid())?,
    )?);
    let actual = fields.get("state").ok_or_else(invalid)?;
    require(
        RecallHistoryRecord::encode_value(actual).map_err(|_| invalid())?
            == RecallHistoryRecord::encode_value(&expected).map_err(|_| invalid())?,
    )?;
    Ok(value)
}

struct Row<'a> {
    native: &'a RecallHistoryRecord,
    identity: Thing,
    fields: Object,
}

fn rows(snapshot: &RecallHistorySnapshot) -> StorageResult<Vec<Row<'_>>> {
    snapshot.verify().map_err(|_| invalid())?;
    let mut rows = Vec::new();
    for native in &snapshot.records {
        let NativeHistoryKey::Record {
            namespace,
            database,
            identity,
        } = native.decoded_key().map_err(|_| invalid())?
        else {
            continue;
        };
        require(namespace == snapshot.namespace && database == snapshot.database)?;
        if native.tombstone {
            continue;
        }
        let Value::Object(fields) = native.decoded_value().map_err(|_| invalid())? else {
            return Err(invalid());
        };
        require(fields.get("id") == Some(&Value::Thing(identity.clone())))?;
        rows.push(Row {
            native,
            identity,
            fields,
        });
    }
    Ok(rows)
}

struct Context<'a> {
    journal: &'a ErasureJournal,
    space: &'a str,
    intents: BTreeMap<String, ErasureIntent>,
    memberships: BTreeSet<String>,
    heads: BTreeMap<String, SourceHead>,
    operations: BTreeMap<String, StoredOperation>,
}

impl Context<'_> {
    fn source(&self, row: &Row<'_>) -> StorageResult<RecordId> {
        require(text(&row.fields, "space_id")? == self.space)?;
        let source = text(&row.fields, "source_id")?;
        let parsed = RecordId::from_str(source).map_err(|_| invalid())?;
        require(parsed.table() == TableName::Source && parsed.to_string() == source)?;
        Ok(parsed)
    }

    fn envelope(
        &self,
        envelope: &SourceAuthorizationEnvelope,
        source: &RecordId,
    ) -> StorageResult<()> {
        require(envelope.space_id == self.space && envelope.source_id == *source)?;
        require(
            envelope
                .subjects
                .iter()
                .all(|subject| subject.id().table() == TableName::Entity),
        )
    }

    fn capture_current_controls(&mut self, rows: &[Row<'_>]) -> StorageResult<()> {
        for row in rows
            .iter()
            .filter(|row| row.native.current && row.identity.tb == "source_operation")
        {
            if text(&row.fields, "space_id")? != self.space {
                continue;
            }
            let operation = control::<StoredOperation>(&row.fields)?;
            require(
                self.operations
                    .insert(operation.receipt.operation_id.clone(), operation)
                    .is_none(),
            )?;
        }
        for row in rows
            .iter()
            .filter(|row| row.native.current && row.identity.tb == "source_head")
        {
            if text(&row.fields, "space_id")? != self.space {
                continue;
            }
            let head = control::<SourceHead>(&row.fields)?;
            require(
                self.heads
                    .insert(head.source_id.to_string(), head)
                    .is_none(),
            )?;
        }
        Ok(())
    }

    fn lifecycle(&self, row: &Row<'_>) -> StorageResult<bool> {
        let table = row.identity.tb.as_str();
        if !matches!(
            table,
            "source_head"
                | "source_authorization"
                | "source_identity"
                | "source_operation"
                | "source_tombstone_ref"
                | "source_erasure_files"
        ) {
            return Ok(false);
        }
        let source = self.source(row)?;
        let source_key = source.to_string();
        let intent = self.intents.get(&source_key);
        match table {
            "source_head" => {
                let head = control::<SourceHead>(&row.fields)?;
                require(row.identity == control_thing(table, &head_key(self.space, &source)))?;
                require(head.space_id == self.space && head.source_id == source)?;
                require(canonical_uuid(&head.generation) && canonical_uuid(&head.operation_id))?;
                if intent.is_some() {
                    require(
                        matches!(
                            head.lifecycle,
                            SourceVisibility::Erasing | SourceVisibility::Erased
                        ) && head.desired_revision.is_none()
                            && head.published_revision.is_none(),
                    )?;
                } else {
                    require(!matches!(
                        head.lifecycle,
                        SourceVisibility::Erasing | SourceVisibility::Erased
                    ))?;
                }
            }
            "source_authorization" => {
                let envelope = control::<SourceAuthorizationEnvelope>(&row.fields)?;
                require(row.identity == control_thing(table, &head_key(self.space, &source)))?;
                self.envelope(&envelope, &source)?;
                require(self.heads.contains_key(&source_key))?;
            }
            "source_identity" => {
                let identity = control::<SourceIdentity>(&row.fields)?;
                // The external identifier is deliberately erased. Its one-way
                // original key cannot be reconstructed from a new input.
                require(canonical_uuid(string_id(&row.identity)?) && identity.source_id == source)?;
                require(self.heads.contains_key(&source_key))?;
            }
            "source_operation" => self.lifecycle_operation(row, &source, intent.is_some())?,
            "source_tombstone_ref" => {
                fields_exact(
                    &row.fields,
                    &["id", "space_id", "source_id", "token", "intent_id"],
                )?;
                let intent = intent.ok_or_else(invalid)?;
                let token = text(&row.fields, "token")?;
                require(
                    canonical_uuid(token)
                        && text(&row.fields, "intent_id")? == intent.intent_id
                        && row.identity
                            == control_thing(table, &stable_key(&[self.space, &source_key, token])),
                )?;
            }
            "source_erasure_files" => {
                let file = control::<ErasureFile>(&row.fields)?;
                require(
                    intent.is_some()
                        && file.asset_id.table() == TableName::MediaAsset
                        && row.identity
                            == control_thing(
                                table,
                                &stable_key(&[self.space, &source_key, &file.asset_id.to_string()]),
                            ),
                )?;
            }
            _ => return Err(invalid()),
        }
        Ok(true)
    }

    fn lifecycle_operation(
        &self,
        row: &Row<'_>,
        source: &RecordId,
        erasing: bool,
    ) -> StorageResult<()> {
        let operation = control::<StoredOperation>(&row.fields)?;
        let receipt = &operation.receipt;
        require(
            row.identity == control_thing("source_operation", &receipt.operation_id)
                && canonical_uuid(&receipt.operation_id)
                && canonical_uuid(&receipt.source_generation)
                && receipt.space_id == self.space
                && receipt.source_id == *source
                && canonical_uuid(&operation.input_fingerprint)
                && !operation.request_fingerprint.is_empty()
                && operation
                    .contact_digest
                    .as_deref()
                    .is_none_or(canonical_uuid),
        )?;
        self.envelope(&operation.authorization, source)?;
        require(self.heads.contains_key(&source.to_string()))?;
        if erasing {
            require(
                receipt.desired_revision.is_none()
                    && receipt.published_revision.is_none()
                    && receipt.error_code.is_none(),
            )?;
            require(match receipt.state {
                OperationState::Erasing => receipt.completed_at.is_none(),
                OperationState::Erased | OperationState::Superseded => {
                    receipt.completed_at.is_some()
                }
                _ => false,
            })?;
        }
        if let Some(previous) = &operation.predecessor_publication {
            require(
                previous.source_id == *source
                    && canonical_uuid(&previous.generation)
                    && canonical_uuid(&previous.operation_id)
                    && !previous.revision_id.is_empty(),
            )?;
        }
        Ok(())
    }

    fn scoped(&self, row: &Row<'_>) -> StorageResult<bool> {
        if self.lifecycle(row)? {
            return Ok(true);
        }
        let fields = &row.fields;
        match row.identity.tb.as_str() {
            "system_event" => return self.completion_event(row),
            MEMBERSHIP_TABLE => {
                fields_exact(fields, &["id", "space_id", "token"])?;
                let token = text(fields, "token")?;
                require(
                    self.memberships.contains(token)
                        && row.identity
                            == control_thing(MEMBERSHIP_TABLE, &stable_key(&[self.space, token])),
                )?;
            }
            GUARD_TABLE => self.reference_guard(row)?,
            "canonical_epoch" => {
                let mut names = vec!["id", "space_id", "epoch"];
                for nonce in ["standing_watch_read_nonce", "queue_read_nonce"] {
                    if fields.contains_key(nonce) {
                        // These original authority readers touch the existing
                        // epoch row. Their exact nonce remains in its proof.
                        require(canonical_uuid(text(fields, nonce)?))?;
                        names.push(nonce);
                    }
                }
                fields_exact(fields, &names)?;
                require(
                    row.identity == control_thing("canonical_epoch", self.space)
                        && canonical_uuid(text(fields, "epoch")?),
                )?;
            }
            "source_decision_sequence" => {
                fields_exact(fields, &["id", "space_id", "sequence"])?;
                require(
                    row.identity == control_thing("source_decision_sequence", self.space)
                        && integer(fields, "sequence")? > 0,
                )?;
            }
            "outbox_checkpoint" => {
                fields_exact(fields, &["id", "space_id", "consumer", "seq", "updated_at"])?;
                let consumer = text(fields, "consumer")?;
                require(
                    !consumer.is_empty()
                        && integer(fields, "seq")? >= 0
                        && matches!(fields.get("updated_at"), Some(Value::Datetime(_)))
                        && row.identity
                            == control_thing(
                                "outbox_checkpoint",
                                &crate::outbox_checkpoint_repository::key(self.space, consumer),
                            ),
                )?;
            }
            "space_grant" => {
                fields_exact(fields, &["id", "space_id", "subject", "payload"])?;
                let grant = payload::<elephant_types::space_grant::SpaceGrant>(fields)?;
                require(
                    grant.space_id == self.space
                        && grant.subject == text(fields, "subject")?
                        && canonical_uuid(&grant.generation)
                        && row.identity
                            == control_thing(
                                "space_grant",
                                &crate::space_grant_repository::grant_key(
                                    self.space,
                                    &grant.subject,
                                ),
                            ),
                )?;
            }
            _ => return Ok(false),
        }
        Ok(true)
    }

    fn reference_guard(&self, row: &Row<'_>) -> StorageResult<()> {
        let fields = &row.fields;
        let mut names = vec![
            "id",
            "space_id",
            "journal_id",
            "key_fingerprint",
            "version",
            "membership_count",
            "revision",
        ];
        if fields.contains_key("erasure_reservation") {
            names.push("erasure_reservation");
            let reservation = text(fields, "erasure_reservation")?;
            require(
                self.intents
                    .values()
                    .any(|intent| intent.intent_id == reservation),
            )?;
        }
        fields_exact(fields, &names)?;
        require(
            row.identity == control_thing(GUARD_TABLE, self.space)
                && text(fields, "journal_id")? == self.journal.identity()
                && text(fields, "key_fingerprint")? == self.journal.reference_key_fingerprint()
                && integer(fields, "version")? == 1
                && integer(fields, "revision")? >= 0
                && usize::try_from(integer(fields, "membership_count")?).ok()
                    == Some(self.memberships.len()),
        )?;
        Ok(())
    }

    fn completion_records(
        row: &Row<'_>,
        payload: &Object,
        source_id: &RecordId,
    ) -> StorageResult<Vec<serde_json::Value>> {
        let Some(Value::Array(records)) = payload.get("records") else {
            return Err(invalid());
        };
        let mut source_deletes = 0;
        let mut expected_records = Vec::new();
        for record in &records.0 {
            let Value::Object(fields) = record else {
                return Err(invalid());
            };
            fields_exact(fields, &["id", "table", "op"])?;
            let id = RecordId::from_str(text(fields, "id")?).map_err(|_| invalid())?;
            let operation = if id == *source_id {
                source_deletes += 1;
                "delete"
            } else {
                "update"
            };
            require(
                text(fields, "id")? == id.to_string()
                    && text(fields, "table")? == id.table().as_str()
                    && text(fields, "op")? == operation,
            )?;
            expected_records
                .push(serde_json::json!({"id":id,"table":id.table().as_str(),"op":operation}));
        }
        require(source_deletes >= 1 && integer(&row.fields, "seq")? >= 0)?;
        Ok(expected_records)
    }

    fn completion_event(&self, row: &Row<'_>) -> StorageResult<bool> {
        if row.fields.get("event_type") != Some(&Value::from("source.erased")) {
            return Ok(false);
        }
        let Some(Value::Object(payload)) = row.fields.get("payload") else {
            return Err(invalid());
        };
        fields_exact(
            payload,
            &["space_id", "source_id", "operation_id", "records"],
        )?;
        let operation = self
            .operations
            .get(text(payload, "operation_id")?)
            .ok_or_else(invalid)?;
        let receipt = &operation.receipt;
        // Replaying a backup that predates erasure keeps its original accepted
        // operation Superseded. complete_erasure emits this same event from
        // that retained head; replay never invents a later Erased operation.
        require(
            matches!(
                receipt.state,
                OperationState::Erased | OperationState::Superseded
            ) && receipt.space_id == self.space
                && receipt.completed_at.is_some()
                && receipt.desired_revision.is_none()
                && receipt.published_revision.is_none()
                && receipt.error_code.is_none()
                && self.intents.contains_key(&receipt.source_id.to_string())
                && text(payload, "space_id")? == self.space
                && text(payload, "source_id")? == receipt.source_id.to_string(),
        )?;
        require(
            self.heads
                .get(&receipt.source_id.to_string())
                .is_some_and(|head| {
                    head.lifecycle == SourceVisibility::Erased
                        && head.operation_id == receipt.operation_id
                }),
        )?;
        let identity = RecordId::new(
            TableName::SystemEvent,
            uuid::Uuid::new_v5(
                &uuid::Uuid::NAMESPACE_OID,
                format!("{}:source.erased", receipt.operation_id).as_bytes(),
            ),
        );
        require(row.identity == crate::surreal::thing_from_record_id(&identity))?;
        let expected_records = Self::completion_records(row, payload, &receipt.source_id)?;
        let stamp = |name: &str| match row.fields.get(name) {
            Some(Value::Datetime(value)) => Ok(value.0),
            _ => Err(invalid()),
        };
        let event = elephant_types::records::SystemEvent {
            base: elephant_types::records::base::BaseFields {
                id: identity.clone(),
                space_id: self.space.into(),
                created_at: stamp("created_at")?,
                updated_at: stamp("updated_at")?,
                created_by: "source-lifecycle".into(),
                deleted_at: None,
                provenance: elephant_types::records::base::Provenance::default(),
                security: operation.authorization.security.clone(),
                subjects: operation.authorization.subjects.clone(),
                subjects_state: operation.authorization.subjects_state,
            },
            seq: integer(&row.fields, "seq")?,
            event_type: "source.erased".into(),
            payload: serde_json::json!({"space_id":self.space,"source_id":receipt.source_id,
                "operation_id":receipt.operation_id,"records":expected_records}),
        };
        let Value::Object(mut expected) = crate::surreal::stored_native_value(
            crate::SystemEventRepository::system_event_to_value(&event)?,
        ) else {
            return Err(invalid());
        };
        expected.insert(
            "id".into(),
            crate::surreal::thing_from_record_id(&identity).into(),
        );
        require(
            RecallHistoryRecord::encode_value(&Value::Object(row.fields.clone()))
                .map_err(|_| invalid())?
                == RecallHistoryRecord::encode_value(&Value::Object(expected))
                    .map_err(|_| invalid())?,
        )?;
        Ok(true)
    }
}

#[derive(Deserialize, Serialize)]
struct SourceIdentity {
    source_id: RecordId,
}

#[derive(Deserialize, Serialize)]
struct ErasureFile {
    asset_id: RecordId,
}

fn system(row: &Row<'_>, journal: &ErasureJournal) -> StorageResult<bool> {
    let fields = &row.fields;
    let table = row.identity.tb.as_str();
    match table {
        "recall_durability" => {
            fields_exact(fields, &["id", "journal_id", "reference_key_fingerprint"])?;
            require(
                row.identity == control_thing(table, "journal")
                    && text(fields, "journal_id")? == journal.identity()
                    && text(fields, "reference_key_fingerprint")?
                        == journal.reference_key_fingerprint(),
            )?;
        }
        "publication_schema" => {
            fields_exact(fields, &["id", "version"])?;
            require(
                matches!(
                    string_id(&row.identity)?,
                    "canonical" | "source_contact" | "work_producer" | "reference_guard"
                ) && integer(fields, "version")? == 1,
            )?;
        }
        "native_read_fence" => {
            fields_exact(fields, &["id", "version", "nonce"])?;
            let (table, bucket) = string_id(&row.identity)?
                .split_once(':')
                .ok_or_else(invalid)?;
            require(
                matches!(table, "doc" | "source_head" | "source_publication_manifest")
                    && bucket.len() == 3
                    && bucket
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                    && integer(fields, "version")? == 1
                    && canonical_uuid(text(fields, "nonce")?),
            )?;
        }
        "migration_history" => {
            fields_exact(fields, &["id", "name", "checksum", "applied_at"])?;
            let name = text(fields, "name")?;
            let checksum = text(fields, "checksum")?;
            require(
                row.identity == control_thing(table, name)
                    && matches!(fields.get("applied_at"), Some(Value::Datetime(_))),
            )?;
            if name == crate::fake_embedding_repair::RECEIPT_NAME {
                require(checksum == crate::fake_embedding_repair::RECEIPT_CHECKSUM)?;
            } else {
                let sql = migration_sql(name).ok_or_else(invalid)?;
                require(checksum == hex::encode(Sha256::digest(sql.as_bytes())))?;
            }
        }
        _ => return Ok(false),
    }
    Ok(true)
}

fn validate_history_authority(
    journal: &ErasureJournal,
    intent: &ErasureIntent,
    snapshot: &RecallHistorySnapshot,
    authority: &RecallHistoryAuthority,
) -> StorageResult<()> {
    require(
        authority.namespace() == snapshot.namespace
            && authority.database() == snapshot.database
            && authority.space() == intent.space_id,
    )?;
    let original_membership = journal.reference_membership(intent)?;
    let expected = RecallHistoryAuthority::new(
        snapshot.namespace.clone(),
        snapshot.database.clone(),
        journal.identity().into(),
        intent.space_id.clone(),
        *journal.reference_key_bytes(),
        TYPED_UUID_TABLES
            .iter()
            .map(|table| (*table).into())
            .collect(),
        original_membership,
    )
    .map_err(|_| invalid())?;
    // Sensitive values remain in memory only; errors never format either side.
    require(authority.to_rpc_value() == expected.to_rpc_value())?;
    Ok(())
}

/// Retention is derived from the exact original journal and full inspected
/// values. It is not a durable claim: retry and restore recompute every entry.
#[allow(
    clippy::mutable_key_type,
    reason = "Native Thing identities remain immutable while held as ordered proof-map keys; preserve native typed identity and ordering instead of stringifying keys."
)]
pub fn retained_history_control_commitments(
    journal: &ErasureJournal,
    intent: &ErasureIntent,
    snapshot: &RecallHistorySnapshot,
    authority: &RecallHistoryAuthority,
) -> StorageResult<BTreeSet<String>> {
    validate_history_authority(journal, intent, snapshot, authority)?;
    let mut intents = BTreeMap::new();
    for original in journal
        .intents()?
        .into_iter()
        .filter(|item| item.space_id == intent.space_id)
    {
        require(
            intents
                .insert(original.source_id.to_string(), original)
                .is_none(),
        )?;
    }
    let mut memberships = BTreeSet::new();
    for item in intents.values() {
        memberships.extend(journal.reference_membership(item)?);
    }
    let rows = rows(snapshot)?;
    let current_rows = rows
        .iter()
        .filter(|row| row.native.current)
        .map(|row| (row.identity.clone(), row))
        .collect::<BTreeMap<_, _>>();
    let mut context = Context {
        journal,
        space: &intent.space_id,
        intents,
        memberships,
        heads: BTreeMap::new(),
        operations: BTreeMap::new(),
    };
    context.capture_current_controls(&rows)?;
    for head in context.heads.values() {
        let authorization_id = control_thing(
            "source_authorization",
            &head_key(context.space, &head.source_id),
        );
        let operation_id = control_thing("source_operation", &head.operation_id);
        let authorization = current_rows.get(&authorization_id).ok_or_else(invalid)?;
        let envelope = control::<SourceAuthorizationEnvelope>(&authorization.fields)?;
        context.envelope(&envelope, &head.source_id)?;
        let operation = current_rows.get(&operation_id).ok_or_else(invalid)?;
        let operation = control::<StoredOperation>(&operation.fields)?;
        require(
            operation.receipt.space_id == context.space
                && operation.receipt.source_id == head.source_id,
        )?;
        // Replay can rotate an old Active head generation while preserving its
        // superseded operation. Only an actual current erasure receipt claims
        // to own the erasure generation.
        if matches!(
            operation.receipt.state,
            OperationState::Erasing | OperationState::Erased
        ) {
            require(operation.receipt.source_generation == head.generation)?;
        }
    }
    let mut retained = BTreeSet::new();
    let mut installed_memberships = BTreeSet::new();
    let mut guard_seen = false;
    for row in &rows {
        let retain = if system(row, journal)? {
            true
        } else if row.native.current
            && row.fields.get("space_id") == Some(&Value::from(context.space))
        {
            let retain = context.scoped(row)?;
            if row.identity.tb == MEMBERSHIP_TABLE {
                installed_memberships.insert(text(&row.fields, "token")?.to_owned());
            }
            if row.identity.tb == GUARD_TABLE {
                require(!guard_seen)?;
                guard_seen = true;
            }
            retain
        } else {
            false
        };
        if retain {
            retained.insert(
                authority
                    .version_commitment(row.native)
                    .map_err(|_| invalid())?,
            );
        }
    }
    require(guard_seen && installed_memberships == context.memberships)?;
    Ok(retained)
}

// Matches migrations::schema_files, including the original SQL checksums. Kept
// here as source references (not copied SQL or a second migration registry),
// since migrations depends on storage and cannot be a storage dependency.
#[allow(
    clippy::too_many_lines,
    reason = "This exhaustive historical migration-to-SQL registry is the checksum authority for retained history; retain one auditable mapping without changing migration identity."
)]
fn migration_sql(name: &str) -> Option<&'static str> {
    match name {
        "tables" => Some(include_str!("../../migrations/sql/001_init.surql")),
        "indexes" => Some(include_str!("../../migrations/sql/002_indexes.surql")),
        "flexible_nested_objects" => Some(include_str!(
            "../../migrations/sql/003_flexible_nested_objects.surql"
        )),
        "space_config" => Some(include_str!("../../migrations/sql/004_space_config.surql")),
        "entity_embedding" => Some(include_str!(
            "../../migrations/sql/005_entity_embedding.surql"
        )),
        "space_purpose" => Some(include_str!(
            "../../migrations/sql/006_add_space_purpose.surql"
        )),
        "drop_doc_chunk" => Some(include_str!(
            "../../migrations/sql/007_drop_doc_chunk.surql"
        )),
        "space_skill" => Some(include_str!(
            "../../migrations/sql/008_add_space_skill.surql"
        )),
        "freeform_entity_type" => Some(include_str!(
            "../../migrations/sql/009_freeform_entity_type.surql"
        )),
        "event_table" => Some(include_str!("../../migrations/sql/010_add_event.surql")),
        "doc_search" => Some(include_str!("../../migrations/sql/011_doc_search.surql")),
        "assertion_search" => Some(include_str!(
            "../../migrations/sql/012_assertion_search.surql"
        )),
        "doc_media_fields" => Some(include_str!(
            "../../migrations/sql/013_doc_media_fields.surql"
        )),
        "doc_revision_table" => Some(include_str!(
            "../../migrations/sql/014_doc_revision_table.surql"
        )),
        "media_asset_payloads" => Some(include_str!(
            "../../migrations/sql/015_media_asset_payloads.surql"
        )),
        "tm_adjudication" => Some(include_str!(
            "../../migrations/sql/016_tm_adjudication.surql"
        )),
        "schemaless_tables" => Some(include_str!(
            "../../migrations/sql/017_schemaless_tables.surql"
        )),
        "rel_endpoint_indexes" => Some(include_str!(
            "../../migrations/sql/018_rel_endpoint_indexes.surql"
        )),
        "media_perf_indexes" => Some(include_str!(
            "../../migrations/sql/019_media_perf_indexes.surql"
        )),
        "entity_embedding_writes" => Some(include_str!(
            "../../migrations/sql/020_entity_embedding_writes.surql"
        )),
        "knowledge_views" => Some(include_str!(
            "../../migrations/sql/021_knowledge_views.surql"
        )),
        "source_watches" => Some(include_str!(
            "../../migrations/sql/022_source_watches.surql"
        )),
        "knowledge_view_dependency_index" => Some(include_str!(
            "../../migrations/sql/023_knowledge_view_dependency_index.surql"
        )),
        "knowledge_view_definitions" => Some(include_str!(
            "../../migrations/sql/024_knowledge_view_definitions.surql"
        )),
        "knowledge_view_read_indexes" => Some(include_str!(
            "../../migrations/sql/025_knowledge_view_read_indexes.surql"
        )),
        "assertion_revision_index" => Some(include_str!(
            "../../migrations/sql/026_assertion_revision_index.surql"
        )),
        "knowledge_view_surfaces" => Some(include_str!(
            "../../migrations/sql/027_knowledge_view_surfaces.surql"
        )),
        "work_queue_indexes" => Some(include_str!(
            "../../migrations/sql/028_work_queue_indexes.surql"
        )),
        "work_history_archives" => Some(include_str!(
            "../../migrations/sql/029_work_history_archives.surql"
        )),
        "cost_ledger_daily_indexes" => Some(include_str!(
            "../../migrations/sql/030_cost_ledger_daily_indexes.surql"
        )),
        "source_publication" => Some(include_str!(
            "../../migrations/sql/031_source_publication.surql"
        )),
        "work_recovery" => Some(include_str!("../../migrations/sql/032_work_recovery.surql")),
        "space_processing" => Some(include_str!(
            "../../migrations/sql/033_space_processing.surql"
        )),
        "storage_durability" => Some(include_str!(
            "../../migrations/sql/034_storage_durability.surql"
        )),
        "source_publication_manifest" => Some(include_str!(
            "../../migrations/sql/035_source_publication_manifest.surql"
        )),
        "space_grants" => Some(include_str!("../../migrations/sql/036_space_grants.surql")),
        "source_publication_manifest_history" => Some(include_str!(
            "../../migrations/sql/037_source_publication_manifest_history.surql"
        )),
        "source_index_visibility" => Some(include_str!(
            "../../migrations/sql/038_source_index_visibility.surql"
        )),
        "private_reference_fence" => Some(include_str!(
            "../../migrations/sql/039_private_reference_fence.surql"
        )),
        "source_contact_intent" => Some(include_str!(
            "../../migrations/sql/040_source_contact_intent.surql"
        )),
        "work_producer_origin" => Some(include_str!(
            "../../migrations/sql/041_work_producer_origin.surql"
        )),
        "watch_delegation" => Some(include_str!(
            "../../migrations/sql/042_watch_delegation.surql"
        )),
        "native_read_fence" => Some(include_str!(
            "../../migrations/sql/043_native_read_fence.surql"
        )),
        "source_head_semantic_epoch" => Some(include_str!(
            "../../migrations/sql/044_source_head_semantic_epoch.surql"
        )),
        "canonical_read_semantic_epoch" => Some(include_str!(
            "../../migrations/sql/045_canonical_read_semantic_epoch.surql"
        )),
        _ => None,
    }
}

#[cfg(test)]
#[path = "source_history_controls_tests.rs"]
mod tests;
