//! Exact native-version causality using the existing canonical decision authority.
//!
//! Inspection follows logical purge under the original maintenance reservation;
//! opaque proofs become durable before the physical generation can be replaced.

use std::collections::{BTreeMap, BTreeSet};

use surrealdb::kvs::recall_history::{
    NativeHistoryKey, RecallHistoryAuthority, RecallHistoryRecord, RecallHistorySnapshot,
};
use surrealdb::sql::{Number, Thing, Value};

use crate::canonical_commit::{native_expected_fields, CanonicalRecord, SourceEffectSupport};
use crate::erasure_journal::{ErasureIntent, ErasureJournal};
use crate::source_decision::{invalidated_decisions, SourceDecisionReceipt, DECISION_TABLE};
use crate::source_publication::{control_thing, stable_key};
use crate::{StorageError, StorageResult};

fn invalid(message: &'static str) -> StorageError {
    StorageError::DataIntegrityError(message.into())
}

struct DecodedRecord<'a> {
    native: &'a RecallHistoryRecord,
    identity: Thing,
    value: Value,
}

struct DecisionVersion {
    receipt: SourceDecisionReceipt,
    version: u64,
    native_token: String,
}

fn record_spaces(rows: &[DecodedRecord<'_>]) -> BTreeSet<String> {
    rows.iter()
        .filter_map(|row| match &row.value {
            Value::Object(fields) => match fields.get("space_id") {
                Some(Value::Strand(space)) => Some(space.as_str().to_owned()),
                _ => None,
            },
            _ => None,
        })
        .collect()
}

fn records(snapshot: &RecallHistorySnapshot) -> StorageResult<Vec<DecodedRecord<'_>>> {
    snapshot
        .verify()
        .map_err(|_| invalid("native history snapshot verification failed"))?;
    let mut result = Vec::new();
    for native in &snapshot.records {
        let key = native
            .decoded_key()
            .map_err(|_| invalid("native history key decoding failed"))?;
        let NativeHistoryKey::Record {
            namespace,
            database,
            identity,
        } = key
        else {
            continue;
        };
        if namespace != snapshot.namespace || database != snapshot.database {
            return Err(invalid("native history record scope differs"));
        }
        if native.tombstone {
            result.push(DecodedRecord {
                native,
                identity,
                value: Value::None,
            });
            continue;
        }
        let value = native
            .decoded_value()
            .map_err(|_| invalid("native history record decoding failed"))?;
        let Value::Object(fields) = &value else {
            return Err(invalid("native historical record is not an object"));
        };
        if fields.get("id") != Some(&Value::Thing(identity.clone())) {
            return Err(invalid(
                "native historical key and document identity differ",
            ));
        }
        result.push(DecodedRecord {
            native,
            identity,
            value,
        });
    }
    Ok(result)
}

fn decisions(
    rows: &[DecodedRecord<'_>],
    space: &str,
    authority: &RecallHistoryAuthority,
) -> StorageResult<Vec<DecisionVersion>> {
    let mut result = Vec::new();
    for row in rows
        .iter()
        .filter(|row| row.identity.tb == DECISION_TABLE && !row.native.tombstone)
    {
        let Value::Object(fields) = &row.value else {
            return Err(invalid("native decision version is not an object"));
        };
        if fields.get("space_id") != Some(&Value::from(space)) {
            continue;
        }
        let Some(Value::Strand(payload)) = fields.get("payload") else {
            return Err(invalid("historical decision payload is missing"));
        };
        let mut receipt: SourceDecisionReceipt = serde_json::from_str(payload.as_str())
            .map_err(|_| invalid("historical decision payload is invalid"))?;
        // Native canonical commits allocate this scalar in the same transaction.
        // The immutable encoded payload may still contain its preparatory zero.
        let Some(Value::Number(Number::Int(sequence))) = fields.get("sequence") else {
            return Err(invalid("historical native decision sequence is missing"));
        };
        receipt.sequence = u64::try_from(*sequence)
            .ok()
            .filter(|sequence| *sequence > 0)
            .ok_or_else(|| invalid("historical native decision sequence is invalid"))?;
        if receipt.space_id != space
            || receipt.commit_id.is_empty()
            || receipt
                .source_ids
                .iter()
                .any(|source| source.table() != elephant_types::TableName::Source)
            || row.identity
                != control_thing(DECISION_TABLE, &stable_key(&[space, &receipt.commit_id]))
            || receipt.effects.iter().any(|effect| {
                effect
                    .before
                    .iter()
                    .chain(&effect.after)
                    .any(|record| record.base().space_id != space)
            })
        {
            return Err(invalid(
                "historical decision identity or effect scope differs",
            ));
        }
        for effect in &receipt.extension_effects {
            crate::canonical_extension::validate_extension_effect(space, effect)?;
        }
        result.push(DecisionVersion {
            receipt,
            version: row.native.version,
            native_token: authority
                .version_commitment(row.native)
                .map_err(|_| invalid("historical decision commitment failed"))?,
        });
    }
    Ok(result)
}

fn native_afterimage(record: &CanonicalRecord) -> StorageResult<(Thing, Value)> {
    Ok((
        crate::surreal::thing_from_record_id(&record.base().id),
        native_expected_fields(record.native()?),
    ))
}

fn same_canonical(
    before: Option<&CanonicalRecord>,
    after: &CanonicalRecord,
) -> StorageResult<bool> {
    before.map_or(Ok(false), |before| {
        Ok(serde_json::to_value(before)
            .map_err(|_| invalid("historical before-image encoding failed"))?
            == serde_json::to_value(after)
                .map_err(|_| invalid("historical after-image encoding failed"))?)
    })
}

fn matches_afterimage(actual: &Value, expected: &Value, space: &str) -> bool {
    let (Value::Object(actual), Value::Object(expected)) = (actual, expected) else {
        return false;
    };
    // Same complete modeled-field comparison as the canonical row guard.
    // The commitment below binds the FULL actual native bytes, including any
    // native metadata/defaults beyond those modeled fields.
    actual.get("space_id") == Some(&Value::from(space))
        && expected
            .iter()
            .all(|(key, value)| actual.get(key).unwrap_or(&Value::None).equal(value))
}

struct IndependentBoundary {
    version: u64,
    fingerprint: String,
}

#[allow(
    clippy::mutable_key_type,
    reason = "Native Thing identities remain immutable while held as ordered proof-map keys; preserve native typed identity and ordering instead of stringifying keys."
)]
fn independent_boundaries(
    rows: &[DecodedRecord<'_>],
    space: &str,
    source: Option<&elephant_types::RecordId>,
    invalidated: &BTreeSet<&str>,
    imported: &BTreeMap<String, BTreeSet<String>>,
    authority: &RecallHistoryAuthority,
) -> StorageResult<BTreeMap<Thing, Vec<IndependentBoundary>>> {
    let mut result = BTreeMap::<Thing, Vec<IndependentBoundary>>::new();
    for row in rows
        .iter()
        .filter(|row| row.identity.tb == "source_effect_support" && !row.native.tombstone)
    {
        let Value::Object(fields) = &row.value else {
            return Err(invalid("historical support is not an object"));
        };
        if fields.get("space_id") != Some(&Value::from(space)) {
            continue;
        }
        if imported.contains_key(
            &authority
                .version_commitment(row.native)
                .map_err(|_| invalid("historical support commitment failed"))?,
        ) {
            // The authenticated imported seed already accounts for its original
            // support boundaries. Import batching is not a new independent write.
            continue;
        }
        let Some(Value::Strand(payload)) = fields.get("payload") else {
            return Err(invalid("historical support payload is missing"));
        };
        let support: SourceEffectSupport = serde_json::from_str(payload.as_str())
            .map_err(|_| invalid("historical support payload is invalid"))?;
        if support.space_id != space
            || row.identity
                != control_thing(
                    "source_effect_support",
                    &crate::canonical_commit::support_key(
                        &support.space_id,
                        &support.source_id,
                        &support.source_revision,
                        &support.record_id,
                    ),
                )
            || fields.get("source_id") != Some(&Value::from(support.source_id.to_string()))
            || fields.get("record_id") != Some(&Value::from(support.record_id.to_string()))
            || fields.get("source_revision") != Some(&Value::from(support.source_revision.clone()))
            || fields.get("independent") != Some(&Value::Bool(support.independent))
            || support.source_id.table() != elephant_types::TableName::Source
            || support.source_revision.is_empty()
            // The canonical writer uses stable_key: a canonical RFC 4122
            // UUIDv5 of the complete modeled row, not a SHA-256 hex digest.
            || !uuid::Uuid::parse_str(&support.record_fingerprint).is_ok_and(|fingerprint| {
                fingerprint.get_version() == Some(uuid::Version::Sha1)
                    && fingerprint.get_variant() == uuid::Variant::RFC4122
                    && fingerprint.to_string() == support.record_fingerprint
            })
            || fields
                .get("record_fingerprint")
                .is_some_and(|value| value != &Value::from(support.record_fingerprint.clone()))
        {
            return Err(invalid("historical support identity or projection differs"));
        }
        if support.independent
            && Some(&support.source_id) != source
            && support
                .last_commit_id
                .as_deref()
                .is_none_or(|commit| !invalidated.contains(commit))
        {
            result
                .entry(crate::surreal::thing_from_record_id(&support.record_id))
                .or_default()
                .push(IndependentBoundary {
                    version: row.native.version,
                    fingerprint: support.record_fingerprint,
                });
        }
    }
    Ok(result)
}

fn without_read_nonce(value: &Value) -> StorageResult<Option<Vec<u8>>> {
    let Value::Object(mut fields) = value.clone() else {
        return Ok(None);
    };
    match fields.remove("canonical_read_nonce") {
        None | Some(Value::None) => {}
        Some(Value::Strand(nonce))
            if uuid::Uuid::parse_str(nonce.as_str())
                .is_ok_and(|uuid| uuid.to_string() == nonce.as_str()) => {}
        _ => return Ok(None),
    }
    RecallHistoryRecord::encode_value(&Value::Object(fields))
        .map(Some)
        .map_err(|_| invalid("native historical continuation encoding failed"))
}

#[allow(
    clippy::too_many_arguments,
    reason = "Historical afterimage validation requires separate immutable authority, version, identity, independent-boundary map, and mutable erasure output; keep those proof inputs explicit."
)]
#[allow(
    clippy::mutable_key_type,
    reason = "Native Thing identities remain immutable while held as ordered proof-map keys; preserve native typed identity and ordering instead of stringifying keys."
)]
fn commit_afterimage(
    rows: &BTreeMap<Thing, Vec<&DecodedRecord<'_>>>,
    authority: &RecallHistoryAuthority,
    original_version: u64,
    identity: &Thing,
    expected: &Value,
    fingerprint: Option<&str>,
    independent: &BTreeMap<Thing, Vec<IndependentBoundary>>,
    erased: &mut BTreeSet<String>,
) -> StorageResult<()> {
    let Some(history) = rows.get(identity) else {
        return Ok(());
    };
    let origins = history
        .iter()
        .enumerate()
        .filter(|(_, row)| {
            row.native.version == original_version
                && !row.native.tombstone
                && matches_afterimage(&row.value, expected, authority.space())
        })
        .collect::<Vec<_>>();
    if origins.len() > 1 {
        return Err(invalid(
            "historical decision has ambiguous native transaction images",
        ));
    }
    let Some((start, origin)) = origins.first() else {
        // A later identical value is not evidence of the original transaction.
        // Authenticated imported seeds are handled separately below.
        return Ok(());
    };
    let mut previous = **origin;
    erased.insert(
        authority
            .version_commitment(origin.native)
            .map_err(|_| invalid("historical version commitment failed"))?,
    );
    let Some(original_bytes) = without_read_nonce(&origin.value)? else {
        return Ok(());
    };
    for row in history.iter().copied().skip(*start + 1) {
        if row.native.tombstone
            || row.native.version <= previous.native.version
            || independent.get(identity).is_some_and(|boundaries| {
                boundaries.iter().any(|boundary| {
                    boundary.version > previous.native.version
                        && boundary.version <= row.native.version
                        && fingerprint.is_none_or(|fingerprint| fingerprint == boundary.fingerprint)
                })
            })
            || without_read_nonce(&row.value)?.as_ref() != Some(&original_bytes)
        {
            break;
        }
        // Contiguous predecessor proof plus exact full native payload equality
        // across the known private read-fence marker. Never jump over a changed
        // value, tombstone, or independently supported canonical publication.
        erased.insert(
            authority
                .version_commitment(row.native)
                .map_err(|_| invalid("historical continuation commitment failed"))?,
        );
        previous = row;
    }
    Ok(())
}

#[allow(
    clippy::mutable_key_type,
    reason = "Native Thing identities remain immutable while held as ordered proof-map keys; preserve native typed identity and ordering instead of stringifying keys."
)]
fn project(
    rows: &[DecodedRecord<'_>],
    space: &str,
    source: Option<&elephant_types::RecordId>,
    authority: &RecallHistoryAuthority,
    imported: &BTreeMap<String, BTreeSet<String>>,
    source_token: &str,
) -> StorageResult<BTreeSet<String>> {
    let versions = decisions(rows, space, authority)?;
    let invalidated = source.map_or_else(Vec::new, |source| {
        invalidated_decisions(
            versions.iter().map(|row| row.receipt.clone()).collect(),
            source,
        )
    });
    let keys = invalidated
        .iter()
        .map(|receipt| (receipt.commit_id.as_str(), receipt.sequence))
        .collect::<BTreeSet<_>>();
    let invalidated_ids = invalidated
        .iter()
        .map(|receipt| receipt.commit_id.as_str())
        .collect::<BTreeSet<_>>();
    let independent =
        independent_boundaries(rows, space, source, &invalidated_ids, imported, authority)?;
    let mut by_identity = BTreeMap::<Thing, Vec<&DecodedRecord<'_>>>::new();
    for row in rows {
        by_identity
            .entry(row.identity.clone())
            .or_default()
            .push(row);
    }
    for history in by_identity.values_mut() {
        history.sort_by_key(|row| (row.native.version, row.native.timestamp));
    }
    let mut erased = BTreeSet::new();
    // Every authenticated import is explicit, including an empty causal set.
    // Zero/zero stores have no historical transaction identity to seed.
    for row in rows
        .iter()
        .filter(|row| !row.native.tombstone && row.native.version != 0)
    {
        let token = authority
            .version_commitment(row.native)
            .map_err(|_| invalid("imported history commitment failed"))?;
        if imported
            .get(&token)
            .is_some_and(|sources| sources.contains(source_token))
        {
            commit_afterimage(
                &by_identity,
                authority,
                row.native.version,
                &row.identity,
                &row.value,
                None,
                &independent,
                &mut erased,
            )?;
        }
    }
    for decision in versions.iter().filter(|row| {
        row.version != 0
            && !imported.contains_key(&row.native_token)
            && keys.contains(&(row.receipt.commit_id.as_str(), row.receipt.sequence))
    }) {
        for effect in &decision.receipt.effects {
            let Some(after) = &effect.after else {
                continue;
            };
            if same_canonical(effect.before.as_ref(), after)? {
                continue;
            }
            let (identity, expected) = native_afterimage(after)?;
            commit_afterimage(
                &by_identity,
                authority,
                decision.version,
                &identity,
                &expected,
                Some(&after.fingerprint()?),
                &independent,
                &mut erased,
            )?;
        }
        for effect in &decision.receipt.extension_effects {
            let Some(after) = &effect.after else {
                continue;
            };
            if effect.before.as_ref() == Some(after) {
                continue;
            }
            commit_afterimage(
                &by_identity,
                authority,
                decision.version,
                &effect.id,
                &native_expected_fields(after.clone()),
                None,
                &independent,
                &mut erased,
            )?;
        }
    }
    Ok(erased)
}

fn source_token(
    journal: &ErasureJournal,
    snapshot: &RecallHistorySnapshot,
    space: &str,
    source: &elephant_types::RecordId,
) -> StorageResult<String> {
    surrealdb::kvs::recall_history::backup_origin_source_commitment(
        journal.identity(),
        &snapshot.namespace,
        &snapshot.database,
        journal.reference_key_bytes(),
        space,
        &source.to_string(),
    )
    .map_err(|_| invalid("backup causal source commitment failed"))
}

/// Complete current-record coverage for the authenticated snapshot sidecar.
///
/// This evaluates all source identities represented by existing decisions, not
/// merely the intents that happen to exist when the backup is exported.
pub fn backup_history_causality(
    journal: &ErasureJournal,
    snapshot: &RecallHistorySnapshot,
) -> StorageResult<BTreeMap<String, BTreeSet<String>>> {
    let rows = records(snapshot)?;
    let mut output = BTreeMap::<String, BTreeSet<String>>::new();
    let mut current = Vec::new();
    let mut proof_edges = 0usize;
    for row in rows
        .iter()
        .filter(|row| row.native.current && !row.native.tombstone)
    {
        let content = surrealdb::kvs::recall_history::backup_origin_content_commitment(
            journal.identity(),
            &snapshot.namespace,
            &snapshot.database,
            journal.reference_key_bytes(),
            &row.native.key,
            &row.native.value,
        )
        .map_err(|_| invalid("backup current record commitment failed"))?;
        if output.insert(content.clone(), BTreeSet::new()).is_some() {
            return Err(invalid("backup current record commitment is ambiguous"));
        }
        current.push((row, content));
    }
    let spaces = record_spaces(&rows);
    for space in spaces {
        let authority = RecallHistoryAuthority::for_backup_inspection(
            snapshot.namespace.clone(),
            snapshot.database.clone(),
            journal.identity().into(),
            space.clone(),
            *journal.reference_key_bytes(),
            crate::reference_fence::TYPED_UUID_TABLES
                .iter()
                .map(|table| (*table).into())
                .collect(),
        )
        .map_err(|_| invalid("backup causal authority is invalid"))?;
        let imported = crate::source_history_restore::restored_history_causality(
            journal, snapshot, &authority,
        )?;
        let current_versions = current
            .iter()
            .map(|(row, content)| {
                authority
                    .version_commitment(row.native)
                    .map(|version| (version, content.clone()))
                    .map_err(|_| invalid("backup current version commitment failed"))
            })
            .collect::<StorageResult<BTreeMap<_, _>>>()?;
        let mut sources = BTreeMap::new();
        for decision in decisions(&rows, &space, &authority)? {
            for source in decision.receipt.source_ids {
                sources.insert(
                    source_token(journal, snapshot, &space, &source)?,
                    Some(source),
                );
            }
        }
        // Preserve already authenticated causes even when a prior payload-free
        // cleanup removed their original decision control from the live export.
        for token in imported.values().flatten() {
            sources.entry(token.clone()).or_insert(None);
        }
        for (token, source) in sources {
            let erased = project(
                &rows,
                &space,
                source.as_ref(),
                &authority,
                &imported,
                &token,
            )?;
            for version in erased {
                if let Some(content) = current_versions.get(&version) {
                    let causes = output
                        .get_mut(content)
                        .ok_or_else(|| invalid("backup current coverage disappeared"))?;
                    if !causes.contains(&token) {
                        proof_edges = proof_edges
                            .checked_add(1)
                            .ok_or_else(|| invalid("backup causal proof size overflow"))?;
                        if proof_edges > surrealdb::kvs::recall_history::MAX_HISTORY_RECORDS {
                            return Err(invalid(
                                "backup causal proof exceeds its bounded edge inventory",
                            ));
                        }
                        causes.insert(token.clone());
                    }
                }
            }
        }
    }
    if output.values().map(BTreeSet::len).sum::<usize>()
        > surrealdb::kvs::recall_history::MAX_HISTORY_RECORDS
    {
        return Err(invalid(
            "backup causal proof exceeds its bounded edge inventory",
        ));
    }
    Ok(output)
}

/// Versionless exports use the existing live inverse planner, which already
/// resolves independent/contact support and composite-field survival.
///
/// Its exact
/// current before-images are authenticated by the same exclusive export capture.
/// They become historical erasure seeds only if restored into a versioned store.
#[allow(
    clippy::mutable_key_type,
    reason = "Native Thing identities remain immutable while held as ordered proof-map keys; preserve native typed identity and ordering instead of stringifying keys."
)]
pub async fn backup_versionless_history_causality(
    client: &crate::StorageClient,
    journal: &ErasureJournal,
    snapshot: &RecallHistorySnapshot,
) -> StorageResult<BTreeMap<String, BTreeSet<String>>> {
    let mut output = backup_history_causality(journal, snapshot)?;
    let rows = records(snapshot)?;
    let current = rows
        .iter()
        .filter(|row| row.native.current && !row.native.tombstone)
        .map(|row| (row.identity.clone(), row))
        .collect::<BTreeMap<_, _>>();
    let spaces = record_spaces(&rows);
    let repository = crate::source_decision::SourceDecisionRepository::new(client.clone());
    for space in spaces {
        let authority = RecallHistoryAuthority::for_backup_inspection(
            snapshot.namespace.clone(),
            snapshot.database.clone(),
            journal.identity().into(),
            space.clone(),
            *journal.reference_key_bytes(),
            crate::reference_fence::TYPED_UUID_TABLES
                .iter()
                .map(|table| (*table).into())
                .collect(),
        )
        .map_err(|_| invalid("backup live causal authority is invalid"))?;
        let sources = decisions(&rows, &space, &authority)?
            .into_iter()
            .flat_map(|decision| decision.receipt.source_ids)
            .collect::<BTreeSet<_>>();
        for source in sources {
            let inverse = repository.invalidation(&space, &source).await?;
            let token = source_token(journal, snapshot, &space, &source)?;
            for effect in inverse.effects {
                let Some(before) = effect.before else {
                    continue;
                };
                let (identity, expected) = native_afterimage(&before)?;
                record_live_inverse(
                    &current,
                    &mut output,
                    journal,
                    snapshot,
                    &space,
                    &identity,
                    &expected,
                    &token,
                )?;
            }
            for effect in inverse.extension_effects {
                let Some(before) = effect.before else {
                    continue;
                };
                record_live_inverse(
                    &current,
                    &mut output,
                    journal,
                    snapshot,
                    &space,
                    &effect.id,
                    &native_expected_fields(before),
                    &token,
                )?;
            }
        }
    }
    drop(repository);
    if output.values().map(BTreeSet::len).sum::<usize>()
        > surrealdb::kvs::recall_history::MAX_HISTORY_RECORDS
    {
        return Err(invalid(
            "backup live causal proof exceeds its bounded edge inventory",
        ));
    }
    Ok(output)
}

#[allow(
    clippy::too_many_arguments,
    reason = "Live inverse retention binds journal, snapshot authority, source, and current-record map while updating only the output commitments; keep these authority inputs explicit."
)]
#[allow(
    clippy::mutable_key_type,
    reason = "Native Thing identities remain immutable while held as ordered proof-map keys; preserve native typed identity and ordering instead of stringifying keys."
)]
fn record_live_inverse(
    current: &BTreeMap<Thing, &DecodedRecord<'_>>,
    output: &mut BTreeMap<String, BTreeSet<String>>,
    journal: &ErasureJournal,
    snapshot: &RecallHistorySnapshot,
    space: &str,
    identity: &Thing,
    expected: &Value,
    source: &str,
) -> StorageResult<()> {
    let observed = current
        .get(identity)
        .ok_or_else(|| invalid("backup live inverse is absent from complete inventory"))?;
    if !matches_afterimage(&observed.value, expected, space) {
        return Err(invalid(
            "backup live inverse differs from exact current inventory",
        ));
    }
    let content = surrealdb::kvs::recall_history::backup_origin_content_commitment(
        journal.identity(),
        &snapshot.namespace,
        &snapshot.database,
        journal.reference_key_bytes(),
        &observed.native.key,
        &observed.native.value,
    )
    .map_err(|_| invalid("backup live inverse commitment failed"))?;
    output
        .get_mut(&content)
        .ok_or_else(|| invalid("backup live inverse current coverage is missing"))?
        .insert(source.to_owned());
    Ok(())
}

// Diagnostic labels are fixed source constants, never stored table names or
// record identities. They do not grant an exemption from native classification.
fn closure_table_class(table: &str) -> &'static str {
    match table {
        "entity" => "entity",
        "rel" => "rel",
        "source" => "source",
        "system_event" => "system_event",
        "audit_log" => "audit_log",
        "source_effect_support" => "source_effect_support",
        "source_commit_receipt" => "source_commit_receipt",
        "source_decision_receipt" => "source_decision_receipt",
        "source_publication_completion" => "source_publication_completion",
        "source_publication_manifest" => "source_publication_manifest",
        "source_contact_intent" => "source_contact_intent",
        _ => "other",
    }
}

fn check_current_record_closure(
    rows: &[DecodedRecord<'_>],
    authority: &RecallHistoryAuthority,
    retained: &BTreeSet<String>,
) -> StorageResult<()> {
    for row in rows.iter().filter(|row| row.native.current) {
        let commitment = authority
            .version_commitment(row.native)
            .map_err(|_| invalid("current native record commitment failed"))?;
        authority
            .classify_record_version(
                &row.native.key,
                &row.native.value,
                row.native.tombstone,
                true,
                retained.contains(&commitment),
            )
            .map_err(|error| {
                let error = match error {
                    surrealdb::error::Db::InvalidArguments { name, message }
                        if name == "recall history authority"
                            && message
                                == "current source-bearing record survived the original purge" =>
                    {
                        surrealdb::error::Db::InvalidArguments {
                            name,
                            message: format!(
                                "{message} (table class: {})",
                                closure_table_class(&row.identity.tb)
                            ),
                        }
                    }
                    error => error,
                };
                StorageError::from(surrealdb::Error::Db(error))
            })?;
    }
    Ok(())
}

/// Called only by the original maintenance owner after logical purge. The full
/// inspected scope is bound to the eventual physical-copy compare-and-swap.
pub fn prepare_history_authority(
    journal: &ErasureJournal,
    intent: &ErasureIntent,
    snapshot: &RecallHistorySnapshot,
    authority: RecallHistoryAuthority,
) -> StorageResult<RecallHistoryAuthority> {
    let original_intents = journal.intents()?;
    if !original_intents.iter().any(|original| original == intent)
        || snapshot.namespace != authority.namespace()
        || snapshot.database != authority.database()
        || intent.space_id != authority.space()
    {
        return Err(invalid(
            "history projection requires its original intent and exact scope",
        ));
    }
    let expected = RecallHistoryAuthority::new(
        snapshot.namespace.clone(),
        snapshot.database.clone(),
        journal.identity().into(),
        intent.space_id.clone(),
        *journal.reference_key_bytes(),
        crate::reference_fence::TYPED_UUID_TABLES
            .iter()
            .map(|table| (*table).into())
            .collect(),
        journal.reference_membership(intent)?,
    )
    .map_err(|_| invalid("history projection original authority is invalid"))?;
    if expected.to_rpc_value() != authority.to_rpc_value() {
        return Err(invalid(
            "history projection original journal authority differs",
        ));
    }
    let rows = records(snapshot)?;
    let imported =
        crate::source_history_restore::restored_history_causality(journal, snapshot, &authority)?;
    let source_token = source_token(journal, snapshot, &intent.space_id, &intent.source_id)?;
    let erased = project(
        &rows,
        &intent.space_id,
        Some(&intent.source_id),
        &authority,
        &imported,
        &source_token,
    )?;
    let retained = crate::source_history_controls::retained_history_control_commitments(
        journal, intent, snapshot, &authority,
    )?;
    // With one intent this is the physical selector's exact retention policy.
    // Multiple intents retain the union of their control proofs, so leave that
    // case entirely to the unchanged native selector instead of narrowing it.
    if original_intents.len() == 1 {
        check_current_record_closure(&rows, &authority, &retained)?;
    }
    let mut union = journal.history_commitments(intent, &snapshot.namespace, &snapshot.database)?;
    union.extend(erased);
    // Validate disjointness/bounds before acknowledging a new durable proof.
    let bound = authority
        .bind_history(snapshot.fingerprint.clone(), union.clone(), retained)
        .map_err(|_| invalid("history projection has contradictory or oversized proofs"))?;
    journal.record_history_commitments(
        intent,
        &snapshot.namespace,
        &snapshot.database,
        &snapshot.fingerprint,
        &union,
    )?;
    Ok(bound)
}

#[cfg(test)]
#[path = "source_history_authority_tests.rs"]
mod tests;
