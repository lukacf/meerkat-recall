//! Permanent, payload-free fences against delayed writes from erased incarnations.

use elephant_types::TableName;

use crate::{ElephantDbError, ElephantDbState, Result};

pub fn check_erasure_guard(
    state: &ElephantDbState,
    table: TableName,
    value: &serde_json::Value,
    reservation: Option<&str>,
) -> Result<()> {
    // Only the admitted original erasure task may emit the completion event.
    // An event_type string from an ordinary producer conveys no authority.
    if reservation.is_some()
        && table == TableName::SystemEvent
        && value.get("event_type").and_then(serde_json::Value::as_str) == Some("source.erased")
    {
        return Ok(());
    }
    let Some(space) = value.get("space_id").and_then(serde_json::Value::as_str) else {
        return Err(ElephantDbError::DataIntegrity(
            "erasure guard missing space".into(),
        ));
    };
    check_serialized_references(state, space, table.as_str(), value, reservation)
}

pub fn check_serialized_references(
    state: &ElephantDbState,
    space: &str,
    table: &str,
    value: &serde_json::Value,
    reservation: Option<&str>,
) -> Result<()> {
    if check_private_references(state, space, table, value, reservation)? {
        return Ok(());
    }
    let Some(tombstones) = state.extension_tables.get("source_tombstone_ref") else {
        return Ok(());
    };
    let encoded = serde_json::to_string(value)
        .map_err(|error| ElephantDbError::Serialization(error.to_string()))?;
    for tombstone in tombstones.values() {
        if tombstone
            .get("space_id")
            .and_then(serde_json::Value::as_str)
            != Some(space)
        {
            continue;
        }
        let token = tombstone
            .get("token")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                ElephantDbError::DataIntegrity("erasure tombstone missing token".into())
            })?;
        let compact = token.replace('-', "");
        if encoded.contains(token) || encoded.contains(&compact) {
            return Err(ElephantDbError::Conflict(
                "source_erased: record references erased incarnation".into(),
            ));
        }
    }
    Ok(())
}

/// Runtime adapter for the one native reference normalizer used by both backends.
/// Implementations hold their private journal key outside all persisted rows.
pub trait ReferenceTokenNormalizer: Send + Sync {
    /// Original independent erasure journal identity.
    fn journal_identity(&self) -> &str;
    /// Domain-separated keyed commitment to that journal key.
    fn key_fingerprint(&self) -> &str;
    /// Current trusted maintenance scope, absent for ordinary writers.
    fn erasure_reservation(&self) -> Option<String> {
        None
    }
    /// Tokens for actual postimage references, including its physical row identity.
    fn reference_tokens(
        &self,
        space: &str,
        table: &str,
        value: &serde_json::Value,
    ) -> Result<std::collections::BTreeSet<String>>;
}

#[derive(Clone)]
pub struct RuntimeReferenceGuard(pub std::sync::Arc<dyn ReferenceTokenNormalizer>);

impl std::fmt::Debug for RuntimeReferenceGuard {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("RuntimeReferenceGuard(<private>)")
    }
}

impl RuntimeReferenceGuard {
    pub(super) fn validate_anchors(&self, state: &ElephantDbState) -> Result<()> {
        if let Some(anchors) = state.extension_tables.get("source_erasure_guard") {
            for anchor in anchors.values() {
                self.validate_anchor(anchor)?;
            }
        }
        Ok(())
    }

    fn check_reservation(anchor: &serde_json::Value, reservation: Option<&str>) -> Result<()> {
        if let Some(owner) = anchor
            .get("erasure_reservation")
            .filter(|value| !value.is_null())
        {
            let owner = owner
                .as_str()
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    ElephantDbError::DataIntegrity("invalid erasure reservation".into())
                })?;
            if reservation != Some(owner) {
                return Err(ElephantDbError::ErasureInProgress);
            }
        }
        Ok(())
    }

    fn validate_anchor(&self, anchor: &serde_json::Value) -> Result<()> {
        if anchor.get("journal_id").and_then(serde_json::Value::as_str)
            != Some(self.0.journal_identity())
            || anchor
                .get("key_fingerprint")
                .and_then(serde_json::Value::as_str)
                != Some(self.0.key_fingerprint())
            || anchor.get("version").and_then(serde_json::Value::as_u64) != Some(1)
        {
            return Err(ElephantDbError::DataIntegrity(
                "private erasure authority mismatch".into(),
            ));
        }
        Ok(())
    }
}

/// Returns true when the exact private fence replaces legacy UUID matching.
fn check_private_references(
    state: &ElephantDbState,
    space: &str,
    table: &str,
    value: &serde_json::Value,
    reservation: Option<&str>,
) -> Result<bool> {
    let anchors: Vec<_> = state
        .extension_tables
        .get("source_erasure_guard")
        .into_iter()
        .flat_map(|rows| rows.values())
        .filter(|row| row.get("space_id").and_then(serde_json::Value::as_str) == Some(space))
        .collect();
    let memberships: Vec<_> = state
        .extension_tables
        .get("source_erasure_membership")
        .into_iter()
        .flat_map(|rows| rows.values())
        .filter(|row| row.get("space_id").and_then(serde_json::Value::as_str) == Some(space))
        .collect();
    if anchors.is_empty() && memberships.is_empty() {
        return Ok(false);
    }
    let [anchor] = anchors.as_slice() else {
        return Err(ElephantDbError::DataIntegrity(
            "private erasure anchor absent or duplicated".into(),
        ));
    };
    let runtime = state
        .reference_guard
        .as_ref()
        .ok_or_else(|| ElephantDbError::DataIntegrity("private erasure key unavailable".into()))?;
    runtime.validate_anchor(anchor)?;
    RuntimeReferenceGuard::check_reservation(anchor, reservation)?;
    let tokens = memberships
        .into_iter()
        .map(|row| {
            row.get("token")
                .and_then(serde_json::Value::as_str)
                .filter(|token| {
                    token.len() == 64
                        && token
                            .bytes()
                            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                })
                .map(str::to_owned)
                .ok_or_else(|| {
                    ElephantDbError::DataIntegrity("invalid private erasure membership".into())
                })
        })
        .collect::<Result<std::collections::BTreeSet<_>>>()?;
    if anchor
        .get("membership_count")
        .and_then(serde_json::Value::as_u64)
        != Some(u64::try_from(tokens.len()).map_err(|_| {
            ElephantDbError::DataIntegrity("private erasure membership overflow".into())
        })?)
    {
        return Err(ElephantDbError::DataIntegrity(
            "private erasure membership replay incomplete".into(),
        ));
    }
    let references = runtime.0.reference_tokens(space, table, value)?;
    if !references.is_disjoint(&tokens) {
        return Err(ElephantDbError::Conflict(
            "source_erased: record references erased incarnation".into(),
        ));
    }
    Ok(true)
}

pub fn next_revision_row(
    state: &ElephantDbState,
    space: &str,
    reservation: Option<&str>,
) -> Result<Option<serde_json::Value>> {
    let anchors: Vec<_> = state
        .extension_tables
        .get("source_erasure_guard")
        .into_iter()
        .flat_map(|rows| rows.values())
        .filter(|row| row.get("space_id").and_then(serde_json::Value::as_str) == Some(space))
        .collect();
    let Some(runtime) = &state.reference_guard else {
        if !anchors.is_empty() {
            return Err(ElephantDbError::DataIntegrity(
                "private erasure key unavailable".into(),
            ));
        }
        return Ok(None);
    };
    let mut row = match anchors.as_slice() {
        [] => serde_json::json!({
            "id":{"tb":"source_erasure_guard","id":space},"space_id":space,
            "journal_id":runtime.0.journal_identity(),"key_fingerprint":runtime.0.key_fingerprint(),
            "version":1,"membership_count":0,"revision":0,
        }),
        [row] => {
            runtime.validate_anchor(row)?;
            RuntimeReferenceGuard::check_reservation(row, reservation)?;
            (**row).clone()
        }
        _ => {
            return Err(ElephantDbError::DataIntegrity(
                "duplicate private erasure anchor".into(),
            ))
        }
    };
    let revision = row
        .get("revision")
        .and_then(serde_json::Value::as_u64)
        .filter(|revision| *revision < i64::MAX.unsigned_abs())
        .and_then(|revision| revision.checked_add(1))
        .ok_or_else(|| {
            ElephantDbError::DataIntegrity("private erasure revision invalid or exhausted".into())
        })?;
    row["revision"] = revision.into();
    Ok(Some(row))
}
