//! Typed evidence dependencies. Text, metadata and policy strings are never links.
use super::{integrity, BTreeSet, RecordId, StorageResult, TableName};
use crate::canonical_commit::CanonicalRecord;

/// References which may establish retained evidence membership. This is not an
/// ownership inference: every resulting graph support remains historical unknown.
pub fn historical_evidence_links(record: &CanonicalRecord) -> StorageResult<BTreeSet<RecordId>> {
    typed_links(record, false)
}
fn append_base_links(record: &CanonicalRecord, refs: &mut BTreeSet<RecordId>) -> StorageResult<()> {
    for subject in &record.base().subjects {
        if subject.id().table() != TableName::Entity {
            return Err(integrity("historical subject has wrong table"));
        }
        refs.insert(subject.id().clone());
    }
    for id in &record.base().provenance.sources {
        if id.table() != TableName::Source {
            return Err(integrity("historical provenance source has wrong table"));
        }
        refs.insert(id.clone());
    }
    Ok(())
}

fn typed_links(record: &CanonicalRecord, snapshot: bool) -> StorageResult<BTreeSet<RecordId>> {
    let mut refs = BTreeSet::new();
    append_base_links(record, &mut refs)?;
    let parse = |value: &str| -> StorageResult<RecordId> {
        value
            .parse()
            .map_err(|_| integrity("historical typed reference invalid"))
    };
    match record {
        CanonicalRecord::Doc(row) => {
            refs.extend(row.source.iter().map(|link| link.id().clone()));
        }
        CanonicalRecord::DocRevision(row) => {
            refs.insert(row.doc.id().clone());
        }
        CanonicalRecord::DocNode(row) => {
            refs.insert(row.revision.id().clone());
        }
        CanonicalRecord::Assertion(row) => {
            append_span(&mut refs, &row.evidence, snapshot);
            refs.extend(
                row.subject_entity
                    .iter()
                    .chain(row.object_entity.iter())
                    .map(|link| link.id().clone()),
            );
        }
        CanonicalRecord::Rel(row) => {
            refs.insert(row.subject.id().clone());
            refs.insert(row.object.id().clone());
            for evidence in &row.evidence {
                refs.insert(evidence.assertion.id().clone());
                append_span(&mut refs, &evidence.span, snapshot);
            }
        }
        CanonicalRecord::Attribute(row) => {
            refs.insert(row.subject.id().clone());
            for evidence in &row.evidence {
                refs.insert(evidence.assertion.id().clone());
                append_span(&mut refs, &evidence.span, snapshot);
            }
        }
        CanonicalRecord::Event(row) => {
            refs.extend(row.assertion_refs.iter().map(|link| link.id().clone()));
            refs.extend(row.source_refs.iter().map(|link| link.id().clone()));
            refs.extend(row.base.subjects.iter().map(|link| link.id().clone()));
        }
        CanonicalRecord::EntityIdentityKey(row) => {
            refs.insert(row.entity.id().clone());
            refs.extend(row.evidence.iter().map(|link| link.id().clone()));
        }
        CanonicalRecord::Entity(row) => {
            for handle in &row.handles {
                refs.extend(handle.evidence.iter().cloned());
            }
        }
        CanonicalRecord::Freshness(row) => {
            let id = parse(&row.target)?;
            if id.table().as_str() != row.target_type {
                return Err(integrity("historical freshness target type differs"));
            }
            refs.insert(id);
        }
        CanonicalRecord::TruthSlot(row) => {
            let subject = parse(&row.subject_entity)?;
            if subject.table() != TableName::Entity {
                return Err(integrity("historical truth subject has wrong table"));
            }
            refs.insert(subject);
            refs.extend(row.conflict_group.iter().map(|link| link.id().clone()));
            for value in &row.current_values {
                refs.insert(parse(&value.claim_id)?);
                for evidence in &value.evidence {
                    refs.insert(evidence.assertion.id().clone());
                    append_span(&mut refs, &evidence.span, snapshot);
                }
            }
        }
        CanonicalRecord::ConflictGroup(row) => {
            for claim in &row.claim_ids {
                refs.insert(parse(claim)?);
            }
        }
        CanonicalRecord::Source(_) => (),
        _ => {
            return Err(integrity(
                "unsupported historical evidence membership table",
            ))
        }
    }
    Ok(refs)
}

/// Snapshot closure preserves explicit empty spans as ungrounded. Assertion
/// references remain mandatory; no placeholder revision/node becomes a quote.
pub fn historical_snapshot_links(record: &CanonicalRecord) -> StorageResult<BTreeSet<RecordId>> {
    typed_links(record, true)
}
fn append_span(
    refs: &mut BTreeSet<RecordId>,
    span: &elephant_types::records::assertion::EvidenceSpan,
    snapshot: bool,
) {
    if snapshot && span.start == 0 && span.end == 0 {
        return;
    }
    refs.insert(span.revision.id().clone());
    refs.extend(span.node.iter().map(|link| link.id().clone()));
}
