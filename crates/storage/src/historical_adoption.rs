//! Offline historical publication, distinct from work-produced publication.
//!
//! A historical proof attests to exact imported rows, not to an extraction run.
//! Missing embedding lineage remains explicitly pending. Normal queue/finalizer
//! entry points cannot install or complete this proof.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};
use elephant_types::publication::SourceRevision;
use elephant_types::records::{Doc, DocNode, DocRevision};
use elephant_types::{RecordId, TableName};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::canonical_commit::source_support_fingerprint;
use crate::source_publication::{integrity, stable_key};
use crate::source_publication_manifest::{SourceDocumentMapping, SourcePublicationManifest};
use crate::StorageResult;

/// Explicit bound for one grouped source and its authenticated unchanged check.
pub const MAX_HISTORICAL_GROUP_DOCUMENTS: usize = 8192;

/// Exact document declaration emitted by the bounded offline planner.
/// Hashes use different domains and must never be substituted for each other.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistoricalDocumentInput {
    /// Unchanged original document record identity.
    pub legacy_doc_id: RecordId,
    /// Stable external identity for this complete historical group.
    pub external_source_id: String,
    /// Digest of the original row in this proof's declared hash domain.
    pub original_row_sha256: String,
    /// Original title-and-text content digest; distinct from request identity.
    pub doc_content_sha256: String,
    /// SHA-256 of the exact original UTF-8 text bytes.
    pub original_text_sha256: String,
    /// Byte length of the original UTF-8 text, not a character count.
    pub original_text_utf8_bytes: usize,
    /// SHA-256 of the original title bytes.
    pub title_sha256: String,
    /// Canonical original JSON security digest from the verified declaration.
    pub security_sha256: String,
    /// Canonical original JSON provenance digest from the verified declaration.
    pub provenance_sha256: String,
    /// Must be absent for initial historical adoption; caller-declared prior acceptance is refused.
    pub accepted_request_fingerprint: Option<String>,
    /// Must be absent for initial historical adoption; caller-declared prior acceptance is refused.
    pub accepted_revision: Option<String>,
}

/// One original document's immutable imported evidence.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistoricalDocumentProof {
    /// Original raw snapshot row, before source association was installed.
    pub original_row_sha256: String,
    /// Original association before the explicit grouped-source projection.
    pub original_source_id: Option<RecordId>,
    /// Actual typed document after explicit source association.
    pub document_fingerprint: String,
    /// Typed original revision support fingerprint.
    pub revision_fingerprint: String,
    /// SHA-256 of the exact original UTF-8 text bytes.
    pub original_text_sha256: String,
    /// Original title-and-text content digest; distinct from request identity.
    pub doc_content_sha256: String,
    /// Exact node rows; no synthetic extraction windows are introduced.
    pub node_fingerprints: BTreeMap<RecordId, String>,
}

/// Historical baseline retains uncertainty rather than inventing independent
/// edits or an extraction history. It is bound to an actual import commit.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistoricalUnknownOwnership {
    /// SHA-256 of the complete immutable source snapshot file.
    pub snapshot_sha256: String,
    /// Digest of the complete verified offline preservation plan.
    pub plan_id: String,
    /// Exact canonical adoption or retirement commit identifier, not a digest.
    pub commit_id: String,
}

/// A separate snapshot evidence anchor. It certifies retained graph bytes,
/// never a document extraction or original authorship. Empty legacy spans
/// remain ungrounded and cannot produce a text quote.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistoricalSnapshotGraphProof {
    /// Version of the serialized historical proof.
    pub version: u32,
    /// Original graph record digests keyed by their unchanged identities.
    pub original_rows_sha256: BTreeMap<RecordId, String>,
    /// Typed outgoing record references for each imported graph row.
    pub typed_references: BTreeMap<RecordId, BTreeSet<RecordId>>,
    /// Complete retained typed reference closure, including non-root rows.
    pub reference_fingerprints: BTreeMap<RecordId, String>,
}

/// Certificate for adoption of existing records. This is not model provenance.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistoricalAdoptionProof {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional snapshot evidence anchor for graph rows without document authorship.
    pub snapshot_graph: Option<HistoricalSnapshotGraphProof>,
    /// Version of the serialized historical proof.
    pub version: u32,
    /// Original namespace retained by the immutable snapshot.
    pub source_space_id: String,
    /// SHA-256 of the complete immutable source snapshot file.
    pub snapshot_sha256: String,
    /// SHA-256 binding the complete original identity mapping.
    pub idmap_sha256: String,
    /// Digest of the complete verified offline preservation plan.
    pub plan_id: String,
    /// Digest of the complete sorted original row-to-content mapping.
    pub document_mapping_sha256: String,
    /// SHA-256 of the ordered offline document declaration file.
    pub documents_sha256: String,
    /// SHA-256 of the ordered offline source-group declaration file.
    pub groups_sha256: String,
    /// Stable external identity for this complete historical group.
    pub external_source_id: String,
    /// Actual adoption timestamp; never presented as original extraction time.
    pub imported_at: DateTime<Utc>,
    /// Original per-document payload identity supports grouped source keys.
    pub documents: BTreeMap<RecordId, HistoricalDocumentProof>,
    /// Exact retained graph records covered by this adoption.
    pub graph_fingerprints: BTreeMap<RecordId, String>,
    /// Exact retained source before installing the explicit imported identity.
    pub original_sources: BTreeMap<RecordId, String>,
    /// This version never claims profile compatibility from model/dimensions.
    pub vector_state: HistoricalVectorState,
}

/// Preservation status of legacy vectors without inferred current-profile compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoricalVectorState {
    /// Legacy vectors remain preserved; current profile coverage is not proved.
    PreservedUnverified,
}

fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// HomeCore's title/text hash is separate from its replay request fingerprint.
/// Object keys are explicit and sorted; UTF-8 text is never normalized.
pub fn original_document_content_sha256(document: &Doc) -> StorageResult<String> {
    let input = BTreeMap::from([("text", &document.text), ("title", &document.title)]);
    serde_json::to_vec(&input)
        .map(|bytes| sha256(&bytes))
        .map_err(|_| integrity("historical document hash serialization failed"))
}

impl HistoricalDocumentInput {
    /// Raw policy/provenance may omit default fields. Their planner hash binds
    /// that original JSON; typed authorization binds the resulting domain row.
    pub fn validate_original_document(
        &self,
        document: &Doc,
        raw: &serde_json::Value,
    ) -> StorageResult<()> {
        self.validate_document(document)?;
        let decoded: Doc = serde_json::from_value(raw.clone())
            .map_err(|_| integrity("historical original document invalid"))?;
        if &decoded != document
            || self.security_sha256
                != canonical_sha256(raw.get("security").unwrap_or(&serde_json::Value::Null))?
            || self.provenance_sha256
                != canonical_sha256(raw.get("provenance").unwrap_or(&serde_json::Value::Null))?
        {
            return Err(integrity(
                "historical original policy or provenance changed",
            ));
        }
        Ok(())
    }

    /// Check exact original document identity and content; imported declarations cannot claim prior acceptance.
    pub fn validate_document(&self, document: &Doc) -> StorageResult<()> {
        if self.legacy_doc_id != document.base.id
            || self.legacy_doc_id.table() != TableName::Doc
            || self.external_source_id.is_empty()
            || self.accepted_request_fingerprint.is_some()
            || self.accepted_revision.is_some()
            || [
                &self.original_row_sha256,
                &self.doc_content_sha256,
                &self.original_text_sha256,
                &self.title_sha256,
                &self.security_sha256,
                &self.provenance_sha256,
            ]
            .iter()
            .any(|digest| !valid_digest(digest))
            || self.original_text_sha256 != sha256(document.text.as_bytes())
            || self.original_text_utf8_bytes != document.text.len()
            || self.title_sha256 != sha256(document.title.as_bytes())
            || self.doc_content_sha256 != original_document_content_sha256(document)?
        {
            return Err(integrity(
                "historical document declaration differs from original input",
            ));
        }
        Ok(())
    }
}

impl HistoricalAdoptionProof {
    pub(crate) fn validate_manifest(
        &self,
        manifest: &SourcePublicationManifest,
    ) -> StorageResult<()> {
        if self.version != 1
            || self.source_space_id.trim().is_empty()
            || self.external_source_id.is_empty()
            || [
                &self.snapshot_sha256,
                &self.idmap_sha256,
                &self.plan_id,
                &self.document_mapping_sha256,
                &self.documents_sha256,
                &self.groups_sha256,
            ]
            .iter()
            .any(|digest| !valid_digest(digest))
            || (self.documents.is_empty() != self.snapshot_graph.is_some())
            || self.documents.len() > MAX_HISTORICAL_GROUP_DOCUMENTS
            || manifest.mode != elephant_types::records::ingest::IngestMode::Graph
            || manifest.configuration.config_id.is_empty()
            || manifest.configuration.policy_id.is_empty()
            || manifest.embedding_profile.is_some()
            || !manifest.metadata_artifact_ids.is_empty()
            || !manifest.planning_artifact_ids.is_empty()
            || !manifest.bundles.is_empty()
            || !manifest.embeddings.is_empty()
            || !manifest.embedding_work_item_ids.is_empty()
            || manifest.documents.len() != self.documents.len()
        {
            return Err(integrity("invalid historical adoption manifest"));
        }
        self.validate_mapping_proof()?;
        let valid_fingerprint = |fingerprint: &str| {
            uuid::Uuid::parse_str(fingerprint).is_ok_and(|id| {
                id.get_version_num() == 5
                    && id.get_variant() == uuid::Variant::RFC4122
                    && id.to_string() == fingerprint
            })
        };
        for (id, encoded) in &self.original_sources {
            let source: elephant_types::records::Source = serde_json::from_str(encoded)
                .map_err(|_| integrity("historical original source proof invalid"))?;
            if &source.base.id != id
                || id.table() != TableName::Source
                || source.base.space_id != manifest.space_id
                || source.base.deleted_at.is_some()
            {
                return Err(integrity("historical original source proof differs"));
            }
        }
        if self.graph_fingerprints.iter().any(|(id, fingerprint)| {
            !crate::canonical_commit::is_derived_graph_table(id.table())
                || !valid_fingerprint(fingerprint)
        }) {
            return Err(integrity("historical graph proof invalid"));
        }
        let mut seen_documents = BTreeSet::new();
        let mut seen_revisions = BTreeSet::new();
        for mapping in &manifest.documents {
            let proof = self
                .documents
                .get(&mapping.doc_id)
                .ok_or_else(|| integrity("historical document proof missing"))?;
            if mapping.space_id != manifest.space_id
                || mapping.publication != manifest.publication
                || mapping.doc_id.table() != TableName::Doc
                || mapping.doc_revision_id.table() != TableName::DocRevision
                || !seen_documents.insert(&mapping.doc_id)
                || !seen_revisions.insert(&mapping.doc_revision_id)
                || [
                    &proof.original_row_sha256,
                    &proof.original_text_sha256,
                    &proof.doc_content_sha256,
                ]
                .iter()
                .any(|digest| !valid_digest(digest))
                || !valid_fingerprint(&proof.document_fingerprint)
                || proof
                    .original_source_id
                    .as_ref()
                    .is_some_and(|id| !self.original_sources.contains_key(id))
                || !valid_fingerprint(&proof.revision_fingerprint)
                || proof.node_fingerprints.iter().any(|(id, fingerprint)| {
                    id.table() != TableName::DocNode || !valid_fingerprint(fingerprint)
                })
            {
                return Err(integrity("invalid historical document proof"));
            }
        }
        Ok(())
    }

    fn validate_mapping_proof(&self) -> StorageResult<()> {
        let mapping = if let Some(snapshot) = &self.snapshot_graph {
            if snapshot.version != 1
                || snapshot.original_rows_sha256.is_empty()
                || snapshot.original_rows_sha256.len() > 256
                || snapshot
                    .original_rows_sha256
                    .keys()
                    .collect::<BTreeSet<_>>()
                    != self.graph_fingerprints.keys().collect()
                || snapshot.typed_references.len() > 8192
                || snapshot
                    .reference_fingerprints
                    .keys()
                    .collect::<BTreeSet<_>>()
                    != snapshot.typed_references.keys().collect()
                || self
                    .graph_fingerprints
                    .keys()
                    .any(|id| !snapshot.typed_references.contains_key(id))
                || snapshot
                    .typed_references
                    .values()
                    .flatten()
                    .any(|id| !snapshot.reference_fingerprints.contains_key(id))
                || snapshot
                    .reference_fingerprints
                    .values()
                    .any(|value| uuid::Uuid::parse_str(value).is_err())
                || snapshot
                    .original_rows_sha256
                    .values()
                    .any(|hash| !valid_digest(hash))
                || !self.original_sources.is_empty()
            {
                return Err(integrity("historical snapshot graph population invalid"));
            }
            document_mapping_sha256(
                snapshot
                    .original_rows_sha256
                    .iter()
                    .map(|(id, hash)| (id, hash.as_str())),
            )?
        } else {
            document_mapping_sha256(
                self.documents
                    .iter()
                    .map(|(id, proof)| (id, proof.original_row_sha256.as_str())),
            )?
        };
        if self.document_mapping_sha256 != mapping {
            return Err(integrity("historical group mapping digest changed"));
        }
        Ok(())
    }

    pub(crate) fn document_matches(
        &self,
        mapping: &SourceDocumentMapping,
        document: &Doc,
        revision: &DocRevision,
        accepted: &SourceRevision,
    ) -> StorageResult<bool> {
        let Some(proof) = self.documents.get(&mapping.doc_id) else {
            return Ok(false);
        };
        Ok(
            accepted.source.external_id.as_deref() == Some(self.external_source_id.as_str())
                && proof.document_fingerprint == source_support_fingerprint(document)?
                && proof.revision_fingerprint == source_support_fingerprint(revision)?
                && proof.original_text_sha256 == sha256(document.text.as_bytes())
                && proof.doc_content_sha256 == original_document_content_sha256(document)?
                && document.text == revision.text
                && revision
                    .text_hash
                    .strip_prefix("sha256:")
                    .unwrap_or(&revision.text_hash)
                    == proof.original_text_sha256,
        )
    }

    pub(crate) fn node_matches(
        &self,
        mapping: &SourceDocumentMapping,
        node: &DocNode,
    ) -> StorageResult<bool> {
        Ok(node.base.space_id == mapping.space_id
            && node.revision.id() == &mapping.doc_revision_id
            && self
                .documents
                .get(&mapping.doc_id)
                .and_then(|proof| proof.node_fingerprints.get(&node.base.id))
                == Some(&node_input_fingerprint(node)?))
    }

    pub(crate) fn commit_id(&self, manifest: &SourcePublicationManifest) -> String {
        format!(
            "historical-adoption:{}",
            stable_key(&[
                &manifest.space_id,
                &manifest.publication.source_id.to_string(),
                &manifest.publication.generation,
                &self.plan_id,
            ])
        )
    }
}

/// Stable original node input, excluding only the two storage-maintained index
/// markers already excluded by normal source embedding input validation.
pub(crate) fn node_input_fingerprint(node: &DocNode) -> StorageResult<String> {
    let mut original = node.clone();
    original.embedding_model = None;
    source_support_fingerprint(&original)
}

/// Hash the sorted original identity and content-hash pairs as newline-delimited JSON arrays.
pub fn document_mapping_sha256<'a>(
    rows: impl IntoIterator<Item = (&'a RecordId, &'a str)>,
) -> StorageResult<String> {
    let rows: BTreeMap<_, _> = rows
        .into_iter()
        .map(|(id, hash)| (id.to_string(), hash))
        .collect();
    let mut digest = Sha256::new();
    for (id, hash) in rows {
        digest.update(
            serde_json::to_vec(&[id.as_str(), hash])
                .map_err(|_| integrity("historical group hash failed"))?,
        );
        digest.update(b"\n");
    }
    Ok(hex::encode(digest.finalize()))
}

/// Actual offline commit provenance. Never an old worker/model completion.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistoricalCommitProvenance {
    /// Version of the serialized historical proof.
    pub version: u32,
    /// Original snapshot namespace before the declared relocation.
    pub source_space_id: String,
    /// Explicit destination namespace for the imported population.
    pub target_space_id: String,
    /// SHA-256 of the complete immutable source snapshot file.
    pub snapshot_sha256: String,
    /// SHA-256 binding the complete original identity mapping.
    pub idmap_sha256: String,
    /// Digest of the complete verified offline preservation plan.
    pub plan_id: String,
    /// SHA-256 of the ordered offline document declaration file.
    pub documents_sha256: String,
    /// SHA-256 of the ordered offline source-group declaration file.
    pub groups_sha256: String,
    /// Digest of the immutable historical publication manifest.
    pub manifest_digest: String,
}

impl HistoricalCommitProvenance {
    pub(crate) fn for_manifest(manifest: &SourcePublicationManifest) -> StorageResult<Self> {
        let proof = manifest
            .historical_adoption
            .as_ref()
            .ok_or_else(|| integrity("historical proof missing"))?;
        Ok(Self {
            version: 1,
            source_space_id: proof.source_space_id.clone(),
            target_space_id: manifest.space_id.clone(),
            snapshot_sha256: proof.snapshot_sha256.clone(),
            idmap_sha256: proof.idmap_sha256.clone(),
            plan_id: proof.plan_id.clone(),
            documents_sha256: proof.documents_sha256.clone(),
            groups_sha256: proof.groups_sha256.clone(),
            manifest_digest: manifest.digest()?,
        })
    }
}

#[path = "historical_adoption_links.rs"]
mod links;
pub use links::{historical_evidence_links, historical_snapshot_links};

#[path = "historical_adoption_noop.rs"]
mod noop;
#[path = "historical_adoption_transaction.rs"]
mod transaction;
pub use noop::{HistoricalNoopReceipt, HistoricalNoopRequest};
#[cfg(test)]
#[path = "historical_adoption_backend_tests.rs"]
mod backend_tests;
#[cfg(test)]
#[path = "historical_adoption_tests.rs"]
mod tests;
pub use transaction::{
    HistoricalAdoptionReceipt, HistoricalAdoptionRepository, HistoricalCommittedGroup,
    HistoricalGroupRows, HistoricalOriginalDocument, HistoricalRowEffect,
    PreparedHistoricalAuthority,
};

/// Closed identity inventory, validated before opening the destination.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistoricalAdoptionInput {
    /// Explicit source namespace; never inferred from target configuration.
    pub source_space_id: String,
    /// Explicit target namespace.
    pub space_id: String,
    /// SHA-256 of the complete immutable source snapshot file.
    pub snapshot_sha256: String,
    /// SHA-256 binding the complete original identity mapping.
    pub idmap_sha256: String,
    /// SHA-256 of the ordered offline document declaration file.
    pub documents_sha256: String,
    /// SHA-256 of the ordered offline source-group declaration file.
    pub groups_sha256: String,
    /// Digest of the complete verified offline preservation plan.
    pub plan_id: String,
    /// Complete ordered source groups declared for adoption.
    pub groups: Vec<HistoricalSourceGroup>,
    /// Exact original source-less graph rows, partitioned before target writes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub snapshot_groups: Vec<HistoricalSnapshotGraphGroup>,
}

/// Bounded source-less graph group identified by complete original row hashes.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistoricalSnapshotGraphGroup {
    /// Stable external identity for this complete historical group.
    pub external_source_id: String,
    /// Digest of the complete sorted original row-to-content mapping.
    pub document_mapping_sha256: String,
    /// SHA256 of canonical original JSON, distinct from lexical spool hashes.
    pub original_rows_sha256: BTreeMap<RecordId, String>,
}
impl HistoricalSnapshotGraphGroup {
    /// Derive and validate a bounded snapshot group from complete original row hashes.
    pub fn from_rows(rows: BTreeMap<RecordId, String>) -> StorageResult<Self> {
        let mapping = document_mapping_sha256(rows.iter().map(|(id, hash)| (id, hash.as_str())))?;
        let group = Self {
            external_source_id: format!("historical-snapshot-graph:{mapping}"),
            document_mapping_sha256: mapping,
            original_rows_sha256: rows,
        };
        group.validate()?;
        Ok(group)
    }
    fn validate(&self) -> StorageResult<()> {
        if self.original_rows_sha256.is_empty()
            || self.original_rows_sha256.len() > 256
            || self.original_rows_sha256.iter().any(|(id, hash)| {
                !crate::canonical_commit::is_derived_graph_table(id.table()) || !valid_digest(hash)
            })
            || self.document_mapping_sha256
                != document_mapping_sha256(
                    self.original_rows_sha256
                        .iter()
                        .map(|(id, hash)| (id, hash.as_str())),
                )?
            || self.external_source_id
                != format!("historical-snapshot-graph:{}", self.document_mapping_sha256)
        {
            return Err(integrity("historical snapshot group declaration invalid"));
        }
        Ok(())
    }
}

/// Complete original document declarations sharing one stable external source key.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistoricalSourceGroup {
    /// Stable external identity for this complete historical group.
    pub external_source_id: String,
    /// Digest of the complete sorted original row-to-content mapping.
    pub document_mapping_sha256: String,
    /// Complete original document population for this source group.
    pub documents: Vec<HistoricalDocumentInput>,
}

impl HistoricalAdoptionInput {
    /// Validate a nonempty initial adoption population and all bound input digests.
    pub fn validate(&self) -> StorageResult<()> {
        self.validate_population(false)
    }

    fn validate_population(&self, allow_empty: bool) -> StorageResult<()> {
        if self.space_id.trim().is_empty()
            || self.source_space_id.trim().is_empty()
            || (!allow_empty && self.groups.is_empty() && self.snapshot_groups.is_empty())
            || [
                &self.snapshot_sha256,
                &self.idmap_sha256,
                &self.documents_sha256,
                &self.groups_sha256,
                &self.plan_id,
            ]
            .iter()
            .any(|v| !valid_digest(v))
        {
            return Err(integrity("historical adoption input is incomplete"));
        }
        let mut identities = BTreeSet::new();
        let mut docs = BTreeSet::new();
        for group in &self.groups {
            if group.external_source_id.is_empty()
                || !identities.insert(&group.external_source_id)
                || group.documents.is_empty()
                || group.document_mapping_sha256
                    != document_mapping_sha256(
                        group
                            .documents
                            .iter()
                            .map(|d| (&d.legacy_doc_id, d.original_row_sha256.as_str())),
                    )?
            {
                return Err(integrity(
                    "historical source group is incomplete or duplicated",
                ));
            }
            for doc in &group.documents {
                if doc.external_source_id != group.external_source_id
                    || !docs.insert(&doc.legacy_doc_id)
                {
                    return Err(integrity(
                        "historical document belongs to multiple source groups",
                    ));
                }
            }
        }
        let mut snapshot_rows = BTreeSet::new();
        if self.snapshot_groups.len() > 100_000 {
            return Err(integrity("historical snapshot group count exceeds bound"));
        }
        for group in &self.snapshot_groups {
            group.validate()?;
            if !identities.insert(&group.external_source_id)
                || group
                    .original_rows_sha256
                    .keys()
                    .any(|id| !snapshot_rows.insert(id))
            {
                return Err(integrity("historical snapshot groups overlap"));
            }
        }
        let mut ordered_groups: Vec<_> = self.groups.iter().collect();
        ordered_groups
            .sort_by(|left, right| left.external_source_id.cmp(&right.external_source_id));
        let mut ordered_documents = Vec::new();
        for group in &ordered_groups {
            let mut documents: Vec<_> = group.documents.iter().collect();
            documents.sort_by_key(|doc| doc.legacy_doc_id.to_string());
            ordered_documents.extend(documents);
        }
        if self.documents_sha256 != jsonl_sha256(ordered_documents)?
            || self.groups_sha256 != jsonl_sha256(ordered_groups.iter().map(|group| serde_json::json!({"external_source_id":group.external_source_id,"documents":group.documents.len(),"document_mapping_sha256":group.document_mapping_sha256})))? {
            return Err(integrity("historical aggregate document/group digest changed"));
        }
        Ok(())
    }
}

fn canonical_sha256<T: Serialize>(value: &T) -> StorageResult<String> {
    let value = serde_json::to_value(value)
        .map_err(|_| integrity("historical canonical encoding failed"))?;
    serde_json::to_vec(&crate::canonical_commit::canonical_json(value))
        .map(|bytes| sha256(&bytes))
        .map_err(|_| integrity("historical canonical encoding failed"))
}

fn jsonl_sha256<T: Serialize>(values: impl IntoIterator<Item = T>) -> StorageResult<String> {
    let mut hash = Sha256::new();
    for value in values {
        let value = serde_json::to_value(value)
            .map_err(|_| integrity("historical inventory encoding failed"))?;
        hash.update(
            serde_json::to_vec(&crate::canonical_commit::canonical_json(value))
                .map_err(|_| integrity("historical inventory encoding failed"))?,
        );
        hash.update(b"\n");
    }
    Ok(hex::encode(hash.finalize()))
}

/// Immutable S0 catch-up source binding. JSON matches HistoricalCatchupSource;
/// this type requires only the initial transfer, not the later delta executor.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoricalInitialCatchupSource {
    /// Canonical source identity for the historical publication.
    pub source_id: RecordId,
    /// Exact canonical adoption or retirement commit identifier, not a digest.
    pub commit_id: String,
    /// SHA-256 of the complete typed original canonical commit receipt.
    pub receipt_sha256: String,
    /// Complete original source head bound to the adoption receipt.
    pub head: elephant_types::publication::SourceHead,
}
impl HistoricalAdoptionRepository {
    /// Capture only under the existing initial exclusive transfer lease.
    /// This is evidence for a later explicit catch-up, never mutation authority.
    pub async fn initial_catchup_binding(
        &self,
        original: &HistoricalAdoptionReceipt,
    ) -> StorageResult<HistoricalInitialCatchupSource> {
        self.client().require_transfer_custody()?;
        self.client().verify_journal_identity().await?;
        self.initial_catchup_binding_verified(original).await
    }

    // Only the standalone strong check or the bounded all-or-error batch may
    // call this helper. It never grants custody or caches archive verification.
    async fn initial_catchup_binding_verified(
        &self,
        original: &HistoricalAdoptionReceipt,
    ) -> StorageResult<HistoricalInitialCatchupSource> {
        let receipt = crate::canonical_commit::CanonicalRepository::new(self.client().clone())
            .receipt_read_only(&original.space_id, &original.commit_id)
            .await?
            .ok_or_else(|| integrity("initial catch-up receipt missing"))?;
        let manifest_fingerprint_matches = receipt.plan_fingerprint == original.manifest_digest;
        if receipt.space_id != original.space_id
            || receipt.commit_id != original.commit_id
            || !manifest_fingerprint_matches
            || receipt
                .historical_import
                .as_ref()
                .is_none_or(|proof| proof.snapshot_sha256 != original.original_snapshot_sha256)
            || receipt.replay_output.as_ref()
                != Some(&serde_json::to_value(original).map_err(|e| integrity(&e.to_string()))?)
        {
            return Err(integrity("initial catch-up receipt differs"));
        }
        let head =
            crate::source_publication::SourcePublicationRepository::new(self.client().clone())
                .head(&original.space_id, &original.source_id)
                .await?
                .ok_or_else(|| integrity("initial catch-up head missing"))?;
        if head.space_id != original.space_id
            || head.source_id != original.source_id
            || head.generation != original.operation.source_generation
            || head.operation_id != original.operation.operation_id
            || head.desired_revision.as_deref() != Some(original.accepted_revision.as_str())
            || head.published_revision != head.desired_revision
            || head.lifecycle != elephant_types::publication::SourceVisibility::Active
        {
            return Err(integrity("initial catch-up head differs"));
        }
        Ok(HistoricalInitialCatchupSource {
            source_id: original.source_id.clone(),
            commit_id: original.commit_id.clone(),
            receipt_sha256: hex::encode(Sha256::digest(
                serde_json::to_vec(&receipt).map_err(|e| integrity(&e.to_string()))?,
            )),
            head,
        })
    }
}

#[path = "historical_adoption_catchup.rs"]
pub(crate) mod catchup;
pub use catchup::PreparedHistoricalEffectIndex;

pub use transaction::{
    HistoricalGraphReplacement, HistoricalRegenerationPlan, HistoricalRegenerationProgress,
    HistoricalRegenerationStep, HistoricalRetirementReceipt, HistoricalS1SourceClosure,
};

pub use transaction::HistoricalSnapshotGraphRow;

#[path = "historical_initial_bindings.rs"]
mod initial_bindings;
pub use initial_bindings::{MAX_INITIAL_BINDING_BATCH_BYTES, MAX_INITIAL_BINDING_BATCH_RECEIPTS};
