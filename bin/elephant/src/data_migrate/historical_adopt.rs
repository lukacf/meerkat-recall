//! Offline source-first adoption from the already verified original row spool.
//! This hook runs under the caller's original transfer custody, never after ready.

use super::source_spool::{file_sha, readonly, SourceSpool};
use anyhow::{ensure, Context};
use elephant_storage::canonical_commit::CanonicalRecord;
use elephant_storage::historical_adoption::{
    document_mapping_sha256, historical_evidence_links, HistoricalAdoptionInput,
    HistoricalAdoptionRepository, HistoricalDocumentInput, HistoricalGroupRows,
    HistoricalOriginalDocument, HistoricalRowEffect, HistoricalSourceGroup,
    MAX_HISTORICAL_GROUP_DOCUMENTS, MAX_INITIAL_BINDING_BATCH_BYTES,
    MAX_INITIAL_BINDING_BATCH_RECEIPTS,
};
use elephant_storage::transfer_batch::{strict_json, MAX_ROW_BYTES};
use elephant_storage::StorageClient;
use elephant_types::records::base::{BaseFields, Security, SubjectsState};
use elephant_types::records::{Doc, DocNode, DocRevision, Source, SpaceConfig};
use elephant_types::{RecordId, TableName};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{BufRead, Read, Write};
use std::path::{Path, PathBuf};

#[path = "historical_snapshot_plan.rs"]
mod snapshot_plan;

const MAX_PLAN_BYTES: u64 = 1024 * 1024;
const MAX_GROUP_BYTES: usize = 128 * 1024 * 1024;
const MAX_GROUP_GRAPH: usize = 20_000;
const GRAPH: &[&str] = &[
    "entity",
    "entity_identity_key",
    "assertion",
    "rel",
    "attribute",
    "event",
    "freshness",
    "truth_slot",
    "conflict_group",
];

pub(super) struct PreparedAdoption {
    spool_path: PathBuf,
    spool_sha256: String,
    index_path: PathBuf,
    index_sha256: String,
    input: HistoricalAdoptionInput,
    config: SpaceConfig,
    config_input_sha256: String,
}

#[derive(Debug, Serialize)]
pub(super) struct AdoptionReport {
    pub original_snapshot_sha256: String,
    pub source_space_id: String,
    pub target_space_id: String,
    pub plan_id: String,
    pub configuration_input_sha256: String,
    pub source_input_sha256: String,
    pub receipts_sha256: String,
    pub groups: usize,
    pub graph_staged_without_provable_membership: BTreeMap<String, u64>,
    pub state: &'static str,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SpaceRequest {
    name: String,
    security: Security,
    embedding_provider: String,
    embedding_model: String,
    embedding_dims: i32,
    processing: elephant_types::provider_binding::SpaceProcessingConfig,
    purpose: Option<String>,
    skill: Option<String>,
}

fn read_small_raw(path: &Path) -> anyhow::Result<String> {
    let file = std::fs::File::open(path)?;
    ensure!(
        file.metadata()?.len() <= MAX_PLAN_BYTES,
        "adoption control exceeds bound"
    );
    let mut raw = String::new();
    file.take(MAX_PLAN_BYTES + 1).read_to_string(&mut raw)?;
    ensure!(
        raw.len() as u64 <= MAX_PLAN_BYTES,
        "adoption control exceeds bound"
    );
    Ok(raw)
}
fn read_small(path: &Path) -> anyhow::Result<serde_json::Value> {
    Ok(strict_json(&read_small_raw(path)?)?)
}

/// Hash the producer's original compact values without parsing floats back into
/// a different serializer domain. Strict parsing checks every nested object for
/// duplicate keys before the borrowed top-level values are selected.
fn producer_plan_digest(raw: &str) -> anyhow::Result<String> {
    strict_json(raw)?;
    let mut fields: BTreeMap<String, &serde_json::value::RawValue> = serde_json::from_str(raw)?;
    fields.remove("plan_id").context("plan identity missing")?;
    fields
        .remove("inventory_sha256")
        .context("inventory identity missing")?;
    let mut digest = Sha256::new();
    digest.update(b"{");
    for (index, (key, value)) in fields.iter().enumerate() {
        ensure!(key.is_ascii(), "unsupported producer plan field name");
        if index != 0 {
            digest.update(b",");
        }
        digest.update(serde_json::to_vec(key)?);
        digest.update(b":");
        digest.update(value.get().as_bytes());
    }
    digest.update(b"}");
    Ok(hex::encode(digest.finalize()))
}
fn original(db: &Connection, table: &str, id: &str) -> anyhow::Result<serde_json::Value> {
    let (size, bytes, hash): (usize, Vec<u8>, String) = db.query_row(
        "SELECT length(lexical),lexical,lexical_sha256 FROM raw_rows WHERE t=? AND k=? AND length(lexical)<=?",
        params![table, id, MAX_ROW_BYTES], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;
    ensure!(
        size == bytes.len() && hex::encode(Sha256::digest(&bytes)) == hash,
        "original lexical row changed"
    );
    Ok(strict_json(std::str::from_utf8(&bytes)?)?)
}

/// Validate the complete adoption plan before opening the destination. The
/// index contains only links and identities; original payload stays in its
/// immutable lexical spool. No model or target database is contacted.
pub(super) fn prepare(
    spool: &SourceSpool,
    plan_path: &Path,
    config_path: &Path,
    index_path: &Path,
) -> anyhow::Result<PreparedAdoption> {
    prepare_mode(spool, plan_path, config_path, index_path, false)
}
pub(super) fn prepare_catchup(
    spool: &SourceSpool,
    plan_path: &Path,
    config_path: &Path,
    index_path: &Path,
) -> anyhow::Result<PreparedAdoption> {
    prepare_mode(spool, plan_path, config_path, index_path, true)
}
fn prepare_mode(
    spool: &SourceSpool,
    plan_path: &Path,
    config_path: &Path,
    index_path: &Path,
    catchup: bool,
) -> anyhow::Result<PreparedAdoption> {
    spool.verify_unchanged()?;
    let original_plan = read_small_raw(plan_path)?;
    let plan = strict_json(&original_plan)?;
    let actual_plan = producer_plan_digest(&original_plan)?;
    ensure!(
        actual_plan == spool.plan_id && plan["snapshot_sha256"] == spool.source_sha256,
        "adoption plan digest differs"
    );
    ensure!(
        (spool.spaces.contains(&spool.space_id) || (catchup && spool.spaces.is_empty()))
            && plan["source_space_id"].as_str() == Some(spool.space_id.as_str()),
        "adoption requires the explicitly declared original scope"
    );
    ensure!(
        spool.tables.get("space_config").copied().unwrap_or(0) == 0,
        "explicit bootstrap configuration requires original space_config=0"
    );
    let envelope = read_small(config_path)?;
    ensure!(
        envelope["method"] == "POST" && envelope["path"] == "/v1/spaces",
        "configuration must be original space-create request"
    );
    let requested: SpaceRequest = serde_json::from_value(envelope["body"].clone())?;
    ensure!(
        plan["target_space_id"] == requested.name
            && envelope["headers"]["X-Space-Id"] == requested.name,
        "configuration space differs"
    );
    requested.processing.validate()?;
    let mut base = BaseFields::new(
        RecordId::new(
            TableName::SpaceConfig,
            uuid::Uuid::new_v5(
                &uuid::Uuid::NAMESPACE_OID,
                format!("historical-space:{}:{}", spool.plan_id, requested.name).as_bytes(),
            ),
        ),
        requested.name.clone(),
        "historical-adoption-v1".into(),
    );
    base.security = requested.security;
    base.subjects_state = SubjectsState::Complete;
    let config = SpaceConfig {
        base,
        embedding_provider: requested.embedding_provider,
        embedding_model: requested.embedding_model,
        embedding_dims: requested.embedding_dims,
        processing: Some(requested.processing),
        purpose: requested.purpose,
        skill: requested.skill,
    };
    let db = readonly(&spool.path)?;
    let mut declarations = db.prepare("SELECT payload FROM documents ORDER BY external,doc")?;
    let mut grouped = BTreeMap::<String, Vec<HistoricalDocumentInput>>::new();
    for payload in declarations.query_map([], |r| r.get::<_, String>(0))? {
        let document: HistoricalDocumentInput = serde_json::from_value(strict_json(&payload?)?)?;
        let group = grouped
            .entry(document.external_source_id.clone())
            .or_default();
        ensure!(
            group.len() < MAX_HISTORICAL_GROUP_DOCUMENTS,
            "historical group document bound exceeded"
        );
        group.push(document);
    }
    let groups = grouped
        .into_iter()
        .map(|(external_source_id, documents)| {
            Ok(HistoricalSourceGroup {
                document_mapping_sha256: document_mapping_sha256(
                    documents
                        .iter()
                        .map(|d| (&d.legacy_doc_id, d.original_row_sha256.as_str())),
                )?,
                external_source_id,
                documents,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut input = HistoricalAdoptionInput {
        source_space_id: plan["source_space_id"]
            .as_str()
            .context("explicit source space")?
            .into(),
        space_id: config.base.space_id.clone(),
        snapshot_sha256: spool.source_sha256.clone(),
        idmap_sha256: plan["idmap_sha256"]
            .as_str()
            .context("idmap digest")?
            .into(),
        documents_sha256: plan["documents_sha256"]
            .as_str()
            .context("documents digest")?
            .into(),
        groups_sha256: plan["groups_sha256"]
            .as_str()
            .context("groups digest")?
            .into(),
        plan_id: spool.plan_id.clone(),
        groups,
        snapshot_groups: Vec::new(),
    };
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(index_path)?.sync_all()?;
    let index = Connection::open(index_path)?;
    index.execute_batch("PRAGMA cache_size=-8192; PRAGMA synchronous=FULL; CREATE TABLE links(t TEXT,k TEXT,rt TEXT,rk TEXT,PRIMARY KEY(t,k,rt,rk)); CREATE INDEX links_reverse ON links(rt,rk); CREATE TABLE memberships(t TEXT,k TEXT,external TEXT,PRIMARY KEY(t,k,external)); CREATE INDEX memberships_external ON memberships(external,t,k); CREATE TABLE graph(t TEXT,k TEXT,PRIMARY KEY(t,k));")?;
    build_index(spool, &index, &input)?;
    input.snapshot_groups =
        snapshot_plan::partition(&db, &index, &input.source_space_id, &input.space_id)?;
    validate_population_for_mode(&input, catchup)?;
    ensure!(
        serde_json::to_vec(&input)?.len() <= 64 * 1024 * 1024,
        "complete adoption declaration byte bound"
    );
    // Preflight the same exact bounded groups used by the writer. This refuses
    // unsupported shapes before any destination bytes are created.
    for group in &input.groups {
        let _ = group_rows(&db, &index, group, &input.source_space_id, &input.space_id)?;
    }
    drop(index);
    spool.verify_unchanged()?;
    Ok(PreparedAdoption {
        spool_path: spool.path.clone(),
        spool_sha256: spool.inventory_sha256.clone(),
        index_path: index_path.into(),
        index_sha256: file_sha(index_path)?,
        input,
        config,
        config_input_sha256: file_sha(config_path)?,
    })
}

fn validate_population_for_mode(
    input: &HistoricalAdoptionInput,
    catchup: bool,
) -> anyhow::Result<()> {
    if catchup {
        elephant_storage::historical_adoption::HistoricalRegenerationPlan::derive(input, &[])?;
    } else {
        input.validate()?;
    }
    Ok(())
}

fn build_index(
    spool: &SourceSpool,
    db: &Connection,
    input: &HistoricalAdoptionInput,
) -> anyhow::Result<()> {
    db.execute_batch("BEGIN IMMEDIATE;")?;
    for group in &input.groups {
        for doc in &group.documents {
            db.execute(
                "INSERT INTO memberships VALUES ('doc',?,?)",
                params![doc.legacy_doc_id.to_string(), group.external_source_id],
            )?;
        }
    }
    db.execute_batch("COMMIT;")?;
    for table in ["source", "doc", "doc_revision", "doc_node"]
        .into_iter()
        .chain(GRAPH.iter().copied())
    {
        let mut cursor = None;
        loop {
            let page = spool.page(table, cursor.as_deref())?;
            if page.is_empty() {
                break;
            }
            db.execute_batch("BEGIN IMMEDIATE;")?;
            for row in &page {
                let value = strict_json(&row.lexical)?;
                if value["space_id"].as_str() != Some(input.source_space_id.as_str())
                    || value["deleted_at"] != serde_json::Value::Null
                {
                    continue;
                }
                if GRAPH.contains(&table) {
                    db.execute("INSERT INTO graph VALUES (?,?)", params![table, row.id])?;
                }
                let record: CanonicalRecord = serde_json::from_value(
                    serde_json::json!({"table":table,"record":elephant_storage::transfer_batch::historical_record_projection(table, &value)?}),
                )?;
                ensure!(
                    record.base().space_id == input.source_space_id,
                    "historical graph has foreign space"
                );
                let refs = historical_evidence_links(&record)?;
                for id in refs {
                    db.execute(
                        "INSERT OR IGNORE INTO links VALUES (?,?,?,?)",
                        params![table, row.id, id.table().as_str(), id.to_string()],
                    )?;
                }
            }
            db.execute_batch("COMMIT;")?;
            cursor = page.last().map(|r| r.id.clone());
        }
    }
    // Source membership follows its documents, then immutable evidence. Graph
    // propagation follows dependency edges only; entities are handled last so
    // two facts about one entity do not acquire each other's source identity.
    db.execute("INSERT OR IGNORE INTO memberships SELECT 'source',l.rk,m.external FROM links l JOIN memberships m ON m.t='doc' AND m.k=l.k WHERE l.t='doc' AND l.rt='source'",[])?;
    // Fixed point over typed non-entity dependencies handles truth/conflict
    // cycles. Entities never spread unrelated facts into each other's source.
    loop {
        let changed = db.execute("INSERT OR IGNORE INTO memberships SELECT l.t,l.k,m.external FROM links l JOIN memberships m ON m.t=l.rt AND m.k=l.rk WHERE l.t NOT IN ('entity','source','doc') AND l.rt!='entity'", [])?;
        if changed == 0 {
            break;
        }
        let too_large: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM memberships GROUP BY external HAVING count(*) > ?)",
            [MAX_GROUP_GRAPH + MAX_HISTORICAL_GROUP_DOCUMENTS],
            |r| r.get(0),
        )?;
        ensure!(
            !too_large,
            "historical dependency closure exceeds group bound"
        );
    }
    db.execute("INSERT OR IGNORE INTO memberships SELECT l.t,l.k,m.external FROM links l JOIN memberships m ON m.t=l.rt AND m.k=l.rk WHERE l.t='entity' AND l.rt='source'",[])?;
    db.execute("INSERT OR IGNORE INTO memberships SELECT 'entity',l.rk,m.external FROM links l JOIN memberships m ON m.t=l.t AND m.k=l.k WHERE l.rt='entity' AND l.t IN ('assertion','rel','attribute','event','entity_identity_key')",[])?;
    Ok(())
}

fn member_ids(db: &Connection, table: &str, external: &str) -> anyhow::Result<Vec<String>> {
    let mut query = db.prepare("SELECT k FROM memberships WHERE t=? AND external=? ORDER BY k")?;
    let rows = query
        .query_map(params![table, external], |r| r.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    ensure!(
        rows.len() <= MAX_GROUP_GRAPH,
        "historical group row bound exceeded"
    );
    Ok(rows)
}
fn group_rows(
    db: &Connection,
    index: &Connection,
    group: &HistoricalSourceGroup,
    source_space: &str,
    target_space: &str,
) -> anyhow::Result<HistoricalGroupRows> {
    let mut bytes = 0usize;
    let mut documents = Vec::new();
    let mut source_ids = BTreeSet::new();
    for declared in &group.documents {
        let raw = original(db, "doc", &declared.legacy_doc_id.to_string())?;
        bytes = bytes
            .checked_add(serde_json::to_vec(&raw)?.len())
            .context("group size overflow")?;
        let mut document: Doc = serde_json::from_value(raw.clone())?;
        declared.validate_original_document(&document, &raw)?;
        let original_hash: String = db.query_row(
            "SELECT h FROM rows WHERE t='doc' AND k=?",
            [declared.legacy_doc_id.to_string()],
            |r| r.get(0),
        )?;
        ensure!(
            original_hash == declared.original_row_sha256,
            "document declaration differs from original inventory row"
        );
        ensure!(
            document.base.space_id == source_space,
            "original document source space differs"
        );
        document.base.space_id = target_space.into();
        if let Some(source) = &document.source {
            source_ids.insert(source.id().clone());
        }
        let mut stmt = index.prepare(
            "SELECT k FROM links WHERE t='doc_revision' AND rt='doc' AND rk=? ORDER BY k",
        )?;
        let revisions = stmt
            .query_map([declared.legacy_doc_id.to_string()], |r| {
                r.get::<_, String>(0)
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let mut active = Vec::new();
        for id in revisions {
            let revision_raw = original(db, "doc_revision", &id)?;
            bytes = bytes
                .checked_add(serde_json::to_vec(&revision_raw)?.len())
                .context("group size overflow")?;
            ensure!(
                bytes <= MAX_GROUP_BYTES,
                "historical revision group bound exceeded"
            );
            let mut revision: DocRevision = serde_json::from_value(revision_raw)?;
            ensure!(
                revision.base.space_id == source_space,
                "original revision source space differs"
            );
            revision.base.space_id = target_space.into();
            if revision.status == elephant_types::records::doc_revision::RevisionStatus::Active {
                active.push(revision);
            }
        }
        ensure!(
            active.len() == 1,
            "original document must have one active immutable revision"
        );
        let revision = active.pop().context("active revision")?;
        let mut stmt = index.prepare(
            "SELECT k FROM links WHERE t='doc_node' AND rt='doc_revision' AND rk=? ORDER BY k",
        )?;
        let ids = stmt
            .query_map([revision.base.id.to_string()], |r| r.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        ensure!(
            ids.len() <= MAX_GROUP_GRAPH,
            "original node population exceeds group bound"
        );
        let mut nodes = Vec::new();
        for id in ids {
            let raw = original(db, "doc_node", &id)?;
            bytes = bytes
                .checked_add(serde_json::to_vec(&raw)?.len())
                .context("group size overflow")?;
            ensure!(
                bytes <= MAX_GROUP_BYTES,
                "historical node group bound exceeded"
            );
            let mut node: DocNode = serde_json::from_value(raw)?;
            ensure!(
                node.base.space_id == source_space,
                "original node source space differs"
            );
            node.base.space_id = target_space.into();
            nodes.push(node);
        }
        ensure!(
            bytes <= MAX_GROUP_BYTES,
            "historical group payload bound exceeded"
        );
        documents.push(HistoricalOriginalDocument {
            document,
            original_document: raw,
            revision,
            nodes,
        });
    }
    let original_sources = source_ids
        .iter()
        .map(|id| {
            let mut source: Source =
                serde_json::from_value(original(db, "source", &id.to_string())?)?;
            ensure!(
                source.base.space_id == source_space,
                "original source space differs"
            );
            source.base.space_id = target_space.into();
            Ok(source)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let first = documents
        .first()
        .context("historical group has no documents")?;
    for row in &documents {
        ensure!(
            row.document.base.deleted_at.is_none()
                && row.document.base.security == first.document.base.security
                && row.document.base.subjects == first.document.base.subjects
                && row.document.base.subjects_state == first.document.base.subjects_state,
            "historical group has differing policy, subjects, completeness or lifecycle"
        );
        ensure!(
            row.revision.base.deleted_at.is_none()
                && row.revision.doc.id() == &row.document.base.id
                && row.revision.text == row.document.text
                && row
                    .revision
                    .text_hash
                    .strip_prefix("sha256:")
                    .unwrap_or(&row.revision.text_hash)
                    == hex::encode(Sha256::digest(row.document.text.as_bytes())),
            "historical original revision lineage or text differs"
        );
        for node in &row.nodes {
            ensure!(
                node.base.deleted_at.is_none()
                    && node.revision.id() == &row.revision.base.id
                    && node.span.start < node.span.end
                    && row
                        .revision
                        .text
                        .get(node.span.start as usize..node.span.end as usize)
                        .is_some(),
                "historical original node span or lineage differs"
            );
        }
    }
    ensure!(
        original_sources
            .iter()
            .all(|source| source.base.deleted_at.is_none()),
        "historical original source is deleted"
    );
    let mut graph = Vec::new();
    for table in GRAPH {
        for id in member_ids(index, table, &group.external_source_id)? {
            let mut raw = original(db, table, &id)?;
            ensure!(
                raw["space_id"] == source_space,
                "original graph source space differs"
            );
            raw["space_id"] = target_space.into();
            bytes = bytes
                .checked_add(serde_json::to_vec(&raw)?.len())
                .context("group size overflow")?;
            ensure!(
                bytes <= MAX_GROUP_BYTES && graph.len() < MAX_GROUP_GRAPH,
                "historical graph group bound exceeded"
            );
            graph.push(serde_json::from_value::<CanonicalRecord>(
                serde_json::json!({"table":table,"record":elephant_storage::transfer_batch::historical_record_projection(table, &raw)?}),
            )?);
        }
    }
    Ok(HistoricalGroupRows {
        documents,
        original_sources,
        graph,
    })
}

impl PreparedAdoption {
    /// Bootstrap before loaded content makes ordinary active-profile creation
    /// inadmissible. The explicit target configuration is never inferred.
    pub async fn bootstrap(&self, dst: &StorageClient) -> anyhow::Result<Vec<HistoricalRowEffect>> {
        Ok(HistoricalAdoptionRepository::new(dst.clone())
            .bootstrap_configuration(&self.input.snapshot_sha256, self.config.clone())
            .await?)
    }
    pub async fn adopt<F>(
        self,
        dst: &StorageClient,
        receipts_path: &Path,
        mut reconcile_effects: F,
    ) -> anyhow::Result<AdoptionReport>
    where
        F: FnMut(&[HistoricalRowEffect]) -> anyhow::Result<()> + Send,
    {
        ensure!(
            file_sha(&self.spool_path)? == self.spool_sha256,
            "immutable spool changed"
        );
        ensure!(
            file_sha(&self.index_path)? == self.index_sha256,
            "prepared historical index changed"
        );
        let db = readonly(&self.spool_path)?;
        let index = readonly(&self.index_path)?;
        let authority =
            HistoricalAdoptionRepository::new(dst.clone()).prepare_authority(self.input.clone())?;
        let mut options = std::fs::OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut output = options.open(receipts_path)?;
        let mut digest = Sha256::new();
        for group in &self.input.groups {
            let rows = group_rows(
                &db,
                &index,
                group,
                &self.input.source_space_id,
                &self.input.space_id,
            )?;
            let committed = authority
                .adopt_group_with_effects(&group.external_source_id, rows)
                .await?;
            reconcile_effects(&committed.native_effects)?;
            let bytes = serde_json::to_vec(&committed.receipt)?;
            output.write_all(&bytes)?;
            output.write_all(b"\n")?;
            digest.update(bytes);
            digest.update(b"\n");
        }
        for group in &self.input.snapshot_groups {
            let rows = snapshot_plan::rows(
                &db,
                group,
                &self.input.source_space_id,
                &self.input.space_id,
            )?;
            let committed = Box::pin(authority.adopt_snapshot_graph(rows)).await?;
            reconcile_effects(&committed.native_effects)?;
            let bytes = serde_json::to_vec(&committed.receipt)?;
            output.write_all(&bytes)?;
            output.write_all(b"\n")?;
            digest.update(bytes);
            digest.update(b"\n");
        }
        drop(authority);
        output.sync_all()?;
        let mut staged = BTreeMap::new();
        let mut query=index.prepare("SELECT g.t,count(*) FROM graph g WHERE NOT EXISTS(SELECT 1 FROM memberships m WHERE m.t=g.t AND m.k=g.k) AND NOT EXISTS(SELECT 1 FROM snapshot_memberships m WHERE m.t=g.t AND m.k=g.k) GROUP BY g.t ORDER BY g.t")?;
        for row in query.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, u64>(1)?)))? {
            let (k, v) = row?;
            staged.insert(k, v);
        }
        ensure!(
            file_sha(&self.spool_path)? == self.spool_sha256,
            "immutable spool changed"
        );
        let source_input_sha256 =
            elephant_storage::historical_adoption::HistoricalRegenerationPlan::derive(
                &self.input,
                &[],
            )?
            .source_plan_sha256;
        Ok(AdoptionReport {
            source_input_sha256,
            original_snapshot_sha256: self.input.snapshot_sha256,
            source_space_id: self.input.source_space_id,
            target_space_id: self.input.space_id,
            plan_id: self.input.plan_id,
            configuration_input_sha256: self.config_input_sha256,
            receipts_sha256: hex::encode(digest.finalize()),
            groups: self.input.groups.len() + self.input.snapshot_groups.len(),
            graph_staged_without_provable_membership: staged,
            state: "historical_graph_adopted_current_embedding_coverage_pending",
        })
    }
}

/// Bind S0 heads and exact canonical receipts while the initial transfer still
/// owns custody. The input receipt stream is hash-checked before and after.
pub(super) async fn write_initial_catchup_bindings(
    dst: &StorageClient,
    receipts_path: &Path,
    receipts_sha256: &str,
    output_path: &Path,
    expected_groups: usize,
) -> anyhow::Result<(String, usize)> {
    ensure!(
        file_sha(receipts_path)? == receipts_sha256,
        "adoption receipt stream changed"
    );
    let mut source = std::io::BufReader::new(std::fs::File::open(receipts_path)?);
    let mut options = std::fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut output = options.open(output_path)?;
    let mut digest = Sha256::new();
    let mut count = 0usize;
    let mut input = InitialBindingInput::default();
    loop {
        let batch = input.read_batch(&mut source, expected_groups)?;
        if batch.is_empty() {
            break;
        }
        count += Box::pin(write_binding_batch(dst, &batch, &mut output, &mut digest)).await?;
    }
    ensure!(
        count == expected_groups && file_sha(receipts_path)? == receipts_sha256,
        "initial binding receipt inventory changed"
    );
    output.sync_all()?;
    #[cfg(unix)]
    std::fs::File::open(
        output_path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new(".")),
    )?
    .sync_all()?;
    Ok((hex::encode(digest.finalize()), count))
}

type InitialReceipt = elephant_storage::historical_adoption::HistoricalAdoptionReceipt;

#[derive(Default)]
struct InitialBindingInput {
    seen: BTreeSet<elephant_types::RecordId>,
    pending: Option<(InitialReceipt, usize)>,
}

impl InitialBindingInput {
    fn read_batch(
        &mut self,
        source: &mut impl std::io::BufRead,
        expected_groups: usize,
    ) -> anyhow::Result<Vec<InitialReceipt>> {
        let mut batch = Vec::new();
        let mut batch_bytes = 0usize;
        while batch.len() < MAX_INITIAL_BINDING_BATCH_RECEIPTS {
            let next = if let Some(pending) = self.pending.take() {
                Some(pending)
            } else {
                self.read_next(source, expected_groups)?
            };
            let Some((original, bytes)) = next else {
                break;
            };
            if !batch.is_empty() && batch_bytes + bytes > MAX_INITIAL_BINDING_BATCH_BYTES {
                self.pending = Some((original, bytes));
                break;
            }
            ensure!(
                bytes <= MAX_INITIAL_BINDING_BATCH_BYTES,
                "initial binding receipt exceeds batch bound"
            );
            batch_bytes += bytes;
            batch.push(original);
        }
        Ok(batch)
    }

    fn read_next(
        &mut self,
        source: &mut impl std::io::BufRead,
        expected_groups: usize,
    ) -> anyhow::Result<Option<(InitialReceipt, usize)>> {
        let mut line = Vec::new();
        let bytes = std::io::Read::by_ref(source)
            .take(8 * 1024 * 1024 + 1)
            .read_until(b'\n', &mut line)?;
        if bytes == 0 {
            return Ok(None);
        }
        ensure!(
            bytes <= 8 * 1024 * 1024 && line.last() == Some(&b'\n'),
            "adoption receipt line exceeds bound or is incomplete"
        );
        let original: InitialReceipt = serde_json::from_slice(&line)?;
        ensure!(
            self.seen.len() < expected_groups && self.seen.insert(original.source_id.clone()),
            "duplicate or extra initial source binding"
        );
        // One legal original line remains accepted. Account for lexical and
        // typed bytes; retain at most one bounded lookahead between batches.
        let accounted_bytes = bytes.max(serde_json::to_vec(&original)?.len());
        Ok(Some((original, accounted_bytes)))
    }
}

async fn write_binding_batch(
    dst: &StorageClient,
    originals: &[elephant_storage::historical_adoption::HistoricalAdoptionReceipt],
    output: &mut std::fs::File,
    digest: &mut Sha256,
) -> anyhow::Result<usize> {
    let bindings = Box::pin(
        HistoricalAdoptionRepository::new(dst.clone()).initial_catchup_bindings(originals),
    )
    .await?;
    ensure!(
        bindings.len() == originals.len(),
        "initial binding batch count differs"
    );
    for binding in bindings {
        let bytes = serde_json::to_vec(&binding)?;
        output.write_all(&bytes)?;
        output.write_all(b"\n")?;
        digest.update(bytes);
        digest.update(b"\n");
    }
    Ok(originals.len())
}

impl PreparedAdoption {
    pub(super) fn catchup_plan(
        &self,
        original: &[elephant_storage::client::HistoricalCatchupSource],
    ) -> anyhow::Result<elephant_storage::historical_adoption::HistoricalRegenerationPlan> {
        self.verify_inputs()?;
        Ok(
            elephant_storage::historical_adoption::HistoricalRegenerationPlan::derive(
                &self.input,
                original,
            )?,
        )
    }
    fn verify_inputs(&self) -> anyhow::Result<()> {
        ensure!(
            file_sha(&self.spool_path)? == self.spool_sha256
                && file_sha(&self.index_path)? == self.index_sha256,
            "immutable S1 source or membership index changed"
        );
        Ok(())
    }
    pub(super) async fn regenerate(
        self,
        sealed: elephant_storage::client::SealedHistoricalCatchupAuthority,
        ledger: &mut elephant_storage::historical_catchup_ledger::HistoricalCatchupLedger,
        #[cfg(test)] interrupt_after_first_commit: bool,
    ) -> anyhow::Result<elephant_storage::historical_adoption::HistoricalS1SourceClosure> {
        self.verify_inputs()?;
        let db = readonly(&self.spool_path)?;
        let index = readonly(&self.index_path)?;
        let snapshots: BTreeMap<_, _> = self
            .input
            .snapshot_groups
            .iter()
            .map(|group| (group.external_source_id.as_str(), group))
            .collect();
        let authority = sealed.prepare_regeneration(self.input.clone()).await?;
        let (committed, total) = authority.regeneration_position()?;
        // Start from the immutable S0 ledger on every invocation. Consume the
        // complete committed prefix before allowing the first new group write.
        for ordinal in 0..committed {
            ledger.apply(authority.recovered_regeneration_effects(ordinal).await?)?;
        }
        authority.verify_zero_row_inventory(ledger).await?;
        let groups = self
            .input
            .groups
            .iter()
            .map(|group| (group.external_source_id.as_str(), group))
            .collect::<BTreeMap<_, _>>();
        let mut next = committed;
        loop {
            let step = authority.next_regeneration_step()?;
            let Some(step) = step else {
                break;
            };
            ensure!(next < total, "S1 regeneration exceeded original plan");
            if let Some(external) = step.external_source_id {
                if let Some(group) = groups.get(external.as_str()) {
                    let rows = group_rows(
                        &db,
                        &index,
                        group,
                        &self.input.source_space_id,
                        &self.input.space_id,
                    )?;
                    authority.adopt_group_with_effects(&external, rows).await?;
                } else {
                    let group = snapshots
                        .get(external.as_str())
                        .context("S1 declared snapshot group missing")?;
                    let rows = snapshot_plan::rows(
                        &db,
                        group,
                        &self.input.source_space_id,
                        &self.input.space_id,
                    )?;
                    Box::pin(authority.adopt_snapshot_graph(rows)).await?;
                }
            } else {
                authority.retire_next_group_with_effects().await?;
            }
            #[cfg(test)]
            if interrupt_after_first_commit && next == 0 {
                anyhow::bail!(
                    "injected process interruption after first regeneration commit before ledger"
                );
            }
            ledger.apply(authority.recovered_regeneration_effects(next).await?)?;
            next += 1;
        }
        ensure!(next == total, "S1 regeneration incomplete");
        self.verify_inputs()?;
        Ok(authority.close_regeneration().await?)
    }
}

#[cfg(test)]
#[path = "historical_initial_binding_input_tests.rs"]
mod binding_input_tests;

#[cfg(test)]
mod plan_digest_tests {
    use super::*;

    #[test]
    fn empty_s1_retires_original_population_but_initial_import_refuses_it() -> anyhow::Result<()> {
        use elephant_storage::client::HistoricalCatchupSource;
        use elephant_storage::historical_adoption::HistoricalRegenerationPlan;
        let empty_hash = hex::encode(Sha256::digest([]));
        let input = HistoricalAdoptionInput {
            source_space_id: "default".into(),
            space_id: "household".into(),
            snapshot_sha256: "a".repeat(64),
            idmap_sha256: "b".repeat(64),
            documents_sha256: empty_hash.clone(),
            groups_sha256: empty_hash,
            plan_id: "c".repeat(64),
            groups: Vec::new(),
            snapshot_groups: Vec::new(),
        };
        assert!(validate_population_for_mode(&input, false).is_err());
        validate_population_for_mode(&input, true).context("complete empty S1 allowed")?;
        let source_id = RecordId::new_v4(TableName::Source);
        let original = HistoricalCatchupSource {
            source_id: source_id.clone(),
            commit_id: "original".into(),
            receipt_sha256: "d".repeat(64),
            head: elephant_types::publication::SourceHead {
                space_id: "household".into(),
                source_id: source_id.clone(),
                generation: "generation".into(),
                desired_revision: Some("revision".into()),
                published_revision: Some("revision".into()),
                operation_id: "operation".into(),
                lifecycle: elephant_types::publication::SourceVisibility::Active,
                updated_at: chrono::Utc::now(),
            },
        };
        let plan = HistoricalRegenerationPlan::derive(&input, &[original])
            .context("retire original source")?;
        assert_eq!(plan.steps.len(), 1);
        assert_eq!(plan.steps[0].source_id, source_id);
        assert!(plan.steps[0].external_source_id.is_none());
        Ok(())
    }
    #[test]
    fn exact_python_plan_bytes_preserve_float_integer_unicode_and_order() -> anyhow::Result<()> {
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let code = r"
import sys
sys.path.insert(0, sys.argv[1])
import preservation_plan as p
value = {'z': {'unicode':'Å\u2028𐀀', 'number': 1e-7, 'integer':9007199254740993},
         'a':[1.0,-0.0,1e20,1e-6,1e-5]}
value['plan_id'] = p.value_hash(value)
value['inventory_sha256'] = 'f' * 64
sys.stdout.buffer.write(p.canonical(value))
";
        let output = std::process::Command::new("python3")
            .args(["-c", code])
            .arg(workspace.join("scripts/migration"))
            .env("PYTHONDONTWRITEBYTECODE", "1")
            .output()?;
        assert!(output.status.success());
        let raw = String::from_utf8(output.stdout)?;
        let plan = strict_json(&raw)?;
        assert_eq!(
            producer_plan_digest(&raw)?,
            plan["plan_id"]
                .as_str()
                .context("Python plan identity missing")?
        );
        assert_ne!(
            producer_plan_digest(&raw.replace("1e-07", "2e-07"))?,
            plan["plan_id"]
                .as_str()
                .context("Python plan identity missing")?
        );
        // Semantically equivalent float spellings still alter producer bytes.
        assert_ne!(
            producer_plan_digest(&raw.replace("1e-07", "0.0000001"))?,
            plan["plan_id"]
                .as_str()
                .context("Python plan identity missing")?
        );
        assert!(producer_plan_digest(&raw.replacen("\"z\":{", "\"z\":{\"number\":2,", 1)).is_err());
        assert!(producer_plan_digest(&raw.replacen('{', "{\"a\":[],", 1)).is_err());
        Ok(())
    }
}
