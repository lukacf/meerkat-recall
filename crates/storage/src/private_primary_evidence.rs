//! Private, same-run qualification evidence. No HTTP route, query language,
//! provider call, database mutation, or historical-source authority is exposed.
//!
//! A hash-bound capability enables exactly 28 original publication captures,
//! two original maintenance captures and two complete current inventories.
//! The owner-only exchange is inert unless both activation variables are set.

#![cfg_attr(not(target_os = "linux"), allow(dead_code, unused_imports))]

use crate::{StorageClient, StorageError, StorageResult};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use chrono::{DateTime, Utc};
use elephant_types::{publication::PublicationFence, RecordId};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

#[path = "private_primary_evidence_export.rs"]
mod export;
#[cfg(target_os = "linux")]
#[path = "private_primary_evidence_io.rs"]
mod io;

const CAPABILITY_FORMAT: &str = "recall-private-primary-evidence-capability/v1";
const REQUEST_FORMAT: &str = "recall-private-primary-evidence-request/v1";
const RESPONSE_FORMAT: &str = "recall-private-primary-evidence-response/v1";
const MAX_REQUEST_BYTES: usize = 32_768;
const MAX_REPLY_BYTES: usize = 131_072;
const MAX_PAGE_BYTES: usize = 262_144;
// Base64 plus its fixed JSON envelope stays below the unchanged page ceiling.
const FRAGMENT_BYTES: usize = MAX_PAGE_BYTES / 2;
const MAX_TOTAL_BYTES: usize = 16_777_216;
const MAX_ROWS: usize = 4096;
const MAX_PROVIDER_RECEIPTS: usize = 432;
const MAX_PAGE_ROWS: usize = 128;
const MAX_PAGES: usize = 64;
const MAX_SELECTIONS: usize = 32;
const CORPUS_SHA256: &str = "fa6a84149483d3511fd559ac9a13e89d6c1c041ae97d6f15084b52b7d7f21c1b";
const SPACES: [&str; 2] = ["semantics-household", "semantics-business"];

type Result<T> = std::result::Result<T, EvidenceError>;

#[derive(Debug, thiserror::Error)]
#[error("private evidence: {0}")]
struct EvidenceError(&'static str);
const fn refusal(code: &'static str) -> EvidenceError {
    EvidenceError(code)
}
fn storage_error(_: StorageError) -> EvidenceError {
    refusal("storage_read_failed")
}
fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    // Value's map is sorted even when the workspace enables preserve_order.
    let value = serde_json::to_value(value).map_err(|_| refusal("serialization_failed"))?;
    serde_json::to_vec(&crate::canonical_commit::canonical_json(value))
        .map_err(|_| refusal("serialization_failed"))
}
fn digest(raw: &[u8]) -> String {
    hex::encode(Sha256::digest(raw))
}
fn valid_nonce(value: &str) -> bool {
    (16..=64).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}
fn valid_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

// Filesystem wire decoding only. Reject duplicate keys at every depth before
// typed validation; nested selectors must not silently keep a last value.
fn strict_json<T: serde::de::DeserializeOwned>(raw: &[u8]) -> Result<T> {
    struct Strict(Value);
    impl<'de> Deserialize<'de> for Strict {
        fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
            struct Visitor;
            impl<'de> serde::de::Visitor<'de> for Visitor {
                type Value = Strict;
                fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    f.write_str("JSON without duplicate fields")
                }
                fn visit_bool<E: serde::de::Error>(
                    self,
                    v: bool,
                ) -> std::result::Result<Strict, E> {
                    Ok(Strict(json!(v)))
                }
                fn visit_i64<E: serde::de::Error>(self, v: i64) -> std::result::Result<Strict, E> {
                    Ok(Strict(json!(v)))
                }
                fn visit_u64<E: serde::de::Error>(self, v: u64) -> std::result::Result<Strict, E> {
                    Ok(Strict(json!(v)))
                }
                fn visit_f64<E: serde::de::Error>(self, v: f64) -> std::result::Result<Strict, E> {
                    serde_json::Number::from_f64(v)
                        .map(|v| Strict(Value::Number(v)))
                        .ok_or_else(|| E::custom("nonfinite number"))
                }
                fn visit_str<E: serde::de::Error>(self, v: &str) -> std::result::Result<Strict, E> {
                    Ok(Strict(json!(v)))
                }
                fn visit_none<E: serde::de::Error>(self) -> std::result::Result<Strict, E> {
                    Ok(Strict(Value::Null))
                }
                fn visit_unit<E: serde::de::Error>(self) -> std::result::Result<Strict, E> {
                    Ok(Strict(Value::Null))
                }
                fn visit_seq<A: serde::de::SeqAccess<'de>>(
                    self,
                    mut seq: A,
                ) -> std::result::Result<Strict, A::Error> {
                    let mut values = Vec::new();
                    while let Some(Strict(value)) = seq.next_element()? {
                        values.push(value);
                    }
                    Ok(Strict(Value::Array(values)))
                }
                fn visit_map<A: serde::de::MapAccess<'de>>(
                    self,
                    mut map: A,
                ) -> std::result::Result<Strict, A::Error> {
                    let mut values = serde_json::Map::new();
                    while let Some((key, Strict(value))) = map.next_entry::<String, Strict>()? {
                        if values.insert(key, value).is_some() {
                            return Err(serde::de::Error::custom("duplicate field"));
                        }
                    }
                    Ok(Strict(Value::Object(values)))
                }
            }
            d.deserialize_any(Visitor)
        }
    }
    let Strict(value) = serde_json::from_slice(raw).map_err(|_| refusal("wire_json_invalid"))?;
    serde_json::from_value(value).map_err(|_| refusal("wire_fields_invalid"))
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SelectedRevision {
    space: String,
    source_key: String,
    revision: String,
    input_sha256: String,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Capability {
    format: String,
    run_nonce: String,
    backend: String,
    corpus_sha256: String,
    expires_at: DateTime<Utc>,
    directory: std::path::PathBuf,
    source_revisions: Vec<SelectedRevision>,
}
impl Capability {
    fn verify(&self, now: DateTime<Utc>) -> Result<()> {
        let expected: Vec<SelectedRevision> =
            serde_json::from_str(include_str!("private_primary_evidence_allowlist.json"))
                .map_err(|_| refusal("compiled_selection_invalid"))?;
        if self.format != CAPABILITY_FORMAT
            || !valid_nonce(&self.run_nonce)
            || !matches!(self.backend.as_str(), "elephant-db" | "surreal-db")
            || self.corpus_sha256 != CORPUS_SHA256
            || self.source_revisions != expected
            || self.expires_at <= now
            || !self.directory.is_absolute()
        {
            return Err(refusal("capability_invalid_or_expired"));
        }
        Ok(())
    }
    fn revision(&self, space: &str, selection: &PublicationSelection) -> Result<&SelectedRevision> {
        self.source_revisions
            .iter()
            .find(|row| {
                row.space == space
                    && row.source_key == selection.source_key
                    && row.revision == selection.publication.revision_id
            })
            .ok_or_else(|| refusal("unselected_revision"))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[allow(
    clippy::struct_field_names,
    reason = "request_id is the existing versioned request wire field."
)]
struct Request {
    format: String,
    request_id: String,
    run_nonce: String,
    backend: String,
    space: String,
    kind: String,
    selector: Value,
}
impl Request {
    fn verify(&self, capability: &Capability, file_id: &str) -> Result<()> {
        let mut original = serde_json::to_value(self).map_err(|_| refusal("request_invalid"))?;
        original
            .as_object_mut()
            .ok_or_else(|| refusal("request_invalid"))?
            .remove("request_id");
        if self.format != REQUEST_FORMAT
            || self.run_nonce != capability.run_nonce
            || self.backend != capability.backend
            || !SPACES.contains(&self.space.as_str())
            || !valid_hash(&self.request_id)
            || self.request_id != file_id
            || digest(&encode(&original)?) != self.request_id
        {
            return Err(refusal("request_identity_mismatch"));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct PublicationSelection {
    source_key: String,
    publication: PublicationFence,
    doc_id: RecordId,
    doc_revision_id: RecordId,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MaintenanceSelection {
    job_id: RecordId,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CurrentSelection {
    current_sources: Vec<CurrentSource>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CurrentSource {
    source_key: String,
    original: Value,
}

#[derive(Default)]
struct Packet {
    evidence: Value,
    tables: BTreeMap<String, Vec<Value>>,
}
#[derive(Default)]
struct Allocation {
    bytes: usize,
    rows: usize,
    calls: BTreeSet<String>,
    // Only already published, complete maintenance rows may supply a later
    // current snapshot. The key binds named space, table and exact row bytes.
    maintenance_rows: BTreeMap<(String, String, String), RetainedRow>,
}
#[derive(Clone)]
struct RetainedPage {
    name: String,
    sha256: String,
    bytes: usize,
    rows: usize,
    table: String,
}
impl RetainedPage {
    fn descriptor(&self) -> Value {
        json!({"name":self.name,"sha256":self.sha256,"bytes":self.bytes,
            "rows":self.rows,"table":self.table})
    }
}
#[derive(Clone)]
struct RetainedRow {
    page: RetainedPage,
    index: usize,
}
#[derive(Default)]
struct PacketPages {
    descriptors: Vec<Value>,
    writes: Vec<(String, Vec<u8>)>,
    rows: usize,
    maintenance_rows: BTreeMap<(String, String, String), RetainedRow>,
}
enum FreshPage {
    Rows(Vec<(Value, usize)>),
    FragmentedRow { raw: Vec<u8>, position: usize },
}
struct PreparedPacket {
    reply: Vec<u8>,
    pages: Vec<(String, Vec<u8>)>,
    bytes: usize,
    rows: usize,
    calls: BTreeSet<String>,
    maintenance_rows: BTreeMap<(String, String, String), RetainedRow>,
}
fn prepare_packet(
    request: &Request,
    request_raw: &[u8],
    packet: Packet,
    error: Option<&str>,
    allocation: &Allocation,
) -> Result<PreparedPacket> {
    let (pages, logical_rows, calls) = prepare_packet_pages(request, packet.tables, allocation)?;
    let reply = encode(
        &json!({"format":RESPONSE_FORMAT,"request_id":request.request_id,
        "run_nonce":request.run_nonce,"backend":request.backend,"space":request.space,"kind":request.kind,
        "request_sha256":digest(request_raw),"status":if error.is_none(){"complete"}else{"incomplete"},
        "error_code":error,"evidence":packet.evidence,"pages":pages.descriptors}),
    )?;
    let bytes = request_raw.len()
        + reply.len()
        + pages
            .writes
            .iter()
            .map(|(_, bytes)| bytes.len())
            .sum::<usize>();
    let total_bytes = allocation
        .bytes
        .checked_add(bytes)
        .ok_or_else(|| refusal("capture_total_byte_limit"))?;
    let total_rows = allocation
        .rows
        .checked_add(pages.rows)
        .ok_or_else(|| refusal("capture_row_limit"))?;
    let provider_receipts = allocation.calls.union(&calls).count();
    let exceeded = if reply.len() > MAX_REPLY_BYTES {
        Some("capture_reply_byte_limit")
    } else if total_bytes > MAX_TOTAL_BYTES {
        Some("capture_total_byte_limit")
    } else if total_rows > MAX_ROWS {
        Some("capture_row_limit")
    } else if provider_receipts > MAX_PROVIDER_RECEIPTS {
        Some("capture_provider_receipt_limit")
    } else {
        None
    };
    if let Some(code) = exceeded {
        // Numeric allocation diagnostics reveal no source, request or provider
        // payload. They distinguish the original failing bound without changing
        // its ceiling or resetting any earlier capture consumption.
        tracing::warn!(
            error_code = code,
            reply_bytes = reply.len(),
            total_bytes,
            total_rows,
            logical_rows,
            reused_pages = pages.descriptors.len() - pages.writes.len(),
            provider_receipts,
            "Private qualification evidence allocation exceeded"
        );
        return Err(refusal(code));
    }
    Ok(PreparedPacket {
        reply,
        pages: pages.writes,
        bytes,
        rows: pages.rows,
        calls,
        maintenance_rows: pages.maintenance_rows,
    })
}

fn prepare_packet_pages(
    request: &Request,
    tables: BTreeMap<String, Vec<Value>>,
    allocation: &Allocation,
) -> Result<(PacketPages, usize, BTreeSet<String>)> {
    let mut pages = PacketPages::default();
    let mut logical_rows = 0usize;
    let mut calls = BTreeSet::new();
    for (table, values) in tables {
        // Group origins independently of current page boundaries. Insertions or
        // deletions do not require rewriting unchanged original row evidence.
        let mut retained: BTreeMap<String, (RetainedPage, Vec<[usize; 2]>)> = BTreeMap::new();
        let mut fresh = Vec::new();
        let mut page = Vec::new();
        let mut page_bytes = 2usize;
        let selected = request.kind == "current_canonical";
        for (position, value) in values.into_iter().enumerate() {
            if table == "provider_call_receipt" {
                calls.insert(
                    value
                        .get("call_id")
                        .and_then(Value::as_str)
                        .ok_or_else(|| refusal("provider_identity_missing"))?
                        .to_owned(),
                );
            } else {
                logical_rows = logical_rows
                    .checked_add(1)
                    .ok_or_else(|| refusal("capture_row_limit"))?;
                if logical_rows > MAX_ROWS {
                    return Err(refusal("capture_row_limit"));
                }
            }
            let one = encode(&value)?;
            if one.len() + 2 > MAX_PAGE_BYTES {
                // Preserve this row's exact canonical JSON bytes. Do not omit
                // payload, increase a ceiling, or borrow fragment authority.
                if one.len() > MAX_TOTAL_BYTES {
                    return Err(refusal("capture_total_byte_limit"));
                }
                if !page.is_empty() {
                    fresh.push(FreshPage::Rows(std::mem::take(&mut page)));
                    page_bytes = 2;
                }
                fresh.push(FreshPage::FragmentedRow { raw: one, position });
                continue;
            }
            let key = (request.space.clone(), table.clone(), digest(&one));
            if selected && export::is_canonical_table(&table) {
                if let Some(origin) = allocation.maintenance_rows.get(&key) {
                    let group = retained
                        .entry(origin.page.name.clone())
                        .or_insert_with(|| (origin.page.clone(), Vec::new()));
                    group.1.push([origin.index, position]);
                    continue;
                }
            }
            let added_bytes = one.len() + usize::from(!page.is_empty());
            if !page.is_empty()
                && (page.len() == MAX_PAGE_ROWS || page_bytes + added_bytes > MAX_PAGE_BYTES)
            {
                fresh.push(FreshPage::Rows(std::mem::take(&mut page)));
                page_bytes = 2;
            }
            page_bytes += one.len() + usize::from(!page.is_empty());
            page.push((value, position));
        }
        if !page.is_empty() {
            fresh.push(FreshPage::Rows(page));
        }
        for (_, (origin, selection)) in retained {
            if pages.descriptors.len() == MAX_PAGES {
                return Err(refusal("page_count_limit"));
            }
            let mut descriptor = origin.descriptor();
            descriptor["selection"] = json!(selection);
            pages.descriptors.push(descriptor);
        }
        for page in fresh {
            match page {
                FreshPage::Rows(rows) => add_page(request, &table, &rows, selected, &mut pages)?,
                FreshPage::FragmentedRow { raw, position } => {
                    add_fragmented_row(request, &table, &raw, position, selected, &mut pages)?;
                }
            }
        }
    }
    Ok((pages, logical_rows, calls))
}
fn add_page(
    request: &Request,
    table: &str,
    page: &[(Value, usize)],
    selected: bool,
    pages: &mut PacketPages,
) -> Result<()> {
    if pages.descriptors.len() == MAX_PAGES {
        return Err(refusal("page_count_limit"));
    }
    let name = format!("{}.{:04}.json", request.request_id, pages.descriptors.len());
    let values: Vec<&Value> = page.iter().map(|(value, _)| value).collect();
    let raw = encode(&values)?;
    let original = RetainedPage {
        name: name.clone(),
        sha256: digest(&raw),
        bytes: raw.len(),
        rows: page.len(),
        table: table.to_owned(),
    };
    let mut descriptor = original.descriptor();
    if selected {
        let selection: Vec<[usize; 2]> = page
            .iter()
            .enumerate()
            .map(|(index, (_, position))| [index, *position])
            .collect();
        descriptor["selection"] = json!(selection);
    }
    pages.descriptors.push(descriptor);
    if request.kind == "original_maintenance" && export::is_canonical_table(table) {
        for (index, (value, _)) in page.iter().enumerate() {
            let key = (
                request.space.clone(),
                table.to_owned(),
                digest(&encode(value)?),
            );
            if pages
                .maintenance_rows
                .insert(
                    key,
                    RetainedRow {
                        page: original.clone(),
                        index,
                    },
                )
                .is_some()
            {
                return Err(refusal("duplicate_maintenance_row"));
            }
        }
    }
    if table != "provider_call_receipt" {
        pages.rows = pages
            .rows
            .checked_add(page.len())
            .ok_or_else(|| refusal("capture_row_limit"))?;
    }
    pages.writes.push((name, raw));
    Ok(())
}

// All fragments are current-request physical pages. They reconstruct exactly
// one logical row and never populate the ordinary maintenance-row reuse cache.
fn add_fragmented_row(
    request: &Request,
    table: &str,
    row: &[u8],
    position: usize,
    selected: bool,
    pages: &mut PacketPages,
) -> Result<()> {
    let count = row.len().div_ceil(FRAGMENT_BYTES);
    if pages.descriptors.len().saturating_add(count) > MAX_PAGES {
        return Err(refusal("page_count_limit"));
    }
    let row_sha256 = digest(row);
    for (index, fragment) in row.chunks(FRAGMENT_BYTES).enumerate() {
        let name = format!("{}.{:04}.json", request.request_id, pages.descriptors.len());
        let raw = encode(&json!({"encoding":"base64","data":BASE64.encode(fragment)}))?;
        if raw.len() > MAX_PAGE_BYTES {
            return Err(refusal("fragment_page_byte_limit"));
        }
        let mut descriptor = json!({"name":name,"sha256":digest(&raw),"bytes":raw.len(),
            "rows":1,"table":table,"row_fragment":{"row_sha256":row_sha256,
            "row_bytes":row.len(),"index":index,"count":count,"offset":index * FRAGMENT_BYTES}});
        if selected {
            descriptor["selection"] = json!([[0, position]]);
        }
        pages.descriptors.push(descriptor);
        pages.writes.push((name, raw));
    }
    if table != "provider_call_receipt" {
        pages.rows = pages
            .rows
            .checked_add(1)
            .ok_or_else(|| refusal("capture_row_limit"))?;
    }
    Ok(())
}

async fn capture_once<T, F>(duration: std::time::Duration, future: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T>>,
{
    tokio::time::timeout(duration, future)
        .await
        .map_err(|_| refusal("capture_timeout"))?
}

/// Process-local evidence worker; capability exhaustion never grants new reads.
/// Keep this value for the service lifetime. Restart cannot reuse a nonempty exchange.
pub struct PrivatePrimaryEvidence {
    capability: Capability,
    #[cfg(target_os = "linux")]
    exchange: io::Exchange,
    seen: BTreeSet<String>,
    selections: BTreeSet<String>,
    publications: BTreeMap<(String, String, String), PublicationSelection>,
    maintenance: BTreeSet<String>,
    allocation: Allocation,
    original_client: Option<StorageClient>,
    failed: bool,
}
impl PrivatePrimaryEvidence {
    /// Both variables absent disables the feature. Partial, invalid or stale
    /// activation fails initialization before a listener is exposed.
    pub fn from_environment() -> StorageResult<Option<Self>> {
        Self::from_activation(
            std::env::var_os("RECALL_PRIVATE_PRIMARY_EVIDENCE"),
            std::env::var_os("RECALL_PRIVATE_PRIMARY_EVIDENCE_SHA256"),
        )
        .map_err(|error| StorageError::DataIntegrityError(error.to_string()))
    }
    fn from_activation(
        path: Option<std::ffi::OsString>,
        expected: Option<std::ffi::OsString>,
    ) -> Result<Option<Self>> {
        match (path, expected) {
            (None, None) => Ok(None),
            (Some(path), Some(expected)) => {
                #[cfg(target_os = "linux")]
                {
                    let expected = expected
                        .to_str()
                        .filter(|s| valid_hash(s))
                        .ok_or_else(|| refusal("capability_digest_invalid"))?;
                    let raw = io::read_capability(std::path::Path::new(&path))?;
                    if digest(&raw) != expected {
                        return Err(refusal("capability_digest_mismatch"));
                    }
                    let capability: Capability = strict_json(&raw)?;
                    capability.verify(Utc::now())?;
                    let exchange = io::Exchange::open(&capability.directory)?;
                    Ok(Some(Self {
                        capability,
                        exchange,
                        seen: BTreeSet::new(),
                        selections: BTreeSet::new(),
                        publications: BTreeMap::new(),
                        maintenance: BTreeSet::new(),
                        allocation: Allocation::default(),
                        original_client: None,
                        failed: false,
                    }))
                }
                #[cfg(not(target_os = "linux"))]
                {
                    let _ = (path, expected);
                    Err(refusal("private_evidence_requires_linux"))
                }
            }
            _ => Err(refusal("capability_activation_incomplete")),
        }
    }
    /// Read at most one original request. Errors are safe fixed codes; the
    /// service must terminate this worker rather than silently recreating it.
    pub async fn poll(&mut self, client: &StorageClient) -> StorageResult<()> {
        if self.failed {
            return Err(StorageError::DataIntegrityError(
                "private evidence: worker_failed".into(),
            ));
        }
        let result = Box::pin(self.poll_inner(client)).await;
        self.failed = result.is_err();
        result.map_err(|error| StorageError::DataIntegrityError(error.to_string()))
    }

    #[cfg_attr(
        not(target_os = "linux"),
        expect(
            clippy::unused_async,
            reason = "the only await is in the Linux-only capture block below"
        )
    )]
    async fn poll_inner(&mut self, client: &StorageClient) -> Result<()> {
        self.capability.verify(Utc::now())?;
        if client.backend().as_str() != self.capability.backend
            || self
                .original_client
                .as_ref()
                .is_some_and(|original| !original.same_incarnation(client))
        {
            return Err(refusal("storage_incarnation_changed"));
        }
        self.original_client.get_or_insert_with(|| client.clone());
        #[cfg(target_os = "linux")]
        {
            let Some((name, raw)) = self.exchange.next(&self.seen)? else {
                return Ok(());
            };
            let request: Request = strict_json(&raw)?;
            request.verify(&self.capability, &name)?;
            if self.seen.len() >= MAX_SELECTIONS {
                return Err(refusal("request_count_limit"));
            }
            self.seen.insert(name);
            let admitted = Box::pin(client.with_activity(async {
                let result = Box::pin(capture_once(
                    std::time::Duration::from_secs(30),
                    self.capture(client, &request),
                ))
                .await;
                self.publish_result(&request, &raw, result)
            }))
            .await;
            match admitted {
                Ok(result) => result?,
                Err(_) => {
                    self.publish_result(
                        &request,
                        &raw,
                        Err(refusal("storage_recovery_not_ready")),
                    )?;
                }
            }
        }
        Ok(())
    }
    #[cfg(target_os = "linux")]
    fn publish_result(
        &mut self,
        request: &Request,
        raw: &[u8],
        result: Result<Packet>,
    ) -> Result<()> {
        let result =
            result.and_then(|packet| prepare_packet(request, raw, packet, None, &self.allocation));
        let (prepared, incomplete) = match result {
            Ok(packet) => (packet, false),
            Err(error) => (
                prepare_packet(
                    request,
                    raw,
                    Packet {
                        evidence: json!({"complete":false}),
                        tables: BTreeMap::new(),
                    },
                    Some(error.0),
                    &self.allocation,
                )?,
                true,
            ),
        };
        self.capability.verify(Utc::now())?;
        for (name, raw) in &prepared.pages {
            self.exchange.publish(name, raw)?;
        }
        self.exchange
            .publish(&format!("{}.json", request.request_id), &prepared.reply)?;
        self.allocation.bytes += prepared.bytes;
        self.allocation.rows += prepared.rows;
        self.allocation.calls.extend(prepared.calls);
        if !incomplete {
            // Commit the reference authority only after all original pages and
            // their complete response were successfully published.
            self.allocation
                .maintenance_rows
                .extend(prepared.maintenance_rows);
        }
        // Do not continue with any in-memory capture marker after an incomplete
        // reply. The original failed request/reply remains immutable evidence.
        if incomplete {
            return Err(refusal("capture_incomplete"));
        }
        Ok(())
    }
    async fn capture(&mut self, client: &StorageClient, request: &Request) -> Result<Packet> {
        let space = &request.space;
        match request.kind.as_str() {
            "original_publication" => {
                let fence = request
                    .selector
                    .get("publication")
                    .and_then(Value::as_object)
                    .ok_or_else(|| refusal("publication_selector_invalid"))?;
                if fence.len() != 4
                    || !["source_id", "generation", "revision_id", "operation_id"]
                        .iter()
                        .all(|key| fence.contains_key(*key))
                {
                    return Err(refusal("publication_selector_invalid"));
                }
                let selected: PublicationSelection =
                    serde_json::from_value(request.selector.clone())
                        .map_err(|_| refusal("publication_selector_invalid"))?;
                let revision = self.capability.revision(space, &selected)?.clone();
                let key = (
                    space.clone(),
                    selected.source_key.clone(),
                    selected.publication.revision_id.clone(),
                );
                if !self.selections.insert(format!("publication:{key:?}")) {
                    return Err(refusal("duplicate_selection"));
                }
                let packet = export::publication(client, space, &selected, &revision).await?;
                self.publications.insert(key, selected);
                Ok(packet)
            }
            "original_maintenance" => {
                let selected: MaintenanceSelection =
                    serde_json::from_value(request.selector.clone())
                        .map_err(|_| refusal("maintenance_selector_invalid"))?;
                if !self.selections.insert(format!("maintenance:{space}")) {
                    return Err(refusal("duplicate_selection"));
                }
                let current = self.current(space)?;
                let packet = export::maintenance(client, space, &selected.job_id, &current).await?;
                self.maintenance.insert(space.clone());
                Ok(packet)
            }
            "current_canonical" => {
                let selected: CurrentSelection =
                    serde_json::from_value(request.selector.clone())
                        .map_err(|_| refusal("current_selector_invalid"))?;
                if !self.selections.insert(format!("current:{space}")) {
                    return Err(refusal("duplicate_selection"));
                }
                if !self.maintenance.contains(space) {
                    return Err(refusal("original_maintenance_missing"));
                }
                let current = self.current(space)?;
                if selected.current_sources.len() != current.len() {
                    return Err(refusal("current_inventory_incomplete"));
                }
                let mut keys = BTreeSet::new();
                for row in selected.current_sources {
                    let prior = current
                        .iter()
                        .find(|p| p.source_key == row.source_key)
                        .ok_or_else(|| refusal("foreign_current_source"))?;
                    if !keys.insert(row.source_key)
                        || row.original.get("desired_revision")
                            != Some(&json!(prior.publication.revision_id))
                        || row.original.get("internal_source_id")
                            != Some(&json!(prior.publication.source_id))
                        || row.original.get("generation")
                            != Some(&json!(prior.publication.generation))
                        || row.original.get("operation_id")
                            != Some(&json!(prior.publication.operation_id))
                        || row.original.get("doc_id") != Some(&json!(prior.doc_id))
                    {
                        return Err(refusal("original_current_identity_mismatch"));
                    }
                }
                export::canonical(client, space, &current).await
            }
            _ => Err(refusal("unapproved_capture_kind")),
        }
    }
    fn current(&self, space: &str) -> Result<Vec<PublicationSelection>> {
        let mut latest = BTreeMap::new();
        for row in self
            .capability
            .source_revisions
            .iter()
            .filter(|row| row.space == space)
        {
            let captured = self
                .publications
                .get(&(
                    space.to_owned(),
                    row.source_key.clone(),
                    row.revision.clone(),
                ))
                .ok_or_else(|| refusal("original_publication_missing"))?;
            latest.insert(row.source_key.clone(), captured.clone());
        }
        Ok(latest.into_values().collect())
    }
}

#[cfg(test)]
#[path = "private_primary_evidence_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "private_primary_evidence_row_reuse_tests.rs"]
mod row_reuse_tests;

#[cfg(test)]
#[path = "private_primary_evidence_fragment_tests.rs"]
mod fragment_tests;
