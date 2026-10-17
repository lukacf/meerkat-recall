//! Versioned source acceptance, immutable revisions and operation polling.
//! Durable heads, receipts and lifecycle mutations remain storage-owned.

use std::{collections::BTreeMap, sync::Arc};

use axum::{
    extract::{
        rejection::{JsonRejection, QueryRejection},
        DefaultBodyLimit, Path, Query, State,
    },
    http::{HeaderMap, StatusCode},
    routing::{get, put},
    Extension, Json, Router,
};
use chrono::{DateTime, Utc};
use elephant_storage::{
    source_publication::{AcceptSourceRevision, SourceMutation, SourcePublicationRepository},
    SpaceConfigRepository, StorageClient,
};
use elephant_types::publication::{
    OperationState, PublicationFence, SourceAuthorizationSnapshot, SourcePrecondition,
    MAX_SOURCE_TEXT_BYTES,
};
use elephant_types::records::source::ManualDocTemplate;
use elephant_types::records::{BaseFields, Security, Source, SourceType, SubjectsState};
use elephant_types::{Link, RecordId, TableName};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::public_error::{ApiFailure, ErrorCode};
use crate::runtime::RuntimeManager;
use crate::{auth::Principal, AuthzService};

pub use elephant_types::publication_wire::AcceptedSource;
use elephant_types::publication_wire::{
    accepted_source as accepted_response, SourceOperation as OperationResponse,
};
#[cfg(test)]
use elephant_types::{publication::SourceVisibility, publication_wire::head_lifecycle};

/// Allows the maximum decoded text even when every byte uses a JSON escape.
pub const MAX_SOURCE_JSON_BYTES: usize = 12_100_000;
const MAX_METADATA_BYTES: usize = 32_768;
const MAX_LABEL_BYTES: usize = 16_384;

#[derive(Clone)]
pub struct PublicSourcesState {
    storage: StorageClient,
    authz: Arc<AuthzService>,
    runtime: Arc<RuntimeManager>,
    cursor_secret: Arc<[u8]>,
}
impl PublicSourcesState {
    pub fn new(
        storage: StorageClient,
        authz: Arc<AuthzService>,
        runtime: Arc<RuntimeManager>,
    ) -> Self {
        // Cursors are process-local capabilities, not bearer credentials.
        let cursor_secret = [*Uuid::new_v4().as_bytes(), *Uuid::new_v4().as_bytes()].concat();
        Self {
            storage,
            authz,
            runtime,
            cursor_secret: cursor_secret.into(),
        }
    }
    fn publication(&self) -> SourcePublicationRepository {
        SourcePublicationRepository::new(self.storage.clone())
    }
}

/// Consumer metadata label, separate from the security envelope's labels.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SourceLabel {
    Scalar(String),
    List(Vec<String>),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PutSourceRequest {
    pub revision: String,
    pub text: String,
    pub media_type: String,
    pub source_url: Option<String>,
    pub observed_at: DateTime<Utc>,
    #[serde(default)]
    pub labels: BTreeMap<String, SourceLabel>,
    pub expected_generation: Option<String>,
    #[serde(default)]
    pub reactivate: bool,
    #[serde(default)]
    pub recreate_after_erasure: bool,
    pub security: Option<Security>,
    pub subjects: Option<Vec<RecordId>>,
    pub subjects_state: Option<SubjectsState>,
    #[serde(default)]
    pub metadata: BTreeMap<String, Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LifecycleRequest {
    expected_generation: String,
}

fn valid_identifier(value: &str, max: usize) -> bool {
    !value.is_empty()
        && value.trim() == value
        && value.len() <= max
        && !value.chars().any(char::is_control)
}

impl PutSourceRequest {
    fn validate(&self) -> Result<(), ApiFailure> {
        if self.text.len() > MAX_SOURCE_TEXT_BYTES {
            return Err(ApiFailure::new(
                ErrorCode::PayloadTooLarge,
                "Source text exceeds 2000000 UTF-8 bytes",
            ));
        }
        if !valid_identifier(&self.revision, 256)
            || self.text.is_empty()
            || !matches!(self.media_type.as_str(), "text/plain" | "text/markdown")
            || self
                .source_url
                .as_ref()
                .is_some_and(|url| !valid_identifier(url, 4096))
            || self
                .expected_generation
                .as_ref()
                .is_some_and(|generation| !valid_identifier(generation, 256))
            || (self.reactivate && self.recreate_after_erasure)
            || ((self.reactivate || self.recreate_after_erasure)
                && self.expected_generation.is_none())
        {
            return Err(ApiFailure::new(
                ErrorCode::InvalidRequest,
                "Invalid source revision, text, media type, URL or lifecycle precondition",
            ));
        }
        if self.labels.len() > 128
            || self.labels.iter().any(|(key, value)| {
                !valid_identifier(key, 128)
                    || match value {
                        SourceLabel::Scalar(value) => !valid_identifier(value, 1024),
                        SourceLabel::List(values) => {
                            values.is_empty()
                                || values.len() > 128
                                || values.iter().any(|value| !valid_identifier(value, 1024))
                        }
                    }
            })
            || serde_json::to_vec(&self.labels).map_or(true, |value| value.len() > MAX_LABEL_BYTES)
        {
            return Err(ApiFailure::new(
                ErrorCode::InvalidRequest,
                "Source labels must be bounded nonempty strings or string lists",
            ));
        }
        if self.metadata.keys().any(|key| key.starts_with('_'))
            || serde_json::to_vec(&self.metadata)
                .map_or(true, |value| value.len() > MAX_METADATA_BYTES)
        {
            return Err(ApiFailure::new(
                ErrorCode::InvalidRequest,
                "Source metadata exceeds its limit or uses reserved control keys",
            ));
        }
        if self.subjects.as_ref().is_some_and(|subjects| {
            subjects.len() > 256 || subjects.iter().any(|id| id.table() != TableName::Entity)
        }) {
            return Err(ApiFailure::new(
                ErrorCode::InvalidRequest,
                "Subjects must be bounded entity identifiers",
            ));
        }
        Ok(())
    }
}

fn decode_body<T>(body: Result<Json<T>, JsonRejection>) -> Result<T, ApiFailure> {
    body.map(|Json(body)| body).map_err(|error| {
        ApiFailure::new(
            if error.status() == StatusCode::PAYLOAD_TOO_LARGE {
                ErrorCode::PayloadTooLarge
            } else {
                ErrorCode::InvalidRequest
            },
            "Invalid or oversized JSON request",
        )
    })
}

fn fingerprint(value: &Value) -> Result<String, ApiFailure> {
    let bytes = serde_json::to_vec(value)
        .map_err(|_| ApiFailure::new(ErrorCode::InvalidRequest, "Cannot encode source request"))?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

fn source_fingerprint(
    space: &str,
    external_id: &str,
    input: &PutSourceRequest,
    base: &BaseFields,
) -> Result<String, ApiFailure> {
    fingerprint(
        &json!({"space":space,"external_source_id":external_id,"revision":input.revision,"text":input.text,"media_type":input.media_type,"source_url":input.source_url,"labels":input.labels,"metadata":input.metadata,"security":base.security,"subjects":base.subjects,"subjects_state":base.subjects_state,"reactivate":input.reactivate,"recreate_after_erasure":input.recreate_after_erasure}),
    )
}

fn request_key(headers: &HeaderMap, fallback: &str) -> Result<String, ApiFailure> {
    let key = headers
        .get("Idempotency-Key")
        .map(|value| value.to_str())
        .transpose()
        .map_err(|_| ApiFailure::new(ErrorCode::InvalidRequest, "Invalid Idempotency-Key"))?
        .unwrap_or(fallback);
    if !valid_identifier(key, 512) {
        return Err(ApiFailure::new(
            ErrorCode::InvalidRequest,
            "Invalid Idempotency-Key",
        ));
    }
    Ok(key.into())
}

#[expect(
    clippy::format_collect,
    reason = "Keep the length-delimited source identity encoding byte-identical"
)]
fn stable_record(table: TableName, parts: &[&str]) -> RecordId {
    let encoded = parts
        .iter()
        .map(|part| format!("{}:{part}", part.len()))
        .collect::<String>();
    RecordId::new(
        table,
        Uuid::new_v5(&Uuid::NAMESPACE_OID, encoded.as_bytes()),
    )
}

async fn require_space(
    state: &PublicSourcesState,
    principal: &Principal,
    space: &str,
    scope: &str,
) -> Result<elephant_types::records::SpaceConfig, ApiFailure> {
    state.authz.authorize_space_scope(principal, space, scope)?;
    if elephant_types::validation::validate_space_id(space).is_err() {
        return Err(ApiFailure::new(ErrorCode::InvalidRequest, "Invalid space"));
    }
    SpaceConfigRepository::new(state.storage.clone())
        .get_by_space_id(space)
        .await?
        .ok_or_else(|| ApiFailure::new(ErrorCode::UnknownSpace, "Unknown space"))
}

async fn authorize_source(
    state: &PublicSourcesState,
    principal: &Principal,
    space: &str,
    source_id: &RecordId,
    write: bool,
) -> Result<SourceAuthorizationSnapshot, ApiFailure> {
    let source = state
        .publication()
        .authorization_snapshot(space, source_id)
        .await?
        .ok_or_else(|| {
            ApiFailure::new(
                ErrorCode::NotReady,
                "Source authorization envelope unavailable",
            )
        })?;
    state
        .authz
        .authorize_record_read(principal, &source.envelope, "read:source")?;
    if write {
        state
            .authz
            .authorize_record_write(principal, &source.envelope, "write:source")?;
    }
    Ok(source)
}

#[expect(
    clippy::too_many_lines,
    reason = "Keep ordered authorization, source lifecycle and configuration checks in their existing operation"
)]
async fn put_source(
    State(state): State<Arc<PublicSourcesState>>,
    Extension(principal): Extension<Principal>,
    Path((space, external_id)): Path<(String, String)>,
    headers: HeaderMap,
    body: Result<Json<PutSourceRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<AcceptedSource>), ApiFailure> {
    let input = decode_body(body)?;
    input.validate()?;
    if !valid_identifier(&external_id, 1024) {
        return Err(ApiFailure::new(
            ErrorCode::InvalidRequest,
            "Invalid external source identity",
        ));
    }
    let config = require_space(&state, &principal, &space, "write:source").await?;
    state
        .authz
        .authorize_space_scope(&principal, &space, "read:source")?;
    if !state.runtime.get_readiness().await.storage_ready {
        return Err(ApiFailure::new(ErrorCode::NotReady, "Storage is not ready"));
    }
    let publication = state.publication();
    let head = publication
        .external_head(&space, SourceType::Manual, &external_id)
        .await?;
    let existing = if let Some(head) = &head {
        Some(authorize_source(&state, &principal, &space, &head.source_id, true).await?)
    } else {
        None
    };
    let source_id = head.as_ref().map_or_else(
        || stable_record(TableName::Source, &[&space, &external_id]),
        |head| head.source_id.clone(),
    );
    let mut base = BaseFields::new(source_id, space.clone(), principal.subject.clone());
    base.security = input.security.clone().unwrap_or_else(|| {
        existing.as_ref().map_or_else(
            || config.base.security.clone(),
            |source| source.envelope.security.clone(),
        )
    });
    base.subjects = input.subjects.as_ref().map_or_else(
        || {
            existing
                .as_ref()
                .map_or_else(Vec::new, |source| source.envelope.subjects.clone())
        },
        |subjects| subjects.iter().cloned().map(Link::new).collect(),
    );
    base.subjects_state = input.subjects_state.unwrap_or_else(|| {
        existing.as_ref().map_or(SubjectsState::Unknown, |source| {
            source.envelope.subjects_state
        })
    });
    let mut source = Source::new(
        base,
        SourceType::Manual,
        hex::encode(Sha256::digest(input.text.as_bytes())),
    );
    source.external_id = Some(external_id.clone());
    source.uri = input.source_url.clone();
    let doc_id = stable_record(
        TableName::Doc,
        &[
            &space,
            &external_id,
            &input.revision,
            if input.recreate_after_erasure {
                input.expected_generation.as_deref().unwrap_or_default()
            } else {
                ""
            },
        ],
    );
    let mut metadata = serde_json::to_value(&input.metadata)
        .map_err(|_| ApiFailure::new(ErrorCode::InvalidRequest, "Invalid metadata"))?;
    metadata["content_type"] = json!(input.media_type);
    metadata["labels"] = json!(input.labels);
    metadata["observed_at"] = json!(input.observed_at);
    source.metadata = metadata.clone();
    source
        .set_manual_doc_template(&ManualDocTemplate {
            doc_id: doc_id.to_string(),
            title: external_id.clone(),
            text: input.text.clone(),
            sidecar_text: None,
            media_attachments: vec![],
            rendition_key: input.revision.clone(),
            metadata,
        })
        .map_err(|_| ApiFailure::new(ErrorCode::InvalidRequest, "Cannot stage source text"))?;
    source.set_requested_ingest_mode(elephant_types::records::IngestMode::Graph);
    state
        .authz
        .authorize_record_write(&principal, &source, "write:source")?;
    state
        .authz
        .authorize_record_read(&principal, &source, "read:source")?;
    let request_fingerprint = source_fingerprint(&space, &external_id, &input, &source.base)?;
    let fallback = fingerprint(&json!(["put", space, external_id, input.revision]))?;
    let key = request_key(&headers, &fallback)?;
    let receipt = publication
        .accept(AcceptSourceRevision {
            authorized_document: None,
            source,
            revision_id: input.revision.clone(),
            expected: input
                .expected_generation
                .map_or(SourcePrecondition::Absent, SourcePrecondition::Generation),
            idempotency_key: key,
            request_fingerprint,
            reactivate: input.reactivate,
            recreate_after_erasure: input.recreate_after_erasure,
            authorized: existing,
        })
        .await?;
    let receipt_envelope = publication
        .operation_authorization(&space, &receipt.operation_id)
        .await?
        .ok_or_else(|| {
            ApiFailure::new(
                ErrorCode::NotReady,
                "Operation authorization envelope unavailable",
            )
        })?;
    state
        .authz
        .authorize_record_read(&principal, &receipt_envelope, "read:source")?;
    if matches!(
        receipt.state,
        OperationState::Accepted | OperationState::FailedRetryable
    ) {
        if let Some(accepted) = publication
            .revision(&space, &receipt.source_id, &input.revision)
            .await?
        {
            let fence = PublicationFence {
                source_id: receipt.source_id.clone(),
                generation: receipt.source_generation.clone(),
                revision_id: input.revision,
                operation_id: receipt.operation_id.clone(),
            };
            if let Err(error) = elephant_pipeline::source_queue::enqueue_accepted_source(
                &state.storage,
                &accepted.source,
                &fence,
                &principal.subject,
            )
            .await
            {
                tracing::warn!(operation_id=%receipt.operation_id,error=%error,"Accepted source awaits durable enqueue recovery");
            }
        }
    }
    Ok((StatusCode::ACCEPTED, Json(accepted_response(&receipt))))
}

async fn get_head(
    State(state): State<Arc<PublicSourcesState>>,
    Extension(principal): Extension<Principal>,
    Path((space, external_id)): Path<(String, String)>,
) -> Result<Json<Value>, ApiFailure> {
    require_space(&state, &principal, &space, "read:source").await?;
    let head = state
        .publication()
        .external_head(&space, SourceType::Manual, &external_id)
        .await?
        .ok_or_else(|| ApiFailure::new(ErrorCode::NotFound, "Source head not found"))?;
    let authorization =
        authorize_source(&state, &principal, &space, &head.source_id, false).await?;
    let receipt = state
        .publication()
        .operation(&space, &head.operation_id)
        .await?
        .ok_or_else(|| {
            ApiFailure::new(
                ErrorCode::Unavailable,
                "Source operation receipt unavailable",
            )
        })?;
    let envelope = state
        .publication()
        .operation_authorization(&space, &head.operation_id)
        .await?
        .ok_or_else(|| {
            ApiFailure::new(
                ErrorCode::NotReady,
                "Head authorization envelope unavailable",
            )
        })?;
    state
        .authz
        .authorize_record_read(&principal, &envelope, "read:source")?;
    if state
        .publication()
        .external_head(&space, SourceType::Manual, &external_id)
        .await?
        .as_ref()
        != Some(&head)
        || state
            .publication()
            .authorization_snapshot(&space, &head.source_id)
            .await?
            .as_ref()
            != Some(&authorization)
    {
        return Err(ApiFailure::new(
            ErrorCode::NotReady,
            "Source head or authorization changed while reading; retry",
        ));
    }
    Ok(Json(elephant_types::publication_wire::head(
        &head, &receipt,
    )))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct InventoryQuery {
    #[serde(default = "default_inventory_limit")]
    limit: usize,
    cursor: Option<String>,
}
const fn default_inventory_limit() -> usize {
    50
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InventoryCursor {
    version: u8,
    binding: String,
    after: String,
    exp: i64,
}

fn cursor_binding(space: &str, subject: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(space.len().to_be_bytes());
    digest.update(space.as_bytes());
    digest.update(subject.len().to_be_bytes());
    digest.update(subject.as_bytes());
    hex::encode(digest.finalize())
}

fn encode_cursor(
    secret: &[u8],
    space: &str,
    subject: &str,
    after: String,
) -> Result<String, ApiFailure> {
    jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256),
        &InventoryCursor {
            version: 1,
            binding: cursor_binding(space, subject),
            after,
            exp: Utc::now().timestamp() + 900,
        },
        &jsonwebtoken::EncodingKey::from_secret(secret),
    )
    .map_err(|_| {
        ApiFailure::new(
            ErrorCode::Unavailable,
            "Cannot encode inventory continuation",
        )
    })
}
fn decode_cursor(
    secret: &[u8],
    space: &str,
    subject: &str,
    token: &str,
) -> Result<String, ApiFailure> {
    let invalid = || {
        ApiFailure::new(
            ErrorCode::InvalidRequest,
            "Invalid or expired inventory cursor; restart listing",
        )
    };
    if token.len() > 2048 {
        return Err(invalid());
    }
    let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::HS256);
    validation.validate_aud = false;
    validation.leeway = 0;
    let cursor = jsonwebtoken::decode::<InventoryCursor>(
        token,
        &jsonwebtoken::DecodingKey::from_secret(secret),
        &validation,
    )
    .map_err(|_| invalid())?
    .claims;
    if cursor.version != 1
        || cursor.binding != cursor_binding(space, subject)
        || Uuid::parse_str(&cursor.after).is_err()
    {
        return Err(invalid());
    }
    Ok(cursor.after)
}

#[expect(
    clippy::items_after_statements,
    clippy::significant_drop_tightening,
    clippy::suspicious_operation_groupings,
    clippy::too_many_lines,
    reason = "Keep the bounded inventory scan constants beside their original use; Retain guard, repository and runtime owners through the original response or state transition scope; OperationReceipt.source_generation matches SourceHead.generation; the head has no source_generation field; Keep ordered authorization, source lifecycle and configuration checks in their existing operation"
)]
async fn list_sources(
    State(state): State<Arc<PublicSourcesState>>,
    Extension(principal): Extension<Principal>,
    Path(space): Path<String>,
    query: Result<Query<InventoryQuery>, QueryRejection>,
) -> Result<Json<Value>, ApiFailure> {
    let query = query
        .map_err(|_| ApiFailure::new(ErrorCode::InvalidRequest, "Invalid inventory query"))?
        .0;
    if !(1..=100).contains(&query.limit) {
        return Err(ApiFailure::new(
            ErrorCode::InvalidRequest,
            "Inventory limit must be 1..100",
        ));
    }
    require_space(&state, &principal, &space, "read:source").await?;
    let after = query
        .cursor
        .as_deref()
        .map(|cursor| decode_cursor(&state.cursor_secret, &space, &principal.subject, cursor))
        .transpose()?;
    let repository = state.publication();
    const MAX_SCAN: usize = 1000;
    const MAX_SCAN_TIME: std::time::Duration = std::time::Duration::from_secs(10);
    let started = std::time::Instant::now();
    let budget_error = || {
        ApiFailure::new(
            ErrorCode::RateLimited,
            "Authorized source inventory scan budget exceeded",
        )
    };
    let mut after = after;
    let mut scanned = 0;
    let mut sources = Vec::new();
    let mut observed = Vec::new();
    let mut last_visible_key = None;
    let mut has_more = false;
    'pages: loop {
        if scanned >= MAX_SCAN || started.elapsed() >= MAX_SCAN_TIME {
            return Err(budget_error());
        }
        let page = repository
            .list_heads_page(&space, after.as_deref(), (MAX_SCAN - scanned).min(100))
            .await?;
        for head in page.heads {
            if started.elapsed() >= MAX_SCAN_TIME {
                return Err(budget_error());
            }
            scanned += 1;
            let authorization =
                match authorize_source(&state, &principal, &space, &head.source_id, false).await {
                    Ok(authorization) => authorization,
                    Err(ApiFailure(error)) if error.error_code == ErrorCode::ForbiddenSpace => {
                        continue
                    }
                    Err(error) => return Err(error),
                };
            let operation_envelope = repository
                .operation_authorization(&space, &head.operation_id)
                .await?
                .ok_or_else(|| {
                    ApiFailure::new(ErrorCode::NotReady, "Inventory authorization unavailable")
                })?;
            match state
                .authz
                .authorize_record_read(&principal, &operation_envelope, "read:source")
                .map_err(ApiFailure::from)
            {
                Ok(()) => {}
                Err(ApiFailure(error)) if error.error_code == ErrorCode::ForbiddenSpace => continue,
                Err(error) => return Err(error),
            }
            let receipt = repository
                .operation(&space, &head.operation_id)
                .await?
                .ok_or_else(|| {
                    ApiFailure::new(ErrorCode::NotReady, "Inventory operation unavailable")
                })?;
            if receipt.source_id != head.source_id || receipt.source_generation != head.generation {
                return Err(ApiFailure::new(
                    ErrorCode::NotReady,
                    "Inventory head changed; retry",
                ));
            }
            if sources.len() == query.limit {
                // Only an authorized lookahead may establish a continuation. Retain
                // its observation for the same final policy fence as returned rows.
                observed.push((head, authorization, receipt));
                has_more = true;
                break 'pages;
            }
            let source = elephant_storage::SourceRepository::new(state.storage.clone())
                .get_with_deleted(&space, &head.source_id)
                .await?;
            if source
                .as_ref()
                .is_some_and(|source| SourceAuthorizationSnapshot::from(source) != authorization)
            {
                return Err(ApiFailure::new(
                    ErrorCode::NotReady,
                    "Inventory authorization changed; retry",
                ));
            }
            let mut row = elephant_types::publication_wire::head(&head, &receipt);
            row["external_source_id"] = json!(source
                .as_ref()
                .and_then(|source| source.external_id.as_deref()));
            row["source_type"] = json!(source.as_ref().map(|source| source.type_));
            sources.push(row);
            last_visible_key = Some(elephant_storage::source_inventory::source_head_cursor_key(
                &head,
            ));
            observed.push((head, authorization, receipt));
        }
        let Some(next) = page.next_key else {
            break;
        };
        after = Some(next);
    }
    // No cross-request snapshot promise; do reject changed observations within
    // this response, including reclassification after the initial ABAC check.
    for (head, authorization, receipt) in observed {
        if repository.head(&space, &head.source_id).await?.as_ref() != Some(&head)
            || repository
                .authorization_snapshot(&space, &head.source_id)
                .await?
                .as_ref()
                != Some(&authorization)
            || repository
                .operation(&space, &head.operation_id)
                .await?
                .as_ref()
                != Some(&receipt)
        {
            return Err(ApiFailure::new(
                ErrorCode::NotReady,
                "Source inventory changed while reading; retry",
            ));
        }
    }
    let next_cursor = if has_more {
        last_visible_key
            .map(|after| encode_cursor(&state.cursor_secret, &space, &principal.subject, after))
            .transpose()?
    } else {
        None
    };
    Ok(Json(json!({"sources":sources,"next_cursor":next_cursor})))
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain the existing dispatch future layout and ownership across the original await; Retain guard, repository and runtime owners through the original response or state transition scope"
)]
async fn mutate_source(
    state: &PublicSourcesState,
    principal: &Principal,
    space: &str,
    external_id: &str,
    action: &str,
    headers: &HeaderMap,
    input: LifecycleRequest,
) -> Result<AcceptedSource, ApiFailure> {
    require_space(state, principal, space, "write:source").await?;
    if !valid_identifier(&input.expected_generation, 256) {
        return Err(ApiFailure::new(
            ErrorCode::InvalidRequest,
            "Expected generation is required",
        ));
    }
    let publication = state.publication();
    let head = publication
        .external_head(space, SourceType::Manual, external_id)
        .await?
        .ok_or_else(|| ApiFailure::new(ErrorCode::NotFound, "Source head not found"))?;
    let authorized = authorize_source(state, principal, space, &head.source_id, true).await?;
    let identity = fingerprint(&json!([
        action,
        space,
        external_id,
        input.expected_generation
    ]))?;
    let request = SourceMutation {
        authorized_document: None,
        space_id: space.into(),
        source_id: head.source_id,
        expected: SourcePrecondition::Generation(input.expected_generation),
        idempotency_key: request_key(headers, &identity)?,
        request_fingerprint: identity,
        actor: principal.subject.clone(),
        authorized,
    };
    let receipt = if action == "erase" {
        publication.erase_live(request).await?
    } else {
        publication.retract(request).await?
    };
    let envelope = publication
        .operation_authorization(space, &receipt.operation_id)
        .await?
        .ok_or_else(|| {
            ApiFailure::new(
                ErrorCode::NotReady,
                "Operation authorization envelope unavailable",
            )
        })?;
    state
        .authz
        .authorize_record_read(principal, &envelope, "read:source")?;
    Ok(accepted_response(&receipt))
}

async fn lifecycle_action(
    State(state): State<Arc<PublicSourcesState>>,
    Extension(principal): Extension<Principal>,
    Path((space, source_action)): Path<(String, String)>,
    headers: HeaderMap,
    body: Result<Json<LifecycleRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<AcceptedSource>), ApiFailure> {
    let (external_id, action) = source_action
        .rsplit_once(':')
        .ok_or_else(|| ApiFailure::new(ErrorCode::NotFound, "Unknown source operation"))?;
    if !matches!(action, "retract" | "erase") {
        return Err(ApiFailure::new(
            ErrorCode::NotFound,
            "Unknown source operation",
        ));
    }
    Ok((
        StatusCode::ACCEPTED,
        Json(
            mutate_source(
                &state,
                &principal,
                &space,
                external_id,
                action,
                &headers,
                decode_body(body)?,
            )
            .await?,
        ),
    ))
}

async fn get_operation(
    State(state): State<Arc<PublicSourcesState>>,
    Extension(principal): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<Json<OperationResponse>, ApiFailure> {
    let space = principal.space_id.as_deref().ok_or_else(|| {
        ApiFailure::new(
            ErrorCode::ForbiddenSpace,
            "Select the operation's space with X-Space-Id",
        )
    })?;
    require_space(&state, &principal, space, "read:source").await?;
    let receipt = state
        .publication()
        .operation(space, &id)
        .await?
        .ok_or_else(|| ApiFailure::new(ErrorCode::NotFound, "Operation not found"))?;
    let authorization = state
        .publication()
        .operation_authorization(space, &receipt.operation_id)
        .await?
        .ok_or_else(|| {
            ApiFailure::new(
                ErrorCode::Unavailable,
                "Operation authorization envelope unavailable",
            )
        })?;
    state
        .authz
        .authorize_record_read(&principal, &authorization, "read:source")?;
    Ok(Json(elephant_types::publication_wire::operation(&receipt)))
}

pub fn public_source_routes() -> Router<Arc<PublicSourcesState>> {
    // Axum parameters occupy whole segments. The POST handler parses the final
    // :retract/:erase suffix; PUT treats the complete segment as external identity.
    Router::new()
        .route("/v1/spaces/{space}/sources", get(list_sources))
        .route(
            "/v1/spaces/{space}/sources/{external_id}",
            put(put_source).post(lifecycle_action),
        )
        .route(
            "/v1/spaces/{space}/sources/{external_id}/head",
            get(get_head),
        )
        .route("/v1/operations/{id}", get(get_operation))
        .layer(DefaultBodyLimit::max(MAX_SOURCE_JSON_BYTES))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::public_error::PublicError;
    fn request(text: String) -> PutSourceRequest {
        PutSourceRequest {
            revision: "r1".into(),
            text,
            media_type: "text/plain".into(),
            source_url: None,
            observed_at: Utc::now(),
            labels: BTreeMap::new(),
            expected_generation: None,
            reactivate: false,
            recreate_after_erasure: false,
            security: None,
            subjects: None,
            subjects_state: None,
            metadata: BTreeMap::new(),
        }
    }
    #[test]
    fn inventory_cursors_bind_space_caller_key_and_expiry() -> Result<(), ApiFailure> {
        let secret = b"inventory-test-key-at-least-32-bytes";
        let after = Uuid::new_v4().to_string();
        let token = encode_cursor(secret, "alpha", "service", after.clone())?;
        assert_eq!(decode_cursor(secret, "alpha", "service", &token)?, after);
        assert!(decode_cursor(secret, "beta", "service", &token).is_err());
        assert!(decode_cursor(secret, "alpha", "other", &token).is_err());
        assert!(decode_cursor(b"another-process-key", "alpha", "service", &token).is_err());
        let expired = jsonwebtoken::encode(
            &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256),
            &InventoryCursor {
                version: 1,
                binding: cursor_binding("alpha", "service"),
                after,
                exp: Utc::now().timestamp() - 1,
            },
            &jsonwebtoken::EncodingKey::from_secret(secret),
        )
        .map_err(|_| ApiFailure::new(ErrorCode::Unavailable, "Test cursor encoding failed"))?;
        assert!(decode_cursor(secret, "alpha", "service", &expired).is_err());
        assert!(decode_cursor(secret, "alpha", "service", &"x".repeat(2049)).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn inventory_routes_page_physical_heads_and_keep_retracted_rows(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (router, _) = route_fixture(true).await?;
        let mut accepted = Vec::new();
        for external in ["one", "two", "with%2Fslash%3Acolon"] {
            let (status, receipt) = request_json(
                router.clone(),
                "PUT",
                &format!("/v1/spaces/alpha/sources/{external}"),
                json!({"revision":"r1","text":"source evidence","media_type":"text/plain",
                    "observed_at":"2026-10-06T20:11:00Z","expected_generation":null}),
            )
            .await?;
            assert_eq!(status, StatusCode::ACCEPTED);
            accepted.push(receipt);
        }
        let (status, _) = request_json(
            router.clone(),
            "POST",
            "/v1/spaces/alpha/sources/one:retract",
            json!({"expected_generation":accepted[0]["generation"]}),
        )
        .await?;
        assert_eq!(status, StatusCode::ACCEPTED);
        let mut path = "/v1/spaces/alpha/sources?limit=1".to_string();
        let mut rows = Vec::new();
        // A bounded test loop also catches a nonprogressing server cursor.
        for _ in 0..4 {
            let (status, page) = request_json(router.clone(), "GET", &path, Value::Null).await?;
            assert_eq!(status, StatusCode::OK);
            let page_rows = page["sources"].as_array().ok_or("missing sources")?;
            assert!(page_rows.len() <= 1);
            rows.extend(page_rows.iter().cloned());
            let Some(cursor) = page["next_cursor"].as_str() else {
                break;
            };
            path = format!("/v1/spaces/alpha/sources?limit=1&cursor={cursor}");
        }
        assert_eq!(rows.len(), 3);
        assert!(rows
            .iter()
            .any(|row| row["external_source_id"] == "one" && row["lifecycle"] == "retracted"));
        assert!(rows
            .iter()
            .any(|row| row["external_source_id"] == "with/slash:colon"));
        for row in rows {
            assert!(row["state"].is_string());
            assert!(accepted
                .iter()
                .any(|receipt| receipt["internal_source_id"] == row["internal_source_id"]));
        }
        for query in [
            "limit=0",
            "limit=101",
            "cursor=invalid",
            "unsupported=value",
        ] {
            let (status, body) = request_json(
                router.clone(),
                "GET",
                &format!("/v1/spaces/alpha/sources?{query}"),
                Value::Null,
            )
            .await?;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert_eq!(body["error_code"], "invalid_request");
        }
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original test fixture and route-state owners alive through the complete assertion scope"
    )]
    async fn inventory_continuations_never_enumerate_denied_heads(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (router, storage) = route_fixture(true).await?;
        for index in 0..3 {
            let (status, _) = request_json(
                router.clone(),
                "PUT",
                &format!("/v1/spaces/alpha/sources/hidden-{index}"),
                json!({"revision":"r1","text":"classified source","media_type":"text/plain",
                    "observed_at":"2026-10-06T20:11:00Z","expected_generation":null,
                    "security":{"policy_id":"default","level":9,"labels":[],"handling":[]}}),
            )
            .await?;
            assert_eq!(status, StatusCode::ACCEPTED);
        }
        let state = Arc::new(PublicSourcesState::new(
            storage.clone(),
            Arc::new(AuthzService::new()),
            Arc::new(RuntimeManager::new_preconfigured(storage)),
        ));
        let principal = Principal {
            subject: "reader".into(),
            space_id: Some("alpha".into()),
            scopes: vec!["read:source".into()],
            ..Principal::default()
        };
        let Json(empty) = list_sources(
            State(state.clone()),
            Extension(principal.clone()),
            Path("alpha".into()),
            Ok(Query(InventoryQuery {
                limit: 1,
                cursor: None,
            })),
        )
        .await?;
        assert_eq!(empty, json!({"sources":[],"next_cursor":null}));
        for index in 0..3 {
            let (status, _) = request_json(
                router.clone(),
                "PUT",
                &format!("/v1/spaces/alpha/sources/visible-{index}"),
                json!({"revision":"r1","text":"readable source","media_type":"text/plain",
                    "observed_at":"2026-10-06T20:11:00Z","expected_generation":null}),
            )
            .await?;
            assert_eq!(status, StatusCode::ACCEPTED);
        }
        let mut cursor = None;
        let mut visible = std::collections::BTreeSet::new();
        for index in 0..3 {
            let Json(page) = list_sources(
                State(state.clone()),
                Extension(principal.clone()),
                Path("alpha".into()),
                Ok(Query(InventoryQuery { limit: 1, cursor })),
            )
            .await?;
            let rows = page["sources"].as_array().ok_or("missing sources")?;
            assert_eq!(
                rows.len(),
                1,
                "denied rows must not create extra empty pages"
            );
            let external = rows[0]["external_source_id"]
                .as_str()
                .ok_or("missing external")?;
            assert!(external.starts_with("visible-"));
            assert!(visible.insert(external.to_string()));
            cursor = page["next_cursor"].as_str().map(str::to_owned);
            assert_eq!(cursor.is_some(), index < 2);
            if let Some(token) = &cursor {
                let after = decode_cursor(&state.cursor_secret, "alpha", "reader", token)?;
                let head = state
                    .publication()
                    .external_head("alpha", SourceType::Manual, external)
                    .await?
                    .ok_or("missing visible head")?;
                assert_eq!(
                    after,
                    elephant_storage::source_inventory::source_head_cursor_key(&head)
                );
            }
        }
        Ok(())
    }

    #[test]
    fn decoded_utf8_bytes_define_the_source_limit() {
        let mut input = request("é".repeat(1_000_000));
        assert!(input.validate().is_ok());
        input.text.push('x');
        assert!(matches!(
            input.validate(),
            Err(ApiFailure(PublicError {
                error_code: ErrorCode::PayloadTooLarge,
                ..
            }))
        ));
    }
    #[test]
    fn labels_and_deliberate_lifecycle_flags_are_validated() {
        let mut input = request("{\"structured\":true}".into());
        input.labels.insert(
            "initiative".into(),
            SourceLabel::List(vec!["a".into(), "b".into()]),
        );
        assert!(input.validate().is_ok());
        input
            .labels
            .insert("initiative".into(), SourceLabel::List(vec![]));
        assert!(input.validate().is_err());
        input.labels.clear();
        input.reactivate = true;
        assert!(input.validate().is_err());
        input.expected_generation = Some("current".into());
        assert!(input.validate().is_ok());
        input.recreate_after_erasure = true;
        assert!(input.validate().is_err());
    }
    #[test]
    fn semantic_fingerprint_ignores_observation_and_retry_precondition() -> Result<(), ApiFailure> {
        let base = BaseFields::new(
            RecordId::new_v4(TableName::Source),
            "alpha".into(),
            "service".into(),
        );
        let mut input = request("exact evidence".into());
        let original = source_fingerprint("alpha", "external", &input, &base)?;
        input.observed_at += chrono::Duration::hours(1);
        input.expected_generation = Some("newly-read".into());
        assert_eq!(
            original,
            source_fingerprint("alpha", "external", &input, &base)?
        );
        input
            .labels
            .insert("scope".into(), SourceLabel::Scalar("changed".into()));
        assert_ne!(
            original,
            source_fingerprint("alpha", "external", &input, &base)?
        );
        Ok(())
    }
    #[test]
    fn internal_erasing_never_leaks_as_a_public_lifecycle() {
        assert_eq!(head_lifecycle(SourceVisibility::Erasing), "erased");
        assert_eq!(head_lifecycle(SourceVisibility::Erased), "erased");
        assert_eq!(head_lifecycle(SourceVisibility::Retracted), "retracted");
    }
    async fn route_fixture(
        configure: bool,
    ) -> Result<(Router, StorageClient), Box<dyn std::error::Error>> {
        use elephant_storage::Repository;
        let storage = StorageClient::new_embedded().await?;
        if configure {
            let config: elephant_types::records::SpaceConfig = serde_json::from_value(
                json!({"id":RecordId::new_v4(TableName::SpaceConfig),"space_id":"alpha","created_at":Utc::now(),"updated_at":Utc::now(),"created_by":"operator","security":{"policy_id":"default","level":0,"labels":[],"handling":[]},"provenance":{},"subjects":[],"subjects_state":"complete","embedding_provider":"fake","embedding_model":"fixture","embedding_dims":3,"purpose":null,"skill":null,"processing":null}),
            )?;
            SpaceConfigRepository::new(storage.clone())
                .create(config)
                .await?;
        }
        let runtime = Arc::new(RuntimeManager::new_with_storage_for_tests(storage.clone()));
        let state = Arc::new(PublicSourcesState::new(
            storage.clone(),
            Arc::new(AuthzService::new()),
            runtime,
        ));
        let principal = Principal {
            subject: "service".into(),
            space_id: Some("alpha".into()),
            bypass_abac: true,
            ..Principal::default()
        };
        Ok((
            public_source_routes()
                .with_state(state)
                .layer(Extension(principal)),
            storage,
        ))
    }
    async fn request_json(
        router: Router,
        method: &str,
        path: &str,
        body: Value,
    ) -> Result<(StatusCode, Value), Box<dyn std::error::Error>> {
        request_json_with_key(router, method, path, body, None).await
    }
    async fn request_json_with_key(
        router: Router,
        method: &str,
        path: &str,
        body: Value,
        key: Option<&str>,
    ) -> Result<(StatusCode, Value), Box<dyn std::error::Error>> {
        use tower::ServiceExt;
        let mut request = axum::http::Request::builder()
            .method(method)
            .uri(path)
            .header("content-type", "application/json");
        if let Some(key) = key {
            request = request.header("Idempotency-Key", key);
        }
        let response = router
            .oneshot(request.body(axum::body::Body::from(serde_json::to_vec(&body)?))?)
            .await?;
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 32_768).await?;
        Ok((status, serde_json::from_slice(&bytes)?))
    }

    #[tokio::test]
    async fn fresh_intent_and_current_generation_reaccept_a_historical_revision(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (router, _) = route_fixture(true).await?;
        let path = "/v1/spaces/alpha/sources/reaccept";
        let original = json!({"revision":"content-A","text":"immutable A","media_type":"text/plain",
            "observed_at":"2026-10-06T20:11:00Z","expected_generation":null});
        let (status, first) = request_json_with_key(
            router.clone(),
            "PUT",
            path,
            original.clone(),
            Some("intent-A1"),
        )
        .await?;
        assert_eq!(status, StatusCode::ACCEPTED);
        let mut replacement = original.clone();
        replacement["revision"] = json!("content-B");
        replacement["text"] = json!("immutable B");
        replacement["expected_generation"] = first["generation"].clone();
        let (status, second) =
            request_json_with_key(router.clone(), "PUT", path, replacement, Some("intent-B"))
                .await?;
        assert_eq!(status, StatusCode::ACCEPTED);

        let (status, historical) = request_json_with_key(
            router.clone(),
            "PUT",
            path,
            original.clone(),
            Some("intent-A1"),
        )
        .await?;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(historical["operation_id"], first["operation_id"]);
        let (_, head) =
            request_json(router.clone(), "GET", &format!("{path}/head"), Value::Null).await?;
        assert_eq!(head["generation"], second["generation"]);

        let (status, error) = request_json_with_key(
            router.clone(),
            "PUT",
            path,
            original.clone(),
            Some("intent-without-cas"),
        )
        .await?;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(error["error_code"], "conflict_generation");
        let mut reaccept = original;
        reaccept["expected_generation"] = second["generation"].clone();
        let (status, third) = request_json_with_key(
            router.clone(),
            "PUT",
            path,
            reaccept.clone(),
            Some("intent-A2"),
        )
        .await?;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_ne!(third["operation_id"], first["operation_id"]);
        assert_ne!(third["generation"], second["generation"]);
        assert_eq!(third["desired_revision"], "content-A");
        assert_eq!(third["internal_source_id"], first["internal_source_id"]);

        let (status, replay) = request_json_with_key(
            router.clone(),
            "PUT",
            path,
            reaccept.clone(),
            Some("intent-A2"),
        )
        .await?;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(replay["operation_id"], third["operation_id"]);
        let (status, error) = request_json_with_key(
            router.clone(),
            "PUT",
            path,
            reaccept.clone(),
            Some("unseen-stale-intent"),
        )
        .await?;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(error["error_code"], "conflict_generation");
        reaccept["expected_generation"] = third["generation"].clone();
        reaccept["text"] = json!("changed payload under content-A");
        let (status, error) =
            request_json_with_key(router.clone(), "PUT", path, reaccept, Some("changed-A")).await?;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(error["error_code"], "conflict_generation");
        let (_, head) = request_json(router, "GET", &format!("{path}/head"), Value::Null).await?;
        assert_eq!(head["generation"], third["generation"]);
        assert_eq!(head["operation_id"], third["operation_id"]);
        assert_eq!(head["state"], "pending");
        Ok(())
    }
    #[tokio::test]
    async fn absent_source_and_absent_space_have_distinct_public_errors(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (router, _) = route_fixture(false).await?;
        let (status, body) = request_json(
            router,
            "GET",
            "/v1/spaces/alpha/sources/example/head",
            Value::Null,
        )
        .await?;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error_code"], "unknown_space");
        let (router, _) = route_fixture(true).await?;
        let (status, body) = request_json(
            router,
            "GET",
            "/v1/spaces/alpha/sources/example/head",
            Value::Null,
        )
        .await?;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error_code"], "not_found");
        Ok(())
    }
    #[tokio::test]
    async fn public_acceptance_replays_observations_and_requires_replacement_generation(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (router, _) = route_fixture(true).await?;
        let mut input = json!({"revision":"r1","text":"exact UTF-8 evidence é","media_type":"text/plain","source_url":null,"observed_at":"2026-10-06T20:11:00Z","labels":{"scope":["one","two"]},"expected_generation":null});
        let (status, first) = request_json(
            router.clone(),
            "PUT",
            "/v1/spaces/alpha/sources/example",
            input.clone(),
        )
        .await?;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert!(first["generation"]
            .as_str()
            .is_some_and(|value| !value.is_empty()));
        input["observed_at"] = json!("2026-10-07T20:11:00Z");
        let (status, replay) = request_json(
            router.clone(),
            "PUT",
            "/v1/spaces/alpha/sources/example",
            input.clone(),
        )
        .await?;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(first, replay);
        input["revision"] = json!("r2");
        input["text"] = json!("replacement");
        let (status, conflict) = request_json(
            router.clone(),
            "PUT",
            "/v1/spaces/alpha/sources/example",
            input.clone(),
        )
        .await?;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(conflict["error_code"], "conflict_generation");
        input["expected_generation"] = first["generation"].clone();
        let (status, second) = request_json(
            router.clone(),
            "PUT",
            "/v1/spaces/alpha/sources/example",
            input,
        )
        .await?;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_ne!(first["generation"], second["generation"]);
        let (status, head) = request_json(
            router,
            "GET",
            "/v1/spaces/alpha/sources/example/head",
            Value::Null,
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(head["desired_revision"], "r2");
        assert_eq!(head["state"], "pending");
        assert_eq!(head["lifecycle"], "active");
        assert!(head.get("text").is_none());
        Ok(())
    }
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original test fixture and route-state owners alive through the complete assertion scope"
    )]
    async fn metadata_only_operation_reads_still_require_original_classification(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (router, storage) = route_fixture(true).await?;
        let (_,accepted)=request_json(router,"PUT","/v1/spaces/alpha/sources/classified",json!({"revision":"r1","text":"classified source payload","media_type":"text/plain","observed_at":"2026-10-06T20:11:00Z","security":{"policy_id":"default","level":2,"labels":[],"handling":[]}})).await?;
        let operation = accepted["operation_id"].as_str().ok_or("missing receipt")?;
        for (level, expected) in [(0, StatusCode::FORBIDDEN), (3, StatusCode::OK)] {
            let principal = Principal {
                subject: "metadata-reader".into(),
                space_id: Some("alpha".into()),
                scopes: vec!["read:source".into()],
                security_level: level,
                ..Principal::default()
            };
            let runtime = Arc::new(RuntimeManager::new_with_storage_for_tests(storage.clone()));
            let state = Arc::new(PublicSourcesState::new(
                storage.clone(),
                Arc::new(AuthzService::new()),
                runtime,
            ));
            let reader = public_source_routes()
                .with_state(state)
                .layer(Extension(principal));
            let (status, body) = request_json(
                reader,
                "GET",
                &format!("/v1/operations/{operation}"),
                Value::Null,
            )
            .await?;
            assert_eq!(status, expected);
            assert!(body.get("text").is_none());
        }
        Ok(())
    }
}
