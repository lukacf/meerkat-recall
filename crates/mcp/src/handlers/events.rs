#![allow(missing_docs, clippy::too_many_lines, clippy::cognitive_complexity)]
//! Domain event MCP handlers.
//!
//! Implements:
//! - `search_events`
//! - `get_event`
//! - `list_recent_events`
//! - `list_upcoming_events`
//! - `add_timeline_event`

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::str::FromStr;
use std::sync::Arc;

use elephant_storage::canonical_commit::CanonicalRecord;
use elephant_storage::public_mutation::{inherit_public_envelope, PublicMutation};
use elephant_storage::{
    parse_id, AssertionRepository, EntityRepository, EventRepository, Repository, SearchCriteria,
    SourceRepository, StorageClient, DEFAULT_UPCOMING_LIMIT, UPCOMING_SCAN_LIMIT,
};
use elephant_types::ids::TableName;
use elephant_types::records::assertion::Assertion;
use elephant_types::records::base::BaseFields;
use elephant_types::records::entity::Entity;
use elephant_types::records::source::Source;
use elephant_types::records::{Event, EventStatus, TimePrecision};
use elephant_types::{Link, RecordId};

use crate::coercion::coerce_params;
use crate::error::{McpError, McpResult};
use crate::profile::ToolProfile;
use crate::tools::ToolResponse;

use super::{ToolContext, ToolHandler};

const fn default_limit() -> usize {
    50
}

const fn default_recent_limit() -> usize {
    20
}

const fn default_days() -> u32 {
    7
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchEventsInput {
    #[serde(default)]
    pub query: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub include_deleted: bool,
    #[serde(default)]
    pub include_candidates: bool,
    #[serde(default)]
    pub statuses: Option<Vec<String>>,
    #[serde(default)]
    pub entity_ids: Option<Vec<String>>,
    #[serde(default)]
    pub mode: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchEventsOutput {
    pub items: Vec<Event>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    pub has_more: bool,
    pub total: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetEventInput {
    pub id: String,
    #[serde(default)]
    pub include_deleted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListRecentEventsInput {
    #[serde(default = "default_recent_limit")]
    pub limit: usize,
    #[serde(default)]
    pub include_candidates: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListUpcomingEventsInput {
    #[serde(default = "default_days")]
    pub days: u32,
    #[serde(default)]
    pub include_candidates: bool,
    /// Maximum items to return; defaults to and is capped at `DEFAULT_UPCOMING_LIMIT`.
    #[serde(default)]
    pub limit: Option<usize>,
    /// `next_cursor` from the previous page of the same read; omit for the first page.
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListEventsOutput {
    pub items: Vec<Event>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListUpcomingEventsOutput {
    pub items: Vec<Event>,
    /// True when more visible upcoming events existed than were returned.
    #[serde(default)]
    pub truncated: bool,
    /// Opaque token; pass it back as `cursor`, with the same `days` and
    /// `include_candidates`, to read the rest of the window. Present only when `truncated`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

/// The `(time_start, id)` an upcoming page resumes after.
type UpcomingPosition = (chrono::DateTime<chrono::Utc>, RecordId);

/// How long a continuation stays usable after the first page of its read.
const UPCOMING_CONTINUATION_TTL_SECONDS: i64 = 3600;

/// What a `list_upcoming_events` continuation carries between pages, sealed.
///
/// `as_of` is the instant the first page was read against. Every later page
/// reuses it, so the lower bound, the day-precision floor and the horizon stay
/// where they were and nothing slips out of the window between pages. The
/// position may be a row the caller cannot read (a page cut by the scan ceiling
/// resumes after the last row scanned), which is why the token is encrypted.
#[derive(Debug, Serialize, Deserialize)]
struct UpcomingContinuation {
    as_of: chrono::DateTime<chrono::Utc>,
    days: u32,
    include_candidates: bool,
    after_start: chrono::DateTime<chrono::Utc>,
    after_id: String,
}

/// What a continuation is bound to: this tool, the caller's space and the caller.
///
/// The caller is the verified subject and issuer when there is one, plus the
/// whole ABAC projection (scopes, clearances, subject allowlist, purposes), so a
/// token only resumes for the identical principal.
fn upcoming_binding(ctx: &ToolContext) -> Vec<u8> {
    fn sorted<'a>(values: impl IntoIterator<Item = &'a String>) -> Vec<&'a str> {
        let mut values: Vec<&str> = values.into_iter().map(String::as_str).collect();
        values.sort_unstable();
        values
    }
    let principal = &ctx.principal;
    let mut clearances: Vec<Value> = principal
        .clearances
        .iter()
        .map(|clearance| {
            json!([
                clearance.policy_id,
                clearance.max_level,
                sorted(&clearance.labels),
                sorted(&clearance.handling_allow),
            ])
        })
        .collect();
    clearances.sort_by_key(ToString::to_string);
    let allowlist = principal.subject_allowlist.as_ref().map(|ids| {
        let mut ids: Vec<String> = ids.iter().map(ToString::to_string).collect();
        ids.sort_unstable();
        ids
    });
    let caller = ctx
        .verified_caller()
        .map(|caller| json!([caller.claims().sub, caller.claims().iss]));
    let binding = json!({
        "tool": "list_upcoming_events",
        "space": principal.space_id,
        "caller": caller,
        "scopes": sorted(&principal.scopes),
        "clearances": clearances,
        "subject_allowlist": allowlist,
        "purpose": sorted(&principal.purpose),
    });
    let mut hasher = Sha256::new();
    hasher.update(binding.to_string().as_bytes());
    hasher.finalize().to_vec()
}

fn invalid_upcoming_cursor() -> McpError {
    McpError::InvalidParameters(
        "Invalid or expired upcoming cursor for this caller and window; start a new read \
         without a cursor"
            .to_string(),
    )
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddTimelineEventInput {
    pub title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(default)]
    pub subject_ids: Vec<String>,
    #[serde(default)]
    pub assertion_refs: Vec<String>,
    #[serde(default)]
    pub source_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_start: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_end: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_precision: Option<String>,
    #[serde(default)]
    pub confidence: Option<f32>,
    #[serde(default)]
    pub time_confidence: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
}

fn parse_statuses(statuses: &[String]) -> Result<Vec<EventStatus>, McpError> {
    statuses
        .iter()
        .map(|status| {
            EventStatus::from_str(status).map_err(|e| McpError::InvalidParameters(e.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()
}

fn search_status_filter(input: &SearchEventsInput) -> Result<Option<Vec<EventStatus>>, McpError> {
    if let Some(statuses) = input.statuses.as_ref() {
        return Ok(Some(parse_statuses(statuses)?));
    }

    if input.include_candidates {
        Ok(None)
    } else {
        Ok(Some(vec![
            EventStatus::Approved,
            EventStatus::Superseded,
            EventStatus::Rejected,
        ]))
    }
}

async fn authorized_events(ctx: &ToolContext, events: Vec<Event>) -> McpResult<Vec<Event>> {
    crate::current_read::filter(ctx, events, "read:event").await
}

fn tool_error_value(message: impl Into<String>) -> McpResult<Value> {
    let response: ToolResponse<()> = ToolResponse::error(message.into());
    serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
}

fn expected_tool_error(err: McpError) -> McpResult<Value> {
    match err {
        McpError::InvalidParameters(message) | McpError::ExecutionError(message) => {
            tool_error_value(message)
        }
        err => Err(err),
    }
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the assertion repository through ordered evidence authorization and dependency collection."
)]
async fn resolve_assertion_links(
    ctx: &ToolContext,
    ids: &[String],
    mutation: &mut PublicMutation,
    evidence_bases: &mut Vec<BaseFields>,
) -> McpResult<Vec<Link<Assertion>>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }

    if let Err(reason) =
        crate::authz::authorize_preflight_scope(&ctx.authz, &ctx.principal, "read:assertion")
    {
        return Err(McpError::ExecutionError(reason));
    }

    let repo = AssertionRepository::new(ctx.storage.clone());
    let space_id = ctx.principal.space_id.clone();
    let mut links = Vec::with_capacity(ids.len());

    for id in ids {
        let record_id = parse_id(id, TableName::Assertion)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;
        let Some(assertion) = repo
            .get(&space_id, &record_id)
            .await
            .map_err(McpError::StorageError)?
        else {
            return Err(McpError::InvalidParameters(format!(
                "assertion reference not found in caller space: {id}"
            )));
        };
        if let Err(reason) =
            ctx.authz
                .authorize_record_read(&ctx.principal, &assertion, "read:assertion")
        {
            return Err(McpError::ExecutionError(format!("Access denied: {reason}")));
        }
        evidence_bases.push(assertion.base.clone());
        mutation.depend_on(CanonicalRecord::Assertion(Box::new(assertion)));
        links.push(Link::new(record_id));
    }

    Ok(links)
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the source repository through ordered evidence authorization and dependency collection."
)]
async fn resolve_source_links(
    ctx: &ToolContext,
    ids: &[String],
    mutation: &mut PublicMutation,
    evidence_bases: &mut Vec<BaseFields>,
) -> McpResult<Vec<Link<Source>>> {
    let repo = SourceRepository::new(ctx.storage.clone());
    let space_id = ctx.principal.space_id.clone();
    let mut links = Vec::with_capacity(ids.len());

    for id in ids {
        let record_id = parse_id(id, TableName::Source)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;
        let Some(source) = repo
            .get(&space_id, &record_id)
            .await
            .map_err(McpError::StorageError)?
        else {
            return Err(McpError::InvalidParameters(format!(
                "source reference not found in caller space: {id}"
            )));
        };
        if let Err(reason) = ctx
            .authz
            .authorize_record_read(&ctx.principal, &source, "read:source")
        {
            return Err(McpError::ExecutionError(format!("Access denied: {reason}")));
        }
        evidence_bases.push(source.base.clone());
        mutation.depend_on(CanonicalRecord::Source(Box::new(source)));
        links.push(Link::new(record_id));
    }

    Ok(links)
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the entity repository through ordered subject authorization and dependency collection."
)]
async fn resolve_subject_links(
    ctx: &ToolContext,
    ids: &[String],
    mutation: &mut PublicMutation,
    evidence_bases: &mut Vec<BaseFields>,
) -> McpResult<Vec<Link<Entity>>> {
    let repo = EntityRepository::new(ctx.storage.clone());
    let space_id = ctx.principal.space_id.clone();
    let mut links = Vec::with_capacity(ids.len());

    for id in ids {
        let record_id = parse_id(id, TableName::Entity)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;
        let Some(entity) = repo
            .get(&space_id, &record_id)
            .await
            .map_err(McpError::StorageError)?
        else {
            return Err(McpError::InvalidParameters(format!(
                "subject entity not found in caller space: {id}"
            )));
        };
        if let Err(reason) = ctx
            .authz
            .authorize_record_read(&ctx.principal, &entity, "read:entity")
        {
            return Err(McpError::ExecutionError(format!("Access denied: {reason}")));
        }
        evidence_bases.push(entity.base.clone());
        mutation.depend_on(CanonicalRecord::Entity(Box::new(entity)));
        links.push(Link::new(record_id));
    }

    Ok(links)
}

fn parse_time_precision(input: Option<&str>) -> McpResult<TimePrecision> {
    input.map_or(Ok(TimePrecision::Unknown), |value| {
        TimePrecision::from_str(value).map_err(|e| McpError::InvalidParameters(e.to_string()))
    })
}

fn parse_event_status(input: Option<&str>) -> McpResult<EventStatus> {
    input.map_or(Ok(EventStatus::Approved), |value| {
        EventStatus::from_str(value).map_err(|e| McpError::InvalidParameters(e.to_string()))
    })
}

pub struct AddTimelineEventHandler {}

impl AddTimelineEventHandler {
    #[must_use]
    pub fn new(_storage: StorageClient) -> Self {
        Self {}
    }
}

#[async_trait]
impl ToolHandler for AddTimelineEventHandler {
    fn name(&self) -> &'static str {
        "add_timeline_event"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Create a domain timeline event backed by explicit evidence references.\n\n",
            "At least one `assertion_refs` or `source_refs` item is required.\n\n",
            "Example:\n",
            r#"{ "title": "Quarterly planning", "source_refs": ["source:550e8400-e29b-41d4-a716-446655440000"] }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["title"],
            "properties": {
                "title": { "type": "string" },
                "summary": { "type": "string" },
                "subject_ids": {
                    "type": "array",
                    "items": { "type": "string" },
                    "default": []
                },
                "assertion_refs": {
                    "type": "array",
                    "items": { "type": "string" },
                    "default": []
                },
                "source_refs": {
                    "type": "array",
                    "items": { "type": "string" },
                    "default": []
                },
                "time_start": { "type": "string", "format": "date-time" },
                "time_end": { "type": "string", "format": "date-time" },
                "time_precision": {
                    "type": "string",
                    "enum": ["exact", "minute", "hour", "day", "month", "year", "unknown"]
                },
                "confidence": { "type": "number", "minimum": 0.0, "maximum": 1.0 },
                "time_confidence": { "type": "number", "minimum": 0.0, "maximum": 1.0 },
                "status": {
                    "type": "string",
                    "enum": ["candidate", "approved", "superseded", "rejected"]
                },
                "metadata": {
                    "type": "object",
                    "required": [],
                    "additionalProperties": true
                }
            }
        })
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the event mutation plan through evidence authorization, commit and response construction."
    )]
    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: AddTimelineEventInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            return tool_error_value("Access denied: missing space_id");
        }

        if input.assertion_refs.is_empty() && input.source_refs.is_empty() {
            return tool_error_value(
                "At least one assertion_refs or source_refs evidence reference is required",
            );
        }

        let mut mutation = PublicMutation::begin(ctx.storage.clone(), &space_id).await?;
        let mut evidence_bases = Vec::new();
        let assertion_refs = match resolve_assertion_links(
            ctx,
            &input.assertion_refs,
            &mut mutation,
            &mut evidence_bases,
        )
        .await
        {
            Ok(value) => value,
            Err(err) => return expected_tool_error(err),
        };
        let source_refs =
            match resolve_source_links(ctx, &input.source_refs, &mut mutation, &mut evidence_bases)
                .await
            {
                Ok(value) => value,
                Err(err) => return expected_tool_error(err),
            };
        let subject_ids = match resolve_subject_links(
            ctx,
            &input.subject_ids,
            &mut mutation,
            &mut evidence_bases,
        )
        .await
        {
            Ok(value) => value,
            Err(err) => return expected_tool_error(err),
        };
        let time_precision = parse_time_precision(input.time_precision.as_deref())?;
        let status = parse_event_status(input.status.as_deref())?;

        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::Event),
            space_id,
            "mcp_tool".to_string(),
        );
        base.subjects_state = elephant_types::records::base::SubjectsState::Complete;
        if let Some(first) = evidence_bases.first() {
            base.security = first.security.clone();
        }
        for evidence in &evidence_bases {
            inherit_public_envelope(&mut base, evidence)?;
        }
        for subject in subject_ids {
            if !base.subjects.contains(&subject) {
                base.subjects.push(subject);
            }
        }
        for source in &source_refs {
            if !base.provenance.sources.contains(source.id()) {
                base.provenance.sources.push(source.id().clone());
            }
        }

        let mut event = Event::new(base, input.title, assertion_refs, source_refs)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;
        event.summary = input.summary;
        event.status = status;
        event.time_start = input.time_start;
        event.time_end = input.time_end;
        event.time_precision = time_precision;
        if let Some(confidence) = input.confidence {
            event.confidence = confidence;
        }
        if let Some(time_confidence) = input.time_confidence {
            event.time_confidence = time_confidence;
        }
        if let Some(metadata) = input.metadata {
            event.metadata = metadata;
        }
        event
            .validate()
            .map_err(|error| McpError::InvalidParameters(error.to_string()))?;

        if let Err(reason) = ctx
            .authz
            .authorize_record_write(&ctx.principal, &event, "write:event")
        {
            return tool_error_value(format!("Access denied: {reason}"));
        }

        if let Err(reason) = ctx
            .authz
            .authorize_record_read(&ctx.principal, &event, "read:event")
        {
            return tool_error_value(format!("Access denied: {reason}"));
        }
        let actor = ctx
            .verified_caller()
            .map_or("trusted:mcp", |caller| caller.claims().sub.as_str());
        let created = mutation
            .create(event, actor)
            .await
            .map_err(McpError::StorageError)?;
        let response = ToolResponse::success(created);
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

pub struct SearchEventsHandler {
    repo: EventRepository,
}

impl SearchEventsHandler {
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: EventRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for SearchEventsHandler {
    fn name(&self) -> &'static str {
        "search_events"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Search domain timeline events in the caller's space.\n\n",
            "Example:\n",
            r#"{ "query": "launch", "limit": 10, "include_candidates": false }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": [],
            "properties": {
                "query": { "type": "string" },
                "limit": { "type": "integer", "default": 50, "maximum": 100 },
                "cursor": { "type": "string" },
                "include_deleted": { "type": "boolean", "default": false },
                "include_candidates": { "type": "boolean", "default": false },
                "statuses": {
                    "type": "array",
                    "items": {
                        "type": "string",
                        "enum": ["candidate", "approved", "superseded", "rejected"]
                    }
                },
                "entity_ids": {
                    "type": "array",
                    "items": { "type": "string" }
                },
                "mode": {
                    "type": "string",
                    "enum": ["any", "all"]
                }
            }
        })
    }

    fn profile(&self) -> ToolProfile {
        ToolProfile::Agent
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: SearchEventsInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let cursor = if let Some(cursor) = input.cursor.as_ref() {
            Some(
                parse_id(cursor, TableName::Event)
                    .map_err(|e| McpError::InvalidParameters(e.to_string()))?,
            )
        } else {
            None
        };

        let mut criteria = SearchCriteria::new()
            .with_space_id(space_id)
            .with_limit(input.limit + 1);
        let statuses = search_status_filter(&input)?;

        if let Some(query) = input.query {
            criteria = criteria.with_query(query);
        }
        if input.include_deleted {
            criteria = criteria.include_deleted();
        }
        if let Some(cursor) = cursor {
            criteria = criteria.with_cursor(cursor.to_string());
        }
        if let Some(entity_ids) = input.entity_ids {
            criteria = criteria.with_entities(entity_ids);
        }
        if let Some(mode) = input.mode {
            criteria = criteria.with_mode(mode);
        }

        let search = self
            .repo
            .search_filtered(&criteria, statuses.as_deref())
            .await
            .map_err(|e| McpError::ExecutionError(e.to_string()))?;

        let mut authorized = authorized_events(ctx, search.results).await?;
        let total = authorized.len();
        let has_more = total > input.limit;
        if has_more {
            authorized.truncate(input.limit);
        }
        let next_cursor = if has_more {
            authorized.last().map(|event| event.base.id.to_string())
        } else {
            None
        };

        let output = SearchEventsOutput {
            total,
            items: authorized,
            next_cursor,
            has_more,
        };
        let response = ToolResponse::success(output);
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

pub struct GetEventHandler {
    repo: EventRepository,
}

impl GetEventHandler {
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: EventRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for GetEventHandler {
    fn name(&self) -> &'static str {
        "get_event"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Get a single domain event by id.\n\n",
            "Example:\n",
            r#"{ "id": "event:550e8400-e29b-41d4-a716-446655440000" }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["id"],
            "properties": {
                "id": { "type": "string" },
                "include_deleted": { "type": "boolean", "default": false }
            }
        })
    }

    fn profile(&self) -> ToolProfile {
        ToolProfile::Agent
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: GetEventInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let id = parse_id(&input.id, TableName::Event)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;
        let event = if input.include_deleted {
            self.repo
                .get_with_deleted(&space_id, &id)
                .await
                .map_err(|e| McpError::ExecutionError(e.to_string()))?
        } else {
            self.repo
                .get(&space_id, &id)
                .await
                .map_err(|e| McpError::ExecutionError(e.to_string()))?
        };

        let Some(event) = event else {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("event not found: {}", input.id));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        };

        if let Err(reason) = ctx
            .authz
            .authorize_record_read(&ctx.principal, &event, "read:event")
        {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Access denied: {reason}"));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let event = crate::current_read::require(ctx, event, "read:event").await?;
        let response = ToolResponse::success(event);
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

pub struct ListRecentEventsHandler {
    repo: EventRepository,
}

impl ListRecentEventsHandler {
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: EventRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for ListRecentEventsHandler {
    fn name(&self) -> &'static str {
        "list_recent_events"
    }

    fn description(&self) -> &'static str {
        concat!(
            "List recent domain events for the caller's space.\n\n",
            "Example:\n",
            r#"{ "limit": 20, "include_candidates": false }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": [],
            "properties": {
                "limit": { "type": "integer", "default": 20, "maximum": 200 },
                "include_candidates": { "type": "boolean", "default": false }
            }
        })
    }

    fn profile(&self) -> ToolProfile {
        ToolProfile::Agent
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: ListRecentEventsInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let mut events = self
            .repo
            .get_recent(&space_id, input.limit)
            .await
            .map_err(|e| McpError::ExecutionError(e.to_string()))?;
        if !input.include_candidates {
            events.retain(|event| event.status != EventStatus::Candidate);
        }
        let output = ListEventsOutput {
            items: authorized_events(ctx, events).await?,
        };
        let response = ToolResponse::success(output);
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

pub struct ListUpcomingEventsHandler {
    repo: EventRepository,
    clock: Option<Arc<dyn Fn() -> chrono::DateTime<chrono::Utc> + Send + Sync>>,
}

impl ListUpcomingEventsHandler {
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: EventRepository::new(storage),
            clock: None,
        }
    }

    #[cfg(test)]
    fn with_clock(
        mut self,
        clock: impl Fn() -> chrono::DateTime<chrono::Utc> + Send + Sync + 'static,
    ) -> Self {
        self.clock = Some(Arc::new(clock));
        self
    }

    fn now(&self) -> chrono::DateTime<chrono::Utc> {
        self.clock
            .as_ref()
            .map_or_else(chrono::Utc::now, |clock| clock())
    }

    /// Opens a continuation for this caller and request: `(as_of, after)`.
    fn resume(
        &self,
        ctx: &ToolContext,
        input: &ListUpcomingEventsInput,
        token: &str,
    ) -> McpResult<(chrono::DateTime<chrono::Utc>, UpcomingPosition)> {
        let plaintext = ctx
            .continuation_key()
            .open(&upcoming_binding(ctx), token)
            .ok_or_else(invalid_upcoming_cursor)?;
        let continuation: UpcomingContinuation =
            serde_json::from_slice(&plaintext).map_err(|_| invalid_upcoming_cursor())?;
        let age = self.now().signed_duration_since(continuation.as_of);
        if continuation.days != input.days
            || continuation.include_candidates != input.include_candidates
            || age.num_seconds() > UPCOMING_CONTINUATION_TTL_SECONDS
        {
            return Err(invalid_upcoming_cursor());
        }
        let after_id = parse_id(&continuation.after_id, TableName::Event)
            .map_err(|_| invalid_upcoming_cursor())?;
        Ok((continuation.as_of, (continuation.after_start, after_id)))
    }
}

#[async_trait]
impl ToolHandler for ListUpcomingEventsHandler {
    fn name(&self) -> &'static str {
        "list_upcoming_events"
    }

    fn description(&self) -> &'static str {
        concat!(
            "List upcoming domain events within a time window.\n\n",
            "Includes events still in progress and day-precision events dated today ",
            "(UTC-conservative: up to 24h back; filter the exact local day client-side). ",
            "Returns at most `limit` items (default and max 50); `truncated` is true when ",
            "more visible events existed, and the opaque `next_cursor` then reads the rest ",
            "of the same window when passed back as `cursor` with the same `days` and ",
            "`include_candidates`.\n\n",
            "Example:\n",
            r#"{ "days": 14, "include_candidates": false }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": [],
            "properties": {
                "days": { "type": "integer", "default": 7, "minimum": 1, "maximum": 365 },
                "include_candidates": { "type": "boolean", "default": false },
                "limit": {
                    "type": "integer",
                    "default": DEFAULT_UPCOMING_LIMIT,
                    "minimum": 1,
                    "maximum": DEFAULT_UPCOMING_LIMIT
                },
                "cursor": { "type": "string" }
            }
        })
    }

    fn profile(&self) -> ToolProfile {
        ToolProfile::Agent
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: ListUpcomingEventsInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let space_id = ctx.principal.space_id.clone();
        if space_id.is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("Access denied: missing space_id");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let limit = input
            .limit
            .unwrap_or(DEFAULT_UPCOMING_LIMIT)
            .clamp(1, DEFAULT_UPCOMING_LIMIT);
        // A continued read keeps the window of its first page (see `UpcomingContinuation`).
        let (as_of, after) = match input.cursor.as_deref() {
            Some(token) => {
                let (as_of, after) = self.resume(ctx, &input, token)?;
                (as_of, Some(after))
            }
            None => (self.now(), None),
        };
        let mut events = self
            .repo
            .get_upcoming_after(
                &space_id,
                input.days,
                as_of,
                after.as_ref(),
                UPCOMING_SCAN_LIMIT,
            )
            .await
            .map_err(|e| McpError::ExecutionError(e.to_string()))?;
        // Hitting the scan ceiling means more rows may exist beyond what was read.
        let scan_exhausted = events.len() >= UPCOMING_SCAN_LIMIT;
        // Where the scan stopped: a page cut by the scan ceiling resumes after this row,
        // even when filtering left nothing of it to return. It may be a row the caller
        // cannot read, so it only ever leaves this server sealed.
        let last_scanned = events
            .last()
            .and_then(|event| Some((event.time_start?, event.base.id.clone())));
        if !input.include_candidates {
            events.retain(|event| event.status != EventStatus::Candidate);
        }
        // Page only after candidate and authorization filtering, so callers never get a
        // short page while more visible rows exist.
        let mut items = authorized_events(ctx, events).await?;
        let truncated = scan_exhausted || items.len() > limit;
        let resume_after = if items.len() > limit {
            items.truncate(limit);
            items
                .last()
                .and_then(|event| Some((event.time_start?, event.base.id.clone())))
        } else if truncated {
            last_scanned
        } else {
            None
        };
        let next_cursor = resume_after
            .map(|(after_start, after_id)| {
                let continuation = UpcomingContinuation {
                    as_of,
                    days: input.days,
                    include_candidates: input.include_candidates,
                    after_start,
                    after_id: after_id.to_string(),
                };
                let plaintext = serde_json::to_vec(&continuation)
                    .map_err(|e| McpError::ExecutionError(e.to_string()))?;
                ctx.continuation_key()
                    .seal(&upcoming_binding(ctx), &plaintext)
                    .ok_or_else(|| {
                        McpError::ExecutionError("could not seal the upcoming cursor".to_string())
                    })
            })
            .transpose()?;
        let output = ListUpcomingEventsOutput {
            items,
            truncated,
            next_cursor,
        };
        let response = ToolResponse::success(output);
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::too_many_lines
    )]

    use super::*;
    use elephant_policy::{Clearance, Principal};
    use elephant_types::records::assertion::{
        Assertion, AssertionRetryState, EvidenceSpan, ResolutionStatus,
    };
    use elephant_types::records::base::Security;
    use elephant_types::records::source::{Source, SourceType};
    use std::collections::HashSet;

    fn make_test_principal(space_id: &str, scopes: &[&str]) -> Principal {
        Principal {
            space_id: space_id.to_string(),
            scopes: scopes.iter().map(|scope| (*scope).to_string()).collect(),
            clearances: vec![Clearance {
                policy_id: "default".to_string(),
                max_level: 10,
                labels: HashSet::new(),
                handling_allow: HashSet::new(),
            }],
            subject_allowlist: None,
            purpose: HashSet::new(),
        }
    }

    async fn canonical_test_space(storage: &StorageClient) {
        let config: elephant_types::records::SpaceConfig = serde_json::from_value(json!({
            "id":RecordId::new_v4(TableName::SpaceConfig),"space_id":"default","created_at":chrono::Utc::now(),"updated_at":chrono::Utc::now(),"created_by":"test",
            "security":{"policy_id":"default","level":0,"labels":[],"handling":[]},"provenance":{},"subjects":[],"subjects_state":"complete","embedding_provider":"fake","embedding_model":"test","embedding_dims":8
        })).expect("space fixture");
        elephant_storage::SpaceConfigRepository::new(storage.clone())
            .create(config)
            .await
            .expect("space");
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn test_add_timeline_event_requires_evidence_refs() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        canonical_test_space(&storage).await;
        let ctx = ToolContext::new(
            storage.clone(),
            make_test_principal("default", &["write:event"]),
        );
        let handler = AddTimelineEventHandler::new(storage);

        let response = handler
            .execute(&ctx, json!({ "title": "Bare event" }))
            .await
            .expect("response");
        let decoded: ToolResponse<()> = serde_json::from_value(response).expect("decode");
        assert!(!decoded.ok);
        assert!(decoded.error.unwrap_or_default().contains("evidence"));
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn test_add_timeline_event_creates_event_from_source_ref() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        canonical_test_space(&storage).await;
        let source_repo = SourceRepository::new(storage.clone());
        let source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "default".to_string(),
                "tester".to_string(),
            ),
            SourceType::Manual,
            "sha256:test".to_string(),
        );
        let source = source_repo.create(source).await.expect("create source");

        let ctx = ToolContext::new(
            storage.clone(),
            make_test_principal("default", &["write:event", "read:event", "read:source"]),
        );
        let handler = AddTimelineEventHandler::new(storage.clone());

        let response = handler
            .execute(
                &ctx,
                json!({
                    "title": "Quarterly planning",
                    "source_refs": [source.base.id.to_string()],
                    "status": "approved"
                }),
            )
            .await
            .expect("response");
        let decoded: ToolResponse<Event> = serde_json::from_value(response).expect("decode");
        assert!(decoded.ok);
        let event = decoded.data.expect("event");
        assert_eq!(event.title, "Quarterly planning");
        assert_eq!(event.status, EventStatus::Approved);
        assert_eq!(event.source_refs.len(), 1);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn timeline_event_keeps_inherited_source_lineage_and_validates_final_fields() {
        for embedded in [true, false] {
            let storage = if embedded {
                StorageClient::new_embedded().await
            } else {
                StorageClient::new_memory().await
            }
            .expect("storage");
            if !embedded {
                storage
                    .query_checked(concat!(
                        include_str!("../../../migrations/sql/031_source_publication.surql"),
                        "\n",
                        include_str!(
                            "../../../migrations/sql/046_source_support_index_fence.surql"
                        )
                    ))
                    .await
                    .expect("canonical schema");
            }
            canonical_test_space(&storage).await;
            let repo = SourceRepository::new(storage.clone());
            let ancestor = Source::new(
                BaseFields::new(
                    RecordId::new_v4(TableName::Source),
                    "default".into(),
                    "tester".into(),
                ),
                SourceType::Manual,
                "ancestor-evidence".into(),
            );
            let ancestor = repo.create(ancestor).await.expect("ancestor");
            let mut source = Source::new(
                BaseFields::new(
                    RecordId::new_v4(TableName::Source),
                    "default".into(),
                    "tester".into(),
                ),
                SourceType::Manual,
                "direct-evidence".into(),
            );
            source.base.provenance.sources = vec![ancestor.base.id.clone()];
            let source = repo.create(source).await.expect("source");
            let ctx = ToolContext::new(
                storage.clone(),
                make_test_principal("default", &["read:event", "write:event", "read:source"]),
            );
            let handler = AddTimelineEventHandler::new(storage.clone());
            let response = handler
                .execute(
                    &ctx,
                    json!({
                        "title":"Derived event",
                        "source_refs":[source.base.id.to_string()],
                        "confidence":0.75,"time_confidence":0.5,
                        "time_start":"2026-01-01T10:00:00Z",
                        "time_end":"2026-01-01T11:00:00Z"
                    }),
                )
                .await
                .expect("event");
            let response: ToolResponse<Event> = serde_json::from_value(response).expect("decode");
            assert!(response.ok);
            let event = response.data.expect("record");
            assert_eq!(event.base.provenance.sources.len(), 2);
            assert!(event.base.provenance.sources.contains(&ancestor.base.id));
            assert!(event.base.provenance.sources.contains(&source.base.id));
            assert_eq!(event.source_refs, vec![Link::new(source.base.id.clone())]);
            assert_eq!(event.confidence.to_bits(), 0.75_f32.to_bits());
            assert_eq!(event.time_confidence.to_bits(), 0.5_f32.to_bits());
            for invalid in [
                json!({"confidence":1.5}),
                json!({"time_confidence":-0.1}),
                json!({"time_start":"2026-01-01T11:00:00Z","time_end":"2026-01-01T10:00:00Z"}),
            ] {
                let mut params = json!({
                    "title":"Invalid event",
                    "source_refs":[source.base.id.to_string()]
                });
                params
                    .as_object_mut()
                    .expect("params")
                    .extend(invalid.as_object().expect("fields").clone());
                assert!(matches!(
                    handler.execute(&ctx, params).await,
                    Err(McpError::InvalidParameters(_))
                ));
            }
            assert_eq!(
                Repository::list(&EventRepository::new(storage.clone()), "default", 100, None)
                    .await
                    .expect("events")
                    .len(),
                1
            );
            assert_eq!(
                elephant_storage::AuditLogRepository::new(storage.clone())
                    .list("default", 100, None)
                    .await
                    .expect("audits")
                    .len(),
                1
            );
            assert_eq!(
                elephant_storage::SystemEventRepository::new(storage)
                    .get_after_seq("default", 0, 100)
                    .await
                    .expect("outbox")
                    .len(),
                1
            );
        }
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn test_add_timeline_event_rejects_missing_source_ref() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        canonical_test_space(&storage).await;
        let ctx = ToolContext::new(
            storage.clone(),
            make_test_principal("default", &["write:event", "read:event", "read:source"]),
        );
        let handler = AddTimelineEventHandler::new(storage);

        let response = handler
            .execute(
                &ctx,
                json!({
                    "title": "Missing evidence",
                    "source_refs": ["source:550e8400-e29b-41d4-a716-446655440000"]
                }),
            )
            .await
            .expect("response");
        let decoded: ToolResponse<()> = serde_json::from_value(response).expect("decode");
        assert!(!decoded.ok);
        assert!(decoded.error.unwrap_or_default().contains("not found"));
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn test_add_timeline_event_rejects_missing_subject_entity() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        canonical_test_space(&storage).await;
        let source_repo = SourceRepository::new(storage.clone());
        let source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "default".to_string(),
                "tester".to_string(),
            ),
            SourceType::Manual,
            "sha256:test".to_string(),
        );
        let source = source_repo.create(source).await.expect("create source");

        let ctx = ToolContext::new(
            storage.clone(),
            make_test_principal("default", &["write:event", "read:source", "read:entity"]),
        );
        let handler = AddTimelineEventHandler::new(storage);

        let response = handler
            .execute(
                &ctx,
                json!({
                    "title": "Quarterly planning",
                    "source_refs": [source.base.id.to_string()],
                    "subject_ids": ["entity:550e8400-e29b-41d4-a716-446655440000"]
                }),
            )
            .await
            .expect("response");
        let decoded: ToolResponse<()> = serde_json::from_value(response).expect("decode");
        assert!(!decoded.ok);
        let error = decoded.error.unwrap_or_default();
        assert!(
            error.contains("subject") || error.contains("entity") || error.contains("not found"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn test_add_timeline_event_creates_event_from_assertion_ref() {
        use sha2::Digest;
        let storage = StorageClient::new_embedded().await.expect("storage");
        canonical_test_space(&storage).await;
        let repo = AssertionRepository::new(storage.clone());
        let doc = elephant_types::records::Doc {
            base: BaseFields::new(
                RecordId::new_v4(TableName::Doc),
                "default".into(),
                "tester".into(),
            ),
            title: "Meeting note".into(),
            text: "Alice met Bob".into(),
            source: None,
            sidecar_text: None,
            media_attachments: vec![],
            rendition_key: "timeline-fixture".into(),
            metadata: json!({}),
        };
        let doc = elephant_storage::DocRepository::new(storage.clone())
            .create(doc)
            .await
            .expect("document");
        let revision = elephant_types::records::DocRevision {
            base: BaseFields::new(
                RecordId::new_v4(TableName::DocRevision),
                "default".into(),
                "tester".into(),
            ),
            doc: Link::new(doc.base.id.clone()),
            text: doc.text.clone(),
            text_hash: format!("sha256:{:x}", sha2::Sha256::digest(doc.text.as_bytes())),
            renderer: elephant_types::records::doc_revision::RendererMetadata {
                name: "fixture".into(),
                version: "1".into(),
                config_hash: None,
            },
            manifest: None,
            supersedes: None,
            status: elephant_types::records::RevisionStatus::Active,
            rendered_at: chrono::Utc::now(),
        };
        let revision = elephant_storage::DocRevisionRepository::new(storage.clone())
            .create(revision)
            .await
            .expect("immutable evidence");
        let assertion = Assertion {
            base: BaseFields::new(
                RecordId::new_v4(TableName::Assertion),
                "default".to_string(),
                "tester".to_string(),
            ),
            subject_text: "Alice".to_string(),
            predicate_raw: "met".to_string(),
            predicate_id: None,
            object_text: Some("Bob".to_string()),
            object_literal: None,
            evidence: EvidenceSpan {
                revision: Link::new(revision.base.id.clone()),
                node: None,
                start: 0,
                end: 13,
                quote_hash: revision.text_hash.clone(),
            },
            source_event_time: None,
            observed_at: chrono::Utc::now(),
            asserted_at: chrono::Utc::now(),
            assertion_key: "assertion:alice-met-bob".to_string(),
            resolution_status: ResolutionStatus::Pending,
            resolution_method: None,
            resolution_confidence: None,
            retry_state: AssertionRetryState::default(),
            subject_entity: None,
            object_entity: None,
            confidence: 1.0,
            subject_entity_type: None,
            object_entity_type: None,
            significance: None,
            lifecycle_tier: "active".to_string(),
        };
        let assertion = repo.create(assertion).await.expect("create assertion");

        let ctx = ToolContext::new(
            storage.clone(),
            make_test_principal("default", &["write:event", "read:event", "read:assertion"]),
        );
        let handler = AddTimelineEventHandler::new(storage);

        let response = handler
            .execute(
                &ctx,
                json!({
                    "title": "Alice met Bob",
                    "assertion_refs": [assertion.base.id.to_string()],
                    "time_precision": "unknown"
                }),
            )
            .await
            .expect("response");
        let decoded: ToolResponse<Event> = serde_json::from_value(response).expect("decode");
        assert!(decoded.ok);
        let event = decoded.data.expect("event");
        assert_eq!(event.assertion_refs.len(), 1);
        assert_eq!(event.time_precision, TimePrecision::Unknown);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn test_add_timeline_event_rejects_assertion_ref_without_record_clearance() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        canonical_test_space(&storage).await;
        let repo = AssertionRepository::new(storage.clone());
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::Assertion),
            "default".to_string(),
            "tester".to_string(),
        );
        base.security = Security {
            policy_id: "default".to_string(),
            level: 5,
            labels: Vec::new(),
            handling: Vec::new(),
        };
        let assertion = Assertion {
            base,
            subject_text: "Alice".to_string(),
            predicate_raw: "met".to_string(),
            predicate_id: None,
            object_text: Some("Bob".to_string()),
            object_literal: None,
            evidence: EvidenceSpan {
                revision: Link::new(RecordId::new_v4(TableName::DocRevision)),
                node: None,
                start: 0,
                end: 10,
                quote_hash: "hash".to_string(),
            },
            source_event_time: None,
            observed_at: chrono::Utc::now(),
            asserted_at: chrono::Utc::now(),
            assertion_key: "assertion:alice-met-bob-secure".to_string(),
            resolution_status: ResolutionStatus::Pending,
            resolution_method: None,
            resolution_confidence: None,
            retry_state: AssertionRetryState::default(),
            subject_entity: None,
            object_entity: None,
            confidence: 1.0,
            subject_entity_type: None,
            object_entity_type: None,
            significance: None,
            lifecycle_tier: "active".to_string(),
        };
        let assertion = repo.create(assertion).await.expect("create assertion");

        let ctx = ToolContext::new(
            storage.clone(),
            Principal {
                space_id: "default".to_string(),
                scopes: ["write:event", "read:assertion"]
                    .into_iter()
                    .map(str::to_string)
                    .collect(),
                clearances: vec![Clearance {
                    policy_id: "default".to_string(),
                    max_level: 0,
                    labels: HashSet::new(),
                    handling_allow: HashSet::new(),
                }],
                subject_allowlist: None,
                purpose: HashSet::new(),
            },
        );
        let handler = AddTimelineEventHandler::new(storage);

        let response = handler
            .execute(
                &ctx,
                json!({
                    "title": "Alice met Bob",
                    "assertion_refs": [assertion.base.id.to_string()],
                }),
            )
            .await
            .expect("response");
        let decoded: ToolResponse<()> = serde_json::from_value(response).expect("decode");
        assert!(!decoded.ok);
        assert!(decoded.error.unwrap_or_default().contains("Access denied"));
    }

    /// Seeds `visible` events readable at clearance level 1 and `hidden` events at level 5,
    /// all starting in the next few hours. Hidden rows come first in time order so a
    /// cap-before-filter would starve the visible page.
    async fn seed_upcoming(storage: &StorageClient, visible: usize, hidden: usize) {
        seed_upcoming_with(storage, visible, hidden, |offset| {
            chrono::Duration::minutes(offset)
        })
        .await;
    }

    /// Seeds `hidden` events the test principal cannot read, then `visible` ones, each
    /// starting `start(n)` after now for its 1-based position `n`.
    async fn seed_upcoming_with(
        storage: &StorageClient,
        visible: usize,
        hidden: usize,
        start: impl Fn(i64) -> chrono::Duration,
    ) {
        let now = chrono::Utc::now();
        let rows: Vec<SeedRow> = (0..visible + hidden)
            .map(|index| {
                let offset = i64::try_from(index).expect("index") + 1;
                SeedRow {
                    title: format!("Event {index}"),
                    start: now + start(offset),
                    end: None,
                    precision: TimePrecision::Minute,
                    hidden: index < hidden,
                }
            })
            .collect();
        seed_rows(storage, &rows).await;
    }

    struct SeedRow {
        title: String,
        start: chrono::DateTime<chrono::Utc>,
        end: Option<chrono::DateTime<chrono::Utc>>,
        precision: TimePrecision,
        hidden: bool,
    }

    fn row(title: &str, start: &str, end: Option<&str>, precision: TimePrecision) -> SeedRow {
        SeedRow {
            title: title.to_string(),
            start: utc(start),
            end: end.map(utc),
            precision,
            hidden: false,
        }
    }

    fn utc(rfc3339: &str) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::parse_from_rfc3339(rfc3339)
            .expect("rfc3339")
            .with_timezone(&chrono::Utc)
    }

    /// Hidden rows are at clearance level 5, above the test principal's 1.
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture storage and repository owners alive through all authorization and pagination assertions"
    )]
    async fn seed_rows(storage: &StorageClient, rows: &[SeedRow]) {
        let repo = EventRepository::new(storage.clone());
        let source = SourceRepository::new(storage.clone())
            .create(Source::new(
                BaseFields::new(
                    RecordId::new_v4(TableName::Source),
                    "default".to_string(),
                    "tester".to_string(),
                ),
                SourceType::Manual,
                "upcoming-events-fixture".to_string(),
            ))
            .await
            .expect("create supporting source");
        for seed in rows {
            let mut base = BaseFields::new(
                RecordId::new_v4(TableName::Event),
                "default".to_string(),
                "tester".to_string(),
            );
            base.security = Security {
                policy_id: "default".to_string(),
                level: if seed.hidden { 5 } else { 0 },
                labels: Vec::new(),
                handling: Vec::new(),
            };
            let mut event = Event::new(
                base,
                seed.title.clone(),
                Vec::new(),
                vec![Link::new(source.base.id.clone())],
            )
            .expect("event");
            event.status = EventStatus::Approved;
            event.time_start = Some(seed.start);
            event.time_end = seed.end;
            event.time_precision = seed.precision;
            repo.create(event).await.expect("create event");
        }
    }

    fn upcoming_ctx(storage: &StorageClient) -> ToolContext {
        ToolContext::new(
            storage.clone(),
            Principal {
                space_id: "default".to_string(),
                scopes: std::iter::once("read:event".to_string()).collect(),
                clearances: vec![Clearance {
                    policy_id: "default".to_string(),
                    max_level: 1,
                    labels: HashSet::new(),
                    handling_allow: HashSet::new(),
                }],
                subject_allowlist: None,
                purpose: HashSet::new(),
            },
        )
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture storage and repository owners alive through all authorization and pagination assertions"
    )]
    async fn list_upcoming(storage: &StorageClient, params: Value) -> ListUpcomingEventsOutput {
        let handler = ListUpcomingEventsHandler::new(storage.clone());
        let response = handler
            .execute(&upcoming_ctx(storage), params)
            .await
            .expect("response");
        let decoded: ToolResponse<ListUpcomingEventsOutput> =
            serde_json::from_value(response).expect("decode");
        assert!(decoded.ok, "error: {:?}", decoded.error);
        decoded.data.expect("data")
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture storage and repository owners alive through all authorization and pagination assertions"
    )]
    async fn test_list_upcoming_caps_after_authorization() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        seed_upcoming(&storage, 40, 20).await;

        let output = list_upcoming(&storage, json!({})).await;
        assert_eq!(output.items.len(), 40);
        assert!(!output.truncated);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture storage and repository owners alive through all authorization and pagination assertions"
    )]
    async fn test_list_upcoming_reports_truncation() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        seed_upcoming(&storage, 55, 5).await;

        let output = list_upcoming(&storage, json!({})).await;
        assert_eq!(output.items.len(), DEFAULT_UPCOMING_LIMIT);
        assert!(output.truncated);
        // Ordered by start time: the earliest visible event comes first.
        assert_eq!(output.items[0].title, "Event 5");

        let output = list_upcoming(&storage, json!({ "limit": 10 })).await;
        assert_eq!(output.items.len(), 10);
        assert!(output.truncated);

        // A limit above the maximum is clamped to the default page size.
        let output = list_upcoming(&storage, json!({ "limit": 500 })).await;
        assert_eq!(output.items.len(), DEFAULT_UPCOMING_LIMIT);
        assert!(output.truncated);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture storage and repository owners alive through all authorization and pagination assertions"
    )]
    async fn test_list_upcoming_exact_fit_is_not_truncated() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        seed_upcoming(&storage, 12, 3).await;

        let output = list_upcoming(&storage, json!({ "limit": 12 })).await;
        assert_eq!(output.items.len(), 12);
        assert!(!output.truncated);
    }

    /// Reads a whole upcoming window by following `next_cursor`, returning every page.
    async fn read_all_upcoming(
        storage: &StorageClient,
        limit: usize,
    ) -> Vec<ListUpcomingEventsOutput> {
        let mut pages = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..100 {
            let mut params = json!({ "limit": limit });
            if let Some(cursor) = cursor.take() {
                params["cursor"] = json!(cursor);
            }
            let page = list_upcoming(storage, params).await;
            cursor.clone_from(&page.next_cursor);
            assert_eq!(page.truncated, page.next_cursor.is_some());
            pages.push(page);
            if cursor.is_none() {
                return pages;
            }
        }
        panic!("cursor never terminated");
    }

    fn titles(pages: &[ListUpcomingEventsOutput]) -> Vec<String> {
        pages
            .iter()
            .flat_map(|page| page.items.iter().map(|event| event.title.clone()))
            .collect()
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture storage and repository owners alive through all authorization and pagination assertions"
    )]
    async fn test_list_upcoming_cursor_reads_the_whole_window() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        seed_upcoming(&storage, 120, 5).await;

        let pages = read_all_upcoming(&storage, DEFAULT_UPCOMING_LIMIT).await;

        assert_eq!(
            pages
                .iter()
                .map(|page| page.items.len())
                .collect::<Vec<_>>(),
            vec![50, 50, 20]
        );
        let expected: Vec<String> = (5..125).map(|index| format!("Event {index}")).collect();
        assert_eq!(titles(&pages), expected);
        let last = pages.last().expect("last page");
        assert!(!last.truncated);
        assert!(last.next_cursor.is_none());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture storage and repository owners alive through all authorization and pagination assertions"
    )]
    async fn test_list_upcoming_cursor_splits_equal_start_times() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        // Every event starts at the same instant, so only the id orders them.
        seed_upcoming_with(&storage, 25, 0, |_| chrono::Duration::minutes(5)).await;

        let pages = read_all_upcoming(&storage, 10).await;

        assert_eq!(pages.len(), 3);
        let mut seen = titles(&pages);
        assert_eq!(seen.len(), 25);
        seen.sort();
        seen.dedup();
        assert_eq!(seen.len(), 25, "a row repeated across pages");
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture storage and repository owners alive through all authorization and pagination assertions"
    )]
    async fn test_list_upcoming_cursor_moves_past_a_scan_of_hidden_rows() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        // The earliest UPCOMING_SCAN_LIMIT rows are all unreadable to the caller.
        seed_upcoming_with(&storage, 10, UPCOMING_SCAN_LIMIT, |offset| {
            chrono::Duration::minutes(30) + chrono::Duration::seconds(offset)
        })
        .await;

        let first = list_upcoming(&storage, json!({})).await;
        assert!(first.items.is_empty());
        assert!(first.truncated);
        let cursor = first.next_cursor.expect("cursor past the scanned rows");

        let second = list_upcoming(&storage, json!({ "cursor": cursor })).await;
        let expected: Vec<String> = (UPCOMING_SCAN_LIMIT..UPCOMING_SCAN_LIMIT + 10)
            .map(|index| format!("Event {index}"))
            .collect();
        assert_eq!(titles(std::slice::from_ref(&second)), expected);
        assert!(!second.truncated);
        assert!(second.next_cursor.is_none());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture storage and repository owners alive through all authorization and pagination assertions"
    )]
    async fn test_list_upcoming_rejects_a_malformed_cursor() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let handler = ListUpcomingEventsHandler::new(storage.clone());
        for cursor in [
            "no-separator",
            "not-a-time|event:x",
            "2026-10-24T06:00:00Z|not-an-id",
            // The plaintext form of the first version of this cursor.
            "2026-10-24T06:00:00.000000000Z|event:00000000-0000-4000-8000-000000000000",
            "c1.",
            "c1.AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        ] {
            let result = handler
                .execute(&upcoming_ctx(&storage), json!({ "cursor": cursor }))
                .await;
            assert!(
                matches!(result, Err(McpError::InvalidParameters(_))),
                "{cursor}: {result:?}"
            );
        }
    }

    /// A context for the same space and clearance as `upcoming_ctx`, carrying a
    /// verified caller with subject `sub`.
    fn verified_ctx(storage: &StorageClient, sub: &str) -> ToolContext {
        let principal = upcoming_ctx(storage).principal;
        let now = chrono::Utc::now();
        let claims = elephant_auth::Claims {
            sub: sub.into(),
            exp: now.timestamp() + 3600,
            iat: now.timestamp(),
            iss: None,
            aud: None,
            space_id: Some(principal.space_id.clone()),
            spaces: None,
            bootstrap_space: None,
            scopes: principal.scopes.iter().cloned().collect(),
            security_level: 1,
            security_labels: Vec::new(),
            clearances: principal.clearances.clone(),
            subject_allowlist: None,
            purpose: Vec::new(),
        };
        let secret = "upcoming-cursor-test-secret-of-32-bytes";
        let token = jsonwebtoken::encode(
            &jsonwebtoken::Header::default(),
            &claims,
            &jsonwebtoken::EncodingKey::from_secret(secret.as_bytes()),
        )
        .expect("token");
        let caller = elephant_auth::LegacyVerifier {
            secret,
            algorithm: jsonwebtoken::Algorithm::HS256,
            audience: None,
            issuer: None,
            mappings: None,
        }
        .verify(&token)
        .expect("verified");
        ToolContext::new(storage.clone(), principal).with_verified_caller(Some(caller))
    }

    async fn upcoming_page(
        handler: &ListUpcomingEventsHandler,
        ctx: &ToolContext,
        params: Value,
    ) -> McpResult<ListUpcomingEventsOutput> {
        let response = handler.execute(ctx, params).await?;
        let decoded: ToolResponse<ListUpcomingEventsOutput> =
            serde_json::from_value(response).expect("decode");
        assert!(decoded.ok, "error: {:?}", decoded.error);
        Ok(decoded.data.expect("data"))
    }

    fn rejected(result: &McpResult<ListUpcomingEventsOutput>) -> bool {
        matches!(result, Err(McpError::InvalidParameters(_)))
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture storage and repository owners alive through all authorization and pagination assertions"
    )]
    async fn test_list_upcoming_cursor_progresses_over_mixed_pages() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        // The first scan reads 995 unreadable rows and 5 readable ones.
        seed_upcoming_with(&storage, 30, UPCOMING_SCAN_LIMIT - 5, |offset| {
            chrono::Duration::minutes(30) + chrono::Duration::seconds(offset)
        })
        .await;

        let pages = read_all_upcoming(&storage, DEFAULT_UPCOMING_LIMIT).await;

        assert_eq!(pages[0].items.len(), 5);
        assert!(pages[0].truncated);
        let expected: Vec<String> = (UPCOMING_SCAN_LIMIT - 5..UPCOMING_SCAN_LIMIT + 25)
            .map(|index| format!("Event {index}"))
            .collect();
        assert_eq!(titles(&pages), expected);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture storage and repository owners alive through all authorization and pagination assertions"
    )]
    async fn test_list_upcoming_cursor_reveals_no_position() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        // The scan ceiling cuts the page at an unreadable row: the cursor's
        // position is a row the caller may not see.
        seed_upcoming_with(&storage, 3, UPCOMING_SCAN_LIMIT, |offset| {
            chrono::Duration::minutes(30) + chrono::Duration::seconds(offset)
        })
        .await;
        let hidden = EventRepository::new(storage.clone())
            .get_upcoming_at("default", 7, chrono::Utc::now(), UPCOMING_SCAN_LIMIT)
            .await
            .expect("scan");
        let position = hidden.last().expect("last scanned row");

        let page = list_upcoming(&storage, json!({})).await;
        assert!(page.items.is_empty());
        let token = page.next_cursor.expect("cursor");

        let id = position.base.id.to_string();
        let uuid = id.rsplit(':').next().expect("uuid").to_string();
        let start = position.time_start.expect("start");
        let instant = start.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        let day = start.format("%Y-%m-%d").to_string();
        let clock = start.format("%H:%M").to_string();
        let raw = base64::Engine::decode(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD,
            token.strip_prefix("c1.").expect("prefix"),
        )
        .expect("base64url");
        for needle in [
            id.as_str(),
            uuid.as_str(),
            instant.as_str(),
            day.as_str(),
            clock.as_str(),
        ] {
            assert!(!token.contains(needle), "token shows {needle}");
            assert!(
                !raw.windows(needle.len()).any(|w| w == needle.as_bytes()),
                "decoded token shows {needle}"
            );
        }
        assert!(!token.contains('|') && !token.contains("event"));
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture storage and repository owners alive through all authorization and pagination assertions"
    )]
    async fn test_list_upcoming_cursor_is_bound_to_principal_space_and_window() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        seed_upcoming(&storage, 5, 0).await;
        let handler = ListUpcomingEventsHandler::new(storage.clone());
        let mint = |ctx: ToolContext, params: Value| {
            let handler = &handler;
            async move {
                upcoming_page(handler, &ctx, params)
                    .await
                    .expect("first page")
                    .next_cursor
                    .expect("cursor")
            }
        };

        // Another principal: a different clearance projection.
        let token = mint(upcoming_ctx(&storage), json!({ "limit": 2 })).await;
        let mut cleared = upcoming_ctx(&storage);
        cleared.principal.clearances[0].max_level = 5;
        let result =
            upcoming_page(&handler, &cleared, json!({ "limit": 2, "cursor": token })).await;
        assert!(rejected(&result), "{result:?}");

        // Another space.
        let mut elsewhere = upcoming_ctx(&storage);
        elsewhere.principal.space_id = "other".to_string();
        let result =
            upcoming_page(&handler, &elsewhere, json!({ "limit": 2, "cursor": token })).await;
        assert!(rejected(&result), "{result:?}");

        // Another verified subject with the same projection, and the unverified caller.
        let alice = mint(verified_ctx(&storage, "alice"), json!({ "limit": 2 })).await;
        let others = [verified_ctx(&storage, "bob"), upcoming_ctx(&storage)];
        for ctx in &others {
            let result = upcoming_page(&handler, ctx, json!({ "limit": 2, "cursor": alice })).await;
            assert!(rejected(&result), "{result:?}");
        }
        let same = upcoming_page(
            &handler,
            &verified_ctx(&storage, "alice"),
            json!({ "limit": 2, "cursor": alice }),
        )
        .await
        .expect("the same subject continues");
        assert_eq!(same.items.len(), 2);

        // Other window or filter parameters.
        let token = mint(upcoming_ctx(&storage), json!({ "limit": 2, "days": 7 })).await;
        for params in [
            json!({ "limit": 2, "days": 8, "cursor": token }),
            json!({ "limit": 2, "days": 7, "include_candidates": true, "cursor": token }),
        ] {
            let result = upcoming_page(&handler, &upcoming_ctx(&storage), params).await;
            assert!(rejected(&result), "{result:?}");
        }

        // Another server secret; the same secret on another instance opens it.
        let keyed = |secret: &[u8]| {
            upcoming_ctx(&storage).with_continuation_key(Arc::new(
                crate::continuation::ContinuationKey::from_secret(secret),
            ))
        };
        let token = mint(
            keyed(b"first server secret, 32 bytes long"),
            json!({ "limit": 2 }),
        )
        .await;
        let result = upcoming_page(
            &handler,
            &keyed(b"other server secret, 32 bytes long"),
            json!({ "limit": 2, "cursor": token }),
        )
        .await;
        assert!(rejected(&result), "{result:?}");
        upcoming_page(
            &handler,
            &keyed(b"first server secret, 32 bytes long"),
            json!({ "limit": 2, "cursor": token }),
        )
        .await
        .expect("an instance sharing the secret continues");
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture storage and repository owners alive through all authorization and pagination assertions"
    )]
    async fn test_list_upcoming_rejects_a_tampered_cursor() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        seed_upcoming(&storage, 5, 0).await;
        let handler = ListUpcomingEventsHandler::new(storage.clone());
        let ctx = upcoming_ctx(&storage);
        let token = upcoming_page(&handler, &ctx, json!({ "limit": 2 }))
            .await
            .expect("first page")
            .next_cursor
            .expect("cursor");

        let mut tampered = Vec::new();
        for at in [3, token.len() / 2, token.len() - 1] {
            let mut bytes = token.clone().into_bytes();
            bytes[at] = if bytes[at] == b'A' { b'B' } else { b'A' };
            tampered.push(String::from_utf8(bytes).expect("ascii"));
        }
        tampered.push(token[..token.len() - 2].to_string());
        tampered.push(format!("{token}AA"));
        for bad in tampered {
            let result = upcoming_page(&handler, &ctx, json!({ "limit": 2, "cursor": bad })).await;
            assert!(rejected(&result), "{bad}: {result:?}");
        }
    }

    /// A handler whose clock the test moves.
    fn clocked(
        storage: &StorageClient,
        start: &str,
    ) -> (
        ListUpcomingEventsHandler,
        Arc<std::sync::Mutex<chrono::DateTime<chrono::Utc>>>,
    ) {
        let now = Arc::new(std::sync::Mutex::new(utc(start)));
        let clock = Arc::clone(&now);
        let handler = ListUpcomingEventsHandler::new(storage.clone())
            .with_clock(move || *clock.lock().expect("clock"));
        (handler, now)
    }

    async fn page_titles(
        handler: &ListUpcomingEventsHandler,
        ctx: &ToolContext,
        params: Value,
    ) -> (Vec<String>, Option<String>) {
        let page = upcoming_page(handler, ctx, params).await.expect("page");
        let titles = page.items.iter().map(|event| event.title.clone()).collect();
        (titles, page.next_cursor)
    }

    #[tokio::test]
    async fn test_list_upcoming_continuation_keeps_its_first_page_window() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        // Read as of 23:50Z with a one-day window, then the clock moves 30 minutes.
        seed_rows(
            &storage,
            &[
                row(
                    "Anchor, long in progress",
                    "2031-03-09T12:00:00Z",
                    Some("2031-03-11T03:00:00Z"),
                    TimePrecision::Minute,
                ),
                // Day floor: 24 h before 23:50Z keeps today's 00:00Z anchor.
                row(
                    "Dated today",
                    "2031-03-10T00:00:00Z",
                    None,
                    TimePrecision::Day,
                ),
                // In progress until 00:00Z, ten minutes after the first page.
                row(
                    "Ends just after",
                    "2031-03-10T23:00:00Z",
                    Some("2031-03-11T00:00:00Z"),
                    TimePrecision::Minute,
                ),
                // Imminent: starts five minutes after the first page.
                row(
                    "Imminent",
                    "2031-03-10T23:55:00Z",
                    None,
                    TimePrecision::Minute,
                ),
                // Horizon: 23:40Z next day is inside, 00:10Z the day after is not.
                row(
                    "Last inside",
                    "2031-03-11T23:40:00Z",
                    None,
                    TimePrecision::Minute,
                ),
                row(
                    "Past the horizon",
                    "2031-03-12T00:10:00Z",
                    None,
                    TimePrecision::Minute,
                ),
            ],
        )
        .await;
        let (handler, now) = clocked(&storage, "2031-03-10T23:50:00Z");
        let ctx = upcoming_ctx(&storage);

        let (first, mut cursor) =
            page_titles(&handler, &ctx, json!({ "days": 1, "limit": 1 })).await;
        assert_eq!(first, vec!["Anchor, long in progress"]);

        *now.lock().expect("clock") = utc("2031-03-11T00:20:00Z");
        let mut rest = Vec::new();
        while let Some(token) = cursor.take() {
            let (titles, next) = page_titles(
                &handler,
                &ctx,
                json!({ "days": 1, "limit": 1, "cursor": token }),
            )
            .await;
            rest.extend(titles);
            cursor = next;
        }
        assert_eq!(
            rest,
            vec!["Dated today", "Ends just after", "Imminent", "Last inside"]
        );

        // Control: a fresh read at the moved clock has a different window, which
        // is exactly what a continuation must not drift into.
        let (fresh, _) = page_titles(&handler, &ctx, json!({ "days": 1, "limit": 50 })).await;
        assert_eq!(
            fresh,
            vec![
                "Anchor, long in progress",
                "Last inside",
                "Past the horizon"
            ]
        );
    }

    #[tokio::test]
    async fn test_list_upcoming_continuation_expires() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        seed_rows(
            &storage,
            &[
                row("First", "2031-03-10T12:00:00Z", None, TimePrecision::Minute),
                row(
                    "Second",
                    "2031-03-10T13:00:00Z",
                    None,
                    TimePrecision::Minute,
                ),
            ],
        )
        .await;
        let (handler, now) = clocked(&storage, "2031-03-10T10:00:00Z");
        let ctx = upcoming_ctx(&storage);
        let (_, cursor) = page_titles(&handler, &ctx, json!({ "limit": 1 })).await;
        let token = cursor.expect("cursor");

        *now.lock().expect("clock") = utc("2031-03-10T10:59:00Z");
        let (titles, _) = page_titles(
            &handler,
            &ctx,
            json!({ "limit": 1, "cursor": token.clone() }),
        )
        .await;
        assert_eq!(titles, vec!["Second"]);

        *now.lock().expect("clock") = utc("2031-03-10T11:01:00Z");
        let result = upcoming_page(&handler, &ctx, json!({ "limit": 1, "cursor": token })).await;
        assert!(rejected(&result), "{result:?}");
    }
}
