//! MCP server integration using the official rmcp SDK.

use std::collections::HashSet;
use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::header::{AUTHORIZATION, ORIGIN};
use axum::http::request::Parts;
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::Router;
use jsonwebtoken::{decode, DecodingKey, Validation};
use rmcp::handler::server::ServerHandler;
use rmcp::model::{
    CallToolRequestParams, CallToolResult, Content, Implementation, ListToolsResult,
    PaginatedRequestParams, ProtocolVersion, ServerCapabilities, ServerInfo, Tool,
};
use rmcp::service::RequestContext;
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use rmcp::RoleServer;
use serde_json::Value;

use elephant_mcp::{
    authz::AuthzService as McpAuthzService, create_agent_dispatcher_with_context,
    create_default_dispatcher_with_context, create_dispatcher_with_context,
    create_maintenance_dispatcher_with_context, handlers::system::visible_tool_names_for_principal,
    is_agent_tool, is_maintenance_tool, is_pipeline_tool, ToolContext,
};
use elephant_policy::{Clearance, Principal as AbacPrincipal};
use elephant_storage::StorageClient;

use crate::auth::{AuthConfig, AuthError, Claims, Principal as AuthPrincipal};
use crate::authz::AuthzService;
use crate::runtime::{RuntimeConfigPatch, RuntimeManager, RuntimeMode, RuntimeStatus};

const TOOL_GET_RUNTIME_STATUS: &str = "get_runtime_status";
const TOOL_INITIALIZE_RUNTIME: &str = "initialize_runtime";
const TOOL_GET_RUNTIME_CONFIG: &str = "get_runtime_config";
const TOOL_CONFIGURE_RUNTIME: &str = "configure_runtime";

#[derive(Clone, Copy)]
enum DispatcherProfile {
    Full,
    Default,
    Agent,
    Maintenance,
}

const FULL_ONLY_WRITE_TOOLS: &[&str] = &[
    "create_source",
    "create_source_simple",
    "create_entity_simple",
    "batch_entities",
    "relate_entities",
    "upsert_relationship",
    "update_relationship",
    "delete_relationship",
    "restore_relationship",
    "batch_relationships",
    "delete_doc",
    "restore_doc",
    "update_source",
    "delete_source",
    "restore_source",
    "create_space",
    "update_space",
    "create_reprocess_job",
    "delete_entity",
    "restore_entity",
];

#[cfg(test)]
const FULL_ONLY_MUTATION_PREFIXES: &[&str] = &[
    "create_", "batch_", "relate_", "upsert_", "update_", "delete_", "restore_",
];

fn is_semantic_ingest_tool(name: &str) -> bool {
    name.starts_with("ingest_") || name.starts_with("batch_ingest_")
}

fn is_write_mutation_tool(name: &str) -> bool {
    is_semantic_ingest_tool(name)
        || is_pipeline_tool(name)
        || (is_maintenance_tool(name) && !is_agent_tool(name))
        || FULL_ONLY_WRITE_TOOLS.contains(&name)
}

fn runtime_allows_tool(name: &str, status: &RuntimeStatus) -> bool {
    if !status.configured {
        return false;
    }
    if !status.semantic_ingest_ready && is_semantic_ingest_tool(name) {
        return false;
    }
    if matches!(status.runtime_mode, RuntimeMode::ReadOnly) && is_write_mutation_tool(name) {
        return false;
    }
    true
}

fn filtered_tools_for_dispatcher(
    dispatcher: &elephant_mcp::ToolDispatcher,
    status: &RuntimeStatus,
) -> Vec<Tool> {
    let registered: Vec<String> = dispatcher
        .all_tool_meta()
        .iter()
        .map(|meta| meta.name.clone())
        .collect();
    let visible: HashSet<String> = visible_tool_names_for_principal(
        dispatcher.context(),
        &registered,
        true,
        dispatcher.context().storage.is_embedded(),
    )
    .into_iter()
    .filter(|tool_name| runtime_allows_tool(tool_name, status))
    .collect();
    dispatcher
        .all_tool_meta()
        .into_iter()
        .filter(|meta| visible.contains(&meta.name))
        .map(tool_from_meta)
        .collect()
}

fn tool_from_meta(meta: elephant_mcp::ToolMeta) -> Tool {
    let input_schema = match meta.input_schema {
        Value::Object(map) => map,
        _ => serde_json::Map::new(),
    };

    Tool::new(meta.name, meta.description, input_schema)
}

/// Extract `X-Space-Id` header from MCP request context (dev mode override).
fn space_id_from_context(context: &RequestContext<RoleServer>, default: &str) -> String {
    context
        .extensions
        .get::<Parts>()
        .and_then(|parts| parts.headers.get("X-Space-Id"))
        .and_then(|h| h.to_str().ok())
        .filter(|s| !s.is_empty())
        .map_or_else(|| default.to_string(), String::from)
}

/// Shared MCP state.
#[derive(Debug)]
pub struct McpState {
    runtime: Arc<RuntimeManager>,
    auth: AuthConfig,
    authz: AuthzService,
    default_space_id: String,
    policy_ids: Vec<String>,
}

impl McpState {
    /// Creates MCP state with environment-driven defaults.
    #[must_use]
    pub fn new(storage: StorageClient, auth: AuthConfig) -> Self {
        Self::new_with_runtime(
            Arc::new(RuntimeManager::new_preconfigured(storage)),
            auth,
            std::env::var("MCP_SPACE_ID").unwrap_or_else(|_| "default".to_string()),
            std::env::var("MCP_POLICY_IDS").ok().map_or_else(
                || {
                    vec![
                        "default".to_string(),
                        "dev".to_string(),
                        "tenant_policy_v1".to_string(),
                    ]
                },
                |value| {
                    value
                        .split(',')
                        .map(|entry| entry.trim().to_string())
                        .filter(|entry| !entry.is_empty())
                        .collect()
                },
            ),
        )
    }

    #[must_use]
    pub fn new_managed(runtime: Arc<RuntimeManager>, auth: AuthConfig) -> Self {
        let default_space_id =
            std::env::var("MCP_SPACE_ID").unwrap_or_else(|_| "default".to_string());
        let policy_ids = std::env::var("MCP_POLICY_IDS").ok().map_or_else(
            || {
                vec![
                    "default".to_string(),
                    "dev".to_string(),
                    "tenant_policy_v1".to_string(),
                ]
            },
            |value| {
                value
                    .split(',')
                    .map(|entry| entry.trim().to_string())
                    .filter(|entry| !entry.is_empty())
                    .collect()
            },
        );

        Self::new_with_runtime(runtime, auth, default_space_id, policy_ids)
    }

    fn new_with_runtime(
        runtime: Arc<RuntimeManager>,
        auth: AuthConfig,
        default_space_id: String,
        policy_ids: Vec<String>,
    ) -> Self {
        Self {
            runtime,
            auth,
            authz: AuthzService::new(),
            default_space_id,
            policy_ids,
        }
    }

    /// Creates MCP state with explicit defaults (for testing, avoids env var races).
    #[cfg(test)]
    #[must_use]
    pub fn new_with_defaults(
        storage: StorageClient,
        auth: AuthConfig,
        default_space_id: String,
        policy_ids: Vec<String>,
    ) -> Self {
        Self::new_with_runtime(
            Arc::new(RuntimeManager::new_preconfigured(storage)),
            auth,
            default_space_id,
            policy_ids,
        )
    }
}

fn runtime_tool_schema() -> Arc<serde_json::Map<String, Value>> {
    Arc::new(serde_json::Map::new())
}

fn runtime_patch_schema() -> Arc<serde_json::Map<String, Value>> {
    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            "runtime_mode": {
                "type": "string",
                "enum": ["local_memory", "shared_memory", "read_only", "maintenance"]
            },
            "ingest_enabled": {"type": "boolean"},
            "backend": {"type": "string", "enum": ["elephant-db", "surreal-db"]},
            "snapshot_path": {"type": "string"},
            "surreal_url": {"type": "string"},
            "surreal_namespace": {"type": "string"},
            "surreal_database": {"type": "string"},
            "surreal_username": {"type": "string"},
            "surreal_password": {"type": "string"},
            "extractor_provider": {"type": "string"},
            "extractor_required": {"type": "boolean"},
            "extractor_mcp_url": {"type": "string"},
            "extractor_model": {"type": "string"},
            "extractor_timeout_secs": {"type": "integer", "minimum": 1},
            "multimodal_model": {"type": "string"},
            "multimodal_timeout_secs": {"type": "integer", "minimum": 1},
            "multimodal_max_render_concurrent": {"type": "integer", "minimum": 1},
            "multimodal_asset_root": {"type": "string"},
            "multimodal_inline_image_max_bytes": {"type": "integer", "minimum": 1},
            "multimodal_inline_video_max_bytes": {"type": "integer", "minimum": 1},
            "budget_daily_euros_retrieval": {"type": "number", "minimum": 0},
            "budget_daily_euros_graph": {"type": "number", "minimum": 0},
            "budget_daily_items_retrieval": {"type": "integer", "minimum": 0},
            "budget_daily_items_graph": {"type": "integer", "minimum": 0},
            "budget_per_item_tokens": {"type": "integer", "minimum": 1},
            "budget_per_item_wall_time_secs": {"type": "integer", "minimum": 1},
            "budget_downgrade_policy": {
                "type": "string",
                "enum": ["strict", "degrade_mode", "degrade_mode_with_queue", "alert_only"]
            }
        },
        "additionalProperties": false
    });

    match schema {
        Value::Object(map) => Arc::new(map),
        _ => runtime_tool_schema(),
    }
}

fn runtime_status_tool() -> Tool {
    Tool::new(
        TOOL_GET_RUNTIME_STATUS,
        "Get Elephant runtime readiness and bootstrap status.",
        runtime_tool_schema(),
    )
}

fn initialize_runtime_tool() -> Tool {
    Tool::new(
        TOOL_INITIALIZE_RUNTIME,
        "Initialize Elephant runtime config and start managed storage/workers.",
        runtime_patch_schema(),
    )
}

fn get_runtime_config_tool() -> Tool {
    Tool::new(
        TOOL_GET_RUNTIME_CONFIG,
        "Read the active Elephant runtime config with secrets redacted.",
        runtime_tool_schema(),
    )
}

fn configure_runtime_tool() -> Tool {
    Tool::new(
        TOOL_CONFIGURE_RUNTIME,
        "Patch Elephant runtime config and apply it live.",
        runtime_patch_schema(),
    )
}

fn runtime_json_result(
    value: Result<Value, serde_json::Error>,
) -> Result<CallToolResult, rmcp::ErrorData> {
    let value = value.map_err(|err| {
        rmcp::ErrorData::internal_error(
            format!("Failed to serialize runtime response: {err}"),
            None,
        )
    })?;
    let content_item =
        Content::json(value).unwrap_or_else(|_| Content::text("Failed to serialize tool response"));
    Ok(CallToolResult::success(vec![content_item]))
}

fn can_manage_runtime(state: &McpState, principal: Option<&AbacPrincipal>) -> bool {
    state.auth.skip_auth
        || principal.is_some_and(|principal| {
            principal.scopes.contains("*") || principal.scopes.contains("write:system")
        })
}

fn can_read_runtime_config(state: &McpState, principal: Option<&AbacPrincipal>) -> bool {
    state.auth.skip_auth
        || principal.is_some_and(|principal| {
            principal.scopes.contains("*")
                || principal.scopes.contains("read:system")
                || principal.scopes.contains("write:system")
        })
}

fn runtime_tools_for(
    state: &McpState,
    status: &RuntimeStatus,
    principal: Option<&AbacPrincipal>,
) -> Vec<Tool> {
    let mut tools = vec![runtime_status_tool()];
    if status.bootstrap_mode && can_manage_runtime(state, principal) {
        tools.push(initialize_runtime_tool());
    }
    if status.configured && can_read_runtime_config(state, principal) {
        tools.push(get_runtime_config_tool());
    }
    if status.configured && can_manage_runtime(state, principal) {
        tools.push(configure_runtime_tool());
    }
    tools
}

async fn handle_runtime_tool(
    state: &McpState,
    name: &str,
    params: Value,
    principal: Option<&AbacPrincipal>,
) -> Result<CallToolResult, rmcp::ErrorData> {
    match name {
        TOOL_GET_RUNTIME_STATUS => {
            let status = state.runtime.get_status().await;
            runtime_json_result(serde_json::to_value(status))
        }
        TOOL_INITIALIZE_RUNTIME => {
            if !can_manage_runtime(state, principal) {
                return Ok(CallToolResult::error(vec![Content::text(
                    "Runtime initialization is not authorized".to_string(),
                )]));
            }
            let patch: RuntimeConfigPatch = serde_json::from_value(params).map_err(|err| {
                rmcp::ErrorData::invalid_params(
                    format!("Invalid initialize_runtime params: {err}"),
                    None,
                )
            })?;
            let result = state
                .runtime
                .initialize_runtime(patch)
                .await
                .map_err(|err| {
                    rmcp::ErrorData::internal_error(
                        format!("Failed to initialize runtime: {err}"),
                        None,
                    )
                })?;
            let _refresh = state.runtime.clone().schedule_post_bind_readiness_refresh();
            runtime_json_result(serde_json::to_value(result))
        }
        TOOL_GET_RUNTIME_CONFIG => {
            if !can_read_runtime_config(state, principal) {
                return Ok(CallToolResult::error(vec![Content::text(
                    "Runtime config is not authorized".to_string(),
                )]));
            }
            let config = state.runtime.get_config_redacted().await;
            runtime_json_result(serde_json::to_value(config))
        }
        TOOL_CONFIGURE_RUNTIME => {
            if !can_manage_runtime(state, principal) {
                return Ok(CallToolResult::error(vec![Content::text(
                    "Runtime configuration is not authorized".to_string(),
                )]));
            }
            let patch: RuntimeConfigPatch = serde_json::from_value(params).map_err(|err| {
                rmcp::ErrorData::invalid_params(
                    format!("Invalid configure_runtime params: {err}"),
                    None,
                )
            })?;
            let result = state
                .runtime
                .configure_runtime(patch)
                .await
                .map_err(|err| {
                    rmcp::ErrorData::internal_error(
                        format!("Failed to configure runtime: {err}"),
                        None,
                    )
                })?;
            let _refresh = state.runtime.clone().schedule_post_bind_readiness_refresh();
            runtime_json_result(serde_json::to_value(result))
        }
        _ => Ok(CallToolResult::error(vec![Content::text(format!(
            "Tool not found: {name}"
        ))])),
    }
}

fn default_principal(state: &McpState) -> AbacPrincipal {
    let mut scopes = HashSet::new();
    scopes.insert("*".to_string());

    let clearances = state
        .policy_ids
        .iter()
        .map(|policy_id| {
            let mut handling_allow = HashSet::new();
            if policy_id == "tenant_policy_v1" {
                handling_allow.insert("handling:trusted_context".to_string());
            }
            Clearance {
                policy_id: policy_id.clone(),
                max_level: 100,
                labels: HashSet::new(),
                handling_allow,
            }
        })
        .collect();

    AbacPrincipal {
        space_id: state.default_space_id.clone(),
        scopes,
        clearances,
        subject_allowlist: None,
        purpose: HashSet::new(),
    }
}

fn bearer_from_context(context: &RequestContext<RoleServer>) -> Option<String> {
    // Server credentials are for outgoing worker requests only. An incoming
    // caller must present its own bearer token and retain its own principal.
    let parts = context.extensions.get::<Parts>()?;
    let header = parts.headers.get(AUTHORIZATION)?.to_str().ok()?;
    header
        .strip_prefix("Bearer ")
        .map(std::string::ToString::to_string)
}

fn decode_claims(state: &McpState, token: &str) -> Result<Claims, AuthError> {
    let key = DecodingKey::from_secret(state.auth.secret.as_bytes());
    let mut validation = Validation::new(state.auth.algorithm);
    if let Some(ref aud) = state.auth.audience {
        validation.set_audience(&[aud]);
    }
    if let Some(ref iss) = state.auth.issuer {
        validation.set_issuer(&[iss]);
    }

    let token_data = decode::<Claims>(token, &key, &validation).map_err(|e| {
        let (error, message) = match e.kind() {
            jsonwebtoken::errors::ErrorKind::ExpiredSignature => {
                ("expired_token", "Token has expired")
            }
            jsonwebtoken::errors::ErrorKind::InvalidToken => ("invalid_token", "Token is invalid"),
            jsonwebtoken::errors::ErrorKind::InvalidSignature => {
                ("invalid_token", "Token signature is invalid")
            }
            jsonwebtoken::errors::ErrorKind::InvalidAudience => {
                ("invalid_token", "Token audience is invalid")
            }
            jsonwebtoken::errors::ErrorKind::InvalidIssuer => {
                ("invalid_token", "Token issuer is invalid")
            }
            _ => ("invalid_token", "Token validation failed"),
        };

        AuthError {
            error: error.to_string(),
            message: message.to_string(),
        }
    })?;

    Ok(token_data.claims)
}

fn principal_from_context(
    state: &McpState,
    context: &RequestContext<RoleServer>,
) -> Result<AbacPrincipal, AuthError> {
    if state.auth.skip_auth {
        let mut principal = default_principal(state);
        principal.space_id = space_id_from_context(context, &state.default_space_id);
        return Ok(principal);
    }

    let token = bearer_from_context(context).ok_or_else(|| AuthError {
        error: "missing_token".to_string(),
        message: "Authorization header with Bearer token required".to_string(),
    })?;

    let claims = decode_claims(state, &token)?;
    let auth_principal = AuthPrincipal::from(claims);
    Ok(state.authz.to_abac_principal(&auth_principal))
}

async fn dispatcher_for(
    state: &McpState,
    principal: AbacPrincipal,
    profile: DispatcherProfile,
) -> Option<elephant_mcp::ToolDispatcher> {
    let storage = state.runtime.current_storage().await?;
    let authz = Arc::new(McpAuthzService::new_with_skip_auth(state.auth.skip_auth));
    let context = ToolContext::with_authz(storage, authz, principal);
    Some(match profile {
        DispatcherProfile::Full => create_dispatcher_with_context(context),
        DispatcherProfile::Default => create_default_dispatcher_with_context(context),
        DispatcherProfile::Agent => create_agent_dispatcher_with_context(context),
        DispatcherProfile::Maintenance => create_maintenance_dispatcher_with_context(context),
    })
}

fn runtime_tools_for_profile(
    state: &McpState,
    status: &RuntimeStatus,
    principal: Option<&AbacPrincipal>,
    profile: DispatcherProfile,
) -> Vec<Tool> {
    if matches!(profile, DispatcherProfile::Agent) {
        vec![runtime_status_tool()]
    } else {
        runtime_tools_for(state, status, principal)
    }
}

async fn list_tools_for_profile(
    state: &McpState,
    profile: DispatcherProfile,
    context: RequestContext<RoleServer>,
) -> Result<ListToolsResult, rmcp::ErrorData> {
    let principal = principal_from_context(state, &context).ok();
    // Readiness, not get_status: listing tools must never wait on a queue scan.
    let status = state.runtime.get_readiness().await;
    let mut tools = runtime_tools_for_profile(state, &status, principal.as_ref(), profile);
    if let Some(principal) = principal {
        if let Some(dispatcher) = dispatcher_for(state, principal, profile).await {
            tools.extend(filtered_tools_for_dispatcher(&dispatcher, &status));
        }
    }
    tools.sort_by(|left, right| left.name.cmp(&right.name));
    tools.dedup_by(|left, right| left.name == right.name);
    Ok(ListToolsResult::with_all_items(tools))
}

async fn call_tool_for_profile(
    state: &McpState,
    profile: DispatcherProfile,
    request: CallToolRequestParams,
    context: RequestContext<RoleServer>,
) -> Result<CallToolResult, rmcp::ErrorData> {
    let params = request
        .arguments
        .map_or_else(|| Value::Object(serde_json::Map::new()), Value::Object);
    let principal = principal_from_context(state, &context).ok();
    let runtime_tool_allowed = matches!(
        request.name.as_ref(),
        TOOL_GET_RUNTIME_STATUS
            | TOOL_INITIALIZE_RUNTIME
            | TOOL_GET_RUNTIME_CONFIG
            | TOOL_CONFIGURE_RUNTIME
    ) && (!matches!(profile, DispatcherProfile::Agent)
        || request.name == TOOL_GET_RUNTIME_STATUS);
    if runtime_tool_allowed {
        return handle_runtime_tool(state, &request.name, params, principal.as_ref()).await;
    }
    let Some(principal) = principal else {
        return Ok(CallToolResult::error(vec![Content::text(
            "Authorization header with Bearer token required".to_string(),
        )]));
    };
    // Readiness, not get_status: gating a tool call must never wait on a queue scan.
    let status = state.runtime.get_readiness().await;
    if !runtime_allows_tool(&request.name, &status) {
        return Ok(CallToolResult::error(vec![Content::text(format!(
            "Tool not available in current runtime state: {}",
            request.name
        ))]));
    }
    let Some(dispatcher) = dispatcher_for(state, principal, profile).await else {
        return Ok(CallToolResult::error(vec![Content::text(
            "Runtime storage is not configured".to_string(),
        )]));
    };

    match dispatcher.execute(&request.name, params).await {
        Ok(result) => {
            let content_item = Content::json(result)
                .unwrap_or_else(|_| Content::text("Failed to serialize tool response"));
            Ok(CallToolResult::success(vec![content_item]))
        }
        Err(err) => Ok(CallToolResult::error(vec![Content::text(err.to_string())])),
    }
}

fn mcp_transport_config(
    allowed_hosts: Option<&str>,
    allowed_origins: Option<&str>,
) -> StreamableHttpServerConfig {
    fn entries(value: &str) -> Vec<String> {
        value
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(str::to_owned)
            .collect()
    }

    let mut config = StreamableHttpServerConfig::default().with_stateful_mode(false);
    if let Some(hosts) = allowed_hosts {
        // Keep loopback access for Elephant's own extraction agents. An empty
        // setting must never disable the SDK's DNS rebinding protection.
        config.allowed_hosts.extend(entries(hosts));
    }
    if let Some(origins) = allowed_origins {
        config.allowed_origins = entries(origins);
    }
    config
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum McpOrigin {
    Null,
    Tuple {
        scheme: String,
        host: String,
        port: u16,
    },
}

impl McpOrigin {
    fn parse(value: &str) -> Option<Self> {
        let value = value.trim();
        if value == "null" {
            return Some(Self::Null);
        }
        let (scheme, authority) = value.split_once("://")?;
        let scheme = scheme.to_ascii_lowercase();
        let default_port = match scheme.as_str() {
            "http" => 80,
            "https" => 443,
            _ => return None,
        };
        // An Origin is an authority tuple, never a URL with credentials,
        // a path, query, or fragment. Authority parsing rejects the latter.
        if authority.contains('@') {
            return None;
        }
        let authority: axum::http::uri::Authority = authority.parse().ok()?;
        let host = authority.host();
        if host.is_empty() {
            return None;
        }
        // http::Authority::port_u16() also returns None for malformed ports.
        // Distinguish an absent port from a present but invalid one.
        let suffix = &authority.as_str()[host.len()..];
        let port = if suffix.is_empty() {
            default_port
        } else {
            let raw_port = suffix.strip_prefix(':')?;
            if raw_port.is_empty() || !raw_port.bytes().all(|byte| byte.is_ascii_digit()) {
                return None;
            }
            raw_port.parse().ok()?
        };
        let host = host.trim_start_matches('[').trim_end_matches(']');
        let host = host
            .parse::<std::net::IpAddr>()
            .map_or_else(|_| host.to_ascii_lowercase(), |address| address.to_string());
        Some(Self::Tuple { scheme, host, port })
    }
}

#[derive(Clone)]
struct McpOriginPolicy(Option<HashSet<McpOrigin>>);

impl McpOriginPolicy {
    fn new(origins: &[String]) -> Self {
        // A nonempty but invalid allowlist stays enabled and denies every
        // supplied Origin, rather than silently removing the restriction.
        Self((!origins.is_empty()).then(|| {
            origins
                .iter()
                .filter_map(|origin| McpOrigin::parse(origin))
                .collect()
        }))
    }
}

async fn enforce_mcp_origin(
    State(policy): State<McpOriginPolicy>,
    request: Request,
    next: Next,
) -> Response {
    if let Some(allowed) = policy.0.as_ref() {
        let mut origins = request.headers().get_all(ORIGIN).iter();
        if let Some(origin) = origins.next() {
            let parsed = origin.to_str().ok().and_then(McpOrigin::parse);
            let Some(origin) = parsed.filter(|_| origins.next().is_none()) else {
                return (StatusCode::BAD_REQUEST, "Invalid Origin header").into_response();
            };
            if !allowed.contains(&origin) {
                return (StatusCode::FORBIDDEN, "Origin header is not allowed").into_response();
            }
        }
    }
    next.run(request).await
}

fn mcp_profile_routes(
    state: Arc<McpState>,
    profile: DispatcherProfile,
    mut config: StreamableHttpServerConfig,
) -> Router {
    // rmcp 1.8 treats a configured origin without a port as allowing any
    // port. Own the exact normalized origin comparison here, and leave the
    // SDK's Host validation enabled. Avoid two disagreeing origin gates.
    let origins = McpOriginPolicy::new(&std::mem::take(&mut config.allowed_origins));
    let router = match profile {
        DispatcherProfile::Default => Router::new().route_service(
            "/mcp",
            StreamableHttpService::new(
                move || Ok(ElephantMcpDefaultServer::new(state.clone())),
                Arc::new(LocalSessionManager::default()),
                config,
            ),
        ),
        DispatcherProfile::Full => Router::new().route_service(
            "/mcp/full",
            StreamableHttpService::new(
                move || Ok(ElephantMcpServer::new(state.clone())),
                Arc::new(LocalSessionManager::default()),
                config,
            ),
        ),
        DispatcherProfile::Agent => Router::new().route_service(
            "/mcp/agent",
            StreamableHttpService::new(
                move || Ok(ElephantMcpAgentServer::new(state.clone())),
                Arc::new(LocalSessionManager::default()),
                config,
            ),
        ),
        DispatcherProfile::Maintenance => Router::new().route_service(
            "/mcp/maintenance",
            StreamableHttpService::new(
                move || Ok(ElephantMcpMaintenanceServer::new(state.clone())),
                Arc::new(LocalSessionManager::default()),
                config,
            ),
        ),
    };
    router.layer(middleware::from_fn_with_state(origins, enforce_mcp_origin))
}

fn configured_mcp_routes(state: Arc<McpState>, profile: DispatcherProfile) -> Router {
    let config = mcp_transport_config(
        std::env::var("MCP_ALLOWED_HOSTS").ok().as_deref(),
        std::env::var("MCP_ALLOWED_ORIGINS").ok().as_deref(),
    );
    mcp_profile_routes(state, profile, config)
}

/// Returns the MCP router mounted at `/mcp`.
///
/// This endpoint exposes the end-user default tool profile (~13 tools)
/// for external LLMs that need to ingest, search, navigate, and inspect provenance.
pub fn mcp_routes(state: Arc<McpState>) -> Router {
    configured_mcp_routes(state, DispatcherProfile::Default)
}

/// Returns the MCP router for full tool access at `/mcp/full`.
///
/// This endpoint is intended for operators and internal tooling.
pub fn mcp_full_routes(state: Arc<McpState>) -> Router {
    configured_mcp_routes(state, DispatcherProfile::Full)
}

/// Returns the MCP router for agent profile tools at `/mcp/agent`.
///
/// Per spec §8.3, this endpoint exposes only read-only tools for extraction agents:
/// - `search_entities`, `get_entity`, `get_entity_context_bundle`
/// - `list_predicates`, `get_predicate`
/// - `search_docs`, `get_doc`, `list_doc_chunks`, `get_doc_chunk`
/// - `get_source`
/// - `list_assertions`, `get_assertion`, `search_assertions`
/// - `list_truth_slots`
/// - `search_timeline`, `search_all`
/// - `get_health`, `get_capabilities`
pub fn mcp_agent_routes(state: Arc<McpState>) -> Router {
    configured_mcp_routes(state, DispatcherProfile::Agent)
}

#[derive(Clone)]
struct ElephantMcpServer {
    state: Arc<McpState>,
}

impl ElephantMcpServer {
    const fn new(state: Arc<McpState>) -> Self {
        Self { state }
    }
}

impl ServerHandler for ElephantMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_protocol_version(ProtocolVersion::LATEST)
            .with_server_info(Implementation::new("elephant", env!("CARGO_PKG_VERSION")))
            .with_instructions("Elephant MCP server: entity, relationship, docs, assertion, truth-slot, and system tools.")
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, rmcp::ErrorData> {
        list_tools_for_profile(&self.state, DispatcherProfile::Full, context).await
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        call_tool_for_profile(&self.state, DispatcherProfile::Full, request, context).await
    }
}

/// MCP server for end-user LLMs with the default tool profile.
///
/// Exposes ~13 tools for ingest, search, navigate, inspect, and browse.
#[derive(Clone)]
struct ElephantMcpDefaultServer {
    state: Arc<McpState>,
}

impl ElephantMcpDefaultServer {
    const fn new(state: Arc<McpState>) -> Self {
        Self { state }
    }
}

impl ServerHandler for ElephantMcpDefaultServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_protocol_version(ProtocolVersion::LATEST)
            .with_server_info(Implementation::new("elephant", env!("CARGO_PKG_VERSION")))
            .with_instructions("Elephant MCP server: entity, relationship, docs, assertion, truth-slot, and system tools.")
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, rmcp::ErrorData> {
        list_tools_for_profile(&self.state, DispatcherProfile::Default, context).await
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        call_tool_for_profile(&self.state, DispatcherProfile::Default, request, context).await
    }
}

/// MCP server for extraction agents with restricted tool access.
///
/// Per spec §8.3, this server only exposes read-only tools (~18 tools instead of ~60).
#[derive(Clone)]
struct ElephantMcpAgentServer {
    state: Arc<McpState>,
}

impl ElephantMcpAgentServer {
    const fn new(state: Arc<McpState>) -> Self {
        Self { state }
    }
}

impl ServerHandler for ElephantMcpAgentServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_protocol_version(ProtocolVersion::LATEST)
            .with_server_info(Implementation::new(
                "elephant-agent",
                env!("CARGO_PKG_VERSION"),
            ))
            .with_instructions(
                "Elephant MCP agent server: read-only tools for extraction agents (§8.3).",
            )
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, rmcp::ErrorData> {
        list_tools_for_profile(&self.state, DispatcherProfile::Agent, context).await
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        call_tool_for_profile(&self.state, DispatcherProfile::Agent, request, context).await
    }
}

/// Returns the MCP router for maintenance profile tools at `/mcp/maintenance`.
///
/// This endpoint exposes all Agent read-only tools plus identity resolution,
/// predicate management, and entity update tools (~24 tools).
pub fn mcp_maintenance_routes(state: Arc<McpState>) -> Router {
    configured_mcp_routes(state, DispatcherProfile::Maintenance)
}

/// MCP server for maintenance agents with identity/predicate/entity write access.
///
/// Exposes all Agent read tools plus identity resolution, predicate management,
/// and entity update tools (~24 tools instead of ~60).
#[derive(Clone)]
struct ElephantMcpMaintenanceServer {
    state: Arc<McpState>,
}

impl ElephantMcpMaintenanceServer {
    const fn new(state: Arc<McpState>) -> Self {
        Self { state }
    }
}

impl ServerHandler for ElephantMcpMaintenanceServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_protocol_version(ProtocolVersion::LATEST)
            .with_server_info(Implementation::new("elephant-maintenance", env!("CARGO_PKG_VERSION")))
            .with_instructions("Elephant MCP maintenance server: agent read tools plus identity resolution, predicate management, and entity updates.")
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, rmcp::ErrorData> {
        list_tools_for_profile(&self.state, DispatcherProfile::Maintenance, context).await
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        call_tool_for_profile(
            &self.state,
            DispatcherProfile::Maintenance,
            request,
            context,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::mixed_attributes_style,
        clippy::similar_names
    )]

    use super::*;
    use chrono::Utc;
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
    use rmcp::model::CallToolRequestParams;
    use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
    use rmcp::transport::StreamableHttpClientTransport;
    use rmcp::ServiceExt;
    use serde_json::Value;
    use std::collections::HashSet;
    use std::str::FromStr;
    use std::sync::{Mutex, OnceLock};

    use elephant_types::ids::{RecordId, TableName};
    use elephant_types::records::base::{BaseFields, Security};
    use elephant_types::records::doc::Doc;
    use elephant_types::records::entity::{Entity, EntityType};
    use elephant_types::records::source::{Source, SourceType};
    use elephant_types::Link;

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn make_claims(space_id: Option<&str>, scopes: &[&str]) -> Claims {
        Claims {
            sub: "mcp-test".to_string(),
            exp: Utc::now().timestamp() + 3600,
            iat: Utc::now().timestamp(),
            iss: None,
            aud: None,
            space_id: space_id.map(std::string::ToString::to_string),
            scopes: scopes.iter().map(|scope| (*scope).to_string()).collect(),
            security_level: 0,
            security_labels: Vec::new(),
            clearances: vec![Clearance {
                policy_id: "default".to_string(),
                max_level: 100,
                labels: HashSet::new(),
                handling_allow: HashSet::new(),
            }],
            subject_allowlist: None,
            purpose: Vec::new(),
        }
    }

    fn looks_like_full_only_mutation_tool(name: &str) -> bool {
        FULL_ONLY_MUTATION_PREFIXES
            .iter()
            .any(|prefix| name.starts_with(prefix))
            && !is_semantic_ingest_tool(name)
            && !is_maintenance_tool(name)
            && !is_pipeline_tool(name)
    }

    #[test]
    fn test_mcp_transport_config_keeps_loopback_protection() {
        let defaults = mcp_transport_config(None, None);
        let blank = mcp_transport_config(Some(" , "), None);
        assert_eq!(blank.allowed_hosts, defaults.allowed_hosts);
        assert!(!defaults.allowed_hosts.is_empty());
        assert!(!defaults.stateful_mode);

        let configured = mcp_transport_config(
            Some(" elephant.example.com, 192.168.1.20:3000 "),
            Some("https://app.example.com"),
        );
        assert!(configured.allowed_hosts.contains(&"127.0.0.1".to_string()));
        assert!(configured
            .allowed_hosts
            .contains(&"elephant.example.com".to_string()));
        assert!(configured
            .allowed_hosts
            .contains(&"192.168.1.20:3000".to_string()));
        assert_eq!(configured.allowed_origins, ["https://app.example.com"]);
    }

    #[test]
    fn test_mcp_origin_normalization_rejects_non_origins() {
        for invalid in [
            "",
            "https://app.example.com/",
            "https://app.example.com/path",
            "https://app.example.com?query",
            "https://app.example.com#fragment",
            "https://user@app.example.com",
            "https://app.example.com:",
            "https://app.example.com:not-a-port",
            "https://app.example.com:65536",
            "https://app.example.com:443 https://other.example.com",
            "file://app.example.com",
        ] {
            assert_eq!(McpOrigin::parse(invalid), None, "origin {invalid}");
        }
        assert_eq!(
            McpOrigin::parse("http://[0:0:0:0:0:0:0:1]"),
            McpOrigin::parse("http://[::1]:80")
        );
        assert_eq!(McpOrigin::parse("null"), Some(McpOrigin::Null));
    }

    const MCP_ORIGIN_CASES: &[(&str, Option<&str>, StatusCode)] = &[
        (
            "https://app.example.com",
            Some("https://app.example.com"),
            StatusCode::OK,
        ),
        (
            "https://app.example.com",
            Some("https://app.example.com:443"),
            StatusCode::OK,
        ),
        (
            "https://app.example.com:443",
            Some("https://APP.EXAMPLE.COM"),
            StatusCode::OK,
        ),
        (
            "http://app.example.com:80",
            Some("http://app.example.com"),
            StatusCode::OK,
        ),
        (
            "http://app.example.com",
            Some("http://app.example.com:80"),
            StatusCode::OK,
        ),
        (
            "https://app.example.com",
            Some("https://app.example.com:8443"),
            StatusCode::FORBIDDEN,
        ),
        (
            "https://app.example.com:8443",
            Some("https://app.example.com"),
            StatusCode::FORBIDDEN,
        ),
        (
            "https://app.example.com:8443",
            Some("https://app.example.com:8443"),
            StatusCode::OK,
        ),
        (
            "https://app.example.com",
            Some("http://app.example.com"),
            StatusCode::FORBIDDEN,
        ),
        (
            "https://app.example.com",
            Some("https://other.example.com"),
            StatusCode::FORBIDDEN,
        ),
        ("https://app.example.com", None, StatusCode::OK),
        (
            "https://app.example.com",
            Some("null"),
            StatusCode::FORBIDDEN,
        ),
        ("null", Some("null"), StatusCode::OK),
        (
            "not-an-origin",
            Some("https://app.example.com"),
            StatusCode::FORBIDDEN,
        ),
        (
            "https://app.example.com",
            Some("https://app.example.com/path"),
            StatusCode::BAD_REQUEST,
        ),
        (
            "https://app.example.com",
            Some("https://app.example.com:65536"),
            StatusCode::BAD_REQUEST,
        ),
    ];

    #[tokio::test]
    async fn test_mcp_profiles_enforce_exact_origins() {
        use axum::body::Body;
        use tower::ServiceExt;

        let storage = StorageClient::new_memory().await.expect("storage");
        let state = Arc::new(McpState::new_with_defaults(
            storage,
            AuthConfig {
                secret: "test-secret".to_string(),
                algorithm: Algorithm::HS256,
                audience: None,
                issuer: None,
                skip_auth: true,
            },
            "default".to_string(),
            vec!["default".to_string()],
        ));
        for (profile, path) in [
            (DispatcherProfile::Default, "/mcp"),
            (DispatcherProfile::Full, "/mcp/full"),
            (DispatcherProfile::Agent, "/mcp/agent"),
            (DispatcherProfile::Maintenance, "/mcp/maintenance"),
        ] {
            for &(allowed, supplied, expected) in MCP_ORIGIN_CASES {
                let router = mcp_profile_routes(
                    state.clone(),
                    profile,
                    mcp_transport_config(None, Some(allowed)),
                );
                let mut request = axum::http::Request::builder()
                    .method("POST")
                    .uri(path)
                    .header("Host", "localhost:3000")
                    .header("Content-Type", "application/json")
                    .header("Accept", "application/json, text/event-stream");
                if let Some(origin) = supplied {
                    request = request.header(ORIGIN, origin);
                }
                let response = router
                    .oneshot(request.body(Body::from(
                        r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}"#,
                    )).unwrap())
                    .await
                    .unwrap();
                assert_eq!(
                    response.status(),
                    expected,
                    "route {path}, allow {allowed}, Origin {supplied:?}"
                );
            }
            let router = mcp_profile_routes(
                state.clone(),
                profile,
                mcp_transport_config(None, Some("https://app.example.com")),
            );
            let duplicated_origin = axum::http::Request::builder()
                .method("POST")
                .uri(path)
                .header("Host", "localhost:3000")
                .header(ORIGIN, "https://app.example.com")
                .header(ORIGIN, "https://attacker.example.com")
                .body(Body::empty())
                .unwrap();
            let response = router.oneshot(duplicated_origin).await.unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "route {path}");
        }
    }

    #[tokio::test]
    async fn test_mcp_profiles_reject_untrusted_host() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        let storage = StorageClient::new_memory().await.expect("storage");
        let state = Arc::new(McpState::new_with_defaults(
            storage,
            AuthConfig {
                secret: "test-secret".to_string(),
                algorithm: Algorithm::HS256,
                audience: None,
                issuer: None,
                skip_auth: true,
            },
            "default".to_string(),
            vec!["default".to_string()],
        ));
        let router = mcp_routes(state.clone())
            .merge(mcp_full_routes(state.clone()))
            .merge(mcp_agent_routes(state.clone()))
            .merge(mcp_maintenance_routes(state));

        for path in ["/mcp", "/mcp/full", "/mcp/agent", "/mcp/maintenance"] {
            let request = Request::builder()
                .method("POST")
                .uri(path)
                .header("Host", "rebound.attacker.invalid")
                .header("Content-Type", "application/json")
                .header("Accept", "application/json, text/event-stream")
                .body(Body::from(
                    r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}"#,
                ))
                .unwrap();
            let response = router.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::FORBIDDEN, "route {path}");
        }
    }

    #[tokio::test]
    async fn test_default_principal_includes_clearances() {
        let prior_policy_ids = std::env::var("MCP_POLICY_IDS").ok();
        let prior_space = std::env::var("MCP_SPACE_ID").ok();
        std::env::set_var("MCP_POLICY_IDS", "default,dev");
        std::env::set_var("MCP_SPACE_ID", "test_space");

        let storage = StorageClient::new_memory().await.expect("storage");
        let auth = AuthConfig {
            secret: "test-secret".to_string(),
            algorithm: Algorithm::HS256,
            audience: None,
            issuer: None,
            skip_auth: true,
        };
        let state = Arc::new(McpState::new(storage, auth));
        let principal = default_principal(&state);
        assert_eq!(principal.space_id, "test_space");
        assert!(principal.scopes.contains("*"));
        assert_eq!(principal.clearances.len(), 2);
        assert!(principal
            .clearances
            .iter()
            .any(|c| c.policy_id == "default"));
        assert!(principal.clearances.iter().any(|c| c.policy_id == "dev"));

        match prior_policy_ids {
            Some(value) => std::env::set_var("MCP_POLICY_IDS", value),
            None => std::env::remove_var("MCP_POLICY_IDS"),
        }
        match prior_space {
            Some(value) => std::env::set_var("MCP_SPACE_ID", value),
            None => std::env::remove_var("MCP_SPACE_ID"),
        }
    }

    #[tokio::test]
    async fn test_tool_mapping_keeps_schema() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let auth = AuthConfig {
            secret: "test-secret".to_string(),
            algorithm: Algorithm::HS256,
            audience: None,
            issuer: None,
            skip_auth: true,
        };
        let state = Arc::new(McpState::new(storage, auth));
        let _server = ElephantMcpServer::new(state);

        let schema = serde_json::json!({
            "type": "object",
            "properties": { "space_id": { "type": "string" } }
        });
        let meta = elephant_mcp::ToolMeta {
            name: "search_entities".to_string(),
            description: "Search".to_string(),
            owner: "core".to_string(),
            input_schema: schema.clone(),
        };

        let tool = tool_from_meta(meta);
        assert_eq!(tool.name.as_ref(), "search_entities");
        let map = schema.as_object().expect("schema map");
        assert_eq!(tool.input_schema.get("type"), map.get("type"));
    }

    #[tokio::test]
    async fn test_mcp_http_list_tools() -> Result<(), Box<dyn std::error::Error>> {
        let storage = StorageClient::new_memory().await?;
        let auth = AuthConfig {
            secret: "test-secret".to_string(),
            algorithm: Algorithm::HS256,
            audience: None,
            issuer: None,
            skip_auth: true,
        };
        // Use explicit defaults to avoid env var race conditions with other tests
        let state = Arc::new(McpState::new_with_defaults(
            storage,
            auth,
            "default".to_string(),
            vec![
                "default".to_string(),
                "dev".to_string(),
                "tenant_policy_v1".to_string(),
            ],
        ));
        let router = mcp_routes(state);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let server_handle = tokio::spawn(async move {
            let _ = axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await;
        });

        let transport = StreamableHttpClientTransport::from_uri(format!("http://{addr}/mcp"));
        let client = ().serve(transport).await?;
        let tools = client.list_all_tools().await?;
        client.cancel().await?;

        let _ = shutdown_tx.send(());
        let _ = server_handle.await;

        let tool_names: Vec<&str> = tools.iter().map(|t| t.name.as_ref()).collect();
        let expected_count = elephant_mcp::DEFAULT_TOOLS.len() + 3;
        assert_eq!(
            tools.len(),
            expected_count,
            "Expected /mcp to expose exactly {} default profile tools, found {}. Tools: {:?}",
            expected_count,
            tools.len(),
            tool_names
        );
        assert!(
            tool_names.contains(&TOOL_GET_RUNTIME_STATUS),
            "Expected /mcp to include get_runtime_status"
        );
        assert!(
            tool_names.contains(&"search_entities"),
            "Expected /mcp to include search_entities"
        );
        assert!(
            tool_names.contains(&"ingest_doc"),
            "Expected /mcp to include ingest_doc"
        );
        assert!(
            tool_names.contains(&"get_provenance"),
            "Expected /mcp to include get_provenance"
        );
        assert!(
            !tool_names.contains(&"batch_entities"),
            "Expected /mcp (default profile) to exclude batch_entities"
        );
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn test_mcp_maintenance_list_tools_hides_snapshot_tools_for_space_principal(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let claims = make_claims(Some("default"), &["read:system", "read:entity"]);
        let token = encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(b"test-secret"),
        )?;

        let storage = StorageClient::new_memory().await?;
        let auth = AuthConfig {
            secret: "test-secret".to_string(),
            algorithm: Algorithm::HS256,
            audience: None,
            issuer: None,
            skip_auth: false,
        };
        let state = Arc::new(McpState::new_with_defaults(
            storage,
            auth,
            "default".to_string(),
            vec!["default".to_string()],
        ));
        let router = mcp_maintenance_routes(state);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let server_handle = tokio::spawn(async move {
            let _ = axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await;
        });

        let transport = StreamableHttpClientTransport::from_config(
            StreamableHttpClientTransportConfig::with_uri(format!("http://{addr}/mcp/maintenance"))
                .auth_header(token),
        );
        let client = ().serve(transport).await?;
        let tools = client.list_all_tools().await?;
        client.cancel().await?;

        let _ = shutdown_tx.send(());
        let _ = server_handle.await;

        assert!(tools.iter().any(|tool| tool.name == "get_entity"));
        assert!(
            !tools.iter().any(|tool| tool.name == "export_snapshot"),
            "space-scoped principal should not see export_snapshot"
        );
        assert!(
            !tools.iter().any(|tool| tool.name == "import_snapshot"),
            "space-scoped principal should not see import_snapshot"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_full_only_write_tools_cover_mutating_full_profile_tools() {
        let expected: HashSet<&str> = FULL_ONLY_WRITE_TOOLS.iter().copied().collect();
        let storage = StorageClient::new_memory()
            .await
            .expect("in-memory storage");
        let state = Arc::new(McpState::new_with_defaults(
            storage.clone(),
            AuthConfig {
                secret: "test-secret".to_string(),
                algorithm: Algorithm::HS256,
                audience: None,
                issuer: None,
                skip_auth: true,
            },
            "default".to_string(),
            vec!["default".to_string()],
        ));
        let context = ToolContext::new(storage, default_principal(&state));
        let dispatcher = create_dispatcher_with_context(context);

        let tool_meta = dispatcher.all_tool_meta();
        let actual: HashSet<&str> = tool_meta
            .iter()
            .map(|tool| tool.name.as_str())
            .filter(|name| !is_agent_tool(name))
            .filter(|name| !is_maintenance_tool(name))
            .filter(|name| !is_pipeline_tool(name))
            .filter(|name| looks_like_full_only_mutation_tool(name))
            .collect();

        assert_eq!(
            actual, expected,
            "FULL_ONLY_WRITE_TOOLS drifted from the mutating full-profile tool set"
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn test_mcp_default_list_tools_hides_global_health_for_space_principal(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let claims = make_claims(Some("default"), &["read:doc:metadata", "read:entity"]);
        let token = encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(b"test-secret"),
        )?;

        let storage = StorageClient::new_memory().await?;
        let auth = AuthConfig {
            secret: "test-secret".to_string(),
            algorithm: Algorithm::HS256,
            audience: None,
            issuer: None,
            skip_auth: false,
        };
        let state = Arc::new(McpState::new_with_defaults(
            storage,
            auth,
            "default".to_string(),
            vec!["default".to_string()],
        ));
        let router = mcp_routes(state);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let server_handle = tokio::spawn(async move {
            let _ = axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await;
        });

        let transport = StreamableHttpClientTransport::from_config(
            StreamableHttpClientTransportConfig::with_uri(format!("http://{addr}/mcp"))
                .auth_header(token),
        );
        let client = ().serve(transport).await?;
        let tools = client.list_all_tools().await?;
        client.cancel().await?;

        let _ = shutdown_tx.send(());
        let _ = server_handle.await;

        assert!(tools.iter().any(|tool| tool.name == "get_entity"));
        assert!(
            !tools.iter().any(|tool| tool.name == "get_health"),
            "space-scoped principal should not see get_health"
        );

        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock, clippy::too_many_lines)]
    async fn incoming_mcp_requests_never_inherit_server_worker_credentials(
    ) -> Result<(), Box<dyn std::error::Error>> {
        struct RestoreToken(Option<String>);
        impl Drop for RestoreToken {
            fn drop(&mut self) {
                match &self.0 {
                    Some(value) => std::env::set_var("MCP_BEARER_TOKEN", value),
                    None => std::env::remove_var("MCP_BEARER_TOKEN"),
                }
            }
        }
        let _guard = env_lock().lock().expect("env lock");
        let _restore = RestoreToken(std::env::var("MCP_BEARER_TOKEN").ok());
        let signing_key = "x".repeat(32);
        let encode_claims = |claims: &Claims| {
            encode(
                &Header::new(Algorithm::HS256),
                claims,
                &EncodingKey::from_secret(signing_key.as_bytes()),
            )
        };
        let server_token = encode_claims(&make_claims(None, &["*"]))?;
        let caller_token = encode_claims(&make_claims(
            Some("caller-space"),
            &["read:system", "read:entity"],
        ))?;
        std::env::set_var("MCP_BEARER_TOKEN", &server_token);
        let storage = StorageClient::new_embedded().await?;
        let state = Arc::new(McpState::new_with_defaults(
            storage,
            AuthConfig {
                secret: signing_key.clone(),
                ..AuthConfig::default()
            },
            "default".to_string(),
            vec!["default".to_string()],
        ));
        let router = mcp_routes(state.clone())
            .merge(mcp_agent_routes(state.clone()))
            .merge(mcp_maintenance_routes(state.clone()))
            .merge(mcp_full_routes(state));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let server = tokio::spawn(async move { axum::serve(listener, router).await });
        for endpoint in ["/mcp", "/mcp/agent", "/mcp/maintenance", "/mcp/full"] {
            for token in [None, Some("invalid-token".to_string())] {
                let mut config = StreamableHttpClientTransportConfig::with_uri(format!(
                    "http://{addr}{endpoint}"
                ));
                config.auth_header = token;
                let client = ().serve(StreamableHttpClientTransport::from_config(config)).await?;
                let tools = client.list_all_tools().await?;
                assert!(!tools.iter().any(|tool| matches!(
                    tool.name.as_ref(),
                    "get_entity" | "export_snapshot" | "configure_runtime"
                )));
                let response = client
                    .call_tool(
                        CallToolRequestParams::new("get_entity").with_arguments(
                            serde_json::json!({"id": "entity:missing"})
                                .as_object()
                                .unwrap()
                                .clone(),
                        ),
                    )
                    .await?;
                assert_eq!(response.is_error, Some(true));
                assert!(serde_json::to_string(&response)?.contains("Authorization"));
                client.cancel().await?;
            }
        }
        // A valid restricted caller must not acquire the environment token's
        // global snapshot privileges either.
        let config =
            StreamableHttpClientTransportConfig::with_uri(format!("http://{addr}/mcp/maintenance"))
                .auth_header(caller_token);
        let client = ().serve(StreamableHttpClientTransport::from_config(config)).await?;
        let tools = client.list_all_tools().await?;
        assert!(tools.iter().any(|tool| tool.name == "get_entity"));
        assert!(!tools
            .iter()
            .any(|tool| matches!(tool.name.as_ref(), "export_snapshot" | "import_snapshot")));
        client.cancel().await?;
        // The same server admits the global token when the caller actually
        // supplies it, so the rejection checks above cannot pass by denying all.
        let client = ()
            .serve(StreamableHttpClientTransport::from_config(
                StreamableHttpClientTransportConfig::with_uri(format!(
                    "http://{addr}/mcp/maintenance"
                ))
                .auth_header(server_token),
            ))
            .await?;
        let tools = client.list_all_tools().await?;
        assert!(tools.iter().any(|tool| tool.name == "export_snapshot"));
        client.cancel().await?;
        server.abort();
        Ok(())
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_mcp_e2e_toy_flow() -> Result<(), Box<dyn std::error::Error>> {
        let storage = StorageClient::new_memory().await?;
        let auth = AuthConfig {
            secret: "test-secret".to_string(),
            algorithm: Algorithm::HS256,
            audience: None,
            issuer: None,
            skip_auth: true,
        };
        // Use explicit defaults to avoid env var race conditions with other tests
        let state = Arc::new(McpState::new_with_defaults(
            storage,
            auth,
            "default".to_string(),
            vec![
                "default".to_string(),
                "dev".to_string(),
                "tenant_policy_v1".to_string(),
            ],
        ));
        let router = mcp_full_routes(state);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let server_handle = tokio::spawn(async move {
            let _ = axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await;
        });

        let transport = StreamableHttpClientTransport::from_uri(format!("http://{addr}/mcp/full"));
        let client = ().serve(transport).await?;

        let mut alice = Entity::new(
            base_fields(TableName::Entity, "default", "mcp_test"),
            EntityType::new("person"),
            "Alice".to_string(),
        );
        alice.aliases.push("A. Example".to_string());
        let bob = Entity::new(
            base_fields(TableName::Entity, "default", "mcp_test"),
            EntityType::new("person"),
            "Bob".to_string(),
        );

        let batch_entities = call_tool_json(
            &client,
            "batch_entities",
            serde_json::json!({ "items": [alice, bob] }),
        )
        .await?;
        let entity_items = batch_entities["data"]["succeeded"]
            .as_array()
            .expect("entity list");
        assert_eq!(entity_items.len(), 2);
        let alice_id = entity_items
            .iter()
            .find(|item| item["display_name"] == "Alice")
            .and_then(|item| item["id"].as_str())
            .expect("alice id");
        let bob_id = entity_items
            .iter()
            .find(|item| item["display_name"] == "Bob")
            .and_then(|item| item["id"].as_str())
            .expect("bob id");

        let mut source = Source::new(
            base_fields(TableName::Source, "default", "mcp_test"),
            SourceType::Manual,
            "hash-alice".to_string(),
        );
        source.uri = Some("file://alice_notes.txt".to_string());
        let source_resp = call_tool_json(
            &client,
            "create_source",
            serde_json::json!({ "source": source }),
        )
        .await?;
        let source_id = source_resp["data"]["id"].as_str().expect("source id");

        let source_rid = RecordId::from_str(source_id)?;
        let mut doc = Doc {
            base: base_fields(TableName::Doc, "default", "mcp_test"),
            title: "Alice Notes".to_string(),
            text: "Alice met Bob in January.".to_string(),
            sidecar_text: None,
            source: Some(Link::new(source_rid)),
            media_attachments: Vec::new(),
            rendition_key: "alice-notes-v1".to_string(),
            metadata: serde_json::json!({}),
        };
        doc.base.security.policy_id = "default".to_string();

        let ingest_doc =
            call_tool_json(&client, "ingest_doc", serde_json::json!({ "doc": doc })).await?;
        assert_eq!(ingest_doc["ok"], true);

        let relate = call_tool_json(
            &client,
            "relate_entities",
            serde_json::json!({
                "from_id": alice_id,
                "to_id": bob_id,
                "rel_type": "knows",
                "description": "Alice knows Bob",
                "space_id": "default"
            }),
        )
        .await?;
        assert_eq!(relate["ok"], true);

        let health = call_tool_json(&client, "get_health", serde_json::json!({})).await?;
        assert!(health["data"]["status"].is_string());

        let search_sources = call_tool_json(
            &client,
            "search_sources",
            serde_json::json!({ "query": "alice" }),
        )
        .await?;
        assert!(!search_sources["data"]["items"]
            .as_array()
            .unwrap()
            .is_empty());

        let search_entities = call_tool_json(
            &client,
            "search_entities",
            serde_json::json!({ "query": "Alice" }),
        )
        .await?;
        assert!(!search_entities["data"]["items"]
            .as_array()
            .unwrap()
            .is_empty());

        let search_docs = call_tool_json(
            &client,
            "search_docs",
            serde_json::json!({ "query": "Alice" }),
        )
        .await?;
        assert!(!search_docs["data"]["items"].as_array().unwrap().is_empty());

        let search_all = call_tool_json(
            &client,
            "search_all",
            serde_json::json!({ "query": "Alice" }),
        )
        .await?;
        assert!(search_all["data"]["entities"].is_object());
        assert!(search_all["data"]["docs"].is_object());
        assert!(search_all["data"]["sources"].is_object());

        client.cancel().await?;
        let _ = shutdown_tx.send(());
        let _ = server_handle.await;
        Ok(())
    }

    async fn call_tool_json(
        client: &rmcp::service::RunningService<rmcp::RoleClient, ()>,
        name: &str,
        params: Value,
    ) -> Result<Value, Box<dyn std::error::Error>> {
        let args = params
            .as_object()
            .cloned()
            .unwrap_or_else(serde_json::Map::new);
        let result = client
            .call_tool(CallToolRequestParams::new(name.to_string()).with_arguments(args))
            .await?;
        let content = result.content.first().expect("tool content");
        let text = content.as_text().expect("text content");
        assert!(
            !result.is_error.unwrap_or(false),
            "tool {name} error: {}",
            text.text
        );
        let value: Value = serde_json::from_str(&text.text)?;
        Ok(value)
    }

    fn base_fields(table: TableName, space_id: &str, created_by: &str) -> BaseFields {
        let mut base = BaseFields::new(
            RecordId::new_v4(table),
            space_id.to_string(),
            created_by.to_string(),
        );
        base.security = Security {
            policy_id: "default".to_string(),
            level: 0,
            labels: Vec::new(),
            handling: Vec::new(),
        };
        base
    }

    /// Test that the three MCP profiles expose different tool sets.
    /// - `/mcp` (default): ~12 end-user tools
    /// - `/mcp/agent`: ~17 read-only tools for extraction agents
    /// - `/mcp/full`: all tools (~60)
    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn test_three_mcp_profiles() -> Result<(), Box<dyn std::error::Error>> {
        let storage = StorageClient::new_memory().await?;
        let auth = AuthConfig {
            secret: "test-secret".to_string(),
            algorithm: Algorithm::HS256,
            audience: None,
            issuer: None,
            skip_auth: true,
        };
        let state = Arc::new(McpState::new_with_defaults(
            storage,
            auth,
            "default".to_string(),
            vec!["default".to_string()],
        ));

        // Build router with all three endpoints
        let router = Router::new()
            .merge(mcp_routes(state.clone()))
            .merge(mcp_agent_routes(state.clone()))
            .merge(mcp_full_routes(state));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let server_handle = tokio::spawn(async move {
            let _ = axum::serve(listener, router)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_rx.await;
                })
                .await;
        });

        // Get tools from full endpoint
        let full_transport =
            StreamableHttpClientTransport::from_uri(format!("http://{addr}/mcp/full"));
        let full_client = ().serve(full_transport).await?;
        let full_tools = full_client.list_all_tools().await?;
        full_client.cancel().await?;

        // Get tools from default endpoint (/mcp)
        let default_transport =
            StreamableHttpClientTransport::from_uri(format!("http://{addr}/mcp"));
        let default_client = ().serve(default_transport).await?;
        let default_tools = default_client.list_all_tools().await?;
        default_client.cancel().await?;

        // Get tools from agent endpoint (/mcp/agent)
        let agent_transport =
            StreamableHttpClientTransport::from_uri(format!("http://{addr}/mcp/agent"));
        let agent_client = ().serve(agent_transport).await?;
        let agent_tools = agent_client.list_all_tools().await?;
        agent_client.cancel().await?;

        let _ = shutdown_tx.send(());
        let _ = server_handle.await;

        // Default profile: exactly DEFAULT_TOOLS count
        let default_tool_names: Vec<&str> = default_tools.iter().map(|t| t.name.as_ref()).collect();
        let expected_default_count = elephant_mcp::DEFAULT_TOOLS.len() + 3;
        assert_eq!(
            default_tools.len(),
            expected_default_count,
            "/mcp should expose exactly {} default tools, found {}: {:?}",
            expected_default_count,
            default_tools.len(),
            default_tool_names
        );

        // Agent profile: exactly AGENT_TOOLS count
        let agent_tool_names: Vec<&str> = agent_tools.iter().map(|t| t.name.as_ref()).collect();
        let expected_agent_count = elephant_mcp::AGENT_TOOLS.len() + 1;
        assert_eq!(
            agent_tools.len(),
            expected_agent_count,
            "/mcp/agent should expose exactly {} agent tools, found {}: {:?}",
            expected_agent_count,
            agent_tools.len(),
            agent_tool_names
        );

        // Full should have significantly more tools (~50+)
        assert!(
            full_tools.len() >= 40,
            "Full endpoint should have at least 40 tools, found {}",
            full_tools.len()
        );

        // Default and Agent are separate curated surfaces. Agent remains a strict
        // read-only extraction surface, while Default now also exposes runtime
        // setup/readiness and ingest tools.
        assert!(
            default_tool_names.contains(&"ingest_doc"),
            "Default should have ingest_doc"
        );
        assert!(
            !agent_tool_names.contains(&"ingest_doc"),
            "Agent should not have ingest_doc"
        );
        assert!(
            agent_tools.len() < full_tools.len(),
            "Agent ({}) should have fewer tools than full ({})",
            agent_tools.len(),
            full_tools.len()
        );
        assert!(
            default_tool_names.contains(&TOOL_GET_RUNTIME_STATUS),
            "Default should expose get_runtime_status"
        );
        assert!(
            default_tool_names.contains(&"get_provenance"),
            "Default should have get_provenance"
        );
        assert!(
            !default_tool_names.contains(&"batch_entities"),
            "Default should not have batch_entities"
        );

        // Agent profile is read-only (no ingest)
        assert!(
            agent_tool_names.contains(&TOOL_GET_RUNTIME_STATUS),
            "Agent should expose get_runtime_status"
        );
        assert!(
            agent_tool_names.contains(&"search_entities"),
            "Agent should have search_entities"
        );
        assert!(
            agent_tool_names.contains(&"search_docs"),
            "Agent should have search_docs"
        );
        assert!(
            agent_tool_names.contains(&"get_health"),
            "Agent should have get_health"
        );

        Ok(())
    }
}
