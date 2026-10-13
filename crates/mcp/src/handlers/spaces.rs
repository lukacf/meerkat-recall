#![allow(clippy::too_many_lines, clippy::cognitive_complexity)]
//! Space management MCP tool handlers.
//!
//! Implements:
//! - `create_space`
//! - `list_spaces`
//! - `get_space`
//! - `update_space`

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use elephant_embeddings::EmbeddingPolicy;
use elephant_storage::{parse_id, Repository, SpaceConfigRepository, StorageClient};
use elephant_types::ids::TableName;
use elephant_types::records::SpaceConfig;
use elephant_types::BaseFieldsBuilder;

use crate::coercion::coerce_params;
use crate::error::{McpError, McpResult};
use crate::tools::ToolResponse;

use super::{ToolContext, ToolHandler};

const fn default_limit() -> usize {
    50
}

/// Embedding configuration for space creation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateSpaceConfigInput {
    /// Explicit processor approvals; changing them requires `manage:provider`.
    #[serde(default)]
    pub processing: Option<elephant_types::provider_binding::SpaceProcessingConfig>,
    /// Default extraction purpose, after document and source-specific guidance.
    #[serde(default)]
    pub purpose: Option<String>,
    /// Default extraction skill, after explicit per-document/source guidance.
    #[serde(default)]
    pub skill: Option<String>,
    /// Embedding provider name (e.g., "fastembed", "openai").
    #[serde(default)]
    pub embedding_provider: Option<String>,
    /// Embedding model identifier.
    #[serde(default)]
    pub embedding_model: Option<String>,
    /// Embedding vector dimensions.
    #[serde(default)]
    pub embedding_dims: Option<i32>,
}

/// Input for `create_space` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateSpaceInput {
    /// Space name/ID.
    pub name: String,
    /// Initial envelope, also inherited by new sources without explicit security.
    /// Ordinary space updates cannot replace this envelope.
    #[serde(default)]
    pub security: Option<elephant_types::records::base::Security>,
    /// Optional per-space config.
    #[serde(default)]
    pub config: Option<CreateSpaceConfigInput>,
}

/// Input for `get_space` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetSpaceInput {
    /// Space ID.
    pub space_id: String,
}

/// Input for `update_space` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateSpaceInput {
    /// Space ID.
    pub space_id: String,
    /// Config fields to update.
    pub config: CreateSpaceConfigInput,
}

/// Input for `list_spaces` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListSpacesInput {
    /// Maximum results.
    #[serde(default = "default_limit")]
    pub limit: usize,
    /// Pagination cursor (`space_config` record ID).
    #[serde(default)]
    pub cursor: Option<String>,
}

/// Output for `list_spaces` tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListSpacesOutput {
    /// Space configs.
    pub items: Vec<SpaceConfig>,
    /// Next cursor.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    /// Whether more results exist.
    pub has_more: bool,
}

fn require_provider_admin(ctx: &ToolContext) -> McpResult<()> {
    if ctx.principal.scopes.contains("manage:provider") {
        return Ok(());
    }
    Err(McpError::Public(
        elephant_types::public_error::PublicError::new(
            elephant_types::public_error::ErrorCode::ForbiddenSpace,
            "Processing changes require explicit manage:provider scope",
        ),
    ))
}

fn policy_from_input(input: &CreateSpaceConfigInput) -> McpResult<EmbeddingPolicy> {
    let mut policy = EmbeddingPolicy::from_env();

    if let Some(provider) = &input.embedding_provider {
        policy.provider.clone_from(provider);
    }
    if let Some(model) = &input.embedding_model {
        policy.model.clone_from(model);
    }
    if let Some(dims) = input.embedding_dims {
        if dims <= 0 {
            return Err(McpError::InvalidParameters(
                "embedding_dims must be > 0".to_string(),
            ));
        }
        policy.dims = usize::try_from(dims)
            .map_err(|_| McpError::InvalidParameters("embedding_dims out of range".to_string()))?;
    }

    Ok(policy)
}

fn space_config_for_policy(space_id: &str, policy: &EmbeddingPolicy) -> McpResult<SpaceConfig> {
    let dims = i32::try_from(policy.dims)
        .map_err(|_| McpError::InvalidParameters("embedding_dims out of range".to_string()))?;
    let base = BaseFieldsBuilder::new(space_id.to_string())
        .for_table(TableName::SpaceConfig)
        .build();
    Ok(SpaceConfig {
        base,
        embedding_provider: policy.provider.clone(),
        embedding_model: policy.model.clone(),
        embedding_dims: dims,
        purpose: None,
        skill: None,
        processing: None,
    })
}

/// Handler for `create_space` tool.
pub struct CreateSpaceHandler {
    repo: SpaceConfigRepository,
}

impl CreateSpaceHandler {
    /// Creates a new `create_space` handler with the given storage client.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: SpaceConfigRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for CreateSpaceHandler {
    fn name(&self) -> &'static str {
        "create_space"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Create a space config record for a new space.\n\n",
            "Example:\n",
            r#"{ "name": "default", "config": { "embedding_provider": "fastembed", "embedding_model": "bge-small-en-v1.5", "embedding_dims": 384 } }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["name"],
            "properties": {
                "name": { "type": "string", "description": "Space ID/name" },
                "security": {
                    "type":"object",
                    "required":["policy_id","level"],
                    "description":"Initial security envelope; caller needs read/write clearance. Ordinary updates cannot relabel it.",
                    "properties":{
                        "policy_id":{"type":"string","minLength":1},
                        "level":{"type":"integer","minimum":0},
                        "labels":{"type":"array","items":{"type":"string"}},
                        "handling":{"type":"array","items":{"type":"string"}}
                    }
                },
                "config": {
                    "type": "object",
                    "required": [],
                    "description": "Optional embedding configuration (defaults from env)",
                    "properties": {
                        "embedding_provider": { "type": "string" },
                        "embedding_model": { "type": "string" },
                        "embedding_dims": { "type": "integer", "minimum": 1 },
                        "purpose": { "type": "string" },
                        "skill": { "type": "string" },
                        "processing": { "type": "object", "required": [], "description": "Explicit approved processing configuration; requires manage:provider" }
                    }
                }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: CreateSpaceInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        if let Err(error) = elephant_types::validation::validate_space_id(&input.name) {
            let response: ToolResponse<()> = ToolResponse::error(error.to_string());
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let policy = match &input.config {
            Some(cfg) => policy_from_input(cfg)?,
            None => EmbeddingPolicy::from_env(),
        };
        let mut config = space_config_for_policy(&input.name, &policy)?;
        if let Some(security) = input.security {
            elephant_types::validation::validate_security(&security)
                .map_err(|error| McpError::InvalidParameters(error.to_string()))?;
            config.base.security = security;
        }
        if let Some(input) = input.config {
            if input.processing.is_some() {
                require_provider_admin(ctx)?;
            }
            config.processing = input.processing;
            config.purpose = input.purpose;
            config.skill = input.skill;
        }

        if let Err(reason) =
            ctx.authz
                .authorize_record_write(&ctx.principal, &config, "write:space_config")
        {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Access denied: {reason}"));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        if let Err(reason) =
            ctx.authz
                .authorize_record_read(&ctx.principal, &config, "read:space_config")
        {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Access denied: {reason}"));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        match self.repo.get_by_space_id(&input.name).await {
            Ok(Some(existing)) => {
                ctx.authz
                    .authorize_record_read(&ctx.principal, &existing, "read:space_config")
                    .map_err(|_| {
                        McpError::Public(elephant_types::public_error::PublicError::new(
                            elephant_types::public_error::ErrorCode::ForbiddenSpace,
                            "Existing space metadata is not authorized",
                        ))
                    })?;
                let response: ToolResponse<()> =
                    ToolResponse::error(format!("Space already exists: {}", input.name));
                return serde_json::to_value(response)
                    .map_err(|e| McpError::ExecutionError(e.to_string()));
            }
            Ok(None) => {}
            Err(error) => return Err(McpError::StorageError(error)),
        }

        let created = self
            .repo
            .create(config)
            .await
            .map_err(McpError::StorageError)?;
        let response = ToolResponse::success(created);
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

/// Handler for `list_spaces` tool.
pub struct ListSpacesHandler {
    repo: SpaceConfigRepository,
}

impl ListSpacesHandler {
    /// Creates a new `list_spaces` handler with the given storage client.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: SpaceConfigRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for ListSpacesHandler {
    fn name(&self) -> &'static str {
        "list_spaces"
    }

    fn description(&self) -> &'static str {
        concat!(
            "List spaces (space_config records).\n\n",
            "Example:\n",
            r#"{ "limit": 50 }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": [],
            "properties": {
                "limit": { "type": "integer", "default": 50, "maximum": 1000 },
                "cursor": { "type": "string", "description": "Pagination cursor (space_config record ID)" }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: ListSpacesInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let cursor = input
            .cursor
            .as_deref()
            .map(|c| parse_id(c, TableName::SpaceConfig))
            .transpose()
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let limit = input.limit.clamp(1, 1000);
        let mut visible = Vec::with_capacity(limit);
        let mut scan_cursor = cursor;
        let mut next_cursor = None;

        loop {
            let items = self
                .repo
                .list_all(limit + 1, scan_cursor.as_ref())
                .await
                .map_err(McpError::StorageError)?;

            let raw_has_more = items.len() > limit;
            let mut page = items;
            if raw_has_more {
                page.truncate(limit);
            }

            let mut last_raw_id = None;
            for item in page {
                let current_raw_id = item.base.id.clone();
                let page_cursor = last_raw_id.clone().or_else(|| scan_cursor.clone());
                last_raw_id = Some(current_raw_id);
                let allowed =
                    ctx.authz
                        .filter_records(&ctx.principal, vec![item], "read:space_config");
                if let Some(item) = allowed.into_iter().next() {
                    if visible.len() < limit {
                        visible.push(item);
                    } else if next_cursor.is_none() {
                        next_cursor = page_cursor.as_ref().map(ToString::to_string);
                    }
                }
            }

            if next_cursor.is_some() || !raw_has_more {
                break;
            }

            let Some(last_raw_id) = last_raw_id else {
                break;
            };
            scan_cursor = Some(last_raw_id);
        }

        let has_more = next_cursor.is_some();

        let response = ToolResponse::success(ListSpacesOutput {
            items: visible,
            next_cursor,
            has_more,
        });
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

/// Handler for `get_space` tool.
pub struct GetSpaceHandler {
    repo: SpaceConfigRepository,
}

impl GetSpaceHandler {
    /// Creates a new `get_space` handler with the given storage client.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: SpaceConfigRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for GetSpaceHandler {
    fn name(&self) -> &'static str {
        "get_space"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Get space configuration by space_id.\n\n",
            "Example:\n",
            r#"{ "space_id": "default" }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["space_id"],
            "properties": {
                "space_id": { "type": "string", "description": "Space ID" }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: GetSpaceInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let Some(cfg) = self
            .repo
            .get_by_space_id(&input.space_id)
            .await
            .map_err(McpError::StorageError)?
        else {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Space not found: {}", input.space_id));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        };

        if let Err(reason) =
            ctx.authz
                .authorize_record_read(&ctx.principal, &cfg, "read:space_config")
        {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Access denied: {reason}"));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let response = ToolResponse::success(cfg);
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

/// Handler for `update_space` tool.
pub struct UpdateSpaceHandler {
    repo: SpaceConfigRepository,
}

impl UpdateSpaceHandler {
    /// Creates a new `update_space` handler with the given storage client.
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self {
            repo: SpaceConfigRepository::new(storage),
        }
    }
}

#[async_trait]
impl ToolHandler for UpdateSpaceHandler {
    fn name(&self) -> &'static str {
        "update_space"
    }

    fn description(&self) -> &'static str {
        concat!(
            "Update space configuration.\n\n",
            "Example:\n",
            r#"{ "space_id": "default", "config": { "embedding_provider": "openai" } }"#,
        )
    }

    fn input_schema(&self) -> Value {
        json!({
            "type": "object",
            "required": ["space_id", "config"],
            "properties": {
                "space_id": { "type": "string", "description": "Space ID" },
                "config": {
                    "type": "object",
                    "required": [],
                    "description": "Config fields to update",
                    "properties": {
                        "embedding_provider": { "type": "string" },
                        "embedding_model": { "type": "string" },
                        "embedding_dims": { "type": "integer", "minimum": 1 },
                        "purpose": { "type": "string" },
                        "skill": { "type": "string" },
                        "processing": { "type": "object", "required": [], "description": "Explicit approved processing configuration; requires manage:provider" }
                    }
                }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: UpdateSpaceInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        let Some(existing) = self
            .repo
            .get_by_space_id(&input.space_id)
            .await
            .map_err(McpError::StorageError)?
        else {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Space not found: {}", input.space_id));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        };

        if let Err(reason) =
            ctx.authz
                .authorize_record_write(&ctx.principal, &existing, "write:space_config")
        {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Access denied: {reason}"));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        if let Err(reason) =
            ctx.authz
                .authorize_record_read(&ctx.principal, &existing, "read:space_config")
        {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Access denied: {reason}"));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }
        if input.config.processing.is_some() {
            require_provider_admin(ctx)?;
        }
        if input.config.embedding_dims.is_some_and(|dims| dims <= 0) {
            return Err(McpError::InvalidParameters(
                "embedding_dims must be > 0".into(),
            ));
        }
        let updated = SpaceConfig {
            base: existing.base.clone(),
            embedding_provider: input
                .config
                .embedding_provider
                .unwrap_or_else(|| existing.embedding_provider.clone()),
            embedding_model: input
                .config
                .embedding_model
                .unwrap_or_else(|| existing.embedding_model.clone()),
            embedding_dims: input
                .config
                .embedding_dims
                .unwrap_or(existing.embedding_dims),
            purpose: input.config.purpose.or_else(|| existing.purpose.clone()),
            skill: input.config.skill.or_else(|| existing.skill.clone()),
            processing: input
                .config
                .processing
                .or_else(|| existing.processing.clone()),
        };
        let saved = self
            .repo
            .replace_authorized(&existing, updated)
            .await
            .map_err(McpError::StorageError)?;

        let response = ToolResponse::success(saved);
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}

#[cfg(test)]
mod authorization_tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use elephant_policy::{Clearance, Principal};
    use std::collections::HashSet;

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn custom_space_security_requires_clearance_and_cannot_be_relabelled() {
        let storage = StorageClient::new_embedded().await.unwrap();
        let mut principal = Principal {
            space_id: "family".into(),
            scopes: HashSet::from(["read:space_config".into(), "write:space_config".into()]),
            clearances: vec![Clearance {
                policy_id: "household-v1".into(),
                max_level: 0,
                labels: HashSet::from(["audience:owner".into()]),
                handling_allow: HashSet::from(["no_voice".into()]),
            }],
            subject_allowlist: None,
            purpose: HashSet::new(),
        };
        let input = json!({
            "name":"family",
            "security":{"policy_id":"household-v1","level":2,"labels":["audience:owner"],"handling":["no_voice"]},
            "config":{"embedding_provider":"fake","embedding_model":"fixture","embedding_dims":3}
        });
        let denied = CreateSpaceHandler::new(storage.clone())
            .execute(
                &ToolContext::new(storage.clone(), principal.clone()),
                input.clone(),
            )
            .await
            .unwrap();
        assert_eq!(denied["ok"], false);
        let repo = SpaceConfigRepository::new(storage.clone());
        assert!(repo.get_by_space_id("family").await.unwrap().is_none());
        principal.clearances[0].max_level = 2;
        let ctx = ToolContext::new(storage.clone(), principal);
        let created = CreateSpaceHandler::new(storage.clone())
            .execute(&ctx, input.clone())
            .await
            .unwrap();
        assert_eq!(created["ok"], true);
        let before = repo.get_by_space_id("family").await.unwrap().unwrap();
        assert_eq!(
            serde_json::to_value(&before.base.security).unwrap(),
            input["security"]
        );
        assert!(UpdateSpaceHandler::new(storage)
            .execute(
                &ctx,
                json!({"space_id":"family","config":{"security":{"policy_id":"default","level":0}}})
            )
            .await
            .is_err());
        let after = repo.get_by_space_id("family").await.unwrap().unwrap();
        assert_eq!(after.base.security, before.base.security);
    }

    #[tokio::test]
    async fn create_space_requires_read_and_write_before_mutation() {
        for scopes in [
            vec!["write:space_config"],
            vec!["read:space_config"],
            vec!["read:space_config", "write:space_config"],
        ] {
            let allowed = scopes.len() == 2;
            let storage = StorageClient::new_embedded().await.unwrap();
            let principal = Principal {
                space_id: "alpha".into(),
                scopes: scopes.into_iter().map(str::to_owned).collect(),
                clearances: vec![Clearance {
                    policy_id: "default".into(),
                    max_level: 5,
                    labels: HashSet::new(),
                    handling_allow: HashSet::new(),
                }],
                subject_allowlist: None,
                purpose: HashSet::new(),
            };
            let ctx = ToolContext::new(storage.clone(), principal);
            let result = CreateSpaceHandler::new(storage.clone()).execute(&ctx, json!({
                "name":"alpha", "config": {"embedding_provider":"fake", "embedding_model":"fixture", "embedding_dims":3}
            })).await.unwrap();
            assert_eq!(result["ok"], allowed);
            assert_eq!(
                SpaceConfigRepository::new(storage)
                    .get_by_space_id("alpha")
                    .await
                    .unwrap()
                    .is_some(),
                allowed
            );
        }
    }
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
    )]
    async fn ordinary_space_update_preserves_embedding_policy() {
        let storage = StorageClient::new_embedded().await.unwrap();
        let principal = Principal {
            space_id: "alpha".into(),
            scopes: HashSet::from(["read:space_config".into(), "write:space_config".into()]),
            clearances: vec![Clearance {
                policy_id: "default".into(),
                max_level: 5,
                labels: HashSet::new(),
                handling_allow: HashSet::new(),
            }],
            subject_allowlist: None,
            purpose: HashSet::new(),
        };
        let ctx = ToolContext::new(storage.clone(), principal);
        let created=CreateSpaceHandler::new(storage.clone()).execute(&ctx,json!({"name":"alpha","config":{"embedding_provider":"fake","embedding_model":"explicit-policy","embedding_dims":7,"purpose":"before"}})).await.unwrap();
        assert_eq!(created["ok"], true);
        let updated = UpdateSpaceHandler::new(storage)
            .execute(
                &ctx,
                json!({"space_id":"alpha","config":{"purpose":"after"}}),
            )
            .await
            .unwrap();
        assert_eq!(updated["ok"], true);
        assert_eq!(updated["data"]["embedding_model"], "explicit-policy");
        assert_eq!(updated["data"]["embedding_dims"], 7);
        assert_eq!(updated["data"]["purpose"], "after");
    }
}
