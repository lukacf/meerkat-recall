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
pub struct CreateSpaceConfigInput {
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
pub struct CreateSpaceInput {
    /// Space name/ID.
    pub name: String,
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
                "config": {
                    "type": "object",
                    "required": [],
                    "description": "Optional embedding configuration (defaults from env)",
                    "properties": {
                        "embedding_provider": { "type": "string" },
                        "embedding_model": { "type": "string" },
                        "embedding_dims": { "type": "integer", "minimum": 1 }
                    }
                }
            }
        })
    }

    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let params = coerce_params(params, &self.input_schema());
        let input: CreateSpaceInput = serde_json::from_value(params)
            .map_err(|e| McpError::InvalidParameters(e.to_string()))?;

        if input.name.trim().is_empty() {
            let response: ToolResponse<()> = ToolResponse::error("name must not be empty");
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        let policy = match input.config {
            Some(cfg) => policy_from_input(&cfg)?,
            None => EmbeddingPolicy::from_env(),
        };
        let config = space_config_for_policy(&input.name, &policy)?;

        if let Err(reason) =
            ctx.authz
                .authorize_record_write(&ctx.principal, &config, "write:space_config")
        {
            let response: ToolResponse<()> =
                ToolResponse::error(format!("Access denied: {reason}"));
            return serde_json::to_value(response)
                .map_err(|e| McpError::ExecutionError(e.to_string()));
        }

        match self.repo.get_by_space_id(&input.name).await {
            Ok(Some(_)) => {
                let response: ToolResponse<()> =
                    ToolResponse::error(format!("Space already exists: {}", input.name));
                return serde_json::to_value(response)
                    .map_err(|e| McpError::ExecutionError(e.to_string()));
            }
            Ok(None) => {}
            Err(e) => return Err(McpError::ExecutionError(e.to_string())),
        }

        let created = self
            .repo
            .create(config)
            .await
            .map_err(|e| McpError::ExecutionError(e.to_string()))?;
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
                .map_err(|e| McpError::ExecutionError(e.to_string()))?;

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
            .map_err(|e| McpError::ExecutionError(e.to_string()))?
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
                        "embedding_dims": { "type": "integer", "minimum": 1 }
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
            .map_err(|e| McpError::ExecutionError(e.to_string()))?
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

        let policy = policy_from_input(&input.config)?;
        let dims = i32::try_from(policy.dims)
            .map_err(|_| McpError::InvalidParameters("embedding_dims out of range".to_string()))?;

        let updated = SpaceConfig {
            base: existing.base,
            embedding_provider: policy.provider,
            embedding_model: policy.model,
            embedding_dims: dims,
            purpose: existing.purpose,
            skill: existing.skill,
        };
        let id = updated.base.id.clone();
        let saved = self
            .repo
            .update(&input.space_id, &id, updated)
            .await
            .map_err(|e| McpError::ExecutionError(e.to_string()))?;

        let response = ToolResponse::success(saved);
        serde_json::to_value(response).map_err(|e| McpError::ExecutionError(e.to_string()))
    }
}
