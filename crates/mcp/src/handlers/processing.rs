//! Explicit provider-administration actions over the storage-owned reindex state.
use async_trait::async_trait;
use elephant_storage::{SpaceConfigRepository, StorageClient};
use elephant_types::public_error::{ErrorCode, PublicError};
use serde::Deserialize;
use serde_json::{json, Value};

use super::{ToolContext, ToolHandler};
use crate::tools::ToolResponse;
use crate::{McpError, McpResult};

/// One operation over the space repository's staged embedding profile.
#[derive(Clone, Copy)]
pub enum ReindexAction {
    /// Stage an approved target and enqueue durable reindex work.
    Start,
    /// Read the current active profile and staged coverage.
    Status,
    /// Activate a fully covered current target under its configuration fence.
    Activate,
    /// Withdraw the staged target while preserving the active profile.
    Cancel,
}

/// Shared HTTP/MCP adapter; the space repository owns reindex state and CAS.
pub struct ReindexHandler {
    storage: StorageClient,
    action: ReindexAction,
}
impl ReindexHandler {
    /// Bind a single administration action to the active storage client.
    #[must_use]
    pub const fn new(storage: StorageClient, action: ReindexAction) -> Self {
        Self { storage, action }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    space_id: String,
    expected_processing_generation: Option<u64>,
    target_profile_id: Option<String>,
    reindex_id: Option<String>,
}
fn public(code: ErrorCode, message: &str) -> McpError {
    McpError::Public(PublicError::new(code, message))
}
fn storage_error(error: elephant_storage::StorageError) -> McpError {
    match error {
        elephant_storage::StorageError::ErasureInProgress => public(
            ErrorCode::NotReady,
            "Source erasure is in progress; retry after cleanup",
        ),
        elephant_storage::StorageError::Conflict(_) => public(
            ErrorCode::ConflictGeneration,
            "Processing configuration or reindex coverage changed; read its current status",
        ),
        elephant_storage::StorageError::NotFound(_) => {
            public(ErrorCode::UnknownSpace, "Unknown space")
        }
        error => {
            tracing::warn!(error=%error,"Reindex administration unavailable");
            public(ErrorCode::Unavailable, "Reindex administration unavailable")
        }
    }
}

#[async_trait]
impl ToolHandler for ReindexHandler {
    fn name(&self) -> &'static str {
        match self.action {
            ReindexAction::Start => "start_embedding_reindex",
            ReindexAction::Status => "get_embedding_reindex",
            ReindexAction::Activate => "activate_embedding_reindex",
            ReindexAction::Cancel => "cancel_embedding_reindex",
        }
    }
    fn description(&self) -> &'static str {
        match self.action {
            ReindexAction::Start => "Start staged embedding reindex for an approved target profile in the selected space. The old profile stays active. Requires read/write:space_config, explicit manage:provider, and the current processing generation.\n\nExample:\n{\"space_id\":\"home\",\"expected_processing_generation\":1,\"target_profile_id\":\"local-384-v2\"}",
            ReindexAction::Status => "Read the selected space's active embedding profile, processing generation, and pending reindex status. Requires read:space_config.\n\nExample:\n{\"space_id\":\"home\"}",
            ReindexAction::Activate => "Activate the pending embedding reindex only after complete coverage. Use the reindex_id and current processing generation returned by get_embedding_reindex. Requires read/write:space_config and explicit manage:provider.\n\nExample:\n{\"space_id\":\"home\",\"expected_processing_generation\":2,\"reindex_id\":\"reindex-from-status\"}",
            ReindexAction::Cancel => "Cancel the selected space's pending embedding reindex while retaining the active profile. Use the reindex_id and current processing generation returned by get_embedding_reindex. Requires read/write:space_config and explicit manage:provider.\n\nExample:\n{\"space_id\":\"home\",\"expected_processing_generation\":2,\"reindex_id\":\"reindex-from-status\"}",
        }
    }
    fn input_schema(&self) -> Value {
        json!({"type":"object","required":["space_id"],"properties":{"space_id":{"type":"string"},"expected_processing_generation":{"type":"integer","minimum":0},"target_profile_id":{"type":"string"},"reindex_id":{"type":"string"}}})
    }
    #[expect(
        clippy::significant_drop_tightening,
        clippy::too_many_lines,
        reason = "Preserve the original handler ownership, future shape, and call ordering."
    )]
    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let input: Input = serde_json::from_value(params)
            .map_err(|error| McpError::InvalidParameters(error.to_string()))?;
        if input.space_id.is_empty() || ctx.principal.space_id != input.space_id {
            return Err(public(
                ErrorCode::ForbiddenSpace,
                "Select this named space explicitly",
            ));
        }
        let write = !matches!(self.action, ReindexAction::Status);
        let has = |scope: &str| {
            ctx.principal.scopes.contains(scope) || ctx.principal.scopes.contains("*")
        };
        if !has("read:space_config")
            || (write
                && (!has("write:space_config")
                    || !ctx.principal.scopes.contains("manage:provider")))
        {
            return Err(public(
                ErrorCode::ForbiddenSpace,
                "Reindex changes require read/write:space_config and explicit manage:provider",
            ));
        }
        let repository = SpaceConfigRepository::new(self.storage.clone());
        let config = repository
            .get_by_space_id(&input.space_id)
            .await
            .map_err(storage_error)?
            .ok_or_else(|| public(ErrorCode::UnknownSpace, "Unknown space"))?;
        ctx.authz
            .authorize_record_read(&ctx.principal, &config, "read:space_config")
            .map_err(|_| {
                public(
                    ErrorCode::ForbiddenSpace,
                    "Space configuration is not readable",
                )
            })?;
        if write {
            ctx.authz
                .authorize_record_write(&ctx.principal, &config, "write:space_config")
                .map_err(|_| {
                    public(
                        ErrorCode::ForbiddenSpace,
                        "Space configuration is not writable",
                    )
                })?;
        }
        let processing = config.processing.as_ref().ok_or_else(|| {
            public(
                ErrorCode::ProviderUnapproved,
                "Space processing is not approved",
            )
        })?;
        if write && input.expected_processing_generation != Some(processing.generation) {
            return Err(public(
                ErrorCode::ConflictGeneration,
                "Expected processing generation is required and must match",
            ));
        }
        if matches!(self.action, ReindexAction::Activate | ReindexAction::Cancel)
            && input.reindex_id.as_deref()
                != processing
                    .pending_reindex
                    .as_ref()
                    .map(|pending| pending.id.as_str())
        {
            return Err(public(
                ErrorCode::ConflictGeneration,
                "Reindex identity changed",
            ));
        }
        let config = match self.action {
            ReindexAction::Status => config,
            ReindexAction::Start => {
                let target = input
                    .target_profile_id
                    .as_deref()
                    .filter(|target| !target.is_empty())
                    .ok_or_else(|| {
                        public(ErrorCode::InvalidRequest, "target_profile_id is required")
                    })?;
                repository
                    .start_embedding_reindex(&config, target)
                    .await
                    .map_err(storage_error)?
            }
            ReindexAction::Activate => repository
                .activate_embedding_reindex(
                    &config,
                    input.reindex_id.as_deref().ok_or_else(|| {
                        public(ErrorCode::InvalidRequest, "reindex_id is required")
                    })?,
                )
                .await
                .map_err(storage_error)?,
            ReindexAction::Cancel => repository
                .cancel_embedding_reindex(&config)
                .await
                .map_err(storage_error)?,
        };
        let mut enqueued = None;
        if matches!(self.action, ReindexAction::Start) {
            let actor = ctx
                .verified_caller()
                .map_or("processing-admin", |caller| caller.claims().sub.as_str());
            match elephant_pipeline::source_queue::enqueue_space_reindex(
                &self.storage,
                &config,
                actor,
            )
            .await
            {
                Ok(_) => enqueued = Some(true),
                Err(error) => {
                    tracing::warn!(space=%input.space_id,error=%error,"Durable pending reindex awaits enqueue recovery");
                    enqueued = Some(false);
                }
            }
        }
        let processing = config.processing.ok_or_else(|| {
            public(
                ErrorCode::Unavailable,
                "Processing configuration disappeared",
            )
        })?;
        let result = ToolResponse::success(
            json!({"space":input.space_id,"processing_generation":processing.generation,"active_embedding_profile_id":processing.active_embedding_profile_id,"pending_reindex":processing.pending_reindex,"enqueued":enqueued}),
        );
        serde_json::to_value(result).map_err(|error| McpError::ExecutionError(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture, ownership, and assertion scope intact."
    )]
    async fn wildcard_cannot_manufacture_provider_administration(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let storage = StorageClient::new_embedded().await?;
        let ctx = ToolContext::new(
            storage.clone(),
            elephant_policy::Principal {
                space_id: "alpha".into(),
                scopes: HashSet::from(["*".into()]),
                clearances: vec![],
                subject_allowlist: None,
                purpose: HashSet::new(),
            },
        );
        let result=ReindexHandler::new(storage,ReindexAction::Start).execute(&ctx,json!({"space_id":"alpha","target_profile_id":"new","expected_processing_generation":1})).await;
        assert!(matches!(
            result,
            Err(McpError::Public(PublicError {
                error_code: ErrorCode::ForbiddenSpace,
                ..
            }))
        ));
        Ok(())
    }
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original fixture, ownership, and assertion scope intact."
    )]
    async fn reindex_refuses_cross_space_before_repository_lookup(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let storage = StorageClient::new_embedded().await?;
        let ctx = ToolContext::new(
            storage.clone(),
            elephant_policy::Principal {
                space_id: "alpha".into(),
                scopes: HashSet::from(["read:space_config".into(), "manage:provider".into()]),
                clearances: vec![],
                subject_allowlist: None,
                purpose: HashSet::new(),
            },
        );
        let result = ReindexHandler::new(storage, ReindexAction::Status)
            .execute(&ctx, json!({"space_id":"beta"}))
            .await;
        assert!(matches!(
            result,
            Err(McpError::Public(PublicError {
                error_code: ErrorCode::ForbiddenSpace,
                ..
            }))
        ));
        Ok(())
    }
}

#[cfg(test)]
mod erasure_error_tests {
    #[test]
    fn reservation_stays_a_retryable_public_refusal() -> crate::McpResult<()> {
        let failure = super::storage_error(elephant_storage::StorageError::ErasureInProgress);
        let crate::McpError::Public(public) = failure else {
            return Err(failure);
        };
        assert_eq!(
            public.error_code,
            elephant_types::public_error::ErrorCode::NotReady
        );
        assert!(public.retryable);
        assert_eq!(
            public.message,
            "Source erasure is in progress; retry after cleanup"
        );
        Ok(())
    }
}
