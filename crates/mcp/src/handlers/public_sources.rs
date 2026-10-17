//! Metadata and lifecycle tools over the same source authority used by HTTP.
use async_trait::async_trait;
use elephant_storage::{
    source_publication::{SourceMutation, SourcePublicationRepository},
    SpaceConfigRepository, StorageClient, StorageError,
};
use elephant_types::{
    public_error::{ErrorCode, PublicError},
    publication::SourcePrecondition,
    publication_wire,
    records::SourceType,
};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use super::{ToolContext, ToolHandler};
use crate::{tools::ToolResponse, McpError, McpResult};

/// A metadata read or explicitly generation-checked source lifecycle mutation.
#[derive(Clone, Copy)]
pub enum SourceAction {
    /// Read the current external source head.
    Head,
    /// Read an immutable operation identity.
    Operation,
    /// Withdraw support while retaining source content.
    Retract,
    /// Physically erase source data under its durable journal barrier.
    Erase,
}
/// Authorized adapter over the sole source publication repository.
pub struct SourceControlHandler {
    storage: StorageClient,
    action: SourceAction,
}
impl SourceControlHandler {
    /// Bind one source action to the hosting storage instance.
    #[must_use]
    pub const fn new(storage: StorageClient, action: SourceAction) -> Self {
        Self { storage, action }
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    external_source_id: Option<String>,
    operation_id: Option<String>,
    expected_generation: Option<String>,
    idempotency_key: Option<String>,
}
fn error(code: ErrorCode, message: &str) -> McpError {
    McpError::Public(PublicError::new(code, message))
}
fn storage_error(failure: StorageError) -> McpError {
    match failure {
        elephant_storage::StorageError::ErasureInProgress => error(
            ErrorCode::NotReady,
            "Source erasure is in progress; retry after cleanup",
        ),
        StorageError::Conflict(code) if code.starts_with("unknown_space") => {
            error(ErrorCode::UnknownSpace, "Unknown space")
        }
        StorageError::Conflict(_) => error(
            ErrorCode::ConflictGeneration,
            "Source generation or authorized envelope changed; read the current head",
        ),
        StorageError::NotFound(_) => error(ErrorCode::NotFound, "Source or operation not found"),
        failure => {
            tracing::warn!(error=%failure,"MCP source operation unavailable");
            error(ErrorCode::Unavailable, "Source operation unavailable")
        }
    }
}
fn required(value: Option<&str>, max_bytes: usize, message: &str) -> McpResult<String> {
    value
        .filter(|value| {
            !value.is_empty()
                && value.len() <= max_bytes
                && value.trim() == *value
                && !value.chars().any(char::is_control)
        })
        .map(str::to_owned)
        .ok_or_else(|| error(ErrorCode::InvalidRequest, message))
}

#[async_trait]
impl ToolHandler for SourceControlHandler {
    fn name(&self) -> &'static str {
        match self.action {
            SourceAction::Head => "get_source_head",
            SourceAction::Operation => "get_source_operation",
            SourceAction::Retract => "retract_source",
            SourceAction::Erase => "erase_source",
        }
    }
    fn description(&self) -> &'static str {
        match self.action {
            SourceAction::Head => concat!(
                "Read accepted source revision coverage in the explicitly selected named space. ",
                "Requires read:source and the retained source classification.\n\nExample:\n",
                r#"{"external_source_id":"notes/meeting-42"}"#,
            ),
            SourceAction::Operation => concat!(
                "Read an original source operation in the explicitly selected named space. ",
                "Requires read:source and the retained operation classification.\n\nExample:\n",
                r#"{"operation_id":"operation-from-acceptance"}"#,
            ),
            SourceAction::Retract => concat!(
                "Withdraw source support using the exact generation returned by get_source_head. ",
                "Requires read:source and write:source in the selected named space.\n\nExample:\n",
                r#"{"external_source_id":"notes/meeting-42","expected_generation":"generation-from-head","idempotency_key":"withdraw-meeting-42"}"#,
            ),
            SourceAction::Erase => concat!(
                "Physically erase source-derived data under the durable journal barrier. ",
                "Requires read:source and write:source in the selected named space and the exact head generation.\n\nExample:\n",
                r#"{"external_source_id":"notes/meeting-42","expected_generation":"generation-from-head","idempotency_key":"erase-meeting-42"}"#,
            ),
        }
    }
    fn input_schema(&self) -> Value {
        let required = match self.action {
            SourceAction::Head => vec!["external_source_id"],
            SourceAction::Operation => vec!["operation_id"],
            SourceAction::Retract | SourceAction::Erase => {
                vec!["external_source_id", "expected_generation"]
            }
        };
        json!({
            "type":"object",
            "additionalProperties":false,
            "required":required,
            "properties":{
                "external_source_id":{"type":"string","minLength":1,"maxLength":1024,"description":"Exact external identity; at most 1024 UTF-8 bytes, trimmed and control-free."},
                "operation_id":{"type":"string","minLength":1,"maxLength":1024,"description":"Opaque operation identity; at most 1024 UTF-8 bytes, trimmed and control-free."},
                "expected_generation":{"type":"string","minLength":1,"maxLength":256,"description":"Exact observed source generation; at most 256 UTF-8 bytes, trimmed and control-free."},
                "idempotency_key":{"type":"string","minLength":1,"maxLength":512,"description":"Optional stable retry identity; at most 512 UTF-8 bytes, trimmed and control-free."}
            }
        })
    }
    #[expect(
        clippy::significant_drop_tightening,
        clippy::too_many_lines,
        reason = "Preserve the original handler ownership, future shape, and call ordering."
    )]
    async fn execute(&self, ctx: &ToolContext, params: Value) -> McpResult<Value> {
        let input: Input = serde_json::from_value(params)
            .map_err(|failure| McpError::InvalidParameters(failure.to_string()))?;
        let space = &ctx.principal.space_id;
        if space.is_empty() {
            return Err(error(
                ErrorCode::ForbiddenSpace,
                "Select the named space with X-Space-Id",
            ));
        }
        let has = |scope: &str| {
            ctx.authz.is_skip_auth()
                || ctx.principal.scopes.contains(scope)
                || ctx.principal.scopes.contains("*")
        };
        let write = matches!(self.action, SourceAction::Retract | SourceAction::Erase);
        if !has("read:source") || (write && !has("write:source")) {
            return Err(error(ErrorCode::ForbiddenSpace,"Source lifecycle requires read:source and write:source; metadata requires read:source"));
        }
        if SpaceConfigRepository::new(self.storage.clone())
            .get_by_space_id(space)
            .await
            .map_err(storage_error)?
            .is_none()
        {
            return Err(error(ErrorCode::UnknownSpace, "Unknown space"));
        }
        let repository = SourcePublicationRepository::new(self.storage.clone());
        let value = if matches!(self.action, SourceAction::Operation) {
            let id = required(input.operation_id.as_deref(), 1024, "Invalid operation_id")?;
            let receipt = repository
                .operation(space, &id)
                .await
                .map_err(storage_error)?
                .ok_or_else(|| error(ErrorCode::NotFound, "Operation not found"))?;
            let envelope = repository
                .operation_authorization(space, &id)
                .await
                .map_err(storage_error)?
                .ok_or_else(|| {
                    error(
                        ErrorCode::NotReady,
                        "Operation authorization envelope unavailable",
                    )
                })?;
            ctx.authz
                .authorize_record_read(&ctx.principal, &envelope, "read:source")
                .map_err(|_| {
                    error(
                        ErrorCode::ForbiddenSpace,
                        "Operation metadata is not authorized",
                    )
                })?;
            serde_json::to_value(publication_wire::operation(&receipt))
                .map_err(|failure| McpError::ExecutionError(failure.to_string()))?
        } else {
            let external = required(
                input.external_source_id.as_deref(),
                1024,
                "Invalid external_source_id",
            )?;
            let head = repository
                .external_head(space, SourceType::Manual, &external)
                .await
                .map_err(storage_error)?
                .ok_or_else(|| error(ErrorCode::NotFound, "Source head not found"))?;
            let authorized = repository
                .authorization_snapshot(space, &head.source_id)
                .await
                .map_err(storage_error)?
                .ok_or_else(|| {
                    error(
                        ErrorCode::NotReady,
                        "Source authorization envelope unavailable",
                    )
                })?;
            ctx.authz
                .authorize_record_read(&ctx.principal, &authorized.envelope, "read:source")
                .map_err(|_| {
                    error(
                        ErrorCode::ForbiddenSpace,
                        "Source metadata is not authorized",
                    )
                })?;
            if write {
                ctx.authz
                    .authorize_record_write(&ctx.principal, &authorized.envelope, "write:source")
                    .map_err(|_| {
                        error(
                            ErrorCode::ForbiddenSpace,
                            "Source lifecycle is not authorized",
                        )
                    })?;
                let generation = required(
                    input.expected_generation.as_deref(),
                    256,
                    "Invalid expected_generation",
                )?;
                let action = if matches!(self.action, SourceAction::Erase) {
                    "erase"
                } else {
                    "retract"
                };
                let fingerprint = hex::encode(Sha256::digest(
                    serde_json::to_vec(&json!([action, space, external, generation]))
                        .map_err(|failure| McpError::InvalidParameters(failure.to_string()))?,
                ));
                let key = input.idempotency_key.as_deref().unwrap_or(&fingerprint);
                let key = required(Some(key), 512, "Invalid idempotency_key")?;
                let actor = ctx
                    .verified_caller()
                    .map_or("mcp-source-operator", |caller| caller.claims().sub.as_str())
                    .to_owned();
                let mutation = SourceMutation {
                    authorized_document: None,
                    space_id: space.clone(),
                    source_id: head.source_id,
                    expected: SourcePrecondition::Generation(generation),
                    idempotency_key: key,
                    request_fingerprint: fingerprint,
                    actor,
                    authorized,
                };
                let receipt = if matches!(self.action, SourceAction::Erase) {
                    repository.erase_live(mutation).await
                } else {
                    repository.retract(mutation).await
                }
                .map_err(storage_error)?;
                let envelope = repository
                    .operation_authorization(space, &receipt.operation_id)
                    .await
                    .map_err(storage_error)?
                    .ok_or_else(|| {
                        error(
                            ErrorCode::NotReady,
                            "Operation authorization envelope unavailable",
                        )
                    })?;
                ctx.authz
                    .authorize_record_read(&ctx.principal, &envelope, "read:source")
                    .map_err(|_| {
                        error(
                            ErrorCode::ForbiddenSpace,
                            "Original operation metadata is not authorized",
                        )
                    })?;
                serde_json::to_value(publication_wire::accepted_source(&receipt))
                    .map_err(|failure| McpError::ExecutionError(failure.to_string()))?
            } else {
                let receipt = repository
                    .operation(space, &head.operation_id)
                    .await
                    .map_err(storage_error)?
                    .ok_or_else(|| error(ErrorCode::NotReady, "Head operation unavailable"))?;
                let envelope = repository
                    .operation_authorization(space, &head.operation_id)
                    .await
                    .map_err(storage_error)?
                    .ok_or_else(|| error(ErrorCode::NotReady, "Head authorization unavailable"))?;
                ctx.authz
                    .authorize_record_read(&ctx.principal, &envelope, "read:source")
                    .map_err(|_| {
                        error(ErrorCode::ForbiddenSpace, "Head metadata is not authorized")
                    })?;
                if repository
                    .external_head(space, SourceType::Manual, &external)
                    .await
                    .map_err(storage_error)?
                    .as_ref()
                    != Some(&head)
                    || repository
                        .authorization_snapshot(space, &head.source_id)
                        .await
                        .map_err(storage_error)?
                        .as_ref()
                        != Some(&authorized)
                {
                    return Err(error(
                        ErrorCode::NotReady,
                        "Source head or authorization changed during read; retry",
                    ));
                }
                publication_wire::head(&head, &receipt)
            }
        };
        serde_json::to_value(ToolResponse::success(value))
            .map_err(|failure| McpError::ExecutionError(failure.to_string()))
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
    async fn head_never_infers_a_space_for_a_global_caller(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let storage = StorageClient::new_embedded().await?;
        let ctx = ToolContext::new(
            storage.clone(),
            elephant_policy::Principal {
                space_id: String::new(),
                scopes: HashSet::from(["*".into()]),
                clearances: vec![],
                subject_allowlist: None,
                purpose: HashSet::new(),
            },
        );
        let result = SourceControlHandler::new(storage, SourceAction::Head)
            .execute(&ctx, json!({"external_source_id":"source"}))
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
