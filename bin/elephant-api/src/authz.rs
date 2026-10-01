//! ABAC authorization layer for request-level access control.
//!
//! This module bridges the JWT authentication layer with the ABAC policy engine,
//! providing record-level access control for API endpoints.

use std::collections::HashSet;
use std::sync::Arc;

use axum::{
    body::Body,
    extract::Request,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use elephant_policy::{
    AbacEngine, AccessDecision, Clearance, Principal as AbacPrincipal, RecordAttributes,
};
use elephant_types::records::base::{Security, SubjectsState};
use elephant_types::records::{Entity, HasSecurity};
use elephant_types::Link;
use serde::Serialize;

use crate::auth::Principal as AuthPrincipal;

/// Authorization error response.
#[derive(Debug, Clone, Serialize)]
pub struct AuthzError {
    /// Error code.
    pub code: String,
    /// Error message.
    pub message: String,
}

impl IntoResponse for AuthzError {
    fn into_response(self) -> Response {
        (StatusCode::FORBIDDEN, Json(self)).into_response()
    }
}

impl AuthzError {
    /// Creates an access denied error.
    #[must_use]
    pub fn access_denied(reason: &str) -> Self {
        Self {
            code: "ACCESS_DENIED".to_string(),
            message: format!("Access denied: {reason}"),
        }
    }
}

/// ABAC authorization service.
#[derive(Debug, Clone)]
pub struct AuthzService {
    /// The ABAC evaluation engine.
    engine: AbacEngine,
}

impl Default for AuthzService {
    fn default() -> Self {
        Self::new()
    }
}

impl AuthzService {
    /// Creates a new authorization service.
    #[must_use]
    pub fn new() -> Self {
        Self {
            engine: AbacEngine::new(),
        }
    }

    /// Creates an authorization service with required purposes.
    #[must_use]
    #[allow(dead_code)]
    pub fn with_purposes(purposes: HashSet<String>) -> Self {
        Self {
            engine: AbacEngine::new().with_required_purposes(purposes),
        }
    }

    /// Converts an auth principal to an ABAC principal.
    ///
    /// This maps JWT claims to the ABAC principal structure.
    #[must_use]
    #[allow(clippy::unused_self)]
    pub fn to_abac_principal(&self, auth_principal: &AuthPrincipal) -> AbacPrincipal {
        let mut scopes: HashSet<String> = auth_principal.scopes.iter().cloned().collect();

        // Backward-compatible scope aliases (spec-driven).
        //
        // docs/spec.md uses `read:doc:metadata` for metadata access, while older
        // clients used `read:doc`. Treat these as equivalent.
        if scopes.contains("read:doc") {
            scopes.insert("read:doc:metadata".to_string());
        }
        if scopes.contains("read:doc:metadata") {
            scopes.insert("read:doc".to_string());
        }
        // Raw doc access implies metadata access.
        if scopes.contains("read:doc:raw") {
            scopes.insert("read:doc:metadata".to_string());
            scopes.insert("read:doc".to_string());
        }

        // Raw source access implies metadata access.
        if scopes.contains("read:source:raw") {
            scopes.insert("read:source".to_string());
        }

        // docs/spec.md uses `read:event` / `write:event` while older clients used
        // `read:timeline` / `write:timeline`.
        if scopes.contains("read:timeline") {
            scopes.insert("read:event".to_string());
        }
        if scopes.contains("read:event") {
            scopes.insert("read:timeline".to_string());
        }
        if scopes.contains("write:timeline") {
            scopes.insert("write:event".to_string());
        }
        if scopes.contains("write:event") {
            scopes.insert("write:timeline".to_string());
        }

        let clearances = if auth_principal.clearances.is_empty() {
            // Backward-compatible fallback for older tokens that only provide a flat
            // security_level + security_labels. Prefer explicit clearances when present.
            let labels: HashSet<String> = auth_principal.security_labels.iter().cloned().collect();

            let default_clearance = Clearance {
                policy_id: "default".to_string(),
                max_level: auth_principal.security_level,
                labels: labels.clone(),
                handling_allow: HashSet::new(),
            };

            // Additional clearance for the generic tenant policy used in examples.
            let tenant_clearance = Clearance {
                policy_id: "tenant_policy_v1".to_string(),
                max_level: auth_principal.security_level,
                labels,
                handling_allow: std::iter::once("handling:trusted_context".to_string()).collect(),
            };

            vec![default_clearance, tenant_clearance]
        } else {
            auth_principal.clearances.clone()
        };

        // An absent allowlist is unrestricted; an explicit empty allowlist is
        // still an attenuation and must retain its restriction.
        let subject_allowlist = auth_principal
            .subject_allowlist
            .as_ref()
            .map(|subjects| subjects.iter().cloned().collect());

        let purpose: HashSet<String> = auth_principal.purpose.iter().cloned().collect();

        AbacPrincipal {
            space_id: auth_principal.space_id.clone().unwrap_or_default(),
            scopes,
            clearances,
            subject_allowlist,
            purpose,
        }
    }

    /// Creates record attributes from a security block and space ID.
    #[must_use]
    #[allow(dead_code)]
    pub fn record_attributes(
        space_id: &str,
        security: &Security,
        subjects: Vec<Link<Entity>>,
        subjects_state: SubjectsState,
    ) -> RecordAttributes {
        RecordAttributes {
            space_id: space_id.to_string(),
            security: security.clone(),
            subjects,
            subjects_state,
        }
    }

    /// Creates record attributes from any object implementing `HasSecurity`.
    #[must_use]
    #[allow(clippy::unused_self)]
    pub fn record_attributes_from_generic(&self, record: &impl HasSecurity) -> RecordAttributes {
        RecordAttributes {
            space_id: record.space_id().to_string(),
            security: record.security().clone(),
            subjects: record.subjects(),
            subjects_state: record.subjects_state(),
        }
    }

    /// Evaluates access for a principal against record attributes.
    ///
    /// # Errors
    ///
    /// Returns an `AuthzError` if access is denied.
    pub fn evaluate(
        &self,
        auth_principal: &AuthPrincipal,
        record: &RecordAttributes,
        required_scope: &str,
    ) -> Result<(), AuthzError> {
        if auth_principal.bypass_abac {
            return Ok(());
        }

        let mut abac_principal = self.to_abac_principal(auth_principal);
        if auth_principal.scopes.iter().any(|s| s == "*") {
            abac_principal.scopes.insert(required_scope.to_string());
        }
        let decision = self
            .engine
            .evaluate(&abac_principal, record, required_scope);

        match decision {
            AccessDecision::Allow => Ok(()),
            AccessDecision::Deny(reason) => Err(AuthzError::access_denied(&reason)),
        }
    }

    /// Generic record read authorization.
    ///
    /// # Errors
    ///
    /// Returns an `AuthzError` if access is denied.
    pub fn authorize_record_read(
        &self,
        auth_principal: &AuthPrincipal,
        record: &impl HasSecurity,
        required_scope: &str,
    ) -> Result<(), AuthzError> {
        let record_attrs = self.record_attributes_from_generic(record);
        self.evaluate(auth_principal, &record_attrs, required_scope)
    }

    /// Generic record write authorization.
    ///
    /// # Errors
    ///
    /// Returns an `AuthzError` if access is denied.
    pub fn authorize_record_write(
        &self,
        auth_principal: &AuthPrincipal,
        record: &impl HasSecurity,
        required_scope: &str,
    ) -> Result<(), AuthzError> {
        let record_attrs = self.record_attributes_from_generic(record);
        self.evaluate(auth_principal, &record_attrs, required_scope)
    }

    /// Generic record list filtering.
    #[must_use]
    pub fn filter_records<T: HasSecurity>(
        &self,
        auth_principal: &AuthPrincipal,
        records: Vec<T>,
        required_scope: &str,
    ) -> Vec<T> {
        records
            .into_iter()
            .filter(|r| {
                self.authorize_record_read(auth_principal, r, required_scope)
                    .is_ok()
            })
            .collect()
    }
}

/// Shared authorization state.
pub type AuthzState = Arc<AuthzService>;

/// Extracts the auth principal from the request.
///
/// Returns None if not authenticated.
#[allow(dead_code)]
pub fn extract_auth_principal(request: &Request<Body>) -> Option<&AuthPrincipal> {
    request.extensions().get::<AuthPrincipal>()
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::mixed_attributes_style,
        clippy::similar_names,
        clippy::type_complexity,
        clippy::float_cmp,
        clippy::const_is_empty,
        clippy::expect_used,
        clippy::panic,
        unused_imports,
        dead_code,
        clippy::too_many_lines,
        clippy::cognitive_complexity
    )]
    use super::*;
    use elephant_types::ids::TableName;
    use elephant_types::records::base::{BaseFields, Provenance};
    use elephant_types::records::entity::EntityType;
    use elephant_types::RecordId;

    fn create_test_auth_principal() -> AuthPrincipal {
        AuthPrincipal {
            subject: "test-user".to_string(),
            space_id: Some("test_space".to_string()),
            scopes: vec!["read:entity".to_string(), "write:entity".to_string()],
            security_level: 2,
            security_labels: vec!["comp:alerts".to_string()],
            ..AuthPrincipal::default()
        }
    }

    fn create_test_entity(level: i32, labels: Vec<String>) -> Entity {
        let id = RecordId::new_v4(TableName::Entity);
        let mut base = BaseFields::new(id, "test_space".to_string(), "test_agent".to_string());
        base.security = Security {
            policy_id: "tenant_policy_v1".to_string(),
            level,
            labels,
            handling: vec![],
        };
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Unknown;

        Entity::new(base, EntityType::new("person"), "Test Entity".to_string())
    }

    #[test]
    fn test_authz_service_creation() {
        let service = AuthzService::new();
        // Verify engine was created
        let _ = service;
    }

    #[test]
    fn test_to_abac_principal() {
        let service = AuthzService::new();
        let auth = create_test_auth_principal();
        let abac = service.to_abac_principal(&auth);

        assert_eq!(abac.space_id, "test_space");
        assert!(abac.scopes.contains("read:entity"));
        assert!(abac.scopes.contains("write:entity"));
        assert_eq!(abac.clearances.len(), 2);
        assert_eq!(abac.clearances[0].max_level, 2);
    }

    #[test]
    fn test_subject_allowlist_preserved_through_claims_and_abac_serialization() {
        let service = AuthzService::new();
        let subject = RecordId::new_v4(TableName::Entity);
        for allowlist in [None, Some(vec![]), Some(vec![subject])] {
            let mut claims_json = serde_json::json!({
                "sub": "test-user",
                "exp": 0,
                "space_id": "test_space",
            });
            if let Some(ref subjects) = allowlist {
                claims_json["subject_allowlist"] = serde_json::to_value(subjects).unwrap();
            }
            let claims = serde_json::from_value::<crate::auth::Claims>(claims_json).unwrap();
            let auth = AuthPrincipal::from(claims);
            assert_eq!(auth.subject_allowlist, allowlist);

            let abac = service.to_abac_principal(&auth);
            let expected: Option<HashSet<RecordId>> =
                allowlist.map(|subjects| subjects.into_iter().collect());
            assert_eq!(abac.subject_allowlist, expected);

            let encoded = serde_json::to_value(&abac).unwrap();
            assert_eq!(
                encoded.get("subject_allowlist").is_some(),
                expected.is_some()
            );
            let decoded: AbacPrincipal = serde_json::from_value(encoded).unwrap();
            assert_eq!(decoded.subject_allowlist, expected);
        }
    }

    #[test]
    fn test_empty_subject_allowlist_denies_entity_reads_writes_and_lists() {
        let service = AuthzService::new();
        let entities = vec![
            create_test_entity(0, vec![]),
            create_test_entity(1, vec!["comp:alerts".to_string()]),
        ];
        for scopes in [
            vec!["read:entity".to_string(), "write:entity".to_string()],
            vec!["*".to_string()],
        ] {
            let mut auth = create_test_auth_principal();
            auth.scopes = scopes;
            for entity in &entities {
                assert!(service
                    .authorize_record_read(&auth, entity, "read:entity")
                    .is_ok());
                assert!(service
                    .authorize_record_write(&auth, entity, "write:entity")
                    .is_ok());
            }

            auth.subject_allowlist = Some(vec![]);
            for entity in &entities {
                let read_error = service
                    .authorize_record_read(&auth, entity, "read:entity")
                    .unwrap_err();
                let write_error = service
                    .authorize_record_write(&auth, entity, "write:entity")
                    .unwrap_err();
                assert!(read_error.message.contains("not in allowlist"));
                assert!(write_error.message.contains("not in allowlist"));
            }
            assert!(service
                .filter_records(&auth, entities.clone(), "read:entity")
                .is_empty());
        }
    }

    #[test]
    fn test_nonempty_subject_allowlist_limits_entity_access() {
        let service = AuthzService::new();
        let mut auth = create_test_auth_principal();
        let allowed = create_test_entity(1, vec![]);
        let denied = create_test_entity(1, vec![]);
        auth.subject_allowlist = Some(vec![allowed.base.id.clone()]);

        assert!(service
            .authorize_record_read(&auth, &allowed, "read:entity")
            .is_ok());
        assert!(service
            .authorize_record_write(&auth, &allowed, "write:entity")
            .is_ok());
        assert!(service
            .authorize_record_read(&auth, &denied, "read:entity")
            .is_err());
        assert!(service
            .authorize_record_write(&auth, &denied, "write:entity")
            .is_err());
    }

    #[test]
    fn test_empty_subject_allowlist_preserves_explicit_development_bypass() {
        let service = AuthzService::new();
        let mut auth = create_test_auth_principal();
        auth.subject_allowlist = Some(vec![]);
        auth.bypass_abac = true;
        let entity = create_test_entity(1, vec![]);

        assert!(service
            .authorize_record_read(&auth, &entity, "read:entity")
            .is_ok());
        assert!(service
            .authorize_record_write(&auth, &entity, "write:entity")
            .is_ok());
    }

    #[tokio::test]
    async fn test_empty_subject_allowlist_does_not_grant_wiki_management() {
        use elephant_mcp::handlers::StopManagedWikiHandler;
        use elephant_mcp::{ToolContext, ToolHandler};
        use elephant_storage::{StorageClient, WorkItemRepository};

        let service = AuthzService::new();
        let storage = StorageClient::new_embedded().await.unwrap();
        let repo = WorkItemRepository::new(storage.clone());
        let grant = repo
            .create_managed_wiki_grant("test_space", "manager")
            .await
            .unwrap();
        let handler = StopManagedWikiHandler::new(storage.clone());
        let mut auth = create_test_auth_principal();
        auth.scopes = vec!["manage:wiki".to_string()];
        auth.subject_allowlist = Some(vec![]);

        let ctx = ToolContext::new(storage.clone(), service.to_abac_principal(&auth));
        let response = handler.execute(&ctx, serde_json::json!({})).await.unwrap();
        assert_eq!(response["ok"], false);
        assert!(response["error"]
            .as_str()
            .unwrap()
            .contains("unrestricted subject scope"));
        assert!(repo
            .managed_wiki_grant_is_active("test_space", &grant)
            .await
            .unwrap());

        auth.subject_allowlist = None;
        let ctx = ToolContext::new(storage, service.to_abac_principal(&auth));
        let response = handler.execute(&ctx, serde_json::json!({})).await.unwrap();
        assert_eq!(response["ok"], true);
        assert!(!repo
            .managed_wiki_grant_is_active("test_space", &grant)
            .await
            .unwrap());
    }

    #[test]
    fn test_authorize_entity_read_allowed() {
        let service = AuthzService::new();
        let auth = create_test_auth_principal();
        let entity = create_test_entity(1, vec!["comp:alerts".to_string()]);

        let result = service.authorize_record_read(&auth, &entity, "read:entity");
        assert!(result.is_ok());
    }

    #[test]
    fn test_authorize_entity_read_denied_level() {
        let service = AuthzService::new();
        let auth = create_test_auth_principal();
        let entity = create_test_entity(5, vec![]); // Level 5 > max_level 2

        let result = service.authorize_record_read(&auth, &entity, "read:entity");
        assert!(result.is_err());
    }

    #[test]
    fn test_authorize_entity_read_denied_space() {
        let service = AuthzService::new();
        let auth = create_test_auth_principal();

        let id = RecordId::new_v4(TableName::Entity);
        let mut base = BaseFields::new(id, "other_space".to_string(), "test_agent".to_string());
        base.security = Security {
            policy_id: "tenant_policy_v1".to_string(),
            level: 1,
            labels: vec![],
            handling: vec![],
        };
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Unknown;

        let entity = Entity::new(base, EntityType::new("person"), "Test Entity".to_string());

        let result = service.authorize_record_read(&auth, &entity, "read:entity");
        assert!(result.is_err());
    }

    #[test]
    fn test_filter_records() {
        let service = AuthzService::new();
        let auth = create_test_auth_principal();

        let entities = vec![
            create_test_entity(1, vec![]), // Allowed (level 1)
            create_test_entity(2, vec![]), // Allowed (level 2)
            create_test_entity(5, vec![]), // Denied (level 5 > max 2)
            create_test_entity(1, vec!["comp:unknown".to_string()]), // Denied (missing label clearance)
        ];

        let filtered = service.filter_records(&auth, entities, "read:entity");
        assert_eq!(filtered.len(), 2);
    }
}
