//! ABAC authorization helpers for MCP tool handlers.
//!
//! This module provides record-level authorization utilities (ABAC) for MCP tools.

use std::collections::HashSet;

use elephant_policy::{AbacEngine, AccessDecision, Principal as AbacPrincipal, RecordAttributes};
use elephant_types::records::base::{Security, SubjectsState};
use elephant_types::records::HasSecurity;

/// ABAC authorization service.
#[derive(Debug, Clone)]
pub struct AuthzService {
    engine: AbacEngine,
    skip_auth: bool,
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
            skip_auth: false,
        }
    }

    /// Creates a new authorization service with optional bypass (dev mode).
    #[must_use]
    pub fn new_with_skip_auth(skip_auth: bool) -> Self {
        Self {
            engine: AbacEngine::new(),
            skip_auth,
        }
    }

    /// Returns whether authz is bypassed for development mode.
    #[must_use]
    pub const fn is_skip_auth(&self) -> bool {
        self.skip_auth
    }

    /// Evaluates access for a principal against record attributes.
    ///
    /// # Errors
    ///
    /// Returns an error string if access is denied.
    pub fn evaluate(
        &self,
        principal: &AbacPrincipal,
        record: &RecordAttributes,
        required_scope: &str,
    ) -> Result<(), String> {
        if self.skip_auth {
            return Ok(());
        }
        let mut principal = principal.clone();
        Self::normalize_scopes(&mut principal.scopes);
        if principal.scopes.contains("*") {
            principal.scopes.insert(required_scope.to_string());
        }

        match self.engine.evaluate(&principal, record, required_scope) {
            AccessDecision::Allow => Ok(()),
            AccessDecision::Deny(reason) => Err(reason),
        }
    }

    fn normalize_scopes(scopes: &mut HashSet<String>) {
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

        // Event/timeline tools use explicit `read:event` / `write:event` scopes directly.
    }

    /// Returns record attributes for a record.
    #[must_use]
    pub fn record_attributes_from_record(&self, record: &impl HasSecurity) -> RecordAttributes {
        RecordAttributes {
            space_id: record.space_id().to_string(),
            security: record.security().clone(),
            subjects: record.subjects(),
            subjects_state: record.subjects_state(),
        }
    }

    /// Generic record read authorization.
    ///
    /// # Errors
    ///
    /// Returns an error string if access is denied.
    pub fn authorize_record_read(
        &self,
        principal: &AbacPrincipal,
        record: &impl HasSecurity,
        required_scope: &str,
    ) -> Result<(), String> {
        let record_attrs = self.record_attributes_from_record(record);
        self.evaluate(principal, &record_attrs, required_scope)
    }

    /// Generic record write authorization.
    ///
    /// # Errors
    ///
    /// Returns an error string if access is denied.
    pub fn authorize_record_write(
        &self,
        principal: &AbacPrincipal,
        record: &impl HasSecurity,
        required_scope: &str,
    ) -> Result<(), String> {
        let record_attrs = self.record_attributes_from_record(record);
        self.evaluate(principal, &record_attrs, required_scope)
    }

    /// Generic record list filtering.
    #[must_use]
    pub fn filter_records<T: HasSecurity>(
        &self,
        principal: &AbacPrincipal,
        records: Vec<T>,
        required_scope: &str,
    ) -> Vec<T> {
        records
            .into_iter()
            .filter(|r| {
                self.authorize_record_read(principal, r, required_scope)
                    .is_ok()
            })
            .collect()
    }

    /// Creates record attributes for a preflight authorization check.
    ///
    /// This is used for operations that do not reference a specific record yet (e.g. search).
    #[must_use]
    pub fn preflight_record(space_id: &str, policy_id: &str) -> RecordAttributes {
        RecordAttributes {
            space_id: space_id.to_string(),
            security: Security {
                policy_id: policy_id.to_string(),
                level: 0,
                labels: Vec::new(),
                handling: Vec::new(),
            },
            subjects: Vec::new(),
            subjects_state: SubjectsState::Complete,
        }
    }
}

/// Authorize a scope using a preflight record for the caller's current space.
///
/// This is used when a tool needs scope-based authorization but there is no
/// concrete `HasSecurity` record shape available for the target object.
///
/// # Errors
///
/// Returns an error string if the caller has no space or the scope is denied.
pub fn authorize_preflight_scope(
    authz: &AuthzService,
    principal: &AbacPrincipal,
    required_scope: &str,
) -> Result<(), String> {
    let space_id = &principal.space_id;
    if space_id.is_empty() {
        return Err("Access denied: missing space_id".to_string());
    }

    let policy_id = principal
        .clearances
        .first()
        .map_or("default", |clearance| clearance.policy_id.as_str());
    let record = AuthzService::preflight_record(space_id, policy_id);
    authz
        .evaluate(principal, &record, required_scope)
        .map_err(|reason| format!("Access denied: {reason}"))
}
