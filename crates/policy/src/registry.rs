//! Policy registry for loading and caching policies.

use std::collections::HashMap;
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

use crate::error::{PolicyError, PolicyResult};

/// A policy definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyDefinition {
    /// Unique policy identifier.
    pub policy_id: String,
    /// Minimum level.
    #[serde(default)]
    pub min_level: i32,
    /// Maximum level.
    pub max_level: i32,
    /// Available labels.
    #[serde(default)]
    pub labels: Vec<String>,
    /// Available handling caveats.
    #[serde(default)]
    pub handling: Vec<String>,
    /// Default level for new records.
    #[serde(default)]
    pub default_level: i32,
    /// Default labels for new records.
    #[serde(default)]
    pub default_labels: Vec<String>,
    /// Default handling for new records.
    #[serde(default)]
    pub default_handling: Vec<String>,
}

impl PolicyDefinition {
    /// Validates a level against this policy.
    ///
    /// # Errors
    ///
    /// Returns an error if the level is outside the policy range.
    pub fn validate_level(&self, level: i32) -> PolicyResult<()> {
        if level < self.min_level || level > self.max_level {
            let min_level = self.min_level;
            let max_level = self.max_level;
            return Err(PolicyError::InvalidPolicy(format!(
                "level {level} out of range [{min_level}, {max_level}]"
            )));
        }
        Ok(())
    }

    /// Validates labels against this policy.
    ///
    /// # Errors
    ///
    /// Returns an error if any label is not allowed by the policy.
    pub fn validate_labels(&self, labels: &[String]) -> PolicyResult<()> {
        for label in labels {
            if !self.labels.contains(label) {
                return Err(PolicyError::InvalidPolicy(format!(
                    "unknown label: {label}"
                )));
            }
        }
        Ok(())
    }
}

/// Thread-safe policy registry with caching.
pub struct PolicyRegistry {
    policies: RwLock<HashMap<String, PolicyDefinition>>,
}

impl PolicyRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            policies: RwLock::new(HashMap::new()),
        }
    }

    /// Registers a policy definition.
    ///
    /// # Errors
    ///
    /// Returns an error if the registry lock is poisoned.
    pub fn register(&self, policy: PolicyDefinition) -> PolicyResult<()> {
        self.policies
            .write()
            .map_err(|_| PolicyError::RegistryError("lock poisoned".to_string()))?
            .insert(policy.policy_id.clone(), policy);
        Ok(())
    }

    /// Gets a policy by ID.
    ///
    /// # Errors
    ///
    /// Returns an error if the registry lock is poisoned or the policy is missing.
    pub fn get(&self, policy_id: &str) -> PolicyResult<PolicyDefinition> {
        let policies = self
            .policies
            .read()
            .map_err(|_| PolicyError::RegistryError("lock poisoned".to_string()))?;
        policies
            .get(policy_id)
            .cloned()
            .ok_or_else(|| PolicyError::PolicyNotFound(policy_id.to_string()))
    }

    /// Lists all registered policy IDs.
    #[must_use = "the returned policy IDs should be used"]
    ///
    /// # Errors
    ///
    /// Returns an error if the registry lock is poisoned.
    pub fn list_policy_ids(&self) -> PolicyResult<Vec<String>> {
        let policies = self
            .policies
            .read()
            .map_err(|_| PolicyError::RegistryError("lock poisoned".to_string()))?;
        Ok(policies.keys().cloned().collect())
    }

    /// Removes a policy by ID.
    ///
    /// # Errors
    ///
    /// Returns an error if the registry lock is poisoned.
    pub fn remove(&self, policy_id: &str) -> PolicyResult<()> {
        self.policies
            .write()
            .map_err(|_| PolicyError::RegistryError("lock poisoned".to_string()))?
            .remove(policy_id);
        Ok(())
    }

    /// Clears all policies.
    ///
    /// # Errors
    ///
    /// Returns an error if the registry lock is poisoned.
    pub fn clear(&self) -> PolicyResult<()> {
        self.policies
            .write()
            .map_err(|_| PolicyError::RegistryError("lock poisoned".to_string()))?
            .clear();
        Ok(())
    }
}

impl Default for PolicyRegistry {
    fn default() -> Self {
        Self::new()
    }
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

    fn make_test_policy() -> PolicyDefinition {
        PolicyDefinition {
            policy_id: "test_policy".to_string(),
            min_level: 0,
            max_level: 3,
            labels: vec!["comp:alerts".to_string(), "vis:shared_context".to_string()],
            handling: vec!["handling:trusted_context".to_string()],
            default_level: 0,
            default_labels: vec!["vis:shared_context".to_string()],
            default_handling: vec![],
        }
    }

    #[test]
    fn test_registry_crud() {
        let registry = PolicyRegistry::new();
        let policy = make_test_policy();

        let registered = registry.register(policy);
        assert!(registered.is_ok());
        let retrieved = registry.get("test_policy");
        assert!(retrieved.is_ok());
        if let Ok(retrieved) = retrieved {
            assert_eq!(retrieved.policy_id, "test_policy");
        }

        let ids = registry.list_policy_ids();
        assert!(ids.is_ok());
        if let Ok(ids) = ids {
            assert!(ids.contains(&"test_policy".to_string()));
        }

        let removed = registry.remove("test_policy");
        assert!(removed.is_ok());
        assert!(registry.get("test_policy").is_err());
    }

    #[test]
    fn test_policy_validation() {
        let policy = make_test_policy();

        assert!(policy.validate_level(0).is_ok());
        assert!(policy.validate_level(3).is_ok());
        assert!(policy.validate_level(4).is_err());
        assert!(policy.validate_level(-1).is_err());

        assert!(policy.validate_labels(&["comp:alerts".to_string()]).is_ok());
        assert!(policy.validate_labels(&["unknown".to_string()]).is_err());
    }
}
