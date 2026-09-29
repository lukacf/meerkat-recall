//! Dynamic embedding partition management.
//!
//! Provides partition-based table management for embedding vectors,
//! enabling security-level isolation per policy/level combination.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::error::{EmbeddingError, EmbeddingResult};
use crate::EmbeddingPolicy;

/// Partition naming convention.
///
/// # Table Name Encoding
///
/// Partition table names use hex encoding for all string components to ensure:
/// 1. **Collision-free mapping**: Different inputs never produce the same table name
/// 2. **Fully reversible**: Original values can be recovered from the table name
/// 3. **Safe identifiers**: Only alphanumeric characters in the resulting name
///
/// Format: `emb__{hex(policy_id)}__{hex(level_bucket)}__{hex(model)}__{dims}`
///
/// Example: `policy1` + `L0` + `model1` + `3072` → `emb__706f6c69637931__4c30__6d6f64656c31__3072`
#[derive(Debug, Clone)]
pub struct PartitionKey {
    /// Policy ID (non-empty).
    pub policy_id: String,
    /// Level bucket (non-empty).
    pub level_bucket: String,
    /// Embedding model (non-empty).
    pub model: String,
    /// Vector dimensions (must be > 0).
    pub dims: usize,
}

impl PartitionKey {
    /// Creates a new partition key.
    ///
    /// # Errors
    ///
    /// Returns an error if any string field is empty or if dims is 0.
    pub fn new(
        policy_id: String,
        level_bucket: String,
        model: String,
        dims: usize,
    ) -> EmbeddingResult<Self> {
        if policy_id.is_empty() {
            return Err(EmbeddingError::PartitionError(
                "policy_id cannot be empty".to_string(),
            ));
        }
        if level_bucket.is_empty() {
            return Err(EmbeddingError::PartitionError(
                "level_bucket cannot be empty".to_string(),
            ));
        }
        if model.is_empty() {
            return Err(EmbeddingError::PartitionError(
                "model cannot be empty".to_string(),
            ));
        }
        if dims == 0 {
            return Err(EmbeddingError::PartitionError(
                "dimensions must be greater than 0".to_string(),
            ));
        }

        Ok(Self {
            policy_id,
            level_bucket,
            model,
            dims,
        })
    }

    /// Generates the `SurrealDB` table name for this partition key.
    #[must_use]
    pub fn table_name(&self) -> String {
        format!(
            "emb__{}__{}__{}__{}",
            hex_encode(&self.policy_id),
            hex_encode(&self.level_bucket),
            hex_encode(&self.model),
            self.dims
        )
    }

    /// Parses a table name back into a partition key.
    ///
    /// # Errors
    ///
    /// Returns an error if the table name is not a valid partition name.
    pub fn from_table_name(name: &str) -> EmbeddingResult<Self> {
        let prefix = "emb__";
        if !name.starts_with(prefix) {
            return Err(EmbeddingError::PartitionError(
                "Invalid partition name: missing prefix".to_string(),
            ));
        }

        let parts: Vec<&str> = name[prefix.len()..].split("__").collect();
        if parts.len() != 4 {
            return Err(EmbeddingError::PartitionError(
                "Invalid partition name: incorrect parts count".to_string(),
            ));
        }

        let policy_id = hex_decode(parts[0])?;
        let level_bucket = hex_decode(parts[1])?;
        let model = hex_decode(parts[2])?;
        let dims = parts[3]
            .parse::<usize>()
            .map_err(|e| EmbeddingError::PartitionError(format!("Invalid dimensions: {e}")))?;

        Self::new(policy_id, level_bucket, model, dims)
    }
}

/// Returns the level bucket name for a security level.
#[must_use]
pub fn level_bucket(level: i32) -> String {
    format!("L{level}")
}

/// Manager for vector storage partitions.
#[derive(Debug, Clone, Default)]
pub struct PartitionManager {
    known_partitions: Arc<RwLock<HashSet<String>>>,
}

impl PartitionManager {
    /// Creates a new partition manager.
    #[must_use]
    pub fn new() -> Self {
        Self {
            known_partitions: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Returns the DDL for creating a partition table.
    #[must_use]
    pub fn create_partition_ddl(key: &PartitionKey) -> String {
        let table_name = key.table_name();
        format!(
            "DEFINE TABLE IF NOT EXISTS {} SCHEMALESS;
             DEFINE INDEX IF NOT EXISTS {}_hnsw ON TABLE {} FIELDS vector HNSW DIMENSION {} TYPE F32 DIST COSINE;",
            table_name, table_name, table_name, key.dims
        )
    }

    /// Returns the list of known partition table names.
    pub async fn known_partition_names(&self) -> Vec<String> {
        let partitions = self.known_partitions.read().await;
        partitions.iter().cloned().collect()
    }

    /// Discovers existing partitions from a list of table names.
    pub async fn discover_partitions(&self, tables: &[String]) {
        let mut partitions = self.known_partitions.write().await;
        for table in tables {
            if table.starts_with("emb__") && PartitionKey::from_table_name(table).is_ok() {
                partitions.insert(table.clone());
            }
        }
    }

    /// Clears the known partitions cache.
    pub async fn clear_cache(&self) {
        let mut partitions = self.known_partitions.write().await;
        partitions.clear();
    }

    /// Ensures a partition exists, creating it if necessary.
    ///
    /// # Errors
    ///
    /// Returns an error if creating the partition or executing DDL fails.
    pub async fn ensure_partition<F, Fut>(
        &self,
        key: &PartitionKey,
        execute_ddl: F,
    ) -> EmbeddingResult<()>
    where
        F: FnOnce(String) -> Fut,
        Fut: std::future::Future<Output = Result<(), String>>,
    {
        let table_name = key.table_name();

        let mut partitions = self.known_partitions.write().await;

        if partitions.contains(&table_name) {
            return Ok(());
        }

        let ddl = Self::create_partition_ddl(key);
        execute_ddl(ddl).await.map_err(|e| {
            EmbeddingError::PartitionError(format!("Failed to create partition: {e}"))
        })?;

        partitions.insert(table_name);
        drop(partitions);

        Ok(())
    }
}

/// Discover accessible partition keys from a set of known table names.
#[must_use]
pub fn discover_accessible_partition_keys(
    tables: &[String],
    policy: &EmbeddingPolicy,
    clearances: &[(String, i32)],
    fallback_policy_id: &str,
    fallback_max_level: i32,
) -> Vec<PartitionKey> {
    let mut max_level_by_policy = HashMap::new();
    for (policy_id, max_level) in clearances {
        if *max_level < 0 {
            continue;
        }
        max_level_by_policy
            .entry(policy_id.clone())
            .and_modify(|existing| {
                if *max_level > *existing {
                    *existing = *max_level;
                }
            })
            .or_insert(*max_level);
    }

    if max_level_by_policy.is_empty() && fallback_max_level >= 0 {
        max_level_by_policy.insert(fallback_policy_id.to_string(), fallback_max_level);
    }

    let mut keys = Vec::new();
    let mut seen = HashSet::new();

    for table in tables {
        if !table.starts_with("emb__") {
            continue;
        }

        let Ok(key) = PartitionKey::from_table_name(table) else {
            continue;
        };
        if key.model != policy.model || key.dims != policy.dims {
            continue;
        }

        let Some(max_level) = max_level_by_policy.get(&key.policy_id).copied() else {
            continue;
        };
        let Some(level) = parse_level_bucket(&key.level_bucket) else {
            continue;
        };
        if level > max_level {
            continue;
        }

        if seen.insert(table.clone()) {
            keys.push(key);
        }
    }

    keys.sort_by(|left, right| {
        left.policy_id
            .cmp(&right.policy_id)
            .then_with(|| left.level_bucket.cmp(&right.level_bucket))
            .then_with(|| left.model.cmp(&right.model))
            .then_with(|| left.dims.cmp(&right.dims))
    });
    keys
}

/// Helper for hex encoding strings.
fn hex_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 2);
    for b in s.as_bytes() {
        use std::fmt::Write;
        let _ = write!(&mut out, "{b:02x}");
    }
    out
}

/// Helper for hex decoding strings.
fn hex_decode(s: &str) -> EmbeddingResult<String> {
    if !s.len().is_multiple_of(2) {
        return Err(EmbeddingError::PartitionError(
            "odd length hex string".to_string(),
        ));
    }
    let mut bytes = Vec::new();
    for i in (0..s.len()).step_by(2) {
        let b = u8::from_str_radix(&s[i..i + 2], 16)
            .map_err(|e| EmbeddingError::PartitionError(format!("invalid hex encoding: {e}")))?;
        bytes.push(b);
    }
    String::from_utf8(bytes)
        .map_err(|e| EmbeddingError::PartitionError(format!("invalid UTF-8 after hex decode: {e}")))
}

fn parse_level_bucket(bucket: &str) -> Option<i32> {
    bucket.strip_prefix('L')?.parse().ok()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn discover_accessible_partition_keys_uses_known_tables_instead_of_expanding_max_level() {
        let policy = EmbeddingPolicy::new("hashed", "hashed-bow-v1", 256);
        let tables = vec![
            PartitionKey::new(
                "default".to_string(),
                level_bucket(0),
                "hashed-bow-v1".to_string(),
                256,
            )
            .unwrap()
            .table_name(),
            PartitionKey::new(
                "default".to_string(),
                level_bucket(4),
                "hashed-bow-v1".to_string(),
                256,
            )
            .unwrap()
            .table_name(),
            PartitionKey::new(
                "other".to_string(),
                level_bucket(0),
                "hashed-bow-v1".to_string(),
                256,
            )
            .unwrap()
            .table_name(),
        ];

        let keys = discover_accessible_partition_keys(
            &tables,
            &policy,
            &[("default".to_string(), i32::MAX)],
            "default",
            0,
        );

        assert_eq!(keys.len(), 2);
        assert!(keys.iter().all(|key| key.policy_id == "default"));
    }
}
