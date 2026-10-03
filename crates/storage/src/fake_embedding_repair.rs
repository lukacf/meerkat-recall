//! Removes `FakeEmbeddingProvider` output from a store, once.
//!
//! Until the embedding default stopped resolving to `fake`, a store with no
//! space config and no `PIPELINE_EMBED_PROVIDER` embedded every chunk, entity
//! name and query with the fake provider, labelled as the real model
//! (`text-embedding-3-large`). Those vectors are a deterministic ramp,
//! `(base + i * 0.001) % 1`, so similarity search over them ranks noise, and
//! the re-embed gates (model name, text hash) never notice them.
//!
//! The purge deletes ramp vectors from every `emb__` partition and clears the
//! owning node's `embedding_model`, so the embed worker picks the node up
//! again; it clears ramp entity name embeddings (and their stale metadata
//! copies) so the maintenance sweep recomputes them. It records a marker when
//! a pass completes, so later starts do not rescan every partition.

use serde::Deserialize;
use surrealdb::sql::{Array, Thing, Value};

use crate::client::StorageClient;
use crate::error::{StorageError, StorageResult};

const BATCH: usize = 500;
const MARKER: &str = "elephant_maintenance:fake_embedding_purge_v1";

/// What one purge pass removed.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct FakeEmbeddingPurge {
    /// Already purged on an earlier pass; nothing was scanned.
    pub skipped: bool,
    /// Ramp vectors deleted, per partition table.
    pub partitions: Vec<(String, usize)>,
    /// Entities whose ramp name embedding was cleared.
    pub entity_names_cleared: usize,
    /// Entities whose stale `metadata.name_embedding` ramp copy was cleared.
    pub entity_metadata_cleared: usize,
}

/// SurrealQL predicate: `field` holds a `FakeEmbeddingProvider` ramp.
///
/// Two four-element windows 100 apart: the ramp wraps at 1.0 once every 1000
/// elements, so at most one window can straddle a wrap.
fn ramp_condition(field: &str) -> String {
    let window = |start: usize| {
        (start..start + 3)
            .map(|i| {
                format!(
                    "math::abs({field}[{}] - {field}[{i}] - 0.001) < 0.00001",
                    i + 1
                )
            })
            .collect::<Vec<_>>()
            .join(" AND ")
    };
    format!(
        "({field} IS NOT NONE AND array::len({field}) > 104 AND (({}) OR ({})))",
        window(0),
        window(100)
    )
}

#[derive(Debug, Deserialize)]
struct RampRow {
    id: Thing,
    node: Option<Thing>,
}

#[derive(Debug, Deserialize)]
struct IdRow {
    id: Thing,
}

fn things(rows: impl Iterator<Item = Thing>) -> Value {
    Value::Array(Array::from(rows.map(Value::from).collect::<Vec<_>>()))
}

fn take<T: serde::de::DeserializeOwned>(
    mut response: surrealdb::Response,
    what: &str,
) -> StorageResult<Vec<T>> {
    response
        .take(0)
        .map_err(|e| StorageError::QueryError(format!("Failed to parse {what}: {e}")))
}

async fn purge_partition(client: &StorageClient, table: &str) -> StorageResult<usize> {
    let select = format!(
        "SELECT id, node FROM {table} WHERE {} LIMIT {BATCH}",
        ramp_condition("vector")
    );
    let mut deleted = 0;
    loop {
        let rows: Vec<RampRow> = take(client.query_checked(&select).await?, "ramp rows")?;
        if rows.is_empty() {
            return Ok(deleted);
        }
        let count = rows.len();
        let ids = things(rows.iter().map(|row| row.id.clone()));
        let nodes = things(rows.into_iter().filter_map(|row| row.node));
        // One transaction: a deleted vector must never leave its node marked
        // as embedded, or the node silently drops out of search for good.
        client
            .query_bind_checked(
                "BEGIN TRANSACTION; \
                 DELETE $ids; \
                 UPDATE $nodes SET embedding_model = NONE; \
                 COMMIT TRANSACTION;",
                crate::bindings! { "ids" => ids, "nodes" => nodes },
            )
            .await?;
        deleted += count;
    }
}

async fn clear_entity_field(
    client: &StorageClient,
    field: &str,
    clear: &str,
) -> StorageResult<usize> {
    let select = format!(
        "SELECT id FROM entity WHERE {} LIMIT {BATCH}",
        ramp_condition(field)
    );
    let update = format!("UPDATE $ids SET {clear}");
    let mut cleared = 0;
    loop {
        let rows: Vec<IdRow> = take(client.query_checked(&select).await?, "ramp entities")?;
        if rows.is_empty() {
            return Ok(cleared);
        }
        cleared += rows.len();
        let ids = things(rows.into_iter().map(|row| row.id));
        client
            .query_bind_checked(&update, crate::bindings! { "ids" => ids })
            .await?;
    }
}

/// Purge fake embeddings once. Call only when a real provider is configured,
/// so the cleared nodes and entities are re-embedded for real.
///
/// # Errors
///
/// Returns an error if a query fails; a partial pass is safe to repeat.
pub async fn purge_fake_embeddings(client: &StorageClient) -> StorageResult<FakeEmbeddingPurge> {
    if client.embedded_store().is_some() {
        return Ok(FakeEmbeddingPurge {
            skipped: true,
            ..FakeEmbeddingPurge::default()
        });
    }
    let done: Vec<IdRow> = take(
        client
            .query_checked(&format!("SELECT id FROM {MARKER}"))
            .await?,
        "purge marker",
    )?;
    if !done.is_empty() {
        return Ok(FakeEmbeddingPurge {
            skipped: true,
            ..FakeEmbeddingPurge::default()
        });
    }

    let mut report = FakeEmbeddingPurge::default();
    let mut tables: Vec<String> = client
        .list_tables()
        .await?
        .into_iter()
        .filter(|table| {
            table.starts_with("emb__")
                && elephant_embeddings::PartitionKey::from_table_name(table).is_ok()
        })
        .collect();
    tables.sort();
    for table in tables {
        let deleted = purge_partition(client, &table).await?;
        report.partitions.push((table, deleted));
    }
    report.entity_names_cleared = clear_entity_field(
        client,
        "name_embedding",
        "name_embedding = NONE, name_embedding_model = NONE, name_embedding_text_hash = NONE",
    )
    .await?;
    report.entity_metadata_cleared = clear_entity_field(
        client,
        "metadata.name_embedding",
        "metadata.name_embedding = NONE, metadata.name_embedding_model = NONE, \
         metadata.name_embedding_text_hash = NONE",
    )
    .await?;
    client
        .query_checked(&format!("UPSERT {MARKER} SET completed_at = time::now()"))
        .await?;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::ramp_condition;

    #[test]
    fn ramp_condition_checks_two_separated_windows_with_a_length_guard() {
        let condition = ramp_condition("vector");
        assert!(condition.contains("array::len(vector) > 104"));
        assert!(condition.contains("vector[1] - vector[0]"));
        assert!(condition.contains("vector[101] - vector[100]"));
    }
}
