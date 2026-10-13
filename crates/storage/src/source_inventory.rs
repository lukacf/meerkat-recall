//! Bounded, read-only paging over the canonical physical source-head inventory.
//!
//! This module owns no state. Authorization and external-identity projection are
//! adapter responsibilities; erased identity strings are never reconstructed.
use elephant_types::publication::SourceHead;
use serde::Deserialize;
use surrealdb::sql::{Id, Thing};

use crate::source_publication::{
    decode_payload, head_key, SourcePublicationRepository, HEAD_TABLE,
};
use crate::{StorageError, StorageResult};

/// One page in stable opaque physical-incarnation key order.
#[derive(Debug)]
pub struct SourceHeadPage {
    /// At most the requested number of physical heads, before authorization.
    pub heads: Vec<SourceHead>,
    /// Resume after this existing head key. None means no later row was observed.
    pub next_key: Option<String>,
}

/// Existing opaque physical key for a head already authorized by the adapter.
/// Never use a denied head's key as a consumer-visible continuation.
#[must_use]
pub fn source_head_cursor_key(head: &SourceHead) -> String {
    head_key(&head.space_id, &head.source_id)
}

impl SourcePublicationRepository {
    /// Read 1..100 physical heads, including retracted and erased incarnations.
    ///
    /// Pagination is not a snapshot. Concurrent insertions behind the cursor need
    /// a later full pass. Both backends select at most limit+1 payloads; callers
    /// must authorize every head and must not expose a denied-row count.
    pub async fn list_heads_page(
        &self,
        space: &str,
        after_key: Option<&str>,
        limit: usize,
    ) -> StorageResult<SourceHeadPage> {
        if !(1..=100).contains(&limit)
            || after_key.is_some_and(|key| uuid::Uuid::parse_str(key).is_err())
        {
            return Err(StorageError::TypeError(
                elephant_types::error::TypeError::ValidationError(
                    "Invalid source inventory page boundary".into(),
                ),
            ));
        }
        let rows = if let Some(store) = self.client.embedded_store() {
            let after = after_key.map(|key| format!("{HEAD_TABLE}:{key}"));
            store
                .extension_rows_page(HEAD_TABLE, space, after.as_deref(), limit + 1)
                .await?
                .into_iter()
                .map(|(key, row)| {
                    let key = key.strip_prefix("source_head:").ok_or_else(invalid_row)?;
                    let payload = row
                        .get("payload")
                        .and_then(serde_json::Value::as_str)
                        .ok_or_else(invalid_row)?;
                    Ok((key.to_owned(), decode_payload::<SourceHead>(payload)?))
                })
                .collect::<StorageResult<Vec<_>>>()?
        } else {
            #[derive(Deserialize)]
            struct HeadDb {
                id: Thing,
                payload: String,
            }
            let after = Thing::from((HEAD_TABLE, after_key.unwrap_or("")));
            let mut response = self.client.query_bind_checked(
                "SELECT id, payload FROM source_head WHERE space_id = $space AND ($first OR id > $after) ORDER BY id LIMIT $limit",
                crate::bindings! { "space" => space, "first" => after_key.is_none(), "after" => after, "limit" => i64::try_from(limit + 1).map_err(|_| invalid_row())? },
            ).await?;
            let rows: Vec<HeadDb> = response
                .take(0)
                .map_err(|error| StorageError::DataIntegrityError(error.to_string()))?;
            rows.into_iter()
                .map(|row| {
                    let Id::String(key) = row.id.id else {
                        return Err(invalid_row());
                    };
                    if row.id.tb != HEAD_TABLE {
                        return Err(invalid_row());
                    }
                    Ok((key, decode_payload::<SourceHead>(&row.payload)?))
                })
                .collect::<StorageResult<Vec<_>>>()?
        };
        finish_page(space, after_key, limit, rows)
    }
}

fn invalid_row() -> StorageError {
    StorageError::DataIntegrityError("Invalid canonical source inventory row".into())
}

fn finish_page(
    space: &str,
    after_key: Option<&str>,
    limit: usize,
    mut rows: Vec<(String, SourceHead)>,
) -> StorageResult<SourceHeadPage> {
    if rows.len() > limit + 1 {
        return Err(invalid_row());
    }
    let mut previous = after_key;
    for (key, head) in &rows {
        if head.space_id != space
            || head_key(space, &head.source_id) != *key
            || previous.is_some_and(|previous| key.as_str() <= previous)
        {
            return Err(invalid_row());
        }
        previous = Some(key);
    }
    let has_more = rows.len() > limit;
    rows.truncate(limit);
    let next_key = if has_more {
        rows.last().map(|(key, _)| key.clone())
    } else {
        None
    };
    Ok(SourceHeadPage {
        heads: rows.into_iter().map(|(_, head)| head).collect(),
        next_key,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use elephant_types::{publication::SourceVisibility, RecordId, TableName};

    fn row(space: &str) -> (String, SourceHead) {
        let source_id = RecordId::new_v4(TableName::Source);
        (
            head_key(space, &source_id),
            SourceHead {
                space_id: space.into(),
                source_id,
                generation: "generation".into(),
                desired_revision: None,
                published_revision: None,
                operation_id: "operation".into(),
                lifecycle: SourceVisibility::Erased,
                updated_at: chrono::Utc::now(),
            },
        )
    }

    #[test]
    fn paging_preserves_erased_heads_without_external_identity() -> StorageResult<()> {
        let mut rows = vec![row("alpha"), row("alpha"), row("alpha")];
        rows.sort_by(|left, right| left.0.cmp(&right.0));
        let page = finish_page("alpha", None, 2, rows.clone())?;
        assert_eq!(page.heads.len(), 2);
        assert_eq!(page.next_key.as_deref(), Some(rows[1].0.as_str()));
        assert!(page
            .heads
            .iter()
            .all(|head| head.lifecycle == SourceVisibility::Erased));
        let tail = finish_page("alpha", page.next_key.as_deref(), 2, vec![rows[2].clone()])?;
        assert_eq!(tail.heads.len(), 1);
        assert!(tail.next_key.is_none());
        Ok(())
    }

    #[test]
    fn corrupt_tenant_key_and_nonprogressing_pages_are_rejected() {
        let valid = row("alpha");
        assert!(finish_page("beta", None, 2, vec![valid.clone()]).is_err());
        assert!(finish_page("alpha", Some(&valid.0), 2, vec![valid.clone()]).is_err());
        let mut corrupt = valid;
        corrupt.0 = uuid::Uuid::new_v4().to_string();
        assert!(finish_page("alpha", None, 2, vec![corrupt]).is_err());
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep client, repository in the original fixture scope through assertions and cleanup."
    )]
    async fn storage_pages(embedded: bool) -> StorageResult<()> {
        let client = if embedded {
            crate::StorageClient::new_embedded().await?
        } else {
            crate::StorageClient::new(crate::StorageConfig::default()).await?
        };
        let mut rows = vec![row("alpha"), row("alpha"), row("beta")];
        rows.sort_by(|left, right| left.0.cmp(&right.0));
        for (key, head) in &rows {
            if let Some(store) = client.embedded_store() {
                store
                    .mutate(|tx| {
                        crate::source_publication::write_control_tx(
                            tx,
                            HEAD_TABLE,
                            key,
                            &head.space_id,
                            &head.source_id,
                            head,
                        )
                    })
                    .await?;
            } else {
                let id = Thing::from((HEAD_TABLE, key.as_str()));
                let payload = serde_json::to_string(head)
                    .map_err(|error| StorageError::SerializationError(error.to_string()))?;
                client
                    .query_bind_checked(
                        "CREATE $id SET space_id = $space, source_id = $source, payload = $payload",
                        crate::bindings! { "id" => id, "space" => head.space_id.as_str(),
                        "source" => head.source_id.to_string(), "payload" => payload },
                    )
                    .await?;
            }
        }
        let repository = SourcePublicationRepository::new(client);
        let first = repository.list_heads_page("alpha", None, 1).await?;
        assert_eq!(first.heads.len(), 1);
        assert!(first.next_key.is_some());
        let second = repository
            .list_heads_page("alpha", first.next_key.as_deref(), 1)
            .await?;
        assert_eq!(second.heads.len(), 1);
        assert!(second.next_key.is_none());
        assert_ne!(first.heads[0].source_id, second.heads[0].source_id);
        assert!(repository
            .list_heads_page("absent", None, 1)
            .await?
            .heads
            .is_empty());
        assert!(repository
            .list_heads_page("alpha", None, 101)
            .await
            .is_err());
        Ok(())
    }

    #[tokio::test]
    async fn embedded_inventory_pages() -> StorageResult<()> {
        storage_pages(true).await
    }
    #[tokio::test]
    async fn surreal_memory_inventory_pages() -> StorageResult<()> {
        storage_pages(false).await
    }
}
