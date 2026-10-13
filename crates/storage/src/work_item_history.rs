//! Targeted historical reads for document status, including archived work.

use std::collections::BTreeMap;

use elephant_types::records::{WorkItem, WorkItemArchive};

use super::{WorkItemDb, WorkItemRepository, WORK_ITEM_PROJECTION};
use crate::{StorageError, StorageResult};

impl WorkItemRepository {
    /// Finds work associated with a document, revision, or its source.
    ///
    /// Each lookup uses its payload index. Historical status must not depend
    /// on the first page of unrelated work in a space or on archival timing.
    /// Callers still select the relevant work kinds and rendition.
    ///
    /// # Errors
    ///
    /// Returns a storage or record-integrity error if the read fails.
    pub async fn find_document_work(
        &self,
        space_id: &str,
        doc_id: &str,
        revision_id: Option<&str>,
        source_id: Option<&str>,
    ) -> StorageResult<Vec<WorkItem>> {
        let matches = |item: &WorkItem| {
            item.base.space_id == space_id
                && item.base.deleted_at.is_none()
                && (item.payload["doc_id"].as_str() == Some(doc_id)
                    || revision_id
                        .is_some_and(|id| item.payload["revision_id"].as_str() == Some(id))
                    || source_id.is_some_and(|id| item.payload["source_id"].as_str() == Some(id)))
        };
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|tx| {
                    let mut items: BTreeMap<_, _> = tx
                        .scan::<WorkItemArchive>()?
                        .into_iter()
                        .filter(|archive| {
                            archive.base.deleted_at.is_none() && matches(&archive.record)
                        })
                        .map(|archive| (archive.original_id, archive.record))
                        .collect();
                    for item in tx.scan::<WorkItem>()? {
                        if matches(&item) {
                            items.insert(item.base.id.clone(), item);
                        }
                    }
                    Ok(items.into_values().collect())
                })
                .await;
        }

        let mut selectors = Vec::new();
        let mut bindings = crate::bindings! { "space" => space_id };
        for (field, value) in [
            ("doc_id", Some(doc_id)),
            ("revision_id", revision_id),
            ("source_id", source_id),
        ] {
            let Some(value) = value else {
                continue;
            };
            bindings.insert(field.to_string(), value.into());
            selectors.push(format!(
                "(SELECT * FROM work_item WHERE space_id = $space AND payload.{field} = ${field} AND deleted_at IS NONE)"
            ));
            selectors.push(format!(
                "(SELECT VALUE record FROM work_item_archive WHERE space_id = $space AND record.payload.{field} = ${field} AND deleted_at IS NONE AND record.deleted_at IS NONE)"
            ));
        }
        // A single query observes both tables at one transaction snapshot.
        let query = format!(
            "SELECT {WORK_ITEM_PROJECTION} FROM array::concat({})",
            selectors.join(", ")
        );
        let mut response = self.client.query_bind(&query, bindings).await?;
        let rows: Vec<WorkItemDb> = response
            .take(0)
            .map_err(|error| StorageError::QueryError(format!("Document work history: {error}")))?;
        let mut items = BTreeMap::new();
        for row in rows {
            let item = WorkItem::try_from(row)?;
            items.insert(item.base.id.clone(), item);
        }
        Ok(items.into_values().collect())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use crate::{Repository, StorageClient};
    use chrono::{Duration, Utc};
    use elephant_types::pipeline_status::{derive_extraction_state, ExtractionState};
    use elephant_types::records::base::BaseFields;
    use elephant_types::records::WorkItemStatus;
    use elephant_types::{RecordId, TableName};

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn history_after_archive(client: StorageClient) {
        let repo = WorkItemRepository::new(client.clone());
        let cutoff = Utc::now() - Duration::days(30);
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::WorkItem),
            "history".into(),
            "test".into(),
        );
        base.updated_at = cutoff - Duration::days(1);
        base.created_at = base.updated_at;
        let item = WorkItem {
            base,
            kind: "extract_assertions".into(),
            payload: serde_json::json!({"revision_id": "revision-current", "doc_id": "doc-current"}),
            status: WorkItemStatus::Done,
            priority: 0,
            idempotency_key: "history-current".into(),
            attempts: 1,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };
        let id = item.base.id.clone();
        repo.create(item.clone()).await.unwrap();
        let before = repo
            .find_document_work("history", "doc-current", Some("revision-current"), None)
            .await
            .unwrap();
        assert_eq!(
            before,
            vec![item.clone()],
            "multiple matching payload fields must not duplicate history"
        );
        let result = crate::work_retention::archive_terminal_work(&client, cutoff, 100)
            .await
            .unwrap();
        assert_eq!(result.items_archived, 1);
        let after = repo
            .find_document_work("history", "doc-current", Some("revision-current"), None)
            .await
            .unwrap();
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].base.id, id);
        assert_eq!(
            derive_extraction_state(0, &[after[0].status]),
            ExtractionState::Completed
        );
        assert!(repo
            .find_document_work("other-space", "doc-current", Some("revision-current"), None)
            .await
            .unwrap()
            .is_empty());
        assert!(repo
            .find_document_work("history", "doc-other", Some("revision-other"), None)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn archived_document_work_keeps_completed_state_surreal() {
        history_after_archive(StorageClient::new_memory().await.unwrap()).await;
    }

    #[tokio::test]
    async fn archived_document_work_keeps_completed_state_embedded() {
        history_after_archive(StorageClient::new_embedded().await.unwrap()).await;
    }
}
