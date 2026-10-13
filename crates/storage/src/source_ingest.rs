//! Shared helpers for source-first manual ingestion flows.

use elephant_types::records::{deterministic_source_ingest_id, Doc, Source};
use elephant_types::Link;

use crate::client::StorageClient;
use crate::error::{StorageError, StorageResult};
use crate::{DocRepository, Repository, SourceRepository};

fn with_source_link(mut doc: Doc, source: &Source) -> Doc {
    doc.source = Some(Link::new(source.base.id.clone()));
    doc
}

fn preserve_immutable_doc_base_fields(existing: &Doc, mut incoming: Doc) -> Doc {
    incoming.base.id = existing.base.id.clone();
    incoming.base.space_id.clone_from(&existing.base.space_id);
    incoming.base.created_at = existing.base.created_at;
    incoming
        .base
        .created_by
        .clone_from(&existing.base.created_by);
    incoming.base.deleted_at = None;
    incoming
}

fn matches_source_reingest_key(existing: &Source, incoming: &Source) -> bool {
    existing.hash == incoming.hash && existing.dedup_key() == incoming.dedup_key()
}

async fn update_existing_doc_after_restore_if_needed(
    repo: &DocRepository,
    existing: &Doc,
    updated: Doc,
) -> StorageResult<Doc> {
    let space_id = existing.base.space_id.clone();
    let doc_id = existing.base.id.clone();
    if existing.base.deleted_at.is_some() {
        repo.restore_and_replace_existing(&space_id, &doc_id, updated)
            .await
    } else {
        repo.update(&space_id, &doc_id, updated).await
    }
}

/// Updates an existing source during re-ingest, restoring tombstoned sources
/// atomically when needed.
///
/// # Errors
///
/// Returns an error if the restore/update sequence fails.
pub async fn update_existing_source_for_reingest(
    repo: &SourceRepository,
    existing: &Source,
    updated: Source,
) -> StorageResult<Source> {
    let space_id = existing.base.space_id.clone();
    let source_id = existing.base.id.clone();
    if existing.base.deleted_at.is_some() {
        repo.restore_and_replace_existing(&space_id, &source_id, updated)
            .await
    } else {
        repo.update(&space_id, &source_id, updated).await
    }
}

async fn ensure_existing_doc_can_be_relinked(
    storage: &StorageClient,
    existing: &Doc,
    source: &Source,
) -> StorageResult<()> {
    if let Some(existing_source) = existing.source.as_ref() {
        if existing_source.id() != &source.base.id {
            let linked_source = SourceRepository::new(storage.clone())
                .get_with_deleted(&existing.base.space_id, existing_source.id())
                .await?;
            let linked_source_is_live = linked_source
                .as_ref()
                .is_some_and(|row| row.base.deleted_at.is_none());
            if linked_source_is_live {
                return Err(StorageError::Conflict(format!(
                    "document {} is already linked to source {}",
                    existing.base.id,
                    existing_source.id()
                )));
            }
        }
    }
    Ok(())
}

/// Finds a reusable live source for a source-first ingest request without a
/// staged manual doc.
///
/// # Errors
///
/// Returns an error if the source lookup fails.
pub async fn find_existing_source_for_source(
    storage: &StorageClient,
    source: &Source,
) -> StorageResult<Option<Source>> {
    let repo = SourceRepository::new(storage.clone());
    if let Some(existing) = repo
        .get_with_deleted(&source.base.space_id, &source.base.id)
        .await?
        .filter(|existing| matches_source_reingest_key(existing, source))
    {
        return Ok(Some(existing));
    }
    let mut matches = repo
        .find_all_by_hash_in_space_with_deleted(&source.base.space_id, &source.hash)
        .await?
        .into_iter()
        .filter(|existing| matches_source_reingest_key(existing, source))
        .collect::<Vec<_>>();

    drop(repo);
    matches.sort_by_key(|existing| {
        (
            existing.base.deleted_at.is_some(),
            existing.base.id.to_string(),
        )
    });
    Ok(matches.into_iter().next())
}

/// Finds the persisted live document for a manual ingest request, if any.
///
/// # Errors
///
/// Returns an error if the document lookup fails.
pub async fn find_existing_doc_for_doc(
    storage: &StorageClient,
    doc: &Doc,
) -> StorageResult<Option<Doc>> {
    DocRepository::new(storage.clone())
        .get_with_deleted(&doc.base.space_id, &doc.base.id)
        .await
}

/// Result of persisting a staged manual document.
#[derive(Debug, Clone)]
pub struct PersistedManualDoc {
    /// The stored document after create/update.
    pub doc: Doc,
    /// The previous live document snapshot, if one existed.
    pub previous: Option<Doc>,
}

/// Finds the live source currently linked to a persisted document, if any.
///
/// # Errors
///
/// Returns an error if the document or source lookup fails.
pub async fn find_existing_source_for_doc(
    storage: &StorageClient,
    doc: &Doc,
) -> StorageResult<Option<Source>> {
    let source_repo = SourceRepository::new(storage.clone());
    let Some(existing_doc) = find_existing_doc_for_doc(storage, doc).await? else {
        return Ok(None);
    };
    let Some(source_link) = existing_doc.source else {
        return Ok(None);
    };

    source_repo
        .get_with_deleted(&existing_doc.base.space_id, source_link.id())
        .await
}

/// Returns the deterministic source ID used for source-first ingest retries
/// that do not carry a staged manual document.
#[must_use]
pub fn deterministic_source_id_for_ingest(source: &Source) -> elephant_types::RecordId {
    deterministic_source_ingest_id(source)
}

/// Persists a staged manual document and links it to its driving source.
///
/// This helper is resilient to the common create-vs-update race:
/// - update succeeds when the doc already exists,
/// - create succeeds when the doc is new,
/// - if create loses a race, it retries as update.
pub async fn persist_manual_doc_for_source(
    storage: &StorageClient,
    source: &Source,
    doc: Doc,
) -> StorageResult<Doc> {
    persist_manual_doc_for_source_detailed(storage, source, doc)
        .await
        .map(|result| result.doc)
}

/// Builds the exact doc row that would be persisted for a staged manual doc.
///
/// This is useful for authorization checks that must evaluate the final target
/// document state before a create/update is attempted.
///
/// # Errors
///
/// Returns an error if the existing persisted doc cannot be relinked to the
/// provided source.
pub async fn preview_persisted_manual_doc_for_source(
    storage: &StorageClient,
    source: &Source,
    doc: &Doc,
) -> StorageResult<Doc> {
    let repo = DocRepository::new(storage.clone());
    let doc = with_source_link(doc.clone(), source);

    let existing = repo
        .get_with_deleted(&doc.base.space_id, &doc.base.id)
        .await?;
    drop(repo);
    if let Some(existing) = existing {
        ensure_existing_doc_can_be_relinked(storage, &existing, source).await?;
        return Ok(preserve_immutable_doc_base_fields(&existing, doc));
    }

    Ok(doc)
}

/// Persists a staged manual document and returns rollback metadata.
///
/// # Errors
///
/// Returns an error if the document cannot be stored or if it is already linked
/// to a different live source.
pub async fn persist_manual_doc_for_source_detailed(
    storage: &StorageClient,
    source: &Source,
    doc: Doc,
) -> StorageResult<PersistedManualDoc> {
    let repo = DocRepository::new(storage.clone());
    let doc_id = doc.base.id.clone();
    let space_id = doc.base.space_id.clone();

    if let Some(existing) = repo.get_with_deleted(&space_id, &doc_id).await? {
        ensure_existing_doc_can_be_relinked(storage, &existing, source).await?;
        let updated = preserve_immutable_doc_base_fields(&existing, with_source_link(doc, source));
        let persisted =
            update_existing_doc_after_restore_if_needed(&repo, &existing, updated).await?;
        drop(repo);
        return Ok(PersistedManualDoc {
            doc: persisted,
            previous: Some(existing),
        });
    }

    let doc = with_source_link(doc, source);
    match repo.create(doc.clone()).await {
        Ok(created) => {
            drop(repo);
            Ok(PersistedManualDoc {
                doc: created,
                previous: None,
            })
        }
        Err(StorageError::Conflict(message)) => {
            let Some(existing) = repo.get_with_deleted(&space_id, &doc_id).await? else {
                return Err(StorageError::Conflict(message));
            };
            ensure_existing_doc_can_be_relinked(storage, &existing, source).await?;
            let updated = preserve_immutable_doc_base_fields(&existing, doc);
            let persisted =
                update_existing_doc_after_restore_if_needed(&repo, &existing, updated).await?;
            drop(repo);
            Ok(PersistedManualDoc {
                doc: persisted,
                previous: Some(existing),
            })
        }
        Err(err) => Err(err),
    }
}

/// Rolls back a staged manual document persistence operation.
///
/// If the document existed before staging, it is restored. Otherwise the newly
/// staged document is hard-deleted.
///
/// # Errors
///
/// Returns an error if rollback cannot be completed.
pub async fn rollback_persisted_manual_doc(
    storage: &StorageClient,
    staged: &PersistedManualDoc,
) -> StorageResult<()> {
    let repo = DocRepository::new(storage.clone());
    if let Some(previous) = staged.previous.clone() {
        let space_id = previous.base.space_id.clone();
        let id = previous.base.id.clone();
        repo.replace_existing_snapshot(&space_id, &id, previous)
            .await?;
        return Ok(());
    }
    repo.hard_delete(&staged.doc.base.space_id, &staged.doc.base.id)
        .await
}

/// Rolls back a staged source persistence operation.
///
/// If the source existed before staging, the exact prior snapshot is restored,
/// including a soft-deleted state. Otherwise the newly staged source is
/// hard-deleted.
///
/// # Errors
///
/// Returns an error if rollback cannot be completed.
pub async fn rollback_persisted_source(
    storage: &StorageClient,
    staged: &Source,
    previous: Option<&Source>,
) -> StorageResult<()> {
    let repo = SourceRepository::new(storage.clone());
    if let Some(previous) = previous {
        repo.replace_existing_snapshot(
            &previous.base.space_id,
            &previous.base.id,
            previous.clone(),
        )
        .await?;
        return Ok(());
    }
    repo.hard_delete(&staged.base.space_id, &staged.base.id)
        .await
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::too_many_lines,
        clippy::cognitive_complexity
    )]

    use super::*;
    use elephant_types::ids::TableName;
    use elephant_types::records::base::BaseFields;
    use elephant_types::records::{Doc, Source, SourceType};
    use elephant_types::RecordId;

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
    )]
    async fn persist_manual_doc_for_source_creates_and_links_doc() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "test_space".to_string(),
                "tester".to_string(),
            ),
            SourceType::Manual,
            "hash".to_string(),
        );
        let doc = Doc {
            base: BaseFields::new(
                RecordId::new_v4(TableName::Doc),
                "test_space".to_string(),
                "tester".to_string(),
            ),
            title: "Hello".to_string(),
            text: "World".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "manual".to_string(),
            metadata: serde_json::json!({}),
        };

        let persisted = persist_manual_doc_for_source(&storage, &source, doc)
            .await
            .expect("persist doc");

        assert_eq!(
            persisted.source.as_ref().map(|link| link.id().to_string()),
            Some(source.base.id.to_string())
        );
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
    )]
    async fn persist_manual_doc_for_source_updates_existing_doc() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "test_space".to_string(),
                "tester".to_string(),
            ),
            SourceType::Manual,
            "hash".to_string(),
        );
        let doc_id = RecordId::new_v4(TableName::Doc);
        let original = Doc {
            base: BaseFields::new(
                doc_id.clone(),
                "test_space".to_string(),
                "tester".to_string(),
            ),
            title: "Original".to_string(),
            text: "Old text".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "manual".to_string(),
            metadata: serde_json::json!({}),
        };
        let original_created_at = original.base.created_at;
        DocRepository::new(storage.clone())
            .create(original)
            .await
            .expect("create doc");

        let replacement = Doc {
            base: BaseFields::new(doc_id, "test_space".to_string(), "tester".to_string()),
            title: "Updated".to_string(),
            text: "New text".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "manual".to_string(),
            metadata: serde_json::json!({"updated": true}),
        };

        let persisted = persist_manual_doc_for_source(&storage, &source, replacement)
            .await
            .expect("persist replacement");

        assert_eq!(persisted.title, "Updated");
        assert_eq!(persisted.base.created_by, "tester");
        assert_eq!(persisted.base.created_at, original_created_at);
        assert_eq!(
            persisted.source.as_ref().map(|link| link.id().to_string()),
            Some(source.base.id.to_string())
        );
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
    )]
    async fn persist_manual_doc_for_source_preserves_immutable_base_fields() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "test_space".to_string(),
                "tester".to_string(),
            ),
            SourceType::Manual,
            "hash".to_string(),
        );
        let doc_id = RecordId::new_v4(TableName::Doc);
        let original = Doc {
            base: BaseFields::new(
                doc_id.clone(),
                "test_space".to_string(),
                "original-author".to_string(),
            ),
            title: "Original".to_string(),
            text: "Old text".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "manual".to_string(),
            metadata: serde_json::json!({}),
        };
        let mut original = original;
        original.base.security.level = 4;
        let provenance_source = RecordId::new_v4(TableName::Source);
        original.base.provenance.sources = vec![provenance_source.clone()];
        let subject_id = RecordId::new_v4(TableName::Entity);
        original.base.subjects = vec![Link::new(subject_id.clone())];
        let original_created_at = original.base.created_at;
        DocRepository::new(storage.clone())
            .create(original)
            .await
            .expect("create doc");

        let replacement = Doc {
            base: BaseFields::new(
                doc_id,
                "test_space".to_string(),
                "replacement-author".to_string(),
            ),
            title: "Updated".to_string(),
            text: "New text".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "manual".to_string(),
            metadata: serde_json::json!({"updated": true}),
        };
        let mut replacement = replacement;
        let replacement_provenance_source = RecordId::new_v4(TableName::Source);
        let replacement_subject_id = RecordId::new_v4(TableName::Entity);
        replacement.base.security.level = 9;
        replacement.base.provenance.sources = vec![replacement_provenance_source.clone()];
        replacement.base.subjects = vec![Link::new(replacement_subject_id.clone())];

        let persisted = persist_manual_doc_for_source(&storage, &source, replacement)
            .await
            .expect("persist replacement");

        assert_eq!(persisted.base.created_by, "original-author");
        assert_eq!(persisted.base.created_at, original_created_at);
        assert_eq!(persisted.base.security.level, 9);
        assert_eq!(
            persisted.base.provenance.sources,
            vec![replacement_provenance_source]
        );
        assert_eq!(
            persisted.base.subjects,
            vec![Link::new(replacement_subject_id)]
        );
        assert_eq!(persisted.title, "Updated");
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
    )]
    async fn find_existing_source_for_doc_returns_linked_source() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "test_space".to_string(),
                "tester".to_string(),
            ),
            SourceType::Manual,
            "hash".to_string(),
        );
        SourceRepository::new(storage.clone())
            .create(source.clone())
            .await
            .expect("create source");
        let doc = Doc {
            base: BaseFields::new(
                RecordId::new_v4(TableName::Doc),
                "test_space".to_string(),
                "tester".to_string(),
            ),
            title: "Hello".to_string(),
            text: "World".to_string(),
            sidecar_text: None,
            source: Some(Link::new(source.base.id.clone())),
            media_attachments: Vec::new(),
            rendition_key: "manual".to_string(),
            metadata: serde_json::json!({}),
        };
        DocRepository::new(storage.clone())
            .create(doc.clone())
            .await
            .expect("create doc");

        let found = find_existing_source_for_doc(&storage, &doc)
            .await
            .expect("lookup source")
            .expect("source");

        assert_eq!(found.base.id, source.base.id);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
    )]
    async fn find_existing_source_for_source_matches_dedup_key() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let mut existing = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "test_space".to_string(),
                "tester".to_string(),
            ),
            SourceType::Manual,
            "hash".to_string(),
        );
        existing.external_id = Some("same-source".to_string());
        SourceRepository::new(storage.clone())
            .create(existing.clone())
            .await
            .expect("create source");

        let mut incoming = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "test_space".to_string(),
                "tester".to_string(),
            ),
            SourceType::Manual,
            "hash".to_string(),
        );
        incoming.external_id = Some("same-source".to_string());

        let found = find_existing_source_for_source(&storage, &incoming)
            .await
            .expect("lookup source")
            .expect("source");

        assert_eq!(found.base.id, existing.base.id);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
    )]
    async fn find_existing_source_for_source_returns_soft_deleted_match() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let mut existing = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "test_space".to_string(),
                "tester".to_string(),
            ),
            SourceType::Web,
            "hash".to_string(),
        );
        existing.external_id = Some("same-source".to_string());
        let repo = SourceRepository::new(storage.clone());
        repo.create(existing.clone()).await.expect("create source");
        repo.delete("test_space", &existing.base.id)
            .await
            .expect("delete source");

        let incoming = existing.clone();
        let found = find_existing_source_for_source(&storage, &incoming)
            .await
            .expect("lookup source")
            .expect("source");

        assert_eq!(found.base.id, existing.base.id);
        assert!(found.base.deleted_at.is_some());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
    )]
    async fn find_existing_source_for_source_returns_soft_deleted_hash_match_with_different_id() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let mut existing = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "test_space".to_string(),
                "tester".to_string(),
            ),
            SourceType::Web,
            "hash".to_string(),
        );
        existing.external_id = Some("same-source".to_string());
        let repo = SourceRepository::new(storage.clone());
        repo.create(existing.clone()).await.expect("create source");
        repo.delete("test_space", &existing.base.id)
            .await
            .expect("delete source");

        let mut incoming = existing.clone();
        incoming.base.id = RecordId::new_v4(TableName::Source);

        let found = find_existing_source_for_source(&storage, &incoming)
            .await
            .expect("lookup source")
            .expect("source");

        assert_eq!(found.base.id, existing.base.id);
        assert!(found.base.deleted_at.is_some());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
    )]
    async fn persist_manual_doc_for_source_can_relink_from_deleted_source() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let old_source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "test_space".to_string(),
                "tester".to_string(),
            ),
            SourceType::Manual,
            "old-hash".to_string(),
        );
        let new_source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "test_space".to_string(),
                "tester".to_string(),
            ),
            SourceType::Manual,
            "new-hash".to_string(),
        );
        let source_repo = SourceRepository::new(storage.clone());
        source_repo
            .create(old_source.clone())
            .await
            .expect("create old source");
        source_repo
            .create(new_source.clone())
            .await
            .expect("create new source");
        source_repo
            .delete("test_space", &old_source.base.id)
            .await
            .expect("delete old source");

        let doc_id = RecordId::new_v4(TableName::Doc);
        let existing = Doc {
            base: BaseFields::new(
                doc_id.clone(),
                "test_space".to_string(),
                "tester".to_string(),
            ),
            title: "Original".to_string(),
            text: "Old text".to_string(),
            sidecar_text: None,
            source: Some(Link::new(old_source.base.id.clone())),
            media_attachments: Vec::new(),
            rendition_key: "manual".to_string(),
            metadata: serde_json::json!({}),
        };
        DocRepository::new(storage.clone())
            .create(existing)
            .await
            .expect("create doc");

        let replacement = Doc {
            base: BaseFields::new(doc_id, "test_space".to_string(), "tester".to_string()),
            title: "Updated".to_string(),
            text: "New text".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "manual".to_string(),
            metadata: serde_json::json!({"updated": true}),
        };

        let persisted = persist_manual_doc_for_source(&storage, &new_source, replacement)
            .await
            .expect("persist replacement");

        assert_eq!(
            persisted.source.as_ref().map(|link| link.id().to_string()),
            Some(new_source.base.id.to_string())
        );
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
    )]
    async fn persist_manual_doc_for_source_restores_soft_deleted_doc() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "test_space".to_string(),
                "tester".to_string(),
            ),
            SourceType::Manual,
            "hash".to_string(),
        );
        let source_repo = SourceRepository::new(storage.clone());
        source_repo
            .create(source.clone())
            .await
            .expect("create source");

        let doc_id = RecordId::new_v4(TableName::Doc);
        let original = Doc {
            base: BaseFields::new(
                doc_id.clone(),
                "test_space".to_string(),
                "tester".to_string(),
            ),
            title: "Original".to_string(),
            text: "Old text".to_string(),
            sidecar_text: None,
            source: Some(Link::new(source.base.id.clone())),
            media_attachments: Vec::new(),
            rendition_key: "manual".to_string(),
            metadata: serde_json::json!({}),
        };
        let repo = DocRepository::new(storage.clone());
        repo.create(original).await.expect("create doc");
        repo.delete("test_space", &doc_id)
            .await
            .expect("delete doc");

        let replacement = Doc {
            base: BaseFields::new(
                doc_id.clone(),
                "test_space".to_string(),
                "new-tester".to_string(),
            ),
            title: "Updated".to_string(),
            text: "New text".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "manual".to_string(),
            metadata: serde_json::json!({"updated": true}),
        };

        let persisted = persist_manual_doc_for_source(&storage, &source, replacement)
            .await
            .expect("restore replacement");

        assert_eq!(persisted.base.id, doc_id);
        assert!(persisted.base.deleted_at.is_none());
        assert_eq!(persisted.base.created_by, "tester");
        assert_eq!(persisted.title, "Updated");
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
    )]
    async fn persist_manual_doc_for_source_redeletes_doc_when_restore_update_fails() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "test_space".to_string(),
                "tester".to_string(),
            ),
            SourceType::Manual,
            "hash".to_string(),
        );
        let source_repo = SourceRepository::new(storage.clone());
        source_repo
            .create(source.clone())
            .await
            .expect("create source");

        let conflicting = Doc {
            base: BaseFields::new(
                RecordId::new_v4(TableName::Doc),
                "test_space".to_string(),
                "tester".to_string(),
            ),
            title: "Existing".to_string(),
            text: "Conflict".to_string(),
            sidecar_text: None,
            source: Some(Link::new(source.base.id.clone())),
            media_attachments: Vec::new(),
            rendition_key: "dup".to_string(),
            metadata: serde_json::json!({}),
        };
        let doc_repo = DocRepository::new(storage.clone());
        doc_repo
            .create(conflicting)
            .await
            .expect("create conflicting doc");

        let doc_id = RecordId::new_v4(TableName::Doc);
        let deleted = Doc {
            base: BaseFields::new(
                doc_id.clone(),
                "test_space".to_string(),
                "tester".to_string(),
            ),
            title: "Deleted".to_string(),
            text: "Old text".to_string(),
            sidecar_text: None,
            source: Some(Link::new(source.base.id.clone())),
            media_attachments: Vec::new(),
            rendition_key: "old".to_string(),
            metadata: serde_json::json!({}),
        };
        doc_repo.create(deleted).await.expect("create deleted doc");
        doc_repo
            .delete("test_space", &doc_id)
            .await
            .expect("delete doc");

        let replacement = Doc {
            base: BaseFields::new(
                doc_id.clone(),
                "test_space".to_string(),
                "new-tester".to_string(),
            ),
            title: "Updated".to_string(),
            text: "New text".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "dup".to_string(),
            metadata: serde_json::json!({"updated": true}),
        };

        let err = persist_manual_doc_for_source(&storage, &source, replacement)
            .await
            .expect_err("conflicting rendition key should fail");
        assert!(matches!(err, StorageError::Conflict(_)));

        let restored = doc_repo
            .get_with_deleted("test_space", &doc_id)
            .await
            .expect("load doc")
            .expect("doc should still exist");
        assert!(restored.base.deleted_at.is_some());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
    )]
    async fn rollback_persisted_manual_doc_restores_deleted_snapshot() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "test_space".to_string(),
                "tester".to_string(),
            ),
            SourceType::Manual,
            "hash".to_string(),
        );
        let source_repo = SourceRepository::new(storage.clone());
        source_repo
            .create(source.clone())
            .await
            .expect("create source");

        let doc_id = RecordId::new_v4(TableName::Doc);
        let doc_repo = DocRepository::new(storage.clone());
        let original = Doc {
            base: BaseFields::new(
                doc_id.clone(),
                "test_space".to_string(),
                "tester".to_string(),
            ),
            title: "Original".to_string(),
            text: "Old text".to_string(),
            sidecar_text: None,
            source: Some(Link::new(source.base.id.clone())),
            media_attachments: Vec::new(),
            rendition_key: "manual".to_string(),
            metadata: serde_json::json!({}),
        };
        doc_repo.create(original).await.expect("create doc");
        doc_repo
            .delete("test_space", &doc_id)
            .await
            .expect("delete doc");
        let previous = doc_repo
            .get_with_deleted("test_space", &doc_id)
            .await
            .expect("load previous")
            .expect("deleted snapshot");

        let replacement = Doc {
            base: BaseFields::new(
                doc_id.clone(),
                "test_space".to_string(),
                "new-tester".to_string(),
            ),
            title: "Updated".to_string(),
            text: "New text".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "manual".to_string(),
            metadata: serde_json::json!({"updated": true}),
        };
        let staged = persist_manual_doc_for_source_detailed(&storage, &source, replacement)
            .await
            .expect("stage replacement");
        assert!(staged.doc.base.deleted_at.is_none());

        rollback_persisted_manual_doc(&storage, &staged)
            .await
            .expect("rollback doc");

        let restored = doc_repo
            .get_with_deleted("test_space", &doc_id)
            .await
            .expect("load restored")
            .expect("restored snapshot");
        assert_eq!(restored.title, previous.title);
        assert_eq!(restored.text, previous.text);
        assert_eq!(restored.base.created_by, previous.base.created_by);
        assert_eq!(restored.base.deleted_at, previous.base.deleted_at);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
    )]
    async fn rollback_persisted_source_restores_deleted_snapshot() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let source_repo = SourceRepository::new(storage.clone());
        let source_id = RecordId::new_v4(TableName::Source);
        let original = Source::new(
            BaseFields::new(
                source_id.clone(),
                "test_space".to_string(),
                "tester".to_string(),
            ),
            SourceType::Manual,
            "hash".to_string(),
        );
        source_repo.create(original).await.expect("create source");
        source_repo
            .delete("test_space", &source_id)
            .await
            .expect("delete source");
        let previous = source_repo
            .get_with_deleted("test_space", &source_id)
            .await
            .expect("load previous")
            .expect("deleted source");

        let updated = Source::new(
            BaseFields::new(
                source_id.clone(),
                "test_space".to_string(),
                "new-tester".to_string(),
            ),
            SourceType::Manual,
            "hash".to_string(),
        );
        let staged = update_existing_source_for_reingest(&source_repo, &previous, updated)
            .await
            .expect("stage source");
        assert!(staged.base.deleted_at.is_none());

        rollback_persisted_source(&storage, &staged, Some(&previous))
            .await
            .expect("rollback source");

        let restored = source_repo
            .get_with_deleted("test_space", &source_id)
            .await
            .expect("load restored")
            .expect("restored source");
        assert_eq!(restored.hash, previous.hash);
        assert_eq!(restored.base.created_by, previous.base.created_by);
        assert_eq!(restored.base.deleted_at, previous.base.deleted_at);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
    )]
    async fn preview_persisted_manual_doc_for_source_preserves_immutable_base_fields() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "test_space".to_string(),
                "tester".to_string(),
            ),
            SourceType::Manual,
            "hash".to_string(),
        );
        let doc_id = RecordId::new_v4(TableName::Doc);
        let created_at = chrono::Utc::now() - chrono::Duration::days(1);
        let existing = Doc {
            base: BaseFields {
                id: doc_id.clone(),
                space_id: "test_space".to_string(),
                created_at,
                updated_at: created_at,
                deleted_at: None,
                created_by: "original".to_string(),
                security: elephant_types::records::base::Security::default(),
                provenance: elephant_types::records::base::Provenance::default(),
                subjects: Vec::new(),
                subjects_state: elephant_types::records::base::SubjectsState::default(),
            },
            title: "Old".to_string(),
            text: "Old body".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "manual".to_string(),
            metadata: serde_json::json!({}),
        };
        let incoming = Doc {
            base: BaseFields::new(doc_id, "test_space".to_string(), "new-actor".to_string()),
            title: "New".to_string(),
            text: "New body".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "manual".to_string(),
            metadata: serde_json::json!({"updated": true}),
        };
        let repo = DocRepository::new(storage.clone());
        repo.create(existing.clone())
            .await
            .expect("create existing doc");

        let preview = preview_persisted_manual_doc_for_source(&storage, &source, &incoming)
            .await
            .expect("preview doc");

        assert_eq!(preview.base.id, existing.base.id);
        assert_eq!(preview.base.created_at, existing.base.created_at);
        assert_eq!(preview.base.created_by, existing.base.created_by);
        assert_eq!(preview.base.space_id, existing.base.space_id);
        assert_eq!(preview.title, "New");
        assert_eq!(preview.text, "New body");
        assert_eq!(
            preview.source.as_ref().map(|link| link.id().to_string()),
            Some(source.base.id.to_string())
        );
    }
}
